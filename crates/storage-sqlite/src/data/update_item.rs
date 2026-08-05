// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` for the SQLite backend.
//!
//! UpdateItem is an upsert: when the item is absent a new one is created from
//! the key plus the applied actions. The condition is evaluated against the
//! pre-update image (or an empty item), then update actions are applied and the
//! result is validated and written — all under the engine write lock (D1).

use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;

use super::index::{enqueue_async_indexes, fetch_indexes_for_table, sync_indexes};
use super::query::check_condition;
use super::transactions::index_key_refs;
use super::tx_helpers::{fetch_item_in_tx, upsert_item_in_tx, write_stream_record_in_tx};
use crate::store::SqliteEngine;

impl SqliteEngine {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn update_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<(Option<Item>, Option<Item>), StorageError> {
        let _writer = self.write_lock.lock().await;
        // Read the index set after acquiring the write lock so a concurrently
        // added GSI (UpdateTable holds the same lock) is not missed.
        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;
        let system_delay = self.gsi_default_delay();

        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let old = fetch_item_in_tx(&mut tx, key_info, key).await?;

        if condition.is_some() {
            let empty = Item::new();
            let target = old.as_ref().unwrap_or(&empty);
            if let Err(e) = check_condition(condition, target, maps) {
                return match e {
                    StorageError::ConditionFailed(_) => Err(StorageError::ConditionFailed(old)),
                    other => Err(other),
                };
            }
        }

        // Start from the existing image, or from the key for a fresh upsert.
        let mut item = old.clone().unwrap_or_else(|| key.clone());
        expression::apply_update(actions, &mut item, maps)
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        validation::validate_item_size(&item, self.max_item_size_bytes)
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        // Secondary-index key validation on the post-update item, matching the
        // TransactWriteItems update path: a wrong-typed index key attribute or
        // an index key set to an empty value is a ValidationException up front,
        // rather than silently producing a malformed / unmatchable index row.
        if !indexes.is_empty() {
            let idx_refs = index_key_refs(&indexes);
            validation::validate_index_key_types(&item, &idx_refs, &key_info.attribute_definitions)
                .map_err(|e| StorageError::Validation(e.to_string()))?;
            validation::validate_index_key_not_empty(
                &item,
                &idx_refs,
                validation::SecondaryIndexEmptyContext::UpdateExpression,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }

        upsert_item_in_tx(&mut tx, key_info, &item).await?;

        if !indexes.is_empty() {
            sync_indexes(
                &mut tx,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &indexes,
                old.as_ref(),
                Some(&item),
                system_delay,
            )
            .await?;
        }
        let enqueued = enqueue_async_indexes(
            &mut tx,
            key_info,
            &indexes,
            old.as_ref(),
            Some(&item),
            system_delay,
        )
        .await?
            > 0;

        if let Some(capture) = stream {
            write_stream_record_in_tx(&mut tx, key_info, capture, old.as_ref(), Some(&item))
                .await?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if enqueued {
            self.gsi_notify.notify_waiters();
        }

        let old_ret = if return_old { old } else { None };
        let new_ret = if return_new { Some(item) } else { None };
        Ok((old_ret, new_ret))
    }
}
