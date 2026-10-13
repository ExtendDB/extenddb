// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_item` for the SQLite backend.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;

use super::index::{enqueue_async_indexes, fetch_indexes_for_table, sync_indexes};
use super::query::check_condition;
use super::tx_helpers::{delete_item_in_tx, fetch_item_in_tx, write_stream_record_in_tx};
use crate::store::SqliteEngine;

impl SqliteEngine {
    pub(crate) async fn delete_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        // Read the propagation delay BEFORE taking the write lock. It is a
        // runtime setting, not an invariant of this write, so it does not need
        // to be read under the lock, and the lock serialises every write in the
        // process: work done inside it is the backend's throughput bottleneck.
        let system_delay = self.index_propagation_delay().await;
        let _writer = self.write_lock.lock().await;
        // Read the index set after acquiring the write lock so a concurrently
        // added GSI (UpdateTable holds the same lock) is not missed.
        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;

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

        // Only mutate when an item actually exists; deleting a missing item is
        // a no-op (no row removed, no index change, no stream record).
        let mut enqueued = false;
        if old.is_some() {
            delete_item_in_tx(&mut tx, key_info, key).await?;

            if !indexes.is_empty() {
                sync_indexes(
                    &mut tx,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    &indexes,
                    old.as_ref(),
                    None,
                    system_delay,
                )
                .await?;
            }
            // Vector rows for this base item are removed too. `new_item` is None,
            // so this is a pure removal, applied in this transaction when the
            // propagation delay is 0 and enqueued otherwise.
            if !key_info.vector_indexes.is_empty()
                && crate::data::vector_index::maintain_vector_indexes(
                    &mut tx,
                    &key_info.table_id,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    old.as_ref(),
                    None,
                    system_delay,
                )
                .await?
                    > 0
            {
                enqueued = true;
            }
            if enqueue_async_indexes(
                &mut tx,
                key_info,
                &indexes,
                old.as_ref(),
                None,
                system_delay,
            )
            .await?
                > 0
            {
                enqueued = true;
            }

            if let Some(capture) = stream {
                write_stream_record_in_tx(&mut tx, key_info, capture, old.as_ref(), None).await?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if enqueued {
            self.gsi_notify.notify_waiters();
        }

        Ok(if return_old { old } else { None })
    }
}
