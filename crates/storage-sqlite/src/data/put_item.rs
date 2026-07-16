// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `put_item` and `get_item` for the SQLite backend.
//!
//! Writes acquire the engine write lock (D1) and run in a transaction, so the
//! condition check, the write, index sync, and stream capture are one atomic
//! unit with no competing writer — no `INSERT ... ON CONFLICT DO NOTHING` race
//! dance is needed.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{pk_to_text, sk_column, sk_info};

use super::index::{enqueue_async_indexes, fetch_indexes_for_table, sync_indexes};
use super::query::check_condition;
use super::tx_helpers::{fetch_item_in_tx, upsert_item_in_tx, write_stream_record_in_tx};
use super::{bind_sk_fetch_optional, data_table_name, json_to_item};
use crate::store::SqliteEngine;

impl SqliteEngine {
    pub(crate) async fn put_item_impl(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        // Read the index set only after acquiring the write lock, so a GSI added
        // by a concurrent UpdateTable (which holds the same lock) cannot be missed
        // and left unmaintained by this write.
        let _writer = self.write_lock.lock().await;
        let indexes = fetch_indexes_for_table(&key_info.table_id, &self.pool).await?;

        // Index key attributes present in the item must match their declared
        // scalar type and be non-empty, matching real DynamoDB. This is up-front
        // input validation (a top-level ValidationException), so it runs before
        // any write work. Mirrors the PostgreSQL backend.
        if !indexes.is_empty() {
            let index_refs: Vec<extenddb_core::validation::IndexKeyRef<'_>> = indexes
                .iter()
                .map(|idx| extenddb_core::validation::IndexKeyRef {
                    index_name: &idx.index_name,
                    key_schema: &idx.key_schema,
                })
                .collect();
            extenddb_core::validation::validate_index_keys(
                &item,
                &index_refs,
                &key_info.attribute_definitions,
            )
            .map_err(|e| StorageError::Validation(e.to_string()))?;
        }

        let system_delay = self.gsi_default_delay();
        let need_old = condition.is_some() || return_old || !indexes.is_empty() || stream.is_some();

        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let old = if need_old {
            fetch_item_in_tx(&mut tx, key_info, &item).await?
        } else {
            None
        };

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

        upsert_item_in_tx(&mut tx, key_info, &item).await?;

        // Synchronous indexes (LSIs + zero-delay GSIs) are applied in-txn;
        // async GSIs are enqueued into gsi_pending within the same txn.
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

        Ok(if return_old { old } else { None })
    }

    pub(crate) async fn get_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);
        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        let json_opt = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = extenddb_storage::util::parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ? AND {sk_col} = ?");
            let row: Option<(serde_json::Value,)> =
                bind_sk_fetch_optional!(&sql, pk_text.as_ref(), &sk, &self.pool)?;
            row.map(|(v,)| v)
        } else {
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = ?");
            let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(pk_text.as_ref())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            row.map(|(v,)| v)
        };

        json_opt.map(json_to_item).transpose()
    }
}
