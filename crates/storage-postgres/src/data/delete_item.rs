// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_item` implementation for the `PostgreSQL` backend.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, pk_to_text, sk_column, sk_info};

use super::index::{enqueue_async_indexes, fetch_write_path_indexes, sync_indexes};
use super::query::check_condition;
use super::tx_helpers::write_stream_record_in_tx;
use super::{data_table_name, json_to_item};
use crate::PostgresEngine;

impl PostgresEngine {
    /// Implementation of `DataEngine::delete_item`.
    pub(crate) async fn delete_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        // Both index families in one catalog visit (D-4: sync + async split for the
        // secondary indexes).
        //
        // Vector indexes come from this fresh read rather than from the cached key
        // info, and the answer decides two things: whether this write needs a
        // transaction at all, and what maintenance runs inside it. A cached empty
        // set would send a write down the no-maintenance fast path and silently
        // leave an index missing a row.
        //
        // What this does and does not remove. The defect being designed out is the
        // cached membership gate, and that is gone: an index takes effect the moment
        // its catalog row commits. What remains is a window between this read and
        // the data transaction's commit, in which an index created concurrently is
        // missed. That window cannot be closed here, because the catalog and the
        // data tables are different databases and no transaction spans them. It is
        // also exactly the window the secondary indexes have, for the same reason
        // and with the same read: parity with a GSI is the bar, and the backfill
        // that publishes a new index is what covers writes older than it.
        let (indexes, vector_metas) =
            fetch_write_path_indexes(&key_info.table_id, &self.pool).await?;
        // Read whenever anything can propagate, secondary or vector. Gating this on
        // the secondary set alone made a vector-only table ignore the configured
        // delay and apply its vector index inline, while a TransactWriteItems on the
        // same table read the delay unconditionally and enqueued: six write sites,
        // two answers, on a setting the differences doc says covers both index kinds.
        let sys_delay = if indexes.is_empty() && vector_metas.is_empty() {
            0
        } else {
            self.index_propagation_delay().await
        };

        let needs_tx = condition.is_some()
            || return_old
            || !indexes.is_empty()
            || !vector_metas.is_empty()
            || stream.is_some();

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            if needs_tx {
                let select_sql = format!(
                    "SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2 FOR UPDATE"
                );
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2");

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> =
                    bind_sk_fetch_optional!(&select_sql, pk_text.as_ref(), &sk, &mut *tx)?;

                if let Some((ref old_json,)) = old {
                    let old_item: Item = json_to_item(old_json.clone())?;
                    match check_condition(condition, &old_item, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(Some(old_item)));
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    // No existing item — condition checks against empty item
                    let empty = std::collections::BTreeMap::new();
                    match check_condition(condition, &empty, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(None));
                        }
                        Err(e) => return Err(e),
                    }
                    // Nothing to delete
                    return Ok(None);
                }

                // Delete the row
                match &sk {
                    SortKeyValue::S(s) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(s)
                            .execute(&mut *tx)
                            .await
                    }
                    SortKeyValue::N(n) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(n)
                            .execute(&mut *tx)
                            .await
                    }
                    SortKeyValue::B(b) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(b)
                            .execute(&mut *tx)
                            .await
                    }
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                // Sync GSI/LSI update within transaction (D-4).
                let old_item_for_idx = if indexes.is_empty() {
                    None
                } else {
                    let oi = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    sync_indexes(
                        &mut tx,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        oi.as_ref(),
                        None,
                        sys_delay,
                    )
                    .await?;
                    oi
                };

                // Write stream record atomically within the transaction.
                if let Some(capture) = stream {
                    let old_for_stream = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    write_stream_record_in_tx(
                        &mut tx,
                        key_info,
                        capture,
                        old_for_stream.as_ref(),
                        None,
                    )
                    .await?;
                }
                // Persist async GSI work inside the same transaction — one row
                // per async index, each honoring its own propagation delay.
                let mut async_enqueued = enqueue_async_indexes(
                    &mut tx,
                    key_info,
                    &indexes,
                    old_item_for_idx.as_ref(),
                    None,
                    sys_delay,
                )
                .await?;

                // The delete has no new image, so removing the indexed row is the
                // whole of the vector work. Read fresh, never from the cache.
                let old_for_vectors = match old_item_for_idx {
                    Some(ref oi) => Some(oi.clone()),
                    None => old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?,
                };
                async_enqueued += crate::data::vector_index::maintain_vector_indexes(
                    &mut tx,
                    &vector_metas,
                    &key_info.table_id,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    old_for_vectors.as_ref(),
                    None,
                    sys_delay,
                )
                .await?;

                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if async_enqueued > 0
                    && let Some(ref q) = self.gsi_queue
                {
                    q.notify_workers();
                }

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2");
                match &sk {
                    SortKeyValue::S(s) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(s)
                            .execute(&self.data_pool)
                            .await
                    }
                    SortKeyValue::N(n) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(n)
                            .execute(&self.data_pool)
                            .await
                    }
                    SortKeyValue::B(b) => {
                        sqlx::query(&delete_sql)
                            .bind(pk_text.as_ref())
                            .bind(b)
                            .execute(&self.data_pool)
                            .await
                    }
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        } else {
            // PK-only table
            if needs_tx {
                let select_sql =
                    format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 FOR UPDATE");
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = $1");

                let mut tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let old: Option<(serde_json::Value,)> = sqlx::query_as(&select_sql)
                    .bind(pk_text.as_ref())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if let Some((ref old_json,)) = old {
                    let old_item: Item = json_to_item(old_json.clone())?;
                    match check_condition(condition, &old_item, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(Some(old_item)));
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    let empty = std::collections::BTreeMap::new();
                    match check_condition(condition, &empty, maps) {
                        Ok(()) => {}
                        Err(StorageError::ConditionFailed(_)) => {
                            return Err(StorageError::ConditionFailed(None));
                        }
                        Err(e) => return Err(e),
                    }
                    return Ok(None);
                }

                sqlx::query(&delete_sql)
                    .bind(pk_text.as_ref())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                // Sync GSI/LSI update within transaction (D-4).
                let old_item_for_idx = if indexes.is_empty() {
                    None
                } else {
                    let oi = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    sync_indexes(
                        &mut tx,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        oi.as_ref(),
                        None,
                        sys_delay,
                    )
                    .await?;
                    oi
                };

                // Write stream record atomically within the transaction.
                if let Some(capture) = stream {
                    let old_for_stream = old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?;
                    write_stream_record_in_tx(
                        &mut tx,
                        key_info,
                        capture,
                        old_for_stream.as_ref(),
                        None,
                    )
                    .await?;
                }
                // Persist async GSI work inside the same transaction — one row
                // per async index, each honoring its own propagation delay.
                let mut async_enqueued = enqueue_async_indexes(
                    &mut tx,
                    key_info,
                    &indexes,
                    old_item_for_idx.as_ref(),
                    None,
                    sys_delay,
                )
                .await?;

                // The delete has no new image, so removing the indexed row is the
                // whole of the vector work. Read fresh, never from the cache.
                let old_for_vectors = match old_item_for_idx {
                    Some(ref oi) => Some(oi.clone()),
                    None => old
                        .as_ref()
                        .map(|(v,)| json_to_item(v.clone()))
                        .transpose()?,
                };
                async_enqueued += crate::data::vector_index::maintain_vector_indexes(
                    &mut tx,
                    &vector_metas,
                    &key_info.table_id,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    old_for_vectors.as_ref(),
                    None,
                    sys_delay,
                )
                .await?;

                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if async_enqueued > 0
                    && let Some(ref q) = self.gsi_queue
                {
                    q.notify_workers();
                }

                if return_old {
                    old.map(|(v,)| json_to_item(v)).transpose()
                } else {
                    Ok(None)
                }
            } else {
                let delete_sql = format!("DELETE FROM {ddb_table} WHERE pk = $1");
                sqlx::query(&delete_sql)
                    .bind(pk_text.as_ref())
                    .execute(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok(None)
            }
        }
    }
}
