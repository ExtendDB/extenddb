// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_item` implementation for the Cassandra backend.

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::query::BatchQueryBuilder;
use cdrs_tokio::types::IntoRustByName;
use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, sk_column, sk_info};

use super::ddl::data_table_name;
use super::index::fetch_indexes_for_table;
use super::{json_to_item, query_with_pk_sk};
use crate::CassandraEngine;
use crate::stream_util::stream_record_statement;

impl CassandraEngine {
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
        let data_keyspace = self.account_keyspace(&key_info.account_id);
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_text = composite_pk_to_text(key, &key_info.key_schema)?;

        let catalog_keyspace = self.catalog_keyspace();
        let indexes =
            fetch_indexes_for_table(&key_info.table_id, &self.session, &catalog_keyspace).await?;
        let sys_delay = if indexes.is_empty() {
            0
        } else {
            self.gsi_default_delay_ms
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            // Table has sort key
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            // Always read to check prepared_txn_id for transaction conflict detection.
            let select_query = format!(
                "SELECT item_data, prepared_txn_id FROM {}.{} WHERE pk = ? AND {} = ?",
                data_keyspace, ddb_table, sk_col
            );

            let old_result =
                query_with_pk_sk(&self.session, &select_query, pk_text.as_ref(), &sk).await?;

            let body = old_result
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse response: {}", e)))?;

            let (old_item_opt, has_prepared_txn) = if let Some(rows) = body.into_rows() {
                if let Some(row) = rows.into_iter().next() {
                    let item_data: String =
                        crate::cassandra_util::get_column(&row, "item_data", "delete_item")?;
                    let prepared_txn_id: Option<uuid::Uuid> =
                        row.get_by_name("prepared_txn_id").ok().flatten();
                    (Some(json_to_item(item_data)?), prepared_txn_id.is_some())
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            };

            // Reject if item is part of an in-flight transaction
            if has_prepared_txn {
                return Err(StorageError::TransactionCanceled(vec![
                    extenddb_core::types::CancellationReason {
                        code: "TransactionConflict".to_owned(),
                        message: Some(
                            "Item is being modified by a concurrent transaction".to_owned(),
                        ),
                        item: None,
                    },
                ]));
            }

            // Evaluate condition against existing item (or empty if doesn't exist)
            if condition.is_some() {
                let condition_item = if let Some(ref existing) = old_item_opt {
                    existing
                } else {
                    &std::collections::BTreeMap::new()
                };
                match super::check_condition(condition, condition_item, maps) {
                    Ok(()) => {}
                    Err(StorageError::ConditionFailed(_)) => {
                        return Err(StorageError::ConditionFailed(old_item_opt));
                    }
                    Err(e) => return Err(e),
                }

                // If condition passed but no item exists, nothing to delete
                if old_item_opt.is_none() {
                    return Ok(None);
                }
            }

            // Delete the item (with index updates if needed).
            let delete_cql = format!(
                "DELETE FROM {}.{} WHERE pk = ? AND {} = ?",
                data_keyspace, ddb_table, sk_col
            );

            // Update partition_max_delete_timestamp before the batch (must precede delete).
            if old_item_opt.is_some() {
                self.update_partition_max_delete_timestamp(&data_keyspace, &ddb_table, &pk_text)
                    .await?;
            }

            let stream_stmt = old_item_opt.as_ref().and_then(|_| {
                stream.and_then(|cap| {
                    stream_record_statement(
                        &data_keyspace,
                        &key_info.table_id,
                        key_info,
                        old_item_opt.as_ref(),
                        None,
                        cap,
                        &self.hlc,
                        self.stream_retention_seconds,
                    )
                })
            });

            if indexes.is_empty() && stream_stmt.is_none() {
                query_with_pk_sk(&self.session, &delete_cql, pk_text.as_ref(), &sk).await?;
            } else {
                use super::index::sk_to_value;
                let delete_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    cdrs_tokio::types::value::Value::from(pk_text.as_str()),
                    sk_to_value(&sk),
                ]);
                let mut batch = BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
                    .add_query(delete_cql, delete_qv);

                if !indexes.is_empty() {
                    super::index::sync_indexes(
                        &mut batch,
                        &data_keyspace,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        old_item_opt.as_ref(),
                        None,
                        sys_delay,
                    )?;
                }

                let async_enqueued = if !indexes.is_empty() {
                    super::index::enqueue_async_indexes(
                        &self.session,
                        &mut batch,
                        &data_keyspace,
                        key_info,
                        &indexes,
                        old_item_opt.as_ref(),
                        None,
                        sys_delay,
                    )
                    .await?
                } else {
                    0
                };

                if let Some(stmt) = stream_stmt {
                    batch =
                        batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
                }

                self.session
                    .batch(
                        batch
                            .build()
                            .map_err(|e| StorageError::Internal(e.to_string()))?,
                    )
                    .await
                    .map_err(|e| StorageError::Internal(format!("Batch execution: {}", e)))?;

                if async_enqueued > 0 {
                    self.gsi_queue.notify_workers();
                }
            }

            Ok(if return_old { old_item_opt } else { None })
        } else {
            // Table has only partition key

            // Always read to check prepared_txn_id for transaction conflict detection
            let select_query = format!(
                "SELECT item_data, prepared_txn_id FROM {}.{} WHERE pk = ?",
                data_keyspace, ddb_table
            );

            let row = crate::cassandra_util::query_optional(
                &self.session,
                &select_query,
                cdrs_tokio::query_values!(pk_text.as_str()),
                "delete_item",
            )
            .await?;

            let (old_item_opt, has_prepared_txn) = if let Some(row) = row {
                let item_data: String =
                    crate::cassandra_util::get_column(&row, "item_data", "delete_item")?;
                let prepared_txn_id: Option<uuid::Uuid> =
                    row.get_by_name("prepared_txn_id").ok().flatten();
                (Some(json_to_item(item_data)?), prepared_txn_id.is_some())
            } else {
                (None, false)
            };

            // Reject if item is part of an in-flight transaction
            if has_prepared_txn {
                return Err(StorageError::TransactionCanceled(vec![
                    extenddb_core::types::CancellationReason {
                        code: "TransactionConflict".to_owned(),
                        message: Some(
                            "Item is being modified by a concurrent transaction".to_owned(),
                        ),
                        item: None,
                    },
                ]));
            }

            // Evaluate condition against existing item (or empty if doesn't exist)
            if condition.is_some() {
                let condition_item = if let Some(ref existing) = old_item_opt {
                    existing
                } else {
                    &std::collections::BTreeMap::new()
                };
                match super::check_condition(condition, condition_item, maps) {
                    Ok(()) => {}
                    Err(StorageError::ConditionFailed(_)) => {
                        return Err(StorageError::ConditionFailed(old_item_opt));
                    }
                    Err(e) => return Err(e),
                }

                // If condition passed but no item exists, nothing to delete
                if old_item_opt.is_none() {
                    return Ok(None);
                }
            }

            // Delete the item (with index updates if needed).
            let delete_cql = format!("DELETE FROM {}.{} WHERE pk = ?", data_keyspace, ddb_table);

            // Update partition_max_delete_timestamp before the batch (must precede delete).
            if old_item_opt.is_some() {
                self.update_partition_max_delete_timestamp(&data_keyspace, &ddb_table, &pk_text)
                    .await?;
            }

            let stream_stmt = old_item_opt.as_ref().and_then(|_| {
                stream.and_then(|cap| {
                    stream_record_statement(
                        &data_keyspace,
                        &key_info.table_id,
                        key_info,
                        old_item_opt.as_ref(),
                        None,
                        cap,
                        &self.hlc,
                        self.stream_retention_seconds,
                    )
                })
            });

            if indexes.is_empty() && stream_stmt.is_none() {
                self.session
                    .query_with_values(&delete_cql, cdrs_tokio::query_values!(pk_text.as_str()))
                    .await
                    .map_err(|e| StorageError::Internal(format!("Delete failed: {}", e)))?;
            } else {
                let delete_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    cdrs_tokio::types::value::Value::from(pk_text.as_str()),
                ]);
                let mut batch = BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
                    .add_query(delete_cql, delete_qv);

                if !indexes.is_empty() {
                    super::index::sync_indexes(
                        &mut batch,
                        &data_keyspace,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        old_item_opt.as_ref(),
                        None,
                        sys_delay,
                    )?;
                }

                let async_enqueued = if !indexes.is_empty() {
                    super::index::enqueue_async_indexes(
                        &self.session,
                        &mut batch,
                        &data_keyspace,
                        key_info,
                        &indexes,
                        old_item_opt.as_ref(),
                        None,
                        sys_delay,
                    )
                    .await?
                } else {
                    0
                };

                if let Some(stmt) = stream_stmt {
                    batch =
                        batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
                }

                self.session
                    .batch(
                        batch
                            .build()
                            .map_err(|e| StorageError::Internal(e.to_string()))?,
                    )
                    .await
                    .map_err(|e| StorageError::Internal(format!("Batch execution: {}", e)))?;

                if async_enqueued > 0 {
                    self.gsi_queue.notify_workers();
                }
            }

            Ok(if return_old { old_item_opt } else { None })
        }
    }

    /// Update `partition_max_delete_timestamp` for a partition using a two-step
    /// LWT approach. This records the latest delete timestamp so that stale
    /// transactions cannot re-create items that were deleted after they started.
    ///
    /// Step 1: Try to set the value if it's currently null (first delete in partition).
    /// Step 2: If already set, update only if our timestamp is higher.
    async fn update_partition_max_delete_timestamp(
        &self,
        keyspace: &str,
        table: &str,
        pk: &str,
    ) -> Result<(), StorageError> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Step 1: Try to set if null
        let query_null = format!(
            "UPDATE {}.{} SET partition_max_delete_timestamp = ? WHERE pk = ? \
             IF partition_max_delete_timestamp = null",
            keyspace, table
        );

        let result = self
            .session
            .query_with_values(&query_null, cdrs_tokio::query_values!(now_ms, pk))
            .await
            .map_err(|e| {
                tracing::error!("update_partition_max_delete_timestamp (null): {e}");
                StorageError::Internal("Database error".to_owned())
            })?;

        // Check if LWT was applied
        let applied = result
            .response_body()
            .ok()
            .and_then(|body| body.into_rows())
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| {
                use cdrs_tokio::types::IntoRustByName;
                let val: bool = row.get_r_by_name("[applied]").ok()?;
                Some(val)
            })
            .unwrap_or(true); // If we can't parse, assume applied

        if !applied {
            // Step 2: Column already has a value - update only if ours is higher
            let query_compare = format!(
                "UPDATE {}.{} SET partition_max_delete_timestamp = ? WHERE pk = ? \
                 IF partition_max_delete_timestamp < ?",
                keyspace, table
            );

            self.session
                .query_with_values(
                    &query_compare,
                    cdrs_tokio::query_values!(now_ms, pk, now_ms),
                )
                .await
                .map_err(|e| {
                    tracing::error!("update_partition_max_delete_timestamp (compare): {e}");
                    StorageError::Internal("Database error".to_owned())
                })?;
            // Don't check LWT result - if another delete set a higher timestamp, that's fine
        }

        Ok(())
    }
}
