// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` implementation for the Cassandra backend.

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::query::BatchQueryBuilder;
use cdrs_tokio::query_values;
use cdrs_tokio::types::IntoRustByName;
use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, KeyType, TableKeyInfo};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_sk, pk_to_text, sk_column};

use super::ddl::data_table_name;
use super::{json_to_item, query_with_pk_sk, query_with_pk_sk_item};
use crate::CassandraEngine;
use crate::stream_util::stream_record_statement;

// 400 KB limit from DynamoDB specification
const MAX_ITEM_SIZE_BYTES: usize = 400 * 1024;

impl CassandraEngine {
    /// Implementation of `DataEngine::update_item`.
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
        let data_keyspace = self.account_keyspace(&key_info.account_id);
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        let catalog_keyspace = self.catalog_keyspace();
        let indexes = super::index::fetch_indexes_for_table(
            &key_info.table_id,
            &self.session,
            &catalog_keyspace,
        )
        .await?;
        let sys_delay = if indexes.is_empty() {
            0
        } else {
            self.gsi_default_delay_ms
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        // Fetch existing item (including prepared_txn_id for transaction conflict detection)
        let old_json = if let Some(sk_elem) = key_info
            .key_schema
            .iter()
            .find(|k| k.key_type == KeyType::Range)
        {
            let sk_name = &sk_elem.attribute_name;
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk_type = key_info
                .attribute_definitions
                .iter()
                .find(|ad| ad.attribute_name == *sk_name)
                .ok_or_else(|| StorageError::Internal("sort key type not found".to_owned()))?
                .attribute_type;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            let select_query = format!(
                "SELECT item_data, prepared_txn_id FROM {}.{} WHERE pk = ? AND {} = ?",
                data_keyspace, ddb_table, sk_col
            );

            let result = query_with_pk_sk(&self.session, &select_query, &pk_text, &sk).await?;

            let body = result
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse response: {}", e)))?;

            let rows = body.into_rows().unwrap_or_default();
            if let Some(row) = rows.first() {
                // Check for in-flight transaction
                let prepared_txn_id: Option<uuid::Uuid> =
                    row.get_by_name("prepared_txn_id").ok().flatten();
                if prepared_txn_id.is_some() {
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
                let item_data: String =
                    crate::cassandra_util::get_column(row, "item_data", "update_item")?;
                Some(json_to_item(item_data)?)
            } else {
                None
            }
        } else {
            let select_query = format!(
                "SELECT item_data, prepared_txn_id FROM {}.{} WHERE pk = ?",
                data_keyspace, ddb_table
            );

            let row = crate::cassandra_util::query_optional(
                &self.session,
                &select_query,
                query_values!(pk_text.as_ref() as &str),
                "update_item",
            )
            .await?;

            if let Some(row) = row {
                // Check for in-flight transaction
                let prepared_txn_id: Option<uuid::Uuid> =
                    row.get_by_name("prepared_txn_id").ok().flatten();
                if prepared_txn_id.is_some() {
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
                let item_data: String =
                    crate::cassandra_util::get_column(&row, "item_data", "update_item")?;
                Some(json_to_item(item_data)?)
            } else {
                None
            }
        };

        // Build working item: existing or new with key attributes only (upsert)
        let item_existed = old_json.is_some();
        let mut item = if let Some(item) = old_json {
            item
        } else {
            key.clone()
        };

        // Only capture pre-mutation item when the item already existed; for upserts
        // (item_existed == false) there is no old image to record.
        let pre_mutation_item = if (!indexes.is_empty() || stream.is_some()) && item_existed {
            Some(item.clone())
        } else {
            None
        };
        let old_item = if return_old { Some(item.clone()) } else { None };

        // Evaluate condition against the existing item (empty if non-existent)
        // DynamoDB treats a non-existent item as having no attributes
        let condition_item = if item_existed {
            &item
        } else {
            &std::collections::BTreeMap::new()
        };
        match super::check_condition(condition, condition_item, maps) {
            Ok(()) => {}
            Err(StorageError::ConditionFailed(_)) => {
                if item_existed {
                    return Err(StorageError::ConditionFailed(Some(item)));
                }
                return Err(StorageError::ConditionFailed(None));
            }
            Err(e) => return Err(e),
        }

        // Apply update actions
        expression::apply_update_validated(actions, &mut item, maps, &[], &[])
            .map_err(|e| StorageError::Validation(e.to_string()))?;

        // Validate item size (400 KB limit)
        validation::validate_item_size(&item, MAX_ITEM_SIZE_BYTES)
            .map_err(|e| StorageError::Validation(e.to_string()))?;

        let new_item = if return_new { Some(item.clone()) } else { None };

        // Write the updated item back
        let item_json =
            serde_json::to_value(&item).map_err(|e| StorageError::Internal(e.to_string()))?;
        let item_json_str = item_json.to_string();

        if let Some(sk_elem) = key_info
            .key_schema
            .iter()
            .find(|k| k.key_type == KeyType::Range)
        {
            let sk_name = &sk_elem.attribute_name;
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk_type = key_info
                .attribute_definitions
                .iter()
                .find(|ad| ad.attribute_name == *sk_name)
                .ok_or_else(|| StorageError::Internal("sort key type not found".to_owned()))?
                .attribute_type;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            let update_cql = format!(
                "UPDATE {}.{} SET item_data = ? WHERE pk = ? AND {} = ?",
                data_keyspace, ddb_table, sk_col
            );

            let stream_stmt = stream.and_then(|cap| {
                stream_record_statement(
                    &data_keyspace,
                    &key_info.table_id,
                    key_info,
                    pre_mutation_item.as_ref(),
                    Some(&item),
                    cap,
                    &self.hlc,
                    self.stream_retention_seconds,
                )
            });

            if indexes.is_empty() && stream_stmt.is_none() {
                query_with_pk_sk_item(&self.session, &update_cql, &pk_text, &sk, &item_json_str)
                    .await?;
            } else {
                let update_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    item_json_str.as_str().into(),
                    cdrs_tokio::types::value::Value::from(pk_text.as_ref() as &str),
                    super::index::sk_to_value(&sk),
                ]);
                let mut batch = BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
                    .add_query(update_cql, update_qv);

                if !indexes.is_empty() {
                    super::index::sync_indexes(
                        &mut batch,
                        &data_keyspace,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        pre_mutation_item.as_ref(),
                        Some(&item),
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
                        pre_mutation_item.as_ref(),
                        Some(&item),
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
        } else {
            let update_cql = format!(
                "UPDATE {}.{} SET item_data = ? WHERE pk = ?",
                data_keyspace, ddb_table
            );

            let stream_stmt = stream.and_then(|cap| {
                stream_record_statement(
                    &data_keyspace,
                    &key_info.table_id,
                    key_info,
                    pre_mutation_item.as_ref(),
                    Some(&item),
                    cap,
                    &self.hlc,
                    self.stream_retention_seconds,
                )
            });

            if indexes.is_empty() && stream_stmt.is_none() {
                crate::cassandra_util::execute(
                    &self.session,
                    &update_cql,
                    query_values!(item_json_str.as_str(), pk_text.as_ref() as &str),
                    "update_item",
                )
                .await?;
            } else {
                let update_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    item_json_str.as_str().into(),
                    cdrs_tokio::types::value::Value::from(pk_text.as_ref() as &str),
                ]);
                let mut batch = BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
                    .add_query(update_cql, update_qv);

                if !indexes.is_empty() {
                    super::index::sync_indexes(
                        &mut batch,
                        &data_keyspace,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        pre_mutation_item.as_ref(),
                        Some(&item),
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
                        pre_mutation_item.as_ref(),
                        Some(&item),
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
        }

        Ok((old_item, new_item))
    }
}
