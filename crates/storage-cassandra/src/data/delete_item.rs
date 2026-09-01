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
        self.delete_item_impl_inner(
            key_info, key, return_old, condition, maps, stream, None, None,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn acquire_ttl_mutation_claim(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        expected_item: Option<&Item>,
    ) -> Result<Option<uuid::Uuid>, StorageError> {
        let Some(expected_item) = expected_item else {
            return Ok(None);
        };
        let claim = uuid::Uuid::new_v4();
        if self
            .claim_ttl_item(key_info, key, expected_item, claim, Some(900))
            .await?
        {
            Ok(Some(claim))
        } else {
            Err(StorageError::TransactionCanceled(vec![
                extenddb_core::types::CancellationReason {
                    code: "TransactionConflict".to_owned(),
                    message: Some("Item is being modified by a concurrent operation".to_owned()),
                    item: None,
                },
            ]))
        }
    }

    #[doc(hidden)]
    pub async fn release_ttl_mutation_claim(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        claim: Option<uuid::Uuid>,
    ) {
        if let Some(claim) = claim {
            let _ = self.release_ttl_claim(key_info, key, claim).await;
        }
    }
    async fn delete_item_impl_inner(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
        allowed_prepared_txn_id: Option<uuid::Uuid>,
        expected_claimed_item: Option<&Item>,
    ) -> Result<Option<Item>, StorageError> {
        // Fence a request that resumes after its expiring TTL claim: its tombstones
        // retain this start timestamp and cannot erase later transaction ownership.
        let mutation_timestamp = chrono::Utc::now().timestamp_micros();
        let data_keyspace = self.account_keyspace(&key_info.account_id);
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_text = composite_pk_to_text(key, &key_info.key_schema)?;

        let catalog_keyspace = self.catalog_keyspace();
        let indexes =
            fetch_indexes_for_table(&key_info.table_id, &self.session, &catalog_keyspace).await?;
        let ttl_config = self
            .ttl_config_for_table(&key_info.account_id, &key_info.table_name)
            .await?;
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
                "SELECT item_data, prepared_txn_id FROM {data_keyspace}.{ddb_table} WHERE pk = ? AND {sk_col} = ?"
            );

            let old_result =
                query_with_pk_sk(&self.session, &select_query, pk_text.as_ref(), &sk).await?;

            let body = old_result
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse response: {e}")))?;

            let (old_item_opt, prepared_txn_id_opt) = if let Some(rows) = body.into_rows() {
                if let Some(row) = rows.into_iter().next() {
                    let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
                    let prepared_txn_id: Option<uuid::Uuid> =
                        row.get_by_name("prepared_txn_id").ok().flatten();
                    (item_data.map(json_to_item).transpose()?, prepared_txn_id)
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

            if let Some(expected_item) = expected_claimed_item {
                if prepared_txn_id_opt != allowed_prepared_txn_id
                    || old_item_opt.as_ref() != Some(expected_item)
                {
                    return Err(StorageError::ConditionFailed(old_item_opt));
                }
            } else if prepared_txn_id_opt.is_some() {
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

            let ttl_claim = if allowed_prepared_txn_id.is_some() {
                allowed_prepared_txn_id
            } else if ttl_config.is_some() {
                self.acquire_ttl_mutation_claim(key_info, key, old_item_opt.as_ref())
                    .await?
            } else {
                None
            };

            // Delete the item (with index updates if needed).
            let delete_cql =
                format!("DELETE FROM {data_keyspace}.{ddb_table} WHERE pk = ? AND {sk_col} = ?");

            // Update partition_max_delete_timestamp before the batch (must precede delete).
            if old_item_opt.is_some() {
                self.update_partition_max_delete_timestamp_at(
                    &data_keyspace,
                    &ddb_table,
                    &pk_text,
                    mutation_timestamp / 1_000,
                )
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

            if indexes.is_empty()
                && stream_stmt.is_none()
                && ttl_config.is_none()
                && ttl_claim.is_none()
            {
                query_with_pk_sk(&self.session, &delete_cql, pk_text.as_ref(), &sk).await?;
            } else {
                use super::index::sk_to_value;
                let delete_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    cdrs_tokio::types::value::Value::from(pk_text.as_str()),
                    sk_to_value(&sk),
                ]);
                let mut batch = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
                if ttl_claim.is_some() {
                    batch = batch.with_timestamp(mutation_timestamp);
                }
                batch = batch.add_query(delete_cql, delete_qv);

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

                if let Some(config) = ttl_config.as_ref() {
                    super::ttl::add_ttl_queue_mutations(
                        &mut batch,
                        &data_keyspace,
                        key_info,
                        &config.attribute,
                        config.generation,
                        old_item_opt.as_ref(),
                        None,
                    )?;
                }

                if let Some(stmt) = stream_stmt {
                    batch =
                        batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
                }

                let built = match batch.build() {
                    Ok(built) => built,
                    Err(error) => {
                        self.release_ttl_mutation_claim(key_info, key, ttl_claim)
                            .await;
                        return Err(StorageError::Internal(error.to_string()));
                    }
                };
                if let Err(error) = self.session.batch(built).await {
                    self.release_ttl_mutation_claim(key_info, key, ttl_claim)
                        .await;
                    return Err(StorageError::Internal(format!("Batch execution: {error}")));
                }
                self.release_ttl_mutation_claim(key_info, key, ttl_claim)
                    .await;

                if async_enqueued > 0 {
                    self.gsi_queue.notify_workers();
                }
            }

            Ok(if return_old { old_item_opt } else { None })
        } else {
            // Table has only partition key

            // Always read to check prepared_txn_id for transaction conflict detection
            let select_query = format!(
                "SELECT item_data, prepared_txn_id FROM {data_keyspace}.{ddb_table} WHERE pk = ?"
            );

            let row = crate::cassandra_util::query_optional(
                &self.session,
                &select_query,
                cdrs_tokio::query_values!(pk_text.as_str()),
                "delete_item",
            )
            .await?;

            let (old_item_opt, prepared_txn_id_opt) = if let Some(row) = row {
                let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
                let prepared_txn_id: Option<uuid::Uuid> =
                    row.get_by_name("prepared_txn_id").ok().flatten();
                (item_data.map(json_to_item).transpose()?, prepared_txn_id)
            } else {
                (None, None)
            };

            if let Some(expected_item) = expected_claimed_item {
                if prepared_txn_id_opt != allowed_prepared_txn_id
                    || old_item_opt.as_ref() != Some(expected_item)
                {
                    return Err(StorageError::ConditionFailed(old_item_opt));
                }
            } else if prepared_txn_id_opt.is_some() {
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

            let ttl_claim = if allowed_prepared_txn_id.is_some() {
                allowed_prepared_txn_id
            } else if ttl_config.is_some() {
                self.acquire_ttl_mutation_claim(key_info, key, old_item_opt.as_ref())
                    .await?
            } else {
                None
            };

            // Delete the item (with index updates if needed).
            let delete_cql = format!("DELETE FROM {data_keyspace}.{ddb_table} WHERE pk = ?");

            // Update partition_max_delete_timestamp before the batch (must precede delete).
            if old_item_opt.is_some() {
                self.update_partition_max_delete_timestamp_at(
                    &data_keyspace,
                    &ddb_table,
                    &pk_text,
                    mutation_timestamp / 1_000,
                )
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

            if indexes.is_empty()
                && stream_stmt.is_none()
                && ttl_config.is_none()
                && ttl_claim.is_none()
            {
                self.session
                    .query_with_values(&delete_cql, cdrs_tokio::query_values!(pk_text.as_str()))
                    .await
                    .map_err(|e| StorageError::Internal(format!("Delete failed: {e}")))?;
            } else {
                let delete_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    cdrs_tokio::types::value::Value::from(pk_text.as_str()),
                ]);
                let mut batch = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
                if ttl_claim.is_some() {
                    batch = batch.with_timestamp(mutation_timestamp);
                }
                batch = batch.add_query(delete_cql, delete_qv);

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

                if let Some(config) = ttl_config.as_ref() {
                    super::ttl::add_ttl_queue_mutations(
                        &mut batch,
                        &data_keyspace,
                        key_info,
                        &config.attribute,
                        config.generation,
                        old_item_opt.as_ref(),
                        None,
                    )?;
                }

                if let Some(stmt) = stream_stmt {
                    batch =
                        batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
                }

                let built = match batch.build() {
                    Ok(built) => built,
                    Err(error) => {
                        self.release_ttl_mutation_claim(key_info, key, ttl_claim)
                            .await;
                        return Err(StorageError::Internal(error.to_string()));
                    }
                };
                if let Err(error) = self.session.batch(built).await {
                    self.release_ttl_mutation_claim(key_info, key, ttl_claim)
                        .await;
                    return Err(StorageError::Internal(format!("Batch execution: {error}")));
                }
                self.release_ttl_mutation_claim(key_info, key, ttl_claim)
                    .await;

                if async_enqueued > 0 {
                    self.gsi_queue.notify_workers();
                }
            }

            Ok(if return_old { old_item_opt } else { None })
        }
    }

    pub(crate) async fn ensure_ttl_work_claim(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        expected_item: &Item,
        work_id: uuid::Uuid,
    ) -> Result<bool, StorageError> {
        if self
            .claim_ttl_item(key_info, key, expected_item, work_id, None)
            .await?
        {
            return Ok(true);
        }
        let keyspace = self.account_keyspace(&key_info.account_id);
        let table = data_table_name(&key_info.table_id);
        let pk = composite_pk_to_text(key, &key_info.key_schema)?;
        let row = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk = parse_sk(
                key.get(sk_name)
                    .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?,
                sk_type,
            )?;
            let query = format!(
                "SELECT item_data, prepared_txn_id FROM {keyspace}.{table} \
                 WHERE pk = ? AND {} = ?",
                sk_column(sk_type)
            );
            query_with_pk_sk(&self.session, &query, pk.as_str(), &sk)
                .await?
                .response_body()
                .ok()
                .and_then(|body| body.into_rows())
                .and_then(|rows| rows.into_iter().next())
        } else {
            let query =
                format!("SELECT item_data, prepared_txn_id FROM {keyspace}.{table} WHERE pk = ?");
            crate::cassandra_util::query_optional(
                &self.session,
                &query,
                cdrs_tokio::query_values!(pk.as_str()),
                "ensure_ttl_work_claim",
            )
            .await?
        };
        let Some(row) = row else {
            return Ok(false);
        };
        let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
        let owner: Option<uuid::Uuid> = row.get_by_name("prepared_txn_id").ok().flatten();
        let expected = serde_json::to_string(expected_item)
            .map_err(|error| StorageError::Internal(error.to_string()))?;
        Ok(owner == Some(work_id) && item_data.as_deref() == Some(expected.as_str()))
    }

    pub(crate) async fn apply_ttl_delete_effects(
        &self,
        key_info: &TableKeyInfo,
        old_item: &Item,
        work_id: uuid::Uuid,
        delete_timestamp_ms: i64,
        stream_plan: Option<&crate::data::ttl::TtlStreamPlan>,
    ) -> Result<(), StorageError> {
        let account_keyspace = self.account_keyspace(&key_info.account_id);
        let table = data_table_name(&key_info.table_id);
        let pk = composite_pk_to_text(old_item, &key_info.key_schema)?;
        self.update_partition_max_delete_timestamp_at(
            &account_keyspace,
            &table,
            &pk,
            delete_timestamp_ms,
        )
        .await?;

        let indexes =
            fetch_indexes_for_table(&key_info.table_id, &self.session, &self.catalog_keyspace())
                .await?;
        let default_delay = self
            .gsi_default_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut batch = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
        let mut has_effects = !indexes.is_empty();
        if !indexes.is_empty() {
            super::index::sync_indexes(
                &mut batch,
                &account_keyspace,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &indexes,
                Some(old_item),
                None,
                default_delay,
            )?;
            super::index::enqueue_async_indexes(
                &self.session,
                &mut batch,
                &account_keyspace,
                key_info,
                &indexes,
                Some(old_item),
                None,
                default_delay,
            )
            .await?;
        }

        if let Some(plan) = stream_plan {
            let capture = extenddb_storage::StreamCapture {
                view_type: plan.view_type,
                user_identity: Some(extenddb_core::types::UserIdentity {
                    identity_type: "Service".to_owned(),
                    principal_id: "dynamodb.amazonaws.com".to_owned(),
                }),
                region: std::sync::Arc::from(plan.region.as_str()),
            };
            let identity = crate::stream_util::StreamRecordIdentity {
                event_id: plan.event_id.clone(),
                sequence_number: plan.sequence_number.clone(),
                created_at_ms: plan.created_at_ms,
            };
            if let Some(statement) = crate::stream_util::stream_record_statement_with_identity(
                &account_keyspace,
                &key_info.table_id,
                key_info,
                Some(old_item),
                None,
                &capture,
                &identity,
                self.stream_retention_seconds,
            ) {
                batch = batch.add_query(
                    statement,
                    cdrs_tokio::query::QueryValues::SimpleValues(vec![]),
                );
                has_effects = true;
            }
        }

        if has_effects {
            let built = batch
                .build()
                .map_err(|error| StorageError::Internal(error.to_string()))?;
            self.session
                .batch(built)
                .await
                .map_err(|error| StorageError::Internal(format!("TTL side effects: {error}")))?;
            if !indexes.is_empty() {
                self.gsi_queue.notify_workers();
            }
        }
        let _ = work_id;
        Ok(())
    }

    pub(crate) async fn delete_ttl_base_exact(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        expected_item: &Item,
        work_id: uuid::Uuid,
    ) -> Result<bool, StorageError> {
        let keyspace = self.account_keyspace(&key_info.account_id);
        let table = data_table_name(&key_info.table_id);
        let pk = composite_pk_to_text(key, &key_info.key_schema)?;
        let expected = serde_json::to_string(expected_item)
            .map_err(|error| StorageError::Internal(error.to_string()))?;
        let owner = cdrs_tokio::types::value::Bytes::new(work_id.as_bytes().to_vec());
        let result = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk = parse_sk(
                key.get(sk_name)
                    .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?,
                sk_type,
            )?;
            let query = format!(
                "DELETE FROM {keyspace}.{table} WHERE pk = ? AND {} = ? \
                 IF prepared_txn_id = ? AND item_data = ?",
                sk_column(sk_type)
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    cdrs_tokio::types::value::Value::from(pk.as_str()),
                    super::index::sk_to_value(&sk),
                    cdrs_tokio::types::value::Value::from(owner),
                    cdrs_tokio::types::value::Value::from(expected.as_str()),
                ]),
            )
            .await?
        } else {
            let query = format!(
                "DELETE FROM {keyspace}.{table} WHERE pk = ? \
                 IF prepared_txn_id = ? AND item_data = ?"
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(pk.as_str(), owner, expected.as_str()),
            )
            .await?
        };
        ttl_lwt_applied(&result)
    }

    async fn claim_ttl_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        expected_item: &Item,
        claim: uuid::Uuid,
        ttl_seconds: Option<u32>,
    ) -> Result<bool, StorageError> {
        let keyspace = self.account_keyspace(&key_info.account_id);
        let table = data_table_name(&key_info.table_id);
        let pk = composite_pk_to_text(key, &key_info.key_schema)?;
        let expected = serde_json::to_string(expected_item)
            .map_err(|error| StorageError::Internal(error.to_string()))?;
        let claim_bytes = cdrs_tokio::types::value::Bytes::new(claim.as_bytes().to_vec());
        let claimed_at = chrono::Utc::now().timestamp_millis();
        let using_ttl = ttl_seconds
            .map(|seconds| format!("USING TTL {seconds} "))
            .unwrap_or_default();

        let result = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk = parse_sk(
                key.get(sk_name)
                    .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?,
                sk_type,
            )?;
            let query = format!(
                "UPDATE {keyspace}.{table} {using_ttl}\
                 SET prepared_txn_id = ?, prepared_txn_timestamp = ? \
                 WHERE pk = ? AND {} = ? \
                 IF prepared_txn_id = null AND item_data = ?",
                sk_column(sk_type)
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    cdrs_tokio::types::value::Value::from(claim_bytes),
                    cdrs_tokio::types::value::Value::from(claimed_at),
                    cdrs_tokio::types::value::Value::from(pk.as_str()),
                    super::index::sk_to_value(&sk),
                    cdrs_tokio::types::value::Value::from(expected.as_str()),
                ]),
            )
            .await
        } else {
            let query = format!(
                "UPDATE {keyspace}.{table} {using_ttl}\
                 SET prepared_txn_id = ?, prepared_txn_timestamp = ? \
                 WHERE pk = ? IF prepared_txn_id = null AND item_data = ?"
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(claim_bytes, claimed_at, pk.as_str(), expected.as_str()),
            )
            .await
        }
        .map_err(|error| StorageError::Internal(format!("Claim TTL item: {error}")))?;
        ttl_lwt_applied(&result)
    }

    pub(crate) async fn release_ttl_claim(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        claim: uuid::Uuid,
    ) -> Result<(), StorageError> {
        let keyspace = self.account_keyspace(&key_info.account_id);
        let table = data_table_name(&key_info.table_id);
        let pk = composite_pk_to_text(key, &key_info.key_schema)?;
        let claim_bytes = cdrs_tokio::types::value::Bytes::new(claim.as_bytes().to_vec());

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk = parse_sk(
                key.get(sk_name)
                    .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?,
                sk_type,
            )?;
            let query = format!(
                "UPDATE {keyspace}.{table} SET prepared_txn_id = null, prepared_txn_timestamp = null \
                 WHERE pk = ? AND {} = ? IF prepared_txn_id = ?",
                sk_column(sk_type)
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query::QueryValues::SimpleValues(vec![
                        cdrs_tokio::types::value::Value::from(pk.as_str()),
                        super::index::sk_to_value(&sk),
                        cdrs_tokio::types::value::Value::from(claim_bytes),
                    ]),
                )
                .await
        } else {
            let query = format!(
                "UPDATE {keyspace}.{table} SET prepared_txn_id = null, prepared_txn_timestamp = null \
                 WHERE pk = ? IF prepared_txn_id = ?"
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(pk.as_str(), claim_bytes),
            )
            .await
        }
        .map_err(|error| StorageError::Internal(format!("Release TTL claim: {error}")))?;
        Ok(())
    }

    /// Update `partition_max_delete_timestamp` for a partition using a two-step
    /// LWT approach. This records the supplied stable delete timestamp so that
    /// stale transactions cannot re-create items deleted after they started.
    async fn update_partition_max_delete_timestamp_at(
        &self,
        keyspace: &str,
        table: &str,
        pk: &str,
        now_ms: i64,
    ) -> Result<(), StorageError> {
        // Step 1: Try to set if null
        let query_null = format!(
            "UPDATE {keyspace}.{table} SET partition_max_delete_timestamp = ? WHERE pk = ? \
             IF partition_max_delete_timestamp = null"
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
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
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
                "UPDATE {keyspace}.{table} SET partition_max_delete_timestamp = ? WHERE pk = ? \
                 IF partition_max_delete_timestamp < ?"
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

fn ttl_lwt_applied(result: &cdrs_tokio::frame::Envelope) -> Result<bool, StorageError> {
    let rows = result
        .response_body()
        .map_err(|error| StorageError::Internal(format!("Parse TTL claim: {error}")))?
        .into_rows()
        .unwrap_or_default();
    let Some(row) = rows.first() else {
        return Err(StorageError::Internal(
            "TTL claim returned no LWT result".to_owned(),
        ));
    };
    row.get_r_by_name("[applied]")
        .map_err(|error| StorageError::Internal(format!("Parse TTL claim result: {error}")))
}
