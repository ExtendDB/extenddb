// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `put_item` and `get_item` implementations for the Cassandra backend.

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::query::BatchQueryBuilder;
use cdrs_tokio::types::IntoRustByName;
use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, pk_to_text, sk_column, sk_info};

use super::ddl::data_table_name;
use super::{json_to_item, query_with_pk_sk, query_with_pk_sk_item};
use crate::CassandraEngine;
use crate::stream_util::stream_record_statement;

impl CassandraEngine {
    /// Implementation of `DataEngine::put_item`.
    ///
    /// On a TTL-enabled table the base-row claim can be lost to a concurrent
    /// writer. That is ordinary contention, not a client error, so the whole
    /// read-claim-commit sequence is retried against a freshly read image
    /// before the conflict is surfaced.
    pub(crate) async fn put_item_impl(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        use super::delete_item::{TTL_CLAIM_MAX_RETRIES, ttl_claim_backoff};

        for attempt in 0..=TTL_CLAIM_MAX_RETRIES {
            match self
                .put_item_impl_inner(key_info, item.clone(), return_old, condition, maps, stream)
                .await
            {
                Err(StorageError::TransactionConflict(message))
                    if attempt == TTL_CLAIM_MAX_RETRIES =>
                {
                    return Err(StorageError::TransactionConflict(message));
                }
                Err(StorageError::TransactionConflict(_)) => ttl_claim_backoff(attempt).await,
                other => return other,
            }
        }
        unreachable!("loop returns on the final attempt")
    }

    async fn put_item_impl_inner(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<Option<Item>, StorageError> {
        let data_keyspace = self.account_keyspace(&key_info.account_id);
        let catalog_keyspace = self.catalog_keyspace();
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_text = composite_pk_to_text(&item, &key_info.key_schema)?;

        let item_json =
            serde_json::to_value(&item).map_err(|e| StorageError::Internal(e.to_string()))?;
        let item_text = item_json.to_string();

        // Fetch indexes for GSI/LSI updates
        let indexes = super::index::fetch_indexes_for_table(
            &key_info.table_id,
            &self.session,
            &catalog_keyspace,
        )
        .await?;
        let ttl_config = self
            .ttl_config_for_table(&key_info.account_id, &key_info.table_name)
            .await?;
        let sys_delay = if indexes.is_empty() {
            0
        } else {
            self.gsi_default_delay_ms
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        // Always use read-then-write path to check prepared_txn_id for transaction safety.
        // This ensures non-transactional writes cannot corrupt in-flight transactions.
        //
        // Exception: attribute_not_exists(<key>) maps directly to a null-aware LWT,
        // which is atomic without a prior read.

        // Collect key attribute names for attribute_not_exists detection.
        let key_attr_names: Vec<&str> = key_info
            .key_schema
            .iter()
            .map(|k| k.attribute_name.as_str())
            .collect();

        let key_not_exists_condition = is_attribute_not_exists_key(condition, &key_attr_names);
        if key_not_exists_condition && ttl_config.is_none() {
            return self
                .put_item_if_not_exists(
                    key_info,
                    item,
                    stream,
                    &data_keyspace,
                    &ddb_table,
                    &pk_text,
                    &item_text,
                    &indexes,
                    sys_delay,
                )
                .await;
        }

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            // Table has sort key
            let sk_value = item
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            // Read old item including prepared_txn_id for transaction conflict detection
            let select_query = format!(
                "SELECT item_data, prepared_txn_id FROM {data_keyspace}.{ddb_table} WHERE pk = ? AND {sk_col} = ?"
            );

            let old_result =
                query_with_pk_sk(&self.session, &select_query, pk_text.as_ref(), &sk).await?;

            let body = old_result
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse response: {e}")))?;

            let (old_item_opt, has_prepared_txn) = if let Some(rows) = body.into_rows() {
                if let Some(row) = rows.into_iter().next() {
                    let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
                    let prepared_txn_id: Option<uuid::Uuid> =
                        row.get_by_name("prepared_txn_id").ok().flatten();
                    (
                        item_data.map(json_to_item).transpose()?,
                        prepared_txn_id.is_some(),
                    )
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            };

            // Reject if item is part of an in-flight transaction
            if has_prepared_txn {
                return Err(super::delete_item::concurrent_owner_error(
                    ttl_config.is_some(),
                ));
            }

            // Evaluate condition against existing item (or empty if doesn't exist)
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

            let stream_stmt = stream.and_then(|cap| {
                stream_record_statement(
                    &data_keyspace,
                    &key_info.table_id,
                    key_info,
                    old_item_opt.as_ref(),
                    Some(&item),
                    cap,
                    &self.hlc,
                    self.stream_retention_seconds,
                )
            });

            if indexes.is_empty() && stream_stmt.is_none() && ttl_config.is_none() {
                // Fast path: no batch needed.
                let insert_query = format!(
                    "INSERT INTO {}.{} \
                     (pk, {}, item_data) \
                     VALUES (?, ?, ?)",
                    data_keyspace, ddb_table, sk_col
                );
                query_with_pk_sk_item(
                    &self.session,
                    &insert_query,
                    pk_text.as_ref(),
                    &sk,
                    &item_text,
                )
                .await?;
            } else {
                // LOGGED BATCH: item insert + optional index updates + optional stream record.
                let insert_cql = format!(
                    "INSERT INTO {}.{} \
                     (pk, {}, item_data) \
                     VALUES (?, ?, ?)",
                    data_keyspace, ddb_table, sk_col
                );
                let insert_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    cdrs_tokio::types::value::Value::from(pk_text.as_str()),
                    super::index::sk_to_value(&sk),
                    item_text.as_str().into(),
                ]);
                let mut batch = BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
                    .add_query(insert_cql, insert_qv);

                if !indexes.is_empty() {
                    super::index::sync_indexes(
                        &mut batch,
                        &data_keyspace,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        old_item_opt.as_ref(),
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
                        old_item_opt.as_ref(),
                        Some(&item),
                        sys_delay,
                    )
                    .await?
                } else {
                    0
                };

                if let Some(config) = ttl_config.as_ref() {
                    super::ttl::add_ttl_reconciliation_mutation(
                        &mut batch,
                        &data_keyspace,
                        key_info,
                        &item,
                    )?;
                    super::ttl::add_ttl_queue_mutations(
                        &mut batch,
                        &data_keyspace,
                        key_info,
                        &config.attribute,
                        config.generation,
                        old_item_opt.as_ref(),
                        Some(&item),
                    )?;
                }

                if let Some(stmt) = stream_stmt {
                    batch =
                        batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
                }

                // Acquire only after every fallible side-effect statement has been
                // prepared, then release this exact claim on every remaining path.
                let ttl_claim = if ttl_config.is_some() {
                    match self
                        .acquire_ttl_mutation_claim(key_info, &item, old_item_opt.as_ref())
                        .await
                    {
                        Ok(claim) => claim,
                        // An absent-row claim that cannot be taken means the row
                        // now exists, which is exactly what this condition
                        // forbids. Report the condition failure rather than
                        // retrying a write that can never apply.
                        Err(StorageError::TransactionConflict(_)) if key_not_exists_condition => {
                            return Err(StorageError::ConditionFailed(old_item_opt));
                        }
                        Err(error) => return Err(error),
                    }
                } else {
                    None
                };
                if let Some(timestamp) = ttl_claim.map(|_| chrono::Utc::now().timestamp_micros()) {
                    batch = batch.with_timestamp(timestamp);
                }

                let built = match batch.build() {
                    Ok(built) => built,
                    Err(error) => {
                        self.release_ttl_mutation_claim(key_info, &item, ttl_claim)
                            .await;
                        return Err(StorageError::Internal(error.to_string()));
                    }
                };
                if let Err(error) = self.session.batch(built).await {
                    self.release_ttl_mutation_claim(key_info, &item, ttl_claim)
                        .await;
                    return Err(StorageError::Internal(format!("Batch execution: {error}")));
                }
                self.release_ttl_mutation_claim(key_info, &item, ttl_claim)
                    .await;

                if async_enqueued > 0 {
                    self.gsi_queue.notify_workers();
                }
            }

            if let Err(error) = self.reconcile_ttl_item(key_info, &item).await {
                tracing::warn!(
                    table = %key_info.table_name,
                    "deferred post-commit TTL reconciliation for PutItem: {error}"
                );
            }
            Ok(if return_old { old_item_opt } else { None })
        } else {
            // PK-only table (no sort key)

            // Read old item including prepared_txn_id for transaction conflict detection
            let select_query = format!(
                "SELECT item_data, prepared_txn_id FROM {data_keyspace}.{ddb_table} WHERE pk = ?"
            );

            let old_result = self
                .session
                .query_with_values(
                    &select_query,
                    cdrs_tokio::query_values!(pk_text.as_ref() as &str),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Select for put_item: {e}")))?;

            let body = old_result
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse response: {e}")))?;

            let (old_item_opt, has_prepared_txn) = if let Some(rows) = body.into_rows() {
                if let Some(row) = rows.into_iter().next() {
                    let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
                    let prepared_txn_id: Option<uuid::Uuid> =
                        row.get_by_name("prepared_txn_id").ok().flatten();
                    (
                        item_data.map(json_to_item).transpose()?,
                        prepared_txn_id.is_some(),
                    )
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            };

            // Reject if item is part of an in-flight transaction
            if has_prepared_txn {
                return Err(super::delete_item::concurrent_owner_error(
                    ttl_config.is_some(),
                ));
            }

            // Evaluate condition
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

            let stream_stmt = stream.and_then(|cap| {
                stream_record_statement(
                    &data_keyspace,
                    &key_info.table_id,
                    key_info,
                    old_item_opt.as_ref(),
                    Some(&item),
                    cap,
                    &self.hlc,
                    self.stream_retention_seconds,
                )
            });

            if indexes.is_empty() && stream_stmt.is_none() && ttl_config.is_none() {
                // Fast path: no batch needed.
                let insert_query = format!(
                    "INSERT INTO {}.{} \
                     (pk, item_data) \
                     VALUES (?, ?)",
                    data_keyspace, ddb_table
                );
                self.session
                    .query_with_values(
                        &insert_query,
                        cdrs_tokio::query_values!(pk_text.as_ref() as &str, item_text.as_str()),
                    )
                    .await
                    .map_err(|e| StorageError::Internal(format!("Insert item: {e}")))?;
            } else {
                // LOGGED BATCH: item insert + optional index updates + optional stream record.
                let insert_cql = format!(
                    "INSERT INTO {}.{} \
                     (pk, item_data) \
                     VALUES (?, ?)",
                    data_keyspace, ddb_table
                );
                let insert_qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    cdrs_tokio::types::value::Value::from(pk_text.as_str()),
                    item_text.as_str().into(),
                ]);
                let mut batch = BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
                    .add_query(insert_cql, insert_qv);

                if !indexes.is_empty() {
                    super::index::sync_indexes(
                        &mut batch,
                        &data_keyspace,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        &indexes,
                        old_item_opt.as_ref(),
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
                        old_item_opt.as_ref(),
                        Some(&item),
                        sys_delay,
                    )
                    .await?
                } else {
                    0
                };

                if let Some(config) = ttl_config.as_ref() {
                    super::ttl::add_ttl_reconciliation_mutation(
                        &mut batch,
                        &data_keyspace,
                        key_info,
                        &item,
                    )?;
                    super::ttl::add_ttl_queue_mutations(
                        &mut batch,
                        &data_keyspace,
                        key_info,
                        &config.attribute,
                        config.generation,
                        old_item_opt.as_ref(),
                        Some(&item),
                    )?;
                }

                if let Some(stmt) = stream_stmt {
                    batch =
                        batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
                }

                // Acquire only after every fallible side-effect statement has been
                // prepared, then release this exact claim on every remaining path.
                let ttl_claim = if ttl_config.is_some() {
                    match self
                        .acquire_ttl_mutation_claim(key_info, &item, old_item_opt.as_ref())
                        .await
                    {
                        Ok(claim) => claim,
                        // An absent-row claim that cannot be taken means the row
                        // now exists, which is exactly what this condition
                        // forbids. Report the condition failure rather than
                        // retrying a write that can never apply.
                        Err(StorageError::TransactionConflict(_)) if key_not_exists_condition => {
                            return Err(StorageError::ConditionFailed(old_item_opt));
                        }
                        Err(error) => return Err(error),
                    }
                } else {
                    None
                };
                if let Some(timestamp) = ttl_claim.map(|_| chrono::Utc::now().timestamp_micros()) {
                    batch = batch.with_timestamp(timestamp);
                }

                let built = match batch.build() {
                    Ok(built) => built,
                    Err(error) => {
                        self.release_ttl_mutation_claim(key_info, &item, ttl_claim)
                            .await;
                        return Err(StorageError::Internal(error.to_string()));
                    }
                };
                if let Err(error) = self.session.batch(built).await {
                    self.release_ttl_mutation_claim(key_info, &item, ttl_claim)
                        .await;
                    return Err(StorageError::Internal(format!("Batch execution: {error}")));
                }
                self.release_ttl_mutation_claim(key_info, &item, ttl_claim)
                    .await;

                if async_enqueued > 0 {
                    self.gsi_queue.notify_workers();
                }
            }

            if let Err(error) = self.reconcile_ttl_item(key_info, &item).await {
                tracing::warn!(
                    table = %key_info.table_name,
                    "deferred post-commit TTL reconciliation for PutItem: {error}"
                );
            }
            Ok(if return_old { old_item_opt } else { None })
        }
    }

    /// Atomic `put_item` for the `attribute_not_exists(<key>)` condition.
    ///
    /// Uses a null-aware LWT so both a physically absent row and a metadata-only
    /// row are treated as logically absent. Returns `ConditionFailed` if another
    /// writer creates the item first.
    #[allow(clippy::too_many_arguments)]
    /// Insert an item only if its key does not exist.
    ///
    /// Reached only when TTL is disabled: a TTL-enabled table cannot use this
    /// fast path, because absence has to be established by the exact base-row
    /// claim instead of by `IF NOT EXISTS`. There is therefore no TTL queue or
    /// reconciliation work to do here.
    async fn put_item_if_not_exists(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        stream: Option<&StreamCapture>,
        data_keyspace: &str,
        ddb_table: &str,
        pk_text: &str,
        item_text: &str,
        indexes: &[super::index::IndexMeta],
        sys_delay: u64,
    ) -> Result<Option<Item>, StorageError> {
        use cdrs_tokio::types::IntoRustByName as _;

        let (insert_cql, insert_qv) = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = item
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);
            let cql = format!(
                "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = 1 \
                 WHERE pk = ? AND {sk_col} = ? \
                 IF item_data = null AND prepared_txn_id = null"
            );
            let qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                item_text.into(),
                cdrs_tokio::types::value::Value::from(pk_text),
                super::index::sk_to_value(&sk),
            ]);
            (cql, qv)
        } else {
            let cql = format!(
                "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = 1 \
                 WHERE pk = ? IF item_data = null AND prepared_txn_id = null"
            );
            let qv = cdrs_tokio::query::QueryValues::SimpleValues(vec![
                item_text.into(),
                cdrs_tokio::types::value::Value::from(pk_text),
            ]);
            (cql, qv)
        };

        let result = self
            .session
            .query_with_values(&insert_cql, insert_qv)
            .await
            .map_err(|e| StorageError::Internal(format!("put_item_if_not_exists: {e}")))?;

        let body = result
            .response_body()
            .map_err(|e| StorageError::Internal(format!("put_item_if_not_exists: {e}")))?;

        let rows = body.into_rows().unwrap_or_default();
        let row = rows.into_iter().next();

        let applied: bool = row
            .as_ref()
            .and_then(|r| r.get_r_by_name("[applied]").ok())
            .unwrap_or(true);

        if !applied {
            // The LWT response includes the existing row — parse item_data from it.
            let existing = row
                .as_ref()
                .and_then(|r| {
                    use cdrs_tokio::types::IntoRustByName as _;
                    r.get_r_by_name("item_data").ok()
                })
                .and_then(|s: String| json_to_item(s).ok());
            return Err(StorageError::ConditionFailed(existing));
        }

        // The LWT linearizes the insert. Persist every secondary effect and a
        // durable TTL reconciliation outbox entry in the following LOGGED BATCH.
        let stream_stmt = stream.and_then(|cap| {
            stream_record_statement(
                data_keyspace,
                &key_info.table_id,
                key_info,
                None,
                Some(&item),
                cap,
                &self.hlc,
                self.stream_retention_seconds,
            )
        });
        if indexes.is_empty() && stream_stmt.is_none() {
            return Ok(None);
        }
        let mut batch = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);

        if !indexes.is_empty() {
            super::index::sync_indexes(
                &mut batch,
                data_keyspace,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                indexes,
                None,
                Some(&item),
                sys_delay,
            )?;
        }

        let async_enqueued = if !indexes.is_empty() {
            super::index::enqueue_async_indexes(
                &self.session,
                &mut batch,
                data_keyspace,
                key_info,
                indexes,
                None,
                Some(&item),
                sys_delay,
            )
            .await?
        } else {
            0
        };

        if let Some(stmt) = stream_stmt {
            batch = batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
        }

        let built = batch
            .build()
            .map_err(|error| StorageError::Internal(error.to_string()))?;
        self.session.batch(built).await.map_err(|error| {
            StorageError::Internal(format!("put_item_if_not_exists batch: {error}"))
        })?;

        if async_enqueued > 0 {
            self.gsi_queue.notify_workers();
        }
        Ok(None) // return_old is always None — item didn't exist
    }

    /// Implementation of `DataEngine::get_item`.
    pub(crate) async fn get_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let data_keyspace = self.account_keyspace(&key_info.account_id);
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            // Table has sort key
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);

            let query = format!(
                "SELECT item_data FROM {data_keyspace}.{ddb_table} WHERE pk = ? AND {sk_col} = ?"
            );

            let result = query_with_pk_sk(&self.session, &query, pk_text.as_ref(), &sk).await?;

            let body = result
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse response: {e}")))?;

            if let Some(rows) = body.into_rows()
                && let Some(row) = rows.into_iter().next()
            {
                let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
                return item_data.map(json_to_item).transpose();
            }

            Ok(None)
        } else {
            // PK-only table
            let query = format!("SELECT item_data FROM {data_keyspace}.{ddb_table} WHERE pk = ?");

            let result = self
                .session
                .query_with_values(&query, cdrs_tokio::query_values!(pk_text.as_ref() as &str))
                .await
                .map_err(|e| StorageError::Internal(format!("Get item: {e}")))?;

            let body = result
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse response: {e}")))?;

            if let Some(rows) = body.into_rows()
                && let Some(row) = rows.into_iter().next()
            {
                let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
                return item_data.map(json_to_item).transpose();
            }

            Ok(None)
        }
    }
}

/// Returns `true` if `condition` is exactly `attribute_not_exists(<key_attr>)` where
/// `<key_attr>` resolves to one of the names in `key_attr_names` after applying
/// expression name substitutions from `maps`.
///
/// This is the only condition we map directly to a null-aware LWT without a
/// prior read.
pub(crate) fn is_attribute_not_exists_key(
    condition: Option<&Expr>,
    key_attr_names: &[&str],
) -> bool {
    let Some(Expr::Function { name, args }) = condition else {
        return false;
    };
    if name != "attribute_not_exists" || args.len() != 1 {
        return false;
    }
    let Expr::Path(path) = &args[0] else {
        return false;
    };
    if path.len() != 1 {
        return false;
    }
    let extenddb_core::expression::PathElement::Attribute(attr) = &path[0] else {
        return false;
    };
    key_attr_names.contains(&attr.as_str())
}
