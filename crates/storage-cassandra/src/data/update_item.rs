// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` implementation for the Cassandra backend.

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::query::BatchQueryBuilder;
use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, KeyType, TableKeyInfo};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_sk, pk_to_text, sk_column};

use super::ddl::data_table_name;
use super::{json_to_item, query_with_pk_sk};
use crate::CassandraEngine;
use crate::stream_util::stream_record_statement;

// 400 KB limit from DynamoDB specification
const MAX_ITEM_SIZE_BYTES: usize = 400 * 1024;

/// Maximum OCC retry attempts before giving up.
const OCC_MAX_RETRIES: u32 = 20;
/// Base sleep for OCC backoff in milliseconds.
const OCC_BASE_DELAY_MS: u64 = 2;
/// Exponent cap for OCC backoff (max sleep = base * 2^cap = 16ms).
const OCC_EXP_CAP: u32 = 3;

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

        // Resolve sort key once — used in every iteration of the OCC loop.
        let (sk_opt, sk_col_opt) = if let Some(sk_elem) = key_info
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
            (Some(parse_sk(sk_value, sk_type)?), Some(sk_column(sk_type)))
        } else {
            (None, None)
        };

        for attempt in 0..=OCC_MAX_RETRIES {
            // --- READ ---
            let (old_json, version) = self
                .occ_read(
                    &data_keyspace,
                    &ddb_table,
                    &pk_text,
                    sk_opt.as_ref(),
                    sk_col_opt,
                )
                .await?;

            let item_existed = old_json.is_some();
            let mut item = old_json.clone().unwrap_or_else(|| key.clone());

            // Evaluate condition
            let condition_item = if item_existed {
                &item
            } else {
                &std::collections::BTreeMap::new()
            };
            match super::check_condition(condition, condition_item, maps) {
                Ok(()) => {}
                Err(StorageError::ConditionFailed(_)) => {
                    return Err(StorageError::ConditionFailed(if item_existed {
                        Some(item)
                    } else {
                        None
                    }));
                }
                Err(e) => return Err(e),
            }

            let old_item = if return_old { Some(item.clone()) } else { None };
            let pre_mutation_item = if (!indexes.is_empty() || stream.is_some()) && item_existed {
                Some(item.clone())
            } else {
                None
            };

            // Apply update actions
            expression::apply_update_validated(actions, &mut item, maps, &[], &[])
                .map_err(|e| StorageError::Validation(e.to_string()))?;
            validation::validate_item_size(&item, MAX_ITEM_SIZE_BYTES)
                .map_err(|e| StorageError::Validation(e.to_string()))?;

            let new_item = if return_new { Some(item.clone()) } else { None };

            let item_json =
                serde_json::to_value(&item).map_err(|e| StorageError::Internal(e.to_string()))?;
            let item_json_str = item_json.to_string();

            // --- WRITE (with OCC guard) ---
            let applied = self
                .occ_write(
                    &data_keyspace,
                    &ddb_table,
                    &pk_text,
                    sk_opt.as_ref(),
                    sk_col_opt,
                    &item_json_str,
                    version,
                    item_existed,
                    &indexes,
                    key_info,
                    pre_mutation_item.as_ref(),
                    &item,
                    sys_delay,
                    stream,
                )
                .await?;

            if applied {
                return Ok((old_item, new_item));
            }

            // Lost the race — back off and retry.
            if attempt < OCC_MAX_RETRIES {
                let window_ms = OCC_BASE_DELAY_MS * (1u64 << attempt.min(OCC_EXP_CAP));
                let sleep_ms = rand::random::<u64>() % window_ms.max(1);
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            }
        }

        Err(StorageError::Internal(
            "update_item: too many concurrent writers on this item".to_owned(),
        ))
    }

    /// Read `item_data`, `version`, and `prepared_txn_id` for OCC.
    ///
    /// Returns `(existing_item, version)`. `version` is `None` for rows that
    /// pre-date the OCC column (treated as version 0 — `IF version = null`).
    ///
    /// Returns `TransactionConflict` if `prepared_txn_id` is set.
    async fn occ_read(
        &self,
        data_keyspace: &str,
        ddb_table: &str,
        pk_text: &str,
        sk: Option<&extenddb_storage::util::SortKeyValue>,
        sk_col: Option<&'static str>,
    ) -> Result<(Option<Item>, Option<i64>), StorageError> {
        use cdrs_tokio::types::IntoRustByName as _;

        let row_opt = if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
            let q = format!(
                "SELECT item_data, version, prepared_txn_id FROM {data_keyspace}.{ddb_table} WHERE pk = ? AND {sk_col} = ?"
            );
            let result = query_with_pk_sk(&self.session, &q, pk_text, sk).await?;
            result
                .response_body()
                .map_err(|e| StorageError::Internal(format!("occ_read response_body: {e}")))?
                .into_rows()
                .unwrap_or_default()
                .into_iter()
                .next()
        } else {
            let q = format!(
                "SELECT item_data, version, prepared_txn_id FROM {data_keyspace}.{ddb_table} WHERE pk = ?"
            );
            crate::cassandra_util::query_optional(
                &self.session,
                &q,
                cdrs_tokio::query_values!(pk_text),
                "occ_read",
            )
            .await?
        };

        let Some(row) = row_opt else {
            return Ok((None, None));
        };

        let prepared_txn_id: Option<uuid::Uuid> = row.get_by_name("prepared_txn_id").ok().flatten();
        if prepared_txn_id.is_some() {
            return Err(StorageError::TransactionCanceled(vec![
                extenddb_core::types::CancellationReason {
                    code: "TransactionConflict".to_owned(),
                    message: Some("Item is being modified by a concurrent transaction".to_owned()),
                    item: None,
                },
            ]));
        }

        let item_data: String = crate::cassandra_util::get_column(&row, "item_data", "occ_read")?;
        let version: Option<i64> = row.get_by_name("version").ok().flatten();

        Ok((Some(json_to_item(item_data)?), version))
    }

    /// Attempt the OCC write. Returns `true` if `[applied]`, `false` on lost race.
    ///
    /// Uses `IF version = ? AND prepared_txn_id = null` for existing items, or
    /// `INSERT ... IF NOT EXISTS` for new items (upsert).
    #[allow(clippy::too_many_arguments)]
    async fn occ_write(
        &self,
        data_keyspace: &str,
        ddb_table: &str,
        pk_text: &str,
        sk: Option<&extenddb_storage::util::SortKeyValue>,
        sk_col: Option<&'static str>,
        item_json_str: &str,
        version: Option<i64>,
        item_existed: bool,
        indexes: &[super::index::IndexMeta],
        key_info: &TableKeyInfo,
        pre_mutation_item: Option<&Item>,
        new_item: &Item,
        sys_delay: u64,
        stream: Option<&StreamCapture>,
    ) -> Result<bool, StorageError> {
        let stream_stmt = stream.and_then(|cap| {
            stream_record_statement(
                data_keyspace,
                &key_info.table_id,
                key_info,
                pre_mutation_item,
                Some(new_item),
                cap,
                &self.hlc,
                self.stream_retention_seconds,
            )
        });

        let next_version = version.unwrap_or(0) + 1;

        // Build the LWT statement and its values.
        let (lwt_cql, lwt_qv) = if item_existed {
            // UPDATE with IF version = ? AND prepared_txn_id = null
            let version_cond = if version.is_some() {
                "version = ?".to_owned()
            } else {
                "version = null".to_owned()
            };
            if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
                let cql = format!(
                    "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = ? \
                     WHERE pk = ? AND {sk_col} = ? \
                     IF {version_cond} AND prepared_txn_id = null"
                );
                let mut vals: Vec<cdrs_tokio::types::value::Value> = vec![
                    item_json_str.into(),
                    next_version.into(),
                    cdrs_tokio::types::value::Value::from(pk_text),
                    super::index::sk_to_value(sk),
                ];
                if version.is_some() {
                    vals.push(version.unwrap().into());
                }
                (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
            } else {
                let cql = format!(
                    "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = ? \
                     WHERE pk = ? \
                     IF {version_cond} AND prepared_txn_id = null"
                );
                let mut vals: Vec<cdrs_tokio::types::value::Value> = vec![
                    item_json_str.into(),
                    next_version.into(),
                    cdrs_tokio::types::value::Value::from(pk_text),
                ];
                if version.is_some() {
                    vals.push(version.unwrap().into());
                }
                (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
            }
        } else {
            // INSERT IF NOT EXISTS for new items
            if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
                let cql = format!(
                    "INSERT INTO {data_keyspace}.{ddb_table} (pk, {sk_col}, item_data, version) \
                     VALUES (?, ?, ?, ?) IF NOT EXISTS"
                );
                let vals = vec![
                    cdrs_tokio::types::value::Value::from(pk_text),
                    super::index::sk_to_value(sk),
                    item_json_str.into(),
                    1i64.into(),
                ];
                (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
            } else {
                let cql = format!(
                    "INSERT INTO {data_keyspace}.{ddb_table} (pk, item_data, version) \
                     VALUES (?, ?, ?) IF NOT EXISTS"
                );
                let vals = vec![
                    cdrs_tokio::types::value::Value::from(pk_text),
                    item_json_str.into(),
                    1i64.into(),
                ];
                (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
            }
        };

        // If no indexes or stream, execute the LWT directly and check [applied].
        if indexes.is_empty() && stream_stmt.is_none() {
            let result = self
                .session
                .query_with_values(&lwt_cql, lwt_qv)
                .await
                .map_err(|e| StorageError::Internal(format!("occ_write: {e}")))?;
            return occ_applied(&result);
        }

        // With indexes/stream we need a LOGGED BATCH. However, Cassandra does not
        // allow LWT statements inside a LOGGED BATCH with non-LWT statements.
        // Strategy: run the LWT alone first; if it applies, run the index/stream
        // updates in a separate UNLOGGED BATCH. The window between the two is safe
        // because the item is already committed — index staleness is acceptable
        // (the async GSI queue handles eventual consistency).
        let result = self
            .session
            .query_with_values(&lwt_cql, lwt_qv)
            .await
            .map_err(|e| StorageError::Internal(format!("occ_write lwt: {e}")))?;

        if !occ_applied(&result)? {
            return Ok(false);
        }

        // LWT applied — now fire index/stream updates in a best-effort batch.
        let mut batch = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);

        if !indexes.is_empty() {
            super::index::sync_indexes(
                &mut batch,
                data_keyspace,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                indexes,
                pre_mutation_item,
                Some(new_item),
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
                pre_mutation_item,
                Some(new_item),
                sys_delay,
            )
            .await?
        } else {
            0
        };

        if let Some(stmt) = stream_stmt {
            batch = batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
        }

        // Only execute the batch if it has statements (sync_indexes / enqueue / stream may add them).
        // BatchQueryBuilder has no public len(); check via build and catch empty-batch errors gracefully.
        if let Ok(built) = batch.build() {
            self.session
                .batch(built)
                .await
                .map_err(|e| StorageError::Internal(format!("occ_write index batch: {e}")))?;
        }

        if async_enqueued > 0 {
            self.gsi_queue.notify_workers();
        }

        Ok(true)
    }
}

/// Extract `[applied]` from an LWT response. Returns `Ok(true)` if applied,
/// `Ok(false)` if not applied (lost race), `Err` only on parse failure.
fn occ_applied(result: &cdrs_tokio::frame::Envelope) -> Result<bool, StorageError> {
    use cdrs_tokio::types::IntoRustByName as _;
    let body = result
        .response_body()
        .map_err(|e| StorageError::Internal(format!("occ_applied response_body: {e}")))?;
    let Some(rows) = body.into_rows() else {
        // No rows in response means the statement was not a conditional write
        // (shouldn't happen here) — treat as applied.
        return Ok(true);
    };
    let Some(row) = rows.into_iter().next() else {
        return Ok(true);
    };
    let applied: bool = row
        .get_r_by_name("[applied]")
        .map_err(|e| StorageError::Internal(format!("occ_applied parse [applied]: {e}")))?;
    Ok(applied)
}
