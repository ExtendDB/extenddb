// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` implementation for the Cassandra backend.

use crate::cassandra_util::PolicySession as _;
use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{Item, KeyType, TableKeyInfo};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_sk, pk_to_text, sk_column};

use super::ddl::data_table_name;
use super::{json_to_item, query_with_pk_sk};
use crate::CassandraEngine;
use crate::cassandra_util::{Stmt, batch};
use crate::stream_util::stream_record_statement;

// 400 KB limit from DynamoDB specification
const MAX_ITEM_SIZE_BYTES: usize = 400 * 1024;

/// Maximum OCC retry attempts before giving up.
const OCC_MAX_RETRIES: u32 = 20;
/// Base sleep for OCC backoff in milliseconds.
const OCC_BASE_DELAY_MS: u64 = 2;
/// Exponent cap for OCC backoff (max sleep = base * 2^cap = 16ms).
const OCC_EXP_CAP: u32 = 3;
/// Attempts to read back the row after a lost-answer LWT, so a dropped
/// connection can reconnect before the commit check gives up (#893).
const COMMIT_CHECK_PROBES: u32 = 6;

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
        // Both are catalog reads with no data dependency; overlap them.
        let (indexes, ttl_config) = futures::try_join!(
            super::index::fetch_indexes_for_table(
                &key_info.table_id,
                &self.session,
                &catalog_keyspace,
            ),
            self.ttl_config_for_table(&key_info.account_id, &key_info.table_name),
        )?;
        // Read live, once per write call (not per OCC attempt), and only
        // when some GSI follows the system setting.
        let sys_delay = self.system_index_delay(&indexes).await;
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
            let read = self
                .occ_read(
                    &data_keyspace,
                    &ddb_table,
                    &pk_text,
                    sk_opt.as_ref(),
                    sk_col_opt,
                    ttl_config.is_some(),
                )
                .await;
            let (old_json, version) = match read {
                Ok(read) => read, // Another owner holds the row. On a TTL-enabled table that is
                // transient, so treat it like a lost OCC race and re-read.
                Err(StorageError::TransactionConflict(message)) => {
                    if attempt == OCC_MAX_RETRIES {
                        return Err(StorageError::TransactionConflict(message));
                    }
                    let window_ms = OCC_BASE_DELAY_MS * (1u64 << attempt.min(OCC_EXP_CAP));
                    let sleep_ms = rand::random::<u64>() % window_ms.max(1);
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };

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

            let old_item = if return_old && item_existed {
                Some(item.clone())
            } else {
                None
            };
            let pre_mutation_item = if item_existed
                && (!indexes.is_empty() || stream.is_some() || ttl_config.is_some())
            {
                Some(item.clone())
            } else {
                None
            };

            // Apply update actions
            expression::apply_update_validated(actions, &mut item, maps, &[], &[])
                .map_err(|e| StorageError::Validation(e.to_string()))?;
            validation::validate_item_size(&item, MAX_ITEM_SIZE_BYTES)
                .map_err(|e| StorageError::Validation(e.to_string()))?;
            // Secondary-index keys on the written item, once the condition has
            // passed and the statement is applied. Literal operands were
            // checked by the engine ahead of the condition; what remains here
            // is a value computed from the existing item (an attribute
            // reference, the fallback of if_not_exists) of the wrong type, and
            // a key left empty. Without this the value reaches the index row
            // write and the malformed row breaks later writes to the same item.
            if !indexes.is_empty() {
                let idx_refs = super::index::index_key_refs(&indexes);
                validation::validate_index_key_types_after_update(
                    &item,
                    &idx_refs,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| StorageError::Validation(e.to_string()))?;
                validation::validate_index_key_not_empty(
                    &item,
                    &idx_refs,
                    validation::SecondaryIndexEmptyContext::UpdateExpression,
                )
                .map_err(|e| StorageError::Validation(e.to_string()))?;
            }

            // REMOVE and DELETE on a key that holds no item change nothing: the
            // service writes no item and emits no stream record (issue #478).
            // The actions were still applied above so that their validation
            // errors surface the same way as on an existing item.
            if !item_existed && expression::removes_only(actions) {
                return Ok((None, None));
            }

            let new_item = if return_new { Some(item.clone()) } else { None };

            let item_json =
                serde_json::to_value(&item).map_err(|e| StorageError::Internal(e.to_string()))?;
            let item_json_str = item_json.to_string();

            let ttl_claim = if ttl_config.is_some() {
                match self
                    .acquire_ttl_mutation_claim(key_info, key, pre_mutation_item.as_ref())
                    .await
                {
                    Ok(claim) => claim,
                    // Losing the claim is the same class of event as losing the
                    // OCC race: the image moved, so re-read and rebuild rather
                    // than failing the caller's write.
                    Err(StorageError::TransactionConflict(_)) => {
                        if attempt < OCC_MAX_RETRIES {
                            let window_ms = OCC_BASE_DELAY_MS * (1u64 << attempt.min(OCC_EXP_CAP));
                            let sleep_ms = rand::random::<u64>() % window_ms.max(1);
                            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                        }
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            let mutation_timestamp = ttl_claim.map(|_| chrono::Utc::now().timestamp_micros());

            // --- WRITE (with OCC or TTL-claim guard) ---
            let write_result = self
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
                    ttl_config.as_ref().map(|config| config.attribute.as_str()),
                    ttl_config.as_ref().map(|config| config.generation),
                    ttl_claim,
                    mutation_timestamp,
                )
                .await;
            // On success the claimed path's batch cleared the claim columns at
            // its pinned timestamp; the detached exact release covers a
            // trailing local clock. A failed write may still hold the claim,
            // so it releases synchronously.
            if write_result.is_err() {
                self.release_ttl_mutation_claim(key_info, key, ttl_claim)
                    .await;
            } else {
                self.spawn_release_ttl_claim(key_info, key, ttl_claim);
            }
            let applied = write_result?;

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

        // The version check lost to other single-item writers for the whole
        // retry budget. No TransactWriteItems holds the row (that is the
        // read-path TransactionConflict above), so this is contention the
        // backend could not serialize now, not a held item. DynamoDB answers a
        // single-item write with TransactionConflictException only while a
        // transaction holds the item, so answer a retryable 503 that the SDKs
        // retry rather than a 400 the application sees.
        Err(StorageError::Transient(
            "update_item: too many concurrent writers on this item".to_owned(),
        ))
    }

    /// Read `item_data`, `version`, and `prepared_txn_id` for OCC.
    ///
    /// Returns `(existing_item, version)`. A null `version` column (rows written
    /// before the OCC protocol was enforced) is treated as `0`.
    ///
    /// Returns `TransactionConflict` if `prepared_txn_id` is set.
    async fn occ_read(
        &self,
        data_keyspace: &str,
        ddb_table: &str,
        pk_text: &str,
        sk: Option<&extenddb_storage::util::SortKeyValue>,
        sk_col: Option<&'static str>,
        ttl_enabled: bool,
    ) -> Result<(Option<Item>, Option<i64>), StorageError> {
        use cdrs_tokio::types::IntoRustByName as _;

        let row_opt = if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
            let q = format!(
                "SELECT item_data, version, prepared_txn_id FROM {data_keyspace}.{ddb_table} WHERE pk = ? AND {sk_col} = ?"
            );
            let result = query_with_pk_sk(&self.session, &q, Stmt::Read, pk_text, sk).await?;
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
            return Err(super::delete_item::concurrent_owner_error(ttl_enabled));
        }

        // Static partition metadata can produce a physical row with no logical item.
        let item_data: Option<String> = row.get_by_name("item_data").ok().flatten();
        let version: Option<i64> = row.get_by_name("version").ok().flatten();

        Ok((item_data.map(json_to_item).transpose()?, version))
    }

    /// Attempt the OCC write. Returns `true` if `[applied]`, `false` on lost race.
    ///
    /// Uses `IF version = ? AND prepared_txn_id = null` for existing items, or
    /// `IF item_data = null AND prepared_txn_id = null` for logical creation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn occ_write(
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
        ttl_attribute: Option<&str>,
        ttl_generation: Option<uuid::Uuid>,
        ttl_claim: Option<uuid::Uuid>,
        mutation_timestamp: Option<i64>,
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

        // The version this write stores, and the write timestamp of the index
        // rows it produces (see `data::version`). On the claimed path the clock
        // reading is the mutation timestamp pinned right after the claim.
        let next_version = super::version::next_row_version_at(
            version,
            mutation_timestamp.unwrap_or_else(super::version::now_micros),
        )?;

        // Build the LWT statement used when no TTL claim owns the existing row.
        let (lwt_cql, lwt_qv) = if item_existed {
            if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
                let cql = format!(
                    "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = ? \
                     WHERE pk = ? AND {sk_col} = ? \
                     IF version = ? AND prepared_txn_id = null"
                );
                let vals = vec![
                    item_json_str.into(),
                    next_version.into(),
                    cdrs_tokio::types::value::Value::from(pk_text),
                    super::index::sk_to_value(sk),
                    super::version::fence_value(version),
                ];
                (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
            } else {
                let cql = format!(
                    "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = ? \
                     WHERE pk = ? IF version = ? AND prepared_txn_id = null"
                );
                let vals = vec![
                    item_json_str.into(),
                    next_version.into(),
                    cdrs_tokio::types::value::Value::from(pk_text),
                    super::version::fence_value(version),
                ];
                (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
            }
        } else if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
            let cql = format!(
                "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = ? \
                 WHERE pk = ? AND {sk_col} = ? \
                 IF item_data = null AND prepared_txn_id = null"
            );
            let vals = vec![
                item_json_str.into(),
                next_version.into(),
                cdrs_tokio::types::value::Value::from(pk_text),
                super::index::sk_to_value(sk),
            ];
            (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
        } else {
            let cql = format!(
                "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = ? \
                 WHERE pk = ? IF item_data = null AND prepared_txn_id = null"
            );
            let vals = vec![
                item_json_str.into(),
                next_version.into(),
                cdrs_tokio::types::value::Value::from(pk_text),
            ];
            (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
        };

        if ttl_claim.is_none() {
            // Preserve f997485's Paxos linearization for ordinary writes.
            match self.session.run(Stmt::Lwt, &lwt_cql, lwt_qv).await {
                Ok(result) => {
                    if !crate::cassandra_util::lwt_applied(&result)? {
                        return Ok(false);
                    }
                }
                // The coordinator lost the answer, but the Paxos round may
                // have committed. A committed write must not surface as a
                // retryable or internal error, or the client's retry doubles a
                // non-idempotent update (#893, same class as #342). Read the
                // row at LOCAL_SERIAL (which sees a committed round, unlike the
                // LOCAL_QUORUM occ_read) and decide from our exact image. Never
                // re-run the body on an unknown outcome.
                Err(e) if crate::cassandra_util::lwt_outcome_unknown(&e) => {
                    use cdrs_tokio::types::IntoRustByName as _;
                    // The serial read confirms whether the lost write
                    // committed. The same fault (a dropped connection) can
                    // break the read too, so retry it to let the pool
                    // reconnect before giving up, or the committed write is
                    // still reported as failed (#893).
                    let row = {
                        let mut got = None;
                        let mut last = None;
                        for probe in 0..COMMIT_CHECK_PROBES {
                            match super::select_by_pk_at(
                                &self.session,
                                data_keyspace,
                                ddb_table,
                                "version, item_data",
                                pk_text,
                                sk,
                                sk_col,
                                Stmt::SerialRead,
                            )
                            .await
                            {
                                Ok(r) => {
                                    got = Some(r);
                                    break;
                                }
                                Err(err) => {
                                    last = Some(err);
                                    if probe + 1 < COMMIT_CHECK_PROBES {
                                        tokio::time::sleep(std::time::Duration::from_millis(
                                            50u64 << probe,
                                        ))
                                        .await;
                                    }
                                }
                            }
                        }
                        match got {
                            Some(r) => r,
                            None => return Err(last.unwrap()),
                        }
                    };
                    let stored_item: Option<String> = row
                        .as_ref()
                        .and_then(|r| r.get_by_name("item_data").ok().flatten());
                    if stored_item.as_deref() != Some(item_json_str) {
                        let stored_version: Option<i64> = row
                            .as_ref()
                            .and_then(|r| r.get_by_name("version").ok().flatten());
                        // Our image is not stored. If the fence is intact (an
                        // existing row still at the fenced version, or a create
                        // whose row is still absent) the write provably did not
                        // land, so retry it like a lost race.
                        let never_applied = if item_existed {
                            stored_version == version
                        } else {
                            stored_item.is_none()
                        };
                        if never_applied {
                            return Ok(false);
                        }
                        // shortcut: the version advanced and our image is not
                        // stored, so a committed-then-overwritten write cannot
                        // be told from one that never committed. Fail honestly
                        // rather than re-apply. The stronger form re-issues this
                        // idempotent full-image LWT; verify it on an RF>1
                        // cluster (#1152) before adding it.
                        return Err(StorageError::Internal(
                            "occ_write lwt outcome unknown: version advanced".to_owned(),
                        ));
                    }
                    // Our exact image is stored: the write committed. Proceed.
                }
                Err(e) => {
                    return Err(StorageError::Internal(format!("occ_write lwt: {e}")));
                }
            }
            if indexes.is_empty() && stream_stmt.is_none() {
                return Ok(true);
            }
        }

        // Cassandra cannot mix an LWT with non-LWT statements in one batch. For a
        // claimed row, the exact claim is the fence and the base write joins this
        // LOGGED BATCH at the pinned mutation timestamp: the claim cells it
        // clears must not land above a later claim's Paxos timestamp, so the
        // batch stays on the wall clock and the claim serializes the writers.
        // For an unclaimed row, the LWT above commits first and this batch
        // durably records all secondary effects and reconciliation work. Its
        // index statements carry the version that LWT stored, so index rows
        // resolve in base-row commit order, and its queue rows and stream
        // record keep Cassandra's timestamp (`data::version`).
        let mut batch = batch();
        if let Some(timestamp) = mutation_timestamp {
            batch = batch.with_timestamp(timestamp);
        }
        let index_stamp = mutation_timestamp.is_none().then_some(next_version);

        if ttl_claim.is_some() {
            let (update_cql, update_qv) = if let (Some(sk), Some(sk_col)) = (sk, sk_col) {
                let cql = format!(
                    "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = ?, \
                     prepared_txn_id = null, prepared_txn_timestamp = null \
                     WHERE pk = ? AND {sk_col} = ?"
                );
                let vals = vec![
                    item_json_str.into(),
                    next_version.into(),
                    cdrs_tokio::types::value::Value::from(pk_text),
                    super::index::sk_to_value(sk),
                ];
                (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
            } else {
                let cql = format!(
                    "UPDATE {data_keyspace}.{ddb_table} SET item_data = ?, version = ?, \
                     prepared_txn_id = null, prepared_txn_timestamp = null WHERE pk = ?"
                );
                let vals = vec![
                    item_json_str.into(),
                    next_version.into(),
                    cdrs_tokio::types::value::Value::from(pk_text),
                ];
                (cql, cdrs_tokio::query::QueryValues::SimpleValues(vals))
            };
            batch = batch.add_query(update_cql, update_qv);
        }

        if !indexes.is_empty() {
            super::index::sync_indexes_at(
                &mut batch,
                data_keyspace,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                indexes,
                pre_mutation_item,
                Some(new_item),
                sys_delay,
                index_stamp,
            )?;
        }

        let async_enqueued = if indexes.is_empty() {
            0
        } else {
            super::index::enqueue_async_indexes(
                &self.session,
                &mut batch,
                data_keyspace,
                key_info,
                indexes,
                pre_mutation_item,
                Some(new_item),
                sys_delay,
                next_version,
            )
            .await?
        };

        if let (Some(_), Some(attribute)) = (ttl_claim, ttl_attribute) {
            super::ttl::add_ttl_reconciliation_mutation(
                &mut batch,
                data_keyspace,
                key_info,
                attribute,
                new_item,
            )?;
        }

        if let (Some(attribute), Some(generation)) = (ttl_attribute, ttl_generation) {
            super::ttl::add_ttl_queue_mutations(
                &mut batch,
                data_keyspace,
                key_info,
                attribute,
                generation,
                pre_mutation_item,
                Some(new_item),
            )?;
        }

        if let Some(stmt) = stream_stmt {
            batch = batch.add_query(stmt, cdrs_tokio::query::QueryValues::SimpleValues(vec![]));
        }

        let built = batch
            .build()
            .map_err(|error| StorageError::Internal(error.to_string()))?;
        self.session
            .run_batch(built)
            .await
            .map_err(|error| StorageError::Internal(format!("occ_write batch: {error}")))?;

        if async_enqueued > 0 {
            self.gsi_queue.notify_workers();
        }

        Ok(true)
    }
}
