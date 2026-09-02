// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Background processing for DynamoDB TTL expiration.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::error::StorageError;
use extenddb_storage::{CancellationToken, MetadataEngine, TableEngine, sleep_or_shutdown};

use crate::CassandraEngine;

const SCAN_INTERVAL: Duration = Duration::from_secs(60);
const BATCH_SIZE: usize = 100;
/// Rows drained per cleanup pass for a retired generation. Cleanup is retried
/// every cycle until the generation is empty, so this only bounds one pass.
const DRAIN_BATCH_SIZE: usize = 100;

pub(crate) async fn ttl_cleanup_worker(
    storage: Arc<CassandraEngine>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    while sleep_or_shutdown(&token, SCAN_INTERVAL).await {
        if let Err(error) = reconcile_pending_once(&storage, 1_000).await {
            tracing::warn!("TTL worker: reconciliation outbox failed: {error}");
        }
        retry_pending_cleanup(&storage).await;
        retry_pending_indexes(&storage).await;
        sweep_once(&storage, &metrics).await;
    }
}

/// Drain up to `limit` durable reconciliation records. Rows are removed only
/// after the current base item has been reconciled into the active generation.
pub async fn reconcile_pending_once(
    storage: &CassandraEngine,
    limit: usize,
) -> Result<usize, StorageError> {
    use cdrs_tokio::types::IntoRustByName;

    let mut processed = 0usize;
    for keyspace in crate::workers::list_account_keyspaces(storage).await? {
        for partition in 0..crate::data::ttl::TTL_SHARDS {
            let remaining = limit.saturating_sub(processed);
            if remaining == 0 {
                return Ok(processed);
            }
            let query = format!(
                "SELECT id, table_id, account_id, table_name, key_data \
                 FROM {keyspace}.ttl_reconcile_pending WHERE worker_partition = ? \
                 LIMIT {remaining}"
            );
            let rows = match crate::cassandra_util::query_rows(
                &storage.session,
                &query,
                cdrs_tokio::query_values!(partition),
                "ttl_reconcile_pending",
            )
            .await
            {
                Ok(rows) => rows,
                Err(error) if crate::workers::is_table_not_found(&error) => break,
                Err(error) => return Err(error),
            };
            for row in rows {
                let id: uuid::Uuid = row.get_r_by_name("id").map_err(|error| {
                    StorageError::Internal(format!("Parse TTL outbox id: {error}"))
                })?;
                let table_id: String =
                    crate::cassandra_util::get_column(&row, "table_id", "ttl_reconcile_pending")?;
                let account_id: String =
                    crate::cassandra_util::get_column(&row, "account_id", "ttl_reconcile_pending")?;
                let table_name: String =
                    crate::cassandra_util::get_column(&row, "table_name", "ttl_reconcile_pending")?;
                let key_data: String =
                    crate::cassandra_util::get_column(&row, "key_data", "ttl_reconcile_pending")?;
                let key: extenddb_core::types::Item =
                    serde_json::from_str(&key_data).map_err(|error| {
                        StorageError::Internal(format!("Parse TTL outbox key: {error}"))
                    })?;

                let reconcile = match storage.fetch_table_key_info(&account_id, &table_name).await {
                    Ok(key_info) if key_info.table_id == table_id => {
                        match storage.get_item_quorum(&key_info, &key).await? {
                            Some(item) => storage.reconcile_ttl_item(&key_info, &item).await,
                            None => Ok(()),
                        }
                    }
                    Ok(_) | Err(StorageError::TableNotFound(_)) => Ok(()),
                    Err(error) => Err(error),
                };
                if let Err(error) = reconcile {
                    tracing::warn!("TTL worker: reconcile {table_name} failed: {error}");
                    continue;
                }

                let delete = format!(
                    "DELETE FROM {keyspace}.ttl_reconcile_pending \
                     WHERE worker_partition = ? AND id = ?"
                );
                storage
                    .session
                    .query_with_values(
                        &delete,
                        cdrs_tokio::query_values!(
                            partition,
                            cdrs_tokio::types::value::Bytes::new(id.as_bytes().to_vec())
                        ),
                    )
                    .await
                    .map_err(|error| {
                        StorageError::Internal(format!("Delete TTL outbox row: {error}"))
                    })?;
                processed += 1;
            }
        }
    }
    Ok(processed)
}

/// Finish or abort work that was already claimed when a TTL generation was
/// retired.
///
/// Disabling TTL, or re-enabling it under a new generation, must not simply
/// delete the old generation's queue rows: a claimed row owns a base-row claim,
/// and an `EFFECTS_APPLIED` row additionally owns index deletions and a
/// published `REMOVE` record. The rule is decided by how much is already
/// durable:
///
/// * `CLAIMED` — nothing externally visible has happened yet, so release the
///   claim and drop the work. Disabling TTL stops the deletion.
/// * `EFFECTS_APPLIED` — index and stream effects are already durable, so the
///   base delete must still be completed, otherwise a live item is left with
///   its index rows removed. If the image has since changed, the writer that
///   changed it rewrote its own index rows, so the work is simply completed.
///
/// `PENDING` rows are left to `clear_ttl_generation`.
pub(crate) async fn drain_retired_generation(
    storage: &CassandraEngine,
    account_id: &str,
    table_name: &str,
    generation: uuid::Uuid,
) -> Result<(), StorageError> {
    use crate::data::ttl::TtlWorkState;

    let key_info = match storage.fetch_table_key_info(account_id, table_name).await {
        Ok(key_info) => key_info,
        // The table is gone; its whole keyspace-level queue is removed by the
        // table-deletion path instead.
        Err(StorageError::TableNotFound(_)) => return Ok(()),
        Err(error) => return Err(error),
    };
    let account_keyspace = storage.account_keyspace(account_id);
    let work = crate::data::ttl::load_generation_work(
        storage,
        &account_keyspace,
        &key_info.table_id,
        generation,
        DRAIN_BATCH_SIZE,
    )
    .await?;

    for row in work {
        if row.state == TtlWorkState::Pending {
            continue;
        }
        let Some(work_id) = row.work_id else {
            continue;
        };
        let Some(work_data) = row.work_data.clone() else {
            continue;
        };
        let key: extenddb_core::types::Item = serde_json::from_str(&row.entry.key_data)
            .map_err(|error| StorageError::Internal(format!("Parse TTL work key: {error}")))?;

        if row.state == TtlWorkState::Claimed {
            storage.release_ttl_claim(&key_info, &key, work_id).await?;
            let _ = crate::data::ttl::abort_claimed_ttl_work(
                storage,
                &account_keyspace,
                &key_info.table_id,
                generation,
                &row,
            )
            .await?;
            continue;
        }

        // EFFECTS_APPLIED.
        let current = storage.get_item_quorum(&key_info, &key).await?;
        if current.as_ref() == Some(&work_data.old_item)
            && storage
                .ensure_ttl_work_claim(&key_info, &key, &work_data.old_item, work_id)
                .await?
        {
            storage
                .delete_ttl_base_exact(&key_info, &key, &work_data.old_item, work_id)
                .await?;
        } else {
            storage.release_ttl_claim(&key_info, &key, work_id).await?;
        }
        let _ = crate::data::ttl::complete_ttl_work(
            storage,
            &account_keyspace,
            &key_info.table_id,
            generation,
            &row,
        )
        .await?;
    }
    Ok(())
}

async fn retry_pending_cleanup(storage: &CassandraEngine) {
    let pending = match storage.pending_ttl_cleanups().await {
        Ok(pending) => pending,
        Err(error) => {
            tracing::warn!("TTL worker: list pending cleanup failed: {error}");
            return;
        }
    };
    for (account_id, table_name, table_id, generation) in pending {
        if let Err(error) = storage
            .complete_ttl_cleanup(&account_id, &table_name, &table_id, generation)
            .await
        {
            tracing::warn!("TTL worker: cleanup retry failed for {table_name}: {error}");
        }
    }
}

/// Retry the queue backfill for any TTL-enabled table that is not yet ready.
///
/// `create_ttl_index` takes the table's control lease internally, so a table is
/// scanned by one host at a time even though every host runs this pass.
async fn retry_pending_indexes(storage: &CassandraEngine) {
    let Ok(enabled) = MetadataEngine::all_tables_with_ttl(storage).await else {
        return;
    };
    let Ok(ready) = MetadataEngine::all_tables_with_ttl_index_ready(storage).await else {
        return;
    };
    let ready_set: std::collections::HashSet<(&str, &str)> = ready
        .iter()
        .map(|(account, table, _)| (account.as_str(), table.as_str()))
        .collect();

    for (account_id, table_name, attribute) in &enabled {
        if !ready_set.contains(&(account_id.as_str(), table_name.as_str()))
            && let Err(error) =
                MetadataEngine::create_ttl_index(storage, account_id, table_name, attribute).await
        {
            tracing::debug!("TTL worker: queue backfill retry failed for {table_name}: {error}");
        }
    }
}

async fn process_ttl_work_row(
    storage: &CassandraEngine,
    key_info: &extenddb_core::types::TableKeyInfo,
    config: &crate::data::ttl::TtlConfig,
    mut work: crate::data::ttl::TtlWorkRow,
) -> Result<bool, StorageError> {
    use crate::data::ttl::{TtlStreamPlan, TtlWorkData, TtlWorkState};

    let account_keyspace = storage.account_keyspace(&key_info.account_id);
    let key: extenddb_core::types::Item = serde_json::from_str(&work.entry.key_data)
        .map_err(|error| StorageError::Internal(format!("Parse TTL work key: {error}")))?;

    if work.state == TtlWorkState::Pending {
        let Some(current) = storage.get_item_quorum(key_info, &key).await? else {
            let _ = crate::data::ttl::retire_pending_ttl_work(
                storage,
                &account_keyspace,
                &key_info.table_id,
                config.generation,
                &work.entry,
            )
            .await?;
            return Ok(false);
        };
        if crate::data::ttl::ttl_epoch_seconds(&current, &config.attribute)
            != Some(work.entry.expires_at)
        {
            if crate::data::ttl::retire_pending_ttl_work(
                storage,
                &account_keyspace,
                &key_info.table_id,
                config.generation,
                &work.entry,
            )
            .await?
            {
                storage.reconcile_ttl_item(key_info, &current).await?;
            }
            return Ok(false);
        }

        let stream = key_info
            .stream_specification
            .as_ref()
            .and_then(|specification| {
                if specification.stream_enabled {
                    specification
                        .stream_view_type
                        .map(|view_type| TtlStreamPlan {
                            event_id: uuid::Uuid::new_v4().to_string(),
                            sequence_number: storage
                                .hlc
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .generate(),
                            created_at_ms: chrono::Utc::now().timestamp_millis(),
                            region: storage.region.clone(),
                            view_type,
                        })
                } else {
                    None
                }
            });
        let work_id = uuid::Uuid::new_v4();
        let work_data = TtlWorkData {
            old_item: current,
            delete_timestamp_ms: chrono::Utc::now().timestamp_millis(),
            stream,
        };
        if !crate::data::ttl::claim_ttl_work(
            storage,
            &account_keyspace,
            &key_info.table_id,
            config.generation,
            &work.entry,
            work_id,
            &work_data,
        )
        .await?
        {
            return Ok(false);
        }
        work.state = TtlWorkState::Claimed;
        work.work_id = Some(work_id);
        work.work_data = Some(work_data);
    }

    let Some(work_id) = work.work_id else {
        return Err(StorageError::Internal(
            "TTL work missing work_id".to_owned(),
        ));
    };
    let Some(work_data) = work.work_data.as_ref() else {
        return Err(StorageError::Internal(
            "TTL work missing work_data".to_owned(),
        ));
    };

    if work.state == TtlWorkState::Claimed {
        let current = storage.get_item_quorum(key_info, &key).await?;
        match current {
            Some(ref item) if item == &work_data.old_item => {
                if !storage
                    .ensure_ttl_work_claim(key_info, &key, &work_data.old_item, work_id)
                    .await?
                {
                    return Ok(false);
                }
            }
            Some(item) => {
                storage.release_ttl_claim(key_info, &key, work_id).await?;
                if crate::data::ttl::abort_claimed_ttl_work(
                    storage,
                    &account_keyspace,
                    &key_info.table_id,
                    config.generation,
                    &work,
                )
                .await?
                {
                    storage.reconcile_ttl_item(key_info, &item).await?;
                }
                return Ok(false);
            }
            None => {
                storage.release_ttl_claim(key_info, &key, work_id).await?;
                let _ = crate::data::ttl::abort_claimed_ttl_work(
                    storage,
                    &account_keyspace,
                    &key_info.table_id,
                    config.generation,
                    &work,
                )
                .await?;
                return Ok(false);
            }
        }

        storage
            .apply_ttl_delete_effects(
                key_info,
                &work_data.old_item,
                work_id,
                work_data.delete_timestamp_ms,
                work_data.stream.as_ref(),
            )
            .await?;
        if !crate::data::ttl::mark_ttl_effects_applied(
            storage,
            &account_keyspace,
            &key_info.table_id,
            config.generation,
            &work,
        )
        .await?
        {
            return Ok(false);
        }
        work.state = TtlWorkState::EffectsApplied;
    }

    let current = storage.get_item_quorum(key_info, &key).await?;
    let deleted = match current {
        Some(ref item) if item == &work_data.old_item => {
            if !storage
                .ensure_ttl_work_claim(key_info, &key, &work_data.old_item, work_id)
                .await?
            {
                return Ok(false);
            }
            storage
                .delete_ttl_base_exact(key_info, &key, &work_data.old_item, work_id)
                .await?
        }
        Some(item) => {
            storage.release_ttl_claim(key_info, &key, work_id).await?;
            if crate::data::ttl::complete_ttl_work(
                storage,
                &account_keyspace,
                &key_info.table_id,
                config.generation,
                &work,
            )
            .await?
            {
                storage.reconcile_ttl_item(key_info, &item).await?;
            }
            return Ok(false);
        }
        None => {
            storage.release_ttl_claim(key_info, &key, work_id).await?;
            false
        }
    };

    let _ = crate::data::ttl::complete_ttl_work(
        storage,
        &account_keyspace,
        &key_info.table_id,
        config.generation,
        &work,
    )
    .await?;
    Ok(deleted)
}

/// Run one TTL sweep. Public for direct backend integration tests and manual
/// operational triggering; normal servers call it through `ttl_cleanup_worker`.
pub async fn sweep_once(storage: &CassandraEngine, metrics: &MetricsCollector) {
    let tables = match MetadataEngine::all_tables_with_ttl_index_ready(storage).await {
        Ok(tables) => tables,
        Err(error) => {
            tracing::warn!("TTL worker: failed to list tables: {error}");
            return;
        }
    };
    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    for (account_id, table_name, ttl_attribute) in &tables {
        let config = match storage.ttl_config_for_table(account_id, table_name).await {
            Ok(Some(config)) if config.attribute == *ttl_attribute => config,
            Ok(_) => continue,
            Err(error) => {
                tracing::warn!("TTL worker: config lookup failed for {table_name}: {error}");
                continue;
            }
        };
        let owner = match storage
            .acquire_ttl_sweep_lease(account_id, table_name, &config)
            .await
        {
            Ok(Some(owner)) => owner,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!("TTL worker: lease acquisition failed for {table_name}: {error}");
                continue;
            }
        };

        let result: Result<usize, StorageError> = async {
            let key_info = TableEngine::table_key_info(storage, account_id, table_name).await?;
            let account_keyspace = storage.account_keyspace(account_id);
            let work = crate::data::ttl::load_due_ttl_work(
                storage,
                &account_keyspace,
                &key_info.table_id,
                config.generation,
                now_epoch,
                BATCH_SIZE,
            )
            .await?;
            let mut deleted = 0usize;
            for row in work {
                if storage.ttl_config_for_table(account_id, table_name).await?
                    != Some(config.clone())
                    || !storage
                        .renew_ttl_sweep_lease(account_id, table_name, &config, owner)
                        .await?
                {
                    break;
                }
                let expires_at = row.entry.expires_at;
                if process_ttl_work_row(storage, &key_info, &config, row).await? {
                    deleted += 1;
                    metrics.record_ttl_deletion(table_name);
                    metrics.record_ttl_staleness(
                        table_name,
                        now_epoch.saturating_sub(expires_at) as f64,
                    );
                }
            }
            Ok(deleted)
        }
        .await;

        let _ = storage
            .release_ttl_sweep_lease(account_id, table_name, owner)
            .await;
        match result {
            Ok(deleted) if deleted > 0 => {
                tracing::info!("TTL worker: deleted {deleted} expired items from {table_name}");
            }
            Ok(_) => {}
            Err(error) => tracing::warn!("TTL worker: sweep failed for {table_name}: {error}"),
        }
    }
}
