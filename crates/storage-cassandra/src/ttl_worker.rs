// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Background processing for DynamoDB TTL expiration.

use std::sync::Arc;

use futures::StreamExt as _;
use std::time::Duration;

use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::error::StorageError;
use extenddb_storage::{CancellationToken, MetadataEngine, TableEngine, sleep_or_shutdown};

use crate::CassandraEngine;

const SCAN_INTERVAL: Duration = Duration::from_secs(60);
/// Queue rows one sweep cycle will process per table. The queue is 64-way
/// sharded into independent partitions and every row transition is
/// conditional on its exact work id, so rows are processed concurrently
/// (see [`TTL_SWEEP_CONCURRENCY`]) and the batch can be sized by throughput
/// need rather than by serial round-trip time. Raising this above the other
/// backends' 100 is deliberate: their sweeps re-scan an indexed table, ours
/// drains a queue, and the per-table lease means the work happens exactly
/// once.
const BATCH_SIZE: usize = 1_000;
/// Concurrent in-flight rows per sweep. Each row costs a handful of quorum
/// reads and LWTs; concurrency hides that latency without changing the
/// protocol, because rows in different queue partitions are independent and
/// two entries for the same item key resolve through the base-row claim (the
/// loser leaves its row for the next cycle).
const TTL_SWEEP_CONCURRENCY: usize = 16;
/// Rows processed between lease renewals and config re-checks.
const TTL_SWEEP_RENEW_EVERY: usize = 64;
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

/// Run ambiguous-destroy repair independently from expiration sweeps.
///
/// Unresolved markers are deliberately non-dischargeable and can accumulate
/// after repeated process crashes. Keeping this pass on its own task prevents
/// that operational debt from delaying the bounded expiration worker.
pub(crate) async fn ttl_repair_worker(
    storage: Arc<CassandraEngine>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    while sleep_or_shutdown(&token, SCAN_INTERVAL).await {
        match reconcile_inflight_repairs_once(&storage).await {
            Ok((observed_markers, _resolved)) => {
                metrics.record_ttl_repair_markers(observed_markers as f64);
            }
            Err(error) => {
                tracing::warn!("TTL worker: inflight repair reconciliation failed: {error}");
            }
        }
    }
}

/// Interval between full audits of a TTL-enabled table's expiration queue.
///
/// The audit is a paged base-table scan, so it is priced like the enable
/// backfill and run rarely. Six hours bounds how long a lost registration can
/// go unnoticed while keeping steady-state cost at four scans per table per
/// day.
const AUDIT_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// Periodically rescan every ready TTL table and re-register any item whose
/// queue entry is missing.
///
/// This is the self-healing pass. Everything else in the TTL design tries to
/// prevent a registration from being lost — the in-batch queue mutation, the
/// write outbox, the destroy handshake — but a queue entry can be created only
/// by a write or by the enable backfill, so any loss that slips through every
/// guard is *permanent* for an item that is never written again, and nothing
/// alarms. The audit converts that class from permanent to
/// eventually-corrected, and its repair metric is the drift signal: a repair
/// means some loss path fired, and sustained repairs mean one is firing
/// repeatedly.
pub(crate) async fn ttl_audit_worker(
    storage: Arc<CassandraEngine>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    while sleep_or_shutdown(&token, AUDIT_INTERVAL).await {
        audit_ttl_queues_once(&storage, &metrics).await;
    }
}

/// One audit pass over every ready TTL table. Public for direct backend
/// integration tests and manual operational triggering.
pub async fn audit_ttl_queues_once(storage: &CassandraEngine, metrics: &MetricsCollector) {
    let tables = match MetadataEngine::all_tables_with_ttl_index_ready(storage).await {
        Ok(tables) => tables,
        Err(error) => {
            tracing::warn!("TTL audit: failed to list tables: {error}");
            return;
        }
    };
    for (account_id, table_name, attribute) in &tables {
        match storage
            .audit_ttl_queue_for_table(account_id, table_name, attribute, metrics)
            .await
        {
            Ok(Some(repaired)) if repaired > 0 => {
                tracing::warn!(
                    account_id,
                    table = %table_name,
                    repaired,
                    "TTL audit repaired missing queue registrations; some loss path fired"
                );
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!("TTL audit: {table_name} failed: {error}");
            }
        }
    }
}

/// Keyspaces the background TTL passes should visit this cycle.
///
/// Steady state visits only accounts that have a TTL-enabled table: on a
/// deployment with many accounts and few TTL users, scanning every keyspace's
/// 64 outbox partitions each minute is almost entirely empty reads. Every
/// [`FULL_SCAN_EVERY`]th cycle widens to all account keyspaces, so rows left
/// behind for since-disabled or since-deleted tables are still eventually
/// drained rather than lingering forever.
async fn ttl_scan_keyspaces(
    storage: &CassandraEngine,
    cycle: u64,
) -> Result<Vec<String>, StorageError> {
    const FULL_SCAN_EVERY: u64 = 10;
    if cycle.is_multiple_of(FULL_SCAN_EVERY) {
        return crate::workers::list_account_keyspaces(storage).await;
    }
    let tables = MetadataEngine::all_tables_with_ttl(storage).await?;
    let mut keyspaces: Vec<String> = tables
        .iter()
        .map(|(account_id, _, _)| storage.account_keyspace(account_id))
        .collect();
    keyspaces.sort_unstable();
    keyspaces.dedup();
    Ok(keyspaces)
}

/// Monotonic pass counter shared by the background TTL passes, driving the
/// periodic widening in [`ttl_scan_keyspaces`].
static TTL_SCAN_CYCLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Drain up to `limit` durable reconciliation records. Rows are removed only
/// after the current base item and its bucket registration have been reconciled
/// into the active generation using quorum-authoritative metadata.
pub async fn reconcile_pending_once(
    storage: &CassandraEngine,
    limit: usize,
) -> Result<usize, StorageError> {
    reconcile_pending_older_than(storage, limit, 0).await
}

/// [`reconcile_pending_once`] with an explicit minimum record age in seconds.
/// A negative age is useful in tests to include a just-inserted coordinator
/// timeuuid. Production no longer needs an age fence: ambiguous destroys retain
/// a separate non-dischargeable inflight marker until their result is definite.
pub async fn reconcile_pending_older_than(
    storage: &CassandraEngine,
    limit: usize,
    min_age_seconds: i64,
) -> Result<usize, StorageError> {
    use cdrs_tokio::types::IntoRustByName;

    let discharge_before_ms = chrono::Utc::now().timestamp_millis() - min_age_seconds * 1_000;
    if limit == 0 {
        return Ok(0);
    }
    // A negative age is the tests' drain-everything mode; it also forces the
    // exhaustive keyspace set so drains are deterministic regardless of where
    // the process-global cycle counter happens to be.
    let cycle = if min_age_seconds < 0 {
        0
    } else {
        TTL_SCAN_CYCLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    };
    let keyspaces = ttl_scan_keyspaces(storage, cycle).await?;
    let slots = crate::data::ttl::TTL_SHARDS as usize;
    // A global first-N scan can starve later keyspaces forever under sustained
    // writes. Treat `limit` as a per-account soft bound and give every one of
    // its 64 partitions a quota each pass. Each non-empty slot receives at
    // least one row.
    let per_slot = limit.div_ceil(slots).max(1);
    let mut processed = 0usize;
    for keyspace in keyspaces {
        for partition in 0..crate::data::ttl::TTL_SHARDS {
            // Page through the partition with a keyset cursor so a prefix of
            // persistently failing rows cannot permanently hide the valid
            // records behind it: each page resumes strictly after the last row
            // of the previous one, whether or not that row was processable.
            // Pages are bounded per cycle; anything beyond carries over.
            let page = per_slot.saturating_mul(4).max(per_slot).min(4_096);
            let mut page_cursor: Option<uuid::Uuid> = None;
            let mut slot_processed = 0usize;
            'pages: for _ in 0..OUTBOX_MAX_PAGES_PER_PARTITION {
                let query = match page_cursor {
                    Some(after) => format!(
                        "SELECT id, table_id, account_id, table_name, key_data \
                     FROM {keyspace}.ttl_reconcile_pending WHERE worker_partition = ? \
                     AND id > {after} AND id < maxTimeuuid({discharge_before_ms}) LIMIT {page}"
                    ),
                    None => format!(
                        "SELECT id, table_id, account_id, table_name, key_data \
                     FROM {keyspace}.ttl_reconcile_pending WHERE worker_partition = ? \
                     AND id < maxTimeuuid({discharge_before_ms}) LIMIT {page}"
                    ),
                };
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
                    // This pass covers every account keyspace; a failure in one
                    // must not abort the rest. The rows stay for the next cycle.
                    Err(error) => {
                        tracing::warn!(
                            "TTL worker: outbox partition {partition} failed in {keyspace}: {error}"
                        );
                        break 'pages;
                    }
                };
                let page_was_full = rows.len() >= page;
                for row in rows {
                    // the durable guarantee that an item reaches the queue at all,
                    // so propagating a per-row failure would starve reconciliation
                    // for every other item — and those items would never expire.
                    // Each failure is confined to its row, which is left in place
                    // for the next cycle.
                    // The cursor advances on the id alone, before the rest of the
                    // row is parsed: a row with a readable id but unreadable
                    // payload must still be paged past, or it stalls the cursor on
                    // itself forever.
                    let id: uuid::Uuid = if let Ok(id) = row.get_r_by_name("id") {
                        id
                    } else {
                        tracing::warn!("TTL worker: outbox row with unreadable id skipped");
                        continue;
                    };
                    page_cursor = Some(id);
                    let parsed = (|| -> Result<_, StorageError> {
                        let table_id: String = crate::cassandra_util::get_column(
                            &row,
                            "table_id",
                            "ttl_reconcile_pending",
                        )?;
                        let account_id: String = crate::cassandra_util::get_column(
                            &row,
                            "account_id",
                            "ttl_reconcile_pending",
                        )?;
                        let table_name: String = crate::cassandra_util::get_column(
                            &row,
                            "table_name",
                            "ttl_reconcile_pending",
                        )?;
                        let key_data: String = crate::cassandra_util::get_column(
                            &row,
                            "key_data",
                            "ttl_reconcile_pending",
                        )?;
                        let key: extenddb_core::types::Item = serde_json::from_str(&key_data)
                            .map_err(|error| {
                                StorageError::Internal(format!("Parse TTL outbox key: {error}"))
                            })?;
                        Ok((table_id, account_id, table_name, key))
                    })();
                    let (table_id, account_id, table_name, key) = match parsed {
                        Ok(parsed) => parsed,
                        Err(error) => {
                            tracing::warn!("TTL worker: unreadable outbox row skipped: {error}");
                            continue;
                        }
                    };

                    let reconcile = match storage
                        .fetch_table_key_info_quorum(&account_id, &table_name)
                        .await
                    {
                        Ok(key_info) if key_info.table_id == table_id => {
                            let config = storage
                                .ttl_config_for_table_quorum(&account_id, &table_name)
                                .await;
                            match (config, storage.get_item_quorum(&key_info, &key).await) {
                                (Ok(Some(config)), Ok(Some(item))) => storage
                                    .reconcile_ttl_item_with_config(&key_info, &item, &config)
                                    .await
                                    .map(|_| ()),
                                (Ok(_), Ok(_)) => Ok(()),
                                (Err(error), _) | (_, Err(error)) => Err(error),
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
                    if let Err(error) = crate::cassandra_util::execute_quorum::<StorageError>(
                        &storage.session,
                        &delete,
                        cdrs_tokio::query_values!(
                            partition,
                            cdrs_tokio::types::value::Bytes::new(id.as_bytes().to_vec())
                        ),
                        "delete TTL outbox row",
                    )
                    .await
                    {
                        // The queue is already reconciled; a row that fails to
                        // delete is re-verified and re-deleted next cycle.
                        tracing::warn!("TTL worker: delete outbox row failed: {error}");
                        continue;
                    }
                    processed += 1;
                    slot_processed += 1;
                    if slot_processed >= per_slot {
                        break 'pages;
                    }
                }
                if !page_was_full {
                    // Partition exhausted within the eligible range.
                    break 'pages;
                }
            }
        }
    }
    Ok(processed)
}

/// Pages the outbox pass may read per partition per cycle. Bounds the work a
/// backlogged or poisoned partition can consume while still guaranteeing the
/// scan advances past a failing prefix.
const OUTBOX_MAX_PAGES_PER_PARTITION: usize = 8;

/// Reconcile every surviving pre-destroy marker.
///
/// Active-generation rows are intentionally not removed by the worker: only
/// the destroyer can hand a definitive result to the normal outbox. Once a
/// quorum metadata read proves the table or recorded generation is retired,
/// the old destroy can no longer affect current queue state and the marker is
/// safely removed. The registry is bounded to 64 rows; an accumulation of marker rows
/// represents operational debt, logged for visibility and reported by the
/// returned observation count.
///
/// Returns `(observed, resolved)`: marker rows seen this pass, and how many
/// were reconciled. `observed` feeds the `TtlRepairMarkerCount` gauge —
/// sustained growth means destroys keep ending ambiguously or a repair is
/// stuck.
pub async fn reconcile_inflight_repairs_once(
    storage: &CassandraEngine,
) -> Result<(usize, usize), StorageError> {
    use cdrs_tokio::types::IntoRustByName;

    let mut observed = 0usize;
    let mut processed = 0usize;
    // Deliberately unfiltered: the discovery registry means an idle keyspace
    // costs exactly one quorum read here, and markers can reference tables
    // whose TTL (or whole table) is already gone — those must still resolve.
    for keyspace in crate::workers::list_account_keyspaces(storage).await? {
        let registry = match crate::cassandra_util::query_rows_quorum(
            &storage.session,
            &format!("SELECT worker_partition FROM {keyspace}.ttl_repair_inflight_partitions"),
            cdrs_tokio::query::QueryValues::SimpleValues(Vec::new()),
            "TTL repair inflight registry",
        )
        .await
        {
            Ok(rows) => rows,
            Err(error) if crate::workers::is_table_not_found(&error) => continue,
            Err(error) => {
                tracing::warn!("TTL worker: inflight registry failed in {keyspace}: {error}");
                continue;
            }
        };

        for registry_row in registry {
            let partition: i32 = match crate::cassandra_util::get_column::<i32, StorageError>(
                &registry_row,
                "worker_partition",
                "TTL repair inflight registry",
            ) {
                Ok(partition) => partition,
                Err(error) => {
                    tracing::warn!("TTL worker: unreadable inflight registry row: {error}");
                    continue;
                }
            };
            // Bounded AND resumable: markers accumulate one per ambiguous
            // incident, so a large partition is itself an anomaly — but active-
            // generation markers persist by design, so a fixed first page would
            // starve everything behind it forever. Resume each cycle where the
            // previous page ended and wrap at the partition's end, so a
            // partition of N markers is fully traversed in ceil(N/256)
            // consecutive cycles. The cursor is in-process; a restart resumes
            // from the ID order's start, which only repeats work.
            let cursor_key = (keyspace.clone(), partition);
            let cursor = storage
                .ttl_repair_scan_cursors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&cursor_key)
                .copied()
                .unwrap_or(uuid::Uuid::nil());
            let cursor_bytes = cdrs_tokio::types::value::Bytes::new(cursor.as_bytes().to_vec());
            let page_query = format!(
                "SELECT repair_id, table_id, account_id, table_name, generation, key_data \
                 FROM {keyspace}.ttl_repair_inflight \
                 WHERE worker_partition = ? AND repair_id > ? LIMIT 256"
            );
            let wrap_query = format!(
                "SELECT repair_id, table_id, account_id, table_name, generation, key_data \
                 FROM {keyspace}.ttl_repair_inflight \
                 WHERE worker_partition = ? AND repair_id <= ? LIMIT 256"
            );
            let mut rows = match crate::cassandra_util::query_rows_quorum::<StorageError>(
                &storage.session,
                &page_query,
                cdrs_tokio::query_values!(partition, cursor_bytes.clone()),
                "TTL repair inflight",
            )
            .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!(
                        "TTL worker: inflight partition {partition} failed in {keyspace}: {error}"
                    );
                    continue;
                }
            };
            let exhausted_forward = rows.len() < 256;
            if exhausted_forward {
                let remaining = 256 - rows.len();
                match crate::cassandra_util::query_rows_quorum::<StorageError>(
                    &storage.session,
                    &wrap_query,
                    cdrs_tokio::query_values!(partition, cursor_bytes),
                    "TTL repair inflight wrap",
                )
                .await
                {
                    Ok(wrapped) => rows.extend(wrapped.into_iter().take(remaining)),
                    Err(error) => {
                        tracing::warn!(
                            "TTL worker: inflight wrap {partition} failed in {keyspace}: {error}"
                        );
                    }
                }
            }
            {
                use cdrs_tokio::types::IntoRustByName;
                // Advance to just past the highest forward-scanned id; if the
                // forward scan was exhausted (we wrapped), restart from nil so
                // the next cycle re-covers the front.
                let next_cursor = if exhausted_forward {
                    uuid::Uuid::nil()
                } else {
                    rows.iter()
                        .filter_map(|row| {
                            let id: Option<uuid::Uuid> =
                                row.get_by_name("repair_id").ok().flatten();
                            id
                        })
                        .max()
                        .unwrap_or(uuid::Uuid::nil())
                };
                storage
                    .ttl_repair_scan_cursors
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(cursor_key, next_cursor);
            }
            observed += rows.len();
            if !rows.is_empty() {
                tracing::warn!(
                    keyspace,
                    partition,
                    count = rows.len(),
                    "TTL repair has unresolved destroy markers"
                );
            }
            for row in rows {
                let parsed = (|| -> Result<_, StorageError> {
                    let repair_id: uuid::Uuid =
                        row.get_r_by_name("repair_id").map_err(|error| {
                            StorageError::Internal(format!("Parse TTL repair id: {error}"))
                        })?;
                    let table_id: String =
                        crate::cassandra_util::get_column(&row, "table_id", "TTL repair inflight")?;
                    let account_id: String = crate::cassandra_util::get_column(
                        &row,
                        "account_id",
                        "TTL repair inflight",
                    )?;
                    let table_name: String = crate::cassandra_util::get_column(
                        &row,
                        "table_name",
                        "TTL repair inflight",
                    )?;
                    let generation: uuid::Uuid = crate::cassandra_util::get_column(
                        &row,
                        "generation",
                        "TTL repair inflight",
                    )?;
                    let key_data: String =
                        crate::cassandra_util::get_column(&row, "key_data", "TTL repair inflight")?;
                    let key: extenddb_core::types::Item =
                        serde_json::from_str(&key_data).map_err(|error| {
                            StorageError::Internal(format!("Parse TTL repair key: {error}"))
                        })?;
                    Ok((repair_id, table_id, account_id, table_name, generation, key))
                })();
                let (repair_id, table_id, account_id, table_name, generation, key) = match parsed {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        tracing::warn!("TTL worker: unreadable inflight repair skipped: {error}");
                        continue;
                    }
                };

                let (reconcile, terminal) = match storage
                    .fetch_table_key_info_quorum(&account_id, &table_name)
                    .await
                {
                    Ok(key_info) if key_info.table_id == table_id => {
                        let config = storage
                            .ttl_config_for_table_quorum(&account_id, &table_name)
                            .await;
                        let item = storage.get_item_quorum(&key_info, &key).await;
                        match (config, item) {
                            (Ok(Some(config)), Ok(Some(item))) => {
                                let terminal = config.generation != generation;
                                (
                                    storage
                                        .reconcile_ttl_item_with_config(&key_info, &item, &config)
                                        .await
                                        .map(|_| ()),
                                    terminal,
                                )
                            }
                            (Ok(Some(config)), Ok(None)) => {
                                (Ok(()), config.generation != generation)
                            }
                            (Ok(None), Ok(_)) => (Ok(()), true),
                            (Err(error), _) | (_, Err(error)) => (Err(error), false),
                        }
                    }
                    Ok(_) | Err(StorageError::TableNotFound(_)) => (Ok(()), true),
                    Err(error) => (Err(error), false),
                };
                match reconcile {
                    Ok(()) => {
                        processed += 1;
                        if terminal {
                            let delete = format!(
                                "DELETE FROM {keyspace}.ttl_repair_inflight \
                                 WHERE worker_partition = ? AND repair_id = ?"
                            );
                            if let Err(error) =
                                crate::cassandra_util::execute_quorum::<StorageError>(
                                    &storage.session,
                                    &delete,
                                    cdrs_tokio::query_values!(
                                        partition,
                                        cdrs_tokio::types::value::Bytes::new(
                                            repair_id.as_bytes().to_vec()
                                        )
                                    ),
                                    "retire obsolete TTL repair inflight marker",
                                )
                                .await
                            {
                                tracing::warn!(
                                    "TTL worker: obsolete inflight marker cleanup failed: {error}"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!("TTL worker: inflight repair {table_name} failed: {error}");
                    }
                }
            }
        }
    }
    Ok((observed, processed))
}

/// Finish or abort work that was already claimed when a TTL generation was
/// retired.
///
/// Disabling TTL, or re-enabling it under a new generation, must not simply
/// delete the old generation's queue rows: a claimed row owns a base-row claim,
/// and an `EFFECTS_APPLYING` or `EFFECTS_APPLIED` row additionally owns a
/// must-complete effects operation. The rule is decided by how much is already
/// durable:
///
/// * `CLAIMED` — cleanup first wins a `CLAIM_ABORTING` LWT against any
///   concurrent effects transition.
/// * `CLAIM_ABORTING` — nothing externally visible has happened; release the
///   exact owner and retire the work.
/// * `EFFECTS_APPLYING` — the exact base owner is sealed and effects may be
///   partially or fully visible, so reapply them idempotently and advance.
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

    for mut row in work {
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
            if !crate::data::ttl::mark_ttl_claim_aborting(
                storage,
                &account_keyspace,
                &key_info.table_id,
                generation,
                &row,
            )
            .await?
            {
                // A stale cleanup attempt must not release an owner after a
                // concurrent sweep has crossed the must-complete boundary.
                continue;
            }
            row.state = TtlWorkState::ClaimAborting;
        }

        if row.state == TtlWorkState::ClaimAborting {
            storage.release_ttl_claim(&key_info, &key, work_id).await?;
            let _ = crate::data::ttl::abort_claimed_ttl_work(
                storage,
                &account_keyspace,
                &key_info,
                generation,
                &row,
            )
            .await?;
            continue;
        }

        if row.state == TtlWorkState::EffectsApplying {
            // Owner-only, for the same reason as the active branch: a stale
            // writer's batch can change the image under the sealed owner, and
            // must-complete work has to go forward from that state, not error.
            if !storage.base_row_owned_by(&key_info, &key, work_id).await? {
                return Err(StorageError::Internal(
                    "retired TTL EFFECTS_APPLYING work lost its sealed owner".to_owned(),
                ));
            }
            storage
                .apply_ttl_delete_effects(
                    &key_info,
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
                generation,
                &row,
            )
            .await?
            {
                return Err(StorageError::Transient(
                    "retired TTL effects state changed during recovery".to_owned(),
                ));
            }
            row.state = TtlWorkState::EffectsApplied;
        }

        // EFFECTS_APPLIED.
        let current = storage.get_item_quorum(&key_info, &key).await?;
        let exact_deleted = if current.as_ref() == Some(&work_data.old_item)
            && storage
                .ensure_ttl_work_claim(
                    &key_info,
                    &key,
                    &work_data.old_item,
                    work_data.expected_version,
                    work_id,
                )
                .await?
        {
            storage
                .delete_ttl_base_exact(
                    &key_info,
                    &key,
                    &work_data.old_item,
                    work_data.expected_version,
                    work_id,
                )
                .await?
        } else {
            false
        };
        if !exact_deleted {
            // Either the image was already changed, or the exact delete lost a
            // race to a stale writer landing between the read and the Paxos
            // delete. Both are the same survivor case: rebuild any index rows
            // the replayed old-image tombstones erased, then release the
            // sealed owner. Completing without this would wedge the key.
            if let Some(item) = storage.get_item_quorum(&key_info, &key).await?.as_ref() {
                storage
                    .restore_sync_indexes_for_item(&key_info, item, work_data.delete_timestamp_ms)
                    .await?;
            }
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
        let Some((current, current_version)) =
            storage.get_item_with_version_quorum(key_info, &key).await?
        else {
            let _ = crate::data::ttl::retire_pending_ttl_work(
                storage,
                &account_keyspace,
                key_info,
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
                key_info,
                config.generation,
                &work.entry,
            )
            .await?
            {
                // Re-read after the destroy: the retire's tombstone may have
                // erased an insert committed by a writer between our read and
                // the retire, so reconciling the image read earlier would
                // re-register stale state and drop the writer's.
                if let Some(current) = storage.get_item_quorum(key_info, &key).await? {
                    storage
                        .reconcile_ttl_item_with_config(key_info, &current, config)
                        .await?;
                }
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
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            expected_version: current_version,
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

    if work.state == TtlWorkState::ClaimAborting {
        storage.release_ttl_claim(key_info, &key, work_id).await?;
        let _ = crate::data::ttl::abort_claimed_ttl_work(
            storage,
            &account_keyspace,
            key_info,
            config.generation,
            &work,
        )
        .await?;
        return Ok(false);
    }

    if work.state == TtlWorkState::Claimed {
        let current = storage.get_item_quorum(key_info, &key).await?;
        match current {
            Some(ref item) if item == &work_data.old_item => {
                if !storage
                    .ensure_ttl_work_claim(
                        key_info,
                        &key,
                        &work_data.old_item,
                        work_data.expected_version,
                        work_id,
                    )
                    .await?
                {
                    return Ok(false);
                }
            }
            Some(item) => {
                if !crate::data::ttl::mark_ttl_claim_aborting(
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
                work.state = TtlWorkState::ClaimAborting;
                storage.release_ttl_claim(key_info, &key, work_id).await?;
                if crate::data::ttl::abort_claimed_ttl_work(
                    storage,
                    &account_keyspace,
                    key_info,
                    config.generation,
                    &work,
                )
                .await?
                {
                    storage
                        .reconcile_ttl_item_with_config(key_info, &item, config)
                        .await?;
                }
                return Ok(false);
            }
            None => {
                if !crate::data::ttl::mark_ttl_claim_aborting(
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
                work.state = TtlWorkState::ClaimAborting;
                storage.release_ttl_claim(key_info, &key, work_id).await?;
                let _ = crate::data::ttl::abort_claimed_ttl_work(
                    storage,
                    &account_keyspace,
                    key_info,
                    config.generation,
                    &work,
                )
                .await?;
                return Ok(false);
            }
        }

        // Last gates before irreversible effects. First renew the exact
        // generation-bound sweep lease, then refresh the exact base-row owner
        // and image. A stale replica cannot satisfy either LWT. If this task
        // was suspended past either lease, it walks away before index or stream
        // mutations become visible.
        // Last lifecycle gate before irreversible effects: a quorum config
        // read, deliberately NOT a lease renewal. Sixteen concurrent rows all
        // renewing would contend on one catalog partition's Paxos and undo
        // the concurrency this sweep exists for; the wave loop already renews
        // the lease between waves, and the per-row fence that matters is the
        // seal below, whose Paxos is per item partition. This read only needs
        // to detect that TTL was disabled or regenerated while this row was
        // in flight.
        let lifecycle_current = match storage
            .ttl_config_for_table_quorum(&key_info.account_id, &key_info.table_name)
            .await
        {
            Ok(current) => current.as_ref() == Some(config),
            Err(error) => return Err(error),
        };
        if !lifecycle_current {
            return Ok(false);
        }
        let owner_current = match storage
            .seal_ttl_work_claim(
                key_info,
                &key,
                &work_data.old_item,
                work_data.expected_version,
                work_id,
            )
            .await
        {
            Ok(current) => current,
            Err(error) => return Err(error),
        };
        if !owner_current {
            return Ok(false);
        }
        if !crate::data::ttl::mark_ttl_effects_applying(
            storage,
            &account_keyspace,
            &key_info.table_id,
            config.generation,
            &work,
        )
        .await?
        {
            // A concurrent/recovered transition may already have advanced this
            // exact work to EFFECTS_APPLYING. Never release the now-sealed base
            // owner on an ambiguous false result; the next quorum scan will
            // observe the authoritative state and complete it.
            return Ok(false);
        }
        work.state = TtlWorkState::EffectsApplying;
    }

    if work.state == TtlWorkState::EffectsApplying {
        // EFFECTS_APPLYING is a durable must-complete state. Its base owner was
        // sealed without TTL before the transition, so lifecycle cleanup cannot
        // abort it and a suspended task cannot lose the fence to a writer.
        //
        // Recovery is fenced on the OWNER only, never on the image. A writer
        // that pinned its batch timestamp before this work sealed the owner can
        // land its unconditional batch afterwards: its owner-null cells lose to
        // the newer seal, but its item cells beat the older image — leaving
        // (current image, sealed owner). That state is valid and must-complete;
        // requiring the image to match would wedge it forever behind a
        // non-expiring owner. Effects replay from the recorded OLD image (the
        // stream identity is persisted, so no second REMOVE), and the
        // post-effects logic below already handles a changed image by releasing
        // the owner and repairing what the replayed tombstones took (see
        // restore_sync_indexes_for_item).
        if !storage.base_row_owned_by(key_info, &key, work_id).await? {
            return Err(StorageError::Internal(
                "TTL EFFECTS_APPLYING work lost its exact sealed base owner".to_owned(),
            ));
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
                .ensure_ttl_work_claim(
                    key_info,
                    &key,
                    &work_data.old_item,
                    work_data.expected_version,
                    work_id,
                )
                .await?
            {
                return Ok(false);
            }
            let deleted = storage
                .delete_ttl_base_exact(
                    key_info,
                    &key,
                    &work_data.old_item,
                    work_data.expected_version,
                    work_id,
                )
                .await?;
            if !deleted {
                // The Paxos delete read a different image than the quorum read
                // moments ago: a stale writer's batch landed in between,
                // changing the item under the sealed owner. Completing now
                // would destroy the queue row while the owner stays sealed and
                // the survivor's index rows stay tombstoned — a permanent
                // wedge. Re-read and treat it exactly like the changed-image
                // arm below; if the image reads as unchanged again, leave the
                // row EFFECTS_APPLIED for the next pass rather than guessing.
                let Some(survivor) = storage.get_item_quorum(key_info, &key).await? else {
                    // Gone: a late row tombstone erased the item but not the
                    // newer sealed owner cells. Release and complete below.
                    storage.release_ttl_claim(key_info, &key, work_id).await?;
                    let _ = crate::data::ttl::complete_live_ttl_work(
                        storage,
                        &account_keyspace,
                        key_info,
                        config.generation,
                        &work,
                    )
                    .await?;
                    return Ok(false);
                };
                if survivor == work_data.old_item {
                    return Ok(false);
                }
                storage
                    .restore_sync_indexes_for_item(
                        key_info,
                        &survivor,
                        work_data.delete_timestamp_ms,
                    )
                    .await?;
                storage.release_ttl_claim(key_info, &key, work_id).await?;
                if crate::data::ttl::complete_live_ttl_work(
                    storage,
                    &account_keyspace,
                    key_info,
                    config.generation,
                    &work,
                )
                .await?
                {
                    storage
                        .reconcile_ttl_item_with_config(key_info, &survivor, config)
                        .await?;
                }
                return Ok(false);
            }
            deleted
        }
        Some(item) => {
            // The replayed effects deleted the OLD image's index rows, and any
            // of the current item's index rows sharing those keys lost to the
            // replay's newer tombstones. Rebuild them from the current image
            // while the sealed owner still fences writers out, then release.
            storage
                .restore_sync_indexes_for_item(key_info, &item, work_data.delete_timestamp_ms)
                .await?;
            storage.release_ttl_claim(key_info, &key, work_id).await?;
            if crate::data::ttl::complete_live_ttl_work(
                storage,
                &account_keyspace,
                key_info,
                config.generation,
                &work,
            )
            .await?
            {
                storage
                    .reconcile_ttl_item_with_config(key_info, &item, config)
                    .await?;
            }
            return Ok(false);
        }
        None => {
            storage.release_ttl_claim(key_info, &key, work_id).await?;
            false
        }
    };

    let _ = crate::data::ttl::complete_live_ttl_work(
        storage,
        &account_keyspace,
        key_info,
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
            for wave in work.chunks(TTL_SWEEP_RENEW_EVERY) {
                // One renewal and config check per wave instead of per row;
                // the pre-effects gate inside process_ttl_work_row still
                // re-verifies the config before anything irreversible.
                if storage.ttl_config_for_table(account_id, table_name).await?
                    != Some(config.clone())
                    || !storage
                        .renew_ttl_sweep_lease(account_id, table_name, &config, owner)
                        .await?
                {
                    break;
                }
                let outcomes = futures::stream::iter(wave.iter().cloned().map(|row| {
                    let key_info = &key_info;
                    let config = &config;
                    async move {
                        let expires_at = row.entry.expires_at;
                        let outcome = process_ttl_work_row(storage, key_info, config, row).await;
                        (expires_at, outcome)
                    }
                }))
                .buffer_unordered(TTL_SWEEP_CONCURRENCY)
                .collect::<Vec<_>>()
                .await;
                for (expires_at, outcome) in outcomes {
                    match outcome {
                        Ok(true) => {
                            deleted += 1;
                            metrics.record_ttl_deletion(table_name);
                            metrics.record_ttl_staleness(
                                table_name,
                                now_epoch.saturating_sub(expires_at) as f64,
                            );
                        }
                        Ok(false) => {}
                        // One row's failure must not abandon the wave or the
                        // table: its durable queue state drives its own retry.
                        Err(error) => {
                            tracing::warn!("TTL worker: row failed in {table_name}: {error}");
                        }
                    }
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
