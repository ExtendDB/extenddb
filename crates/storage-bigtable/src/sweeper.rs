// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! 2PC Recovery & Version-Tombstone Reclamation Sweeper Worker.
//!
//! 1. Sweeps `__extenddb_txn_log__`:
//!    - CAS-transitions timed-out `PENDING` transactions to `ABORTED`
//!      (`try_abort_pending`) before clearing participant intents.
//!    - Rolls forward `COMMITTED` transactions and post-commit side-effect
//!      records idempotently (`roll_forward`).
//! 2. Sweeps due `b"tomb:"` entries in `__extenddb_ttl_index__`, atomically
//!    reclaiming pure version-only tombstone rows (`reclaim_version_only_row`)
//!    once the intent-timeout grace window has elapsed.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use extenddb_storage::CancellationToken;
use extenddb_storage::error::StorageError;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::DeleteFromColumn;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet, TimestampRange,
    mutation,
};

use crate::BigtableEngine;
use crate::data::encoding::ttl_key::{
    TOMBSTONE_GC_PREFIX, TOMBSTONE_GC_QUALIFIER, TTL_INDEX_TABLE, decode_tombstone_gc_value,
    tombstone_gc_scan_end_exclusive,
};
use crate::data::item_ops::{ItemOps, next_millis_exclusive, now_millis, now_millis_micros};
use crate::runtime_hooks::run_leased_worker;
use crate::transact::{
    ParticipantRow, TXN_LOG_TABLE, TxnCoordinator, TxnState, build_txn_log_latest_filter,
    decode_txn_state_cells,
};

const TOMBSTONE_SWEEP_BATCH: i64 = 200;

pub(crate) fn base_table_family_key(table_short: &str) -> &str {
    table_short.split('_').next().unwrap_or(table_short)
}

fn record_blocked_txn_tables(blocked: &mut HashSet<String>, st: &TxnState) {
    if let Some(ref parts) = st.participants {
        for p in parts {
            blocked.insert(base_table_family_key(&p.data_table).to_owned());
        }
    }
    if let Some(ref muts) = st.mutations {
        for m in muts {
            blocked.insert(base_table_family_key(&m.participant.data_table).to_owned());
        }
    }
    if let Some(ref guard) = st.base_guard {
        blocked.insert(base_table_family_key(&guard.participant.data_table).to_owned());
    }
}

fn record_unsettled_committed_rows(unsettled: &mut HashSet<(String, Vec<u8>)>, st: &TxnState) {
    if let Some(ref parts) = st.participants {
        for p in parts {
            unsettled.insert((p.data_table.clone(), p.row_key.clone()));
        }
    }
    if let Some(ref muts) = st.mutations {
        for m in muts {
            unsettled.insert((
                m.participant.data_table.clone(),
                m.participant.row_key.clone(),
            ));
        }
    }
    if let Some(ref guard) = st.base_guard {
        unsettled.insert((
            guard.participant.data_table.clone(),
            guard.participant.row_key.clone(),
        ));
    }
}

struct TxnSweepOutcome {
    blocked_tables: HashSet<String>,
    unsettled_committed_rows: HashSet<(String, Vec<u8>)>,
}

pub async fn run(engine: Arc<BigtableEngine>, cadence: Duration, shutdown: CancellationToken) {
    tracing::info!("bigtable 2PC sweeper worker started; cadence={cadence:?}");
    let catalog_client = engine.catalog_client_arc();
    let owner = engine.node_id().to_owned();
    let lease_duration = (cadence * 3).max(Duration::from_secs(30));

    run_leased_worker(
        catalog_client,
        "sweeper",
        owner,
        cadence,
        lease_duration,
        shutdown,
        |pass_token| {
            let engine = engine.clone();
            async move {
                if let Err(e) = sweep_inner(&engine, None, None, &pass_token).await {
                    tracing::warn!("2PC sweeper sweep error: {e}");
                }
            }
        },
    )
    .await;
    tracing::info!("bigtable 2PC sweeper worker stopped");
}

/// Run one sweep pass over `__extenddb_txn_log__` and due row-tombstone GC entries.
///
/// # Errors
///
/// Returns an error description if reading `__extenddb_txn_log__` or
/// `__extenddb_ttl_index__` fails.
pub async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    let token = CancellationToken::new();
    sweep_inner(engine, None, None, &token).await
}

/// Run one sweep pass with an optional cutoff timestamp override (for testing
/// immediate recovery and tombstone reclamation without sleeping `intent_timeout_secs`).
///
/// # Errors
///
/// Returns an error description if reading `__extenddb_txn_log__` or
/// `__extenddb_ttl_index__` fails.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn sweep_with_cutoff_override(
    engine: &BigtableEngine,
    cutoff_override_micros: Option<i64>,
) -> Result<(), String> {
    let token = CancellationToken::new();
    sweep_inner(engine, cutoff_override_micros, None, &token).await
}

/// Run one sweep pass overriding only the tombstone GC readiness clock while
/// keeping `__extenddb_txn_log__` `PENDING` timeout evaluation on live wall time.
///
/// # Errors
///
/// Returns an error description if reading `__extenddb_txn_log__` or
/// `__extenddb_ttl_index__` fails.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn sweep_once_with_tombstone_cutoff(
    engine: &BigtableEngine,
    tomb_now_ms_override: Option<u64>,
) -> Result<(), String> {
    let token = CancellationToken::new();
    sweep_inner(engine, None, tomb_now_ms_override, &token).await
}

async fn sweep_inner(
    engine: &BigtableEngine,
    cutoff_override_micros: Option<i64>,
    tomb_now_ms_override: Option<u64>,
    shutdown: &CancellationToken,
) -> Result<(), String> {
    let outcome = sweep_txn_log(engine, cutoff_override_micros, shutdown).await?;
    if shutdown.is_cancelled() {
        return Ok(());
    }
    let tomb_now_ms = tomb_now_ms_override.unwrap_or_else(|| {
        cutoff_override_micros.map_or_else(now_millis, |us| {
            u64::try_from((us / 1_000).max(0)).unwrap_or(u64::MAX)
        })
    });
    sweep_due_tombstones(
        engine,
        tomb_now_ms,
        tomb_now_ms_override.is_some() || cutoff_override_micros.is_some(),
        &outcome,
        shutdown,
    )
    .await
}

// Justification: coordinator log paging, CLEANED/ABORTED cleanup, timed-out PENDING CAS abort, COMMITTED/post-* roll-forward, and unsettled-row tracking run in one pass.
#[allow(clippy::too_many_lines)]
async fn sweep_txn_log(
    engine: &BigtableEngine,
    cutoff_override_micros: Option<i64>,
    shutdown: &CancellationToken,
) -> Result<TxnSweepOutcome, String> {
    let mut blocked_tables = engine.flush_pending_side_effect_recoveries().await;
    let mut unsettled_committed_rows = HashSet::new();
    if shutdown.is_cancelled() {
        return Ok(TxnSweepOutcome {
            blocked_tables,
            unsettled_committed_rows,
        });
    }

    let client = engine.client_ref();
    let mut data = client.data();
    let txn_log_table = client.full_table_name(TXN_LOG_TABLE);

    let start_prefix = b"txn:".to_vec();
    let mut end_key = start_prefix.clone();
    end_key.push(0xFF);

    let limit = 100;
    let mut start_key = start_prefix;
    let mut exclude_start = false;

    let now_micros = now_millis_micros();
    let intent_timeout_micros =
        i64::try_from(engine.intent_timeout_secs().saturating_mul(1_000_000)).unwrap_or(i64::MAX);
    let cutoff = cutoff_override_micros.unwrap_or_else(|| now_micros - intent_timeout_micros);

    let coord = TxnCoordinator::new(
        client,
        Duration::from_secs(engine.intent_timeout_secs().max(1)),
    );

    loop {
        if shutdown.is_cancelled() {
            return Ok(TxnSweepOutcome {
                blocked_tables,
                unsettled_committed_rows,
            });
        }
        let range = RowRange {
            start_key: Some(if exclude_start {
                StartKey::StartKeyOpen(start_key.clone())
            } else {
                StartKey::StartKeyClosed(start_key.clone())
            }),
            end_key: Some(EndKey::EndKeyClosed(end_key.clone())),
        };

        let req = ReadRowsRequest {
            table_name: txn_log_table.clone(),
            app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: limit,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![range],
            }),
            filter: Some(build_txn_log_latest_filter()),
            ..ReadRowsRequest::default()
        };

        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| format!("ReadRows from TXN log: {e}"))?;

        if resp.is_empty() {
            break;
        }
        let batch_len = resp.len();
        let mut last_key = None;

        for (raw_key, cells) in resp {
            if shutdown.is_cancelled() {
                return Ok(TxnSweepOutcome {
                    blocked_tables,
                    unsettled_committed_rows,
                });
            }
            last_key = Some(raw_key.clone());
            let Ok(s) = String::from_utf8(raw_key) else {
                continue;
            };
            let Some(txn_id) = s.strip_prefix("txn:").map(str::to_owned) else {
                continue;
            };

            let Some(txn_state) = decode_txn_state_cells(cells) else {
                continue;
            };
            let Some(start) = txn_state.started_at else {
                continue;
            };

            if txn_state.state == "CLEANED" {
                if let Some(ref tm) = txn_state.token_meta
                    && let Err(e) = engine
                        .persist_idempotency_if_unexpired(
                            &tm.account_id,
                            &tm.token,
                            &tm.fingerprint,
                            &txn_id,
                            txn_state.state_ts_micros.max(start),
                        )
                        .await
                {
                    tracing::warn!(
                        "2PC Sweeper: failed to persist idempotency record for CLEANED txn {txn_id}: {e}"
                    );
                    continue;
                }
                if let Err(e) = coord.drop_matching_start(&txn_id, start).await {
                    tracing::warn!(
                        "2PC Sweeper: failed to drop CLEANED coordinator row for {txn_id}: {e}"
                    );
                }
                continue;
            }

            if txn_state.state == "ABORTED" {
                let _ = clear_aborted_intents_and_drop(
                    &coord,
                    &txn_id,
                    start,
                    txn_state.participants.as_deref(),
                )
                .await;
                continue;
            }

            // Post-commit side-effect recovery records (`participants == Some([])`)
            // roll forward immediately without waiting `intent_timeout_secs`.
            let is_side_effect_recovery = txn_state.state == "COMMITTED"
                && txn_state.participants.as_ref().is_some_and(Vec::is_empty);

            if is_side_effect_recovery || start <= cutoff {
                match txn_state.state.as_str() {
                    "PENDING" => {
                        match coord
                            .try_abort_pending(&txn_id, start, start.max(txn_state.state_ts_micros))
                            .await
                        {
                            Ok(true) => {
                                let _ = clear_aborted_intents_and_drop(
                                    &coord,
                                    &txn_id,
                                    start,
                                    txn_state.participants.as_deref(),
                                )
                                .await;
                            }
                            Ok(false) => match coord.get_state(&txn_id).await {
                                Ok(Some(fresh))
                                    if fresh.state == "COMMITTED"
                                        && fresh.started_at == Some(start) =>
                                {
                                    if let Err(e) = engine.roll_forward(&txn_id, &fresh).await {
                                        tracing::error!(
                                            "2PC Sweeper: rollforward after CAS race failed for {txn_id}: {e}"
                                        );
                                        record_blocked_txn_tables(&mut blocked_tables, &fresh);
                                    }
                                }
                                Ok(Some(fresh))
                                    if fresh.state == "ABORTED"
                                        && fresh.started_at == Some(start) =>
                                {
                                    let _ = clear_aborted_intents_and_drop(
                                        &coord,
                                        &txn_id,
                                        start,
                                        fresh.participants.as_deref(),
                                    )
                                    .await;
                                }
                                Ok(Some(fresh)) if fresh.state == "PENDING" => {
                                    record_blocked_txn_tables(&mut blocked_tables, &fresh);
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::warn!(
                                        "2PC Sweeper: get_state after CAS miss failed for {txn_id}: {e}"
                                    );
                                    record_blocked_txn_tables(&mut blocked_tables, &txn_state);
                                }
                            },
                            Err(e) => {
                                tracing::warn!(
                                    "2PC Sweeper: try_abort_pending failed for {txn_id}: {e}"
                                );
                                record_blocked_txn_tables(&mut blocked_tables, &txn_state);
                            }
                        }
                    }
                    "COMMITTED" => {
                        if let Err(e) = engine.roll_forward(&txn_id, &txn_state).await {
                            tracing::error!(
                                "2PC Sweeper: rollforward failed for txn {txn_id}: {e}"
                            );
                            record_blocked_txn_tables(&mut blocked_tables, &txn_state);
                        }
                    }
                    other => {
                        tracing::warn!("2PC Sweeper: unknown txn state {other}");
                        record_blocked_txn_tables(&mut blocked_tables, &txn_state);
                    }
                }
            } else if txn_state.state == "COMMITTED" {
                record_unsettled_committed_rows(&mut unsettled_committed_rows, &txn_state);
            }
        }

        if i64::try_from(batch_len).unwrap_or(i64::MAX) < limit {
            break;
        }
        if let Some(key) = last_key {
            start_key = key;
            exclude_start = true;
        } else {
            break;
        }
    }

    Ok(TxnSweepOutcome {
        blocked_tables,
        unsettled_committed_rows,
    })
}

// Justification: shard iteration, tombstone queue ReadRows paging, per-table/per-row gating, and guarded row deletion stay together.
#[allow(clippy::too_many_lines)]
async fn sweep_due_tombstones(
    engine: &BigtableEngine,
    now_ms: u64,
    has_cutoff_override: bool,
    outcome: &TxnSweepOutcome,
    shutdown: &CancellationToken,
) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(TTL_INDEX_TABLE);

    let mut start_key = TOMBSTONE_GC_PREFIX.to_vec();
    let end_key = tombstone_gc_scan_end_exclusive(now_ms);
    let mut exclude_start = false;

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let range = RowRange {
            start_key: Some(if exclude_start {
                StartKey::StartKeyOpen(start_key.clone())
            } else {
                StartKey::StartKeyClosed(start_key.clone())
            }),
            end_key: Some(EndKey::EndKeyOpen(end_key.clone())),
        };
        let req = ReadRowsRequest {
            table_name: full_index_table.clone(),
            app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
            rows_limit: TOMBSTONE_SWEEP_BATCH,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![range],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            ..ReadRowsRequest::default()
        };
        let resp = data
            .read_rows(req)
            .await
            .map_err(|e| format!("ReadRows tombstone GC queue: {e}"))?;
        if resp.is_empty() {
            break;
        }

        let batch_len = resp.len();
        let mut last_key = None;
        for (queue_key, cells) in resp {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            last_key = Some(queue_key.clone());
            let Some(cell) = cells
                .into_iter()
                .find(|c| c.family_name == "d" && c.qualifier.as_slice() == TOMBSTONE_GC_QUALIFIER)
            else {
                continue;
            };
            let cell_ts = cell.timestamp_micros;
            if let Some((expected_ver, table_short, raw_row_key)) =
                decode_tombstone_gc_value(&cell.value)
            {
                if outcome
                    .blocked_tables
                    .contains(base_table_family_key(table_short))
                    || outcome
                        .unsettled_committed_rows
                        .contains(&(table_short.to_owned(), raw_row_key.to_vec()))
                {
                    continue;
                }
                let cur_wall_ms = now_millis();
                if has_cutoff_override
                    && cur_wall_ms <= expected_ver
                    && expected_ver.saturating_sub(cur_wall_ms) <= 50
                {
                    tokio::time::sleep(Duration::from_millis(
                        expected_ver.saturating_sub(cur_wall_ms).saturating_add(1),
                    ))
                    .await;
                }
                if now_millis() <= expected_ver {
                    continue;
                }
                let ops = ItemOps::new(client, table_short, 0);
                match ops
                    .reclaim_version_only_row(raw_row_key, expected_ver)
                    .await
                {
                    Ok(true) | Err(StorageError::TableNotFound(_)) => {}
                    Ok(false) => match ops.get_row_snapshot_by_raw_key(raw_row_key).await {
                        Ok(snap)
                            if snap.version == Some(expected_ver)
                                && (snap.item.is_none()
                                    || !snap.intents.is_empty()
                                    || snap.in_progress_chunk) =>
                        {
                            continue;
                        }
                        Ok(_) | Err(StorageError::TableNotFound(_)) => {}
                        Err(_) => continue,
                    },
                    Err(e) => {
                        tracing::debug!(
                            table = %table_short,
                            error = %e,
                            "deferred version-only tombstone reclamation retry"
                        );
                        continue;
                    }
                }
            }

            let del_req = MutateRowRequest {
                table_name: full_index_table.clone(),
                app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
                row_key: queue_key,
                mutations: vec![Mutation {
                    mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
                        family_name: "d".to_string(),
                        column_qualifier: TOMBSTONE_GC_QUALIFIER.to_vec(),
                        time_range: Some(TimestampRange {
                            start_timestamp_micros: 0,
                            end_timestamp_micros: next_millis_exclusive(cell_ts),
                        }),
                    })),
                }],
                ..MutateRowRequest::default()
            };
            let _ = data.mutate_row(del_req).await;
        }

        if i64::try_from(batch_len).unwrap_or(i64::MAX) < TOMBSTONE_SWEEP_BATCH {
            break;
        }
        if let Some(k) = last_key {
            start_key = k;
            exclude_start = true;
        } else {
            break;
        }
    }

    Ok(())
}

async fn clear_aborted_intents_and_drop(
    coord: &TxnCoordinator<'_>,
    txn_id: &str,
    started_at_micros: i64,
    participants: Option<&[ParticipantRow]>,
) -> bool {
    let mut success = true;
    if let Some(parts) = participants {
        for p in parts {
            if let Err(e) = coord.clear_intent(txn_id, p).await {
                tracing::warn!(
                    "2PC Sweeper: rollback failed to clear intent on {} for txn {txn_id}: {e}",
                    p.data_table
                );
                success = false;
            }
        }
    }
    if success {
        if let Err(e) = coord.drop_matching_start(txn_id, started_at_micros).await {
            tracing::warn!("2PC Sweeper: failed to drop coordinator row for txn {txn_id}: {e}");
            success = false;
        } else {
            tracing::info!("2PC Sweeper: rolled back stale txn {txn_id}");
        }
    }
    success
}
