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
    ParticipantMutation, ParticipantRow, TXN_FAMILY, TXN_LOG_TABLE, TxnCoordinator, TxnState,
    TxnStreamRecord, TxnTokenMeta, TxnTtlMutation, build_txn_log_latest_filter,
};

const TOMBSTONE_SWEEP_BATCH: i64 = 200;

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
        |_pass_token| {
            let engine = engine.clone();
            async move {
                if let Err(e) = sweep_once(&engine).await {
                    tracing::warn!("2PC sweeper sweep error: {e}");
                }
            }
        },
    )
    .await;
    tracing::info!("bigtable 2PC sweeper worker stopped");
}

/// Run one sweep pass over `__extenddb_txn_log__` and due row-tombstone GC entries.
pub async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    sweep_inner(engine, None).await
}

/// Run one sweep pass with an optional cutoff timestamp override (for testing
/// immediate recovery and tombstone reclamation without sleeping `intent_timeout_secs`).
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn sweep_with_cutoff_override(
    engine: &BigtableEngine,
    cutoff_override_micros: Option<i64>,
) -> Result<(), String> {
    sweep_inner(engine, cutoff_override_micros).await
}

async fn sweep_inner(
    engine: &BigtableEngine,
    cutoff_override_micros: Option<i64>,
) -> Result<(), String> {
    sweep_txn_log(engine, cutoff_override_micros).await?;
    let tomb_now_ms = cutoff_override_micros
        .map(|us| u64::try_from((us / 1_000).max(0)).unwrap_or(u64::MAX))
        .unwrap_or_else(now_millis);
    sweep_due_tombstones(engine, tomb_now_ms).await
}

async fn sweep_txn_log(
    engine: &BigtableEngine,
    cutoff_override_micros: Option<i64>,
) -> Result<(), String> {
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
    #[allow(clippy::cast_possible_wrap)]
    let intent_timeout_micros = (engine.intent_timeout_secs().saturating_mul(1_000_000)) as i64;
    let cutoff = cutoff_override_micros.unwrap_or_else(|| now_micros - intent_timeout_micros);

    let coord = TxnCoordinator::new(
        client,
        Duration::from_secs(engine.intent_timeout_secs().max(1)),
    );

    loop {
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

        let mut last_key = None;

        for (raw_key, cells) in resp {
            last_key = Some(raw_key.clone());
            let Ok(s) = String::from_utf8(raw_key) else {
                continue;
            };
            let Some(txn_id) = s.strip_prefix("txn:").map(str::to_owned) else {
                continue;
            };

            let mut state: Option<String> = None;
            let mut state_ts_micros: i64 = 0;
            let mut started_at: Option<i64> = None;
            let mut participants: Option<Vec<ParticipantRow>> = None;
            let mut mutations: Option<Vec<ParticipantMutation>> = None;
            let mut stream_records: Option<Vec<TxnStreamRecord>> = None;
            let mut ttl_mutations: Option<Vec<TxnTtlMutation>> = None;
            let mut token_meta: Option<TxnTokenMeta> = None;

            for cell in cells {
                if cell.family_name == TXN_FAMILY {
                    match cell.qualifier.as_slice() {
                        b"state" if state.is_none() => {
                            state_ts_micros = cell.timestamp_micros;
                            state = String::from_utf8(cell.value).ok();
                        }
                        b"started_at" if started_at.is_none() => {
                            started_at = String::from_utf8(cell.value)
                                .ok()
                                .and_then(|s| s.parse::<i64>().ok());
                        }
                        b"participants" if participants.is_none() => {
                            participants = serde_json::from_slice(&cell.value).ok();
                        }
                        b"mutations" if mutations.is_none() => {
                            mutations = serde_json::from_slice(&cell.value).ok();
                        }
                        b"stream_records" if stream_records.is_none() => {
                            stream_records = serde_json::from_slice(&cell.value).ok();
                        }
                        b"ttl_mutations" if ttl_mutations.is_none() => {
                            ttl_mutations = serde_json::from_slice(&cell.value).ok();
                        }
                        b"token_meta" if token_meta.is_none() => {
                            token_meta = serde_json::from_slice(&cell.value).ok();
                        }
                        _ => {}
                    }
                }
            }

            let Some(st) = state else {
                continue;
            };
            let Some(start) = started_at else {
                continue;
            };

            if st == "CLEANED" {
                if let Some(ref tm) = token_meta
                    && let Err(e) = engine
                        .persist_idempotency_if_unexpired(
                            &tm.account_id,
                            &tm.token,
                            &tm.fingerprint,
                            &txn_id,
                            state_ts_micros.max(start),
                        )
                        .await
                {
                    tracing::warn!(
                        "2PC Sweeper: failed to persist idempotency record for CLEANED txn {txn_id}: {e}"
                    );
                    continue;
                }
                let _ = coord.drop_matching_start(&txn_id, start).await;
                continue;
            }

            if st == "ABORTED" {
                clear_aborted_intents_and_drop(&coord, &txn_id, start, participants.as_deref())
                    .await;
                continue;
            }

            // Post-commit side-effect recovery records (`participants == Some([])`)
            // can roll forward immediately without waiting `intent_timeout_secs`.
            let is_side_effect_recovery =
                st == "COMMITTED" && participants.as_ref().is_some_and(Vec::is_empty);

            if is_side_effect_recovery || start <= cutoff {
                match st.as_str() {
                    "PENDING" => {
                        match coord
                            .try_abort_pending(&txn_id, start, start.max(state_ts_micros))
                            .await
                        {
                            Ok(true) => {
                                clear_aborted_intents_and_drop(
                                    &coord,
                                    &txn_id,
                                    start,
                                    participants.as_deref(),
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
                                    }
                                }
                                Ok(Some(fresh))
                                    if fresh.state == "ABORTED"
                                        && fresh.started_at == Some(start) =>
                                {
                                    clear_aborted_intents_and_drop(
                                        &coord,
                                        &txn_id,
                                        start,
                                        fresh.participants.as_deref(),
                                    )
                                    .await;
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::warn!(
                                        "2PC Sweeper: get_state after CAS miss failed for {txn_id}: {e}"
                                    );
                                }
                            },
                            Err(e) => {
                                tracing::warn!(
                                    "2PC Sweeper: try_abort_pending failed for {txn_id}: {e}"
                                );
                            }
                        }
                    }
                    "COMMITTED" => {
                        let txn_state = TxnState {
                            state: st,
                            state_ts_micros,
                            started_at: Some(start),
                            participants,
                            mutations,
                            stream_records,
                            ttl_mutations,
                            token_meta,
                        };
                        if let Err(e) = engine.roll_forward(&txn_id, &txn_state).await {
                            tracing::error!(
                                "2PC Sweeper: rollforward failed for txn {txn_id}: {e}"
                            );
                        }
                    }
                    _ => {
                        tracing::warn!("2PC Sweeper: unknown txn state {st}");
                    }
                }
            }
        }

        if let Some(key) = last_key {
            start_key = key;
            exclude_start = true;
        } else {
            break;
        }
    }

    Ok(())
}

async fn sweep_due_tombstones(engine: &BigtableEngine, now_ms: u64) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(TTL_INDEX_TABLE);

    let mut start_key = TOMBSTONE_GC_PREFIX.to_vec();
    let end_key = tombstone_gc_scan_end_exclusive(now_ms);
    let mut exclude_start = false;

    loop {
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
                let ops = ItemOps::new(client, table_short, 0);
                match ops
                    .reclaim_version_only_row(raw_row_key, expected_ver)
                    .await
                {
                    Ok(_) | Err(StorageError::TableNotFound(_)) => {}
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
) {
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
        } else {
            tracing::info!("2PC Sweeper: rolled back stale txn {txn_id}");
        }
    }
}
