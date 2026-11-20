// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! 2PC Recovery Sweeper Worker.
//!
//! Sweeps the coordinator transaction log table `__extenddb_txn_log__` and:
//! - CAS-transitions timed-out `PENDING` transactions to `ABORTED` (`try_abort_pending`)
//!   *before* clearing participant intents (`BUG-01`, `AllOrNothing2PCAtomicity`),
//!   preventing partial 2PC commits when racing a live coordinator.
//! - Rolls forward `COMMITTED` transactions idempotently (`roll_forward`),
//!   applying base mutations, versioned GSI shadow writes, `TxnTtlMutation`s
//!   (`BUG-06`), ordered deduplicated stream records (`BUG-04`, `BUG-05`), and
//!   `ClientRequestToken` `idem:` entries (`BUG-07c`).

use std::sync::Arc;
use std::time::Duration;

use extenddb_storage::{CancellationToken, sleep_or_shutdown};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    ReadRowsRequest, RowRange, RowSet,
};

use crate::BigtableEngine;
use crate::data::item_ops::now_millis_micros;
use crate::transact::{
    ParticipantMutation, ParticipantRow, TXN_FAMILY, TXN_LOG_TABLE, TxnCoordinator, TxnState,
    TxnStreamRecord, TxnTokenMeta, TxnTtlMutation, build_txn_log_latest_filter,
};

pub async fn run(engine: Arc<BigtableEngine>, cadence: Duration, shutdown: CancellationToken) {
    tracing::info!("bigtable 2PC sweeper worker started; cadence={cadence:?}");
    let lock_name = "sweeper";
    let owner = engine.node_id();
    let lease_duration = cadence * 2;

    while sleep_or_shutdown(&shutdown, cadence).await {
        let cat = engine.cat();
        match cat.try_lock(lock_name, owner, lease_duration).await {
            Ok(true) => {
                tracing::debug!("acquired lock '{lock_name}' for node '{owner}'");
                if let Err(e) = sweep_once(&engine).await {
                    tracing::warn!("2PC sweeper sweep error: {e}");
                }
                if let Err(e) = cat.release_lock(lock_name, owner).await {
                    tracing::warn!("failed to release lock '{lock_name}': {e}");
                }
            }
            Ok(false) => {
                tracing::debug!("lock '{lock_name}' is busy, skipping sweep");
            }
            Err(e) => {
                tracing::warn!("error trying to acquire lock '{lock_name}': {e}");
            }
        }
    }
    tracing::info!("bigtable 2PC sweeper worker stopped");
}

/// Run one sweep pass over `__extenddb_txn_log__`.
pub async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    sweep_with_cutoff_override(engine, None).await
}

/// Run one sweep pass with an optional cutoff timestamp override (for testing
/// immediate recovery without sleeping `intent_timeout_secs`).
pub async fn sweep_with_cutoff_override(
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
    let mut start_key = start_prefix.clone();
    let mut exclude_start = false;

    let now_micros = now_millis_micros();
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

            if start <= cutoff {
                tracing::info!(
                    "2PC Sweeper: found stale transaction {txn_id} (state={st}, started {}s ago)",
                    (now_micros - start) / 1_000_000
                );

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
                            Ok(false) => {
                                // Coordinator raced us: check if this incarnation committed!
                                match coord.get_state(&txn_id).await {
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
                                }
                            }
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
                        } else {
                            tracing::info!("2PC Sweeper: rolled forward stale txn {txn_id}");
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
