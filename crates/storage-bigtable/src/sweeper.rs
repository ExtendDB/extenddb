//! 2PC Recovery Sweeper Worker.
//!
//! Sweeps the coordinator transaction log table `__extenddb_txn_log__` and
//! rolls back aborted transaction intents that exceed `intent_timeout_secs`,
//! or rolls forward committed ones.

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    ReadRowsRequest, RowFilter, RowRange, RowSet,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};

use crate::BigtableEngine;
use crate::transact::{
    TXN_LOG_TABLE, TXN_FAMILY, TxnCoordinator, ParticipantRow, ParticipantMutation, TxnStreamRecord, TxnState,
};
use crate::catalog::Catalog;

pub async fn run(engine: Arc<BigtableEngine>, cadence: Duration) {
    tracing::info!("bigtable 2PC sweeper worker started; cadence={:?}", cadence);
    let mut tick = tokio::time::interval(cadence);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let lock_name = "sweeper";
    let owner = engine.node_id();
    let lease_duration = cadence * 2;

    loop {
        tick.tick().await;
        let cat = Catalog::new(engine.client_ref());
        match cat.try_lock(lock_name, owner, lease_duration).await {
            Ok(true) => {
                tracing::debug!("acquired lock '{}' for node '{}'", lock_name, owner);
                if let Err(e) = sweep_once(&engine).await {
                    tracing::warn!("2PC sweeper sweep error: {e}");
                }
                if let Err(e) = cat.release_lock(lock_name, owner).await {
                    tracing::warn!("failed to release lock '{}': {e}", lock_name);
                }
            }
            Ok(false) => {
                tracing::debug!("lock '{}' is busy, skipping sweep", lock_name);
            }
            Err(e) => {
                tracing::warn!("error trying to acquire lock '{}': {e}", lock_name);
            }
        }
    }
}

async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let txn_log_table = client.full_table_name(TXN_LOG_TABLE);

    let start_prefix = b"txn:".to_vec();
    let mut end_key = start_prefix.clone();
    end_key.push(0xFF);

    let limit = 100; // Paginate by 100 rows
    let mut start_key = start_prefix.clone();
    let mut exclude_start = false;

    let now_micros = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    
    let intent_timeout_micros = (engine.intent_timeout_secs() * 1_000_000) as i64;
    let cutoff = now_micros - intent_timeout_micros;

    let coord = TxnCoordinator::new(client, Duration::from_secs(engine.intent_timeout_secs()));

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
            rows_limit: limit,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![range],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(TXN_FAMILY.to_string())),
            }),
            ..ReadRowsRequest::default()
        };

        let resp = data.read_rows(req)
            .await
            .map_err(|e| format!("ReadRows from TXN log: {e}"))?;

        if resp.is_empty() {
            break;
        }

        let mut last_key = None;

        for (raw_key, cells) in resp {
            last_key = Some(raw_key.clone());
            let txn_id = match String::from_utf8(raw_key.clone()) {
                Ok(s) => {
                    if let Some(id) = s.strip_prefix("txn:") {
                        id.to_string()
                    } else {
                        continue;
                    }
                }
                Err(_) => continue,
            };

            let mut state: Option<String> = None;
            let mut started_at: Option<i64> = None;
            let mut participants: Option<Vec<ParticipantRow>> = None;
            let mut mutations: Option<Vec<ParticipantMutation>> = None;
            let mut stream_records: Option<Vec<TxnStreamRecord>> = None;

            for cell in cells {
                if cell.family_name == TXN_FAMILY {
                    match cell.qualifier.as_slice() {
                        b"state" => {
                            state = String::from_utf8(cell.value).ok();
                        }
                        b"started_at" => {
                            started_at = String::from_utf8(cell.value)
                                .ok()
                                .and_then(|s| s.parse::<i64>().ok());
                        }
                        b"participants" => {
                            participants = serde_json::from_slice(&cell.value).ok();
                        }
                        b"mutations" => {
                            mutations = serde_json::from_slice(&cell.value).ok();
                        }
                        b"stream_records" => {
                            stream_records = serde_json::from_slice(&cell.value).ok();
                        }
                        _ => {}
                    }
                }
            }

            let Some(st) = state else { continue; };
            let Some(start) = started_at else { continue; };

            if st == "CLEANED" {
                let _ = coord.drop(&txn_id).await;
                continue;
            }

            if start < cutoff {
                tracing::info!("2PC Sweeper: found stale transaction {} (state={}, started {}s ago)", 
                    txn_id, st, (now_micros - start) / 1_000_000);
                
                match st.as_str() {
                    "PENDING" | "ABORTED" => {
                        // Rollback: clear intents on all participants
                        let mut success = true;
                        if let Some(parts) = participants {
                            for p in parts {
                                if let Err(e) = coord.clear_intent(&txn_id, &p).await {
                                    tracing::warn!("2PC Sweeper: rollback failed to clear intent on {} for txn {}: {}", 
                                        p.data_table, txn_id, e);
                                    success = false;
                                }
                            }
                        }
                        if success {
                            if let Err(e) = coord.drop(&txn_id).await {
                                tracing::warn!("2PC Sweeper: failed to drop coordinator row for txn {}: {}", txn_id, e);
                            } else {
                                tracing::info!("2PC Sweeper: rolled back stale txn {}", txn_id);
                            }
                        } else {
                            if st == "PENDING" {
                                let _ = coord.aborted(&txn_id).await;
                            }
                        }
                    }
                    "COMMITTED" => {
                        // Roll-forward: apply mutations using engine.roll_forward
                        let txn_state = TxnState {
                            state: st,
                            participants,
                            mutations,
                            stream_records,
                        };
                        if let Err(e) = engine.roll_forward(&txn_id, &txn_state).await {
                            tracing::error!("2PC Sweeper: rollforward failed for txn {}: {}", txn_id, e);
                        } else {
                            tracing::info!("2PC Sweeper: rolled forward stale txn {}", txn_id);
                        }
                    }
                    _ => {
                        tracing::warn!("2PC Sweeper: unknown txn state {}", st);
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
