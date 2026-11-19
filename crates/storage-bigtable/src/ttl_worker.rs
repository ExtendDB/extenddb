// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL cleanup worker.
//!
//! Sweeps the `__extenddb_ttl_index__` table in shards, verifies each candidate
//! against the live base item's TTL attribute (including resolving `COMMITTED`
//! 2PC intents), evicts any item whose current TTL epoch is in `(0, now_epoch_s]`
//! via a conditional `DataEngine::delete_item`, re-asserts future TTL index
//! entries when an item's TTL was extended (`> now_epoch_s`), and cleans up
//! scanned TTL index cells using millisecond-aligned timestamp-bounded
//! `DeleteFromColumn` (`[0, next_millis_exclusive(ts))`) rather than unbounded
//! `DeleteFromRow` so concurrent same-second TTL updates are never wiped.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement};
use extenddb_core::metrics::MetricsCollector;
use extenddb_core::types::{AttributeValue, StreamViewType, TimeToLiveStatus, UserIdentity};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    CancellationToken, DataEngine, MetadataEngine, StreamCapture, TableEngine, sleep_or_shutdown,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet, TimestampRange,
    mutation,
    mutation::DeleteFromColumn,
    row_filter::Filter,
    row_range::{EndKey, StartKey},
};

use crate::BigtableEngine;
use crate::data::client::BigtableClient;
use crate::data::item_ops::{
    next_millis_exclusive, parse_ttl_epoch_secs, version_to_timestamp_micros,
};

const TTL_PRINCIPAL_ID: &str = "dynamodb.amazonaws.com";
const TTL_PRINCIPAL_TYPE: &str = "Service";

pub async fn run(engine: Arc<BigtableEngine>, cadence: Duration, shutdown: CancellationToken) {
    run_with_metrics(engine, cadence, None, shutdown).await;
}

pub async fn run_with_metrics(
    engine: Arc<BigtableEngine>,
    cadence: Duration,
    metrics: Option<Arc<MetricsCollector>>,
    shutdown: CancellationToken,
) {
    tracing::info!("bigtable TTL worker started; cadence={cadence:?}");

    if let Err(e) = ensure_ttl_index_table(engine.client_ref()).await {
        tracing::warn!("could not ensure TTL index table: {e}");
    }

    let lock_name = "ttl_worker";
    let owner = engine.node_id();
    let lease_duration = cadence * 2;

    while sleep_or_shutdown(&shutdown, cadence).await {
        let cat = engine.cat();
        match cat.try_lock(lock_name, owner, lease_duration).await {
            Ok(true) => {
                tracing::debug!("acquired lock '{lock_name}' for node '{owner}'");
                if let Err(e) = sweep_once_with_metrics(&engine, metrics.as_deref()).await {
                    tracing::warn!("TTL worker sweep error: {e}");
                }
                if let Err(e) = cat.release_lock(lock_name, owner).await {
                    tracing::warn!("failed to release lock '{lock_name}': {e}");
                }
            }
            Ok(false) => {
                tracing::debug!("lock '{lock_name}' is busy, skipping TTL sweep");
            }
            Err(e) => {
                tracing::warn!("error trying to acquire lock '{lock_name}': {e}");
            }
        }
    }
    tracing::info!("bigtable TTL worker stopped");
}

pub async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    sweep_once_with_metrics(engine, None).await
}

pub async fn sweep_once_with_metrics(
    engine: &BigtableEngine,
    metrics: Option<&MetricsCollector>,
) -> Result<(), String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs() as i64;

    for shard in 0..crate::data::encoding::ttl_key::NUM_SHARDS {
        if let Err(e) = sweep_shard(engine, shard, now, metrics).await {
            tracing::warn!("TTL worker sweep failed for shard {shard}: {e}");
        }
    }
    Ok(())
}

async fn sweep_shard(
    engine: &BigtableEngine,
    shard_id: u8,
    now_epoch_s: i64,
    metrics: Option<&MetricsCollector>,
) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE);

    let end_key = {
        let mut k = Vec::with_capacity(9);
        k.push(shard_id);
        k.extend_from_slice(&(now_epoch_s + 1).to_be_bytes());
        k
    };

    let limit = 100;
    let mut start_key = {
        let mut k = Vec::with_capacity(9);
        k.push(shard_id);
        k.extend_from_slice(&0i64.to_be_bytes());
        k
    };
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
            rows_limit: limit,
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
            .map_err(|e| format!("ReadRows from TTL index: {e}"))?;

        if resp.is_empty() {
            break;
        }

        let mut last_key = None;

        for (raw_key, cells) in resp {
            last_key = Some(raw_key.clone());
            let idx_ts_micros = cells.iter().map(|c| c.timestamp_micros).max().unwrap_or(0);

            let (_, expiry, account_id, table_name, base_row_key) =
                match crate::data::encoding::ttl_key::decode_ttl_key(&raw_key) {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        tracing::warn!("failed to decode TTL index key: {e}");
                        continue;
                    }
                };

            if expiry <= 0 || expiry > now_epoch_s {
                continue;
            }

            let key_info = match TableEngine::table_key_info(engine, account_id, table_name).await {
                Ok(ki) => ki,
                Err(StorageError::TableNotFound(_)) => {
                    let _ = delete_ttl_index_entry_up_to(engine, &raw_key, idx_ts_micros).await;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        "could not fetch key info for table {account_id}/{table_name}: {e}"
                    );
                    continue;
                }
            };

            let ttl_desc = match MetadataEngine::describe_ttl(engine, account_id, table_name).await
            {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("could not describe TTL for {account_id}/{table_name}: {e}");
                    continue;
                }
            };
            let Some(ttl_attr) = ttl_desc
                .attribute_name
                .filter(|_| ttl_desc.time_to_live_status == TimeToLiveStatus::Enabled)
            else {
                let _ = delete_ttl_index_entry_up_to(engine, &raw_key, idx_ts_micros).await;
                continue;
            };

            let base_key = match crate::data::encoding::row_key::decode_key(
                base_row_key,
                &key_info.key_schema,
            ) {
                Ok(k) => k,
                Err(e) => {
                    tracing::warn!(
                        "failed to decode base row key for table {account_id}/{table_name}: {e}"
                    );
                    let _ = delete_ttl_index_entry_up_to(engine, &raw_key, idx_ts_micros).await;
                    continue;
                }
            };

            // Read live item + OCC version (cooperatively rolling forward any
            // COMMITTED 2PC intent on the row).
            let (live_item, live_ver) =
                match engine.get_item_with_version(&key_info, &base_key).await {
                    Ok((Some(it), ver)) => (it, ver),
                    Ok((None, ver)) => {
                        let bound_ts =
                            idx_ts_micros.max(ver.map_or(0, version_to_timestamp_micros));
                        let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!("TTL get_item failed in {account_id}/{table_name}: {e}");
                        continue;
                    }
                };

            let bound_ts = idx_ts_micros.max(live_ver.map_or(0, version_to_timestamp_micros));

            let live_ttl_parsed = live_item.get(&ttl_attr).and_then(|v| match v {
                AttributeValue::N(n) => parse_ttl_epoch_secs(n).map(|secs| (secs, n.clone())),
                _ => None,
            });

            let Some((live_secs, live_exp_str)) = live_ttl_parsed else {
                // Live item no longer carries a positive numeric TTL attribute.
                let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                continue;
            };

            if live_secs > now_epoch_s {
                // Live item's TTL was extended into the future: ensure the future
                // TTL index entry exists at `live_ver` before retiring this stale
                // expired index cell.
                if let Some(lv) = live_ver {
                    let _ = engine
                        .ensure_ttl_index_entry(&key_info, base_row_key, live_secs, lv)
                        .await;
                }
                let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                continue;
            }

            // `0 < live_secs <= now_epoch_s`: evict regardless of whether `live_secs`
            // equals the scanned `expiry` (ABA-safe), conditional on the
            // exact live TTL attribute value `#ttl = :ttl_exp` so a concurrent
            // extension between `get_item_with_version` and `delete_item` is safe.
            let cond = Expr::Compare {
                left: Box::new(Expr::Path(vec![PathElement::Attribute("#ttl".to_string())])),
                op: CompareOp::Eq,
                right: Box::new(Expr::Placeholder("ttl_exp".to_string())),
            };
            let mut names = HashMap::with_capacity(1);
            names.insert("ttl".to_string(), ttl_attr);
            let mut values = HashMap::with_capacity(1);
            values.insert("ttl_exp".to_string(), AttributeValue::N(live_exp_str));
            let maps = ExpressionMaps::new(names, values);

            let stream_capture = key_info
                .stream_specification
                .as_ref()
                .filter(|s| s.stream_enabled)
                .map(|s| StreamCapture {
                    view_type: s
                        .stream_view_type
                        .unwrap_or(StreamViewType::NewAndOldImages),
                    user_identity: Some(UserIdentity {
                        principal_id: TTL_PRINCIPAL_ID.to_owned(),
                        identity_type: TTL_PRINCIPAL_TYPE.to_owned(),
                    }),
                    region: Arc::from(engine.region()),
                });

            match DataEngine::delete_item(
                engine,
                &key_info,
                &base_key,
                stream_capture.is_some(),
                Some(&cond),
                &maps,
                stream_capture.as_ref(),
            )
            .await
            {
                Ok(_) => {
                    if let Some(m) = metrics {
                        m.record_ttl_deletion(account_id, table_name);
                        let staleness = now_epoch_s.saturating_sub(live_secs).max(0);
                        #[allow(clippy::cast_precision_loss)]
                        m.record_ttl_staleness(account_id, table_name, staleness as f64);
                    }
                    let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                }
                Err(StorageError::ConditionFailed(_)) => {
                    // Only retire cells up to `bound_ts`: if a concurrent same-second
                    // update ("1000" -> "1000.5") caused ConditionFailed, its newer
                    // cell on `raw_key` at `ts(v2) >= next_millis_exclusive(bound_ts)`
                    // remains intact!
                    let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                }
                Err(e) => {
                    tracing::warn!(
                        "TTL delete of expired item in {account_id}/{table_name} failed: {e}"
                    );
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

/// Delete cells on `raw_key` in `__extenddb_ttl_index__` stamped at or before
/// `max_inclusive_ts_micros` using a millisecond-aligned `[0, next_millis_exclusive)`
/// `DeleteFromColumn` (never `DeleteFromRow`) so any newer concurrent TTL index
/// stamp survives.
async fn delete_ttl_index_entry_up_to(
    engine: &BigtableEngine,
    raw_key: &[u8],
    max_inclusive_ts_micros: i64,
) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE);

    let mutation = Mutation {
        mutation: Some(mutation::Mutation::DeleteFromColumn(DeleteFromColumn {
            family_name: "d".to_string(),
            column_qualifier: vec![],
            time_range: Some(TimestampRange {
                start_timestamp_micros: 0,
                end_timestamp_micros: next_millis_exclusive(max_inclusive_ts_micros),
            }),
        })),
    };

    let req = MutateRowRequest {
        table_name: full_index_table,
        app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
        row_key: raw_key.to_vec(),
        mutations: vec![mutation],
        ..MutateRowRequest::default()
    };
    data.mutate_row(req)
        .await
        .map_err(|e| format!("delete TTL index up_to: {e}"))?;
    Ok(())
}

pub async fn ensure_ttl_index_table(client: &BigtableClient) -> Result<(), String> {
    let mut admin = crate::data::admin::AdminClient::connect(client).await?;
    admin
        .create_table(
            crate::data::encoding::ttl_key::TTL_INDEX_TABLE,
            &[("d", Some(crate::data::admin::gc_max_versions(1)))],
        )
        .await
        .map_err(|e| format!("create TTL index table: {e}"))?;
    Ok(())
}
