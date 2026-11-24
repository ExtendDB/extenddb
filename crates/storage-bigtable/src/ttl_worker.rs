// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL backfill recovery and expiration worker.
//!
//! - Completes any interrupted `ENABLING` table backfills whose per-table
//!   `lock:ttl_backfill:<table_id>` lease is free.
//! - Sweeps `__extenddb_ttl_index__` shards `0..16`, verifying each candidate
//!   against the live base item's TTL attribute (resolving `COMMITTED` 2PC
//!   intents), evicting items whose current TTL epoch is in `(0, now_epoch_s]`
//!   when the table's TTL status is `ENABLED`, and leaving index entries intact
//!   whenever TTL is `ENABLING` or `DISABLED` so no item is missed across status
//!   transitions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement};
use extenddb_core::metrics::MetricsCollector;
use extenddb_core::types::{AttributeValue, StreamViewType, TimeToLiveStatus, UserIdentity};
use extenddb_storage::error::StorageError;
use extenddb_storage::{CancellationToken, DataEngine, MetadataEngine, StreamCapture, TableEngine};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet, TimestampRange,
    mutation,
    mutation::DeleteFromColumn,
    row_filter::Filter,
    row_range::{EndKey, StartKey},
};

use crate::BigtableEngine;
use crate::data::client::BigtableClient;
pub use crate::data::encoding::ttl_key::TTL_INDEX_TABLE;
use crate::data::item_ops::{
    next_millis_exclusive, parse_ttl_epoch_secs, version_to_timestamp_micros,
};
use crate::runtime_hooks::run_leased_worker;

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
    let catalog_client = engine.catalog_client_arc();
    let owner = engine.node_id().to_owned();
    let lease_duration = (cadence * 3).max(Duration::from_secs(30));

    run_leased_worker(
        catalog_client,
        "ttl_worker",
        owner,
        cadence,
        lease_duration,
        shutdown,
        |pass_token| {
            let engine = engine.clone();
            let metrics = metrics.clone();
            async move {
                if let Err(e) =
                    sweep_once_with_token(&engine, metrics.as_deref(), &pass_token).await
                {
                    tracing::warn!("TTL worker sweep error: {e}");
                }
            }
        },
    )
    .await;
    tracing::info!("bigtable TTL worker stopped");
}

/// Run one TTL sweep pass across all 16 shards without metrics collection.
///
/// # Errors
///
/// Returns an error description if reading system clock fails.
pub async fn sweep_once(engine: &BigtableEngine) -> Result<(), String> {
    sweep_once_with_metrics(engine, None).await
}

/// Run one TTL sweep pass across all 16 shards with optional metrics collection.
///
/// # Errors
///
/// Returns an error description if reading system clock fails.
pub async fn sweep_once_with_metrics(
    engine: &BigtableEngine,
    metrics: Option<&MetricsCollector>,
) -> Result<(), String> {
    let token = CancellationToken::new();
    sweep_once_with_token(engine, metrics, &token).await
}

async fn sweep_once_with_token(
    engine: &BigtableEngine,
    metrics: Option<&MetricsCollector>,
    shutdown: &CancellationToken,
) -> Result<(), String> {
    if let Err(e) = engine.resume_enabling_ttl_tables(shutdown).await {
        tracing::warn!("TTL worker backfill recovery error: {e}");
    }
    if shutdown.is_cancelled() {
        return Ok(());
    }

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs(),
    )
    .unwrap_or(i64::MAX);

    for shard in 0..crate::data::encoding::ttl_key::NUM_SHARDS {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        if let Err(e) = sweep_shard(engine, shard, now, metrics, shutdown).await {
            tracing::warn!("TTL worker sweep failed for shard {shard}: {e}");
        }
    }
    Ok(())
}

// Justification: per-shard TTL sweep pages chronological index entries, verifies table_id and live item state, emits service stream records, and retires stale entries.
#[allow(clippy::too_many_lines)]
async fn sweep_shard(
    engine: &BigtableEngine,
    shard_id: u8,
    now_epoch_s: i64,
    metrics: Option<&MetricsCollector>,
    shutdown: &CancellationToken,
) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(TTL_INDEX_TABLE);

    let end_key = {
        let mut k = Vec::with_capacity(9);
        k.push(shard_id);
        k.extend_from_slice(&now_epoch_s.saturating_add(1).to_be_bytes());
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
        let batch_len = resp.len();

        let mut last_key = None;

        for (raw_key, cells) in resp {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            last_key = Some(raw_key.clone());
            let idx_ts_micros = cells.iter().map(|c| c.timestamp_micros).max().unwrap_or(0);
            let cell_val = cells
                .iter()
                .find(|c| c.family_name == "d" && c.qualifier.is_empty())
                .map_or(&[][..], |c| c.value.as_slice());

            let (_, expiry, account_id, table_name, entry_table_id, base_row_key) =
                match crate::data::encoding::ttl_key::decode_ttl_entry(&raw_key, cell_val) {
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

            if entry_table_id.is_some_and(|tid| tid != key_info.table_id) {
                let _ = delete_ttl_index_entry_up_to(engine, &raw_key, idx_ts_micros).await;
                continue;
            }

            let ttl_desc = match MetadataEngine::describe_ttl(engine, account_id, table_name).await
            {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("could not describe TTL for {account_id}/{table_name}: {e}");
                    continue;
                }
            };

            // Never delete TTL index entries while TTL is `ENABLING`, `DISABLED`,
            // or `DISABLING`: `ENABLING` is actively populating due entries, and
            // preserving off-state entries ensures re-enabling never misses items.
            let Some(ttl_attr) = ttl_desc
                .attribute_name
                .filter(|_| ttl_desc.time_to_live_status == TimeToLiveStatus::Enabled)
            else {
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
                let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                continue;
            };

            if live_secs > now_epoch_s {
                if let Some(lv) = live_ver {
                    let _ = engine
                        .ensure_ttl_index_entry(&key_info, base_row_key, live_secs, lv)
                        .await;
                }
                let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                continue;
            }

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
                        // Justification: TTL staleness in seconds fits comfortably within f64's 53-bit mantissa for metric histograms.
                        #[allow(clippy::cast_precision_loss)]
                        m.record_ttl_staleness(account_id, table_name, staleness as f64);
                    }
                    let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                }
                Err(StorageError::ConditionFailed(_)) => {
                    let _ = delete_ttl_index_entry_up_to(engine, &raw_key, bound_ts).await;
                }
                Err(e) => {
                    tracing::warn!(
                        "TTL delete of expired item in {account_id}/{table_name} failed: {e}"
                    );
                }
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
    Ok(())
}

/// Delete cells on `raw_key` in `__extenddb_ttl_index__` stamped at or before
/// `max_inclusive_ts_micros` using a millisecond-aligned `[0, next_millis_exclusive)`
/// `DeleteFromColumn` (never `DeleteFromRow`).
pub(crate) async fn delete_ttl_index_entry_up_to(
    engine: &BigtableEngine,
    raw_key: &[u8],
    max_inclusive_ts_micros: i64,
) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(TTL_INDEX_TABLE);

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

/// Ensure the `__extenddb_ttl_index__` system table exists on the Bigtable
/// data instance (called from Migration 1 `M0001DataBaseline`).
///
/// # Errors
///
/// Returns [`crate::data::admin::AdminError`] if connecting or creating the table fails.
pub async fn ensure_ttl_index_table(
    client: &BigtableClient,
) -> Result<(), crate::data::admin::AdminError> {
    let mut admin = crate::data::admin::AdminClient::connect(client).await?;
    admin
        .create_table(
            TTL_INDEX_TABLE,
            &[("d", Some(crate::data::admin::gc_max_versions(1)))],
        )
        .await
}
