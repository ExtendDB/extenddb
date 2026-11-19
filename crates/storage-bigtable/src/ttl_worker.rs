// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL cleanup worker.
//!
//! Sweeps the `__extenddb_ttl_index__` table in shards, verifies each candidate
//! against the live base item's TTL attribute, and deletes genuinely expired
//! items via a conditional `DataEngine::delete_item` carrying the DynamoDB TTL
//! service `StreamCapture` (`dynamodb.amazonaws.com`) so stream emission is
//! fenced atomically through `__extenddb_txn_log__`.

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
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet, mutation,
    mutation::DeleteFromRow,
    row_filter::Filter,
    row_range::{EndKey, StartKey},
};

use crate::BigtableEngine;
use crate::data::client::BigtableClient;
use crate::data::item_ops::parse_ttl_epoch_secs;

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

        for (raw_key, _) in resp {
            last_key = Some(raw_key.clone());
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
                    let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
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
                // TTL disabled on table; drop stale index entry without deleting live item.
                let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
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
                    let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
                    continue;
                }
            };

            // Verify the live item still exists and still carries the exact expired
            // TTL value before deleting. If the item was deleted or updated to a
            // different expiry, clean up only the stale TTL index entry.
            let live_item = match DataEngine::get_item(engine, &key_info, &base_key).await {
                Ok(Some(it)) => it,
                Ok(None) => {
                    let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
                    continue;
                }
                Err(e) => {
                    tracing::warn!("TTL get_item failed in {account_id}/{table_name}: {e}");
                    continue;
                }
            };

            let live_expiry = live_item.get(&ttl_attr).and_then(|v| match v {
                AttributeValue::N(n) => parse_ttl_epoch_secs(n),
                _ => None,
            });
            let Some(live_exp_str) = live_item.get(&ttl_attr).and_then(|v| match v {
                AttributeValue::N(n) if live_expiry == Some(expiry) => Some(n.clone()),
                _ => None,
            }) else {
                // Stale TTL index entry: live item no longer matches this expiry.
                let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
                continue;
            };

            // Build condition `#ttl = :ttl_exp` via `ExpressionMaps::new` so
            // `parsed_numbers` is populated and a concurrent UpdateItem extending
            // TTL between `get_item` and `delete_item` is never clobbered.
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
                        let staleness = now_epoch_s.saturating_sub(expiry).max(0);
                        #[allow(clippy::cast_precision_loss)]
                        m.record_ttl_staleness(account_id, table_name, staleness as f64);
                    }
                    let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
                }
                Err(StorageError::ConditionFailed(_)) => {
                    let _ = delete_ttl_index_entry_raw(engine, &raw_key).await;
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

async fn delete_ttl_index_entry_raw(engine: &BigtableEngine, raw_key: &[u8]) -> Result<(), String> {
    let client = engine.client_ref();
    let mut data = client.data();
    let full_index_table = client.full_table_name(crate::data::encoding::ttl_key::TTL_INDEX_TABLE);

    let req = MutateRowRequest {
        table_name: full_index_table,
        app_profile_id: client.app_profile_id.clone().unwrap_or_default(),
        row_key: raw_key.to_vec(),
        mutations: vec![Mutation {
            mutation: Some(mutation::Mutation::DeleteFromRow(DeleteFromRow {})),
        }],
        ..MutateRowRequest::default()
    };
    data.mutate_row(req)
        .await
        .map_err(|e| format!("delete raw TTL index: {e}"))?;
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
