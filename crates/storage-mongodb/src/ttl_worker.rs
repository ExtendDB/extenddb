// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL cleanup background worker for `MongoDB`.

use std::sync::Arc;
use std::time::Duration;

use bson::{Document, doc};
use extenddb_core::metrics::MetricsCollector;
use extenddb_core::types::{KeySchemaElement, Projection, ProjectionType, UserIdentity};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, MetadataEngine, StreamEngine, TableEngine, WorkerStore};
use futures::TryStreamExt;

use crate::MongoEngine;
use crate::data_engine::GsiBackfillMode;

const SCAN_INTERVAL: Duration = Duration::from_secs(60);
const BATCH_SIZE: usize = 100;
const STREAM_RETENTION_HOURS: i64 = 24;
const STREAM_CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);
const GSI_BACKFILL_INTERVAL: Duration = Duration::from_secs(5);
const GSI_BACKFILL_BATCH: i64 = 500;
/// How often to flip due `CREATING` tables to `ACTIVE`. Short enough that the
/// window closes promptly after `control_plane_delay_seconds` (default 0.25s)
/// without busy-spinning.
const CONTROL_PLANE_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub(crate) async fn ttl_cleanup_worker(storage: Arc<MongoEngine>, metrics: Arc<MetricsCollector>) {
    let region_arc: Arc<str> = Arc::from(storage.region.as_str());

    loop {
        tokio::time::sleep(SCAN_INTERVAL).await;
        retry_pending_indexes(&storage).await;
        sweep_expired_items(&storage, &metrics, &region_arc).await;
    }
}

/// Defense-in-depth stream-record deletion. A MongoDB TTL index on
/// `stream_records.created_at` already deletes records ~1 minute after
/// created_at + 24h; this loop covers the case where that index is
/// missing (init predates the schema change) or lagging.
pub(crate) async fn stream_record_cleanup_worker(storage: Arc<MongoEngine>) {
    loop {
        tokio::time::sleep(STREAM_CLEANUP_INTERVAL).await;
        match StreamEngine::cleanup_expired_stream_records(&*storage, STREAM_RETENTION_HOURS).await
        {
            Ok(0) => {}
            Ok(n) => tracing::info!("Stream cleanup worker: deleted {n} expired record(s)"),
            Err(e) => tracing::warn!("Stream cleanup worker: delete failed: {e}"),
        }
    }
}

/// Background worker that turns CREATING GSIs into ACTIVE ones.
///
/// UpdateTable's GSI-create path leaves the index in `index_status:
/// "CREATING"` after inserting the catalog document. This worker
/// discovers each such row, iterates the base collection with a
/// persistent cursor, upserts projected items into the index
/// collection, and — once the base is fully scanned — flips the
/// index to `ACTIVE`. Restart-safe: the cursor is persisted after
/// every batch so a mid-backfill crash resumes where it left off.
///
/// Live writes during the backfill window continue to route through
/// `sync_indexes` / `sync_indexes_in_session`, which write to
/// CREATING indexes too (indexes catalog membership, not status, is
/// what gates the write path). Both paths use a transaction over the
/// base and index rows, so a concurrent mutation serializes with or
/// aborts the backfill rather than being overwritten by a stale upsert
/// — RFC-0003 §2.4.
pub(crate) async fn gsi_backfill_worker(storage: Arc<MongoEngine>) {
    loop {
        tokio::time::sleep(GSI_BACKFILL_INTERVAL).await;

        let indexes_coll = storage.catalog_db.collection::<Document>("indexes");
        let cursor = match indexes_coll
            .find(doc! { "index_status": "CREATING", "index_type": "GSI" })
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("GSI backfill worker: list failed: {e}");
                continue;
            }
        };
        let jobs: Vec<Document> = match cursor.try_collect().await {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!("GSI backfill worker: collect failed: {e}");
                continue;
            }
        };

        for job in jobs {
            if let Err(e) = run_gsi_backfill_job(&storage, &job).await {
                tracing::warn!(
                    "GSI backfill worker: job failed for index_id={}: {e}",
                    job.get_str("index_id").unwrap_or("?"),
                );
            }
        }
    }
}

async fn run_gsi_backfill_job(storage: &MongoEngine, job: &Document) -> Result<(), StorageError> {
    let index_id = job
        .get_str("index_id")
        .map_err(|_| StorageError::Internal("missing index_id".to_owned()))?
        .to_owned();
    let id_doc = job
        .get_document("_id")
        .map_err(|_| StorageError::Internal("missing _id".to_owned()))?;
    let table_id = id_doc
        .get_str("table_id")
        .map_err(|_| StorageError::Internal("missing _id.table_id".to_owned()))?
        .to_owned();

    let key_info = storage.table_key_info_by_table_id_impl(&table_id).await?;

    let idx_key_schema_bson = job
        .get("key_schema")
        .ok_or_else(|| StorageError::Internal("missing key_schema".to_owned()))?;
    let idx_key_schema: Vec<KeySchemaElement> = bson::from_bson(idx_key_schema_bson.clone())
        .map_err(|e| StorageError::Internal(format!("key_schema parse: {e}")))?;

    let projection: Projection = job
        .get("projection")
        .and_then(|p| bson::from_bson(p.clone()).ok())
        .unwrap_or(Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        });

    let mut cursor = job.get("backfill_cursor").cloned();
    let indexes_coll = storage.catalog_db.collection::<Document>("indexes");

    loop {
        let progress = storage
            .backfill_gsi_batch(
                &key_info,
                &index_id,
                &idx_key_schema,
                &projection,
                cursor.as_ref(),
                GSI_BACKFILL_BATCH,
                GsiBackfillMode::Live,
            )
            .await?;

        if progress.done {
            // Full-scan complete. Flip to ACTIVE and drop the cursor.
            indexes_coll
                .update_one(
                    doc! { "index_id": &index_id, "index_status": "CREATING" },
                    doc! {
                        "$set": { "index_status": "ACTIVE" },
                        "$unset": { "backfill_cursor": "" },
                    },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            tracing::info!(
                "GSI backfill worker: index_id={index_id} ACTIVE (last batch scanned {} docs)",
                progress.scanned,
            );
            return Ok(());
        }

        if let Some(ref last_id) = progress.last_id {
            indexes_coll
                .update_one(
                    doc! { "index_id": &index_id },
                    doc! { "$set": { "backfill_cursor": last_id.clone() } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            cursor = Some(last_id.clone());
        } else {
            // Empty batch but the scan did not report completion. This
            // shouldn't happen — backfill_gsi_batch marks `done` whenever it
            // scans fewer than batch_size docs — so surface it rather than
            // silently returning: the index stays CREATING and this worker
            // will re-pick it up on the next interval, so a persistent
            // occurrence means a GSI is stuck in CREATING.
            tracing::warn!(
                "GSI backfill worker: index_id={index_id} returned an empty, \
                 non-final batch; index remains CREATING and will be retried",
            );
            return Ok(());
        }
    }
}

async fn retry_pending_indexes(storage: &MongoEngine) {
    let Ok(pending) = MetadataEngine::all_tables_with_ttl(storage).await else {
        return;
    };
    let Ok(ready) = MetadataEngine::all_tables_with_ttl_index_ready(storage).await else {
        return;
    };
    let ready_set: std::collections::HashSet<(&str, &str)> = ready
        .iter()
        .map(|(a, t, _)| (a.as_str(), t.as_str()))
        .collect();
    for (account_id, table_name, ttl_attr) in &pending {
        if !ready_set.contains(&(account_id.as_str(), table_name.as_str())) {
            if let Err(e) =
                MetadataEngine::create_ttl_index(storage, account_id, table_name, ttl_attr).await
            {
                tracing::debug!("TTL worker: index creation retry failed for {table_name}: {e}");
            } else {
                tracing::info!("TTL worker: index created for {table_name}");
            }
        }
    }
}

async fn sweep_expired_items(storage: &MongoEngine, metrics: &MetricsCollector, region: &Arc<str>) {
    let ttl_identity = UserIdentity {
        identity_type: "Service".to_owned(),
        principal_id: "dynamodb.amazonaws.com".to_owned(),
    };

    let tables = match MetadataEngine::all_tables_with_ttl_index_ready(storage).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!("TTL worker: failed to list tables: {e}");
            return;
        }
    };

    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    for (account_id, table_name, ttl_attribute) in &tables {
        let items = match MetadataEngine::find_expired_items_indexed(
            storage,
            account_id,
            table_name,
            ttl_attribute,
            BATCH_SIZE,
        )
        .await
        {
            Ok(items) => items,
            Err(e) => {
                tracing::warn!("TTL worker: find expired failed for {table_name}: {e}");
                continue;
            }
        };

        if items.is_empty() {
            continue;
        }

        let key_info = match TableEngine::table_key_info(storage, account_id, table_name).await {
            Ok(ki) => ki,
            Err(e) => {
                tracing::warn!("TTL worker: key info failed for {table_name}: {e}");
                continue;
            }
        };

        let view_type = stream_view_type(&key_info);
        let (condition_expr, maps) = build_ttl_condition(ttl_attribute, now_epoch);

        let mut deleted = 0usize;
        for item in &items {
            let staleness = item
                .get(ttl_attribute.as_str())
                .and_then(|av| {
                    if let extenddb_core::types::AttributeValue::N(n) = av {
                        n.parse::<u64>().ok()
                    } else {
                        None
                    }
                })
                .map(|ttl_val| now_epoch.saturating_sub(ttl_val));

            let key: extenddb_core::types::Item = key_info
                .key_schema
                .iter()
                .filter_map(|ks| {
                    item.get(&ks.attribute_name)
                        .map(|v| (ks.attribute_name.clone(), v.clone()))
                })
                .collect();

            let return_old = view_type.is_some();
            let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: Some(ttl_identity.clone()),
                region: region.clone(),
            });
            match DataEngine::delete_item(
                storage,
                &key_info,
                &key,
                return_old,
                Some(&condition_expr),
                &maps,
                stream.as_ref(),
            )
            .await
            {
                Err(StorageError::ConditionFailed(_)) => {}
                Err(e) => {
                    tracing::warn!("TTL worker: delete failed for {table_name}: {e}");
                }
                Ok(_old_item) => {
                    deleted += 1;
                    metrics.record_ttl_deletion(table_name);
                    if let Some(s) = staleness {
                        #[allow(clippy::cast_precision_loss)]
                        metrics.record_ttl_staleness(table_name, s as f64);
                    }
                }
            }
        }

        if deleted > 0 {
            tracing::info!("TTL worker: deleted {deleted} expired items from {table_name}");
        }
    }
}

fn stream_view_type(
    key_info: &extenddb_core::types::TableKeyInfo,
) -> Option<extenddb_core::types::StreamViewType> {
    key_info.stream_specification.as_ref().and_then(|spec| {
        if spec.stream_enabled {
            spec.stream_view_type
        } else {
            None
        }
    })
}

fn build_ttl_condition(
    ttl_attribute: &str,
    now_epoch: u64,
) -> (
    extenddb_core::expression::Expr,
    extenddb_core::expression::ExpressionMaps,
) {
    use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement};
    use std::collections::HashMap;

    let ttl_path = vec![PathElement::Attribute("#ttl".to_owned())];
    let condition_expr = Expr::And(
        Box::new(Expr::Function {
            name: "attribute_exists".to_owned(),
            args: vec![Expr::Path(ttl_path.clone())],
        }),
        Box::new(Expr::Compare {
            left: Box::new(Expr::Path(ttl_path)),
            op: CompareOp::Le,
            right: Box::new(Expr::Placeholder("now".to_owned())),
        }),
    );

    let mut names = HashMap::new();
    names.insert("ttl".to_owned(), ttl_attribute.to_owned());
    let mut values = HashMap::new();
    values.insert(
        "now".to_owned(),
        extenddb_core::types::AttributeValue::N(now_epoch.to_string()),
    );

    (condition_expr, ExpressionMaps::new(names, values))
}

/// Background poller that flips tables out of the transient `CREATING` state
/// once their scheduled `status_transition_at` has passed. See
/// [`crate::worker_store`] for how rows enter `CREATING`.
pub(crate) async fn control_plane_worker(storage: Arc<MongoEngine>) {
    loop {
        tokio::time::sleep(CONTROL_PLANE_POLL_INTERVAL).await;
        match WorkerStore::process_control_plane_transitions(&*storage).await {
            Ok(t) if t.is_empty() => {}
            Ok(transitions) => {
                for (name, transition) in &transitions {
                    tracing::info!("Table '{name}': {transition}");
                }
            }
            Err(e) => tracing::warn!("Control-plane transition poll failed: {e}"),
        }
    }
}
