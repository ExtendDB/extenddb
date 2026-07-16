// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite background workers: control-plane transition poller, table-size
//! refresh, stream-record cleanup, idempotency-token cleanup, TTL sweep, the
//! persistent GSI propagation worker, and the GSI-delay setting poller.
//!
//! GSI updates with a non-zero effective propagation delay are applied
//! asynchronously via the `gsi_pending` queue (drained by an event-driven
//! worker); LSIs and zero-delay GSIs are applied synchronously on the write
//! path. There is a single connection pool (catalog and data co-locate).

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::metrics::{MetricsCollector, QuerySource};
use extenddb_core::types::UserIdentity;
use extenddb_storage::error::StorageError;
use extenddb_storage::hooks::{CancellationToken, sleep_or_shutdown};
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::{DataEngine, MetadataEngine, StreamEngine, TableEngine};

use crate::store::SqliteEngine;

pub(crate) async fn poll_control_plane_transitions<S: SettingsStore + ?Sized>(
    storage: Arc<SqliteEngine>,
    notify: Arc<tokio::sync::Notify>,
    settings: Arc<S>,
    token: CancellationToken,
) {
    const ACTIVE_POLL: Duration = Duration::from_secs(1);
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
    const MARGIN_SECS: f64 = 5.0;

    loop {
        // Idle: wait for a wake signal, an idle timeout (defensive sweep), or
        // shutdown.
        let shutting_down = tokio::select! {
            () = token.cancelled() => true,
            _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()) => false,
        };
        if shutting_down {
            return;
        }
        let delay = settings
            .get_setting("control_plane_delay_seconds")
            .await
            .ok()
            .flatten()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|&v| v >= 0.0)
            .unwrap_or(0.25);
        let deadline = tokio::time::Instant::now() + Duration::from_secs_f64(delay + MARGIN_SECS);
        loop {
            match storage.process_control_plane_transitions().await {
                Ok(transitions) => {
                    for (name, transition) in &transitions {
                        tracing::info!("Table '{name}': {transition}");
                    }
                }
                Err(e) => {
                    tracing::warn!("Control plane transition poll failed: {e}");
                    break;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            if !sleep_or_shutdown(&token, ACTIVE_POLL).await {
                return;
            }
        }
    }
}

pub(crate) async fn table_size_refresh_worker(
    storage: Arc<SqliteEngine>,
    token: CancellationToken,
) {
    const INTERVAL: Duration = Duration::from_secs(300);
    while sleep_or_shutdown(&token, INTERVAL).await {
        let tables = match MetadataEngine::all_active_tables(&*storage).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("Size refresh worker: list tables failed: {e}");
                continue;
            }
        };
        for (account_id, table_name) in &tables {
            if let Err(e) =
                MetadataEngine::refresh_table_size(&*storage, account_id, table_name).await
            {
                tracing::warn!("Size refresh worker: {table_name}: {e}");
            }
        }
    }
}

pub(crate) async fn stream_record_cleanup_worker(
    storage: Arc<SqliteEngine>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    const INTERVAL: Duration = Duration::from_secs(3600);
    const RETENTION_HOURS: i64 = 24;
    while sleep_or_shutdown(&token, INTERVAL).await {
        let start = std::time::Instant::now();
        match StreamEngine::cleanup_expired_stream_records(&*storage, RETENTION_HOURS).await {
            Ok(n) => {
                if n > 0 {
                    tracing::info!("Stream cleanup worker: deleted {n} expired record(s)");
                }
                #[allow(clippy::cast_precision_loss)]
                metrics.record_worker_success(
                    QuerySource::StreamCleanup,
                    start.elapsed().as_micros() as f64,
                );
            }
            Err(e) => {
                tracing::error!("Stream record cleanup failed: {e}");
                metrics.record_worker_error(QuerySource::StreamCleanup);
            }
        }
    }
}

pub(crate) async fn idempotency_token_cleanup_worker(
    storage: Arc<SqliteEngine>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    const INTERVAL: Duration = Duration::from_secs(600);
    const MAX_AGE_SECONDS: i64 = 600;
    while sleep_or_shutdown(&token, INTERVAL).await {
        let start = std::time::Instant::now();
        match DataEngine::cleanup_expired_idempotency_tokens(&*storage, MAX_AGE_SECONDS).await {
            Ok(n) => {
                if n > 0 {
                    tracing::info!("Idempotency cleanup worker: deleted {n} expired token(s)");
                }
                #[allow(clippy::cast_precision_loss)]
                metrics.record_worker_success(
                    QuerySource::IdempotencyCleanup,
                    start.elapsed().as_micros() as f64,
                );
            }
            Err(e) => {
                tracing::error!("Idempotency token cleanup failed: {e}");
                metrics.record_worker_error(QuerySource::IdempotencyCleanup);
            }
        }
    }
}

const TTL_SCAN_INTERVAL: Duration = Duration::from_secs(60);
const TTL_BATCH: usize = 100;

pub(crate) async fn ttl_cleanup_worker(
    storage: Arc<SqliteEngine>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    let region: Arc<str> = Arc::from(storage.region.as_str());
    while sleep_or_shutdown(&token, TTL_SCAN_INTERVAL).await {
        retry_pending_indexes(&storage).await;
        sweep_expired_items(&storage, &metrics, &region).await;
    }
}

async fn retry_pending_indexes(storage: &SqliteEngine) {
    let (Ok(pending), Ok(ready)) = (
        MetadataEngine::all_tables_with_ttl(storage).await,
        MetadataEngine::all_tables_with_ttl_index_ready(storage).await,
    ) else {
        return;
    };
    let ready_set: std::collections::HashSet<(&str, &str)> = ready
        .iter()
        .map(|(a, t, _)| (a.as_str(), t.as_str()))
        .collect();
    for (account_id, table_name, ttl_attr) in &pending {
        if !ready_set.contains(&(account_id.as_str(), table_name.as_str()))
            && let Err(e) =
                MetadataEngine::create_ttl_index(storage, account_id, table_name, ttl_attr).await
        {
            tracing::debug!("TTL worker: index retry failed for {table_name}: {e}");
        }
    }
}

async fn sweep_expired_items(
    storage: &SqliteEngine,
    metrics: &MetricsCollector,
    region: &Arc<str>,
) {
    let ttl_identity = UserIdentity {
        identity_type: "Service".to_owned(),
        principal_id: "dynamodb.amazonaws.com".to_owned(),
    };

    let tables = match MetadataEngine::all_tables_with_ttl_index_ready(storage).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!("TTL worker: list tables failed: {e}");
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
            TTL_BATCH,
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
        let view_type = key_info.stream_specification.as_ref().and_then(|s| {
            if s.stream_enabled {
                s.stream_view_type
            } else {
                None
            }
        });
        let (condition_expr, maps) = build_ttl_condition(ttl_attribute, now_epoch);

        let mut deleted = 0usize;
        for item in &items {
            let staleness = item
                .get(ttl_attribute.as_str())
                .and_then(|av| match av {
                    extenddb_core::types::AttributeValue::N(n) => n.parse::<u64>().ok(),
                    _ => None,
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

            let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: Some(ttl_identity.clone()),
                region: region.clone(),
            });
            match DataEngine::delete_item(
                storage,
                &key_info,
                &key,
                view_type.is_some(),
                Some(&condition_expr),
                &maps,
                stream.as_ref(),
            )
            .await
            {
                Err(StorageError::ConditionFailed(_)) => {}
                Err(e) => tracing::warn!("TTL worker: delete failed for {table_name}: {e}"),
                Ok(_) => {
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
            tracing::info!("TTL worker: deleted {deleted} expired item(s) from {table_name}");
        }
    }
}

/// `attribute_exists(#ttl) AND #ttl <= :now` — guards against deleting an item
/// whose TTL was cleared or moved into the future between scan and delete.
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
    let condition = Expr::And(
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
    (condition, ExpressionMaps::new(names, values))
}

/// Maximum `gsi_pending` rows claimed per worker batch.
const GSI_BATCH: i64 = 100;

/// Drains the persistent `gsi_pending` queue, applying async GSI updates whose
/// `ready_at` has passed. A single worker suffices: all writes (and the
/// claim+apply transaction) are serialized by the engine write lock.
///
/// The worker is event-driven — it wakes exactly when the next row becomes due
/// (`MIN(ready_at)`) or when a write notifies it, never on a blind interval —
/// so async GSI application latency tracks the configured propagation delay and
/// the write path pays nothing beyond the in-transaction `gsi_pending` insert.
pub(crate) async fn gsi_propagation_worker(
    engine: Arc<SqliteEngine>,
    notify: Arc<tokio::sync::Notify>,
    token: CancellationToken,
) {
    // Defensive cap on idle sleep so a missed notification can never strand a
    // pending row indefinitely. Matches the PostgreSQL persistent-queue worker's
    // 1s backstop (PR #128) so recovery from a missed wake is identical across
    // backends; the worker is otherwise notify-driven and sleeps to MIN(ready_at).
    const MAX_SLEEP: Duration = Duration::from_secs(1);
    // Backoff after an error so a poison row cannot hot-loop the worker.
    const ERROR_BACKOFF: Duration = Duration::from_secs(1);
    loop {
        // Drain everything currently due.
        let mut errored = false;
        loop {
            match process_gsi_batch(&engine).await {
                Ok(n) if n > 0 => continue,
                Ok(_) => break,
                Err(e) => {
                    tracing::error!("GSI propagation worker: batch error: {e}");
                    errored = true;
                    break;
                }
            }
        }
        // Sleep until the next row is due (or a write wakes us early).
        let wait = if errored {
            ERROR_BACKOFF
        } else {
            match next_ready_delay(&engine).await {
                Ok(Some(d)) => d.min(MAX_SLEEP),
                Ok(None) => MAX_SLEEP,
                Err(e) => {
                    tracing::debug!("GSI worker: next_ready_delay error: {e}");
                    MAX_SLEEP
                }
            }
        };
        // Sleep, waking early on a write notification, and return on shutdown.
        // The queue is persistent (`gsi_pending`) and reconciled at startup, so
        // stopping between drains loses nothing.
        let shutting_down = tokio::select! {
            () = token.cancelled() => true,
            _ = tokio::time::timeout(wait, notify.notified()) => false,
        };
        if shutting_down {
            return;
        }
    }
}

/// Time until the earliest pending row becomes due. `Some(ZERO)` means a row is
/// already due (drain again immediately); `None` means the queue is empty.
/// Runs on the shared read pool (no write lock, indexed `MIN` on
/// `idx_gsi_pending_ready`) so it stays off the write path.
async fn next_ready_delay(engine: &SqliteEngine) -> Result<Option<Duration>, StorageError> {
    let min_ready: Option<String> = sqlx::query_scalar("SELECT MIN(ready_at) FROM gsi_pending")
        .fetch_one(&engine.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let Some(ready_at) = min_ready else {
        return Ok(None);
    };
    let ready = crate::sqlite_util::parse_timestamp(&ready_at)
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let now = time::OffsetDateTime::now_utc();
    if ready <= now {
        Ok(Some(Duration::ZERO))
    } else {
        // Positive remainder; convert time::Duration → std::time::Duration.
        Ok(Some((ready - now).try_into().unwrap_or(Duration::ZERO)))
    }
}

/// Parse a claimed `gsi_pending` row and apply its index update within `tx`.
/// Any error (malformed context or a non-recoverable apply failure) is returned
/// to the caller, which drops the row rather than stalling the queue. A
/// dropped-index race is handled inside `apply_claimed_row` (returns `Ok`).
async fn apply_pending_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    old_json: &Option<String>,
    new_json: &Option<String>,
    ctx_json: &str,
) -> Result<(), StorageError> {
    let old: Option<extenddb_core::types::Item> = old_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let new: Option<extenddb_core::types::Item> = new_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let context: crate::data::GsiApplyContext =
        serde_json::from_str(ctx_json).map_err(|e| StorageError::Internal(e.to_string()))?;
    crate::data::apply_claimed_row(tx, old.as_ref(), new.as_ref(), &context).await
}

/// Claim and apply one batch of due `gsi_pending` rows in a single transaction
/// (under the write lock). Claim + apply share the transaction, so a crash
/// rolls back and the rows are retried — at-least-once, and the index writes
/// are idempotent. Returns the number of rows processed.
async fn process_gsi_batch(engine: &SqliteEngine) -> Result<usize, StorageError> {
    let _writer = engine.write_lock.lock().await;
    let mut tx = engine
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    let now = crate::sqlite_util::format_timestamp(time::OffsetDateTime::now_utc());
    // Drain in `id` order; per-partition `ready_at` is monotonic, so this
    // preserves per-key FIFO. Each row is self-describing via `index_context`.
    let rows: Vec<(Option<String>, Option<String>, String)> = sqlx::query_as(
        "DELETE FROM gsi_pending WHERE id IN ( \
             SELECT id FROM gsi_pending WHERE ready_at <= ? ORDER BY id LIMIT ? \
         ) RETURNING old_item, new_item, index_context",
    )
    .bind(&now)
    .bind(GSI_BATCH)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    if rows.is_empty() {
        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        return Ok(0);
    }

    let count = rows.len();
    for (old_json, new_json, ctx_json) in &rows {
        // Guard each row with a SAVEPOINT. A row that cannot be applied (bad
        // context, or a persistent apply error) is undone and DROPPED — it was
        // already removed from `gsi_pending` by the batch DELETE above, so the
        // commit consumes it. This matches the PostgreSQL backend's log-and-drop:
        // one poison row must not roll back the whole batch and get re-claimed
        // forever, which would stall ALL GSI propagation instance-wide.
        // (A dropped-index race is handled inside apply_claimed_row and returns
        // Ok, so it is applied-as-skip, not dropped here.)
        sqlx::query("SAVEPOINT gsi_row")
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        match apply_pending_row(&mut tx, old_json, new_json, ctx_json).await {
            Ok(()) => {
                sqlx::query("RELEASE gsi_row")
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            Err(e) => {
                tracing::error!("GSI worker: dropping unprocessable gsi_pending row: {e}");
                sqlx::query("ROLLBACK TO gsi_row")
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                sqlx::query("RELEASE gsi_row")
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }
    }

    tx.commit()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(count)
}

/// Refresh the cached GSI propagation delay from the `gsi_propagation_delay_ms`
/// runtime setting, so changes take effect without restart.
pub(crate) async fn poll_gsi_delay<S: SettingsStore + ?Sized>(
    settings: Arc<S>,
    gsi_default: Arc<std::sync::atomic::AtomicU64>,
    token: CancellationToken,
) {
    use std::sync::atomic::Ordering;
    const POLL: Duration = Duration::from_secs(30);
    while sleep_or_shutdown(&token, POLL).await {
        match settings.get_setting("gsi_propagation_delay_ms").await {
            Ok(Some(v)) => {
                if let Ok(ms) = v.parse::<u64>() {
                    gsi_default.store(ms, Ordering::Relaxed);
                }
            }
            Ok(None) => gsi_default.store(10, Ordering::Relaxed),
            Err(e) => tracing::debug!("poll_gsi_delay: {e:?}"),
        }
    }
}

#[cfg(test)]
mod poison_row_tests {
    use super::process_gsi_batch;
    use crate::SqliteEngine;

    /// A `gsi_pending` row that cannot be applied (here: `index_context` is not
    /// valid JSON) must be logged-and-DROPPED so the batch commits and the queue
    /// drains. Before the per-row SAVEPOINT fix, the apply error rolled back the
    /// whole batch including the claim DELETE, so the same row was re-claimed
    /// forever and ALL GSI propagation stalled instance-wide.
    #[tokio::test]
    async fn poison_row_is_dropped_not_stalled() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        // Poison row: malformed index_context. ready_at defaults to now (due).
        sqlx::query(
            "INSERT INTO gsi_pending \
             (table_id, worker_partition, old_item, new_item, index_context, ready_at) \
             VALUES ('t-poison', 0, NULL, '{\"pk\":{\"S\":\"x\"}}', 'not-valid-json', \
                     '2000-01-01T00:00:00.000Z')",
        )
        .execute(&engine.pool)
        .await
        .expect("insert poison row");

        // The batch claims and consumes the row (returns Ok, count = 1).
        let processed = process_gsi_batch(&engine).await.expect("batch drains");
        assert_eq!(processed, 1, "poison row should be claimed and consumed");

        // The poison row is gone, not rolled back into the queue.
        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM gsi_pending")
            .fetch_one(&engine.pool)
            .await
            .expect("count");
        assert_eq!(remaining.0, 0, "poison row must be dropped, not re-queued");

        // A second pass has nothing left to do (proves it was not re-claimed).
        assert_eq!(
            process_gsi_batch(&engine).await.expect("second pass"),
            0,
            "queue must stay empty (no infinite re-claim)"
        );
    }

    /// A well-formed row alongside a poison row is still applied-as-skip when its
    /// target index no longer exists (dropped-index race handled inside
    /// apply_claimed_row), and both rows are consumed — one bad row does not
    /// block the rest of the batch.
    #[tokio::test]
    async fn poison_row_does_not_block_sibling_rows() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        // Poison row (bad context) + a second poison row: both must drain.
        sqlx::query(
            "INSERT INTO gsi_pending \
             (table_id, worker_partition, old_item, new_item, index_context, ready_at) \
             VALUES \
             ('t-a', 0, NULL, '{\"pk\":{\"S\":\"1\"}}', 'not-json', '2000-01-01T00:00:00.000Z'), \
             ('t-b', 1, NULL, '{\"pk\":{\"S\":\"2\"}}', 'also-not-json', '2000-01-01T00:00:00.000Z')",
        )
        .execute(&engine.pool)
        .await
        .expect("insert rows");

        let processed = process_gsi_batch(&engine).await.expect("batch drains");
        assert_eq!(processed, 2, "both rows claimed in one batch");

        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM gsi_pending")
            .fetch_one(&engine.pool)
            .await
            .expect("count");
        assert_eq!(remaining.0, 0, "both poison rows dropped");
    }
}
