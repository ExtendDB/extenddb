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
                // Elapsed microseconds fit exactly in f64's 53-bit mantissa for
                // the latency ranges recorded here (telemetry, not exact math).
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
                // Elapsed microseconds fit exactly in f64's 53-bit mantissa for
                // the latency ranges recorded here (telemetry, not exact math).
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
                        // Staleness seconds are small and well within f64's
                        // exact-integer range; used only for telemetry.
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
    // A CREATING vector index with no live backfill task, seen on the previous
    // pass. Two consecutive sightings are required before recovery, because the
    // catalog row commits before the build task registers itself: a single
    // sighting can be a healthy build in that window, but one still unregistered
    // a full pass later (at least MAX_SLEEP apart) is dead. Without recovery it
    // wedges every queued index write for its table until a restart.
    let mut stuck_last_pass: std::collections::HashSet<String> = std::collections::HashSet::new();
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
        // The stuck-build sweep. Cheap when nothing is CREATING, which is the
        // steady state: one indexed catalog read per pass. Only ids seen stuck
        // on TWO consecutive passes are recovered, per index: a first sighting
        // can be a healthy build inside its commit-to-register window, and a
        // stuck sibling must not drag it into a rebuild.
        match engine.stuck_vector_build_candidates().await {
            Ok(candidates) => {
                let confirmed: std::collections::HashSet<String> = candidates
                    .iter()
                    .filter(|id| stuck_last_pass.contains(*id))
                    .cloned()
                    .collect();
                if !confirmed.is_empty()
                    && let Err(e) = engine.recover_stuck_vector_builds(&confirmed).await
                {
                    tracing::error!("GSI propagation worker: stuck-build recovery failed: {e}");
                }
                stuck_last_pass = candidates.into_iter().collect();
            }
            Err(e) => {
                tracing::debug!("GSI worker: stuck-build probe error: {e}");
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
/// dropped-index race is handled inside the apply, for both index kinds, and
/// returns `Ok`.
/// Test-only fault injection: when armed, the next `apply_pending_row` call
/// fails with `Transient` exactly once. A real SQLITE_BUSY cannot be produced
/// mid-batch under the single-connection test pool (the batch already holds
/// the write transaction), so the boundary is faulted directly; the classifier
/// that produces `Transient` from real sqlx errors has its own unit tests.
/// Serializes tests that drive `process_gsi_batch`: the one-shot transient
/// injection below is process-global, so a concurrently running drain in
/// another test can consume an armed injection (or absorb one meant for its
/// sibling), which surfaced as an intermittent 2-test failure once a fourth
/// concurrent caller existed. tokio's Mutex, because the guard is held across
/// awaits.
#[cfg(test)]
static DRAIN_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
static INJECT_TRANSIENT_ONCE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

async fn apply_pending_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    old_json: &Option<String>,
    new_json: &Option<String>,
    ctx_json: &str,
) -> Result<(), StorageError> {
    #[cfg(test)]
    if INJECT_TRANSIENT_ONCE.swap(false, std::sync::atomic::Ordering::SeqCst) {
        return Err(StorageError::Transient(
            "injected transient failure".to_owned(),
        ));
    }
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
    // One queue carries GSI and vector work; the context's shape says which.
    let context: crate::data::PendingApplyContext =
        serde_json::from_str(ctx_json).map_err(|e| StorageError::Internal(e.to_string()))?;
    crate::data::apply_pending_context(tx, old.as_ref(), new.as_ref(), &context).await
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
    // Claim the oldest due rows, then apply them in `id` order. Per-partition
    // `ready_at` is monotonic, so `id` order is write order and applying in it
    // preserves per-key FIFO across both index kinds.
    //
    // Rows for a table with a vector index still in CREATING are NOT claimed, so
    // writes that land during a backfill accumulate and are applied only once the
    // index goes ACTIVE. Without that hold, the backfill's snapshot could be written
    // AFTER a newer queued write had already been applied, and because each apply
    // replaces the row wholesale the index would converge on the stale generation
    // until the next write to that key.
    //
    // The hold is deliberately per TABLE rather than per index, even though only the
    // building index is at risk. Holding just the vector rows would let a GSI row and
    // a vector row for the same item be applied out of order relative to each other,
    // which is the cross-kind FIFO property this queue is documented to provide.
    //
    // The sort below is load-bearing and is NOT redundant with the `ORDER BY id`
    // in the subselect. That clause only chooses WHICH rows the `LIMIT` takes;
    // SQLite defines the order of `RETURNING` output as undefined, and it
    // demonstrably ignores the subselect's ordering (a `DESC` subselect still
    // returns ascending). Relying on it left per-key FIFO resting on an
    // implementation detail: two writes to one item claimed in the same batch
    // could be applied newest-first, and because each apply overwrites the row
    // wholesale, the earlier write would win and the later one be lost.
    let mut rows: Vec<(i64, Option<String>, Option<String>, String)> = sqlx::query_as(
        "DELETE FROM gsi_pending WHERE id IN ( \
             SELECT id FROM gsi_pending WHERE ready_at <= ? \
               AND table_id NOT IN ( \
                   SELECT table_id FROM vector_indexes WHERE index_status = 'CREATING' \
               ) \
             ORDER BY id LIMIT ? \
         ) RETURNING id, old_item, new_item, index_context",
    )
    .bind(&now)
    .bind(GSI_BATCH)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    rows.sort_unstable_by_key(|(id, ..)| *id);

    if rows.is_empty() {
        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        return Ok(0);
    }

    let count = rows.len();
    for (id, old_json, new_json, ctx_json) in &rows {
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
            // Transient failures abort the whole batch WITHOUT committing: the
            // claiming DELETE ran inside this transaction, so returning the
            // error rolls everything back, the rows reappear in gsi_pending,
            // and the worker loop's error path retries next pass. Before this
            // distinction existed, a SQLITE_BUSY or I/O hiccup here was
            // indistinguishable from a poison row and the claimed row was
            // dropped, silently losing its index write forever.
            Err(e @ StorageError::Transient(_)) => {
                tracing::warn!(
                    "GSI worker: transient error applying row {id}; \
                     rolling back the batch for retry: {e}"
                );
                return Err(e);
            }
            Err(e) => {
                tracing::error!("GSI worker: dropping unprocessable gsi_pending row {id}: {e}");
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

/// Read the propagation-delay setting through the settings store, preferring the
/// canonical key and falling back to the pre-rename one.
///
/// The raw-SQL read on the write path uses one query to express the same preference;
/// this path goes through the `SettingsStore` trait, which fetches by exact key, so the
/// fallback is two lookups instead. It runs once per poll interval, not per write.
async fn read_index_propagation_delay<S: SettingsStore + ?Sized>(
    settings: &S,
) -> extenddb_storage::management_store::OpResult<Option<String>> {
    if let Some(v) = settings
        .get_setting(extenddb_core::settings_keys::INDEX_PROPAGATION_DELAY_MS)
        .await?
    {
        return Ok(Some(v));
    }
    settings
        .get_setting(extenddb_core::settings_keys::LEGACY_GSI_PROPAGATION_DELAY_MS)
        .await
}

/// Refresh the cached secondary-index propagation delay from the
/// `index_propagation_delay_ms` runtime setting, so changes take effect without a
/// restart. Falls back to the pre-rename key for a catalog created before it.
pub(crate) async fn poll_index_propagation_delay<S: SettingsStore + ?Sized>(
    settings: Arc<S>,
    index_delay_cache: Arc<std::sync::atomic::AtomicU64>,
    token: CancellationToken,
) {
    use std::sync::atomic::Ordering;
    const POLL: Duration = Duration::from_secs(30);
    while sleep_or_shutdown(&token, POLL).await {
        match read_index_propagation_delay(settings.as_ref()).await {
            Ok(Some(v)) => {
                if let Ok(ms) = v.parse::<u64>() {
                    index_delay_cache.store(ms, Ordering::Relaxed);
                }
            }
            Ok(None) => index_delay_cache
                .store(crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS, Ordering::Relaxed),
            Err(e) => tracing::debug!("poll_gsi_delay: {e:?}"),
        }
    }
}

#[cfg(test)]
mod poison_row_tests {
    /// A TRANSIENT failure mid-batch must abort the batch without consuming the
    /// claimed rows: they reappear in `gsi_pending` and the next pass applies
    /// them. This is the counterpart of the poison tests above, and the review
    /// finding on this file: without the classification, a SQLITE_BUSY or I/O
    /// hiccup was dropped exactly like a poison row, silently losing the index
    /// write forever. Discriminating: under the pre-fix behaviour the row count
    /// after the first pass is 0 and the second pass applies nothing.
    #[tokio::test]
    async fn transient_error_requeues_the_batch_instead_of_dropping_rows() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        // A row whose context parses but whose target index is gone is
        // applied-as-skip (Ok), so on the SECOND pass it drains cleanly.
        sqlx::query(
            "INSERT INTO gsi_pending \
             (table_id, worker_partition, old_item, new_item, index_context, ready_at) \
             VALUES ('t-tr', 0, NULL, '{\"pk\":{\"S\":\"x\"}}', 'not-valid-json', \
                     '2000-01-01T00:00:00.000Z')",
        )
        .execute(&engine.pool)
        .await
        .expect("insert row");

        // Arm the one-shot transient fault: the first pass must FAIL and leave
        // the row queued.
        super::INJECT_TRANSIENT_ONCE.store(true, std::sync::atomic::Ordering::SeqCst);
        let err = process_gsi_batch(&engine)
            .await
            .expect_err("transient failure must abort the batch");
        assert!(
            matches!(err, extenddb_storage::error::StorageError::Transient(_)),
            "expected Transient, got {err:?}"
        );
        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM gsi_pending")
            .fetch_one(&engine.pool)
            .await
            .expect("count");
        assert_eq!(
            remaining.0, 1,
            "the claimed row must be rolled back into the queue, not consumed"
        );

        // Injection disarmed: the second pass processes the row (as poison,
        // which is this row's real classification) and the queue drains.
        let processed = process_gsi_batch(&engine).await.expect("second pass");
        assert_eq!(processed, 1, "the requeued row is retried");
        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM gsi_pending")
            .fetch_one(&engine.pool)
            .await
            .expect("count");
        assert_eq!(remaining.0, 0);
    }

    use super::process_gsi_batch;
    use crate::SqliteEngine;

    /// A `gsi_pending` row that cannot be applied (here: `index_context` is not
    /// valid JSON) must be logged-and-DROPPED so the batch commits and the queue
    /// drains. Before the per-row SAVEPOINT fix, the apply error rolled back the
    /// whole batch including the claim DELETE, so the same row was re-claimed
    /// forever and ALL GSI propagation stalled instance-wide.
    #[tokio::test]
    async fn poison_row_is_dropped_not_stalled() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
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
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
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

#[cfg(test)]
mod vector_propagation_tests {
    use super::process_gsi_batch;
    use crate::SqliteEngine;
    use extenddb_core::types::{
        AttributeDefinition, Item, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    use serde_json::json;

    const INDEX_ID: &str = "vidx-async";

    fn base_ks() -> Vec<KeySchemaElement> {
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }]
    }

    fn base_ad() -> Vec<AttributeDefinition> {
        vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }]
    }

    /// A real table with one unscoped vector index and its data table, built
    /// through the engine rather than by hand so the catalog rows and the data
    /// table are shaped exactly as production makes them.
    async fn table_with_vector_index() -> (SqliteEngine, String) {
        table_with_vector_index_at(":memory:").await
    }

    async fn table_with_vector_index_at(db: &str) -> (SqliteEngine, String) {
        let engine = SqliteEngine::new(db, 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind("000000000000")
            .execute(&engine.pool)
            .await
            .expect("account");
        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl("000000000000", input)
            .await
            .expect("create table");
        let (table_id,): (String,) =
            sqlx::query_as("SELECT table_id FROM tables WHERE table_name = 't'")
                .fetch_one(&engine.pool)
                .await
                .expect("table_id");

        sqlx::query(
            "INSERT INTO vector_indexes \
             (table_id, index_id, index_name, dimensions, distance_function, vector_attribute, \
              projection, index_status) \
             VALUES (?, ?, 'vidx', 2, 'COSINE', ?, ?, 'ACTIVE')",
        )
        .bind(&table_id)
        .bind(INDEX_ID)
        .bind(json!({"AttributeName": "emb"}).to_string())
        .bind(json!({"ProjectionType": "ALL"}).to_string())
        .execute(&engine.pool)
        .await
        .expect("insert vector index");

        let mut tx = engine.pool.begin_with("BEGIN IMMEDIATE").await.expect("tx");
        SqliteEngine::create_vector_data_table(
            &mut tx,
            &table_id,
            INDEX_ID,
            &base_ks(),
            &base_ad(),
        )
        .await
        .expect("create vector data table");
        tx.commit().await.expect("commit ddl");
        (engine, table_id)
    }

    fn item(pk: &str, generation: i32) -> Item {
        serde_json::from_value(json!({
            "pk": {"S": pk},
            "gen": {"N": generation.to_string()},
            "emb": {"L": [{"N": "1"}, {"N": "0"}]},
        }))
        .expect("item")
    }

    /// Drive one write's vector maintenance the way a write path does, committing
    /// it. Returns the number of pending rows enqueued.
    async fn write(
        engine: &SqliteEngine,
        table_id: &str,
        old: Option<&Item>,
        new: Option<&Item>,
        delay_ms: u64,
    ) -> usize {
        let mut tx = engine.pool.begin_with("BEGIN IMMEDIATE").await.expect("tx");
        let n = crate::data::vector_index::maintain_vector_indexes(
            &mut tx,
            table_id,
            &base_ks(),
            &base_ad(),
            old,
            new,
            delay_ms,
        )
        .await
        .expect("maintain");
        tx.commit().await.expect("commit write");
        n
    }

    async fn indexed_rows(engine: &SqliteEngine, table_id: &str) -> Vec<String> {
        let vec_table = crate::data::vector_table_name(table_id, INDEX_ID);
        sqlx::query_as::<_, (String,)>(&format!(
            "SELECT item_data FROM {vec_table} ORDER BY base_pk"
        ))
        .fetch_all(&engine.pool)
        .await
        .expect("read vector rows")
        .into_iter()
        .map(|(json,)| json)
        .collect()
    }

    async fn queue_depth(engine: &SqliteEngine) -> i64 {
        sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM gsi_pending")
            .fetch_one(&engine.pool)
            .await
            .expect("count")
            .0
    }

    /// Mark every queued row due, so a drain can be tested without sleeping for the
    /// propagation delay. Deliberately not a sleep: a test that waits out a real
    /// delay is slow and still races a loaded machine.
    async fn make_all_rows_due(engine: &SqliteEngine) {
        sqlx::query("UPDATE gsi_pending SET ready_at = '2000-01-01T00:00:00.000Z'")
            .execute(&engine.pool)
            .await
            .expect("backdate");
    }

    /// Crash durability of async propagation: an enqueued-but-unapplied index
    /// write survives the death of the process that enqueued it, because the
    /// `gsi_pending` INSERT rides the caller's transaction (`data/index.rs`)
    /// and commits atomically with the base write. There is no in-memory queue
    /// to lose.
    ///
    /// Two engine lifetimes on one database file: the first commits three
    /// writes with a long propagation delay and is dropped with the queue full
    /// and the index empty; the second is a cold start that sees only what
    /// reached disk, and its drain converges the index to all three items.
    ///
    /// What this does and does not simulate: dropping the first engine closes
    /// SQLite cleanly, where SIGKILL would leave an uncheckpointed WAL. The
    /// difference is below the property under test — SQLite recovers committed
    /// WAL transactions on the next open by design — so "committed" is the
    /// boundary this test pins: everything committed before the crash is
    /// applied after restart, and nothing here depends on process memory.
    #[tokio::test]
    async fn enqueued_propagation_survives_a_process_crash() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let dir = std::env::temp_dir().join(format!(
            "eb-vec-crash-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("test dir");
        let db_path = dir.join("crash.db");
        let db = db_path.to_str().expect("utf8 path");

        // Lifetime 1: enqueue three writes, prove they are committed but
        // unapplied, then die.
        let table_id = {
            let (engine, table_id) = table_with_vector_index_at(db).await;
            for pk in ["a", "b", "c"] {
                let enqueued = write(&engine, &table_id, None, Some(&item(pk, 1)), 60_000).await;
                assert_eq!(enqueued, 1, "each write must enqueue exactly one row");
            }
            assert_eq!(queue_depth(&engine).await, 3, "all three rows queued");
            assert!(
                indexed_rows(&engine, &table_id).await.is_empty(),
                "nothing may reach the index before the crash"
            );
            table_id
            // `engine` dropped here: the only surviving state is the file.
        };

        // Lifetime 2: a cold start on the same file. The queue must have
        // survived, and one due drain must converge the index.
        let engine = SqliteEngine::new(db, 1, "us-east-1", 409_600)
            .await
            .expect("engine restart");
        assert_eq!(
            queue_depth(&engine).await,
            3,
            "committed enqueues must survive the restart"
        );
        make_all_rows_due(&engine).await;
        let processed = process_gsi_batch(&engine).await.expect("drain");
        assert_eq!(
            processed, 3,
            "the restarted worker applies all lost-process work"
        );
        assert_eq!(queue_depth(&engine).await, 0, "queue drained");
        assert_eq!(
            indexed_rows(&engine, &table_id).await.len(),
            3,
            "the index converges to every item written before the crash"
        );

        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The whole point of the change: with a propagation delay, a write does not
    /// touch the vector index at all. It queues, stays unapplied until it is due,
    /// and only the worker applies it.
    ///
    /// All four claims are asserted in one test on purpose. Split apart, each half
    /// passes for the wrong reason — "not indexed yet" is indistinguishable from
    /// "never indexed", and "indexed after a drain" is indistinguishable from
    /// "indexed by the write".
    #[tokio::test]
    async fn a_write_reaches_the_vector_index_only_through_the_worker() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let (engine, table_id) = table_with_vector_index().await;

        let enqueued = write(&engine, &table_id, None, Some(&item("a", 1)), 60_000).await;
        assert_eq!(enqueued, 1, "the write must enqueue exactly one row");
        assert!(
            indexed_rows(&engine, &table_id).await.is_empty(),
            "an async write must not index inline"
        );
        assert_eq!(queue_depth(&engine).await, 1, "the row must be durable");

        // Not yet due: draining must leave it alone rather than apply it early.
        assert_eq!(
            process_gsi_batch(&engine).await.expect("drain"),
            0,
            "a row that is not due must not be claimed"
        );
        assert!(
            indexed_rows(&engine, &table_id).await.is_empty(),
            "the delay must be honoured, not merely recorded"
        );

        make_all_rows_due(&engine).await;
        assert_eq!(
            process_gsi_batch(&engine).await.expect("drain"),
            1,
            "the due row must be claimed"
        );
        let rows = indexed_rows(&engine, &table_id).await;
        assert_eq!(rows.len(), 1, "the worker must index the item");
        assert!(
            rows[0].contains("\"gen\""),
            "payload projected: {}",
            rows[0]
        );
        assert_eq!(queue_depth(&engine).await, 0, "the queue must drain");
    }

    /// The other half of the branch. A zero delay is the documented way to ask for
    /// synchronous maintenance, and it must apply in the caller's transaction and
    /// enqueue nothing at all.
    #[tokio::test]
    async fn a_zero_delay_write_is_applied_inline_and_queues_nothing() {
        let (engine, table_id) = table_with_vector_index().await;

        let enqueued = write(&engine, &table_id, None, Some(&item("a", 1)), 0).await;
        assert_eq!(enqueued, 0, "a synchronous write has nothing to enqueue");
        assert_eq!(
            indexed_rows(&engine, &table_id).await.len(),
            1,
            "a zero-delay write must index in its own transaction"
        );
        assert_eq!(queue_depth(&engine).await, 0, "and must not queue");
    }

    /// Two writes to one item must reach the index in write order, whatever jitter
    /// each drew. The guarantee comes from the queue: both rows hash to the base
    /// key's partition and `ready_at` is clamped monotonic within it, so draining in
    /// `id` order cannot invert them.
    ///
    /// Asserted on the surviving payload rather than on timestamps, because the
    /// property that matters is which write won, not what the clamp computed.
    #[tokio::test]
    async fn successive_writes_to_one_item_are_applied_in_write_order() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let (engine, table_id) = table_with_vector_index().await;
        let first = item("a", 1);
        let second = item("a", 2);

        write(&engine, &table_id, None, Some(&first), 60_000).await;
        write(&engine, &table_id, Some(&first), Some(&second), 60_000).await;

        let partitions: Vec<(i64,)> =
            sqlx::query_as("SELECT DISTINCT worker_partition FROM gsi_pending")
                .fetch_all(&engine.pool)
                .await
                .expect("partitions");
        assert_eq!(
            partitions.len(),
            1,
            "both writes to one base key must share a partition, or ordering is not enforced"
        );

        make_all_rows_due(&engine).await;
        assert_eq!(process_gsi_batch(&engine).await.expect("drain"), 2);

        let rows = indexed_rows(&engine, &table_id).await;
        assert_eq!(rows.len(), 1, "one base item indexes to one row");
        assert!(
            rows[0].contains("\"2\""),
            "the later write must win, found: {}",
            rows[0]
        );
    }

    /// An item that loses its vector attribute must leave the index. This is why the
    /// enqueue is unconditional: the row carries no vector to insert, so a write path
    /// that skipped queueing "because there is nothing to index" would leave the
    /// stale row searchable forever.
    #[tokio::test]
    async fn an_item_that_loses_its_vector_is_removed_from_the_index() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let (engine, table_id) = table_with_vector_index().await;
        let with_vector = item("a", 1);
        let without_vector: Item =
            serde_json::from_value(json!({"pk": {"S": "a"}, "gen": {"N": "2"}})).expect("item");

        write(&engine, &table_id, None, Some(&with_vector), 60_000).await;
        make_all_rows_due(&engine).await;
        process_gsi_batch(&engine).await.expect("drain first");
        assert_eq!(indexed_rows(&engine, &table_id).await.len(), 1);

        let enqueued = write(
            &engine,
            &table_id,
            Some(&with_vector),
            Some(&without_vector),
            60_000,
        )
        .await;
        assert_eq!(
            enqueued, 1,
            "a write with no vector must still enqueue: the removal is the work"
        );
        make_all_rows_due(&engine).await;
        process_gsi_batch(&engine).await.expect("drain second");
        assert!(
            indexed_rows(&engine, &table_id).await.is_empty(),
            "the row must be removed once the item stops carrying a vector"
        );
    }

    /// A delete removes the row, driven only by the old item.
    #[tokio::test]
    async fn a_delete_removes_the_indexed_row() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let (engine, table_id) = table_with_vector_index().await;
        let existing = item("a", 1);

        write(&engine, &table_id, None, Some(&existing), 60_000).await;
        make_all_rows_due(&engine).await;
        process_gsi_batch(&engine).await.expect("drain put");
        assert_eq!(indexed_rows(&engine, &table_id).await.len(), 1);

        write(&engine, &table_id, Some(&existing), None, 60_000).await;
        make_all_rows_due(&engine).await;
        process_gsi_batch(&engine).await.expect("drain delete");
        assert!(indexed_rows(&engine, &table_id).await.is_empty());
    }

    /// The index can be dropped while a row is in flight. The batch must still
    /// commit and a sibling row in the same batch must still land: the batch shares
    /// one transaction, so an unhandled failure would take the sibling down with it.
    ///
    /// Deliberately not claimed as a test of the missing-table tolerance in
    /// `apply_vector_context`. Removing that tolerance leaves this test passing,
    /// because the per-row savepoint rolls the row back and drops it, reaching the
    /// same end state. The difference between the two paths is an ERROR log line, not
    /// data, and this test cannot see it.
    #[tokio::test]
    async fn a_row_whose_index_was_dropped_does_not_take_the_batch_down() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let (engine, table_id) = table_with_vector_index().await;
        write(&engine, &table_id, None, Some(&item("a", 1)), 60_000).await;

        // A second table and index, standing in for unrelated work in the same batch.
        let survivor_index = "vidx-survivor";
        sqlx::query(
            "INSERT INTO vector_indexes \
             (table_id, index_id, index_name, dimensions, distance_function, vector_attribute, \
              projection, index_status) \
             VALUES (?, ?, 'vidx2', 2, 'COSINE', ?, ?, 'ACTIVE')",
        )
        .bind(&table_id)
        .bind(survivor_index)
        .bind(json!({"AttributeName": "emb"}).to_string())
        .bind(json!({"ProjectionType": "ALL"}).to_string())
        .execute(&engine.pool)
        .await
        .expect("insert survivor index");
        let mut tx = engine.pool.begin_with("BEGIN IMMEDIATE").await.expect("tx");
        SqliteEngine::create_vector_data_table(
            &mut tx,
            &table_id,
            survivor_index,
            &base_ks(),
            &base_ad(),
        )
        .await
        .expect("create survivor table");
        tx.commit().await.expect("commit ddl");

        // This write enqueues for both indexes; then the first index's table is
        // dropped out from under its queued row.
        write(&engine, &table_id, None, Some(&item("b", 7)), 60_000).await;
        let dropped = crate::data::vector_table_name(&table_id, INDEX_ID);
        sqlx::query(&format!("DROP TABLE {dropped}"))
            .execute(&engine.pool)
            .await
            .expect("drop index data table");

        make_all_rows_due(&engine).await;
        let processed = process_gsi_batch(&engine).await.expect("drain");
        assert_eq!(processed, 3, "every claimed row must be consumed");
        assert_eq!(queue_depth(&engine).await, 0, "no row may be left behind");

        let survivor = crate::data::vector_table_name(&table_id, survivor_index);
        let (rows,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {survivor}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count survivor");
        assert_eq!(
            rows, 1,
            "the sibling index must still be maintained when another index vanishes"
        );
    }

    /// Applying the same claimed context twice must leave the same single row.
    ///
    /// The batch is at-least-once: claim and apply share a transaction, so a crash
    /// mid-apply rolls back and the row is claimed again. That story is only safe if
    /// an apply is idempotent, which is asserted here directly rather than by
    /// implication.
    ///
    /// Being exact about its strength: this is a structural regression guard, not a
    /// discriminating test. Its assertions cannot fail today without a schema change,
    /// because the vector row's primary key is the base item key, so a replay can only
    /// ever overwrite. Measured, not assumed: making the delete conditional on
    /// `old_item` leaves this test PASSING, since the replayed insert then collides on
    /// the primary key, the row is dropped by the savepoint, and the end state is the
    /// same single row. The test below is the one that catches that refactor.
    #[tokio::test]
    async fn applying_the_same_row_twice_is_idempotent() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let (engine, table_id) = table_with_vector_index().await;
        write(&engine, &table_id, None, Some(&item("a", 1)), 60_000).await;

        // Capture the claimed row's payload, then replay it a second time.
        let claimed: (Option<String>, Option<String>, String) =
            sqlx::query_as("SELECT old_item, new_item, index_context FROM gsi_pending LIMIT 1")
                .fetch_one(&engine.pool)
                .await
                .expect("read the queued row");

        make_all_rows_due(&engine).await;
        process_gsi_batch(&engine).await.expect("first apply");
        let after_first = indexed_rows(&engine, &table_id).await;
        assert_eq!(after_first.len(), 1, "first apply indexes the item");

        // Re-enqueue the identical context, exactly as a rolled-back batch would
        // leave it, and drain again.
        sqlx::query(
            "INSERT INTO gsi_pending \
             (table_id, worker_partition, old_item, new_item, index_context, ready_at) \
             VALUES (?, 0, ?, ?, ?, '2000-01-01T00:00:00.000Z')",
        )
        .bind(&table_id)
        .bind(&claimed.0)
        .bind(&claimed.1)
        .bind(&claimed.2)
        .execute(&engine.pool)
        .await
        .expect("replay the row");
        process_gsi_batch(&engine).await.expect("second apply");

        let after_second = indexed_rows(&engine, &table_id).await;
        assert_eq!(
            after_second.len(),
            1,
            "a replayed apply must not duplicate the row"
        );
        assert_eq!(
            after_first, after_second,
            "a replayed apply must reach an identical end state"
        );
    }

    /// A write that carries NO old image must still replace the indexed row.
    ///
    /// This is a real production path, not a contrived one: `put_item` only reads the
    /// old image when a condition, a stream, `ReturnValues`, or a GSI needs it, so a
    /// table whose only index is a vector index writes with `old_item = None` on the
    /// common path. The apply therefore cannot rely on the old image to find the row
    /// to displace, and keys the delete off `old_item.or(new_item)` because the base
    /// key is immutable.
    ///
    /// This is the test that guards the invariant behind the plain `INSERT` in
    /// `insert_vector_row`. Making the delete conditional on `old_item` fails it: the
    /// second insert collides on the primary key, the savepoint drops the row, and the
    /// STALE payload survives. That failure is silent in production, which is why it
    /// is worth a dedicated test rather than leaving it to the idempotency case above.
    #[tokio::test]
    async fn a_write_with_no_old_image_still_replaces_the_indexed_row() {
        let _serial = super::DRAIN_TEST_LOCK.lock().await;
        let (engine, table_id) = table_with_vector_index().await;

        write(&engine, &table_id, None, Some(&item("a", 1)), 60_000).await;
        make_all_rows_due(&engine).await;
        process_gsi_batch(&engine).await.expect("first apply");
        assert!(
            indexed_rows(&engine, &table_id).await[0].contains("\"1\""),
            "precondition: the first generation is indexed"
        );

        // The second write supplies no old image, exactly as put_item does when
        // nothing else needs it.
        write(&engine, &table_id, None, Some(&item("a", 2)), 60_000).await;
        make_all_rows_due(&engine).await;
        process_gsi_batch(&engine).await.expect("second apply");

        let rows = indexed_rows(&engine, &table_id).await;
        assert_eq!(rows.len(), 1, "one base item indexes to one row: {rows:?}");
        assert!(
            rows[0].contains("\"2\""),
            "the newer write must replace the row even with no old image, found: {}",
            rows[0]
        );
    }
}
