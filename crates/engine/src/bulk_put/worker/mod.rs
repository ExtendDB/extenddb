// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bulk operation worker and the retention worker.
//!
//! [`bulk_operation_worker`] claims runnable operations from storage on a
//! fixed one second tick and drives each through [`runner::run`] in its own
//! task, bounded by `bulk_operation_worker_concurrency` slots overall and
//! `bulk_operation_worker_per_account` slots per account. The loops live in
//! this crate because the runner they drive is private to it; the server
//! spawns both functions next to its own workers with the same
//! cancellation-token pattern, and both stop at their next tick after the
//! token fires.
//!
//! [`bulk_operation_retention_worker`] purges terminal operation records
//! older than `bulk_operation_retention_days` once at startup and then
//! daily. Each pass lists the purgeable operations, deletes each one's
//! row-outcome ledger rows (which live in the data database, out of the
//! catalog purge's reach), and purges only the operations whose ledger
//! delete succeeded, so ledger rows die while their operation record
//! still exists and a crash at any point re-lists the same operations on
//! the next pass.
//!
//! A backend that answers `StorageError::Validation` from `claim_runnable`
//! or `list_purgeable_before` does not carry bulk operations; each loop
//! logs that once at info and stops rather than logging every tick.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use extenddb_core::limits::LimitsConfig;
use extenddb_core::metrics::{MetricsCollector, QuerySource};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BulkOperationRecord, BulkOperationStore, CancellationToken, FinishState, StorageEngine,
    Terminal, sleep_or_shutdown as tick,
};

use crate::bulk_put::authorize::BulkAuthorizer;
use crate::bulk_put::pacer::WritePacer;
use crate::bulk_put::runner::{self, RunnerConfig, RunnerDeps, RunnerError};
use crate::bulk_put::source::{BulkSink, BulkSource};
use crate::bulk_put::store::BulkStore;

mod claim;
mod retention;

#[cfg(test)]
mod tests;

use claim::{cancel_only_loop, claim_loop};

pub use retention::bulk_operation_retention_worker;

/// The claim tick. Fixed rather than configured: the lease length is the
/// tunable that matters, and a saturated worker is paced by its slots, not
/// by the tick.
const CLAIM_INTERVAL: Duration = Duration::from_secs(1);

/// Milliseconds since the Unix epoch, saturating into `i64`. The terminal
/// transition records this as the operation's end time.
fn epoch_millis() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// Microseconds elapsed since `start`, for the worker cycle latency metric.
#[allow(clippy::cast_precision_loss)] // a cycle's microseconds fit f64 exactly
fn elapsed_us(start: Instant) -> f64 {
    start.elapsed().as_micros() as f64
}

/// Worker tuning, read once from [`LimitsConfig`] at spawn.
pub(crate) struct BulkWorkerConfig {
    /// Operations running at once in this process.
    pub concurrency: usize,
    /// Of those, at most this many per account.
    pub per_account: usize,
    /// Lease length passed to every claim and checkpoint.
    pub lease: Duration,
    /// Rows per checkpoint.
    pub batch_size: usize,
    /// Rows per storage transaction.
    pub write_batch_rows: usize,
    /// Longest accepted input line in bytes.
    pub max_line_bytes: usize,
    /// Physical lines one operation may consume.
    pub max_item_count: u64,
    /// Report records kept per operation.
    pub max_report_records: u64,
    /// Serialized bytes of report records kept per operation.
    pub max_report_bytes: u64,
    /// Process-wide write ceiling in write request units per second; `0`
    /// means unbounded.
    pub write_units_per_second: u64,
}

/// `value`, floored at 1, with a warn log naming the key when the floor
/// applies. The keys' documented meanings assume positive values: a zero
/// lease makes every claim instantly expired, zero concurrency or batch
/// size admits no work or never reaches a checkpoint boundary, and a zero
/// per-account share still admits one run, which the figure then misstates.
fn floored<T>(value: T, key: &str) -> T
where
    T: Copy + Ord + From<u8> + std::fmt::Display,
{
    let one = T::from(1u8);
    if value >= one {
        return value;
    }
    tracing::warn!(key, configured = %value, "bulk worker setting floored at 1");
    one
}

/// Upper bound for `bulk_put_batch_size`. The byte-bounded report flush
/// (`PENDING_FLUSH_BYTES`) already caps a runner's heap, so this bound is
/// about checkpoint cadence rather than memory: a batch far larger than
/// this only widens the window of rows a crash re-does, with no throughput
/// gain (the per-row cost is a storage round trip, not the checkpoint). A
/// value above the cap is clamped with a warn naming the key.
const MAX_BULK_PUT_BATCH_SIZE: usize = 10_000;

/// Upper bound for `bulk_put_write_batch_rows`. A write batch holds its
/// row locks, and on SQLite the process write lock, for one transaction;
/// an online write to a key inside the batch waits for the batch's commit.
/// The cap bounds that wait and the staged items' memory (at most this
/// many items of at most the item size limit). A value above it is clamped
/// with a warn naming the key.
const MAX_BULK_PUT_WRITE_BATCH_ROWS: usize = 100;

/// `value` clamped to `max`, with a warn log naming the key when the clamp
/// applies.
fn capped(value: usize, max: usize, key: &str) -> usize {
    if value <= max {
        return value;
    }
    tracing::warn!(key, configured = value, max, "bulk worker setting clamped");
    max
}

impl BulkWorkerConfig {
    /// Read the worker tuning out of the limits configuration.
    /// `bulk_operation_lease_seconds`, `bulk_operation_worker_concurrency`,
    /// `bulk_operation_worker_per_account`, and `bulk_put_batch_size` are
    /// floored at 1.
    #[must_use]
    pub(crate) fn from_limits(limits: &LimitsConfig) -> Self {
        let batch_size = capped(
            floored(limits.bulk_put_batch_size, "bulk_put_batch_size"),
            MAX_BULK_PUT_BATCH_SIZE,
            "bulk_put_batch_size",
        );
        Self {
            concurrency: floored(
                limits.bulk_operation_worker_concurrency,
                "bulk_operation_worker_concurrency",
            ),
            per_account: floored(
                limits.bulk_operation_worker_per_account,
                "bulk_operation_worker_per_account",
            ),
            lease: Duration::from_secs(floored(
                limits.bulk_operation_lease_seconds,
                "bulk_operation_lease_seconds",
            )),
            batch_size,
            // The write batch is flushed at every checkpoint, so it can never
            // hold more rows than the checkpoint batch: a value above
            // `bulk_put_batch_size` would silently behave as that value. The
            // clamp says so at startup, naming both keys.
            write_batch_rows: capped(
                capped(
                    floored(
                        limits.bulk_put_write_batch_rows,
                        "bulk_put_write_batch_rows",
                    ),
                    MAX_BULK_PUT_WRITE_BATCH_ROWS,
                    "bulk_put_write_batch_rows",
                ),
                batch_size,
                "bulk_put_write_batch_rows (bounded by bulk_put_batch_size)",
            ),
            max_line_bytes: limits.max_bulk_line_bytes,
            max_item_count: limits.max_bulk_item_count,
            max_report_records: limits.max_bulk_report_records,
            max_report_bytes: limits.max_bulk_report_bytes,
            write_units_per_second: limits.bulk_put_write_units_per_second,
        }
    }
}

/// The lease owner prefix for this process: host name, pid, and a random
/// suffix, the same shape the vector index maintenance owner label uses.
/// The suffix keeps a recycled pid on the same host from colliding with
/// the leases its predecessor left behind. Each claim appends a sequence
/// number to this prefix, so no two claims ever share an owner string.
fn process_owner_id() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|h| h.trim().to_owned())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    format!(
        "{host}-{}-{:08x}",
        std::process::id(),
        rand::random::<u32>()
    )
}

/// Claim and run bulk operations until `token` is cancelled.
///
/// With no `[bulk]` store configured the worker idles: it keeps ticking
/// so it stops on the token like every other worker, but claims nothing,
/// and says so once at info. When the token fires, the worker waits one
/// second for its in-flight runs to checkpoint and release their leases,
/// then aborts each run still running and awaits it, so no run outlives
/// the worker. An aborted run keeps its lease until the lease lapses;
/// every checkpoint is transactional and lease-conditional, so a later
/// claim resumes the operation from its last committed checkpoint.
///
/// `authorizer` evaluates the S3 actions each run performs as its
/// operation's caller; the server passes `None` in developer mode, where no
/// such check runs.
#[allow(clippy::too_many_arguments)] // the worker's dependencies, passed once at startup
pub async fn bulk_operation_worker(
    storage: Arc<dyn StorageEngine>,
    limits: Arc<LimitsConfig>,
    region: Arc<str>,
    bulk: Option<Arc<BulkStore>>,
    authorizer: Option<Arc<dyn BulkAuthorizer>>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    bulk_operation_worker_with_interval(
        storage,
        limits,
        region,
        bulk,
        authorizer,
        metrics,
        token,
        CLAIM_INTERVAL,
    )
    .await;
}

#[allow(clippy::too_many_arguments)] // as `bulk_operation_worker`, plus the claim interval
pub(crate) async fn bulk_operation_worker_with_interval(
    storage: Arc<dyn StorageEngine>,
    limits: Arc<LimitsConfig>,
    region: Arc<str>,
    bulk: Option<Arc<BulkStore>>,
    authorizer: Option<Arc<dyn BulkAuthorizer>>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
    interval: Duration,
) {
    let Some(bulk) = bulk else {
        // No [bulk] store: there is no input to read, so no IN_PROGRESS
        // operation can make progress. A cancel reads no input, so the
        // worker still drains CANCELLING rows to CANCELLED through the
        // store's terminal transition. Without it a customer's cancel after
        // an operator removes the section sits CANCELLING forever, holds its
        // table's one-in-flight slot, and never ages into retention. The
        // startup warning naming the stranded count is logged by the server
        // (serve.rs), which can enumerate accounts; the worker drives the
        // cancellations.
        tracing::info!(
            "Bulk operation worker: no [bulk] store configured, so only cancellations run; \
             other operations do not progress until a store is configured"
        );
        let owner = process_owner_id();
        let lease = Duration::from_secs(floored(
            limits.bulk_operation_lease_seconds,
            "bulk_operation_lease_seconds",
        ));
        cancel_only_loop(storage, owner, lease, interval, token).await;
        return;
    };
    let config = BulkWorkerConfig::from_limits(&limits);
    let owner = process_owner_id();
    let source: Arc<dyn BulkSource> = Arc::clone(&bulk.source);
    let sink: Arc<dyn BulkSink> = Arc::clone(&bulk.sink);
    let store_kind = crate::bulk_put::run_context::store_kind(&bulk);
    let store: Arc<dyn BulkOperationStore> = storage.clone();
    // One pacer for the process: every runner takes its write units from
    // the same bucket, so the ceiling holds across concurrent operations.
    let pacer = Arc::new(WritePacer::new(config.write_units_per_second));
    if pacer.is_bounded() {
        tracing::info!(
            write_units_per_second = config.write_units_per_second,
            "bulk put writes paced per process"
        );
    }
    let shutdown = token.clone();
    claim_loop(
        store,
        config.concurrency,
        config.per_account,
        config.lease,
        interval,
        owner,
        Arc::clone(&metrics),
        token,
        // One `RunnerDeps` per run, because the owner is per claim: the
        // runner's checkpoints, `finish`, and the lease re-read before a
        // report rename must carry the owner string the claim wrote.
        move |record, claim_owner| {
            let deps = RunnerDeps {
                storage: Arc::clone(&storage),
                source: Arc::clone(&source),
                sink: Arc::clone(&sink),
                limits: Arc::clone(&limits),
                region: Arc::clone(&region),
                owner: claim_owner,
                config: RunnerConfig {
                    batch_size: config.batch_size,
                    write_batch_rows: config.write_batch_rows,
                    max_line_bytes: config.max_line_bytes,
                    max_item_count: config.max_item_count,
                    max_report_records: config.max_report_records,
                    max_report_bytes: config.max_report_bytes,
                    lease: config.lease,
                },
                metrics: Arc::clone(&metrics),
                pacer: Arc::clone(&pacer),
                store_kind,
                authorizer: authorizer.clone(),
                shutdown: shutdown.clone(),
            };
            async move { runner::run(&deps, record).await }
        },
    )
    .await;
}
