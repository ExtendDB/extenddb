// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Point-in-time recovery: the item-history store a backend keeps for a table
//! whose recovery is enabled, so a restore can rebuild the table's state at a
//! chosen time.
//!
//! A backend records one history row for every committed mutation of such a
//! table, inside the write's own transaction, so a mutation and its row commit
//! together. That write is internal to each backend's data path. This trait is
//! the read and retention side the engine calls.
//!
//! Measured on the live service: a restore to a time T holds the latest image
//! of each key committed at or before T, and keys deleted by T are absent. The
//! service lands a few seconds before T; the store reconstructs exactly at the
//! time it is given, and the engine decides which time to pass.

#[cfg(feature = "test-hooks")]
pub mod conformance;

use std::time::{SystemTime, UNIX_EPOCH};

use extenddb_core::types::{Item, TableKeyInfo};
use futures::future::BoxFuture;

use crate::error::StorageError;

/// One page of a reconstruct. `next` is the cursor for the following page, or
/// `None` after the last.
#[derive(Debug, Default)]
pub struct ReconstructPage {
    /// The items of this page, in history order.
    pub items: Vec<Item>,
    /// Pass as `after` to read the next page.
    pub next: Option<i64>,
}

/// The retention a fresh enable takes when the request gives none (measured).
pub const DEFAULT_RECOVERY_PERIOD_DAYS: u8 = 35;

/// How long a reconstruct keeps the history it reads from pruning after it
/// reads a page. A page read later than this after the previous one fails
/// with [`StorageError::SnapshotExpired`] when a prune has passed its time
/// meanwhile.
pub const RECONSTRUCT_PIN_LEASE: std::time::Duration = std::time::Duration::from_secs(900);

/// A table's recovery setting: present while recovery is enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PitrState {
    /// Retention, 1 to 35 days.
    pub recovery_period_days: u8,
    /// When recovery was last enabled.
    pub enabled_at: SystemTime,
    /// The cutoff of the latest prune that removed a row: the history holds
    /// every state from it on, and may not hold an earlier one. The Unix
    /// epoch until a prune removes a row.
    pub pruned_to: SystemTime,
}

impl PitrState {
    /// The state of a fresh enable: `recovery_period_days` of retention from
    /// `enabled_at`, nothing pruned.
    #[must_use]
    pub const fn new(recovery_period_days: u8, enabled_at: SystemTime) -> Self {
        Self {
            recovery_period_days,
            enabled_at,
            pruned_to: UNIX_EPOCH,
        }
    }
}

/// The per-backend item-history store.
pub trait PitrCaptureStore: Send + Sync {
    /// The table's recovery setting, `None` while recovery is disabled.
    fn pitr_state<'a>(
        &'a self,
        table_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<PitrState>, StorageError>>;

    /// Enable recovery on the table `key_info` names, as the service does
    /// (measured). On a table without recovery: record the state with
    /// `period` or [`DEFAULT_RECOVERY_PERIOD_DAYS`] and `at` as the enable
    /// instant, and record every item the table holds as a history row at
    /// `at`, the baseline a restore starts from. On a table with recovery:
    /// change only the period, and only when `period` is given. No write is
    /// left out of both the baseline and the history: the backend excludes
    /// the table's writers while it takes the baseline. Two enables of one
    /// table at once both succeed, one after the other. Returns the state.
    /// A table that is gone is [`StorageError::TableNotFound`] with its name.
    fn enable_pitr<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        period: Option<u8>,
        at: SystemTime,
    ) -> BoxFuture<'a, Result<PitrState, StorageError>>;

    /// Every table with recovery enabled and its setting, for the retention
    /// sweep.
    fn pitr_tables(&self) -> BoxFuture<'_, Result<Vec<(String, PitrState)>, StorageError>>;

    /// Remove at most `limit` history rows, and then the setting, of tables
    /// the catalog no longer has. Returns the rows removed. A backend whose
    /// catalog removes them with the table returns 0.
    fn remove_orphaned_history(&self, limit: u32) -> BoxFuture<'_, Result<u64, StorageError>>;

    /// Disable recovery: remove the state and every history row of the
    /// table, excluding its writers while it does, so no row of a write in
    /// flight survives the disable. A disabled table is left as it is. A
    /// table that is gone is [`StorageError::TableNotFound`] with its name.
    fn disable_pitr<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// One page of the table's state at `at`: the latest image of each key
    /// committed at or before `at`, with keys whose latest record by then is a
    /// delete omitted. `after` is the previous page's `next`, `None` for the
    /// first page. At most `limit` items.
    ///
    /// Each page pins `at` for [`RECONSTRUCT_PIN_LEASE`]: no prune removes a
    /// row the state at `at` reads while the pin holds, so the pages of one
    /// reconstruct return one state even when a sweep runs between them.
    /// When the history no longer holds the state at `at` (a prune passed it
    /// while no pin held it, or recovery was disabled), the page is
    /// [`StorageError::SnapshotExpired`] and the reconstruct must start again.
    fn reconstruct_page<'a>(
        &'a self,
        table_id: &'a str,
        at: SystemTime,
        after: Option<i64>,
        limit: u32,
    ) -> BoxFuture<'a, Result<ReconstructPage, StorageError>>;

    /// Remove at most `limit` history rows that no reconstruct at or after
    /// `cutoff` reads: rows superseded by a later row committed at or before
    /// `cutoff`, and deletes that are the only row of their key at or before
    /// `cutoff`. The state at `cutoff` is unchanged. A reconstruct's pin
    /// earlier than `cutoff` lowers the cutoff to it. When a row goes, the
    /// state's `pruned_to` moves to the cutoff used. Returns the number of
    /// rows removed; fewer than `limit` means nothing removable is left.
    fn prune_before<'a>(
        &'a self,
        table_id: &'a str,
        cutoff: SystemTime,
        limit: u32,
    ) -> BoxFuture<'a, Result<u64, StorageError>>;
}

/// The time `micros` microseconds after the Unix epoch.
#[must_use]
pub fn from_unix_micros(micros: i64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_micros(u64::try_from(micros).unwrap_or(0))
}

/// Microseconds since the Unix epoch, the unit history rows store.
///
/// # Errors
///
/// A time before the epoch or past the `i64` range.
pub fn unix_micros(t: SystemTime) -> Result<i64, StorageError> {
    t.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_micros()).ok())
        .ok_or_else(|| StorageError::Internal(format!("time out of range: {t:?}")))
}

/// The floor of a table's restorable window at `now`: the latest of the
/// enable instant, `now` less the retention, and the cutoff of the latest
/// prune, so the floor is never earlier than a state the history holds after
/// the period is raised. A sweep prunes to it, so every state a restore
/// inside the window reads stays.
#[must_use]
pub fn retention_floor(state: &PitrState, now: SystemTime) -> SystemTime {
    let retention = std::time::Duration::from_secs(u64::from(state.recovery_period_days) * 86_400);
    let floor = now
        .checked_sub(retention)
        .map_or(state.enabled_at, |t| t.max(state.enabled_at));
    floor.max(state.pruned_to)
}

/// A stored retention, checked: a value outside 1 to 35 is a corrupt row,
/// reported rather than read as some other period.
///
/// # Errors
///
/// `days` is outside 1 to 35.
pub fn stored_period(table_id: &str, days: i64) -> Result<u8, StorageError> {
    u8::try_from(days)
        .ok()
        .filter(|d| (1..=35).contains(d))
        .ok_or_else(|| {
            StorageError::Internal(format!(
                "pitr_state of table {table_id} holds recovery_period_days {days}, outside 1 to 35"
            ))
        })
}

/// A page at `at_us` reads a state the history holds: recovery is enabled
/// and no prune has passed `at_us`. `pruned` is the table's `pruned_to`
/// in microseconds, `None` without a recovery setting.
///
/// # Errors
///
/// [`StorageError::SnapshotExpired`] when the state is no longer held.
pub fn reconstruct_held(
    table_id: &str,
    at_us: i64,
    pruned: Option<i64>,
) -> Result<(), StorageError> {
    match pruned {
        None => Err(StorageError::SnapshotExpired(format!(
            "point-in-time recovery of table {table_id} was disabled during the reconstruct"
        ))),
        Some(p) if at_us < p => Err(StorageError::SnapshotExpired(format!(
            "the history of table {table_id} was pruned past the reconstruct time"
        ))),
        Some(_) => Ok(()),
    }
}

/// The cutoff a prune uses: `cutoff_us`, or a pin that holds at `now_us`
/// when that is earlier. `pin` is the pinned time and its expiry, in
/// microseconds.
#[must_use]
pub fn pinned_cutoff(
    cutoff_us: i64,
    (pinned, expires): (Option<i64>, Option<i64>),
    now_us: i64,
) -> i64 {
    match (pinned, expires) {
        (Some(p), Some(e)) if e >= now_us => cutoff_us.min(p),
        _ => cutoff_us,
    }
}

/// What one retention sweep removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    /// History rows pruned below the floors.
    pub pruned: u64,
    /// History rows of deleted tables removed.
    pub orphaned: u64,
    /// Whether the deadline stopped the sweep before it finished.
    pub stopped_at_deadline: bool,
}

/// One retention sweep: every enabled table pruned to its floor at `now`, in
/// batches of `batch`, then the history of deleted tables, until done or
/// past `deadline`. A batch that removes fewer rows than `batch` ends that
/// table's work.
///
/// # Errors
///
/// A store read or delete fails.
pub async fn sweep(
    store: &dyn PitrCaptureStore,
    now: SystemTime,
    batch: u32,
    deadline: std::time::Instant,
) -> Result<SweepReport, StorageError> {
    let mut report = SweepReport::default();
    for (table_id, state) in store.pitr_tables().await? {
        let floor = retention_floor(&state, now);
        loop {
            if std::time::Instant::now() >= deadline {
                report.stopped_at_deadline = true;
                return Ok(report);
            }
            let n = store.prune_before(&table_id, floor, batch).await?;
            report.pruned += n;
            if n < u64::from(batch) {
                break;
            }
        }
    }
    loop {
        if std::time::Instant::now() >= deadline {
            report.stopped_at_deadline = true;
            return Ok(report);
        }
        let n = store.remove_orphaned_history(batch).await?;
        report.orphaned += n;
        if n < u64::from(batch) {
            return Ok(report);
        }
    }
}

/// How often the retention sweep runs, how many rows one delete removes, and
/// how long one sweep may run.
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);
const SWEEP_BATCH: u32 = 5_000;
const SWEEP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(300);

/// The background retention sweep a backend with a store spawns: [`sweep`]
/// once an hour, under a deadline, until shutdown. A sweep the deadline
/// stopped resumes at the next run.
pub async fn retention_worker<S: PitrCaptureStore>(
    store: std::sync::Arc<S>,
    metrics: std::sync::Arc<extenddb_core::metrics::MetricsCollector>,
    token: crate::hooks::CancellationToken,
) {
    use extenddb_core::metrics::QuerySource;
    while crate::hooks::sleep_or_shutdown(&token, SWEEP_INTERVAL).await {
        let start = std::time::Instant::now();
        match sweep(
            &*store,
            SystemTime::now(),
            SWEEP_BATCH,
            start + SWEEP_DEADLINE,
        )
        .await
        {
            Ok(report) => {
                if report.pruned + report.orphaned > 0 || report.stopped_at_deadline {
                    tracing::info!(
                        pruned = report.pruned,
                        orphaned = report.orphaned,
                        stopped_at_deadline = report.stopped_at_deadline,
                        "point-in-time recovery retention sweep"
                    );
                }
                #[allow(clippy::cast_precision_loss)]
                metrics.record_worker_success(
                    QuerySource::PitrRetention,
                    start.elapsed().as_micros() as f64,
                );
            }
            Err(e) => {
                tracing::error!("point-in-time recovery retention sweep failed: {e}");
                metrics.record_worker_error(QuerySource::PitrRetention);
            }
        }
    }
}

#[cfg(test)]
#[path = "pitr/worker_tests.rs"]
mod worker_tests;
