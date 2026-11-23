// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! In-memory metrics collector using an `RwLock`-protected map for
//! per-key aggregation.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use super::accumulator::Accumulator;
use super::segments::SegmentStore;
use super::types::{LatencySegments, MetricName, TimeWindow};

/// Returns `None` for `AllTime` (no cutoff), or `Some(cutoff)` for bounded windows.
pub(super) fn window_cutoff(window: TimeWindow, now: Instant) -> Option<Instant> {
    let dur = match window {
        TimeWindow::LastMinute => Duration::from_secs(60),
        TimeWindow::Last5Minutes => Duration::from_secs(300),
        TimeWindow::LastHour => Duration::from_secs(3600),
        TimeWindow::LastDay => Duration::from_secs(86400),
        TimeWindow::AllTime => return None,
    };
    Some(now.checked_sub(dur).unwrap_or(now))
}

/// Composite key for metric storage.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MetricKey {
    pub(super) metric: MetricName,
    /// The account whose request or table the point belongs to; `None` for a
    /// metric of the whole deployment (workers, pools, backup jobs).
    pub(super) account_id: Option<String>,
    pub(super) table_name: Option<String>,
    pub(super) index_name: Option<String>,
    pub(super) operation: Option<String>,
}

/// Thread-safe in-memory metrics collector.
///
/// Uses `RwLock` for the map (writes are infrequent relative to reads in the
/// metrics endpoint). Individual `record_*` calls take a write lock briefly.
pub struct MetricsCollector {
    pub(super) data: RwLock<HashMap<MetricKey, Accumulator>>,
    /// Per-account, per-operation latency segment sums for the console
    /// deep-dive, bucketed by time so memory is bounded by the number of
    /// accounts and operations.
    pub(super) segments: RwLock<SegmentStore>,
    /// Serializes drains, so each one takes and keeps points in record order.
    /// `record` and `prune` never take it.
    pub(super) drain_lock: Mutex<()>,
}

impl MetricsCollector {
    /// Create a new empty collector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            segments: RwLock::new(SegmentStore::new(Instant::now())),
            drain_lock: Mutex::new(()),
        }
    }

    /// Record a counter metric (e.g., consumed capacity, error count).
    ///
    /// `account_id` is the account of the request or table the value belongs
    /// to, and `None` for a metric of the whole deployment. Points of two
    /// accounts never share a key, so two accounts' tables of the same name
    /// are kept apart.
    pub fn record(
        &self,
        metric: MetricName,
        value: f64,
        account_id: Option<&str>,
        table_name: Option<&str>,
        index_name: Option<&str>,
        operation: Option<&str>,
    ) {
        let key = MetricKey {
            metric,
            account_id: account_id.map(ToOwned::to_owned),
            table_name: table_name.map(ToOwned::to_owned),
            index_name: index_name.map(ToOwned::to_owned),
            operation: operation.map(ToOwned::to_owned),
        };
        self.record_point(key, value, Instant::now(), SystemTime::now());
    }

    pub(super) fn record_point(
        &self,
        key: MetricKey,
        value: f64,
        now: Instant,
        wall_time: SystemTime,
    ) {
        // Poisoned lock: silently skip rather than panic (async safety rule).
        let Ok(mut map) = self.data.write() else {
            return;
        };
        map.entry(key)
            .or_insert_with(Accumulator::new)
            .record(value, now, wall_time);
    }

    /// Record per-segment latency breakdown for a request.
    ///
    /// The breakdown is summed into the current time buckets of the account's
    /// operation, so the memory held is bounded by the ring lengths, whatever
    /// the request count. The account is a dimension because `query_segments`
    /// filters on it. The table is not: a per-table breakdown would multiply
    /// the bound by the table count, and the console reads per-operation
    /// averages.
    pub fn record_segments(&self, account_id: &str, operation: &str, segments: &LatencySegments) {
        self.record_segments_with(account_id, operation, segments, Instant::now);
    }

    /// `record_segments` with the clock it reads. The clock is read under the
    /// lock, as in `query_segments`, so a stored record is never ahead of a
    /// query that follows it; otherwise a writer that crosses a bucket
    /// boundary between its clock read and its lock could drop the oldest
    /// in-window bucket before a reader with an earlier clock reads it.
    pub(super) fn record_segments_with(
        &self,
        account_id: &str,
        operation: &str,
        segments: &LatencySegments,
        clock: impl FnOnce() -> Instant,
    ) {
        let Ok(mut store) = self.segments.write() else {
            return;
        };
        store.record(account_id, operation, segments, clock());
    }

    /// Prune data points older than 1 day.
    ///
    /// Segment buckets leave their ring as time advances; the prune drops the
    /// rings of an account's operation once its newest bucket is older than a
    /// day, so an account that stopped sending requests, or was deleted, frees
    /// its segment buckets after a day.
    pub fn prune(&self) {
        self.prune_at(Instant::now());
    }

    /// `prune` at the clock `now`, which may be read before the locks: a
    /// point or a segment recorded in between is newer than `now`, so the
    /// prune keeps it.
    pub(super) fn prune_at(&self, now: Instant) {
        let cutoff = now.checked_sub(Duration::from_secs(86400)).unwrap_or(now);
        let Ok(mut map) = self.data.write() else {
            return;
        };
        for acc in map.values_mut() {
            acc.prune(cutoff);
        }
        map.retain(|_, acc| !acc.is_empty());
        drop(map);
        if let Ok(mut store) = self.segments.write() {
            store.prune(now);
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "collector_tests.rs"]
mod tests;
