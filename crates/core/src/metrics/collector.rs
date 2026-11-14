// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! In-memory metrics collector using an `RwLock`-protected map for
//! per-key aggregation.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use super::accumulator::Accumulator;
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
    /// Per-operation latency segment breakdowns for the console deep-dive.
    pub(super) segments: RwLock<Vec<SegmentPoint>>,
    /// Serializes drains, so each one takes and keeps points in record order.
    /// `record` and `prune` never take it.
    pub(super) drain_lock: Mutex<()>,
}

/// A single latency segment data point with metadata.
#[derive(Debug, Clone)]
pub(super) struct SegmentPoint {
    pub(super) operation: String,
    /// The account of the request, which `query_segments` filters on.
    pub(super) account_id: String,
    #[allow(dead_code)] // TODO(cleanup): used when console adds table-scoped latency breakdown
    pub(super) table_name: Option<String>,
    pub(super) segments: LatencySegments,
    pub(super) timestamp: Instant,
}

impl MetricsCollector {
    /// Create a new empty collector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            segments: RwLock::new(Vec::with_capacity(256)),
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
    pub fn record_segments(
        &self,
        account_id: &str,
        operation: &str,
        table_name: Option<&str>,
        segments: LatencySegments,
    ) {
        let point = SegmentPoint {
            operation: operation.to_owned(),
            account_id: account_id.to_owned(),
            table_name: table_name.map(ToOwned::to_owned),
            segments,
            timestamp: Instant::now(),
        };
        let Ok(mut vec) = self.segments.write() else {
            return;
        };
        vec.push(point);
    }

    /// Prune data points older than 1 day.
    pub fn prune(&self) {
        let now = Instant::now();
        let cutoff = now.checked_sub(Duration::from_secs(86400)).unwrap_or(now);
        let Ok(mut map) = self.data.write() else {
            return;
        };
        for acc in map.values_mut() {
            acc.prune(cutoff);
        }
        map.retain(|_, acc| !acc.is_empty());
        drop(map);
        // Prune segment points too.
        if let Ok(mut segs) = self.segments.write() {
            segs.retain(|p| p.timestamp >= cutoff);
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
