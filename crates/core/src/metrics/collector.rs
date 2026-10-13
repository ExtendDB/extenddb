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
    pub fn record(
        &self,
        metric: MetricName,
        value: f64,
        table_name: Option<&str>,
        index_name: Option<&str>,
        operation: Option<&str>,
    ) {
        let key = MetricKey {
            metric,
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

    /// Record a latency observation in microseconds.
    pub fn record_latency(&self, table_name: Option<&str>, operation: &str, latency_us: f64) {
        self.record(
            MetricName::SuccessfulRequestLatency,
            latency_us,
            table_name,
            None,
            Some(operation),
        );
    }

    /// Record a successful request's consumed read capacity.
    pub fn record_read_capacity(&self, table_name: &str, operation: &str, units: f64) {
        self.record(
            MetricName::ConsumedReadCapacityUnits,
            units,
            Some(table_name),
            None,
            Some(operation),
        );
    }

    /// Record a successful request's consumed write capacity.
    pub fn record_write_capacity(&self, table_name: &str, operation: &str, units: f64) {
        self.record(
            MetricName::ConsumedWriteCapacityUnits,
            units,
            Some(table_name),
            None,
            Some(operation),
        );
    }

    /// Record a user error (4xx).
    pub fn record_user_error(&self, table_name: Option<&str>, operation: &str) {
        self.record(
            MetricName::UserErrors,
            1.0,
            table_name,
            None,
            Some(operation),
        );
    }

    /// Record a system error (5xx).
    pub fn record_system_error(&self, table_name: Option<&str>, operation: &str) {
        self.record(
            MetricName::SystemErrors,
            1.0,
            table_name,
            None,
            Some(operation),
        );
    }

    /// Record a conditional check failure.
    pub fn record_conditional_check_failure(&self, table_name: Option<&str>, operation: &str) {
        self.record(
            MetricName::ConditionalCheckFailedRequests,
            1.0,
            table_name,
            None,
            Some(operation),
        );
    }

    /// Record a transaction conflict.
    pub fn record_transaction_conflict(&self, operation: &str) {
        self.record(
            MetricName::TransactionConflict,
            1.0,
            None,
            None,
            Some(operation),
        );
    }

    /// Record returned item count from a query/scan.
    #[allow(clippy::cast_precision_loss)] // count values are small enough for f64
    pub fn record_returned_items(&self, table_name: &str, operation: &str, count: u64) {
        self.record(
            MetricName::ReturnedItemCount,
            count as f64,
            Some(table_name),
            None,
            Some(operation),
        );
    }

    /// Record returned bytes from a query/scan.
    #[allow(clippy::cast_precision_loss)] // byte counts within 1 MB page limit
    pub fn record_returned_bytes(&self, table_name: &str, operation: &str, bytes: u64) {
        self.record(
            MetricName::ReturnedBytes,
            bytes as f64,
            Some(table_name),
            None,
            Some(operation),
        );
    }

    /// Record a TTL deletion.
    pub fn record_ttl_deletion(&self, table_name: &str) {
        self.record(
            MetricName::TimeToLiveDeletedItemCount,
            1.0,
            Some(table_name),
            None,
            None,
        );
    }

    /// Record TTL deletion staleness (seconds past expiry).
    pub fn record_ttl_staleness(&self, table_name: &str, staleness_secs: f64) {
        self.record(
            MetricName::TtlDeletionStaleness,
            staleness_secs,
            Some(table_name),
            None,
            None,
        );
    }

    /// P120c: Record an HTTP request (dimensions: operation).
    pub fn record_request_count(&self, operation: &str) {
        self.record(MetricName::RequestCount, 1.0, None, None, Some(operation));
    }

    /// P120c: Record a storage query execution (dimensions: source, category).
    pub fn record_storage_query(
        &self,
        source: super::types::QuerySource,
        category: super::types::QueryCategory,
        latency_us: f64,
    ) {
        let source_str = source.to_string();
        let category_str = category.to_string();
        // Count
        self.record(
            MetricName::StorageQueryCount,
            1.0,
            None,
            Some(&source_str),
            Some(&category_str),
        );
        // Latency
        self.record(
            MetricName::StorageQueryLatency,
            latency_us,
            None,
            Some(&source_str),
            Some(&category_str),
        );
    }

    /// P120d: Record pool connection gauge values.
    pub fn record_pool_state(&self, active: u32, idle: u32) {
        #[allow(clippy::cast_precision_loss)]
        {
            self.record(
                MetricName::PoolActiveConnections,
                f64::from(active),
                None,
                None,
                None,
            );
            self.record(
                MetricName::PoolIdleConnections,
                f64::from(idle),
                None,
                None,
                None,
            );
        }
    }

    /// P120d: Record pool acquire latency in microseconds.
    pub fn record_pool_acquire_latency(&self, latency_us: f64) {
        self.record(MetricName::PoolAcquireLatency, latency_us, None, None, None);
    }

    /// P120e: Record a worker's last successful cycle timestamp.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_worker_success(&self, source: super::types::QuerySource, latency_us: f64) {
        let worker_str = source.to_string();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.record(
            MetricName::WorkerLastSuccess,
            now_secs as f64,
            None,
            None,
            Some(&worker_str),
        );
        self.record(
            MetricName::WorkerCycleLatency,
            latency_us,
            None,
            None,
            Some(&worker_str),
        );
    }

    /// P120e: Record a worker error.
    pub fn record_worker_error(&self, source: super::types::QuerySource) {
        let worker_str = source.to_string();
        self.record(
            MetricName::WorkerErrorCount,
            1.0,
            None,
            None,
            Some(&worker_str),
        );
    }

    /// Record per-segment latency breakdown for a request.
    pub fn record_segments(
        &self,
        operation: &str,
        table_name: Option<&str>,
        segments: LatencySegments,
    ) {
        let point = SegmentPoint {
            operation: operation.to_owned(),
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
