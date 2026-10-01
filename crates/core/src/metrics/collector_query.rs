// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and drain methods for `MetricsCollector`.
//!
//! Split from `collector.rs` to keep both files under the 500-line limit.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::{Duration, Instant, SystemTime};

use super::accumulator::{Accumulator, DataPoint};
use super::collector::{MetricKey, MetricsCollector, window_cutoff};
use super::types::{
    Dimension, FlushBucket, MetricName, MetricSnapshot, MetricsQuery, OperationSegments,
    Percentiles, TimeWindow,
};

/// Accumulator for per-operation segment sums: (auth, authz, throttle, dispatch, response, total, count).
type SegmentAccum = (f64, f64, f64, f64, f64, f64, u64);

impl MetricsCollector {
    /// Query metrics and return snapshots.
    ///
    /// While a [`drain`](Self::drain) splits its points, the buffered points
    /// are out of the map and this query does not see them.
    #[must_use]
    pub fn query(&self, params: &MetricsQuery) -> Vec<MetricSnapshot> {
        let now = Instant::now();
        let window = params.window.unwrap_or(TimeWindow::Last5Minutes);

        let Ok(map) = self.data.read() else {
            return Vec::new();
        };

        let mut results = Vec::new();
        for (key, acc) in map.iter() {
            // Filter by table name if specified.
            if let Some(ref tn) = params.table_name
                && key.table_name.as_deref() != Some(tn.as_str())
            {
                continue;
            }
            // Filter by metric name if specified.
            if let Some(ref m) = params.metric
                && key.metric != *m
            {
                continue;
            }

            let Some(snap) = acc.snapshot(window, now) else {
                continue;
            };

            let mut dimensions = Vec::new();
            if let Some(ref tn) = key.table_name {
                dimensions.push(Dimension::TableName(tn.clone()));
            }
            if let Some(ref idx) = key.index_name {
                dimensions.push(Dimension::GlobalSecondaryIndexName(idx.clone()));
            }
            if let Some(ref op) = key.operation {
                dimensions.push(Dimension::Operation(op.clone()));
            }

            let percentiles = if matches!(
                key.metric,
                MetricName::SuccessfulRequestLatency
                    | MetricName::StorageQueryLatency
                    | MetricName::PoolAcquireLatency
                    | MetricName::WorkerCycleLatency
            ) {
                let mut vals = snap.values;
                vals.sort_by(|a: &f64, b: &f64| {
                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                });
                Some(compute_percentiles(&vals))
            } else {
                None
            };

            results.push(MetricSnapshot {
                metric: key.metric,
                dimensions,
                window,
                sum: snap.sum,
                count: snap.count,
                min: snap.min,
                max: snap.max,
                percentiles,
            });
        }
        results
    }

    /// Drain data points older than `age` and aggregate them into 1-minute
    /// `FlushBucket`s for DB persistence. Points newer than `age` are kept
    /// in memory for the next flush cycle.
    ///
    /// The drain takes the write lock twice: to swap the map out, and to put
    /// the younger points back ahead of the points recorded meanwhile. Neither
    /// step touches the points held, so `record_*` calls wait a bounded time
    /// however many points are buffered. Between the two steps an in-memory
    /// `query` does not see the buffered points; the server reads metrics
    /// from its store. Concurrent drains run one at a time. The split and the
    /// aggregation are CPU-bound: async callers should run the drain on a
    /// blocking thread.
    pub fn drain(&self, age: Duration) -> Vec<FlushBucket> {
        let cutoff = Instant::now().checked_sub(age).unwrap_or(Instant::now());
        aggregate(self.drain_points(cutoff, |_| {}).0)
    }

    /// The points of a drain, and the chunks its second locked step moved.
    /// `between` sees the map taken by the first locked step, before the
    /// split, without the write lock.
    pub(super) fn drain_points(
        &self,
        cutoff: Instant,
        between: impl FnOnce(&PointMap),
    ) -> (Drained, usize) {
        let _serial = self
            .drain_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut held = {
            let Ok(mut map) = self.data.write() else {
                return (Vec::new(), 0);
            };
            std::mem::take(&mut *map)
        };
        between(&held);
        let drained = take_expired(&mut held, cutoff);
        let Ok(mut map) = self.data.write() else {
            return (drained, 0);
        };
        let moved = reattach(&mut map, held);
        (drained, moved)
    }

    /// Query average latency segments for the console deep-dive.
    ///
    /// Returns per-operation averages over the given time window.
    #[must_use]
    pub fn query_segments(&self, window: TimeWindow) -> Vec<OperationSegments> {
        let now = Instant::now();
        let cutoff = window_cutoff(window, now);
        let Ok(vec) = self.segments.read() else {
            return Vec::new();
        };
        let mut by_op: HashMap<String, SegmentAccum> = HashMap::new();
        for p in vec.iter() {
            if let Some(c) = cutoff
                && p.timestamp < c
            {
                continue;
            }
            let e = by_op.entry(p.operation.clone()).or_default();
            e.0 += p.segments.auth_us;
            e.1 += p.segments.authz_us;
            e.2 += p.segments.throttle_us;
            e.3 += p.segments.dispatch_us;
            e.4 += p.segments.response_us;
            e.5 += p.segments.total_us;
            e.6 += 1;
        }
        by_op
            .into_iter()
            .map(
                |(op, (auth, authz, throttle, dispatch, response, total, count))| {
                    #[allow(clippy::cast_precision_loss)]
                    let c = count as f64;
                    OperationSegments {
                        operation: op,
                        count,
                        avg: super::types::LatencySegments {
                            auth_us: auth / c,
                            authz_us: authz / c,
                            throttle_us: throttle / c,
                            dispatch_us: dispatch / c,
                            response_us: response / c,
                            total_us: total / c,
                        },
                    }
                },
            )
            .collect()
    }
}

/// Compute percentiles from a pre-sorted slice of values.
///
/// # Panics
/// Panics if `sorted` is empty. Callers must ensure non-empty input.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn compute_percentiles(sorted: &[f64]) -> Percentiles {
    let len = sorted.len();
    let pct = |p: f64| -> f64 {
        let idx = ((p / 100.0) * (len as f64 - 1.0)).round() as usize;
        sorted[idx.min(len - 1)]
    };
    Percentiles {
        p50: pct(50.0),
        p90: pct(90.0),
        p95: pct(95.0),
        p99: pct(99.0),
    }
}

/// Truncate a `SystemTime` to the start of its minute (seconds = 0).
fn truncate_to_minute(t: SystemTime) -> i64 {
    let secs = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    #[allow(clippy::cast_possible_wrap)]
    let secs_i64 = secs as i64;
    secs_i64 - (secs_i64 % 60)
}

/// Drained points per key, in record order.
type Drained = Vec<(MetricKey, Vec<Vec<DataPoint>>)>;

pub(super) type PointMap = HashMap<MetricKey, Accumulator>;

/// Removes every point at or before `cutoff` from `held`, which the caller
/// has taken out of the collector, and drops the keys left empty.
pub(super) fn take_expired(held: &mut PointMap, cutoff: Instant) -> Drained {
    let mut drained = Vec::new();
    for (key, acc) in held.iter_mut() {
        let old = acc.take_expired(cutoff);
        if !old.is_empty() {
            drained.push((key.clone(), old));
        }
    }
    held.retain(|_, acc| !acc.is_empty());
    drained
}

/// The second locked step of a drain: makes `young` the map again and appends
/// the points recorded since the first step. Returns the chunks it moved, which
/// are only the chunks of those new points.
pub(super) fn reattach(map: &mut PointMap, young: PointMap) -> usize {
    let recorded = std::mem::replace(map, young);
    let mut moved = 0;
    for (key, acc) in recorded {
        moved += acc.chunk_count();
        match map.entry(key) {
            Entry::Occupied(mut e) => e.get_mut().append(acc),
            Entry::Vacant(e) => {
                e.insert(acc);
            }
        }
    }
    moved
}

/// Sums drained points into 1-minute buckets per key, in record order.
pub(super) fn aggregate(drained: Drained) -> Vec<FlushBucket> {
    let mut out = Vec::new();
    for (key, chunks) in drained {
        let mut by_minute: HashMap<i64, (f64, u64, f64, f64)> = HashMap::new();
        for dp in chunks.iter().flatten() {
            let e = by_minute
                .entry(truncate_to_minute(dp.wall_time))
                .or_insert((0.0, 0, f64::INFINITY, f64::NEG_INFINITY));
            e.0 += dp.value;
            e.1 += 1;
            e.2 = e.2.min(dp.value);
            e.3 = e.3.max(dp.value);
        }
        out.extend(
            by_minute
                .into_iter()
                .map(|(minute, (sum, count, min, max))| FlushBucket {
                    bucket: SystemTime::UNIX_EPOCH
                        + Duration::from_secs(u64::try_from(minute).unwrap_or(0)),
                    metric: key.metric,
                    table_name: key.table_name.clone().unwrap_or_default(),
                    index_name: key.index_name.clone().unwrap_or_default(),
                    operation: key.operation.clone().unwrap_or_default(),
                    sum,
                    count,
                    min,
                    max,
                }),
        );
    }
    out
}

#[cfg(test)]
#[path = "drain_tests.rs"]
mod tests;
