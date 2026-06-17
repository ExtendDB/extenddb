// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query and drain methods for `MetricsCollector`.
//!
//! Split from `collector.rs` to keep both files under the 500-line limit.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};

use super::collector::{MetricsCollector, window_cutoff};
use super::types::{
    Dimension, MetricName, MetricSnapshot, MetricsQuery, OperationSegments, Percentiles, TimeWindow,
};

/// Accumulator for per-operation segment sums: (auth, authz, throttle, dispatch, response, total, count).
type SegmentAccum = (f64, f64, f64, f64, f64, f64, u64);

impl MetricsCollector {
    /// Query metrics and return snapshots.
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
    /// The write lock is held only for the partition step; aggregation into
    /// buckets happens outside the lock to avoid blocking `record_*` calls.
    pub fn drain(&self, age: Duration) -> Vec<super::types::FlushBucket> {
        let cutoff = Instant::now().checked_sub(age).unwrap_or(Instant::now());

        // Phase 1: under write lock, partition old points out and collect them.
        let drained: Vec<(
            super::collector::MetricKey,
            Vec<super::collector::DataPoint>,
        )> = {
            let Ok(mut map) = self.data.write() else {
                return Vec::new();
            };
            let mut result = Vec::new();
            for (key, acc) in map.iter_mut() {
                let (old, new): (Vec<_>, Vec<_>) =
                    acc.points.drain(..).partition(|p| p.timestamp <= cutoff);
                acc.points = new;
                if !old.is_empty() {
                    result.push((key.clone(), old));
                }
            }
            map.retain(|_, acc| !acc.points.is_empty());
            result
        };

        // Phase 2: aggregate outside the lock.
        let mut buckets: HashMap<(super::collector::MetricKey, i64), (f64, u64, f64, f64)> =
            HashMap::new();
        for (key, points) in drained {
            for dp in points {
                let minute = truncate_to_minute(dp.wall_time);
                let e = buckets.entry((key.clone(), minute)).or_insert((
                    0.0,
                    0,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                ));
                e.0 += dp.value;
                e.1 += 1;
                e.2 = e.2.min(dp.value);
                e.3 = e.3.max(dp.value);
            }
        }

        buckets
            .into_iter()
            .map(
                |((key, minute), (sum, count, min, max))| super::types::FlushBucket {
                    bucket: SystemTime::UNIX_EPOCH
                        + Duration::from_secs(u64::try_from(minute).unwrap_or(0)),
                    metric: key.metric,
                    table_name: key.table_name.unwrap_or_default(),
                    index_name: key.index_name.unwrap_or_default(),
                    operation: key.operation.unwrap_or_default(),
                    sum,
                    count,
                    min,
                    max,
                },
            )
            .collect()
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
