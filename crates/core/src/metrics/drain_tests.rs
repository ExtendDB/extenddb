// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the drain: what it moves under the write lock, and that it drains
//! and keeps exactly the points a plain partition by cutoff would.

use super::*;
use crate::metrics::accumulator::CHUNK_CAP;
use crate::metrics::types::{MetricsQuery, TimeWindow};

/// Small deterministic generator (xorshift64) for timestamp jitter.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// One recorded point, as the reference model sees it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Pt {
    value: f64,
    timestamp: Instant,
    wall_time: SystemTime,
}

impl From<&DataPoint> for Pt {
    fn from(p: &DataPoint) -> Self {
        Self {
            value: p.value,
            timestamp: p.timestamp,
            wall_time: p.wall_time,
        }
    }
}

fn keys(n: usize) -> Vec<MetricKey> {
    (0..n)
        .map(|i| MetricKey {
            metric: MetricName::SuccessfulRequestLatency,
            table_name: Some(format!("table-{i}")),
            index_name: None,
            operation: Some("GetItem".to_owned()),
        })
        .collect()
}

/// Records `per_key` points for every key, interleaved across keys like the
/// request path does. Point `i` is at `base + i` microseconds, moved by up to
/// `jitter_us` either way, and its wall time advances 1 ms per point.
/// Returns the points per key in record order.
fn fill(
    c: &MetricsCollector,
    keys: &[MetricKey],
    per_key: usize,
    base: Instant,
    jitter_us: u64,
    rng: &mut Rng,
) -> Vec<Vec<Pt>> {
    let wall0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
    let mut recorded = vec![Vec::with_capacity(per_key); keys.len()];
    for i in 0..per_key {
        for (k, key) in keys.iter().enumerate() {
            let shift = if jitter_us == 0 {
                0
            } else {
                rng.next() % (2 * jitter_us + 1)
            };
            let micros = (i as u64 + jitter_us).saturating_sub(shift);
            let p = Pt {
                value: (i * keys.len() + k) as f64,
                timestamp: base + Duration::from_micros(micros),
                wall_time: wall0 + Duration::from_millis(i as u64),
            };
            c.record_point(key.clone(), p.value, p.timestamp, p.wall_time);
            recorded[k].push(p);
        }
    }
    recorded
}

fn drain_at(c: &MetricsCollector, cutoff: Instant) -> Drained {
    c.drain_points(cutoff, |_| {}, |_| {}).0
}

fn kept(c: &MetricsCollector, key: &MetricKey) -> Vec<Pt> {
    let map = c.data.read().expect("lock");
    map.get(key)
        .map(|acc| acc.points().map(Pt::from).collect())
        .unwrap_or_default()
}

fn drained_of(drained: &Drained, key: &MetricKey) -> Vec<Pt> {
    drained
        .iter()
        .filter(|(k, _)| k == key)
        .flat_map(|(_, chunks)| chunks.iter().flatten().map(Pt::from))
        .collect()
}

/// Under the lock a drain moves no buffered point: the first step swaps the
/// map out and releases the lock before the split, the lock stays free and the
/// drain mutex stays held until the split is done, and the second step moves
/// back only the chunks recorded in between, the same count with 100 times
/// more points held. `moved` is reported by the code under test, so this
/// proves the structure of the second step, not its time.
#[test]
fn drain_moves_only_newly_recorded_chunks_under_the_lock() {
    let keys = keys(7);
    let mut moved_by_size = Vec::new();
    for per_key in [4_000, 400_000] {
        let c = MetricsCollector::new();
        let base = Instant::now();
        let recorded = fill(&c, &keys, per_key, base, 0, &mut Rng(1));
        let half = (per_key / 2) as u64;
        let late = base + Duration::from_secs(10);
        let fresh = MetricKey {
            operation: Some("PutItem".to_owned()),
            ..keys[0].clone()
        };
        let wall = SystemTime::UNIX_EPOCH;
        let (drained, moved) = c.drain_points(
            base + Duration::from_micros(half),
            |held| {
                // Every point is still in the taken map (the split has not
                // run) while the write lock is free and the collector's map is
                // empty.
                let unsplit: usize = held.values().map(|acc| acc.points().count()).sum();
                assert_eq!(
                    unsplit,
                    keys.len() * per_key,
                    "split ran before the write lock was released"
                );
                assert!(c.data.try_write().is_ok(), "lock held before the split");
                assert!(c.data.read().expect("lock").is_empty());
                assert!(c.drain_lock.try_lock().is_err(), "drains not serialized");
                for v in [1.0, 2.0] {
                    c.record_point(keys[0].clone(), -v, late, wall);
                }
                c.record_point(fresh.clone(), -4.0, late, wall);
            },
            |held| {
                // The split is done: only the younger points are left. The
                // write lock is still free and the drain mutex still held.
                let young: usize = held.values().map(|acc| acc.points().count()).sum();
                assert_eq!(young, keys.len() * (per_key - per_key / 2 - 1));
                assert!(c.data.try_write().is_ok(), "lock held over the split");
                assert!(
                    c.drain_lock.try_lock().is_err(),
                    "drain mutex released before the split ended"
                );
                c.record_point(keys[0].clone(), -3.0, late, wall);
            },
        );
        moved_by_size.push(moved);

        let drained_points: usize = drained
            .iter()
            .flat_map(|(_, chunks)| chunks.iter())
            .map(Vec::len)
            .sum();
        assert_eq!(drained_points, keys.len() * (per_key / 2 + 1));
        // The points recorded during the drain follow the kept ones.
        let mut want = recorded[0][per_key / 2 + 1..].to_vec();
        for v in [1.0, 2.0, 3.0] {
            want.push(Pt {
                value: -v,
                timestamp: late,
                wall_time: wall,
            });
        }
        assert_eq!(kept(&c, &keys[0]), want);
        assert_eq!(kept(&c, &fresh).len(), 1);
        assert_eq!(kept(&c, &keys[1]), recorded[1][per_key / 2 + 1..].to_vec());
    }
    assert_eq!(moved_by_size, vec![2, 2], "chunks moved under the lock");
}

/// Points recorded out of order (the request path reads the clock before it
/// takes the lock) are drained and kept exactly as a partition would.
#[test]
fn drain_matches_a_partition_with_out_of_order_points() {
    let keys = keys(3);
    let per_key = 3 * CHUNK_CAP + 17;
    for (seed, cut) in [(7, 0), (11, 500), (13, CHUNK_CAP), (17, 2_000), (19, 5_000)] {
        let c = MetricsCollector::new();
        let base = Instant::now();
        let recorded = fill(&c, &keys, per_key, base, 300, &mut Rng(seed));
        let cutoff = base + Duration::from_micros(cut as u64);
        let drained = drain_at(&c, cutoff);

        for (k, key) in keys.iter().enumerate() {
            let (old, young): (Vec<Pt>, Vec<Pt>) =
                recorded[k].iter().partition(|p| p.timestamp <= cutoff);
            assert_eq!(drained_of(&drained, key), old, "drained, cutoff {cut}");
            assert_eq!(kept(&c, key), young, "kept, cutoff {cut}");
        }
    }
}

/// A second drain at a later cutoff takes the next points, and a drain of
/// everything leaves no key behind.
#[test]
fn successive_drains_take_every_point_once() {
    let keys = keys(2);
    let per_key = 5 * CHUNK_CAP;
    let c = MetricsCollector::new();
    let base = Instant::now();
    let recorded = fill(&c, &keys, per_key, base, 50, &mut Rng(3));

    let mut seen = vec![Vec::new(); keys.len()];
    for cut in [1_000_u64, 1_001, 2_500, 4_000, 10_000] {
        let drained = drain_at(&c, base + Duration::from_micros(cut));
        for (k, key) in keys.iter().enumerate() {
            seen[k].extend(drained_of(&drained, key));
        }
    }
    for k in 0..keys.len() {
        let mut want = recorded[k].clone();
        want.sort_by_key(|p| p.timestamp);
        seen[k].sort_by_key(|p| p.timestamp);
        assert_eq!(seen[k], want);
    }
    assert!(c.data.read().expect("lock").is_empty());
}

type BucketRow = (
    String,
    String,
    String,
    String,
    SystemTime,
    u64,
    u64,
    u64,
    u64,
);

fn rows(buckets: Vec<FlushBucket>) -> Vec<BucketRow> {
    let mut rows: Vec<BucketRow> = buckets
        .into_iter()
        .map(|b| {
            (
                b.metric.to_string(),
                b.table_name,
                b.index_name,
                b.operation,
                b.bucket,
                b.sum.to_bits(),
                b.count,
                b.min.to_bits(),
                b.max.to_bits(),
            )
        })
        .collect();
    rows.sort_by(|a, b| (&a.0, &a.1, &a.2, &a.3, a.4).cmp(&(&b.0, &b.1, &b.2, &b.3, b.4)));
    rows
}

/// The flush buckets are the per-minute sum, count, min, and max of the
/// drained points, summed in record order.
#[test]
fn drain_buckets_match_a_per_minute_reference() {
    let keys = keys(3);
    let per_key = 150_000; // 150 s of wall time per key: three minute buckets
    let c = MetricsCollector::new();
    let base = Instant::now();
    let recorded = fill(&c, &keys, per_key, base, 200, &mut Rng(5));
    let cutoff = base + Duration::from_micros(100_000);
    let drained = drain_at(&c, cutoff);

    let mut want = Vec::new();
    for (k, key) in keys.iter().enumerate() {
        let mut by_minute: Vec<(i64, (f64, u64, f64, f64))> = Vec::new();
        for p in recorded[k].iter().filter(|p| p.timestamp <= cutoff) {
            let minute = truncate_to_minute(p.wall_time);
            if by_minute.last().is_none_or(|(m, _)| *m != minute) {
                by_minute.push((minute, (0.0, 0, f64::INFINITY, f64::NEG_INFINITY)));
            }
            let e = &mut by_minute.last_mut().expect("bucket").1;
            e.0 += p.value;
            e.1 += 1;
            e.2 = e.2.min(p.value);
            e.3 = e.3.max(p.value);
        }
        for (minute, (sum, count, min, max)) in by_minute {
            want.push(FlushBucket {
                bucket: SystemTime::UNIX_EPOCH + Duration::from_secs(minute as u64),
                metric: key.metric,
                table_name: key.table_name.clone().unwrap_or_default(),
                index_name: String::new(),
                operation: key.operation.clone().unwrap_or_default(),
                sum,
                count,
                min,
                max,
            });
        }
    }
    assert_eq!(rows(aggregate(drained)), rows(want));
}

/// `drain(ZERO)`, the shutdown flush, drains every point recorded before it.
#[test]
fn drain_zero_takes_everything() {
    let c = MetricsCollector::new();
    for i in 0..(2 * CHUNK_CAP + 5) {
        c.record_latency(Some("T"), "GetItem", i as f64);
        c.record_request_count("GetItem");
    }
    let buckets = c.drain(Duration::ZERO);
    let count: u64 = buckets.iter().map(|b| b.count).sum();
    assert_eq!(count, 2 * (2 * CHUNK_CAP as u64 + 5));
    assert!(c.data.read().expect("lock").is_empty());
}

/// `record` never grows a chunk past `CHUNK_CAP`, so its largest reallocation
/// under the lock is one chunk, not every point of the key.
#[test]
fn record_keeps_every_chunk_within_the_cap() {
    let c = MetricsCollector::new();
    for i in 0..(10 * CHUNK_CAP + 3) {
        c.record_latency(Some("T"), "GetItem", i as f64);
    }
    let map = c.data.read().expect("lock");
    let acc = map.values().next().expect("one key");
    let shapes = acc.chunk_shapes();
    assert_eq!(shapes.len(), 11);
    for (len, capacity) in &shapes {
        assert!(
            *len <= CHUNK_CAP && *capacity <= CHUNK_CAP,
            "{len}/{capacity}"
        );
    }
    let values: Vec<f64> = acc.points().map(|p| p.value).collect();
    let want: Vec<f64> = (0..(10 * CHUNK_CAP + 3)).map(|i| i as f64).collect();
    assert_eq!(values, want, "points out of record order");
}

/// The prune keeps exactly the points at or after its cutoff.
#[test]
fn prune_matches_a_retain_with_out_of_order_points() {
    let keys = keys(2);
    let per_key = 4 * CHUNK_CAP;
    for cut in [0_u64, 777, 2_048, 3_333, 9_999] {
        let c = MetricsCollector::new();
        let base = Instant::now();
        let recorded = fill(&c, &keys, per_key, base, 100, &mut Rng(23));
        let cutoff = base + Duration::from_micros(cut);
        for acc in c.data.write().expect("lock").values_mut() {
            acc.prune(cutoff);
        }
        for (k, key) in keys.iter().enumerate() {
            let want: Vec<Pt> = recorded[k]
                .iter()
                .copied()
                .filter(|p| p.timestamp >= cutoff)
                .collect();
            assert_eq!(kept(&c, key), want, "cutoff {cut}");
        }
    }
}

/// An in-memory query after a drain reports the same sum, count, min, and
/// max as the kept points, summed in record order.
#[test]
fn query_after_drain_reports_the_kept_points() {
    let keys = keys(1);
    let c = MetricsCollector::new();
    let base = Instant::now();
    let recorded = fill(&c, &keys, 3 * CHUNK_CAP, base, 40, &mut Rng(29));
    let cutoff = base + Duration::from_micros(1_500);
    drain_at(&c, cutoff);

    let young: Vec<f64> = recorded[0]
        .iter()
        .filter(|p| p.timestamp > cutoff)
        .map(|p| p.value)
        .collect();
    let snaps = c.query(&MetricsQuery {
        window: Some(TimeWindow::AllTime),
        ..Default::default()
    });
    assert_eq!(snaps.len(), 1);
    let s = &snaps[0];
    assert_eq!(s.count, young.len() as u64);
    assert_eq!(s.sum.to_bits(), young.iter().sum::<f64>().to_bits());
    assert_eq!(s.min, young.iter().copied().fold(f64::INFINITY, f64::min));
    assert_eq!(
        s.max,
        young.iter().copied().fold(f64::NEG_INFINITY, f64::max)
    );
}

/// A fully old chunk behind a chunk that holds a young point is taken whole,
/// and the result still matches a partition.
#[test]
fn drain_takes_a_fully_old_chunk_behind_a_young_point() {
    let key = keys(1).remove(0);
    let c = MetricsCollector::new();
    let base = Instant::now();
    let wall = SystemTime::UNIX_EPOCH;
    let mut recorded = Vec::new();
    for i in 0..(2 * CHUNK_CAP) {
        // One late-read young point at the head of the first chunk.
        let micros = if i == 0 { 10_000 } else { i as u64 };
        let p = Pt {
            value: i as f64,
            timestamp: base + Duration::from_micros(micros),
            wall_time: wall,
        };
        c.record_point(key.clone(), p.value, p.timestamp, p.wall_time);
        recorded.push(p);
    }
    let cutoff = base + Duration::from_micros(5_000);
    let drained = drain_at(&c, cutoff);
    let (old, young): (Vec<Pt>, Vec<Pt>) = recorded.iter().partition(|p| p.timestamp <= cutoff);
    assert_eq!(drained_of(&drained, &key), old);
    assert_eq!(kept(&c, &key), young);
}
