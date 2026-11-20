// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the memory bound and the prune of [`SegmentStore`].

use std::time::{Duration, Instant};

use super::tests::{ACCOUNTS, OPS, at, count_of, segs};
use super::*;

#[test]
fn bucket_is_64_bytes() {
    assert_eq!(SegmentStore::bucket_size(), 64);
    assert_eq!(BUCKETS_PER_OPERATION, 1742);
}

/// Issue #397: five million requests over three simulated days across three
/// accounts and five operations fill the rings and allocate nothing more.
#[test]
fn boundedness_five_million_records() {
    let epoch = Instant::now();
    let mut store = SegmentStore::new(epoch);
    let total: u64 = 5_000_000;
    // 52 ms apart: 260 000 s, three days, so every ring laps many times.
    // 3 and 5 are coprime, so every account records every operation, every
    // second and every minute.
    for i in 0..total {
        let now = epoch + Duration::from_millis(i * 52);
        #[allow(clippy::cast_possible_truncation)]
        let account = ACCOUNTS[(i % 3) as usize];
        #[allow(clippy::cast_possible_truncation)]
        let op = OPS[(i % 5) as usize];
        store.record(account, op, &segs(1.0), now);
    }
    let bound = ACCOUNTS.len() * OPS.len() * BUCKETS_PER_OPERATION;
    assert!(
        store.bucket_count() <= bound,
        "bucket count {} exceeds bound {bound}",
        store.bucket_count()
    );
    assert!(store.bucket_capacity() * SegmentStore::bucket_size() <= bound * 64);
    // Every slot of every ring had a request, so the bound is reached
    // exactly, held and allocated.
    assert_eq!(store.bucket_count(), bound);
    assert_eq!(store.bucket_capacity(), bound);
    // The whole-ring query still answers, with at most 1441 minutes of data.
    let now = epoch + Duration::from_millis(total * 52);
    let out = store.query(TimeWindow::AllTime, None, now);
    let in_day: u64 = out.iter().map(|o| o.count).sum();
    assert!(in_day > 0 && in_day <= 86_460_000 / 52 + OPS.len() as u64);
}

/// A bucket whose slot has left the ring is dropped by the next record, and a
/// bucket that is still in the ring but outside the window is excluded by the
/// window filter. `AllTime` is the whole 24-hour ring.
#[test]
fn ring_lap_and_stale_slots() {
    let epoch = Instant::now();
    let mut store = SegmentStore::new(epoch);
    store.record(ACCOUNTS[0], "Scan", &segs(1.0), at(epoch, 0));
    // One day minus a second later minute 0 is still inside the ring.
    assert_eq!(
        count_of(
            &store.query(TimeWindow::AllTime, None, at(epoch, 86_459)),
            "Scan"
        ),
        1
    );
    assert_eq!(
        count_of(
            &store.query(TimeWindow::LastDay, None, at(epoch, 86_459)),
            "Scan"
        ),
        1
    );
    // At 1441 minutes the ring has moved past minute 0 without a record.
    assert!(
        store
            .query(TimeWindow::AllTime, None, at(epoch, 86_460))
            .is_empty()
    );
    assert!(
        store
            .query(TimeWindow::LastDay, None, at(epoch, 86_460))
            .is_empty()
    );
    assert_eq!(store.bucket_count(), 2);
    // A record one full ring later drops minute 0 and second 0.
    store.record(ACCOUNTS[0], "Scan", &segs(5.0), at(epoch, 1441 * 60));
    let out = store.query(TimeWindow::AllTime, None, at(epoch, 1441 * 60));
    assert_eq!(count_of(&out, "Scan"), 1);
    assert!((out[0].avg.auth_us - 5.0).abs() < f64::EPSILON);
    assert_eq!(store.bucket_count(), 2);
}

/// The prune drops the rings of an account's operation once its newest coarse
/// bucket has left the whole-ring window, and the account once it holds no
/// operation. An account whose last request is still in the window keeps its
/// rings and the data in them.
#[test]
fn prune_drops_idle_accounts_and_keeps_active_ones() {
    let epoch = Instant::now();
    let mut store = SegmentStore::new(epoch);
    store.record(ACCOUNTS[0], "GetItem", &segs(1.0), at(epoch, 0));
    store.record(ACCOUNTS[0], "Scan", &segs(1.0), at(epoch, 0));
    store.record(ACCOUNTS[1], "GetItem", &segs(1.0), at(epoch, 0));
    store.record(ACCOUNTS[1], "GetItem", &segs(1.0), at(epoch, 60));
    // Two buckets per pair with one request, two per ring for the pair with
    // requests in two seconds and two minutes.
    assert_eq!(store.bucket_count(), 2 + 2 + 4);

    // At 1440 minutes, minute 0 is the oldest slot of the ring: nothing goes.
    store.prune(at(epoch, 1440 * 60));
    assert_eq!(store.by_account.len(), 2);
    assert_eq!(store.bucket_count(), 8);
    assert_eq!(
        count_of(
            &store.query(TimeWindow::AllTime, None, at(epoch, 1440 * 60)),
            "Scan"
        ),
        1
    );

    // One minute later minute 0 has left the ring. The first account's two
    // operations have no newer bucket, so their rings go and the account with
    // them. The second account's GetItem ring has minute 1 and stays.
    store.prune(at(epoch, 1441 * 60));
    assert_eq!(store.by_account.len(), 1);
    assert_eq!(store.bucket_count(), 4);
    assert!(
        store
            .query(TimeWindow::AllTime, Some(ACCOUNTS[0]), at(epoch, 1441 * 60))
            .is_empty()
    );
    assert_eq!(
        count_of(
            &store.query(TimeWindow::AllTime, Some(ACCOUNTS[1]), at(epoch, 1441 * 60)),
            "GetItem"
        ),
        1
    );
    // The dropped account's next request allocates its rings again.
    store.record(ACCOUNTS[0], "Scan", &segs(2.0), at(epoch, 1441 * 60));
    assert_eq!(store.by_account.len(), 2);
    assert_eq!(store.bucket_count(), 6);
}

/// A memory case: label, accounts, operations, request times in seconds (the
/// same for every pair), then the buckets held and the buckets allocated
/// expected per pair.
type MemoryCase<'a> = (&'a str, usize, usize, &'a [u64], usize, usize);

/// Buckets exist only for the seconds and minutes that had a request, and a
/// ring's allocation doubles up to its length and stops.
#[test]
fn bucket_memory_follows_the_requests_up_to_the_ring_length() {
    const DAY: u64 = COARSE_SLOTS as u64 * COARSE_WIDTH_SECS;
    let many: Vec<String> = (0..1000)
        .map(|i| format!("{:012}", 100_000_000_000u64 + i))
        .collect();
    let every_second: Vec<u64> = (0..2 * DAY).collect();
    let cases: [MemoryCase; 5] = [
        // One request a day holds one bucket per ring.
        (
            "1,000 accounts x 40 operations, one request",
            1000,
            40,
            &[0],
            2,
            2,
        ),
        // Six seconds of one minute: six fine buckets in a capacity of 8, one coarse.
        ("six consecutive seconds", 1, 1, &[0, 1, 2, 3, 4, 5], 7, 9),
        // Five requests an hour apart: each drops the fine bucket before it;
        // five coarse buckets in a capacity of 8.
        (
            "five requests an hour apart",
            1,
            1,
            &[0, 3600, 7200, 10_800, 14_400],
            6,
            9,
        ),
        // Two days of a request every second fill both rings exactly.
        (
            "a request every second for two days",
            1,
            1,
            &every_second,
            BUCKETS_PER_OPERATION,
            BUCKETS_PER_OPERATION,
        ),
        // A request a day later than the first drops the first's buckets.
        ("two requests a ring apart", 1, 1, &[0, DAY], 2, 2),
    ];
    for (label, accounts, operations, times, buckets, capacity) in cases {
        let epoch = Instant::now();
        let mut store = SegmentStore::new(epoch);
        let ops: Vec<String> = (0..operations)
            .map(|i| format!("Operation{i:02}"))
            .collect();
        for account in &many[..accounts] {
            for op in &ops {
                for &t in times {
                    store.record(account, op, &segs(1.0), at(epoch, t));
                }
            }
        }
        let pairs = accounts * operations;
        assert_eq!(store.bucket_count(), pairs * buckets, "{label}: buckets");
        assert_eq!(
            store.bucket_capacity(),
            pairs * capacity,
            "{label}: capacity"
        );
        assert!(
            store.bucket_capacity() <= pairs * BUCKETS_PER_OPERATION,
            "{label}: capacity over the ring length"
        );
        // Every request is still in the whole-ring window.
        let now = at(epoch, times[times.len() - 1]);
        let total: u64 = store
            .query(TimeWindow::AllTime, None, now)
            .iter()
            .map(|o| o.count)
            .sum();
        let in_ring = times
            .iter()
            .filter(|&&t| t / 60 + COARSE_SLOTS as u64 > now.duration_since(epoch).as_secs() / 60)
            .count();
        assert_eq!(total, (pairs * in_ring) as u64, "{label}: count");
    }
}

/// Write-path and read-path cost of the previous one-point-per-request
/// `Vec` against the bucketed store, one million single-threaded calls each.
/// Run with `cargo test -p extenddb-core -- --ignored bench --nocapture`, in
/// debug and in `--release`.
#[test]
#[ignore = "timing, run by hand"]
fn bench_old_push_vs_new_record() {
    use std::sync::RwLock;

    #[allow(dead_code)]
    struct OldPoint {
        operation: String,
        table_name: Option<String>,
        segments: LatencySegments,
        timestamp: Instant,
    }

    const N: u64 = 1_000_000;
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    let old = RwLock::new(Vec::<OldPoint>::with_capacity(256));
    let start = Instant::now();
    for i in 0..N {
        #[allow(clippy::cast_possible_truncation)]
        let op = OPS[(i % 5) as usize];
        let point = OldPoint {
            operation: op.to_owned(),
            table_name: Some("T".to_owned()),
            segments: segs(1.0),
            timestamp: Instant::now(),
        };
        if let Ok(mut v) = old.write() {
            v.push(point);
        }
    }
    let old_ns = start.elapsed().as_nanos() / u128::from(N);
    let old_bytes = old
        .read()
        .map_or(0, |v| v.capacity() * std::mem::size_of::<OldPoint>());

    let new = RwLock::new(SegmentStore::new(Instant::now()));
    let start = Instant::now();
    for i in 0..N {
        #[allow(clippy::cast_possible_truncation)]
        let op = OPS[(i % 5) as usize];
        if let Ok(mut s) = new.write() {
            s.record(ACCOUNTS[0], op, &segs(1.0), Instant::now());
        }
    }
    let new_ns = start.elapsed().as_nanos() / u128::from(N);
    let new_bytes = new
        .read()
        .map_or(0, |s| s.bucket_count() * SegmentStore::bucket_size());

    // Read path: the old query_segments loop over every point, versus the store.
    let start = Instant::now();
    let mut by_op: std::collections::HashMap<String, (f64, u64)> = std::collections::HashMap::new();
    if let Ok(v) = old.read() {
        for p in v.iter() {
            let e = by_op.entry(p.operation.clone()).or_default();
            e.0 += p.segments.total_us;
            e.1 += 1;
        }
    }
    let old_query_us = start.elapsed().as_micros();
    let start = Instant::now();
    let out = new
        .read()
        .map(|s| s.query(TimeWindow::AllTime, None, Instant::now()))
        .unwrap_or_default();
    let new_query_us = start.elapsed().as_micros();

    println!(
        "bench[{profile}] record: old push {old_ns} ns/call, new record {new_ns} ns/call; \
         memory after {N} calls: old {old_bytes} B (Vec capacity, Strings excluded), new {new_bytes} B; \
         query over {N} points: old {old_query_us} us ({} ops), new {new_query_us} us ({} ops)",
        by_op.len(),
        out.len()
    );
}
