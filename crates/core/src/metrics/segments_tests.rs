// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Window and reference tests for [`SegmentStore`]; the bound and prune tests
//! are in `segments_bound_tests.rs`.

use web_time::{Duration, Instant};

use super::*;

pub(super) const OPS: [&str; 5] = ["GetItem", "PutItem", "Query", "Scan", "UpdateItem"];
pub(super) const ACCOUNTS: [&str; 3] = ["111111111111", "222222222222", "333333333333"];

pub(super) fn segs(v: f64) -> LatencySegments {
    LatencySegments {
        auth_us: v,
        authz_us: 2.0 * v,
        throttle_us: 0.5 * v,
        dispatch_us: 4.0 * v,
        response_us: 0.25 * v,
        total_us: 7.75 * v,
    }
}

/// Deterministic xorshift64* generator, so the random workload needs no crate.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub(super) fn at(epoch: Instant, secs: u64) -> Instant {
    epoch + Duration::from_secs(secs)
}

pub(super) fn count_of(out: &[OperationSegments], op: &str) -> u64 {
    out.iter()
        .find(|o| o.operation == op)
        .map_or(0, |o| o.count)
}

/// Naive reference: for a window of `w` seconds read from `b`-second buckets,
/// the store returns every point whose bucket starts at or after the bucket
/// that contains `now - w`. That set contains the exact last `w` seconds and
/// is contained in the exact last `w + b` seconds. Without an account filter
/// the reference is every account's points; with one, that account's points.
/// A point is `(account index, operation index, ms after the epoch, segments)`.
#[test]
fn random_workload_matches_naive_reference() {
    let epoch = Instant::now();
    let mut store = SegmentStore::new(epoch);
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut points: Vec<(usize, usize, u64, LatencySegments)> = Vec::new();
    let mut t_ms: u64 = 0;
    // 300 000 points, 0 to 40 ms apart: about 6 000 s, so the hour and the
    // minute windows both cut inside the data.
    for _ in 0..300_000 {
        t_ms += rng.below(41);
        #[allow(clippy::cast_possible_truncation)]
        let account = rng.below(ACCOUNTS.len() as u64) as usize;
        #[allow(clippy::cast_possible_truncation)]
        let op = rng.below(OPS.len() as u64) as usize;
        #[allow(clippy::cast_precision_loss)]
        let v = rng.below(10_000) as f64 / 7.0;
        let s = segs(v);
        store.record(
            ACCOUNTS[account],
            OPS[op],
            &s,
            epoch + Duration::from_millis(t_ms),
        );
        points.push((account, op, t_ms, s));
    }
    let windows = [
        (TimeWindow::LastMinute, 60, 1),
        (TimeWindow::Last5Minutes, 300, 1),
        (TimeWindow::LastHour, 3600, 60),
        (TimeWindow::LastDay, 86400, 60),
    ];
    let filters = [None, Some(0), Some(1), Some(2)];
    for now_ms in [t_ms, t_ms + 500, t_ms + 59_500, t_ms + 61_000] {
        let now = epoch + Duration::from_millis(now_ms);
        let now_secs = now_ms / 1000;
        for (window, w, b) in windows {
            let first_slot = now_secs.saturating_sub(w) / b;
            for filter in filters {
                let out = store.query(window, filter.map(|a| ACCOUNTS[a]), now);
                assert_matches_reference(&out, &points, filter, (window, w, b), now_ms, first_slot);
            }
        }
    }
}

/// Checks `out`, the store's answer for the window `(window, w, b)` at
/// `now_ms`, against the points of the account at index `filter` in
/// `ACCOUNTS` (every account when `None`) summed by hand: the count is exact,
/// the means agree to 1e-9, and the count lies between the exact `w` and
/// `w + b` second windows. The label names the index, not the account id.
fn assert_matches_reference(
    out: &[OperationSegments],
    points: &[(usize, usize, u64, LatencySegments)],
    filter: Option<usize>,
    (window, w, b): (TimeWindow, u64, u64),
    now_ms: u64,
    first_slot: u64,
) {
    for (i, op) in OPS.iter().enumerate() {
        let label = format!(
            "{op} {window:?} at {now_ms} ms, filter {}",
            filter.map_or_else(|| "none".to_owned(), |a| format!("index {a}"))
        );
        let mut count = 0u64;
        let mut sum = LatencySegments::default();
        let mut exact_w = 0u64;
        let mut exact_w_plus_b = 0u64;
        for (p_account, p_op, p_ms, s) in points {
            if *p_op != i || filter.is_some_and(|a| *p_account != a) {
                continue;
            }
            if *p_ms + w * 1000 >= now_ms {
                exact_w += 1;
            }
            if *p_ms + (w + b) * 1000 >= now_ms {
                exact_w_plus_b += 1;
            }
            if (*p_ms / 1000) / b >= first_slot {
                count += 1;
                sum.auth_us += s.auth_us;
                sum.authz_us += s.authz_us;
                sum.throttle_us += s.throttle_us;
                sum.dispatch_us += s.dispatch_us;
                sum.response_us += s.response_us;
                sum.total_us += s.total_us;
            }
        }
        let got = out.iter().find(|o| &o.operation == op);
        if count == 0 {
            assert!(got.is_none(), "{label}: expected absent");
            continue;
        }
        let got = got.unwrap_or_else(|| panic!("{label}: missing"));
        assert_eq!(got.count, count, "{label}: count");
        assert!(
            exact_w <= count && count <= exact_w_plus_b,
            "{label}: {exact_w} <= {count} <= {exact_w_plus_b}"
        );
        #[allow(clippy::cast_precision_loss)]
        let c = count as f64;
        let close = |a: f64, b: f64| ((a - b) / b).abs() < 1e-9;
        assert!(close(got.avg.auth_us, sum.auth_us / c), "{label}: auth");
        assert!(close(got.avg.authz_us, sum.authz_us / c), "{label}: authz");
        assert!(
            close(got.avg.throttle_us, sum.throttle_us / c),
            "{label}: throttle"
        );
        assert!(
            close(got.avg.dispatch_us, sum.dispatch_us / c),
            "{label}: dispatch"
        );
        assert!(
            close(got.avg.response_us, sum.response_us / c),
            "{label}: response"
        );
        assert!(close(got.avg.total_us, sum.total_us / c), "{label}: total");
    }
}

/// `LastHour` reads 1-minute buckets: points 0 s and 59 s after the epoch
/// share a bucket, and that bucket is included until the hour window's
/// cutoff leaves the minute it starts in.
#[test]
fn window_filter_at_minute_boundary() {
    let epoch = Instant::now();
    let mut store = SegmentStore::new(epoch);
    for t in [0, 59, 60] {
        store.record(ACCOUNTS[0], "GetItem", &segs(1.0), at(epoch, t));
    }
    // The fine ring cuts at the second: 119 s keeps 59 s and 60 s, 120 s drops 59 s.
    assert_eq!(
        count_of(
            &store.query(TimeWindow::LastMinute, None, at(epoch, 119)),
            "GetItem"
        ),
        2
    );
    assert_eq!(
        count_of(
            &store.query(TimeWindow::LastMinute, None, at(epoch, 120)),
            "GetItem"
        ),
        1
    );
    // 3659 s: cutoff 59 s lies in minute 0, so minute 0 is included.
    store.record(ACCOUNTS[0], "GetItem", &segs(1.0), at(epoch, 3659));
    assert_eq!(
        count_of(
            &store.query(TimeWindow::LastHour, None, at(epoch, 3659)),
            "GetItem"
        ),
        4
    );
    // An exact filter would have returned 3 here (59, 60, 3659).
    // 3660 s: cutoff 60 s starts minute 1, so minute 0 is excluded.
    assert_eq!(
        count_of(
            &store.query(TimeWindow::LastHour, None, at(epoch, 3660)),
            "GetItem"
        ),
        2
    );
}

/// `LastMinute` and `Last5Minutes` read 1-second buckets, so they are at most
/// one second wider than the exact window.
#[test]
fn fine_windows_cut_at_the_second() {
    let epoch = Instant::now();
    let mut store = SegmentStore::new(epoch);
    for t in [0, 1, 2, 300, 301] {
        store.record(ACCOUNTS[0], "Query", &segs(1.0), at(epoch, t));
    }
    // 301 s: cutoff 1 s, buckets 1, 2, 300, 301.
    assert_eq!(
        count_of(
            &store.query(TimeWindow::Last5Minutes, None, at(epoch, 301)),
            "Query"
        ),
        4
    );
    // 302 s: cutoff 2 s.
    assert_eq!(
        count_of(
            &store.query(TimeWindow::Last5Minutes, None, at(epoch, 302)),
            "Query"
        ),
        3
    );
    // 361 s: cutoff 301 s for the minute window.
    assert_eq!(
        count_of(
            &store.query(TimeWindow::LastMinute, None, at(epoch, 361)),
            "Query"
        ),
        1
    );
    assert_eq!(
        count_of(
            &store.query(TimeWindow::LastMinute, None, at(epoch, 362)),
            "Query"
        ),
        0
    );
    assert!(
        store
            .query(TimeWindow::LastMinute, None, at(epoch, 362))
            .is_empty()
    );
}

/// One request every second for three laps of the coarse ring, queried at
/// every ring and window boundary and every 977 s in between. The expected
/// count and mean have a closed form (an arithmetic series over the included
/// seconds) that does not go through the ring, so a coarse ring that loses or
/// leaks a bucket after its first lap fails here.
#[test]
fn continuous_load_three_coarse_laps_exact_count_and_mean() {
    // (window, W seconds, bucket width b, ring length)
    const WINDOWS: [(TimeWindow, u64, u64, u64); 5] = [
        (
            TimeWindow::LastMinute,
            60,
            FINE_WIDTH_SECS,
            FINE_SLOTS as u64,
        ),
        (
            TimeWindow::Last5Minutes,
            300,
            FINE_WIDTH_SECS,
            FINE_SLOTS as u64,
        ),
        (
            TimeWindow::LastHour,
            3600,
            COARSE_WIDTH_SECS,
            COARSE_SLOTS as u64,
        ),
        (
            TimeWindow::LastDay,
            86_400,
            COARSE_WIDTH_SECS,
            COARSE_SLOTS as u64,
        ),
        (
            TimeWindow::AllTime,
            86_400,
            COARSE_WIDTH_SECS,
            COARSE_SLOTS as u64,
        ),
    ];
    let epoch = Instant::now();
    let mut store = SegmentStore::new(epoch);
    let end: u64 = 3 * COARSE_SLOTS as u64 * COARSE_WIDTH_SECS + 123;
    let mut query_times: Vec<u64> = (0..=end).step_by(977).collect();
    for (_, w, b, len) in WINDOWS {
        query_times.extend([
            w - 1,
            w,
            w + 1,
            w + b - 1,
            w + b,
            len * b - 1,
            len * b,
            len * b + 1,
            2 * len * b,
            2 * len * b + b - 1,
            end,
        ]);
    }
    query_times.sort_unstable();
    query_times.dedup();
    let mut next = 0usize;
    let mut checks = 0u64;
    for t in 0..=end {
        #[allow(clippy::cast_precision_loss)]
        let v = t as f64;
        store.record(
            ACCOUNTS[0],
            "Scan",
            &LatencySegments {
                auth_us: v,
                authz_us: 0.0,
                throttle_us: 1.0,
                dispatch_us: 0.0,
                response_us: 0.0,
                total_us: v,
            },
            at(epoch, t),
        );
        if next >= query_times.len() || query_times[next] != t {
            continue;
        }
        next += 1;
        for (window, w, b, _) in WINDOWS {
            // First included second: the start of the bucket holding t - W.
            let first = t.saturating_sub(w) / b * b;
            let want_count = t - first + 1;
            let want_sum = (first + t) * want_count / 2;
            let out = store.query(window, None, at(epoch, t));
            let Some(o) = out.iter().find(|o| o.operation == "Scan") else {
                panic!("{window:?} t={t}: Scan absent");
            };
            assert_eq!(o.count, want_count, "{window:?} t={t} count");
            #[allow(clippy::cast_precision_loss)]
            let mean = want_sum as f64 / want_count as f64;
            assert!(
                (o.avg.auth_us - mean).abs() <= mean * 1e-12,
                "{window:?} t={t} mean got {} want {mean}",
                o.avg.auth_us
            );
            assert!(
                (o.avg.throttle_us - 1.0).abs() < 1e-12,
                "{window:?} t={t} throttle"
            );
            checks += 1;
        }
    }
    assert!(checks > 1000, "only {checks} checks ran");
}

#[test]
fn operations_sorted_and_zero_count_absent() {
    let epoch = Instant::now();
    let mut store = SegmentStore::new(epoch);
    store.record(ACCOUNTS[0], "Scan", &segs(1.0), at(epoch, 10));
    store.record(ACCOUNTS[0], "GetItem", &segs(1.0), at(epoch, 10));
    store.record(ACCOUNTS[0], "PutItem", &segs(1.0), at(epoch, 1000));
    let out = store.query(TimeWindow::LastMinute, None, at(epoch, 1000));
    let names: Vec<&str> = out.iter().map(|o| o.operation.as_str()).collect();
    assert_eq!(names, ["PutItem"]);
    let out = store.query(TimeWindow::AllTime, None, at(epoch, 1000));
    let names: Vec<&str> = out.iter().map(|o| o.operation.as_str()).collect();
    assert_eq!(names, ["GetItem", "PutItem", "Scan"]);
    // An account that sent nothing has no buckets to read.
    assert!(
        store
            .query(TimeWindow::AllTime, Some(ACCOUNTS[1]), at(epoch, 1000))
            .is_empty()
    );
}
