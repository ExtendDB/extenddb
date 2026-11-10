// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded storage for the per-operation latency segment breakdown.
//!
//! The console shows, per operation, the request count and the mean of each
//! latency segment over a time window. Those are sums, so a request does not
//! need to be stored: it is added into a time bucket at record time. Each
//! operation owns two fixed rings of buckets, one of 1-second buckets covering
//! the last 5 minutes and one of 1-minute buckets covering the last 24 hours.
//! A bucket holds the count and six running sums, 64 bytes, and is overwritten
//! in place when its ring position comes around again, so the memory per
//! process is at most `operations * (FINE_SLOTS + COARSE_SLOTS) * 64` bytes.
//! The server only records operations that pass `is_known_operation`, which
//! caps `operations` at the size of the dispatch table.
//!
//! Issue #397: one `SegmentPoint` per request grew to about 4.3 GiB in a day at
//! 274 requests per second.

use std::collections::HashMap;
use std::time::Instant;

use super::types::{LatencySegments, OperationSegments, TimeWindow};

/// Width of a fine bucket, in seconds. Serves `LastMinute` and `Last5Minutes`.
pub(super) const FINE_WIDTH_SECS: u64 = 1;
/// Fine buckets per operation: the 300 s of `Last5Minutes` plus the bucket
/// that contains the window cutoff.
pub(super) const FINE_SLOTS: usize = 301;
/// Width of a coarse bucket, in seconds. Serves `LastHour`, `LastDay`, `AllTime`.
pub(super) const COARSE_WIDTH_SECS: u64 = 60;
/// Coarse buckets per operation: the 1440 minutes of `LastDay` plus the bucket
/// that contains the window cutoff.
pub(super) const COARSE_SLOTS: usize = 1441;

/// Buckets held per operation, the constant in the memory bound.
#[cfg(test)]
pub(super) const BUCKETS_PER_OPERATION: usize = FINE_SLOTS + COARSE_SLOTS;

/// One time bucket: the slot it currently holds, the request count and the
/// sum of each segment over those requests.
#[derive(Debug, Clone, Copy, Default)]
struct Bucket {
    slot: u64,
    count: u64,
    auth_us: f64,
    authz_us: f64,
    throttle_us: f64,
    dispatch_us: f64,
    response_us: f64,
    total_us: f64,
}

impl Bucket {
    fn add(&mut self, s: &LatencySegments) {
        self.count += 1;
        self.auth_us += s.auth_us;
        self.authz_us += s.authz_us;
        self.throttle_us += s.throttle_us;
        self.dispatch_us += s.dispatch_us;
        self.response_us += s.response_us;
        self.total_us += s.total_us;
    }

    fn add_bucket(&mut self, b: &Bucket) {
        self.count += b.count;
        self.auth_us += b.auth_us;
        self.authz_us += b.authz_us;
        self.throttle_us += b.throttle_us;
        self.dispatch_us += b.dispatch_us;
        self.response_us += b.response_us;
        self.total_us += b.total_us;
    }
}

/// A fixed ring of buckets. Slot `k` covers elapsed seconds
/// `[k * width, (k + 1) * width)` and lives at position `k % len`.
#[derive(Debug)]
struct Ring {
    width_secs: u64,
    buckets: Box<[Bucket]>,
}

impl Ring {
    fn new(width_secs: u64, slots: usize) -> Self {
        Self {
            width_secs,
            buckets: vec![Bucket::default(); slots].into_boxed_slice(),
        }
    }

    fn slots(&self) -> u64 {
        self.buckets.len() as u64
    }

    /// Adds one request observed `elapsed_secs` after the epoch.
    fn record(&mut self, elapsed_secs: u64, segments: &LatencySegments) {
        let slot = elapsed_secs / self.width_secs;
        #[allow(clippy::cast_possible_truncation)] // slot % len < len <= usize::MAX
        let position = (slot % self.slots()) as usize;
        let bucket = &mut self.buckets[position];
        if bucket.slot < slot {
            // The position comes around again: drop what the previous lap left.
            *bucket = Bucket {
                slot,
                ..Bucket::default()
            };
        }
        // `bucket.slot > slot` means this record lags the newest one by a full
        // ring, which monotonic time rules out; it is counted where it lands.
        bucket.add(segments);
    }

    /// Sums every bucket whose slot is at or after `first_slot` into `acc`.
    /// Slots left over from an earlier lap are older than `first_slot` for
    /// every window this ring serves, so no separate staleness check is needed.
    fn sum_from(&self, first_slot: u64, acc: &mut Bucket) {
        for b in &self.buckets {
            if b.slot >= first_slot && b.count > 0 {
                acc.add_bucket(b);
            }
        }
    }

    /// First slot included in a query made `elapsed_secs` after the epoch for
    /// a window of `window_secs`, or for the whole ring when `None`.
    fn first_slot(&self, elapsed_secs: u64, window_secs: Option<u64>) -> u64 {
        match window_secs {
            Some(w) => elapsed_secs.saturating_sub(w) / self.width_secs,
            None => (elapsed_secs / self.width_secs).saturating_sub(self.slots() - 1),
        }
    }
}

/// The two rings of one operation.
#[derive(Debug)]
struct OperationRings {
    fine: Ring,
    coarse: Ring,
}

impl OperationRings {
    fn new() -> Self {
        Self {
            fine: Ring::new(FINE_WIDTH_SECS, FINE_SLOTS),
            coarse: Ring::new(COARSE_WIDTH_SECS, COARSE_SLOTS),
        }
    }

    fn record(&mut self, elapsed_secs: u64, segments: &LatencySegments) {
        self.fine.record(elapsed_secs, segments);
        self.coarse.record(elapsed_secs, segments);
    }
}

/// Which ring a window reads, and the window length in seconds (`None` for
/// the whole ring).
fn ring_for(window: TimeWindow) -> (bool, Option<u64>) {
    match window {
        TimeWindow::LastMinute => (true, Some(60)),
        TimeWindow::Last5Minutes => (true, Some(300)),
        TimeWindow::LastHour => (false, Some(3600)),
        TimeWindow::LastDay => (false, Some(86400)),
        TimeWindow::AllTime => (false, None),
    }
}

/// Per-operation segment buckets. Lives behind the collector's lock.
#[derive(Debug)]
pub(super) struct SegmentStore {
    /// Time zero for slot arithmetic.
    epoch: Instant,
    by_operation: HashMap<String, OperationRings>,
}

impl SegmentStore {
    pub(super) fn new(epoch: Instant) -> Self {
        Self {
            epoch,
            by_operation: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn epoch(&self) -> Instant {
        self.epoch
    }

    fn elapsed_secs(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.epoch).as_secs()
    }

    /// Adds one request. Steady state is one hash lookup and two bucket
    /// updates; the only allocation is the rings of an operation seen for the
    /// first time.
    pub(super) fn record(&mut self, operation: &str, segments: &LatencySegments, now: Instant) {
        let elapsed = self.elapsed_secs(now);
        if let Some(rings) = self.by_operation.get_mut(operation) {
            rings.record(elapsed, segments);
            return;
        }
        self.by_operation
            .entry(operation.to_owned())
            .or_insert_with(OperationRings::new)
            .record(elapsed, segments);
    }

    /// Per-operation count and mean segments over `window`, sorted by
    /// operation name. Operations with no request in the window are omitted.
    ///
    /// A window of `W` seconds read from a ring of `b`-second buckets covers
    /// at least the last `W` seconds and at most the last `W + b` seconds.
    pub(super) fn query(&self, window: TimeWindow, now: Instant) -> Vec<OperationSegments> {
        let elapsed = self.elapsed_secs(now);
        let (fine, window_secs) = ring_for(window);
        let mut out: Vec<OperationSegments> = self
            .by_operation
            .iter()
            .filter_map(|(operation, rings)| {
                let ring = if fine { &rings.fine } else { &rings.coarse };
                let mut acc = Bucket::default();
                ring.sum_from(ring.first_slot(elapsed, window_secs), &mut acc);
                (acc.count > 0).then(|| {
                    #[allow(clippy::cast_precision_loss)]
                    let c = acc.count as f64;
                    OperationSegments {
                        operation: operation.clone(),
                        count: acc.count,
                        avg: LatencySegments {
                            auth_us: acc.auth_us / c,
                            authz_us: acc.authz_us / c,
                            throttle_us: acc.throttle_us / c,
                            dispatch_us: acc.dispatch_us / c,
                            response_us: acc.response_us / c,
                            total_us: acc.total_us / c,
                        },
                    }
                })
            })
            .collect();
        out.sort_by(|a, b| a.operation.cmp(&b.operation));
        out
    }

    /// Number of operations with rings allocated.
    #[cfg(test)]
    pub(super) fn operation_count(&self) -> usize {
        self.by_operation.len()
    }

    /// Number of buckets allocated, the quantity the memory bound is stated in.
    #[cfg(test)]
    pub(super) fn bucket_count(&self) -> usize {
        self.by_operation
            .values()
            .map(|r| r.fine.buckets.len() + r.coarse.buckets.len())
            .sum()
    }

    /// Size in bytes of one bucket, for stating the bound in bytes.
    #[cfg(test)]
    pub(super) const fn bucket_size() -> usize {
        std::mem::size_of::<Bucket>()
    }
}

#[cfg(test)]
#[path = "segments_tests.rs"]
mod tests;
