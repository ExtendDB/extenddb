// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded storage for the per-operation latency segment breakdown.
//!
//! The console shows, per operation, the request count and the mean of each
//! latency segment over a time window. Those are sums, so a request does not
//! need to be stored: it is added into a time bucket at record time. Each
//! operation of each account owns two rings of buckets, one of 1-second
//! buckets covering the last 5 minutes and one of 1-minute buckets covering
//! the last 24 hours. A bucket holds the count and six running sums, 64 bytes.
//! A ring holds only the buckets of the slots that had a request, in slot
//! order, and a record drops the buckets that have left the ring's window, so
//! a ring never holds more than `FINE_SLOTS` or `COARSE_SLOTS` buckets and
//! its allocation grows by doubling up to that length and no further. The
//! allocation does not shrink: a pair keeps the largest one it reached until
//! the prune drops its rings. The memory per process is therefore at most
//! `accounts * operations * (FINE_SLOTS + COARSE_SLOTS) * 64` bytes of
//! buckets. A pair holds that many buckets only with a request in every
//! second of the last five minutes and every minute of the last day; a pair
//! with one request a day holds two buckets. The server only records
//! operations that pass `is_known_operation`, which caps `operations` at the
//! size of the dispatch table, and only for a request that reached dispatch.
//! `accounts` is the number of accounts that sent a request in the last 1,440
//! to about 1,446 minutes. The collector's prune runs every five minutes. It
//! drops the rings of an account's operation once its newest bucket has left
//! the coarse ring's window, and the account once it holds no operation.
//!
//! The account is a key because `/metrics` filters the segments by account.
//! A query without the filter sums every account's buckets per operation.

use std::collections::{BTreeMap, HashMap, VecDeque};
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

/// Buckets held per operation of one account, the constant in the memory
/// bound.
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
    /// A bucket at `slot` that holds one request.
    fn new(slot: u64, s: &LatencySegments) -> Self {
        Self {
            slot,
            count: 1,
            auth_us: s.auth_us,
            authz_us: s.authz_us,
            throttle_us: s.throttle_us,
            dispatch_us: s.dispatch_us,
            response_us: s.response_us,
            total_us: s.total_us,
        }
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

/// A ring of buckets. Slot `k` covers elapsed seconds
/// `[k * width, (k + 1) * width)`. Only the slots that had a request have a
/// bucket, in slot order with the newest at the back, and never more than
/// `len` of them: a record at slot `k` drops the buckets of slots before
/// `k + 1 - len` first.
#[derive(Debug)]
struct Ring {
    width_secs: u64,
    /// Most buckets held, and the capacity the deque grows to and no further.
    len: usize,
    buckets: VecDeque<Bucket>,
}

impl Ring {
    fn new(width_secs: u64, len: usize) -> Self {
        Self {
            width_secs,
            len,
            buckets: VecDeque::new(),
        }
    }

    fn slots(&self) -> u64 {
        self.len as u64
    }

    /// Adds one request observed `elapsed_secs` after the epoch.
    fn record(&mut self, elapsed_secs: u64, segments: &LatencySegments) {
        let slot = elapsed_secs / self.width_secs;
        if let Some(newest) = self.buckets.back_mut()
            && newest.slot >= slot
        {
            // The same slot, or a record behind the newest one, which the
            // clock read under the lock rules out: counted in the newest.
            newest.add_bucket(&Bucket::new(slot, segments));
            return;
        }
        let first_kept = (slot + 1).saturating_sub(self.slots());
        while self.buckets.front().is_some_and(|b| b.slot < first_kept) {
            self.buckets.pop_front();
        }
        if self.buckets.len() == self.buckets.capacity() {
            // Double the capacity, up to the ring length: after the pops at
            // most `len - 1` buckets are held, so `len - held` is at least 1.
            let held = self.buckets.len();
            self.buckets.reserve_exact(held.max(1).min(self.len - held));
        }
        self.buckets.push_back(Bucket::new(slot, segments));
    }

    /// Sums every bucket whose slot is at or after `first_slot` into `acc`.
    /// The buckets are in slot order, so this reads only the window's slots.
    fn sum_from(&self, first_slot: u64, acc: &mut Bucket) {
        let start = self.buckets.partition_point(|b| b.slot < first_slot);
        for b in self.buckets.range(start..) {
            acc.add_bucket(b);
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

/// Per-account, per-operation segment buckets. Lives behind the collector's
/// lock.
#[derive(Debug)]
pub(super) struct SegmentStore {
    /// Time zero for slot arithmetic.
    epoch: Instant,
    /// The rings of each operation, under the account that sent the request.
    /// Two maps rather than one keyed by the pair, so a record probes with
    /// the borrowed names and allocates no key after the first request of an
    /// account's operation.
    by_account: HashMap<String, HashMap<String, OperationRings>>,
}

impl SegmentStore {
    pub(super) fn new(epoch: Instant) -> Self {
        Self {
            epoch,
            by_account: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn epoch(&self) -> Instant {
        self.epoch
    }

    fn elapsed_secs(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.epoch).as_secs()
    }

    /// Adds one request of `account_id`. Steady state is two hash lookups and
    /// two bucket updates. The allocations are the keys of a pair's first
    /// request and the growth of its rings, which doubles up to the ring
    /// length and stops there.
    pub(super) fn record(
        &mut self,
        account_id: &str,
        operation: &str,
        segments: &LatencySegments,
        now: Instant,
    ) {
        let elapsed = self.elapsed_secs(now);
        if let Some(rings) = self
            .by_account
            .get_mut(account_id)
            .and_then(|operations| operations.get_mut(operation))
        {
            rings.record(elapsed, segments);
            return;
        }
        self.by_account
            .entry(account_id.to_owned())
            .or_default()
            .entry(operation.to_owned())
            .or_insert_with(OperationRings::new)
            .record(elapsed, segments);
    }

    /// Per-operation count and mean segments over `window`, sorted by
    /// operation name: the requests of `account_id` when it is given and not
    /// empty (an empty account is no filter, as in the stores), every
    /// account's requests summed per operation when it is not. Operations
    /// with no request in the window are omitted.
    ///
    /// A window of `W` seconds read from a ring of `b`-second buckets covers
    /// at least the last `W` seconds and at most the last `W + b` seconds.
    pub(super) fn query(
        &self,
        window: TimeWindow,
        account_id: Option<&str>,
        now: Instant,
    ) -> Vec<OperationSegments> {
        let account_id = account_id.filter(|a| !a.is_empty());
        let elapsed = self.elapsed_secs(now);
        let (fine, window_secs) = ring_for(window);
        let mut by_operation: BTreeMap<&str, Bucket> = BTreeMap::new();
        // One account's rings when filtered, every account's when not.
        let one = account_id.and_then(|a| self.by_account.get(a));
        let all = account_id.is_none().then(|| self.by_account.values());
        for operations in one.into_iter().chain(all.into_iter().flatten()) {
            for (operation, rings) in operations {
                let ring = if fine { &rings.fine } else { &rings.coarse };
                let acc = by_operation.entry(operation).or_default();
                ring.sum_from(ring.first_slot(elapsed, window_secs), acc);
            }
        }
        by_operation
            .into_iter()
            .filter(|(_, acc)| acc.count > 0)
            .map(|(operation, acc)| {
                #[allow(clippy::cast_precision_loss)]
                let c = acc.count as f64;
                OperationSegments {
                    operation: operation.to_owned(),
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
            .collect()
    }

    /// Drops the rings of each account's operation whose newest coarse bucket
    /// has left the whole-ring window at `now`, then each account left with
    /// no operation. The fine ring's windows lie inside the coarse ring's, so
    /// no query answer changes; an account that has sent no request for the
    /// length of the coarse ring releases its memory to the allocator. The
    /// check is one comparison per pair.
    pub(super) fn prune(&mut self, now: Instant) {
        let elapsed = self.elapsed_secs(now);
        self.by_account.retain(|_, operations| {
            operations.retain(|_, rings| {
                let ring = &rings.coarse;
                let first = ring.first_slot(elapsed, None);
                ring.buckets.back().is_some_and(|b| b.slot >= first)
            });
            !operations.is_empty()
        });
    }

    /// Buckets held, the quantity the memory bound is stated in, and buckets
    /// allocated, held or spare. The bucket memory is the second number times
    /// [`Self::bucket_size`].
    #[cfg(test)]
    pub(super) fn buckets(&self) -> (usize, usize) {
        self.by_account
            .values()
            .flat_map(HashMap::values)
            .flat_map(|r| [&r.fine.buckets, &r.coarse.buckets])
            .fold((0, 0), |(held, allocated), b| {
                (held + b.len(), allocated + b.capacity())
            })
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

#[cfg(test)]
#[path = "segments_bound_tests.rs"]
mod bound_tests;
