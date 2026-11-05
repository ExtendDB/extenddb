// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Write pacing for bulk put runners.
//!
//! A [`WritePacer`] is a token bucket denominated in write request units
//! (1 KB of item, rounded up, the same unit `BulkPutBytes` charges). Every
//! row write first takes its units from the bucket; when the bucket is in
//! deficit the runner sleeps until the deficit is repaid, so sustained
//! throughput converges on the configured rate.
//!
//! Two buckets pace a run:
//!
//! - the process bucket, shared by every runner in this server and sized by
//!   `limits.bulk_put_write_units_per_second`. It bounds how much of the
//!   storage write path bulk loads can take from request traffic on the same
//!   server. `0` disables it.
//! - the operation bucket, sized by the request's `MaxWriteRequestUnits`
//!   when present. The hosted service treats that field as a ceiling on the
//!   operation's write rate; pacing at or below it keeps a local run inside
//!   the same ceiling. Absent, only the process bucket applies.
//!
//! The bucket holds one second of capacity and starts full, so a run opens
//! at full rate and a short operation is not slowed. Tokens may go negative:
//! a row larger than the whole bucket is written after its deficit is paid
//! rather than refused, so a single 400 KB item at a low rate waits rather
//! than wedges. The clock is read under the lock, so two runners cannot
//! both see the same refill interval. The clock is tokio's, so tests drive
//! it with a paused runtime; outside tests it is the monotonic clock.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

/// Write request units for an item of `item_bytes` bytes: 1 KB each,
/// rounded up, at least one.
pub(crate) fn write_units(item_bytes: usize) -> u64 {
    u64::try_from(item_bytes.div_ceil(1024).max(1)).unwrap_or(u64::MAX)
}

#[derive(Debug)]
struct Bucket {
    /// Tokens on hand; negative is a deficit the next acquire sleeps off.
    tokens: f64,
    last_refill: Instant,
}

/// A token bucket in write request units. See the module documentation.
#[derive(Debug)]
pub(crate) struct WritePacer {
    /// Units per second, or `None` for no pacing.
    rate: Option<f64>,
    bucket: Mutex<Bucket>,
    /// Total time runners have slept on this bucket, in microseconds, for
    /// tests and the end-of-run log line.
    waited_micros: AtomicU64,
}

impl WritePacer {
    /// A pacer at `units_per_second`; `0` means unbounded.
    pub(crate) fn new(units_per_second: u64) -> Self {
        let rate = (units_per_second > 0).then_some(units_per_second as f64);
        Self {
            rate,
            bucket: Mutex::new(Bucket {
                tokens: rate.unwrap_or(0.0),
                last_refill: Instant::now(),
            }),
            waited_micros: AtomicU64::new(0),
        }
    }

    /// A pacer that never waits.
    #[cfg(test)]
    pub(crate) fn unbounded() -> Self {
        Self::new(0)
    }

    pub(crate) fn is_bounded(&self) -> bool {
        self.rate.is_some()
    }

    /// Total time spent waiting on this bucket so far.
    pub(crate) fn waited(&self) -> Duration {
        Duration::from_micros(self.waited_micros.load(Ordering::Relaxed))
    }

    /// Take `units` from the bucket, sleeping first if the bucket is in
    /// deficit after the take. Returns the time slept.
    pub(crate) async fn acquire(&self, units: u64) -> Duration {
        let Some(rate) = self.rate else {
            return Duration::ZERO;
        };
        let wait = {
            // A poisoned lock means a panic mid-update on another runner;
            // the bucket's numbers are still usable, so pacing continues.
            let mut bucket = self
                .bucket
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
            bucket.tokens = (bucket.tokens + elapsed * rate).min(rate);
            bucket.last_refill = now;
            bucket.tokens -= units as f64;
            if bucket.tokens >= 0.0 {
                Duration::ZERO
            } else {
                Duration::from_secs_f64(-bucket.tokens / rate)
            }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
            self.waited_micros.fetch_add(
                u64::try_from(wait.as_micros()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        wait
    }

    /// The tokens the bucket held when it last settled (the last
    /// `acquire`), with no refill applied for the time since. Tests read
    /// the balance here rather than through a timed `acquire`, whose sleep
    /// drifts under host load.
    #[cfg(test)]
    pub(crate) fn settled_tokens(&self) -> f64 {
        self.bucket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_units_round_up_to_a_kilobyte_with_a_floor_of_one() {
        assert_eq!(write_units(0), 1);
        assert_eq!(write_units(1), 1);
        assert_eq!(write_units(1024), 1);
        assert_eq!(write_units(1025), 2);
        assert_eq!(write_units(400 * 1024), 400);
    }

    #[tokio::test]
    async fn an_unbounded_pacer_never_waits() {
        let pacer = WritePacer::unbounded();
        assert!(!pacer.is_bounded());
        for _ in 0..1000 {
            assert_eq!(pacer.acquire(400).await, Duration::ZERO);
        }
        assert_eq!(pacer.waited(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_bounded_pacer_opens_at_full_rate_then_converges_on_it() {
        let pacer = WritePacer::new(10);
        // The first second of capacity is free.
        for _ in 0..10 {
            assert_eq!(pacer.acquire(1).await, Duration::ZERO);
        }
        // Ten more units at 10/s take one second in total.
        let before = Instant::now();
        for _ in 0..10 {
            pacer.acquire(1).await;
        }
        let elapsed = before.elapsed();
        assert!(
            elapsed >= Duration::from_millis(950) && elapsed <= Duration::from_millis(1100),
            "20 units at 10/s from a full bucket take about one second, got {elapsed:?}"
        );
        assert!(pacer.waited() >= Duration::from_millis(950));
    }

    #[tokio::test(start_paused = true)]
    async fn a_row_larger_than_the_bucket_waits_for_its_deficit_rather_than_wedging() {
        let pacer = WritePacer::new(100);
        // 400 units against a 100-unit bucket: written now, then a
        // three-second deficit before the next row.
        let first = pacer.acquire(400).await;
        assert_eq!(first, Duration::from_secs(3));
        let before = Instant::now();
        pacer.acquire(1).await;
        // The deficit was repaid during the first sleep; the bucket is at
        // roughly zero, so a one-unit row waits about 10 ms.
        assert!(before.elapsed() <= Duration::from_millis(50));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_refills_at_most_one_second_of_capacity() {
        let pacer = WritePacer::new(10);
        tokio::time::advance(Duration::from_secs(60)).await;
        // Sixty idle seconds do not bank sixty seconds of burst.
        for _ in 0..10 {
            assert_eq!(pacer.acquire(1).await, Duration::ZERO);
        }
        assert!(pacer.acquire(1).await > Duration::ZERO);
    }
}
