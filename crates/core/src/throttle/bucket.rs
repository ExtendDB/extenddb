// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The token bucket every throttle limit is built from.

use std::time::Instant;

/// Default burst window: 300 seconds of accumulated capacity.
const BURST_SECONDS: f64 = 300.0;

/// A single token bucket that refills at a constant rate.
#[derive(Debug)]
pub(super) struct TokenBucket {
    /// Current token count (can go negative after a large operation).
    tokens: f64,
    /// Maximum tokens (burst capacity).
    max_tokens: f64,
    /// Tokens added per second (= provisioned CU/s).
    refill_rate: f64,
    /// Last time tokens were refilled. `None` until the first operation,
    /// preventing burst accumulation during idle time between bucket
    /// creation and first use.
    last_refill: Option<Instant>,
}

impl TokenBucket {
    pub(super) fn new(refill_rate: f64) -> Self {
        let max_tokens = refill_rate * BURST_SECONDS;
        // Start with 1 second of capacity rather than the full burst window.
        // Burst accumulates over time as unused capacity builds up, matching
        // real DynamoDB's "unused capacity" burst model. Starting full would
        // make throttling untestable for newly created tables.
        //
        // `last_refill` starts as `None` — the refill clock only begins on
        // the first `has_capacity` or `consume` call. This prevents idle
        // time between table creation and first data operation from
        // accumulating burst tokens.
        Self {
            tokens: refill_rate,
            max_tokens,
            refill_rate,
            last_refill: None,
        }
    }

    /// Refill tokens based on elapsed time since the last operation.
    /// On the first call, starts the refill clock without adding tokens.
    fn refill(&mut self, now: Instant) {
        if let Some(last) = self.last_refill {
            let elapsed = now.duration_since(last).as_secs_f64();
            if elapsed > 0.0 {
                self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
            }
        }
        self.last_refill = Some(now);
    }

    /// Check if the bucket has any tokens available (pre-dispatch check).
    pub(super) fn has_capacity(&mut self, now: Instant) -> bool {
        self.refill(now);
        self.tokens > 0.0
    }

    /// Consume tokens after a successful operation.
    pub(super) fn consume(&mut self, amount: f64, now: Instant) {
        self.refill(now);
        self.tokens -= amount;
    }

    /// Update the refill rate (e.g., after `UpdateTable` changes throughput).
    pub(super) fn update_rate(&mut self, new_rate: f64) {
        let old_max = self.max_tokens;
        self.max_tokens = new_rate * BURST_SECONDS;
        self.refill_rate = new_rate;
        // Scale current tokens proportionally.
        if old_max > 0.0 {
            self.tokens = (self.tokens / old_max * self.max_tokens).min(self.max_tokens);
        } else {
            self.tokens = self.max_tokens;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_starts_with_one_second_capacity() {
        let bucket = TokenBucket::new(100.0);
        assert_eq!(bucket.max_tokens, 100.0 * BURST_SECONDS);
        assert_eq!(bucket.tokens, 100.0); // 1 second of capacity
        assert!(bucket.last_refill.is_none()); // lazy — clock not started
    }

    #[test]
    fn token_bucket_consume_and_refill() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new(100.0);
        // First consume starts the refill clock, then deducts tokens.
        bucket.consume(100.0, now);
        assert!(bucket.tokens <= 0.0);
        assert!(!bucket.has_capacity(now));
    }

    #[test]
    fn token_bucket_no_burst_accumulation_before_first_use() {
        let mut bucket = TokenBucket::new(5.0);
        // Simulate idle time: create bucket, wait, then check capacity.
        // With lazy refill, the bucket should still have only 5 tokens
        // (1 second of capacity) regardless of wall-clock delay.
        let later = Instant::now();
        // First call starts the clock — no tokens added.
        assert!(bucket.has_capacity(later));
        assert_eq!(bucket.tokens, 5.0); // unchanged from initial
    }

    #[test]
    fn update_rate_adjusts_bucket() {
        let mut bucket = TokenBucket::new(100.0);
        assert_eq!(bucket.max_tokens, 100.0 * BURST_SECONDS);
        bucket.update_rate(200.0);
        assert_eq!(bucket.max_tokens, 200.0 * BURST_SECONDS);
        assert_eq!(bucket.refill_rate, 200.0);
    }
}
