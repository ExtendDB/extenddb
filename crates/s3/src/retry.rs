// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transient S3 failures, retried by a store after the SDK's own attempts.
//! A call that fails with a connection failure, a timeout, a response that
//! could not be read, an HTTP 429, 500, 502, 503 or 504, or the codes
//! `SlowDown`, `RequestTimeout`, `InternalError` or `ServiceUnavailable` is
//! sent again after a wait that starts at [`FIRST_WAIT`] and doubles up to
//! [`MAX_WAIT`]; no new try starts once [`TOTAL`] has passed since the call
//! began, and the last failure then goes to the caller. Credentials that
//! cannot be resolved and every other client error are answered at once.

use std::time::{Duration, Instant};

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};

/// The first wait before a call is sent again.
pub const FIRST_WAIT: Duration = Duration::from_secs(1);
/// The longest wait between two tries of a call.
pub const MAX_WAIT: Duration = Duration::from_secs(30);
/// No try of a call starts once this long has passed since it began.
pub const TOTAL: Duration = Duration::from_secs(120);

/// The bound on one call's retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// The wait before the first retry.
    pub first: Duration,
    /// The longest wait between two tries.
    pub max: Duration,
    /// No try starts once this long has passed since the call began.
    pub total: Duration,
}

impl RetryPolicy {
    /// The store's bound: [`FIRST_WAIT`], [`MAX_WAIT`] and [`TOTAL`].
    pub const STORE: Self = Self {
        first: FIRST_WAIT,
        max: MAX_WAIT,
        total: TOTAL,
    };

    /// No retry: the first failure surfaces. A test hook, behind the
    /// `test-util` feature.
    #[cfg(any(test, feature = "test-util"))]
    pub const NONE: Self = Self {
        first: Duration::ZERO,
        max: Duration::ZERO,
        total: Duration::ZERO,
    };

    /// The waits of one call, starting now.
    pub fn start(self) -> Backoff {
        Backoff {
            policy: self,
            started: Instant::now(),
            retries: 0,
        }
    }

    /// The wait before retry number `retries` (from 0) of a call that has
    /// run for `elapsed`, or `None` once the call has spent its total: the
    /// first wait doubled per earlier retry, capped at the longest wait and
    /// at what is left of the total.
    pub fn wait(self, retries: u32, elapsed: Duration) -> Option<Duration> {
        let left = self
            .total
            .checked_sub(elapsed)
            .filter(|left| !left.is_zero())?;
        let doubled = self
            .first
            .checked_mul(1u32.checked_shl(retries).unwrap_or(u32::MAX))
            .unwrap_or(self.max);
        Some(doubled.min(self.max).min(left))
    }
}

/// The retries of one call.
pub struct Backoff {
    policy: RetryPolicy,
    started: Instant,
    retries: u32,
}

impl Backoff {
    /// The wait before the next try, or `None` once the call has spent its
    /// total; each `Some` counts one retry.
    pub fn next_wait(&mut self) -> Option<Duration> {
        let wait = self.policy.wait(self.retries, self.started.elapsed())?;
        self.retries += 1;
        Some(wait)
    }

    /// Retries granted so far.
    pub fn retries(&self) -> u32 {
        self.retries
    }
}

/// Whether a failed call is transient after the SDK's own attempts.
pub fn is_transient<E: ProvideErrorMetadata>(error: &SdkError<E, HttpResponse>) -> bool {
    match error {
        // A connection failure, a timeout, or a failure the HTTP client
        // marks as retryable, such as a response that ended before its
        // end. The SDK reports credentials it cannot resolve as a dispatch
        // failure of no kind; that one, and a request the SDK could not
        // build or send as asked, are answered at once.
        SdkError::DispatchFailure(failure) => {
            failure.is_io() || failure.is_timeout() || failure.as_other().is_some()
        }
        SdkError::TimeoutError(_) | SdkError::ResponseError(_) => true,
        SdkError::ServiceError(_) => {
            // 429 is how some S3-compatible services throttle; AWS S3
            // answers 503 SlowDown.
            let status = error.raw_response().map(|r| r.status().as_u16());
            matches!(status, Some(429 | 500 | 502 | 503 | 504))
                || matches!(
                    error.code(),
                    Some("SlowDown" | "RequestTimeout" | "InternalError" | "ServiceUnavailable")
                )
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_waits_double_from_one_second_to_thirty_within_two_minutes() {
        let policy = RetryPolicy::STORE;
        let mut elapsed = Duration::ZERO;
        let mut waits = Vec::new();
        // At most 64 retries are asked for, so a bound that never ends
        // fails the comparison below instead of looping.
        for retries in 0..64 {
            let Some(wait) = policy.wait(retries, elapsed) else {
                break;
            };
            waits.push(wait.as_secs());
            elapsed += wait;
        }
        assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30, 29]);
        assert_eq!(elapsed, TOTAL);
        assert_eq!(policy.wait(0, TOTAL), None);
        assert_eq!(policy.wait(40, Duration::ZERO), Some(MAX_WAIT));
        assert_eq!(RetryPolicy::NONE.wait(0, Duration::ZERO), None);
    }
}
