// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the startup configuration checks.

use super::*;

#[test]
fn a_request_timeout_of_zero_is_refused() {
    let err = check_request_timeout_secs(0).expect_err("0 is refused");
    assert!(err.contains("must be at least 1"), "{err}");
}

#[test]
fn a_request_timeout_above_a_day_is_refused() {
    let err =
        check_request_timeout_secs(MAX_REQUEST_TIMEOUT_SECS + 1).expect_err("86401 is refused");
    assert!(err.contains("must be at most 86400"), "{err}");

    // The value that panicked every connection task before the ceiling.
    let err = check_request_timeout_secs(u64::MAX).expect_err("u64::MAX is refused");
    assert!(err.contains("must be at most 86400"), "{err}");
}

#[test]
fn the_accepted_range_runs_from_one_second_to_a_day() {
    for secs in [1, 30, 3600, MAX_REQUEST_TIMEOUT_SECS] {
        check_request_timeout_secs(secs).unwrap_or_else(|e| panic!("{secs} is accepted: {e}"));
    }
}

/// The ceiling keeps the per-connection timers clear of the overflow that
/// made every connection task panic: the deadline hyper builds for a
/// connection is the current instant plus the timeout.
#[test]
fn the_ceiling_added_to_the_current_instant_does_not_overflow() {
    let now = std::time::Instant::now();
    assert!(
        now.checked_add(Duration::from_secs(MAX_REQUEST_TIMEOUT_SECS))
            .is_some(),
        "the ceiling must be addable to an instant"
    );
    assert!(
        now.checked_add(Duration::from_secs(u64::MAX)).is_none(),
        "u64::MAX seconds overflows, which is what the ceiling refuses"
    );
}
