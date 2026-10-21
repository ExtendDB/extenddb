// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The base-row `version` token.
//!
//! Every base-row mutation is serialized by a lightweight transaction that
//! compares `version` with the value the writer read (`IF version = ?`). The
//! value written back has two jobs:
//!
//! 1. It is the OCC token the next writer must present, so it only has to
//!    differ from every earlier value of the same row.
//! 2. It is the write timestamp of the secondary-index rows that the write
//!    produces. Index rows are written in a non-LWT batch after the base row
//!    commits, so two writers' batches can reach the index table in the
//!    opposite order from their base-row commits. Cassandra keeps the cell or
//!    tombstone with the larger timestamp, and a tombstone wins a tie, so a
//!    later version's index mutation beats an earlier version's whatever the
//!    arrival order, as long as the timestamps increase with the version.
//!
//! The value is therefore `max(previous + 1, now)` in microseconds since the
//! epoch. It is strictly increasing across the row's history by the `+ 1`,
//! and it stays in the same domain as the wall-clock timestamps Cassandra
//! assigns to writes that carry none (index backfill, legacy rows, TTL
//! expiration), so those writes keep their relative order against it.
//!
//! A row that a transaction created under the earlier scheme carries a null
//! `version`. Readers keep that as `None`, the fence binds a null (which
//! `IF version = null` matches), and the next write gives the row a value.

/// Microseconds since the Unix epoch, the domain of Cassandra write timestamps.
pub fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

/// The version a write stores for a row whose current version is `previous`
/// (`None` when the row is absent or has never been versioned), and the
/// write timestamp of the index rows that write produces.
pub fn next_row_version(previous: Option<i64>) -> i64 {
    next_row_version_at(previous, now_micros())
}

/// [`next_row_version`] with the clock reading supplied, for tests and for
/// callers that pinned a timestamp earlier in the same request.
pub fn next_row_version_at(previous: Option<i64>, now: i64) -> i64 {
    match previous {
        Some(previous) => previous.saturating_add(1).max(now),
        None => now,
    }
}

/// The value bound to the `IF version = ?` fence for a row whose version was
/// read as `previous`. A null column only matches a null operand, so the
/// fence binds `Null` rather than a sentinel number.
pub fn fence_value(previous: Option<i64>) -> cdrs_tokio::types::value::Value {
    cdrs_tokio::types::value::Value::from(previous)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_row_takes_the_clock() {
        assert_eq!(next_row_version_at(None, 1_000), 1_000);
    }

    #[test]
    fn a_later_write_exceeds_the_previous_version_even_when_the_clock_is_behind() {
        assert_eq!(next_row_version_at(Some(5_000), 1_000), 5_001);
    }

    #[test]
    fn a_later_write_takes_the_clock_when_it_is_ahead() {
        assert_eq!(next_row_version_at(Some(1_000), 5_000), 5_000);
    }

    #[test]
    fn the_sequence_is_strictly_increasing_whatever_the_clock_does() {
        let clock = [100, 50, 50, 200, 199, 1_000, 999];
        let mut previous = None;
        for now in clock {
            let next = next_row_version_at(previous, now);
            if let Some(previous) = previous {
                assert!(next > previous, "{next} must exceed {previous}");
            }
            previous = Some(next);
        }
    }

    #[test]
    fn the_largest_version_does_not_wrap() {
        assert_eq!(next_row_version_at(Some(i64::MAX), 1), i64::MAX);
    }

    #[test]
    fn a_null_version_fences_as_null_and_a_number_as_itself() {
        use cdrs_tokio::types::value::Value;
        assert!(matches!(fence_value(None), Value::Null));
        assert!(matches!(fence_value(Some(7)), Value::Some(_)));
    }
}
