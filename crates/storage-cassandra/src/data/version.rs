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
//! assigns to writes that carry none (legacy rows, TTL expiration), so those
//! writes keep their relative order against it.
//!
//! A delete leaves no version behind, so the create that follows it has no
//! `previous` and stamps its index rows with its own clock reading, taken
//! before its LWT. The delete's index tombstones are therefore stamped from
//! the deleted row's version rather than from the clock ([`delete_stamp`]).
//!
//! A row that a transaction created under the earlier scheme carries a null
//! `version`. Readers keep that as `None`, the fence binds a null (which
//! `IF version = null` matches), and the next write gives the row a value.
//!
//! `previous + 1` carries a version written from a host whose clock runs
//! ahead forward for the rest of the row's history, so the ExtendDB hosts'
//! clocks must agree. A write that finds a version more than
//! [`AHEAD_WARNING_MICROS`] ahead of its own clock logs a warning, at most
//! once a minute. Only index rows carry the version as their write timestamp:
//! the queue rows and stream records written beside them keep Cassandra's,
//! so the GSI worker's delete of an applied queue entry, which carries
//! Cassandra's timestamp, always lands above the entry.

use std::sync::atomic::{AtomicI64, Ordering};

use extenddb_storage::error::StorageError;

/// How far ahead of the writer's clock a stored version may be before the
/// write logs a warning: one second, in microseconds.
pub const AHEAD_WARNING_MICROS: i64 = 1_000_000;

/// Microseconds since the Unix epoch, the domain of Cassandra write timestamps.
pub fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

/// The version a write stores for a row whose current version is `previous`
/// (`None` when the row is absent or has never been versioned), and the
/// write timestamp of the index rows that write produces.
///
/// # Errors
/// [`StorageError::Internal`] when `previous` is the largest `bigint`: the
/// next version would equal it, and the fence could not tell the two writes
/// apart.
pub fn next_row_version(previous: Option<i64>) -> Result<i64, StorageError> {
    next_row_version_at(previous, now_micros())
}

/// [`next_row_version`] with the clock reading supplied, for tests and for
/// callers that pinned a timestamp earlier in the same request.
///
/// # Errors
/// As [`next_row_version`].
pub fn next_row_version_at(previous: Option<i64>, now: i64) -> Result<i64, StorageError> {
    let Some(previous) = previous else {
        return Ok(now);
    };
    let next = previous.checked_add(1).ok_or_else(version_exhausted)?;
    if next > now.saturating_add(AHEAD_WARNING_MICROS) {
        warn_version_ahead(next, now);
    }
    Ok(next.max(now))
}

fn version_exhausted() -> StorageError {
    StorageError::Internal("the item's version has reached the largest bigint".to_owned())
}

/// Log that a row's version is ahead of this host's clock, at most once a
/// minute.
fn warn_version_ahead(next: i64, now: i64) {
    static LAST_WARNED: AtomicI64 = AtomicI64::new(i64::MIN);
    let last = LAST_WARNED.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 60_000_000
        || LAST_WARNED
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    tracing::warn!(
        ahead_micros = next.saturating_sub(now),
        "an item's version is ahead of this host's clock; the ExtendDB hosts' clocks \
         disagree, or one ran ahead when it wrote the item"
    );
}

/// Versions at or above this value are clock readings this scheme stored. A
/// row last written before the scheme carries a null or a write counter, and a
/// counter starts at 1 and does not reach 10^15 (September 2001 in
/// microseconds since the epoch).
pub const CLOCK_VERSION_FLOOR: i64 = 1_000_000_000_000_000;

/// The write timestamp of the index tombstones that a delete of a row whose
/// version was read as `previous` writes. The delete takes it before its fence.
///
/// Every index row of a row this scheme wrote carries a stamp no greater than
/// `previous`, so `previous + 1` removes all of them whenever the delete runs.
/// A create of the same key that commits after the delete stamps its index
/// rows with a clock reading taken before its own LWT. When the create was
/// sent after the write that stored `previous`, that reading exceeds
/// `previous + 1` and the create's rows land above the tombstones, even when
/// the create read its clock before the delete did. A stamp read from the
/// clock after the fence ordered the two by their clock readings instead, and
/// a create that read its clock first and committed second lost its index
/// rows. A create that read its clock before the write that stored `previous`
/// can still land under the tombstones; ADR-0027 records that case.
///
/// A row with a null or counter version has index rows stamped with
/// coordinator time, which `previous + 1` does not exceed, so its delete takes
/// the clock.
///
/// # Errors
/// As [`next_row_version`], when `previous` is the largest `bigint`.
pub fn delete_stamp(previous: Option<i64>) -> Result<i64, StorageError> {
    match previous {
        Some(previous) if previous >= CLOCK_VERSION_FLOOR => {
            previous.checked_add(1).ok_or_else(version_exhausted)
        }
        _ => Ok(now_micros()),
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
        assert_eq!(next_row_version_at(None, 1_000).unwrap(), 1_000);
    }

    #[test]
    fn a_later_write_exceeds_the_previous_version_even_when_the_clock_is_behind() {
        assert_eq!(next_row_version_at(Some(5_000), 1_000).unwrap(), 5_001);
    }

    #[test]
    fn a_later_write_takes_the_clock_when_it_is_ahead() {
        assert_eq!(next_row_version_at(Some(1_000), 5_000).unwrap(), 5_000);
    }

    #[test]
    fn the_sequence_is_strictly_increasing_whatever_the_clock_does() {
        let clock = [100, 50, 50, 200, 199, 1_000, 999];
        let mut previous = None;
        for now in clock {
            let next = next_row_version_at(previous, now).unwrap();
            if let Some(previous) = previous {
                assert!(next > previous, "{next} must exceed {previous}");
            }
            previous = Some(next);
        }
    }

    #[test]
    fn a_write_or_delete_of_a_row_at_the_largest_version_is_refused() {
        assert!(next_row_version_at(Some(i64::MAX), 1).is_err());
        assert!(delete_stamp(Some(i64::MAX)).is_err());
        assert_eq!(
            next_row_version_at(Some(i64::MAX - 1), 1).unwrap(),
            i64::MAX
        );
    }

    #[test]
    fn a_row_this_scheme_wrote_is_deleted_one_above_its_version() {
        let version = 1_762_000_000_000_000;
        assert_eq!(delete_stamp(Some(version)).unwrap(), version + 1);
        assert_eq!(
            delete_stamp(Some(CLOCK_VERSION_FLOOR)).unwrap(),
            CLOCK_VERSION_FLOOR + 1
        );
    }

    #[test]
    fn a_row_with_a_counter_or_null_version_is_deleted_at_the_clock() {
        let before = now_micros();
        assert!(delete_stamp(Some(7)).unwrap() >= before);
        assert!(delete_stamp(Some(CLOCK_VERSION_FLOOR - 1)).unwrap() >= before);
        assert!(delete_stamp(None).unwrap() >= before);
    }

    #[test]
    fn a_null_version_fences_as_null_and_a_number_as_itself() {
        use cdrs_tokio::types::value::Value;
        assert!(matches!(fence_value(None), Value::Null));
        assert!(matches!(fence_value(Some(7)), Value::Some(_)));
    }
}
