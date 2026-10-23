// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Persisting `WarmThroughput` records, shared by every backend.
//!
//! A backend stores one JSON value per table and per global secondary index
//! (see `extenddb_core::types::WarmThroughputRecord`) and loads it back into
//! the description's `warm_throughput_record`; the engine derives the wire
//! member from it. Nothing else reads the value: it does not pre-warm, scale,
//! throttle or size anything.

use extenddb_core::types::{WarmThroughput, WarmThroughputRecord, epoch_seconds_now};

use crate::error::StorageError;

/// The epoch second until which a record written now reports `UPDATING`:
/// `delay_secs` from now, the control-plane delay that also times the
/// CREATING to ACTIVE flip. Zero delay means ACTIVE at once.
#[must_use]
pub fn warm_throughput_updating_until(delay_secs: f64) -> f64 {
    epoch_seconds_now() + delay_secs.max(0.0)
}

/// The record a create path stores for a request's `WarmThroughput`, or
/// `None` when the client sent none (the defaults are then reported).
#[must_use]
pub fn warm_throughput_record_for_create(
    spec: Option<&WarmThroughput>,
    delay_secs: f64,
) -> Option<WarmThroughputRecord> {
    spec.map(|s| WarmThroughputRecord::from_request(s, warm_throughput_updating_until(delay_secs)))
}

/// The record an update path stores over the stored JSON (`None` when the
/// resource never had one).
///
/// # Errors
///
/// `Internal` when the stored JSON does not parse.
pub fn warm_throughput_record_for_update(
    existing_json: Option<&str>,
    spec: &WarmThroughput,
    delay_secs: f64,
) -> Result<WarmThroughputRecord, StorageError> {
    let existing = parse_warm_throughput_record(existing_json)?;
    let now = epoch_seconds_now();
    Ok(WarmThroughputRecord::apply_update(
        existing,
        spec,
        now + delay_secs.max(0.0),
        now,
    ))
}

/// The stored JSON of a record.
///
/// # Errors
///
/// `Internal` on a serialization failure.
pub fn warm_throughput_record_json(record: &WarmThroughputRecord) -> Result<String, StorageError> {
    serde_json::to_string(record)
        .map_err(|e| StorageError::Internal(format!("warm throughput record: {e}")))
}

/// A record from its stored JSON; `None` in, `None` out.
///
/// # Errors
///
/// `Internal` when the text does not parse as a record.
pub fn parse_warm_throughput_record(
    json: Option<&str>,
) -> Result<Option<WarmThroughputRecord>, StorageError> {
    json.map(|text| {
        serde_json::from_str(text)
            .map_err(|e| StorageError::Internal(format!("warm throughput record: {e}")))
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_create_without_the_member_stores_nothing() {
        assert_eq!(warm_throughput_record_for_create(None, 0.25), None);
    }

    #[test]
    fn a_create_with_the_member_stores_it_updating_for_the_delay() {
        let spec = WarmThroughput {
            read_units_per_second: Some(20_000),
            write_units_per_second: None,
        };
        let record = warm_throughput_record_for_create(Some(&spec), 5.0).expect("stored");
        assert_eq!(record.read_units_per_second, Some(20_000));
        assert_eq!(record.write_units_per_second, None);
        assert!(record.is_updating_at(epoch_seconds_now() + 4.0));
        assert!(!record.is_updating_at(epoch_seconds_now() + 6.0));
    }

    #[test]
    fn an_update_merges_into_the_stored_json_and_round_trips() {
        let stored = warm_throughput_record_json(&WarmThroughputRecord {
            read_units_per_second: Some(100),
            write_units_per_second: Some(50),
            previous_read_units_per_second: None,
            previous_write_units_per_second: None,
            updating_until: 0.0,
        })
        .expect("json");
        let updated = warm_throughput_record_for_update(
            Some(&stored),
            &WarmThroughput {
                read_units_per_second: None,
                write_units_per_second: Some(75),
            },
            0.0,
        )
        .expect("merged");
        assert_eq!(updated.read_units_per_second, Some(100));
        assert_eq!(updated.write_units_per_second, Some(75));
        let again = parse_warm_throughput_record(Some(
            &warm_throughput_record_json(&updated).expect("json"),
        ))
        .expect("parses")
        .expect("present");
        assert_eq!(
            (again.read_units_per_second, again.write_units_per_second),
            (
                updated.read_units_per_second,
                updated.write_units_per_second
            )
        );
        // serde_json's float parser is not exact to the last bit without its
        // float_roundtrip feature; a microsecond is far below what the status
        // check can see.
        assert!((again.updating_until - updated.updating_until).abs() < 1e-6);
    }

    #[test]
    fn garbage_in_the_column_is_an_internal_error_not_a_default() {
        let err = parse_warm_throughput_record(Some("{\"x\":")).expect_err("refused");
        assert!(
            matches!(err, StorageError::Internal(m) if m.starts_with("warm throughput record:"))
        );
    }
}
