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
/// resource never had one, or when it does not parse).
#[must_use]
pub fn warm_throughput_record_for_update(
    existing_json: Option<&str>,
    spec: &WarmThroughput,
    delay_secs: f64,
) -> WarmThroughputRecord {
    let existing = parse_warm_throughput_record(existing_json);
    let now = epoch_seconds_now();
    WarmThroughputRecord::apply_update(existing, spec, now + delay_secs.max(0.0), now)
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
/// A value that does not parse as a record (a hand edit, or a record shape
/// this build does not know) is logged and read as never set, so the
/// resource reports the defaults for its billing mode, as the neighbouring
/// `on_demand_throughput` column does, and an `UpdateTable` that sets the
/// member writes a record over it.
#[must_use]
pub fn parse_warm_throughput_record(json: Option<&str>) -> Option<WarmThroughputRecord> {
    let text = json?;
    match serde_json::from_str(text) {
        Ok(record) => Some(record),
        Err(e) => {
            tracing::warn!(
                "stored warm_throughput does not parse as a record, reporting the defaults: {e}"
            );
            None
        }
    }
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
        );
        assert_eq!(updated.read_units_per_second, Some(100));
        assert_eq!(updated.write_units_per_second, Some(75));
        let again = parse_warm_throughput_record(Some(
            &warm_throughput_record_json(&updated).expect("json"),
        ))
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

    /// A value that does not parse reads as never set (the defaults are
    /// reported), and an update writes a whole record over it.
    #[test]
    fn garbage_in_the_column_reads_as_never_set_and_an_update_replaces_it() {
        for garbage in ["{\"x\":", "{\"ReadUnitsPerSecond\": \"lots\"}", "\"text\""] {
            assert_eq!(
                parse_warm_throughput_record(Some(garbage)),
                None,
                "{garbage}"
            );
            let updated = warm_throughput_record_for_update(
                Some(garbage),
                &WarmThroughput {
                    read_units_per_second: Some(13_000),
                    write_units_per_second: None,
                },
                0.0,
            );
            assert_eq!(updated.read_units_per_second, Some(13_000));
            assert_eq!(updated.write_units_per_second, None);
        }
    }
}
