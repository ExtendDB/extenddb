// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WarmThroughput` shapes and the stored record.

use super::*;

fn on_demand() -> WarmThroughputDefaults {
    WarmThroughputDefaults::for_billing(true, 0, 0)
}

#[test]
fn on_demand_defaults_are_the_measured_12000_and_4000() {
    assert_eq!(
        on_demand(),
        WarmThroughputDefaults {
            read_units_per_second: 12_000,
            write_units_per_second: 4_000,
            pay_per_request: true,
        }
    );
}

#[test]
fn provisioned_defaults_are_the_provisioned_units() {
    assert_eq!(
        WarmThroughputDefaults::for_billing(false, 1, 1),
        WarmThroughputDefaults {
            read_units_per_second: 1,
            write_units_per_second: 1,
            pay_per_request: false,
        }
    );
}

/// Rows T7a to T7d: a read-only record `{100}` on a provisioned 5/5 table
/// reads `{100, 5}`, follows a capacity change to `{100, 10}`, and resets to
/// `{12000, 4000}` once the table is on-demand (a side below the on-demand
/// default is reported as the default), after which `12000/4000` is not a
/// decrease.
#[test]
fn an_on_demand_resource_reports_at_least_the_on_demand_defaults() {
    let record = WarmThroughputRecord::from_request(
        &WarmThroughput {
            read_units_per_second: Some(100),
            write_units_per_second: None,
        },
        0.0,
    );
    assert_eq!(
        record.effective(WarmThroughputDefaults::for_billing(false, 5, 5)),
        (100, 5)
    );
    assert_eq!(
        record.effective(WarmThroughputDefaults::for_billing(false, 10, 10)),
        (100, 10)
    );
    assert_eq!(record.effective(on_demand()), (12_000, 4_000));
    // A stored side above the default is kept on an on-demand resource.
    let high = WarmThroughputRecord::from_request(
        &WarmThroughput {
            read_units_per_second: Some(20_000),
            write_units_per_second: Some(50),
        },
        0.0,
    );
    assert_eq!(high.effective(on_demand()), (20_000, 4_000));
}

/// Row T1: `WarmThroughput: {}` on CreateTable reports no member while the
/// table is CREATING, and the defaults once ACTIVE.
#[test]
fn an_empty_request_reports_no_member_while_creating() {
    let record = WarmThroughputRecord::from_request(&WarmThroughput::default(), 10.0);
    assert_eq!(
        WarmThroughputRecord::describe_for(Some(&record), true, on_demand(), 1.0),
        None
    );
    assert_eq!(
        WarmThroughputRecord::describe_for(Some(&record), false, on_demand(), 11.0),
        Some(on_demand().describe())
    );
}

/// Rows U2, S2a, S2c, U3 and S2b: while UPDATING the record reports the pair
/// the window started from, and that pair is what a decrease is measured
/// against; once settled the new pair is reported and compared.
#[test]
fn the_previous_pair_is_reported_and_compared_while_updating() {
    // Never set (defaults 12000/4000), raised to 24000/8000 at t=0, window to 10.
    let raised = WarmThroughputRecord::apply_update(
        None,
        &WarmThroughput {
            read_units_per_second: Some(24_000),
            write_units_per_second: Some(8_000),
        },
        10.0,
        0.0,
    );
    assert_eq!(raised.reported(on_demand(), 5.0), (12_000, 4_000));
    assert_eq!(
        raised.describe(on_demand(), 5.0),
        WarmThroughputDescription {
            read_units_per_second: Some(12_000),
            write_units_per_second: Some(4_000),
            status: WarmThroughputStatus::Updating,
        }
    );
    assert_eq!(raised.reported(on_demand(), 10.0), (24_000, 8_000));
    // U3: inside the window a request carrying the previous values validates
    // against 12000/4000 and, applied, keeps reporting 12000/4000.
    let inside = WarmThroughputRecord::apply_update(
        Some(raised),
        &WarmThroughput {
            read_units_per_second: Some(12_000),
            write_units_per_second: Some(4_000),
        },
        15.0,
        5.0,
    );
    assert_eq!(inside.reported(on_demand(), 6.0), (12_000, 4_000));
    assert_eq!(inside.reported(on_demand(), 15.0), (12_000, 4_000));
    // S2c: the same values re-sent on a settled table report themselves UPDATING.
    let same = WarmThroughputRecord::apply_update(
        Some(raised),
        &WarmThroughput {
            read_units_per_second: Some(24_000),
            write_units_per_second: Some(8_000),
        },
        20.0,
        11.0,
    );
    assert_eq!(same.reported(on_demand(), 12.0), (24_000, 8_000));
    assert_eq!(
        same.describe(on_demand(), 12.0).status,
        WarmThroughputStatus::Updating
    );
}

/// The service omits the member on a CREATING table that was created
/// without one, and reports the defaults ACTIVE once the table is.
#[test]
fn no_record_is_absent_while_creating_and_defaults_once_active() {
    assert_eq!(
        WarmThroughputRecord::describe_for(None, true, on_demand(), 0.0),
        None
    );
    let json = serde_json::to_value(
        WarmThroughputRecord::describe_for(None, false, on_demand(), 0.0).expect("present"),
    )
    .expect("serialises");
    assert_eq!(
        json,
        serde_json::json!({
            "ReadUnitsPerSecond": 12000,
            "WriteUnitsPerSecond": 4000,
            "Status": "ACTIVE"
        })
    );
}

/// Measured: `{ReadUnitsPerSecond: 20000}` on CreateTable was echoed as
/// `{ReadUnitsPerSecond: 20000, Status: UPDATING}` with no write member,
/// and read back as `{20000, 4000, ACTIVE}` once the table was ACTIVE.
#[test]
fn a_partial_request_is_echoed_while_creating_and_filled_once_active() {
    let spec = WarmThroughput {
        read_units_per_second: Some(20_000),
        write_units_per_second: None,
    };
    let record = WarmThroughputRecord::from_request(&spec, 10.0);
    let creating = serde_json::to_value(
        WarmThroughputRecord::describe_for(Some(&record), true, on_demand(), 1.0).expect("present"),
    )
    .expect("serialises");
    assert_eq!(
        creating,
        serde_json::json!({ "ReadUnitsPerSecond": 20000, "Status": "UPDATING" })
    );
    let active = WarmThroughputRecord::describe_for(Some(&record), false, on_demand(), 11.0)
        .expect("present");
    assert_eq!(
        active,
        WarmThroughputDescription {
            read_units_per_second: Some(20_000),
            write_units_per_second: Some(4_000),
            status: WarmThroughputStatus::Active,
        }
    );
}

#[test]
fn the_status_is_updating_until_the_deadline_then_active() {
    let record = WarmThroughputRecord::from_request(&WarmThroughput::default(), 100.0);
    assert_eq!(
        record.describe(on_demand(), 99.9).status,
        WarmThroughputStatus::Updating
    );
    assert_eq!(
        record.describe(on_demand(), 100.0).status,
        WarmThroughputStatus::Active
    );
}

/// An update names one side: that side changes, the other keeps its stored
/// value, and the deadline restarts.
#[test]
fn an_update_replaces_only_the_sides_it_names() {
    let existing = WarmThroughputRecord {
        read_units_per_second: Some(100),
        write_units_per_second: Some(50),
        previous_read_units_per_second: None,
        previous_write_units_per_second: None,
        updating_until: 5.0,
    };
    let updated = WarmThroughputRecord::apply_update(
        Some(existing),
        &WarmThroughput {
            read_units_per_second: Some(200),
            write_units_per_second: None,
        },
        9.0,
        6.0,
    );
    assert_eq!(
        updated,
        WarmThroughputRecord {
            read_units_per_second: Some(200),
            write_units_per_second: Some(50),
            previous_read_units_per_second: Some(100),
            previous_write_units_per_second: Some(50),
            updating_until: 9.0,
        }
    );
    // No stored record: the unnamed side stays unset and reports the default.
    let fresh = WarmThroughputRecord::apply_update(
        None,
        &WarmThroughput {
            read_units_per_second: None,
            write_units_per_second: Some(8_000),
        },
        9.0,
        6.0,
    );
    assert_eq!(fresh.effective(on_demand()), (12_000, 8_000));
}

/// The persisted shape round-trips, and a record written without a
/// deadline (or a side) reads back.
#[test]
fn the_record_round_trips_through_json() {
    let record = WarmThroughputRecord {
        read_units_per_second: Some(1),
        write_units_per_second: None,
        previous_read_units_per_second: Some(3),
        previous_write_units_per_second: None,
        updating_until: 1.5,
    };
    let text = serde_json::to_string(&record).expect("serialises");
    assert_eq!(
        serde_json::from_str::<WarmThroughputRecord>(&text).expect("parses"),
        record
    );
    let sparse: WarmThroughputRecord =
        serde_json::from_str(r#"{"WriteUnitsPerSecond": 7}"#).expect("parses");
    assert_eq!(sparse.write_units_per_second, Some(7));
    assert_eq!(sparse.updating_until, 0.0);
}

#[test]
fn the_request_shape_accepts_either_side_alone() {
    let read_only: WarmThroughput =
        serde_json::from_str(r#"{"ReadUnitsPerSecond": 20000}"#).expect("parses");
    assert_eq!(read_only.read_units_per_second, Some(20_000));
    assert_eq!(read_only.write_units_per_second, None);
    let empty: WarmThroughput = serde_json::from_str("{}").expect("parses");
    assert_eq!(empty, WarmThroughput::default());
}

/// Rows S8a and V5: an index created by `UpdateTable` is past CREATING at
/// once here, and inside its window reports what was requested with
/// `UPDATING`, then the same values `ACTIVE`; a side left out reports the
/// default.
#[test]
fn a_created_resource_reports_the_request_while_updating() {
    let record = WarmThroughputRecord::from_request(
        &WarmThroughput {
            read_units_per_second: Some(30_000),
            write_units_per_second: Some(8_000),
        },
        10.0,
    );
    assert_eq!(
        WarmThroughputRecord::describe_for(Some(&record), false, on_demand(), 5.0),
        Some(WarmThroughputDescription {
            read_units_per_second: Some(30_000),
            write_units_per_second: Some(8_000),
            status: WarmThroughputStatus::Updating,
        })
    );
    assert_eq!(
        WarmThroughputRecord::describe_for(Some(&record), false, on_demand(), 10.0),
        Some(WarmThroughputDescription {
            read_units_per_second: Some(30_000),
            write_units_per_second: Some(8_000),
            status: WarmThroughputStatus::Active,
        })
    );
    let partial = WarmThroughputRecord::from_request(
        &WarmThroughput {
            read_units_per_second: Some(20_000),
            write_units_per_second: None,
        },
        10.0,
    );
    assert_eq!(partial.reported(on_demand(), 5.0), (20_000, 4_000));
}

/// Row S9b: the service refuses `UpdateTable` with `WarmThroughput: {}` with
/// an empty `InternalFailure`; ExtendDB accepts it, keeps both values and
/// restarts the window.
#[test]
fn an_empty_update_keeps_the_values_and_restarts_the_window() {
    let settled = WarmThroughputRecord::apply_update(
        None,
        &WarmThroughput {
            read_units_per_second: Some(24_000),
            write_units_per_second: Some(8_000),
        },
        1.0,
        0.0,
    );
    let empty =
        WarmThroughputRecord::apply_update(Some(settled), &WarmThroughput::default(), 12.0, 2.0);
    assert_eq!(empty.effective(on_demand()), (24_000, 8_000));
    assert_eq!(empty.reported(on_demand(), 3.0), (24_000, 8_000));
    assert!(empty.is_updating_at(3.0));
}
