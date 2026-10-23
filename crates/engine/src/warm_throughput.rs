// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateTable` checks for `WarmThroughput`, against the table's current
//! description.
//!
//! The create-time rules need only the request; these need what the table
//! reports now (an update may not lower a value, and a created index follows
//! the table's billing mode), so they run after a `DescribeTable` of the
//! target. Measured against Amazon DynamoDB on 2026-10-23; see
//! `extenddb_core::validation::warm_throughput` for the sentences.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    BillingMode, ON_DEMAND_WARM_READ_UNITS, ON_DEMAND_WARM_WRITE_UNITS, TableDescription,
    UpdateTableInput, WarmThroughput, WarmThroughputDefaults, epoch_seconds_now,
};
use extenddb_core::validation::{
    WarmThroughputSubject, validate_created_gsi_warm_throughput, validate_warm_throughput_on_update,
};

/// Whether `input` carries any `WarmThroughput`, on the table or in a
/// `GlobalSecondaryIndexUpdates` action.
#[must_use]
pub(crate) fn touches_warm_throughput(input: &UpdateTableInput) -> bool {
    input.warm_throughput.is_some()
        || input
            .global_secondary_index_updates
            .iter()
            .flatten()
            .any(|u| {
                u.update
                    .as_ref()
                    .is_some_and(|a| a.warm_throughput.is_some())
                    || u.create
                        .as_ref()
                        .is_some_and(|a| a.warm_throughput.is_some())
            })
}

/// The warm throughput the table reports now (the pair the window started
/// from while UPDATING, measured rows U3 and S2b) and the billing mode it is
/// under, from a description the backend returned.
fn current_table_values(existing: &TableDescription) -> (BillingMode, (i64, i64)) {
    let now = epoch_seconds_now();
    let pay_per_request = existing
        .billing_mode_summary
        .as_ref()
        .is_some_and(|s| s.billing_mode == BillingMode::PayPerRequest);
    let defaults = WarmThroughputDefaults::for_billing(
        pay_per_request,
        existing.provisioned_throughput.read_capacity_units,
        existing.provisioned_throughput.write_capacity_units,
    );
    let current = existing.warm_throughput_record.as_ref().map_or_else(
        || {
            (
                defaults.read_units_per_second,
                defaults.write_units_per_second,
            )
        },
        |r| r.reported(defaults, now),
    );
    let mode = if pay_per_request {
        BillingMode::PayPerRequest
    } else {
        BillingMode::Provisioned
    };
    (mode, current)
}

/// The `WarmThroughput` an `UpdateTable` that switches a provisioned table to
/// PAY_PER_REQUEST stores with the switch, or `None` when the switch changes
/// nothing about it.
///
/// Measured against Amazon DynamoDB on 2026-10-23: a provisioned table whose
/// warm throughput is below the on-demand minimum reads 12000 read and 4000
/// write units once it is on-demand, whether the client set it (`{100}` on a
/// 5/5 table, rows T7a to T7d; `100/50` on an explicit table, row S7c) or
/// never did (a never-set 10/10 table, row S7a); a side at or above the
/// minimum is kept. The values stay when the table is switched back to
/// provisioned (row S7b: `12000/4000` after the flip back to 5/5), so they are
/// stored, not derived from the billing mode at read time. Each side becomes
/// the larger of what the table settles on (its stored side, else its
/// provisioned units) and the on-demand minimum, and both sides are stored.
///
/// The service answers the switch with the previous values and `Status:
/// ACTIVE` while the table is `UPDATING`; ExtendDB stores the values through
/// the ordinary update path, so the member reads `UPDATING` for
/// `control_plane_delay_seconds`, with the previous values only when the
/// record held them (a never-set table reports the on-demand values at
/// once). The client cannot send `WarmThroughput` beside `BillingMode` (the
/// only-operation rule refuses it), so a request reaching a backend with both
/// carries this reset.
///
/// Global secondary indexes are not stored here: a provisioned index never
/// set reports the on-demand minimum while the table is on-demand, and its
/// own units again after a switch back; the service's behaviour for an index
/// was not measured.
#[must_use]
pub(crate) fn on_demand_switch_warm_throughput(
    existing: &TableDescription,
) -> Option<WarmThroughput> {
    let already_on_demand = existing
        .billing_mode_summary
        .as_ref()
        .is_some_and(|s| s.billing_mode == BillingMode::PayPerRequest);
    if already_on_demand {
        return None;
    }
    let provisioned = WarmThroughputDefaults::for_billing(
        false,
        existing.provisioned_throughput.read_capacity_units,
        existing.provisioned_throughput.write_capacity_units,
    );
    let record = existing.warm_throughput_record.as_ref();
    let (read, write) = record.map_or(
        (
            provisioned.read_units_per_second,
            provisioned.write_units_per_second,
        ),
        |r| r.effective(provisioned),
    );
    let raised = (
        read.max(ON_DEMAND_WARM_READ_UNITS),
        write.max(ON_DEMAND_WARM_WRITE_UNITS),
    );
    let both_stored = record
        .is_some_and(|r| r.read_units_per_second.is_some() && r.write_units_per_second.is_some());
    if both_stored && raised == (read, write) {
        return None;
    }
    Some(WarmThroughput {
        read_units_per_second: Some(raised.0),
        write_units_per_second: Some(raised.1),
    })
}

/// Apply the measured update rules to `input` against `existing`:
///
/// - the table's `WarmThroughput` may not go below its current values
///   (the "for table" sentence);
/// - an `Update` action's `WarmThroughput` may not go below the named
///   index's current values (the "for index <name>" sentence), and the
///   index must exist;
/// - a `Create` action's `WarmThroughput` follows the create rule under the
///   table's current billing mode.
///
/// # Errors
///
/// `ValidationException` with the measured sentence;
/// `ResourceNotFoundException` for an `Update` naming an index the table
/// does not have.
pub(crate) fn validate_warm_throughput_updates(
    input: &UpdateTableInput,
    existing: &TableDescription,
) -> Result<(), DynamoDbError> {
    let (billing_mode, current) = current_table_values(existing);
    let now = epoch_seconds_now();
    if let Some(spec) = &input.warm_throughput {
        validate_warm_throughput_on_update(spec, WarmThroughputSubject::Table, current)?;
    }
    let pay_per_request = billing_mode == BillingMode::PayPerRequest;
    for action in input.global_secondary_index_updates.iter().flatten() {
        if let Some(create) = &action.create {
            validate_created_gsi_warm_throughput(create, billing_mode)?;
        }
        if let Some(update) = &action.update
            && let Some(spec) = &update.warm_throughput
        {
            let gsi = existing
                .global_secondary_indexes
                .iter()
                .flatten()
                .find(|g| g.index_name == update.index_name)
                .ok_or_else(|| {
                    DynamoDbError::ResourceNotFoundException(format!(
                        "Requested resource not found: Index {} for table {}",
                        update.index_name, input.table_name
                    ))
                })?;
            let (rcu, wcu) = gsi.provisioned_throughput.as_ref().map_or((0, 0), |pt| {
                (pt.read_capacity_units, pt.write_capacity_units)
            });
            let defaults = WarmThroughputDefaults::for_billing(pay_per_request, rcu, wcu);
            let current = gsi.warm_throughput_record.as_ref().map_or_else(
                || {
                    (
                        defaults.read_units_per_second,
                        defaults.write_units_per_second,
                    )
                },
                |r| r.reported(defaults, now),
            );
            validate_warm_throughput_on_update(
                spec,
                WarmThroughputSubject::Index(&update.index_name),
                current,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{
        BillingModeSummary, GsiDescription, Projection, ProjectionType,
        ProvisionedThroughputDescription, WarmThroughputRecord,
    };
    use serde_json::json;

    fn on_demand_table(record: Option<WarmThroughputRecord>) -> TableDescription {
        TableDescription {
            table_name: "t".to_owned(),
            billing_mode_summary: Some(BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: None,
            }),
            warm_throughput_record: record,
            global_secondary_indexes: Some(vec![GsiDescription {
                index_name: "gsi".to_owned(),
                key_schema: Vec::new(),
                projection: Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
                index_status: "ACTIVE".to_owned(),
                provisioned_throughput: Some(ProvisionedThroughputDescription::default()),
                index_size_bytes: 0,
                item_count: 0,
                index_arn: String::new(),
                warm_throughput: None,
                warm_throughput_record: None,
            }]),
            ..Default::default()
        }
    }

    fn input(v: serde_json::Value) -> UpdateTableInput {
        let mut v = v;
        v["TableName"] = json!("t");
        serde_json::from_value(v).expect("input")
    }

    /// U4: below the on-demand default on a table that never set one.
    #[test]
    fn a_table_update_below_the_current_default_is_refused() {
        let err = validate_warm_throughput_updates(
            &input(json!({"WarmThroughput": {"ReadUnitsPerSecond": 11999}})),
            &on_demand_table(None),
        )
        .expect_err("refused");
        assert!(matches!(err, DynamoDbError::ValidationException(m) if m.contains("decreasing")));
    }

    /// U1 and U2: same and higher are accepted.
    #[test]
    fn a_table_update_at_or_above_current_is_accepted() {
        let existing = on_demand_table(Some(WarmThroughputRecord {
            read_units_per_second: Some(20_000),
            write_units_per_second: None,
            previous_read_units_per_second: None,
            previous_write_units_per_second: None,
            updating_until: 0.0,
        }));
        validate_warm_throughput_updates(
            &input(json!({"WarmThroughput": {"ReadUnitsPerSecond": 20000, "WriteUnitsPerSecond": 4000}})),
            &existing,
        )
        .expect("same");
        validate_warm_throughput_updates(
            &input(json!({"WarmThroughput": {"ReadUnitsPerSecond": 20001}})),
            &existing,
        )
        .expect("higher");
        // The stored read side is what "current" means.
        validate_warm_throughput_updates(
            &input(json!({"WarmThroughput": {"ReadUnitsPerSecond": 12000}})),
            &existing,
        )
        .expect_err("below the stored 20000");
    }

    /// U6: an index update follows the same rule against the index's values.
    #[test]
    fn an_index_update_is_checked_against_the_index_and_must_name_one() {
        let existing = on_demand_table(None);
        validate_warm_throughput_updates(
            &input(json!({"GlobalSecondaryIndexUpdates": [{"Update": {
                "IndexName": "gsi",
                "WarmThroughput": {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000}
            }}]})),
            &existing,
        )
        .expect("same as the index default");
        let low = validate_warm_throughput_updates(
            &input(json!({"GlobalSecondaryIndexUpdates": [{"Update": {
                "IndexName": "gsi",
                "WarmThroughput": {"WriteUnitsPerSecond": 3999}
            }}]})),
            &existing,
        )
        .expect_err("below");
        assert!(
            matches!(&low, DynamoDbError::ValidationException(m)
                if m.contains("WriteUnitsPerSecond for WarmThroughput for index gsi is lower than current")),
            "{low:?}"
        );
        let missing = validate_warm_throughput_updates(
            &input(json!({"GlobalSecondaryIndexUpdates": [{"Update": {
                "IndexName": "nope",
                "WarmThroughput": {"ReadUnitsPerSecond": 12000}
            }}]})),
            &existing,
        )
        .expect_err("no such index");
        assert!(matches!(
            missing,
            DynamoDbError::ResourceNotFoundException(_)
        ));
    }

    /// A created index's WarmThroughput follows the create rule under the
    /// table's billing mode.
    #[test]
    fn a_created_index_is_checked_under_the_table_billing_mode() {
        let err = validate_warm_throughput_updates(
            &input(json!({"GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "new",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "WarmThroughput": {"ReadUnitsPerSecond": 1}
            }}]})),
            &on_demand_table(None),
        )
        .expect_err("below the on-demand minimum");
        assert!(
            matches!(&err, DynamoDbError::ValidationException(m)
                if m.contains("for index new is lower than initial throughput for OnDemand")),
            "{err:?}"
        );
    }

    fn provisioned_table(units: i64, record: Option<WarmThroughputRecord>) -> TableDescription {
        TableDescription {
            table_name: "t".to_owned(),
            provisioned_throughput: ProvisionedThroughputDescription {
                read_capacity_units: units,
                write_capacity_units: units,
                ..Default::default()
            },
            warm_throughput_record: record,
            ..Default::default()
        }
    }

    fn stored(read: Option<i64>, write: Option<i64>) -> Option<WarmThroughputRecord> {
        Some(WarmThroughputRecord {
            read_units_per_second: read,
            write_units_per_second: write,
            previous_read_units_per_second: None,
            previous_write_units_per_second: None,
            updating_until: 0.0,
        })
    }

    fn pair(w: Option<WarmThroughput>) -> Option<(i64, i64)> {
        w.map(|w| {
            (
                w.read_units_per_second.expect("read"),
                w.write_units_per_second.expect("write"),
            )
        })
    }

    /// Rows S7a, S7c, T7c: a switch to on-demand stores at least the on-demand
    /// minimum on both sides, from a never-set table, a read-only record and
    /// an explicit pair below it.
    #[test]
    fn a_switch_to_on_demand_stores_the_minimum_for_a_low_warm_throughput() {
        assert_eq!(
            pair(on_demand_switch_warm_throughput(&provisioned_table(
                10, None
            ))),
            Some((12_000, 4_000)),
            "S7a: never set, 10/10"
        );
        assert_eq!(
            pair(on_demand_switch_warm_throughput(&provisioned_table(
                10,
                stored(Some(100), None)
            ))),
            Some((12_000, 4_000)),
            "T7c: {{100}} on a table now at 10/10"
        );
        assert_eq!(
            pair(on_demand_switch_warm_throughput(&provisioned_table(
                10,
                stored(Some(100), Some(50))
            ))),
            Some((12_000, 4_000)),
            "S7c: explicit 100/50"
        );
    }

    /// A side at or above the minimum is kept; nothing is stored when both
    /// sides are stored and already at or above it, and none when the table is
    /// on-demand already.
    #[test]
    fn a_switch_to_on_demand_keeps_sides_at_or_above_the_minimum() {
        assert_eq!(
            pair(on_demand_switch_warm_throughput(&provisioned_table(
                10,
                stored(Some(20_000), None)
            ))),
            Some((20_000, 4_000))
        );
        assert_eq!(
            pair(on_demand_switch_warm_throughput(&provisioned_table(
                10,
                stored(Some(30_000), Some(9_000))
            ))),
            None
        );
        assert_eq!(
            on_demand_switch_warm_throughput(&on_demand_table(None)),
            None
        );
        // Provisioned units above the minimum are what the table settles on.
        assert_eq!(
            pair(on_demand_switch_warm_throughput(&provisioned_table(
                50_000, None
            ))),
            Some((50_000, 50_000))
        );
    }

    #[test]
    fn touches_is_true_for_any_warm_throughput_member() {
        assert!(!touches_warm_throughput(&input(
            json!({"DeletionProtectionEnabled": true})
        )));
        assert!(touches_warm_throughput(&input(
            json!({"WarmThroughput": {}})
        )));
        assert!(touches_warm_throughput(&input(json!({
            "GlobalSecondaryIndexUpdates": [{"Update": {"IndexName": "g", "WarmThroughput": {}}}]
        }))));
    }
}
