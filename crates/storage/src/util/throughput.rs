// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A table's stored `provisioned_throughput`, shared by every backend.
//!
//! CreateTable stores the request's two capacity members. UpdateTable stores a
//! [`TableThroughputRecord`], which adds the instant the request raised a unit.
//! A value without that instant reads as never raised, and a reader that knows
//! only the two capacity members ignores the others.

use extenddb_core::types::{
    ProvisionedThroughput, ProvisionedThroughputDescription, epoch_seconds_now,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct TableThroughputRecord {
    #[serde(rename = "ReadCapacityUnits")]
    pub read_capacity_units: i64,
    #[serde(rename = "WriteCapacityUnits")]
    pub write_capacity_units: i64,
    /// Epoch seconds of the last UpdateTable that raised a unit.
    #[serde(
        rename = "LastIncreaseDateTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_increase_date_time: Option<f64>,
    /// Epoch seconds from which `last_increase_date_time` is not reported.
    #[serde(
        rename = "LastIncreaseReportedUntil",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub reported_until: Option<f64>,
}

impl TableThroughputRecord {
    /// The record an UpdateTable that sets `requested` writes over `stored`.
    /// `was_provisioned` is the table's billing mode before the request, and
    /// `updating_until` is the end of the `UPDATING` window the request opens,
    /// the instant the backend writes to `status_transition_at` (`None` when
    /// it writes none).
    ///
    /// Amazon DynamoDB answers a request that raises either unit with
    /// `LastIncreaseDateTime` in whole milliseconds, and every DescribeTable
    /// after it carries one. A request that lowers both units leaves it as it
    /// was. After a switch from `PAY_PER_REQUEST`, whose units read as zero,
    /// the member is sent only while the table is `UPDATING` (measured in
    /// us-east-1 on 2026-11-14, 2026-11-16 and 2026-11-19).
    #[must_use]
    pub fn for_update(
        stored: Option<Self>,
        was_provisioned: bool,
        requested: &ProvisionedThroughput,
        updating_until: Option<f64>,
    ) -> Self {
        let before = stored.filter(|_| was_provisioned).unwrap_or_default();
        let raises = requested.read_capacity_units > before.read_capacity_units
            || requested.write_capacity_units > before.write_capacity_units;
        let now = epoch_seconds_now();
        let stamp = Some((now * 1000.0).round() / 1000.0);
        let (last_increase_date_time, reported_until) = match (raises, was_provisioned) {
            (false, _) => (before.last_increase_date_time, before.reported_until),
            (true, true) => (stamp, None),
            (true, false) => (stamp, Some(updating_until.unwrap_or(now))),
        };
        Self {
            read_capacity_units: requested.read_capacity_units,
            write_capacity_units: requested.write_capacity_units,
            last_increase_date_time,
            reported_until,
        }
    }

    /// The `ProvisionedThroughput` a description of the table reports at
    /// `now`, in epoch seconds.
    #[must_use]
    pub fn description(self, now: f64) -> ProvisionedThroughputDescription {
        ProvisionedThroughputDescription {
            read_capacity_units: self.read_capacity_units,
            write_capacity_units: self.write_capacity_units,
            last_increase_date_time: self
                .last_increase_date_time
                .filter(|_| self.reported_until.is_none_or(|until| now < until)),
            ..ProvisionedThroughputDescription::default()
        }
    }
}

/// The `ProvisionedThroughput` a description reports now for a table whose
/// stored value is `stored`: all zero when there is none.
#[must_use]
pub fn table_throughput_description(
    stored: Option<TableThroughputRecord>,
) -> ProvisionedThroughputDescription {
    stored
        .map(|record| record.description(epoch_seconds_now()))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_stored_shapes_read() {
        let switched = TableThroughputRecord {
            read_capacity_units: 5,
            write_capacity_units: 7,
            last_increase_date_time: Some(100.0),
            reported_until: Some(105.0),
        };
        let created: TableThroughputRecord =
            serde_json::from_str(r#"{"ReadCapacityUnits":5,"WriteCapacityUnits":7}"#)
                .expect("a record");
        assert_eq!(created.last_increase_date_time, None);
        let units: ProvisionedThroughput =
            serde_json::from_str(&serde_json::to_string(&switched).expect("json"))
                .expect("two members");
        assert_eq!(
            (units.read_capacity_units, units.write_capacity_units),
            (5, 7)
        );
    }
}
