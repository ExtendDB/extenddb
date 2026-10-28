// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WarmThroughput` on tables and global secondary indexes.
//!
//! Measured against Amazon DynamoDB (us-east-1, 2026-10-23). The service
//! reports `WarmThroughput` as `{ReadUnitsPerSecond, WriteUnitsPerSecond,
//! Status}` on an ACTIVE table and on each of its ACTIVE global secondary
//! indexes; local secondary indexes carry none. A client that sends none gets
//! 12000 read and 4000 write units on an on-demand table and its indexes, and
//! the provisioned units on a provisioned table and each provisioned index.
//!
//! ExtendDB stores the values a client sends and reports them with the same
//! shape, defaults and validation. Nothing acts on them: there is no
//! pre-warming and no scaling at the database level, and the stored numbers
//! have no effect on throughput, throttling or storage. They exist so clients
//! that read the member (the Terraform AWS provider since 6.13.0 waits for
//! `Status: ACTIVE` after every table and index change) work.

use serde::{Deserialize, Serialize};

/// Read units an on-demand table or index reports when the client sent none.
pub const ON_DEMAND_WARM_READ_UNITS: i64 = 12_000;
/// Write units an on-demand table or index reports when the client sent none.
pub const ON_DEMAND_WARM_WRITE_UNITS: i64 = 4_000;

/// `WarmThroughput` as a client sends it on `CreateTable`, `UpdateTable`, a
/// `GlobalSecondaryIndexes` element and a `GlobalSecondaryIndexUpdates`
/// `Create` or `Update` action. Either member may be omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WarmThroughput {
    #[serde(
        rename = "ReadUnitsPerSecond",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub read_units_per_second: Option<i64>,
    #[serde(
        rename = "WriteUnitsPerSecond",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub write_units_per_second: Option<i64>,
}

/// The `Status` member of a reported `WarmThroughput`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WarmThroughputStatus {
    #[serde(rename = "ACTIVE")]
    Active,
    #[serde(rename = "UPDATING")]
    Updating,
}

/// `WarmThroughput` as the service reports it on `TableDescription` and
/// `GlobalSecondaryIndexDescription`.
///
/// Both unit members are optional because the `CreateTable` response echoes a
/// partial request as sent: a request of `{ReadUnitsPerSecond: 20000}` was
/// answered with `{ReadUnitsPerSecond: 20000, Status: UPDATING}` and no write
/// member, and `DescribeTable` once ACTIVE then read `{20000, 4000, ACTIVE}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmThroughputDescription {
    #[serde(
        rename = "ReadUnitsPerSecond",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub read_units_per_second: Option<i64>,
    #[serde(
        rename = "WriteUnitsPerSecond",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub write_units_per_second: Option<i64>,
    #[serde(rename = "Status")]
    pub status: WarmThroughputStatus,
}

/// What a backend persists for a table or index whose client set
/// `WarmThroughput`: the units the client sent (a side the client never sent
/// stays `None` and is reported from the defaults), the units the resource
/// reported before the last `UpdateTable` (reported again while `UPDATING`,
/// as the service does), and the epoch second until which the record reports
/// `UPDATING`.
///
/// Stored as one JSON value in the catalog. Nothing reads these numbers except
/// the description builder: they do not pre-warm anything, scale anything or
/// change how a request is throttled or stored.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub struct WarmThroughputRecord {
    #[serde(rename = "ReadUnitsPerSecond", default)]
    pub read_units_per_second: Option<i64>,
    #[serde(rename = "WriteUnitsPerSecond", default)]
    pub write_units_per_second: Option<i64>,
    /// The side the resource reported when the last `UpdateTable` was
    /// validated (`None` means the default of the moment). Reported, and
    /// decreases are validated against it, while the record is `UPDATING`.
    #[serde(rename = "PreviousReadUnitsPerSecond", default)]
    pub previous_read_units_per_second: Option<i64>,
    #[serde(rename = "PreviousWriteUnitsPerSecond", default)]
    pub previous_write_units_per_second: Option<i64>,
    /// Epoch seconds. `Status` is `UPDATING` while `now < updating_until`.
    #[serde(rename = "UpdatingUntil", default)]
    pub updating_until: f64,
}

/// The defaults a table or index reports when the client never set warm
/// throughput, and the values that fill a side the client left out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WarmThroughputDefaults {
    pub read_units_per_second: i64,
    pub write_units_per_second: i64,
    /// On an on-demand resource a stored side below the default is reported
    /// as the default: switching a provisioned table to PAY_PER_REQUEST
    /// resets a below-minimum warm throughput to 12000/4000 (measured, rows
    /// T7c and T7d).
    pub pay_per_request: bool,
}

impl WarmThroughputDefaults {
    /// On-demand: 12000 read, 4000 write. Provisioned: the resource's own
    /// `ReadCapacityUnits` and `WriteCapacityUnits`. Measured on a
    /// PAY_PER_REQUEST table and on a 1/1 provisioned table with a 1/1 index.
    #[must_use]
    pub fn for_billing(
        pay_per_request: bool,
        read_capacity_units: i64,
        write_capacity_units: i64,
    ) -> Self {
        if pay_per_request {
            Self {
                read_units_per_second: ON_DEMAND_WARM_READ_UNITS,
                write_units_per_second: ON_DEMAND_WARM_WRITE_UNITS,
                pay_per_request,
            }
        } else {
            Self {
                read_units_per_second: read_capacity_units,
                write_units_per_second: write_capacity_units,
                pay_per_request,
            }
        }
    }

    /// The pair `(read, write)` of stored sides resolved against these
    /// defaults: an unset side is the default; on an on-demand resource a
    /// side below the default is the default.
    #[must_use]
    pub fn resolve(self, read: Option<i64>, write: Option<i64>) -> (i64, i64) {
        let side = |stored: Option<i64>, default: i64| match stored {
            Some(v) if self.pay_per_request => v.max(default),
            Some(v) => v,
            None => default,
        };
        (
            side(read, self.read_units_per_second),
            side(write, self.write_units_per_second),
        )
    }

    /// The description an untouched resource reports once it is ACTIVE.
    #[must_use]
    pub fn describe(self) -> WarmThroughputDescription {
        WarmThroughputDescription {
            read_units_per_second: Some(self.read_units_per_second),
            write_units_per_second: Some(self.write_units_per_second),
            status: WarmThroughputStatus::Active,
        }
    }
}

impl WarmThroughputRecord {
    /// The record a `CreateTable` (or a GSI `Create`) writes: the request as
    /// sent, reporting `UPDATING` until `updating_until`.
    ///
    /// The previous pair is the request itself: a created resource that is
    /// past CREATING inside its window (an index added by `UpdateTable` is
    /// `ACTIVE` here at once) reports the requested values with `UPDATING`
    /// and then the same values with `ACTIVE`, as the service does for an
    /// index being created (rows S8a, V5). A side the request leaves out
    /// reports the default.
    #[must_use]
    pub fn from_request(spec: &WarmThroughput, updating_until: f64) -> Self {
        Self {
            read_units_per_second: spec.read_units_per_second,
            write_units_per_second: spec.write_units_per_second,
            previous_read_units_per_second: spec.read_units_per_second,
            previous_write_units_per_second: spec.write_units_per_second,
            updating_until,
        }
    }

    /// The record an `UpdateTable` writes over `existing` at `now`: each side
    /// the request names replaces the stored side, a side it leaves out keeps
    /// the stored value (or stays unset and keeps reporting the default), the
    /// `UPDATING` deadline restarts, and the previous pair becomes what
    /// `existing` reported at `now` (its own previous pair while it was still
    /// `UPDATING`, its stored sides otherwise), so a request inside the window
    /// keeps reporting the values the window started from.
    ///
    /// A `spec` with neither side (`WarmThroughput: {}`) changes no value and
    /// restarts the window. The service answers `InternalFailure` with an empty
    /// message to that request on `UpdateTable` (row S9b); ExtendDB accepts it
    /// and does not imitate a service fault.
    #[must_use]
    pub fn apply_update(
        existing: Option<Self>,
        spec: &WarmThroughput,
        updating_until: f64,
        now: f64,
    ) -> Self {
        let existing = existing.unwrap_or_default();
        let (previous_read, previous_write) = if existing.is_updating_at(now) {
            (
                existing.previous_read_units_per_second,
                existing.previous_write_units_per_second,
            )
        } else {
            (
                existing.read_units_per_second,
                existing.write_units_per_second,
            )
        };
        Self {
            read_units_per_second: spec
                .read_units_per_second
                .or(existing.read_units_per_second),
            write_units_per_second: spec
                .write_units_per_second
                .or(existing.write_units_per_second),
            previous_read_units_per_second: previous_read,
            previous_write_units_per_second: previous_write,
            updating_until,
        }
    }

    /// The effective units once the window has passed: the stored side where
    /// set, the default otherwise (clamped on an on-demand resource).
    #[must_use]
    pub fn effective(&self, defaults: WarmThroughputDefaults) -> (i64, i64) {
        defaults.resolve(self.read_units_per_second, self.write_units_per_second)
    }

    /// The pair the resource reports at `now`: the previous pair while
    /// `UPDATING` (measured: the service keeps reporting the values the
    /// window started from, rows U2, S2a, S2c), the effective pair afterwards.
    /// Decreases are validated against this pair (row U3: inside the window a
    /// request carrying the previous values is accepted; row S2b: on a
    /// settled table the same request is a decrease).
    #[must_use]
    pub fn reported(&self, defaults: WarmThroughputDefaults, now: f64) -> (i64, i64) {
        if self.is_updating_at(now) {
            defaults.resolve(
                self.previous_read_units_per_second,
                self.previous_write_units_per_second,
            )
        } else {
            self.effective(defaults)
        }
    }

    /// Whether the record still reports `UPDATING` at `now` (epoch seconds).
    #[must_use]
    pub fn is_updating_at(&self, now: f64) -> bool {
        now < self.updating_until
    }

    /// The description once the owning table or index is past CREATING: both
    /// sides filled, the previous pair with `UPDATING` until the deadline,
    /// then the new pair with `ACTIVE`.
    #[must_use]
    pub fn describe(
        &self,
        defaults: WarmThroughputDefaults,
        now: f64,
    ) -> WarmThroughputDescription {
        let (read, write) = self.reported(defaults, now);
        WarmThroughputDescription {
            read_units_per_second: Some(read),
            write_units_per_second: Some(write),
            status: if self.is_updating_at(now) {
                WarmThroughputStatus::Updating
            } else {
                WarmThroughputStatus::Active
            },
        }
    }

    /// The `CreateTable` response shape while the resource is still CREATING:
    /// the request echoed as sent (a side the client left out is absent) with
    /// `Status: UPDATING`. Measured on a `{ReadUnitsPerSecond: 20000}` request.
    #[must_use]
    pub fn echo_while_creating(&self) -> WarmThroughputDescription {
        WarmThroughputDescription {
            read_units_per_second: self.read_units_per_second,
            write_units_per_second: self.write_units_per_second,
            status: WarmThroughputStatus::Updating,
        }
    }

    /// The member a response carries for a resource in `status`, or `None`
    /// when the service omits it.
    ///
    /// - No record and the resource is CREATING: absent (measured on the
    ///   `CreateTable` response and the first `DescribeTable`). A record with
    ///   neither side (a `WarmThroughput: {}` request) is the same: the
    ///   service answers no member while CREATING (row T1).
    /// - No record otherwise: the defaults, `ACTIVE`.
    /// - A record while CREATING: the request echoed, `UPDATING`.
    /// - A record otherwise: both sides filled, the previous pair with
    ///   `UPDATING` until the deadline, then the new pair with `ACTIVE`.
    #[must_use]
    pub fn describe_for(
        record: Option<&Self>,
        creating: bool,
        defaults: WarmThroughputDefaults,
        now: f64,
    ) -> Option<WarmThroughputDescription> {
        match (record, creating) {
            (None, true) => None,
            (Some(record), true)
                if record.read_units_per_second.is_none()
                    && record.write_units_per_second.is_none() =>
            {
                None
            }
            (None, false) => Some(defaults.describe()),
            (Some(record), true) => Some(record.echo_while_creating()),
            (Some(record), false) => Some(record.describe(defaults, now)),
        }
    }
}

/// Seconds since the Unix epoch, for the `UPDATING` deadline and its check.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn epoch_seconds_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

#[cfg(test)]
mod tests;
