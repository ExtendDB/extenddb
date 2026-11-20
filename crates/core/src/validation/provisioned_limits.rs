// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The account limits `CreateTable` and `UpdateTable` enforce: the table quota
//! (`max_tables_per_account`) and the provisioned-capacity maxima
//! (`per_table_max_rcu`, `per_table_max_wcu`, `per_account_max_rcu`,
//! `per_account_max_wcu`).
//!
//! The checks are pure: the engine reads the account's tables and passes in
//! what it found. Every refusal is `LimitExceededException`. The capacity
//! sentences are the ones Amazon `DynamoDB` returns, measured on 2026-10-31;
//! the table-quota sentence was not measured.

use crate::error::DynamoDbError;
use crate::limits::LimitsConfig;
use crate::types::{
    BillingMode, CreateTableInput, ProvisionedThroughput, TableDescription, UpdateTableInput,
};

/// The closing sentence of every provisioned-capacity refusal.
const LIMITS_GUIDE: &str = "Refer to the Amazon DynamoDB Developer Guide for current limits \
                            and how to request higher limits.";

/// Read and write capacity units held by a table and its global secondary
/// indexes, or by every table in an account.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CapacityUnits {
    pub read: i64,
    pub write: i64,
}

impl CapacityUnits {
    /// `self` plus `other`, saturating at `i64::MAX`.
    #[must_use]
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            read: self.read.saturating_add(other.read),
            write: self.write.saturating_add(other.write),
        }
    }

    fn of(throughput: &ProvisionedThroughput) -> Self {
        Self {
            read: throughput.read_capacity_units,
            write: throughput.write_capacity_units,
        }
    }
}

/// The two units a refusal can name, in the order the service checks them.
#[derive(Debug, Clone, Copy)]
enum Unit {
    Read,
    Write,
}

impl Unit {
    const BOTH: [Self; 2] = [Self::Read, Self::Write];

    fn name(self) -> &'static str {
        match self {
            Self::Read => "ReadCapacityUnits",
            Self::Write => "WriteCapacityUnits",
        }
    }

    fn of(self, units: CapacityUnits) -> i64 {
        match self {
            Self::Read => units.read,
            Self::Write => units.write,
        }
    }

    fn table_max(self, limits: &LimitsConfig) -> i64 {
        let max = match self {
            Self::Read => limits.per_table_max_rcu,
            Self::Write => limits.per_table_max_wcu,
        };
        i64::try_from(max).unwrap_or(i64::MAX)
    }

    fn account_max(self, limits: &LimitsConfig) -> i64 {
        let max = match self {
            Self::Read => limits.per_account_max_rcu,
            Self::Write => limits.per_account_max_wcu,
        };
        i64::try_from(max).unwrap_or(i64::MAX)
    }
}

/// What a `CreateTable` request provisions: the table's units plus those of
/// every global secondary index. `None` for an on-demand table.
#[must_use]
pub fn create_table_capacity(input: &CreateTableInput) -> Option<CapacityUnits> {
    if input.billing_mode.unwrap_or(BillingMode::Provisioned) == BillingMode::PayPerRequest {
        return None;
    }
    let table = CapacityUnits::of(input.provisioned_throughput.as_ref()?);
    Some(
        input
            .global_secondary_indexes
            .iter()
            .flatten()
            .filter_map(|gsi| gsi.provisioned_throughput.as_ref())
            .map(CapacityUnits::of)
            .fold(table, CapacityUnits::saturating_add),
    )
}

/// What a table and its global secondary indexes hold, as `DescribeTable`
/// reports them. An on-demand table holds none.
#[must_use]
pub fn table_capacity(desc: &TableDescription) -> CapacityUnits {
    if is_on_demand(desc) {
        return CapacityUnits::default();
    }
    let table = CapacityUnits {
        read: desc.provisioned_throughput.read_capacity_units,
        write: desc.provisioned_throughput.write_capacity_units,
    };
    desc.global_secondary_indexes
        .iter()
        .flatten()
        .filter_map(|gsi| gsi.provisioned_throughput.as_ref())
        .map(|t| CapacityUnits {
            read: t.read_capacity_units,
            write: t.write_capacity_units,
        })
        .fold(table, CapacityUnits::saturating_add)
}

fn is_on_demand(desc: &TableDescription) -> bool {
    desc.billing_mode_summary
        .as_ref()
        .is_some_and(|b| b.billing_mode == BillingMode::PayPerRequest)
}

/// Whether `input` sets a provisioned throughput, on the table or on a global
/// secondary index it creates. Only such a request can raise the capacity a
/// table holds.
#[must_use]
pub fn changes_provisioned_capacity(input: &UpdateTableInput) -> bool {
    input.provisioned_throughput.is_some()
        || input
            .global_secondary_index_updates
            .iter()
            .flatten()
            .filter_map(|u| u.create.as_ref())
            .any(|c| c.provisioned_throughput.is_some())
}

/// What the table `existing` and its global secondary indexes would hold once
/// `input` applies: its throughput or the one `input` sets, the indexes it
/// keeps and the indexes `input` creates. `None` when `input` sets no
/// provisioned throughput, or when the table is on-demand afterwards.
#[must_use]
pub fn update_table_capacity(
    existing: &TableDescription,
    input: &UpdateTableInput,
) -> Option<CapacityUnits> {
    if !changes_provisioned_capacity(input) {
        return None;
    }
    let on_demand_after = match input.billing_mode {
        Some(mode) => mode == BillingMode::PayPerRequest,
        None => is_on_demand(existing),
    };
    if on_demand_after {
        return None;
    }
    let updates = input
        .global_secondary_index_updates
        .as_deref()
        .unwrap_or(&[]);
    let deleted = |name: &str| {
        updates
            .iter()
            .filter_map(|u| u.delete.as_ref())
            .any(|d| d.index_name == name)
    };
    let table = input.provisioned_throughput.as_ref().map_or(
        CapacityUnits {
            read: existing.provisioned_throughput.read_capacity_units,
            write: existing.provisioned_throughput.write_capacity_units,
        },
        CapacityUnits::of,
    );
    let kept = existing
        .global_secondary_indexes
        .iter()
        .flatten()
        .filter(|gsi| !deleted(&gsi.index_name))
        .filter_map(|gsi| gsi.provisioned_throughput.as_ref())
        .map(|t| CapacityUnits {
            read: t.read_capacity_units,
            write: t.write_capacity_units,
        });
    let created = updates
        .iter()
        .filter_map(|u| u.create.as_ref())
        .filter_map(|c| c.provisioned_throughput.as_ref())
        .map(CapacityUnits::of);
    Some(
        kept.chain(created)
            .fold(table, CapacityUnits::saturating_add),
    )
}

/// Refuse a `CreateTable` that would provision more than the account or one
/// table may hold.
///
/// `requested` is what the request provisions ([`create_table_capacity`]) and
/// `reserved` what the account's tables hold already. The account maximum is
/// checked first, read units before write units, as measured. The per-table
/// maximum bounds the table and its global secondary indexes together; its
/// sentence on this path was not measured, so it is the sentence `UpdateTable`
/// returns.
///
/// # Errors
///
/// `LimitExceededException` naming the first unit over a maximum.
pub fn check_create_table_capacity(
    requested: CapacityUnits,
    reserved: CapacityUnits,
    limits: &LimitsConfig,
    region: &str,
) -> Result<(), DynamoDbError> {
    for unit in Unit::BOTH {
        let limit = unit.account_max(limits);
        let (reserved, requested) = (unit.of(reserved), unit.of(requested));
        if reserved.saturating_add(requested) > limit {
            return Err(account_refusal(
                unit, region, reserved, limit, None, requested,
            ));
        }
    }
    for unit in Unit::BOTH {
        let max = unit.table_max(limits);
        if unit.of(requested) > max {
            return Err(per_table_refusal(unit, unit.of(requested), max, region));
        }
    }
    Ok(())
}

/// Refuse an `UpdateTable` that would raise a table or the account above its
/// maximum.
///
/// `table_request` is the table's own `ProvisionedThroughput` from the request,
/// checked against the per-table maximum first, as measured. Then the account:
/// `reserved` is what the account holds now, including `current`, what this
/// table and its indexes hold now, which `after` replaces. A unit the request
/// does not raise is not refused, so a table can still be lowered after an
/// operator lowers a maximum below what it holds.
///
/// # Errors
///
/// `LimitExceededException` naming the first unit over a maximum.
pub fn check_update_table_capacity(
    table_request: Option<&ProvisionedThroughput>,
    existing: &TableDescription,
    reserved: CapacityUnits,
    after: CapacityUnits,
    limits: &LimitsConfig,
    region: &str,
) -> Result<(), DynamoDbError> {
    if let Some(request) = table_request {
        let requested = CapacityUnits::of(request);
        let held = CapacityUnits {
            read: existing.provisioned_throughput.read_capacity_units,
            write: existing.provisioned_throughput.write_capacity_units,
        };
        for unit in Unit::BOTH {
            let max = unit.table_max(limits);
            if unit.of(requested) > max && unit.of(requested) > unit.of(held) {
                return Err(per_table_refusal(unit, unit.of(requested), max, region));
            }
        }
    }
    let current = table_capacity(existing);
    for unit in Unit::BOTH {
        let limit = unit.account_max(limits);
        let (reserved, current, after) = (unit.of(reserved), unit.of(current), unit.of(after));
        if after > current && reserved.saturating_sub(current).saturating_add(after) > limit {
            return Err(account_refusal(
                unit,
                region,
                reserved,
                limit,
                Some(current),
                after,
            ));
        }
    }
    Ok(())
}

/// Refuse a `CreateTable` in an account that holds `max_tables_per_account`
/// tables already.
///
/// The service's sentence for this refusal was not measured. This one states
/// the quota the way the `LimitExceededException` reference documents it.
///
/// # Errors
///
/// `LimitExceededException` when `existing` is at or above the quota.
pub fn check_table_count(existing: usize, limits: &LimitsConfig) -> Result<(), DynamoDbError> {
    if existing >= limits.max_tables_per_account {
        return Err(DynamoDbError::LimitExceededException(format!(
            "There is a soft account quota of {} tables.",
            limits.max_tables_per_account
        )));
    }
    Ok(())
}

fn per_table_refusal(unit: Unit, requested: i64, max: i64, region: &str) -> DynamoDbError {
    let unit = unit.name();
    DynamoDbError::LimitExceededException(format!(
        "The requested {unit}, {requested}, is above the per table maximum for the account in \
         {region}. Per table maximum: {max}. {LIMITS_GUIDE}"
    ))
}

/// The account sentence. `table_now`, set on `UpdateTable`, is what the table
/// and its indexes hold before the request, and `requested` what they would
/// hold after it; on `CreateTable` `requested` is the new table's units.
fn account_refusal(
    unit: Unit,
    region: &str,
    reserved: i64,
    limit: i64,
    table_now: Option<i64>,
    requested: i64,
) -> DynamoDbError {
    let unit = unit.name();
    let table = table_now.map_or_else(String::new, |now| {
        format!(" Current {unit} provisioned for the table and indexes: {now}.")
    });
    DynamoDbError::LimitExceededException(format!(
        "This request would have caused the {unit} limit to be exceeded for the account in \
         {region}. Current {unit} reserved by the account: {reserved}. Limit: {limit}.{table} \
         Requested: {requested}. {LIMITS_GUIDE}"
    ))
}

#[cfg(test)]
#[path = "provisioned_limits_tests.rs"]
mod tests;
