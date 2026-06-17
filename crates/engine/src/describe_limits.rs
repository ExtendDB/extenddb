// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DescribeLimits` operation handler.

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::DescribeLimitsOutput;
use serde_json::Value;

use crate::serialize_output;

/// Handle `DescribeLimits` — return account-level throughput limits.
///
/// REQ-CTRL-019: `DescribeLimits` returns configured account and table limits.
///
/// # Errors
///
/// Returns `InternalServerError` if serialization fails.
pub fn handle_describe_limits(limits: &LimitsConfig) -> Result<Value, DynamoDbError> {
    let output = DescribeLimitsOutput {
        account_max_read_capacity_units: i64::try_from(limits.per_account_max_rcu)
            .unwrap_or(i64::MAX),
        account_max_write_capacity_units: i64::try_from(limits.per_account_max_wcu)
            .unwrap_or(i64::MAX),
        table_max_read_capacity_units: i64::try_from(limits.per_table_max_rcu).unwrap_or(i64::MAX),
        table_max_write_capacity_units: i64::try_from(limits.per_table_max_wcu).unwrap_or(i64::MAX),
    };
    serialize_output(&output)
}
