// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Throttle integration helpers for the server layer.
//!
//! Bridges the `ThrottleManager` (in `extenddb_core::throttle`) with the server's
//! request handling: classifying operations, extracting throughput from
//! responses, and managing bucket lifecycle.

use extenddb_core::throttle::ThrottleManager;
use extenddb_core::types::type_descriptor;
use serde_json::Value;

/// Convert a `TableDescription` to a `TableThroughput` for throttle registration.
pub(crate) fn table_description_to_throughput(
    desc: &extenddb_core::types::TableDescription,
) -> extenddb_core::throttle::TableThroughput {
    let is_on_demand = desc
        .billing_mode_summary
        .as_ref()
        .is_some_and(|bms| bms.billing_mode == extenddb_core::types::BillingMode::PayPerRequest);
    if is_on_demand {
        extenddb_core::throttle::TableThroughput::on_demand()
    } else {
        extenddb_core::throttle::TableThroughput::provisioned(
            desc.provisioned_throughput.read_capacity_units,
            desc.provisioned_throughput.write_capacity_units,
        )
    }
}

/// Classify a `DynamoDB` operation as read, write, or control-plane.
///
/// Returns `(is_read, is_write)`. Control-plane operations (`CreateTable`,
/// `DeleteTable`, etc.) return `(false, false)` and are not throttled.
/// Batch and transact operations are classified by their dominant type.
pub(crate) fn classify_data_operation(operation: &str) -> (bool, bool) {
    match operation {
        "GetItem" | "Query" | "Scan" | "BatchGetItem" | "TransactGetItems" => (true, false),
        "PutItem" | "DeleteItem" | "UpdateItem" | "BatchWriteItem" | "TransactWriteItems" => {
            (false, true)
        }
        _ => (false, false),
    }
}

#[cfg(test)]
mod tests;

/// Update throttle buckets after a successful table lifecycle operation.
///
/// - `CreateTable` / `DescribeTable`: registers the table's throughput.
/// - `UpdateTable`: updates the table's throughput.
/// - `DeleteTable`: removes the table's buckets.
///
/// Extracts throughput from the response body's `TableDescription` to avoid
/// an extra database round-trip.
pub(crate) fn update_throttle_buckets(
    throttle: &ThrottleManager,
    operation: &str,
    account_id: &str,
    table_name: Option<&str>,
    body: &Value,
) {
    let Some(tn) = table_name else { return };
    match operation {
        "CreateTable" | "UpdateTable" | "DescribeTable" => {
            if let Some(throughput) = extract_throughput_from_response(body) {
                throttle.register_table(account_id, tn, throughput);
            }
        }
        "DeleteTable" => {
            throttle.remove_table(account_id, tn);
        }
        _ => {}
    }
}

/// Extract table throughput configuration from a `CreateTable`/`UpdateTable`/
/// `DescribeTable` response body.
fn extract_throughput_from_response(
    body: &Value,
) -> Option<extenddb_core::throttle::TableThroughput> {
    let desc = body.get("TableDescription").or_else(|| body.get("Table"))?;

    // Check billing mode.
    let is_on_demand = desc
        .get("BillingModeSummary")
        .and_then(|bms| bms.get("BillingMode"))
        .and_then(|bm| bm.as_str())
        .is_some_and(|bm| bm == "PAY_PER_REQUEST");

    if is_on_demand {
        return Some(extenddb_core::throttle::TableThroughput::on_demand());
    }

    let pt = desc.get("ProvisionedThroughput")?;
    let rcu = pt.get("ReadCapacityUnits")?.as_i64()?;
    let wcu = pt.get("WriteCapacityUnits")?.as_i64()?;
    Some(extenddb_core::throttle::TableThroughput::provisioned(
        rcu, wcu,
    ))
}

/// Extract the partition key value from a request body for per-partition throttling.
///
/// For item-level operations (`GetItem`, `PutItem`, `DeleteItem`, `UpdateItem`),
/// extracts the first attribute value from `Key` or `Item` as a string, read by
/// the member the engine's deserializer reads it by (`type_descriptor`).
/// Returns `None` for batch, transact, query, scan, and control-plane
/// operations, and for a value with no type descriptor or with two, which the
/// engine refuses.
pub(crate) fn extract_partition_value(input: &Value, operation: &str) -> Option<String> {
    let map = match operation {
        "GetItem" | "DeleteItem" | "UpdateItem" => input.get("Key")?,
        "PutItem" => input.get("Item")?,
        _ => return None,
    };
    // The first key in the map is the partition key (DynamoDB key maps are ordered).
    let obj = map.as_object()?;
    let (_, type_val) = obj.iter().next()?;
    let (_, val) = type_descriptor(type_val.as_object()?).ok()?;
    Some(val.as_str().unwrap_or_default().to_owned())
}
