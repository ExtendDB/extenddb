// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The provisioned global secondary indexes a table description lists, for
//! the throttle manager's index buckets, and the index a read names.

use extenddb_core::throttle::IndexThroughput;
use serde_json::Value;

/// The throughput of every provisioned global secondary index in `desc`, in
/// the order the description lists them. An on-demand table's indexes have no
/// capacity of their own and give an empty list, as does an index whose
/// description carries no `ProvisionedThroughput` or carries `0` in both
/// directions (the placeholder a backend describes for an index created on a
/// provisioned table without throughput; a bucket at rate 0 would refuse
/// every request).
pub(crate) fn table_description_to_index_throughputs(
    desc: &extenddb_core::types::TableDescription,
) -> Vec<IndexThroughput> {
    let is_on_demand = desc
        .billing_mode_summary
        .as_ref()
        .is_some_and(|bms| bms.billing_mode == extenddb_core::types::BillingMode::PayPerRequest);
    if is_on_demand {
        return Vec::new();
    }
    desc.global_secondary_indexes
        .iter()
        .flatten()
        .filter_map(|gsi| {
            let pt = gsi.provisioned_throughput.as_ref()?;
            if pt.read_capacity_units < 1 && pt.write_capacity_units < 1 {
                return None;
            }
            Some(IndexThroughput::provisioned(
                gsi.index_name.clone(),
                pt.read_capacity_units,
                pt.write_capacity_units,
            ))
        })
        .collect()
}

/// The same list read from a `CreateTable`, `UpdateTable` or `DescribeTable`
/// response body. `None` when the body carries no table description.
pub(crate) fn extract_index_throughputs_from_response(
    body: &Value,
) -> Option<Vec<IndexThroughput>> {
    let desc = body.get("TableDescription").or_else(|| body.get("Table"))?;
    let is_on_demand = desc
        .get("BillingModeSummary")
        .and_then(|bms| bms.get("BillingMode"))
        .and_then(Value::as_str)
        .is_some_and(|bm| bm == "PAY_PER_REQUEST");
    if is_on_demand {
        return Some(Vec::new());
    }
    Some(
        desc.get("GlobalSecondaryIndexes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|gsi| {
                let name = gsi.get("IndexName")?.as_str()?;
                let pt = gsi.get("ProvisionedThroughput")?;
                let rcu = pt.get("ReadCapacityUnits")?.as_i64()?;
                let wcu = pt.get("WriteCapacityUnits")?.as_i64()?;
                if rcu < 1 && wcu < 1 {
                    return None;
                }
                Some(IndexThroughput::provisioned(name, rcu, wcu))
            })
            .collect(),
    )
}

/// The index a `Query` or `Scan` names in `IndexName`. Every other operation
/// reads or writes the base table and gives `None`; a `BatchGetItem` or
/// `TransactGetItems` cannot name an index.
pub(crate) fn extract_index_name(input: &Value, operation: &str) -> Option<String> {
    match operation {
        "Query" | "Scan" => input.get("IndexName")?.as_str().map(str::to_owned),
        _ => None,
    }
}
