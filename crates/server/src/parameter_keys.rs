// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `dynamodb:Select`, `dynamodb:ReturnValues` and
//! `dynamodb:ReturnConsumedCapacity` for request authorization.
//!
//! Each key holds the value of the request parameter it is named after. When a
//! request that takes the parameter leaves it out, the service evaluates the
//! value the operation then uses, so the key is present whether or not the
//! caller spells that value out (measured 2026-11-01):
//!
//! - `dynamodb:Select` on `GetItem`, `Query`, `Scan`, each table of a
//!   `BatchGetItem` and each `Get` of a `TransactGetItems`:
//!   `SPECIFIC_ATTRIBUTES` for a read with a projection (`ProjectionExpression`
//!   or `AttributesToGet`), `ALL_ATTRIBUTES` for a read of a table without one,
//!   and the `Select` of a `Query` or `Scan` that has one. `GetItem`,
//!   `BatchGetItem` and `TransactGetItems` take no `Select` parameter, so a
//!   `Select` member in one of them is not read: the engine ignores it and
//!   returns what the projection asks for.
//! - `dynamodb:ReturnValues` on `PutItem`, `UpdateItem` and `DeleteItem`: the
//!   request's `ReturnValues`, or `NONE`. The `Put`, `Update` and `Delete` items
//!   of a `TransactWriteItems` take no `ReturnValues` and hold `NONE`. A
//!   `BatchWriteItem` and a `ConditionCheck` leave the key absent.
//! - `dynamodb:ReturnConsumedCapacity` on the item operations: the request's
//!   `ReturnConsumedCapacity`, or `NONE`. A batch or transaction request carries
//!   one value, and each of its tables or items is evaluated with it.
//!
//! Measured on 2026-11-01: `dynamodb:Select` on `GetItem` and `Query` with and
//! without a projection, on `Scan` without one and with `Select` `COUNT`, on
//! `Query` with `Select` `COUNT`, and on `BatchGetItem` and a `TransactGetItems`
//! `Get` with and without a projection; `dynamodb:ReturnValues` on `PutItem`,
//! `UpdateItem` and `DeleteItem` with and without the parameter, on a
//! `BatchWriteItem`, and on the `Put`, `Update` and `Delete` items of a
//! `TransactWriteItems`; `dynamodb:ReturnConsumedCapacity` on `GetItem` with and
//! without the parameter, on a `BatchWriteItem` and on a `TransactGetItems`
//! `Get`. `dynamodb:ReturnConsumedCapacity` on the other item operations follows
//! those. A read that projects with `AttributesToGet` is treated as one with a
//! `ProjectionExpression`, which the engine reads it as; that was not measured.
//! A `Query` or `Scan` of an index with no projection and no `Select` holds
//! `ALL_PROJECTED_ATTRIBUTES`, the default the DynamoDB API reference gives for
//! a read of an index; that was not measured either.
//!
//! A key the operation does not take holds the request's parameter as given,
//! as before: a `Select` member in a `PutItem` reaches the policy as written.
//! The batch and transaction targets carry only the keys their operation takes.

use serde_json::Value;

/// `dynamodb:Select` for a read with a projection and no `Select`.
const SPECIFIC_ATTRIBUTES: &str = "SPECIFIC_ATTRIBUTES";
/// `dynamodb:Select` for a read of a table with neither.
const ALL_ATTRIBUTES: &str = "ALL_ATTRIBUTES";
/// `dynamodb:Select` for a `Query` or `Scan` of an index with neither (the API
/// reference's default, not measured).
const ALL_PROJECTED_ATTRIBUTES: &str = "ALL_PROJECTED_ATTRIBUTES";
/// `dynamodb:ReturnValues` and `dynamodb:ReturnConsumedCapacity` for a request
/// that takes the parameter and leaves it out.
const NONE: &str = "NONE";

/// The `dynamodb:Select`, `dynamodb:ReturnValues` and
/// `dynamodb:ReturnConsumedCapacity` values of one authorization. `None`
/// leaves a key absent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub(crate) struct ParameterKeys {
    pub select: Option<String>,
    pub return_values: Option<String>,
    pub return_consumed_capacity: Option<String>,
}

/// The keys of a request authorized once, against one resource.
pub(crate) fn single_table(operation: &str, input: &Value) -> ParameterKeys {
    let mut keys = ParameterKeys {
        select: given(input, "Select"),
        return_values: given(input, "ReturnValues"),
        return_consumed_capacity: given(input, "ReturnConsumedCapacity"),
    };
    match operation {
        "GetItem" => keys.select = Some(projection_select(input).to_owned()),
        "Query" | "Scan" => {
            keys.select = Some(
                keys.select
                    .unwrap_or_else(|| range_read_select(input).to_owned()),
            );
        }
        "PutItem" | "UpdateItem" | "DeleteItem" => {
            keys.return_values = Some(keys.return_values.unwrap_or_else(|| NONE.to_owned()));
        }
        _ => return keys,
    }
    keys.return_consumed_capacity = Some(consumed_capacity(input));
    keys
}

/// The keys of one table's part of a `BatchGetItem` or `BatchWriteItem`:
/// `body` is that table's `RequestItems` entry and `request` the whole
/// request, which carries `ReturnConsumedCapacity`.
pub(crate) fn batch_table(operation: &str, body: &Value, request: &Value) -> ParameterKeys {
    ParameterKeys {
        select: (operation == "BatchGetItem").then(|| projection_select(body).to_owned()),
        return_values: None,
        return_consumed_capacity: Some(consumed_capacity(request)),
    }
}

/// The keys of one transaction item: `sub` is the item's member name (`Get`,
/// `Put`, `Update`, `Delete` or `ConditionCheck`), `item` its body and
/// `request` the whole request, which carries `ReturnConsumedCapacity`.
pub(crate) fn transact_item(sub: &str, item: &Value, request: &Value) -> ParameterKeys {
    ParameterKeys {
        select: (sub == "Get").then(|| projection_select(item).to_owned()),
        return_values: matches!(sub, "Put" | "Update" | "Delete").then(|| NONE.to_owned()),
        return_consumed_capacity: Some(consumed_capacity(request)),
    }
}

/// The request's own value of `parameter`, when it is a string. A `null` is
/// absent, as the engine reads it.
fn given(request: &Value, parameter: &str) -> Option<String> {
    request
        .get(parameter)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

/// Whether a read projects. The engine projects whenever the request carries
/// `ProjectionExpression` or `AttributesToGet`, and refuses an empty one, so a
/// read without either returns every attribute. A `null` member is absent.
fn has_projection(read: &Value) -> bool {
    read.get("ProjectionExpression")
        .is_some_and(Value::is_string)
        || read.get("AttributesToGet").is_some_and(Value::is_array)
}

/// `dynamodb:Select` of a read that takes no `Select` parameter.
fn projection_select(read: &Value) -> &'static str {
    if has_projection(read) {
        SPECIFIC_ATTRIBUTES
    } else {
        ALL_ATTRIBUTES
    }
}

/// `dynamodb:Select` of a `Query` or `Scan` with no `Select`.
fn range_read_select(read: &Value) -> &'static str {
    if has_projection(read) {
        SPECIFIC_ATTRIBUTES
    } else if read.get("IndexName").is_some_and(Value::is_string) {
        ALL_PROJECTED_ATTRIBUTES
    } else {
        ALL_ATTRIBUTES
    }
}

/// `dynamodb:ReturnConsumedCapacity` of a request that takes the parameter.
fn consumed_capacity(request: &Value) -> String {
    given(request, "ReturnConsumedCapacity").unwrap_or_else(|| NONE.to_owned())
}

#[cfg(test)]
mod tests;
