// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The text of a refused `ConditionalOperator` on the five operations that
//! take one. Query and Scan send the enum clause alone, ahead of the operand
//! count of a legacy filter condition. PutItem, UpdateItem and DeleteItem
//! send it inside the "1 validation error detected" envelope. Measured
//! 2026-11-13 and 2026-11-14 (us-east-1).

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use crate::dispatch;
use crate::partiql::test_context::context;

const ENVELOPE: &str = "1 validation error detected: ";

/// The operations that take a `ConditionalOperator`, with the envelope the
/// service puts around the enum clause on each.
const OPERATIONS: [(&str, &str); 5] = [
    ("Query", ""),
    ("Scan", ""),
    ("PutItem", ENVELOPE),
    ("UpdateItem", ENVELOPE),
    ("DeleteItem", ENVELOPE),
];

/// The refused values of the measured requests.
const VALUES: [&str; 5] = ["XOR", "and", "", "ALL_OLD", "ALL"];

fn condition(operator: &str, values: &[&str]) -> Value {
    let values: Vec<Value> = values.iter().map(|v| json!({"S": v})).collect();
    json!({"ComparisonOperator": operator, "AttributeValueList": values})
}

/// One row per measured request: the operation, its `ConditionalOperator`,
/// the first of its two legacy conditions, and the envelope of the answer.
/// The last two rows hold a `BETWEEN` with one value.
fn rows() -> Vec<(&'static str, &'static str, Value, &'static str)> {
    let mut rows = Vec::new();
    for (operation, envelope) in OPERATIONS {
        for value in VALUES {
            rows.push((operation, value, condition("EQ", &["x"]), envelope));
        }
    }
    for operation in ["Query", "Scan"] {
        rows.push((operation, "XOR", condition("BETWEEN", &["x"]), ""));
    }
    rows
}

/// `operation`'s request with `op` as its `ConditionalOperator`, and `first`
/// and an `EQ` as the two entries of its `QueryFilter`, `ScanFilter` or
/// `Expected`.
fn request(operation: &str, op: &str, first: Value) -> Value {
    let two = json!({"a": first, "b": condition("EQ", &["y"])});
    let key = json!({"pk": {"S": "k"}, "sk": {"S": "s"}});
    let mut body = match operation {
        "Query" => json!({"KeyConditions": {"pk": condition("EQ", &["k"])}, "QueryFilter": two}),
        "Scan" => json!({"ScanFilter": two}),
        "PutItem" => json!({"Item": key, "Expected": two}),
        "UpdateItem" => json!({
            "Key": key,
            "Expected": two,
            "AttributeUpdates": {"c": {"Value": {"S": "v"}}},
        }),
        "DeleteItem" => json!({"Key": key, "Expected": two}),
        other => panic!("no request shape for {other}"),
    };
    body["TableName"] = json!("tbl");
    body["ConditionalOperator"] = json!(op);
    body
}

#[tokio::test]
async fn refused_conditional_operator_text() {
    let ctx = context(None).await;
    for (operation, value, first, envelope) in rows() {
        let body = request(operation, value, first);
        let shown = body.to_string();
        let expected = format!(
            "{envelope}Value '{value}' at 'conditionalOperator' failed to satisfy \
             constraint: Member must satisfy enum value set: [AND, OR]"
        );
        match dispatch(operation, body, &ctx, "").await {
            Err(DynamoDbError::ValidationException(msg)) => {
                assert_eq!(msg, expected, "{operation} {shown}");
            }
            Err(other) => panic!("{operation} answered {other:?} to {shown}"),
            Ok(_) => panic!("{operation} accepted {shown}"),
        }
    }
}

/// A string where the `AttributesToGet` list goes, written ahead of an
/// invalid `ConditionalOperator`, answers `SerializationException`, the
/// exception DynamoDB sends (measured 2026-11-17, us-east-1).
#[tokio::test]
async fn unreadable_attributes_to_get_answers_before_conditional_operator() {
    let ctx = context(None).await;
    for operation in ["Query", "Scan"] {
        let mut body = request(operation, "XOR", condition("EQ", &["x"]));
        // The workspace build turns on serde_json's preserve_order, which
        // reads members in the order written, so ConditionalOperator goes last.
        body.as_object_mut().unwrap().remove("ConditionalOperator");
        body["AttributesToGet"] = json!("x");
        body["ConditionalOperator"] = json!("XOR");
        match dispatch(operation, body.clone(), &ctx, "").await {
            Err(DynamoDbError::SerializationException(_)) => {}
            Err(other) => panic!("{operation} answered {other:?} to {body}"),
            Ok(_) => panic!("{operation} accepted {body}"),
        }
    }
}
