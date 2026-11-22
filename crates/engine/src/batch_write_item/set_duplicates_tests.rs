// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The text each operation gives for a set that holds a member twice.
//! Measured on the service in us-east-1 on 2026-11-22, each request twice,
//! on a table keyed on `pk` (S). `PutItem` (an item at any depth),
//! `UpdateItem` and `DeleteItem` put `1 validation error detected: ` ahead of
//! a text that lists the members sorted, and `TransactWriteItems`, `Query`
//! and `Scan` give that text as it is. `BatchWriteItem` lists the members as
//! sent, and so does every operation for a `BS` with a member that only the
//! second reading decodes. The answers come ahead of the table lookup, so
//! the rows send each request through [`dispatch`] with no table.

use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;

use super::padding_tests::context;
use crate::dispatch;

const TABLE: &str = "keyed-on-s";

/// The code and the message of the service's answer.
type Answer = (&'static str, String);

fn invalid(text: &str) -> Answer {
    (
        "ValidationException",
        format!("One or more parameter values were invalid: {text}"),
    )
}

fn single_write(text: &str) -> Answer {
    let (code, message) = invalid(text);
    (code, format!("1 validation error detected: {message}"))
}

/// An item with `value` in `s`.
fn item(value: &Value) -> Value {
    json!({"pk": {"S": "k"}, "s": value})
}

fn update(value: &Value) -> Value {
    json!({"TableName": TABLE, "Key": {"pk": {"S": "k"}}, "UpdateExpression": "SET s = :v",
           "ExpressionAttributeValues": {":v": value}})
}

fn batch(value: &Value) -> Value {
    json!({"RequestItems": {TABLE: [{"PutRequest": {"Item": item(value)}}]}})
}

fn transact(action: &str, request: &Value) -> Value {
    json!({"TransactItems": [{action: request}]})
}

#[tokio::test]
async fn each_operation_gives_its_duplicates_text() {
    let ss = json!({"SS": ["b", "a", "b"]});
    let ns = json!({"NS": ["1", "1.0"]});
    let bs = json!({"BS": ["Yg==", "YQ==", "Yg"]});
    let second = json!({"BS": ["Y=Q=", "YQ==", "//Q="]});
    let sorted_ss = r#"Input collection ["a", "b", "b"] contains duplicates."#;
    let sorted_bs = r#"Input collection ["YQ==","Yg==","Yg=="]of type BS contains duplicates."#;
    let no_list = "Input collection contains duplicates.";
    let second_bs = "Input collection [//Q=, YQ==, //Q=]of type BS contains duplicates.";
    let put = |item: Value| json!({"TableName": TABLE, "Item": item});
    let transact_put = |value: &Value| transact("Put", &put(item(value)));
    #[rustfmt::skip]
    let rows: [(&str, &str, Value, Answer); 19] = [
        ("O1", "UpdateItem", update(&ss), single_write(sorted_ss)),
        ("O2", "UpdateItem", update(&bs), single_write(sorted_bs)),
        ("O3", "UpdateItem", update(&ns), single_write(no_list)),
        ("O4", "UpdateItem", json!({"TableName": TABLE, "Key": {"pk": {"S": "k"}},
                                    "AttributeUpdates": {"s": {"Action": "PUT", "Value": ss}}}),
         single_write(sorted_ss)),
        ("O5", "BatchWriteItem", batch(&ss), invalid("Input collection [b, a, b] contains duplicates.")),
        ("O6", "TransactWriteItems", transact_put(&ss), invalid(sorted_ss)),
        ("O7", "Query", json!({"TableName": TABLE, "KeyConditionExpression": "pk = :k",
                               "FilterExpression": "s = :v",
                               "ExpressionAttributeValues": {":k": {"S": "x"}, ":v": ss}}),
         invalid(sorted_ss)),
        ("O8", "Scan", json!({"TableName": TABLE, "FilterExpression": "s = :v",
                              "ExpressionAttributeValues": {":v": bs}}),
         invalid(sorted_bs)),
        ("O9", "PutItem", put(json!({"pk": {"S": "k"}, "m": {"M": {"s": ss}}})), single_write(sorted_ss)),
        ("O10", "PutItem", put(json!({"pk": {"S": "k"}, "l": {"L": [bs]}})), single_write(sorted_bs)),
        ("O11", "DeleteItem", json!({"TableName": TABLE, "Key": {"pk": {"S": "k"}},
                                     "ConditionExpression": "s = :v",
                                     "ExpressionAttributeValues": {":v": ns}}),
         single_write(no_list)),
        // `a` and `b` stand for the service's `s` and `n`: they come in this
        // order whether or not the build keeps the order of a JSON map.
        ("O12", "PutItem", put(json!({"pk": {"S": "k"}, "a": ss, "b": ns})), single_write(sorted_ss)),
        ("O13", "BatchWriteItem", batch(&ns),
         ("ValidationException", "Input collection contains duplicates".to_owned())),
        ("O14", "BatchWriteItem", batch(&bs),
         ("SerializationException",
          "Base64 encoded length is expected a multiple of 4 bytes but found: 2".to_owned())),
        ("O15", "TransactWriteItems", transact("Update", &update(&ss)), invalid(sorted_ss)),
        ("O16", "TransactWriteItems", transact_put(&ns), invalid(no_list)),
        ("O17", "TransactWriteItems", transact_put(&bs), invalid(sorted_bs)),
        ("O18", "BatchWriteItem", batch(&second), invalid(second_bs)),
        ("P1", "PutItem", put(item(&second)), invalid(second_bs)),
    ];
    let (ctx, _dir) = context().await;
    let mut wrong = Vec::new();
    for (row, operation, body, want) in rows {
        let got = match dispatch(operation, body, &ctx, "").await {
            Err(DynamoDbError::ValidationException(message)) => ("ValidationException", message),
            Err(DynamoDbError::SerializationException(message)) => {
                ("SerializationException", message)
            }
            Err(other) => ("another error", format!("{other:?}")),
            Ok(_) => ("accepted", String::new()),
        };
        if got != want {
            wrong.push(format!("{row}: {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
