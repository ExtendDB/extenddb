// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `CreateTable` with an empty `GlobalSecondaryIndexes` or
//! `LocalSecondaryIndexes` list, read from the request body as the handler
//! reads it. DynamoDB refused every row in us-east-1, the first six on
//! 2026-11-14 and the others on 2026-11-18.

use serde_json::{Value, json};

use super::validate_create_table;
use crate::limits::LimitsConfig;
use crate::types::CreateTableInput;

/// An on-demand table keyed on `pk` and `sk`, with a string definition for
/// each name in `defs`, and `members` added.
fn request(defs: &[&str], members: Value) -> Value {
    let defs: Vec<Value> = defs
        .iter()
        .map(|name| json!({"AttributeName": name, "AttributeType": "S"}))
        .collect();
    let mut body = json!({
        "TableName": "empty-index-list",
        "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": defs,
    });
    let Value::Object(members) = members else {
        unreachable!("members is an object")
    };
    body.as_object_mut().expect("an object").extend(members);
    body
}

fn answer(body: &Value) -> Result<(), String> {
    let input: CreateTableInput = serde_json::from_value(body.clone()).expect("the body reads");
    validate_create_table(&input, &LimitsConfig::default()).map_err(|e| e.to_string())
}

/// Each row names the member DynamoDB names. A row with `None` has another
/// fault that DynamoDB answers first: it gets the answer of the same request
/// without `GlobalSecondaryIndexes`, which is a refusal.
#[test]
fn an_empty_index_list_gets_the_answer_dynamodb_gives() {
    let gsi = json!([{
        "IndexName": "by-label",
        "KeySchema": [{"AttributeName": "label", "KeyType": "HASH"}],
        "Projection": {"ProjectionType": "ALL"},
    }]);
    let lsi = json!([{
        "IndexName": "by-stamp",
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "stamp", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    }]);
    let four = ["pk", "sk", "label", "stamp"];
    let two = ["pk", "sk"];
    let two_hash_keys = json!([
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "HASH"},
    ]);
    let rows = [
        (
            request(&four, json!({"GlobalSecondaryIndexes": []})),
            Some("GlobalSecondaryIndexes"),
        ),
        (
            request(&four, json!({"LocalSecondaryIndexes": []})),
            Some("LocalSecondaryIndexes"),
        ),
        (
            request(
                &four,
                json!({"GlobalSecondaryIndexes": [], "LocalSecondaryIndexes": []}),
            ),
            Some("LocalSecondaryIndexes"),
        ),
        (
            request(
                &four,
                json!({"GlobalSecondaryIndexes": [], "LocalSecondaryIndexes": lsi}),
            ),
            Some("GlobalSecondaryIndexes"),
        ),
        (
            request(
                &four,
                json!({"GlobalSecondaryIndexes": gsi, "LocalSecondaryIndexes": []}),
            ),
            Some("LocalSecondaryIndexes"),
        ),
        (
            request(&two, json!({"GlobalSecondaryIndexes": []})),
            Some("GlobalSecondaryIndexes"),
        ),
        (
            request(
                &two,
                json!({"GlobalSecondaryIndexes": [], "BillingMode": "PROVISIONED"}),
            ),
            None,
        ),
        (
            request(
                &two,
                json!({"GlobalSecondaryIndexes": [], "KeySchema": two_hash_keys}),
            ),
            None,
        ),
        (
            request(&["pk"], json!({"GlobalSecondaryIndexes": []})),
            None,
        ),
        (
            request(
                &two,
                json!({"GlobalSecondaryIndexes": [], "LocalSecondaryIndexes": lsi}),
            ),
            None,
        ),
    ];
    for (body, named) in rows {
        let expected = match named {
            Some(member) => Err(format!(
                "One or more parameter values were invalid: List of {member} is empty"
            )),
            None => {
                let mut without = body.clone();
                without
                    .as_object_mut()
                    .expect("an object")
                    .remove("GlobalSecondaryIndexes");
                let refusal = answer(&without);
                assert!(refusal.is_err(), "{without}");
                refusal
            }
        };
        assert_eq!(answer(&body), expected, "{body}");
    }
    // An absent member is not a list: the sixth request passes without it.
    assert_eq!(answer(&request(&two, json!({}))), Ok(()));
}
