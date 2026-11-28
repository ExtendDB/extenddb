// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A JSON null in the members Amazon `DynamoDB` reads as absent or refuses
//! with a `ValidationException` (measured 2026-11-14, and 2026-11-17 for a
//! null item attribute that is not the key).

use serde_json::{Value, json};

use super::{PutItemInput, ReturnConsumedCapacity, ScanInput};

/// The `ReturnConsumedCapacity` the input of `operation` reads from `body`,
/// or the text of the deserializer's refusal.
fn read(operation: &str, body: Value) -> Result<ReturnConsumedCapacity, String> {
    match operation {
        "Scan" => serde_json::from_value(body).map(|i: ScanInput| i.return_consumed_capacity),
        "PutItem" => serde_json::from_value(body).map(|i: PutItemInput| i.return_consumed_capacity),
        other => panic!("no input type for {other}"),
    }
    .map_err(|e| e.to_string())
}

#[test]
fn a_json_null_member_is_read_as_absent_or_refused_as_the_service_refuses_it() {
    let null_item = "The supplied Item contains a null AttributeValue";
    let empty_value =
        "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";
    let not_a_map = "invalid type: string \"x\", expected a DynamoDB AttributeValue map with \
                     exactly one type descriptor";
    let number_in_s = "S value must be a string";
    let rows = [
        (
            "Scan",
            json!({"TableName": "t", "ReturnConsumedCapacity": null}),
            Ok(ReturnConsumedCapacity::None),
        ),
        (
            "Scan",
            json!({"TableName": "t", "AttributesToGet": [null]}),
            Err("Empty attribute name".to_owned()),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": null}}),
            Err(format!("{null_item}: pk is null")),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k1"}, "a": null, "b": {}}}),
            Err(format!("{null_item}: a is null")),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k1"}, "a": {}, "b": null}}),
            Err(empty_value.to_owned()),
        ),
        // A value of the wrong JSON type is refused ahead of a null or
        // empty value wherever it stands (measured 2026-11-17).
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k1"}, "a": null, "b": "x"}}),
            Err(not_a_map.to_owned()),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k1"}, "a": null, "b": {"S": 1}}}),
            Err(number_in_s.to_owned()),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k1"}, "b": "x", "a": null}}),
            Err(not_a_map.to_owned()),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k1"}, "a": {"S": 1}, "b": null}}),
            Err(number_in_s.to_owned()),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k1"}, "a": {}, "b": {"S": 1}}}),
            Err(number_in_s.to_owned()),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k1"}, "a": {}, "b": "x"}}),
            Err(not_a_map.to_owned()),
        ),
    ];
    for (operation, body, expected) in rows {
        let request = body.to_string();
        assert_eq!(read(operation, body), expected, "{operation} {request}");
    }
}
