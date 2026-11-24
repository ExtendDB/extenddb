// SPDX-License-Identifier: Apache-2.0

//! Tests for the place of the `ExpressionAttributeNames` checks among the
//! request-time `Query` checks.

use serde_json::json;

use super::tests::{
    EMPTY, INVALID_KEY, INVALID_VALUE, LIMIT_BELOW_ONE, LIMIT_ZERO, MIX, NULL, NUMERIC_BOB,
    RCC_BAD, SELECT_BAD, SEVERAL, body_with, message, one_error,
};
use super::validate_query_request_body;

#[test]
fn the_names_checks_take_the_service_order() {
    // Measured 2026-11-19, each body with the names map ahead of the other
    // members and after them, with the same answer in both orders. With
    // readable values, the names checks come after a Limit of 0, an invalid
    // ReturnConsumedCapacity and the mix, and ahead of an N value that is not
    // a number, a set with a duplicate member and an invalid Select. Beside a
    // value the service cannot read as one type, the key checks and the empty
    // map come after the reported-together Select, ReturnConsumedCapacity and
    // Limit step and the mix, and ahead of the value. An empty name comes
    // after the value, and is reported when no value has a fault of its own
    // (`{"S": "a", "X": "1"}` reads as an S). With KeyConditions and no
    // KeyConditionExpression, the names are checked though no expression
    // reads them.
    //
    // Each row holds: whether the key condition is KeyConditions (else `pk =
    // :p` with `:p = {"S": "x"}`), the names map, the members (those that
    // start with `:` go into ExpressionAttributeValues), and the answer.
    let (rcc, select, limit) = (
        one_error(RCC_BAD),
        one_error(SELECT_BAD),
        one_error(LIMIT_ZERO),
    );
    let value = |reason: &str, key: &str| format!("{INVALID_VALUE}{reason} for key {key}");
    let (empty_x, empty_v) = (value(EMPTY, ":x"), value(EMPTY, ":v"));
    let (several_v, null_x) = (value(SEVERAL, ":v"), value(NULL, ":x"));
    let empty_map = "ExpressionAttributeNames must not be empty";
    let empty_name =
        "ExpressionAttributeNames contains invalid value: Empty attribute name for key #a";
    let key_b = r#"ExpressionAttributeNames contains invalid key: Syntax error; key: "b""#;
    let rows = [
        json!([false, {"a": "b"}, {":n": {"N": "bob"}}, INVALID_KEY]),
        json!([false, {}, {":n": {"N": "bob"}}, empty_map]),
        json!([false, {"#a": ""}, {":n": {"N": "bob"}}, empty_name]),
        json!([false, {"a": "b"}, {":d": {"NS": ["1", "1"]}}, INVALID_KEY]),
        json!([false, {"a": "b"}, {"Select": "bad"}, INVALID_KEY]),
        json!([false, {"a": "b"}, {":n": {"N": "bob"}, "Limit": 0}, LIMIT_BELOW_ONE]),
        json!([false, {"a": "b"}, {":n": {"N": "bob"}, "ReturnConsumedCapacity": "bad"}, rcc]),
        json!([false, {"a": "b"}, {":n": {"N": "bob"}, "AttributesToGet": ["a"]}, MIX]),
        json!([false, {"#a": "a"}, {":n": {"N": "bob"}}, NUMERIC_BOB]),
        json!([false, {"a": "b"}, {":x": {"X": "1"}}, INVALID_KEY]),
        json!([false, {"a": "b"}, {":x": {}}, INVALID_KEY]),
        json!([false, {"a": "b"}, {":x": {"N": "1", "S": "x"}}, INVALID_KEY]),
        json!([false, {"a": "b"}, {":x": {"X": "1"}, "Limit": 0}, limit]),
        json!([false, {"a": "b"}, {":x": {"X": "1"}, "Select": "bad"}, select]),
        json!([false, {"a": "b"}, {":x": {"X": "1"}, "ReturnConsumedCapacity": "bad"}, rcc]),
        json!([false, {"a": "b"}, {":x": {"X": "1"}, "AttributesToGet": ["a"]}, MIX]),
        json!([false, {"#a": ""}, {":x": {"X": "1"}}, empty_x]),
        json!([false, {"#a": ""}, {":x": {}}, empty_x]),
        json!([false, {"#a": ""}, {":v": {"N": "1", "S": "x"}}, several_v]),
        json!([false, {"#a": ""}, {":x": null}, null_x]),
        json!([false, {"#a": ""}, {":v": {"S": null}}, empty_v]),
        json!([false, {"#a": "", "b": "c"}, {":x": {"X": "1"}}, key_b]),
        json!([false, {"#a": ""}, {":a": {"S": "a", "X": "1"}}, empty_name]),
        json!([false, {"#a": ""}, {":v": {"S": null, "N": "1"}}, empty_name]),
        json!([true, {"a": "b"}, {}, INVALID_KEY]),
        json!([true, {"a": "b"}, {":v": {"S": "x"}}, INVALID_KEY]),
        json!([true, {"a": "b"}, {":v": {"N": "bob"}}, INVALID_KEY]),
        json!([true, {"#a": "a"}, {":v": {"N": "bob"}}, NUMERIC_BOB]),
    ];
    for row in rows {
        let mut body = if row[0] == true {
            json!({"TableName": "t", "KeyConditions": {
                "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}}})
        } else {
            body_with(json!({":p": {"S": "x"}}), json!({}))
        };
        body["ExpressionAttributeNames"] = row[1].clone();
        for (member, value) in row[2].as_object().into_iter().flatten() {
            if member.starts_with(':') {
                body["ExpressionAttributeValues"][member] = value.clone();
            } else {
                body[member] = value.clone();
            }
        }
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            row[3].as_str(),
            "{body}"
        );
    }
}
