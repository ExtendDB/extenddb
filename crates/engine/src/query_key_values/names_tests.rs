// SPDX-License-Identifier: Apache-2.0

//! Tests for the place of the `ExpressionAttributeNames` check among the
//! request-time `Query` checks.

use serde_json::{Value, json};

use super::tests::{body_with, message};
use super::{LIMIT_BELOW_ONE, validate_query_request_body};

const INVALID_KEY: &str =
    r#"ExpressionAttributeNames contains invalid key: Syntax error; key: "a""#;
const NUMERIC_BOB: &str = "The parameter cannot be converted to a numeric value: bob";
const RCC: &str = "1 validation error detected: Value 'bad' at 'returnConsumedCapacity' \
                   failed to satisfy constraint: Member must satisfy enum value set: \
                   [INDEXES, TOTAL, NONE]";
const SELECT: &str = "1 validation error detected: Value 'bad' at 'select' failed to satisfy \
                      constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, \
                      COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]";
const MIX: &str = "Can not use both expression and non-expression parameters in the same \
                   request: Non-expression parameters: {AttributesToGet} Expression \
                   parameters: {KeyConditionExpression}";

/// The answer for a body with `values` in `ExpressionAttributeValues`, `names`
/// in `ExpressionAttributeNames` and the `extra` members.
fn answer(values: Value, names: Value, extra: Value) -> Option<String> {
    let mut body = body_with(values, extra);
    body["ExpressionAttributeNames"] = names;
    message(validate_query_request_body(&body))
}

#[test]
fn an_invalid_names_map_is_reported_after_the_mix_and_ahead_of_the_numbers() {
    // Measured 2026-11-19, with the names map ahead of the other members and
    // after them: each names fault beside `{"N": "bob"}` answers the names
    // text, as an invalid key does beside a set with a duplicate member and
    // beside an invalid Select. A Limit of 0, an invalid
    // ReturnConsumedCapacity and a mix are reported ahead of an invalid key.
    let bad_number = json!({":p": {"S": "x"}, ":n": {"N": "bob"}});
    for (names, expected) in [
        (json!({"a": "b"}), INVALID_KEY),
        (json!({}), "ExpressionAttributeNames must not be empty"),
        (
            json!({"#a": ""}),
            "ExpressionAttributeNames contains invalid value: Empty attribute name for key #a",
        ),
    ] {
        assert_eq!(
            answer(bad_number.clone(), names.clone(), json!({})).as_deref(),
            Some(expected),
            "{names}"
        );
    }
    for (values, extra) in [
        (
            json!({":p": {"S": "x"}, ":d": {"NS": ["1", "1"]}}),
            json!({}),
        ),
        (json!({":p": {"S": "x"}}), json!({"Select": "bad"})),
    ] {
        assert_eq!(
            answer(values, json!({"a": "b"}), extra.clone()).as_deref(),
            Some(INVALID_KEY),
            "{extra}"
        );
    }
    for (extra, expected) in [
        (json!({"Limit": 0}), LIMIT_BELOW_ONE),
        (json!({"ReturnConsumedCapacity": "bad"}), RCC),
        (json!({"AttributesToGet": ["a"]}), MIX),
    ] {
        assert_eq!(
            answer(bad_number.clone(), json!({"a": "b"}), extra.clone()).as_deref(),
            Some(expected),
            "{extra}"
        );
    }
    // A names map the deserializer accepts, used or not, leaves the answer to
    // the number check.
    assert_eq!(
        answer(bad_number, json!({"#a": "a"}), json!({})).as_deref(),
        Some(NUMERIC_BOB)
    );
}

#[test]
fn beside_a_value_the_service_cannot_read_the_names_map_follows_the_mix() {
    // Measured 2026-11-19, in either member order: an invalid key is reported
    // ahead of a value the service cannot read as one type, and the
    // reported-together Select, ReturnConsumedCapacity and Limit step and the
    // mix are reported ahead of the key.
    for value in [json!({"X": "1"}), json!({}), json!({"N": "1", "S": "x"})] {
        assert_eq!(
            answer(
                json!({":p": {"S": "x"}, ":x": value}),
                json!({"a": "b"}),
                json!({})
            )
            .as_deref(),
            Some(INVALID_KEY),
            "{value}"
        );
    }
    let limit = "1 validation error detected: Value '0' at 'limit' failed to satisfy \
                 constraint: Member must have value greater than or equal to 1";
    for (extra, expected) in [
        (json!({"Limit": 0}), limit),
        (json!({"Select": "bad"}), SELECT),
        (json!({"ReturnConsumedCapacity": "bad"}), RCC),
        (json!({"AttributesToGet": ["a"]}), MIX),
    ] {
        assert_eq!(
            answer(
                json!({":p": {"S": "x"}, ":x": {"X": "1"}}),
                json!({"a": "b"}),
                extra.clone()
            )
            .as_deref(),
            Some(expected),
            "{extra}"
        );
    }
}

#[test]
fn the_names_map_is_checked_when_no_expression_reads_it() {
    // Measured 2026-11-19 with KeyConditions and no KeyConditionExpression: an
    // invalid key is reported alone, beside a value nothing reads and beside
    // `{"N": "bob"}`. A valid names map leaves the number check to answer.
    let key_conditions =
        json!({"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}});
    for values in [
        None,
        Some(json!({":v": {"S": "x"}})),
        Some(json!({":v": {"N": "bob"}})),
    ] {
        let mut body = json!({
            "TableName": "t",
            "KeyConditions": key_conditions,
            "ExpressionAttributeNames": {"a": "b"},
        });
        if let Some(values) = values {
            body["ExpressionAttributeValues"] = values;
        }
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            Some(INVALID_KEY),
            "{body}"
        );
    }
    let body = json!({
        "TableName": "t",
        "KeyConditions": key_conditions,
        "ExpressionAttributeNames": {"#a": "a"},
        "ExpressionAttributeValues": {":v": {"N": "bob"}},
    });
    assert_eq!(
        message(validate_query_request_body(&body)).as_deref(),
        Some(NUMERIC_BOB)
    );
}
