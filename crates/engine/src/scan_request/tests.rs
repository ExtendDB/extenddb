// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Every expected text below was measured against the service on 2026-10-25
//! (us-east-1, each request twice).

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::validate_scan_request_body;

const NUMERIC_BOB: &str = "The parameter cannot be converted to a numeric value: bob";
const LIMIT_RANGE: &str = "Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1";
const RCC_ENUM: &str = "1 validation error detected: Value 'NOPE' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]";
const SELECT_ENUM: &str = "1 validation error detected: Value 'NOPE' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]";

fn message(result: Result<(), DynamoDbError>) -> Option<String> {
    match result {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

fn check(body: &Value) -> Option<String> {
    message(validate_scan_request_body(body))
}

/// A Scan whose filter reads `:n = {"N": "bob"}`.
fn bad_number() -> Value {
    json!({
        "TableName": "t",
        "FilterExpression": "a = :n",
        "ExpressionAttributeValues": {":n": {"N": "bob"}},
    })
}

fn with(mut body: Value, field: &str, value: Value) -> Value {
    body[field] = value;
    body
}

#[test]
fn numbers_are_checked_whether_or_not_an_expression_reads_them() {
    assert_eq!(check(&bad_number()).as_deref(), Some(NUMERIC_BOB));
    // Nothing reads :n; the filter reads :p.
    assert_eq!(
        check(&json!({
            "TableName": "t",
            "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":n": {"N": "bob"}},
        }))
        .as_deref(),
        Some(NUMERIC_BOB)
    );
    // No expression at all, a ScanFilter only, a ProjectionExpression only.
    for body in [
        json!({"TableName": "t", "ExpressionAttributeValues": {":n": {"N": "bob"}}}),
        json!({
            "TableName": "t",
            "ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}},
            "ExpressionAttributeValues": {":n": {"N": "bob"}},
        }),
        json!({
            "TableName": "t",
            "ProjectionExpression": "a",
            "ExpressionAttributeValues": {":n": {"N": "bob"}},
        }),
    ] {
        assert_eq!(check(&body).as_deref(), Some(NUMERIC_BOB), "{body}");
    }
}

#[test]
fn nested_numbers_and_the_other_number_faults_are_reported_with_the_service_text() {
    let cases = [
        (json!({"L": [{"S": "a"}, {"N": "bob"}]}), NUMERIC_BOB),
        (json!({"M": {"a": {"N": "bob"}}}), NUMERIC_BOB),
        (json!({"NS": ["1", "bob"]}), NUMERIC_BOB),
        (
            json!({"M": {"a": {"L": [{"M": {"b": {"N": "bob"}}}]}}}),
            NUMERIC_BOB,
        ),
        (
            json!({"N": "1e200"}),
            "Number overflow. Attempting to store a number with magnitude larger than supported range",
        ),
        (
            json!({"N": "1e-200"}),
            "Number underflow. Attempting to store a number with magnitude smaller than supported range",
        ),
        (
            json!({"N": "1".repeat(39)}),
            "Attempting to store more than 38 significant digits in a Number",
        ),
        (
            json!({"N": ""}),
            "The parameter cannot be converted to a numeric value: ",
        ),
        (
            json!({"N": " 1"}),
            "The parameter cannot be converted to a numeric value:  1",
        ),
    ];
    for (value, expected) in cases {
        let body = json!({
            "TableName": "t",
            "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":n": value},
        });
        assert_eq!(check(&body).as_deref(), Some(expected), "{body}");
    }
    // A well-formed exponent passes.
    assert!(
        check(&json!({
            "TableName": "t",
            "FilterExpression": "a = :n",
            "ExpressionAttributeValues": {":n": {"N": "1.0e1"}},
        }))
        .is_none()
    );
}

#[test]
fn limit_segment_and_total_segments_ranges_come_first_and_aggregate() {
    let base = bad_number();
    assert_eq!(
        check(&with(base.clone(), "Limit", json!(0))).as_deref(),
        Some(&*format!("1 validation error detected: {LIMIT_RANGE}"))
    );
    assert_eq!(
        check(&with(base.clone(), "Limit", json!(-1))).as_deref(),
        Some(&*format!("1 validation error detected: {LIMIT_RANGE}"))
    );
    // Each segment field is range checked when present, with or without the
    // other one.
    for (field, value, text) in [
        (
            "Segment",
            -1,
            "Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0",
        ),
        (
            "Segment",
            1_000_000,
            "Value at 'Segment' failed to satisfy constraint: Member must have value less than or equal to 999999",
        ),
        (
            "TotalSegments",
            0,
            "Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1",
        ),
        (
            "TotalSegments",
            1_000_001,
            "Value at 'TotalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000",
        ),
    ] {
        assert_eq!(
            check(&with(base.clone(), field, json!(value))).as_deref(),
            Some(&*format!("1 validation error detected: {text}")),
            "{field} {value}"
        );
        let partner = if field == "Segment" {
            ("TotalSegments", 2)
        } else {
            ("Segment", 0)
        };
        let paired = with(
            with(base.clone(), field, json!(value)),
            partner.0,
            json!(partner.1),
        );
        assert_eq!(
            check(&paired).as_deref(),
            Some(&*format!("1 validation error detected: {text}")),
            "{field} {value} with {}",
            partner.0
        );
    }
    // Limit, Segment, TotalSegments, in that order, one message.
    let three = with(
        with(
            with(json!({"TableName": "t"}), "Limit", json!(0)),
            "Segment",
            json!(-1),
        ),
        "TotalSegments",
        json!(1_000_001),
    );
    assert_eq!(
        check(&three).as_deref(),
        Some(
            "3 validation errors detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0; Value at 'TotalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"
        )
    );
    // Values at the bounds pass.
    assert!(
        check(
            &json!({"TableName": "t", "Limit": 1, "Segment": 999_999, "TotalSegments": 1_000_000})
        )
        .is_none()
    );
    // A range fault beats the parameter mix and the ReturnConsumedCapacity enum.
    let mix = with(
        with(base.clone(), "Segment", json!(-1)),
        "ScanFilter",
        json!({"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}}),
    );
    assert!(check(&mix).is_some_and(|m| m.contains("Value at 'Segment'")));
    let rcc = with(
        with(base, "Limit", json!(0)),
        "ReturnConsumedCapacity",
        json!("NOPE"),
    );
    assert_eq!(
        check(&rcc).as_deref(),
        Some(&*format!("1 validation error detected: {LIMIT_RANGE}"))
    );
}

#[test]
fn parameter_mix_beats_the_enum_and_the_numbers() {
    let scan_filter =
        json!({"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}});
    let mix = with(bad_number(), "ScanFilter", scan_filter.clone());
    assert_eq!(
        check(&mix).as_deref(),
        Some(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}"
        )
    );
    assert_eq!(
        check(&with(mix, "ReturnConsumedCapacity", json!("NOPE"))).as_deref(),
        Some(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}"
        )
    );
    let atg = with(
        with(bad_number(), "AttributesToGet", json!(["a"])),
        "ProjectionExpression",
        json!("a"),
    );
    assert_eq!(
        check(&atg).as_deref(),
        Some(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression, FilterExpression}"
        )
    );
    assert_eq!(
        check(&with(bad_number(), "ConditionalOperator", json!("AND"))).as_deref(),
        Some(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {ConditionalOperator} Expression parameters: {FilterExpression}"
        )
    );
    // ConditionalOperator with no expression does not join the mix; the
    // numbers are reported.
    assert_eq!(
        check(&json!({
            "TableName": "t",
            "ConditionalOperator": "AND",
            "ExpressionAttributeValues": {":n": {"N": "bob"}},
        }))
        .as_deref(),
        Some(NUMERIC_BOB)
    );
    // An empty ScanFilter or AttributesToGet does not join the mix.
    assert!(
        check(&json!({
            "TableName": "t",
            "ScanFilter": {},
            "AttributesToGet": [],
            "FilterExpression": "a = :n",
            "ExpressionAttributeValues": {":n": {"N": "1"}},
        }))
        .is_none()
    );
}

#[test]
fn enum_checks_sit_on_either_side_of_the_numbers() {
    assert_eq!(
        check(&with(bad_number(), "ReturnConsumedCapacity", json!("NOPE"))).as_deref(),
        Some(RCC_ENUM)
    );
    assert_eq!(
        check(&with(bad_number(), "Select", json!("NOPE"))).as_deref(),
        Some(NUMERIC_BOB)
    );
    // Both invalid: ReturnConsumedCapacity alone, not aggregated with Select.
    let both = with(
        with(json!({"TableName": "t"}), "Select", json!("NOPE")),
        "ReturnConsumedCapacity",
        json!("NOPE"),
    );
    assert_eq!(check(&both).as_deref(), Some(RCC_ENUM));
    // With the numbers valid, an invalid Select is reported.
    assert_eq!(
        check(&json!({"TableName": "t", "Select": "NOPE"})).as_deref(),
        Some(SELECT_ENUM)
    );
    // A valid request passes.
    assert!(
        check(&json!({
            "TableName": "t",
            "Select": "COUNT",
            "ReturnConsumedCapacity": "TOTAL",
            "Limit": 5,
            "FilterExpression": "a = :n",
            "ExpressionAttributeValues": {":n": {"N": "5"}},
        }))
        .is_none()
    );
}

#[test]
fn values_that_do_not_deserialize_are_left_to_the_deserializer() {
    // A string Limit and a malformed ExpressionAttributeValues are not range
    // or number faults here; the typed deserializer reports them next.
    assert!(check(&json!({"TableName": "t", "Limit": "0"})).is_none());
    assert!(
        check(&json!({"TableName": "t", "ExpressionAttributeValues": {":n": {"X": "bob"}}}))
            .is_none()
    );
}
