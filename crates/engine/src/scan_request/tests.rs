// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The expected texts below were measured against the service on 2026-10-25
//! (us-east-1, each request twice), except where a test says otherwise: the
//! deserializer test pins this module's hand-off, not a service text. The
//! range constraint rows are in `range_tests`.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::{CheckOrder, validate_scan_request_body};

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
    message(validate_scan_request_body(body, CheckOrder::of(body), None))
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
fn range_faults_come_first() {
    const SEGMENT_LOW: &str = "1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0";
    let base = bad_number();
    for limit in [0, -1] {
        assert_eq!(
            check(&with(base.clone(), "Limit", json!(limit))).as_deref(),
            Some(&*format!("1 validation error detected: {LIMIT_RANGE}"))
        );
    }
    // A range fault beats the parameter mix and the ReturnConsumedCapacity enum.
    let scan_filter =
        json!({"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}});
    let mix = with(
        with(base.clone(), "Segment", json!(-1)),
        "ScanFilter",
        scan_filter.clone(),
    );
    assert_eq!(check(&mix).as_deref(), Some(SEGMENT_LOW));
    let rcc = with(
        with(base.clone(), "Segment", json!(-1)),
        "ReturnConsumedCapacity",
        json!("NOPE"),
    );
    assert_eq!(check(&rcc).as_deref(), Some(SEGMENT_LOW));
    let rcc = with(
        with(base.clone(), "Limit", json!(0)),
        "ReturnConsumedCapacity",
        json!("NOPE"),
    );
    assert_eq!(
        check(&rcc).as_deref(),
        Some(&*format!("1 validation error detected: {LIMIT_RANGE}"))
    );
    // Every range fault is reported, ahead of the mix, both enums and the number.
    let all = with(
        with(
            with(with(base.clone(), "Limit", json!(0)), "Segment", json!(-1)),
            "TotalSegments",
            json!(0),
        ),
        "ScanFilter",
        scan_filter.clone(),
    );
    assert_eq!(
        check(&all).as_deref(),
        Some(
            "3 validation errors detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0; Value at 'TotalSegments' failed to satisfy constraint: Member must have value greater than or equal to 1"
        )
    );
    let mut two = with(with(base, "Limit", json!(0)), "Segment", json!(1_000_000));
    two["ScanFilter"] = scan_filter;
    two["ReturnConsumedCapacity"] = json!("NOPE");
    two["Select"] = json!("NOPE");
    assert_eq!(
        check(&two).as_deref(),
        Some(
            "2 validation errors detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value at 'Segment' failed to satisfy constraint: Member must have value less than or equal to 999999"
        )
    );
    // Values at the bounds pass.
    assert!(
        check(
            &json!({"TableName": "t", "Limit": 1, "Segment": 999_999, "TotalSegments": 1_000_000})
        )
        .is_none()
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
    // The ReturnConsumedCapacity enum comes ahead of the mix (measured
    // 2026-11-05).
    assert_eq!(
        check(&with(mix, "ReturnConsumedCapacity", json!("NOPE"))).as_deref(),
        Some(RCC_ENUM)
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
    // An empty ScanFilter alone is accepted, and an unused N bob beside it
    // answers the number (measured 2026-10-25).
    assert!(check(&json!({"TableName": "t", "ScanFilter": {}})).is_none());
    assert_eq!(
        check(&json!({
            "TableName": "t",
            "ScanFilter": {},
            "ExpressionAttributeValues": {":n": {"N": "bob"}},
        }))
        .as_deref(),
        Some(NUMERIC_BOB)
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
    // With every value readable (order R) the Select enum is not a
    // request-time check: the handler reports it after the no-expression check
    // and the nesting check (measured 2026-11-05); beside an unreadable value
    // (order U) it is one.
    assert_eq!(check(&json!({"TableName": "t", "Select": "NOPE"})), None);
    assert_eq!(
        check(&json!({
            "TableName": "t",
            "Select": "NOPE",
            "ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}},
        }))
        .as_deref(),
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
fn a_shape_fault_anywhere_in_the_body_is_reported_first() {
    // Measured 2026-10-25: a field of the wrong
    // JSON type in another field answers SerializationException ahead of the
    // range, mix and number checks.
    let shape_faults = [
        json!({"TableName": "t", "Limit": 0, "ExpressionAttributeValues": "x"}),
        json!({"TableName": "t", "Segment": -1, "TotalSegments": "2"}),
        with(bad_number(), "ExpressionAttributeNames", json!("x")),
        json!({"TableName": "t", "AttributesToGet": {"a": 1}, "ProjectionExpression": "a"}),
        json!({"TableName": "t", "ScanFilter": [1], "FilterExpression": "a = :n",
               "ExpressionAttributeValues": {":n": {"N": "1"}}}),
        json!({"TableName": "t", "Limit": 0, "Select": 5}),
    ];
    for body in shape_faults {
        assert!(
            matches!(
                validate_scan_request_body(&body, CheckOrder::of(&body), None),
                Err(DynamoDbError::SerializationException(_))
            ),
            "{body}"
        );
    }
    // A string enum that is not a member is the checks' answer (not a shape
    // fault), and a ValidationException the deserializer raises (an empty NS)
    // does not preempt a check.
    assert_eq!(
        check(&with(
            with(bad_number(), "Limit", json!(0)),
            "Select",
            json!("NOPE")
        ))
        .as_deref(),
        Some(&*format!("1 validation error detected: {LIMIT_RANGE}"))
    );
    assert_eq!(
        check(&json!({
            "TableName": "t", "Limit": 0,
            "ExpressionAttributeValues": {":s": {"NS": []}},
        }))
        .as_deref(),
        Some(&*format!("1 validation error detected: {LIMIT_RANGE}"))
    );
    // A body that passes every check is left to the handler's deserializer: a
    // string Limit and a malformed AttributeValue are not range or number
    // faults here.
    assert!(check(&json!({"TableName": "t", "Limit": "0"})).is_none());
    assert!(
        check(&json!({"TableName": "t", "ExpressionAttributeValues": {":n": {"X": "bob"}}}))
            .is_none()
    );
}

#[test]
fn unicode_decimal_digits_pass_the_number_check() {
    // Measured 2026-10-25: fullwidth and Arabic-Indic digits are read as digits, thirty nine
    // Devanagari ones answer the 38 digit message, a superscript, a fraction, a
    // fullwidth minus and a fullwidth full stop are refused with the value as
    // sent.
    for raw in ["\u{ff11}", "\u{661}", "\u{ff17}", "\u{ff11}e2"] {
        assert!(
            check(&with(
                bad_number(),
                "ExpressionAttributeValues",
                json!({":n": {"N": raw}})
            ))
            .is_none(),
            "{raw:?}"
        );
    }
    assert!(
        check(&json!({
            "TableName": "t", "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":s": {"NS": ["1", "\u{ff12}"]}},
        }))
        .is_none()
    );
    assert_eq!(
        check(&with(
            bad_number(),
            "ExpressionAttributeValues",
            json!({":n": {"N": "\u{967}".repeat(39)}})
        ))
        .as_deref(),
        Some("Attempting to store more than 38 significant digits in a Number")
    );
    for raw in ["\u{b2}", "\u{bd}", "\u{ff0d}1", "1\u{ff0e}5"] {
        assert_eq!(
            check(&with(
                bad_number(),
                "ExpressionAttributeValues",
                json!({":n": {"N": raw}})
            ))
            .as_deref(),
            Some(format!("The parameter cannot be converted to a numeric value: {raw}").as_str()),
            "{raw:?}"
        );
    }
}
