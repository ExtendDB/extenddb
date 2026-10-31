// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `ScanFilter` in the parameter mix, the `AttributesToGet` length, the
//! numbers in `ExclusiveStartKey` and the nesting depth of
//! `ExpressionAttributeValues`.
//!
//! The rows in every test but the last are requests sent to the service
//! (us-east-1, each request twice, a Scan of an existing table) with the
//! answers it gave: on 2026-10-25 at 10:38 and 10:55 UTC, and on 2026-10-30 at
//! 12:40 UTC where a test says so. The last test applies the same rules to
//! requests that were not sent.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::validate_scan_request_body;

const MIX: &str = "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}";
const LENGTH: &str = "1 validation error detected: Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1";
const NUMERIC_BOB: &str = "The parameter cannot be converted to a numeric value: bob";
const NESTING: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit";

fn check(body: &Value) -> Option<String> {
    match validate_scan_request_body(body) {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// A Scan body with the fields of `extra` added to the table name.
fn scan(extra: Value) -> Value {
    let mut body = json!({"TableName": "t"});
    for (field, value) in extra.as_object().expect("an object") {
        body[field] = value.clone();
    }
    body
}

/// A filter that reads `:n`, which holds `number`.
fn filter(number: &str) -> Value {
    json!({
        "FilterExpression": "a = :n",
        "ExpressionAttributeValues": {":n": {"N": number}},
    })
}

fn with(mut body: Value, extra: Value) -> Value {
    for (field, value) in extra.as_object().expect("an object") {
        body[field] = value.clone();
    }
    body
}

/// `leaf` inside `levels` lists, each list one level.
fn nested(leaf: &Value, levels: usize) -> Value {
    (0..levels).fold(leaf.clone(), |inner, _| json!({"L": [inner]}))
}

/// A filter that reads `:p`, with `value` under `:d`, which nothing reads.
fn with_unused_value(value: Value) -> Value {
    scan(json!({
        "FilterExpression": "a = :p",
        "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": value},
    }))
}

#[test]
fn an_empty_scan_filter_counts_as_present_for_the_parameter_mix() {
    let mix = with(scan(filter("1")), json!({"ScanFilter": {}}));
    assert_eq!(check(&mix).as_deref(), Some(MIX));
    // Alone, an empty ScanFilter is accepted.
    assert_eq!(check(&scan(json!({"ScanFilter": {}}))), None);
    // Beside an unused malformed number, the number text.
    let unused = scan(json!({
        "ScanFilter": {},
        "ExpressionAttributeValues": {":n": {"N": "bob"}},
    }));
    assert_eq!(check(&unused).as_deref(), Some(NUMERIC_BOB));
}

#[test]
fn an_empty_attributes_to_get_is_a_length_fault_ahead_of_the_number_check() {
    let empty = json!({"AttributesToGet": []});
    for body in [
        scan(empty.clone()),
        with(scan(filter("1")), empty.clone()),
        with(scan(filter("bob")), empty.clone()),
        // Measured 2026-10-30 12:40 UTC: ahead of the parameter mix too.
        with(scan(filter("1")), with(empty, json!({"ScanFilter": {}}))),
    ] {
        assert_eq!(check(&body).as_deref(), Some(LENGTH), "{body}");
    }
    assert_eq!(check(&scan(json!({"AttributesToGet": ["a"]}))), None);
}

/// Measured 2026-10-30 12:40 UTC: the length constraint is aggregated with the
/// range constraints, ahead of `Limit`.
#[test]
fn an_empty_attributes_to_get_joins_the_range_faults() {
    let body = scan(json!({"AttributesToGet": [], "Limit": 0}));
    assert_eq!(
        check(&body).as_deref(),
        Some(
            "2 validation errors detected: Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1; Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
        )
    );
}

#[test]
fn a_malformed_number_in_the_exclusive_start_key_is_reported_with_the_number_text() {
    let key = |number: &str| json!({"ExclusiveStartKey": {"pk": {"N": number}, "sk": {"S": "x"}}});
    let bad = with(scan(filter("1")), key("bob"));
    assert_eq!(check(&bad).as_deref(), Some(NUMERIC_BOB));
    assert_eq!(check(&with(scan(filter("1")), key("1"))), None);
    // Measured 2026-10-30 12:40 UTC: with no filter, the same text; the
    // ReturnConsumedCapacity enum is checked first.
    assert_eq!(check(&scan(key("bob"))).as_deref(), Some(NUMERIC_BOB));
    let enum_first = with(scan(key("bob")), json!({"ReturnConsumedCapacity": "BAD"}));
    assert_eq!(
        check(&enum_first).as_deref(),
        Some(
            "1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
        )
    );
}

#[test]
fn a_value_nested_32_levels_deep_is_refused_and_31_levels_pass() {
    let one = json!({"N": "1"});
    for levels in [32, 33, 40, 60] {
        let body = with_unused_value(nested(&one, levels));
        assert_eq!(check(&body).as_deref(), Some(NESTING), "{levels} levels");
    }
    // 31 levels reach the checks the handler makes after deserializing.
    assert_eq!(check(&with_unused_value(nested(&one, 31))), None);
    // Measured 2026-10-30 12:40 UTC: ahead of the Select enum.
    let select = with(
        with_unused_value(nested(&one, 32)),
        json!({"Select": "BAD"}),
    );
    assert_eq!(check(&select).as_deref(), Some(NESTING));
}

#[test]
fn the_number_check_runs_before_the_nesting_check() {
    let bob = json!({"N": "bob"});
    for levels in [31, 32, 33, 40, 60] {
        let body = with_unused_value(nested(&bob, levels));
        assert_eq!(
            check(&body).as_deref(),
            Some(NUMERIC_BOB),
            "{levels} levels"
        );
    }
    // An M at the top, 31 lists below it.
    let body = with_unused_value(json!({"M": {"k": nested(&bob, 31)}}));
    assert_eq!(check(&body).as_deref(), Some(NUMERIC_BOB));
}

/// Requests the service was not sent. The answers follow from the measured
/// rows and from the order the checks are made in.
#[test]
fn combinations_the_service_was_not_sent_follow_the_order_of_the_checks() {
    // The mix names whichever expression parameter is present.
    let projection = scan(json!({"ScanFilter": {}, "ProjectionExpression": "a"}));
    assert_eq!(
        check(&projection).as_deref(),
        Some(
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {ProjectionExpression}"
        )
    );
    // A null ScanFilter is absent.
    let null_filter = with(scan(filter("1")), json!({"ScanFilter": null}));
    assert_eq!(check(&null_filter), None);
    // The length constraint applies beside a ProjectionExpression too.
    let beside_projection = scan(json!({"AttributesToGet": [], "ProjectionExpression": "a"}));
    assert_eq!(check(&beside_projection).as_deref(), Some(LENGTH));
    // The length clause keeps its place ahead of the range clauses beside a
    // Segment fault, as measured beside a Limit fault.
    let segment = scan(json!({"AttributesToGet": [], "Segment": -1, "TotalSegments": 2}));
    assert_eq!(
        check(&segment).as_deref(),
        Some(
            "2 validation errors detected: Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1; Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0"
        )
    );
    // In the older form the clauses are reversed and the values echoed, so
    // the empty list comes last.
    let older = scan(json!({"AttributesToGet": [], "Limit": 0.0}));
    assert_eq!(
        check(&older).as_deref(),
        Some(
            "2 validation errors detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1; Value '[]' at 'attributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1"
        )
    );
    // The values are checked ahead of the key.
    let both = with(
        scan(filter("bob")),
        json!({"ExclusiveStartKey": {"pk": {"N": "fred"}}}),
    );
    assert_eq!(check(&both).as_deref(), Some(NUMERIC_BOB));
    // A map nests as a list does.
    let map =
        |levels: usize| (0..levels).fold(json!({"N": "1"}), |inner, _| json!({"M": {"k": inner}}));
    assert_eq!(check(&with_unused_value(map(32))).as_deref(), Some(NESTING));
    assert_eq!(check(&with_unused_value(map(31))), None);
}
