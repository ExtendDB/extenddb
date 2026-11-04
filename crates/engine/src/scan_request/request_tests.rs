// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `ScanFilter` in the parameter mix, the `AttributesToGet` length, the
//! numbers in `ExclusiveStartKey` and the nesting depth of
//! `ExpressionAttributeValues`.
//!
//! Every row is a request sent to the service (us-east-1, each request twice,
//! a Scan of an existing table) with the answer it gave: on 2026-10-25 at
//! 10:38 and 10:55 UTC, on 2026-10-30 at 12:40 UTC, and on 2026-10-31 between
//! 23:54 and 23:56 UTC where a test says so.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::{StartKeyBody, validate_scan_request_body};

const MIX: &str = "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}";
const LENGTH: &str = "1 validation error detected: Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1";
const NUMERIC_BOB: &str = "The parameter cannot be converted to a numeric value: bob";
const NESTING: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit";
const EMPTY_KEY_VALUE: &str = "The provided starting key is invalid: Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";

fn check(body: &Value) -> Option<String> {
    match validate_scan_request_body(body) {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// The key's own checks on `body`, as the handler makes them after the
/// request-time checks: `StartKeyBody::take` on a copy of the body, then
/// `validate`. Panics on a `SerializationException`, which no row here
/// expects.
fn check_key(body: &Value) -> Option<String> {
    let mut body = body.clone();
    match StartKeyBody::take(&mut body).validate() {
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

/// A Scan body with `key` as its `ExclusiveStartKey`.
fn with_key(key: Value) -> Value {
    scan(json!({"ExclusiveStartKey": key}))
}

#[test]
fn a_malformed_number_in_the_exclusive_start_key_is_reported_with_the_number_text() {
    let key = |number: &str| json!({"pk": {"N": number}, "sk": {"S": "x"}});
    let bad = with(with_key(key("bob")), filter("1"));
    // The request-time checks leave the key to its own stage.
    assert_eq!(check(&bad), None);
    assert_eq!(check_key(&bad).as_deref(), Some(NUMERIC_BOB));
    assert_eq!(check_key(&with(with_key(key("1")), filter("1"))), None);
    // Measured 2026-10-30 12:40 UTC: with no filter, the same text.
    assert_eq!(
        check_key(&with_key(key("bob"))).as_deref(),
        Some(NUMERIC_BOB)
    );
    // Measured 2026-10-31 between 23:54 and 23:56 UTC: a number as an NS
    // member, as an L member, and in the L of an attribute that is not a key
    // attribute; the number texts for a leading space, an empty text and 39
    // digits (on an N hash key).
    for key in [
        json!({"pk": {"NS": ["bob"]}, "sk": {"S": "x"}}),
        json!({"pk": {"L": [{"N": "bob"}]}, "sk": {"S": "x"}}),
        json!({"pk": {"S": "p0"}, "sk": {"S": "x"}, "z": {"L": [{"N": "bob"}]}}),
    ] {
        assert_eq!(
            check_key(&with_key(key.clone())).as_deref(),
            Some(NUMERIC_BOB),
            "{key}"
        );
    }
    for (number, text) in [
        (
            " 1",
            "The parameter cannot be converted to a numeric value:  1",
        ),
        ("", "The parameter cannot be converted to a numeric value: "),
        (
            "111111111111111111111111111111111111111",
            "Attempting to store more than 38 significant digits in a Number",
        ),
    ] {
        let key = with_key(json!({"pk": {"N": number}}));
        assert_eq!(check_key(&key).as_deref(), Some(text), "{number:?}");
    }
}

/// Measured 2026-10-31 between 23:54 and 23:56 UTC with `{"pk": {"N": "bob"},
/// "sk": {"S": "x"}}` as the key: each request-time check answers its own text
/// ahead of the key's number (the `Select` rule for `SPECIFIC_ATTRIBUTES`, the
/// filter syntax and the unused-value check, which the handler makes after
/// deserializing, do too; the wire tests send those rows). Measured
/// 2026-10-30 12:40 UTC: the `ReturnConsumedCapacity` enum ahead of the key.
#[test]
fn every_request_time_check_comes_ahead_of_the_number_in_the_exclusive_start_key() {
    let bob = json!({"pk": {"N": "bob"}, "sk": {"S": "x"}});
    for (extra, text) in [
        (
            json!({"Limit": 0}),
            "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
        ),
        (
            json!({"ReturnConsumedCapacity": "BAD"}),
            "1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
        ),
        (
            json!({"Segment": -1, "TotalSegments": 2}),
            "1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0",
        ),
        (
            json!({"Select": "BAD"}),
            "1 validation error detected: Value 'BAD' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]",
        ),
        (with(filter("1"), json!({"ScanFilter": {}})), MIX),
        (json!({"AttributesToGet": []}), LENGTH),
        (
            json!({
                "FilterExpression": "a = :p",
                "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": nested(&json!({"N": "1"}), 32)},
            }),
            NESTING,
        ),
        // The values ahead of the key, with `fred` in the key.
        (
            json!({
                "FilterExpression": "a = :n",
                "ExpressionAttributeValues": {":n": {"N": "bob"}},
                "ExclusiveStartKey": {"pk": {"N": "fred"}, "sk": {"S": "x"}},
            }),
            NUMERIC_BOB,
        ),
    ] {
        let body = with(with_key(bob.clone()), extra);
        assert_eq!(check(&body).as_deref(), Some(text), "{body}");
    }
}

/// Measured 2026-10-31 between 23:54 and 23:56 UTC: inside the key a number
/// fault comes ahead of a value that is empty or has an unknown type
/// descriptor, in either member order; with two number faults the first
/// member is named; a key whose only fault is an empty value answers the
/// service's key text.
#[test]
fn a_number_fault_in_the_key_comes_ahead_of_an_empty_or_unknown_descriptor_value() {
    for key in [
        json!({"pk": {}, "sk": {"N": "bob"}}),
        json!({"sk": {"N": "bob"}, "pk": {}}),
        json!({"pk": {"X": "1"}, "sk": {"N": "bob"}}),
        json!({"pk": {"N": "bob"}, "sk": {"N": "fred"}}),
    ] {
        let body = with_key(key.clone());
        // The request-time checks leave the key to its own stage.
        assert_eq!(check(&body), None, "{key}");
        assert_eq!(check_key(&body).as_deref(), Some(NUMERIC_BOB), "{key}");
    }
    let empty = with_key(json!({"pk": {}, "sk": {"S": "x"}}));
    assert_eq!(check_key(&empty).as_deref(), Some(EMPTY_KEY_VALUE));
    // Measured 2026-10-31 between 23:54 and 23:56 UTC too: a Limit fault
    // beside the empty value answers the Limit clause, in the older form.
    assert_eq!(
        check(&with(empty, json!({"Limit": 0}))).as_deref(),
        Some(
            "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
        )
    );
}

/// `StartKeyBody::take` holds back a key the deserializer refuses for its
/// value and leaves one of the wrong JSON shape in the body, where the typed
/// deserialization reports it ahead of every check.
#[test]
fn a_key_of_the_wrong_shape_stays_in_the_body_and_an_unreadable_value_is_held_back() {
    for key in [json!("x"), json!({"pk": "x"}), json!({"pk": {"N": 5}})] {
        let mut body = with_key(key.clone());
        let start_key = StartKeyBody::take(&mut body);
        assert!(body.get("ExclusiveStartKey").is_some(), "{key}");
        assert!(start_key.validate().is_ok(), "{key}");
        let shape = serde_json::from_value::<extenddb_core::types::ScanInput>(body)
            .map_err(crate::deserialize_error);
        assert!(
            matches!(shape, Err(DynamoDbError::SerializationException(_))),
            "{key}: {shape:?}"
        );
    }
    for key in [
        json!({"pk": {}}),
        json!({"pk": {"X": "1"}}),
        json!({"pk": {"S": "x", "N": "1"}}),
    ] {
        let mut body = with_key(key.clone());
        let start_key = StartKeyBody::take(&mut body);
        assert!(body.get("ExclusiveStartKey").is_none(), "{key}");
        assert!(start_key.validate().is_err(), "{key}");
        assert!(
            serde_json::from_value::<extenddb_core::types::ScanInput>(body).is_ok(),
            "{key}"
        );
    }
    // A readable key stays in the body and passes.
    let mut body = with_key(json!({"pk": {"S": "p0"}, "sk": {"S": "x"}}));
    let start_key = StartKeyBody::take(&mut body);
    assert!(body.get("ExclusiveStartKey").is_some());
    assert!(start_key.validate().is_ok());
    // No key at all.
    let mut body = scan(json!({}));
    assert!(StartKeyBody::take(&mut body).validate().is_ok());
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

/// Combinations the other tests do not send, each measured 2026-10-31 between
/// 23:54 and 23:56 UTC with the answer asserted here (the key row with `{"pk":
/// {"N": "fred"}}` and with `"sk": {"S": "x"}` added to it).
#[test]
fn combinations_measured_on_2026_10_31_follow_the_order_of_the_checks() {
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
