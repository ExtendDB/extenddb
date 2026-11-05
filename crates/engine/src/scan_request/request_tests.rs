// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `ScanFilter` in the parameter mix, the `AttributesToGet` length, the
//! numbers in `ExclusiveStartKey` and the nesting depth of
//! `ExpressionAttributeValues`.
//!
//! Every row is a request sent to the service (us-east-1, each request twice,
//! a Scan of a table the probe created) with the answer it gave: on 2026-10-25
//! at 10:38 and 10:55 UTC, on 2026-10-30 at 12:40 UTC, on 2026-10-31 between
//! 23:54 and 23:56 UTC and on 2026-11-05 where a test says so. The tests of
//! `StartKeyBody::take` and `ExpressionValuesBody::take` check which values
//! they hold back, with no service answer, and the test with faults in two key
//! members accepts either member's fault.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::{StartKeyBody, validate_scan_request_body};

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

/// A Scan body with `key` as its `ExclusiveStartKey`.
fn with_key(key: Value) -> Value {
    scan(json!({"ExclusiveStartKey": key}))
}

/// The key's shape stage on `body`, as the handler makes it after the request
/// parameter checks: `StartKeyBody::take` on a copy of the body, then
/// `validate_shape`.
fn check_key_shape(body: &Value) -> Option<String> {
    let mut body = body.clone();
    match StartKeyBody::take(&mut body).validate_shape() {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// The key's value stage on `body`, as the handler makes it after the
/// expression checks: `StartKeyBody::take` on a copy of the body, then
/// `validate_values`, for a key whose shape stage passes.
fn check_key_values(body: &Value) -> Option<String> {
    let mut body = body.clone();
    let key = StartKeyBody::take(&mut body);
    assert!(key.validate_shape().is_ok(), "{body}");
    match key.validate_values() {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// The message of a `ValidationException`, `None` for `Ok`.
fn validation_message(result: Result<(), DynamoDbError>) -> Option<String> {
    match result {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// Both stages in the handler's order: the first fault of either.
fn check_key(body: &Value) -> Option<String> {
    check_key_shape(body).or_else(|| check_key_values(body))
}

const KEY_PREFIX: &str = "The provided starting key is invalid: ";
const EMPTY_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";
const INVALID: &str = "One or more parameter values were invalid: ";

#[test]
fn a_malformed_number_in_the_exclusive_start_key_is_reported_with_the_number_text() {
    let key = |number: &str| json!({"pk": {"N": number}, "sk": {"S": "x"}});
    let bad = with(with_key(key("bob")), filter("1"));
    // The request-time checks and the shape stage leave the number to the
    // value stage.
    assert_eq!(check(&bad), None);
    assert_eq!(check_key_shape(&bad), None);
    assert_eq!(check_key_values(&bad).as_deref(), Some(NUMERIC_BOB));
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

/// Measured 2026-10-31 between 23:54 and 23:56 UTC and 2026-11-05 with `{"pk":
/// {"N": "bob"}, "sk": {"S": "x"}}` as the key: each request-time check answers
/// its own text ahead of the key's number, in the newer form; beside a key
/// value that is empty or carries an unknown type descriptor the constraint
/// clauses take the older form (2026-11-05). The request parameter checks, the
/// expression checks and the Segment pairing, which the handler makes after
/// deserializing, come ahead of the key's number too; the wire tests send
/// those rows.
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
    // Beside a key value the shape stage refuses, the older form (2026-11-05).
    for key in [
        json!({"pk": {}, "sk": {"S": "x"}}),
        json!({"pk": {"X": "1"}, "sk": {"S": "x"}}),
    ] {
        assert_eq!(
            check(&with(with_key(key.clone()), json!({"Limit": 0}))).as_deref(),
            Some(
                "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
            ),
            "{key}"
        );
    }
}

/// Measured 2026-11-05: a key value that is empty, carries an unknown or
/// lower-case type descriptor, or carries two, at its top or inside an `L` or
/// `M`, in a key member or in a member that is not a key attribute, answers the
/// service's text with the key prefix at the shape stage.
#[test]
fn a_shape_fault_in_a_key_value_answers_the_prefixed_text_at_the_shape_stage() {
    let empty = format!("{KEY_PREFIX}{EMPTY_VALUE}");
    for key in [
        json!({"pk": {}, "sk": {"S": "x"}}),
        json!({"pk": {"X": "1"}, "sk": {"S": "x"}}),
        json!({"pk": {"s": "x"}, "sk": {"S": "x"}}),
        json!({"pk": {"L": [{}]}, "sk": {"S": "x"}}),
        json!({"pk": {"M": {"k": {}}}, "sk": {"S": "x"}}),
        json!({"pk": {"S": "p0"}, "sk": {"S": "x"}, "z": {}}),
        json!({"pk": {}}),
    ] {
        let body = with_key(key.clone());
        assert_eq!(check(&body), None, "{key}");
        assert_eq!(
            check_key_shape(&body).as_deref(),
            Some(empty.as_str()),
            "{key}"
        );
    }
    let two = with_key(json!({"pk": {"S": "x", "N": "1"}, "sk": {"S": "x"}}));
    assert_eq!(
        check_key_shape(&two).as_deref(),
        Some(
            "The provided starting key is invalid: Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"
        )
    );
    // A readable key passes the shape stage.
    assert_eq!(
        check_key_shape(&with_key(json!({"pk": {"N": "bob"}, "sk": {"NS": []}}))),
        None
    );
}

/// Measured 2026-11-05: the set faults of a key answer the service's texts with
/// no prefix at the value stage, after every number in the key.
#[test]
fn a_set_fault_in_a_key_value_answers_the_service_text_without_a_prefix() {
    for (key, text) in [
        (json!({"NS": []}), "An number set  may not be empty"),
        (json!({"SS": []}), "An string set  may not be empty"),
        (json!({"BS": []}), "Binary sets should not be empty"),
        (
            json!({"NS": ["1", "1"]}),
            "Input collection contains duplicates.",
        ),
        (
            json!({"NS": ["1", "1.0"]}),
            "Input collection contains duplicates.",
        ),
        (
            json!({"SS": ["a", "a"]}),
            r#"Input collection ["a", "a"] contains duplicates."#,
        ),
    ] {
        let body = with_key(json!({"pk": key, "sk": {"S": "x"}}));
        assert_eq!(
            check_key_values(&body),
            Some(format!("{INVALID}{text}")),
            "{key}"
        );
    }
    // A number in a set is reported with the number text.
    let bob = with_key(json!({"pk": {"NS": ["bob"]}, "sk": {"S": "x"}}));
    assert_eq!(check_key_values(&bob).as_deref(), Some(NUMERIC_BOB));
    // A set with distinct numbers passes.
    assert_eq!(
        check_key_values(&with_key(
            json!({"pk": {"NS": ["1", "2"]}, "sk": {"S": "x"}})
        )),
        None
    );
}

/// Faults in two members of one key. The service's choice follows the attribute
/// names and, on some tables, the request's member order (measured 2026-11-05
/// on three tables): `{"pk": {}, "sk": {"N": "bob"}}` answered `bob` on
/// 2026-10-31 and `{"a": {}, "b": {"N": "bob"}}` the empty value. The code
/// reports a shape fault in any member ahead of a value fault in any member,
/// then the members in sorted order; this test accepts either member's fault.
#[test]
fn two_faulty_members_answer_one_of_the_two_faults() {
    let empty = format!("{KEY_PREFIX}{EMPTY_VALUE}");
    let ns_empty = format!("{INVALID}An number set  may not be empty");
    let fred = NUMERIC_BOB.replace("bob", "fred");
    for (key, either) in [
        (
            json!({"pk": {}, "sk": {"N": "bob"}}),
            [empty.as_str(), NUMERIC_BOB],
        ),
        (
            json!({"sk": {"N": "bob"}, "pk": {}}),
            [empty.as_str(), NUMERIC_BOB],
        ),
        (
            json!({"pk": {"X": "1"}, "sk": {"N": "bob"}}),
            [empty.as_str(), NUMERIC_BOB],
        ),
        (
            json!({"pk": {"N": "bob"}, "sk": {}}),
            [empty.as_str(), NUMERIC_BOB],
        ),
        (
            json!({"pk": {"N": "bob"}, "sk": {"N": "fred"}}),
            [NUMERIC_BOB, fred.as_str()],
        ),
        (
            json!({"pk": {"NS": []}, "sk": {"N": "bob"}}),
            [ns_empty.as_str(), NUMERIC_BOB],
        ),
        (
            json!({"pk": {}, "sk": {"NS": []}}),
            [empty.as_str(), ns_empty.as_str()],
        ),
    ] {
        let answer = check_key(&with_key(key.clone()));
        assert!(
            either.contains(&answer.as_deref().unwrap_or_default()),
            "{key}: {answer:?}"
        );
    }
}

/// `StartKeyBody::take` holds back a key the deserializer refuses for its
/// value and leaves one of the wrong JSON shape in the body, where the typed
/// deserialization reports it ahead of every check; no service answer is
/// asserted here.
#[test]
fn a_key_of_the_wrong_shape_stays_in_the_body_and_an_unreadable_value_is_held_back() {
    for key in [json!("x"), json!({"pk": "x"}), json!({"pk": {"N": 5}})] {
        let mut body = with_key(key.clone());
        let start_key = StartKeyBody::take(&mut body);
        assert!(body.get("ExclusiveStartKey").is_some(), "{key}");
        assert!(start_key.validate_shape().is_ok(), "{key}");
        assert!(start_key.validate_values().is_ok(), "{key}");
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
        json!({"pk": {"NS": []}}),
        json!({"pk": {"SS": ["a", "a"]}}),
    ] {
        let mut body = with_key(key.clone());
        let start_key = StartKeyBody::take(&mut body);
        assert!(body.get("ExclusiveStartKey").is_none(), "{key}");
        assert!(
            start_key
                .validate_shape()
                .and(start_key.validate_values())
                .is_err(),
            "{key}"
        );
        assert!(
            serde_json::from_value::<extenddb_core::types::ScanInput>(body).is_ok(),
            "{key}"
        );
    }
    // A readable key stays in the body and passes both stages.
    let mut body = with_key(json!({"pk": {"S": "p0"}, "sk": {"S": "x"}}));
    let start_key = StartKeyBody::take(&mut body);
    assert!(body.get("ExclusiveStartKey").is_some());
    assert!(start_key.validate_shape().is_ok());
    assert!(start_key.validate_values().is_ok());
    // No key at all.
    let mut body = scan(json!({}));
    let start_key = StartKeyBody::take(&mut body);
    assert!(
        start_key
            .validate_shape()
            .and(start_key.validate_values())
            .is_ok()
    );
}

/// `ExpressionValuesBody::take` puts a readable stand-in in the body for a
/// value the deserializer refuses for its content and keeps the deserializer's
/// fault for the map as written (measured 2026-11-05: the fault comes after the
/// key's value stage); a value of the wrong JSON shape stays in the body.
#[test]
fn an_unreadable_expression_value_is_held_back_and_a_stand_in_takes_its_place() {
    let mut body = scan(json!({
        "FilterExpression": "a = :a",
        "ExpressionAttributeValues": {":a": {}},
    }));
    let values = super::ExpressionValuesBody::take(&mut body);
    assert_eq!(body["ExpressionAttributeValues"], json!({":a": {"S": ""}}));
    assert_eq!(
        validation_message(values.validate()).as_deref(),
        Some(
            "ExpressionAttributeValues contains invalid value: Supplied AttributeValue is empty, must contain exactly one of the supported datatypes for key :a"
        )
    );
    let mut body = scan(json!({"ExpressionAttributeValues": {":a": {"X": "1"}}}));
    let values = super::ExpressionValuesBody::take(&mut body);
    assert_eq!(body["ExpressionAttributeValues"], json!({":a": {"S": ""}}));
    assert!(
        matches!(values.validate(), Err(DynamoDbError::SerializationException(msg)) if msg.contains("unknown AttributeValue type descriptor: X"))
    );
    // A readable value and a value of the wrong JSON shape stay as written.
    for value in [json!({"S": "x"}), json!({"N": 5})] {
        let mut body = scan(json!({"ExpressionAttributeValues": {":a": value}}));
        let values = super::ExpressionValuesBody::take(&mut body);
        assert_eq!(body["ExpressionAttributeValues"], json!({":a": value}));
        assert!(values.validate().is_ok());
    }
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
