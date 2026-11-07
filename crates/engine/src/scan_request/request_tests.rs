// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `ScanFilter` in the parameter mix, the `AttributesToGet` length, the
//! nesting depth of `ExpressionAttributeValues`, the two check orders, and the
//! `ExclusiveStartKey` and `ExpressionAttributeValues` checks of each order.
//!
//! Every row is a request sent to the service (us-east-1, each request twice,
//! a Scan of a table the probe created) with the answer it gave: on 2026-10-25
//! at 10:38 and 10:55 UTC, on 2026-10-30 at 12:40 UTC, on 2026-10-31 between
//! 23:54 and 23:56 UTC and on 2026-11-05 where a test says so. The tests of
//! `StartKeyBody::take` and `SelectBody::take` check which values they hold
//! back, with no service answer; the tests of `ExpressionValuesBody::take` and
//! `ScanFilterBody::take` also check the measured texts they keep; the tests of
//! the member order check the fitted rule's buckets and masks; the rows with
//! two value faults in two key members accept either member's fault.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::{
    CheckOrder, ExpressionValuesBody, ScanFilterBody, SelectBody, StartKeyBody,
    fitted_member_order, hash_table_mask, java_hash_bucket, member_order,
    validate_scan_request_body,
};

const MIX: &str = "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}";
const LENGTH: &str = "1 validation error detected: Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1";
const NUMERIC_BOB: &str = "The parameter cannot be converted to a numeric value: bob";
const NESTING: &str = "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit";
const SELECT_BAD: &str = "1 validation error detected: Value 'BAD' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]";
const KEY_PREFIX: &str = "The provided starting key is invalid: ";
const EMPTY_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";
const MORE_THAN_ONE: &str = "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes";
const INVALID: &str = "One or more parameter values were invalid: ";

/// The message of a `ValidationException`, `None` for `Ok`.
fn validation_message(result: Result<(), DynamoDbError>) -> Option<String> {
    match result {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

/// The request-time checks on `body`, in the order of the body's class.
fn check(body: &Value) -> Option<String> {
    validation_message(validate_scan_request_body(body, CheckOrder::of(body), None))
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

/// A Scan body with `key` as its `ExclusiveStartKey`.
fn with_key(key: Value) -> Value {
    scan(json!({"ExclusiveStartKey": key}))
}

/// The key's check on `body` in the order of the body's class, as the handler
/// makes it: `StartKeyBody::take` on a copy of the body with the members in
/// `request_order` (the `json!` macro sorts them), then `validate`.
fn check_key_in(body: &Value, request_order: &[&str]) -> Option<String> {
    let mut body = body.clone();
    let order = CheckOrder::of(&body);
    let names: Vec<String> = request_order
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let request_order = if names.is_empty() {
        None
    } else {
        Some(names.as_slice())
    };
    validation_message(StartKeyBody::take(&mut body, request_order).validate(order))
}

/// [`check_key_in`] with the parsed body's member order.
fn check_key(body: &Value) -> Option<String> {
    check_key_in(body, &[])
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

/// The order flag reads `ExpressionAttributeValues`, `ExclusiveStartKey` and
/// the `ScanFilter` lists, at any depth of `L` and `M`; a set fault, a number
/// fault and a readable value of the wrong type for the key leave the request
/// in order R.
#[test]
fn the_order_follows_an_unreadable_value_anywhere() {
    for body in [
        with_key(json!({"pk": {}, "sk": {"S": "x"}})),
        with_key(json!({"pk": {"X": "1"}, "sk": {"S": "x"}})),
        with_key(json!({"pk": {"s": "x"}, "sk": {"S": "x"}})),
        with_key(json!({"pk": {"S": "x", "N": "1"}, "sk": {"S": "x"}})),
        with_key(json!({"pk": {"L": [{"M": {"k": {}}}]}, "sk": {"S": "x"}})),
        scan(json!({"ExpressionAttributeValues": {":e": {}}})),
        scan(
            json!({"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": {"X": "1"}}}),
        ),
        scan(
            json!({"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{}]}}}),
        ),
    ] {
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable, "{body}");
    }
    for body in [
        scan(json!({})),
        with_key(json!({"pk": {"N": "bob"}, "sk": {"NS": []}})),
        with_key(json!({"pk": {"SS": ["a", "a"]}, "sk": {"S": "x"}})),
        scan(json!({"ExpressionAttributeValues": {":e": {"NS": []}}})),
        scan(json!({"ExpressionAttributeValues": {":n": {"N": "bob"}}})),
        with_key(json!({"pk": {"N": "1"}, "sk": {"S": "x"}})),
    ] {
        assert_eq!(CheckOrder::of(&body), CheckOrder::Readable, "{body}");
    }
}

#[test]
fn a_malformed_number_in_the_exclusive_start_key_is_reported_with_the_number_text() {
    let key = |number: &str| json!({"pk": {"N": number}, "sk": {"S": "x"}});
    let bad = with(with_key(key("bob")), filter("1"));
    // The request-time checks leave the number to the key's stage.
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
    // digits (on an N hash key). Measured 2026-11-05: `{"pk": {"N": "bob"}}`
    // alone, with no `sk`, on the `pk`, `sk` table.
    for key in [
        json!({"pk": {"NS": ["bob"]}, "sk": {"S": "x"}}),
        json!({"pk": {"L": [{"N": "bob"}]}, "sk": {"S": "x"}}),
        json!({"pk": {"S": "p0"}, "sk": {"S": "x"}, "z": {"L": [{"N": "bob"}]}}),
        json!({"pk": {"N": "bob"}}),
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
/// {"N": "bob"}, "sk": {"S": "x"}}` as the key: each request-time check of
/// order R answers its own text ahead of the key's number, in the newer form;
/// beside a key value that is empty or carries an unknown type descriptor the
/// constraint clauses take the older form and the `Select` enum joins the
/// request-time checks (2026-11-05). The request parameter checks, the
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
    // In order R the Select enum is not a request-time check: SelectBody
    // holds it back for the handler (measured 2026-11-05: after the
    // no-expression check and the nesting check).
    let select = with(with_key(bob), json!({"Select": "BAD"}));
    assert_eq!(check(&select), None);
    assert_eq!(check_select(&select).as_deref(), Some(SELECT_BAD));
    // Beside a key value the shape stage refuses, the older form, and the
    // Select enum at request time (2026-11-05).
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
        assert_eq!(
            check(&with(with_key(key.clone()), json!({"Select": "BAD"}))).as_deref(),
            Some(SELECT_BAD),
            "{key}"
        );
    }
    // An unused empty expression value beside Select BAD: the Select clause
    // (2026-11-05, order U).
    let unused_empty = with(with_unused_value(json!({})), json!({"Select": "BAD"}));
    assert_eq!(check(&unused_empty).as_deref(), Some(SELECT_BAD));
}

/// `SelectBody::take` on a copy of `body`, then `validate`.
fn check_select(body: &Value) -> Option<String> {
    let mut body = body.clone();
    let order = CheckOrder::of(&body);
    validation_message(SelectBody::take(&mut body, order).validate())
}

/// `SelectBody::take` holds an invalid `Select` back in order R, so that the
/// typed deserialization does not report it, and leaves it in order U, where
/// the request-time checks report it; no service answer is asserted here.
#[test]
fn an_invalid_select_is_held_back_in_order_r_only() {
    let mut body = scan(json!({"Select": "BAD"}));
    let select = SelectBody::take(&mut body, CheckOrder::Readable);
    assert!(body.get("Select").is_none());
    assert_eq!(
        validation_message(select.validate()).as_deref(),
        Some(SELECT_BAD)
    );
    assert!(serde_json::from_value::<extenddb_core::types::ScanInput>(body).is_ok());
    let mut body = scan(json!({"Select": "BAD", "ExclusiveStartKey": {"pk": {}}}));
    let select = SelectBody::take(&mut body, CheckOrder::Unreadable);
    assert!(body.get("Select").is_some());
    assert!(select.validate().is_ok());
    // A valid Select and a Select of the wrong JSON type stay as written.
    for value in [json!("COUNT"), json!(5)] {
        let mut body = scan(json!({"Select": value}));
        let select = SelectBody::take(&mut body, CheckOrder::Readable);
        assert_eq!(body["Select"], value);
        assert!(select.validate().is_ok());
    }
}

/// Measured 2026-11-05: a key value that is empty, carries an unknown or
/// lower-case type descriptor, or carries two, at its top or inside an `L` or
/// `M`, in a key member or in a member that is not a key attribute, answers the
/// service's text with the key prefix at the key's stage of order U.
#[test]
fn a_shape_fault_in_a_key_value_answers_the_prefixed_text() {
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
        assert_eq!(check_key(&body).as_deref(), Some(empty.as_str()), "{key}");
    }
    let two = with_key(json!({"pk": {"S": "x", "N": "1"}, "sk": {"S": "x"}}));
    assert_eq!(
        check_key(&two),
        Some(format!("{KEY_PREFIX}{MORE_THAN_ONE}"))
    );
    // Inside one member a shape fault comes ahead of a number (2026-11-05,
    // `{"M": {"k": {"N": "bob"}, "j": {}}}` in a non-key member).
    let inside = with_key(json!({
        "pk": {"S": "x"}, "sk": {"S": "x"}, "z": {"M": {"k": {"N": "bob"}, "j": {}}},
    }));
    assert_eq!(check_key(&inside).as_deref(), Some(empty.as_str()));
}

/// Measured 2026-11-05: in order R the set faults of a key answer the service's
/// texts with no prefix, the members JSON-quoted, after every number in the
/// key.
#[test]
fn a_set_fault_in_a_key_value_answers_the_order_r_text_without_a_prefix() {
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
        (
            json!({"SS": ["a\"b", "a\"b"]}),
            r#"Input collection ["a\"b", "a\"b"] contains duplicates."#,
        ),
        (
            json!({"BS": ["YQ==", "YQ=="]}),
            r#"Input collection ["YQ==","YQ=="]of type BS contains duplicates."#,
        ),
    ] {
        let body = with_key(json!({"pk": key, "sk": {"S": "x"}}));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Readable, "{key}");
        assert_eq!(check_key(&body), Some(format!("{INVALID}{text}")), "{key}");
    }
    // A number in a set is reported with the number text, ahead of a
    // duplicate in the same set (2026-11-05, `["bob", "1", "1"]` and `["1",
    // "1", "bob"]`).
    for members in [
        json!(["bob"]),
        json!(["bob", "1", "1"]),
        json!(["1", "1", "bob"]),
    ] {
        let bob = with_key(json!({"pk": {"S": "x"}, "sk": {"S": "x"}, "z": {"NS": members}}));
        assert_eq!(check_key(&bob).as_deref(), Some(NUMERIC_BOB), "{members}");
    }
    // A set with distinct numbers passes.
    assert_eq!(
        check_key(&with_key(
            json!({"pk": {"NS": ["1", "2"]}, "sk": {"S": "x"}})
        )),
        None
    );
}

/// Measured 2026-11-05: in order U (here, the other member of the key is
/// unreadable and the set member comes first in the fitted order) the set texts
/// have no JSON quotes, the `NS` duplicate text is bare, and the empty `SS`
/// text carries the key prefix. The key's number keeps its text.
#[test]
fn a_set_fault_in_a_key_value_answers_the_order_u_text() {
    for (sk, text) in [
        (
            json!({"NS": []}),
            format!("{INVALID}An number set  may not be empty"),
        ),
        (
            json!({"SS": []}),
            format!("{KEY_PREFIX}{INVALID}An string set  may not be empty"),
        ),
        (
            json!({"NS": ["1", "1"]}),
            "Input collection contains duplicates".to_owned(),
        ),
        (
            json!({"SS": ["a", "a"]}),
            format!("{INVALID}Input collection [a, a] contains duplicates."),
        ),
        (json!({"N": "bob"}), NUMERIC_BOB.to_owned()),
    ] {
        for pk in [json!({}), json!({"X": "1"}), json!({"S": "x", "N": "1"})] {
            let body = with_key(json!({"pk": pk, "sk": sk}));
            assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable);
            assert_eq!(check_key(&body).as_deref(), Some(text.as_str()), "{body}");
        }
    }
    // Measured 2026-11-05 beside an unreadable key member: an empty BS with
    // the key prefix, as the empty SS; a BS with duplicates with the members
    // unquoted and no prefix.
    let bs_empty = with_key(json!({"pk": {}, "sk": {"BS": []}}));
    assert_eq!(
        check_key(&bs_empty),
        Some(format!(
            "{KEY_PREFIX}{INVALID}Binary sets should not be empty"
        ))
    );
    let bs_dup = with_key(json!({"pk": {}, "sk": {"BS": ["YQ==", "YQ=="]}}));
    assert_eq!(
        check_key(&bs_dup),
        Some(format!(
            "{INVALID}Input collection [YQ==, YQ==]of type BS contains duplicates."
        ))
    );
}

/// The fitted member order: `String.hashCode()` of the name, spread and masked
/// to four buckets, buckets ascending, the request's order inside a bucket
/// (measured 2026-10-31, 2026-11-04 and 2026-11-05 on five tables).
#[test]
fn the_fitted_member_order_follows_the_hash_buckets_then_the_request() {
    for (name, bucket) in [
        ("sk", 0),
        ("a", 1),
        ("b", 2),
        ("pk", 3),
        ("gpk", 3),
        ("id", 3),
        ("ts", 3),
        ("\u{e4}", 0),
        ("\u{e9}", 1),
        (":n", 0),
        (":b", 0),
        (":p", 2),
        (":a", 3),
        (":e", 3),
    ] {
        assert_eq!(java_hash_bucket(name, 3), bucket, "{name}");
    }
    // The table doubles when it holds more than three quarters of its size.
    for (names, mask) in [
        (0, 3),
        (1, 3),
        (3, 3),
        (4, 7),
        (6, 7),
        (7, 15),
        (12, 15),
        (13, 31),
    ] {
        assert_eq!(hash_table_mask(names), mask, "{names} names");
    }
    for (name, bucket) in [("sk", 0), ("gpk", 3), ("pk", 3), ("gsk", 6)] {
        assert_eq!(java_hash_bucket(name, 7), bucket, "{name}");
    }
    let order = |names: &[&str]| fitted_member_order(names.iter().copied());
    // The pk, sk table: sk first in both request orders.
    assert_eq!(order(&["pk", "sk"]), ["sk", "pk"]);
    assert_eq!(order(&["sk", "pk"]), ["sk", "pk"]);
    // HASH a, RANGE b: a first; HASH pk, RANGE a: a first.
    assert_eq!(order(&["b", "a"]), ["a", "b"]);
    assert_eq!(order(&["pk", "a"]), ["a", "pk"]);
    assert_eq!(order(&["a", "pk"]), ["a", "pk"]);
    // ts and id share a bucket: the request's order.
    assert_eq!(order(&["ts", "id"]), ["ts", "id"]);
    assert_eq!(order(&["id", "ts"]), ["id", "ts"]);
    // The GSI key of three members: sk, then gpk and pk in the request's order.
    assert_eq!(order(&["gpk", "pk", "sk"]), ["sk", "gpk", "pk"]);
    assert_eq!(order(&["pk", "sk", "gpk"]), ["sk", "pk", "gpk"]);
    // Four members (measured 2026-11-05 on a GSI with a range key): eight
    // buckets, so gpk and pk (3) come ahead of gsk (6) in both request orders;
    // with four buckets gsk (2) would come ahead of them.
    assert_eq!(
        order(&["gpk", "gsk", "pk", "sk"]),
        ["sk", "gpk", "pk", "gsk"]
    );
    assert_eq!(
        order(&["sk", "pk", "gsk", "gpk"]),
        ["sk", "pk", "gpk", "gsk"]
    );
    // HASH \u{e4}, RANGE \u{e9}: \u{e4} first in both request orders.
    assert_eq!(order(&["\u{e4}", "\u{e9}"]), ["\u{e4}", "\u{e9}"]);
    assert_eq!(order(&["\u{e9}", "\u{e4}"]), ["\u{e4}", "\u{e9}"]);
    // ExpressionAttributeValues entries (order U, 2026-11-05): :n ahead of :e,
    // :b ahead of :p and :a, :a and :e in the request's order.
    assert_eq!(order(&[":p", ":a", ":e"]), [":p", ":a", ":e"]);
    assert_eq!(order(&[":p", ":n", ":e"]), [":n", ":p", ":e"]);
    assert_eq!(order(&[":p", ":b", ":a"]), [":b", ":p", ":a"]);
    // Seven names: sixteen buckets.
    let seven = ["a", "b", "c", "d", "e", "f", "g"];
    assert_eq!(order(&seven).len(), 7);
    assert_eq!(hash_table_mask(seven.len()), 15);
}

/// `member_order` reads a top-level object's member names from the request
/// text in document order; a repeated name keeps its first position; a body
/// without the field, a field that is not an object or text that does not
/// parse gives `None`.
#[test]
fn the_request_order_of_the_members_is_read_from_the_raw_body() {
    let raw = br#"{"TableName": "t", "ExclusiveStartKey": {"ts": {"N": "bob"}, "id": {}}, "ExpressionAttributeValues": {":p": {"S": "x"}, ":n": {"N": "bob"}}, "ScanFilter": {"b": {}, "a": {}}}"#;
    let names = |names: &[&str]| Some(names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>());
    assert_eq!(member_order(raw, "ExclusiveStartKey"), names(&["ts", "id"]));
    assert_eq!(
        member_order(raw, "ExpressionAttributeValues"),
        names(&[":p", ":n"])
    );
    assert_eq!(member_order(raw, "ScanFilter"), names(&["b", "a"]));
    let repeated =
        br#"{"ExclusiveStartKey": {"b": {}, "a": {}, "b": {"S": "x"}}, "TableName": "t"}"#;
    assert_eq!(
        member_order(repeated, "ExclusiveStartKey"),
        names(&["b", "a"])
    );
    assert_eq!(member_order(raw, "ProjectionExpression"), None);
    assert_eq!(
        member_order(br#"{"TableName": "t"}"#, "ExclusiveStartKey"),
        None
    );
    assert_eq!(
        member_order(
            br#"{"TableName": "t", "ExclusiveStartKey": "x"}"#,
            "ExclusiveStartKey"
        ),
        None
    );
    assert_eq!(member_order(b"not json", "ExclusiveStartKey"), None);
    assert_eq!(member_order(b"", "ExclusiveStartKey"), None);
}

/// Faults in two members of one key: the first faulty member in the fitted
/// order is reported. Measured rows: `{"pk": {}, "sk": {"N": "bob"}}` and the
/// reversed request order answer `bob` (2026-10-31); `{"pk": {"X": "1"}, "sk":
/// {"N": "bob"}}` answers `bob`; `{"pk": {}, "sk": {"NS": []}}` answers the
/// empty NS text and `{"pk": {"S": "x", "N": "1"}, "sk": {"SS": []}}` the
/// prefixed SS text (2026-11-05); on HASH `a`, RANGE `b` `{"a": {}, "b": {"N":
/// "bob"}}` answers the empty value and `{"a": {"N": "bob"}, "b": {}}` `bob`;
/// on HASH `pk`, RANGE `a` `{"pk": {}, "a": {"N": "bob"}}` answers `bob` in
/// both request orders; on HASH `id`, RANGE `ts` the member first in the
/// request; on the GSI `{"gpk": {}, "pk": {"N": "bob"}, "sk": {"S": "x"}}`
/// the empty value. With two value faults the service's two sends disagreed on
/// 8 of 22 rows: those rows accept either member's fault.
#[test]
fn two_faulty_members_answer_the_first_in_the_fitted_order() {
    let empty = format!("{KEY_PREFIX}{EMPTY_VALUE}");
    let ns_empty = format!("{INVALID}An number set  may not be empty");
    let ss_empty = format!("{KEY_PREFIX}{INVALID}An string set  may not be empty");
    for (key, request_order, text) in [
        (
            json!({"pk": {}, "sk": {"N": "bob"}}),
            ["pk", "sk"],
            NUMERIC_BOB,
        ),
        (
            json!({"sk": {"N": "bob"}, "pk": {}}),
            ["sk", "pk"],
            NUMERIC_BOB,
        ),
        (
            json!({"pk": {"X": "1"}, "sk": {"N": "bob"}}),
            ["pk", "sk"],
            NUMERIC_BOB,
        ),
        (
            json!({"pk": {"N": "bob"}, "sk": {}}),
            ["pk", "sk"],
            empty.as_str(),
        ),
        (
            json!({"pk": {}, "sk": {"NS": []}}),
            ["pk", "sk"],
            ns_empty.as_str(),
        ),
        (
            json!({"pk": {"S": "x", "N": "1"}, "sk": {"SS": []}}),
            ["pk", "sk"],
            ss_empty.as_str(),
        ),
        (
            json!({"a": {}, "b": {"N": "bob"}}),
            ["a", "b"],
            empty.as_str(),
        ),
        (json!({"a": {"N": "bob"}, "b": {}}), ["a", "b"], NUMERIC_BOB),
        (
            json!({"pk": {}, "a": {"N": "bob"}}),
            ["pk", "a"],
            NUMERIC_BOB,
        ),
        (
            json!({"a": {"N": "bob"}, "pk": {}}),
            ["a", "pk"],
            NUMERIC_BOB,
        ),
        (
            json!({"id": {}, "ts": {"N": "bob"}}),
            ["id", "ts"],
            empty.as_str(),
        ),
        (
            json!({"ts": {"N": "bob"}, "id": {}}),
            ["ts", "id"],
            NUMERIC_BOB,
        ),
    ] {
        let body = with_key(key.clone());
        assert_eq!(
            check_key_in(&body, &request_order).as_deref(),
            Some(text),
            "{key} in the order {request_order:?}"
        );
    }
    let gsi = with_key(json!({"gpk": {}, "pk": {"N": "bob"}, "sk": {"S": "x"}}));
    assert_eq!(
        check_key_in(&gsi, &["gpk", "pk", "sk"]).as_deref(),
        Some(empty.as_str())
    );
    // Two value faults: either member's fault.
    let fred = NUMERIC_BOB.replace("bob", "fred");
    for (key, either) in [
        (
            json!({"pk": {"N": "bob"}, "sk": {"N": "fred"}}),
            [NUMERIC_BOB, fred.as_str()],
        ),
        (
            json!({"pk": {"NS": []}, "sk": {"N": "bob"}}),
            [ns_empty.as_str(), NUMERIC_BOB],
        ),
    ] {
        let answer = check_key_in(&with_key(key.clone()), &["pk", "sk"]);
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
        let order = CheckOrder::of(&body);
        let start_key = StartKeyBody::take(&mut body, None);
        assert!(body.get("ExclusiveStartKey").is_some(), "{key}");
        assert!(start_key.validate(order).is_ok(), "{key}");
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
        let order = CheckOrder::of(&body);
        let start_key = StartKeyBody::take(&mut body, None);
        assert!(body.get("ExclusiveStartKey").is_none(), "{key}");
        assert!(start_key.validate(order).is_err(), "{key}");
        assert!(
            serde_json::from_value::<extenddb_core::types::ScanInput>(body).is_ok(),
            "{key}"
        );
    }
    // A readable key stays in the body and passes.
    let mut body = with_key(json!({"pk": {"S": "p0"}, "sk": {"S": "x"}}));
    let start_key = StartKeyBody::take(&mut body, None);
    assert!(body.get("ExclusiveStartKey").is_some());
    assert!(start_key.validate(CheckOrder::Readable).is_ok());
    // No key at all.
    let mut body = scan(json!({}));
    let start_key = StartKeyBody::take(&mut body, None);
    assert!(start_key.validate(CheckOrder::Readable).is_ok());
    // A request order that does not name the key's members is ignored.
    let mut body = with_key(json!({"pk": {}, "sk": {"N": "bob"}}));
    let names = ["a".to_owned(), "b".to_owned()];
    let start_key = StartKeyBody::take(&mut body, Some(&names));
    assert_eq!(
        validation_message(start_key.validate(CheckOrder::Unreadable)).as_deref(),
        Some(NUMERIC_BOB)
    );
}

/// `ExpressionValuesBody::take` puts a readable stand-in in the body for an
/// entry the checks classify and keeps the fault with the service's text: in
/// order U the `ExpressionAttributeValues contains invalid value: ... for key
/// :a` text for an empty value, an unknown descriptor and two descriptors
/// (2026-10-31 B4-7, 2026-11-05); in order R the set texts with the members
/// JSON-quoted (2026-11-05). An entry of the wrong JSON shape stays in the
/// body.
#[test]
fn a_faulty_expression_value_is_held_back_and_a_stand_in_takes_its_place() {
    let wrapper =
        |text: &str| format!("ExpressionAttributeValues contains invalid value: {text} for key :a");
    for (value, text) in [
        (json!({}), wrapper(EMPTY_VALUE)),
        (json!({"X": "1"}), wrapper(EMPTY_VALUE)),
        (json!({"S": "x", "N": "1"}), wrapper(MORE_THAN_ONE)),
        (json!({"L": [{}]}), wrapper(EMPTY_VALUE)),
    ] {
        let mut body = scan(json!({
            "FilterExpression": "a = :a",
            "ExpressionAttributeValues": {":a": value},
        }));
        let order = CheckOrder::of(&body);
        assert_eq!(order, CheckOrder::Unreadable, "{value}");
        let values = ExpressionValuesBody::take(&mut body, order, None);
        assert_eq!(body["ExpressionAttributeValues"], json!({":a": {"S": ""}}));
        assert_eq!(validation_message(values.validate()), Some(text), "{value}");
    }
    for (value, text) in [
        (json!({"NS": []}), "An number set  may not be empty"),
        (
            json!({"SS": ["a", "a"]}),
            r#"Input collection ["a", "a"] contains duplicates."#,
        ),
    ] {
        let mut body = scan(json!({"ExpressionAttributeValues": {":a": value}}));
        let order = CheckOrder::of(&body);
        assert_eq!(order, CheckOrder::Readable, "{value}");
        let values = ExpressionValuesBody::take(&mut body, order, None);
        assert_eq!(body["ExpressionAttributeValues"], json!({":a": {"S": ""}}));
        assert_eq!(
            validation_message(values.validate()),
            Some(format!("{INVALID}{text}")),
            "{value}"
        );
    }
    // Two faulty entries: the first in the fitted order is reported (measured
    // 2026-11-05: `:b` ahead of `:a`, which sorted order would reverse).
    let mut body =
        scan(json!({"ExpressionAttributeValues": {":p": {"S": "x"}, ":b": {}, ":a": {"X": "1"}}}));
    let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, None);
    assert_eq!(
        validation_message(values.validate()),
        Some(wrapper(EMPTY_VALUE).replace(":a", ":b"))
    );
    // A readable value and a value of the wrong JSON shape stay as written.
    for value in [json!({"S": "x"}), json!({"N": 5})] {
        let mut body = scan(json!({"ExpressionAttributeValues": {":a": value}}));
        let values = ExpressionValuesBody::take(&mut body, CheckOrder::Readable, None);
        assert_eq!(body["ExpressionAttributeValues"], json!({":a": value}));
        assert!(values.validate().is_ok());
    }
}

/// Measured 2026-11-05 (order U): a malformed number in `ExpressionAttributeValues`
/// is reported at the map's step in the invalid-value form, the entries visited
/// in the fitted order: `{":p": {"S": "x"}, ":a": {"N": "bob"}, ":e": {}}`
/// names `:a`, the same with `:n` names `:n`, and the request-time checks pass
/// over the number. In order R the number is a request-time check with the
/// plain text.
#[test]
fn a_malformed_expression_value_number_in_order_u_takes_the_invalid_value_form() {
    let wrapper = |name: &str| {
        format!("ExpressionAttributeValues contains invalid value: {NUMERIC_BOB} for key {name}")
    };
    for (name, request_order) in [(":a", [":p", ":a", ":e"]), (":n", [":p", ":n", ":e"])] {
        let mut body = scan(json!({
            "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, name: {"N": "bob"}, ":e": {}},
        }));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable);
        assert_eq!(check(&body), None, "{name}");
        let names: Vec<String> = request_order.iter().map(|n| (*n).to_owned()).collect();
        let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, Some(&names));
        assert_eq!(
            validation_message(values.validate()),
            Some(wrapper(name)),
            "{name}"
        );
        assert_eq!(body["ExpressionAttributeValues"][name], json!({"S": ""}));
    }
    // With the unreadable value in the key, the number is still held back for
    // the map's step, after the key's faults.
    let mut body = scan(json!({
        "FilterExpression": "a = :n",
        "ExpressionAttributeValues": {":n": {"N": "bob"}},
        "ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}},
    }));
    assert_eq!(check(&body), None);
    let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, None);
    assert_eq!(validation_message(values.validate()), Some(wrapper(":n")));
    // Order R: the request-time check, the plain text.
    let body = scan(json!({"ExpressionAttributeValues": {":n": {"N": "bob"}}}));
    assert_eq!(CheckOrder::of(&body), CheckOrder::Readable);
    assert_eq!(check(&body).as_deref(), Some(NUMERIC_BOB));
}

/// Measured 2026-11-05 (order R): the request-time number check visits the
/// `ExpressionAttributeValues` entries in the hash order the key and the
/// order-U step use. `{":n": {"N": "fred"}, ":a": {"N": "bob"}}` answers
/// `fred` (`:n` in bucket 0, `:a` in 3), whichever order the request wrote and
/// whatever order the parsed body keeps; sorted name order would answer `bob`.
#[test]
fn the_order_r_number_check_visits_the_entries_in_the_hash_order() {
    let numeric =
        |text: &str| format!("The parameter cannot be converted to a numeric value: {text}");
    let body = scan(json!({
        "FilterExpression": "a = :a",
        "ExpressionAttributeValues": {":n": {"N": "fred"}, ":a": {"N": "bob"}},
    }));
    assert_eq!(CheckOrder::of(&body), CheckOrder::Readable);
    assert_eq!(check(&body), Some(numeric("fred")));
    for request_order in [[":n", ":a"], [":a", ":n"]] {
        let names: Vec<String> = request_order.iter().map(|n| (*n).to_owned()).collect();
        assert_eq!(
            validation_message(validate_scan_request_body(
                &body,
                CheckOrder::Readable,
                Some(&names)
            )),
            Some(numeric("fred")),
            "{request_order:?}"
        );
    }
    // Two names in one bucket: the request's order decides (`:a` and `:e`
    // share bucket 3).
    let body = scan(json!({
        "FilterExpression": "a = :p",
        "ExpressionAttributeValues": {":p": {"S": "x"}, ":a": {"N": "bob"}, ":e": {"N": "fred"}},
    }));
    for (request_order, first) in [([":p", ":a", ":e"], "bob"), ([":p", ":e", ":a"], "fred")] {
        let names: Vec<String> = request_order.iter().map(|n| (*n).to_owned()).collect();
        assert_eq!(
            validation_message(validate_scan_request_body(
                &body,
                CheckOrder::Readable,
                Some(&names)
            )),
            Some(numeric(first)),
            "{request_order:?}"
        );
    }
}

/// Measured 2026-11-05 (order R): with two set faults, `{":n": {"NS": []},
/// ":a": {"SS": []}}` answered the empty `SS` text in one send and the empty
/// `NS` text in the other. The set pass visits the entries in sorted name
/// order, whatever order the backend's parsed body keeps, so that the answer
/// does not depend on the backend: it names `:a`. A number in an entry is not
/// the set pass's to report (`eavR-NSbobdup-and-nest`: the number inside a
/// refused entry comes after the nesting check).
#[test]
fn the_order_r_set_pass_visits_the_entries_in_sorted_name_order() {
    let body = scan(json!({"ExpressionAttributeValues": {":n": {"NS": []}, ":a": {"SS": []}}}));
    assert_eq!(CheckOrder::of(&body), CheckOrder::Readable);
    assert_eq!(
        check(&body),
        Some(format!("{INVALID}An string set  may not be empty"))
    );
    let body = scan(json!({"ExpressionAttributeValues": {":a": {"NS": []}, ":n": {"SS": []}}}));
    assert_eq!(
        check(&body),
        Some(format!("{INVALID}An number set  may not be empty"))
    );
    // A malformed number inside an entry with a set fault below it: skipped by
    // the request-time checks (the nesting check of another entry answers
    // first), reported by `ExpressionValuesBody` with the plain text.
    let one = json!({"N": "1"});
    let mut body = scan(json!({
        "FilterExpression": "a = :p",
        "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": nested(&one, 32), ":a": {"NS": ["bob", "1", "1"]}},
    }));
    assert_eq!(check(&body).as_deref(), Some(NESTING));
    body["ExpressionAttributeValues"]
        .as_object_mut()
        .expect("an object")
        .remove(":d");
    assert_eq!(check(&body), None);
    let values = ExpressionValuesBody::take(&mut body, CheckOrder::Readable, None);
    assert_eq!(
        validation_message(values.validate()).as_deref(),
        Some(NUMERIC_BOB)
    );
}

/// Measured 2026-11-05 (order U): a set fault in an `ExpressionAttributeValues`
/// entry takes the invalid-value form at the map's step, with the set texts of
/// order U, in the entries' hash order: `{":p": {"S": "x"}, ":s": {"NS": []},
/// ":e": {}}` names `:s` (bucket 1, ahead of `:e` in 3); an `SS` with
/// duplicates answers `... Input collection [a, a] contains duplicates. for key
/// :s`. Inside one entry the first fault in the value's element order is named:
/// `{"L": [{"N": "bob"}, {}]}` answers the number (measured), `{"L": [{},
/// {"N": "bob"}]}` the empty value (not measured, the element order).
#[test]
fn an_order_u_set_fault_in_an_expression_value_takes_the_invalid_value_form() {
    let wrapper = |text: &str, name: &str| {
        format!("ExpressionAttributeValues contains invalid value: {text} for key {name}")
    };
    for (set, text) in [
        (
            json!({"NS": []}),
            format!("{INVALID}An number set  may not be empty"),
        ),
        (
            json!({"SS": ["a", "a"]}),
            format!("{INVALID}Input collection [a, a] contains duplicates."),
        ),
        (
            json!({"BS": []}),
            format!("{INVALID}Binary sets should not be empty"),
        ),
    ] {
        let mut body = scan(json!({
            "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":s": set, ":e": {}},
        }));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable);
        assert_eq!(check(&body), None, "{set}");
        let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, None);
        assert_eq!(
            validation_message(values.validate()),
            Some(wrapper(&text, ":s")),
            "{set}"
        );
        assert_eq!(body["ExpressionAttributeValues"][":s"], json!({"S": ""}));
        assert_eq!(body["ExpressionAttributeValues"][":e"], json!({"S": ""}));
    }
    // Inside one entry: the element order.
    for (value, text) in [
        (json!({"L": [{"N": "bob"}, {}]}), NUMERIC_BOB),
        (json!({"L": [{}, {"N": "bob"}]}), EMPTY_VALUE),
        (
            json!({"L": [{"NS": []}, {}]}),
            "One or more parameter values were invalid: An number set  may not be empty",
        ),
        (
            json!({"L": [{"L": [{"N": "bob"}]}, {"S": "x", "N": "1"}]}),
            NUMERIC_BOB,
        ),
    ] {
        let mut body = scan(json!({
            "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":l": value},
        }));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable, "{value}");
        let values = ExpressionValuesBody::take(&mut body, CheckOrder::Unreadable, None);
        assert_eq!(
            validation_message(values.validate()),
            Some(wrapper(text, ":l")),
            "{value}"
        );
    }
}

/// Measured 2026-11-05: in order U `ScanFilterBody::take` holds a `ScanFilter`
/// list value back for the step after the no-expression checks: a value that is
/// not one AttributeValue answers `Supplied AttributeValue is empty, must
/// contain exactly one of the supported datatypes` with no prefix, for `{}` and
/// for `{"X": "1"}`. In order R nothing is taken. A malformed number in a list
/// is #507's check (#564 item 3): the request-time checks pass it and `take`
/// leaves it in the body in both orders.
#[test]
fn a_scan_filter_list_value_is_checked_at_its_step() {
    let list = |values: Value| {
        scan(
            json!({"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": values}}}),
        )
    };
    // The numbers of the lists are #507's: not a request-time check here.
    let mut bob = list(json!([{"N": "bob"}, {"NS": ["bob"]}]));
    assert_eq!(CheckOrder::of(&bob), CheckOrder::Readable);
    assert_eq!(check(&bob), None);
    let beside_key = with(
        list(json!([{"N": "bob"}])),
        json!({"ExclusiveStartKey": {"pk": {"N": "fred"}, "sk": {"S": "x"}}}),
    );
    assert_eq!(check(&beside_key), None);
    assert!(
        ScanFilterBody::take(&mut bob, CheckOrder::Readable, None)
            .validate()
            .is_ok()
    );
    assert_eq!(
        bob["ScanFilter"]["a"]["AttributeValueList"],
        json!([{"N": "bob"}, {"NS": ["bob"]}])
    );
    // Order U: the list value is held back.
    for value in [json!({}), json!({"X": "1"}), json!({"L": [{}]})] {
        let mut body = list(json!([value]));
        assert_eq!(CheckOrder::of(&body), CheckOrder::Unreadable, "{value}");
        assert_eq!(check(&body), None, "{value}");
        let filter = ScanFilterBody::take(&mut body, CheckOrder::Unreadable, None);
        assert_eq!(
            body["ScanFilter"]["a"]["AttributeValueList"],
            json!([{"S": ""}]),
            "{value}"
        );
        assert_eq!(
            validation_message(filter.validate()).as_deref(),
            Some(EMPTY_VALUE),
            "{value}"
        );
        assert!(
            serde_json::from_value::<extenddb_core::types::ScanInput>(body).is_ok(),
            "{value}"
        );
    }
    let mut two = list(json!([{"S": "x", "N": "1"}]));
    let filter = ScanFilterBody::take(&mut two, CheckOrder::Unreadable, None);
    assert_eq!(
        validation_message(filter.validate()).as_deref(),
        Some(MORE_THAN_ONE)
    );
    // A number beside an unreadable key member stays in the body in order U
    // too (#507's check).
    let mut number = with(
        list(json!([{"N": "bob"}])),
        json!({"ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}}),
    );
    assert_eq!(check(&number), None);
    let filter = ScanFilterBody::take(&mut number, CheckOrder::Unreadable, None);
    assert!(filter.validate().is_ok());
    assert_eq!(
        number["ScanFilter"]["a"]["AttributeValueList"],
        json!([{"N": "bob"}])
    );
    // Order R: nothing is taken, a readable list stays as written.
    let mut readable = list(json!([{"N": "1"}]));
    let filter = ScanFilterBody::take(&mut readable, CheckOrder::Readable, None);
    assert_eq!(
        readable["ScanFilter"]["a"]["AttributeValueList"],
        json!([{"N": "1"}])
    );
    assert!(filter.validate().is_ok());
    // No ScanFilter at all.
    let mut none = scan(json!({}));
    assert!(
        ScanFilterBody::take(&mut none, CheckOrder::Unreadable, None)
            .validate()
            .is_ok()
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
    // Measured 2026-10-31 (B4-5a): with no expression at all, the nesting text
    // ahead of the no-expression check.
    let no_expression = scan(json!({"ExpressionAttributeValues": {":d": nested(&one, 32)}}));
    assert_eq!(check(&no_expression).as_deref(), Some(NESTING));
    // Measured 2026-11-05 (eavR-NSe-and-nest, both sends): an empty set in
    // another entry answers the set text ahead of the nesting text, with the
    // text of order R; a duplicate set too (not measured beside a nested
    // value). A number in a third entry comes ahead of both.
    let beside_empty_set = scan(
        json!({"FilterExpression": "a = :p", "ExpressionAttributeValues":
        {":p": {"S": "x"}, ":d": nested(&one, 32), ":e": {"NS": []}}}),
    );
    assert_eq!(CheckOrder::of(&beside_empty_set), CheckOrder::Readable);
    assert_eq!(
        check(&beside_empty_set).as_deref(),
        Some("One or more parameter values were invalid: An number set  may not be empty")
    );
    let beside_duplicates = scan(
        json!({"FilterExpression": "a = :p", "ExpressionAttributeValues":
        {":p": {"S": "x"}, ":d": nested(&one, 32), ":a": {"SS": ["a", "a"]}}}),
    );
    assert_eq!(
        check(&beside_duplicates).as_deref(),
        Some(
            "One or more parameter values were invalid: Input collection [\"a\", \"a\"] contains duplicates."
        )
    );
    let number_first = scan(
        json!({"FilterExpression": "a = :p", "ExpressionAttributeValues":
        {":p": {"S": "x"}, ":d": nested(&one, 32), ":e": {"NS": []}, ":n": {"N": "bob"}}}),
    );
    assert_eq!(check(&number_first).as_deref(), Some(NUMERIC_BOB));
    // In order U the nesting check leaves request time: beside an unused
    // empty value (B4-7) or a key shape fault (2026-11-05) the request-time
    // checks pass and the handler reports those first.
    let beside_empty = scan(json!({
        "FilterExpression": "a = :p",
        "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": nested(&one, 32), ":e": {}},
    }));
    assert_eq!(check(&beside_empty), None);
    let beside_key = with(
        with_unused_value(nested(&one, 32)),
        json!({"ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}}),
    );
    assert_eq!(check(&beside_key), None);
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
    // The ReturnConsumedCapacity enum comes ahead of the parameter mix
    // (2026-11-05).
    let rcc_mix = with(
        with(scan(filter("1")), json!({"ScanFilter": {}})),
        json!({"ReturnConsumedCapacity": "BAD"}),
    );
    assert_eq!(
        check(&rcc_mix).as_deref(),
        Some(
            "1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
        )
    );
}
