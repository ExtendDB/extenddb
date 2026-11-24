// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks at request time: the `ScanFilter` in the parameter mix,
//! the `AttributesToGet` length, the two check orders, the numbers, the set
//! faults and the nesting depth of `ExpressionAttributeValues`, and a value of
//! the wrong JSON type. The rows are the service's answers; see
//! `test_support` for the dates.

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::test_support::{
    INVALID, LENGTH, MIX, NESTING, NUMERIC_BOB, SELECT_BAD, check, check_select, filter, nested,
    scan, with, with_key, with_unused_value,
};
use super::{CheckOrder, ExpressionValuesBody, request_checks};

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
                "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": nested(&json!({"N": "1"}), 33)},
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

/// Order R, two entries with a malformed number: the request-time number
/// check visits the `ExpressionAttributeValues` entries in placeholder order
/// and names `:a` here. The service named either entry: `{":n": {"N":
/// "fred"}, ":a": {"N": "bob"}}` answered `fred` in 4 of 6 sends and `bob` in
/// 2 on 2026-11-23 at 14:12 UTC (`fred` on 2026-11-05), so the wire row
/// accepts either answer.
#[test]
fn two_malformed_numbers_in_order_r_answer_one_of_the_two() {
    let body = scan(json!({
        "FilterExpression": "a = :a",
        "ExpressionAttributeValues": {":n": {"N": "fred"}, ":a": {"N": "bob"}},
    }));
    assert_eq!(CheckOrder::of(&body), CheckOrder::Readable);
    let fred = NUMERIC_BOB.replace("bob", "fred");
    let answer = check(&body);
    assert!(
        [NUMERIC_BOB, fred.as_str()].contains(&answer.as_deref().unwrap_or_default()),
        "{answer:?}"
    );
}

/// Order R: the set pass visits the entries in placeholder order, whatever
/// order the backend's parsed body keeps. With two set faults, `{":n": {"NS":
/// []}, ":a": {"SS": []}}` answered the empty `SS` text in one send and the
/// empty `NS` text in the other (2026-11-05); the placeholder order names
/// `:a`. A malformed number inside an entry with a set fault is one of the
/// numbers, which come first: `{"NS": ["bob", "1", "1"]}` beside a value nested
/// 33 levels in another entry answered `bob` in one of two sends and the
/// nesting text in the other on 2026-11-24 at 08:46 UTC.
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
    let one = json!({"N": "1"});
    let mut body = scan(json!({
        "FilterExpression": "a = :p",
        "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": nested(&one, 33), ":a": {"NS": ["bob", "1", "1"]}},
    }));
    assert_eq!(check(&body).as_deref(), Some(NUMERIC_BOB));
    // In order R `ExpressionValuesBody` takes nothing: every fault of the map
    // is a request-time check.
    let values = ExpressionValuesBody::take(&mut body, CheckOrder::Readable, None);
    assert!(values.validate().is_ok());
    assert_eq!(
        body["ExpressionAttributeValues"][":a"],
        json!({"NS": ["bob", "1", "1"]})
    );
}

/// A member of the wrong JSON type in a value is found behind a value fault
/// that comes first in the same map or list, ahead of every check. Measured
/// 2026-11-23 (us-east-1, each body twice): `{"L": [{"S": 5}, {}]}` as an
/// entry and `{"pk": {"NS": []}, "sk": {"S": 5}}` as the key answer
/// `SerializationException: NUMBER_VALUE cannot be converted to String`, and
/// an empty `AttributesToGet` beside a key with an empty `NS` and `"Limit":
/// "5"` answers `STRING_VALUE cannot be converted to Integer`. In the review
/// of 2026-11-12, `{"pk": {}, "sk": {"N": 5}}` as the key and `{":a": {"NS":
/// []}, ":b": {"S": 5}}` answered `SerializationException` too. The service's
/// texts for a value differ from the deserializer's, so only the exception
/// is asserted for them.
#[test]
fn a_value_of_the_wrong_json_type_is_found_behind_a_value_fault() {
    let list =
        |values: Value| json!({"a": {"ComparisonOperator": "EQ", "AttributeValueList": values}});
    for body in [
        scan(json!({"FilterExpression": "a = :a",
                    "ExpressionAttributeValues": {":a": {"L": [{"S": 5}, {}]}}})),
        with_key(json!({"pk": {"NS": []}, "sk": {"S": 5}})),
        with_key(json!({"pk": {}, "sk": {"N": 5}})),
        scan(json!({"ExpressionAttributeValues": {":a": {"NS": []}, ":b": {"S": 5}}})),
        scan(json!({"ScanFilter": list(json!([{"L": [{"S": 5}, {}]}]))})),
    ] {
        assert!(
            matches!(
                request_checks(&body),
                Err(DynamoDbError::SerializationException(_))
            ),
            "{body}"
        );
    }
    let limit = scan(
        json!({"AttributesToGet": [], "ExclusiveStartKey": {"pk": {"NS": []}},
                            "Limit": "5"}),
    );
    match request_checks(&limit) {
        Err(DynamoDbError::SerializationException(msg)) => {
            assert_eq!(msg, "STRING_VALUE cannot be converted to Integer");
        }
        other => panic!("{other:?}"),
    }
}

/// Measured 2026-11-24 at 08:11 UTC (us-east-1, each request twice, a table
/// that does not exist): an `L` chain and an `M` chain around `{"N": "1"}` in
/// a `FilterExpression` value pass at 30, 31 and 32 levels and answer the
/// nesting text at 33, 34 and 62 levels. At 08:46 UTC, 33 levels answered the
/// nesting text ahead of the `Select` enum, the filter syntax, the
/// no-expression check and the key's number, and after an empty `NS` in
/// another entry; beside `{"SS": ["a", "a"]}` the duplicates text in one of
/// two sends and the nesting text in the other.
#[test]
fn a_value_nested_33_levels_deep_is_refused_and_32_levels_pass() {
    let one = json!({"N": "1"});
    let map = |levels: usize| (0..levels).fold(one.clone(), |inner, _| json!({"M": {"m": inner}}));
    for levels in [33, 34, 40, 62] {
        let body = with_unused_value(nested(&one, levels));
        assert_eq!(check(&body).as_deref(), Some(NESTING), "{levels} levels");
        let body = with_unused_value(map(levels));
        assert_eq!(
            check(&body).as_deref(),
            Some(NESTING),
            "{levels} map levels"
        );
    }
    // 32 levels reach the checks the handler makes after deserializing.
    assert_eq!(check(&with_unused_value(nested(&one, 32))), None);
    assert_eq!(check(&with_unused_value(map(32))), None);
    // Ahead of the Select enum.
    let select = with(
        with_unused_value(nested(&one, 33)),
        json!({"Select": "BAD"}),
    );
    assert_eq!(check(&select).as_deref(), Some(NESTING));
    // With no expression at all, the nesting text ahead of the no-expression
    // check (B4-5a).
    let no_expression = scan(json!({"ExpressionAttributeValues": {":d": nested(&one, 33)}}));
    assert_eq!(check(&no_expression).as_deref(), Some(NESTING));
    // The set pass comes ahead of the nesting check: an empty set in another
    // entry answers the set text, with the text of order R, and a duplicate
    // set too. A number in a third entry comes ahead of both.
    let beside_empty_set = scan(
        json!({"FilterExpression": "a = :p", "ExpressionAttributeValues":
        {":p": {"S": "x"}, ":d": nested(&one, 33), ":e": {"NS": []}}}),
    );
    assert_eq!(CheckOrder::of(&beside_empty_set), CheckOrder::Readable);
    assert_eq!(
        check(&beside_empty_set).as_deref(),
        Some("One or more parameter values were invalid: An number set  may not be empty")
    );
    let beside_duplicates = scan(
        json!({"FilterExpression": "a = :p", "ExpressionAttributeValues":
        {":p": {"S": "x"}, ":d": nested(&one, 33), ":a": {"SS": ["a", "a"]}}}),
    );
    assert_eq!(
        check(&beside_duplicates).as_deref(),
        Some(
            "One or more parameter values were invalid: Input collection [\"a\", \"a\"] contains duplicates."
        )
    );
    let number_first = scan(
        json!({"FilterExpression": "a = :p", "ExpressionAttributeValues":
        {":p": {"S": "x"}, ":d": nested(&one, 33), ":e": {"NS": []}, ":n": {"N": "bob"}}}),
    );
    assert_eq!(check(&number_first).as_deref(), Some(NUMERIC_BOB));
    // In order U the nesting check leaves request time: beside an unused
    // empty value (B4-7) or a key shape fault the request-time checks pass,
    // and the handler reports the key's fault and then the faulty entries in
    // the hash order, the nested one among them.
    let beside_empty = scan(json!({
        "FilterExpression": "a = :p",
        "ExpressionAttributeValues": {":p": {"S": "x"}, ":d": nested(&one, 33), ":e": {}},
    }));
    assert_eq!(check(&beside_empty), None);
    let beside_key = with(
        with_unused_value(nested(&one, 33)),
        json!({"ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}}),
    );
    assert_eq!(check(&beside_key), None);
}

/// The body a PartiQL `SELECT` is translated into has the limit of a `Scan`
/// request: `SELECT * FROM "t" WHERE a <> ?` with a parameter of 32 maps
/// around a string returns the items, and with 33 maps answers the nesting
/// text (measured 2026-11-09 at 11:44 UTC, each request once), which the
/// PartiQL parameter check gives before the statement is read.
#[test]
fn the_body_of_a_partiql_select_passes_32_levels() {
    let deep = |levels: usize| {
        (0..levels).fold(json!({"S": "leaf"}), |inner, _| json!({"M": {"n": inner}}))
    };
    let select = |value: Value| {
        scan(json!({
            "FilterExpression": "#n0 <> :v0",
            "ExpressionAttributeNames": {"#n0": "a"},
            "ExpressionAttributeValues": {":v0": value},
        }))
    };
    let body = select(deep(32));
    assert_eq!(CheckOrder::of(&body), CheckOrder::Readable);
    assert_eq!(check(&body), None);
    assert_eq!(check(&select(deep(33))).as_deref(), Some(NESTING));
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
    // An M at the top, 32 lists below it.
    let body = with_unused_value(json!({"M": {"k": nested(&bob, 32)}}));
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
    // In the older form the values are echoed and the empty list comes last
    // (measured 2026-11-23 21:49 UTC).
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
