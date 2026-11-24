// SPDX-License-Identifier: Apache-2.0

//! Tests for a `Query` on the service's older validation path: one whose
//! `ExpressionAttributeValues`, `ExclusiveStartKey` or condition lists hold a
//! value the service cannot read as one type, or whose `Limit` is a float or
//! an integer outside int32. The set check of that path is tested on `Scan`
//! and `Query` here: `Query` makes it in `older_form_checks`, and `Scan` in
//! the checks the server runs ahead of the IAM decision
//! (`scan_request::validate_before_authorization`).

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::tests::{
    EMPTY, INVALID_KEY, INVALID_VALUE, LIMIT_ZERO, MIX, NULL, NUMERIC_BOB, RCC_BAD, SELECT_BAD,
    SEVERAL, body_with, message, one_error,
};
use super::validate_query_request_body;
use crate::scan_request::validate_before_authorization;

#[test]
fn a_value_the_service_cannot_read_as_one_type_is_named_with_its_placeholder() {
    // Measured 2026-11-11, each value alone beside `:p = {"S": "x"}`.
    for (value, reason) in [
        (json!({"X": "1"}), EMPTY.to_owned()),
        (Value::Null, NULL.to_owned()),
        (json!({}), EMPTY.to_owned()),
        (json!({"S": "a", "N": "1"}), SEVERAL.to_owned()),
        (json!({"L": [{"X": "1"}]}), EMPTY.to_owned()),
        (json!({"L": [null]}), NULL.to_owned()),
        (json!({"L": [{}]}), EMPTY.to_owned()),
        (json!({"L": [{"S": "a", "N": "1"}]}), SEVERAL.to_owned()),
        (json!({"L": [{"S": "a"}, {"X": "1"}]}), EMPTY.to_owned()),
        (json!({"M": {"k": {"X": "1"}}}), EMPTY.to_owned()),
        (json!({"M": {"k": {}}}), EMPTY.to_owned()),
        (json!({"M": {"k": null}}), format!("{NULL}: k is null")),
        (
            json!({"M": {"k": {"M": {"j": null}}}}),
            format!("{NULL}: j is null"),
        ),
        (json!({"M": {"k": {"L": [null]}}}), NULL.to_owned()),
        (
            json!({"L": [{"M": {"k": null}}]}),
            format!("{NULL}: k is null"),
        ),
        // The first fault in document order.
        (
            json!({"L": [{"N": "bob"}, {"X": "1"}]}),
            NUMERIC_BOB.to_owned(),
        ),
        (json!({"L": [{"X": "1"}, {"N": "bob"}]}), EMPTY.to_owned()),
    ] {
        let body = body_with(json!({":p": {"S": "x"}, ":z": value}), json!({}));
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :z")),
            "{value}"
        );
    }
}

#[test]
fn a_number_fault_in_a_value_with_several_types_is_named_ahead_of_the_several_types_text() {
    // Measured 2026-11-19, each value beside `:p = {"S": "x"}`: the service
    // reads every payload, N ahead of NS, NS and M ahead of L, whatever order
    // the request wrote them in, and answers the more than one datatypes text
    // only when no payload has a fault.
    let numeric =
        |text: &str| format!("The parameter cannot be converted to a numeric value: {text}");
    for (value, reason) in [
        (json!({"N": "bob", "S": "x"}), numeric("bob")),
        (json!({"N": "1", "S": "x"}), SEVERAL.to_owned()),
        (
            json!({"N": "", "S": "x"}),
            "The parameter cannot be converted to a numeric value".to_owned(),
        ),
        (
            json!({"N": "1e200", "S": "x"}),
            "Number overflow. Attempting to store a number with magnitude larger than \
             supported range"
                .to_owned(),
        ),
        (json!({"NS": ["bob"], "S": "x"}), numeric("bob")),
        (json!({"BOOL": true, "N": "bob"}), numeric("bob")),
        (json!({"S": "x", "N": "bob", "X": "1"}), numeric("bob")),
        (json!({"L": [{"N": "bob", "S": "x"}]}), numeric("bob")),
        (json!({"S": "x", "L": [{"N": "bob"}]}), numeric("bob")),
        (json!({"S": "x", "M": {"a": {"N": "bob"}}}), numeric("bob")),
        (json!({"S": "x", "L": [{"X": "1"}]}), EMPTY.to_owned()),
        (json!({"S": "x", "L": [null]}), NULL.to_owned()),
        (json!({"N": "aaa", "NS": ["bbb"]}), numeric("aaa")),
        (json!({"N": "bbb", "L": [{"N": "aaa"}]}), numeric("bbb")),
        (json!({"NS": ["aaa"], "L": [{"N": "bbb"}]}), numeric("aaa")),
        (
            json!({"M": {"k": {"N": "aaa"}}, "L": [{"N": "bbb"}]}),
            numeric("aaa"),
        ),
    ] {
        let body = body_with(json!({":p": {"S": "x"}, ":z": value}), json!({}));
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :z")),
            "{value}"
        );
    }
    // Such a value still takes the order of a value the service cannot read.
    let body = body_with(
        json!({":p": {"S": "x"}, ":z": {"N": "bob", "S": "x"}}),
        json!({"Limit": 0}),
    );
    assert_eq!(
        message(validate_query_request_body(&body)),
        Some(one_error(LIMIT_ZERO))
    );
}

#[test]
fn numbers_beside_a_value_the_service_cannot_read_are_wrapped_and_echoed_as_sent() {
    // Measured 2026-11-11 beside `:a = {"S": "a", "X": "1"}`, which the service
    // reads as an S: the numeric texts are wrapped with the placeholder, the
    // value is echoed without folding its digits, an empty N has no colon, and
    // a decimal digit below U+10000 still counts as a digit.
    let numeric = "The parameter cannot be converted to a numeric value";
    for (value, reason) in [
        (json!({"N": "bob"}), format!("{numeric}: bob")),
        (json!({"N": ""}), numeric.to_owned()),
        (json!({"NS": ["1", "bob"]}), format!("{numeric}: bob")),
        (json!({"L": [{"N": "bob"}]}), format!("{numeric}: bob")),
        (
            json!({"N": "\u{ff11}bob"}),
            format!("{numeric}: \u{ff11}bob"),
        ),
        (json!({"N": "\u{1d7cf}"}), format!("{numeric}: \u{1d7cf}")),
        (
            json!({"N": "1e200"}),
            "Number overflow. Attempting to store a number with magnitude larger than \
             supported range"
                .to_owned(),
        ),
        (
            json!({"N": "1".repeat(39)}),
            "Attempting to store more than 38 significant digits in a Number".to_owned(),
        ),
    ] {
        let body = body_with(
            json!({":p": {"S": "x"}, ":a": {"S": "a", "X": "1"}, ":u": value}),
            json!({}),
        );
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :u")),
            "{value}"
        );
    }
    let body = body_with(
        json!({":p": {"S": "x"}, ":a": {"S": "a", "X": "1"}, ":u": {"N": "\u{ff11}"}}),
        json!({}),
    );
    assert!(validate_query_request_body(&body).is_ok());
}

#[test]
fn a_value_the_service_cannot_read_is_named_ahead_of_a_number_fault() {
    // Measured 2026-11-11: beside `:u = {"N": "bob"}`, the service names `:z`
    // or `:a` when it holds a value it cannot read as one type (28 of 28
    // requests for `:z = {"X": "1"}`), and names `:u` when the other value has
    // no fault of its own.
    for (value, reason) in [
        (json!({"X": "1"}), EMPTY.to_owned()),
        (Value::Null, NULL.to_owned()),
        (json!({}), EMPTY.to_owned()),
        (json!({"S": "a", "N": "1"}), SEVERAL.to_owned()),
        (json!({"L": [{"X": "1"}]}), EMPTY.to_owned()),
        (json!({"L": [null]}), NULL.to_owned()),
        (json!({"L": [{}]}), EMPTY.to_owned()),
        (json!({"L": [{"S": "a", "N": "1"}]}), SEVERAL.to_owned()),
        (json!({"M": {"k": {"X": "1"}}}), EMPTY.to_owned()),
        (json!({"M": {"k": null}}), format!("{NULL}: k is null")),
        (json!({"L": [{"S": "a"}, {"X": "1"}]}), EMPTY.to_owned()),
        (
            json!({"L": [{"N": "bob"}, {"X": "1"}]}),
            NUMERIC_BOB.to_owned(),
        ),
        (json!({"L": [{"X": "1"}, {"N": "bob"}]}), EMPTY.to_owned()),
    ] {
        let body = body_with(
            json!({":p": {"S": "x"}, ":u": {"N": "bob"}, ":z": value}),
            json!({}),
        );
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :z")),
            "{value}"
        );
    }
    for (value, reason) in [
        (json!({"X": "1"}), EMPTY),
        (Value::Null, NULL),
        (json!({}), EMPTY),
        (json!({"S": "a", "N": "1"}), SEVERAL),
        (json!({"L": [{"X": "1"}]}), EMPTY),
    ] {
        let body = body_with(
            json!({":p": {"S": "x"}, ":a": value, ":u": {"N": "bob"}}),
            json!({}),
        );
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{reason} for key :a")),
            "{value}"
        );
    }
    for values in [
        json!({":p": {"S": "x"}, ":u": {"N": "bob"}, ":z": {"S": "a", "X": "1"}}),
        json!({":p": {"S": "x"}, ":a": {"S": "a", "X": "1"}, ":u": {"N": "bob"}}),
    ] {
        let body = body_with(values.clone(), json!({}));
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(format!("{INVALID_VALUE}{NUMERIC_BOB} for key :u")),
            "{values}"
        );
    }
}

#[test]
fn beside_a_value_the_service_cannot_read_the_enums_and_limit_are_reported_together() {
    // Measured 2026-11-11 beside `:z = {"X": "1"}`: Select, then
    // ReturnConsumedCapacity, then Limit, in one message, ahead of the
    // parameter mix; the mix ahead of the values.
    let values = json!({":p": {"S": "x"}, ":u": {"N": "bob"}, ":z": {"X": "1"}});
    for (extra, expected) in [
        (json!({"Limit": 0}), one_error(LIMIT_ZERO)),
        (
            json!({"Limit": -1}),
            one_error(&LIMIT_ZERO.replace("'0'", "'-1'")),
        ),
        (json!({"Select": "bad"}), one_error(SELECT_BAD)),
        (json!({"ReturnConsumedCapacity": "bad"}), one_error(RCC_BAD)),
        (
            json!({"Limit": 0, "ReturnConsumedCapacity": "bad"}),
            format!("2 validation errors detected: {RCC_BAD}; {LIMIT_ZERO}"),
        ),
        (
            json!({"Limit": 0, "Select": "bad", "AttributesToGet": ["a"]}),
            format!("2 validation errors detected: {SELECT_BAD}; {LIMIT_ZERO}"),
        ),
        (
            json!({"Limit": 0, "ReturnConsumedCapacity": "bad", "Select": "bad"}),
            format!("3 validation errors detected: {SELECT_BAD}; {RCC_BAD}; {LIMIT_ZERO}"),
        ),
        (json!({"AttributesToGet": ["a"]}), MIX.to_owned()),
    ] {
        let body = body_with(values.clone(), extra.clone());
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(expected),
            "{extra}"
        );
    }
    // A null value takes the same path.
    let body = body_with(
        json!({":p": {"S": "x"}, ":z": null}),
        json!({"Limit": 0, "ReturnConsumedCapacity": "bad"}),
    );
    assert_eq!(
        message(validate_query_request_body(&body)),
        Some(format!(
            "2 validation errors detected: {RCC_BAD}; {LIMIT_ZERO}"
        ))
    );
}

#[test]
fn a_shape_fault_is_still_reported_ahead_of_a_value_the_service_cannot_read() {
    for extra in [
        json!({"Select": 5}),
        json!({"ExpressionAttributeNames": "x"}),
    ] {
        for value in [json!({"X": "1"}), Value::Null] {
            let body = body_with(json!({":p": {"S": "x"}, ":z": value}), extra.clone());
            assert!(
                matches!(
                    validate_query_request_body(&body),
                    Err(DynamoDbError::SerializationException(_))
                ),
                "{body}"
            );
        }
    }
}

#[test]
fn a_json_type_fault_inside_a_value_the_service_cannot_read_is_a_shape_fault() {
    // Measured 2026-11-11: each answers SerializationException.
    for (values, extra) in [
        (json!({":z": {"L": [null, "x"]}}), json!({})),
        (json!({":z": {"L": [{"X": "1"}, {"N": 5}]}}), json!({})),
        (json!({":z": {"S": 5, "X": "1"}}), json!({"Limit": 0})),
        (json!({":z": {"L": [null, {"B": "!!"}]}}), json!({})),
        (json!({":z": {"S": 5, "N": "1"}}), json!({"Limit": 0})),
        (json!({":z": {"L": [{"S": "a", "N": "1"}, "x"]}}), json!({})),
        (
            json!({":a": {"S": "a", "N": "1"}, ":b": {"N": 5}}),
            json!({}),
        ),
        (json!({":z": {"S": "a", "N": 1}}), json!({})),
    ] {
        let mut values = values;
        values[":p"] = json!({"S": "x"});
        let body = body_with(values, extra);
        assert!(
            matches!(
                validate_query_request_body(&body),
                Err(DynamoDbError::SerializationException(_))
            ),
            "{body}"
        );
    }
    // Two known descriptors with no JSON-type fault: the Limit text.
    let body = body_with(
        json!({":p": {"S": "x"}, ":z": {"S": "a", "N": "1"}}),
        json!({"Limit": 0}),
    );
    assert_eq!(
        message(validate_query_request_body(&body)),
        Some(one_error(LIMIT_ZERO))
    );
}

#[test]
fn a_type_descriptor_with_a_null_payload_counts_as_absent() {
    // Measured 2026-11-19 on Query, each body twice: the service reads a null
    // payload as no payload, so `{"S": null}` is a value it cannot read as one
    // type, and the request takes that order of checks.
    let empty = format!("{INVALID_VALUE}{EMPTY} for key :v");
    for (value, extra, expected) in [
        (json!({"S": null}), json!({}), empty.clone()),
        (json!({"N": null}), json!({}), empty.clone()),
        (json!({"L": [{"S": null}]}), json!({}), empty),
        (
            json!({"S": null, "N": "bob"}),
            json!({}),
            format!("{INVALID_VALUE}{NUMERIC_BOB} for key :v"),
        ),
        (
            json!({"S": null}),
            json!({"Select": "bad"}),
            one_error(SELECT_BAD),
        ),
        (
            json!({"S": null}),
            json!({"Limit": 0}),
            one_error(LIMIT_ZERO),
        ),
        (
            json!({"S": null}),
            json!({"Select": "bad", "Limit": 0}),
            format!("2 validation errors detected: {SELECT_BAD}; {LIMIT_ZERO}"),
        ),
        (
            json!({"S": null}),
            json!({"ReturnConsumedCapacity": "bad"}),
            one_error(RCC_BAD),
        ),
        (
            json!({"S": null}),
            json!({"AttributesToGet": ["a"]}),
            MIX.to_owned(),
        ),
        (
            json!({"S": null}),
            json!({"ExpressionAttributeNames": {"a": "b"}}),
            INVALID_KEY.to_owned(),
        ),
    ] {
        let body = body_with(json!({":p": {"S": "x"}, ":v": value}), extra);
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(expected),
            "{body}"
        );
    }
    // `{"S": null, "N": "1"}` reads as `{"N": "1"}`: the service accepts it in
    // a FilterExpression.
    let body = body_with(
        json!({":p": {"S": "x"}, ":v": {"S": null, "N": "1"}}),
        json!({"FilterExpression": "a = :v"}),
    );
    assert!(validate_query_request_body(&body).is_ok());
}

/// Measured 2026-11-23 14:51 and 18:45 UTC (us-east-1, each request twice, a
/// table that does not exist): a `Limit` that is a float or outside int32
/// puts a `Query` on the older path without any value the service cannot
/// read. The `limit` clause echoes the value as the service reads it,
/// saturated to int32, after the `select` and `returnConsumedCapacity`
/// clauses; a float `Limit` that satisfies its constraint leaves the enum
/// clauses alone; the aggregate comes ahead of the parameter mix, and the
/// numbers are wrapped as on that path. Beside `{}` the echoed value is
/// saturated too (measured 2026-10-31 12:36 UTC on the wire).
#[test]
fn a_limit_that_is_a_float_or_outside_int32_takes_the_older_path() {
    let limit_min = LIMIT_ZERO.replace("'0'", "'-2147483648'");
    let query_filter =
        json!({"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}});
    let p = json!({":p": {"S": "p0"}});
    for (values, extra, expected) in [
        (
            p.clone(),
            json!({"Limit": -2_147_483_649_i64}),
            one_error(&limit_min),
        ),
        (p.clone(), json!({"Limit": 0.0}), one_error(LIMIT_ZERO)),
        (
            json!({":p": {"S": "p0"}, ":a": {}}),
            json!({"Limit": -2_147_483_649_i64}),
            one_error(&limit_min),
        ),
        (
            json!({":p": {"S": "p0"}, ":a": {}}),
            json!({"Limit": 0, "ReturnConsumedCapacity": "BAD"}),
            format!(
                "2 validation errors detected: {}; {LIMIT_ZERO}",
                RCC_BAD.replace("'bad'", "'BAD'")
            ),
        ),
        (
            p.clone(),
            json!({"Limit": -2_147_483_649_i64, "Select": "BAD", "ReturnConsumedCapacity": "BAD"}),
            format!(
                "3 validation errors detected: {}; {}; {limit_min}",
                SELECT_BAD.replace("'bad'", "'BAD'"),
                RCC_BAD.replace("'bad'", "'BAD'")
            ),
        ),
        (
            p.clone(),
            json!({"Limit": 1.5, "Select": "BAD", "ReturnConsumedCapacity": "BAD"}),
            format!(
                "2 validation errors detected: {}; {}",
                SELECT_BAD.replace("'bad'", "'BAD'"),
                RCC_BAD.replace("'bad'", "'BAD'")
            ),
        ),
        (
            json!({":p": {"S": "p0"}, ":n": {"N": "bob"}}),
            json!({"Limit": 1.5, "FilterExpression": "a = :n"}),
            format!("{INVALID_VALUE}{NUMERIC_BOB} for key :n"),
        ),
        (
            p,
            json!({"Limit": 0.0, "QueryFilter": query_filter, "FilterExpression": "a = :p"}),
            one_error(LIMIT_ZERO),
        ),
    ] {
        let body = body_with(values, extra.clone());
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(expected),
            "{body}"
        );
    }
}

/// Measured 2026-11-12 (each row twice): a null `AttributeValueList`
/// entry of `KeyConditions` and an `L` nested in an `L` are values the service
/// cannot read, so a `Limit` of 0 beside them takes the older form on `Query`.
#[test]
fn a_null_list_entry_and_a_nested_list_member_take_the_older_path() {
    let bodies = [
        json!({
            "TableName": "t", "Limit": 0,
            "KeyConditions": {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [null]}},
        }),
        body_with(
            json!({":p": {"S": "p0"}, ":d": {"L": [{"L": [{}]}]}}),
            json!({"Limit": 0}),
        ),
    ];
    for body in bodies {
        assert_eq!(
            message(validate_query_request_body(&body)),
            Some(one_error(LIMIT_ZERO)),
            "{body}"
        );
    }
}

/// Measured 2026-11-24 05:41 UTC (us-east-1, each request twice, a table that
/// does not exist), each beside `Limit: 1.5`, which puts the request on the
/// older path: an empty set and a set with a duplicate member are refused
/// with this path's wrapped text, `Input collection contains duplicates` for
/// a number set, the deserializer's text for an empty number set and a
/// string set. An invalid names key comes ahead of the set, an empty name
/// after it. With no expression that can read a value, the service answers
/// the expressions-only text instead, which `Scan` gives ahead of the set.
/// Beside a set fault in one value and a number fault in another, the service
/// named `:n` in both arrangements, as `Scan`'s hash order of the entries
/// does; `Query`'s `older_form_checks` reads the numbers of every value ahead
/// of the sets (#1013).
#[test]
fn an_empty_or_duplicate_set_is_refused_on_the_older_path() {
    const EXPRESSIONS_ONLY: &str = "ExpressionAttributeValues can only be specified when using \
                                    expressions: FilterExpression is null";
    let duplicate_ns = json!({"NS": ["1", "1"]});
    let dup = format!("{INVALID_VALUE}Input collection contains duplicates for key :a");
    let scan = |fields: Value| {
        let mut body = json!({"TableName": "t", "Limit": 1.5});
        if let (Value::Object(body), Value::Object(fields)) = (&mut body, fields) {
            body.extend(fields);
        }
        body
    };
    let reads_a = |value: Value| {
        scan(json!({"FilterExpression": "a = :a", "ExpressionAttributeValues": {":a": value}}))
    };
    let rows = [
        (reads_a(duplicate_ns.clone()), Some(dup.clone())),
        (
            reads_a(json!({"NS": []})),
            Some(format!(
                "{INVALID_VALUE}One or more parameter values were invalid: An number set  may \
                 not be empty for key :a"
            )),
        ),
        (
            reads_a(json!({"SS": ["x", "x"]})),
            Some(format!(
                "{INVALID_VALUE}One or more parameter values were invalid: Input collection \
                 [x, x] contains duplicates. for key :a"
            )),
        ),
        (
            reads_a(json!({"L": [duplicate_ns.clone()]})),
            Some(dup.clone()),
        ),
        // The names key check comes first, the empty name check after.
        (
            scan(
                json!({"FilterExpression": "a = :a", "ExpressionAttributeNames": {"a": "b"},
                        "ExpressionAttributeValues": {":a": duplicate_ns.clone()}}),
            ),
            Some(INVALID_KEY.to_owned()),
        ),
        (
            scan(
                json!({"FilterExpression": "#a = :a", "ExpressionAttributeNames": {"#a": ""},
                        "ExpressionAttributeValues": {":a": duplicate_ns.clone()}}),
            ),
            Some(dup.clone()),
        ),
        // A value the service cannot read is named ahead of the set.
        (
            scan(json!({"FilterExpression": "a = :a AND b = :z",
                        "ExpressionAttributeValues": {":a": duplicate_ns.clone(), ":z": {"X": "1"}}})),
            Some(format!("{INVALID_VALUE}{EMPTY} for key :z")),
        ),
        // The number in `:n` ahead of the set in `:a` (the service named `:n`).
        (
            scan(json!({"FilterExpression": "a = :a AND b = :n",
                        "ExpressionAttributeValues": {":a": duplicate_ns.clone(), ":n": {"N": "bob"}}})),
            Some(format!("{INVALID_VALUE}{NUMERIC_BOB} for key :n")),
        ),
        // The set in `:n` ahead of the number in `:a` (the service named `:n`).
        (
            scan(json!({"FilterExpression": "a = :a AND b = :n",
                        "ExpressionAttributeValues": {":a": {"N": "bob"}, ":n": duplicate_ns.clone()}})),
            Some(format!(
                "{INVALID_VALUE}Input collection contains duplicates for key :n"
            )),
        ),
        // No expression, or a ProjectionExpression alone: the expressions-only text.
        (
            scan(json!({"ExpressionAttributeValues": {":a": duplicate_ns.clone()}})),
            Some(EXPRESSIONS_ONLY.to_owned()),
        ),
        (
            scan(json!({"ProjectionExpression": "a",
                        "ExpressionAttributeValues": {":a": duplicate_ns.clone()}})),
            Some(EXPRESSIONS_ONLY.to_owned()),
        ),
    ];
    for (body, expected) in rows {
        assert_eq!(
            message(validate_before_authorization(
                &body,
                body.to_string().as_bytes()
            )),
            expected,
            "{body}"
        );
    }
    let query = body_with(
        json!({":p": {"S": "p0"}, ":a": duplicate_ns}),
        json!({"Limit": 1.5, "FilterExpression": "a = :a"}),
    );
    assert_eq!(message(validate_query_request_body(&query)), Some(dup));
}
