// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    ExpressionMaps, KeyCondition, parse_key_condition, tokenize_with_limit,
};
use extenddb_core::types::{AttributeDefinition, AttributeValue, ScalarAttributeType};
use serde_json::{Value, json};

use super::{
    KEY_CONDITION_TYPE_MISMATCH, validate_key_condition_types, validate_query_request_body,
};

/// The request-time number check on typed values, serialized as a request
/// carries them.
fn validate_expression_attribute_value_numbers(
    values: &HashMap<String, AttributeValue>,
) -> Result<(), DynamoDbError> {
    let Value::Object(raw) = serde_json::to_value(values).expect("the values serialize") else {
        panic!("a map serializes to an object");
    };
    super::validate_expression_attribute_value_numbers(&raw)
}

fn defs(pairs: &[(&str, ScalarAttributeType)]) -> Vec<AttributeDefinition> {
    pairs
        .iter()
        .map(|(name, ty)| AttributeDefinition {
            attribute_name: (*name).to_owned(),
            attribute_type: *ty,
        })
        .collect()
}

fn parse(kce: &str) -> KeyCondition {
    let tokens = tokenize_with_limit(kce, 1000).expect("tokenize");
    parse_key_condition(&tokens).expect("parse")
}

fn maps(names: &[(&str, &str)], values: &[(&str, AttributeValue)]) -> ExpressionMaps {
    ExpressionMaps::new(
        names
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        values
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    )
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

pub(super) fn message(result: Result<(), DynamoDbError>) -> Option<String> {
    match result {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(msg)) => Some(msg),
        Err(other) => panic!("unexpected error {other:?}"),
    }
}

const SS: &[(&str, ScalarAttributeType)] = &[
    ("pk", ScalarAttributeType::S),
    ("sk", ScalarAttributeType::S),
];

#[test]
fn partition_value_of_another_type_is_refused() {
    let kc = parse("pk = :u");
    for wrong in [
        n("5"),
        AttributeValue::B(vec![1]),
        AttributeValue::Bool(true),
        AttributeValue::Null,
    ] {
        let m = maps(&[], &[("u", wrong)]);
        assert_eq!(
            message(validate_key_condition_types(&kc, &m, &defs(SS))).as_deref(),
            Some(KEY_CONDITION_TYPE_MISMATCH)
        );
    }
}

#[test]
fn matching_types_pass() {
    let kc = parse("pk = :u AND sk BETWEEN :a AND :b");
    let m = maps(&[], &[("u", s("x")), ("a", s("a")), ("b", s("b"))]);
    assert!(validate_key_condition_types(&kc, &m, &defs(SS)).is_ok());

    let kc = parse("pk = :u AND begins_with(sk, :p)");
    let m = maps(&[], &[("u", s("x")), ("p", s("a"))]);
    assert!(validate_key_condition_types(&kc, &m, &defs(SS)).is_ok());

    let n_sk = defs(&[
        ("pk", ScalarAttributeType::S),
        ("sk", ScalarAttributeType::N),
    ]);
    let kc = parse("pk = :u AND sk > :v");
    let m = maps(&[], &[("u", s("x")), ("v", n("5"))]);
    assert!(validate_key_condition_types(&kc, &m, &n_sk).is_ok());
}

#[test]
fn sort_key_value_of_another_type_is_refused_for_every_operator() {
    for kce in [
        "pk = :u AND sk = :v",
        "pk = :u AND sk < :v",
        "pk = :u AND sk <= :v",
        "pk = :u AND sk > :v",
        "pk = :u AND sk >= :v",
        "pk = :u AND :v <= sk",
        "pk = :u AND begins_with(sk, :v)",
    ] {
        let kc = parse(kce);
        let m = maps(&[], &[("u", s("x")), ("v", n("5"))]);
        assert_eq!(
            message(validate_key_condition_types(&kc, &m, &defs(SS))).as_deref(),
            Some(KEY_CONDITION_TYPE_MISMATCH),
            "{kce}"
        );
    }
}

#[test]
fn either_between_bound_of_another_type_is_refused() {
    let kc = parse("pk = :u AND sk BETWEEN :a AND :b");
    for (a, b) in [(n("1"), s("b")), (s("a"), n("2")), (n("1"), n("2"))] {
        let m = maps(&[], &[("u", s("x")), ("a", a), ("b", b)]);
        assert_eq!(
            message(validate_key_condition_types(&kc, &m, &defs(SS))).as_deref(),
            Some(KEY_CONDITION_TYPE_MISMATCH)
        );
    }
}

#[test]
fn string_on_a_number_or_binary_key_is_refused() {
    let n_sk = defs(&[
        ("pk", ScalarAttributeType::S),
        ("sk", ScalarAttributeType::N),
    ]);
    let kc = parse("pk = :u AND sk = :v");
    let m = maps(&[], &[("u", s("x")), ("v", s("5"))]);
    assert_eq!(
        message(validate_key_condition_types(&kc, &m, &n_sk)).as_deref(),
        Some(KEY_CONDITION_TYPE_MISMATCH)
    );

    let b_pk = defs(&[
        ("bpk", ScalarAttributeType::B),
        ("bsk", ScalarAttributeType::B),
    ]);
    let kc = parse("bpk = :u");
    for wrong in [s("x"), n("5")] {
        let m = maps(&[], &[("u", wrong)]);
        assert_eq!(
            message(validate_key_condition_types(&kc, &m, &b_pk)).as_deref(),
            Some(KEY_CONDITION_TYPE_MISMATCH)
        );
    }
}

#[test]
fn name_reference_resolves_to_the_defined_attribute() {
    let kc = parse("#p = :u");
    let m = maps(&[("p", "pk")], &[("u", n("5"))]);
    assert_eq!(
        message(validate_key_condition_types(&kc, &m, &defs(SS))).as_deref(),
        Some(KEY_CONDITION_TYPE_MISMATCH)
    );
    let m = maps(&[("p", "pk")], &[("u", s("x"))]);
    assert!(validate_key_condition_types(&kc, &m, &defs(SS)).is_ok());
}

#[test]
fn index_key_schema_uses_the_index_attribute_definition() {
    // A GSI on gpk (S) with a range key gsk (S) on a table whose base sort key is N.
    let d = defs(&[
        ("pk", ScalarAttributeType::S),
        ("sk", ScalarAttributeType::N),
        ("gpk", ScalarAttributeType::S),
        ("gsk", ScalarAttributeType::S),
    ]);
    let kc = parse("gpk = :g AND gsk = :s");
    let m = maps(&[], &[("g", s("x")), ("s", n("5"))]);
    assert_eq!(
        message(validate_key_condition_types(&kc, &m, &d)).as_deref(),
        Some(KEY_CONDITION_TYPE_MISMATCH)
    );
    let m = maps(&[], &[("g", s("x")), ("s", s("g"))]);
    assert!(validate_key_condition_types(&kc, &m, &d).is_ok());
}

#[test]
fn multipart_hash_conditions_are_checked() {
    let d = defs(&[
        ("g1", ScalarAttributeType::S),
        ("g2", ScalarAttributeType::N),
        ("r", ScalarAttributeType::S),
    ]);
    let mut kc = parse("g1 = :a AND g2 = :b AND r > :c");
    let names = HashMap::new();
    kc.resolve_multipart(&["g1", "g2"], &["r"], &names)
        .expect("resolve");
    assert_eq!(kc.extra_pk_conditions.len(), 1);
    let m = maps(&[], &[("a", s("x")), ("b", s("1")), ("c", s("r"))]);
    assert_eq!(
        message(validate_key_condition_types(&kc, &m, &d)).as_deref(),
        Some(KEY_CONDITION_TYPE_MISMATCH)
    );
    let m = maps(&[], &[("a", s("x")), ("b", n("1")), ("c", s("r"))]);
    assert!(validate_key_condition_types(&kc, &m, &d).is_ok());
}

#[test]
fn undefined_placeholder_and_undefined_attribute_are_left_to_other_checks() {
    let kc = parse("pk = :u AND sk = :zz");
    let m = maps(&[], &[("u", n("5"))]);
    // The partition value is wrong, and :zz is undefined: the service reports
    // the undefined placeholder first, so the type check stands aside.
    assert!(validate_key_condition_types(&kc, &m, &defs(SS)).is_ok());
    let m = maps(&[], &[("u", s("x"))]);
    assert!(validate_key_condition_types(&kc, &m, &defs(SS)).is_ok());

    // No AttributeDefinition for the attribute: not this check's refusal.
    let kc = parse("other = :u");
    let m = maps(&[], &[("u", n("5"))]);
    assert!(validate_key_condition_types(&kc, &m, &defs(SS)).is_ok());
}

#[test]
fn number_values_that_are_not_numbers_are_refused_with_the_bare_message() {
    let cases = [
        (
            "bob",
            "The parameter cannot be converted to a numeric value: bob",
        ),
        (
            "1e",
            "The parameter cannot be converted to a numeric value: 1e",
        ),
        (
            "12abc",
            "The parameter cannot be converted to a numeric value: 12abc",
        ),
        (
            " 5",
            "The parameter cannot be converted to a numeric value:  5",
        ),
        ("", "The parameter cannot be converted to a numeric value: "),
        (
            "1e200",
            "Number overflow. Attempting to store a number with magnitude larger than supported range",
        ),
    ];
    for (raw, expected) in cases {
        let values = HashMap::from([(":u".to_owned(), n(raw))]);
        assert_eq!(
            message(validate_expression_attribute_value_numbers(&values)).as_deref(),
            Some(expected),
            "{raw:?}"
        );
    }
    let values = HashMap::from([(":u".to_owned(), n(&"1".repeat(39)))]);
    assert_eq!(
        message(validate_expression_attribute_value_numbers(&values)).as_deref(),
        Some("Attempting to store more than 38 significant digits in a Number")
    );
}

#[test]
fn number_values_that_are_numbers_and_other_types_pass() {
    let values = HashMap::from([
        (":u".to_owned(), n("5")),
        (":v".to_owned(), n("-1.5e3")),
        (":w".to_owned(), s("bob")),
        (":x".to_owned(), AttributeValue::Bool(true)),
        (":y".to_owned(), AttributeValue::L(vec![n("1"), s("bob")])),
    ]);
    assert!(validate_expression_attribute_value_numbers(&values).is_ok());
}

#[test]
fn every_value_in_the_map_is_checked_whatever_reads_it() {
    // :n is read by nothing in the key condition: checked all the same.
    let values = HashMap::from([(":p".to_owned(), s("x")), (":n".to_owned(), n("bob"))]);
    assert_eq!(
        message(validate_expression_attribute_value_numbers(&values)).as_deref(),
        Some("The parameter cannot be converted to a numeric value: bob")
    );
}

#[test]
fn numbers_nested_in_sets_lists_and_maps_are_checked() {
    let nested = [
        AttributeValue::NS(["1".to_owned(), "bob".to_owned()].into_iter().collect()),
        AttributeValue::L(vec![s("a"), n("bob")]),
        AttributeValue::M([("a".to_owned(), n("bob"))].into_iter().collect()),
        AttributeValue::M(
            [(
                "a".to_owned(),
                AttributeValue::L(vec![AttributeValue::M(
                    [("b".to_owned(), n("bob"))].into_iter().collect(),
                )]),
            )]
            .into_iter()
            .collect(),
        ),
    ];
    for value in nested {
        let values = HashMap::from([(":p".to_owned(), s("x")), (":n".to_owned(), value.clone())]);
        assert_eq!(
            message(validate_expression_attribute_value_numbers(&values)).as_deref(),
            Some("The parameter cannot be converted to a numeric value: bob"),
            "{value:?}"
        );
    }
}

#[test]
fn first_invalid_number_in_placeholder_order_is_reported() {
    let values = HashMap::from([(":b".to_owned(), n("two")), (":a".to_owned(), n("one"))]);
    assert_eq!(
        message(validate_expression_attribute_value_numbers(&values)).as_deref(),
        Some("The parameter cannot be converted to a numeric value: one")
    );
}

#[test]
fn unicode_decimal_digits_count_as_the_digits_they_stand_for() {
    // Measured 2026-10-25: a fullwidth, Arabic-Indic or Devanagari digit is accepted as a
    // key condition, filter or unused value; thirty nine Devanagari ones answer
    // the 38 digit message; `1\u{ff12}` and `1e\u{ff12}` are accepted.
    for raw in [
        "\u{ff11}",
        "\u{661}",
        "\u{967}",
        "1\u{ff12}",
        "1e\u{ff12}",
        "\u{ff17}",
    ] {
        let values = HashMap::from([(":u".to_owned(), n(raw))]);
        assert!(
            validate_expression_attribute_value_numbers(&values).is_ok(),
            "{raw:?}"
        );
    }
    let values = HashMap::from([(":u".to_owned(), n(&"\u{967}".repeat(39)))]);
    assert_eq!(
        message(validate_expression_attribute_value_numbers(&values)).as_deref(),
        Some("Attempting to store more than 38 significant digits in a Number")
    );
    // A fullwidth digit as an NS member.
    let values = HashMap::from([(
        ":u".to_owned(),
        AttributeValue::NS(["\u{ff11}".to_owned()].into_iter().collect()),
    )]);
    assert!(validate_expression_attribute_value_numbers(&values).is_ok());
}

#[test]
fn characters_that_are_not_decimal_digits_are_refused_with_the_text_as_sent() {
    // Measured 2026-10-25: a superscript,
    // a vulgar fraction, a fullwidth minus and a fullwidth full stop are refused
    // and the message echoes the value as the request wrote it.
    for raw in ["\u{b2}", "\u{bd}", "\u{ff0d}1", "1\u{ff0e}5"] {
        let values = HashMap::from([(":u".to_owned(), n(raw))]);
        assert_eq!(
            message(validate_expression_attribute_value_numbers(&values)).as_deref(),
            Some(format!("The parameter cannot be converted to a numeric value: {raw}").as_str()),
            "{raw:?}"
        );
    }
}

#[test]
fn a_refused_value_is_echoed_with_each_decimal_digit_folded_to_ascii() {
    // Measured 2026-11-11: the service folds every decimal digit below U+10000
    // in the echoed value, and a digit from U+104A0 up is refused and echoed
    // as sent.
    for (raw, echo) in [
        ("\u{ff11}bob", "1bob"),
        ("\u{661}\u{662}x", "12x"),
        ("bob\u{ff11}", "bob1"),
        ("\u{967}\u{968}a\u{969}", "12a3"),
        ("\u{ff11}\u{1d7cf}", "1\u{1d7cf}"),
        ("\u{1d7cf}", "\u{1d7cf}"),
        ("\u{104a1}", "\u{104a1}"),
    ] {
        let values = HashMap::from([(":u".to_owned(), n(raw))]);
        assert_eq!(
            message(validate_expression_attribute_value_numbers(&values)).as_deref(),
            Some(format!("The parameter cannot be converted to a numeric value: {echo}").as_str()),
            "{raw:?}"
        );
    }
}

#[test]
fn decimal_digits_from_u_104a0_up_are_not_numbers() {
    // Measured 2026-11-11: digit one of each of the 31 decimal digit runs from
    // U+104A0 up is refused as a filter value, and digit one of the last run
    // below U+10000 is accepted.
    for raw in [
        "\u{104a1}",
        "\u{11067}",
        "\u{1d7cf}",
        "\u{1e951}",
        "\u{1fbf1}",
    ] {
        let values = HashMap::from([(":u".to_owned(), n(raw))]);
        assert_eq!(
            message(validate_expression_attribute_value_numbers(&values)).as_deref(),
            Some(format!("The parameter cannot be converted to a numeric value: {raw}").as_str()),
            "{raw:?}"
        );
    }
    let values = HashMap::from([(":u".to_owned(), n("\u{abf1}"))]);
    assert!(validate_expression_attribute_value_numbers(&values).is_ok());
}

/// A Query body with `values` as its `ExpressionAttributeValues` and `extra`
/// beside them.
pub(super) fn body_with(values: Value, extra: Value) -> Value {
    let mut body = json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": values,
    });
    if let (Value::Object(body), Value::Object(extra)) = (&mut body, extra) {
        body.extend(extra);
    }
    body
}

#[test]
fn a_value_that_does_not_deserialize_does_not_stop_the_number_check_of_another() {
    // Measured 2026-11-11: beside `:u = {"N": "bob"}`, an empty set, a
    // duplicate set member or a NULL of false answered either that value's
    // fault or the numeric fault over ten requests each, and a B that is not
    // base64 answered SerializationException.
    for other in [
        json!({"NS": []}),
        json!({"SS": ["a", "a"]}),
        json!({"NULL": false}),
        json!({"L": [{"NS": []}]}),
    ] {
        let body = body_with(
            json!({":p": {"S": "x"}, ":u": {"N": "bob"}, ":z": other}),
            json!({}),
        );
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            Some("The parameter cannot be converted to a numeric value: bob"),
            "{other}"
        );
    }
    let body = body_with(
        json!({":p": {"S": "x"}, ":u": {"N": "bob"}, ":z": {"B": "!!"}}),
        json!({}),
    );
    assert!(matches!(
        validate_query_request_body(&body),
        Err(DynamoDbError::SerializationException(_))
    ));
}
