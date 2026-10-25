// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    ExpressionMaps, KeyCondition, parse_key_condition, tokenize_with_limit,
};
use extenddb_core::types::{AttributeDefinition, AttributeValue, ScalarAttributeType};

use super::{
    KEY_CONDITION_TYPE_MISMATCH, LIMIT_BELOW_ONE, validate_expression_attribute_value_numbers,
    validate_key_condition_types, validate_query_request_body,
};

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

fn message(result: Result<(), DynamoDbError>) -> Option<String> {
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
    kc.resolve_multipart(&["g1", "g2"], &names)
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
fn request_time_checks_run_limit_then_mix_then_numbers() {
    let bad_number = serde_json::json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :u",
        "ExpressionAttributeValues": {":u": {"N": "bob"}},
    });

    let mut with_limit = bad_number.clone();
    with_limit["Limit"] = serde_json::json!(0);
    assert_eq!(
        message(validate_query_request_body(&with_limit)).as_deref(),
        Some(LIMIT_BELOW_ONE)
    );

    let mut with_mix = bad_number.clone();
    with_mix["KeyConditions"] = serde_json::json!({
        "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}
    });
    assert_eq!(
        message(validate_query_request_body(&with_mix)).as_deref(),
        Some(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {KeyConditions} Expression parameters: {KeyConditionExpression}"
        )
    );

    assert_eq!(
        message(validate_query_request_body(&bad_number)).as_deref(),
        Some("The parameter cannot be converted to a numeric value: bob")
    );

    // A number only a FilterExpression reads, and one nothing reads.
    for body in [
        serde_json::json!({
            "TableName": "t",
            "KeyConditionExpression": "pk = :p",
            "FilterExpression": "a = :n",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":n": {"N": "bob"}},
        }),
        serde_json::json!({
            "TableName": "t",
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}, ":n": {"N": "bob"}},
        }),
    ] {
        assert_eq!(
            message(validate_query_request_body(&body)).as_deref(),
            Some("The parameter cannot be converted to a numeric value: bob")
        );
    }

    // Legacy KeyConditions alone does not join the mix check; a well-formed
    // request passes.
    let legacy = serde_json::json!({
        "TableName": "t",
        "KeyConditions": {
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]}
        },
        "FilterExpression": "a = :n",
        "ExpressionAttributeValues": {":n": {"N": "5"}},
        "Limit": 1,
    });
    assert!(validate_query_request_body(&legacy).is_ok());
}

#[test]
fn enum_checks_sit_on_either_side_of_the_numbers() {
    // Measured 2026-10-24: an invalid ReturnConsumedCapacity is reported ahead
    // of an N value that is not a number, and that value ahead of an invalid
    // Select; a Limit of 0 and a parameter mix ahead of either enum.
    let bad_number = serde_json::json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :u",
        "ExpressionAttributeValues": {":u": {"N": "bob"}},
    });
    let mut rcc = bad_number.clone();
    rcc["ReturnConsumedCapacity"] = serde_json::json!("bad");
    assert_eq!(
        message(validate_query_request_body(&rcc)).as_deref(),
        Some(
            "1 validation error detected: Value 'bad' at 'returnConsumedCapacity' failed to \
             satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
        )
    );
    let mut select = bad_number.clone();
    select["Select"] = serde_json::json!("bad");
    assert_eq!(
        message(validate_query_request_body(&select)).as_deref(),
        Some("The parameter cannot be converted to a numeric value: bob")
    );
    for field in ["ReturnConsumedCapacity", "Select"] {
        let mut limit = bad_number.clone();
        limit[field] = serde_json::json!("bad");
        limit["Limit"] = serde_json::json!(0);
        assert_eq!(
            message(validate_query_request_body(&limit)).as_deref(),
            Some(LIMIT_BELOW_ONE),
            "{field}"
        );
    }
    // With the numbers valid, an invalid Select is still reported.
    let mut only_select = serde_json::json!({
        "TableName": "t",
        "KeyConditionExpression": "pk = :u",
        "ExpressionAttributeValues": {":u": {"S": "bob"}},
    });
    only_select["Select"] = serde_json::json!("bad");
    assert!(
        message(validate_query_request_body(&only_select))
            .is_some_and(|m| m.contains("at 'select' failed to satisfy constraint"))
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
    for raw in ["\u{b2}", "\u{bd}", "\u{ff0d}1", "1\u{ff0e}5", "\u{ff11}bob"] {
        let values = HashMap::from([(":u".to_owned(), n(raw))]);
        assert_eq!(
            message(validate_expression_attribute_value_numbers(&values)).as_deref(),
            Some(format!("The parameter cannot be converted to a numeric value: {raw}").as_str()),
            "{raw:?}"
        );
    }
}

#[test]
fn a_shape_fault_in_another_field_is_reported_ahead_of_the_checks() {
    // Measured 2026-10-25: a field of the wrong JSON type
    // anywhere in the body answers SerializationException even when a Limit,
    // mix, enum or number check would fail.
    let shape_faults = [
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "Limit": 0, "ExpressionAttributeValues": "x",
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeNames": "x",
            "ExpressionAttributeValues": {":p": {"N": "bob"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "Limit": "1", "ExpressionAttributeValues": {":p": {"N": "bob"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "AttributesToGet": {"a": 1}, "ProjectionExpression": "a",
            "ExpressionAttributeValues": {":p": {"S": "x"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "QueryFilter": [1], "FilterExpression": "a = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "Select": 5, "ExpressionAttributeValues": {":p": {"N": "bob"}},
        }),
        serde_json::json!({
            "TableName": "t", "KeyConditionExpression": "pk = :p",
            "Limit": 0, "Select": 5, "ExpressionAttributeValues": {":p": {"S": "x"}},
        }),
    ];
    for body in shape_faults {
        assert!(
            matches!(
                validate_query_request_body(&body),
                Err(DynamoDbError::SerializationException(_))
            ),
            "{body}"
        );
    }

    // A string enum value that is not a member is the checks' own answer, not a
    // shape fault.
    let mut select = serde_json::json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p",
        "Limit": 0, "ExpressionAttributeValues": {":p": {"S": "x"}},
    });
    select["Select"] = serde_json::json!("bad");
    assert_eq!(
        message(validate_query_request_body(&select)).as_deref(),
        Some(LIMIT_BELOW_ONE)
    );
    select["ReturnConsumedCapacity"] = serde_json::json!("bad");
    select["Limit"] = serde_json::json!(1);
    assert!(
        message(validate_query_request_body(&select))
            .is_some_and(|m| m.contains("at 'returnConsumedCapacity' failed to satisfy constraint"))
    );

    // A ValidationException the deserializer raises does not preempt a check:
    // an empty NS beside a Limit of 0 answers the Limit text.
    let empty_set = serde_json::json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p",
        "Limit": 0, "ExpressionAttributeValues": {":p": {"S": "x"}, ":s": {"NS": []}},
    });
    assert_eq!(
        message(validate_query_request_body(&empty_set)).as_deref(),
        Some(LIMIT_BELOW_ONE)
    );

    // Neither does an unknown type descriptor, which the deserializer reports
    // as a SerializationException: the service answers the Limit text
    // (measured 2026-10-25).
    let unknown_type = serde_json::json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p",
        "Limit": 0, "ExpressionAttributeValues": {":p": {"S": "x"}, ":a": {"X": "1"}},
    });
    assert_eq!(
        message(validate_query_request_body(&unknown_type)).as_deref(),
        Some(LIMIT_BELOW_ONE)
    );

    // A body that passes every check is left to the handler's deserializer.
    let shape_fault_only = serde_json::json!({
        "TableName": "t", "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": "x",
    });
    assert!(validate_query_request_body(&shape_fault_only).is_ok());
}
