// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key size limits on a `Query` key condition, driven from the expression
//! text. Each case is a row measured on the service on 2026-10-21 (index with
//! HASH `h` and RANGE `a`, `b`, `c`, `d`), 2026-10-22 (base table with HASH
//! `pk` and RANGE `sk`) or 2026-10-23 (the operator, `BETWEEN`, binary and
//! ordering rows, recorded in the issue's fold file) or 2026-10-23 (the
//! multi-attribute, multi-byte and undefined-reference rows, recorded in
//! `service-435-review.jsonl` and `service-435-440-review-2.jsonl`). The check
//! does not depend on how the key condition is arranged, so it runs on the
//! parser's output without the schema classification step.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{AttributeValue, Condition, KeySchemaElement, KeyType};
use extenddb_core::validation::validate_query_key_sizes;

use crate::expression_helpers::parse_key_condition_expr;
use crate::legacy_filter::desugar_key_conditions;

const HASH_MSG: &str = "One or more parameter values were invalid: \
    Size of hashkey has exceeded the maximum size limit of2048 bytes";
const HASH_TOTAL_MSG: &str = "One or more parameter values were invalid: \
    Aggregated size of all hash keys has exceeded the size limit of 2048 bytes";
const RANGE_MSG: &str = "One or more parameter values were invalid: \
    Aggregated size of all range keys has exceeded the size limit of 1024 bytes";

fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn base_schema() -> Vec<KeySchemaElement> {
    vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)]
}

fn index_schema() -> Vec<KeySchemaElement> {
    vec![
        ks("h", KeyType::Hash),
        ks("a", KeyType::Range),
        ks("b", KeyType::Range),
        ks("c", KeyType::Range),
        ks("d", KeyType::Range),
    ]
}

fn s(len: usize) -> AttributeValue {
    AttributeValue::S("x".repeat(len))
}

/// Parse `expr` and check it against `schema` with the given placeholder
/// values (`:name` keys are given without the colon).
fn run(
    expr: &str,
    schema: &[KeySchemaElement],
    values: &[(&str, AttributeValue)],
) -> Result<(), DynamoDbError> {
    run_with_names(expr, schema, &[], values)
}

/// [`run`] with `ExpressionAttributeNames` (`#name` keys are given without the
/// hash).
fn run_with_names(
    expr: &str,
    schema: &[KeySchemaElement],
    names: &[(&str, &str)],
    values: &[(&str, AttributeValue)],
) -> Result<(), DynamoDbError> {
    let limits = LimitsConfig::default();
    let key_condition = parse_key_condition_expr(expr, &limits).expect("expression parses");
    let values: HashMap<String, AttributeValue> = values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect();
    let names: HashMap<String, String> = names
        .iter()
        .map(|(name, attr)| ((*name).to_owned(), (*attr).to_owned()))
        .collect();
    let maps = ExpressionMaps::new(names, values);
    validate_query_key_sizes(&key_condition, schema, &maps, &limits)
}

fn refused(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn index_hash_value_of_2049_bytes_is_refused() {
    let result = run("h = :big", &index_schema(), &[("big", s(2049))]);
    assert_eq!(refused(result), HASH_MSG);
}

#[test]
fn index_range_value_of_1025_bytes_is_refused() {
    let result = run(
        "h = :h AND a = :big",
        &index_schema(),
        &[("h", s(1)), ("big", s(1025))],
    );
    assert_eq!(refused(result), RANGE_MSG);
}

#[test]
fn index_two_600_byte_range_values_are_refused_together() {
    // The wire row is `TestGsiQueryKeySize::test_two_600_byte_range_values_are_refused_together`.
    // On a build without the multi-attribute range change the handler refuses
    // this shape as `Query condition missed key schema element: h` before it
    // reaches this function, so only the wire row, gated on that change, shows
    // the end-to-end answer.
    let values = [("h", s(1)), ("s", s(600)), ("b", s(1))];
    let result = run(
        "h = :h AND a = :s AND b = :b AND c = :s",
        &index_schema(),
        &values,
    );
    assert_eq!(refused(result), RANGE_MSG);
}

#[test]
fn index_one_600_byte_range_value_is_accepted() {
    // Gated wire row: `TestGsiQueryKeySize::test_two_600_byte_range_values_are_refused_together`
    // (the same shape with the second 600-byte value removed passes on the
    // service). The handler refuses three clauses on one HASH and several
    // RANGE attributes before this function on a build without the
    // multi-attribute range change.
    let values = [("h", s(1)), ("s", s(600)), ("b", s(1))];
    let result = run("h = :h AND a = :s AND b = :b", &index_schema(), &values);
    assert!(result.is_ok());
}

#[test]
fn base_hash_value_of_2048_bytes_is_accepted() {
    assert!(run("pk = :v", &base_schema(), &[("v", s(2048))]).is_ok());
}

#[test]
fn base_hash_value_of_2049_bytes_is_refused() {
    let result = run("pk = :v", &base_schema(), &[("v", s(2049))]);
    assert_eq!(refused(result), HASH_MSG);
}

#[test]
fn base_range_value_of_1024_bytes_is_accepted() {
    let result = run(
        "pk = :p AND sk = :v",
        &base_schema(),
        &[("p", s(1)), ("v", s(1024))],
    );
    assert!(result.is_ok());
}

#[test]
fn base_range_value_of_1025_bytes_is_refused() {
    let result = run(
        "pk = :p AND sk = :v",
        &base_schema(),
        &[("p", s(1)), ("v", s(1025))],
    );
    assert_eq!(refused(result), RANGE_MSG);
}

#[test]
fn base_between_with_two_600_byte_bounds_is_accepted() {
    // The bounds are not added together: 600 and 600 pass.
    let values = [("p", s(1)), ("l", s(600)), ("u", s(600))];
    let result = run("pk = :p AND sk BETWEEN :l AND :u", &base_schema(), &values);
    assert!(result.is_ok());
}

#[test]
fn base_between_with_a_1025_byte_bound_is_refused() {
    let values = [("p", s(1)), ("l", s(1)), ("u", s(1025))];
    let result = run("pk = :p AND sk BETWEEN :l AND :u", &base_schema(), &values);
    assert_eq!(refused(result), RANGE_MSG);
}

#[test]
fn base_non_equality_operators_and_begins_with_are_sized() {
    for expr in [
        "pk = :p AND sk > :v",
        "pk = :p AND sk >= :v",
        "pk = :p AND sk < :v",
        "pk = :p AND sk <= :v",
        "pk = :p AND begins_with(sk, :v)",
    ] {
        let result = run(expr, &base_schema(), &[("p", s(1)), ("v", s(1025))]);
        assert_eq!(refused(result), RANGE_MSG, "{expr}");
        let result = run(expr, &base_schema(), &[("p", s(1)), ("v", s(1024))]);
        assert!(result.is_ok(), "{expr}");
    }
}

#[test]
fn binary_values_are_sized_by_byte_length() {
    let b = |len: usize| AttributeValue::B(vec![1u8; len]);
    assert!(run("pk = :v", &base_schema(), &[("v", b(2048))]).is_ok());
    let result = run("pk = :v", &base_schema(), &[("v", b(2049))]);
    assert_eq!(refused(result), HASH_MSG);
    let values = [("p", b(1)), ("v", b(1024))];
    assert!(run("pk = :p AND sk = :v", &base_schema(), &values).is_ok());
    let values = [("p", b(1)), ("v", b(1025))];
    let result = run("pk = :p AND sk = :v", &base_schema(), &values);
    assert_eq!(refused(result), RANGE_MSG);
}

#[test]
fn both_limits_exceeded_gives_the_hash_sentence() {
    let values = [("p", s(2049)), ("v", s(1025))];
    let result = run("pk = :p AND sk = :v", &base_schema(), &values);
    assert_eq!(refused(result), HASH_MSG);
}

#[test]
fn clause_order_does_not_change_the_result() {
    let result = run(
        "sk = :v AND pk = :p",
        &base_schema(),
        &[("p", s(1)), ("v", s(1025))],
    );
    assert_eq!(refused(result), RANGE_MSG);
    let result = run(
        "sk = :v AND pk = :p",
        &base_schema(),
        &[("p", s(2049)), ("v", s(1))],
    );
    assert_eq!(refused(result), HASH_MSG);
}

#[test]
fn legacy_key_conditions_are_checked_like_the_expression() {
    // Measured 2026-10-23 (rows `6 legacy hash 2049` and `6 legacy range 1025`
    // of `service-435-shapes.jsonl`): the service sizes legacy `KeyConditions`
    // with the same sentences. Both syntaxes reach the check as one key
    // condition.
    let eq = |value: AttributeValue| Condition {
        comparison_operator: "EQ".to_owned(),
        attribute_value_list: vec![value],
    };
    let schema = [("pk".to_owned(), true), ("sk".to_owned(), false)];
    let conditions = HashMap::from([("pk".to_owned(), eq(s(1))), ("sk".to_owned(), eq(s(1025)))]);
    let (kc, maps) = desugar_key_conditions(&conditions, &schema).expect("desugars");
    let result = validate_query_key_sizes(&kc, &base_schema(), &maps, &LimitsConfig::default());
    assert_eq!(refused(result), RANGE_MSG);
}

fn two_hash_schema() -> Vec<KeySchemaElement> {
    vec![
        ks("h1", KeyType::Hash),
        ks("h2", KeyType::Hash),
        ks("r", KeyType::Range),
    ]
}

#[test]
fn two_hash_one_value_over_gets_the_single_sentence() {
    let values = [("a", s(2049)), ("b", s(1))];
    let result = run("h1 = :a AND h2 = :b", &two_hash_schema(), &values);
    assert_eq!(refused(result), HASH_MSG);
}

#[test]
fn two_hash_values_adding_up_over_the_limit_get_the_aggregated_sentence() {
    for (a, b) in [(1025, 1024), (2048, 2048)] {
        let values = [("a", s(a)), ("b", s(b))];
        let result = run("h1 = :a AND h2 = :b", &two_hash_schema(), &values);
        assert_eq!(refused(result), HASH_TOTAL_MSG, "{a} + {b}");
    }
}

#[test]
fn an_undefined_value_in_the_key_condition_wins_over_the_size() {
    // Measured 2026-10-23: the undefined attribute value is reported.
    let result = run(
        "pk = :v AND sk = :missing",
        &base_schema(),
        &[("v", s(2049))],
    );
    assert!(result.is_ok());
}

#[test]
fn an_undefined_name_in_the_key_condition_wins_over_the_size() {
    // Measured 2026-10-23: the undefined attribute name is reported.
    let result = run_with_names(
        "pk = :v AND #n = :w",
        &base_schema(),
        &[],
        &[("v", s(2049)), ("w", s(1))],
    );
    assert!(result.is_ok());
}

#[test]
fn a_defined_name_reference_is_sized() {
    let result = run_with_names("#p = :v", &base_schema(), &[("p", "pk")], &[("v", s(2049))]);
    assert_eq!(refused(result), HASH_MSG);
}

#[test]
fn base_between_with_a_1025_byte_ordered_lower_bound_is_refused() {
    // Measured 2026-10-23: lower bound `a` x 1025 and upper bound `z`.
    let values = [
        ("p", s(1)),
        ("l", AttributeValue::S("a".repeat(1025))),
        ("u", AttributeValue::S("z".to_owned())),
    ];
    let result = run("pk = :p AND sk BETWEEN :l AND :u", &base_schema(), &values);
    assert_eq!(refused(result), RANGE_MSG);
    let values = [
        ("p", s(1)),
        ("l", AttributeValue::S("a".repeat(1024))),
        ("u", AttributeValue::S("z".to_owned())),
    ];
    assert!(run("pk = :p AND sk BETWEEN :l AND :u", &base_schema(), &values).is_ok());
}
