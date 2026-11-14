// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The condition count limit and the duplicate-attribute message, as measured
//! against the live service on 2026-10-29 in us-east-1 with an index declaring a
//! HASH and 18 inline filters. Each request shape was sent twice with the same
//! response both times.

use super::*;

const TOO_MANY: &str = "Invalid SearchConditionExpression: SearchConditionExpression cannot \
     have more than 1 partition key and more than 18 inline filter key comparisons";

const DUPLICATE: &str = "Invalid SearchConditionExpression: SearchConditionExpression must \
     only contain one condition per attribute";

/// Build `a0 = :v0 AND a1 = :v1 ...` with one placeholder per attribute.
fn expression(attrs: &[&str]) -> (String, HashMap<String, AttributeValue>) {
    let expr = attrs
        .iter()
        .enumerate()
        .map(|(i, a)| format!("{a} = :v{i}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let values = (0..attrs.len())
        .map(|i| (format!(":v{i}"), AttributeValue::S(format!("x{i}"))))
        .collect();
    (expr, values)
}

fn validate(attrs: &[&str]) -> Result<Vec<SearchCondition>, DynamoDbError> {
    let (expr, values) = expression(attrs);
    validate_search_condition_expression(&expr, None, Some(&values))
}

fn message(attrs: &[&str]) -> String {
    match validate(attrs).unwrap_err() {
        DynamoDbError::ValidationException(m) => m,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

/// `h` followed by `f1` to `f18`: the HASH plus the full set of inline filters.
fn nineteen() -> Vec<String> {
    std::iter::once("h".to_owned())
        .chain((1..=18).map(|i| format!("f{i}")))
        .collect()
}

fn refs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

#[test]
fn nineteen_conditions_are_accepted() {
    let attrs = nineteen();
    assert_eq!(validate(&refs(&attrs)).unwrap().len(), 19);
}

#[test]
fn a_twentieth_condition_is_refused_with_the_count_message() {
    let mut attrs = nineteen();
    attrs.push("x".to_owned());
    assert_eq!(message(&refs(&attrs)), TOO_MANY);
}

#[test]
fn twenty_one_conditions_get_the_count_message() {
    let mut attrs = nineteen();
    attrs.extend(["x".to_owned(), "y".to_owned()]);
    assert_eq!(message(&refs(&attrs)), TOO_MANY);
}

/// The 20th condition repeats `f1`. The service refuses it for the count, not
/// for the repeat, so the count is checked before that condition's other checks.
#[test]
fn the_count_is_checked_before_a_repeat_in_the_twentieth_condition() {
    let mut attrs = nineteen();
    attrs.push("f1".to_owned());
    assert_eq!(message(&refs(&attrs)), TOO_MANY);
}

#[test]
fn a_repeat_within_the_limit_gets_the_prefixed_duplicate_message() {
    let mut attrs = nineteen();
    attrs[18] = "f1".to_owned();
    assert_eq!(message(&refs(&attrs)), DUPLICATE);
}

#[test]
fn the_limit_is_one_hash_plus_eighteen_inline_filters() {
    assert_eq!(MAX_PARTITION_KEY_CONDITIONS, 1);
    assert_eq!(MAX_INLINE_FILTER_CONDITIONS, 18);
    assert_eq!(MAX_SEARCH_CONDITIONS, 19);
}
