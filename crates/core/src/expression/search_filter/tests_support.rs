// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Fixtures shared by the search filter tests. Each test file states where
//! its expected answers were measured.

use std::collections::HashMap;

use super::lexer::{Kind, lex};
use super::{SearchFilter, parse_search_filter};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

pub(super) const OK: &str = "Ok";
pub(super) const PREFIX: &str = "Invalid SearchConditionExpression: ";

pub(super) fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

pub(super) fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

/// The values every grammar test draws from.
pub(super) fn all_values() -> HashMap<String, AttributeValue> {
    [
        (":s", s("US")),
        (":s2", s("CA")),
        (":n", n("5")),
        (":n2", n("10")),
        (":t", AttributeValue::Bool(true)),
        (":null", AttributeValue::Null),
        (":ty", s("S")),
        (":b", AttributeValue::B(b"hi".to_vec())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

pub(super) fn names() -> HashMap<String, String> {
    [("#x", "a"), ("#y", "year")]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

/// The values of `all` that `expr` references, so no test trips the unused
/// value check by accident.
pub(super) fn used_values(
    expr: &str,
    all: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    lex(expr)
        .iter()
        .filter(|t| t.kind == Kind::ValueRef)
        .filter_map(|t| {
            let key = &expr[t.start..t.end];
            all.get(key).map(|v| (key.to_owned(), v.clone()))
        })
        .collect()
}

pub(super) fn parse_with(expr: &str, max: usize) -> Result<SearchFilter, DynamoDbError> {
    let values = used_values(expr, &all_values());
    parse_search_filter(
        expr,
        Some(&names()),
        Some(&values),
        max,
        &crate::limits::LimitsConfig::default(),
    )
}

/// `Ok`, or the message of the error.
pub(super) fn outcome_with(expr: &str, max: usize) -> String {
    match parse_with(expr, max) {
        Ok(_) => OK.to_owned(),
        Err(DynamoDbError::ValidationException(m)) => m,
        Err(other) => format!("{other:?}"),
    }
}

pub(super) fn outcome(expr: &str) -> String {
    outcome_with(expr, super::DEFAULT_MAX_CONDITIONS)
}

/// Assert every `(expression, expected)` pair, reporting all mismatches.
pub(super) fn assert_outcomes<E: AsRef<str>, X: AsRef<str>>(
    max: usize,
    cases: impl IntoIterator<Item = (E, X)>,
) {
    let mismatches: Vec<String> = cases
        .into_iter()
        .filter_map(|(expr, expected)| {
            let (expr, expected) = (expr.as_ref(), expected.as_ref());
            let actual = outcome_with(expr, max);
            (actual != expected).then(|| format!("{expr:?}: expected {expected:?}, got {actual:?}"))
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

/// The message with the expression prefix.
pub(super) fn p(body: &str) -> String {
    format!("{PREFIX}{body}")
}

/// `a0 = :n AND a1 = :n AND ...` with `count` comparisons.
pub(super) fn and_chain(count: usize) -> String {
    (0..count)
        .map(|i| format!("a{i} = :n"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// `a IN (:s, :s, ...)` with `count` values.
pub(super) fn in_list(count: usize) -> String {
    format!("a IN ({})", vec![":s"; count].join(", "))
}

/// The message of a `ValidationException`.
pub(super) fn message(err: DynamoDbError) -> String {
    match err {
        DynamoDbError::ValidationException(m) => m,
        other => format!("{other:?}"),
    }
}
