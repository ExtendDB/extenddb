// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Parenthesised operands.
//!
//! The expected answers were measured on the live service on 2026-11-23,
//! two requests each, against a missing table and an existing vector table.
//! `Ok` stands for a request the service passed to the table lookup.

use std::collections::HashMap;

use super::parse_search_filter;
use super::tests_support::{OK, p, used_values};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

/// `:v` is a string and `:w` a number, as in the measured requests.
pub(super) fn assert_measured(cases: &[(&str, String)]) {
    let all: HashMap<String, AttributeValue> = [
        (":v".to_owned(), AttributeValue::S("x".to_owned())),
        (":w".to_owned(), AttributeValue::N("2".to_owned())),
    ]
    .into();
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(expr, expected)| {
            let values = used_values(expr, &all);
            let actual = match parse_search_filter(expr, None, Some(&values), 50) {
                Ok(_) => OK.to_owned(),
                Err(DynamoDbError::ValidationException(m)) => m,
                Err(other) => format!("{other:?}"),
            };
            (&actual != expected)
                .then(|| format!("{expr:?}: expected {expected:?}, got {actual:?}"))
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

pub(super) fn syntax(token: &str, near: &str) -> String {
    p(&format!(
        "Syntax error; token: \"{token}\", near: \"{near}\""
    ))
}

#[test]
fn an_operand_may_be_parenthesised() {
    let mixed_bounds = p(
        "The BETWEEN operator requires same data type for lower and upper \
                          bounds; lower bound operand: AttributeValue: {S:x}, upper bound \
                          operand: AttributeValue: {N:2}",
    );
    let not_allowed = |f: &str| {
        p(&format!(
            "The function is not allowed in a search condition; function: {f}"
        ))
    };
    assert_measured(&[
        ("(a) = :v", OK.to_owned()),
        ("a = (:v)", OK.to_owned()),
        ("(:v) = a", OK.to_owned()),
        ("NOT (a) = :v", OK.to_owned()),
        ("(a) = :v AND b = :v", OK.to_owned()),
        ("((a) = :v)", OK.to_owned()),
        ("(a = :v) OR (b) = :v", OK.to_owned()),
        ("(a = :v OR b = :v) AND (c) = :v", OK.to_owned()),
        ("(a = (:v))", OK.to_owned()),
        ("(a) IN (:v)", OK.to_owned()),
        ("a IN ((:v), (:w))", OK.to_owned()),
        ("a IN ((:v))", OK.to_owned()),
        ("a BETWEEN (:v) AND :w", mixed_bounds.clone()),
        ("a BETWEEN (:v) AND (:w)", mixed_bounds.clone()),
        ("(a) BETWEEN :v AND :w", mixed_bounds),
        ("attribute_exists((a))", OK.to_owned()),
        ("(attribute_exists(a))", OK.to_owned()),
        ("(size(a)) = :w", not_allowed("size")),
        ("size((a)) = :w", not_allowed("size")),
        ("begins_with((a), (:v))", not_allowed("begins_with")),
    ]);
}

#[test]
fn a_parenthesised_operand_ends_the_operand() {
    assert_measured(&[
        ("(a)", syntax("<EOF>", ")")),
        ("(a).b = :v", syntax(".", ").b")),
        ("(a)[0] = :v", syntax("[", ")[0")),
        ("a.(b) = :v", syntax("(", ".(b")),
        ("(a) = :v)", syntax(")", ":v)")),
        ("(a = :v) = :v", syntax("=", ") = :v")),
        ("() = :v", syntax(")", "() =")),
    ]);
}

/// A group whose whole content is a group is redundant. The `(` of a call and
/// of an IN list does not count as a group.
#[test]
fn only_doubled_groups_are_redundant() {
    let redundant = p("The expression has redundant parentheses;");
    assert_measured(&[
        ("((a)) = :v", redundant.clone()),
        ("(((a))) = :v", redundant.clone()),
        ("a BETWEEN ((:v)) AND :w", redundant.clone()),
        ("attribute_exists(((a)))", redundant),
        ("attribute_exists((a))", OK.to_owned()),
        ("a IN ((:v))", OK.to_owned()),
    ]);
}
