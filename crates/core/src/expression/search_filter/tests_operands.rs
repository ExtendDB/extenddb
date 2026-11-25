// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Parenthesised operands, and the operands a function takes.
//!
//! The expected answers were measured on the live service on 2026-11-23,
//! two requests each, against a missing table and an existing vector table.
//! `Ok` stands for a request the service passed to the table lookup.

use std::collections::HashMap;

use super::parse_search_filter;
use super::tests_support::{OK, n, p, s, used_values};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

/// `:v` is a string and `:w` a number, as in the measured requests.
fn assert_measured(cases: &[(&str, String)]) {
    assert_measured_with(&[(":v", s("x")), (":w", n("2"))], cases);
}

fn assert_measured_with(values: &[(&str, AttributeValue)], cases: &[(&str, String)]) {
    let all: HashMap<String, AttributeValue> = values
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(expr, expected)| {
            let values = used_values(expr, &all);
            let actual = match parse_search_filter(
                expr,
                None,
                Some(&values),
                50,
                &crate::limits::LimitsConfig::default(),
            ) {
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

fn syntax(token: &str, near: &str) -> String {
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

fn operand_type(function: &str, code: &str) -> String {
    p(&format!(
        "Incorrect operand type for operator or function; operator or function: {function}, \
         operand type: {code}"
    ))
}

fn not_allowed(function: &str) -> String {
    p(&format!(
        "The function is not allowed in a search condition; function: {function}"
    ))
}

/// size takes a string, binary, set, list or map, and returns a number;
/// begins_with takes strings and binaries. A path may hold any type. Only the
/// existence functions require a path. A function that passes these checks
/// meets the search rules, which allow only the existence functions.
#[test]
fn function_operands_are_checked_by_type() {
    let path = |f: &str| {
        p(&format!(
            "Operator or function requires a document path; operator or function: {f}"
        ))
    };
    assert_measured(&[
        ("size(size(a)) = :w", operand_type("size", "N")),
        ("size(size(size(a))) = :w", operand_type("size", "N")),
        ("a = size(size(b))", operand_type("size", "N")),
        ("size(size(:v)) = :w", operand_type("size", "N")),
        ("begins_with(size(a), :v)", operand_type("begins_with", "N")),
        ("begins_with(a, size(b))", operand_type("begins_with", "N")),
        ("begins_with(size(a), :w)", operand_type("begins_with", "N")),
        (
            "begins_with(size(a), size(a))",
            operand_type("begins_with", "N"),
        ),
        ("begins_with(:w, a)", operand_type("begins_with", "N")),
        (
            "attribute_type(size(a), :w)",
            operand_type("attribute_type", "N"),
        ),
        (
            "attribute_type(a, size(b))",
            operand_type("attribute_type", "N"),
        ),
        (
            "attribute_type(a, b)",
            operand_type(
                "attribute_type",
                "{S,SS,N,NS,B,BS,BOOL,NULL,L,M,HD,DOUBLE,FLOAT,HDS,FS,DOUBLESET,DICT,DECIMAL,\
                 INT,DECIMALSET,INTSET}",
            ),
        ),
        (
            "attribute_type(a, a)",
            p(
                "The first operand must be distinct from the remaining operands for this \
               operator or function; operator: attribute_type, first operand: [a]",
            ),
        ),
        (
            "begins_with(a, a)",
            p(
                "The first operand must be distinct from the remaining operands for this \
               operator or function; operator: begins_with, first operand: [a]",
            ),
        ),
        (
            "attribute_type(size(a), :v)",
            p("Invalid attribute type name found; type: x, valid types: \
               {S,SS,N,NS,B,BS,BOOL,NULL,L,M}"),
        ),
        (
            "size(attribute_exists(a)) = :w",
            p(
                "The function is not allowed to be used this way in an expression; \
               function: attribute_exists",
            ),
        ),
        ("attribute_exists(:v)", path("attribute_exists")),
        ("attribute_exists(size(a))", path("attribute_exists")),
        (
            "attribute_not_exists(size(a))",
            path("attribute_not_exists"),
        ),
        ("attribute_not_exists(:v)", path("attribute_not_exists")),
        ("size(:v) = :w", not_allowed("size")),
        ("begins_with(:v, :v)", not_allowed("begins_with")),
        ("begins_with(:v, a)", not_allowed("begins_with")),
        ("contains(size(a), :v)", not_allowed("contains")),
        ("contains(:w, a)", not_allowed("contains")),
    ]);
    assert_measured_with(
        &[(":t", s("S")), (":v", s("x"))],
        &[
            ("attribute_type(size(a), :t)", not_allowed("attribute_type")),
            ("attribute_type(:v, :t)", not_allowed("attribute_type")),
        ],
    );
    assert_measured_with(
        &[
            (":n", n("2")),
            (":w", n("2")),
            (":t", AttributeValue::Bool(true)),
            (":ss", AttributeValue::SS(["x".to_owned()].into())),
            (":x", s("X")),
        ],
        &[
            ("size(:n) = :w", operand_type("size", "N")),
            ("size(:t) = :w", operand_type("size", "BOOL")),
            ("size(:ss) = :w", not_allowed("size")),
            ("begins_with(a, :t)", operand_type("begins_with", "BOOL")),
            (
                "attribute_type(a, :x)",
                p("Invalid attribute type name found; type: X, valid types: \
                   {S,SS,N,NS,B,BS,BOOL,NULL,L,M}"),
            ),
        ],
    );
}

/// The expression, a name for each use of its key and a value for each use
/// of its key take at most 1,048,576 bytes. The condition below uses `:v`
/// 100 times, sized to each total. Measured on the live service in
/// eu-north-1 on 2026-11-25, each request twice with the same answer:
/// 1,048,576 bytes answer 200 and 1,048,577 the size refusal, with and
/// without the name.
#[test]
fn the_combined_size_is_at_most_one_mebibyte() {
    use std::collections::HashMap;

    use super::parse_search_filter;
    use super::tests_support::{message, p, s};
    use crate::limits::LimitsConfig;

    let uses = 100;
    for (attribute, names) in [
        ("Category", None),
        (
            "#c",
            Some(HashMap::from([("#c".to_owned(), "Category".to_owned())])),
        ),
    ] {
        let expr = format!(
            "Country = :h AND {attribute} IN ({})",
            vec![":v"; uses].join(", ")
        );
        let fixed = expr.len() + if names.is_some() { "Category".len() } else { 0 };
        for (total, served) in [(1_048_576, true), (1_048_577, false)] {
            let each = (total - fixed) / uses;
            let head = total - fixed - uses * each;
            let values = HashMap::from([
                (":h".to_owned(), s(&"U".repeat(head))),
                (":v".to_owned(), s(&"x".repeat(each))),
            ]);
            let got = parse_search_filter(
                &expr,
                names.as_ref(),
                Some(&values),
                50,
                &LimitsConfig::default(),
            );
            if served {
                assert!(got.is_ok(), "{total} {attribute}: {:?}", got.err());
            } else {
                assert_eq!(
                    got.err().map(message),
                    Some(p("Expression size has exceeded the maximum allowed size;")),
                    "{total} {attribute}"
                );
            }
        }
    }
}
