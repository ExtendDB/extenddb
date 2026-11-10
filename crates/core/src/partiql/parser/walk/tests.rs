// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the order in which the refusals of a statement are reported.

use crate::partiql::ast::Statement;
use crate::partiql::from_path::NAME_RULE;
use crate::partiql::number::{OVERFLOW_MESSAGE, TOO_MANY_DIGITS_MESSAGE, UNDERFLOW_MESSAGE};
use crate::partiql::parse;
use crate::partiql::parser::limits::{
    literal_nesting_refusal, too_many_in_operands, too_many_operators,
};
use crate::partiql::parser::{
    IN_OPERANDS, IS_LEFT_SIDE, PARAMETER_COUNT_MISMATCH, parse_for_request,
};

const WF: &str = "Statement wasn't well formed, can't be processed: ";
const SELECT: &str = "SELECT * FROM \"tbl\" WHERE ";
const PLUS: &str = "Unsupported operator in Condition Expression. Operator: +";
const MISSING_VALUE: &str = "MISSING must be compared using IS operator";

fn err(sql: &str) -> String {
    parse(sql).unwrap_err().message
}

fn refusal(sql: &str, given: usize) -> String {
    parse_for_request(sql, given).unwrap_err().message
}

fn many() -> String {
    "NOT ".repeat(300) + "a = 1"
}

fn overflow() -> String {
    format!("{OVERFLOW_MESSAGE} under root")
}

fn deep() -> String {
    format!("{}1{}", "[".repeat(33), "]".repeat(33))
}

fn nesting() -> String {
    literal_nesting_refusal(&format!("root{}", "[0]".repeat(32)))
}

/// The refusal of the call to `foo` in `sql`.
fn unknown_function(sql: &str) -> String {
    let column = sql.find("foo(").unwrap() + 1;
    format!("Unrecognized function: foo at 1:{column}:3")
}

/// The refusal of the first `?` in `sql`, a parameter inside a list.
fn parameter_in_a_list(sql: &str) -> String {
    let column = sql.find('?').unwrap() + 1;
    format!("Unsupported data type: Parameter under key root[0] at 1:{column}:1")
}

/// Assert the answer for `condition` within the operator limit and past
/// it, for a request with no parameters.
fn assert_walk(condition: &str, expected: impl Fn(&str) -> String) {
    let many = many();
    for sql in [
        format!("{SELECT}{condition}"),
        format!("{SELECT}{condition} AND {many}"),
    ] {
        assert_eq!(refusal(&sql, 0), expected(&sql), "{sql}");
    }
}

#[test]
fn a_number_breaking_a_rule_in_the_condition_comes_before_the_operator_count() {
    let many = many();
    let digits39 = "123456789012345678901234567890123456789";
    for (sql, expected) in [
        (format!("{SELECT}{many} AND b = 1e126"), overflow()),
        (
            format!("{SELECT}{many} AND b = 1e-131"),
            format!("{UNDERFLOW_MESSAGE} under root"),
        ),
        (
            format!("{SELECT}{many} AND b = {digits39}"),
            format!("{TOO_MANY_DIGITS_MESSAGE} under root"),
        ),
        (format!("{SELECT}b = 1e126 AND {many}"), overflow()),
        // The first in the statement, named by its place in the value.
        (
            format!("{SELECT}b = [1, 1e-131] AND c = 1e126 AND {many}"),
            format!("{UNDERFLOW_MESSAGE} under root[1]"),
        ),
        (
            format!("{SELECT}b = {{'k': 1e-131}} AND {many}"),
            format!("{UNDERFLOW_MESSAGE} under root.k"),
        ),
        (
            format!("{SELECT}c = 1e126 AND d = {digits39} AND {many}"),
            overflow(),
        ),
    ] {
        assert_eq!(err(&sql), expected, "{sql}");
    }
    // A number within the rules leaves the operator count.
    assert_eq!(
        err(&format!("{SELECT}b = 9.9E125 AND {many}")),
        too_many_operators()
    );
}

#[test]
fn the_walk_reports_the_first_problem_in_the_condition() {
    let like = "Unsupported operator in Condition Expression. Operator: like";
    let deep = deep();
    for (condition, expected) in [
        ("b = 1e126 AND c = ?".to_owned(), overflow()),
        (
            "c = ? AND b = 1e126".to_owned(),
            PARAMETER_COUNT_MISMATCH.to_owned(),
        ),
        ("b = 1 + 1 AND c = 1e126".to_owned(), PLUS.to_owned()),
        ("c = 1e126 AND b = 1 + 1".to_owned(), overflow()),
        ("c = 1e126 AND foo(b)".to_owned(), overflow()),
        ("c = 1e126 AND b = MISSING".to_owned(), overflow()),
        (
            "b = MISSING AND c = 1e126".to_owned(),
            MISSING_VALUE.to_owned(),
        ),
        ("c = 1e126 AND b = 'x' || 'y'".to_owned(), overflow()),
        ("c = 1e126 AND a LIKE 'x'".to_owned(), overflow()),
        ("a LIKE 'x' AND c = 1e126".to_owned(), like.to_owned()),
        ("c = 1e126 AND a IS INT4".to_owned(), overflow()),
        (format!("c = 1e126 AND b = {deep}"), overflow()),
        (format!("b = {deep} AND c = 1e126"), nesting()),
    ] {
        assert_walk(&condition, |_| expected.clone());
    }
    assert_walk("foo(b) AND c = 1e126", unknown_function);
}

#[test]
fn the_walk_meets_an_operator_before_its_operands() {
    for condition in ["b = 1e126 + 1", "b = 1 + 1e126"] {
        assert_walk(condition, |_| PLUS.to_owned());
    }
    assert_walk("? IS NULL AND c = 1e126", |_| IS_LEFT_SIDE.to_owned());
}

#[test]
fn the_walk_meets_the_arguments_of_a_call_before_its_name() {
    let deep = deep();
    assert_walk("foo(1e126)", |_| overflow());
    assert_walk(&format!("foo({deep})"), |_| nesting());
    assert_walk("foo(1 + 1)", |_| PLUS.to_owned());
    assert_walk("foo([?])", parameter_in_a_list);
    assert_walk("foo(b)", unknown_function);
}

#[test]
fn a_parameter_the_request_does_not_give_is_met_by_the_walk() {
    let many = many();
    for sql in [
        format!("{SELECT}{many} AND b = ?"),
        format!("{SELECT}b = ? AND {many}"),
    ] {
        assert_eq!(refusal(&sql, 0), PARAMETER_COUNT_MISMATCH, "{sql}");
        // Enough parameters, or more, leave the operator count.
        assert_eq!(refusal(&sql, 1), too_many_operators(), "{sql}");
        assert_eq!(refusal(&sql, 2), too_many_operators(), "{sql}");
        // Without the request the count is not known.
        assert_eq!(err(&sql), too_many_operators(), "{sql}");
    }
    let two = format!("{SELECT}b = ? AND {many} AND c = ?");
    assert_eq!(refusal(&two, 1), PARAMETER_COUNT_MISMATCH);
    // The placeholders are numbered in statement order, the SET clause's
    // first.
    let update = format!("UPDATE \"tbl\" SET q = ? WHERE pk = 'a' AND c = ? AND {many}");
    assert_eq!(refusal(&update, 1), PARAMETER_COUNT_MISMATCH);
    assert_eq!(refusal(&update, 2), too_many_operators());
    // Within the operator limit too, in its place in the walk.
    assert_eq!(
        refusal(&format!("{SELECT}c = ? AND b = 1 + 1"), 0),
        PARAMETER_COUNT_MISMATCH
    );
    assert_eq!(refusal(&format!("{SELECT}b = 1 + 1 AND c = ?"), 0), PLUS);
    assert!(parse_for_request(&format!("{SELECT}b = ?"), 1).is_ok());
    // Without the request it is left to the execution.
    assert!(parse(&format!("{SELECT}b = ?")).is_ok());
}

#[test]
fn the_in_list_limit_comes_after_the_walk() {
    let members: Vec<String> = (0..101).map(|i| i.to_string()).collect();
    let list = format!("a IN [{}]", members.join(", "));
    for (condition, expected) in [
        (format!("{list} AND c = 1e126"), overflow()),
        (format!("{list} AND b = 1 + 1"), PLUS.to_owned()),
        (
            format!("{list} AND c = ?"),
            PARAMETER_COUNT_MISMATCH.to_owned(),
        ),
        (list.clone(), too_many_in_operands(101)),
    ] {
        let sql = format!("{SELECT}{condition}");
        assert_eq!(refusal(&sql, 0), expected, "{condition}");
    }
}

#[test]
fn an_unknown_function_or_missing_waits_for_a_syntax_error() {
    for condition in ["foo(b) AND (", "b = MISSING AND ("] {
        assert_eq!(
            err(&format!("{SELECT}{condition}")),
            format!("{WF}Unexpected term"),
            "{condition}"
        );
    }
    // Within the operator limit each keeps its place in the walk.
    assert_eq!(
        err(&format!("{SELECT}b = MISSING AND c = 1 + 1")),
        MISSING_VALUE
    );
    assert_eq!(err(&format!("{SELECT}c = 1 + 1 AND b = MISSING")), PLUS);
    assert_eq!(
        err(&format!("{SELECT}attribute_exists(missing)")),
        MISSING_VALUE
    );
}

#[test]
fn the_refusal_of_is_int4_comes_after_its_left_side() {
    let int4 = |column: usize| {
        format!(
            "Unsupported data type for IS operator: Integer4Type. Supported data types are \
             [MISSING, NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] at 1:{column}:1"
        )
    };
    assert_walk("1e126 IS INT4", |_| overflow());
    assert_walk("a IS INT4 AND c = 1e126", |sql| {
        int4(sql.find("a IS").unwrap() + 1)
    });
    // The other types keep the place of an operator.
    assert_walk("1e126 IS INT8", |sql| {
        let column = sql.find("1e126").unwrap() + 1;
        format!(
            "Unsupported data type for IS operator: Integer8Type. Supported data types are \
             [MISSING, NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] at 1:{column}:5"
        )
    });
}

#[test]
fn a_refusal_that_stops_the_parse_comes_after_those_ahead_of_its_part() {
    assert_eq!(err(&format!("{SELECT}b = 1 + 1 AND a IN 1")), PLUS);
    assert_eq!(err(&format!("{SELECT}c = 1e126 AND a IN 1")), overflow());
    // A SET clause written before the condition comes after it.
    assert_eq!(
        err("UPDATE \"tbl\" SET q = +b WHERE pk = 'a' AND a IN 1"),
        IN_OPERANDS
    );
}

mod statement;
