// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the limits on a statement's size and shape.

use super::*;
use crate::partiql::ast::{Operand, WhereExpr};
use crate::partiql::parse;
use crate::partiql::parser::parse_with_full_caps;
use crate::types::AttributeValue;

const WF: &str = "Statement wasn't well formed, can't be processed: ";
const SELECT: &str = "SELECT * FROM \"t\" WHERE ";

fn err(sql: &str) -> String {
    parse(sql).unwrap_err().message
}

fn too_deep() -> String {
    format!("{WF}{TOO_DEEP}")
}

fn groups(n: usize) -> String {
    format!("{SELECT}{}a = 1{}", "(".repeat(n), ")".repeat(n))
}

fn nots(n: usize) -> String {
    format!("{SELECT}{}a = 1", "NOT ".repeat(n))
}

fn chain(n: usize, joiner: &str) -> String {
    let terms: Vec<String> = (0..n).map(|i| format!("a{i} = 1")).collect();
    format!("{SELECT}{}", terms.join(joiner))
}

fn list(n: usize) -> String {
    format!(
        "INSERT INTO \"t\" VALUE {{'pk': 'x', 'l': {}1{}}}",
        "[".repeat(n),
        "]".repeat(n)
    )
}

fn map(n: usize) -> String {
    format!(
        "INSERT INTO \"t\" VALUE {{'pk': 'x', 'm': {}1{}}}",
        "{'a': ".repeat(n),
        "}".repeat(n)
    )
}

fn calls(n: usize) -> String {
    format!(
        "UPDATE \"t\" SET a = {}x{} WHERE pk = 'p'",
        "f(".repeat(n),
        ")".repeat(n)
    )
}

fn nesting(step: &str) -> String {
    literal_nesting_refusal(&format!("root{}", step.repeat(MAX_LITERAL_NESTING)))
}

#[test]
fn a_statement_of_8192_characters_is_not_too_long() {
    let head = format!("{SELECT}pk = 'a'");
    let at_limit = format!("{head}{}", " ".repeat(MAX_STATEMENT_LENGTH - head.len()));
    assert_eq!(statement_length_refusal(&at_limit), None);
    let over = format!("{at_limit} ");
    assert_eq!(
        statement_length_refusal(&over),
        Some(format!(
            "1 validation error detected: Value '{over}' at 'statement' failed to \
             satisfy constraint: Member must have length less than or equal to 8192"
        ))
    );
}

#[test]
fn a_condition_may_hold_300_operators() {
    // Each NOT and the comparison count.
    assert!(parse(&nots(MAX_OPERATORS - 1)).is_ok());
    assert_eq!(err(&nots(MAX_OPERATORS)), too_many_operators());
    assert_eq!(
        too_many_operators(),
        "The expression contains too many operators; operator count: 301"
    );
    // Each AND or OR and each comparison count: 150 terms are 299.
    for joiner in [" AND ", " OR "] {
        assert!(parse(&chain(150, joiner)).is_ok(), "{joiner}");
        assert_eq!(err(&chain(151, joiner)), too_many_operators(), "{joiner}");
        assert_eq!(err(&chain(299, joiner)), too_many_operators(), "{joiner}");
    }
    // A function counts like an operator.
    let functions: Vec<&str> = vec!["attribute_exists(a)"; 151];
    assert_eq!(
        err(&format!("{SELECT}{}", functions.join(" AND "))),
        too_many_operators()
    );
}

#[test]
fn a_run_of_nots_keeps_its_parity() {
    let negations = |sql: &str| {
        let Ok(crate::partiql::Statement::Select(s)) = parse(sql) else {
            panic!("{sql}");
        };
        let mut depth = 0;
        let mut expr = s.where_clause.unwrap();
        while let WhereExpr::Not(inner) = expr {
            depth += 1;
            expr = *inner;
        }
        assert!(matches!(expr, WhereExpr::Compare(..)), "{sql}");
        depth
    };
    assert_eq!(negations(&nots(299)), 299);
    assert_eq!(negations(&nots(2)), 2);
}

#[test]
fn a_long_condition_is_refused_without_overflowing() {
    // 8,189 and about 7,700 characters.
    assert_eq!(err(&nots(2040)), too_many_operators());
    assert_eq!(err(&chain(600, " AND ")), too_many_operators());
    assert_eq!(err(&chain(600, " OR ")), too_many_operators());
}

#[test]
fn a_condition_may_nest_1000_groups() {
    assert!(parse(&groups(MAX_GROUP_DEPTH)).is_ok());
    assert_eq!(err(&groups(MAX_GROUP_DEPTH + 1)), too_deep());
    // 8,029 characters.
    assert_eq!(err(&groups(4000)), too_deep());
    // A syntax error before the cap is reached wins.
    assert_eq!(
        err(&format!("{SELECT}{}a ={}", "(".repeat(10), ")".repeat(10))),
        format!("{WF}Unexpected term")
    );
}

#[test]
fn signs_before_a_name_are_refused_naming_the_operator() {
    let refusal = "Unsupported operator in Condition Expression. Operator: +";
    for n in [1, 4000, 8150] {
        assert_eq!(
            err(&format!("{SELECT}a = {}a", "+".repeat(n))),
            refusal,
            "{n}"
        );
    }
    // The sign next to the operand is named.
    assert_eq!(
        err(&format!("{SELECT}a = + - + -a")),
        "Unsupported operator in Condition Expression. Operator: -"
    );
    assert_eq!(
        err(&format!(
            "UPDATE \"t\" SET a = {}b WHERE pk = 'p'",
            "+".repeat(4000)
        )),
        "Incorrect number of operands for operator or function; operator or function: +, \
         number of operands: 1"
    );
}

#[test]
fn signs_before_a_number_fold_into_it_however_many() {
    let literal = |sql: &str| match parse(sql) {
        Ok(crate::partiql::Statement::Select(s)) => match s.where_clause {
            Some(WhereExpr::Compare(_, _, Operand::Literal(AttributeValue::N(n)))) => n,
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    };
    assert_eq!(literal(&format!("{SELECT}a = {}1", "+".repeat(8000))), "1");
    assert_eq!(literal(&format!("{SELECT}a = {}1", "- ".repeat(4000))), "1");
    assert_eq!(
        literal(&format!("{SELECT}a = {}1", "- ".repeat(3999))),
        "-1"
    );
}

#[test]
fn two_minus_signs_start_a_comment() {
    for n in [2, 10, 100] {
        for operand in ["1", "b"] {
            assert_eq!(
                err(&format!("{SELECT}a = {}{operand}", "-".repeat(n))),
                format!("{WF}Missing right-hand side expression of infix operator"),
                "{n} {operand}"
            );
        }
    }
}

#[test]
fn a_literal_value_inside_32_containers_is_refused_naming_its_path() {
    assert!(parse(&list(MAX_LITERAL_NESTING - 1)).is_ok());
    assert!(parse(&map(MAX_LITERAL_NESTING - 1)).is_ok());
    for n in [32, 33, 34, 40, 100, 1000] {
        assert_eq!(err(&list(n)), nesting("[0]"), "list {n}");
        assert_eq!(err(&map(n)), nesting(".a"), "map {n}");
    }
    for sql in [
        format!("{SELECT}x = {}1{}", "[".repeat(33), "]".repeat(33)),
        format!("{SELECT}x = {}1{}", "[".repeat(1000), "]".repeat(1000)),
        format!(
            "UPDATE \"t\" SET x = {}1{} WHERE pk = 'u'",
            "[".repeat(33),
            "]".repeat(33)
        ),
    ] {
        assert_eq!(err(&sql), nesting("[0]"), "{sql}");
    }
    // The path names where the deep value sits, and a syntax error wins.
    assert_eq!(
        err(&format!(
            "INSERT INTO \"t\" VALUE {{'pk': 'x', 'v': [1, {{'k': {}1{}}}]}}",
            "[".repeat(30),
            "]".repeat(30)
        )),
        literal_nesting_refusal(&format!("root[1].k{}", "[0]".repeat(30)))
    );
    assert_eq!(
        err(&format!(
            "INSERT INTO \"t\" VALUE {{'pk':'mix','x':{{'a':[{{'b':{}1{}}}]}}}}}}}}",
            "[".repeat(30),
            "]".repeat(30)
        )),
        format!("{WF}Unexpected token after expression")
    );
}

#[test]
fn a_literal_may_nest_1000_containers() {
    assert_eq!(err(&list(MAX_LITERAL_DEPTH + 1)), too_deep());
    assert_eq!(err(&map(MAX_LITERAL_DEPTH + 1)), too_deep());
    // 8,041 characters.
    assert_eq!(err(&list(4000)), too_deep());
    let bags = format!("{SELECT}a = {}1{}", "<<".repeat(2000), ">>".repeat(2000));
    assert_eq!(err(&bags), too_deep());
}

#[test]
fn an_operand_may_nest_100_operands() {
    // The innermost `x` is the hundredth operand.
    assert!(parse(&calls(MAX_OPERAND_DEPTH - 1)).is_ok());
    assert_eq!(err(&calls(MAX_OPERAND_DEPTH)), too_deep());
    assert_eq!(err(&calls(2700)), too_deep());
    assert_eq!(
        err(&format!("{SELECT}a = {}b", "not ".repeat(2000))),
        too_deep()
    );
}

/// The worst parse the caps allow: groups, operands and a literal each
/// at their cap.
fn worst() -> String {
    format!(
        "{SELECT}{}a = {}{}1{}{}{}",
        "(".repeat(MAX_GROUP_DEPTH),
        "size(".repeat(MAX_OPERAND_DEPTH - 1),
        "{'a': ".repeat(MAX_LITERAL_DEPTH),
        "}".repeat(MAX_LITERAL_DEPTH),
        ")".repeat(MAX_OPERAND_DEPTH - 1),
        ")".repeat(MAX_GROUP_DEPTH)
    )
}

#[test]
fn the_worst_parse_fits_half_the_large_stack() {
    let statement = worst();
    let answer = std::thread::Builder::new()
        .stack_size(LARGE_STACK_BYTES / 2)
        .spawn(move || parse_with_full_caps(&statement).map(|_| ()))
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(answer.unwrap_err().message, nesting(".a"));
}

#[test]
fn a_deep_statement_takes_little_of_the_callers_stack() {
    // The inline budget keeps the caller's share to about 310 KB on a
    // debug build; the rest of the parse runs on its own thread.
    let answers = std::thread::Builder::new()
        .stack_size(1 << 20)
        .spawn(|| {
            [
                parse(&groups(MAX_GROUP_DEPTH)).map(|_| ()),
                parse(&worst()).map(|_| ()),
            ]
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(answers[0], Ok(()));
    assert_eq!(answers[1].clone().unwrap_err().message, nesting(".a"));
}
