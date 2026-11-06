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

/// `head`, as many `open` as fit in the length limit, `inner`, as many
/// `close`, then `tail`.
fn fill(head: &str, open: &str, inner: &str, close: &str, tail: &str) -> String {
    let times =
        (MAX_STATEMENT_LENGTH - head.len() - inner.len() - tail.len()) / (open.len() + close.len());
    format!(
        "{head}{}{inner}{}{tail}",
        open.repeat(times),
        close.repeat(times)
    )
}

/// What a statement is answered: `Ok(())` when it parses, else the message.
fn answer(sql: &str) -> Result<(), String> {
    parse(sql).map(|_| ()).map_err(|e| e.message)
}

/// Each form a statement can nest in, written out to the length limit, and
/// the answer it gets. The service's answers are in
/// tests/test_partiql_limits.py; where the service fails with an internal
/// error, the parser refuses the statement as nested too deep.
fn nesting_forms() -> Vec<(&'static str, String, Result<(), String>)> {
    const WHERE: &str = "SELECT * FROM \"t\" WHERE ";
    const KEYED: &str = "SELECT * FROM \"t\" WHERE pk = 'p' AND ";
    const KEY: &str = " WHERE pk = 'p'";
    const INSERT: &str = "INSERT INTO \"t\" VALUE {'pk': 'x', 'v': ";
    let deep = || Err(too_deep());
    let refused_at = |column: usize, length: usize| {
        Err(format!("Unexpected path component at 1:{column}:{length}"))
    };
    let not_simple = || Err(format!("{WF}Invalid path component for simple path"));
    let operator = |op: &str| {
        Err(format!(
            "Unsupported operator in Condition Expression. Operator: {op}"
        ))
    };
    // A condition after a key equality, `head` starting it.
    let keyed = |head: &str, open: &str, inner: &str, close: &str, tail: &str| {
        fill(&format!("{KEYED}{head}"), open, inner, close, tail)
    };
    // A SET value, `head` starting it.
    let set = |head: &str, open: &str, inner: &str, close: &str| {
        fill(&format!("UPDATE \"t\" SET {head}"), open, inner, close, KEY)
    };
    vec![
        (
            "brackets",
            keyed("m[", "k[", "k", "]", "] = 1"),
            refused_at(40, 1),
        ),
        (
            "strings in brackets",
            keyed("m[", "'k'[", "'k'", "]", "] = 1"),
            refused_at(40, 3),
        ),
        (
            "quoted names in brackets",
            keyed("m[", "\"k\"[", "\"k\"", "]", "] = 1"),
            refused_at(40, 3),
        ),
        (
            "paths with dots in brackets",
            keyed("m[", "k.j[", "k", "]", "] = 1"),
            refused_at(40, 1),
        ),
        (
            "dots in brackets",
            keyed("m[k", ".j", "", "", "] = 1"),
            refused_at(40, 1),
        ),
        (
            "brackets left open",
            keyed("m[", "k[", "k", "]", " = 1"),
            Err(format!("{WF}Expected RIGHT_BRACKET")),
        ),
        (
            "brackets in a SET target",
            fill(
                "UPDATE \"t\" SET m[",
                "k[",
                "k",
                "]",
                &format!("] = 1{KEY}"),
            ),
            not_simple(),
        ),
        (
            "brackets in a REMOVE target",
            fill("UPDATE \"t\" REMOVE m[", "k[", "k", "]", &format!("]{KEY}")),
            not_simple(),
        ),
        (
            "brackets in a SET value",
            fill(
                "UPDATE \"t\" SET x = m[",
                "k[",
                "k",
                "]",
                &format!("]{KEY}"),
            ),
            refused_at(22, 1),
        ),
        (
            "brackets in the select list",
            fill("SELECT m[", "k[", "k", "]", "] FROM \"t\""),
            refused_at(10, 1),
        ),
        (
            "brackets in ORDER BY",
            fill(
                "SELECT * FROM \"t\" WHERE pk = 'p' ORDER BY m[",
                "k[",
                "k",
                "]",
                "]",
            ),
            Ok(()),
        ),
        (
            "calls in an inner bracket",
            keyed("m[k[", "f(", "x", ")", "]] = 1"),
            deep(),
        ),
        (
            "groups in an inner bracket",
            keyed("m[k[", "(", "x", ")", "]] = 1"),
            deep(),
        ),
        (
            "lists in an inner bracket",
            keyed("m[k[", "[", "1", "]", "]] = 1"),
            deep(),
        ),
        (
            "calls and brackets in an inner bracket",
            keyed("m[k[", "f(j[", "x", "])", "]] = 1"),
            deep(),
        ),
        (
            "comparisons in inner brackets",
            keyed("m[k[", "j[", "x", "] = 1", "]] = 1"),
            refused_at(40, 1),
        ),
        ("groups", fill(WHERE, "(", "a = 1", ")", ""), deep()),
        ("a path in groups", keyed("", "(", "a", ")", " = 1"), deep()),
        (
            "a name in parentheses",
            keyed("a = ", "(", "b", ")", ""),
            deep(),
        ),
        (
            "a number in parentheses",
            keyed("a = ", "(", "1", ")", ""),
            Ok(()),
        ),
        (
            "a string in parentheses",
            keyed("a = ", "(", "'x'", ")", ""),
            deep(),
        ),
        (
            "a negated number in parentheses",
            keyed("a = ", "-(", "1", ")", ""),
            Ok(()),
        ),
        (
            "a SET value in parentheses",
            set("x = ", "(", "a", ")"),
            deep(),
        ),
        (
            "a SET number in parentheses",
            set("x = ", "(", "5", ")"),
            Ok(()),
        ),
        (
            "an INSERT value in parentheses",
            fill(INSERT, "(", "'v'", ")", "}"),
            deep(),
        ),
        (
            "an INSERT number in parentheses",
            fill(INSERT, "(", "5", ")", "}"),
            Ok(()),
        ),
        (
            "list elements in parentheses",
            fill(&format!("{INSERT}["), "(", "1", ")", "]}"),
            Ok(()),
        ),
        (
            "an IN member in parentheses",
            keyed("a IN (", "(", "1", ")", ")"),
            deep(),
        ),
        (
            "lists in parentheses",
            keyed("a IN (", "(1, ", "1", ")", ")"),
            deep(),
        ),
        ("size calls", keyed("", "size(", "a", ")", " = 1"), deep()),
        (
            "calls in a condition function",
            keyed("attribute_exists(", "f(", "a", ")", ")"),
            deep(),
        ),
        ("calls in a SET value", set("x = ", "f(", "a", ")"), deep()),
        (
            "NOTs",
            fill(WHERE, "NOT ", "a = 1", "", ""),
            Err(too_many_operators()),
        ),
        ("NOTs in a SET value", set("x = ", "NOT ", "a", ""), deep()),
        (
            "NOTs in a bare statement",
            fill("", "NOT ", "a", "", ""),
            Err(crate::partiql::parser::EXISTS_OUTSIDE_TRANSACTION.to_owned()),
        ),
        (
            "arithmetic in parentheses",
            set("x = ", "(", "a", " + 1)"),
            deep(),
        ),
        (
            "signs before parentheses",
            set("x = ", "-(", "a", ")"),
            deep(),
        ),
        ("CASE words", keyed("a = ", "CASE ", "b", "", ""), deep()),
        (
            "lists in an INSERT value",
            fill(INSERT, "[", "1", "]", "}"),
            deep(),
        ),
        (
            "maps in an INSERT value",
            fill(INSERT, "{'a': ", "1", "}", "}"),
            deep(),
        ),
        (
            "lists in a condition",
            keyed("a = ", "[", "1", "]", ""),
            deep(),
        ),
        (
            "lists in an IN list",
            keyed("a IN ", "[", "1", "]", ""),
            deep(),
        ),
        ("bags", keyed("a = ", "<<", "1", ">>", ""), deep()),
        (
            "comparisons with true",
            keyed("a = 1", " = true", "", "", ""),
            Err(too_many_operators()),
        ),
        ("plus signs", keyed("a = ", "+", "a", "", ""), operator("+")),
        (
            "spaced minus signs",
            keyed("a = ", "- ", "b", "", ""),
            operator("-"),
        ),
    ]
}

#[test]
fn every_nesting_form_at_the_length_limit_parses_on_a_small_stack() {
    // Each statement is parsed on a thread with half the stack of a server
    // worker thread. On a debug build a recursion with no bound overflows
    // it, which fails the test run.
    for (form, sql, want) in nesting_forms() {
        assert!(sql.len() <= MAX_STATEMENT_LENGTH, "{form}");
        assert!(
            sql.len() > MAX_STATEMENT_LENGTH - 8,
            "{form}: {}",
            sql.len()
        );
        let got = std::thread::Builder::new()
            .stack_size(1 << 20)
            .spawn(move || answer(&sql))
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(got, want, "{form}");
    }
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

/// The most deeply nested statements that parse, by the bound on each: the
/// operator count for a condition's tree, the operand cap for calls, the
/// literal nesting rule for a value, and the literal cap for the condition
/// of a SELECT with a call in its select list, which the parser leaves to
/// the engine (see `walk`).
fn deepest_that_parse() -> Vec<String> {
    vec![
        nots(MAX_OPERATORS - 1),
        format!(
            "{SELECT}{}a = 1{}",
            "(a = 1 AND ".repeat(149),
            ")".repeat(149)
        ),
        format!(
            "{SELECT}{}a = 1{}",
            "NOT (a = 1 AND ".repeat(99),
            ")".repeat(99)
        ),
        format!("{SELECT}a = 1{}", " = true".repeat(MAX_OPERATORS - 1)),
        format!(
            "{SELECT}{}a{} = 1",
            "size(".repeat(MAX_OPERAND_DEPTH - 1),
            ")".repeat(MAX_OPERAND_DEPTH - 1)
        ),
        format!(
            "{SELECT}x = {}1{}",
            "[".repeat(MAX_LITERAL_NESTING - 1),
            "]".repeat(MAX_LITERAL_NESTING - 1)
        ),
        calls(MAX_OPERAND_DEPTH - 1),
        format!(
            "SELECT COUNT(*) FROM \"t\" WHERE x = {}1{}",
            "[".repeat(MAX_LITERAL_DEPTH),
            "]".repeat(MAX_LITERAL_DEPTH)
        ),
    ]
}

#[test]
fn the_deepest_statements_that_parse_are_walked_on_a_small_stack() {
    use crate::partiql::number::check_literal_numbers;
    use crate::partiql::operand_types::check_ordering_operands;
    use crate::partiql::plan::{
        KeyNames, WhereAnalysis, condition_key_path, keyed_condition, path_tested_against_values,
    };
    // The checks and the plan walk a statement's tree recursively, and the
    // parser's bounds keep each walk and the drop of the tree within half
    // the stack of a server worker thread on a debug build (the most, 750
    // KB, for the number check of a literal 1000 levels deep, measured
    // 2026-11-06).
    let statements: Vec<Statement> = deepest_that_parse()
        .iter()
        .map(|sql| parse(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}")))
        .collect();
    std::thread::Builder::new()
        .stack_size(1 << 20)
        .spawn(move || {
            let keys = KeyNames {
                partition: "pk",
                sort: Some("sk"),
            };
            for statement in statements {
                let _ = statement.parameter_count();
                let _ = check_literal_numbers(&statement);
                let _ = check_ordering_operands(&statement, &[]);
                let condition = match &statement {
                    Statement::Select(select) => select.where_clause.as_ref(),
                    Statement::Update(update) => update.where_clause.as_ref(),
                    _ => None,
                };
                if let Some(condition) = condition {
                    let analysis = WhereAnalysis::new(condition, keys, &|_| None);
                    let _ = analysis.path_into_key_refusal(keys);
                    let _ = keyed_condition(condition, keys);
                    let _ = path_tested_against_values(condition, keys);
                    let _ = condition_key_path(&[condition], keys);
                }
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
