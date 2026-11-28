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
const KEYED: &str = "SELECT * FROM \"t\" WHERE pk = 'q' AND ";
const KEY: &str = " WHERE pk = 'q'";

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
fn a_condition_may_nest_2000_groups() {
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
fn an_operand_may_nest_500_operands() {
    // The innermost `x` is the five hundredth operand. Past 300 calls a SET
    // value is refused for its operators once it is read.
    assert_eq!(err(&calls(MAX_OPERAND_DEPTH - 1)), too_many_operators());
    assert_eq!(err(&calls(MAX_OPERAND_DEPTH)), too_deep());
    assert_eq!(err(&calls(2700)), too_deep());
}

/// The deepest statement found within the length limit: groups round a
/// condition, round a bracket in a bracket, round calls with a bracket in
/// their argument, round nested `FROM ... WHERE` forms, round lists (the
/// module comment of `limits.rs` gives the stack it uses).
fn worst() -> String {
    format!(
        "{SELECT}{}m[k[{}a = {}b = {}1{}{}]] = 1{}",
        "(".repeat(MAX_GROUP_DEPTH),
        "f(j[".repeat(131),
        "FROM a WHERE ".repeat(104),
        "[".repeat(MAX_LITERAL_DEPTH),
        "]".repeat(MAX_LITERAL_DEPTH),
        "])".repeat(131),
        ")".repeat(MAX_GROUP_DEPTH)
    )
}

#[test]
fn the_worst_parse_fits_half_the_large_stack() {
    let statement = worst();
    assert!(
        statement.len() <= MAX_STATEMENT_LENGTH,
        "{}",
        statement.len()
    );
    let answer = std::thread::Builder::new()
        .stack_size(LARGE_STACK_BYTES / 2)
        .spawn(move || parse_with_full_caps(&statement).map(|_| ()))
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(
        answer.unwrap_err().message,
        format!("{WF}Expected data manipulation")
    );
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
        (
            "NOTs in a SET value",
            set("x = ", "NOT ", "a", ""),
            Err(format!("{WF}Unexpected operator")),
        ),
        (
            "NOTs in a bare statement",
            fill("", "NOT ", "a", "", ""),
            Err(crate::partiql::parser::EXISTS_OUTSIDE_TRANSACTION.to_owned()),
        ),
        // 1,359 `+`, past the operators the SET values may hold.
        (
            "arithmetic in parentheses",
            set("x = ", "(", "a", " + 1)"),
            Err(too_many_operators()),
        ),
        (
            "signs before parentheses",
            set("x = ", "-(", "a", ")"),
            deep(),
        ),
        (
            "CASE words",
            keyed("a = ", "CASE ", "b", "", ""),
            Err(format!("{WF}Unexpected keyword")),
        ),
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

/// The depths the service was measured at, with the answer each statement
/// gets: `Ok(())` where it parses. Measured 2026-11-24 on a table that does
/// not exist, each statement twice: the service read every statement here
/// that parses (it answered for the table), except that it failed with an
/// internal error once each 1500 and 1800 levels of parentheses round a
/// name, 1500 round a path and 1500 round an INSERT value; it failed 700
/// nested `size` calls and 2000 nested bags, which the parser refuses as
/// nested past the caps, and it refused 301 and 440 nested `if_not_exists`
/// calls for their operators, as the parser does. Where the service refused
/// a statement for another reason (nested `size` calls for the operand
/// type, `NOT` before a SET value, `CASE` words, 1160 nested maps for their
/// nesting), the parser's own answer is given. The rows one group past the
/// cap are the parser's own boundary, where the service is not measured.
fn measured_depths() -> Vec<(&'static str, String, Result<(), String>)> {
    let deep = || Err(too_deep());
    let parens = |n: usize, inner: &str| format!("{}{inner}{}", "(".repeat(n), ")".repeat(n));
    let set_value = |n: usize, open: &str, inner: &str, close: &str| {
        format!(
            "UPDATE \"t\" SET x = {}{inner}{}{KEY}",
            open.repeat(n),
            close.repeat(n)
        )
    };
    let mut rows = Vec::new();
    for n in [99, 100, 101, 1000, 1001, 1200, 1500, 1800, MAX_GROUP_DEPTH] {
        rows.push((
            "a name in parentheses",
            format!("{KEYED}a = {}", parens(n, "b")),
            Ok(()),
        ));
    }
    rows.push((
        "a name in parentheses",
        format!("{KEYED}a = {}", parens(MAX_GROUP_DEPTH + 1, "b")),
        deep(),
    ));
    for n in [100, 1001] {
        rows.push((
            "a string in parentheses",
            format!("{KEYED}a = {}", parens(n, "'x'")),
            Ok(()),
        ));
        rows.push((
            "a SET value in parentheses",
            format!("UPDATE \"t\" SET x = {}{KEY}", parens(n, "a")),
            Ok(()),
        ));
    }
    for n in [1000, 1001, 1500, MAX_GROUP_DEPTH] {
        rows.push((
            "a path in groups",
            format!("{KEYED}{} = 1", parens(n, "a")),
            Ok(()),
        ));
    }
    rows.push((
        "a path in groups",
        format!("{KEYED}{} = 1", parens(MAX_GROUP_DEPTH + 1, "a")),
        deep(),
    ));
    for (n, want) in [
        (1001, Ok(())),
        (1500, Ok(())),
        (MAX_GROUP_DEPTH + 1, deep()),
    ] {
        rows.push((
            "an INSERT value in parentheses",
            format!(
                "INSERT INTO \"t\" VALUE {{'pk': 'q', 'x': {}}}",
                parens(n, "'v'")
            ),
            want,
        ));
    }
    // The outer pair of parentheses is the IN list's.
    for (n, want) in [
        (1002, Ok(())),
        (1500, Ok(())),
        (MAX_GROUP_DEPTH + 2, deep()),
    ] {
        rows.push((
            "an IN member in parentheses",
            format!("{KEYED}a IN {}", parens(n, "1")),
            want,
        ));
    }
    for n in [100, 101, 300] {
        rows.push((
            "nested if_not_exists calls",
            set_value(n, "if_not_exists(x, ", "1", ")"),
            Ok(()),
        ));
        rows.push((
            "nested list_append calls",
            set_value(n, "list_append(", "l", ", [1])"),
            Ok(()),
        ));
    }
    for n in [301, 440] {
        rows.push((
            "nested if_not_exists calls",
            set_value(n, "if_not_exists(x, ", "1", ")"),
            Err(too_many_operators()),
        ));
    }
    // A size call in parentheses is a syntax error at any depth, where the
    // service refuses the operand type.
    for (n, want) in [
        (101, Err(format!("{WF}Expected a comparison operator"))),
        (700, deep()),
    ] {
        rows.push((
            "nested size calls in parentheses",
            format!("{KEYED}{}a{} = 1", "(size(".repeat(n), "))".repeat(n)),
            want,
        ));
    }
    for n in [100, 2031] {
        rows.push((
            "NOTs before a SET value",
            set_value(n, "NOT ", "a", ""),
            Err(format!("{WF}Unexpected operator")),
        ));
    }
    for n in [100, 1626] {
        rows.push((
            "CASE words",
            format!("{KEYED}a = {}b", "CASE ".repeat(n)),
            Err(format!("{WF}Unexpected keyword")),
        ));
    }
    // The service reads 1160 nested maps, the most the length limit holds,
    // and refuses them for their nesting; the literal cap is 1000.
    rows.push((
        "maps in an INSERT value",
        format!(
            "INSERT INTO \"t\" VALUE {{'pk': 'q', 'x': {}1{}}}",
            "{'a': ".repeat(1160),
            "}".repeat(1160)
        ),
        deep(),
    ));
    rows.push((
        "bags",
        format!("{KEYED}a = {}1{}", "<<".repeat(2000), ">>".repeat(2000)),
        deep(),
    ));
    rows
}

#[test]
fn every_measured_depth_gets_its_answer() {
    for (form, sql, want) in measured_depths() {
        assert!(sql.len() <= MAX_STATEMENT_LENGTH, "{form}: {}", sql.len());
        assert_eq!(answer(&sql), want, "{form}: {}", sql.len());
    }
}

#[test]
fn a_deep_statement_takes_little_of_the_callers_stack() {
    // The inline budget keeps the caller's share within 700 KB on a debug
    // build (a chain of `FROM ... WHERE`; 324 KB for these two); the rest of
    // the parse runs on its own thread.
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
    assert_eq!(
        answers[1].clone().unwrap_err().message,
        format!("{WF}Expected data manipulation")
    );
}

/// The most deeply nested statements that parse, by the bound on each: the
/// operator count for a condition's tree, calls in it included, the
/// operator count of the SET values for calls and arithmetic in a value,
/// the literal nesting rule for a value, and the literal cap for the
/// condition of a SELECT with a call in its select list, which the parser
/// leaves to the engine (see `walk`).
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
            "size(".repeat(MAX_OPERATORS - 1),
            ")".repeat(MAX_OPERATORS - 1)
        ),
        format!(
            "{SELECT}x = {}1{}",
            "[".repeat(MAX_LITERAL_NESTING - 1),
            "]".repeat(MAX_LITERAL_NESTING - 1)
        ),
        calls(MAX_OPERATORS),
        format!(
            "UPDATE \"t\" SET x = {}a{} WHERE pk = 'p'",
            "(".repeat(MAX_OPERATORS),
            " + 1)".repeat(MAX_OPERATORS)
        ),
        format!(
            "SELECT COUNT(*) FROM \"t\" WHERE x = {}1{}",
            "[".repeat(MAX_LITERAL_DEPTH),
            "]".repeat(MAX_LITERAL_DEPTH)
        ),
    ]
}

#[test]
fn the_deepest_statements_that_parse_are_walked_on_a_small_stack() {
    use crate::partiql::operand_types::check_ordering_operands;
    use crate::partiql::plan::{
        KeyNames, WhereAnalysis, condition_key_path, keyed_condition, path_tested_against_values,
    };
    use crate::partiql::values::{check_condition_values, check_written_values};
    // The checks the server runs on a parsed statement and the plan walk its
    // tree recursively, on the thread that serves the request: a tokio
    // worker, whose stack is tokio's default of 2 MiB. The parser's bounds
    // keep each walk and the drop of the tree within that stack on a debug
    // build. The most is for the literal checks of the last statement's
    // condition, a value 1000 levels deep: 880 KB on Linux x86_64 (measured
    // 2026-11-24 with mincore; 870 KB on 2026-11-06), and about 1.1 MB on
    // macOS arm64, whose debug frames are larger (measured 2026-11-06). The
    // 300 nested calls take 200 KB and the 300 levels of arithmetic 150 KB,
    // and ExecuteStatement takes 983 KB of the request's thread for 300
    // nested `if_not_exists` calls or 300 levels of arithmetic in a SET
    // value, the engine's translation included (measured 2026-11-24). The
    // server refuses the last statement for its projected call before these
    // checks run.
    const WORKER_STACK_BYTES: usize = 2 << 20;
    let statements: Vec<(String, Statement)> = deepest_that_parse()
        .into_iter()
        .map(|sql| {
            let statement = parse(&sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            (sql, statement)
        })
        .collect();
    std::thread::Builder::new()
        .stack_size(WORKER_STACK_BYTES)
        .spawn(move || {
            let keys = KeyNames {
                partition: "pk",
                sort: Some("sk"),
            };
            for (sql, statement) in statements {
                let _ = statement.parameter_count();
                let _ = check_condition_values(&statement);
                let _ = check_ordering_operands(&statement, &[]);
                let _ = check_written_values(&statement, &sql);
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
