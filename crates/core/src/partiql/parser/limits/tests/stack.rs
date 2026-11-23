// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the stack a statement takes: each nesting form at the
//! length limit, and the deepest statements that parse.

use super::*;

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
            "queries in parentheses",
            keyed(
                "a = ",
                "(SELECT * FROM \"t\" WHERE a = ",
                "123456789012345",
                ")",
                "",
            ),
            deep(),
        ),
        (
            "queries in IN lists",
            keyed(
                "a IN ",
                "[SELECT a FROM \"t\" WHERE a IN ",
                "[12345678901234567890]",
                "]",
                "",
            ),
            deep(),
        ),
        (
            "DELETEs as values",
            keyed("a = ", "DELETE FROM \"t\" WHERE a = ", "1234567890", "", ""),
            deep(),
        ),
        (
            "DELETEs in IN lists",
            keyed(
                "a IN ",
                "[DELETE FROM \"t\" WHERE a IN ",
                "[1234567890123456789012]",
                "]",
                "",
            ),
            deep(),
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
            "a key in parentheses",
            fill(&format!("{INSERT}{{"), "(", "'k'", ")", ": 1}}"),
            deep(),
        ),
        (
            "maps as keys",
            fill(INSERT, "{", "'k'", ": 1}", "}"),
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
    assert_eq!(answers[1].clone().unwrap_err().message, worst_refusal());
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
    // condition, a value 1000 levels deep: 870 KB on Linux x86_64 (measured
    // 2026-11-06), and about 1.1 MB on macOS arm64, whose debug frames are
    // larger. The server refuses that statement for its projected call
    // before these checks run.
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
