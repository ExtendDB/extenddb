// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the answers for a reserved word where a term or a name starts.

use crate::partiql::{Statement, parse};

const WF: &str = "Statement wasn't well formed, can't be processed: ";

fn wf(reason: &str) -> String {
    format!("{WF}{reason}")
}

/// The parse failure, or the projection refusal a SELECT carries, or
/// "parsed".
fn answer(sql: &str) -> String {
    match parse(sql) {
        Err(e) => e.message,
        Ok(Statement::Select(s)) => s.projection_error.unwrap_or_else(|| "parsed".to_owned()),
        Ok(_) => "parsed".to_owned(),
    }
}

/// A word's answers in the four measured positions: a condition, the
/// argument of attribute_exists, a SET target and a projection.
fn positions(word: &str) -> [String; 4] {
    [
        answer(&format!("SELECT * FROM \"tbl\" WHERE {word} = 1")),
        answer(&format!(
            "SELECT * FROM \"tbl\" WHERE attribute_exists({word})"
        )),
        answer(&format!("UPDATE \"tbl\" SET {word} = 1 WHERE pk = 'a'")),
        answer(&format!("SELECT {word} FROM \"tbl\"")),
    ]
}

fn in_select_list(word: &str) -> String {
    wf(&format!(
        "Expected identifier or an expression but found unexpected keyword '{word}' in a select list."
    ))
}

#[test]
fn a_reserved_word_in_the_four_positions() {
    let keyword = wf("Unexpected keyword");
    let operator = wf("Unexpected operator");
    let term = wf("Unexpected term");
    let simple = wf("Expected identifier for simple path");
    let manipulation = wf("Expected data manipulation");
    let date = wf("Expected date string followed by the keyword DATE, found SYMBOL");
    let time = wf(
        "Expected time string followed by the keyword TIME OR TIME WITH TIME ZONE, found SYMBOL",
    );
    let cases: Vec<(&str, [String; 4])> = vec![
        ("order", [keyword.clone(), keyword.clone(), simple.clone(), keyword.clone()]),
        ("INNER", [keyword.clone(), keyword.clone(), simple.clone(), keyword.clone()]),
        ("count", [keyword.clone(), keyword.clone(), simple.clone(), keyword.clone()]),
        ("insert", [keyword.clone(), keyword.clone(), simple.clone(), keyword.clone()]),
        ("and", [operator.clone(), operator.clone(), simple.clone(), operator.clone()]),
        ("between", [operator.clone(), operator.clone(), simple.clone(), operator.clone()]),
        ("as", [term.clone(), term.clone(), simple.clone(), term.clone()]),
        ("desc", [term.clone(), term.clone(), simple.clone(), term.clone()]),
        ("not", [operator.clone(), term.clone(), simple.clone(), manipulation.clone()]),
        ("case", [operator.clone(), term.clone(), simple.clone(), manipulation.clone()]),
        ("pivot", [operator.clone(), term.clone(), simple.clone(), manipulation.clone()]),
        ("select", [operator.clone(), term.clone(), simple.clone(), in_select_list("select")]),
        ("from", [operator.clone(), term.clone(), simple.clone(), in_select_list("from")]),
        ("all", [keyword.clone(), keyword.clone(), simple.clone(), in_select_list("from")]),
        ("distinct", [keyword.clone(), keyword.clone(), simple.clone(), in_select_list("from")]),
        ("value", [keyword.clone(), keyword.clone(), simple.clone(), manipulation.clone()]),
        ("where", [keyword.clone(), keyword.clone(), simple.clone(), in_select_list("where")]),
        (
            "delete",
            [
                wf("Expected FROM after DELETE"),
                wf("Expected FROM after DELETE"),
                simple.clone(),
                in_select_list("delete"),
            ],
        ),
        ("set", [simple.clone(), simple.clone(), simple.clone(), in_select_list("set")]),
        ("update", [simple.clone(), simple.clone(), simple.clone(), in_select_list("update")]),
        ("remove", [simple.clone(), simple.clone(), simple.clone(), in_select_list("remove")]),
        (
            "exec",
            [
                wf("Keyword = only expected at the top level in the query"),
                wf("Expected RIGHT_PAREN"),
                simple.clone(),
                wf("Expected FROM after SELECT list"),
            ],
        ),
        ("date", [date.clone(), date.clone(), simple.clone(), date.clone()]),
        ("time", [time.clone(), time.clone(), simple.clone(), wf("Expected WITH keyword")]),
        (
            "cast",
            [
                wf("Missing left parenthesis after cast"),
                wf("Missing left parenthesis after cast"),
                simple.clone(),
                wf("Missing left parenthesis after cast"),
            ],
        ),
        (
            "nullif",
            [
                wf("Missing left parenthesis after nullif"),
                wf("Missing left parenthesis after nullif"),
                simple.clone(),
                wf("Missing left parenthesis after nullif"),
            ],
        ),
        (
            "trim",
            [
                wf("Expected LEFT_PAREN"),
                wf("Expected LEFT_PAREN"),
                simple.clone(),
                wf("Expected LEFT_PAREN"),
            ],
        ),
        (
            "values",
            [
                wf("Expected LEFT_PAREN for row value constructor"),
                wf("Expected LEFT_PAREN for row value constructor"),
                simple.clone(),
                wf("Expected LEFT_PAREN for row value constructor"),
            ],
        ),
        (
            "create",
            [
                wf("Unexpected token following CREATE"),
                wf("Unexpected token following CREATE"),
                simple.clone(),
                wf("Unexpected token following CREATE"),
            ],
        ),
        (
            "drop",
            [
                wf("Unexpected token following DROP"),
                wf("Unexpected token following DROP"),
                simple.clone(),
                wf("Unexpected token following DROP"),
            ],
        ),
        (
            "missing",
            [
                "MISSING must be compared using IS operator".to_owned(),
                "MISSING must be compared using IS operator".to_owned(),
                simple.clone(),
                "Unexpected path component at 1:8:7".to_owned(),
            ],
        ),
        (
            "null",
            [
                "parsed".to_owned(),
                "Operator or function requires a document path; operator or function: attribute_exists"
                    .to_owned(),
                simple.clone(),
                "Unexpected path component at 1:8:4".to_owned(),
            ],
        ),
        (
            "false",
            [
                "parsed".to_owned(),
                "Operator or function requires a document path; operator or function: attribute_exists"
                    .to_owned(),
                simple.clone(),
                "Unexpected path component at 1:8:5".to_owned(),
            ],
        ),
    ];
    for (word, expected) in cases {
        assert_eq!(positions(word), expected, "{word}");
    }
}

#[test]
fn a_word_the_service_does_not_reserve_is_a_name_everywhere() {
    for word in ["year", "partition", "leading", "upsert", "timezone_hour"] {
        assert_eq!(
            positions(word),
            ["parsed", "parsed", "parsed", "parsed"],
            "{word}"
        );
    }
}

#[test]
fn a_reserved_word_where_a_from_source_starts() {
    for (word, expected) in [
        ("where", wf("Unexpected keyword")),
        ("count", wf("Unexpected keyword")),
        ("as", wf("Unexpected term")),
        ("and", wf("Unexpected operator")),
        ("set", wf("Expected identifier for simple path")),
        ("select", in_select_list("where")),
        ("null", "Unexpected from source".to_owned()),
    ] {
        assert_eq!(
            answer(&format!("SELECT * FROM {word} WHERE pk = 'p'")),
            expected,
            "{word}"
        );
    }
    for word in [
        "where", "value", "select", "inner", "as", "and", "null", "set",
    ] {
        assert_eq!(
            answer(&format!("DELETE FROM {word} WHERE pk = 'p'")),
            wf("Expected identifier for simple path"),
            "{word}"
        );
    }
}

#[test]
fn the_forms_that_take_an_expression_report_the_next_token() {
    // FROM-led form with a WHERE and no data manipulation.
    assert_eq!(
        answer("SELECT case FROM \"tbl\" WHERE pk = 'p'"),
        wf("Expected data manipulation")
    );
    // EXEC reads to the end of its arguments; the next clause is checked.
    assert_eq!(
        answer("SELECT exec FROM \"tbl\" WHERE pk = 'p'"),
        wf("Expected FROM after SELECT list")
    );
    assert_eq!(
        answer("SELECT * FROM \"tbl\" WHERE pk = 'p' AND exec = 1"),
        wf("Keyword = only expected at the top level in the query")
    );
}

#[test]
fn ion_type_names() {
    use super::ion_type_name;
    assert_eq!(ion_type_name("1"), "INT");
    assert_eq!(ion_type_name("-12"), "INT");
    assert_eq!(ion_type_name("1.5"), "DECIMAL");
    assert_eq!(ion_type_name("1e0"), "FLOAT");
    assert_eq!(ion_type_name("\"a\""), "STRING");
    assert_eq!(ion_type_name("{a: 1}"), "STRUCT");
    assert_eq!(ion_type_name("[1]"), "LIST");
    assert_eq!(ion_type_name("abc"), "SYMBOL");
}
