// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Deeply nested expressions up to the 4096-byte limit.
//!
//! A tokio worker gets a 2 MiB stack, and in a server the handler frames
//! above the parse take part of it. So an expression the checks reject must
//! need no stack in proportion to its depth: each rejected case runs on a
//! thread with a 64 KiB stack, and the deepest accepted condition, whose
//! evaluation still recurses once per level, runs on 2 MiB. In the test
//! profile's debug build a phase that recurses once per nesting level
//! overflows these stacks below the byte limit, which aborts the test
//! process. The expected answers were measured on the live service on
//! 2026-11-23, two requests each.

use std::collections::HashMap;

use super::{DEFAULT_MAX_CONDITIONS, parse_search_filter};
use crate::error::DynamoDbError;
use crate::types::{AttributeValue, Item};

const WORKER_STACK: usize = 2 << 20;
const SMALL_STACK: usize = 64 << 10;
const OK: &str = "Ok";

fn on_stack<T: Send>(stack: usize, f: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(stack)
            .spawn_scoped(scope, f)
            .unwrap_or_else(|e| panic!("spawn: {e}"))
            .join()
            .unwrap_or_else(|_| panic!("the parse panicked"))
    })
}

/// The values an expression names: `:v` is a string, `:w` a number.
fn values_for(expr: &str) -> HashMap<String, AttributeValue> {
    let mut values = HashMap::new();
    if expr.contains(":v") {
        values.insert(":v".to_owned(), AttributeValue::S("x".to_owned()));
    }
    if expr.contains(":w") {
        values.insert(":w".to_owned(), AttributeValue::N("2".to_owned()));
    }
    values
}

/// `Ok`, or the message of the error, parsed on a thread with `stack` bytes.
fn outcome_on(stack: usize, expr: &str) -> String {
    let expr = expr.to_owned();
    on_stack(stack, move || {
        let values = values_for(&expr);
        match parse_search_filter(&expr, None, Some(&values), DEFAULT_MAX_CONDITIONS) {
            Ok(_) => OK.to_owned(),
            Err(DynamoDbError::ValidationException(m)) => m,
            Err(other) => format!("{other:?}"),
        }
    })
}

fn outcome(expr: &str) -> String {
    outcome_on(SMALL_STACK, expr)
}

fn p(body: &str) -> String {
    format!("Invalid SearchConditionExpression: {body}")
}

fn syntax(token: &str, near: &str) -> String {
    p(&format!(
        "Syntax error; token: \"{token}\", near: \"{near}\""
    ))
}

fn operators(count: usize) -> String {
    p(&format!(
        "The expression contains too many operators; operator count: {count}"
    ))
}

fn nested(open: &str, inner: &str, close: &str, depth: usize) -> String {
    format!("{}{inner}{}", open.repeat(depth), close.repeat(depth))
}

fn assert_cases(cases: &[(String, String)]) {
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(expr, expected)| {
            let actual = outcome(expr);
            (&actual != expected).then(|| {
                format!(
                    "{} bytes starting {:?}: expected {expected:?}, got {actual:?}",
                    expr.len(),
                    &expr[..expr.len().min(24)]
                )
            })
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn unclosed_parentheses_answer_a_syntax_error_at_the_end() {
    let eof = syntax("<EOF>", ":v");
    let mut cases: Vec<(String, String)> = [1, 2, 10, 100, 299, 300, 301, 1000, 1500, 4089]
        .into_iter()
        .map(|depth| ("(".repeat(depth) + "a = :v", eof.clone()))
        .collect();
    cases.extend([
        ("(".repeat(4090) + "a = :v", eof.clone()),
        (
            format!("a = :v AND {}b = :v", "(".repeat(2000)),
            eof.clone(),
        ),
        ("NOT (".repeat(800) + "a = :v", eof.clone()),
        ("NOT (".repeat(818) + "a = :v", eof),
        (
            "(".repeat(4091) + "a = :v",
            p("Expression size has exceeded the maximum allowed size;"),
        ),
        ("a = :v".to_owned() + &")".repeat(1500), syntax(")", ":v))")),
    ]);
    assert_eq!(cases[10].0.len(), 4096);
    assert_cases(&cases);
}

#[test]
fn closed_nesting_answers_the_operator_count_or_redundant_parentheses() {
    let redundant = p("The expression has redundant parentheses;");
    let cases = [
        (nested("(", "a = :v", ")", 2), redundant.clone()),
        (nested("(", "a = :v", ")", 1000), redundant.clone()),
        (nested("(", "a = :v", ")", 2045), redundant),
        (nested("(a=:v OR ", "a=:v", ")", 300), operators(601)),
        (nested("(a=:v OR ", "a=:v", ")", 409), operators(819)),
        (nested("NOT(", "a=:v", ")", 300), operators(301)),
        (nested("NOT(", "a=:v", ")", 818), operators(819)),
        ("NOT ".repeat(300) + "a = :v", operators(301)),
        ("NOT ".repeat(1022) + "a = :v", operators(1023)),
        ("NOT ".repeat(1000) + "a = = :v", syntax("=", "= = :v")),
        (
            nested("(a=:v AND ", "b=:u", ")", 350),
            p(
                "An expression attribute value used in expression is not defined; \
               attribute value: :u",
            ),
        ),
    ];
    assert_eq!(cases[2].0.len(), 4096);
    assert_cases(&cases);
}

#[test]
fn deeply_nested_calls_answer_as_the_service_does() {
    let expr = nested("f(", "x", ")", 1365);
    assert_eq!(expr.len(), 4096);
    assert_eq!(outcome(&expr), p("Invalid function name; function: f"));
    let expr = "attribute_exists(".to_owned() + &"size(".repeat(815) + "a";
    assert_eq!(outcome(&expr), syntax("<EOF>", "a"));
}

#[test]
fn parentheses_inside_between_in_and_function_arguments() {
    let cases = [
        (
            "a BETWEEN ".to_owned() + &"(".repeat(2000) + ":v AND :w",
            syntax("AND", ":v AND :w"),
        ),
        (
            "a IN (".to_owned() + &"(".repeat(2000) + ":v)",
            syntax("<EOF>", ")"),
        ),
        (
            "attribute_exists(".to_owned() + &"(".repeat(2000) + "a)",
            syntax("<EOF>", ")"),
        ),
    ];
    assert_cases(&cases);
}

/// A size of a size answers the operand type of the innermost call that
/// takes a number.
#[test]
fn nested_size_calls_answer_the_operand_type() {
    let size_of_n = p(
        "Incorrect operand type for operator or function; operator or \
                       function: size, operand type: N",
    );
    let cases = [
        (
            "a BETWEEN ".to_owned() + &nested("size(", "b", ")", 678) + " AND :w",
            size_of_n.clone(),
        ),
        (
            "a IN (".to_owned() + &nested("size(", "b", ")", 680) + ")",
            size_of_n.clone(),
        ),
        (nested("size(", "a", ")", 681) + " = :v", size_of_n.clone()),
        (nested("size(", "a", ")", 150) + " = :v", size_of_n.clone()),
        (
            "begins_with(".to_owned() + &nested("size(", "a", ")", 600) + ", :v)",
            size_of_n,
        ),
    ];
    assert_cases(&cases);
}

/// 300 operators is the most the grammar takes, so the deepest accepted tree
/// is 300 levels. It parses, evaluates and drops on the worker stack.
#[test]
fn the_deepest_accepted_condition_parses_and_evaluates() {
    let accepted = nested("NOT(", "a=:v", ")", 299);
    assert_eq!(outcome_on(WORKER_STACK, &accepted), OK);
    let matched = on_stack(WORKER_STACK, move || {
        let values = values_for(&accepted);
        let filter = parse_search_filter(&accepted, None, Some(&values), DEFAULT_MAX_CONDITIONS)
            .unwrap_or_else(|e| panic!("{e:?}"));
        let item: Item = Item::from([("a".to_owned(), AttributeValue::S("x".to_owned()))]);
        let matched = filter.matches(&item).unwrap_or_else(|e| panic!("{e:?}"));
        drop(filter);
        matched
    });
    // 299 NOTs over a true comparison.
    assert!(!matched);
    assert_eq!(
        outcome_on(WORKER_STACK, &("NOT ".repeat(299) + "a = :v")),
        OK,
        "299 NOTs and one comparison are 300 operators"
    );
}
