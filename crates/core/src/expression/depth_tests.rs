// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The nesting depth limit of the update and condition parsers: an
//! expression nested past `max_depth` is refused, and one nested far past it
//! is refused on a thread with a small stack instead of overflowing it.

use super::parser::parse_condition_with_depth_limit;
use super::test_timeout::within_10_s;
use super::tokenizer::tokenize_with_limit;
use super::update_parser::parse_update_from;
use crate::error::DynamoDbError;

const LIMIT: usize = 150;

/// Far past the limit: the parse of this many levels took more than the
/// stack below before the parsers checked the depth.
const FAR: usize = 50_000;

/// Half the stack of a server worker thread (2 MiB).
const SMALL_STACK: usize = 1 << 20;

const UPDATE_TOO_DEEP: &str = "Invalid UpdateExpression: expression nesting depth exceeded";
const CONDITION_TOO_DEEP: &str = "Invalid ConditionExpression: expression nesting depth exceeded";

/// `SET qty = (qty + (qty + ... :v))` with `levels` groups.
fn nested_groups(levels: usize) -> String {
    format!(
        "SET qty = {}:v{}",
        "(qty + ".repeat(levels),
        ")".repeat(levels)
    )
}

/// `SET qty = list_append(qty, list_append(qty, ... :v))` with `levels` calls.
fn nested_calls(levels: usize) -> String {
    format!(
        "SET qty = {}:v{}",
        "list_append(qty, ".repeat(levels),
        ")".repeat(levels)
    )
}

/// `SET qty = if_not_exists(qty, (qty + if_not_exists(qty, (... :v))))`,
/// a call and a group per `levels` step.
fn nested_mixed(levels: usize) -> String {
    format!(
        "SET qty = {}:v{}",
        "if_not_exists(qty, (qty + ".repeat(levels),
        "))".repeat(levels)
    )
}

/// `size(size(... qty)) > :v` with `levels` calls.
fn nested_sizes(levels: usize) -> String {
    format!("{}qty{} > :v", "size(".repeat(levels), ")".repeat(levels))
}

fn update(expr: &str) -> Result<(), DynamoDbError> {
    let tokens = tokenize_with_limit(expr, usize::MAX)?;
    parse_update_from(&tokens, expr, LIMIT).map(drop)
}

fn condition(expr: &str) -> Result<(), DynamoDbError> {
    let tokens = tokenize_with_limit(expr, usize::MAX)?;
    parse_condition_with_depth_limit(&tokens, LIMIT).map(drop)
}

fn message(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        other => panic!("expected a ValidationException, got {other:?}"),
    }
}

/// Run `parse` on `expr` on a thread with [`SMALL_STACK`], failing the test
/// after 10 seconds.
fn on_small_stack(
    what: &'static str,
    expr: String,
    parse: fn(&str) -> Result<(), DynamoDbError>,
) -> Result<(), DynamoDbError> {
    within_10_s(what, move || {
        std::thread::Builder::new()
            .stack_size(SMALL_STACK)
            .spawn(move || parse(&expr))
            .expect("spawn the parse thread")
            .join()
            .expect("the parse thread did not panic")
    })
}

#[test]
fn update_groups_nest_to_the_limit_and_no_further() {
    assert!(update(&nested_groups(LIMIT)).is_ok());
    assert_eq!(message(update(&nested_groups(LIMIT + 1))), UPDATE_TOO_DEEP);
}

#[test]
fn update_calls_nest_to_the_limit_and_no_further() {
    assert!(update(&nested_calls(LIMIT)).is_ok());
    assert_eq!(message(update(&nested_calls(LIMIT + 1))), UPDATE_TOO_DEEP);
}

#[test]
fn update_calls_and_groups_count_together() {
    // Each step is a call and a group, two levels.
    assert!(update(&nested_mixed(LIMIT / 2)).is_ok());
    assert_eq!(
        message(update(&nested_mixed(LIMIT / 2 + 1))),
        UPDATE_TOO_DEEP
    );
}

#[test]
fn update_depth_is_counted_per_action() {
    let two = format!(
        "{}, b = {}",
        nested_groups(LIMIT),
        nested_groups(LIMIT).trim_start_matches("SET qty = ")
    );
    assert!(update(&two).is_ok());
}

#[test]
fn update_far_past_the_limit_is_refused_without_overflowing_the_stack() {
    for (what, expr) in [
        ("deep update groups", nested_groups(FAR)),
        ("deep update calls", nested_calls(FAR)),
        ("deep update calls and groups", nested_mixed(FAR)),
    ] {
        assert_eq!(message(on_small_stack(what, expr, update)), UPDATE_TOO_DEEP);
    }
}

#[test]
fn condition_size_calls_nest_to_the_limit_and_no_further() {
    assert!(condition(&nested_sizes(LIMIT)).is_ok());
    assert_eq!(
        message(condition(&nested_sizes(LIMIT + 1))),
        CONDITION_TOO_DEEP
    );
}

#[test]
fn condition_size_calls_inside_a_function_count_toward_the_limit() {
    let inside = |levels: usize| {
        format!(
            "contains(qty, {}qty{})",
            "size(".repeat(levels),
            ")".repeat(levels)
        )
    };
    assert!(condition(&inside(LIMIT)).is_ok());
    assert_eq!(message(condition(&inside(LIMIT + 1))), CONDITION_TOO_DEEP);
}

#[test]
fn condition_far_past_the_limit_is_refused_without_overflowing_the_stack() {
    let expr = nested_sizes(FAR);
    assert_eq!(
        message(on_small_stack("deep condition sizes", expr, condition)),
        CONDITION_TOO_DEEP
    );
}
