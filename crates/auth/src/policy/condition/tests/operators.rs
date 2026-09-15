// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Basic per-operator condition tests (String*/Numeric*/Bool/Null/Arn*/Date*).

use super::*;

// --- StringEquals ---

#[test]
fn string_equals_match() {
    let ctx = TestContext::new().with("k", vec!["hello"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::StringEquals, "k", vec!["hello"]),
        &ctx
    ));
}

#[test]
fn string_equals_no_match() {
    let ctx = TestContext::new().with("k", vec!["hello"]);
    assert!(!evaluate_condition(
        &cond(ConditionOperator::StringEquals, "k", vec!["world"]),
        &ctx
    ));
}

#[test]
fn string_equals_absent_key() {
    let ctx = TestContext::new();
    assert!(!evaluate_condition(
        &cond(ConditionOperator::StringEquals, "k", vec!["hello"]),
        &ctx
    ));
}

#[test]
fn string_equals_multiple_policy_values() {
    let ctx = TestContext::new().with("k", vec!["b"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::StringEquals, "k", vec!["a", "b", "c"]),
        &ctx
    ));
}

// --- StringLike ---

#[test]
fn string_like_wildcard() {
    let ctx = TestContext::new().with("k", vec!["hello-world"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::StringLike, "k", vec!["hello-*"]),
        &ctx
    ));
}

#[test]
fn string_not_like() {
    let ctx = TestContext::new().with("k", vec!["hello"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::StringNotLike, "k", vec!["world*"]),
        &ctx
    ));
}

// --- StringEqualsIgnoreCase ---

#[test]
fn string_equals_ignore_case() {
    let ctx = TestContext::new().with("k", vec!["Hello"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::StringEqualsIgnoreCase,
            "k",
            vec!["hello"]
        ),
        &ctx
    ));
}

// --- Numeric ---

#[test]
fn numeric_equals() {
    let ctx = TestContext::new().with("k", vec!["42"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::NumericEquals, "k", vec!["42"]),
        &ctx
    ));
}

#[test]
fn numeric_less_than() {
    let ctx = TestContext::new().with("k", vec!["5"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::NumericLessThan, "k", vec!["10"]),
        &ctx
    ));
}

#[test]
fn numeric_greater_than_equals() {
    let ctx = TestContext::new().with("k", vec!["10"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::NumericGreaterThanEquals, "k", vec!["10"]),
        &ctx
    ));
}

#[test]
fn numeric_invalid_parse() {
    let ctx = TestContext::new().with("k", vec!["abc"]);
    assert!(!evaluate_condition(
        &cond(ConditionOperator::NumericEquals, "k", vec!["42"]),
        &ctx
    ));
}

// --- Bool ---

#[test]
fn bool_true() {
    let ctx = TestContext::new().with("k", vec!["true"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::Bool, "k", vec!["true"]),
        &ctx
    ));
}

#[test]
fn bool_case_insensitive() {
    let ctx = TestContext::new().with("k", vec!["True"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::Bool, "k", vec!["true"]),
        &ctx
    ));
}

// --- Null ---

#[test]
fn null_true_absent_key() {
    let ctx = TestContext::new();
    assert!(evaluate_condition(
        &cond(ConditionOperator::Null, "k", vec!["true"]),
        &ctx
    ));
}

#[test]
fn null_true_present_key() {
    let ctx = TestContext::new().with("k", vec!["val"]);
    assert!(!evaluate_condition(
        &cond(ConditionOperator::Null, "k", vec!["true"]),
        &ctx
    ));
}

#[test]
fn null_false_present_key() {
    let ctx = TestContext::new().with("k", vec!["val"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::Null, "k", vec!["false"]),
        &ctx
    ));
}

#[test]
fn null_false_absent_key() {
    let ctx = TestContext::new();
    assert!(!evaluate_condition(
        &cond(ConditionOperator::Null, "k", vec!["false"]),
        &ctx
    ));
}

// --- ArnLike ---

#[test]
fn arn_like_match() {
    let ctx = TestContext::new().with("k", vec!["arn:aws:dynamodb:us-east-1:123:table/Users"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ArnLike,
            "k",
            vec!["arn:aws:dynamodb:*:*:table/User*"]
        ),
        &ctx
    ));
}

#[test]
fn arn_not_like() {
    let ctx = TestContext::new().with("k", vec!["arn:aws:dynamodb:us-east-1:123:table/Orders"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ArnNotLike,
            "k",
            vec!["arn:aws:dynamodb:*:*:table/User*"]
        ),
        &ctx
    ));
}

// --- IfExists ---

#[test]
fn if_exists_absent_key_passes() {
    let ctx = TestContext::new();
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::IfExists(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["val"]
        ),
        &ctx
    ));
}

#[test]
fn if_exists_present_key_evaluates() {
    let ctx = TestContext::new().with("k", vec!["val"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::IfExists(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["val"]
        ),
        &ctx
    ));
}

#[test]
fn if_exists_present_key_fails() {
    let ctx = TestContext::new().with("k", vec!["other"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::IfExists(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["val"]
        ),
        &ctx
    ));
}

// --- ForAllValues ---

#[test]
fn for_all_values_all_match() {
    let ctx = TestContext::new().with("k", vec!["a", "b"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["a", "b", "c"]
        ),
        &ctx
    ));
}

#[test]
fn for_all_values_one_missing() {
    let ctx = TestContext::new().with("k", vec!["a", "d"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["a", "b", "c"]
        ),
        &ctx
    ));
}

#[test]
fn for_all_values_absent_key_vacuously_true() {
    let ctx = TestContext::new();
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["a"]
        ),
        &ctx
    ));
}

// --- ForAnyValue ---

#[test]
fn for_any_value_one_match() {
    let ctx = TestContext::new().with("k", vec!["x", "a"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["a"]
        ),
        &ctx
    ));
}

#[test]
fn for_any_value_no_match() {
    let ctx = TestContext::new().with("k", vec!["x", "y"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["a"]
        ),
        &ctx
    ));
}

#[test]
fn for_any_value_absent_key_false() {
    let ctx = TestContext::new();
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringEquals)),
            "k",
            vec!["a"]
        ),
        &ctx
    ));
}

#[test]
fn for_any_value_if_exists_absent_key_true() {
    let ctx = TestContext::new();
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ForAnyValue(Box::new(ConditionOperator::IfExists(Box::new(
                ConditionOperator::StringEquals
            )))),
            "k",
            vec!["a"]
        ),
        &ctx
    ));
}

// --- Date operators ---

#[test]
fn date_equals() {
    let ctx = TestContext::new().with("k", vec!["2026-01-01T00:00:00Z"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::DateEquals,
            "k",
            vec!["2026-01-01T00:00:00Z"]
        ),
        &ctx
    ));
}

#[test]
fn date_less_than() {
    let ctx = TestContext::new().with("k", vec!["2025-01-01T00:00:00Z"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::DateLessThan,
            "k",
            vec!["2026-01-01T00:00:00Z"]
        ),
        &ctx
    ));
}
