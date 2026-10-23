// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Arn* operator tests. Every expectation here was measured on live IAM with
//! `iam simulate-custom-policy` (2026-10-23): ArnEquals evaluates the same as
//! ArnLike and ArnNotEquals the same as ArnNotLike, `*` and `?` match in every
//! ARN field, matching is case sensitive in every field, and both sides must
//! have the same number of `:`-separated fields.

use super::*;

const V: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/Tother";
const OTHER: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/Other";
const T_STAR: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/T*";
const T_Q: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/T?ther";
const T_EXACT: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/T";
const TABLE_STAR: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/*";
const Z_STAR: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/Z*";

/// Single-valued key present with value `V`; condition over `pattern`.
fn single(op: ConditionOperator, pattern: &str) -> bool {
    let ctx = TestContext::new().with("k", vec![V]);
    evaluate_condition(&cond(op, "k", vec![pattern]), &ctx)
}

/// Single-valued key present with `value`; condition over `pattern`.
fn single_value(op: ConditionOperator, pattern: &str, value: &str) -> bool {
    let ctx = TestContext::new().with("k", vec![value]);
    evaluate_condition(&cond(op, "k", vec![pattern]), &ctx)
}

/// Key absent from the context.
fn absent(op: ConditionOperator) -> bool {
    let ctx = TestContext::new();
    evaluate_condition(&cond(op, "k", vec![T_STAR]), &ctx)
}

/// Multivalued key (`dynamodb:LeadingKeys` in the test context) holding
/// `[V, OTHER]`; `T*` matches only the first.
fn multi(op: ConditionOperator, pattern: &str) -> bool {
    let ctx = TestContext::new().with("dynamodb:LeadingKeys", vec![V, OTHER]);
    evaluate_condition(&cond(op, "dynamodb:LeadingKeys", vec![pattern]), &ctx)
}

fn if_exists(op: ConditionOperator) -> ConditionOperator {
    ConditionOperator::IfExists(Box::new(op))
}

fn for_any(op: ConditionOperator) -> ConditionOperator {
    ConditionOperator::ForAnyValue(Box::new(op))
}

fn for_all(op: ConditionOperator) -> ConditionOperator {
    ConditionOperator::ForAllValues(Box::new(op))
}

use ConditionOperator::{ArnEquals, ArnLike, ArnNotEquals, ArnNotLike};

// --- wildcards in the resource path, key present ---

#[test]
fn arn_equals_star_matches() {
    assert!(single(ArnEquals, T_STAR));
    assert!(single(ArnLike, T_STAR));
}

#[test]
fn arn_equals_question_mark_matches() {
    assert!(single(ArnEquals, T_Q));
    assert!(single(ArnLike, T_Q));
}

#[test]
fn arn_equals_exact_matches_and_other_does_not() {
    assert!(single(ArnEquals, V));
    assert!(!single(ArnEquals, T_EXACT));
}

#[test]
fn arn_not_equals_star_does_not_match() {
    assert!(!single(ArnNotEquals, T_STAR));
    assert!(!single(ArnNotEquals, T_Q));
    assert!(!single(ArnNotEquals, V));
    assert!(!single(ArnNotLike, T_STAR));
}

#[test]
fn arn_not_equals_other_matches() {
    assert!(single(ArnNotEquals, T_EXACT));
    assert!(single(ArnNotLike, T_EXACT));
}

#[test]
fn arn_equals_if_exists_present_uses_wildcards() {
    assert!(single(if_exists(ArnEquals), T_STAR));
    assert!(!single(if_exists(ArnEquals), T_EXACT));
    assert!(!single(if_exists(ArnNotEquals), T_STAR));
    assert!(single(if_exists(ArnNotEquals), T_EXACT));
}

// --- key absent ---

#[test]
fn arn_operators_absent_key() {
    assert!(!absent(ArnEquals));
    assert!(absent(ArnNotEquals));
    assert!(!absent(ArnLike));
    assert!(absent(ArnNotLike));
}

#[test]
fn arn_operators_absent_key_if_exists_all_pass() {
    for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
        assert!(absent(if_exists(op)));
    }
}

#[test]
fn arn_operators_absent_key_for_any_value_fails() {
    for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
        assert!(!absent(for_any(op)));
    }
}

#[test]
fn arn_operators_absent_key_for_all_values_passes() {
    for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
        assert!(absent(for_all(op.clone())));
        assert!(absent(for_all(if_exists(op))));
    }
}

// --- multivalued key [V, OTHER] ---

#[test]
fn arn_operators_bare_on_multivalued_never_match() {
    for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
        assert!(!multi(op, T_STAR));
    }
}

#[test]
fn arn_equals_for_any_value_wildcard() {
    assert!(multi(for_any(ArnEquals), T_STAR));
    assert!(!multi(for_any(ArnEquals), Z_STAR));
    assert!(multi(for_any(if_exists(ArnEquals)), T_STAR));
    assert!(multi(for_any(ArnLike), T_STAR));
}

#[test]
fn arn_equals_for_all_values_wildcard() {
    assert!(!multi(for_all(ArnEquals), T_STAR));
    assert!(multi(for_all(ArnEquals), TABLE_STAR));
    assert!(!multi(for_all(if_exists(ArnEquals)), T_STAR));
    assert!(!multi(for_all(ArnLike), T_STAR));
    assert!(multi(for_all(ArnLike), TABLE_STAR));
}

#[test]
fn arn_not_equals_for_any_value_wildcard() {
    // OTHER does not match T*, so at least one value is "not equal".
    assert!(multi(for_any(ArnNotEquals), T_STAR));
    assert!(multi(for_any(ArnNotEquals), Z_STAR));
    assert!(multi(for_any(if_exists(ArnNotEquals)), T_STAR));
    assert!(multi(for_any(ArnNotLike), T_STAR));
}

#[test]
fn arn_not_equals_for_all_values_wildcard() {
    // V matches T*, so not every value is "not equal".
    assert!(!multi(for_all(ArnNotEquals), T_STAR));
    assert!(!multi(for_all(ArnNotEquals), TABLE_STAR));
    assert!(!multi(for_all(if_exists(ArnNotEquals)), T_STAR));
    assert!(!multi(for_all(ArnNotLike), T_STAR));
}

// --- wildcards in every field ---

#[test]
fn arn_equals_wildcard_in_each_field() {
    let patterns = [
        "arn:*:dynamodb:us-east-1:123456789012:table/Tother",
        "arn:a?s:dynamodb:us-east-1:123456789012:table/Tother",
        "arn:aws:*:us-east-1:123456789012:table/Tother",
        "arn:aws:dynamod?:us-east-1:123456789012:table/Tother",
        "arn:aws:dynamodb:*:123456789012:table/Tother",
        "arn:aws:dynamodb:us-east-?:123456789012:table/Tother",
        "arn:aws:dynamodb:us-east-1:*:table/Tother",
        "arn:aws:dynamodb:us-east-1:12345678901?:table/Tother",
        "arn:aws:dynamodb:us-east-1:123456789012:*/Tother",
        "arn:aws:dynamodb:us-east-1:123456789012:tabl?/Tother",
        TABLE_STAR,
        T_Q,
        "arn:aws:dynamodb:us-east-1:123456789012:*",
        "arn:*:*:*:*:*",
    ];
    for p in patterns {
        assert!(single(ArnEquals, p), "ArnEquals {p}");
        assert!(single(ArnLike, p), "ArnLike {p}");
        assert!(!single(ArnNotEquals, p), "ArnNotEquals {p}");
    }
}

#[test]
fn arn_equals_resource_star_spans_slash_and_colon() {
    let idx = "arn:aws:dynamodb:us-east-1:123456789012:table/T/index/I";
    assert!(single_value(ArnEquals, TABLE_STAR, idx));
    assert!(single_value(
        ArnEquals,
        "arn:aws:dynamodb:us-east-1:123456789012:*",
        idx
    ));
    let extra = "arn:aws:dynamodb:us-east-1:123456789012:table/Tother:extra";
    assert!(single_value(ArnEquals, TABLE_STAR, extra));
    assert!(!single_value(ArnEquals, V, extra));
    let stream = "arn:aws:dynamodb:us-east-1:123456789012:table/T/stream/2026-04-08T08:40:22.123";
    assert!(single_value(
        ArnEquals,
        "arn:aws:dynamodb:us-east-1:123456789012:table/T/stream/*",
        stream
    ));
}

#[test]
fn arn_equals_star_matches_empty_field_question_mark_does_not() {
    let bob = "arn:aws:iam::123456789012:user/bob";
    assert!(single_value(
        ArnEquals,
        "arn:aws:iam::123456789012:user/*",
        bob
    ));
    assert!(single_value(
        ArnEquals,
        "arn:aws:iam:*:123456789012:user/*",
        bob
    ));
    assert!(!single_value(
        ArnEquals,
        "arn:aws:iam:?:123456789012:user/bob",
        bob
    ));
}

// --- field count ---

#[test]
fn arn_equals_field_count_must_agree() {
    // 7-field pattern vs 6-field value.
    assert!(!single(
        ArnEquals,
        "arn:aws:dynamodb:us-east-1:123456789012:table/Tother:extra"
    ));
    // 6-field wildcard pattern vs 5-field value.
    assert!(!single_value(
        ArnEquals,
        "arn:aws:dynamodb:*:*:*",
        "arn:aws:dynamodb:us-east-1:table/T"
    ));
    // 6-field pattern vs value that is not an ARN.
    assert!(!single_value(ArnEquals, TABLE_STAR, "notanarn"));
    assert!(!single_value(ArnEquals, TABLE_STAR, "arn:aws:dynamodb"));
}

/// Matrix rows x-nonarn-value, x-nonarn-pattern, x-partial-arn-value and
/// x-value-fewer-fields-exact: a context value with fewer than six fields
/// matches neither the positive nor the negative operator. ArnNotEquals and
/// ArnNotLike are false here, not true by negation.
#[test]
fn arn_operators_non_arn_value_matches_neither_operator() {
    let values = [
        "notanarn",
        "arn:aws:dynamodb",
        "arn:aws:dynamodb:us-east-1:table/T",
    ];
    for v in values {
        for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
            assert!(!single_value(op.clone(), TABLE_STAR, v), "{op:?} {v}");
            assert!(!single_value(op.clone(), T_EXACT, v), "{op:?} {v}");
            assert!(
                !single_value(op.clone(), "notanarn", v),
                "{op:?} notanarn {v}"
            );
        }
    }
}

// --- case ---

#[test]
fn arn_equals_is_case_sensitive_in_every_field() {
    let patterns = [
        "ARN:aws:dynamodb:us-east-1:123456789012:table/Tother",
        "arn:AWS:dynamodb:us-east-1:123456789012:table/Tother",
        "arn:aws:DynamoDB:us-east-1:123456789012:table/Tother",
        "arn:aws:dynamodb:US-EAST-1:123456789012:table/Tother",
        "arn:aws:dynamodb:us-east-1:123456789012:TABLE/Tother",
        "arn:aws:dynamodb:us-east-1:123456789012:table/tother",
    ];
    for p in patterns {
        assert!(!single(ArnEquals, p), "ArnEquals {p}");
        assert!(!single(ArnLike, p), "ArnLike {p}");
        assert!(single(ArnNotEquals, p), "ArnNotEquals {p}");
    }
}

// --- policy variables ---

#[test]
fn arn_equals_expands_policy_variable_then_matches_wildcards() {
    let bob = "arn:aws:iam::123456789012:user/bob";
    let ctx = TestContext::new()
        .with("k", vec![bob])
        .with("aws:username", vec!["bob"]);
    for pattern in [
        "arn:aws:iam::123456789012:user/${aws:username}",
        "arn:aws:iam::123456789012:user/${aws:username}*",
    ] {
        assert!(evaluate_condition(
            &cond(ArnEquals, "k", vec![pattern]),
            &ctx
        ));
        assert!(evaluate_condition(&cond(ArnLike, "k", vec![pattern]), &ctx));
        assert!(!evaluate_condition(
            &cond(ArnNotEquals, "k", vec![pattern]),
            &ctx
        ));
        assert!(!evaluate_condition(
            &cond(ArnNotLike, "k", vec![pattern]),
            &ctx
        ));
    }
}

// --- several policy values ---

#[test]
fn arn_equals_any_of_several_wildcard_values() {
    let ctx = TestContext::new().with("k", vec![V]);
    assert!(evaluate_condition(
        &cond(ArnEquals, "k", vec![Z_STAR, T_STAR]),
        &ctx
    ));
    assert!(!evaluate_condition(
        &cond(
            ArnEquals,
            "k",
            vec![Z_STAR, "arn:aws:dynamodb:us-east-1:123456789012:table/Y*"]
        ),
        &ctx
    ));
    assert!(!evaluate_condition(
        &cond(ArnNotEquals, "k", vec![Z_STAR, T_STAR]),
        &ctx
    ));
    assert!(evaluate_condition(
        &cond(
            ArnNotEquals,
            "k",
            vec![Z_STAR, "arn:aws:dynamodb:us-east-1:123456789012:table/Y*"]
        ),
        &ctx
    ));
}
