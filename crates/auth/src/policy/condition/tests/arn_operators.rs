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
/// The ARN member of the sets measured on 2026-10-28.
const TABLE_V: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/V";
/// The second member of the all-ARN set measured on 2026-10-28.
const TABLE_OTHER: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/OTHER";

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

/// A bare Arn operator on a multi-valued key is false even when every member
/// satisfies it, so per-value evaluation would answer true in each row here.
/// Measured on 2026-10-28 with `aws:TagKeys` as a string list holding
/// [`table/V`, `table/OTHER`]: ArnEquals and ArnLike with `table/*` (both
/// members match) and ArnNotEquals and ArnNotLike with `table/T*` (neither
/// member matches) were implicitDeny.
#[test]
fn arn_operators_bare_on_multivalued_never_match() {
    let set = vec![TABLE_V, TABLE_OTHER];
    for op in [ArnEquals, ArnLike] {
        assert!(
            !set_of(op.clone(), TABLE_STAR, set.clone()),
            "{op:?} table/*"
        );
    }
    for op in [ArnNotEquals, ArnNotLike] {
        assert!(!set_of(op.clone(), T_STAR, set.clone()), "{op:?} table/T*");
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
        assert!(!single(ArnNotLike, p), "ArnNotLike {p}");
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

/// A bare `*` policy value. Over `notanarn` and over `arn:aws:dynamodb` all
/// four operators were implicitDeny (simulate-custom-policy, 2026-10-28,
/// `aws:SourceArn` as a string). Over an ARN the simulator refuses the value
/// (InvalidInput), so the first two assertions are derived, not measured:
/// Access Analyzer reports `ArnEquals: "*"` as an error (MISSING_ARN_FIELD)
/// and as equivalent to `Null: false`, and `ArnNotEquals: "*"` as equivalent
/// to `Null: true`; over a present value those are true and false.
#[test]
fn arn_operators_bare_star_value() {
    assert!(single(ArnEquals, "*"));
    assert!(!single(ArnNotEquals, "*"));
    for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
        for v in ["notanarn", "arn:aws:dynamodb"] {
            assert!(!single_value(op.clone(), "*", v), "{op:?} * {v}");
        }
    }
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
        assert!(single(ArnNotLike, p), "ArnNotLike {p}");
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

// --- rows measured on 2026-10-23 14:10 UTC (simulate-custom-policy) ---

/// Multi-valued key holding `[V, notanarn]`.
fn multi_malformed(op: ConditionOperator, pattern: &str) -> bool {
    let ctx = TestContext::new().with("dynamodb:LeadingKeys", vec![V, "notanarn"]);
    evaluate_condition(&cond(op, "dynamodb:LeadingKeys", vec![pattern]), &ctx)
}

/// Multi-valued key (`dynamodb:LeadingKeys` in the test context) holding
/// `values`.
fn set_of(op: ConditionOperator, pattern: &str, values: Vec<&str>) -> bool {
    let ctx = TestContext::new().with("dynamodb:LeadingKeys", values);
    evaluate_condition(&cond(op, "dynamodb:LeadingKeys", vec![pattern]), &ctx)
}

/// A multi-valued key with one value that is not an ARN fails every Arn
/// operator in every set form, whether or not the pattern matches the
/// well-formed value: 20 rows, all implicitDeny. The `IfExists` set forms
/// were measured on 2026-10-28 with `aws:TagKeys` as a string list holding
/// [`table/V`, `notanarn`]: `ForAnyValue:` and `ForAllValues:` with each of
/// the four operators and `IfExists`, patterns `table/*` and `table/Z*`, 16
/// rows, all implicitDeny.
#[test]
fn arn_operators_multivalued_with_non_arn_value_never_match() {
    for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
        assert!(!multi_malformed(op.clone(), TABLE_STAR), "bare {op:?}");
        assert!(
            !multi_malformed(for_any(op.clone()), TABLE_STAR),
            "ForAnyValue {op:?} table/*"
        );
        assert!(
            !multi_malformed(for_all(op.clone()), TABLE_STAR),
            "ForAllValues {op:?} table/*"
        );
        assert!(
            !multi_malformed(for_any(op.clone()), Z_STAR),
            "ForAnyValue {op:?} table/Z*"
        );
        assert!(
            !multi_malformed(for_all(op.clone()), Z_STAR),
            "ForAllValues {op:?} table/Z*"
        );
        for pattern in [TABLE_STAR, Z_STAR] {
            let set = vec![TABLE_V, "notanarn"];
            assert!(
                !set_of(for_any(if_exists(op.clone())), pattern, set.clone()),
                "ForAnyValue {op:?}IfExists {pattern}"
            );
            assert!(
                !set_of(for_all(if_exists(op.clone())), pattern, set),
                "ForAllValues {op:?}IfExists {pattern}"
            );
        }
    }
}

/// The set-level rule does not reach non-Arn operators: `StringLike` over the
/// same set still evaluates per value.
#[test]
fn string_operators_over_set_with_non_arn_value_are_unchanged() {
    let ctx = TestContext::new().with("dynamodb:LeadingKeys", vec![V, "notanarn"]);
    let any = for_any(ConditionOperator::StringLike);
    assert!(evaluate_condition(
        &cond(any, "dynamodb:LeadingKeys", vec![TABLE_STAR]),
        &ctx
    ));
    let all = for_all(ConditionOperator::StringNotLike);
    assert!(evaluate_condition(
        &cond(all, "dynamodb:LeadingKeys", vec![Z_STAR]),
        &ctx
    ));
}

/// Rows v-empty and v-space: an empty or whitespace value fails all four.
#[test]
fn arn_operators_empty_and_whitespace_value_match_neither() {
    for v in ["", "  "] {
        for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
            assert!(!single_value(op.clone(), TABLE_STAR, v), "{op:?} {v:?}");
        }
    }
}

/// Rows v6-noarn-prefix, v6-noarn-prefix-anypattern and v6-upper-arn-prefix:
/// a six-field value whose first field is not `arn` is compared as an ARN.
/// The positive operators fail on the first field; the negative ones hold.
#[test]
fn arn_operators_six_field_value_without_arn_prefix_fails_positive_only() {
    let all_star = "arn:*:*:*:*:*";
    let rows = [
        (
            "xyz:aws:dynamodb:us-east-1:123456789012:table/Tother",
            TABLE_STAR,
        ),
        (
            "xyz:aws:dynamodb:us-east-1:123456789012:table/Tother",
            all_star,
        ),
        (
            "ARN:aws:dynamodb:us-east-1:123456789012:table/Tother",
            TABLE_STAR,
        ),
    ];
    for (v, p) in rows {
        assert!(!single_value(ArnEquals, p, v), "ArnEquals {p} {v}");
        assert!(!single_value(ArnLike, p, v), "ArnLike {p} {v}");
        assert!(single_value(ArnNotEquals, p, v), "ArnNotEquals {p} {v}");
        assert!(single_value(ArnNotLike, p, v), "ArnNotLike {p} {v}");
    }
}

/// Rows v7-vs-p6-exact, v7-vs-p6-star and v6-vs-p7-exact: a colon in the
/// resource stays in the resource field, so `table/*` matches
/// `table/Tother:extra` and an exact pattern does not; a seven-field pattern
/// does not match a six-field value.
#[test]
fn arn_operators_seven_field_value_or_pattern() {
    let extra = "arn:aws:dynamodb:us-east-1:123456789012:table/Tother:extra";
    // (pattern, value, positive result)
    let rows = [
        (V, extra, false),
        (TABLE_STAR, extra, true),
        (extra, V, false),
    ];
    for (p, v, positive) in rows {
        assert_eq!(single_value(ArnEquals, p, v), positive, "ArnEquals {p} {v}");
        assert_eq!(single_value(ArnLike, p, v), positive, "ArnLike {p} {v}");
        assert_eq!(
            single_value(ArnNotEquals, p, v),
            !positive,
            "ArnNotEquals {p} {v}"
        );
        assert_eq!(
            single_value(ArnNotLike, p, v),
            !positive,
            "ArnNotLike {p} {v}"
        );
    }
}

/// Rows v-empty-partition, v-empty-service, v-empty-resource and
/// v-empty-region-account: `*` matches an empty field, so `arn:*:*:*:*:*`
/// matches a six-field value with empty fields and the negatives fail.
#[test]
fn arn_operators_empty_fields_match_star() {
    let all_star = "arn:*:*:*:*:*";
    let values = [
        "arn::dynamodb:us-east-1:123456789012:table/Tother",
        "arn:aws::us-east-1:123456789012:table/Tother",
        "arn:aws:dynamodb:us-east-1:123456789012:",
        "arn:aws:dynamodb:::table/Tother",
    ];
    for v in values {
        assert!(single_value(ArnEquals, all_star, v), "ArnEquals {v}");
        assert!(single_value(ArnLike, all_star, v), "ArnLike {v}");
        assert!(!single_value(ArnNotEquals, all_star, v), "ArnNotEquals {v}");
        assert!(!single_value(ArnNotLike, all_star, v), "ArnNotLike {v}");
    }
}

/// Rows ifexists-malformed: `IfExists` with a present value that is not an
/// ARN fails all four operators.
#[test]
fn arn_operators_if_exists_present_non_arn_value_match_neither() {
    for op in [ArnEquals, ArnNotEquals, ArnLike, ArnNotLike] {
        assert!(
            !single_value(if_exists(op.clone()), TABLE_STAR, "notanarn"),
            "{op:?}"
        );
    }
}

/// Rows neg-7field-pattern and neg-index-table-star for the negative
/// operators: a seven-field pattern does not match V, so the negatives hold;
/// `table/*` matches an index ARN, so they fail.
#[test]
fn arn_not_operators_seven_field_pattern_and_index_value() {
    let seven = "arn:aws:dynamodb:us-east-1:123456789012:table/Tother:extra";
    let idx = "arn:aws:dynamodb:us-east-1:123456789012:table/T/index/I";
    for op in [ArnNotEquals, ArnNotLike] {
        assert!(single(op.clone(), seven), "{op:?} seven-field pattern");
        assert!(!single_value(op.clone(), TABLE_STAR, idx), "{op:?} index");
    }
}
