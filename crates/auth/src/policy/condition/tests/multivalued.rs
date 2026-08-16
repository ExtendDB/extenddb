// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! BR-7085 multivalued-key arity tests, set qualifiers, negated ops, policy variables.

use super::*;

// --- BR-7085: bare operators on multivalued keys must never match ---
//
// Verified against real AWS IAM (Policy Simulator + end-to-end DynamoDB): a bare
// (non-ForAllValues/ForAnyValue) operator applied to a multivalued key
// (dynamodb:Attributes, dynamodb:LeadingKeys) never matches, regardless of the
// operator or how many values are present. Only the set qualifiers match.

#[test]
fn bare_op_multivalued_single_value_no_match() {
    // Even a single requested attribute equal to the policy value does NOT match
    // (real AWS returns "allowed"). This is the reporter's "control test", which
    // AWS does not deny.
    let ctx = TestContext::new().with("dynamodb:Attributes", vec!["ssn"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringEquals,
            "dynamodb:Attributes",
            vec!["ssn"]
        ),
        &ctx
    ));
}

#[test]
fn bare_op_multivalued_all_match_no_match() {
    let ctx = TestContext::new().with("dynamodb:Attributes", vec!["ssn", "salary"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringEquals,
            "dynamodb:Attributes",
            vec!["ssn", "salary"]
        ),
        &ctx
    ));
}

#[test]
fn bare_op_multivalued_extra_value_no_match() {
    // The reported "bypass" shape: request ssn + fullname, policy denies [ssn].
    let ctx = TestContext::new().with("dynamodb:Attributes", vec!["ssn", "fullname"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringEquals,
            "dynamodb:Attributes",
            vec!["ssn"]
        ),
        &ctx
    ));
}

#[test]
fn bare_negative_op_multivalued_no_match() {
    let ctx = TestContext::new().with("dynamodb:Attributes", vec!["fullname"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringNotEquals,
            "dynamodb:Attributes",
            vec!["ssn"]
        ),
        &ctx
    ));
}

#[test]
fn bare_string_like_multivalued_no_match() {
    let ctx = TestContext::new().with("dynamodb:Attributes", vec!["ssn"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringLike,
            "dynamodb:Attributes",
            vec!["ss*"]
        ),
        &ctx
    ));
}

#[test]
fn bare_op_multivalued_leading_keys_no_match() {
    let ctx = TestContext::new().with("dynamodb:LeadingKeys", vec!["user1"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringEquals,
            "dynamodb:LeadingKeys",
            vec!["user1"]
        ),
        &ctx
    ));
}

#[test]
fn if_exists_bare_op_multivalued_present_no_match() {
    let ctx = TestContext::new().with("dynamodb:Attributes", vec!["ssn"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::IfExists(Box::new(ConditionOperator::StringEquals)),
            "dynamodb:Attributes",
            vec!["ssn"]
        ),
        &ctx
    ));
}

#[test]
fn if_exists_multivalued_absent_passes() {
    // IfExists still passes when the key is absent.
    let ctx = TestContext::new();
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::IfExists(Box::new(ConditionOperator::StringEquals)),
            "dynamodb:Attributes",
            vec!["ssn"]
        ),
        &ctx
    ));
}

#[test]
fn for_any_value_multivalued_deny_matches() {
    // The CORRECT deny pattern: ForAnyValue:StringEquals fires when ANY requested
    // attribute is in the denied set.
    let ctx = TestContext::new().with("dynamodb:Attributes", vec!["ssn", "fullname"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringEquals)),
            "dynamodb:Attributes",
            vec!["ssn"]
        ),
        &ctx
    ));
}

#[test]
fn for_all_values_multivalued_allowlist() {
    // Allowlist pattern: ForAllValues:StringEquals allows only when EVERY requested
    // attribute is in the allowed set.
    let ctx_ok = TestContext::new().with("dynamodb:Attributes", vec!["pk", "fullname"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringEquals)),
            "dynamodb:Attributes",
            vec!["pk", "fullname"]
        ),
        &ctx_ok
    ));
    let ctx_bad = TestContext::new().with("dynamodb:Attributes", vec!["pk", "ssn"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringEquals)),
            "dynamodb:Attributes",
            vec!["pk", "fullname"]
        ),
        &ctx_bad
    ));
}

#[test]
fn bare_op_single_valued_key_still_matches() {
    // Regression: bare operators on single-valued keys are unaffected.
    let ctx = TestContext::new().with("dynamodb:Select", vec!["ALL_ATTRIBUTES"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::StringEquals,
            "dynamodb:Select",
            vec!["ALL_ATTRIBUTES"]
        ),
        &ctx
    ));
}

// --- Negative operators with multiple policy values ---

#[test]
fn string_not_equals_single_policy_value() {
    let ctx = TestContext::new().with("k", vec!["admin"]);
    // "admin" == "admin", so StringNotEquals should be false
    assert!(!evaluate_condition(
        &cond(ConditionOperator::StringNotEquals, "k", vec!["admin"]),
        &ctx
    ));
}

#[test]
fn string_not_equals_multiple_policy_values_match() {
    let ctx = TestContext::new().with("k", vec!["admin"]);
    // "admin" is in {"admin", "root"}, so StringNotEquals should be false
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringNotEquals,
            "k",
            vec!["admin", "root"]
        ),
        &ctx
    ));
}

#[test]
fn string_not_equals_multiple_policy_values_no_match() {
    let ctx = TestContext::new().with("k", vec!["user"]);
    // "user" is NOT in {"admin", "root"}, so StringNotEquals should be true
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::StringNotEquals,
            "k",
            vec!["admin", "root"]
        ),
        &ctx
    ));
}

#[test]
fn string_not_like_multiple_policy_values() {
    let ctx = TestContext::new().with("k", vec!["hello-world"]);
    // "hello-world" matches "hello-*", so StringNotLike should be false
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringNotLike,
            "k",
            vec!["hello-*", "foo-*"]
        ),
        &ctx
    ));
}

#[test]
fn string_not_like_multiple_policy_values_no_match() {
    let ctx = TestContext::new().with("k", vec!["bar-baz"]);
    // "bar-baz" matches neither "hello-*" nor "foo-*"
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::StringNotLike,
            "k",
            vec!["hello-*", "foo-*"]
        ),
        &ctx
    ));
}

// --- Policy variable expansion ---

#[test]
fn policy_variable_expansion_principal_tag() {
    let ctx = TestContext::new()
        .with("dynamodb:ResourceTag/Team", vec!["Alpha"])
        .with("aws:PrincipalTag/Team", vec!["Alpha"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::StringEquals,
            "dynamodb:ResourceTag/Team",
            vec!["${aws:PrincipalTag/Team}"]
        ),
        &ctx
    ));
}

#[test]
fn policy_variable_expansion_mismatch() {
    let ctx = TestContext::new()
        .with("dynamodb:ResourceTag/Team", vec!["Beta"])
        .with("aws:PrincipalTag/Team", vec!["Alpha"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::StringEquals,
            "dynamodb:ResourceTag/Team",
            vec!["${aws:PrincipalTag/Team}"]
        ),
        &ctx
    ));
}

#[test]
fn policy_variable_no_expansion_needed() {
    let ctx = TestContext::new().with("k", vec!["hello"]);
    assert!(evaluate_condition(
        &cond(ConditionOperator::StringEquals, "k", vec!["hello"]),
        &ctx
    ));
}

#[test]
fn policy_variable_unresolvable_left_literal() {
    let ctx = TestContext::new().with("k", vec!["${aws:PrincipalTag/Missing}"]);
    // Unresolvable variable stays literal — matches if context has the literal
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::StringEquals,
            "k",
            vec!["${aws:PrincipalTag/Missing}"]
        ),
        &ctx
    ));
}

// --- ForAllValues with negative operators ---

#[test]
fn for_all_values_string_not_equals_context_in_policy_set() {
    // ForAllValues:StringNotEquals with context ["admin"] and policy ["admin", "root"]
    // "admin" IS in the set, so the condition should be FALSE (deny applies).
    let ctx = TestContext::new().with("k", vec!["admin"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringNotEquals)),
            "k",
            vec!["admin", "root"]
        ),
        &ctx
    ));
}

#[test]
fn for_all_values_string_not_equals_context_not_in_policy_set() {
    // ForAllValues:StringNotEquals with context ["user"] and policy ["admin", "root"]
    // "user" is NOT in the set, so the condition should be TRUE.
    let ctx = TestContext::new().with("k", vec!["user"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringNotEquals)),
            "k",
            vec!["admin", "root"]
        ),
        &ctx
    ));
}

#[test]
fn for_all_values_string_not_equals_mixed_context() {
    // ForAllValues:StringNotEquals with context ["user", "admin"] and policy ["admin", "root"]
    // "admin" IS in the set, so the condition should be FALSE.
    let ctx = TestContext::new().with("k", vec!["user", "admin"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::ForAllValues(Box::new(ConditionOperator::StringNotEquals)),
            "k",
            vec!["admin", "root"]
        ),
        &ctx
    ));
}

#[test]
fn for_any_value_string_not_equals_all_in_set() {
    // ForAnyValue:StringNotEquals with context ["admin", "root"] and policy ["admin", "root"]
    // Both context values are in the policy set, so none satisfy "not in set" → FALSE.
    let ctx = TestContext::new().with("k", vec!["admin", "root"]);
    assert!(!evaluate_condition(
        &cond(
            ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringNotEquals)),
            "k",
            vec!["admin", "root"]
        ),
        &ctx
    ));
}

#[test]
fn for_any_value_string_not_equals_one_outside_set() {
    // ForAnyValue:StringNotEquals with context ["admin", "user"] and policy ["admin", "root"]
    // "user" is NOT in the set, so at least one satisfies "not in set" → TRUE.
    let ctx = TestContext::new().with("k", vec!["admin", "user"]);
    assert!(evaluate_condition(
        &cond(
            ConditionOperator::ForAnyValue(Box::new(ConditionOperator::StringNotEquals)),
            "k",
            vec!["admin", "root"]
        ),
        &ctx
    ));
}
