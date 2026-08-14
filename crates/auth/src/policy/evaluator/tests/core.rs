// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Core evaluation-phase tests: allow/deny precedence, boundary, session policy.

use super::*;

// --- Basic Allow/Deny ---

#[test]
fn simple_allow() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:PutItem","Resource":"*"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
}

#[test]
fn implicit_deny_no_matching_allow() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

#[test]
fn explicit_deny_overrides_allow() {
    let allow = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    let deny = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:DeleteTable","Resource":"*"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            &[allow, deny],
            None,
            None,
            "dynamodb:DeleteTable",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

// --- Case-insensitive action matching ---

#[test]
fn action_matching_is_case_insensitive() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:putitem","Resource":"*"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
}

#[test]
fn deny_case_insensitive() {
    let allow = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    let deny = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:deletetable","Resource":"*"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            &[allow, deny],
            None,
            None,
            "dynamodb:DeleteTable",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

// --- NotAction ---

#[test]
fn not_action_deny() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","NotAction":["dynamodb:GetItem"],"Resource":"*"
            }]}"#,
    );
    // PutItem is not in the NotAction list, so the Deny applies
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

#[test]
fn not_action_deny_excluded_action_not_denied() {
    let deny = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","NotAction":["dynamodb:GetItem"],"Resource":"*"
            }]}"#,
    );
    let allow = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
    );
    // GetItem is excluded from the Deny, and explicitly allowed
    assert_eq!(
        evaluate_policies(
            &[deny, allow],
            None,
            None,
            "dynamodb:GetItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
}

// --- NotResource ---

#[test]
fn not_resource_deny() {
    let deny = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:*",
                "NotResource":["arn:aws:dynamodb:*:*:table/AllowedTable"]
            }]}"#,
    );
    let allow = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    // Access to AllowedTable is not denied (excluded from NotResource)
    assert_eq!(
        evaluate_policies(
            &[deny.clone(), allow.clone()],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/AllowedTable",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
    // Access to OtherTable is denied
    assert_eq!(
        evaluate_policies(
            &[deny, allow],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/OtherTable",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

// --- Permissions Boundary ---

#[test]
fn boundary_restricts_allow() {
    let identity = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    let boundary = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
    );
    // Identity allows all, but boundary only allows GetItem
    assert_eq!(
        evaluate_policies(
            std::slice::from_ref(&identity),
            Some(&boundary),
            None,
            "dynamodb:GetItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
    assert_eq!(
        evaluate_policies(
            &[identity],
            Some(&boundary),
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

#[test]
fn boundary_deny_overrides() {
    let identity = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    let boundary = parse(
        r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"dynamodb:*","Resource":"*"},
                {"Effect":"Deny","Action":"dynamodb:DeleteTable","Resource":"*"}
            ]}"#,
    );
    assert_eq!(
        evaluate_policies(
            &[identity],
            Some(&boundary),
            None,
            "dynamodb:DeleteTable",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

// --- Session Policy ---

#[test]
fn session_policy_restricts() {
    let role = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    let session = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            std::slice::from_ref(&role),
            None,
            Some(&session),
            "dynamodb:GetItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
    assert_eq!(
        evaluate_policies(
            &[role],
            None,
            Some(&session),
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

// --- Conditions (ABAC) ---

#[test]
fn condition_tag_match_allows() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*",
                "Condition":{"StringEquals":{"aws:PrincipalTag/Department":"Eng"}}
            }]}"#,
    );
    let ctx = Ctx::empty().with("aws:PrincipalTag/Department", vec!["Eng"]);
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &ctx
        ),
        AuthzDecision::Allow
    );
}

#[test]
fn condition_tag_mismatch_denies() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*",
                "Condition":{"StringEquals":{"aws:PrincipalTag/Department":"Eng"}}
            }]}"#,
    );
    let ctx = Ctx::empty().with("aws:PrincipalTag/Department", vec!["Sales"]);
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &ctx
        ),
        AuthzDecision::Deny
    );
}
