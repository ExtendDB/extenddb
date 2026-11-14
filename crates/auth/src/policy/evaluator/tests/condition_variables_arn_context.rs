// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The four `Arn*` operators on a value whose variable may have no value,
//! against a context value that is an ARN and one that is not.
//!
//! One test per cell measured with the IAM policy simulator on 2026-10-31
//! (the rows are in the description of PR #541): an `Allow dynamodb:GetItem`
//! on `*` alone in a `2012-10-17` document, with the condition
//! `<operator> aws:SourceArn` against
//! `arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team}`,
//! action `dynamodb:GetItem`. `Allow` is the simulator's `allowed` and `Deny`
//! its `implicitDeny`. A context value that is not an ARN matches none of the
//! four operators, whatever the policy value. Against an ARN, a value whose
//! variable has no value matches nothing: `ArnLike` and `ArnEquals` are false
//! and `ArnNotLike` and `ArnNotEquals` true.

use super::*;

const SRC: &str = "aws:SourceArn";
const TEAM: &str = "aws:PrincipalTag/team";
const VALUE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/${aws:PrincipalTag/team}";
const ARN_X: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/x";
const NOT_AN_ARN: &str = "notanarn";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/probe";

/// GetItem under `Allow dynamodb:GetItem` on `*` if `operator aws:SourceArn`
/// matches [`VALUE`], for a caller whose `team` tag is `team` (absent when
/// `None`) and a request whose `aws:SourceArn` is `source_arn`.
fn get(operator: &str, team: Option<&str>, source_arn: &str) -> AuthzDecision {
    let policy = parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{{
            "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{{"{operator}":{{"{SRC}":"{VALUE}"}}}}
        }}]}}"#
    ));
    let mut ctx = Ctx::empty().with(SRC, vec![source_arn]);
    if let Some(team) = team {
        ctx = ctx.with(TEAM, vec![team]);
    }
    evaluate_policies(&[policy], None, None, "dynamodb:GetItem", TABLE, &ctx)
}

// --- No `team` tag, `aws:SourceArn` an ARN ---

#[test]
fn arn_like_with_no_team_against_an_arn_is_false() {
    assert_eq!(get("ArnLike", None, ARN_X), AuthzDecision::Deny);
}

#[test]
fn arn_equals_with_no_team_against_an_arn_is_false() {
    assert_eq!(get("ArnEquals", None, ARN_X), AuthzDecision::Deny);
}

#[test]
fn arn_not_like_with_no_team_against_an_arn_is_true() {
    assert_eq!(get("ArnNotLike", None, ARN_X), AuthzDecision::Allow);
}

#[test]
fn arn_not_equals_with_no_team_against_an_arn_is_true() {
    assert_eq!(get("ArnNotEquals", None, ARN_X), AuthzDecision::Allow);
}

// --- No `team` tag, `aws:SourceArn` not an ARN ---

#[test]
fn arn_like_with_no_team_against_a_non_arn_is_false() {
    assert_eq!(get("ArnLike", None, NOT_AN_ARN), AuthzDecision::Deny);
}

#[test]
fn arn_equals_with_no_team_against_a_non_arn_is_false() {
    assert_eq!(get("ArnEquals", None, NOT_AN_ARN), AuthzDecision::Deny);
}

#[test]
fn arn_not_like_with_no_team_against_a_non_arn_is_false() {
    assert_eq!(get("ArnNotLike", None, NOT_AN_ARN), AuthzDecision::Deny);
}

#[test]
fn arn_not_equals_with_no_team_against_a_non_arn_is_false() {
    assert_eq!(get("ArnNotEquals", None, NOT_AN_ARN), AuthzDecision::Deny);
}

// --- `team=x`, `aws:SourceArn` the ARN the value expands to ---

#[test]
fn arn_like_with_team_against_the_expanded_arn_is_true() {
    assert_eq!(get("ArnLike", Some("x"), ARN_X), AuthzDecision::Allow);
}

#[test]
fn arn_equals_with_team_against_the_expanded_arn_is_true() {
    assert_eq!(get("ArnEquals", Some("x"), ARN_X), AuthzDecision::Allow);
}

#[test]
fn arn_not_like_with_team_against_the_expanded_arn_is_false() {
    assert_eq!(get("ArnNotLike", Some("x"), ARN_X), AuthzDecision::Deny);
}

#[test]
fn arn_not_equals_with_team_against_the_expanded_arn_is_false() {
    assert_eq!(get("ArnNotEquals", Some("x"), ARN_X), AuthzDecision::Deny);
}

// --- `team=x`, `aws:SourceArn` not an ARN ---

#[test]
fn arn_like_with_team_against_a_non_arn_is_false() {
    assert_eq!(get("ArnLike", Some("x"), NOT_AN_ARN), AuthzDecision::Deny);
}

#[test]
fn arn_equals_with_team_against_a_non_arn_is_false() {
    assert_eq!(get("ArnEquals", Some("x"), NOT_AN_ARN), AuthzDecision::Deny);
}

#[test]
fn arn_not_like_with_team_against_a_non_arn_is_false() {
    assert_eq!(
        get("ArnNotLike", Some("x"), NOT_AN_ARN),
        AuthzDecision::Deny
    );
}

#[test]
fn arn_not_equals_with_team_against_a_non_arn_is_false() {
    assert_eq!(
        get("ArnNotEquals", Some("x"), NOT_AN_ARN),
        AuthzDecision::Deny
    );
}
