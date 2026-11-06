// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A tag whose value is the empty string, in a `Resource` or `NotResource`
//! pattern: the variable expands to no characters, so
//! `table/${aws:PrincipalTag/team}*` becomes `table/*` for a caller tagged
//! `team=""`, while with no `team` tag the pattern matches nothing. A
//! `StringLike` condition `?*` on the tag keeps such a caller out.
//!
//! Each test repeats rows measured with the IAM policy simulator on
//! 2026-11-03, `dynamodb:GetItem`, two samples per row with the same answer;
//! a `Deny` row has `Allow dynamodb:*` on `*` beside it. The rows that read a
//! table with an empty name (`table/`) are not repeated: the simulator
//! denies that resource whatever the pattern, and a table name has at least
//! three characters.

use super::*;

const TEAM: &str = "aws:PrincipalTag/team";
const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/";
const TEAM_STAR: &str = "${aws:PrincipalTag/team}*";

fn document(statements: &str) -> Vec<PolicyDocument> {
    vec![parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{statements}]}}"#
    ))]
}

/// Only an `Allow dynamodb:GetItem` on `table/<resource>`, with `condition`.
fn allow_only(resource: &str, condition: &str) -> Vec<PolicyDocument> {
    document(&format!(
        r#"{{"Effect":"Allow","Action":"dynamodb:GetItem","Resource":"{ARN}{resource}"{condition}}}"#
    ))
}

/// `Allow dynamodb:*` on `*` and a `Deny dynamodb:GetItem` with `element`
/// (`Resource` or `NotResource`) on `table/<resource>`.
fn deny(element: &str, resource: &str) -> Vec<PolicyDocument> {
    document(&format!(
        r#"{{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}},
           {{"Effect":"Deny","Action":"dynamodb:GetItem","{element}":"{ARN}{resource}"}}"#
    ))
}

fn get(policies: &[PolicyDocument], target: &str, ctx: &Ctx) -> AuthzDecision {
    evaluate_policies(
        policies,
        None,
        None,
        "dynamodb:GetItem",
        &format!("{ARN}{target}"),
        ctx,
    )
}

fn team(value: &str) -> Ctx {
    Ctx::empty().with(TEAM, vec![value])
}

#[test]
fn e1_to_e5_an_empty_value_before_a_wildcard_matches_every_table() {
    let p = allow_only(TEAM_STAR, "");
    assert_eq!(get(&p, "red", &team("")), AuthzDecision::Allow, "E1");
    assert_eq!(get(&p, "blue", &team("")), AuthzDecision::Allow, "E2");
    assert_eq!(get(&p, "red", &Ctx::empty()), AuthzDecision::Deny, "E3");
    assert_eq!(get(&p, "red", &team("blue")), AuthzDecision::Deny, "E4");
    assert_eq!(get(&p, "blue-x", &team("blue")), AuthzDecision::Allow, "E5");
}

#[test]
fn e6_e7_a_deny_on_an_empty_value_before_a_wildcard() {
    let p = deny("Resource", TEAM_STAR);
    assert_eq!(get(&p, "red", &team("")), AuthzDecision::Deny, "E6");
    let p = deny("NotResource", TEAM_STAR);
    assert_eq!(get(&p, "red", &team("")), AuthzDecision::Allow, "E7");
}

#[test]
fn e8_an_empty_value_after_a_star_leaves_the_star() {
    let p = document(
        r#"{"Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*${aws:PrincipalTag/suffix}"}"#,
    );
    let ctx = Ctx::empty().with("aws:PrincipalTag/suffix", vec![""]);
    assert_eq!(get(&p, "red", &ctx), AuthzDecision::Allow);
}

#[test]
fn e9_an_empty_value_beside_written_text_adds_nothing() {
    let empty = team("");
    let p = allow_only("${aws:PrincipalTag/team}x", "");
    assert_eq!(get(&p, "x", &empty), AuthzDecision::Allow, "E9d");
    let p = allow_only("x${aws:PrincipalTag/team}", "");
    assert_eq!(get(&p, "x", &empty), AuthzDecision::Allow, "E9e");
    let p = allow_only("${aws:PrincipalTag/team}", "");
    assert_eq!(get(&p, "x", &empty), AuthzDecision::Deny, "E9f");
    let p = allow_only("x${aws:PrincipalTag/team}y", "");
    assert_eq!(get(&p, "xy", &empty), AuthzDecision::Allow, "E9h");
}

#[test]
fn e10_e11_a_string_like_guard_keeps_an_empty_value_out() {
    let guard = r#","Condition":{"StringLike":{"aws:PrincipalTag/team":"?*"}}"#;
    let p = allow_only(TEAM_STAR, guard);
    assert_eq!(get(&p, "red", &team("")), AuthzDecision::Deny, "E10");
    assert_eq!(
        get(&p, "blue-x", &team("blue")),
        AuthzDecision::Allow,
        "E11"
    );
}
