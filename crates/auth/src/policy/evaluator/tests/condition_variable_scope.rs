// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Where a condition value reads the `${key, 'default'}` form and a trimmed
//! key name: only in a `2012-10-17` document and under a string or ARN
//! operator. Elsewhere the text between the braces is the key name as
//! written, as it was before the default form was read, so the default form
//! is compared as written and `${key}` expands as before.
//!
//! The tests named after rows repeat cases measured with the IAM policy
//! simulator on 2026-11-03, `dynamodb:GetItem`, two samples per row with the
//! same answer; a `Deny` row has `Allow dynamodb:*` on `*` in the same
//! document. The group, boundary and session policy cases, the trimmed key
//! in a `2008-10-17` document, the set qualifier and `IfExists` wrapping a
//! numeric operator, and the unresolvable key follow from the rows and were
//! not measured. Row N4b is not repeated: IAM reads a `Bool` value written as
//! a variable as `false`, which ExtendDB does not do for either form. Row N6
//! (`IpAddress`) is not repeated either: ExtendDB refuses a document whose
//! `IpAddress` value is not a CIDR, so such a value never reaches evaluation.

use super::*;
use serde_json::{Value, json};

const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/";
const ALLOW_ALL: &str = r#"{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}"#;

/// A document with `version` (none when `None`) and `statements`.
fn document(version: Option<&str>, statements: &[Value]) -> PolicyDocument {
    let mut doc = json!({"Statement": statements});
    if let Some(version) = version {
        doc["Version"] = json!(version);
    }
    parse(&doc.to_string())
}

/// A statement on `dynamodb:GetItem` and every resource whose condition
/// compares `key` with `value` under `operator`.
fn get_if(effect: &str, operator: &str, key: &str, value: &str) -> Value {
    json!({
        "Effect": effect, "Action": "dynamodb:GetItem", "Resource": "*",
        "Condition": {operator: {key: value}},
    })
}

fn allow_all() -> Value {
    serde_json::from_str(ALLOW_ALL).unwrap()
}

/// `Allow dynamodb:*` on `*` and a `Deny` gated on the condition, in one
/// document.
fn deny_if(version: Option<&str>, operator: &str, key: &str, value: &str) -> Vec<PolicyDocument> {
    vec![document(
        version,
        &[allow_all(), get_if("Deny", operator, key, value)],
    )]
}

/// Only an `Allow` gated on the condition.
fn allow_only_if(
    version: Option<&str>,
    operator: &str,
    key: &str,
    value: &str,
) -> Vec<PolicyDocument> {
    vec![document(version, &[get_if("Allow", operator, key, value)])]
}

fn get(policies: &[PolicyDocument], ctx: &Ctx) -> AuthzDecision {
    get_with(policies, None, None, ctx)
}

fn get_with(
    policies: &[PolicyDocument],
    boundary: Option<&PolicyDocument>,
    session: Option<&PolicyDocument>,
    ctx: &Ctx,
) -> AuthzDecision {
    evaluate_policies(
        policies,
        boundary,
        session,
        "dynamodb:GetItem",
        &format!("{ARN}x"),
        ctx,
    )
}

const TEAM: &str = "aws:PrincipalTag/team";
const DEFAULTED_USERNAME: &str = "${aws:username, 'x'}";

/// A user `alice` tagged `team=alice`.
fn alice_on_team_alice() -> Ctx {
    Ctx::empty()
        .with("aws:username", vec!["alice"])
        .with(TEAM, vec!["alice"])
}

// --- The `Version` of the document ---

#[test]
fn v1_a_2008_deny_compares_the_default_form_as_written() {
    let p = deny_if(
        Some("2008-10-17"),
        "StringNotEquals",
        TEAM,
        DEFAULTED_USERNAME,
    );
    assert_eq!(get(&p, &alice_on_team_alice()), AuthzDecision::Deny);
}

#[test]
fn v2_v3_v4_v5_an_allow_reads_the_default_form_only_in_a_2012_document() {
    let ctx = alice_on_team_alice();
    for version in [Some("2008-10-17"), None] {
        let p = allow_only_if(version, "StringEquals", TEAM, DEFAULTED_USERNAME);
        assert_eq!(get(&p, &ctx), AuthzDecision::Deny, "{version:?}");
    }
    let p = allow_only_if(Some("2012-10-17"), "StringEquals", TEAM, DEFAULTED_USERNAME);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
    // V5: the tag holds the text as written.
    let p = allow_only_if(Some("2008-10-17"), "StringEquals", TEAM, DEFAULTED_USERNAME);
    let literal = Ctx::empty()
        .with("aws:username", vec!["alice"])
        .with(TEAM, vec![DEFAULTED_USERNAME]);
    assert_eq!(get(&p, &literal), AuthzDecision::Allow);
}

#[test]
fn a_2008_document_does_not_trim_the_key_name() {
    let p = allow_only_if(
        Some("2008-10-17"),
        "StringEquals",
        TEAM,
        "${ aws:username }",
    );
    assert_eq!(get(&p, &alice_on_team_alice()), AuthzDecision::Deny);
    let p = allow_only_if(
        Some("2012-10-17"),
        "StringEquals",
        TEAM,
        "${ aws:username }",
    );
    assert_eq!(get(&p, &alice_on_team_alice()), AuthzDecision::Allow);
}

#[test]
fn a_group_policy_follows_its_own_version() {
    // A group policy is one more identity document beside the user's own.
    let user = document(
        Some("2012-10-17"),
        &[json!({"Effect": "Allow", "Action": "dynamodb:PutItem", "Resource": "*"})],
    );
    let group = document(
        Some("2008-10-17"),
        &[get_if("Allow", "StringEquals", TEAM, DEFAULTED_USERNAME)],
    );
    assert_eq!(
        get(&[user, group], &alice_on_team_alice()),
        AuthzDecision::Deny
    );
}

#[test]
fn a_boundary_and_a_session_policy_follow_their_own_version() {
    let identity = [document(Some("2012-10-17"), &[allow_all()])];
    let ctx = alice_on_team_alice();
    for version in [Some("2008-10-17"), None] {
        let gate = document(
            version,
            &[get_if("Allow", "StringEquals", TEAM, DEFAULTED_USERNAME)],
        );
        assert_eq!(
            get_with(&identity, Some(&gate), None, &ctx),
            AuthzDecision::Deny,
            "boundary {version:?}"
        );
        assert_eq!(
            get_with(&identity, None, Some(&gate), &ctx),
            AuthzDecision::Deny,
            "session {version:?}"
        );
    }
    let gate = document(
        Some("2012-10-17"),
        &[get_if("Allow", "StringEquals", TEAM, DEFAULTED_USERNAME)],
    );
    assert_eq!(
        get_with(&identity, Some(&gate), None, &ctx),
        AuthzDecision::Allow
    );
    assert_eq!(
        get_with(&identity, None, Some(&gate), &ctx),
        AuthzDecision::Allow
    );
}

// --- The operator ---

#[test]
fn n1_n2_numeric_equals_does_not_read_a_default() {
    let age = Ctx::empty().with("aws:MultiFactorAuthAge", vec!["300"]);
    let value = "${aws:PrincipalTag/m, '300'}";
    let p = allow_only_if(
        Some("2012-10-17"),
        "NumericEquals",
        "aws:MultiFactorAuthAge",
        value,
    );
    assert_eq!(get(&p, &age), AuthzDecision::Deny);
    let p = deny_if(
        Some("2012-10-17"),
        "NumericEquals",
        "aws:MultiFactorAuthAge",
        value,
    );
    assert_eq!(get(&p, &age), AuthzDecision::Allow);
    // The same under a set qualifier and `IfExists`.
    for operator in ["ForAnyValue:NumericEquals", "NumericEqualsIfExists"] {
        let p = allow_only_if(
            Some("2012-10-17"),
            operator,
            "aws:MultiFactorAuthAge",
            value,
        );
        assert_eq!(get(&p, &age), AuthzDecision::Deny, "{operator}");
    }
}

#[test]
fn n3_n5_date_and_null_operators_do_not_read_a_default() {
    let now = Ctx::empty().with("aws:CurrentTime", vec!["2026-11-03T12:00:00Z"]);
    let p = deny_if(
        Some("2012-10-17"),
        "DateGreaterThan",
        "aws:CurrentTime",
        "${aws:PrincipalTag/d, '2000-01-01T00:00:00Z'}",
    );
    assert_eq!(get(&p, &now), AuthzDecision::Allow, "N3");
    let p = deny_if(
        Some("2012-10-17"),
        "Null",
        TEAM,
        "${aws:PrincipalTag/f, 'true'}",
    );
    assert_eq!(get(&p, &Ctx::empty()), AuthzDecision::Allow, "N5");
}

#[test]
fn n4_bool_does_not_read_a_default() {
    let secure = Ctx::empty().with("aws:SecureTransport", vec!["true"]);
    let p = deny_if(
        Some("2012-10-17"),
        "Bool",
        "aws:SecureTransport",
        "${aws:PrincipalTag/f, 'true'}",
    );
    assert_eq!(get(&p, &secure), AuthzDecision::Allow);
}

#[test]
fn s1_a1_string_and_arn_operators_read_a_default() {
    let owner = Ctx::empty().with("aws:ResourceTag/owner", vec!["anon"]);
    let p = deny_if(
        Some("2012-10-17"),
        "StringEquals",
        "aws:ResourceTag/owner",
        "${aws:username, 'anon'}",
    );
    assert_eq!(get(&p, &owner), AuthzDecision::Deny, "S1");
    let source = Ctx::empty().with(
        "aws:SourceArn",
        vec!["arn:aws:iam::123456789012:user/alice"],
    );
    let p = deny_if(
        Some("2012-10-17"),
        "ArnLike",
        "aws:SourceArn",
        "arn:aws:iam::123456789012:user/${aws:PrincipalTag/n, 'alice'}",
    );
    assert_eq!(get(&p, &source), AuthzDecision::Deny, "A1");
}

// --- A defaulted variable naming an unresolvable key ---

#[test]
fn a_defaulted_unresolvable_key_fails_closed_only_where_the_default_form_is_read() {
    let ctx = Ctx::empty()
        .with(TEAM, vec!["blue"])
        .unresolvable("aws:PrincipalOrgID");
    let value = "${aws:PrincipalOrgID, 'blue'}";
    // Read with IAM's syntax, the variable names the unresolvable key and the
    // Deny fails closed.
    let p = deny_if(Some("2012-10-17"), "StringEquals", TEAM, value);
    assert_eq!(get(&p, &ctx), AuthzDecision::Deny);
    // Elsewhere the value names the key `aws:PrincipalOrgID, 'blue'`, which
    // the request does not carry, and is compared as written.
    let p = deny_if(Some("2008-10-17"), "StringEquals", TEAM, value);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
    let p = deny_if(
        Some("2012-10-17"),
        "NumericEquals",
        "aws:MultiFactorAuthAge",
        "${aws:PrincipalOrgID, '300'}",
    );
    let age = Ctx::empty()
        .with("aws:MultiFactorAuthAge", vec!["300"])
        .unresolvable("aws:PrincipalOrgID");
    assert_eq!(get(&p, &age), AuthzDecision::Allow);
}
