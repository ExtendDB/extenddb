// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy variables in condition values, the review cells of issue #534:
//! which operators expand a variable, a wildcard inside a substituted value,
//! `IfExists` and the set qualifiers with an unresolved variable. Every
//! expected decision was measured with the IAM policy simulator on
//! 2026-10-25 (`Allow dynamodb:*` on `*` beside a `Deny dynamodb:GetItem` on
//! `*` with the condition under test). The `Null` and `Bool` cells N1a to N1f
//! and N2a to N2e were measured the same way on 2026-10-28, two samples each;
//! N1f is an `Allow dynamodb:GetItem` with the condition and nothing else.
//! `bool_reads_a_request_value_true_in_any_case_as_true` is not measured.

use super::*;

const BLUE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/blue";
const USER: &str = "arn:aws:iam::123456789012:user/";

fn deny_if(condition: &str) -> Vec<PolicyDocument> {
    vec![
        parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
        ),
        parse(&format!(
            r#"{{"Version":"2012-10-17","Statement":[{{
                "Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
                "Condition":{condition}
            }}]}}"#
        )),
    ]
}

fn get(policies: &[PolicyDocument], ctx: &Ctx) -> AuthzDecision {
    evaluate_policies(policies, None, None, "dynamodb:GetItem", BLUE, ctx)
}

// --- Only the string and ARN operators expand a variable (cell C4a) ---

#[test]
fn numeric_equals_compares_a_variable_as_written() {
    let p = deny_if(r#"{"NumericEquals":{"aws:MultiFactorAuthAge":"${aws:PrincipalTag/age}"}}"#);
    let ctx = Ctx::empty()
        .with("aws:PrincipalTag/age", vec!["300"])
        .with("aws:MultiFactorAuthAge", vec!["300"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
}

// --- `Null` and `Bool` read a variable as the boolean `false` (N1a to N2e) ---

fn allow_only(condition: &str) -> Vec<PolicyDocument> {
    vec![parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{{
            "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{condition}
        }}]}}"#
    ))]
}

/// The cells whose decision differs from the one IAM gave.
fn differ(cells: &[(&'static str, Vec<PolicyDocument>, Ctx, AuthzDecision)]) -> Vec<&'static str> {
    cells
        .iter()
        .filter(|(_, policies, ctx, iam)| get(policies, ctx) != *iam)
        .map(|(cell, ..)| *cell)
        .collect()
}

#[test]
fn null_reads_a_variable_as_false() {
    use AuthzDecision::{Allow, Deny};
    // `Null` on `team` holds when `team` is present, whatever `flag` holds.
    let null = |value: &str| format!(r#"{{"Null":{{"aws:PrincipalTag/team":"{value}"}}}}"#);
    let flag = "${aws:PrincipalTag/flag}";
    let flag_or_false = "${aws:PrincipalTag/flag, 'false'}";
    let team = || Ctx::empty().with("aws:PrincipalTag/team", vec!["blue"]);
    let flag_true = |ctx: Ctx| ctx.with("aws:PrincipalTag/flag", vec!["true"]);
    let cells = [
        ("N1a", deny_if(&null(flag)), team(), Deny),
        ("N1b", deny_if(&null(flag_or_false)), team(), Deny),
        ("N1c", deny_if(&null(flag)), flag_true(team()), Deny),
        ("N1d", deny_if(&null(flag)), flag_true(Ctx::empty()), Allow),
        ("N1e", deny_if(&null(flag)), Ctx::empty(), Allow),
        ("N1f", allow_only(&null(flag_or_false)), team(), Allow),
    ];
    let wrong = differ(&cells);
    assert!(wrong.is_empty(), "differ from IAM: {wrong:?}");
}

#[test]
fn bool_reads_a_variable_as_false() {
    use AuthzDecision::{Allow, Deny};
    // `Bool` holds when `aws:SecureTransport` is `false`, whatever `flag`
    // holds. N2c is cell C4b, measured on 2026-10-25.
    let p = || deny_if(r#"{"Bool":{"aws:SecureTransport":"${aws:PrincipalTag/flag}"}}"#);
    let request = |secure: &str, flag: Option<&str>| {
        let ctx = Ctx::empty().with("aws:SecureTransport", vec![secure]);
        match flag {
            Some(flag) => ctx.with("aws:PrincipalTag/flag", vec![flag]),
            None => ctx,
        }
    };
    let cells = [
        ("N2a", p(), request("false", Some("false")), Deny),
        ("N2b", p(), request("false", Some("true")), Deny),
        ("N2c", p(), request("true", Some("true")), Allow),
        ("N2d", p(), request("true", Some("false")), Allow),
        ("N2e", p(), request("false", None), Deny),
    ];
    let wrong = differ(&cells);
    assert!(wrong.is_empty(), "differ from IAM: {wrong:?}");
}

#[test]
fn bool_reads_a_request_value_true_in_any_case_as_true() {
    // Not measured. Cell N2c with the request value spelled `TRUE`: the
    // request value is read as `true` ignoring case, as a `Bool` value without
    // a variable compares it, so the Deny does not apply.
    let p = deny_if(r#"{"Bool":{"aws:SecureTransport":"${aws:PrincipalTag/flag}"}}"#);
    let ctx = Ctx::empty()
        .with("aws:SecureTransport", vec!["TRUE"])
        .with("aws:PrincipalTag/flag", vec!["true"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
}

// --- A wildcard inside a substituted value is literal (cells C1, C2f, C2g) ---

#[test]
fn string_like_reads_a_substituted_star_as_a_character() {
    let p = deny_if(r#"{"StringLike":{"aws:ResourceTag/owner":"${aws:PrincipalTag/team}"}}"#);
    let team = |owner: &str| {
        Ctx::empty()
            .with("aws:PrincipalTag/team", vec!["a*"])
            .with("aws:ResourceTag/owner", vec![owner])
    };
    assert_eq!(get(&p, &team("abc")), AuthzDecision::Allow);
    assert_eq!(get(&p, &team("a*")), AuthzDecision::Deny);
}

#[test]
fn arn_like_and_arn_equals_read_a_substituted_star_as_a_character() {
    for op in ["ArnLike", "ArnEquals"] {
        let p = deny_if(&format!(
            r#"{{"{op}":{{"aws:SourceArn":"{USER}${{aws:PrincipalTag/name}}"}}}}"#
        ));
        let ctx = |name: &str| {
            Ctx::empty()
                .with("aws:PrincipalTag/name", vec![name])
                .with("aws:SourceArn", vec![format!("{USER}alice").as_str()])
        };
        assert_eq!(get(&p, &ctx("alice")), AuthzDecision::Deny, "{op}");
        assert_eq!(get(&p, &ctx("al*")), AuthzDecision::Allow, "{op}");
    }
}

// --- IfExists and the set qualifiers with an unresolved variable (C3, C6) ---

#[test]
fn if_exists_with_the_key_present_and_the_variable_unresolved() {
    let owner = Ctx::empty().with("aws:ResourceTag/owner", vec!["alice"]);
    let p = deny_if(r#"{"StringEqualsIfExists":{"aws:ResourceTag/owner":"${aws:username}"}}"#);
    assert_eq!(get(&p, &owner), AuthzDecision::Allow);
    let p = deny_if(r#"{"StringNotEqualsIfExists":{"aws:ResourceTag/owner":"${aws:username}"}}"#);
    assert_eq!(get(&p, &owner), AuthzDecision::Deny);
}

#[test]
fn set_qualifiers_with_an_unresolved_variable() {
    let keys = Ctx::empty().with("aws:TagKeys", vec!["a", "b"]);
    let p = deny_if(r#"{"ForAllValues:StringNotEquals":{"aws:TagKeys":"${aws:username}"}}"#);
    assert_eq!(get(&p, &keys), AuthzDecision::Deny);
    let p = deny_if(r#"{"ForAnyValue:StringEquals":{"aws:TagKeys":"${aws:username}"}}"#);
    assert_eq!(get(&p, &keys), AuthzDecision::Allow);
}
