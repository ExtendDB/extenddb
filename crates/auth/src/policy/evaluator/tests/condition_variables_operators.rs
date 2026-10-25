// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy variables in condition values, the review cells of issue #534:
//! which operators expand a variable, a wildcard inside a substituted value,
//! `IfExists` and the set qualifiers with an unresolved variable. Every
//! expected decision was measured with the IAM policy simulator on
//! 2026-10-25 (`Allow dynamodb:*` on `*` beside a `Deny dynamodb:GetItem` on
//! `*` with the condition under test).

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

// --- Only the string and ARN operators expand a variable (cells C4a, C4b) ---

#[test]
fn numeric_equals_compares_a_variable_as_written() {
    let p = deny_if(r#"{"NumericEquals":{"aws:MultiFactorAuthAge":"${aws:PrincipalTag/age}"}}"#);
    let ctx = Ctx::empty()
        .with("aws:PrincipalTag/age", vec!["300"])
        .with("aws:MultiFactorAuthAge", vec!["300"]);
    assert_eq!(get(&p, &ctx), AuthzDecision::Allow);
}

#[test]
fn bool_compares_a_variable_as_written() {
    let p = deny_if(r#"{"Bool":{"aws:SecureTransport":"${aws:PrincipalTag/flag}"}}"#);
    let ctx = Ctx::empty()
        .with("aws:PrincipalTag/flag", vec!["true"])
        .with("aws:SecureTransport", vec!["true"]);
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
