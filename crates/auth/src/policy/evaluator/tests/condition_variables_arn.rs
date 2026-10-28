// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! An `Arn*` condition value that holds a policy variable and fewer than five
//! `:` written outside its `${...}` references, such as
//! `${aws:ResourceTag/blockedPrincipal}`. On 2026-10-28 the IAM policy
//! simulator refused the `ArnLike` Deny (cell A7) and the `ArnNotLike` Allow
//! (cell A8) below with `InvalidInput`, two samples each, and IAM Access
//! Analyzer reported `MISSING_ARN_FIELD` for the value. IAM gives no decision
//! for such a value, so ExtendDB fails the statement closed: a `Deny` applies
//! and an `Allow` does not grant. The `ArnEquals` and `ArnNotEquals` rows and
//! the qualified forms apply the same rule and were not sent to the simulator.

use super::*;

const BLUE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/blue";
const ALICE: &str = "arn:aws:iam::123456789012:user/alice";

fn allow_all() -> PolicyDocument {
    parse(
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
        }]}"#,
    )
}

fn statement(effect: &str, condition: &str) -> PolicyDocument {
    parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{{
            "Effect":"{effect}","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{condition}
        }}]}}"#
    ))
}

fn get(policies: &[PolicyDocument], ctx: &Ctx) -> AuthzDecision {
    evaluate_policies(policies, None, None, "dynamodb:GetItem", BLUE, ctx)
}

/// The caller `alice` on a table whose `blockedPrincipal` tag names her.
fn alice_blocked() -> Ctx {
    Ctx::empty()
        .with("aws:PrincipalArn", vec![ALICE])
        .with("aws:ResourceTag/blockedPrincipal", vec![ALICE])
}

/// The cells that are not `Deny` for each operator: A7 is `Allow dynamodb:*`
/// beside a `Deny` with the condition, A8 an `Allow` with the condition alone.
fn not_denied(operators: &[&str]) -> Vec<String> {
    let mut wrong = Vec::new();
    for op in operators {
        let condition =
            format!(r#"{{"{op}":{{"aws:PrincipalArn":"${{aws:ResourceTag/blockedPrincipal}}"}}}}"#);
        let a7 = vec![allow_all(), statement("Deny", &condition)];
        if get(&a7, &alice_blocked()) != AuthzDecision::Deny {
            wrong.push(format!("A7 {op}"));
        }
        let a8 = vec![statement("Allow", &condition)];
        if get(&a8, &alice_blocked()) != AuthzDecision::Deny {
            wrong.push(format!("A8 {op}"));
        }
    }
    wrong
}

#[test]
fn a_value_that_is_one_variable_fails_closed_under_every_arn_operator() {
    let wrong = not_denied(&["ArnLike", "ArnNotLike", "ArnEquals", "ArnNotEquals"]);
    assert!(wrong.is_empty(), "not denied: {wrong:?}");
}

#[test]
fn the_rule_reads_the_operator_inside_if_exists_and_the_set_qualifiers() {
    let wrong = not_denied(&[
        "ArnNotLikeIfExists",
        "ForAllValues:ArnNotLike",
        "ForAnyValue:ArnLike",
    ]);
    assert!(wrong.is_empty(), "not denied: {wrong:?}");
}

#[test]
fn a_variable_after_five_written_colons_still_matches() {
    // The value has its six fields, so the variable is expanded and matched as
    // before. `ArnLike` and `ArnEquals` on this shape are measured cells
    // (`arn_like_and_arn_equals_read_a_substituted_star_as_a_character`); the
    // `Not` forms are their negation.
    let condition = |op: &str| {
        format!(
            r#"{{"{op}":{{"aws:PrincipalArn":"arn:aws:iam::123456789012:user/${{aws:username}}"}}}}"#
        )
    };
    let caller = |name: &str| {
        Ctx::empty()
            .with("aws:PrincipalArn", vec![ALICE])
            .with("aws:username", vec![name])
    };
    for (op, alice, bob) in [
        ("ArnLike", AuthzDecision::Deny, AuthzDecision::Allow),
        ("ArnEquals", AuthzDecision::Deny, AuthzDecision::Allow),
        ("ArnNotLike", AuthzDecision::Allow, AuthzDecision::Deny),
        ("ArnNotEquals", AuthzDecision::Allow, AuthzDecision::Deny),
    ] {
        let p = vec![allow_all(), statement("Deny", &condition(op))];
        assert_eq!(get(&p, &caller("alice")), alice, "{op} alice");
        assert_eq!(get(&p, &caller("bob")), bob, "{op} bob");
    }
}
