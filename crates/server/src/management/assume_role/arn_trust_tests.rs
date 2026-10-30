// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Arn condition values in role trust policies, at write and at evaluation.
//!
//! Live IAM rejects at `CreateRole` and `UpdateAssumeRolePolicy` a trust
//! policy whose Arn condition value does not split into six ARN fields, with
//! `MalformedPolicyDocument` and a message naming the value, and it reads a
//! JSON number or boolean as its text, so it rejects one the same way. It
//! accepted every six-field value tried, an empty partition included. Measured
//! on 2026-10-30 under `ArnEquals`, `ArnNotEquals`, `ArnLike` and
//! `ArnNotLike`. A shorter value can only be one stored before ExtendDB
//! checked it, and it fails closed: a Deny blocks and an Allow does not grant.

use super::tests::ctx;
use super::{trust_decision, validate_trust_policy};
use serde_json::{Value, json};

const CALLER: &str = "arn:aws:iam::123456789012:user/alice";

/// Values the service rejected in a trust policy Arn condition.
const SHORT: [&str; 4] = [
    "*",
    "",
    "arn:aws:dynamodb:us-east-1:123456789012",
    "arn:aws:iam:*",
];

fn stmt(effect: &str, principal: &str, condition: Option<Value>) -> Value {
    let mut s = json!({"Effect": effect, "Principal": {"AWS": principal},
        "Action": "sts:AssumeRole"});
    if let Some(c) = condition {
        s["Condition"] = c;
    }
    s
}

fn arn_cond(op: &str, value: &str) -> Value {
    json!({op: {"aws:PrincipalArn": value}})
}

fn doc(statements: &[Value]) -> Value {
    json!({"Version": "2012-10-17", "Statement": statements})
}

#[test]
fn write_rejects_a_short_value_with_the_service_message() {
    for op in ["ArnEquals", "ArnNotEquals"] {
        for v in SHORT {
            let d = doc(&[stmt("Allow", "*", Some(arn_cond(op, v)))]);
            let want = format!(
                "MalformedPolicyDocument: value used for ARN condition is incorrect type: \
                 STRING\\nstring: \\\"{v}\\\"\\n"
            );
            assert_eq!(validate_trust_policy(&d), Err(want), "{op} {v:?}");
        }
    }
}

#[test]
fn write_rejects_a_number_or_boolean_with_the_service_message() {
    // Measured at `UpdateAssumeRolePolicy` under `ArnEquals`, twice each:
    // 123, true and 1.5 were rejected with their text in the message.
    for (value, text) in [
        (json!(123), "123"),
        (json!(true), "true"),
        (json!(1.5), "1.5"),
        (json!(["arn:aws:iam::123456789012:user/alice", 123]), "123"),
    ] {
        let cond = json!({"ArnEquals": {"aws:PrincipalArn": value}});
        let d = doc(&[stmt("Allow", "*", Some(cond))]);
        let want = format!(
            "MalformedPolicyDocument: value used for ARN condition is incorrect type: \
             STRING\\nstring: \\\"{text}\\\"\\n"
        );
        assert_eq!(validate_trust_policy(&d), Err(want), "{text}");
    }
}

#[test]
fn write_accepts_six_field_values() {
    for v in [
        "arn:aws:iam::123456789012:user/alice",
        "arn:aws:iam::123456789012:user/${aws:username}",
        "arn::dynamodb:us-east-1:123456789012:table/t",
        "*:*:*:*:*:*",
        "arn:aws:dynamodb:us-east-1:123456789012:",
    ] {
        let d = doc(&[stmt("Allow", "*", Some(arn_cond("ArnEquals", v)))]);
        assert_eq!(validate_trust_policy(&d), Ok(()), "{v:?}");
    }
}

#[test]
fn stored_short_value_under_an_allow_does_not_grant() {
    // With the IAM matcher alone each of these holds for the caller (a short
    // value that matches nothing negates to true), so the Allow would grant.
    for op in ["ArnNotEquals", "ArnNotLike", "ArnNotEqualsIfExists"] {
        for v in ["*", "", "arn:aws:dynamodb:*"] {
            let s = [stmt("Allow", "*", Some(arn_cond(op, v)))];
            assert!(!trust_decision(&s, CALLER, &ctx(), false), "{op} {v:?}");
        }
    }
}

#[test]
fn stored_short_value_under_a_deny_blocks() {
    // With the IAM matcher alone none of these matches the caller, so the
    // Deny would not apply and the unconditional Allow would grant.
    for op in ["ArnEquals", "ArnLike", "ForAnyValue:ArnLike"] {
        for v in ["*", "", "arn:aws:dynamodb:*"] {
            let s = [
                stmt("Allow", CALLER, None),
                stmt("Deny", "*", Some(arn_cond(op, v))),
            ];
            assert!(!trust_decision(&s, CALLER, &ctx(), false), "{op} {v:?}");
        }
    }
}

#[test]
fn six_field_values_still_evaluate() {
    let bob = "arn:aws:iam::123456789012:user/bob";
    let allow = [stmt("Allow", "*", Some(arn_cond("ArnNotEquals", bob)))];
    assert!(trust_decision(&allow, CALLER, &ctx(), false));
    let deny_other = [
        stmt("Allow", CALLER, None),
        stmt("Deny", "*", Some(arn_cond("ArnEquals", bob))),
    ];
    assert!(trust_decision(&deny_other, CALLER, &ctx(), false));
    let deny_caller = [
        stmt("Allow", CALLER, None),
        stmt(
            "Deny",
            "*",
            Some(arn_cond("ArnLike", "arn:aws:iam::*:user/ali*")),
        ),
    ];
    assert!(!trust_decision(&deny_caller, CALLER, &ctx(), false));
}
