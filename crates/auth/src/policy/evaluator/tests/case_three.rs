// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Identity-policy evaluation of a key the service populates but the context
//! cannot resolve (case 3): a Deny fails closed and blocks, an Allow gated on
//! it does not grant, and the negated-operator-on-absent rule cannot fire.

use super::*;

const RES: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/T";

/// Allow dynamodb:* plus a Deny GetItem gated on StringEquals aws:PrincipalOrgID.
fn allow_all_deny_get_on_orgid() -> PolicyDocument {
    parse(
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"dynamodb:*","Resource":"*"},
            {"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
             "Condition":{"StringEquals":{"aws:PrincipalOrgID":"o-example1"}}}
        ]}"#,
    )
}

#[test]
fn deny_on_unresolvable_orgid_blocks_fail_closed() {
    // The org id is populated by the service but unresolvable here, so the Deny
    // fails closed and blocks GetItem.
    let ctx = Ctx::empty().unresolvable("aws:PrincipalOrgID");
    assert_eq!(
        evaluate_policies(
            &[allow_all_deny_get_on_orgid()],
            None,
            None,
            "dynamodb:GetItem",
            RES,
            &ctx
        ),
        AuthzDecision::Deny
    );
    // A different action the Deny does not name is still allowed.
    assert_eq!(
        evaluate_policies(
            &[allow_all_deny_get_on_orgid()],
            None,
            None,
            "dynamodb:PutItem",
            RES,
            &ctx
        ),
        AuthzDecision::Allow
    );
}

#[test]
fn allow_gated_on_unresolvable_orgid_does_not_grant() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{"StringEquals":{"aws:PrincipalOrgID":"o-example1"}}
        }]}"#,
    );
    let ctx = Ctx::empty().unresolvable("aws:PrincipalOrgID");
    assert_eq!(
        evaluate_policies(&[policy], None, None, "dynamodb:GetItem", RES, &ctx),
        AuthzDecision::Deny
    );
}

#[test]
fn negated_allow_on_unresolvable_key_does_not_overgrant() {
    // StringNotEquals on an unresolvable key must not grant via the
    // negated-absent rule: the Allow does not grant, so the request is denied.
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{"StringNotEquals":{"aws:PrincipalOrgID":"o-example1"}}
        }]}"#,
    );
    let ctx = Ctx::empty().unresolvable("aws:PrincipalOrgID");
    assert_eq!(
        evaluate_policies(&[policy], None, None, "dynamodb:GetItem", RES, &ctx),
        AuthzDecision::Deny
    );
}

#[test]
fn negated_deny_on_unresolvable_key_fails_closed() {
    // A Deny with a negated operator on an unresolvable key fails closed and
    // blocks (over-denies relative to the service, the documented safe
    // direction) rather than being dropped.
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"dynamodb:*","Resource":"*"},
            {"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
             "Condition":{"StringNotEquals":{"aws:PrincipalOrgID":"o-example1"}}}
        ]}"#,
    );
    let ctx = Ctx::empty().unresolvable("aws:PrincipalOrgID");
    assert_eq!(
        evaluate_policies(&[policy], None, None, "dynamodb:GetItem", RES, &ctx),
        AuthzDecision::Deny
    );
}

#[test]
fn resolved_principal_arn_allows_normally() {
    // aws:PrincipalArn is resolvable on the identity path; a matching Allow
    // grants (not fail closed).
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{"StringEquals":{"aws:PrincipalArn":"arn:aws:iam::123456789012:user/alice"}}
        }]}"#,
    );
    let ctx = Ctx::empty().with(
        "aws:PrincipalArn",
        vec!["arn:aws:iam::123456789012:user/alice"],
    );
    assert_eq!(
        evaluate_policies(&[policy], None, None, "dynamodb:GetItem", RES, &ctx),
        AuthzDecision::Allow
    );
}
