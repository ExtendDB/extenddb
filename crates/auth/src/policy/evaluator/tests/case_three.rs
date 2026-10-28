// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Identity-policy evaluation of a key the service populates but the context
//! cannot resolve (case 3): a Deny fails closed and blocks, an Allow gated on
//! it does not grant, and the negated-operator-on-absent rule cannot fire. The
//! same holds when the key is named by a `${key}` variable in `Resource` or
//! `NotResource` of a `2012-10-17` document; a `2008-10-17` or no-`Version`
//! document matches such a pattern as written. A variable naming the key in a
//! condition value of a `2012-10-17` document makes the value match nothing,
//! with or without a default, and the condition does not fail closed.
//!
//! Every expectation here is an ExtendDB rule with no service measurement:
//! the service has a value for such a key, so there is no service case to
//! compare with, and no key is listed in `FAIL_CLOSED_GLOBAL_KEYS` in this
//! build.

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

// --- A `${key}` variable in Resource or NotResource naming such a key ---

const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/";

fn only(statement: &str) -> PolicyDocument {
    parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#
    ))
}

/// `Allow dynamodb:*` on `*` beside `statement`.
fn allow_all_and(statement: &str) -> Vec<PolicyDocument> {
    vec![
        only(r#"{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}"#),
        only(statement),
    ]
}

/// GetItem on table `T` with `aws:PrincipalOrgID` unresolvable.
fn get_t(policies: &[PolicyDocument]) -> AuthzDecision {
    let ctx = Ctx::empty().unresolvable("aws:PrincipalOrgID");
    evaluate_policies(policies, None, None, "dynamodb:GetItem", RES, &ctx)
}

#[test]
fn deny_naming_an_unresolvable_key_in_resource_blocks_fail_closed() {
    // As for the Deny gated on the key in a Condition above. The default is
    // not used, because the service would have a value for the key.
    for resource in ["${aws:PrincipalOrgID}", "${aws:PrincipalOrgID, 'none'}"] {
        let p = allow_all_and(&format!(
            r#"{{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"{TABLE}{resource}"}}"#
        ));
        assert_eq!(get_t(&p), AuthzDecision::Deny, "{resource}");
    }
    let p = allow_all_and(&format!(
        r#"{{"Effect":"Deny","Action":"dynamodb:GetItem","NotResource":"{TABLE}${{aws:PrincipalOrgID}}"}}"#
    ));
    assert_eq!(get_t(&p), AuthzDecision::Deny);
}

#[test]
fn allow_naming_an_unresolvable_key_in_resource_does_not_grant() {
    // Read as an absent key, the default would grant `table/T` and the
    // NotResource would grant every table.
    for (element, resource) in [
        ("Resource", "${aws:PrincipalOrgID, 'T'}"),
        ("NotResource", "${aws:PrincipalOrgID}"),
    ] {
        let p = [only(&format!(
            r#"{{"Effect":"Allow","Action":"dynamodb:GetItem","{element}":"{TABLE}{resource}"}}"#
        ))];
        assert_eq!(get_t(&p), AuthzDecision::Deny, "{element} {resource}");
    }
}

#[test]
fn a_pattern_that_decides_alone_wins_over_an_unresolvable_one() {
    // `table/T` in the list decides whatever the unresolvable key holds, and
    // so does an absent key with no default, which makes its pattern match
    // nothing.
    let allow = [only(&format!(
        r#"{{"Effect":"Allow","Action":"dynamodb:GetItem","Resource":["{TABLE}${{aws:PrincipalOrgID}}","{TABLE}T"]}}"#
    ))];
    assert_eq!(get_t(&allow), AuthzDecision::Allow);
    let spared = allow_all_and(&format!(
        r#"{{"Effect":"Deny","Action":"dynamodb:GetItem","NotResource":["{TABLE}T","{TABLE}${{aws:PrincipalOrgID}}"]}}"#
    ));
    assert_eq!(get_t(&spared), AuthzDecision::Allow);
    let absent = allow_all_and(&format!(
        r#"{{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"{TABLE}${{aws:PrincipalTag/team}}${{aws:PrincipalOrgID}}"}}"#
    ));
    assert_eq!(get_t(&absent), AuthzDecision::Allow);
}

#[test]
fn an_unresolvable_key_in_a_2008_or_no_version_document_is_matched_as_written() {
    // Only a `2012-10-17` document expands variables, so here the pattern is
    // text: it names the table `${aws:PrincipalOrgID}` and no other, and the
    // Deny does not fail closed.
    let ctx = Ctx::empty().unresolvable("aws:PrincipalOrgID");
    let literal = format!("{TABLE}${{aws:PrincipalOrgID}}");
    for (label, version) in [
        ("2008-10-17", r#""Version":"2008-10-17","#),
        ("no Version", ""),
    ] {
        let deny = parse(&format!(
            r#"{{{version}"Statement":[{{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"{literal}"}}]}}"#
        ));
        let p = [
            only(r#"{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}"#),
            deny,
        ];
        assert_eq!(get_t(&p), AuthzDecision::Allow, "{label}");
        assert_eq!(
            evaluate_policies(&p, None, None, "dynamodb:GetItem", &literal, &ctx),
            AuthzDecision::Deny,
            "{label}"
        );
    }
}

// --- A `${key}` variable in a condition value naming such a key ---

/// GetItem on table `T` by a caller tagged `team=blue`, with `ctx` for the
/// other keys.
fn get_t_as_blue(policies: &[PolicyDocument], ctx: Ctx) -> AuthzDecision {
    let ctx = ctx.with("aws:PrincipalTag/team", vec!["blue"]);
    evaluate_policies(policies, None, None, "dynamodb:GetItem", RES, &ctx)
}

#[test]
fn a_condition_value_naming_an_unresolvable_key_matches_nothing() {
    let p = allow_all_and(
        r#"{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{"StringEquals":{"aws:PrincipalTag/team":"${aws:PrincipalOrgID, 'blue'}"}}}"#,
    );
    // Unresolvable: the value matches nothing, as for an absent key with no
    // default, so it does not match `blue` and the Deny does not apply; the
    // condition does not fail closed. The default is not used.
    let unresolvable = Ctx::empty().unresolvable("aws:PrincipalOrgID");
    assert_eq!(get_t_as_blue(&p, unresolvable), AuthzDecision::Allow);
    // Absent: the default `blue` is used and the Deny applies.
    assert_eq!(get_t_as_blue(&p, Ctx::empty()), AuthzDecision::Deny);
    // Present: the key's value is used, not the default.
    let org = |value| Ctx::empty().with("aws:PrincipalOrgID", vec![value]);
    assert_eq!(get_t_as_blue(&p, org("blue")), AuthzDecision::Deny);
    assert_eq!(get_t_as_blue(&p, org("red")), AuthzDecision::Allow);
}
