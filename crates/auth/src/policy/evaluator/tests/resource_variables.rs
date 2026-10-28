// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy variables in `Resource` and `NotResource` (issue #533).
//!
//! One test per row of the issue's table, each measured with the IAM policy
//! simulator on 2026-10-24 with `dynamodb:GetItem`, plus the escapes, the
//! default form in a condition value, the `2008-10-17` and no-`Version`
//! documents for `NotResource`, and the boundary and session policy paths.

use super::*;

const TEAM: &str = "aws:PrincipalTag/team";
const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/";

fn table(name: &str) -> String {
    format!("{ARN}{name}")
}

fn allow_all() -> PolicyDocument {
    parse(
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
        }]}"#,
    )
}

/// `Allow dynamodb:* on *` beside a `Deny dynamodb:GetItem` on `resource`.
fn allow_all_deny_get(version: Option<&str>, element: &str, resource: &str) -> Vec<PolicyDocument> {
    let version = version.map_or(String::new(), |v| format!(r#""Version":"{v}","#));
    vec![
        allow_all(),
        parse(&format!(
            r#"{{{version}"Statement":[{{
                "Effect":"Deny","Action":"dynamodb:GetItem","{element}":"{ARN}{resource}"
            }}]}}"#
        )),
    ]
}

fn get(policies: &[PolicyDocument], target: &str, ctx: &Ctx) -> AuthzDecision {
    evaluate_policies(
        policies,
        None,
        None,
        "dynamodb:GetItem",
        &table(target),
        ctx,
    )
}

fn team_blue() -> Ctx {
    Ctx::empty().with(TEAM, vec!["blue"])
}

// --- Rows 1 to 3: Deny Resource table/${aws:PrincipalTag/team} ---

#[test]
fn row1_deny_variable_resource_denies_own_team_table() {
    let p = allow_all_deny_get(Some("2012-10-17"), "Resource", "${aws:PrincipalTag/team}");
    assert_eq!(get(&p, "blue", &team_blue()), AuthzDecision::Deny);
}

#[test]
fn row2_deny_variable_resource_leaves_other_table_allowed() {
    let p = allow_all_deny_get(Some("2012-10-17"), "Resource", "${aws:PrincipalTag/team}");
    assert_eq!(get(&p, "red", &team_blue()), AuthzDecision::Allow);
}

#[test]
fn row3_unresolved_variable_resource_denies_nothing() {
    let p = allow_all_deny_get(Some("2012-10-17"), "Resource", "${aws:PrincipalTag/team}");
    assert_eq!(get(&p, "blue", &Ctx::empty()), AuthzDecision::Allow);
}

// --- Row 4: wildcard after the variable ---

#[test]
fn row4_wildcard_after_variable_still_matches() {
    let p = allow_all_deny_get(Some("2012-10-17"), "Resource", "${aws:PrincipalTag/team}*");
    assert_eq!(get(&p, "blue-x", &team_blue()), AuthzDecision::Deny);
    assert_eq!(get(&p, "red-x", &team_blue()), AuthzDecision::Allow);
}

// --- Rows 5 and 6: Allow only table/${aws:username} ---

fn allow_own_table() -> Vec<PolicyDocument> {
    vec![parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{{
            "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"{ARN}${{aws:username}}"
        }}]}}"#
    ))]
}

#[test]
fn row5_allow_variable_resource_grants_own_table() {
    let ctx = Ctx::empty().with("aws:username", vec!["alice"]);
    assert_eq!(get(&allow_own_table(), "alice", &ctx), AuthzDecision::Allow);
}

#[test]
fn row6_allow_variable_resource_does_not_grant_other_table() {
    let ctx = Ctx::empty().with("aws:username", vec!["alice"]);
    assert_eq!(get(&allow_own_table(), "bob", &ctx), AuthzDecision::Deny);
}

// --- Rows 7 and 8: 2008-10-17 and no Version match the text as written ---

#[test]
fn row7_version_2008_does_not_expand_resource() {
    let p = allow_all_deny_get(Some("2008-10-17"), "Resource", "${aws:PrincipalTag/team}");
    assert_eq!(get(&p, "blue", &team_blue()), AuthzDecision::Allow);
}

#[test]
fn row8_no_version_does_not_expand_resource() {
    let p = allow_all_deny_get(None, "Resource", "${aws:PrincipalTag/team}");
    assert_eq!(get(&p, "blue", &team_blue()), AuthzDecision::Allow);
}

// --- Rows 9 and 10: Deny NotResource table/${aws:PrincipalTag/team} ---

#[test]
fn row9_deny_not_resource_variable_spares_own_team_table() {
    let p = allow_all_deny_get(
        Some("2012-10-17"),
        "NotResource",
        "${aws:PrincipalTag/team}",
    );
    assert_eq!(get(&p, "blue", &team_blue()), AuthzDecision::Allow);
}

#[test]
fn row10_deny_not_resource_variable_denies_other_table() {
    let p = allow_all_deny_get(
        Some("2012-10-17"),
        "NotResource",
        "${aws:PrincipalTag/team}",
    );
    assert_eq!(get(&p, "red", &team_blue()), AuthzDecision::Deny);
}

// --- Row 11: default form ---

#[test]
fn row11_default_form_expands_when_key_absent() {
    let p = allow_all_deny_get(
        Some("2012-10-17"),
        "Resource",
        "${aws:PrincipalTag/team, 'none'}",
    );
    assert_eq!(get(&p, "none", &Ctx::empty()), AuthzDecision::Deny);
    assert_eq!(get(&p, "blue", &Ctx::empty()), AuthzDecision::Allow);
    // With the key present the value wins over the default.
    assert_eq!(get(&p, "blue", &team_blue()), AuthzDecision::Deny);
    assert_eq!(get(&p, "none", &team_blue()), AuthzDecision::Allow);
}

// --- Row 12: ${*} is a literal star, not a wildcard (measured) ---

#[test]
fn row12_escaped_star_in_resource_is_literal() {
    let p = allow_all_deny_get(Some("2012-10-17"), "Resource", "${*}");
    assert_eq!(get(&p, "x", &Ctx::empty()), AuthzDecision::Allow);
    assert_eq!(get(&p, "*", &Ctx::empty()), AuthzDecision::Deny);
}

#[test]
fn escaped_question_and_dollar_in_resource_are_literal() {
    let q = allow_all_deny_get(Some("2012-10-17"), "Resource", "${?}");
    assert_eq!(get(&q, "x", &Ctx::empty()), AuthzDecision::Allow);
    assert_eq!(get(&q, "?", &Ctx::empty()), AuthzDecision::Deny);
    let d = allow_all_deny_get(Some("2012-10-17"), "Resource", "${$}x");
    assert_eq!(get(&d, "$x", &Ctx::empty()), AuthzDecision::Deny);
    assert_eq!(get(&d, "x", &Ctx::empty()), AuthzDecision::Allow);
}

// --- NotResource with 2008-10-17, no Version, and an unresolved variable ---

#[test]
fn not_resource_version_2008_matches_text_as_written() {
    // Unexpanded, the pattern matches no real table, so every table is
    // "not in" it and the Deny applies to all of them.
    let p = allow_all_deny_get(
        Some("2008-10-17"),
        "NotResource",
        "${aws:PrincipalTag/team}",
    );
    assert_eq!(get(&p, "blue", &team_blue()), AuthzDecision::Deny);
    assert_eq!(get(&p, "red", &team_blue()), AuthzDecision::Deny);
}

#[test]
fn not_resource_no_version_matches_text_as_written() {
    let p = allow_all_deny_get(None, "NotResource", "${aws:PrincipalTag/team}");
    assert_eq!(get(&p, "blue", &team_blue()), AuthzDecision::Deny);
}

#[test]
fn not_resource_unresolved_variable_applies_to_every_resource() {
    // Derived from the measured rule, not measured: a pattern with an
    // unresolved variable matches nothing, so every resource is outside it.
    let p = allow_all_deny_get(
        Some("2012-10-17"),
        "NotResource",
        "${aws:PrincipalTag/team}",
    );
    assert_eq!(get(&p, "blue", &Ctx::empty()), AuthzDecision::Deny);
}

#[test]
fn allow_not_resource_with_unresolved_variable_grants_every_table() {
    // Measured: an `Allow` alone, with no `team` tag. The pattern matches
    // nothing, so every table is outside it and the Allow grants both.
    let p = vec![parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{{
            "Effect":"Allow","Action":"dynamodb:GetItem","NotResource":"{ARN}${{aws:PrincipalTag/team}}"
        }}]}}"#
    ))];
    assert_eq!(get(&p, "blue", &Ctx::empty()), AuthzDecision::Allow);
    assert_eq!(get(&p, "red", &Ctx::empty()), AuthzDecision::Allow);
}

#[test]
fn default_form_without_a_space_expands_when_key_absent() {
    // Measured: no space after the comma.
    let p = allow_all_deny_get(
        Some("2012-10-17"),
        "Resource",
        "${aws:PrincipalTag/team,'none'}",
    );
    assert_eq!(get(&p, "none", &Ctx::empty()), AuthzDecision::Deny);
}

// --- Default form in a condition value ---

#[test]
fn condition_value_default_form_expands_when_key_absent() {
    let p = parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{{
            "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"{ARN}*",
            "Condition":{{"StringEquals":{{"aws:ResourceTag/owner":"${{aws:username, 'anon'}}"}}}}
        }}]}}"#
    ));
    let p = std::slice::from_ref(&p);
    let owned_by_anon = Ctx::empty().with("aws:ResourceTag/owner", vec!["anon"]);
    assert_eq!(get(p, "T", &owned_by_anon), AuthzDecision::Allow);
    let owned_by_alice = Ctx::empty().with("aws:ResourceTag/owner", vec!["alice"]);
    assert_eq!(get(p, "T", &owned_by_alice), AuthzDecision::Deny);
    let alice_on_alice = owned_by_alice.with("aws:username", vec!["alice"]);
    assert_eq!(get(p, "T", &alice_on_alice), AuthzDecision::Allow);
}

// --- Boundary and session policy paths ---

fn allow_own_table_doc() -> PolicyDocument {
    allow_own_table().remove(0)
}

#[test]
fn permissions_boundary_expands_resource_variable() {
    let ctx = Ctx::empty().with("aws:username", vec!["alice"]);
    let boundary = allow_own_table_doc();
    let decide = |target: &str| {
        evaluate_policies(
            &[allow_all()],
            Some(&boundary),
            None,
            "dynamodb:GetItem",
            &table(target),
            &ctx,
        )
    };
    assert_eq!(decide("alice"), AuthzDecision::Allow);
    assert_eq!(decide("bob"), AuthzDecision::Deny);
}

#[test]
fn session_policy_expands_resource_variable() {
    let ctx = Ctx::empty().with("aws:username", vec!["alice"]);
    let session = allow_own_table_doc();
    let decide = |target: &str| {
        evaluate_policies(
            &[allow_all()],
            None,
            Some(&session),
            "dynamodb:GetItem",
            &table(target),
            &ctx,
        )
    };
    assert_eq!(decide("alice"), AuthzDecision::Allow);
    assert_eq!(decide("bob"), AuthzDecision::Deny);
}

#[test]
fn explicit_deny_matches_arc_expands_resource_variable() {
    use std::sync::Arc;
    let p: Vec<Arc<PolicyDocument>> =
        allow_all_deny_get(Some("2012-10-17"), "Resource", "${aws:PrincipalTag/team}")
            .into_iter()
            .map(Arc::new)
            .collect();
    let deny_on = |target: &str, ctx: &Ctx| {
        explicit_deny_matches_arc(&p, None, None, "dynamodb:GetItem", &table(target), ctx)
    };
    assert!(deny_on("blue", &team_blue()));
    assert!(!deny_on("red", &team_blue()));
    assert!(!deny_on("blue", &Ctx::empty()));
}
