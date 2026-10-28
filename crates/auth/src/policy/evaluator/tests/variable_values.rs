// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Text that comes from a variable value or a default: it is not expanded a
//! second time, and a `*` or `?` in it is not a wildcard. Also the two
//! condition-value rules, the `${key, 'default'}` form and no expansion for a
//! multivalued key, under the set operators, `IfExists` and `Null`.
//!
//! None of these expectations was measured on its own. Each follows from a
//! rule the PR #535 body table measured with the IAM policy simulator on
//! 2026-10-24: text from a value is matched literally (`team=bl*` matches only
//! `table/bl*`), and a condition value naming `dynamodb:LeadingKeys` is not
//! expanded under `StringEquals`. The default form in a condition value was
//! not measured.

use super::*;

const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/";

fn table(name: &str) -> String {
    format!("{ARN}{name}")
}

fn doc(statement: &str) -> PolicyDocument {
    parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#
    ))
}

fn allow_all() -> PolicyDocument {
    doc(r#"{"Effect":"Allow","Action":"dynamodb:*","Resource":"*"}"#)
}

/// `Allow dynamodb:*` on `*` beside a `Deny dynamodb:GetItem` on
/// `table/<resource>`.
fn deny_get_on(resource: &str) -> Vec<PolicyDocument> {
    vec![
        allow_all(),
        doc(&format!(
            r#"{{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"{ARN}{resource}"}}"#
        )),
    ]
}

/// `Allow dynamodb:*` on `*` beside a `Deny dynamodb:GetItem` on `*` whose
/// condition compares `key` with `value` under `operator`.
fn deny_get_if(operator: &str, key: &str, value: &str) -> Vec<PolicyDocument> {
    vec![
        allow_all(),
        doc(&format!(
            r#"{{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
                "Condition":{{"{operator}":{{"{key}":"{value}"}}}}}}"#
        )),
    ]
}

/// Only an `Allow dynamodb:GetItem` on every table whose condition compares
/// `key` with `value` under `operator`.
fn allow_get_if(operator: &str, key: &str, value: &str) -> Vec<PolicyDocument> {
    vec![doc(&format!(
        r#"{{"Effect":"Allow","Action":"dynamodb:GetItem","Resource":"{ARN}*",
            "Condition":{{"{operator}":{{"{key}":"{value}"}}}}}}"#
    ))]
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

// --- A substituted value is not expanded again ---

#[test]
fn a_resource_value_holding_a_variable_is_not_expanded_again() {
    // A resource tag can hold any text. The value `${aws:username}` names the
    // table `${aws:username}`, not the caller's own table.
    let p = deny_get_on("${aws:ResourceTag/alias}");
    let ctx = Ctx::empty()
        .with("aws:ResourceTag/alias", vec!["${aws:username}"])
        .with("aws:username", vec!["alice"]);
    assert_eq!(get(&p, "alice", &ctx), AuthzDecision::Allow);
    assert_eq!(get(&p, "${aws:username}", &ctx), AuthzDecision::Deny);
}

#[test]
fn a_condition_value_holding_a_variable_is_not_expanded_again() {
    // `${aws:UserAgent}` expands to the text `${aws:username}`, which is
    // compared as written, so the user name `alice` does not equal it.
    let p = deny_get_if("StringEquals", "aws:username", "${aws:UserAgent}");
    let ctx = Ctx::empty()
        .with("aws:UserAgent", vec!["${aws:username}"])
        .with("aws:username", vec!["alice"]);
    assert_eq!(get(&p, "T", &ctx), AuthzDecision::Allow);
    let equal = Ctx::empty()
        .with("aws:UserAgent", vec!["alice"])
        .with("aws:username", vec!["alice"]);
    assert_eq!(get(&p, "T", &equal), AuthzDecision::Deny);
}

// --- A `*` or `?` in a default is literal ---

#[test]
fn a_wildcard_in_a_resource_default_is_literal() {
    let star = deny_get_on("${aws:PrincipalTag/team, '*'}");
    assert_eq!(get(&star, "x", &Ctx::empty()), AuthzDecision::Allow);
    assert_eq!(get(&star, "*", &Ctx::empty()), AuthzDecision::Deny);
    let question = deny_get_on("${aws:PrincipalTag/team, 'b?'}");
    assert_eq!(get(&question, "bx", &Ctx::empty()), AuthzDecision::Allow);
    assert_eq!(get(&question, "b?", &Ctx::empty()), AuthzDecision::Deny);
}

// --- The default form in a condition value, under every operator form ---

/// A request on a table tagged `owner=<owner>` by a caller with no
/// `aws:username`.
fn owned_by(owner: &str) -> Ctx {
    Ctx::empty().with("aws:ResourceTag/owner", vec![owner])
}

#[test]
fn the_default_form_expands_under_set_operators_and_if_exists() {
    for operator in [
        "ForAnyValue:StringEquals",
        "ForAllValues:StringEquals",
        "StringEqualsIfExists",
    ] {
        let p = allow_get_if(operator, "aws:ResourceTag/owner", "${aws:username, 'anon'}");
        assert_eq!(
            get(&p, "T", &owned_by("anon")),
            AuthzDecision::Allow,
            "{operator}"
        );
        assert_eq!(
            get(&p, "T", &owned_by("alice")),
            AuthzDecision::Deny,
            "{operator}"
        );
    }
}

#[test]
fn the_default_form_expands_in_a_null_condition() {
    // With no `flag` tag the value is `false`, so the Allow needs `team`.
    let p = allow_get_if(
        "Null",
        "aws:PrincipalTag/team",
        "${aws:PrincipalTag/flag, 'false'}",
    );
    let with_team = Ctx::empty().with("aws:PrincipalTag/team", vec!["blue"]);
    assert_eq!(get(&p, "T", &with_team), AuthzDecision::Allow);
    assert_eq!(get(&p, "T", &Ctx::empty()), AuthzDecision::Deny);
}

// --- A multivalued key in a condition value, under every operator form ---

#[test]
fn a_multivalued_key_in_a_condition_value_is_not_expanded_under_any_form() {
    // Expanded to its first value, `${dynamodb:LeadingKeys}` would equal the
    // request's own key `blue` and each Deny would apply.
    let ctx = Ctx::empty()
        .with("dynamodb:LeadingKeys", vec!["blue"])
        .with("aws:PrincipalTag/team", vec!["blue"]);
    for (operator, key) in [
        ("ForAnyValue:StringEquals", "dynamodb:LeadingKeys"),
        ("ForAllValues:StringEquals", "dynamodb:LeadingKeys"),
        ("StringEqualsIfExists", "aws:PrincipalTag/team"),
    ] {
        let p = deny_get_if(operator, key, "${dynamodb:LeadingKeys}");
        assert_eq!(get(&p, "T", &ctx), AuthzDecision::Allow, "{operator}");
    }
    // Under `Null`, the first value `false` would say that `team` is present,
    // which it is, and the Deny would apply.
    let null = Ctx::empty()
        .with("dynamodb:LeadingKeys", vec!["false"])
        .with("aws:PrincipalTag/team", vec!["blue"]);
    let p = deny_get_if("Null", "aws:PrincipalTag/team", "${dynamodb:LeadingKeys}");
    assert_eq!(get(&p, "T", &null), AuthzDecision::Allow, "Null");
}
