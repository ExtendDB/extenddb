// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How the text between `${` and `}` is read: the code points up to U+0020
//! are trimmed around the key name, around the comma of the default form and
//! before the closing brace, and around an escape, in `Resource` and in a
//! condition value. The text inside the quotes of a default keeps its spaces.
//!
//! Each test repeats rows measured with the IAM policy simulator on
//! 2026-11-03, `dynamodb:GetItem` with `Allow dynamodb:*` on `*` beside each
//! `Deny`, two samples per row with the same answer. The row names are in
//! the comments.

use super::*;
use serde_json::json;

const TEAM: &str = "aws:PrincipalTag/team";
const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/";

/// `Allow dynamodb:*` on `*` and `Deny dynamodb:GetItem` with `deny_fields`.
fn allow_all_and_deny(deny_fields: &serde_json::Value) -> Vec<PolicyDocument> {
    let mut deny = json!({"Effect": "Deny", "Action": "dynamodb:GetItem"});
    for (field, value) in deny_fields.as_object().unwrap() {
        deny[field] = value.clone();
    }
    let document = json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}, deny],
    });
    vec![parse(&document.to_string())]
}

fn deny_get_on(resource: &str) -> Vec<PolicyDocument> {
    allow_all_and_deny(&json!({"Resource": format!("{ARN}{resource}")}))
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
fn spaces_around_the_key_are_trimmed() {
    // W1, W2, W3.
    for written in [
        "${ aws:PrincipalTag/team }",
        "${aws:PrincipalTag/team }",
        "${ aws:PrincipalTag/team}",
    ] {
        let p = deny_get_on(written);
        assert_eq!(
            get(&p, "blue", &team("blue")),
            AuthzDecision::Deny,
            "{written}"
        );
        assert_eq!(
            get(&p, "red", &team("blue")),
            AuthzDecision::Allow,
            "{written}"
        );
    }
    // W1b: the table named with the text as written is not the one denied.
    let p = deny_get_on("${ aws:PrincipalTag/team }");
    assert_eq!(
        get(&p, "${ aws:PrincipalTag/team }", &team("blue")),
        AuthzDecision::Allow
    );
}

#[test]
fn spaces_around_the_comma_and_the_brace_are_trimmed() {
    // W4 and W5: spaces on both sides of the key and before the comma.
    let p = deny_get_on("${ aws:PrincipalTag/team , 'none'}");
    assert_eq!(get(&p, "blue", &team("blue")), AuthzDecision::Deny);
    assert_eq!(get(&p, "none", &Ctx::empty()), AuthzDecision::Deny);
    // W6: a space before the comma and none after it.
    let p = deny_get_on("${aws:PrincipalTag/team ,'none'}");
    assert_eq!(get(&p, "blue", &team("blue")), AuthzDecision::Deny);
    // W7: two spaces after the comma.
    let p = deny_get_on("${aws:PrincipalTag/team,  'none'}");
    assert_eq!(get(&p, "none", &Ctx::empty()), AuthzDecision::Deny);
    // W8: a space before the closing brace.
    let p = deny_get_on("${aws:PrincipalTag/team, 'none' }");
    assert_eq!(get(&p, "none", &Ctx::empty()), AuthzDecision::Deny);
}

#[test]
fn spaces_inside_the_quotes_of_a_default_are_kept() {
    // T4 and T4b.
    let p = deny_get_on("${aws:PrincipalTag/team, ' none '}");
    assert_eq!(get(&p, " none ", &Ctx::empty()), AuthzDecision::Deny);
    assert_eq!(get(&p, "none", &Ctx::empty()), AuthzDecision::Allow);
}

#[test]
fn code_points_up_to_space_are_trimmed_and_no_others() {
    // T1 (tab), T2 (newline, before the key only), T5 (U+000B), T6 (U+0001),
    // T7 (U+000C), T8 (U+000D) and T9 (U+001F) are trimmed.
    for (before, after) in [
        ("\t", "\t"),
        ("\n", ""),
        ("\u{b}", "\u{b}"),
        ("\u{1}", "\u{1}"),
        ("\u{c}", "\u{c}"),
        ("\r", "\r"),
        ("\u{1f}", "\u{1f}"),
    ] {
        let p = deny_get_on(&format!("${{{before}{TEAM}{after}}}"));
        assert_eq!(
            get(&p, "blue", &team("blue")),
            AuthzDecision::Deny,
            "{before:?}"
        );
    }
    // T3 (U+00A0, before the key only), T10 (U+0085) and T11 (U+3000) are
    // not, so the key the variable names is not in the request.
    for (before, after) in [
        ("\u{a0}", ""),
        ("\u{85}", "\u{85}"),
        ("\u{3000}", "\u{3000}"),
    ] {
        let p = deny_get_on(&format!("${{{before}{TEAM}{after}}}"));
        assert_eq!(
            get(&p, "blue", &team("blue")),
            AuthzDecision::Allow,
            "{before:?}"
        );
    }
}

#[test]
fn spaces_around_an_escape_are_trimmed() {
    // X1, X1b and the control X1c: `${ * }` stands for a literal `*`.
    let p = deny_get_on("${ * }");
    assert_eq!(get(&p, "*", &Ctx::empty()), AuthzDecision::Deny);
    assert_eq!(get(&p, "x", &Ctx::empty()), AuthzDecision::Allow);
    let p = deny_get_on("${*}");
    assert_eq!(get(&p, "*", &Ctx::empty()), AuthzDecision::Deny);
    // X2: `${ $ }` stands for a literal `$`.
    let p = deny_get_on("a${ $ }");
    assert_eq!(get(&p, "a$", &Ctx::empty()), AuthzDecision::Deny);
}

#[test]
fn spaces_around_the_key_are_trimmed_in_a_condition_value() {
    // W9.
    let p = allow_all_and_deny(&json!({
        "Resource": "*",
        "Condition": {"StringEquals": {"aws:ResourceTag/owner": "${ aws:username }"}},
    }));
    let ctx = Ctx::empty()
        .with("aws:username", vec!["alice"])
        .with("aws:ResourceTag/owner", vec!["alice"]);
    assert_eq!(get(&p, "x", &ctx), AuthzDecision::Deny);
}
