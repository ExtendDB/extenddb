// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A quoted default in a policy variable: a `}` inside the quotes does not
//! end the reference, and a doubled `''` is one `'`. Each row repeats a
//! measurement with the IAM policy simulator on 2026-11-09, two samples per
//! row with the same answer: `Version` `2012-10-17`, `Allow dynamodb:GetItem`
//! on `*` with `StringEquals` on `aws:ResourceTag/owner`, and no
//! `aws:username` in the request, so the default is used.

use super::*;

const OWNER: &str = "aws:ResourceTag/owner";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/";

fn allow(element: &str) -> Vec<PolicyDocument> {
    vec![parse(
        &serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [serde_json::from_str::<serde_json::Value>(element).unwrap()],
        })
        .to_string(),
    )]
}

fn condition(value: &str) -> Vec<PolicyDocument> {
    allow(
        &serde_json::json!({
            "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"StringEquals": {OWNER: value}},
        })
        .to_string(),
    )
}

fn get(policies: &[PolicyDocument], table: &str, owner: Option<&str>) -> AuthzDecision {
    let ctx = match owner {
        Some(o) => Ctx::empty().with(OWNER, vec![o]),
        None => Ctx::empty(),
    };
    evaluate_policies(
        policies,
        None,
        None,
        "dynamodb:GetItem",
        &format!("{TABLE}{table}"),
        &ctx,
    )
}

#[test]
fn a_quoted_brace_does_not_end_the_reference() {
    let rows: &[(&str, &str, &[&str])] = &[
        ("a${aws:username, '}x'}", "a}x", &["a", "a}x'}"]),
        ("a${aws:username, '}'}", "a}", &["a", "a}x'}"]),
        ("a${aws:username,'}'}", "a}", &[]),
        ("a${aws:username, 'x${'}", "ax${", &["a", "a}x'}"]),
        ("a${aws:username, 'a,b'}", "aa,b", &[]),
        ("a${aws:username, ' '}", "a ", &["a"]),
        ("a${\taws:username\t, 'z'}", "az", &[]),
    ];
    for &(value, allowed, denied) in rows {
        let p = condition(value);
        assert_eq!(
            get(&p, "blue", Some(allowed)),
            AuthzDecision::Allow,
            "{value}"
        );
        for owner in denied {
            assert_eq!(
                get(&p, "blue", Some(owner)),
                AuthzDecision::Deny,
                "{value} {owner}"
            );
        }
    }
}

#[test]
fn a_doubled_quote_in_a_default_is_one_quote() {
    let rows: &[(&str, &str, &[&str])] = &[
        ("a${aws:username, 'x''y'}", "ax'y", &["ax''y", "axy"]),
        ("a${aws:username, ''''}", "a'", &["a''", "a"]),
        ("a${aws:username, 'x'''}", "ax'", &["ax"]),
        ("a${aws:username, '''x'}", "a'x", &["ax"]),
        ("a${aws:username, '\\'}'", "a\\'", &["a\\", "a\\}'"]),
    ];
    for &(value, allowed, denied) in rows {
        let p = condition(value);
        assert_eq!(
            get(&p, "blue", Some(allowed)),
            AuthzDecision::Allow,
            "{value}"
        );
        for owner in denied {
            assert_eq!(
                get(&p, "blue", Some(owner)),
                AuthzDecision::Deny,
                "{value} {owner}"
            );
        }
    }
}

#[test]
fn a_quoted_brace_in_a_resource_default() {
    let p = allow(
        &serde_json::json!({
            "Effect": "Allow", "Action": "dynamodb:GetItem",
            "Resource": format!("{TABLE}${{aws:username, '}}x'}}"),
        })
        .to_string(),
    );
    assert_eq!(get(&p, "}x", None), AuthzDecision::Allow);
    assert_eq!(get(&p, "blue", None), AuthzDecision::Deny);
}

/// An escape decides before a default: `${*, 'x'}` is a literal `*`. A key
/// name that only starts like an escape uses its default. Simulator rows,
/// two samples each.
#[test]
fn an_escape_with_a_default_is_the_escape() {
    let rows: &[(&str, &str, &[&str])] = &[
        ("a${*, 'x'}", "a*", &["ax"]),
        ("a${?, 'x'}", "a?", &["ax"]),
        ("a${$, 'x'}", "a$", &["ax"]),
        ("a${ * , 'x'}", "a*", &["ax"]),
        ("a${\u{1}*\u{1}, 'x'}", "a*", &["ax"]),
        ("a${*, '}'}", "a*", &["a}"]),
        ("a${*x, 'y'}", "ay", &[]),
    ];
    for &(value, allowed, denied) in rows {
        let p = condition(value);
        assert_eq!(
            get(&p, "blue", Some(allowed)),
            AuthzDecision::Allow,
            "{value}"
        );
        for owner in denied {
            assert_eq!(
                get(&p, "blue", Some(owner)),
                AuthzDecision::Deny,
                "{value} {owner}"
            );
        }
    }
    let p = allow(
        &serde_json::json!({
            "Effect": "Allow", "Action": "dynamodb:GetItem",
            "Resource": format!("{TABLE}${{*, 'x'}}"),
        })
        .to_string(),
    );
    assert_eq!(get(&p, "*", None), AuthzDecision::Allow);
    assert_eq!(get(&p, "x", None), AuthzDecision::Deny);
}

/// A reference whose text starts with `}` is accepted at write and matches
/// nothing at evaluation: its default is not used. Simulator rows, two
/// samples each.
#[test]
fn a_reference_starting_with_a_brace_uses_no_default() {
    for (value, owner) in [
        ("a${}, 'x'}", "ax"),
        ("a${ }, 'x'}", "ax"),
        ("a${}x, 'y'}", "ay"),
        ("a${}a, 'x'}", "ax"),
        ("a${}, '}'}", "a}"),
        ("a${}aws:username, '}'}", "a}"),
        ("a${}}", "a}"),
    ] {
        assert_eq!(
            get(&condition(value), "blue", Some(owner)),
            AuthzDecision::Deny,
            "{value}"
        );
        assert_eq!(
            get(&condition(value), "blue", Some("a")),
            AuthzDecision::Deny,
            "{value}"
        );
    }
    let not_equals = allow(
        &serde_json::json!({
            "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"StringNotEquals": {OWNER: "a${}, 'x'}"}},
        })
        .to_string(),
    );
    assert_eq!(get(&not_equals, "blue", Some("ax")), AuthzDecision::Allow);
    let p = allow(
        &serde_json::json!({
            "Effect": "Allow", "Action": "dynamodb:GetItem",
            "Resource": format!("{TABLE}${{}}, 'x'}}"),
        })
        .to_string(),
    );
    assert_eq!(get(&p, "x", None), AuthzDecision::Deny);
}

/// An Arn value counts its fields with each `${` ending at the next `}`, as
/// IAM does: `${x, '}:::::'}` has five written `:` after `${x, '}`, so it is
/// not short of fields, and `ArnNotLike` with it holds for the caller
/// (simulator, two samples per `Version`, allowed).
#[test]
fn an_arn_value_counts_fields_at_the_first_brace() {
    let p = allow(
        &serde_json::json!({
            "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"ArnNotLike": {"aws:PrincipalArn": "${x, '}:::::'}"}},
        })
        .to_string(),
    );
    let ctx = Ctx::empty().with(
        "aws:PrincipalArn",
        vec!["arn:aws:iam::123456789012:user/alice"],
    );
    let d = evaluate_policies(
        &p,
        None,
        None,
        "dynamodb:GetItem",
        &format!("{TABLE}blue"),
        &ctx,
    );
    assert_eq!(d, AuthzDecision::Allow);
}
