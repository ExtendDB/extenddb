// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The four `Arn*` operators on a value that holds a policy variable, through
//! the evaluator. The value is matched on the tokens of PR #541, so text from
//! a principal tag is matched as characters of its field, and the pattern
//! follows the field rules of `arn_value::arn_condition_match_tokens`, the
//! same rules as a value written in full (issue #665).

use super::*;

const PRINCIPAL: &str = "aws:PrincipalArn";
const BOB: &str = "arn:aws:iam::123456789012:user/bob";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/t";
const OPERATORS: [&str; 4] = ["ArnLike", "ArnEquals", "ArnNotLike", "ArnNotEquals"];

/// `GetItem` by bob, whose principal tags are `tags`, under one statement
/// with `effect` on `dynamodb:GetItem` and `*` that holds when `operator` on
/// `aws:PrincipalArn` holds for `value`. A `Deny` statement comes with an
/// unconditional `Allow`.
fn get(effect: &str, operator: &str, value: &str, tags: &[(&str, &str)]) -> AuthzDecision {
    let gated = format!(
        r#"{{"Effect":"{effect}","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{{"{operator}":{{"{PRINCIPAL}":"{value}"}}}}}}"#
    );
    let statements = if effect == "Deny" {
        format!(r#"{{"Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"}},{gated}"#)
    } else {
        gated
    };
    let policy = parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{statements}]}}"#
    ));
    let mut ctx = Ctx::empty().with(PRINCIPAL, vec![BOB]);
    for (key, tag) in tags {
        ctx = ctx.with(&format!("aws:PrincipalTag/{key}"), vec![tag]);
    }
    evaluate_policies(&[policy], None, None, "dynamodb:GetItem", TABLE, &ctx)
}

/// Whether `operator` holds for bob, read from an `Allow` gated on it.
fn holds(operator: &str, value: &str, tags: &[(&str, &str)]) -> bool {
    get("Allow", operator, value, tags) == AuthzDecision::Allow
}

#[test]
fn a_tag_value_holding_a_star_is_matched_as_the_character() {
    let value = "arn:aws:iam::123456789012:user/${aws:PrincipalTag/name}";
    // A tag value of `*` names the user `*`, not every user.
    assert!(!holds("ArnLike", value, &[("name", "*")]));
    assert!(!holds("ArnEquals", value, &[("name", "*")]));
    assert!(holds("ArnNotLike", value, &[("name", "*")]));
    assert!(holds("ArnNotEquals", value, &[("name", "*")]));
    assert_eq!(
        get("Deny", "ArnNotLike", value, &[("name", "b*")]),
        AuthzDecision::Deny
    );
    // The tag value `bob` names bob.
    assert!(holds("ArnLike", value, &[("name", "bob")]));
    assert!(!holds("ArnNotEquals", value, &[("name", "bob")]));
}

#[test]
fn a_pattern_with_a_variable_follows_the_rules_of_one_written_in_full() {
    let pairs = [
        (
            "arn:aws:iam::123456789012:user/${aws:PrincipalTag/name}",
            "arn:aws:iam::123456789012:user/bob",
        ),
        (
            "arn:*:iam::*:user/${aws:PrincipalTag/name}",
            "arn:*:iam::*:user/bob",
        ),
        (
            "arn:aws:iam::123456789012:${aws:PrincipalTag/res}",
            "arn:aws:iam::123456789012:user/bob",
        ),
        (
            "arn:aws:iam::123456789012:user/${aws:PrincipalTag/other}",
            "arn:aws:iam::123456789012:user/alice",
        ),
    ];
    let tags = [("name", "bob"), ("res", "user/bob"), ("other", "alice")];
    for (with_variable, written) in pairs {
        for op in OPERATORS {
            assert_eq!(
                holds(op, with_variable, &tags),
                holds(op, written, &tags),
                "{op} {with_variable}"
            );
        }
    }
    // A short value written in full follows the measured rules.
    assert!(holds("ArnLike", "arn:aws:iam:*", &tags));
    assert!(!holds("ArnNotEquals", "arn:aws:iam", &tags));
    assert!(!holds("ArnEquals", "*", &tags));
    assert!(holds("ArnNotLike", "*", &tags));
}

/// `GetItem` under `statements` for a request context `ctx`.
fn decide(statements: &str, ctx: &Ctx) -> AuthzDecision {
    let policy = parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{statements}]}}"#
    ));
    evaluate_policies(&[policy], None, None, "dynamodb:GetItem", TABLE, ctx)
}

/// One statement with `effect` on `dynamodb:GetItem` and `*` that holds when
/// `operator` on `aws:PrincipalArn` holds for `value`.
fn gated(effect: &str, operator: &str, value: &str) -> String {
    format!(
        r#"{{"Effect":"{effect}","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{{"{operator}":{{"{PRINCIPAL}":"{value}"}}}}}}"#
    )
}

const ALLOW_ALL: &str = r#"{"Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"}"#;

#[test]
fn the_only_me_policy_loads_and_evaluates_as_written() {
    // IAM rejects this value at write (the variable's own `:` puts `${aws` in
    // the account field), so ExtendDB refuses it at write too. A policy
    // stored before that check loads and matches the caller's own ARN.
    let value = "arn:aws:iam::${aws:PrincipalAccount}:user/${aws:username}";
    let statement = gated("Allow", "ArnEquals", value);
    let json = format!(r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#);
    assert!(PolicyDocument::from_json_for_write(&json).is_err());
    let caller = |user: &str| {
        Ctx::empty()
            .with(PRINCIPAL, vec![BOB])
            .with("aws:PrincipalAccount", vec!["123456789012"])
            .with("aws:username", vec![user])
    };
    assert_eq!(decide(&statement, &caller("bob")), AuthzDecision::Allow);
    assert_eq!(decide(&statement, &caller("alice")), AuthzDecision::Deny);
    let deny = format!("{ALLOW_ALL},{}", gated("Deny", "ArnEquals", value));
    assert_eq!(decide(&deny, &caller("bob")), AuthzDecision::Deny);
    assert_eq!(decide(&deny, &caller("alice")), AuthzDecision::Allow);
}

#[test]
fn a_tag_value_holding_a_star_does_not_widen_the_account_field() {
    let value = "arn:aws:iam::${aws:PrincipalTag/acct}:user/bob";
    assert!(!holds("ArnLike", value, &[("acct", "*")]));
    assert!(holds("ArnNotLike", value, &[("acct", "*")]));
    assert!(holds("ArnLike", value, &[("acct", "123456789012")]));
    assert_eq!(
        get("Deny", "ArnNotEquals", value, &[("acct", "*")]),
        AuthzDecision::Deny
    );
}

#[test]
fn a_tag_value_holding_a_colon_does_not_add_fields() {
    // Three written fields and a variable: the tag's `:` would make the
    // expanded text bob's six-field ARN. Only a written `:` separates fields,
    // so the value has too few fields and its condition fails closed.
    let value = "arn:aws:${aws:PrincipalTag/svc}";
    let tags = [("svc", "iam::123456789012:user/bob")];
    for op in OPERATORS {
        assert!(!holds(op, value, &tags), "{op}");
        assert_eq!(get("Deny", op, value, &tags), AuthzDecision::Deny, "{op}");
    }
}

#[test]
fn a_stored_value_with_no_meaning_fails_closed() {
    // Values the write check refuses that no ARN can match, and a short
    // pattern with a wildcard in its first field, whose evaluation by IAM was
    // not measured: a Deny holding one applies and an Allow does not grant,
    // under the positive and the negative operators.
    for value in [
        "",
        "foo:*",
        "ARN:aws:iam::123456789012:user/bob",
        "*:aws:iam:*",
        "ar?:aws:iam",
    ] {
        for op in OPERATORS {
            assert!(!holds(op, value, &[]), "{op} {value:?}");
            assert_eq!(
                get("Deny", op, value, &[]),
                AuthzDecision::Deny,
                "{op} {value:?}"
            );
        }
    }
}

#[test]
fn a_stored_value_iam_rejects_at_write_otherwise_evaluates_as_written() {
    // An empty partition and `/` in the service field match no ARN, as on
    // main, so the negative operators hold for them.
    for value in [
        "arn::iam::123456789012:user/bob",
        "arn:*:role/x",
        "arn:aws:IAM:*",
    ] {
        assert!(!holds("ArnLike", value, &[]), "{value:?}");
        assert!(holds("ArnNotLike", value, &[]), "{value:?}");
    }
}
