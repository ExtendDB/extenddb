// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the three-case condition rule: a key the context resolves (case
//! 1), a key genuinely absent (case 2), and a key the service populates but the
//! context cannot resolve (case 3, fail closed).

use super::*;
use crate::policy::condition::{
    ConditionMatch, evaluate_condition_match, evaluate_statement_conditions,
};

#[test]
fn present_key_matches_or_not() {
    let ctx = TestContext::new().with("aws:PrincipalArn", vec!["arn:a"]);
    let eq = cond(
        ConditionOperator::StringEquals,
        "aws:PrincipalArn",
        vec!["arn:a"],
    );
    assert_eq!(
        evaluate_condition_match(&eq, true, &ctx),
        ConditionMatch::Match
    );
    let ne = cond(
        ConditionOperator::StringEquals,
        "aws:PrincipalArn",
        vec!["arn:b"],
    );
    assert_eq!(
        evaluate_condition_match(&ne, true, &ctx),
        ConditionMatch::NoMatch
    );
}

#[test]
fn absent_key_keeps_iam_semantics() {
    // Case 2: a genuinely absent single-valued key. A positive operator does
    // not match; a negated operator matches (the negated-absent rule).
    let ctx = TestContext::new();
    let pos = cond(
        ConditionOperator::StringEquals,
        "aws:PrincipalTag/team",
        vec!["x"],
    );
    assert_eq!(
        evaluate_condition_match(&pos, true, &ctx),
        ConditionMatch::NoMatch
    );
    let neg = cond(
        ConditionOperator::StringNotEquals,
        "aws:PrincipalTag/team",
        vec!["x"],
    );
    assert_eq!(
        evaluate_condition_match(&neg, true, &ctx),
        ConditionMatch::Match
    );
}

#[test]
fn unresolvable_key_is_unresolvable_for_every_operator() {
    // Case 3: the context reports the key as service-populated but unresolved.
    // Every operator yields Unresolvable, including a negated one (so the
    // negated-absent rule cannot fire) and Null.
    let ctx = TestContext::new().unresolvable("aws:PrincipalOrgID");
    for op in [
        ConditionOperator::StringEquals,
        ConditionOperator::StringNotEquals,
        ConditionOperator::Null,
    ] {
        let c = cond(op, "aws:PrincipalOrgID", vec!["o-1"]);
        assert_eq!(
            evaluate_condition_match(&c, true, &ctx),
            ConditionMatch::Unresolvable
        );
    }
}

#[test]
fn variable_naming_unresolvable_key_is_unresolvable_with_or_without_default() {
    // Case 3 read through a condition value: the variable has no value to
    // expand to, and the service, which has one, would not use the default.
    // Every operator that expands the value (the string and ARN ones) yields
    // Unresolvable, so a Deny applies and an Allow does not grant whatever the
    // operator says. A later value of the condition that names the key counts
    // as the first one does.
    let ctx = TestContext::new()
        .with("aws:PrincipalTag/team", vec!["blue"])
        .unresolvable("aws:PrincipalOrgID");
    for values in [
        vec!["${aws:PrincipalOrgID}"],
        vec!["${aws:PrincipalOrgID, 'blue'}"],
        vec!["${aws:PrincipalOrgID,'blue'}"],
        vec!["x-${aws:PrincipalOrgID, 'blue'}-y"],
        vec!["red", "${aws:PrincipalOrgID, 'blue'}"],
    ] {
        for op in [
            ConditionOperator::StringEquals,
            ConditionOperator::StringNotEquals,
            ConditionOperator::StringLike,
        ] {
            let c = cond(op, "aws:PrincipalTag/team", values.clone());
            assert_eq!(
                evaluate_condition_match(&c, true, &ctx),
                ConditionMatch::Unresolvable,
                "{values:?}"
            );
        }
    }
    // The key present: its value is used, not the default.
    let present = TestContext::new()
        .with("aws:PrincipalTag/team", vec!["blue"])
        .with("aws:PrincipalOrgID", vec!["blue"])
        .unresolvable("aws:PrincipalOrgID");
    let c = cond(
        ConditionOperator::StringEquals,
        "aws:PrincipalTag/team",
        vec!["${aws:PrincipalOrgID, 'red'}"],
    );
    assert_eq!(
        evaluate_condition_match(&c, true, &present),
        ConditionMatch::Match
    );
    // The key absent, not unresolvable: the default is used.
    let absent = TestContext::new().with("aws:PrincipalTag/team", vec!["blue"]);
    let c = cond(
        ConditionOperator::StringEquals,
        "aws:PrincipalTag/team",
        vec!["${aws:PrincipalOrgID, 'blue'}"],
    );
    assert_eq!(
        evaluate_condition_match(&c, true, &absent),
        ConditionMatch::Match
    );
}

#[test]
fn resolved_unresolvable_key_uses_its_value() {
    // If the context both lists a key as unresolvable and resolves it, the
    // resolved value wins (the guard only fires when resolve_key is None).
    let ctx = TestContext::new()
        .with("aws:PrincipalArn", vec!["arn:a"])
        .unresolvable("aws:PrincipalArn");
    let c = cond(
        ConditionOperator::StringEquals,
        "aws:PrincipalArn",
        vec!["arn:a"],
    );
    assert_eq!(
        evaluate_condition_match(&c, true, &ctx),
        ConditionMatch::Match
    );
}

#[test]
fn statement_nomatch_dominates_unresolvable() {
    // AND of conditions: a definite NoMatch means the statement does not apply,
    // even if another condition is unresolvable.
    let ctx = TestContext::new()
        .with("aws:PrincipalTag/team", vec!["employees"])
        .unresolvable("aws:PrincipalOrgID");
    let conds = vec![
        cond(
            ConditionOperator::StringEquals,
            "aws:PrincipalTag/team",
            vec!["contractors"],
        ),
        cond(
            ConditionOperator::StringEquals,
            "aws:PrincipalOrgID",
            vec!["o-1"],
        ),
    ];
    assert_eq!(
        evaluate_statement_conditions(&conds, true, &ctx),
        ConditionMatch::NoMatch
    );
}

#[test]
fn statement_unresolvable_when_only_unresolvable_present() {
    let ctx = TestContext::new()
        .with("aws:PrincipalTag/team", vec!["contractors"])
        .unresolvable("aws:PrincipalOrgID");
    let conds = vec![
        cond(
            ConditionOperator::StringEquals,
            "aws:PrincipalTag/team",
            vec!["contractors"],
        ),
        cond(
            ConditionOperator::StringEquals,
            "aws:PrincipalOrgID",
            vec!["o-1"],
        ),
    ];
    assert_eq!(
        evaluate_statement_conditions(&conds, true, &ctx),
        ConditionMatch::Unresolvable
    );
}

#[test]
fn statement_all_match() {
    let ctx = TestContext::new().with("aws:PrincipalTag/team", vec!["contractors"]);
    let conds = vec![cond(
        ConditionOperator::StringEquals,
        "aws:PrincipalTag/team",
        vec!["contractors"],
    )];
    assert_eq!(
        evaluate_statement_conditions(&conds, true, &ctx),
        ConditionMatch::Match
    );
    // An empty condition block always matches.
    assert_eq!(
        evaluate_statement_conditions(&[], true, &ctx),
        ConditionMatch::Match
    );
}
