// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy variables in `Resource`: multivalued keys, variables outside the
//! resource field, a `:` inside a value, and the wildcards beside a variable
//! (issue #533). Every expected decision was measured with the IAM policy
//! simulator on 2026-10-24 with `dynamodb:GetItem`.

use super::*;

const P: &str = "arn:aws:dynamodb:us-east-1:123456789012";
const LEADING_KEYS: &str = "dynamodb:LeadingKeys";

fn allow_all_and(statement: &str) -> Vec<PolicyDocument> {
    vec![
        parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
        ),
        parse(&format!(
            r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#
        )),
    ]
}

fn deny_get_on(resource: &str) -> Vec<PolicyDocument> {
    allow_all_and(&format!(
        r#"{{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"{resource}"}}"#
    ))
}

fn get(policies: &[PolicyDocument], target: &str, ctx: &Ctx) -> AuthzDecision {
    evaluate_policies(policies, None, None, "dynamodb:GetItem", target, ctx)
}

fn leading_keys(values: Vec<&str>) -> Ctx {
    Ctx::empty().with(LEADING_KEYS, values)
}

// --- A multivalued key is never a variable (cells C1, D1) ---

#[test]
fn a_deny_on_a_multivalued_key_variable_denies_no_table() {
    let p = deny_get_on(&format!("{P}:table/${{dynamodb:LeadingKeys}}"));
    for ctx in [
        leading_keys(vec!["blue", "red"]),
        leading_keys(vec!["blue"]),
    ] {
        assert_eq!(
            get(&p, &format!("{P}:table/blue"), &ctx),
            AuthzDecision::Allow
        );
        assert_eq!(
            get(&p, &format!("{P}:table/red"), &ctx),
            AuthzDecision::Allow
        );
    }
}

#[test]
fn an_allow_on_a_multivalued_key_variable_grants_no_table() {
    let p = vec![parse(&format!(
        r#"{{"Version":"2012-10-17","Statement":[{{
            "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"{P}:table/${{dynamodb:LeadingKeys}}"
        }}]}}"#
    ))];
    let ctx = leading_keys(vec!["blue", "red"]);
    assert_eq!(
        get(&p, &format!("{P}:table/blue"), &ctx),
        AuthzDecision::Deny
    );
}

#[test]
fn a_condition_value_naming_a_multivalued_key_is_not_expanded() {
    let p = allow_all_and(
        r#"{"Effect":"Deny","Action":"dynamodb:GetItem","Resource":"*",
            "Condition":{"StringEquals":{"aws:PrincipalTag/team":"${dynamodb:LeadingKeys}"}}}"#,
    );
    for values in [vec!["blue", "red"], vec!["blue"]] {
        let ctx = leading_keys(values).with("aws:PrincipalTag/team", vec!["blue"]);
        assert_eq!(get(&p, &format!("{P}:table/x"), &ctx), AuthzDecision::Allow);
    }
}

// --- Variables before the fifth colon expand (cells C2, D3) ---

#[test]
fn a_variable_in_the_region_field_expands() {
    let p = deny_get_on("arn:aws:dynamodb:${aws:RequestedRegion}:123456789012:table/blue");
    let ctx = Ctx::empty().with("aws:RequestedRegion", vec!["us-east-1"]);
    assert_eq!(
        get(&p, &format!("{P}:table/blue"), &ctx),
        AuthzDecision::Deny
    );
}

#[test]
fn a_variable_in_the_account_field_expands() {
    let p = deny_get_on("arn:aws:dynamodb:us-east-1:${aws:PrincipalAccount}:table/blue");
    let ctx = Ctx::empty().with("aws:PrincipalAccount", vec!["123456789012"]);
    assert_eq!(
        get(&p, &format!("{P}:table/blue"), &ctx),
        AuthzDecision::Deny
    );
}

// --- A ':' inside a value stays in its field (cells C3, D2) ---

#[test]
fn a_colon_in_a_region_field_value_does_not_separate_fields() {
    let p = deny_get_on("arn:aws:dynamodb:${aws:PrincipalTag/region}:*:table/T");
    let ctx = Ctx::empty().with("aws:PrincipalTag/region", vec!["us-east-1:123456789012"]);
    assert_eq!(get(&p, &format!("{P}:table/T"), &ctx), AuthzDecision::Allow);
    assert_eq!(
        get(&p, &format!("{P}:table/A/stream/x:table/T"), &ctx),
        AuthzDecision::Allow
    );
}

#[test]
fn a_colon_in_a_resource_field_value_matches_the_same_name() {
    let p = deny_get_on(&format!("{P}:table/${{aws:PrincipalTag/team}}"));
    let ctx = Ctx::empty().with("aws:PrincipalTag/team", vec!["blue:x"]);
    assert_eq!(
        get(&p, &format!("{P}:table/blue:x"), &ctx),
        AuthzDecision::Deny
    );
    assert_eq!(
        get(&p, &format!("{P}:table/blue"), &ctx),
        AuthzDecision::Allow
    );
}

// --- Wildcards beside a variable (cells C9, D4) ---

#[test]
fn an_unresolved_variable_before_a_wildcard_matches_nothing() {
    // An unresolved variable makes the pattern match nothing; expanding it to
    // the empty string would leave `table/*` and deny every table.
    let p = deny_get_on(&format!("{P}:table/${{aws:PrincipalTag/team}}*"));
    assert_eq!(
        get(&p, &format!("{P}:table/blue"), &Ctx::empty()),
        AuthzDecision::Allow
    );
}

#[test]
fn a_question_mark_written_beside_a_variable_is_a_wildcard() {
    let p = deny_get_on(&format!("{P}:table/b?${{aws:PrincipalTag/team}}"));
    let ctx = Ctx::empty().with("aws:PrincipalTag/team", vec!["ue"]);
    assert_eq!(
        get(&p, &format!("{P}:table/blue"), &ctx),
        AuthzDecision::Deny
    );
}

// --- A wildcard inside a value is literal ---

#[test]
fn a_wildcard_in_a_value_matches_only_the_same_text() {
    let p = deny_get_on(&format!("{P}:table/${{aws:PrincipalTag/team}}"));
    let ctx = Ctx::empty().with("aws:PrincipalTag/team", vec!["bl*"]);
    assert_eq!(
        get(&p, &format!("{P}:table/blue"), &ctx),
        AuthzDecision::Allow
    );
    assert_eq!(
        get(&p, &format!("{P}:table/bl*"), &ctx),
        AuthzDecision::Deny
    );
}
