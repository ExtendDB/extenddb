// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Combined multi-phase evaluation and FGAC (BR-7085) tests.

use super::*;

// --- Combined: boundary + session + conditions ---

#[test]
fn full_evaluation_all_phases() {
    let identity = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*",
                "Condition":{"StringEquals":{"aws:PrincipalTag/Team":"Alpha"}}
            }]}"#,
    );
    let boundary = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":["dynamodb:GetItem","dynamodb:Query"],"Resource":"*"
            }]}"#,
    );
    let session = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
    );
    let ctx = Ctx::empty().with("aws:PrincipalTag/Team", vec!["Alpha"]);

    // All phases pass
    assert_eq!(
        evaluate_policies(
            std::slice::from_ref(&identity),
            Some(&boundary),
            Some(&session),
            "dynamodb:GetItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &ctx
        ),
        AuthzDecision::Allow
    );

    // Wrong tag → identity policy condition fails
    let ctx_wrong = Ctx::empty().with("aws:PrincipalTag/Team", vec!["Beta"]);
    assert_eq!(
        evaluate_policies(
            &[identity],
            Some(&boundary),
            Some(&session),
            "dynamodb:GetItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &ctx_wrong
        ),
        AuthzDecision::Deny
    );
}

// --- Wildcard action matching ---

#[test]
fn wildcard_action() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
}

// --- Resource ARN matching ---

#[test]
fn resource_arn_restricts() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*",
                "Resource":"arn:aws:dynamodb:*:*:table/Users"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            std::slice::from_ref(&policy),
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/Users",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/Orders",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

// --- Multiple identity policies ---

#[test]
fn multiple_policies_any_allow() {
    let read_only = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
    );
    let write_only = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:PutItem","Resource":"*"
            }]}"#,
    );
    assert_eq!(
        evaluate_policies(
            &[read_only, write_only],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Allow
    );
}

// --- Empty policies ---

#[test]
fn no_policies_implicit_deny() {
    assert_eq!(
        evaluate_policies(
            &[],
            None,
            None,
            "dynamodb:PutItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &Ctx::empty()
        ),
        AuthzDecision::Deny
    );
}

// --- ForAllValues with leading keys (FGAC pattern) ---

#[test]
fn fgac_leading_keys() {
    let policy = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*",
                "Condition":{
                    "ForAllValues:StringEquals":{
                        "dynamodb:LeadingKeys":["user-123"]
                    }
                }
            }]}"#,
    );
    let ctx = Ctx::empty().with("dynamodb:LeadingKeys", vec!["user-123"]);
    assert_eq!(
        evaluate_policies(
            std::slice::from_ref(&policy),
            None,
            None,
            "dynamodb:GetItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &ctx
        ),
        AuthzDecision::Allow
    );

    let ctx_wrong = Ctx::empty().with("dynamodb:LeadingKeys", vec!["user-456"]);
    assert_eq!(
        evaluate_policies(
            &[policy],
            None,
            None,
            "dynamodb:GetItem",
            "arn:aws:dynamodb:us-east-1:123:table/T",
            &ctx_wrong
        ),
        AuthzDecision::Deny
    );
}

// --- BR-7085: explicit-Deny handling for the dynamodb:Attributes FGAC pattern ---

fn fgac_policies() -> [PolicyDocument; 2] {
    let allow = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    // Misconfigured denylist using a BARE StringEquals on the multivalued key.
    let deny_bare = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:*",
                "Resource":"arn:aws:dynamodb:us-east-1:123:table/Employees",
                "Condition":{"StringEquals":{"dynamodb:Attributes":["ssn"]}}
            }]}"#,
    );
    [allow, deny_bare]
}

fn eval_attrs(policies: &[PolicyDocument], attrs: Vec<&str>) -> AuthzDecision {
    let ctx = Ctx::empty().with("dynamodb:Attributes", attrs);
    evaluate_policies(
        policies,
        None,
        None,
        "dynamodb:GetItem",
        "arn:aws:dynamodb:us-east-1:123:table/Employees",
        &ctx,
    )
}

#[test]
fn bare_deny_on_attributes_is_a_noop_ssn_alone_allowed() {
    // Matches real AWS: requesting ssn alone is ALLOWED (bare Deny never fires).
    // ExtendDB previously DENIED this, creating a false sense of security.
    let p = fgac_policies();
    assert_eq!(eval_attrs(&p, vec!["ssn"]), AuthzDecision::Allow);
}

#[test]
fn bare_deny_on_attributes_is_a_noop_ssn_plus_fullname_allowed() {
    let p = fgac_policies();
    assert_eq!(
        eval_attrs(&p, vec!["ssn", "fullname"]),
        AuthzDecision::Allow
    );
}

#[test]
fn bare_deny_on_attributes_fullname_allowed() {
    let p = fgac_policies();
    assert_eq!(eval_attrs(&p, vec!["fullname"]), AuthzDecision::Allow);
}

#[test]
fn for_any_value_deny_on_attributes_is_effective() {
    // The CORRECT pattern: ForAnyValue:StringEquals denies whenever ssn is requested,
    // whether alone or alongside another attribute.
    let allow = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
    );
    let deny_any = parse(
        r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:*",
                "Resource":"arn:aws:dynamodb:us-east-1:123:table/Employees",
                "Condition":{"ForAnyValue:StringEquals":{"dynamodb:Attributes":["ssn"]}}
            }]}"#,
    );
    let p = [allow, deny_any];
    assert_eq!(eval_attrs(&p, vec!["ssn"]), AuthzDecision::Deny);
    assert_eq!(eval_attrs(&p, vec!["ssn", "fullname"]), AuthzDecision::Deny);
    assert_eq!(eval_attrs(&p, vec!["fullname"]), AuthzDecision::Allow);
}
