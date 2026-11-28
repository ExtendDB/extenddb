// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A bare negated operator over an absent `dynamodb:Attributes`, evaluated
//! against the request context the server builds.
//!
//! IAM matches a bare negated operator on an absent multivalued key, measured
//! on IAM for `dynamodb:Attributes`. The service sets no `dynamodb:Attributes`
//! value on a `SearchVectors` (measured 2026-11-21 in us-east-1: a Deny gated
//! on `Null` `true` refused every search, a projected one included), and
//! ExtendDB sets none either. On a `SearchVectors` the operator therefore
//! matches: a Deny gated on it refuses the search and an Allow gated on it
//! grants it. On the other operations ExtendDB leaves the key absent on
//! requests where the service populates it, so there the operator keeps the
//! no-match rule.

use super::*;
use crate::policy::context::{RequestContext, RequestParams};

const INDEX: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/t/index/v";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/t";
const NEGATED: [&str; 3] = [
    "StringNotEquals",
    "StringNotLike",
    "StringNotEqualsIgnoreCase",
];

fn policy(effect: &str, action: &str, operator: &str) -> Vec<PolicyDocument> {
    let mut statements = vec![serde_json::json!({
        "Effect": effect, "Action": action, "Resource": "*",
        "Condition": {operator: {"dynamodb:Attributes": "x"}},
    })];
    if effect == "Deny" {
        statements.push(serde_json::json!({
            "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*",
        }));
    }
    vec![parse(
        &serde_json::json!({"Version": "2012-10-17", "Statement": statements}).to_string(),
    )]
}

fn context(search: bool, attributes: Option<Vec<String>>) -> RequestContext {
    RequestContext::build(
        HashMap::new(),
        HashMap::new(),
        false,
        RequestParams {
            attributes,
            attributes_absent_on_service: search,
            ..Default::default()
        },
    )
}

#[test]
fn a_bare_negated_deny_refuses_every_search() {
    for operator in NEGATED {
        let policies = policy("Deny", "dynamodb:SearchVectors", operator);
        let decision = evaluate_policies(
            &policies,
            None,
            None,
            "dynamodb:SearchVectors",
            INDEX,
            &context(true, None),
        );
        assert_eq!(decision, AuthzDecision::Deny, "{operator} Deny on a search");
    }
}

#[test]
fn a_bare_negated_allow_grants_a_search() {
    for operator in NEGATED {
        let policies = policy("Allow", "dynamodb:SearchVectors", operator);
        let decision = evaluate_policies(
            &policies,
            None,
            None,
            "dynamodb:SearchVectors",
            INDEX,
            &context(true, None),
        );
        assert_eq!(
            decision,
            AuthzDecision::Allow,
            "{operator} Allow on a search"
        );
    }
}

#[test]
fn other_operations_keep_the_no_match_rule() {
    for (action, resource) in [("dynamodb:Scan", TABLE), ("dynamodb:GetItem", TABLE)] {
        for operator in NEGATED {
            let deny = policy("Deny", action, operator);
            let decision =
                evaluate_policies(&deny, None, None, action, resource, &context(false, None));
            assert_eq!(
                decision,
                AuthzDecision::Allow,
                "{operator} Deny on {action}"
            );
            let allow = policy("Allow", action, operator);
            let decision =
                evaluate_policies(&allow, None, None, action, resource, &context(false, None));
            assert_eq!(
                decision,
                AuthzDecision::Deny,
                "{operator} Allow on {action}"
            );
        }
    }
}

#[test]
fn leading_keys_keep_the_no_match_rule_on_a_search() {
    let ctx = context(true, None);
    assert!(ctx.bare_negated_matches_absent_multivalued_key("dynamodb:Attributes"));
    assert!(!ctx.bare_negated_matches_absent_multivalued_key("dynamodb:LeadingKeys"));
    assert!(
        !context(false, None).bare_negated_matches_absent_multivalued_key("dynamodb:Attributes")
    );
}
