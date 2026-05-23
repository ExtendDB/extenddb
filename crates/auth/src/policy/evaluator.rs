// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM policy evaluation algorithm.
//!
//! Implements the 5-phase evaluation: explicit deny → permissions boundary →
//! session policy → identity allow → implicit deny. This is the same algorithm
//! used by real AWS IAM, supporting IBAC, RBAC, and ABAC patterns.

use super::condition::evaluate_condition;
use super::context::ConditionContext;
use super::document::{ActionMatch, Effect, PolicyDocument, ResourceMatch, Statement};
use super::matcher::{arn_match, wildcard_match_ignore_case};

/// The result of policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthzDecision {
    /// Request is allowed by an explicit Allow statement.
    Allow,
    /// Request is denied (explicit deny or implicit deny).
    Deny,
}

/// Evaluate policies using the 5-phase IAM evaluation algorithm.
///
/// 1. **Explicit Deny** — any Deny statement in any policy that matches → DENY.
/// 2. **Permissions Boundary** — if set, must find an Allow → else DENY.
/// 3. **Session Policy** — if set, must find an Allow → else DENY.
/// 4. **Identity Allow** — find an Allow in identity policies → ALLOW.
/// 5. **Implicit Deny** → DENY.
///
/// # Parameters
///
/// - `identity_policies`: user + group policies, or role policies.
/// - `permissions_boundary`: optional boundary policy on the user or role.
/// - `session_policy`: optional inline policy from AssumeRole.
/// - `action`: the DynamoDB action (e.g., "dynamodb:PutItem").
/// - `resource_arn`: the target resource ARN.
/// - `context`: condition context for evaluating condition blocks.
pub fn evaluate_policies(
    identity_policies: &[PolicyDocument],
    permissions_boundary: Option<&PolicyDocument>,
    session_policy: Option<&PolicyDocument>,
    action: &str,
    resource_arn: &str,
    context: &impl ConditionContext,
) -> AuthzDecision {
    evaluate_with(
        identity_policies.iter(),
        permissions_boundary,
        session_policy,
        action,
        resource_arn,
        context,
    )
}

/// Variant of [`evaluate_policies`] accepting `Arc<PolicyDocument>` slices.
///
/// Used by the request hot path with the parsed-document cache to avoid
/// cloning policies out of `Arc` on every authorization decision.
pub fn evaluate_policies_arc(
    identity_policies: &[std::sync::Arc<PolicyDocument>],
    permissions_boundary: Option<&PolicyDocument>,
    session_policy: Option<&PolicyDocument>,
    action: &str,
    resource_arn: &str,
    context: &impl ConditionContext,
) -> AuthzDecision {
    evaluate_with(
        identity_policies.iter().map(std::sync::Arc::as_ref),
        permissions_boundary,
        session_policy,
        action,
        resource_arn,
        context,
    )
}

fn evaluate_with<'a, I>(
    identity_policies: I,
    permissions_boundary: Option<&'a PolicyDocument>,
    session_policy: Option<&'a PolicyDocument>,
    action: &str,
    resource_arn: &str,
    context: &impl ConditionContext,
) -> AuthzDecision
where
    I: Iterator<Item = &'a PolicyDocument> + Clone,
{
    // Collect all policies for the explicit deny scan.
    let all_policies: Vec<&PolicyDocument> = identity_policies
        .clone()
        .chain(permissions_boundary)
        .chain(session_policy)
        .collect();

    // Phase 1: Explicit Deny — any Deny statement in any policy
    for policy in &all_policies {
        for stmt in &policy.statements {
            if stmt.effect == Effect::Deny
                && action_matches(stmt, action)
                && resource_matches(stmt, resource_arn)
                && conditions_match(stmt, context)
            {
                return AuthzDecision::Deny;
            }
        }
    }

    // Phase 2: Permissions Boundary — must find Allow (if boundary exists)
    if let Some(boundary) = permissions_boundary {
        let boundary_allows = boundary.statements.iter().any(|stmt| {
            stmt.effect == Effect::Allow
                && action_matches(stmt, action)
                && resource_matches(stmt, resource_arn)
                && conditions_match(stmt, context)
        });
        if !boundary_allows {
            return AuthzDecision::Deny;
        }
    }

    // Phase 3: Session Policy — must find Allow (if session policy exists)
    if let Some(session) = session_policy {
        let session_allows = session.statements.iter().any(|stmt| {
            stmt.effect == Effect::Allow
                && action_matches(stmt, action)
                && resource_matches(stmt, resource_arn)
                && conditions_match(stmt, context)
        });
        if !session_allows {
            return AuthzDecision::Deny;
        }
    }

    // Phase 4: Identity Policy Allow
    for policy in identity_policies {
        for stmt in &policy.statements {
            if stmt.effect == Effect::Allow
                && action_matches(stmt, action)
                && resource_matches(stmt, resource_arn)
                && conditions_match(stmt, context)
            {
                return AuthzDecision::Allow;
            }
        }
    }

    // Phase 5: Implicit Deny
    AuthzDecision::Deny
}

/// Check if the request action matches the statement's action constraint.
/// Action matching is case-insensitive per AWS IAM specification.
fn action_matches(statement: &Statement, request_action: &str) -> bool {
    match &statement.action_match {
        ActionMatch::Actions(patterns) => patterns
            .iter()
            .any(|p| wildcard_match_ignore_case(p, request_action)),
        ActionMatch::NotActions(patterns) => !patterns
            .iter()
            .any(|p| wildcard_match_ignore_case(p, request_action)),
    }
}

/// Check if the request resource matches the statement's resource constraint.
fn resource_matches(statement: &Statement, request_resource: &str) -> bool {
    match &statement.resource_match {
        ResourceMatch::Resources(patterns) => {
            patterns.iter().any(|p| arn_match(p, request_resource))
        }
        ResourceMatch::NotResources(patterns) => {
            !patterns.iter().any(|p| arn_match(p, request_resource))
        }
    }
}

/// Check if all conditions in the statement are satisfied.
fn conditions_match(statement: &Statement, context: &impl ConditionContext) -> bool {
    statement
        .conditions
        .iter()
        .all(|c| evaluate_condition(c, context))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::context::ConditionContext;
    use crate::policy::document::PolicyDocument;
    use std::collections::HashMap;

    /// Simple test context.
    struct Ctx(HashMap<String, Vec<String>>);

    impl Ctx {
        fn empty() -> Self {
            Self(HashMap::new())
        }

        fn with(mut self, key: &str, values: Vec<&str>) -> Self {
            self.0.insert(
                key.to_owned(),
                values.into_iter().map(ToOwned::to_owned).collect(),
            );
            self
        }
    }

    impl ConditionContext for Ctx {
        fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
            self.0
                .get(key)
                .map(|v| v.iter().map(|s| s.as_str()).collect())
        }
    }

    fn parse(json: &str) -> PolicyDocument {
        PolicyDocument::from_json(json).unwrap()
    }

    // --- Basic Allow/Deny ---

    #[test]
    fn simple_allow() {
        let policy = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:PutItem","Resource":"*"
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

    #[test]
    fn implicit_deny_no_matching_allow() {
        let policy = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
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
            AuthzDecision::Deny
        );
    }

    #[test]
    fn explicit_deny_overrides_allow() {
        let allow = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
        );
        let deny = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:DeleteTable","Resource":"*"
            }]}"#,
        );
        assert_eq!(
            evaluate_policies(
                &[allow, deny],
                None,
                None,
                "dynamodb:DeleteTable",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Deny
        );
    }

    // --- Case-insensitive action matching ---

    #[test]
    fn action_matching_is_case_insensitive() {
        let policy = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:putitem","Resource":"*"
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

    #[test]
    fn deny_case_insensitive() {
        let allow = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
        );
        let deny = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:deletetable","Resource":"*"
            }]}"#,
        );
        assert_eq!(
            evaluate_policies(
                &[allow, deny],
                None,
                None,
                "dynamodb:DeleteTable",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Deny
        );
    }

    // --- NotAction ---

    #[test]
    fn not_action_deny() {
        let policy = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","NotAction":["dynamodb:GetItem"],"Resource":"*"
            }]}"#,
        );
        // PutItem is not in the NotAction list, so the Deny applies
        assert_eq!(
            evaluate_policies(
                &[policy],
                None,
                None,
                "dynamodb:PutItem",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Deny
        );
    }

    #[test]
    fn not_action_deny_excluded_action_not_denied() {
        let deny = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","NotAction":["dynamodb:GetItem"],"Resource":"*"
            }]}"#,
        );
        let allow = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
        );
        // GetItem is excluded from the Deny, and explicitly allowed
        assert_eq!(
            evaluate_policies(
                &[deny, allow],
                None,
                None,
                "dynamodb:GetItem",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Allow
        );
    }

    // --- NotResource ---

    #[test]
    fn not_resource_deny() {
        let deny = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Deny","Action":"dynamodb:*",
                "NotResource":["arn:aws:dynamodb:*:*:table/AllowedTable"]
            }]}"#,
        );
        let allow = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
        );
        // Access to AllowedTable is not denied (excluded from NotResource)
        assert_eq!(
            evaluate_policies(
                &[deny.clone(), allow.clone()],
                None,
                None,
                "dynamodb:PutItem",
                "arn:aws:dynamodb:us-east-1:123:table/AllowedTable",
                &Ctx::empty()
            ),
            AuthzDecision::Allow
        );
        // Access to OtherTable is denied
        assert_eq!(
            evaluate_policies(
                &[deny, allow],
                None,
                None,
                "dynamodb:PutItem",
                "arn:aws:dynamodb:us-east-1:123:table/OtherTable",
                &Ctx::empty()
            ),
            AuthzDecision::Deny
        );
    }

    // --- Permissions Boundary ---

    #[test]
    fn boundary_restricts_allow() {
        let identity = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
        );
        let boundary = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
        );
        // Identity allows all, but boundary only allows GetItem
        assert_eq!(
            evaluate_policies(
                std::slice::from_ref(&identity),
                Some(&boundary),
                None,
                "dynamodb:GetItem",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Allow
        );
        assert_eq!(
            evaluate_policies(
                &[identity],
                Some(&boundary),
                None,
                "dynamodb:PutItem",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Deny
        );
    }

    #[test]
    fn boundary_deny_overrides() {
        let identity = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
        );
        let boundary = parse(
            r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"dynamodb:*","Resource":"*"},
                {"Effect":"Deny","Action":"dynamodb:DeleteTable","Resource":"*"}
            ]}"#,
        );
        assert_eq!(
            evaluate_policies(
                &[identity],
                Some(&boundary),
                None,
                "dynamodb:DeleteTable",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Deny
        );
    }

    // --- Session Policy ---

    #[test]
    fn session_policy_restricts() {
        let role = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*"
            }]}"#,
        );
        let session = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:GetItem","Resource":"*"
            }]}"#,
        );
        assert_eq!(
            evaluate_policies(
                std::slice::from_ref(&role),
                None,
                Some(&session),
                "dynamodb:GetItem",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Allow
        );
        assert_eq!(
            evaluate_policies(
                &[role],
                None,
                Some(&session),
                "dynamodb:PutItem",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &Ctx::empty()
            ),
            AuthzDecision::Deny
        );
    }

    // --- Conditions (ABAC) ---

    #[test]
    fn condition_tag_match_allows() {
        let policy = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*",
                "Condition":{"StringEquals":{"aws:PrincipalTag/Department":"Eng"}}
            }]}"#,
        );
        let ctx = Ctx::empty().with("aws:PrincipalTag/Department", vec!["Eng"]);
        assert_eq!(
            evaluate_policies(
                &[policy],
                None,
                None,
                "dynamodb:PutItem",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &ctx
            ),
            AuthzDecision::Allow
        );
    }

    #[test]
    fn condition_tag_mismatch_denies() {
        let policy = parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Action":"dynamodb:*","Resource":"*",
                "Condition":{"StringEquals":{"aws:PrincipalTag/Department":"Eng"}}
            }]}"#,
        );
        let ctx = Ctx::empty().with("aws:PrincipalTag/Department", vec!["Sales"]);
        assert_eq!(
            evaluate_policies(
                &[policy],
                None,
                None,
                "dynamodb:PutItem",
                "arn:aws:dynamodb:us-east-1:123:table/T",
                &ctx
            ),
            AuthzDecision::Deny
        );
    }

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
}
