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
/// - `session_policy`: optional inline policy from `AssumeRole`.
/// - `action`: the `DynamoDB` action (e.g., "dynamodb:PutItem").
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
mod tests;
