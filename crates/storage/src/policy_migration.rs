// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Rewrite stored IAM policies from `dynamodb:ResourceTag/<key>` to
//! `aws:ResourceTag/<key>`.
//!
//! The live service evaluates resource tags only as `aws:ResourceTag/<key>` and
//! leaves `dynamodb:ResourceTag/<key>` absent, so ExtendDB no longer resolves
//! the `dynamodb:` form (see `extenddb_auth::policy::context`). A policy that
//! was written against the `dynamodb:` form therefore changed meaning on
//! upgrade. The rewrite restores the author's intent: the `dynamodb:` form and
//! the `aws:` form resolved identically before the evaluator change, so
//! replacing the former with the latter keeps every policy's decision while
//! moving it onto the key the service (and this build) actually evaluates.
//!
//! This module has two layers:
//! - [`rewrite_policy_document`], a pure transform over a single policy
//!   document (unit-tested per operator form). It defines catalog migration 2
//!   and lives, frozen, in that migration's module; it is re-exported here.
//! - [`migrate_resource_tag_condition_keys`], which drives it over every stored
//!   policy reachable through [`ManagementStore`], backend-independently.

use serde_json::Value;

use crate::management_store::{ManagementStore, OpError, SessionPolicyEntry};
pub use crate::migrations::m0002_resource_tag_condition_keys::{
    RewriteOutcome, rewrite_policy_document,
};

/// A policy that the migration could not rewrite automatically, with enough
/// identity for an operator to find and fix it by hand. Carries no policy body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeedsAttention {
    pub account_id: String,
    pub principal: String,
    pub policy_name: String,
}

/// A policy the migration would rewrite (detect mode) or did rewrite (apply
/// mode), with its identity and the number of rewrites. Carries no policy body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rewritten {
    pub account_id: String,
    pub principal: String,
    pub policy_name: String,
    pub rewrites: usize,
}

/// Result of a migration pass over every stored policy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Policies that were (or, in detect mode, would be) rewritten.
    pub rewritten: Vec<Rewritten>,
    /// Policies left unchanged because of a collision.
    pub needs_attention: Vec<NeedsAttention>,
    /// Count of storage errors encountered. The pass keeps going past an
    /// error so a single bad row does not hide the rest; the caller treats a
    /// non-zero count as a failed migration.
    pub errors: u64,
    /// The first storage error. On a backend whose transaction rejects every
    /// statement after a failed one, it is the one that says why, and its
    /// database error code (a lock timeout) decides whether the runner runs
    /// the body again.
    pub first_error: Option<OpError>,
}

impl MigrationReport {
    /// Total rewrites across all policies (for the summary line).
    #[must_use]
    pub fn total_rewrites(&self) -> usize {
        self.rewritten.iter().map(|r| r.rewrites).sum()
    }

    /// Count a storage error, keeping the first.
    fn failed(&mut self, e: OpError) {
        self.errors += 1;
        self.first_error.get_or_insert(e);
    }
}

/// Rewrite `dynamodb:ResourceTag/` condition keys to `aws:ResourceTag/` across
/// every stored policy reachable through `store`, backend-independently.
///
/// Scope: inline user, group and role policies, user and role permissions
/// boundaries, and stored session policies. Role trust policies are not
/// rewritten: the `AssumeRole` path never resolved the key, so their behaviour
/// does not change.
///
/// With `apply` false nothing is written: the report lists what would change.
/// With `apply` true each changed policy is written back. Every storage error
/// is counted and the pass continues, so an interrupted or partial run is
/// finished by the next `migrate`. Idempotent: a second pass finds nothing to
/// rewrite. No policy body is ever logged or returned.
pub async fn migrate_resource_tag_condition_keys(
    store: &dyn ManagementStore,
    apply: bool,
) -> MigrationReport {
    let mut report = MigrationReport::default();

    let accounts = match store.list_all_accounts().await {
        Ok(a) => a,
        Err(e) => {
            report.failed(e);
            return report;
        }
    };

    for (account_id, _name) in &accounts {
        migrate_account(store, account_id, apply, &mut report).await;
    }

    migrate_sessions(store, apply, &mut report).await;

    report
}

/// Classify a policy, record it in the report, and return the rewritten
/// document when it should be written back. `principal` and `policy_name` are
/// for reporting only; no policy body is recorded.
fn record_outcome(
    report: &mut MigrationReport,
    account_id: &str,
    principal: &str,
    policy_name: &str,
    document: &Value,
) -> Option<Value> {
    let outcome = rewrite_policy_document(document);
    if outcome.collision {
        report.needs_attention.push(NeedsAttention {
            account_id: account_id.to_owned(),
            principal: principal.to_owned(),
            policy_name: policy_name.to_owned(),
        });
        return None;
    }
    if !outcome.changed() {
        return None;
    }
    report.rewritten.push(Rewritten {
        account_id: account_id.to_owned(),
        principal: principal.to_owned(),
        policy_name: policy_name.to_owned(),
        rewrites: outcome.rewrites,
    });
    Some(outcome.document)
}

async fn migrate_account(
    store: &dyn ManagementStore,
    account_id: &str,
    apply: bool,
    report: &mut MigrationReport,
) {
    // Inline policies on users, groups and roles.
    match store.list_users(account_id).await {
        Ok(users) => {
            for (_a, user_name, ..) in users {
                migrate_inline(store, account_id, "user", &user_name, apply, report).await;
                migrate_user_boundary(store, account_id, &user_name, apply, report).await;
            }
        }
        Err(e) => report.failed(e),
    }
    match store.list_groups(account_id).await {
        Ok(groups) => {
            for (_a, group_name, ..) in groups {
                migrate_inline(store, account_id, "group", &group_name, apply, report).await;
            }
        }
        Err(e) => report.failed(e),
    }
    match store.list_roles(account_id).await {
        Ok(roles) => {
            for (_a, role_name, ..) in roles {
                migrate_inline(store, account_id, "role", &role_name, apply, report).await;
                migrate_role_boundary(store, account_id, &role_name, apply, report).await;
            }
        }
        Err(e) => report.failed(e),
    }
}

async fn migrate_inline(
    store: &dyn ManagementStore,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
    apply: bool,
    report: &mut MigrationReport,
) {
    let policies = match store
        .list_policies(account_id, principal_type, principal_name)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            report.failed(e);
            return;
        }
    };
    let principal = format!("{principal_type}/{principal_name}");
    for (policy_name, document, _created) in policies {
        if let Some(new_doc) =
            record_outcome(report, account_id, &principal, &policy_name, &document)
            && apply
            && let Err(e) = store
                .put_policy(
                    account_id,
                    principal_type,
                    principal_name,
                    &policy_name,
                    &new_doc,
                )
                .await
        {
            report.failed(e);
        }
    }
}

async fn migrate_user_boundary(
    store: &dyn ManagementStore,
    account_id: &str,
    user_name: &str,
    apply: bool,
    report: &mut MigrationReport,
) {
    match store.get_user_boundary(account_id, user_name).await {
        Ok(Some(document)) => {
            let principal = format!("user/{user_name}");
            if let Some(new_doc) = record_outcome(
                report,
                account_id,
                &principal,
                "<permissions-boundary>",
                &document,
            ) && apply
                && let Err(e) = store
                    .set_user_boundary(account_id, user_name, &new_doc)
                    .await
            {
                report.failed(e);
            }
        }
        Ok(None) => {}
        Err(e) => report.failed(e),
    }
}

async fn migrate_role_boundary(
    store: &dyn ManagementStore,
    account_id: &str,
    role_name: &str,
    apply: bool,
    report: &mut MigrationReport,
) {
    match store.get_role_boundary(account_id, role_name).await {
        Ok(Some(document)) => {
            let principal = format!("role/{role_name}");
            if let Some(new_doc) = record_outcome(
                report,
                account_id,
                &principal,
                "<permissions-boundary>",
                &document,
            ) && apply
                && let Err(e) = store
                    .set_role_boundary(account_id, role_name, &new_doc)
                    .await
            {
                report.failed(e);
            }
        }
        Ok(None) => {}
        Err(e) => report.failed(e),
    }
}

async fn migrate_sessions(store: &dyn ManagementStore, apply: bool, report: &mut MigrationReport) {
    let sessions = match store.list_session_policies().await {
        Ok(s) => s,
        Err(e) => {
            report.failed(e);
            return;
        }
    };
    for SessionPolicyEntry {
        session_token,
        account_id,
        role_name,
        session_name,
        policy,
    } in sessions
    {
        let principal = format!("assumed-role/{role_name}/{session_name}");
        if let Some(new_doc) =
            record_outcome(report, &account_id, &principal, "<session-policy>", &policy)
            && apply
            && let Err(e) = store.update_session_policy(&session_token, &new_doc).await
        {
            report.failed(e);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    /// The prefixes the evaluator resolves resource tags under, before and
    /// after the change migration 2 follows.
    const DYNAMODB_PREFIX: &str = "dynamodb:ResourceTag/";
    const AWS_PREFIX: &str = "aws:ResourceTag/";

    fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
        let head = s.get(..prefix.len())?;
        head.eq_ignore_ascii_case(prefix)
            .then(|| &s[prefix.len()..])
    }
    use serde_json::json;

    fn stmt_cond(cond: Value) -> Value {
        json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": "*",
                "Condition": cond
            }]
        })
    }

    // ── Condition-key rewrites, per operator form ────────────────────────

    #[test]
    fn bare_key_rewritten() {
        let doc = stmt_cond(json!({"StringEquals": {"dynamodb:ResourceTag/env": "prod"}}));
        let out = rewrite_policy_document(&doc);
        assert!(out.changed());
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({"StringEquals": {"aws:ResourceTag/env": "prod"}}))
        );
    }

    #[test]
    fn negated_key_rewritten() {
        let doc = stmt_cond(json!({"StringNotEquals": {"dynamodb:ResourceTag/env": "prod"}}));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({"StringNotEquals": {"aws:ResourceTag/env": "prod"}}))
        );
    }

    #[test]
    fn if_exists_key_rewritten() {
        let doc = stmt_cond(json!({"StringEqualsIfExists": {"dynamodb:ResourceTag/env": "prod"}}));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({"StringEqualsIfExists": {"aws:ResourceTag/env": "prod"}}))
        );
    }

    #[test]
    fn for_all_values_key_rewritten() {
        let doc = stmt_cond(
            json!({"ForAllValues:StringEquals": {"dynamodb:ResourceTag/team": ["a", "b"]}}),
        );
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({"ForAllValues:StringEquals": {"aws:ResourceTag/team": ["a", "b"]}}))
        );
    }

    #[test]
    fn for_any_value_if_exists_key_rewritten() {
        let doc = stmt_cond(
            json!({"ForAnyValue:StringEqualsIfExists": {"dynamodb:ResourceTag/team": ["a"]}}),
        );
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({"ForAnyValue:StringEqualsIfExists": {"aws:ResourceTag/team": ["a"]}}))
        );
    }

    #[test]
    fn null_key_rewritten() {
        let doc = stmt_cond(json!({"Null": {"dynamodb:ResourceTag/env": "true"}}));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({"Null": {"aws:ResourceTag/env": "true"}}))
        );
    }

    #[test]
    fn mixed_case_prefix_rewritten_tag_key_preserved() {
        let doc = stmt_cond(json!({"StringEquals": {"DynamoDB:ResourceTag/Env": "prod"}}));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        // Prefix normalised to aws:ResourceTag/, tag key "Env" case preserved.
        assert_eq!(
            out.document,
            stmt_cond(json!({"StringEquals": {"aws:ResourceTag/Env": "prod"}}))
        );
    }

    // ── Policy-variable rewrites ─────────────────────────────────────────

    #[test]
    fn policy_variable_rewritten() {
        let doc = stmt_cond(json!({
            "StringEquals": {"aws:PrincipalTag/team": "${dynamodb:ResourceTag/team}"}
        }));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({
                "StringEquals": {"aws:PrincipalTag/team": "${aws:ResourceTag/team}"}
            }))
        );
    }

    #[test]
    fn policy_variable_mixed_case_rewritten() {
        let doc = stmt_cond(json!({
            "StringEquals": {"aws:PrincipalTag/team": "${DYNAMODB:resourcetag/team}"}
        }));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({
                "StringEquals": {"aws:PrincipalTag/team": "${aws:ResourceTag/team}"}
            }))
        );
    }

    #[test]
    fn policy_variable_in_array_and_amongst_other_text() {
        let doc = stmt_cond(json!({
            "StringLike": {"aws:PrincipalTag/x": [
                "pre-${dynamodb:ResourceTag/a}-post",
                "${aws:PrincipalTag/y}"
            ]}
        }));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({
                "StringLike": {"aws:PrincipalTag/x": [
                    "pre-${aws:ResourceTag/a}-post",
                    "${aws:PrincipalTag/y}"
                ]}
            }))
        );
    }

    #[test]
    fn literal_value_not_wrapped_in_variable_is_left_alone() {
        // The key name appearing as a bare value (not inside ${...}) is a
        // literal comparison value, not a variable, and must not be rewritten.
        let doc = stmt_cond(json!({
            "StringEquals": {"aws:PrincipalTag/x": "dynamodb:ResourceTag/env"}
        }));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 0);
        assert!(!out.changed());
        assert_eq!(out.document, doc);
    }

    #[test]
    fn key_and_variable_both_rewritten_counts_two() {
        let doc = stmt_cond(json!({
            "StringEquals": {"dynamodb:ResourceTag/env": "${dynamodb:ResourceTag/env}"}
        }));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 2);
        assert_eq!(
            out.document,
            stmt_cond(json!({
                "StringEquals": {"aws:ResourceTag/env": "${aws:ResourceTag/env}"}
            }))
        );
    }

    // ── Collision ────────────────────────────────────────────────────────

    #[test]
    fn collision_leaves_document_unchanged() {
        let doc = stmt_cond(json!({
            "StringEquals": {
                "aws:ResourceTag/env": "prod",
                "dynamodb:ResourceTag/env": "dev"
            }
        }));
        let out = rewrite_policy_document(&doc);
        assert!(out.collision);
        assert!(!out.changed());
        assert_eq!(out.rewrites, 0);
        assert_eq!(
            out.document, doc,
            "collision must leave the policy untouched"
        );
    }

    #[test]
    fn same_value_duplicate_drops_the_dynamodb_key() {
        // Both prefixes resolved from the same table tags, so the two
        // conditions are one condition; the dynamodb: key is dropped.
        let doc = stmt_cond(json!({
            "StringEquals": {
                "aws:ResourceTag/env": "prod",
                "dynamodb:ResourceTag/env": "prod"
            }
        }));
        let out = rewrite_policy_document(&doc);
        assert!(!out.collision);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document,
            stmt_cond(json!({"StringEquals": {"aws:ResourceTag/env": "prod"}}))
        );
    }

    #[test]
    fn two_dynamodb_spellings_of_one_key() {
        // Same value: one aws: key remains.
        let same = stmt_cond(json!({
            "StringEquals": {
                "dynamodb:ResourceTag/env": "prod",
                "DynamoDB:resourcetag/env": "prod"
            }
        }));
        let out = rewrite_policy_document(&same);
        assert!(!out.collision);
        assert_eq!(out.rewrites, 2);
        assert_eq!(
            out.document,
            stmt_cond(json!({"StringEquals": {"aws:ResourceTag/env": "prod"}}))
        );
        // Different values: a collision, left unchanged.
        let different = stmt_cond(json!({
            "StringEquals": {
                "dynamodb:ResourceTag/env": "prod",
                "DynamoDB:resourcetag/env": "dev"
            }
        }));
        let out = rewrite_policy_document(&different);
        assert!(out.collision);
        assert_eq!(out.document, different);
    }

    #[test]
    fn text_outside_condition_blocks_is_never_rewritten() {
        let doc = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "dynamodbResourceTagenv",
                "Effect": "Allow",
                "Action": "dynamodb:ResourceTag/env",
                "Resource": "arn:aws:dynamodb:us-east-1:123456789012:table/dynamodb:ResourceTag/env",
                "Condition": {"StringEquals": {"dynamodb:ResourceTag/env": "prod"}}
            }]
        });
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        let stmt = &out.document["Statement"][0];
        assert_eq!(stmt["Sid"], doc["Statement"][0]["Sid"]);
        assert_eq!(stmt["Action"], doc["Statement"][0]["Action"]);
        assert_eq!(stmt["Resource"], doc["Statement"][0]["Resource"]);
        assert_eq!(
            stmt["Condition"],
            json!({"StringEquals": {"aws:ResourceTag/env": "prod"}})
        );
    }

    #[test]
    fn no_collision_when_aws_key_is_in_a_different_operator_block() {
        // aws:ResourceTag/env under StringEquals and dynamodb:ResourceTag/env
        // under StringNotEquals are different JSON objects, so no collision.
        let doc = stmt_cond(json!({
            "StringEquals": {"aws:ResourceTag/env": "prod"},
            "StringNotEquals": {"dynamodb:ResourceTag/env": "dev"}
        }));
        let out = rewrite_policy_document(&doc);
        assert!(!out.collision);
        assert_eq!(out.rewrites, 1);
    }

    // ── Idempotency and untouched shapes ─────────────────────────────────

    #[test]
    fn idempotent_second_pass_is_a_noop() {
        let doc = stmt_cond(json!({"StringEquals": {"dynamodb:ResourceTag/env": "prod"}}));
        let once = rewrite_policy_document(&doc);
        let twice = rewrite_policy_document(&once.document);
        assert_eq!(twice.rewrites, 0);
        assert!(!twice.changed());
        assert_eq!(twice.document, once.document);
    }

    #[test]
    fn single_statement_object_form_supported() {
        let doc = json!({
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": "*",
                "Condition": {"StringEquals": {"dynamodb:ResourceTag/env": "prod"}}
            }
        });
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 1);
        assert_eq!(
            out.document
                .pointer("/Statement/Condition/StringEquals")
                .and_then(|v| v.as_object())
                .map(|m| m.contains_key("aws:ResourceTag/env")),
            Some(true)
        );
    }

    #[test]
    fn policy_without_resource_tag_is_untouched() {
        let doc = stmt_cond(json!({"StringEquals": {"aws:PrincipalTag/team": "eng"}}));
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 0);
        assert!(!out.changed());
        assert_eq!(out.document, doc);
    }

    #[test]
    fn statement_without_condition_is_untouched() {
        let doc = json!({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"}]
        });
        let out = rewrite_policy_document(&doc);
        assert_eq!(out.rewrites, 0);
        assert_eq!(out.document, doc);
    }

    #[test]
    fn total_rewrites_sums_per_policy() {
        let report = MigrationReport {
            rewritten: vec![
                Rewritten {
                    account_id: "a".into(),
                    principal: "user/u".into(),
                    policy_name: "p".into(),
                    rewrites: 2,
                },
                Rewritten {
                    account_id: "a".into(),
                    principal: "role/r".into(),
                    policy_name: "q".into(),
                    rewrites: 3,
                },
            ],
            needs_attention: vec![],
            errors: 0,
            first_error: None,
        };
        assert_eq!(report.total_rewrites(), 5);
    }

    // ── Equivalence: rewrite preserves the decision ──────────────────────
    //
    // On main both `aws:ResourceTag/*` and `dynamodb:ResourceTag/*` resolve
    // from the resource-tag map; after the evaluator change only `aws:` does.
    // The rewrite swaps the prefix, so a stored `dynamodb:` policy evaluated on
    // main must reach the same decision as the rewritten `aws:` policy
    // evaluated on the new semantics. These contexts model the two evaluators.

    use extenddb_auth::policy::condition::evaluate_condition;
    use extenddb_auth::policy::context::ConditionContext;
    use extenddb_auth::policy::document::{Condition, ConditionOperator};
    use std::collections::HashMap;

    /// Main semantics: resource tags resolve under both prefixes.
    struct MainCtx {
        tags: HashMap<String, String>,
    }
    /// New semantics: resource tags resolve only as `aws:ResourceTag/*`.
    struct NewCtx {
        tags: HashMap<String, String>,
    }

    fn resolve_tag<'a>(tags: &'a HashMap<String, String>, sub: &str) -> Option<Vec<&'a str>> {
        tags.get(sub).map(|v| vec![v.as_str()])
    }

    impl ConditionContext for MainCtx {
        fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
            if let Some(sub) = strip_prefix_ci(key, AWS_PREFIX) {
                resolve_tag(&self.tags, sub)
            } else if let Some(sub) = strip_prefix_ci(key, DYNAMODB_PREFIX) {
                resolve_tag(&self.tags, sub)
            } else {
                None
            }
        }
        fn is_multivalued_key(&self, _key: &str) -> bool {
            false
        }
    }
    impl ConditionContext for NewCtx {
        fn resolve_key(&self, key: &str) -> Option<Vec<&str>> {
            strip_prefix_ci(key, AWS_PREFIX).and_then(|sub| resolve_tag(&self.tags, sub))
        }
        fn is_multivalued_key(&self, _key: &str) -> bool {
            false
        }
    }

    /// Pull the single (operator-name, key, values) out of a one-statement,
    /// one-operator, one-key policy document.
    fn sole_condition(doc: &Value) -> (String, String, Vec<String>) {
        let cond = doc
            .pointer("/Statement/0/Condition")
            .and_then(Value::as_object)
            .expect("one condition block");
        let (op, block) = cond.iter().next().expect("one operator");
        let map = block.as_object().expect("operator block object");
        let (key, val) = map.iter().next().expect("one condition key");
        let values = match val {
            Value::String(s) => vec![s.clone()],
            Value::Array(a) => a
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect(),
            _ => vec![],
        };
        (op.clone(), key.clone(), values)
    }

    fn build_condition(op_name: &str, key: &str, values: &[String]) -> Condition {
        let base = |n: &str| match n {
            "StringEquals" => ConditionOperator::StringEquals,
            "StringNotEquals" => ConditionOperator::StringNotEquals,
            "Null" => ConditionOperator::Null,
            other => panic!("unexpected operator {other}"),
        };
        let operator = if let Some(inner) = op_name.strip_prefix("ForAllValues:") {
            ConditionOperator::ForAllValues(Box::new(base(inner)))
        } else if let Some(inner) = op_name.strip_prefix("ForAnyValue:") {
            ConditionOperator::ForAnyValue(Box::new(base(inner)))
        } else if let Some(inner) = op_name.strip_suffix("IfExists") {
            ConditionOperator::IfExists(Box::new(base(inner)))
        } else {
            base(op_name)
        };
        Condition {
            operator,
            key: key.to_owned(),
            values: values.to_vec(),
        }
    }

    #[test]
    fn rewrite_preserves_decision_across_shapes_and_tag_states() {
        // (operator block in a dynamodb:ResourceTag policy)
        let shapes = [
            json!({"StringEquals": {"dynamodb:ResourceTag/env": "prod"}}),
            json!({"StringNotEquals": {"dynamodb:ResourceTag/env": "prod"}}),
            json!({"StringEqualsIfExists": {"dynamodb:ResourceTag/env": "prod"}}),
            json!({"ForAllValues:StringEquals": {"dynamodb:ResourceTag/env": ["prod"]}}),
            json!({"ForAnyValue:StringEquals": {"dynamodb:ResourceTag/env": ["prod"]}}),
            json!({"Null": {"dynamodb:ResourceTag/env": "true"}}),
            json!({"Null": {"dynamodb:ResourceTag/env": "false"}}),
        ];
        // tag states: matching, mismatching, absent
        let tag_states = [
            HashMap::from([("env".to_owned(), "prod".to_owned())]),
            HashMap::from([("env".to_owned(), "dev".to_owned())]),
            HashMap::new(),
        ];

        for shape in &shapes {
            let original = stmt_cond(shape.clone());
            let (op_dyn, key_dyn, vals) = sole_condition(&original);
            assert!(key_dyn.starts_with("dynamodb:ResourceTag/"));

            // Run the real rewrite and confirm it produced the aws: key, same
            // operator and same values, and dropped the dynamodb: key.
            let out = rewrite_policy_document(&original);
            assert!(out.changed());
            let (op_aws, key_aws, vals_aws) = sole_condition(&out.document);
            assert_eq!(op_aws, op_dyn);
            assert_eq!(key_aws, "aws:ResourceTag/env");
            assert_eq!(vals_aws, vals);

            let cond_dyn = build_condition(&op_dyn, &key_dyn, &vals);
            let cond_aws = build_condition(&op_aws, &key_aws, &vals_aws);

            for tags in &tag_states {
                let main_ctx = MainCtx { tags: tags.clone() };
                let new_ctx = NewCtx { tags: tags.clone() };
                let before = evaluate_condition(&cond_dyn, &main_ctx);
                let after = evaluate_condition(&cond_aws, &new_ctx);
                assert_eq!(
                    before, after,
                    "shape {op_dyn} with tags {tags:?}: main={before} new={after}"
                );
            }
        }
    }
}
