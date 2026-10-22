// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Resource-tag condition-key evaluation on the real `RequestContext`.
//!
//! The live service evaluates resource tags only as `aws:ResourceTag/<key>` and
//! leaves `dynamodb:ResourceTag/<key>` absent. These tests pin that for both
//! prefixes under the bare, `IfExists`, `ForAllValues:`, `ForAnyValue:` and
//! `Null` forms, so a regression that reintroduced the `dynamodb:` resolution
//! would change at least one outcome here.

use std::collections::HashMap;

use crate::policy::condition::evaluate_condition;
use crate::policy::context::{RequestContext, RequestParams};
use crate::policy::document::{Condition, ConditionOperator};

/// A request against a resource tagged `env=prod`.
fn ctx_env_prod() -> RequestContext {
    RequestContext::build(
        HashMap::new(),
        HashMap::from([("env".to_owned(), "prod".to_owned())]),
        false,
        RequestParams::default(),
    )
}

fn cond(op: ConditionOperator, key: &str, values: &[&str]) -> Condition {
    Condition {
        operator: op,
        key: key.to_owned(),
        values: values.iter().map(|s| (*s).to_owned()).collect(),
    }
}

fn bare(op: ConditionOperator) -> ConditionOperator {
    op
}
fn if_exists(op: ConditionOperator) -> ConditionOperator {
    ConditionOperator::IfExists(Box::new(op))
}
fn for_all(op: ConditionOperator) -> ConditionOperator {
    ConditionOperator::ForAllValues(Box::new(op))
}
fn for_any(op: ConditionOperator) -> ConditionOperator {
    ConditionOperator::ForAnyValue(Box::new(op))
}

// ── Bare positive ────────────────────────────────────────────────────────

#[test]
fn bare_string_equals_aws_matches_dynamodb_absent() {
    let ctx = ctx_env_prod();
    // aws:ResourceTag/env resolves to "prod" and matches.
    assert!(evaluate_condition(
        &cond(bare(ConditionOperator::StringEquals), "aws:ResourceTag/env", &["prod"]),
        &ctx
    ));
    // dynamodb:ResourceTag/env is absent: a positive bare operator never matches.
    assert!(!evaluate_condition(
        &cond(bare(ConditionOperator::StringEquals), "dynamodb:ResourceTag/env", &["prod"]),
        &ctx
    ));
}

// ── Bare negated ─────────────────────────────────────────────────────────

#[test]
fn bare_string_not_equals_dynamodb_absent_matches() {
    let ctx = ctx_env_prod();
    // aws:ResourceTag/env resolves to "prod": StringNotEquals "prod" is false.
    assert!(!evaluate_condition(
        &cond(bare(ConditionOperator::StringNotEquals), "aws:ResourceTag/env", &["prod"]),
        &ctx
    ));
    // dynamodb:ResourceTag/env is absent: a negated bare operator matches
    // (absent-key semantics), it never fails closed.
    assert!(evaluate_condition(
        &cond(bare(ConditionOperator::StringNotEquals), "dynamodb:ResourceTag/env", &["prod"]),
        &ctx
    ));
}

// ── IfExists ─────────────────────────────────────────────────────────────

#[test]
fn if_exists_dynamodb_absent_passes_aws_present_evaluates() {
    let ctx = ctx_env_prod();
    // IfExists passes when the key is absent: dynamodb:ResourceTag/env absent.
    assert!(evaluate_condition(
        &cond(if_exists(ConditionOperator::StringEquals), "dynamodb:ResourceTag/env", &["dev"]),
        &ctx
    ));
    // aws:ResourceTag/env is present ("prod"), so IfExists evaluates the base
    // operator: StringEquals "dev" does not match.
    assert!(!evaluate_condition(
        &cond(if_exists(ConditionOperator::StringEquals), "aws:ResourceTag/env", &["dev"]),
        &ctx
    ));
}

// ── ForAllValues ─────────────────────────────────────────────────────────

#[test]
fn for_all_values_dynamodb_absent_vacuously_true() {
    let ctx = ctx_env_prod();
    // Absent key: ForAllValues is vacuously true.
    assert!(evaluate_condition(
        &cond(for_all(ConditionOperator::StringEquals), "dynamodb:ResourceTag/env", &["dev"]),
        &ctx
    ));
    // Present aws key with a non-matching value: not all values match.
    assert!(!evaluate_condition(
        &cond(for_all(ConditionOperator::StringEquals), "aws:ResourceTag/env", &["dev"]),
        &ctx
    ));
}

// ── ForAnyValue ──────────────────────────────────────────────────────────

#[test]
fn for_any_value_dynamodb_absent_false_aws_present_matches() {
    let ctx = ctx_env_prod();
    // Absent key: ForAnyValue is false.
    assert!(!evaluate_condition(
        &cond(for_any(ConditionOperator::StringEquals), "dynamodb:ResourceTag/env", &["prod"]),
        &ctx
    ));
    // Present aws key with a matching value: at least one value matches.
    assert!(evaluate_condition(
        &cond(for_any(ConditionOperator::StringEquals), "aws:ResourceTag/env", &["prod"]),
        &ctx
    ));
}

// ── Null ─────────────────────────────────────────────────────────────────

#[test]
fn null_true_matches_absent_dynamodb_not_present_aws() {
    let ctx = ctx_env_prod();
    // Null:true means "key is absent": true for dynamodb, false for aws.
    assert!(evaluate_condition(
        &cond(ConditionOperator::Null, "dynamodb:ResourceTag/env", &["true"]),
        &ctx
    ));
    assert!(!evaluate_condition(
        &cond(ConditionOperator::Null, "aws:ResourceTag/env", &["true"]),
        &ctx
    ));
}

#[test]
fn null_false_matches_present_aws_not_absent_dynamodb() {
    let ctx = ctx_env_prod();
    // Null:false means "key is present": true for aws, false for dynamodb.
    assert!(evaluate_condition(
        &cond(ConditionOperator::Null, "aws:ResourceTag/env", &["false"]),
        &ctx
    ));
    assert!(!evaluate_condition(
        &cond(ConditionOperator::Null, "dynamodb:ResourceTag/env", &["false"]),
        &ctx
    ));
}

// ── Case-insensitive prefix ──────────────────────────────────────────────

#[test]
fn dynamodb_prefix_absent_in_any_case() {
    let ctx = ctx_env_prod();
    for key in [
        "dynamodb:ResourceTag/env",
        "dynamodb:resourcetag/env",
        "DYNAMODB:RESOURCETAG/env",
        "DynamoDB:ResourceTag/env",
    ] {
        assert!(
            evaluate_condition(&cond(ConditionOperator::Null, key, &["true"]), &ctx),
            "{key} should be absent (Null:true)"
        );
    }
    // aws:ResourceTag resolves in any case.
    for key in ["aws:ResourceTag/env", "aws:resourcetag/env", "AWS:RESOURCETAG/env"] {
        assert!(
            evaluate_condition(
                &cond(ConditionOperator::StringEquals, key, &["prod"]),
                &ctx
            ),
            "{key} should resolve to prod"
        );
    }
}
