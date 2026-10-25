// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A trust policy's `Version` decides whether policy variables in its
//! condition values expand (issue #534). Measured with AssumeRole on fresh
//! roles whose trust policy allows the caller under
//! `StringEquals aws:PrincipalArn: ${aws:PrincipalArn}` and under the same
//! condition with `StringNotEquals`: in a `2012-10-17` document the
//! `StringEquals` form grants and the `StringNotEquals` form refuses; in a
//! `2008-10-17` document, and in one with no `Version`, the reverse.

use std::collections::HashMap;

use serde_json::{Value, json};

use super::{trust_decision_for, trust_policy_expands_variables};
use extenddb_auth::policy::context::AssumeRoleContext;

const CALLER: &str = "arn:aws:iam::123456789012:user/alice";

fn context() -> AssumeRoleContext {
    AssumeRoleContext {
        principal_tags: HashMap::new(),
        external_id: None,
        session_name: Some("s".to_owned()),
        principal_arn: CALLER.to_owned(),
        principal_account: Some("123456789012".to_owned()),
        request_tags: HashMap::new(),
        region: Some("us-east-1".to_owned()),
        secure_transport: true,
        principal_type: Some("User".to_owned()),
        username: Some("alice".to_owned()),
        source_ip: None,
        user_agent: None,
        userid: None,
        resource_account: Some("123456789012".to_owned()),
        current_time: "2026-01-01T00:00:00Z".to_owned(),
        epoch_time: "1767225600".to_owned(),
    }
}

fn trust_policy(version: Option<&str>, operator: &str) -> Value {
    let mut doc = json!({"Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": CALLER},
        "Action": "sts:AssumeRole",
        "Condition": {operator: {"aws:PrincipalArn": "${aws:PrincipalArn}"}}
    }]});
    if let Some(v) = version {
        doc["Version"] = json!(v);
    }
    doc
}

fn assumable(doc: &Value) -> bool {
    let statements = doc["Statement"].as_array().cloned().unwrap_or_default();
    trust_decision_for(
        &statements,
        CALLER,
        &context(),
        false,
        trust_policy_expands_variables(doc),
    )
}

#[test]
fn a_2012_trust_policy_expands_condition_variables() {
    assert!(assumable(&trust_policy(Some("2012-10-17"), "StringEquals")));
    assert!(!assumable(&trust_policy(
        Some("2012-10-17"),
        "StringNotEquals"
    )));
}

#[test]
fn a_2008_trust_policy_compares_condition_variables_as_text() {
    assert!(!assumable(&trust_policy(
        Some("2008-10-17"),
        "StringEquals"
    )));
    assert!(assumable(&trust_policy(
        Some("2008-10-17"),
        "StringNotEquals"
    )));
}

#[test]
fn a_trust_policy_with_no_version_compares_condition_variables_as_text() {
    assert!(!assumable(&trust_policy(None, "StringEquals")));
    assert!(assumable(&trust_policy(None, "StringNotEquals")));
}
