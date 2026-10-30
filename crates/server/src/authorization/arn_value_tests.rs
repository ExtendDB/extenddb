// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stored identity policies holding Arn condition values, on the request path.
//!
//! A value that policy writes reject (the live service rejects it with
//! `MalformedPolicyDocument`) can only be one stored before ExtendDB checked
//! it. The policy then fails to load and the request is denied, whether the
//! statement holding the value is an Allow or a Deny, the same as a stored
//! malformed CIDR, and the one line logged for the denial names the policy's
//! owner. A value the write check accepts is evaluated the way the live
//! service evaluated it on 2026-10-30.

use super::check_user_authorization;
use crate::authz_cache::CachedAuthzStore;
use crate::authz_cache::tests::{CountingAuthzStore, fast_authz_cfg};
use extenddb_auth::policy::context::RequestParams;
use extenddb_core::error::DynamoDbError;
use serde_json::json;
use std::future::Future;
use std::sync::{Arc, Mutex};

const ACCOUNT: &str = "123456789012";
const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/t";
/// The denial a policy that fails to load produces.
const LOAD_DENIED: &str = "Not authorized to perform this action (policy evaluation error)";

/// Values the service rejected at `CreatePolicy` or `PutRolePolicy`.
const REJECTED: [&str; 5] = [
    "",
    "arn::dynamodb:us-east-1:123456789012:table/t",
    "foo:*",
    "arn:*:role/x",
    "arn:aws:iam::${aws:PrincipalAccount}:user/u",
];

fn allow_if(op: &str, value: &str) -> String {
    json!({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
        "Condition": {op: {"aws:PrincipalArn": value}}}]})
    .to_string()
}

fn allow_all_deny_if(op: &str, value: &str) -> String {
    json!({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"},
        {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
         "Condition": {op: {"aws:PrincipalArn": value}}}]})
    .to_string()
}

/// A store holding `policy` as the only inline policy of user `u` (ARN
/// `arn:aws:iam::123456789012:user/u`) and `boundary` as its boundary.
fn store_for(policy: String, boundary: Option<String>) -> CountingAuthzStore {
    let s = CountingAuthzStore::default();
    let key = (ACCOUNT.to_owned(), "u".to_owned());
    s.user_policies
        .lock()
        .unwrap()
        .insert(key.clone(), vec![policy]);
    s.user_boundary.lock().unwrap().insert(key, boundary);
    s
}

/// `GetItem` by user `u` against `store`.
async fn get_item_from(store: CountingAuthzStore) -> Result<(), DynamoDbError> {
    let cache = CachedAuthzStore::new(Arc::new(store), fast_authz_cfg());
    let params = RequestParams::default();
    check_user_authorization(&cache, ACCOUNT, "u", "GetItem", TABLE, false, params).await
}

/// `GetItem` by user `u` with `policy` as its only inline policy and
/// `boundary` as its boundary.
async fn get_item(policy: String, boundary: Option<String>) -> Result<(), DynamoDbError> {
    get_item_from(store_for(policy, boundary)).await
}

/// Collects the formatted log lines written while a test's subscriber is set.
#[derive(Clone, Default)]
struct LogLines(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogLines {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogLines {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run the request `request` builds and return its result with the warning
/// and error lines it logged on this thread.
///
/// While only one subscriber is registered, tracing computes a callsite's
/// interest from the default subscriber of whichever thread registers or
/// rebuilds it, without a lock, and caches it. Other tests reach the same
/// callsites from threads with no subscriber, which can leave them disabled
/// while this test's subscriber is set. A second registered subscriber makes
/// tracing compute the interest from every registered subscriber under a
/// lock. The request also runs once before this test's subscriber is set, so
/// its callsites are registered, and their interest is rebuilt after.
async fn logged<F>(request: impl Fn() -> F) -> (Result<(), DynamoDbError>, Vec<String>)
where
    F: Future<Output = Result<(), DynamoDbError>>,
{
    let _second = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .with_max_level(tracing::Level::WARN)
            .finish(),
    );
    let _ = request().await;
    let lines = LogLines::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(lines.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    tracing::callsite::rebuild_interest_cache();
    let r = request().await;
    let text = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
    (r, text.lines().map(str::to_owned).collect())
}

fn denied_at_load(r: &Result<(), DynamoDbError>) -> bool {
    matches!(r, Err(DynamoDbError::AccessDeniedException(m)) if m == LOAD_DENIED)
}

#[tokio::test]
async fn stored_rejected_value_under_an_allow_denies() {
    // Before the write and load checks each of these Allows granted: the
    // negated condition held for every caller.
    for op in ["ArnNotEquals", "ArnNotLike", "ArnNotEqualsIfExists"] {
        for v in REJECTED {
            let r = get_item(allow_if(op, v), None).await;
            assert!(denied_at_load(&r), "{op} {v:?}: {r:?}");
        }
    }
}

#[tokio::test]
async fn stored_rejected_value_under_a_deny_denies() {
    // Before the write and load checks none of these Denys applied, so the
    // Allow granted.
    for op in ["ArnEquals", "ArnLike", "ForAnyValue:ArnLike"] {
        for v in REJECTED {
            let r = get_item(allow_all_deny_if(op, v), None).await;
            assert!(denied_at_load(&r), "{op} {v:?}: {r:?}");
        }
    }
}

#[tokio::test]
async fn stored_rejected_value_in_a_boundary_denies() {
    let all = allow_all_deny_if("ArnEquals", "arn:aws:iam::123456789012:user/nobody");
    let r = get_item(all.clone(), Some(allow_if("ArnNotEquals", ""))).await;
    assert!(denied_at_load(&r), "{r:?}");
    let r = get_item(all, Some(allow_if("ArnNotEquals", "arn:aws:dynamodb:*"))).await;
    assert!(r.is_ok(), "{r:?}");
}

#[tokio::test]
async fn stored_rejected_value_logs_one_line_naming_the_owner() {
    let (r, lines) = logged(|| get_item(allow_if("ArnNotEquals", ""), None)).await;
    assert!(denied_at_load(&r), "{r:?}");
    assert_eq!(lines.len(), 1, "{lines:#?}");
    assert!(
        lines[0].contains(
            "an inline policy of user u in account 123456789012 holds an Arn condition value \
             that policy writes reject"
        ),
        "{lines:#?}"
    );
    assert!(
        lines[0].contains(r#"Arn condition on aws:PrincipalArn holds """#),
        "{lines:#?}"
    );
}

#[tokio::test]
async fn stored_rejected_value_in_a_group_policy_names_the_group() {
    // The user's own policy is valid; the group's policy is not.
    let store = || {
        let s = store_for(allow_if("ArnNotEquals", "arn:aws:dynamodb:*"), None);
        s.user_group_policies.lock().unwrap().insert(
            (ACCOUNT.to_owned(), "u".to_owned()),
            vec![("g".to_owned(), allow_all_deny_if("ArnEquals", "foo:*"))],
        );
        s
    };
    let (r, lines) = logged(|| get_item_from(store())).await;
    assert!(denied_at_load(&r), "{r:?}");
    assert_eq!(lines.len(), 1, "{lines:#?}");
    assert!(
        lines[0].contains(
            "an inline policy of group g in account 123456789012 (user u is a member) holds an \
             Arn condition value that policy writes reject"
        ),
        "{lines:#?}"
    );
}

#[tokio::test]
async fn accepted_values_evaluate_as_the_service_does() {
    // (policy, granted): the caller ARN is arn:aws:iam::123456789012:user/u.
    let cases = [
        // `arn:aws:dynamodb:*` names another service, so it does not match.
        (allow_if("ArnNotEquals", "arn:aws:dynamodb:*"), true),
        // `arn:aws:iam:*` and `arn:aws:iam` match every iam ARN.
        (allow_if("ArnNotEquals", "arn:aws:iam:*"), false),
        (allow_all_deny_if("ArnEquals", "arn:aws:iam"), false),
        // A bare `*` matches nothing, so ArnNotEquals `*` holds for everyone.
        (allow_if("ArnEquals", "*"), false),
        (allow_all_deny_if("ArnNotEquals", "*"), false),
        (
            allow_if("ArnEquals", "arn:aws:iam::123456789012:user/u"),
            true,
        ),
    ];
    for (policy, granted) in cases {
        let r = get_item(policy.clone(), None).await;
        assert_eq!(r.is_ok(), granted, "{policy}: {r:?}");
        assert!(!denied_at_load(&r), "{policy}: {r:?}");
    }
}
