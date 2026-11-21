// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization of an action named by its full action string.
//!
//! `check_authorization` evaluates a `DynamoDB` operation and builds its
//! `dynamodb:` action from the operation name. [`authorize_action`] takes the
//! action as given, so an action of another service, such as `s3:GetObject`
//! on an object ARN, is evaluated by the same user and role evaluators: the
//! identity and group policies, the permissions boundary, a role session's
//! session policy, the principal's tags, and the condition keys the caller
//! passes.

use std::collections::HashMap;
use std::sync::Arc;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::RequestParams;
use extenddb_auth::policy::document::PolicyDocument;
use extenddb_core::error::DynamoDbError;

use crate::authz_cache::CachedAuthzStore;
use crate::authz_resource::AuthzTarget;

/// A role session's data as recorded earlier (a bulk operation records it at
/// Start), evaluated in place of the session's current data: its session
/// policy, its session tags and its issue time. The role's own policies,
/// boundary and tags are still read when the evaluation runs.
#[derive(Debug, Clone, Default)]
pub(crate) struct SessionSnapshot {
    pub(crate) policy: Option<Arc<PolicyDocument>>,
    pub(crate) tags: HashMap<String, String>,
    pub(crate) token_issue_time: Option<String>,
}

/// Evaluate whether `identity` may perform `action` on `resource_arn`.
///
/// `action` is the full action string, for example `s3:ListBucket`, and is
/// matched against each statement's `Action` the way a `dynamodb:` action is.
/// `params` carries the request's condition keys, such as `s3_prefix` for an
/// `s3:ListBucket`; the principal's keys are filled from `identity` as for
/// every other request. `dynamodb:FullTableScan` is absent, and
/// `aws:ResourceAccount` for an action of another service is the value in
/// `params` (see [`set_resource_account`]). For a role session, `session`
/// replaces the session's current policy, tags and issue time when given;
/// it is ignored for an IAM user.
///
/// # Errors
///
/// `AccessDeniedException` when the policies do not allow the action (see
/// [`denial_is_explicit`]), and the errors `check_authorization` returns when
/// a policy cannot be read.
pub(crate) async fn authorize_action(
    cache: &CachedAuthzStore,
    identity: &AuthIdentity,
    session: Option<&SessionSnapshot>,
    action: &str,
    resource_arn: &str,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    // The evaluators read the resource fields of the target and take the
    // action as passed, so the target's own action field is not read. The
    // resource is matched and named in a denial as given.
    let target = AuthzTarget::new(action, resource_arn.to_owned());
    match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => {
            super::check_user_authorization(
                cache, account_id, user_name, action, &target, false, params,
            )
            .await
        }
        AuthIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
        } => {
            super::check_role_authorization(
                cache,
                account_id,
                role_name,
                session_name,
                session,
                action,
                &target,
                false,
                params,
            )
            .await
        }
    }
}

/// Whether `message`, the text of an `AccessDeniedException` that
/// [`authorize_action`] returned for `identity`, `action` and
/// `resource_arn`, reports a matching Deny statement. Both evaluators render
/// the text with `deny_message`, so it is compared with that function's
/// explicit form for the principal ARN the evaluators name; any other denial
/// text, such as the fail-closed text of a policy that does not parse, is not
/// explicit.
pub(crate) fn denial_is_explicit(
    identity: &AuthIdentity,
    action: &str,
    resource_arn: &str,
    message: &str,
) -> bool {
    let principal = match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => format!("arn:aws:iam::{account_id}:user/{user_name}"),
        AuthIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
        } => format!("arn:aws:iam::{account_id}:assumed-role/{role_name}/{session_name}"),
    };
    message == super::deny_message(&principal, action, resource_arn, true)
}

/// The session policy, the role and session tags merged (session tags win on
/// a key both carry, as on the live path) and the issue time, for a role
/// session evaluation: from `snapshot` when one is given, the role's tags
/// read live; otherwise all read live.
pub(super) async fn session_and_tags(
    cache: &CachedAuthzStore,
    account_id: &str,
    role_name: &str,
    session_name: &str,
    snapshot: Option<&SessionSnapshot>,
) -> Result<
    (
        Option<Arc<PolicyDocument>>,
        HashMap<String, String>,
        Option<String>,
    ),
    DynamoDbError,
> {
    let Some(snapshot) = snapshot else {
        return super::fetch_session_data_and_tags(cache, account_id, role_name, session_name)
            .await;
    };
    let role_tags = super::wrap_tags(cache.fetch_role_tags(account_id, role_name)).await?;
    let mut tags = (*role_tags).clone();
    for (key, value) in &snapshot.tags {
        tags.insert(key.clone(), value.clone());
    }
    Ok((
        snapshot.policy.clone(),
        tags,
        snapshot.token_issue_time.clone(),
    ))
}

/// Set the `aws:ResourceAccount` value for `action`, evaluated for a caller
/// in `account_id`. ExtendDB builds every `DynamoDB` resource ARN with the
/// caller's account, so a `dynamodb:` action's resource account is that
/// account. Another service's resource names no account (an S3 bucket or
/// object ARN carries none), so for its actions the value the caller of the
/// evaluation passed stands: the bulk authorizer passes the caller's
/// account on the filesystem store, where every bucket is in it, and none
/// on the S3 store.
pub(super) fn set_resource_account(params: &mut RequestParams, action: &str, account_id: &str) {
    if action.starts_with("dynamodb:") {
        params.resource_account = Some(account_id.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use extenddb_auth::AuthIdentity;
    use extenddb_auth::policy::context::RequestParams;
    use extenddb_core::error::DynamoDbError;
    use serde_json::{Value, json};

    use super::authorize_action;
    use crate::authorization::check_authorization;
    use crate::authz_cache::CachedAuthzStore;
    use crate::authz_cache::tests::{CountingAuthzStore, fast_authz_cfg};
    use crate::authz_resource::AuthzTarget;

    const ACCOUNT: &str = "123456789012";

    fn user() -> AuthIdentity {
        AuthIdentity::User {
            account_id: ACCOUNT.to_owned(),
            user_name: "u".to_owned(),
        }
    }

    fn role_session() -> AuthIdentity {
        AuthIdentity::RoleSession {
            account_id: ACCOUNT.to_owned(),
            role_name: "r".to_owned(),
            session_name: "s".to_owned(),
        }
    }

    /// A cache over a store in which user `u` and role `r` both hold
    /// `policies` as their identity policies.
    fn cache_with(policies: &[Value]) -> CachedAuthzStore {
        let store = CountingAuthzStore::default();
        let docs: Vec<String> = policies.iter().map(Value::to_string).collect();
        store
            .user_policies
            .lock()
            .unwrap()
            .insert((ACCOUNT.to_owned(), "u".to_owned()), docs.clone());
        store
            .role_policies
            .lock()
            .unwrap()
            .insert((ACCOUNT.to_owned(), "r".to_owned()), docs);
        CachedAuthzStore::new(Arc::new(store), fast_authz_cfg())
    }

    fn statement(effect: &str, action: &str, resource: &str) -> Value {
        json!({"Effect": effect, "Action": action, "Resource": resource})
    }

    fn policy(statements: Vec<Value>) -> Value {
        json!({"Version": "2012-10-17", "Statement": statements})
    }

    fn listing(prefix: Option<&str>) -> RequestParams {
        RequestParams {
            s3_prefix: prefix.map(ToOwned::to_owned),
            ..Default::default()
        }
    }

    /// The decision `authorize_action` reaches: `true` for an allow, `false`
    /// for an `AccessDeniedException`. Any other error fails the test.
    async fn allowed(
        cache: &CachedAuthzStore,
        who: &AuthIdentity,
        action: &str,
        resource: &str,
        params: RequestParams,
    ) -> bool {
        match authorize_action(cache, who, None, action, resource, params).await {
            Ok(()) => true,
            Err(DynamoDbError::AccessDeniedException(_)) => false,
            Err(other) => panic!("{action} on {resource}: expected a decision, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_object_is_allowed_only_on_keys_in_the_granted_bucket() {
        let cache = cache_with(&[policy(vec![statement(
            "Allow",
            "s3:GetObject",
            "arn:aws:s3:::b/*",
        )])]);
        for who in [user(), role_session()] {
            let decide = |action: &'static str, resource: &'static str| {
                allowed(&cache, &who, action, resource, RequestParams::default())
            };
            assert!(decide("s3:GetObject", "arn:aws:s3:::b/k").await, "{who:?}");
            assert!(
                decide("s3:GetObject", "arn:aws:s3:::b/a/b/c").await,
                "{who:?}"
            );
            // A key in another bucket, including one whose name starts with
            // the granted bucket's name, is denied.
            assert!(
                !decide("s3:GetObject", "arn:aws:s3:::other/k").await,
                "{who:?}"
            );
            assert!(
                !decide("s3:GetObject", "arn:aws:s3:::bb/k").await,
                "{who:?}"
            );
            // The bucket ARN is not an object in it, and the grant is for one
            // action only.
            assert!(!decide("s3:GetObject", "arn:aws:s3:::b").await, "{who:?}");
            assert!(!decide("s3:PutObject", "arn:aws:s3:::b/k").await, "{who:?}");
        }
    }

    #[tokio::test]
    async fn list_bucket_under_a_string_like_prefix_condition() {
        let cache = cache_with(&[policy(vec![json!({
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::b",
            "Condition": {"StringLike": {"s3:prefix": "loads/*"}}
        })])]);
        for who in [user(), role_session()] {
            let decide = |prefix: Option<&'static str>| {
                allowed(
                    &cache,
                    &who,
                    "s3:ListBucket",
                    "arn:aws:s3:::b",
                    listing(prefix),
                )
            };
            assert!(decide(Some("loads/")).await, "{who:?}");
            assert!(decide(Some("loads/2026/")).await, "{who:?}");
            assert!(!decide(Some("other/")).await, "{who:?}");
            // The whole bucket (an empty prefix) and a listing with no prefix
            // key at all do not match the pattern.
            assert!(!decide(Some("")).await, "{who:?}");
            assert!(!decide(None).await, "{who:?}");
            // The grant names the bucket: another bucket is denied with a
            // matching prefix.
            assert!(
                !allowed(
                    &cache,
                    &who,
                    "s3:ListBucket",
                    "arn:aws:s3:::c",
                    listing(Some("loads/"))
                )
                .await,
                "{who:?}"
            );
        }
    }

    #[tokio::test]
    async fn an_explicit_deny_wins_over_an_allow() {
        // The Allow and the Deny sit in separate policies, so the Deny binds
        // across the principal's policy set.
        let cache = cache_with(&[
            policy(vec![statement("Allow", "s3:*", "*")]),
            policy(vec![statement(
                "Deny",
                "s3:GetObject",
                "arn:aws:s3:::b/private/*",
            )]),
        ]);
        for who in [user(), role_session()] {
            let decide = |action: &'static str, resource: &'static str| {
                allowed(&cache, &who, action, resource, RequestParams::default())
            };
            assert!(
                !decide("s3:GetObject", "arn:aws:s3:::b/private/k").await,
                "{who:?}"
            );
            assert!(
                decide("s3:GetObject", "arn:aws:s3:::b/public/k").await,
                "{who:?}"
            );
            // The Deny names one action; the Allow still grants the others on
            // the same key.
            assert!(
                decide("s3:PutObject", "arn:aws:s3:::b/private/k").await,
                "{who:?}"
            );
        }
    }

    #[tokio::test]
    async fn an_s3_action_has_no_resource_account_and_a_dynamodb_action_keeps_it() {
        let absent = json!({"Null": {"aws:ResourceAccount": "true"}});
        let own = json!({"StringEquals": {"aws:ResourceAccount": ACCOUNT}});
        let cache = cache_with(&[policy(vec![
            json!({"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*",
                   "Condition": absent.clone()}),
            json!({"Effect": "Allow", "Action": "s3:PutObject", "Resource": "*",
                   "Condition": own.clone()}),
            json!({"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
                   "Condition": absent}),
            json!({"Effect": "Allow", "Action": "dynamodb:PutItem", "Resource": "*",
                   "Condition": own}),
        ])]);
        let table = format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/t");
        for who in [user(), role_session()] {
            let s3 = |action: &'static str| {
                allowed(
                    &cache,
                    &who,
                    action,
                    "arn:aws:s3:::b/k",
                    RequestParams::default(),
                )
            };
            // An S3 ARN names no account, so the key is absent for S3 actions.
            assert!(s3("s3:GetObject").await, "{who:?}");
            assert!(!s3("s3:PutObject").await, "{who:?}");
            // A DynamoDB operation still carries the caller's account.
            let dynamodb = |operation: &'static str| {
                let target = AuthzTarget::new(operation, table.clone());
                let (cache, who) = (&cache, &who);
                async move {
                    check_authorization(cache, who, &target, false, RequestParams::default()).await
                }
            };
            assert!(dynamodb("PutItem").await.is_ok(), "{who:?}");
            assert!(
                matches!(
                    dynamodb("GetItem").await,
                    Err(DynamoDbError::AccessDeniedException(_))
                ),
                "{who:?}"
            );
        }
    }
}
