// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization layer for `DynamoDB` requests.
//!
//! After authentication resolves an `AuthIdentity`, this module fetches the
//! applicable IAM policies, permissions boundary, and session policy via
//! [`CachedAuthzStore`] (which sits on top of the storage [`AuthorizationStore`]
//! trait), builds a `RequestContext`, and evaluates authorization using the
//! policy engine from `extenddb-auth`.
//!
//! All policy documents are pre-parsed by the cache, so this layer never
//! invokes `PolicyDocument::from_json` on the request hot path.
//!
//! See `docs/design/12-auth-authz-cache.md`.

use std::collections::HashMap;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::{RequestContext, RequestParams};
use extenddb_auth::policy::evaluator::{
    AuthzDecision, evaluate_policies_arc, explicit_deny_matches_arc,
};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::management_store::OpError;

use crate::authz_cache::{CachedAuthzStore, PolicyList, TagMap};
use crate::authz_resource::AuthzTarget;

/// Evaluate whether the authenticated identity is authorized for the given
/// `DynamoDB` operation on the given resource.
///
/// Policies are matched against `target.resource`, the canonical ARN for this
/// deployment; a denial names `target.supplied`, the resource as the caller
/// wrote it, as the service does.
///
/// For `AuthIdentity::User` and `AuthIdentity::RoleSession`, the full IAM
/// evaluation algorithm runs: explicit deny → permissions boundary → session
/// policy → identity allow → implicit deny.
pub async fn check_authorization(
    cache: &CachedAuthzStore,
    identity: &AuthIdentity,
    target: &AuthzTarget,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => check_user_authorization(cache, account_id, user_name, target, is_scan, params).await,
        AuthIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
        } => {
            check_role_authorization(
                cache,
                account_id,
                role_name,
                session_name,
                target,
                is_scan,
                params,
            )
            .await
        }
    }
}

async fn check_user_authorization(
    cache: &CachedAuthzStore,
    account_id: &str,
    user_name: &str,
    target: &AuthzTarget,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{}", target.action);
    let resource_arn = target.resource.as_str();

    // Resource tags apply only to a table, and to an index through its base
    // table (indexes inherit the table's tags). A stream, backup or any other
    // resource has none, so `aws:ResourceTag/*` is absent there, as on the
    // service (see `resource_tag_lookup_arn`).
    let tag_arn = crate::authz_resource::resource_tag_lookup_arn(resource_arn);

    // Fetch all authz inputs concurrently; they are independent cache
    // lookups (or DB queries, on cold start). All come back pre-parsed.
    let (user_policies, group_policies, boundary, principal_tags, resource_tags, created_at) = tokio::try_join!(
        wrap_policies(cache.fetch_user_policies(account_id, user_name)),
        wrap_policies(cache.fetch_user_group_policies(account_id, user_name)),
        wrap_boundary(cache.fetch_user_boundary(account_id, user_name)),
        wrap_tags(cache.fetch_user_tags(account_id, user_name)),
        wrap_tags(fetch_authz_resource_tags(cache, tag_arn.as_deref())),
        wrap_created_at(cache.fetch_user_created_at(account_id, user_name)),
    )?;

    // Combine identity policies. Each side is an `Arc<Vec<Arc<PolicyDocument>>>`;
    // we build a single Vec<Arc<...>> for the evaluator. The per-element
    // clones are atomic-only (Arc inner ref counts).
    let mut identity_policies: Vec<
        std::sync::Arc<extenddb_auth::policy::document::PolicyDocument>,
    > = Vec::with_capacity(user_policies.len() + group_policies.len());
    identity_policies.extend(user_policies.iter().cloned());
    identity_policies.extend(group_policies.iter().cloned());

    // Build request context. RequestContext::build expects owned HashMaps,
    // so we deref through the Arc and clone once. This is the only place
    // where the tag map allocation is unavoidable; future iterations could
    // change RequestContext to hold Arc<HashMap<...>> for true zero-copy.
    let mut params = params;
    params.principal_arn = Some(format!("arn:aws:iam::{account_id}:user/{user_name}"));
    params.principal_account = Some(account_id.to_owned());
    params.principal_type = Some("User".to_owned());
    params.username = Some(user_name.to_owned());
    params.userid = Some(extenddb_auth::policy::principal_id::user_unique_id(
        account_id,
        user_name,
        created_at
            .as_deref()
            .map(String::as_str)
            .unwrap_or_default(),
    ));
    // The target table's owner account. ExtendDB builds the resource ARN with
    // the caller's account, so the resource account is that account.
    params.resource_account = Some(account_id.to_owned());
    let context = RequestContext::build(
        (*principal_tags).clone(),
        (*resource_tags).clone(),
        is_scan,
        params,
    );

    let decision = evaluate_policies_arc(
        &identity_policies,
        boundary.as_deref(),
        None,
        &action,
        resource_arn,
        &context,
    );

    if decision == AuthzDecision::Allow {
        Ok(())
    } else {
        tracing::warn!(
            principal = format!("arn:aws:iam::{account_id}:user/{user_name}"),
            action = action,
            resource = resource_arn,
            "Authorization denied"
        );
        let principal = format!("arn:aws:iam::{account_id}:user/{user_name}");
        let explicit = explicit_deny_matches_arc(
            &identity_policies,
            boundary.as_deref(),
            None,
            &action,
            resource_arn,
            &context,
        );
        Err(DynamoDbError::AccessDeniedException(deny_message(
            &principal,
            &action,
            &target.supplied,
            explicit,
        )))
    }
}

async fn check_role_authorization(
    cache: &CachedAuthzStore,
    account_id: &str,
    role_name: &str,
    session_name: &str,
    target: &AuthzTarget,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{}", target.action);
    let resource_arn = target.resource.as_str();

    // Resource tags for a table or index only (see the user path).
    let tag_arn = crate::authz_resource::resource_tag_lookup_arn(resource_arn);

    let (
        identity_policies,
        boundary,
        (session_policy, principal_tags, token_issue_time),
        resource_tags,
        role_created_at,
    ) = tokio::try_join!(
        wrap_policies(cache.fetch_role_policies(account_id, role_name)),
        wrap_boundary(cache.fetch_role_boundary(account_id, role_name)),
        fetch_session_data_and_tags(cache, account_id, role_name, session_name),
        wrap_tags(fetch_authz_resource_tags(cache, tag_arn.as_deref())),
        wrap_created_at(cache.fetch_role_created_at(account_id, role_name)),
    )?;

    let mut params = params;
    params.principal_arn = Some(format!(
        "arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}"
    ));
    params.principal_account = Some(account_id.to_owned());
    // A role session's principal type is AssumedRole; the service does not
    // populate aws:username for it, so username stays None (absent).
    params.principal_type = Some("AssumedRole".to_owned());
    params.userid = Some(extenddb_auth::policy::principal_id::role_session_userid(
        account_id,
        role_name,
        session_name,
        role_created_at
            .as_deref()
            .map(String::as_str)
            .unwrap_or_default(),
    ));
    params.resource_account = Some(account_id.to_owned());
    // aws:TokenIssueTime for a role session is the session's issue time.
    params.token_issue_time = token_issue_time;
    // A role session is assumed without MFA, so aws:MultiFactorAuthPresent is
    // present and false, matching the service. An IAM user leaves it absent.
    params.multi_factor_auth_present = Some(false);
    let context = RequestContext::build(principal_tags, (*resource_tags).clone(), is_scan, params);

    // Convert the Arc<Vec<...>> to a slice for the evaluator. We materialize
    // a fresh Vec to allow appending session_policy in the future without
    // altering the cached value. For now the slice is read-only.
    let identity_slice: Vec<std::sync::Arc<extenddb_auth::policy::document::PolicyDocument>> =
        identity_policies.iter().cloned().collect();
    let decision = evaluate_policies_arc(
        &identity_slice,
        boundary.as_deref(),
        session_policy.as_deref(),
        &action,
        resource_arn,
        &context,
    );

    if decision == AuthzDecision::Allow {
        Ok(())
    } else {
        tracing::warn!(
            principal =
                format!("arn:aws:iam::{account_id}:assumed-role/{role_name}/{session_name}"),
            action = action,
            resource = resource_arn,
            "Authorization denied"
        );
        let principal =
            format!("arn:aws:iam::{account_id}:assumed-role/{role_name}/{session_name}");
        let explicit = explicit_deny_matches_arc(
            &identity_slice,
            boundary.as_deref(),
            session_policy.as_deref(),
            &action,
            resource_arn,
            &context,
        );
        Err(DynamoDbError::AccessDeniedException(deny_message(
            &principal,
            &action,
            &target.supplied,
            explicit,
        )))
    }
}

// ---------------------------------------------------------------------------
// Helpers — translate cache results to authorization-layer types
// ---------------------------------------------------------------------------

/// Build the AccessDeniedException message in the form the service uses, which
/// depends on why the request was denied. `action` already carries the
/// `dynamodb:` prefix; `resource_arn` is the resource as the caller supplied
/// it, partition, service and region included. An explicit Deny statement yields "with an explicit deny
/// in an identity-based policy"; any other denial yields "because no
/// identity-based policy allows the <action> action", measured against live
/// DynamoDB.
pub(crate) fn deny_message(
    principal: &str,
    action: &str,
    resource_arn: &str,
    explicit: bool,
) -> String {
    let head = format!(
        "User: {principal} is not authorized to perform: {action} on resource: {resource_arn}"
    );
    if explicit {
        format!("{head} with an explicit deny in an identity-based policy")
    } else {
        format!("{head} because no identity-based policy allows the {action} action")
    }
}

/// Translate an `OpResult` from the cache into a `DynamoDbError`.
///
/// Parse failures are reported by the cache as `OpError::Internal("policy
/// parse failed: ...")`; those map to `AccessDeniedException` (fail-closed).
/// Other internal errors map to `InternalServerError`.
fn op_err_to_dynamo(e: OpError) -> DynamoDbError {
    match &e {
        OpError::Internal(msg) if msg.starts_with("policy parse failed") => {
            tracing::error!("Authorization: {msg}");
            DynamoDbError::AccessDeniedException(
                "Not authorized to perform this action (policy evaluation error)".to_owned(),
            )
        }
        _ => {
            tracing::error!("Authorization: cache load failed: {e:?}");
            DynamoDbError::InternalServerError("Internal error during authorization".to_owned())
        }
    }
}

async fn wrap_policies(
    fut: impl std::future::Future<Output = extenddb_storage::management_store::OpResult<PolicyList>>,
) -> Result<PolicyList, DynamoDbError> {
    fut.await.map_err(op_err_to_dynamo)
}

async fn wrap_boundary(
    fut: impl std::future::Future<
        Output = extenddb_storage::management_store::OpResult<
            Option<std::sync::Arc<extenddb_auth::policy::document::PolicyDocument>>,
        >,
    >,
) -> Result<Option<std::sync::Arc<extenddb_auth::policy::document::PolicyDocument>>, DynamoDbError>
{
    fut.await.map_err(op_err_to_dynamo)
}

/// The resource tags for authorization: those stored under `tag_arn`, or an
/// empty set when the resource carries none (`tag_arn` is `None`).
async fn fetch_authz_resource_tags(
    cache: &CachedAuthzStore,
    tag_arn: Option<&str>,
) -> extenddb_storage::management_store::OpResult<TagMap> {
    match tag_arn {
        Some(arn) => cache.fetch_resource_tags(arn).await,
        None => Ok(TagMap::default()),
    }
}

async fn wrap_tags(
    fut: impl std::future::Future<Output = extenddb_storage::management_store::OpResult<TagMap>>,
) -> Result<TagMap, DynamoDbError> {
    fut.await.map_err(op_err_to_dynamo)
}

async fn wrap_created_at(
    fut: impl std::future::Future<
        Output = extenddb_storage::management_store::OpResult<Option<std::sync::Arc<String>>>,
    >,
) -> Result<Option<std::sync::Arc<String>>, DynamoDbError> {
    fut.await.map_err(op_err_to_dynamo)
}

async fn fetch_session_data_and_tags(
    cache: &CachedAuthzStore,
    account_id: &str,
    role_name: &str,
    session_name: &str,
) -> Result<
    (
        Option<std::sync::Arc<extenddb_auth::policy::document::PolicyDocument>>,
        HashMap<String, String>,
        Option<String>,
    ),
    DynamoDbError,
> {
    // Fetch role tags and session data concurrently.
    let (role_tags, session_data) = tokio::try_join!(
        wrap_tags(cache.fetch_role_tags(account_id, role_name)),
        async {
            cache
                .fetch_session_data(account_id, role_name, session_name)
                .await
                .map_err(op_err_to_dynamo)
        },
    )?;

    // Merge: start from a clone of role tags (we need to mutate-and-overlay
    // session tags). The clone happens once per request even if the session
    // is absent — acceptable; merging into-place is the simplest semantics.
    let mut tags: HashMap<String, String> = (*role_tags).clone();
    let mut session_policy = None;
    let mut token_issue_time = None;
    if let Some(data) = session_data {
        session_policy = data.session_policy.clone();
        token_issue_time = data.token_issue_time.clone();
        // Merge session tags (session wins on conflict). The session_data
        // value is shared via Arc; iterate borrowed and clone-on-insert.
        for (k, v) in &data.session_tags {
            tags.insert(k.clone(), v.clone());
        }
    }
    Ok((session_policy, tags, token_issue_time))
}

/// The value of `aws:SourceIp` for a request that arrived from `peer`.
///
/// An IPv4 client that reaches a dual-stack (`::`) listener appears as the
/// IPv4-mapped IPv6 address `::ffff:a.b.c.d`. The service evaluates such a
/// client as its IPv4 address, so an IPv4 CIDR in `IpAddress` or `NotIpAddress`
/// matches it. The mapped form is converted back to IPv4 here; any other
/// address is returned as is.
pub(crate) fn condition_source_ip(peer: &std::net::SocketAddr) -> String {
    peer.ip().to_canonical().to_string()
}

#[cfg(test)]
mod source_ip_tests {
    use super::condition_source_ip;
    use std::net::SocketAddr;

    #[test]
    fn ipv4_mapped_peer_is_evaluated_as_ipv4() {
        let mapped: SocketAddr = "[::ffff:127.0.0.1]:443".parse().unwrap();
        assert_eq!(condition_source_ip(&mapped), "127.0.0.1");
        let mapped_private: SocketAddr = "[::ffff:10.1.2.3]:443".parse().unwrap();
        assert_eq!(condition_source_ip(&mapped_private), "10.1.2.3");
    }

    #[test]
    fn other_peers_are_unchanged() {
        let v4: SocketAddr = "10.1.2.3:443".parse().unwrap();
        assert_eq!(condition_source_ip(&v4), "10.1.2.3");
        let v6: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        assert_eq!(condition_source_ip(&v6), "2001:db8::1");
        let loopback6: SocketAddr = "[::1]:443".parse().unwrap();
        assert_eq!(condition_source_ip(&loopback6), "::1");
    }
}

#[cfg(test)]
mod tests {
    use super::deny_message;

    const P: &str = "arn:aws:iam::964157134968:user/alice";
    const R: &str = "arn:aws:dynamodb:us-east-1:964157134968:table/bb";

    #[test]
    fn explicit_deny_message_matches_service() {
        assert_eq!(
            deny_message(P, "dynamodb:TagResource", R, true),
            "User: arn:aws:iam::964157134968:user/alice is not authorized to perform: \
             dynamodb:TagResource on resource: arn:aws:dynamodb:us-east-1:964157134968:table/bb \
             with an explicit deny in an identity-based policy"
        );
    }

    #[test]
    fn implicit_deny_message_matches_service() {
        assert_eq!(
            deny_message(P, "dynamodb:GetItem", R, false),
            "User: arn:aws:iam::964157134968:user/alice is not authorized to perform: \
             dynamodb:GetItem on resource: arn:aws:dynamodb:us-east-1:964157134968:table/bb \
             because no identity-based policy allows the dynamodb:GetItem action"
        );
    }
}

#[cfg(test)]
mod resource_tag_tests {
    use super::{check_role_authorization, check_user_authorization, fetch_authz_resource_tags};
    use crate::authz_cache::CachedAuthzStore;
    use crate::authz_cache::tests::{CountingAuthzStore, fast_authz_cfg};
    use crate::authz_resource::AuthzTarget;
    use extenddb_auth::policy::context::RequestParams;
    use extenddb_core::error::DynamoDbError;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    const ACCOUNT: &str = "123456789012";
    const TABLE: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/t";
    const STREAM: &str =
        "arn:aws:dynamodb:us-east-1:123456789012:table/t/stream/2026-10-20T16:17:40.188";
    const BACKUP: &str =
        "arn:aws:dynamodb:us-east-1:123456789012:table/t/backup/01792513080555-82c5bd04";

    const POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
        "Action":["dynamodb:DescribeTable","dynamodb:DescribeStream","dynamodb:DescribeBackup"],
        "Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/k":"v"}}}]}"#;

    /// User `u` and role `r` are allowed DescribeTable, DescribeStream and
    /// DescribeBackup only when `aws:ResourceTag/k` is `v`, with the table, the
    /// stream and the backup ARN all carrying `k=v` in the store.
    fn store() -> Arc<CountingAuthzStore> {
        let s = CountingAuthzStore::default();
        s.user_policies.lock().unwrap().insert(
            (ACCOUNT.to_owned(), "u".to_owned()),
            vec![POLICY.to_owned()],
        );
        s.role_policies.lock().unwrap().insert(
            (ACCOUNT.to_owned(), "r".to_owned()),
            vec![POLICY.to_owned()],
        );
        for arn in [TABLE, STREAM, BACKUP] {
            s.resource_tags
                .lock()
                .unwrap()
                .insert(arn.to_owned(), vec![("k".to_owned(), "v".to_owned())]);
        }
        Arc::new(s)
    }

    async fn check(cache: &CachedAuthzStore, op: &str, arn: &str) -> Result<(), DynamoDbError> {
        check_user_authorization(
            cache,
            ACCOUNT,
            "u",
            &AuthzTarget::new(op, arn.to_owned()),
            false,
            RequestParams::default(),
        )
        .await
    }

    /// The policy is matched against the canonical resource and the denial
    /// names the resource as the caller supplied it (issue #447): a respelled
    /// region is not allowed by the tag condition on the canonical table (the
    /// stream carries no tags), and the message carries the respelled ARN.
    #[tokio::test]
    async fn denial_names_the_supplied_resource() {
        let inner = store();
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());
        let supplied =
            "arn:aws-cn:dynamodb:us-east-1:123456789012:table/t/stream/2026-10-20T16:17:40.188";
        let target = AuthzTarget {
            action: "DescribeStream".to_owned(),
            resource: STREAM.to_owned(),
            supplied: supplied.to_owned(),
        };
        let err = check_user_authorization(
            &cache,
            ACCOUNT,
            "u",
            &target,
            false,
            RequestParams::default(),
        )
        .await
        .unwrap_err();
        let DynamoDbError::AccessDeniedException(msg) = err else {
            panic!("expected AccessDeniedException, got {err:?}");
        };
        assert_eq!(
            msg,
            format!(
                "User: arn:aws:iam::{ACCOUNT}:user/u is not authorized to perform: \
                 dynamodb:DescribeStream on resource: {supplied} because no \
                 identity-based policy allows the dynamodb:DescribeStream action"
            )
        );
        let err = check_role_authorization(
            &cache,
            ACCOUNT,
            "r",
            "s",
            &target,
            false,
            RequestParams::default(),
        )
        .await
        .unwrap_err();
        let DynamoDbError::AccessDeniedException(msg) = err else {
            panic!("expected AccessDeniedException, got {err:?}");
        };
        assert!(
            msg.contains(&format!("on resource: {supplied} because")),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn stream_and_backup_operations_read_no_resource_tags() {
        let inner = store();
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());
        // The table's tag satisfies the condition.
        assert!(check(&cache, "DescribeTable", TABLE).await.is_ok());
        let fetched = inner.resource_tags_calls.load(Ordering::SeqCst);
        // Tags stored under the stream and backup ARNs are never read, so the
        // same condition does not match there.
        for (op, arn) in [("DescribeStream", STREAM), ("DescribeBackup", BACKUP)] {
            assert!(
                matches!(
                    check(&cache, op, arn).await,
                    Err(DynamoDbError::AccessDeniedException(_))
                ),
                "{op} on {arn}"
            );
        }
        assert_eq!(inner.resource_tags_calls.load(Ordering::SeqCst), fetched);
    }

    #[tokio::test]
    async fn role_session_stream_and_backup_operations_read_no_resource_tags() {
        let inner = store();
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());
        let role = |op: &'static str, arn: &'static str| async {
            check_role_authorization(
                &cache,
                ACCOUNT,
                "r",
                "s",
                &AuthzTarget::new(op, arn.to_owned()),
                false,
                RequestParams::default(),
            )
            .await
        };
        assert!(role("DescribeTable", TABLE).await.is_ok());
        let fetched = inner.resource_tags_calls.load(Ordering::SeqCst);
        for (op, arn) in [("DescribeStream", STREAM), ("DescribeBackup", BACKUP)] {
            assert!(
                matches!(
                    role(op, arn).await,
                    Err(DynamoDbError::AccessDeniedException(_))
                ),
                "{op} on {arn}"
            );
        }
        assert_eq!(inner.resource_tags_calls.load(Ordering::SeqCst), fetched);
    }

    #[tokio::test]
    async fn absent_tag_arn_yields_empty_tags_without_a_lookup() {
        let inner = store();
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());
        assert!(
            fetch_authz_resource_tags(&cache, None)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(inner.resource_tags_calls.load(Ordering::SeqCst), 0);
        let tags = fetch_authz_resource_tags(&cache, Some(TABLE))
            .await
            .unwrap();
        assert_eq!(tags.get("k").map(String::as_str), Some("v"));
    }
}
