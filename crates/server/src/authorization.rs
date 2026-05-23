// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization layer for DynamoDB requests.
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
use extenddb_auth::policy::evaluator::{AuthzDecision, evaluate_policies_arc};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::management_store::OpError;

use crate::authz_cache::{CachedAuthzStore, PolicyList, TagMap};

/// Evaluate whether the authenticated identity is authorized for the given
/// DynamoDB operation on the given resource.
///
/// For `AuthIdentity::User` and `AuthIdentity::RoleSession`, the full IAM
/// evaluation algorithm runs: explicit deny → permissions boundary → session
/// policy → identity allow → implicit deny.
pub async fn check_authorization(
    cache: &CachedAuthzStore,
    identity: &AuthIdentity,
    operation: &str,
    resource_arn: &str,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => {
            check_user_authorization(
                cache,
                account_id,
                user_name,
                operation,
                resource_arn,
                is_scan,
                params,
            )
            .await
        }
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
                operation,
                resource_arn,
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
    operation: &str,
    resource_arn: &str,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{operation}");

    // Fetch all 5 authz inputs concurrently — they are independent cache
    // lookups (or DB queries, on cold start). All come back pre-parsed.
    let (user_policies, group_policies, boundary, principal_tags, resource_tags) = tokio::try_join!(
        wrap_policies(cache.fetch_user_policies(account_id, user_name)),
        wrap_policies(cache.fetch_user_group_policies(account_id, user_name)),
        wrap_boundary(cache.fetch_user_boundary(account_id, user_name)),
        wrap_tags(cache.fetch_user_tags(account_id, user_name)),
        wrap_tags(cache.fetch_resource_tags(resource_arn)),
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
        Err(DynamoDbError::AccessDeniedException(format!(
            "User: arn:aws:iam::{account_id}:user/{user_name} is not authorized \
             to perform: {action} on resource: {resource_arn}"
        )))
    }
}

#[allow(clippy::too_many_arguments)]
async fn check_role_authorization(
    cache: &CachedAuthzStore,
    account_id: &str,
    role_name: &str,
    session_name: &str,
    operation: &str,
    resource_arn: &str,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{operation}");

    let (identity_policies, boundary, (session_policy, principal_tags), resource_tags) = tokio::try_join!(
        wrap_policies(cache.fetch_role_policies(account_id, role_name)),
        wrap_boundary(cache.fetch_role_boundary(account_id, role_name)),
        fetch_session_data_and_tags(cache, account_id, role_name, session_name),
        wrap_tags(cache.fetch_resource_tags(resource_arn)),
    )?;

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
        Err(DynamoDbError::AccessDeniedException(format!(
            "User: arn:aws:iam::{account_id}:assumed-role/{role_name}/{session_name} \
             is not authorized to perform: {action} on resource: {resource_arn}"
        )))
    }
}

// ---------------------------------------------------------------------------
// Helpers — translate cache results to authorization-layer types
// ---------------------------------------------------------------------------

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

async fn wrap_tags(
    fut: impl std::future::Future<Output = extenddb_storage::management_store::OpResult<TagMap>>,
) -> Result<TagMap, DynamoDbError> {
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
    if let Some(data) = session_data {
        session_policy = data.session_policy.clone();
        // Merge session tags (session wins on conflict). The session_data
        // value is shared via Arc; iterate borrowed and clone-on-insert.
        for (k, v) in &data.session_tags {
            tags.insert(k.clone(), v.clone());
        }
    }
    Ok((session_policy, tags))
}
