// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization layer for DynamoDB requests.
//!
//! After authentication resolves an `AuthIdentity`, this module fetches the
//! applicable IAM policies, permissions boundary, and session policy via the
//! [`AuthorizationStore`] trait, builds a `RequestContext`, and evaluates
//! authorization using the policy engine from `extenddb-auth`.

use std::collections::HashMap;

use extenddb_auth::AuthIdentity;
use extenddb_auth::policy::context::{RequestContext, RequestParams};
use extenddb_auth::policy::document::PolicyDocument;
use extenddb_auth::policy::evaluator::{AuthzDecision, evaluate_policies};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::authorization_store::AuthorizationStore;
use extenddb_storage::management_store::OpError;

/// Evaluate whether the authenticated identity is authorized for the given
/// DynamoDB operation on the given resource.
///
/// For `AuthIdentity::User` and `AuthIdentity::RoleSession`, the full IAM
/// evaluation algorithm runs: explicit deny → permissions boundary → session
/// policy → identity allow → implicit deny.
pub async fn check_authorization(
    store: &dyn AuthorizationStore,
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
                store,
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
            access_key_id,
        } => {
            check_role_authorization(
                store,
                account_id,
                role_name,
                session_name,
                access_key_id,
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
    store: &dyn AuthorizationStore,
    account_id: &str,
    user_name: &str,
    operation: &str,
    resource_arn: &str,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{operation}");

    let authorization = store
        .fetch_user_authorization(account_id, user_name, resource_arn)
        .await
        .map_err(authz_store_error)?;
    let identity_policies = parse_policy_documents(&authorization.identity_policies, "policy")?;
    let boundary =
        parse_optional_policy(authorization.boundary.as_deref(), "permissions boundary")?;

    // Build request context.
    let context = RequestContext::build(
        tags_to_map(authorization.principal_tags),
        tags_to_map(authorization.resource_tags),
        is_scan,
        params,
    );

    let decision = evaluate_policies(
        &identity_policies,
        boundary.as_ref(),
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
    store: &dyn AuthorizationStore,
    account_id: &str,
    role_name: &str,
    session_name: &str,
    access_key_id: &str,
    operation: &str,
    resource_arn: &str,
    is_scan: bool,
    params: RequestParams,
) -> Result<(), DynamoDbError> {
    let action = format!("dynamodb:{operation}");

    let authorization = store
        .fetch_role_authorization(
            account_id,
            role_name,
            session_name,
            access_key_id,
            resource_arn,
        )
        .await
        .map_err(authz_store_error)?;
    let identity_policies = parse_policy_documents(&authorization.identity_policies, "policy")?;
    let boundary =
        parse_optional_policy(authorization.boundary.as_deref(), "permissions boundary")?;
    let session_policy =
        parse_optional_policy(authorization.session_policy.as_deref(), "session policy")?;

    // Build request context.
    let context = RequestContext::build(
        tags_to_map(authorization.principal_tags),
        tags_to_map(authorization.resource_tags),
        is_scan,
        params,
    );

    let decision = evaluate_policies(
        &identity_policies,
        boundary.as_ref(),
        session_policy.as_ref(),
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
// Helpers — convert store results to authorization types
// ---------------------------------------------------------------------------

fn authz_store_error(error: OpError) -> DynamoDbError {
    tracing::error!("Authorization: fetch metadata failed: {error:?}");
    DynamoDbError::InternalServerError("Internal error during authorization".to_owned())
}

/// Parse policy JSON strings into `PolicyDocument`s. Fail closed on parse errors.
fn parse_policy_documents(
    jsons: &[String],
    label: &str,
) -> Result<Vec<PolicyDocument>, DynamoDbError> {
    let mut docs = Vec::with_capacity(jsons.len());
    for json_str in jsons {
        match PolicyDocument::from_json(json_str) {
            Ok(doc) => docs.push(doc),
            Err(e) => {
                // Fail closed: an unparseable stored policy denies access rather
                // than being silently skipped.
                tracing::error!("Authorization: unparseable {label}: {e}");
                return Err(DynamoDbError::AccessDeniedException(
                    "Not authorized to perform this action (policy evaluation error)".to_owned(),
                ));
            }
        }
    }
    Ok(docs)
}

/// Parse a boundary policy JSON string into a `PolicyDocument`. Fail closed on parse errors.
fn parse_optional_policy(
    json: Option<&str>,
    label: &str,
) -> Result<Option<PolicyDocument>, DynamoDbError> {
    match json {
        Some(json_str) => match PolicyDocument::from_json(json_str) {
            Ok(doc) => Ok(Some(doc)),
            Err(e) => {
                tracing::error!("Authorization: unparseable {label}: {e}");
                Err(DynamoDbError::AccessDeniedException(
                    "Not authorized to perform this action (policy evaluation error)".to_owned(),
                ))
            }
        },
        None => Ok(None),
    }
}

fn tags_to_map(tags: Vec<(String, String)>) -> HashMap<String, String> {
    tags.into_iter().collect()
}
