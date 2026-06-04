// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Admin break-glass cache invalidation endpoint.
//!
//! `POST /management/cache/invalidate` — drop cached entries on demand
//! from any of the auth/authz/table-key-info caches. Complements the
//! automatic write-through hooks (see `docs/design/12-auth-authz-cache.md`
//! §6 / §6.1) for cases the hooks miss: off-instance changes before TTL,
//! bugs in the write-through plumbing, or test scenarios needing a
//! deterministic flush.
//!
//! Both this endpoint and the console form (`/console/cache/invalidate`)
//! delegate to [`apply`] so behavior is identical regardless of how it's
//! invoked.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::ManagementState;
use super::auth::authenticate_admin;

/// Request body for `POST /management/cache/invalidate`. The `scope`
/// discriminator drives validation of `selectors`.
#[derive(Debug, Deserialize)]
pub struct InvalidateRequest {
    pub scope: Scope,
    #[serde(default)]
    pub selectors: Selectors,
}

/// Scope discriminator. Matches the CLI subcommands and the `scope`
/// field of the design doc's table.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    All,
    Account,
    Credential,
    User,
    Role,
    GroupMembers,
    TableKeyInfo,
    ResourceTags,
}

impl Scope {
    /// Canonical snake_case name (matches the JSON wire format, the CLI
    /// subcommand names, and the design doc table). Used in audit logs
    /// and the console success page so operators see the same
    /// identifier across every surface.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Account => "account",
            Self::Credential => "credential",
            Self::User => "user",
            Self::Role => "role",
            Self::GroupMembers => "group_members",
            Self::TableKeyInfo => "table_key_info",
            Self::ResourceTags => "resource_tags",
        }
    }
}

/// Scope-specific parameters. Each field is optional; the handler
/// validates that the right ones are present for the chosen scope so
/// invalid combinations return 400 with a clear message rather than
/// silently no-op'ing.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Selectors {
    pub account_id: Option<String>,
    pub user_name: Option<String>,
    pub role_name: Option<String>,
    pub user_names: Option<Vec<String>>,
    pub access_key_id: Option<String>,
    pub table_name: Option<String>,
    pub arn: Option<String>,
    /// For `scope: all`, callers must pass `confirm: true`. The CLI sets
    /// this when `--yes` is given; the console template uses a typed
    /// confirmation field. Refused without it to avoid catastrophic
    /// flushes from a stray request.
    pub confirm: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct InvalidateResponse {
    pub scope: Scope,
    /// Names of subcaches that were touched. Useful for confirming that
    /// composite scopes (`user`, `role`) did what the operator expected.
    pub invalidated: Vec<&'static str>,
}

/// `POST /management/cache/invalidate` — admin-only manual invalidation.
pub async fn invalidate(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    body: Result<axum::Json<InvalidateRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let admin_name = match authenticate_admin(
        &headers,
        &*state.catalog_store,
        &*state.catalog_store,
        None,
    )
    .await
    {
        Ok(name) => name,
        Err(e) => return e,
    };

    let body = match body {
        Ok(axum::Json(b)) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, e.body_text()).into_response(),
    };

    match apply(&state.auth_cache, body, &admin_name).await {
        Ok(resp) => (StatusCode::OK, axum::Json(resp)).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// Apply an invalidation request. Shared between the management API
/// and the console form handler so the two paths stay byte-identical.
///
/// Takes only the registry; both `Scope::All` and the per-scope
/// composites flow through registry methods, including
/// `invalidate_all_caches` for the global flush.
///
/// On success returns the response body to send (or render). On
/// validation failure returns a human-readable error string.
pub async fn apply(
    auth_cache: &extenddb_auth::AuthCacheRegistry,
    body: InvalidateRequest,
    admin_name: &str,
) -> Result<InvalidateResponse, String> {
    let s = body.selectors;
    let scope = body.scope;

    let invalidated: Vec<&'static str> = match scope {
        Scope::All => {
            if !s.confirm.unwrap_or(false) {
                return Err(
                    "scope 'all' requires \"confirm\": true to prevent accidental flushes"
                        .to_owned(),
                );
            }
            auth_cache.invalidate_all_caches();
            vec!["authz", "table_key_info", "credentials"]
        }
        Scope::Account => {
            let account_id = require(&s.account_id, "account_id")?;
            auth_cache.invalidate_account(account_id).await;
            vec!["authz_account", "credentials_account"]
        }
        Scope::Credential => {
            let key = require(&s.access_key_id, "access_key_id")?;
            auth_cache.invalidate_credential(key).await;
            vec!["credential"]
        }
        Scope::User => {
            let account_id = require(&s.account_id, "account_id")?;
            let user_name = require(&s.user_name, "user_name")?;
            auth_cache
                .invalidate_user_policies(account_id, user_name)
                .await;
            auth_cache
                .invalidate_user_group_policies(account_id, user_name)
                .await;
            auth_cache
                .invalidate_user_boundary(account_id, user_name)
                .await;
            auth_cache.invalidate_user_tags(account_id, user_name).await;
            auth_cache
                .invalidate_principal_credentials(account_id, user_name)
                .await;
            vec![
                "user_policies",
                "user_group_policies",
                "user_boundary",
                "user_tags",
                "principal_credentials",
            ]
        }
        Scope::Role => {
            let account_id = require(&s.account_id, "account_id")?;
            let role_name = require(&s.role_name, "role_name")?;
            auth_cache
                .invalidate_role_policies(account_id, role_name)
                .await;
            auth_cache
                .invalidate_role_boundary(account_id, role_name)
                .await;
            auth_cache.invalidate_role_tags(account_id, role_name).await;
            auth_cache
                .invalidate_role_sessions(account_id, role_name)
                .await;
            auth_cache
                .invalidate_principal_credentials(account_id, role_name)
                .await;
            vec![
                "role_policies",
                "role_boundary",
                "role_tags",
                "role_sessions",
                "principal_credentials",
            ]
        }
        Scope::GroupMembers => {
            let account_id = require(&s.account_id, "account_id")?;
            let user_names = s
                .user_names
                .as_deref()
                .ok_or_else(|| "user_names is required for scope 'group_members'".to_owned())?;
            if user_names.is_empty() {
                return Err("user_names must not be empty for scope 'group_members'".to_owned());
            }
            auth_cache
                .invalidate_users_group_policies(account_id, user_names)
                .await;
            vec!["user_group_policies"]
        }
        Scope::TableKeyInfo => {
            let account_id = require(&s.account_id, "account_id")?;
            let table_name = require(&s.table_name, "table_name")?;
            auth_cache
                .invalidate_table_key_info(account_id, table_name)
                .await;
            vec!["table_key_info"]
        }
        Scope::ResourceTags => {
            let arn = require(&s.arn, "arn")?;
            auth_cache.invalidate_resource_tags(arn).await;
            vec!["resource_tags"]
        }
    };

    // Forensic audit trail. Includes every selector that was set so a
    // post-incident reader can identify the exact target — `?selectors`
    // skips `None` fields. Operators correlate this with the matching
    // jump in the `invalidations` counter on /auth-cache-metrics.
    tracing::info!(
        admin = admin_name,
        scope = scope.as_str(),
        selectors = ?s,
        "auth_cache: admin-triggered invalidation"
    );

    Ok(InvalidateResponse { scope, invalidated })
}

fn require<'a>(field: &'a Option<String>, name: &'static str) -> Result<&'a str, String> {
    field
        .as_deref()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("{name} is required for the chosen scope"))
}
