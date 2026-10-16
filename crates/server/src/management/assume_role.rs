// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AssumeRole` endpoint — generates temporary credentials for an IAM role.
//!
//! Authenticates via Basic auth (admin only). The caller ARN is provided in
//! the request body — the admin asserts which principal is assuming the role.
//! Trust policy evaluates against the provided caller ARN.

use std::sync::Arc;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::ManagementState;
use super::auth::authenticate_admin;
use super::crypto::{
    encrypt_secret, generate_secret_key, generate_session_key_id, generate_session_token,
};
use extenddb_auth::policy::condition::evaluate_condition;
use extenddb_auth::policy::context::AssumeRoleContext;
use extenddb_auth::policy::document::parse_conditions;

#[derive(Deserialize)]
pub struct AssumeRoleRequest {
    /// The ARN of the principal assuming the role.
    caller_arn: String,
    /// Session name (identifies this particular assumed-role session).
    session_name: String,
    /// Optional session tags (merged with role tags; session wins on conflict).
    #[serde(default)]
    session_tags: Option<Value>,
    /// Optional inline session policy (further restricts permissions).
    #[serde(default)]
    session_policy: Option<Value>,
    /// Session duration in seconds (default 3600, min 900, max 43200).
    #[serde(default = "default_duration")]
    duration_seconds: i64,
    /// Optional external ID for trust policy condition evaluation (`sts:ExternalId`).
    #[serde(default)]
    external_id: Option<String>,
}

fn default_duration() -> i64 {
    3600
}

#[derive(Serialize)]
struct AssumeRoleResponse {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expiration: String,
}

/// `POST /management/accounts/{id}/roles/{name}/assume` — assume a role.
///
/// Authenticates via Basic auth (admin only). The caller ARN is provided in
/// the request body. Trust policy evaluates against the provided caller ARN.
/// Returns ASIA* temporary credentials.
pub async fn assume_role(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path((account_id, role_name)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Response {
    // Authenticate via Basic auth — admin only.
    let source_ip = addr.ip().to_string();
    if let Err(e) = authenticate_admin(
        &headers,
        &*state.catalog_store,
        &*state.catalog_store,
        Some(&source_ip),
    )
    .await
    {
        return e;
    }

    // Parse request body.
    let request: AssumeRoleRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Invalid request body: {e}"),
            )
                .into_response();
        }
    };

    let caller_arn = &request.caller_arn;

    // Validate duration.
    if request.duration_seconds < 900 || request.duration_seconds > 43200 {
        return (
            StatusCode::BAD_REQUEST,
            "duration_seconds must be between 900 and 43200",
        )
            .into_response();
    }

    // Validate session_name: 2-64 chars, [a-zA-Z0-9_=,.@-] per AWS STS rules.
    if request.session_name.len() < 2
        || request.session_name.len() > 64
        || !request.session_name.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'_' | b'=' | b',' | b'.' | b'@' | b'-')
        })
    {
        return (
            StatusCode::BAD_REQUEST,
            "session_name must be 2-64 characters: alphanumeric, _=,.@-",
        )
            .into_response();
    }

    // Validate session policy if provided.
    if let Some(ref sp) = request.session_policy
        && (sp.get("Version").is_none() || sp.get("Statement").is_none())
    {
        return (
            StatusCode::BAD_REQUEST,
            "Session policy must contain Version and Statement",
        )
            .into_response();
    }

    // Load the role and its trust policy.
    let trust_policy = match state
        .catalog_store
        .get_role_trust_policy(&account_id, &role_name)
        .await
    {
        Ok(Some(tp)) => tp,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, "IAM role not found").into_response();
        }
        Err(e) => {
            tracing::error!("Management API: fetch role for assume-role failed: {e:?}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Evaluate trust policy against the provided caller ARN.
    if !evaluate_trust_policy(
        &trust_policy,
        caller_arn,
        request.external_id.as_deref(),
        &*state.catalog_store,
    )
    .await
    {
        return (
            StatusCode::FORBIDDEN,
            "Caller is not authorized to assume this role",
        )
            .into_response();
    }

    generate_and_store_session(
        &*state.catalog_store,
        &state.auth_cache,
        &account_id,
        &role_name,
        &request,
        caller_arn,
    )
    .await
}

/// Generate ASIA* credentials, encrypt, store session, and return the response.
async fn generate_and_store_session(
    store: &dyn extenddb_storage::CatalogStore,
    auth_cache: &extenddb_auth::AuthCacheRegistry,
    account_id: &str,
    role_name: &str,
    request: &AssumeRoleRequest,
    caller_arn: &str,
) -> Response {
    // P119: Use cached encryption key if available, fall back to DB query.
    let enc_key_b64: String =
        if let Some(k) = extenddb_storage::CatalogStore::cached_encryption_key(store) {
            k
        } else {
            match store.get_setting("encryption_key").await {
                Ok(Some(k)) => k,
                Ok(None) => {
                    tracing::error!("Management API: encryption key not found in settings");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                Err(e) => {
                    tracing::error!("Management API: fetch encryption key failed: {e:?}");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            }
        };

    // Generate ASIA* temporary credentials.
    let access_key_id = generate_session_key_id();
    let secret_key = generate_secret_key();
    let session_token = generate_session_token();

    let encrypted = match encrypt_secret(&secret_key, &enc_key_b64, &access_key_id) {
        Ok(e) => e,
        Err(msg) => {
            tracing::error!("Management API: encrypt session secret failed: {msg}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let expires_at =
        time::OffsetDateTime::now_utc() + time::Duration::seconds(request.duration_seconds);

    match store
        .store_session(
            &session_token,
            &access_key_id,
            &encrypted,
            account_id,
            role_name,
            &request.session_name,
            &request.session_tags,
            &request.session_policy,
            expires_at,
        )
        .await
    {
        Ok(()) => {
            // Drop any negative-cached entry for this newly-issued access
            // key (e.g. from a probe before the session existed). Also drop
            // any cached session-data for this exact (role, session) tuple
            // — overwriting an existing session via re-assume would
            // otherwise serve stale policies/tags until TTL.
            auth_cache.invalidate_credential(&access_key_id).await;
            auth_cache
                .invalidate_session(account_id, role_name, &request.session_name)
                .await;
            tracing::warn!(
                target: "extenddb::audit::manage",
                "assume-role: account={}, role={}, session={}, caller={}",
                account_id, role_name, request.session_name, caller_arn,
            );
            let expiration = expires_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default();
            (
                StatusCode::CREATED,
                axum::Json(AssumeRoleResponse {
                    access_key_id,
                    secret_access_key: secret_key,
                    session_token,
                    expiration,
                }),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Management API: store assume-role session failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Trust policy evaluation
// ---------------------------------------------------------------------------

/// Evaluate a trust policy to determine if `caller_arn` is allowed to assume
/// the role.
///
/// Follows IAM evaluation order: an explicit `Deny` that matches the caller
/// wins over any `Allow`, exactly as the identity-policy evaluator does
/// (`policy/evaluator.rs`, phase 1). Only if no matching `Deny` is found does
/// a matching `Allow` grant assumption. See ADR-0022.
///
/// A statement "matches" when its Principal matches the caller and all of its
/// conditions pass. Principal matching supports:
/// - `"*"` (any caller)
/// - `{"AWS": "arn:..."}` (single ARN)
/// - `{"AWS": ["arn:...", ...]}` (list of ARNs)
///
/// Conditions are evaluated using the policy engine's `AssumeRoleContext`,
/// which supports `aws:PrincipalTag/*` and `sts:ExternalId` keys. A statement
/// whose `Condition` block fails to parse is treated as non-matching: it can
/// neither grant via `Allow` nor block via `Deny`. Treating an unparseable
/// `Deny` as "does not match" is the fail-open direction for that one
/// statement, but a malformed trust policy is an administrator error surfaced
/// at role creation, and matching the parse-failure handling of the `Allow`
/// path keeps the two consistent.
async fn evaluate_trust_policy(
    trust_policy: &Value,
    caller_arn: &str,
    external_id: Option<&str>,
    store: &dyn extenddb_storage::CatalogStore,
) -> bool {
    let Some(statements) = trust_policy.get("Statement").and_then(|s| s.as_array()) else {
        return false;
    };

    let principal_tags = fetch_caller_tags(caller_arn, store).await;
    let context = AssumeRoleContext {
        principal_tags,
        external_id: external_id.map(String::from),
    };

    trust_decision(statements, caller_arn, &context)
}

/// Decide assumption from already-resolved statements and context.
///
/// Split out from `evaluate_trust_policy` so the Deny-over-Allow ordering is
/// unit-testable without a `CatalogStore` (the store is only needed to build
/// the context's principal tags).
fn trust_decision(statements: &[Value], caller_arn: &str, context: &AssumeRoleContext) -> bool {
    // Phase 1: explicit Deny. Any matching Deny statement blocks assumption
    // outright, regardless of any Allow present in the same policy.
    for stmt in statements {
        if effect_of(stmt) == "Deny" && statement_matches(stmt, caller_arn, context) {
            return false;
        }
    }

    // Phase 2: Allow. With no matching Deny, a matching Allow grants assumption.
    for stmt in statements {
        if effect_of(stmt) == "Allow" && statement_matches(stmt, caller_arn, context) {
            return true;
        }
    }

    // Implicit deny: no statement granted assumption.
    false
}

/// The `Effect` string of a statement, or `""` when absent/non-string.
fn effect_of(stmt: &Value) -> &str {
    stmt.get("Effect").and_then(|e| e.as_str()).unwrap_or("")
}

/// Whether a trust-policy statement matches the caller: its Principal matches
/// `caller_arn` and every condition passes. Independent of the statement's
/// `Effect`, so both the Deny and Allow phases share one matching definition.
fn statement_matches(stmt: &Value, caller_arn: &str, context: &AssumeRoleContext) -> bool {
    if !principal_matches(&stmt["Principal"], caller_arn) {
        return false;
    }
    match parse_conditions(&stmt["Condition"]) {
        Ok(conds) => conds.iter().all(|c| evaluate_condition(c, context)),
        Err(_) => false,
    }
}

/// Check if a Principal field matches the given caller ARN.
fn principal_matches(principal: &Value, caller_arn: &str) -> bool {
    match principal {
        // "*" matches any caller.
        Value::String(s) if s == "*" => true,
        // {"AWS": "arn:..."} or {"AWS": ["arn:...", ...]}
        Value::Object(map) => {
            if let Some(aws) = map.get("AWS") {
                match aws {
                    Value::String(arn) => arn == "*" || arn == caller_arn,
                    Value::Array(arns) => arns
                        .iter()
                        .any(|a| a.as_str().is_some_and(|s| s == "*" || s == caller_arn)),
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Caller tag lookup
// ---------------------------------------------------------------------------

/// Fetch tags for the caller principal from the database.
///
/// Parses the caller ARN to determine if it's a user or role, then fetches
/// the appropriate tags. Returns an empty map if the ARN format is unrecognized
/// or the query fails.
async fn fetch_caller_tags(
    caller_arn: &str,
    store: &dyn extenddb_storage::CatalogStore,
) -> std::collections::HashMap<String, String> {
    let parts: Vec<&str> = caller_arn.splitn(6, ':').collect();
    if parts.len() < 6 {
        return std::collections::HashMap::new();
    }
    let account_id = parts[4];
    let resource = parts[5];

    match store.fetch_caller_tags(account_id, resource).await {
        Ok(tags) => tags.into_iter().collect(),
        Err(e) => {
            tracing::error!("fetch_caller_tags failed for {caller_arn}: {e:?}");
            std::collections::HashMap::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn ctx() -> AssumeRoleContext {
        AssumeRoleContext {
            principal_tags: HashMap::new(),
            external_id: None,
        }
    }

    fn ctx_with_tags(tags: &[(&str, &str)]) -> AssumeRoleContext {
        AssumeRoleContext {
            principal_tags: tags
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            external_id: None,
        }
    }

    const CALLER: &str = "arn:aws:iam::123456789012:user/alice";

    // ---- principal_matches ------------------------------------------------

    #[test]
    fn principal_wildcard_string_matches_any() {
        assert!(principal_matches(&json!("*"), CALLER));
    }

    #[test]
    fn principal_aws_single_arn_matches_exact() {
        assert!(principal_matches(&json!({"AWS": CALLER}), CALLER));
        assert!(!principal_matches(
            &json!({"AWS": "arn:aws:iam::123456789012:user/bob"}),
            CALLER
        ));
    }

    #[test]
    fn principal_aws_wildcard_matches_any() {
        assert!(principal_matches(&json!({"AWS": "*"}), CALLER));
    }

    #[test]
    fn principal_aws_list_matches_any_member() {
        let p = json!({"AWS": ["arn:aws:iam::123456789012:user/bob", CALLER]});
        assert!(principal_matches(&p, CALLER));
        let p2 = json!({"AWS": ["arn:aws:iam::123456789012:user/bob"]});
        assert!(!principal_matches(&p2, CALLER));
    }

    #[test]
    fn principal_missing_aws_does_not_match() {
        assert!(!principal_matches(
            &json!({"Service": "ec2.amazonaws.com"}),
            CALLER
        ));
        assert!(!principal_matches(&json!(null), CALLER));
    }

    // ---- effect_of --------------------------------------------------------

    #[test]
    fn effect_of_reads_string_or_defaults_empty() {
        assert_eq!(effect_of(&json!({"Effect": "Allow"})), "Allow");
        assert_eq!(effect_of(&json!({"Effect": "Deny"})), "Deny");
        assert_eq!(effect_of(&json!({})), "");
        assert_eq!(effect_of(&json!({"Effect": 5})), "");
    }

    // ---- trust_decision: core regression ---------------------------------

    #[test]
    fn allow_grants_when_principal_matches() {
        let stmts = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER}})];
        assert!(trust_decision(&stmts, CALLER, &ctx()));
    }

    #[test]
    fn no_matching_allow_is_implicit_deny() {
        let stmts = vec![json!({
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:user/bob"}
        })];
        assert!(!trust_decision(&stmts, CALLER, &ctx()));
    }

    /// The ticket regression: an explicit Deny must override a broad Allow.
    /// Before the fix, Deny statements were skipped entirely and the caller
    /// was granted assumption via the `Allow *`.
    #[test]
    fn explicit_deny_overrides_allow_regardless_of_order() {
        // Deny listed after Allow.
        let allow_then_deny = vec![
            json!({"Effect": "Allow", "Principal": "*"}),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}}),
        ];
        assert!(
            !trust_decision(&allow_then_deny, CALLER, &ctx()),
            "explicit Deny after Allow must block assumption"
        );

        // Deny listed before Allow.
        let deny_then_allow = vec![
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}}),
            json!({"Effect": "Allow", "Principal": "*"}),
        ];
        assert!(
            !trust_decision(&deny_then_allow, CALLER, &ctx()),
            "explicit Deny before Allow must block assumption"
        );
    }

    #[test]
    fn deny_targeting_other_principal_does_not_block() {
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": "*"}),
            json!({"Effect": "Deny", "Principal": {"AWS": "arn:aws:iam::123456789012:user/bob"}}),
        ];
        assert!(
            trust_decision(&stmts, CALLER, &ctx()),
            "a Deny aimed at a different principal must not block this caller"
        );
    }

    #[test]
    fn conditional_deny_only_blocks_when_condition_matches() {
        // Deny alice only when her PrincipalTag/team == "contractors".
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": "*"}),
            json!({
                "Effect": "Deny",
                "Principal": {"AWS": CALLER},
                "Condition": {"StringEquals": {"aws:PrincipalTag/team": "contractors"}}
            }),
        ];

        // Tag matches → Deny applies → blocked.
        assert!(!trust_decision(
            &stmts,
            CALLER,
            &ctx_with_tags(&[("team", "contractors")])
        ));

        // Tag does not match → Deny inert → Allow grants.
        assert!(trust_decision(
            &stmts,
            CALLER,
            &ctx_with_tags(&[("team", "employees")])
        ));
    }

    #[test]
    fn empty_statement_list_denies() {
        assert!(!trust_decision(&[], CALLER, &ctx()));
    }
}
