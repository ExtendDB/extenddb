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
use extenddb_auth::policy::condition::{ConditionMatch, evaluate_statement_conditions};
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
    let source_ip = crate::authorization::condition_source_ip(&addr);
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

    // Session tags passed on the call require sts:TagSession in the trust
    // policy, so the STS action set the decision checks depends on their
    // presence. They also populate aws:RequestTag/* and aws:TagKeys for
    // condition evaluation.
    let has_session_tags = request.session_tags.as_ref().is_some_and(value_has_entries);
    let request_tags = parse_session_tags(request.session_tags.as_ref());

    if let Some(st) = request.session_tags.as_ref() {
        let pairs = session_tag_pairs(st);
        if let Err(e) = super::validate_iam_tags(&pairs) {
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
        if let Some(dup) = super::duplicate_tag_key_ci(&pairs) {
            return (
                StatusCode::BAD_REQUEST,
                format!("Duplicate session tag keys found (tag keys are case insensitive): {dup}"),
            )
                .into_response();
        }
    }

    // Evaluate trust policy against the provided caller ARN. aws:SourceIp and
    // aws:UserAgent are the AssumeRole connection's peer address and User-Agent,
    // which is what STS records for the call.
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(ToOwned::to_owned);
    if !evaluate_trust_policy(
        &trust_policy,
        caller_arn,
        request.external_id.as_deref(),
        &request.session_name,
        has_session_tags,
        request_tags,
        &state.region,
        state.secure_transport,
        Some(source_ip.as_str()),
        user_agent.as_deref(),
        &account_id,
        &*state.catalog_store,
    )
    .await
    {
        tracing::warn!(
            target: "extenddb::audit::manage",
            "assume-role denied: account={}, role={}, caller={}",
            account_id, role_name, caller_arn,
        );
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

/// Evaluate a trust policy to decide whether `caller_arn` may assume the role,
/// given the external id and whether session tags were passed.
///
/// Follows IAM order per requested STS action. An `AssumeRole` call exercises
/// `sts:AssumeRole`; passing session tags additionally exercises
/// `sts:TagSession`. ExtendDB does not implement source identity, so
/// `sts:SetSourceIdentity` is never exercised. Assumption is granted only when
/// every exercised action has a matching `Allow` and no matching `Deny`, with
/// an explicit `Deny` winning over any `Allow` (ADR-0022).
///
/// A statement matches an action when its `Principal` matches the caller, its
/// `Action`/`NotAction` matches the action, and its `Condition` block passes. A
/// `Deny` whose condition fails to parse fails closed: it blocks, so a
/// malformed stored `Deny` cannot silently grant assumption. Documents are
/// validated at write time (`validate_trust_policy`), so a well-formed store
/// does not reach the fail-closed path in normal operation.
#[allow(clippy::too_many_arguments)]
async fn evaluate_trust_policy(
    trust_policy: &Value,
    caller_arn: &str,
    external_id: Option<&str>,
    session_name: &str,
    has_session_tags: bool,
    request_tags: std::collections::HashMap<String, String>,
    region: &str,
    secure_transport: bool,
    source_ip: Option<&str>,
    user_agent: Option<&str>,
    account_id: &str,
    store: &dyn extenddb_storage::CatalogStore,
) -> bool {
    let Some(statements) = statements_vec(trust_policy) else {
        return false;
    };

    let principal_tags = fetch_caller_tags(caller_arn, store).await;
    let caller_created_at = fetch_caller_created_at(caller_arn, store).await;
    let (current_time, epoch_time) = extenddb_auth::policy::context::request_time_strings();
    let context = AssumeRoleContext {
        principal_tags,
        external_id: external_id.map(String::from),
        // sts:RoleSessionName is the session name on the AssumeRole call; STS
        // populates it for every call.
        session_name: Some(session_name.to_owned()),
        principal_arn: caller_arn.to_owned(),
        principal_account: caller_account(caller_arn).map(ToOwned::to_owned),
        request_tags,
        region: Some(region.to_owned()),
        secure_transport,
        principal_type: caller_principal_type(caller_arn).map(ToOwned::to_owned),
        username: caller_username(caller_arn).map(ToOwned::to_owned),
        source_ip: source_ip.map(ToOwned::to_owned),
        user_agent: user_agent.map(ToOwned::to_owned),
        // aws:userid of the caller (an AIDA... id for a user, role-id:session
        // for a role session), derived from the caller ARN and the caller
        // principal's creation time so a recreated principal gets a new id.
        userid: extenddb_auth::policy::principal_id::userid_for_caller_arn(
            caller_arn,
            caller_created_at.as_deref(),
        ),
        // aws:ResourceAccount is the account that owns the role being assumed.
        resource_account: Some(account_id.to_owned()),
        current_time,
        epoch_time,
    };

    trust_decision_for(
        &statements,
        caller_arn,
        &context,
        has_session_tags,
        trust_policy_expands_variables(trust_policy),
    )
}

/// Whether a trust policy expands policy variables in its condition values:
/// only a `2012-10-17` document does. A `2008-10-17` one, or one with no
/// `Version`, compares `${...}` as literal text (measured with AssumeRole on
/// fresh roles, issue #534).
fn trust_policy_expands_variables(trust_policy: &Value) -> bool {
    trust_policy.get("Version").and_then(Value::as_str) == Some("2012-10-17")
}

/// The `aws:PrincipalType` for a caller ARN: `User` for an IAM user ARN,
/// `AssumedRole` for an assumed-role ARN. `None` for any other form, which then
/// fails closed if a trust policy conditions on `aws:PrincipalType`. Measured
/// live: an IAM user caller is `User`, a role session is `AssumedRole`.
fn caller_principal_type(caller_arn: &str) -> Option<&'static str> {
    let resource = caller_arn.splitn(6, ':').nth(5)?;
    if resource.starts_with("user/") {
        Some("User")
    } else if resource.starts_with("assumed-role/") {
        Some("AssumedRole")
    } else {
        None
    }
}

/// The `aws:username` for a caller ARN: the user name for an IAM user ARN;
/// `None` otherwise (the service does not populate it for a role session).
fn caller_username(caller_arn: &str) -> Option<&str> {
    caller_arn.splitn(6, ':').nth(5)?.strip_prefix("user/")
}

/// The statements of a policy document as an owned list. `Statement` may be a
/// single object or an array; both are supported, matching IAM. Returns `None`
/// when `Statement` is absent or neither an object nor an array.
fn statements_vec(trust_policy: &Value) -> Option<Vec<Value>> {
    match trust_policy.get("Statement") {
        Some(Value::Array(arr)) => Some(arr.clone()),
        Some(obj @ Value::Object(_)) => Some(vec![obj.clone()]),
        _ => None,
    }
}

/// Decide assumption from resolved statements and context, for a
/// `2012-10-17` trust policy. Kept for the unit tests, whose statements stand
/// for such a document.
#[cfg(test)]
fn trust_decision(
    statements: &[Value],
    caller_arn: &str,
    context: &AssumeRoleContext,
    has_session_tags: bool,
) -> bool {
    trust_decision_for(statements, caller_arn, context, has_session_tags, true)
}

/// Decide assumption from resolved statements and context. Split out so the
/// per-action Deny-over-Allow ordering is unit-testable without a store.
/// `expand_variables` is whether the trust policy's `Version` expands policy
/// variables in condition values.
fn trust_decision_for(
    statements: &[Value],
    caller_arn: &str,
    context: &AssumeRoleContext,
    has_session_tags: bool,
    expand_variables: bool,
) -> bool {
    requested_actions(has_session_tags)
        .iter()
        .all(|action| action_permitted(statements, caller_arn, context, action, expand_variables))
}

/// The STS actions an `AssumeRole` request exercises.
fn requested_actions(has_session_tags: bool) -> Vec<&'static str> {
    let mut actions = vec!["sts:AssumeRole"];
    if has_session_tags {
        actions.push("sts:TagSession");
    }
    actions
}

/// Whether a single STS action is permitted: a matching explicit `Deny` blocks
/// it; otherwise a matching `Allow` grants it; with neither it is implicitly
/// denied.
fn action_permitted(
    statements: &[Value],
    caller_arn: &str,
    context: &AssumeRoleContext,
    action: &str,
    expand_variables: bool,
) -> bool {
    for stmt in statements {
        if effect_of(stmt).eq_ignore_ascii_case("Deny")
            && deny_statement_blocks(stmt, caller_arn, context, action, expand_variables)
        {
            return false;
        }
    }
    for stmt in statements {
        if effect_of(stmt).eq_ignore_ascii_case("Allow")
            && allow_statement_grants(stmt, caller_arn, context, action, expand_variables)
        {
            return true;
        }
    }
    false
}

/// The `Effect` string of a statement, or `""` when absent/non-string. Compared
/// case-insensitively by the caller, matching the identity-policy parser.
fn effect_of(stmt: &Value) -> &str {
    stmt.get("Effect").and_then(|e| e.as_str()).unwrap_or("")
}

/// Whether a `Deny` blocks the action: its principal and action match and its
/// condition passes. A condition that fails to parse fails closed (blocks).
fn deny_statement_blocks(
    stmt: &Value,
    caller_arn: &str,
    context: &AssumeRoleContext,
    action: &str,
    expand_variables: bool,
) -> bool {
    if !principal_matches(&stmt["Principal"], caller_arn) || !action_matches(stmt, action) {
        return false;
    }
    matches!(
        evaluate_conditions(stmt, context, expand_variables),
        ConditionOutcome::Pass | ConditionOutcome::Unparseable
    )
}

/// Whether an `Allow` grants the action: principal, action and condition all
/// match. A condition that fails to parse does not grant.
fn allow_statement_grants(
    stmt: &Value,
    caller_arn: &str,
    context: &AssumeRoleContext,
    action: &str,
    expand_variables: bool,
) -> bool {
    if !principal_matches(&stmt["Principal"], caller_arn) || !action_matches(stmt, action) {
        return false;
    }
    matches!(
        evaluate_conditions(stmt, context, expand_variables),
        ConditionOutcome::Pass
    )
}

/// Outcome of a statement's `Condition` block for a given context.
enum ConditionOutcome {
    Pass,
    Fail,
    Unparseable,
}

/// Evaluate a statement's `Condition` block. Scalar condition values (numbers,
/// booleans) are coerced to strings, matching IAM. An unknown operator or a
/// structurally invalid block is reported as `Unparseable`.
fn evaluate_conditions(
    stmt: &Value,
    context: &AssumeRoleContext,
    expand_variables: bool,
) -> ConditionOutcome {
    let normalized = normalize_condition_scalars(&stmt["Condition"]);
    match parse_conditions(&normalized) {
        Ok(conds) => match evaluate_statement_conditions(&conds, expand_variables, context) {
            ConditionMatch::Match => ConditionOutcome::Pass,
            ConditionMatch::NoMatch => ConditionOutcome::Fail,
            // A condition on a key STS populates but ExtendDB cannot resolve
            // here fails closed, the same as an operator the evaluator cannot
            // parse: a Deny blocks, an Allow does not grant.
            ConditionMatch::Unresolvable => ConditionOutcome::Unparseable,
        },
        Err(_) => ConditionOutcome::Unparseable,
    }
}

/// Whether a statement's `Action`/`NotAction` covers `action`, case-insensitive
/// and with `*` and prefix wildcards. A statement with neither field covers no
/// action.
fn action_matches(stmt: &Value, action: &str) -> bool {
    if let Some(actions) = action_list(stmt.get("Action")) {
        actions.iter().any(|p| action_pattern_matches(p, action))
    } else if let Some(not_actions) = action_list(stmt.get("NotAction")) {
        !not_actions
            .iter()
            .any(|p| action_pattern_matches(p, action))
    } else {
        false
    }
}

/// A statement's action field as a list of patterns, or `None` when absent.
fn action_list(v: Option<&Value>) -> Option<Vec<String>> {
    match v {
        Some(Value::String(s)) => Some(vec![s.clone()]),
        Some(Value::Array(arr)) => Some(
            arr.iter()
                .filter_map(|a| a.as_str().map(ToOwned::to_owned))
                .collect(),
        ),
        _ => None,
    }
}

/// Match an action pattern against an action, case-insensitive. Supports `*`
/// (any action) and a trailing-`*` prefix wildcard such as `sts:*`.
fn action_pattern_matches(pattern: &str, action: &str) -> bool {
    let p = pattern.to_ascii_lowercase();
    let a = action.to_ascii_lowercase();
    if p == "*" {
        return true;
    }
    if let Some(prefix) = p.strip_suffix('*') {
        return a.starts_with(prefix);
    }
    p == a
}

/// Whether a Principal field matches the caller ARN. Supports `"*"`, `{"AWS":
/// "<arn>"|"*"|"<account-id>"|"<account-root-arn>"}` and lists thereof.
/// Account-id and account-root principals match any caller in that account.
/// Objects without an `AWS` key (for example `Service` principals) never match
/// an IAM user or assumed-role caller.
fn principal_matches(principal: &Value, caller_arn: &str) -> bool {
    match principal {
        Value::String(s) => s == "*",
        Value::Object(map) => match map.get("AWS") {
            Some(Value::String(arn)) => principal_entry_matches(arn, caller_arn),
            Some(Value::Array(arns)) => arns
                .iter()
                .filter_map(Value::as_str)
                .any(|a| principal_entry_matches(a, caller_arn)),
            _ => false,
        },
        _ => false,
    }
}

/// Whether one `AWS` principal entry matches the caller. Exact ARN, `"*"`, bare
/// account id, and account-root ARN (`arn:aws:iam::<account>:root`) all match.
fn principal_entry_matches(entry: &str, caller_arn: &str) -> bool {
    if entry == "*" || entry == caller_arn {
        return true;
    }
    if let Some(account) = caller_account(caller_arn) {
        if entry == account {
            return true;
        }
        if let Some(root_account) = entry
            .strip_prefix("arn:aws:iam::")
            .and_then(|r| r.strip_suffix(":root"))
            && root_account == account
        {
            return true;
        }
    }
    false
}

/// The account id field of an ARN (`arn:partition:service:region:account:...`).
fn caller_account(caller_arn: &str) -> Option<&str> {
    caller_arn.split(':').nth(4).filter(|s| !s.is_empty())
}

/// Whether a JSON value is a non-empty object or array.
fn value_has_entries(v: &Value) -> bool {
    match v {
        Value::Object(o) => !o.is_empty(),
        Value::Array(a) => !a.is_empty(),
        _ => false,
    }
}

/// Extract session-tag `(key, value)` pairs preserving every entry (including
/// keys that collide by ASCII case), for write validation. Object and list
/// forms; scalar values coerced to strings.
fn session_tag_pairs(session_tags: &Value) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    match session_tags {
        Value::Object(obj) => {
            for (k, v) in obj {
                if let Some(s) = scalar_to_string(v) {
                    pairs.push((k.clone(), s));
                }
            }
        }
        Value::Array(arr) => {
            for entry in arr {
                let key = entry.get("Key").and_then(Value::as_str);
                let value = entry.get("Value").and_then(scalar_to_string);
                if let (Some(k), Some(v)) = (key, value) {
                    pairs.push((k.to_owned(), v));
                }
            }
        }
        _ => {}
    }
    pairs
}

/// Parse the request `session_tags` into a key/value map for condition
/// evaluation (`aws:RequestTag/*`, `aws:TagKeys`). Accepts the object form
/// `{"env": "prod"}` and the list form `[{"Key": "env", "Value": "prod"}]`.
/// Scalar values are coerced to their string form; non-scalar values are
/// skipped.
fn parse_session_tags(session_tags: Option<&Value>) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    match session_tags {
        Some(Value::Object(obj)) => {
            for (k, v) in obj {
                if let Some(s) = scalar_to_string(v) {
                    map.insert(k.clone(), s);
                }
            }
        }
        Some(Value::Array(arr)) => {
            for entry in arr {
                let key = entry.get("Key").and_then(Value::as_str);
                let value = entry.get("Value").and_then(scalar_to_string);
                if let (Some(k), Some(v)) = (key, value) {
                    map.insert(k.to_owned(), v);
                }
            }
        }
        _ => {}
    }
    map
}

/// A JSON scalar (string, number, bool) as its string form; `None` otherwise.
fn scalar_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Coerce scalar condition values (numbers, booleans) to strings, matching
/// IAM's handling of condition values, so a numeric or boolean value parses and
/// evaluates as its string form rather than being rejected.
fn normalize_condition_scalars(v: &Value) -> Value {
    match v {
        Value::Number(n) => Value::String(n.to_string()),
        Value::Bool(b) => Value::String(b.to_string()),
        Value::Array(a) => Value::Array(a.iter().map(normalize_condition_scalars).collect()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, val)| (k.clone(), normalize_condition_scalars(val)))
                .collect(),
        ),
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// Trust policy write-time validation
// ---------------------------------------------------------------------------

/// Allowed actions in an `AssumeRole` trust policy, lowercase for comparison.
const STS_TRUST_ACTIONS: [&str; 3] = ["sts:assumerole", "sts:tagsession", "sts:setsourceidentity"];

/// Validate a trust policy document at write time, rejecting the documents IAM
/// rejects for an `AssumeRole` trust policy and accepting those it accepts. The
/// error text is ExtendDB's own; the accepted and rejected sets match IAM.
///
/// Rejects: missing or non-string `Effect` and values other than Allow/Deny;
/// `NotPrincipal`; a missing `Principal`; a bare string principal (including
/// `"*"`); an `AWS` principal ARN containing a wildcard; a missing
/// `Action`/`NotAction`; an action outside the STS trust set; a
/// `Resource`/`NotResource` field; and a `Condition` with an unknown operator
/// or invalid structure. Accepts a missing `Version`, a single-object
/// `Statement`, numeric condition values and unknown condition keys.
pub(crate) fn validate_trust_policy(trust_policy: &Value) -> Result<(), String> {
    let Some(statements) = statements_vec(trust_policy) else {
        return Err("Trust policy must contain a Statement object or array".to_owned());
    };
    if statements.is_empty() {
        return Err("Trust policy Statement must not be empty".to_owned());
    }
    // The live service does not store a trust policy that contains a character
    // above U+00FF anywhere in the document (a key name, a value, a Sid, or a
    // tag key): CreateRole returns InternalFailure (MalformedPolicyDocument for
    // a Sid). Latin-1 characters (up to U+00FF, including the sharp s and
    // accented letters) are stored, though a non-ASCII condition key name or
    // tag key then reads as absent at evaluation (the trust path matches
    // ASCII-case only). Reject a document with a character above U+00FF to
    // match what the service will store.
    if json_has_char_above_latin1(trust_policy) {
        return Err("Trust policy must not contain a character above U+00FF".to_owned());
    }
    // Measured on live IAM CreateRole: a Sid is accepted only when it is empty
    // or entirely ASCII letters and digits; a hyphen, underscore, space, dot or
    // non-ASCII character is rejected with MalformedPolicyDocument. Identity
    // PutUserPolicy applies the same rule (see validate_statement_sids).
    for stmt in &statements {
        if let Some(sid) = stmt.get("Sid").and_then(serde_json::Value::as_str)
            && !sid.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return Err("Trust policy Sid must be ASCII letters and digits only".to_owned());
        }
    }
    for stmt in &statements {
        validate_trust_statement(stmt)?;
    }
    Ok(())
}

/// Whether any object key or string value in `v` contains a code point above
/// U+00FF. The live service will not store a trust policy with one.
fn json_has_char_above_latin1(v: &Value) -> bool {
    match v {
        Value::String(s) => s.chars().any(|c| c as u32 > 0xFF),
        Value::Array(a) => a.iter().any(json_has_char_above_latin1),
        Value::Object(o) => o
            .iter()
            .any(|(k, val)| k.chars().any(|c| c as u32 > 0xFF) || json_has_char_above_latin1(val)),
        _ => false,
    }
}

/// A statement field, treating JSON null as absent.
fn non_null(v: Option<&Value>) -> Option<&Value> {
    v.filter(|v| !v.is_null())
}

fn validate_trust_statement(stmt: &Value) -> Result<(), String> {
    match stmt.get("Effect").and_then(Value::as_str) {
        Some(e) if e.eq_ignore_ascii_case("Allow") || e.eq_ignore_ascii_case("Deny") => {}
        Some(e) => return Err(format!("invalid Effect: {e}")),
        None => return Err("statement is missing Effect".to_owned()),
    }

    if non_null(stmt.get("NotPrincipal")).is_some() {
        return Err("NotPrincipal is not allowed in a trust policy".to_owned());
    }
    match non_null(stmt.get("Principal")) {
        None => return Err("statement is missing Principal".to_owned()),
        Some(p) => validate_trust_principal(p)?,
    }

    match (
        non_null(stmt.get("Action")),
        non_null(stmt.get("NotAction")),
    ) {
        (None, None) => return Err("statement is missing Action".to_owned()),
        (Some(_), Some(_)) => return Err("statement has both Action and NotAction".to_owned()),
        (Some(a), None) | (None, Some(a)) => validate_trust_actions(a)?,
    }

    if non_null(stmt.get("Resource")).is_some() || non_null(stmt.get("NotResource")).is_some() {
        return Err("Resource is not allowed in a trust policy".to_owned());
    }

    if let Some(cond) = non_null(stmt.get("Condition")) {
        validate_trust_condition(cond)?;
    }

    Ok(())
}

/// Validate a `Condition` block at write time. Accepts the full IAM operator
/// vocabulary (including operators ExtendDB's evaluator does not implement, such
/// as `IpAddress`; a `Deny` using one of those fails closed at evaluation).
/// Rejects an invalid operator name, a non-object block, and a non-object
/// operator entry. Accepts string, number and boolean values and unknown
/// condition keys, matching IAM.
fn validate_trust_condition(cond: &Value) -> Result<(), String> {
    let obj = cond
        .as_object()
        .ok_or_else(|| "Condition must be an object".to_owned())?;
    for (op_name, keys) in obj {
        if !is_known_iam_condition_operator(op_name) {
            return Err(format!("invalid Condition operator: {op_name}"));
        }
        let keys_obj = keys
            .as_object()
            .ok_or_else(|| format!("Condition {op_name} must be an object"))?;
        for vals in keys_obj.values() {
            validate_condition_values(vals)?;
        }
    }
    Ok(())
}

fn validate_condition_values(v: &Value) -> Result<(), String> {
    let is_scalar = |x: &Value| x.is_string() || x.is_number() || x.is_boolean();
    match v {
        _ if is_scalar(v) => Ok(()),
        Value::Array(arr) if arr.iter().all(is_scalar) => Ok(()),
        _ => Err("condition value must be a string, number, boolean or array of those".to_owned()),
    }
}

/// Whether `name` is a known IAM condition operator (base name, after stripping
/// a `ForAllValues:`/`ForAnyValue:` prefix and an `IfExists` suffix). Used only
/// by write-time validation so the accepted operator set matches IAM even for
/// operators ExtendDB does not evaluate.
fn is_known_iam_condition_operator(name: &str) -> bool {
    let base = name
        .strip_prefix("ForAllValues:")
        .or_else(|| name.strip_prefix("ForAnyValue:"))
        .unwrap_or(name);
    let base = base.strip_suffix("IfExists").unwrap_or(base);
    matches!(
        base,
        "StringEquals"
            | "StringNotEquals"
            | "StringEqualsIgnoreCase"
            | "StringNotEqualsIgnoreCase"
            | "StringLike"
            | "StringNotLike"
            | "NumericEquals"
            | "NumericNotEquals"
            | "NumericLessThan"
            | "NumericLessThanEquals"
            | "NumericGreaterThan"
            | "NumericGreaterThanEquals"
            | "DateEquals"
            | "DateNotEquals"
            | "DateLessThan"
            | "DateLessThanEquals"
            | "DateGreaterThan"
            | "DateGreaterThanEquals"
            | "Bool"
            | "BinaryEquals"
            | "IpAddress"
            | "NotIpAddress"
            | "ArnEquals"
            | "ArnLike"
            | "ArnNotEquals"
            | "ArnNotLike"
            | "Null"
    )
}

fn validate_trust_principal(principal: &Value) -> Result<(), String> {
    match principal {
        Value::String(_) => {
            Err("Principal must be an object such as {\"AWS\": \"<arn>\"}".to_owned())
        }
        Value::Object(map) => {
            if map.is_empty() {
                return Err("Principal object must not be empty".to_owned());
            }
            if let Some(aws) = non_null(map.get("AWS")) {
                for entry in principal_entry_strings(aws)? {
                    validate_aws_principal_entry(&entry)?;
                }
            }
            Ok(())
        }
        _ => Err("invalid Principal value".to_owned()),
    }
}

fn principal_entry_strings(v: &Value) -> Result<Vec<String>, String> {
    match v {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Array(arr) => arr
            .iter()
            .map(|a| {
                a.as_str()
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| "principal entry must be a string".to_owned())
            })
            .collect(),
        _ => Err("principal value must be a string or array of strings".to_owned()),
    }
}

fn validate_aws_principal_entry(entry: &str) -> Result<(), String> {
    if entry == "*" {
        return Ok(());
    }
    if entry.contains('*') {
        return Err(format!(
            "wildcard is not allowed in a principal ARN: {entry}"
        ));
    }
    Ok(())
}

fn validate_trust_actions(actions: &Value) -> Result<(), String> {
    let list = match actions {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr
            .iter()
            .map(|a| {
                a.as_str()
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| "Action entry must be a string".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err("Action must be a string or array of strings".to_owned()),
    };
    if list.is_empty() {
        return Err("Action must not be empty".to_owned());
    }
    for a in &list {
        if !STS_TRUST_ACTIONS.contains(&a.to_ascii_lowercase().as_str()) {
            return Err(format!(
                "trust policy Action must be one of sts:AssumeRole, sts:TagSession, sts:SetSourceIdentity: {a}"
            ));
        }
    }
    Ok(())
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

/// Resolve the caller principal's creation time (RFC 3339), part of its
/// `aws:userid`. A user ARN resolves the user's creation time, an assumed-role
/// ARN the role's. `None` when the caller is not a local principal (an external
/// ARN), where `aws:userid` is then derived without it.
async fn fetch_caller_created_at(
    caller_arn: &str,
    store: &dyn extenddb_storage::CatalogStore,
) -> Option<String> {
    let parts: Vec<&str> = caller_arn.splitn(6, ':').collect();
    if parts.len() < 6 {
        return None;
    }
    let account_id = parts[4];
    let resource = parts[5];
    let result = if let Some(user_name) = resource.strip_prefix("user/") {
        store.fetch_user_created_at(account_id, user_name).await
    } else if let Some(rest) = resource.strip_prefix("assumed-role/") {
        let role = rest.split('/').next().unwrap_or("");
        store.fetch_role_created_at(account_id, role).await
    } else {
        Ok(None)
    };
    match result {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("fetch_caller_created_at failed for {caller_arn}: {e:?}");
            None
        }
    }
}

#[cfg(test)]
mod trust_version_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    const ACCT: &str = "123456789012";
    const CALLER: &str = "arn:aws:iam::123456789012:user/alice";
    const BOB: &str = "arn:aws:iam::123456789012:user/bob";

    fn mk_ctx(
        principal_tags: HashMap<String, String>,
        external_id: Option<String>,
        request_tags: HashMap<String, String>,
    ) -> AssumeRoleContext {
        AssumeRoleContext {
            principal_tags,
            external_id,
            session_name: Some("bb-session".to_owned()),
            principal_arn: CALLER.to_owned(),
            principal_account: Some(ACCT.to_owned()),
            request_tags,
            region: Some("us-east-1".to_owned()),
            secure_transport: true,
            principal_type: Some("User".to_owned()),
            username: Some("alice".to_owned()),
            source_ip: Some("203.0.113.9".to_owned()),
            user_agent: Some("aws-cli/2.0 Botocore/1.0".to_owned()),
            userid: Some("AIDAEXAMPLEALICEUSERID".to_owned()),
            resource_account: Some(ACCT.to_owned()),
            current_time: "2026-01-01T00:00:00Z".to_owned(),
            epoch_time: "1767225600".to_owned(),
        }
    }

    fn ctx() -> AssumeRoleContext {
        mk_ctx(HashMap::new(), None, HashMap::new())
    }

    fn ctx_ext(id: &str) -> AssumeRoleContext {
        mk_ctx(HashMap::new(), Some(id.to_owned()), HashMap::new())
    }

    fn ctx_with_tags(tags: &[(&str, &str)]) -> AssumeRoleContext {
        mk_ctx(
            tags.iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            None,
            HashMap::new(),
        )
    }

    fn ctx_with_request_tags(tags: &[(&str, &str)]) -> AssumeRoleContext {
        mk_ctx(
            HashMap::new(),
            None,
            tags.iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        )
    }

    /// Allow alice to assume (sts:AssumeRole).
    fn allow_alice() -> Value {
        json!({"Effect": "Allow", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"})
    }

    // ---- principal_matches ------------------------------------------------

    #[test]
    fn principal_wildcard_string_matches_any() {
        assert!(principal_matches(&json!("*"), CALLER));
    }

    #[test]
    fn principal_aws_single_arn_matches_exact() {
        assert!(principal_matches(&json!({"AWS": CALLER}), CALLER));
        assert!(!principal_matches(&json!({"AWS": BOB}), CALLER));
    }

    #[test]
    fn principal_aws_wildcard_matches_any() {
        assert!(principal_matches(&json!({"AWS": "*"}), CALLER));
    }

    #[test]
    fn principal_aws_list_matches_any_member() {
        assert!(principal_matches(&json!({"AWS": [BOB, CALLER]}), CALLER));
        assert!(!principal_matches(&json!({"AWS": [BOB]}), CALLER));
    }

    #[test]
    fn principal_account_root_matches_any_caller_in_account() {
        let root = format!("arn:aws:iam::{ACCT}:root");
        assert!(principal_matches(&json!({"AWS": root}), CALLER));
        let other_root = "arn:aws:iam::999999999999:root";
        assert!(!principal_matches(&json!({"AWS": other_root}), CALLER));
    }

    #[test]
    fn principal_bare_account_id_matches_any_caller_in_account() {
        assert!(principal_matches(&json!({"AWS": ACCT}), CALLER));
        assert!(!principal_matches(&json!({"AWS": "999999999999"}), CALLER));
    }

    #[test]
    fn principal_missing_aws_does_not_match() {
        assert!(!principal_matches(
            &json!({"Service": "ec2.amazonaws.com"}),
            CALLER
        ));
        assert!(!principal_matches(&json!(null), CALLER));
    }

    // ---- effect_of (case-insensitive compare at call sites) ---------------

    #[test]
    fn effect_of_reads_string_or_defaults_empty() {
        assert_eq!(effect_of(&json!({"Effect": "Allow"})), "Allow");
        assert_eq!(effect_of(&json!({"Effect": "Deny"})), "Deny");
        assert_eq!(effect_of(&json!({})), "");
        assert_eq!(effect_of(&json!({"Effect": 5})), "");
    }

    #[test]
    fn effect_lowercase_deny_blocks() {
        // IAM accepts and enforces a lowercase "deny".
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}),
            json!({"Effect": "deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    // ---- action_matches ---------------------------------------------------

    #[test]
    fn action_matches_exact_and_wildcards() {
        let a = json!({"Action": "sts:AssumeRole"});
        assert!(action_matches(&a, "sts:AssumeRole"));
        assert!(!action_matches(&a, "sts:TagSession"));
        let star = json!({"Action": "*"});
        assert!(action_matches(&star, "sts:AssumeRole"));
        let prefix = json!({"Action": "sts:*"});
        assert!(action_matches(&prefix, "sts:AssumeRole"));
        let list = json!({"Action": ["sts:AssumeRole", "sts:TagSession"]});
        assert!(action_matches(&list, "sts:TagSession"));
        let none = json!({"Principal": "*"});
        assert!(!action_matches(&none, "sts:AssumeRole"));
    }

    #[test]
    fn action_matches_not_action() {
        let na = json!({"NotAction": "sts:TagSession"});
        assert!(action_matches(&na, "sts:AssumeRole"));
        assert!(!action_matches(&na, "sts:TagSession"));
    }

    // ---- trust_decision: Deny-over-Allow core ----------------------------

    #[test]
    fn allow_grants_when_principal_matches() {
        assert!(trust_decision(&[allow_alice()], CALLER, &ctx(), false));
    }

    #[test]
    fn no_matching_allow_is_implicit_deny() {
        let stmts =
            vec![json!({"Effect": "Allow", "Principal": {"AWS": BOB}, "Action": "sts:AssumeRole"})];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    /// The ticket regression: an explicit Deny must override a broad Allow.
    #[test]
    fn explicit_deny_overrides_allow_regardless_of_order() {
        let allow_then_deny = vec![
            json!({"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}),
        ];
        assert!(!trust_decision(&allow_then_deny, CALLER, &ctx(), false));

        let deny_then_allow = vec![
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}),
            json!({"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}),
        ];
        assert!(!trust_decision(&deny_then_allow, CALLER, &ctx(), false));
    }

    #[test]
    fn deny_targeting_other_principal_does_not_block() {
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}),
            json!({"Effect": "Deny", "Principal": {"AWS": BOB}, "Action": "sts:AssumeRole"}),
        ];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn deny_on_account_root_blocks_caller() {
        let root = format!("arn:aws:iam::{ACCT}:root");
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": root}, "Action": "sts:AssumeRole"}),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn deny_on_bare_account_id_blocks_caller() {
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": ACCT}, "Action": "sts:AssumeRole"}),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    // ---- conditional Deny -------------------------------------------------

    #[test]
    fn conditional_deny_only_blocks_when_tag_matches() {
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}),
            json!({
                "Effect": "Deny",
                "Principal": {"AWS": CALLER},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"aws:PrincipalTag/team": "contractors"}}
            }),
        ];
        assert!(!trust_decision(
            &stmts,
            CALLER,
            &ctx_with_tags(&[("team", "contractors")]),
            false
        ));
        assert!(trust_decision(
            &stmts,
            CALLER,
            &ctx_with_tags(&[("team", "employees")]),
            false
        ));
    }

    #[test]
    fn external_id_notequals_deny_blocks_when_external_id_absent() {
        // Confused-deputy guard: Deny unless the caller presents the external
        // id. With no external id the negated condition holds, so Deny blocks.
        let stmts = vec![
            allow_alice(),
            json!({
                "Effect": "Deny",
                "Principal": {"AWS": CALLER},
                "Action": "sts:AssumeRole",
                "Condition": {"StringNotEquals": {"sts:ExternalId": "secret"}}
            }),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
        assert!(!trust_decision(&stmts, CALLER, &ctx_ext("wrong"), false));
        assert!(trust_decision(&stmts, CALLER, &ctx_ext("secret"), false));
    }

    #[test]
    fn string_not_equals_ignore_case_external_id() {
        // Deny alice unless the external id equals "secret" case-insensitively.
        // With a matching id (any case) the negated condition is false, so the
        // Deny does not apply and the Allow grants.
        let stmts = vec![
            allow_alice(),
            json!({
                "Effect": "Deny",
                "Principal": {"AWS": CALLER},
                "Action": "sts:AssumeRole",
                "Condition": {"StringNotEqualsIgnoreCase": {"sts:ExternalId": "secret"}}
            }),
        ];
        // Present and equal (mixed case): condition false, Deny inert, allowed.
        assert!(trust_decision(&stmts, CALLER, &ctx_ext("SECRET"), false));
        assert!(trust_decision(&stmts, CALLER, &ctx_ext("secret"), false));
        // Present and different: condition true, Deny applies, blocked.
        assert!(!trust_decision(&stmts, CALLER, &ctx_ext("other"), false));
        // Absent: a negated operator on an absent single-valued key matches, so
        // the Deny applies and blocks (confused-deputy guard).
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    // ---- fail closed on an unparseable Deny -------------------------------
    #[test]
    fn deny_with_unparseable_condition_fails_closed() {
        // BinaryEquals is a valid IAM operator the evaluator does not implement,
        // so a Deny using it must not be silently dropped.
        let stmts = vec![
            allow_alice(),
            json!({
                "Effect": "Deny",
                "Principal": {"AWS": CALLER},
                "Action": "sts:AssumeRole",
                "Condition": {"BinaryEquals": {"aws:PrincipalTag/x": "AAAA"}}
            }),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn allow_with_unparseable_condition_does_not_grant() {
        let stmts = vec![json!({
            "Effect": "Allow",
            "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"BinaryEquals": {"aws:PrincipalTag/x": "AAAA"}}
        })];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    // ---- resolved SourceIp / UserAgent on the trust path ------------------
    #[test]
    fn trust_ipaddress_source_ip_matches_caller() {
        // The caller address is 203.0.113.9 (mk_ctx). IpAddress on the enclosing
        // range grants; a range the caller is not in does not. Matches live STS.
        let in_range = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"IpAddress": {"aws:SourceIp": "203.0.113.0/24"}}})];
        assert!(trust_decision(&in_range, CALLER, &ctx(), false));
        let out_of_range = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"IpAddress": {"aws:SourceIp": "198.51.100.0/24"}}})];
        assert!(!trust_decision(&out_of_range, CALLER, &ctx(), false));
    }

    #[test]
    fn trust_notipaddress_deny_blocks_caller_outside_range() {
        // Deny unless the caller is inside 203.0.113.0/24. The caller is inside,
        // so NotIpAddress is false and the Deny does not apply.
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole",
                "Condition": {"NotIpAddress": {"aws:SourceIp": "203.0.113.0/24"}}}),
        ];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn trust_useragent_stringlike_grants() {
        let stmts = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringLike": {"aws:UserAgent": "*Botocore*"}}})];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn trust_userid_resolves_from_caller_arn() {
        // The caller ARN is an IAM user, so aws:userid is the derived AIDA id.
        let ctx = ctx();
        let expected = ctx.userid.clone().expect("userid set in mk_ctx");
        let stmts = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:userid": expected}}})];
        assert!(trust_decision(&stmts, CALLER, &ctx, false));
    }

    #[test]
    fn trust_rolesessionname_resolves_from_session_name() {
        // mk_ctx sets session_name = "bb-session". A Deny gated on the matching
        // session name blocks; a non-matching name does not. An Allow gated on
        // the matching name grants.
        let ctx = ctx();
        let deny_match = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:RoleSessionName": "bb-session"}}}),
        ];
        assert!(!trust_decision(&deny_match, CALLER, &ctx, false));
        let deny_other = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:RoleSessionName": "other"}}}),
        ];
        assert!(trust_decision(&deny_other, CALLER, &ctx, false));
        let allow_gate = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:RoleSessionName": "bb-session"}}})];
        assert!(trust_decision(&allow_gate, CALLER, &ctx, false));
        // Case-folded key name resolves too.
        let allow_folded = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:rolesessionname": "bb-session"}}})];
        assert!(trust_decision(&allow_folded, CALLER, &ctx, false));
    }

    // ---- resolved keys: PrincipalArn / PrincipalAccount -------------------

    #[test]
    fn deny_principalarn_eq_self_blocks() {
        // x04: Deny {"AWS":"*"} if StringEquals aws:PrincipalArn = caller.
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"aws:PrincipalArn": CALLER}}}),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn deny_principalarn_notequals_self_does_not_block() {
        // x07: Deny {"AWS":"*"} if StringNotEquals aws:PrincipalArn = caller.
        // The caller IS alice, so the negated condition is false and the Deny
        // does not apply; the Allow grants.
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole",
                "Condition": {"StringNotEquals": {"aws:PrincipalArn": CALLER}}}),
        ];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn allow_gate_principalarn_grants() {
        // x01: Allow {"AWS":"*"} if StringEquals aws:PrincipalArn = caller.
        let stmts = vec![json!({"Effect": "Allow", "Principal": {"AWS": "*"},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:PrincipalArn": CALLER}}})];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn allow_gate_principalaccount_grants() {
        // x08: Allow {"AWS":"*"} if StringEquals aws:PrincipalAccount = acct.
        let stmts = vec![json!({"Effect": "Allow", "Principal": {"AWS": "*"},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:PrincipalAccount": ACCT}}})];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn deny_principalaccount_notequals_in_account_does_not_block() {
        // x09: Deny {"AWS":"*"} if StringNotEquals aws:PrincipalAccount = acct.
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole",
                "Condition": {"StringNotEquals": {"aws:PrincipalAccount": ACCT}}}),
        ];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    // ---- resolved keys: RequestTag / TagKeys (session tags) ---------------

    #[test]
    fn deny_requesttag_match_blocks_tagged_call() {
        // x17: session tag env=prod, Deny if StringEquals aws:RequestTag/env.
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
                "Action": ["sts:AssumeRole", "sts:TagSession"]}),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER},
                "Action": ["sts:AssumeRole", "sts:TagSession"],
                "Condition": {"StringEquals": {"aws:RequestTag/env": "prod"}}}),
        ];
        assert!(!trust_decision(
            &stmts,
            CALLER,
            &ctx_with_request_tags(&[("env", "prod")]),
            true
        ));
    }

    #[test]
    fn allow_gate_requesttag_grants_tagged_call() {
        // x18: session tag env=dev, Allow if StringEquals aws:RequestTag/env.
        let stmts = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": ["sts:AssumeRole", "sts:TagSession"],
            "Condition": {"StringEquals": {"aws:RequestTag/env": "dev"}}})];
        assert!(trust_decision(
            &stmts,
            CALLER,
            &ctx_with_request_tags(&[("env", "dev")]),
            true
        ));
    }

    #[test]
    fn deny_tagkeys_foranyvalue_blocks_tagged_call() {
        // x19: session tag env=prod, Deny if ForAnyValue:StringEquals
        // aws:TagKeys = env.
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
                "Action": ["sts:AssumeRole", "sts:TagSession"]}),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER},
                "Action": ["sts:AssumeRole", "sts:TagSession"],
                "Condition": {"ForAnyValue:StringEquals": {"aws:TagKeys": "env"}}}),
        ];
        assert!(!trust_decision(
            &stmts,
            CALLER,
            &ctx_with_request_tags(&[("env", "prod")]),
            true
        ));
    }

    // ---- organization keys: absent for a standalone account (R1) ----------

    #[test]
    fn deny_on_absent_org_key_does_not_block() {
        // Deny {"AWS":"*"} if StringEquals aws:PrincipalOrgID = org. An ExtendDB
        // account is a standalone AWS account, so the organization key is absent;
        // StringEquals on an absent single-valued key does not match, so the Deny
        // does not apply and the Allow grants. Matches IAM for a standalone
        // account.
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"aws:PrincipalOrgID": "o-abc123"}}}),
        ];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn deny_negated_on_absent_org_key_blocks() {
        // Deny {"AWS":"*"} if StringNotEquals aws:PrincipalOrgID = org. The key
        // is absent for a standalone account, and a negated operator on an absent
        // single-valued key matches, so the Deny applies and blocks. Matches IAM
        // for a standalone account (the key is genuinely absent, not fail-closed).
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole",
                "Condition": {"StringNotEquals": {"aws:PrincipalOrgID": "o-abc123"}}}),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn allow_gate_on_absent_org_key_does_not_grant() {
        // Allow {"AWS":"*"} if StringEquals aws:PrincipalOrgID = org. The key is
        // absent, so the positive operator does not match and the Allow does not
        // grant: implicit deny. Matches IAM for a standalone account.
        let stmts = vec![json!({"Effect": "Allow", "Principal": {"AWS": "*"},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:PrincipalOrgID": "o-abc123"}}})];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    // ---- Action and session tags -----------------------------------------

    #[test]
    fn deny_tagsession_only_does_not_block_plain_assume() {
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:TagSession"}),
        ];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn deny_setsourceidentity_only_does_not_block_plain_assume() {
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:SetSourceIdentity"}),
        ];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn allow_tagsession_only_does_not_grant_plain_assume() {
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": {"AWS": CALLER}, "Action": "sts:TagSession"}),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn session_tags_require_tagsession_allow() {
        // Allow covers only sts:AssumeRole; passing session tags needs
        // sts:TagSession, which no Allow grants, so the call is denied.
        assert!(!trust_decision(&[allow_alice()], CALLER, &ctx(), true));
        // Allow covers both actions: granted.
        let both = vec![json!({
            "Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": ["sts:AssumeRole", "sts:TagSession"]
        })];
        assert!(trust_decision(&both, CALLER, &ctx(), true));
    }

    #[test]
    fn deny_tagsession_blocks_tagged_call_but_allows_plain() {
        let stmts = vec![
            json!({"Effect": "Allow", "Principal": {"AWS": CALLER}, "Action": ["sts:AssumeRole", "sts:TagSession"]}),
            json!({"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:TagSession"}),
        ];
        assert!(!trust_decision(&stmts, CALLER, &ctx(), true));
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn empty_statement_list_denies() {
        assert!(!trust_decision(&[], CALLER, &ctx(), false));
    }

    // ---- statements_vec: single-object Statement --------------------------

    #[test]
    fn single_object_statement_is_one_statement() {
        let doc = json!({"Version": "2012-10-17", "Statement": {
            "Effect": "Allow", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"
        }});
        let stmts = statements_vec(&doc).expect("single object Statement");
        assert_eq!(stmts.len(), 1);
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn missing_statement_is_none() {
        assert!(statements_vec(&json!({"Version": "2012-10-17"})).is_none());
    }

    // ---- validate_trust_policy: accept set --------------------------------

    fn ok(doc: Value) {
        assert!(validate_trust_policy(&doc).is_ok(), "should accept: {doc}");
    }

    fn rejected(doc: Value) {
        assert!(validate_trust_policy(&doc).is_err(), "should reject: {doc}");
    }

    #[test]
    fn validate_accepts_well_formed() {
        ok(json!({"Version": "2012-10-17", "Statement": [allow_alice()]}));
    }

    #[test]
    fn validate_accepts_missing_version() {
        ok(json!({"Statement": [allow_alice()]}));
    }

    #[test]
    fn validate_accepts_single_object_statement() {
        ok(json!({"Version": "2012-10-17", "Statement": allow_alice()}));
    }

    #[test]
    fn validate_rejects_non_alphanumeric_sid() {
        for sid in ["Sid-X", "Sid_X", "Sid X", "Sid.X", "Sid\u{00e9}"] {
            let doc = json!({"Version": "2012-10-17",
                "Statement": [{"Sid": sid, "Effect": "Allow",
                    "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}]});
            assert!(
                validate_trust_policy(&doc).is_err(),
                "should reject Sid {sid:?}"
            );
        }
    }

    #[test]
    fn validate_accepts_alphanumeric_or_empty_sid() {
        for sid in ["Abc123", "", "aaaaaaaaaa"] {
            let doc = json!({"Version": "2012-10-17",
                "Statement": [{"Sid": sid, "Effect": "Allow",
                    "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}]});
            assert!(
                validate_trust_policy(&doc).is_ok(),
                "should accept Sid {sid:?}"
            );
        }
    }

    #[test]
    fn validate_accepts_lowercase_effect() {
        ok(
            json!({"Statement": [{"Effect": "deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}]}),
        );
    }

    #[test]
    fn validate_accepts_numeric_condition_value() {
        ok(json!({"Statement": [{
            "Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:PrincipalTag/team": 5}}
        }]}));
    }

    #[test]
    fn validate_accepts_unknown_condition_key() {
        ok(json!({"Statement": [{
            "Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:NoSuchKey": "x"}}
        }]}));
    }

    #[test]
    fn validate_accepts_known_iam_operator_extenddb_does_not_evaluate() {
        // BinaryEquals is a valid IAM operator the evaluator does not implement;
        // IAM accepts the document, and a Deny using it fails closed at
        // evaluation.
        ok(json!({"Statement": [{
            "Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole",
            "Condition": {"BinaryEquals": {"aws:PrincipalTag/x": "AAAA"}}
        }]}));
    }

    #[test]
    fn validate_accepts_aws_wildcard_principal_and_account() {
        ok(
            json!({"Statement": [{"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"}]}),
        );
        ok(
            json!({"Statement": [{"Effect": "Deny", "Principal": {"AWS": ACCT}, "Action": "sts:AssumeRole"}]}),
        );
    }

    #[test]
    fn validate_accepts_tagsession_and_setsourceidentity_actions() {
        ok(
            json!({"Statement": [{"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"]}]}),
        );
    }

    // ---- validate_trust_policy: reject set --------------------------------

    #[test]
    fn validate_rejects_missing_statement() {
        rejected(json!({"Version": "2012-10-17"}));
    }

    #[test]
    fn validate_rejects_missing_effect() {
        rejected(
            json!({"Statement": [{"Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}]}),
        );
    }

    #[test]
    fn validate_rejects_invalid_effect() {
        rejected(
            json!({"Statement": [{"Effect": "Permit", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}]}),
        );
    }

    #[test]
    fn validate_rejects_not_principal() {
        rejected(
            json!({"Statement": [{"Effect": "Deny", "NotPrincipal": {"AWS": BOB}, "Action": "sts:AssumeRole"}]}),
        );
    }

    #[test]
    fn validate_rejects_missing_principal() {
        rejected(json!({"Statement": [{"Effect": "Deny", "Action": "sts:AssumeRole"}]}));
    }

    #[test]
    fn validate_rejects_bare_star_principal() {
        rejected(
            json!({"Statement": [{"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}]}),
        );
    }

    #[test]
    fn validate_rejects_wildcard_in_principal_arn() {
        rejected(json!({"Statement": [{"Effect": "Deny",
            "Principal": {"AWS": format!("arn:aws:iam::{ACCT}:user/dev-*")}, "Action": "sts:AssumeRole"}]}));
    }

    #[test]
    fn validate_rejects_missing_action() {
        rejected(json!({"Statement": [{"Effect": "Deny", "Principal": {"AWS": CALLER}}]}));
    }

    #[test]
    fn validate_rejects_sts_star_action() {
        rejected(
            json!({"Statement": [{"Effect": "Allow", "Principal": {"AWS": CALLER}, "Action": "sts:*"}]}),
        );
    }

    #[test]
    fn validate_rejects_non_sts_action() {
        rejected(
            json!({"Statement": [{"Effect": "Allow", "Principal": {"AWS": CALLER}, "Action": "dynamodb:GetItem"}]}),
        );
    }

    #[test]
    fn validate_rejects_resource_present() {
        rejected(
            json!({"Statement": [{"Effect": "Deny", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole", "Resource": "*"}]}),
        );
    }

    #[test]
    fn validate_rejects_char_above_latin1() {
        // The service will not store a trust policy with a code point above
        // U+00FF (CreateRole InternalFailure). A key name with U+0100 is
        // rejected; a Latin-1 key (sharp s) is accepted (stored, reads absent
        // at evaluation).
        rejected(
            json!({"Statement": [{"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:Reque\u{0100}edRegion": "us-east-1"}}}]}),
        );
        // A high code point in a value is rejected too.
        rejected(
            json!({"Statement": [{"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:RequestedRegion": "us-east-1\u{212a}"}}}]}),
        );
        // Latin-1 is accepted.
        ok(
            json!({"Statement": [{"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:Reque\u{00df}edRegion": "us-east-1"}}}]}),
        );
    }

    #[test]
    fn validate_rejects_unknown_condition_operator() {
        rejected(
            json!({"Statement": [{"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole",
            "Condition": {"StringEqualz": {"aws:PrincipalTag/team": "contractors"}}}]}),
        );
    }

    #[test]
    fn validate_rejects_miscased_condition_operators() {
        // The live service treats operator names, set qualifiers and the
        // IfExists suffix as case-sensitive and rejects a miscased one at the
        // trust write (measured); ExtendDB matches.
        for op in [
            "stringequals",
            "bool",
            "forallvalues:StringEquals",
            "ForAllValues:stringequals",
            "StringEqualsifexists",
        ] {
            rejected(
                json!({"Statement": [{"Effect": "Deny", "Principal": {"AWS": CALLER},
                "Action": "sts:AssumeRole",
                "Condition": {op: {"aws:PrincipalType": "User"}}}]}),
            );
        }
    }

    #[test]
    fn validate_rejects_condition_not_object() {
        rejected(
            json!({"Statement": [{"Effect": "Deny", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": "contractors"}}]}),
        );
    }

    // ---- resolved request-wide keys on the trust path --------------------

    #[test]
    fn trust_tls_guard_does_not_block_over_tls() {
        // T8: Allow alice, Deny * if Bool aws:SecureTransport=false. The call is
        // over TLS, so SecureTransport is true, the deny does not fire, and the
        // allow grants. Matches live STS (ALLOW).
        let stmts = vec![
            allow_alice(),
            json!({"Effect": "Deny", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole",
                "Condition": {"Bool": {"aws:SecureTransport": "false"}}}),
        ];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn trust_secure_transport_true_grants() {
        let stmts = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"Bool": {"aws:SecureTransport": "true"}}})];
        assert!(trust_decision(&stmts, CALLER, &ctx(), false));
    }

    #[test]
    fn trust_principal_type_user_grants_and_assumedrole_does_not() {
        // The caller is an IAM user, so PrincipalType=User grants and
        // PrincipalType=AssumedRole does not. Matches live STS.
        let user_gate = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:PrincipalType": "User"}}})];
        assert!(trust_decision(&user_gate, CALLER, &ctx(), false));
        let role_gate = vec![json!({"Effect": "Allow", "Principal": {"AWS": CALLER},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:PrincipalType": "AssumedRole"}}})];
        assert!(!trust_decision(&role_gate, CALLER, &ctx(), false));
    }

    #[test]
    fn caller_principal_type_and_username_derivation() {
        assert_eq!(caller_principal_type(CALLER), Some("User"));
        assert_eq!(caller_username(CALLER), Some("alice"));
        let role = "arn:aws:sts::123456789012:assumed-role/app-role/session-1";
        assert_eq!(caller_principal_type(role), Some("AssumedRole"));
        assert_eq!(caller_username(role), None);
        let root = "arn:aws:iam::123456789012:root";
        assert_eq!(caller_principal_type(root), None);
        assert_eq!(caller_username(root), None);
    }
}
