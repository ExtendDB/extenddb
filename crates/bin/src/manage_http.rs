// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! HTTP transport and command dispatch for `extenddb manage`.
//!
//! Handles TLS and plain-text connections. Extracted from `cmd_manage.rs`
//! to keep all files under the 500-line limit.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use base64::Engine;

use crate::config;
use crate::manage_types::{CacheAction, CacheInvalidateScope, ManageCommand};

/// Resolve the management API endpoint from CLI args or config file.
///
/// Returns a `(host_port, use_tls, cert_path)` tuple. TLS is inferred from:
/// 1. `https://` scheme in `--endpoint`, or
/// 2. `server.tls.enabled` in the config file.
///
/// When TLS is enabled, `cert_path` is resolved from the config file so the
/// caller doesn't need to reload it.
pub fn resolve_endpoint(
    endpoint_arg: Option<&str>,
    config_path: &str,
) -> anyhow::Result<(String, bool, Option<String>)> {
    if let Some(ep) = endpoint_arg {
        let ep = ep.trim_end_matches('/');
        if let Some(rest) = ep.strip_prefix("https://") {
            let app_config = config::load(config_path)?;
            let cert = config::expand_tilde(&app_config.server.tls.cert_path);
            return Ok((rest.to_owned(), true, Some(cert)));
        }
        if let Some(rest) = ep.strip_prefix("http://") {
            return Ok((rest.to_owned(), false, None));
        }
        let app_config = config::load(config_path)?;
        let cert = if app_config.server.tls.enabled {
            Some(config::expand_tilde(&app_config.server.tls.cert_path))
        } else {
            None
        };
        return Ok((ep.to_owned(), app_config.server.tls.enabled, cert));
    }
    if !std::path::Path::new(config_path).exists() {
        anyhow::bail!(
            "Config file '{config_path}' not found. Use --endpoint <url> to specify the \
             server address, or --config <path> to specify a config file.",
        );
    }
    let app_config = config::load(config_path)?;
    let addr = if app_config.server.bind_addr == "0.0.0.0" {
        "127.0.0.1"
    } else {
        &app_config.server.bind_addr
    };
    let cert = if app_config.server.tls.enabled {
        Some(config::expand_tilde(&app_config.server.tls.cert_path))
    } else {
        None
    };
    Ok((
        format!("{addr}:{}", app_config.server.port),
        app_config.server.tls.enabled,
        cert,
    ))
}

/// Resolve the password from: CLI arg or `EXTENDDB_PASSWORD` env var.
///
/// Never reads from stdin. Errors if no password source is available.
pub fn resolve_password(cli_password: Option<String>) -> anyhow::Result<String> {
    if let Some(pw) = cli_password {
        return Ok(pw);
    }
    if let Ok(pw) = std::env::var("EXTENDDB_PASSWORD") {
        return Ok(pw);
    }
    anyhow::bail!("No password provided. Use --password VALUE or set EXTENDDB_PASSWORD.")
}

/// Build a Basic auth header value.
pub fn basic_auth(user: &str, password: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    format!("Basic {encoded}")
}

/// Minimal HTTP/1.1 client over TCP (plain or TLS).
fn http_request(
    method: &str,
    host_port: &str,
    path: &str,
    auth: &str,
    body: Option<&serde_json::Value>,
    use_tls: bool,
    cert_path: Option<&str>,
) -> anyhow::Result<(u16, String)> {
    let body_bytes = body
        .map(|b| serde_json::to_vec(b).unwrap_or_default())
        .unwrap_or_default();

    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {host_port}\r\n\
         Authorization: {auth}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body_bytes.len(),
    );

    if use_tls {
        let cp = cert_path.ok_or_else(|| {
            anyhow::anyhow!("TLS enabled but no cert path resolved — check config")
        })?;
        http_request_tls(host_port, &request, &body_bytes, cp)
    } else {
        http_request_plain(host_port, &request, &body_bytes)
    }
}

fn http_request_plain(
    host_port: &str,
    request: &str,
    body_bytes: &[u8],
) -> anyhow::Result<(u16, String)> {
    let mut stream = TcpStream::connect(host_port)
        .map_err(|e| anyhow::anyhow!("Cannot connect to {host_port}: {e}"))?;
    stream.write_all(request.as_bytes())?;
    if !body_bytes.is_empty() {
        stream.write_all(body_bytes)?;
    }
    read_http_response(&mut stream)
}

fn http_request_tls(
    host_port: &str,
    request: &str,
    body_bytes: &[u8],
    cert_path: &str,
) -> anyhow::Result<(u16, String)> {
    // rustls 0.23 requires an explicit CryptoProvider. Install once (idempotent).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cert_pem = std::fs::read(cert_path)
        .map_err(|e| anyhow::anyhow!("Cannot read TLS cert {cert_path}: {e}"))?;

    let mut root_store = rustls::RootCertStore::empty();
    let certs = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse PEM certs from {cert_path}: {e}"))?;
    for cert in &certs {
        root_store
            .add(cert.clone())
            .map_err(|e| anyhow::anyhow!("Failed to add cert to root store: {e}"))?;
    }

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let hostname = host_port.split(':').next().unwrap_or("localhost");
    let server_name = rustls::pki_types::ServerName::try_from(hostname.to_owned())
        .map_err(|e| anyhow::anyhow!("Invalid server name '{hostname}': {e}"))?;

    let mut conn = rustls::ClientConnection::new(Arc::new(tls_config), server_name)
        .map_err(|e| anyhow::anyhow!("TLS handshake setup failed: {e}"))?;
    let mut tcp = TcpStream::connect(host_port)
        .map_err(|e| anyhow::anyhow!("Cannot connect to {host_port}: {e}"))?;

    let mut tls_stream = rustls::Stream::new(&mut conn, &mut tcp);
    tls_stream.write_all(request.as_bytes())?;
    if !body_bytes.is_empty() {
        tls_stream.write_all(body_bytes)?;
    }

    let mut response = String::new();
    match tls_stream.read_to_string(&mut response) {
        Ok(_) => {}
        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
        Err(e) => return Err(anyhow::anyhow!("TLS read error: {e}")),
    }
    Ok(parse_http_response(&response))
}

fn read_http_response(stream: &mut impl Read) -> anyhow::Result<(u16, String)> {
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(parse_http_response(&response))
}

fn parse_http_response(response: &str) -> (u16, String) {
    let (head, body) = response.split_once("\r\n\r\n").unwrap_or((response, ""));
    let status_line = head.lines().next().unwrap_or("");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, body.to_owned())
}

// ── Command dispatch ───────────────────────────────────────────────────

fn parse_json(label: &str, s: &str) -> anyhow::Result<serde_json::Value> {
    serde_json::from_str(s).map_err(|e| anyhow::anyhow!("Invalid {label} JSON: {e}"))
}

struct Ctx<'a> {
    host_port: &'a str,
    auth: &'a str,
    use_tls: bool,
    cert_path: Option<&'a str>,
}

impl Ctx<'_> {
    fn req(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> anyhow::Result<(u16, String)> {
        http_request(
            method,
            self.host_port,
            path,
            self.auth,
            body,
            self.use_tls,
            self.cert_path,
        )
    }
}

/// Parse `account_id/user_name` from the `--user` flag value.
/// Returns `None` if the user is an admin (no `/` separator).
fn parse_iam_identity(user: &str) -> Option<(&str, &str)> {
    user.split_once('/')
}

/// Dispatch a `ManageCommand` to the appropriate HTTP endpoint.
#[allow(clippy::too_many_lines)]
pub fn dispatch(
    host_port: &str,
    auth: &str,
    use_tls: bool,
    cert_path: Option<&str>,
    user: &str,
    cmd: ManageCommand,
) -> anyhow::Result<(u16, String)> {
    use ManageCommand as M;
    let c = Ctx {
        host_port,
        auth,
        use_tls,
        cert_path,
    };
    match cmd {
        M::CreateAdmin { admin_name, admin_password } => c.req(
            "POST", "/management/admins",
            Some(&serde_json::json!({ "admin_name": admin_name, "password": admin_password })),
        ),
        M::ListAdmins => c.req("GET", "/management/admins", None),
        M::DeleteAdmin { admin_name } =>
            c.req("DELETE", &format!("/management/admins/{admin_name}"), None),
        M::ChangeAdminPassword { admin_name, new_password } => c.req(
            "PUT", &format!("/management/admins/{admin_name}/password"),
            Some(&serde_json::json!({ "password": new_password })),
        ),
        M::CreateAccount { account_id, account_name } => {
            let mut body = serde_json::json!({ "account_name": account_name });
            if let Some(id) = &account_id {
                body["account_id"] = serde_json::Value::String(id.clone());
            }
            let r = c.req("POST", "/management/accounts", Some(&body))?;
            if (200..300).contains(&r.0) {
                // Read server-generated account_id from response body.
                if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&r.1) {
                    if let Some(id) = resp.get("account_id").and_then(|v| v.as_str()) {
                        eprintln!("Account ID: {id}");
                    }
                }
            }
            Ok(r)
        }
        M::ListAccounts => c.req("GET", "/management/accounts", None),
        M::DeleteAccount { account_id } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}"), None),
        M::CreateUser { account_id, user_name, user_password } => {
            if user_password.is_none() {
                eprintln!("Warning: No console password set — this user cannot log into the management console.");
            }
            let mut b = serde_json::json!({ "user_name": user_name });
            if let Some(pw) = user_password { b["password"] = serde_json::Value::String(pw); }
            c.req("POST", &format!("/management/accounts/{account_id}/users"), Some(&b))
        }
        M::ListUsers { account_id } =>
            c.req("GET", &format!("/management/accounts/{account_id}/users"), None),
        M::DeleteUser { account_id, user_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/users/{user_name}"), None),
        M::CreateAccessKey { account_id, user_name } => {
            let (aid, uname) = match (account_id, user_name) {
                (Some(a), Some(u)) => (a, u),
                (None, None) => {
                    let (a, u) = parse_iam_identity(user).ok_or_else(|| {
                        anyhow::anyhow!(
                            "--account-id and --user-name are required when authenticating as admin"
                        )
                    })?;
                    (a.to_owned(), u.to_owned())
                }
                _ => anyhow::bail!(
                    "Provide both --account-id and --user-name, or omit both to infer from --user"
                ),
            };
            c.req("POST", &format!("/management/accounts/{aid}/users/{uname}/access-keys"), None)
        }
        M::ListAccessKeys { account_id, user_name } =>
            c.req("GET", &format!("/management/accounts/{account_id}/users/{user_name}/access-keys"), None),
        M::DeleteAccessKey { account_id, user_name, access_key_id } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/users/{user_name}/access-keys/{access_key_id}"), None),
        M::ImportAccessKey { account_id, user_name, access_key_id, secret_access_key, yes } => {
            if !yes {
                anyhow::bail!(
                    "--yes is required for import-access-key. This command stores a real AWS \
                     secret access key in the local PostgreSQL database."
                );
            }
            c.req("POST", &format!("/management/accounts/{account_id}/users/{user_name}/access-keys/import"),
                Some(&serde_json::json!({ "access_key_id": access_key_id, "secret_access_key": secret_access_key })))
        }
        M::ChangeUserPassword { account_id, user_name, new_password } => c.req(
            "PUT", &format!("/management/accounts/{account_id}/users/{user_name}/password"),
            Some(&serde_json::json!({ "password": new_password })),
        ),
        M::CreateGroup { account_id, group_name } => c.req(
            "POST", &format!("/management/accounts/{account_id}/groups"),
            Some(&serde_json::json!({ "group_name": group_name })),
        ),
        M::ListGroups { account_id } =>
            c.req("GET", &format!("/management/accounts/{account_id}/groups"), None),
        M::DeleteGroup { account_id, group_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/groups/{group_name}"), None),
        M::AddGroupMember { account_id, group_name, user_name } => c.req(
            "POST", &format!("/management/accounts/{account_id}/groups/{group_name}/members"),
            Some(&serde_json::json!({ "user_name": user_name })),
        ),
        M::RemoveGroupMember { account_id, group_name, user_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/groups/{group_name}/members/{user_name}"), None),
        M::PutUserPolicy { account_id, user_name, policy_name, policy_document } =>
            c.req("PUT", &format!("/management/accounts/{account_id}/users/{user_name}/policy/{policy_name}"),
                Some(&parse_json("policy document", &policy_document)?)),
        M::ListUserPolicies { account_id, user_name } =>
            c.req("GET", &format!("/management/accounts/{account_id}/users/{user_name}/policies"), None),
        M::DeleteUserPolicy { account_id, user_name, policy_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/users/{user_name}/policy/{policy_name}"), None),
        M::PutGroupPolicy { account_id, group_name, policy_name, policy_document } =>
            c.req("PUT", &format!("/management/accounts/{account_id}/groups/{group_name}/policy/{policy_name}"),
                Some(&parse_json("policy document", &policy_document)?)),
        M::ListGroupPolicies { account_id, group_name } =>
            c.req("GET", &format!("/management/accounts/{account_id}/groups/{group_name}/policies"), None),
        M::DeleteGroupPolicy { account_id, group_name, policy_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/groups/{group_name}/policy/{policy_name}"), None),
        M::TagUser { account_id, user_name, tags } =>
            c.req("PUT", &format!("/management/accounts/{account_id}/users/{user_name}/tags"),
                Some(&serde_json::json!({ "tags": parse_json("tags", &tags)? }))),
        M::UntagUser { account_id, user_name, tag_keys } => {
            let keys: Vec<&str> = tag_keys.split(',').map(str::trim).collect();
            c.req("DELETE", &format!("/management/accounts/{account_id}/users/{user_name}/tags"),
                Some(&serde_json::json!({ "tag_keys": keys })))
        }
        M::ListUserTags { account_id, user_name } =>
            c.req("GET", &format!("/management/accounts/{account_id}/users/{user_name}/tags"), None),
        M::CreateRole { account_id, role_name, trust_policy } =>
            c.req("POST", &format!("/management/accounts/{account_id}/roles"),
                Some(&serde_json::json!({ "role_name": role_name, "trust_policy": parse_json("trust policy", &trust_policy)? }))),
        M::ListRoles { account_id } =>
            c.req("GET", &format!("/management/accounts/{account_id}/roles"), None),
        M::DeleteRole { account_id, role_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/roles/{role_name}"), None),
        M::TagRole { account_id, role_name, tags } =>
            c.req("PUT", &format!("/management/accounts/{account_id}/roles/{role_name}/tags"),
                Some(&serde_json::json!({ "tags": parse_json("tags", &tags)? }))),
        M::UntagRole { account_id, role_name, tag_keys } => {
            let keys: Vec<&str> = tag_keys.split(',').map(str::trim).collect();
            c.req("DELETE", &format!("/management/accounts/{account_id}/roles/{role_name}/tags"),
                Some(&serde_json::json!({ "tag_keys": keys })))
        }
        M::ListRoleTags { account_id, role_name } =>
            c.req("GET", &format!("/management/accounts/{account_id}/roles/{role_name}/tags"), None),
        M::PutRolePolicy { account_id, role_name, policy_name, policy_document } =>
            c.req("PUT", &format!("/management/accounts/{account_id}/roles/{role_name}/policy/{policy_name}"),
                Some(&parse_json("policy document", &policy_document)?)),
        M::ListRolePolicies { account_id, role_name } =>
            c.req("GET", &format!("/management/accounts/{account_id}/roles/{role_name}/policies"), None),
        M::DeleteRolePolicy { account_id, role_name, policy_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/roles/{role_name}/policy/{policy_name}"), None),
        M::AssumeRole { account_id, role_name, caller_arn, session_name, session_tags, session_policy, duration_seconds } => {
            let mut b = serde_json::json!({ "caller_arn": caller_arn, "session_name": session_name, "duration_seconds": duration_seconds });
            if let Some(t) = session_tags { b["session_tags"] = parse_json("session tags", &t)?; }
            if let Some(p) = session_policy { b["session_policy"] = parse_json("session policy", &p)?; }
            c.req("POST", &format!("/management/accounts/{account_id}/roles/{role_name}/assume"), Some(&b))
        }
        M::SetUserBoundary { account_id, user_name, policy_document } =>
            c.req("PUT", &format!("/management/accounts/{account_id}/users/{user_name}/permissions-boundary"),
                Some(&parse_json("policy document", &policy_document)?)),
        M::GetUserBoundary { account_id, user_name } =>
            c.req("GET", &format!("/management/accounts/{account_id}/users/{user_name}/permissions-boundary"), None),
        M::DeleteUserBoundary { account_id, user_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/users/{user_name}/permissions-boundary"), None),
        M::SetRoleBoundary { account_id, role_name, policy_document } =>
            c.req("PUT", &format!("/management/accounts/{account_id}/roles/{role_name}/permissions-boundary"),
                Some(&parse_json("policy document", &policy_document)?)),
        M::GetRoleBoundary { account_id, role_name } =>
            c.req("GET", &format!("/management/accounts/{account_id}/roles/{role_name}/permissions-boundary"), None),
        M::DeleteRoleBoundary { account_id, role_name } =>
            c.req("DELETE", &format!("/management/accounts/{account_id}/roles/{role_name}/permissions-boundary"), None),
        M::Cache { action } => dispatch_cache(&c, action),
    }
}

fn dispatch_cache(c: &Ctx<'_>, action: CacheAction) -> anyhow::Result<(u16, String)> {
    let CacheAction::Invalidate { scope } = action;
    // Build the body matching the management API's `InvalidateRequest`
    // shape. The CLI exposes only the subset of selector fields that
    // each scope actually uses, so unused fields stay omitted.
    let body = match scope {
        CacheInvalidateScope::All { yes } => {
            if !yes {
                anyhow::bail!(
                    "cache invalidate all requires --yes. This drops every cached \
                     entry on the local instance and will cause a reload storm."
                );
            }
            serde_json::json!({ "scope": "all", "selectors": { "confirm": true } })
        }
        CacheInvalidateScope::Account { account_id } => serde_json::json!({
            "scope": "account",
            "selectors": { "account_id": account_id },
        }),
        CacheInvalidateScope::Credential { access_key_id } => serde_json::json!({
            "scope": "credential",
            "selectors": { "access_key_id": access_key_id },
        }),
        CacheInvalidateScope::User {
            account_id,
            user_name,
        } => serde_json::json!({
            "scope": "user",
            "selectors": { "account_id": account_id, "user_name": user_name },
        }),
        CacheInvalidateScope::Role {
            account_id,
            role_name,
        } => serde_json::json!({
            "scope": "role",
            "selectors": { "account_id": account_id, "role_name": role_name },
        }),
        CacheInvalidateScope::GroupMembers {
            account_id,
            user_names,
        } => {
            // Comma-separated → Vec<String>. Leading/trailing whitespace
            // and empty entries are filtered so users can paste lists
            // freely.
            let names: Vec<String> = user_names
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if names.is_empty() {
                anyhow::bail!("--user-names must contain at least one non-empty name");
            }
            serde_json::json!({
                "scope": "group_members",
                "selectors": { "account_id": account_id, "user_names": names },
            })
        }
        CacheInvalidateScope::TableKeyInfo {
            account_id,
            table_name,
        } => serde_json::json!({
            "scope": "table_key_info",
            "selectors": { "account_id": account_id, "table_name": table_name },
        }),
        CacheInvalidateScope::ResourceTags { arn } => serde_json::json!({
            "scope": "resource_tags",
            "selectors": { "arn": arn },
        }),
    };
    c.req("POST", "/management/cache/invalidate", Some(&body))
}
