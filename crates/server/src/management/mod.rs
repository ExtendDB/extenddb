// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Management API for extenddb — admin, account, IAM user, group, role, and policy CRUD.
//!
//! All endpoints live under `/management/*` and are authenticated via HTTP
//! Basic auth. Admin users authenticate with `admin_name:password`. IAM users
//! authenticate with `account_id/user_name:password` for self-service operations.
//! The management API reads/writes the catalog database directly.

mod account;
pub(crate) use account::generate_account_id;
mod admin;
mod assume_role;
mod auth;
pub mod cache_invalidate;
mod cache_metrics;
pub(crate) mod crypto;
mod iam_group;
mod iam_policy;
mod iam_role;
mod iam_user;
mod iam_user_self;
pub(crate) mod ops;
pub mod ops_settings;
pub(crate) mod password;
mod permissions_boundary;
mod settings;

use std::sync::Arc;

use axum::Router;
use axum::routing::{delete, get, post, put};

pub use auth::CallerIdentity;

/// Shared state for management API handlers.
pub struct ManagementState {
    /// Catalog store implementing operational storage traits.
    pub catalog_store: Arc<dyn extenddb_storage::CatalogStore>,
    /// Auth/authz cache registry. Used by mutation handlers to issue
    /// write-through invalidations so self-induced IAM changes propagate
    /// instantly within the local instance.
    pub auth_cache: extenddb_auth::AuthCacheRegistry,
    /// Concrete authorization cache handle, used by the `/management/auth-cache-metrics`
    /// endpoint to expose per-sub-cache counters. The same instance is held
    /// trait-object-style in `auth_cache.authz` for invalidation calls.
    pub authz_cache: Arc<crate::CachedAuthzStore>,
    /// Concrete TableKeyInfo cache handle, used by the auth-cache-metrics
    /// endpoint. Same instance is held in `auth_cache.table_key_info`.
    pub table_key_info_cache: Arc<crate::CachedTableKeyInfoStore>,
}

/// Build the management API router.
pub fn router() -> Router<Arc<ManagementState>> {
    Router::new()
        // Admin user endpoints
        .route("/admins", post(admin::create_admin))
        .route("/admins", get(admin::list_admins))
        .route("/admins/{name}", delete(admin::delete_admin))
        .route("/admins/{name}/password", put(admin::change_admin_password))
        // Account endpoints
        .route("/accounts", post(account::create_account))
        .route("/accounts", get(account::list_accounts))
        .route("/accounts/{id}", delete(account::delete_account))
        // IAM user endpoints (admin only)
        .route("/accounts/{id}/users", post(iam_user::create_user))
        .route("/accounts/{id}/users", get(iam_user::list_users))
        .route("/accounts/{id}/users/{name}", delete(iam_user::delete_user))
        // IAM user self-service endpoints
        .route(
            "/accounts/{id}/users/{name}/access-keys",
            post(iam_user_self::create_access_key),
        )
        .route(
            "/accounts/{id}/users/{name}/access-keys",
            get(iam_user_self::list_access_keys),
        )
        .route(
            "/accounts/{id}/users/{name}/access-keys/import",
            post(iam_user_self::import_access_key),
        )
        .route(
            "/accounts/{id}/users/{name}/access-keys/{key_id}",
            delete(iam_user_self::delete_access_key),
        )
        .route(
            "/accounts/{id}/users/{name}/password",
            put(iam_user_self::change_user_password),
        )
        // IAM group endpoints (admin only)
        .route("/accounts/{id}/groups", post(iam_group::create_group))
        .route("/accounts/{id}/groups", get(iam_group::list_groups))
        .route(
            "/accounts/{id}/groups/{name}",
            delete(iam_group::delete_group),
        )
        .route(
            "/accounts/{id}/groups/{name}/members",
            post(iam_group::add_member),
        )
        .route(
            "/accounts/{id}/groups/{name}/members/{user}",
            delete(iam_group::remove_member),
        )
        // IAM role endpoints (admin only)
        .route("/accounts/{id}/roles", post(iam_role::create_role))
        .route("/accounts/{id}/roles", get(iam_role::list_roles))
        .route("/accounts/{id}/roles/{name}", delete(iam_role::delete_role))
        // IAM role tag endpoints (admin only)
        .route("/accounts/{id}/roles/{name}/tags", put(iam_role::tag_role))
        .route(
            "/accounts/{id}/roles/{name}/tags",
            delete(iam_role::untag_role),
        )
        .route(
            "/accounts/{id}/roles/{name}/tags",
            get(iam_role::list_role_tags),
        )
        // AssumeRole endpoint (admin only)
        .route(
            "/accounts/{id}/roles/{name}/assume",
            post(assume_role::assume_role),
        )
        // IAM policy endpoints (admin only)
        .route(
            "/accounts/{id}/users/{name}/policy/{policy}",
            put(iam_policy::put_user_policy),
        )
        .route(
            "/accounts/{id}/users/{name}/policies",
            get(iam_policy::list_user_policies),
        )
        .route(
            "/accounts/{id}/users/{name}/policy/{policy}",
            delete(iam_policy::delete_user_policy),
        )
        .route(
            "/accounts/{id}/groups/{name}/policy/{policy}",
            put(iam_policy::put_group_policy),
        )
        .route(
            "/accounts/{id}/groups/{name}/policies",
            get(iam_policy::list_group_policies),
        )
        .route(
            "/accounts/{id}/groups/{name}/policy/{policy}",
            delete(iam_policy::delete_group_policy),
        )
        // Role policy endpoints (admin only)
        .route(
            "/accounts/{id}/roles/{name}/policy/{policy}",
            put(iam_policy::put_role_policy),
        )
        .route(
            "/accounts/{id}/roles/{name}/policies",
            get(iam_policy::list_role_policies),
        )
        .route(
            "/accounts/{id}/roles/{name}/policy/{policy}",
            delete(iam_policy::delete_role_policy),
        )
        // IAM user tag endpoints (admin only)
        .route("/accounts/{id}/users/{name}/tags", put(iam_user::tag_user))
        .route(
            "/accounts/{id}/users/{name}/tags",
            delete(iam_user::untag_user),
        )
        .route(
            "/accounts/{id}/users/{name}/tags",
            get(iam_user::list_user_tags),
        )
        // Permissions boundary endpoints (admin only)
        .route(
            "/accounts/{id}/users/{name}/permissions-boundary",
            put(permissions_boundary::set_user_boundary),
        )
        .route(
            "/accounts/{id}/users/{name}/permissions-boundary",
            get(permissions_boundary::get_user_boundary),
        )
        .route(
            "/accounts/{id}/users/{name}/permissions-boundary",
            delete(permissions_boundary::delete_user_boundary),
        )
        .route(
            "/accounts/{id}/roles/{name}/permissions-boundary",
            put(permissions_boundary::set_role_boundary),
        )
        .route(
            "/accounts/{id}/roles/{name}/permissions-boundary",
            get(permissions_boundary::get_role_boundary),
        )
        .route(
            "/accounts/{id}/roles/{name}/permissions-boundary",
            delete(permissions_boundary::delete_role_boundary),
        )
        // Settings endpoints (admin only)
        .route("/settings", get(settings::list_settings))
        .route("/settings/{key}", get(settings::get_setting))
        .route("/settings/{key}", put(settings::set_setting))
        // Cache observability (admin only). Exposes per-cache hit/miss/
        // refresh counters and current entry counts. Authenticated via the
        // standard admin Basic auth.
        .route(
            "/auth-cache-metrics",
            get(cache_metrics::auth_cache_metrics),
        )
        // Manual cache invalidation (admin only break-glass tool).
        // See docs/design/12-auth-authz-cache.md §6.1.
        .route("/cache/invalidate", post(cache_invalidate::invalidate))
}

/// Validate an IAM name: 1-128 chars, alphanumeric, hyphens, underscores, dots, plus, equals, at.
/// Matches AWS IAM naming rules.
pub(crate) fn is_valid_iam_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'+' | b'=' | b'@')
        })
}

/// Validate admin name: 1-64 chars, alphanumeric, hyphens, or underscores.
fn is_valid_admin_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
