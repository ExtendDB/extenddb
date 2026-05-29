// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Management web console for extenddb.
//!
//! A server-rendered HTML interface for the management API. Provides login,
//! account/user/group/role CRUD, policy editing, and IAM user self-service.
//! Mounted at `/console/*` on the same server as the DynamoDB API.
//!
//! Sessions are stored in-memory with random tokens in cookies. No external
//! dependencies — all HTML is generated with Rust string formatting.
//!
//! Write operations (create, delete, update) are routed through shared
//! functions in `management::ops`, ensuring validation and business logic
//! are defined once. Read operations query the catalog DB directly for
//! rendering.

pub mod docs_embed;
mod html;
mod pages;
pub mod session;

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use session::SessionStore;

/// Shared state for console handlers.
pub struct ConsoleState {
    pub sessions: SessionStore,
    /// Version string displayed in the console footer (e.g. "0.1.0 · catalog 2.1.0 · abc1234").
    pub version_info: Arc<str>,
    /// The URL the server is listening on (e.g. "https://127.0.0.1:8000").
    /// Displayed in the footer so users know which address the self-signed
    /// certificate is bound to.
    pub listen_url: String,
    /// Static configuration entries from the `.toml` file, as `(key, value)` pairs.
    /// Populated at server startup. Values for sensitive keys are pre-redacted.
    pub config_entries: Vec<(String, String)>,
    /// Catalog store implementing operational storage traits.
    pub catalog_store: Arc<dyn extenddb_storage::CatalogStore>,
    /// Runtime documentation store. `None` if `docs_dir` is not configured or
    /// the directory is missing/invalid.
    pub docs_store: Option<docs_embed::DocsStore>,
    /// Auth/authz cache registry. Used by mutation handlers to issue
    /// write-through invalidations after IAM changes — the same hooks the
    /// management API calls. Also used by the `/console/cache` admin
    /// break-glass page to drive manual invalidation. Without this,
    /// console-driven mutations leave stale entries in the cache for up
    /// to `auth.cache.ttl_seconds`.
    pub auth_cache: extenddb_auth::AuthCacheRegistry,
}

/// Build the console router.
pub fn router() -> Router<Arc<ConsoleState>> {
    Router::new()
        // Login
        .route("/login", get(pages::login_page))
        .route("/login", post(pages::login_submit))
        .route("/logout", post(pages::logout))
        // Documentation (accessible without login)
        .route("/docs", get(pages::docs_page))
        .route("/docs/{slug}", get(pages::docs_view))
        .route("/docs/{slug}/pdf", get(pages::docs_pdf))
        // Dashboard (handle both with and without trailing slash)
        .route("/", get(pages::dashboard))
        // Metrics
        .route("/metrics", get(pages::metrics_page))
        // Settings (read-only, admin-only)
        .route("/settings", get(pages::settings_page))
        // Cache (admin-only break-glass invalidation).
        // See docs/design/12-auth-authz-cache.md §6.1.
        .route("/cache", get(pages::cache_page))
        .route("/cache/invalidate", post(pages::invalidate_cache))
        // Accounts
        .route("/accounts", get(pages::list_accounts))
        .route("/accounts/new", get(pages::new_account_form))
        .route("/accounts/new", post(pages::create_account))
        .route("/accounts/{id}", get(pages::account_detail))
        .route("/accounts/{id}/delete", post(pages::delete_account))
        // Users
        .route("/accounts/{id}/users/new", get(pages::new_user_form))
        .route("/accounts/{id}/users/new", post(pages::create_user))
        .route("/accounts/{id}/users/{name}", get(pages::user_detail))
        .route(
            "/accounts/{id}/users/{name}/delete",
            post(pages::delete_user),
        )
        // Access keys (self-service)
        .route(
            "/accounts/{id}/users/{name}/access-keys/new",
            post(pages::create_access_key),
        )
        .route(
            "/accounts/{id}/users/{name}/access-keys/{key_id}/delete",
            post(pages::delete_access_key),
        )
        // Groups
        .route("/accounts/{id}/groups/new", get(pages::new_group_form))
        .route("/accounts/{id}/groups/new", post(pages::create_group))
        .route("/accounts/{id}/groups/{name}", get(pages::group_detail))
        .route(
            "/accounts/{id}/groups/{name}/delete",
            post(pages::delete_group),
        )
        .route(
            "/accounts/{id}/groups/{name}/members/add",
            post(pages::add_group_member),
        )
        .route(
            "/accounts/{id}/groups/{name}/members/{user}/remove",
            post(pages::remove_group_member),
        )
        // Roles
        .route("/accounts/{id}/roles/new", get(pages::new_role_form))
        .route("/accounts/{id}/roles/new", post(pages::create_role))
        .route("/accounts/{id}/roles/{name}", get(pages::role_detail))
        .route(
            "/accounts/{id}/roles/{name}/delete",
            post(pages::delete_role),
        )
        // Policies
        .route(
            "/accounts/{id}/users/{name}/policies/new",
            get(pages::new_user_policy_form),
        )
        .route(
            "/accounts/{id}/users/{name}/policies/new",
            post(pages::put_user_policy),
        )
        .route(
            "/accounts/{id}/users/{name}/policies/{policy}/delete",
            post(pages::delete_user_policy),
        )
        .route(
            "/accounts/{id}/groups/{name}/policies/new",
            get(pages::new_group_policy_form),
        )
        .route(
            "/accounts/{id}/groups/{name}/policies/new",
            post(pages::put_group_policy),
        )
        .route(
            "/accounts/{id}/groups/{name}/policies/{policy}/delete",
            post(pages::delete_group_policy),
        )
        .route(
            "/accounts/{id}/roles/{name}/policies/new",
            get(pages::new_role_policy_form),
        )
        .route(
            "/accounts/{id}/roles/{name}/policies/new",
            post(pages::put_role_policy),
        )
        .route(
            "/accounts/{id}/roles/{name}/policies/{policy}/delete",
            post(pages::delete_role_policy),
        )
}
