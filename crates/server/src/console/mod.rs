// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Management web console for extenddb.
//!
//! A server-rendered HTML interface for the management API. Provides login,
//! account/user/group/role CRUD, policy editing, and IAM user self-service.
//! Mounted at `/console/*` on the same server as the `DynamoDB` API.
//!
//! Sessions are stored in-memory with random tokens in cookies. No external
//! dependencies — all HTML is generated with Rust string formatting.
//!
//! Write operations (create, delete, update) are routed through shared
//! functions in `management::ops`, ensuring validation and business logic
//! are defined once. Read operations query the catalog DB directly for
//! rendering.

pub mod docs_embed;
pub mod html;
mod pages;
pub mod session;
#[cfg(test)]
mod tab_gate_tests;
#[cfg(test)]
mod test_catalog;

use std::sync::Arc;

use axum::Router;
use axum::http::Method;
use axum::routing::{get, post};

use session::SessionStore;

/// Shared state for console handlers.
pub struct ConsoleState {
    pub sessions: SessionStore,
    /// Links the navigation bar shows after the built-in ones: `(label,
    /// href)`, one per extension tab (see
    /// [`ConsoleTab`](crate::extensions::ConsoleTab)). Shared by pointer with
    /// every [`ConsoleIdentity`] the session check builds.
    pub tabs: Arc<[(String, String)]>,
    /// Version string displayed in the console footer (e.g. "0.1.0 · catalog 2.1.0 · abc1234").
    pub version_info: Arc<str>,
    /// The URL the server is listening on (e.g. "<https://127.0.0.1:18443>").
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
    /// In-memory metrics collector, used to enrich the session-authed
    /// `/console/metrics-data` response with latency segments (same data the
    /// top-level `/metrics` handler adds).
    pub metrics: Arc<extenddb_core::metrics::MetricsCollector>,
    /// Whether the session cookie carries `Secure`: true when the server
    /// serves TLS, false in dev mode, which serves plain HTTP.
    pub secure_cookies: bool,
}

/// The console user behind a request to an extension tab, and what the page
/// needs to render the console's layout the way the built-in pages do.
/// Inserted as a request extension by [`require_console_session`]. `label` is
/// what the navigation bar shows for the user; `tabs` and `version_info` are
/// the arguments the built-in pages pass to [`html::nav_bar`] and
/// [`html::layout_with_version_csrf`]. Every console user reaches a tab, an
/// admin or an IAM user with a console password; `is_admin` tells them apart.
/// A request that may change state reaches the tab only with the session's
/// CSRF token in its `_csrf` form field (see [`require_console_session`]).
/// Read by field; the struct may gain fields in a minor release, so build one
/// for a test with [`ConsoleIdentity::new`]. It has no `Debug`, so that no log
/// line prints the token.
#[derive(Clone)]
#[non_exhaustive]
pub struct ConsoleIdentity {
    /// What the navigation bar shows for the user (`Admin: name` or
    /// `account/user`).
    pub label: String,
    /// The session's CSRF token, which the layout writes into the page's
    /// `csrf-token` meta tag. A script in the layout adds it as the `_csrf`
    /// field to each POST form on the page when the page loads.
    pub csrf_token: String,
    /// Whether the session is an admin login (full management access) rather
    /// than an IAM user's self-service session. The session check admits
    /// both. A page meant for admins refuses an IAM-user session as the
    /// built-in admin pages do: `/console/settings`, `/console/cache` and
    /// `/console/metrics` redirect it to `/console`, and
    /// `/console/metrics-data` answers 403.
    pub is_admin: bool,
    /// Every extension tab as `(label, href)`, for the navigation bar.
    pub tabs: Arc<[(String, String)]>,
    /// The version line the footer shows.
    pub version_info: Arc<str>,
}

impl ConsoleIdentity {
    /// An identity for a test of a tab page. The server builds the real one.
    #[must_use]
    pub fn new(
        label: impl Into<String>,
        csrf_token: impl Into<String>,
        is_admin: bool,
        tabs: Arc<[(String, String)]>,
        version_info: Arc<str>,
    ) -> Self {
        Self {
            label: label.into(),
            csrf_token: csrf_token.into(),
            is_admin,
            tabs,
            version_info,
        }
    }
}

/// Middleware for extension tabs: the session check of the built-in pages. A
/// request without a live console session is redirected to the login page,
/// and one whose session the catalog cannot check gets 503. A request that
/// may change state (any method but GET, HEAD, OPTIONS and TRACE) gets 403
/// unless its body is a form whose `_csrf` field is the session's CSRF token,
/// the check a built-in page makes on its own forms. A request that passes
/// carries a [`ConsoleIdentity`] extension for the page to render the
/// navigation bar.
pub async fn require_console_session(
    axum::extract::State(state): axum::extract::State<Arc<ConsoleState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let session = match pages::require_session(req.headers(), &state).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    // The four methods are named here because `Method::is_safe` follows the
    // `http` crate's list, and http 1.5.0 adds QUERY to it.
    let read_only = matches!(
        *req.method(),
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    );
    let mut req = if read_only {
        req
    } else {
        match pages::require_form_csrf(req, &session).await {
            Ok(req) => req,
            Err(response) => return response,
        }
    };
    let nav = pages::identity_label(&session.identity);
    req.extensions_mut().insert(ConsoleIdentity {
        label: nav.label,
        csrf_token: session.csrf_token,
        is_admin: nav.is_admin,
        tabs: session.tabs,
        version_info: Arc::clone(&state.version_info),
    });
    next.run(req).await
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
        // Metrics (admin-only; an IAM-user session is redirected to /console)
        .route("/metrics", get(pages::metrics_page))
        // Metrics data (JSON) for the dashboard's client-side charts, for
        // admin sessions only; an IAM-user session gets 403. Replaces the
        // browser fetch of the top-level `/metrics` (now Basic-auth only, for
        // Prometheus).
        .route("/metrics-data", get(pages::metrics_data))
        // Settings (read-only, admin-only; an IAM-user session is redirected
        // to /console)
        .route("/settings", get(pages::settings_page))
        // Cache (admin-only break-glass invalidation; an IAM-user session is
        // redirected to /console). See docs/design/12-auth-authz-cache.md §6.1.
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
