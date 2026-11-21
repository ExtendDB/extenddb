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
mod html;
mod pages;
pub mod session;

use std::sync::Arc;

use axum::Router;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use session::SessionStore;

/// The Content-Security-Policy of the console's HTML pages. Scripts run only
/// from the server's own origin, and an inline script, an event handler
/// attribute or a `javascript:` URL does not run. The console's scripts are
/// the two files under `/console/assets/`. Styles can still be inline,
/// because the pages use `style` attributes. Forms post to the server's own
/// origin, and no page loads a plugin or shows in a frame.
const CONTENT_SECURITY_POLICY: &str = "default-src 'none'; script-src 'self'; \
     style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; \
     base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

/// Shared state for console handlers.
pub struct ConsoleState {
    pub sessions: SessionStore,
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
        // Scripts (accessible without login: the files are the repository's)
        .route(
            "/assets/console.js",
            get(|| async { script(include_str!("assets/console.js")) }),
        )
        .route(
            "/assets/metrics.js",
            get(|| async { script(include_str!("assets/metrics.js")) }),
        )
}

/// A script file of the console. With `no-cache`, the browser asks the server
/// for the file each time, so a page that an upgraded server sends runs that
/// server's script.
fn script(body: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

/// [`CONTENT_SECURITY_POLICY`] for an HTML response, which `start_server`
/// sets on the responses of the console router. A script, JSON, PDF or
/// redirect response gets none.
pub(crate) fn html_policy(response: &Response) -> Option<HeaderValue> {
    let kind = response.headers().get(header::CONTENT_TYPE)?.as_bytes();
    kind.get(..9)
        .is_some_and(|k| k.eq_ignore_ascii_case(b"text/html"))
        .then(|| HeaderValue::from_static(CONTENT_SECURITY_POLICY))
}

#[cfg(test)]
mod tests {
    use axum::response::{Html, Redirect};

    use super::*;

    /// An HTML page gets the policy, with any case in its media type, and the
    /// other responses of the console (a script, JSON, a PDF, a redirect) get
    /// none.
    #[test]
    fn the_policy_goes_on_html_responses_only() {
        let upper = ([(header::CONTENT_TYPE, "Text/HTML")], "<p>x</p>").into_response();
        let pdf = ([(header::CONTENT_TYPE, "application/pdf")], "%PDF").into_response();
        let json = axum::Json(serde_json::json!({})).into_response();
        let rows: [(&str, Response, bool); 6] = [
            ("page", Html("<p>x</p>").into_response(), true),
            ("upper-case page", upper, true),
            ("script", script("var x;"), false),
            ("json", json, false),
            ("pdf", pdf, false),
            ("redirect", Redirect::to("/console").into_response(), false),
        ];
        for (name, response, expected) in rows {
            assert_eq!(html_policy(&response).is_some(), expected, "{name}");
        }
        assert_eq!(
            CONTENT_SECURITY_POLICY,
            "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
             img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'self'; \
             frame-ancestors 'none'"
        );
    }

    /// The scripts read the names that the pages and the server write: the
    /// `data-confirm` question of a delete form, the `_csrf` field that the
    /// POST form handlers read, the `data-ops` list of the metrics page, and
    /// the session-authenticated metrics feed with each row's account. The
    /// rows hold those names and the calls and events that use them. This
    /// checks the text of the scripts, not what they do.
    #[test]
    fn the_scripts_read_the_names_the_pages_write() {
        let console = include_str!("assets/console.js");
        let metrics = include_str!("assets/metrics.js");
        for (script, name) in [
            (console, "addEventListener('DOMContentLoaded'"),
            (console, "addEventListener('submit'"),
            (console, "getAttribute('data-confirm')"),
            (console, "window.confirm(question)"),
            (console, "e.preventDefault()"),
            (console, "i.name = '_csrf'"),
            (console, "meta[name=\"csrf-token\"]"),
            (metrics, "document.currentScript.dataset.ops"),
            (metrics, "/console/metrics-data"),
            (metrics, "d.AccountId"),
        ] {
            assert!(script.contains(name), "{name}");
        }
        assert!(!metrics.contains("accountFilter"));
    }
}
