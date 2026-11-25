// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! HTTP server for extenddb.
//!
//! Provides the axum-based HTTP layer that accepts Virtual `DynamoDB` requests,
//! authenticates them, dispatches to the engine, and returns wire-format responses
//! with correct headers (x-amzn-RequestId, x-amz-crc32, Content-Type).
//! Supports TLS via `axum-server` with rustls when configured.

pub(crate) mod arn_gate;
pub(crate) mod authorization;
pub mod authz_cache;
pub(crate) mod authz_resource;
pub mod backup_store;
pub mod console;
pub mod dev_credentials;
pub mod extensions;
mod handler;
mod import_export_paths;
mod import_export_roots;
mod item_authz;
pub mod key_info_cache;
mod leading_keys;
mod listener;
pub mod management;
mod metrics_endpoint;
pub mod rate_limit;
mod registration;
mod repeated_headers;
mod request_attributes;
mod request_helpers;
mod request_tags;
mod response;
mod serve;
mod table_name_gate;
mod throttle_helpers;
mod workers;

/// The `axum` this server is built with, for a thin binary's extension routers.
pub use axum;
pub use console::ConsoleIdentity;
pub use extensions::{
    ConsoleTab, ExtensionMountError, Extensions, RequestObserver, RequestRecord,
    validate_console_slugs, validate_extension_prefixes,
};
pub use serve::{
    BuildInfo, CatalogRefusal, JOB_DRAIN_TIMEOUT, LogTarget, RegistrationLost, ServeParams,
    WORKER_STOP_TIMEOUT, catalog_refusal, log_to_syslog_raw, serve,
};

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect};
use axum::routing::{get, post};
use extenddb_auth::{AuthCacheRegistry, AuthProvider};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::metrics::MetricsCollector;
use extenddb_core::throttle::ThrottleManager;

pub use authz_cache::{AuthzCacheConfig, CachedAuthzStore, CachedSessionData};
pub use key_info_cache::CachedTableKeyInfoStore;
use serde_json::json;
use tower::ServiceBuilder;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::RequestBodyTimeoutLayer;

/// Application state shared across all handlers.
pub struct AppState {
    pub storage: Arc<dyn extenddb_storage::StorageEngine>,
    /// The job slots and configured backup store the backup operations read.
    pub backup_runtime: Arc<extenddb_engine::BackupRuntime>,
    pub auth: Arc<dyn AuthProvider>,
    pub limits: Arc<LimitsConfig>,
    // Fix #9: Use Arc<str> to avoid per-request cloning
    pub region: Arc<str>,
    pub server_addr: String,
    /// Catalog store implementing operational storage traits.
    pub catalog_store: Option<Arc<dyn extenddb_storage::CatalogStore>>,
    /// Version string for the web console footer.
    pub version_info: Arc<str>,
    /// In-memory metrics collector for `DynamoDB` `CloudWatch`-style metrics.
    pub metrics: Arc<MetricsCollector>,
    /// Whether TLS is enabled (affects cookie Secure flag).
    pub tls_enabled: bool,
    /// Header read timeout and body inactivity timeout for every connection.
    pub request_timeout: Duration,
    /// Developer mode: authorization is opened for authenticated callers (SigV4
    /// is still verified). Only ever true in a `dev-mode` build bound to
    /// loopback; the bin's serve path enforces those preconditions.
    pub dev_mode: bool,
    /// Allowed directories for import file operations. Empty means imports
    /// are disabled (secure default).
    pub import_paths: Arc<[Arc<std::path::PathBuf>]>,
    /// Allowed directories for export file operations. Empty means exports
    /// are disabled (secure default).
    pub export_paths: Arc<[Arc<std::path::PathBuf>]>,
    /// Token bucket throttle manager for provisioned throughput enforcement.
    pub throttle: Arc<ThrottleManager>,
    /// Auth/authz cache handles. Used by the management API to issue
    /// write-through invalidations after IAM mutations. Empty when caching
    /// is disabled.
    pub auth_cache: AuthCacheRegistry,
    /// Authorization cache used by the request hot path. The same underlying
    /// cache instance is referenced trait-object style by `auth_cache.authz`.
    pub authz_cache: Arc<CachedAuthzStore>,
    /// `TableKeyInfo` cache used by the request hot path and engine batch /
    /// transact paths. Invalidated by control-plane handlers after Create /
    /// Update / Delete table.
    pub table_key_info_cache: Arc<CachedTableKeyInfoStore>,
    /// Static configuration entries from the `.toml` file for the console
    /// settings page. Each entry is `(key, display_value)` — sensitive values
    /// are pre-redacted by the caller.
    pub config_entries: Vec<(String, String)>,
    /// Runtime documentation store. `None` if `docs_dir` is not configured.
    pub docs_store: Option<console::docs_embed::DocsStore>,
    /// Called once per request after the response is built, when a thin
    /// binary installed one through [`Extensions::request_observer`]. `None`
    /// leaves the request path as it was and builds no record.
    pub request_observer: Option<Arc<dyn extensions::RequestObserver>>,
    /// Whether new requests are admitted, and whether the server must stop:
    /// set while the backend's serving registration is lost
    /// ([`registration`]).
    pub(crate) admission: registration::Admission,
}

/// The response security headers over the whole app: `X-Content-Type-Options`,
/// `X-Frame-Options` and `Referrer-Policy`. HSTS is added in [`start_server`]
/// when TLS is on. A function of its own so the extension mount tests apply
/// the layer the server applies.
fn apply_security_headers(app: Router) -> Router {
    let security_layers = ServiceBuilder::new()
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::REFERRER_POLICY,
            HeaderValue::from_static("strict-origin-when-cross-origin"),
        ));
    app.layer(security_layers)
}

/// TLS configuration passed from the binary crate.
pub struct ServerTlsConfig {
    /// Path to the PEM certificate file.
    pub cert_path: PathBuf,
    /// Path to the PEM private key file.
    pub key_path: PathBuf,
}

/// How long the listener drains active connections after a stop, on HTTPS
/// and on plain HTTP alike: connections still open at the bound are closed,
/// and the PID file is removed once the drain ends. `extenddb stop` waits for
/// the sum of the shutdown bounds.
pub const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Build and start the HTTP server on a pre-bound listener.
///
/// The caller is responsible for binding the `TcpListener` before passing it in.
/// This supports the bind-before-fork pattern: the socket is bound in the sync
/// context before daemonizing, so port conflicts are reported to stderr before
/// the parent process exits.
///
/// When `tls` is `Some`, the server serves HTTPS using `axum-server` with rustls.
/// When `tls` is `None`, the server serves plaintext HTTP.
///
/// `extension_routers` are a thin binary's HTTP routers (see
/// [`Extensions::http`]). Each is mounted after the built-in routes and before
/// the response security layer, so an extension response carries the security
/// headers too. A prefix that is not absolute, is `/`, or collides with a
/// built-in route or an earlier extension is an error and the server does not
/// start. `console_tabs` are the thin binary's console pages (see
/// [`extensions::ConsoleTab`]); each is nested at `/console/<slug>` behind the
/// console's session check and listed in the navigation bar. A malformed,
/// reserved or repeated slug is an error whether or not the console is
/// mounted.
///
/// It stops on SIGINT or SIGTERM, or when `state`'s admission asks for a
/// stop after the serving registration was lost; both drain the listener
/// the same way and remove the PID file.
pub async fn start_server(
    listener: tokio::net::TcpListener,
    state: AppState,
    pid_file: Option<PathBuf>,
    tls: Option<ServerTlsConfig>,
    extension_routers: Vec<(String, Router)>,
    console_tabs: Vec<extensions::ConsoleTab>,
) -> Result<(), anyhow::Error> {
    // SP-WIRE-007: DynamoDB request body limit is 16 MB.
    const DYNAMODB_BODY_LIMIT: usize = 16 * 1024 * 1024;

    let tls_enabled = state.tls_enabled;
    let request_timeout = state.request_timeout;
    let catalog_store = state.catalog_store.clone();
    let auth_cache_for_mgmt = state.auth_cache.clone();
    let auth_cache_for_console = state.auth_cache.clone();
    let metrics_for_console = state.metrics.clone();
    let authz_cache_for_mgmt = state.authz_cache.clone();
    let table_key_info_cache_for_mgmt = state.table_key_info_cache.clone();
    let mgmt_region = state.region.clone();
    let version_info = state.version_info.clone();
    let docs_store = state.docs_store.clone();
    let listen_url = {
        let addr = listener.local_addr()?;
        let scheme = if tls_enabled { "https" } else { "http" };
        format!("{scheme}://{addr}")
    };
    let config_entries = state.config_entries.clone();
    let admission = state.admission.clone();
    let shared = Arc::new(state);

    let mut app = Router::new()
        .route("/", post(handler::handle_request))
        .layer(DefaultBodyLimit::max(DYNAMODB_BODY_LIMIT))
        .route("/health", get(health_check))
        // REQ-OBS-005: Prometheus-compatible metrics endpoint. Requires admin
        // HTTP Basic auth (enforced in the handler) because the payload carries
        // per-table CloudWatch-style dimensions, so it is served only to an
        // authorized caller. Prometheus scrapers configure
        // `basic_auth`. The web console dashboard uses the session-authed
        // `/console/metrics-data` route instead.
        .route("/metrics", get(metrics_endpoint::metrics_endpoint))
        // S-6: Explicit small body limit for non-DynamoDB endpoints.
        .layer(DefaultBodyLimit::max(1024))
        .with_state(shared);

    // Tab slugs are checked in every deployment, as extension prefixes are,
    // so a bad slug is a startup error whether or not the console is mounted.
    extensions::validate_console_slugs(&console_tabs).map_err(|e| anyhow::anyhow!("{e}"))?;

    // Mount management API and web console if catalog store is available.
    if let Some(catalog_store) = catalog_store {
        let mgmt_state = Arc::new(management::ManagementState {
            catalog_store: catalog_store.clone(),
            auth_cache: auth_cache_for_mgmt,
            authz_cache: authz_cache_for_mgmt,
            table_key_info_cache: table_key_info_cache_for_mgmt,
            region: mgmt_region,
            secure_transport: tls_enabled,
        });
        let mgmt_router = management::router().with_state(mgmt_state);
        app = app.nest("/management", mgmt_router);

        // Each tab is nested at /console/<slug> behind the console's session
        // check and listed in the navigation bar.
        let tab_links: Arc<[(String, String)]> = console_tabs
            .iter()
            .map(|t| (t.label.clone(), format!("/console/{}", t.slug)))
            .collect();
        let console_state = Arc::new(console::ConsoleState {
            sessions: console::session::SessionStore::new(),
            tabs: tab_links,
            version_info,
            listen_url,
            config_entries,
            catalog_store,
            docs_store: docs_store.clone(),
            auth_cache: auth_cache_for_console,
            metrics: metrics_for_console,
            secure_cookies: tls_enabled,
        });
        // With a tab mounted, the console's own fallback is explicit so that
        // the fallbacks nested below it (each tab's, inside its session layer)
        // are kept when the console is nested into the app; axum drops a
        // child's fallback when its parent still has the default one. With no
        // tab the console keeps the default, so an unmatched console path gets
        // the app's 404, headers included.
        let mut console_router = console::router();
        if !console_tabs.is_empty() {
            console_router = console_router.fallback(|| async { StatusCode::NOT_FOUND });
        }
        let mut console_router = console_router.with_state(console_state.clone());
        for tab in console_tabs {
            let gate = axum::middleware::from_fn_with_state(
                console_state.clone(),
                console::require_console_session,
            );
            // The fallback sits inside the layer, so every path under the tab
            // (its routes and its non-routes alike) meets the session check.
            let gated = tab
                .router
                .fallback(|| async { StatusCode::NOT_FOUND })
                .layer(gate.clone());
            // The trailing-slash form redirects to the page, as /console/ does
            // below, behind the same check. Without a session every path under
            // the slug answers 303 to the login page, as a built-in page such
            // as /console/accounts does, and an unmounted slug answers 404, as
            // an unknown console path does.
            let page = format!("/console/{}", tab.slug);
            let slash = Router::new()
                .route("/", get(move || async move { Redirect::permanent(&page) }))
                .layer(gate);
            console_router = console_router
                .nest(&format!("/{}", tab.slug), gated)
                .nest(&format!("/{}/", tab.slug), slash);
        }
        app = app
            .nest("/console", console_router)
            // Redirect /console/ (trailing slash) to /console so the nested
            // root route matches. Browsers and users naturally add trailing slashes.
            .route(
                "/console/",
                get(|| async { Redirect::permanent("/console") }),
            );
    }

    // Thin-bin extension routers go after the built-in nests and before the
    // security layer, so their responses carry the security headers too. A
    // refused prefix is a startup error.
    let app =
        extensions::mount_extensions(app, extension_routers).map_err(|e| anyhow::anyhow!("{e}"))?;

    // While the serving registration is lost, every route answers
    // ServiceUnavailable before its handler runs.
    let app = app.layer(axum::middleware::from_fn_with_state(
        admission.clone(),
        registration::refuse_while_unregistered,
    ));

    // Refuse a repeated host or authorization header on every route, then
    // apply security headers to all routes, built-in and extension (the
    // refusal carries them too). A body that stops delivering data for
    // `request_timeout` errors out, so a client that announces a
    // Content-Length it never sends cannot hold the connection open. The
    // handler answers that error with 408.
    let app = app.layer(axum::middleware::from_fn(
        repeated_headers::refuse_repeated_headers,
    ));
    let app = apply_security_headers(app).layer(RequestBodyTimeoutLayer::new(request_timeout));

    // Add HSTS header only when TLS is enabled.
    let app = if tls_enabled {
        app.layer(SetResponseHeaderLayer::overriding(
            axum::http::header::STRICT_TRANSPORT_SECURITY,
            HeaderValue::from_static("max-age=63072000; includeSubDomains"),
        ))
    } else {
        app
    };

    listener::serve_app(listener, app, request_timeout, tls, pid_file, admission).await
}

/// GET /health — REQ-OBS-006.
///
/// Always 200 while the server answers: the data plane is up. `status` is
/// `healthy`, or `degraded` when a component is unhealthy, and `components`
/// carries each component the server has. Today the one component is
/// `backup_store`, present when a store is configured: `status` `healthy`,
/// `checking` (an S3 store's first check has not returned; the server's
/// `status` stays `healthy`) or `unhealthy`, `kind` (`filesystem` or `s3`)
/// and, when unhealthy, `reason`, the problem's category in one or two
/// words. The endpoint is unauthenticated, so the reason carries no id, ARN,
/// table name or root; the operator's log line does. A store problem never
/// takes the data plane out of rotation, which is why the HTTP status stays
/// 200.
async fn health_check(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> impl IntoResponse {
    let mut components = serde_json::Map::new();
    let mut status = "healthy";
    if let Some(store) = state.backup_runtime.store_health() {
        let mut component = json!({
            "status": store.status,
            "kind": store.kind,
        });
        if let Some(reason) = store.reason {
            component["reason"] = serde_json::Value::String(reason.to_owned());
            status = "degraded";
        }
        components.insert("backup_store".to_owned(), component);
    }
    (
        StatusCode::OK,
        axum::Json(json!({ "status": status, "components": components })),
    )
}
