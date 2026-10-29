// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! HTTP server for extenddb.
//!
//! Provides the axum-based HTTP layer that accepts Virtual `DynamoDB` requests,
//! authenticates them, dispatches to the engine, and returns wire-format responses
//! with correct headers (x-amzn-RequestId, x-amz-crc32, Content-Type).
//! Supports TLS via `axum-server` with rustls when configured.

pub(crate) mod authorization;
pub mod authz_cache;
pub(crate) mod authz_resource;
pub mod console;
pub mod dev_credentials;
mod handler;
mod import_export_paths;
pub mod key_info_cache;
mod leading_keys;
mod listener;
pub mod management;
mod metrics_endpoint;
pub mod rate_limit;
mod request_helpers;
mod request_tags;
mod response;
mod serve;
mod throttle_helpers;
mod workers;

pub use serve::{BuildInfo, LogTarget, ServeParams, log_to_syslog_raw, serve};

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
}

/// TLS configuration passed from the binary crate.
pub struct ServerTlsConfig {
    /// Path to the PEM certificate file.
    pub cert_path: PathBuf,
    /// Path to the PEM private key file.
    pub key_path: PathBuf,
}

/// Build and start the HTTP server on a pre-bound listener.
///
/// The caller is responsible for binding the `TcpListener` before passing it in.
/// This supports the bind-before-fork pattern: the socket is bound in the sync
/// context before daemonizing, so port conflicts are reported to stderr before
/// the parent process exits.
///
/// When `tls` is `Some`, the server serves HTTPS using `axum-server` with rustls.
/// When `tls` is `None`, the server serves plaintext HTTP.
pub async fn start_server(
    listener: tokio::net::TcpListener,
    state: AppState,
    pid_file: Option<PathBuf>,
    tls: Option<ServerTlsConfig>,
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
    let shared = Arc::new(state);

    // Security headers applied to all responses.
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

        let console_state = Arc::new(console::ConsoleState {
            sessions: console::session::SessionStore::new(),
            version_info,
            listen_url,
            config_entries,
            catalog_store,
            docs_store: docs_store.clone(),
            auth_cache: auth_cache_for_console,
            metrics: metrics_for_console,
        });
        let console_router = console::router().with_state(console_state);
        app = app
            .nest("/console", console_router)
            // Redirect /console/ (trailing slash) to /console so the nested
            // root route matches. Browsers and users naturally add trailing slashes.
            .route(
                "/console/",
                get(|| async { Redirect::permanent("/console") }),
            );
    }

    // Apply security headers to all routes.
    // A body that stops delivering data for `request_timeout` errors out, so a
    // client that announces a Content-Length it never sends cannot hold the
    // connection open. The handler answers that error with 408.
    let app = app
        .layer(security_layers)
        .layer(RequestBodyTimeoutLayer::new(request_timeout));

    // Add HSTS header only when TLS is enabled.
    let app = if tls_enabled {
        app.layer(SetResponseHeaderLayer::overriding(
            axum::http::header::STRICT_TRANSPORT_SECURITY,
            HeaderValue::from_static("max-age=63072000; includeSubDomains"),
        ))
    } else {
        app
    };

    listener::serve_app(listener, app, request_timeout, tls, pid_file).await
}

/// GET /health — REQ-OBS-006
async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, axum::Json(json!({"status": "healthy"})))
}
