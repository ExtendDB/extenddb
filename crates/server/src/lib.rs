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
pub mod console;
mod handler;
pub mod key_info_cache;
pub mod management;
mod metrics_endpoint;
pub mod rate_limit;
mod request_helpers;
mod response;
mod throttle_helpers;

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
    let catalog_store = state.catalog_store.clone();
    let auth_cache_for_mgmt = state.auth_cache.clone();
    let auth_cache_for_console = state.auth_cache.clone();
    let authz_cache_for_mgmt = state.authz_cache.clone();
    let table_key_info_cache_for_mgmt = state.table_key_info_cache.clone();
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
        // REQ-OBS-005: Prometheus-compatible metrics endpoint. Public by
        // design (Prometheus scrapers expect no auth on the scrape URL);
        // operators who need stricter access should expose this on a
        // separate bind via firewall/proxy. Returns aggregate metrics
        // only; per-request data is in the management API.
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
    let app = app.layer(security_layers);

    // Add HSTS header only when TLS is enabled.
    let app = if tls_enabled {
        app.layer(SetResponseHeaderLayer::overriding(
            axum::http::header::STRICT_TRANSPORT_SECURITY,
            HeaderValue::from_static("max-age=63072000; includeSubDomains"),
        ))
    } else {
        app
    };

    let local_addr = listener.local_addr()?;

    if let Some(tls_cfg) = tls {
        // P57 Bug 2 fix: rustls 0.23 requires an explicit CryptoProvider.
        // Install aws-lc-rs as the default before creating any TLS config.
        // Ignore the error if a provider was already installed (e.g., by sqlx).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        tracing::info!("extenddb listening on {local_addr} (HTTPS)");
        let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
            &tls_cfg.cert_path,
            &tls_cfg.key_path,
        )
        .await
        // AI-3: The error intentionally does not include the file path to avoid
        // leaking filesystem structure in logs. See docs/troubleshooting.md for
        // the "Failed to load TLS certificates" entry.
        .map_err(|e| anyhow::anyhow!("Failed to load TLS certificates: {e}"))?;

        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        let shutdown_pid = pid_file.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(5)));
            // P50: Log timeout but don't call std::process::exit — let the
            // runtime shut down normally so destructors (including ZeroizeOnDrop) run.
            tokio::time::sleep(Duration::from_secs(10)).await;
            tracing::warn!("Graceful shutdown timed out, forcing PID file cleanup");
            cleanup_pid_file(shutdown_pid.as_deref());
        });

        let std_listener = listener.into_std()?;
        // AI-2: Use a custom acceptor that peeks the first byte of each
        // connection. If it's plain HTTP (not 0x16 TLS ClientHello), write
        // a 301 redirect to HTTPS and reject the connection before the TLS
        // handshake. This gives users a helpful redirect instead of a
        // confusing TLS handshake failure.
        let redirect_acceptor = HttpsRedirectAcceptor { addr: local_addr };
        axum_server::from_tcp_rustls(std_listener, rustls_config)?
            .map(|tls| tls.acceptor(redirect_acceptor))
            .handle(handle)
            .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await?;
    } else {
        tracing::info!("extenddb listening on {local_addr}");
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(graceful_shutdown(pid_file.clone()))
        .await?;
    }

    // Normal shutdown path — clean up PID file after connections drain.
    cleanup_pid_file(pid_file.as_deref());

    Ok(())
}

/// GET /health — REQ-OBS-006
async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, axum::Json(json!({"status": "healthy"})))
}

async fn graceful_shutdown(pid_file: Option<PathBuf>) {
    shutdown_signal().await;
    // P50: Spawn a timeout task that cleans up the PID file if connections
    // don't drain. Does NOT call std::process::exit — the runtime shuts down
    // normally so destructors (including ZeroizeOnDrop) run.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        tracing::warn!("Graceful shutdown timed out after 5s, forcing PID file cleanup");
        cleanup_pid_file(pid_file.as_deref());
    });
}

/// Remove the PID file if it exists. Best-effort — log but don't fail.
fn cleanup_pid_file(pid_file: Option<&std::path::Path>) {
    if let Some(path) = pid_file
        && let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("Failed to remove PID file {}: {e}", path.display());
    }
}

/// AI-2: Acceptor that detects plain HTTP connections on the TLS port.
///
/// Peeks the first byte of each connection. If it's `0x16` (TLS ClientHello),
/// the connection passes through to the TLS acceptor. If it's a plain HTTP
/// verb, a 301 redirect to `https://` is written and the connection is
/// rejected with an IO error (which `axum_server` handles by dropping it).
#[derive(Clone)]
struct HttpsRedirectAcceptor {
    addr: std::net::SocketAddr,
}

impl<S: Send + 'static> axum_server::accept::Accept<tokio::net::TcpStream, S>
    for HttpsRedirectAcceptor
{
    type Stream = tokio::net::TcpStream;
    type Service = S;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::io::Result<(Self::Stream, Self::Service)>> + Send,
        >,
    >;

    fn accept(&self, stream: tokio::net::TcpStream, service: S) -> Self::Future {
        let addr = self.addr;
        Box::pin(async move {
            let mut peek_buf = [0u8; 1];
            match stream.peek(&mut peek_buf).await {
                Ok(0) => {
                    // Connection closed before sending data.
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "empty connection",
                    ))
                }
                Ok(_) if peek_buf[0] == 0x16 => {
                    // TLS ClientHello — pass through to TLS acceptor.
                    Ok((stream, service))
                }
                Ok(_) => {
                    // Plain HTTP — send redirect and reject.
                    send_https_redirect(&stream, addr);
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "plain HTTP redirected to HTTPS",
                    ))
                }
                Err(e) => Err(e),
            }
        })
    }
}

/// Write an HTTP 301 redirect response to HTTPS on the raw TCP stream.
fn send_https_redirect(stream: &tokio::net::TcpStream, addr: std::net::SocketAddr) {
    let location = format!("https://{addr}/");
    let response = format!(
        "HTTP/1.1 301 Moved Permanently\r\n\
         Location: {location}\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\r\n"
    );
    // Best-effort write — if it fails, the client already got a connection error.
    let _ = stream.try_write(response.as_bytes());
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let Ok(mut sigterm) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    else {
        tracing::error!("Failed to register SIGTERM handler, falling back to Ctrl+C only");
        let _ = ctrl_c.await;
        tracing::info!("Shutdown signal received, draining connections...");
        return;
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm.recv() => {},
    }
    tracing::info!("Shutdown signal received, draining connections...");
}
