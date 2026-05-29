// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb serve` — start the Virtual `DynamoDB` server.

use std::net::TcpListener;
use std::sync::Arc;

use clap::Args;
use daemonize::Daemonize;
use extenddb_server::AppState;
use syslog_tracing::{Facility, Options, Syslog};
use tracing_subscriber::{
    EnvFilter, Layer, fmt, fmt::writer::BoxMakeWriter, layer::SubscriberExt, reload,
    util::SubscriberInitExt,
};

use crate::config;
use crate::serve_helpers::{
    check_config_permissions, log_to_syslog_raw, pid_file_path, verify_daemon_started,
};
use crate::workers;

#[derive(Args, Default)]
pub struct ServeArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Override server port
    #[arg(short, long)]
    port: Option<u16>,

    /// Run in the foreground without daemonizing.
    ///
    /// Useful for running under a container or process supervisor (Docker,
    /// Kubernetes, systemd Type=simple, runit, etc.). In foreground mode logs
    /// are written to stderr instead of syslog so the supervisor can capture
    /// them.
    #[arg(long, alias = "no-daemon")]
    foreground: bool,
}

/// Bind the listening socket, daemonize, then start the tokio runtime.
/// Binding before forking ensures port conflicts are reported to stderr
/// before the parent process exits (D-4).
pub fn run(args: &ServeArgs) -> anyhow::Result<()> {
    // P50: Check config file permissions before loading. The config file may
    // contain the encryption key (via `extenddb init`). Reject if more permissive
    // than 0600 (owner read/write only).
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Run 'extenddb init' to create one, \
             or use --config <path> to specify a different location.",
            args.config,
        );
    }
    check_config_permissions(&args.config)?;

    // Load config early so bind address is known before fork.
    let app_config = config::load(&args.config)?;

    // D5: TLS is mandatory. Reject explicit opt-out.
    if !app_config.server.tls.enabled {
        anyhow::bail!("TLS is mandatory. Remove `tls.enabled = false` from your config file.");
    }

    // D6: Auth is mandatory. Only "builtin" is supported.
    if app_config.auth.provider == "none" {
        anyhow::bail!(
            "auth.provider = \"none\" is no longer supported. \
             Set auth.provider = \"builtin\" and run `extenddb init`."
        );
    }
    if app_config.auth.provider != "builtin" {
        anyhow::bail!(
            "Unknown auth provider '{}'. Only 'builtin' is supported.",
            app_config.auth.provider
        );
    }

    // Check backend is supported by this build
    let backend = &app_config.storage._backend;
    #[cfg(not(feature = "postgres"))]
    if backend == "postgres" {
        anyhow::bail!("PostgreSQL backend not enabled. Rebuild with --features postgres");
    }
    #[cfg(feature = "postgres")]
    if backend != "postgres" {
        anyhow::bail!(
            "Unknown backend '{}'. This build only supports 'postgres'.",
            backend
        );
    }

    let port = args.port.unwrap_or(app_config.server.port);
    let bind_addr = format!("{}:{}", app_config.server.bind_addr, port);

    // Bind in sync context — errors go to stderr before daemonizing.
    let std_listener = TcpListener::bind(&bind_addr)
        .map_err(|e| anyhow::anyhow!("Failed to bind {bind_addr}: {e}"))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|e| anyhow::anyhow!("Failed to set listener non-blocking: {e}"))?;

    // D-2: Print startup banner before daemonizing so the user gets
    // confirmation the server is starting. P57 Bug 4 fix: say "starting" not
    // "listening" — the server isn't actually accepting connections yet.
    //
    // In daemon mode the banner goes to stdout (the user invoking `extenddb
    // serve` reads it before the parent exits). In foreground mode we route
    // it to stderr so a process supervisor receives banner and tracing logs
    // on the same stream — mixing stdout and stderr makes container log
    // capture noisier than necessary.
    let backend = &app_config.storage._backend;
    let catalog_version = extenddb_storage::operations::catalog_version(backend)
        .unwrap_or_else(|_| "unknown".to_string());
    let banner_line1 = format!(
        "extenddb {} (catalog {}) starting on {}",
        env!("CARGO_PKG_VERSION"),
        catalog_version,
        bind_addr,
    );
    let banner_line2 = format!(
        "  storage: {} ({})",
        backend,
        config::redact_password(backend, app_config.storage.connection_config()),
    );
    if args.foreground {
        eprintln!("{banner_line1}");
        eprintln!("{banner_line2}");
    } else {
        println!("{banner_line1}");
        println!("{banner_line2}");
    }

    // D-3: Write PID file so `extenddb status` can report the daemon PID.
    let run_dir = config::expand_tilde(&app_config.server.run_dir);
    std::fs::create_dir_all(&run_dir)
        .map_err(|e| anyhow::anyhow!("Failed to create run directory {run_dir}: {e}"))?;
    let pid_file = pid_file_path(&run_dir, port);

    // P57 Bug 7 fix: Use execute() instead of start() so the parent can
    // verify the daemon child is healthy before exiting. start() exits the
    // parent immediately after fork, hiding child startup failures.
    //
    // When --foreground is set, skip daemonization entirely so the process
    // can be supervised by Docker, Kubernetes, systemd Type=simple, etc.
    // The PID file is still written below by `start_server`, and graceful
    // shutdown on SIGINT/SIGTERM still works.
    if !args.foreground {
        let daemon = Daemonize::new().pid_file(&pid_file);
        match daemon.execute() {
            daemonize::Outcome::Parent(Ok(_)) => {
                // Parent process: wait for the PID file to appear (written by
                // the grandchild after the double-fork), then verify the daemon
                // is still alive. This catches crashes during early startup
                // (bad config, missing tables, TLS cert errors).
                return verify_daemon_started(&pid_file, &bind_addr);
            }
            daemonize::Outcome::Parent(Err(e)) => {
                return Err(anyhow::anyhow!("Failed to daemonize: {e}"));
            }
            daemonize::Outcome::Child(Ok(_)) => {
                // Child (daemon) process: continue to start the server.
            }
            daemonize::Outcome::Child(Err(e)) => {
                return Err(anyhow::anyhow!("Failed to daemonize (child): {e}"));
            }
        }

        // P57 Bug 3 fix: After daemonize, stderr is /dev/null. Install a panic
        // hook that writes to syslog so panics are visible. Without this, the
        // child process silently disappears on panic.
        std::panic::set_hook(Box::new(|info| {
            // Best-effort syslog write. We can't use tracing here because the
            // subscriber may not be initialized yet (it's set up in serve_inner).
            let msg = format!("extenddb panic: {info}");
            // SAFETY: openlog/syslog are POSIX-standard C functions. The ident
            // string is a static C string literal with 'static lifetime.
            unsafe {
                libc::openlog(
                    c"extenddb".as_ptr(),
                    libc::LOG_PID | libc::LOG_NDELAY,
                    libc::LOG_DAEMON,
                );
                // Use CString to ensure null-termination for the format arg.
                if let Ok(cmsg) = std::ffi::CString::new(msg) {
                    libc::syslog(libc::LOG_CRIT, c"%s".as_ptr(), cmsg.as_ptr());
                }
            }
        }));
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve(
            app_config,
            std_listener,
            port,
            run_dir,
            args.foreground,
        ))
}

/// Async entry point: initializes syslog logging, storage, and auth, then
/// starts the HTTP server on the pre-bound listener.
async fn serve(
    app_config: config::AppConfig,
    std_listener: TcpListener,
    port: u16,
    run_dir: String,
    foreground: bool,
) -> anyhow::Result<()> {
    // CB-27: Clean up PID file if serve() fails before reaching the HTTP
    // server (e.g., Postgres connection failure). The PID file was already
    // written by Daemonize in run().
    let pid_path = pid_file_path(&run_dir, port);
    let backend = app_config.storage._backend.clone();
    let result = serve_inner(app_config, std_listener, port, run_dir, backend, foreground).await;
    if let Err(ref e) = result {
        let _ = std::fs::remove_file(&pid_path);
        // P57 Bug 7: Log fatal errors to syslog. After daemonize, stderr is
        // /dev/null so anyhow's error display is lost. Use tracing if
        // available, fall back to raw syslog if tracing isn't initialized yet.
        // In foreground mode, also echo to stderr since the supervisor
        // captures stderr rather than syslog.
        tracing::error!("extenddb fatal: {e:#}");
        if foreground {
            eprintln!("extenddb fatal: {e:#}");
        } else {
            log_to_syslog_raw(&format!("extenddb fatal: {e:#}"));
        }
    }
    result
}

/// Inner serve function — separated so the outer `serve` can clean up the PID
/// file on any error path.
async fn serve_inner(
    app_config: config::AppConfig,
    std_listener: TcpListener,
    port: u16,
    run_dir: String,
    backend: String,
    foreground: bool,
) -> anyhow::Result<()> {
    let catalog_version = extenddb_storage::operations::catalog_version(&backend)
        .unwrap_or_else(|_| "unknown".to_string());

    // In foreground mode, daemonize was skipped so the PID file was never
    // written. Write it now so `extenddb status`/`stop` and `start_server`'s
    // graceful shutdown cleanup still work. The grandchild PID written by
    // daemonize matches `std::process::id()` post-fork, so this stays
    // consistent with daemon mode.
    if foreground {
        let pid_file = pid_file_path(&run_dir, port);
        std::fs::write(&pid_file, format!("{}\n", std::process::id()))
            .map_err(|e| anyhow::anyhow!("Failed to write PID file {}: {e}", pid_file.display()))?;
    }

    // Init logging (REQ-LOG-003, REQ-LOG-006) — syslog in daemon mode, stderr
    // in foreground mode so a container/process supervisor can capture logs.
    // D-3: sqlx messages are controlled by an independent `sqlx_log_level`
    // runtime setting (default: warn). Both extenddb and sqlx messages use the
    // `extenddb` syslog identifier (POSIX syslog supports only one identity per
    // process). sqlx messages are identifiable by their `sqlx::query` target.
    // Filter with: `journalctl -t extenddb | grep -v sqlx` (exclude) or
    // `journalctl -t extenddb | grep sqlx` (include only).
    //
    // The EnvFilter encodes both levels: `{app_level},sqlx={sqlx_level}`.
    // The poll_log_level worker reloads the filter when either setting changes.
    let filter_str = format!("{},sqlx=warn", &app_config.logging.level);
    // CB-29: Always use the config file log level, never RUST_LOG. The runtime
    // settings poller handles dynamic level changes. RUST_LOG silently
    // overriding the config is an operational surprise.
    let filter = EnvFilter::new(&filter_str);
    let (filter_layer, reload_handle) = reload::Layer::new(filter);

    // Pick the writer first (foreground → stderr, daemon → syslog), then the
    // format (text vs json). syslog supplies its own timestamps, so we strip
    // them with `.without_time()` only on the syslog path.
    let (writer, with_time): (BoxMakeWriter, bool) = if foreground {
        (BoxMakeWriter::new(std::io::stderr), true)
    } else {
        let syslog = Syslog::new(
            c"extenddb",
            Options::LOG_PID | Options::LOG_NDELAY,
            Facility::Daemon,
        )
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Failed to initialize syslog — another syslog logger may already be active"
            )
        })?;
        (BoxMakeWriter::new(syslog), false)
    };

    let fmt_layer = match (with_time, app_config.logging.format == "json") {
        (true, true) => fmt::layer().json().with_writer(writer).boxed(),
        (true, false) => fmt::layer().with_writer(writer).boxed(),
        (false, true) => fmt::layer()
            .json()
            .without_time()
            .with_writer(writer)
            .boxed(),
        (false, false) => fmt::layer().without_time().with_writer(writer).boxed(),
    };

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("Failed to initialize tracing: {e}"))?;

    // Create server components via factory pattern
    let components = extenddb_storage::create_server_components(
        &backend,
        app_config.storage.as_trait(),
        &app_config.server.region,
    )
    .await?;

    let storage = components.engine;
    let catalog_store = components.catalog_store;
    let cred_store = components.credential_store;
    let runtime_hooks = components.runtime_hooks;

    // Build SwrCacheConfig values from the [auth.cache] TOML section.
    let cache_cfg = &app_config.auth.cache;
    let cache_enabled = cache_cfg.enabled;
    let make_cache_cfg = |name: &'static str| -> extenddb_cache::SwrCacheConfig {
        extenddb_cache::SwrCacheConfig {
            ttl: std::time::Duration::from_secs(cache_cfg.ttl_seconds),
            soft_ttl: std::time::Duration::from_secs(cache_cfg.soft_ttl_seconds),
            negative_ttl: std::time::Duration::from_secs(cache_cfg.negative_ttl_seconds),
            max_entries: cache_cfg.max_entries,
            name,
        }
    };
    // Validate config eagerly so misconfiguration fails fast at startup.
    // Today every named subcache shares the same TTL/max_entries shape (only
    // `name` differs), so a single `validate()` check suffices. If per-cache
    // tuning is ever added, validate every constructed config here.
    if let Err(e) = make_cache_cfg("__validate__").validate() {
        anyhow::bail!(
            "Invalid [auth.cache] configuration: {e}. Check ttl_seconds, \
             soft_ttl_seconds, negative_ttl_seconds, max_entries."
        );
    }
    if !cache_enabled {
        tracing::warn!(
            "auth.cache.enabled = false — auth/authz caches are in pass-through mode \
             (every lookup hits the catalog directly)"
        );
    }

    // Phase 2: Wrap the raw credential store. In pass-through mode the
    // wrapper bypasses the cache and forwards every lookup to the inner
    // store; otherwise it caches per the TOML config.
    let cached_cred_store = Arc::new(if cache_enabled {
        extenddb_auth::CachedCredentialStore::with_arc(cred_store, make_cache_cfg("credential"))
    } else {
        extenddb_auth::CachedCredentialStore::pass_through_arc(
            cred_store,
            make_cache_cfg("credential"),
        )
    });
    let auth: Arc<dyn extenddb_auth::AuthProvider> = Arc::new(
        extenddb_auth::BuiltinAuthProvider::new((*cached_cred_store).clone()),
    );

    // Phase 3: Build the authorization cache.
    let authz_cache: Arc<extenddb_server::CachedAuthzStore> = {
        let store: Arc<dyn extenddb_storage::authorization_store::AuthorizationStore> =
            catalog_store.clone();
        let cfg = extenddb_server::AuthzCacheConfig {
            identity_policies: make_cache_cfg("identity_policies"),
            group_policies: make_cache_cfg("group_policies"),
            boundary: make_cache_cfg("boundary"),
            principal_tags: make_cache_cfg("principal_tags"),
            resource_tags: make_cache_cfg("resource_tags"),
            session_data: make_cache_cfg("session_data"),
        };
        Arc::new(if cache_enabled {
            extenddb_server::CachedAuthzStore::new(store, cfg)
        } else {
            extenddb_server::CachedAuthzStore::pass_through(store, cfg)
        })
    };

    // Phase 4: Build the TableKeyInfo cache.
    let table_key_info_cache: Arc<extenddb_server::CachedTableKeyInfoStore> =
        Arc::new(if cache_enabled {
            extenddb_server::CachedTableKeyInfoStore::new(
                storage.clone(),
                make_cache_cfg("table_key_info"),
            )
        } else {
            extenddb_server::CachedTableKeyInfoStore::pass_through(
                storage.clone(),
                make_cache_cfg("table_key_info"),
            )
        });

    // Assemble the cache registry threaded into AppState for write-through
    // invalidations from the management API.
    let auth_cache =
        extenddb_auth::AuthCacheRegistry::empty()
            .with_credential(cached_cred_store)
            .with_authz_invalidator(
                authz_cache.clone() as Arc<dyn extenddb_auth::AuthzCacheInvalidator>
            )
            .with_table_key_info_invalidator(table_key_info_cache.clone()
                as Arc<dyn extenddb_auth::TableKeyInfoCacheInvalidator>);

    let data_db_info = runtime_hooks
        .as_ref()
        .and_then(|h| h.backend_info())
        .unwrap_or_else(|| "(unknown)".to_owned());

    // REQ-LOG-001: Startup banner with effective configuration.
    // REQ-LOG-002: Connection strings redact passwords.
    let log_output = if foreground { "stderr" } else { "syslog" };
    tracing::info!(
        "extenddb {} (catalog {}) starting — bind={}:{}, region={}, auth={}, catalog_db={}, data_db={}, log_output={}, log_level={}",
        env!("CARGO_PKG_VERSION"),
        catalog_version,
        app_config.server.bind_addr,
        port,
        app_config.server.region,
        app_config.auth.provider,
        config::redact_password(&backend, app_config.storage.connection_config()),
        data_db_info,
        log_output,
        app_config.logging.level,
    );

    // Convert pre-bound std listener to tokio (D-4: bind before fork).
    let listener = tokio::net::TcpListener::from_std(std_listener)?;

    // P120e: Create metrics collector early so workers can record health.
    let metrics = Arc::new(extenddb_core::metrics::MetricsCollector::new());

    let tls_enabled = app_config.server.tls.enabled;

    // P53: Resolve import and export path lists. Supports both the new
    // [import]/[export] sections and the deprecated import_export_root.
    let resolve_paths = |raw_paths: &[String],
                         label: &str|
     -> anyhow::Result<Vec<Arc<std::path::PathBuf>>> {
        let mut resolved = Vec::new();
        for raw in raw_paths {
            let expanded = config::expand_tilde(raw);
            let path = std::path::PathBuf::from(&expanded);
            if !path.exists() {
                std::fs::create_dir_all(&path)
                    .map_err(|e| anyhow::anyhow!("Cannot create {label} path {expanded}: {e}"))?;
            }
            let canonical = path
                .canonicalize()
                .map_err(|e| anyhow::anyhow!("Cannot canonicalize {label} path {expanded}: {e}"))?;
            resolved.push(Arc::new(canonical));
        }
        Ok(resolved)
    };

    // Build effective path lists: new config takes precedence over deprecated.
    let mut import_paths_raw = app_config.import_config.paths.clone();
    let mut export_paths_raw = app_config.export_config.paths.clone();
    if let Some(ref legacy) = app_config.import_export_root {
        if import_paths_raw.is_empty() {
            import_paths_raw.push(legacy.clone());
        }
        if export_paths_raw.is_empty() {
            export_paths_raw.push(legacy.clone());
        }
        if !app_config.import_config.paths.is_empty() && !app_config.export_config.paths.is_empty()
        {
            tracing::warn!(
                "Both import_export_root and [import]/[export] sections configured; import_export_root is ignored"
            );
        }
    }

    let import_paths: Arc<[Arc<std::path::PathBuf>]> =
        Arc::from(resolve_paths(&import_paths_raw, "import")?);
    let export_paths: Arc<[Arc<std::path::PathBuf>]> =
        Arc::from(resolve_paths(&export_paths_raw, "export")?);

    if import_paths.is_empty() {
        tracing::info!("Import disabled (no [import] paths configured)");
    } else {
        for p in import_paths.iter() {
            tracing::info!("Import enabled, path: {}", p.display());
        }
    }
    if export_paths.is_empty() {
        tracing::info!("Export disabled (no [export] paths configured)");
    } else {
        for p in export_paths.iter() {
            tracing::info!("Export enabled, path: {}", p.display());
        }
    }

    // D9: Build static config entries for the console settings page.
    // Must be called before `app_config.limits` is moved.
    let config_entries = config::build_config_entries(&app_config);

    // AI-1: Load runtime documentation from docs_dir if configured.
    let docs_store = app_config.docs_dir.as_ref().and_then(|raw| {
        let expanded = config::expand_tilde(raw);
        let path = std::path::PathBuf::from(&expanded);
        match extenddb_server::console::docs_embed::DocsStore::load(&path) {
            Ok(store) => {
                tracing::info!("Documentation loaded from {}", path.display());
                Some(store)
            }
            Err(e) => {
                tracing::warn!("Documentation unavailable: {e}");
                None
            }
        }
    });

    let limits = Arc::new({
        let mut limits = app_config.limits;
        if let Some(max_bytes) = app_config.max_import_bytes {
            limits.max_import_file_bytes = max_bytes;
        }
        limits
    });

    let config_throttling = app_config.server.throttling_enabled.unwrap_or(false);
    let initial_throttling = catalog_store
        .get_setting("throttling_enabled")
        .await
        .ok()
        .flatten()
        .map_or(config_throttling, |v| v == "true");

    let throttle = Arc::new(extenddb_core::throttle::ThrottleManager::new(
        limits.per_account_max_rcu,
        limits.per_account_max_wcu,
        initial_throttling,
    ));

    let state = AppState {
        storage,
        auth,
        limits,
        region: Arc::from(app_config.server.region.as_str()),
        server_addr: format!("localhost:{port}"),
        catalog_store: Some(catalog_store.clone()),
        version_info: Arc::from(
            format!(
                "{} · catalog {} · {}",
                env!("CARGO_PKG_VERSION"),
                catalog_version,
                env!("EXTENDDB_GIT_HASH"),
            )
            .as_str(),
        ),
        metrics: metrics.clone(),
        tls_enabled,
        import_paths,
        export_paths,
        throttle: throttle.clone(),
        auth_cache,
        authz_cache,
        table_key_info_cache,
        config_entries,
        docs_store,
    };

    // D-22: Spawn background task to poll log_level from settings table.
    tokio::spawn(workers::poll_log_level(
        catalog_store.clone(),
        reload_handle.clone(),
        app_config.logging.level.clone(),
    ));
    // Poll throttling_enabled runtime setting.
    tokio::spawn(workers::poll_throttling_enabled(
        catalog_store.clone(),
        throttle,
        config_throttling,
    ));
    // Spawn background tasks for metrics pruning and flushing.
    tokio::spawn(workers::metrics_prune_worker(metrics.clone()));
    tokio::spawn(workers::metrics_flush_worker(
        metrics.clone(),
        catalog_store.clone(),
    ));
    // Spawn background task to clean up old login attempt records.
    tokio::spawn(workers::login_attempt_cleanup_worker(catalog_store.clone()));
    // Phase 11a: Spawn background task to warn about approximate consumed capacity.
    tokio::spawn(workers::capacity_warning_worker());

    // Spawn backend-specific workers via runtime hooks
    if let Some(hooks) = runtime_hooks {
        let worker_ctx = extenddb_storage::WorkerContext {
            metrics: metrics.clone(),
            catalog_store: catalog_store.clone(),
            reload_handle: reload_handle.clone(),
            config_log_level: app_config.logging.level.clone(),
        };
        hooks.spawn_workers(&worker_ctx).await;
    }

    let tls_config = if tls_enabled {
        let cert_path = crate::config::expand_tilde(&app_config.server.tls.cert_path);
        let key_path = crate::config::expand_tilde(&app_config.server.tls.key_path);
        Some(extenddb_server::ServerTlsConfig {
            cert_path: std::path::PathBuf::from(cert_path),
            key_path: std::path::PathBuf::from(key_path),
        })
    } else {
        None
    };

    extenddb_server::start_server(
        listener,
        state,
        Some(pid_file_path(&run_dir, port)),
        tls_config,
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ServeArgs;
    use clap::Parser;

    /// Test wrapper so clap has a top-level `Parser` to drive `ServeArgs`.
    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        args: ServeArgs,
    }

    fn parse(argv: &[&str]) -> ServeArgs {
        TestCli::try_parse_from(argv)
            .expect("ServeArgs should parse from valid argv")
            .args
    }

    #[test]
    fn defaults_run_in_daemon_mode() {
        // No --foreground flag preserves the historical daemon behavior so
        // existing users and scripts are unaffected by the new flag.
        let args = parse(&["extenddb-serve"]);
        assert!(!args.foreground);
        assert_eq!(args.config, "extenddb.toml");
        assert!(args.port.is_none());
    }

    #[test]
    fn foreground_flag_is_recognized() {
        let args = parse(&["extenddb-serve", "--foreground"]);
        assert!(args.foreground);
    }

    #[test]
    fn no_daemon_alias_is_recognized() {
        // The issue proposed either `--foreground` or `--no-daemon`; make
        // sure the alias keeps working so users have a choice.
        let args = parse(&["extenddb-serve", "--no-daemon"]);
        assert!(args.foreground);
    }

    #[test]
    fn foreground_combines_with_other_flags() {
        let args = parse(&[
            "extenddb-serve",
            "--config",
            "/etc/extenddb/extenddb.toml",
            "--port",
            "9000",
            "--foreground",
        ]);
        assert!(args.foreground);
        assert_eq!(args.config, "/etc/extenddb/extenddb.toml");
        assert_eq!(args.port, Some(9000));
    }

    #[test]
    fn unknown_flag_is_rejected() {
        // Guard against accidental future renames silently dropping the flag.
        let result = TestCli::try_parse_from(["extenddb-serve", "--daemon-off"]);
        assert!(result.is_err());
    }
}
