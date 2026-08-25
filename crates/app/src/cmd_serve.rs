// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb serve` — start the Virtual `DynamoDB` server.

use std::net::TcpListener;

use clap::Args;
#[cfg(unix)]
use daemonize::Daemonize;

use crate::serve_helpers::check_config_permissions;
#[cfg(unix)]
use crate::serve_helpers::verify_daemon_started;
use extenddb_config as config;
use extenddb_config::pid_file_path;
use extenddb_server::{BuildInfo, LogTarget, ServeParams};

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

    /// Write a PID file in --foreground mode.
    ///
    /// Foreground mode normally writes none, because the supervisor owns the
    /// process and a read-only root filesystem then needs no run directory.
    /// Pass this when you want `extenddb status` and `extenddb stop` to work
    /// against a foreground server, for example when running it from a shell.
    /// It goes to the same `run_dir` path daemon mode uses, so `stop` and
    /// `status` find it with no extra arguments. Ignored in daemon mode, which
    /// always writes one.
    #[arg(long)]
    write_pid_file: bool,
}

/// Bind the listening socket, daemonize, then start the tokio runtime.
/// Binding before forking ensures port conflicts are reported to stderr
/// before the parent process exits (D-4).
pub fn run(args: &ServeArgs, build: BuildInfo) -> anyhow::Result<()> {
    // Load config early so bind address is known before fork. A dev-mode
    // build serves with built-in defaults when no config file exists
    // (zero-config local/CI use: loopback, in-memory storage, seeded dev
    // credential — a drop-in for DynamoDB Local). Production builds require
    // an `init`-generated config.
    let config_exists = std::path::Path::new(&args.config).exists();
    let app_config = if config_exists {
        // P50: Check config file permissions before loading. The config file
        // may contain the encryption key (via `extenddb init`). Reject if more
        // permissive than 0600 (owner read/write only).
        check_config_permissions(&args.config)?;
        config::load(&args.config)?
    } else if cfg!(feature = "dev-mode") {
        config::load_builtin_defaults()?
    } else {
        anyhow::bail!(
            "Config file '{}' not found. Run 'extenddb init' to create one, \
             or use --config <path> to specify a different location.",
            args.config,
        );
    };

    // D5: TLS is mandatory — except in a dev-mode build, which deliberately
    // serves plain HTTP on loopback for frictionless local/CI use.
    if !cfg!(feature = "dev-mode") && !app_config.server.tls.enabled {
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

    // Validate backend is supported and get catalog version (fail fast before binding port)
    let backend = &app_config.storage.backend;
    let catalog_version = extenddb_storage::operations::catalog_version()?;

    let port = args.port.unwrap_or(app_config.server.port);

    // Dev mode serves plain HTTP with relaxed authorization; confine it to
    // loopback so it can never be exposed off-host.
    //
    // A container is the one case where a loopback bind is not the right lever.
    // Inside a network namespace, binding 0.0.0.0 is not exposure: what a
    // container publishes is decided by the port mapping on the host, so the
    // image must bind 0.0.0.0 and containment moves to `-p 127.0.0.1:...`.
    // `EXTENDDB_DEV_ALLOW_ANY_BIND=1` is that escape hatch, set by the dev
    // image and nothing else. It is read only when `dev-mode` is compiled in,
    // so a production build has no such lever at all: `postgres` and `mongodb`
    // cannot enable `dev-mode` (see the compile_error in the bin crate).
    if cfg!(feature = "dev-mode") {
        let host = app_config.server.bind_addr.trim();
        let loopback = host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .map(|ip| ip.is_loopback())
                .unwrap_or(false);
        let allow_any_bind = std::env::var("EXTENDDB_DEV_ALLOW_ANY_BIND")
            .is_ok_and(|v| matches!(v.trim(), "1" | "true"));
        if !loopback && !allow_any_bind {
            anyhow::bail!(
                "dev-mode builds may only bind to loopback (127.0.0.1, ::1, localhost); \
                 got server.bind_addr = '{host}'. Set EXTENDDB_DEV_ALLOW_ANY_BIND=1 only \
                 inside a container, where the published port decides exposure."
            );
        }
        if !loopback && allow_any_bind {
            tracing::warn!(
                bind_addr = host,
                "dev-mode is binding a non-loopback address: plain HTTP with relaxed \
                 authorization. Publish it on loopback only (-p 127.0.0.1:PORT:PORT) and \
                 never expose it to a shared network."
            );
        }
    }

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
    let banner_line1 = format!(
        "extenddb {} (catalog {}) starting on {}",
        build.version, catalog_version, bind_addr,
    );
    let banner_line2 = format!(
        "  storage: {} ({})",
        backend,
        config::redact_password(app_config.storage.connection_config()),
    );
    if args.foreground {
        eprintln!("{banner_line1}");
        eprintln!("{banner_line2}");
    } else {
        println!("{banner_line1}");
        println!("{banner_line2}");
    }
    if cfg!(feature = "dev-mode") {
        let msg = format!(
            "  DEVELOPER MODE: plain HTTP on loopback, authorization open \
             (SigV4 still enforced). Serving storage: {}. Not for production.",
            config::redact_password(app_config.storage.connection_config()),
        );
        if args.foreground {
            eprintln!("{msg}");
        } else {
            println!("{msg}");
        }
    }

    // D-3: A PID file lets `extenddb status` and `extenddb stop` find the
    // process. Daemon mode always writes one. Foreground mode writes one only on
    // request, so by default it needs no run directory at all and the container
    // can use a read-only root filesystem.
    let run_dir = config::expand_tilde(&app_config.server.run_dir);
    let pid_file = if args.foreground && !args.write_pid_file {
        None
    } else {
        std::fs::create_dir_all(&run_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create run directory {run_dir}: {e}"))?;
        Some(pid_file_path(&run_dir, port))
    };

    // P57 Bug 7 fix: Use execute() instead of start() so the parent can
    // verify the daemon child is healthy before exiting. start() exits the
    // parent immediately after fork, hiding child startup failures.
    //
    // When --foreground is set, skip daemonization entirely so the process
    // can be supervised by Docker, Kubernetes, systemd Type=simple, etc.
    // Graceful shutdown on SIGINT/SIGTERM still works.
    if !args.foreground {
        // Daemon mode (double fork, PID file, syslog) is POSIX-only. On
        // Windows the server must be run with `--foreground` under an
        // external supervisor (a terminal, a service wrapper, or the npm
        // launcher, which always passes `--foreground`).
        #[cfg(not(unix))]
        anyhow::bail!(
            "daemon mode is not supported on this platform; \
             run `extenddb serve --foreground`"
        );

        #[cfg(unix)]
        {
            let pid_file = pid_file
                .as_ref()
                .expect("daemon mode always has a PID file path");
            // The listening socket bound successfully above, which proves no live
            // server owns this port. Any existing PID file is therefore stale (a
            // prior run that crashed or was killed without cleanup). Remove it
            // before daemonizing so startup verification reads the new daemon's
            // PID rather than a dead one from a previous run.
            let _ = std::fs::remove_file(pid_file);
            let daemon = Daemonize::new().pid_file(pid_file);
            match daemon.execute() {
                daemonize::Outcome::Parent(Ok(_)) => {
                    // Parent process: wait for the PID file to appear (written by
                    // the grandchild after the double-fork), then verify the daemon
                    // is still alive. This catches crashes during early startup
                    // (bad config, missing tables, TLS cert errors).
                    return verify_daemon_started(pid_file, &bind_addr);
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
            // child process silently disappears on panic. Reuses the server crate's
            // raw syslog writer so there is one implementation of it — tracing is
            // unusable here because the subscriber is only set up inside `serve`.
            std::panic::set_hook(Box::new(|info| {
                extenddb_server::log_to_syslog_raw(&format!("extenddb panic: {info}"));
            }));
        }
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(extenddb_server::serve(
            ServeParams::new(app_config, std_listener, pid_file, build)
                .with_log_target(if args.foreground {
                    LogTarget::Stderr
                } else {
                    LogTarget::Syslog
                })
                .with_dev_mode(cfg!(feature = "dev-mode")),
        ))
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
