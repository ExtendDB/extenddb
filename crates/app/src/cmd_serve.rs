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

    // Fail fast, before binding the port, when no backend is installed.
    let backend = &app_config.storage.backend;
    extenddb_storage::backend_name()
        .ok_or_else(|| anyhow::anyhow!("no storage backend installed"))?;
    let catalog_version = extenddb_storage::migrations::binary_catalog_label();

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
        let loopback = is_loopback_host(host);
        let allow_any_bind = std::env::var("EXTENDDB_DEV_ALLOW_ANY_BIND")
            .is_ok_and(|v| matches!(v.trim(), "1" | "true"));
        if !loopback && !allow_any_bind {
            anyhow::bail!(
                "dev-mode builds may only bind to loopback (127.0.0.1, ::1, localhost); \
                 got server.bind_addr = '{host}'. Set EXTENDDB_DEV_ALLOW_ANY_BIND=1 only \
                 inside a container, where the published port decides exposure."
            );
        }
        // A permitted non-loopback bind is warned about in the dev-mode banner
        // (`dev_mode_banner_lines`), which prints to the console. A
        // `tracing::warn!` here would print nothing: no subscriber is installed
        // until `serve_inner` runs.
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
    //
    // The wordmark is pure 7-bit ASCII on purpose: it renders identically in
    // dumb terminals, container log viewers, and syslog-adjacent collectors
    // that mangle wide Unicode. It prints once per boot, never per request.
    // Color is applied only when the destination stream is an interactive
    // terminal (and NO_COLOR/TERM=dumb are respected), so pipes, `docker
    // logs`, and supervisors always receive the plain form.
    let wordmark = render_wordmark(banner_colors_enabled(args.foreground));
    let banner_line1 = format!(
        "extenddb {} (catalog {}) starting on {}",
        build.version, catalog_version, bind_addr,
    );
    let banner_line2 = format!(
        "  storage: {} ({})",
        backend,
        config::redact_password(app_config.storage.connection_config()),
    );
    let banner_line3 = format!(
        "  {}",
        extenddb_server::backup_store::banner_line(
            &app_config.backup,
            app_config.storage.database_file().as_deref(),
        )
    );
    let mut warnings = app_config.storage.startup_warnings();
    warnings.extend(app_config.server.startup_warnings());
    let startup_warnings = render_startup_warnings(&warnings);
    if args.foreground {
        eprint!("{wordmark}");
        eprintln!("{banner_line1}");
        eprintln!("{banner_line2}");
        eprintln!("{banner_line3}");
        eprint!("{startup_warnings}");
    } else {
        print!("{wordmark}");
        println!("{banner_line1}");
        println!("{banner_line2}");
        println!("{banner_line3}");
        print!("{startup_warnings}");
    }
    if cfg!(feature = "dev-mode") {
        print_dev_mode_banner(&app_config, port, args.foreground)?;
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

    let mut params = ServeParams::new(app_config, std_listener, pid_file, build)
        .with_log_target(if args.foreground {
            LogTarget::Stderr
        } else {
            LogTarget::Syslog
        })
        .with_dev_mode(cfg!(feature = "dev-mode"));
    if config_exists {
        params = params.with_config_path(&args.config);
    }
    // The runtime is ended with a bound (`run_then_shut_down`): a backup
    // store check abandoned on a mount that has stopped answering leaves a
    // blocking thread parked there, and a plain drop of the runtime would
    // join it, holding the process open after the drain until SIGKILL.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = crate::run_then_shut_down(runtime, extenddb_server::serve(params));
    // `serve` has printed the refusal; end with its code, which `migrate`
    // and `verify` use for the same status.
    if let Err(e) = &result
        && let Some(refusal) = e.downcast_ref::<extenddb_server::CatalogRefusal>()
    {
        std::process::exit(i32::from(refusal.exit_code));
    }
    // `serve` stopped through its shutdown path, which removed the PID file,
    // and has logged why.
    if let Err(e) = &result
        && e.downcast_ref::<extenddb_server::RegistrationLost>()
            .is_some()
    {
        std::process::exit(1);
    }
    result
}

/// The five wordmark rows (figlet slant), without trailing newlines.
const WORDMARK_LINES: [&str; 5] = [
    "    ______     __                 ______  ____",
    "   / ____/  __/ /____  ____  ____/ / __ \\/ __ )",
    "  / __/ | |/_/ __/ _ \\/ __ \\/ __  / / / / __  |",
    " / /____>  </ /_/  __/ / / / /_/ / /_/ / /_/ /",
    "/_____/_/|_|\\__/\\___/_/ /_/\\__,_/_____/_____/",
];
const TAGLINE: &str = "  DynamoDB-compatible: any SDK, CLI, or tool, unchanged.";

/// Whether the banner's destination stream is an interactive terminal that
/// wants color: never for pipes and `docker logs`, and both the NO_COLOR
/// convention (<https://no-color.org>) and `TERM=dumb` opt out.
fn banner_colors_enabled(foreground: bool) -> bool {
    use std::io::IsTerminal;

    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if std::env::var("TERM").is_ok_and(|t| t == "dumb") {
        return false;
    }
    // Match the banner's stream split: stderr in foreground mode, stdout for
    // the daemon parent.
    if foreground {
        std::io::stderr().is_terminal()
    } else {
        std::io::stdout().is_terminal()
    }
}

/// Render the wordmark block: five art rows, a blank line, the tagline, and a
/// closing blank line. With color, the rows fade gold to deep orange
/// (256-color codes, an ANSI level every interactive terminal supports) and
/// the tagline dims; without, the output is byte-identical to the historical
/// plain form.
fn render_wordmark(color: bool) -> String {
    // Gold fading to deep orange, one shade per row.
    const ROW_COLORS: [u8; 5] = [220, 214, 208, 202, 166];

    let mut out = String::new();
    for (i, line) in WORDMARK_LINES.iter().enumerate() {
        if color {
            out.push_str(&format!("\x1b[38;5;{}m{line}\x1b[0m\n", ROW_COLORS[i]));
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out.push('\n');
    if color {
        out.push_str(&format!("\x1b[2m{TAGLINE}\x1b[0m\n"));
    } else {
        out.push_str(TAGLINE);
        out.push('\n');
    }
    out.push('\n');
    out
}

fn render_startup_warnings(warnings: &[extenddb_storage::config::StartupWarning]) -> String {
    let mut out = String::new();
    for warning in warnings {
        out.push_str("\n  WARNING: ");
        out.push_str(&warning.title);
        out.push('\n');
        for detail in &warning.details {
            out.push_str("  ");
            out.push_str(detail);
            out.push('\n');
        }
    }
    out
}

/// True for a bind address that only this host can reach.
fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

/// Print the developer-mode banner lines: the mode notice, the credentials to
/// sign with, a warning block when the server is not bound to loopback, and an
/// advisory when an ambient `AWS_ACCESS_KEY_ID` would not verify.
///
/// Resolving here (not just in `serve()`) fails fast on a misconfigured
/// `EXTENDDB_DEV_*` pair before daemonizing, and gives the invoking user the
/// credentials without digging through logs. The built-in example pair is
/// printed in full: it is public AWS documentation, not a secret. An
/// operator-supplied secret is never printed.
fn print_dev_mode_banner(
    app_config: &config::AppConfig,
    port: u16,
    foreground: bool,
) -> anyhow::Result<()> {
    use extenddb_server::dev_credentials;

    let dev_creds = dev_credentials::resolve()?;
    let lines = dev_mode_banner_lines(
        &config::redact_password(app_config.storage.connection_config()),
        app_config.server.bind_addr.trim(),
        port,
        &dev_creds,
        dev_credentials::ignored_aws_env_key(&dev_creds).as_deref(),
    );
    // Same stream split as the main banner: stdout for the daemon parent,
    // stderr in foreground mode so supervisors capture one stream.
    for msg in lines {
        if foreground {
            eprintln!("{msg}");
        } else {
            println!("{msg}");
        }
    }
    Ok(())
}

/// Build the developer-mode banner. Pure, so the wording is unit tested.
///
/// A non-loopback bind gets a multi-line warning block rather than one log
/// line: it is the dev image's normal state (a container must bind 0.0.0.0
/// for port publishing to work), and whether that is safe depends on the
/// host's `-p` mapping, which the server cannot see. So the block cannot say
/// "you are exposed", only what exposure would mean and how to avoid it.
fn dev_mode_banner_lines(
    storage: &str,
    host: &str,
    port: u16,
    dev_creds: &[extenddb_server::dev_credentials::DevCredential],
    ignored_aws_key: Option<&str>,
) -> Vec<String> {
    use extenddb_server::dev_credentials;

    let loopback = is_loopback_host(host);
    let example_active = dev_credentials::example_pair_active(dev_creds);
    let where_ = if loopback {
        "on loopback".to_owned()
    } else {
        format!("on {host}")
    };
    let mut lines = vec![
        format!(
            "  DEVELOPER MODE: plain HTTP {where_}, authorization open \
             (SigV4 still enforced). Serving storage: {storage}. Not for production.",
        ),
        format!(
            "  Sign requests with: {}",
            dev_credentials::describe(dev_creds)
        ),
    ];
    if !loopback {
        let credential = if example_active {
            "the signing credential above is public, so anyone who can reach this port \
             can read, change and delete every table"
        } else {
            "whoever holds the signing credential above can read, change and delete \
             every table"
        };
        lines.extend([
            String::new(),
            format!("  WARNING: dev mode is listening on {host}:{port}, not loopback."),
            format!("  Requests are plain HTTP and authorization is open: {credential}."),
            format!(
                "  In a container, publish the port on loopback only: \
                 -p 127.0.0.1:{port}:{port} (not -p {port}:{port})."
            ),
            "  Never expose dev mode to a shared or routable network.".to_owned(),
        ]);
        if example_active {
            lines.push(format!(
                "  On a shared host or CI runner, set {}=1 with your own \
                 EXTENDDB_DEV_ACCESS_KEY_ID / EXTENDDB_DEV_SECRET_ACCESS_KEY.",
                dev_credentials::ENV_DISABLE_EXAMPLE,
            ));
        }
        lines.push(String::new());
    }
    if let Some(ignored) = ignored_aws_key {
        lines.push(format!(
            "  Note: AWS_ACCESS_KEY_ID is set but not adopted; requests signed with \
             '{ignored}' will be rejected. Set EXTENDDB_DEV_ACCESS_KEY_ID and \
             EXTENDDB_DEV_SECRET_ACCESS_KEY to seed an additional credential."
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::{ServeArgs, dev_mode_banner_lines, is_loopback_host, render_startup_warnings};
    use clap::Parser;
    use extenddb_server::dev_credentials::{
        DevCredential, ENV_DISABLE_EXAMPLE, EXAMPLE_ACCESS_KEY_ID, EXAMPLE_SECRET_ACCESS_KEY,
    };
    use extenddb_storage::config::StartupWarning;

    fn example() -> DevCredential {
        DevCredential {
            access_key_id: EXAMPLE_ACCESS_KEY_ID.to_owned(),
            secret_access_key: EXAMPLE_SECRET_ACCESS_KEY.to_owned(),
            is_builtin: true,
        }
    }

    fn team() -> DevCredential {
        DevCredential {
            access_key_id: "AKIADEVTEAMKEY000001".to_owned(),
            secret_access_key: "team-secret".to_owned(),
            is_builtin: false,
        }
    }

    #[test]
    fn loopback_hosts_are_recognised() {
        for host in ["127.0.0.1", "127.1.2.3", "::1", "localhost"] {
            assert!(is_loopback_host(host), "{host}");
        }
        for host in ["0.0.0.0", "::", "192.168.1.10", "example.com", ""] {
            assert!(!is_loopback_host(host), "{host}");
        }
    }

    #[test]
    fn loopback_dev_banner_has_no_exposure_warning() {
        let text =
            dev_mode_banner_lines(":memory:", "127.0.0.1", 18080, &[example()], None).join("\n");
        assert!(text.contains("plain HTTP on loopback"), "{text}");
        assert!(!text.contains("WARNING"), "{text}");
    }

    #[test]
    fn non_loopback_dev_banner_warns_with_the_publish_command() {
        let text =
            dev_mode_banner_lines(":memory:", "0.0.0.0", 18080, &[example()], None).join("\n");
        // The banner used to say "on loopback" whatever the bind was.
        assert!(!text.contains("plain HTTP on loopback"), "{text}");
        assert!(text.contains("plain HTTP on 0.0.0.0"), "{text}");
        assert!(
            text.contains("WARNING: dev mode is listening on 0.0.0.0:18080"),
            "{text}"
        );
        assert!(text.contains("credential above is public"), "{text}");
        assert!(text.contains("-p 127.0.0.1:18080:18080"), "{text}");
        assert!(text.contains(&format!("{ENV_DISABLE_EXAMPLE}=1")), "{text}");
    }

    #[test]
    fn non_loopback_dev_banner_without_the_example_pair_drops_the_public_wording() {
        let text = dev_mode_banner_lines(":memory:", "0.0.0.0", 9000, &[team()], None).join("\n");
        assert!(
            text.contains("WARNING: dev mode is listening on 0.0.0.0:9000"),
            "{text}"
        );
        assert!(text.contains("-p 127.0.0.1:9000:9000"), "{text}");
        assert!(!text.contains("is public"), "{text}");
        assert!(!text.contains(EXAMPLE_ACCESS_KEY_ID), "{text}");
        // The switch is already set; do not tell the operator to set it.
        assert!(
            !text.contains(&format!("{ENV_DISABLE_EXAMPLE}=1")),
            "{text}"
        );
        assert!(!text.contains("team-secret"), "{text}");
    }

    #[test]
    fn dev_banner_keeps_the_ignored_aws_key_note() {
        let text = dev_mode_banner_lines(
            ":memory:",
            "127.0.0.1",
            18080,
            &[example()],
            Some("ASIAFOREIGNSESSION01"),
        )
        .join("\n");
        assert!(
            text.contains("'ASIAFOREIGNSESSION01' will be rejected"),
            "{text}"
        );
    }

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

    #[test]
    fn startup_warning_banner_is_prominent_and_actionable() {
        let rendered = render_startup_warnings(&[StartupWarning {
            title: "transaction_read_concern = \"majority\" (default: \"snapshot\").".to_owned(),
            details: vec![
                "Transactions run below DynamoDB isolation.".to_owned(),
                "Writes remain atomic and durable.".to_owned(),
                "Remove the setting to restore full DynamoDB transaction semantics.".to_owned(),
            ],
        }]);

        assert!(rendered.starts_with("\n  WARNING: transaction_read_concern"));
        assert!(rendered.contains("below DynamoDB isolation"));
        assert!(rendered.contains("Writes remain atomic and durable"));
        assert!(rendered.contains("restore full DynamoDB transaction semantics"));
    }
}
