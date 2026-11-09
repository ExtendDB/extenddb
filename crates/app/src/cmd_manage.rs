// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb manage` — thin CLI client for the management API.
//!
//! All subcommands dispatch HTTP calls to the management API endpoints
//! on the running extenddb server. Requires admin or IAM user credentials.
//!
//! Types are in `manage_types.rs`; HTTP transport and dispatch are in
//! `manage_http.rs`.

use std::io::Write;

use crate::manage_http::{basic_auth, dispatch, resolve_endpoint};
pub use crate::manage_types::ManageArgs;

/// Printed on stderr when the password comes from `--password`.
const PASSWORD_FLAG_WARNING: &str = "Warning: --password is visible in process listings and \
                                     shell history. Omit it and set EXTENDDB_PASSWORD instead.";

/// Choose the password from the `--password` value and the `EXTENDDB_PASSWORD`
/// value. The flag takes precedence when both are set.
fn choose_password(flag: Option<String>, env: Option<String>) -> anyhow::Result<String> {
    flag.or(env).ok_or_else(|| {
        anyhow::anyhow!("No password provided. Use --password VALUE or set EXTENDDB_PASSWORD.")
    })
}

/// The warning to print on stderr: `Some` when `--password` was given, `None`
/// when it was not. The flag takes precedence, so `Some` means the password
/// came from the flag. The result depends only on `flag_given`, so the printed
/// text never holds the password.
///
/// CodeQL's `rust/cleartext-logging` query traces a printed value back to the
/// password when both come from one function result. So the warning comes from
/// this function, which takes only a `bool`, and not from `choose_password`.
fn flag_warning(flag_given: bool) -> Option<&'static str> {
    flag_given.then_some(PASSWORD_FLAG_WARNING)
}

/// Resolve the password from the `--password` value or the `EXTENDDB_PASSWORD`
/// value, and write the warning to `warnings` when it came from the flag.
///
/// Never reads from stdin. Errors if no password source is available. A failed
/// write of the warning does not stop the command.
fn resolve_password_with(
    cli_password: Option<String>,
    env_password: Option<String>,
    warnings: &mut impl Write,
) -> anyhow::Result<String> {
    let warning = flag_warning(cli_password.is_some());
    let password = choose_password(cli_password, env_password)?;
    if let Some(warning) = warning {
        let _ = writeln!(warnings, "{warning}");
    }
    Ok(password)
}

/// [`resolve_password_with`] for the command line: reads `EXTENDDB_PASSWORD`
/// and writes the warning to stderr.
fn resolve_password(cli_password: Option<String>) -> anyhow::Result<String> {
    resolve_password_with(
        cli_password,
        std::env::var("EXTENDDB_PASSWORD").ok(),
        &mut std::io::stderr(),
    )
}

// Caller (`run_interactive`) requires a Future; removing `async` would need a
// manual `impl Future` wrapper for no benefit.
#[allow(clippy::unused_async)]
pub async fn run(args: ManageArgs) -> anyhow::Result<()> {
    let (host_port, use_tls, cert_path) = resolve_endpoint(args.endpoint.as_deref(), &args.config)?;
    let password = resolve_password(args.password)?;
    let auth = basic_auth(&args.user, &password);

    let (status, body) = dispatch(
        &host_port,
        &auth,
        use_tls,
        cert_path.as_deref(),
        &args.user,
        args.command,
    )?;

    if (200..300).contains(&status) {
        if !body.is_empty() {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                println!("{}", serde_json::to_string_pretty(&json)?);
            } else {
                println!("{body}");
            }
        }
        Ok(())
    } else {
        anyhow::bail!("HTTP {status}: {body}")
    }
}

#[cfg(test)]
mod tests {
    use super::{PASSWORD_FLAG_WARNING, resolve_password_with};

    fn value(s: &str) -> Option<String> {
        Some(s.to_owned())
    }

    /// Runs `resolve_password_with` and returns its result with what it wrote.
    fn resolve(flag: Option<String>, env: Option<String>) -> (anyhow::Result<String>, String) {
        let mut warnings = Vec::new();
        let result = resolve_password_with(flag, env, &mut warnings);
        (result, String::from_utf8(warnings).unwrap())
    }

    #[test]
    fn a_password_from_the_flag_is_used_with_the_warning() {
        let (password, warnings) = resolve(value("from-flag"), None);
        assert_eq!(password.unwrap(), "from-flag");
        assert_eq!(warnings, format!("{PASSWORD_FLAG_WARNING}\n"));
        assert!(!warnings.contains("from-flag"));
    }

    #[test]
    fn a_password_from_the_variable_is_used_with_no_warning() {
        let (password, warnings) = resolve(None, value("from-env"));
        assert_eq!(password.unwrap(), "from-env");
        assert_eq!(warnings, "");
    }

    #[test]
    fn the_flag_takes_precedence_over_the_variable_and_warns() {
        let (password, warnings) = resolve(value("from-flag"), value("from-env"));
        assert_eq!(password.unwrap(), "from-flag");
        assert_eq!(warnings, format!("{PASSWORD_FLAG_WARNING}\n"));
    }

    #[test]
    fn no_flag_and_no_variable_is_an_error_with_no_warning() {
        let (password, warnings) = resolve(None, None);
        assert_eq!(
            password.unwrap_err().to_string(),
            "No password provided. Use --password VALUE or set EXTENDDB_PASSWORD."
        );
        assert_eq!(warnings, "");
    }
}
