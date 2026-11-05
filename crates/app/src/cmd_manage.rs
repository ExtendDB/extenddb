// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb manage` — thin CLI client for the management API.
//!
//! All subcommands dispatch HTTP calls to the management API endpoints
//! on the running extenddb server. Requires admin or IAM user credentials.
//!
//! Types are in `manage_types.rs`; HTTP transport and dispatch are in
//! `manage_http.rs`.

use crate::manage_http::{basic_auth, dispatch, resolve_endpoint};
pub use crate::manage_types::ManageArgs;

/// Printed on stderr when the password comes from `--password`.
const PASSWORD_FLAG_WARNING: &str = "Warning: --password is visible in process listings and \
                                     shell history. Omit it and set EXTENDDB_PASSWORD instead.";

/// Choose the password from the `--password` value and the `EXTENDDB_PASSWORD`
/// value. The flag takes precedence when both are set. The second element is
/// the warning to print on stderr: `Some` when the password came from the
/// flag, `None` when it came from the variable.
fn choose_password(
    flag: Option<String>,
    env: Option<String>,
) -> anyhow::Result<(String, Option<&'static str>)> {
    match (flag, env) {
        (Some(password), _) => Ok((password, Some(PASSWORD_FLAG_WARNING))),
        (None, Some(password)) => Ok((password, None)),
        (None, None) => Err(anyhow::anyhow!(
            "No password provided. Use --password VALUE or set EXTENDDB_PASSWORD."
        )),
    }
}

/// Resolve the password from the `--password` flag or the `EXTENDDB_PASSWORD`
/// env var, and warn on stderr when it came from the flag.
///
/// Never reads from stdin. Errors if no password source is available.
fn resolve_password(cli_password: Option<String>) -> anyhow::Result<String> {
    let (password, warning) =
        choose_password(cli_password, std::env::var("EXTENDDB_PASSWORD").ok())?;
    if let Some(warning) = warning {
        eprintln!("{warning}");
    }
    Ok(password)
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
    use super::{PASSWORD_FLAG_WARNING, choose_password};

    fn value(s: &str) -> Option<String> {
        Some(s.to_owned())
    }

    #[test]
    fn a_password_from_the_flag_is_used_with_the_warning() {
        let (password, warning) = choose_password(value("from-flag"), None).unwrap();
        assert_eq!(password, "from-flag");
        assert_eq!(warning, Some(PASSWORD_FLAG_WARNING));
    }

    #[test]
    fn a_password_from_the_variable_is_used_without_a_warning() {
        let (password, warning) = choose_password(None, value("from-env")).unwrap();
        assert_eq!(password, "from-env");
        assert_eq!(warning, None);
    }

    #[test]
    fn the_flag_takes_precedence_over_the_variable_and_warns() {
        let (password, warning) = choose_password(value("from-flag"), value("from-env")).unwrap();
        assert_eq!(password, "from-flag");
        assert_eq!(warning, Some(PASSWORD_FLAG_WARNING));
    }

    #[test]
    fn no_flag_and_no_variable_is_an_error() {
        let err = choose_password(None, None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "No password provided. Use --password VALUE or set EXTENDDB_PASSWORD."
        );
    }
}
