// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb manage` — thin CLI client for the management API.
//!
//! All subcommands dispatch HTTP calls to the management API endpoints
//! on the running extenddb server. Requires admin or IAM user credentials.
//!
//! Types are in `manage_types.rs`; HTTP transport and dispatch are in
//! `manage_http.rs`.

use crate::manage_http::{basic_auth, dispatch, resolve_endpoint, resolve_password};
pub use crate::manage_types::ManageArgs;

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
