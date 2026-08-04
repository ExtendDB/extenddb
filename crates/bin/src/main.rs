// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! extenddb — the PostgreSQL-backed ExtendDB server binary.
//!
//! This is the reference thin bin for the per-backend packaging model: it
//! installs exactly one backend and hands off to the shared `extenddb-app` CLI.
//! A third-party backend author copies this file, swaps the `backend()` call for
//! their crate, and ships their own `extenddb-<backend>` image — with no edits to
//! any ExtendDB core crate.

fn main() -> anyhow::Result<()> {
    // Install the compiled-in backend before dispatch. The compiler checks this
    // call; there is no link-time auto-registration and no name to resolve, so a
    // missing or mistyped backend cannot become a runtime error.
    extenddb_storage::set_backend(extenddb_storage_postgres::backend())?;

    extenddb_app::run(extenddb_app::BuildInfo {
        // Read from the bin crate so the reported version is the deployed
        // artifact's, not a library crate's.
        version: env!("CARGO_PKG_VERSION"),
        git_hash: env!("EXTENDDB_GIT_HASH"),
        build_time: env!("EXTENDDB_BUILD_TIME"),
    })
}
