// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! extenddb — the PostgreSQL-backed ExtendDB server binary.
//!
//! This is the reference thin bin for the per-backend packaging model: it wires
//! exactly one backend into the registry and hands off to the shared
//! `extenddb-app` CLI. A third-party backend author copies this file, swaps the
//! `register` call for their crate, and ships their own `extenddb-<backend>`
//! image — with no edits to any ExtendDB core crate.

fn main() -> anyhow::Result<()> {
    // Wire the compiled-in backend into the process registry before dispatch.
    // The compiler checks this call; there is no link-time auto-registration.
    let mut registry = extenddb_storage::BackendRegistry::new();
    extenddb_storage_postgres::register(&mut registry);
    extenddb_storage::set_registry(registry)?;

    extenddb_app::run(extenddb_app::BuildInfo {
        // Read from the bin crate so the reported version is the deployed
        // artifact's, not a library crate's.
        version: env!("CARGO_PKG_VERSION"),
        git_hash: env!("EXTENDDB_GIT_HASH"),
        build_time: env!("EXTENDDB_BUILD_TIME"),
    })
}
