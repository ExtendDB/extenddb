// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb migrate` — apply catalog schema migrations (REQ-CAT-014).
//!
//! The command's arguments and the error helpers the other commands share.
//! Every backend migrates through `cmd_migrate_runner`, whose exit statuses
//! are 0, 1, 3 (work pending and `--yes` not given), 4, 5 and 6 (see that
//! module).

use clap::Args;

use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::OpError;

/// The config file `migrate` and `verify` read when `--config` is not given.
pub(crate) const DEFAULT_CONFIG: &str = "extenddb.toml";

/// Seconds `--yes` and `--forget` wait for the migration lock by default.
pub(crate) const DEFAULT_LOCK_TIMEOUT_SECS: u64 = 60;

#[derive(Args)]
pub struct MigrateArgs {
    /// Path to configuration file (default: extenddb.toml; a dev-mode build
    /// without one reads the EXTENDDB__ environment variables, as serve does)
    #[arg(short, long)]
    pub(crate) config: Option<String>,

    /// `PostgreSQL` admin role for the pgvector extension attempt; schema
    /// work runs as the application role in the config file
    #[arg(long)]
    pub(crate) pg_user: Option<String>,

    /// `PostgreSQL` admin password.
    /// Prefer `EXTENDDB_PG_PASSWORD` to keep the value out of process arguments.
    #[arg(long)]
    pub(crate) pg_pass: Option<String>,

    /// `PostgreSQL` data database to migrate (PostgreSQL only; ignored on
    /// other backends). Normally not needed: the name `init` recorded in the
    /// catalog's settings is used, else the catalog database name without its
    /// `_catalog` suffix. Pass it for a catalog that has no recorded name and
    /// a data database named otherwise.
    #[arg(long)]
    pub(crate) data_db: Option<String>,

    /// Cassandra role to connect as instead of the role the config file
    /// names (for example the role `init` used)
    #[arg(long)]
    pub(crate) cassandra_user: Option<String>,

    /// Password of the `--cassandra-user` role.
    /// Prefer `EXTENDDB_CASSANDRA_PASSWORD` to keep the value out of process arguments.
    #[arg(long)]
    pub(crate) cassandra_pass: Option<String>,

    /// Apply the pending migrations. Without it, migrate only reports what is
    /// pending and exits 3 if there is any (0 if everything is up to date)
    #[arg(long)]
    pub(crate) yes: bool,

    /// Print every migration this binary carries, with its kind on this
    /// backend, and exit without opening the catalog
    #[arg(long, conflicts_with_all = ["yes", "forget", "reconverge"])]
    pub(crate) list: bool,

    /// With --yes: run every migration again, to repair a catalog a release
    /// before 1.0 wrote to after it was migrated
    #[arg(long, requires = "yes", conflicts_with = "forget")]
    pub(crate) reconverge: bool,

    /// Delete the ledger rows of an unreleased migration whose body changed on
    /// a development database, so the next `migrate --yes` runs it again
    #[arg(long, value_name = "ID", conflicts_with = "yes")]
    pub(crate) forget: Option<u32>,

    /// Seconds --yes and --forget wait for a server or another migration to
    /// release the catalog before exiting 6 (default 60, at most 86400)
    #[arg(long, value_name = "SECONDS", value_parser = clap::value_parser!(u64).range(0..=86_400))]
    pub(crate) lock_timeout: Option<u64>,
}

/// The message of a bootstrapper error, without the variant name that `{e:?}`
/// adds (which printed `Internal("Internal(\"Cannot connect ...\")")`).
pub(crate) fn op_err(e: OpError) -> anyhow::Error {
    match e {
        OpError::Validation(m)
        | OpError::AlreadyExists(m)
        | OpError::NotFound(m)
        | OpError::HasDependents(m)
        | OpError::Internal(m)
        | OpError::Database { message: m, .. } => anyhow::anyhow!(m),
    }
}

/// The message of a storage error; `Internal` carries a complete sentence
/// already, so its "Internal error: " prefix is dropped.
pub(crate) fn storage_err(e: StorageError) -> anyhow::Error {
    match e {
        StorageError::Internal(m) => anyhow::anyhow!(m),
        other => anyhow::anyhow!(other.to_string()),
    }
}

/// Run `extenddb migrate`; returns the process exit status (see
/// `cmd_migrate_runner`).
pub async fn run(args: MigrateArgs, release: &str) -> anyhow::Result<u8> {
    let migrations = extenddb_storage::migrations::installed()
        .ok_or_else(|| anyhow::anyhow!("no storage backend is installed"))?;
    crate::cmd_migrate_runner::run(&args, migrations, release).await
}
