// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb backup`: the operator's commands over the configured backup
//! store.
//!
//! `backup list` prints the catalog's rows for the configured store's kind
//! and root, every status and every store id the rows carry, with `--failed`
//! narrowing to the FAILED tombstones the reconciler keeps for the operator;
//! each line carries the `store_root` and the `store_id` so an operator sees
//! which root and which id a row names. It is a read: it opens the catalog
//! beside a running server (on SQLite without the serve lock and without the
//! startup recovery a server runs), reads the root's identity file without
//! creating one, and runs on an unhealthy store, printing the store's
//! problem in its header, since an operator whose store is unhealthy is the
//! one who most wants to see which rows name the root.
//!
//! `backup purge --confirm` deletes every backup of the configured store: the
//! prefix of each row (`<account>/<table>/<backup id>/`, derived from the
//! ARN, never a whole account prefix, so a prefix with no row survives) and
//! the row itself, outright, leaving no tombstone. It leaves
//! `extenddb-store.json`, which is under no account prefix, so the root
//! keeps its identity, and the `exports/` component. It needs the store
//! healthy, its identity known: a purge over an unhealthy store would act on
//! rows it cannot trust. Before it deletes it takes the catalog-level
//! maintenance claim, which it heartbeats on a timer for the claim's whole
//! life, at the period the jobs heartbeat their rows at
//! (`extenddb_storage::backup_catalog::JOB_HEARTBEAT_INTERVAL`, so the
//! `stale_job_after_secs` floor that keeps a live job from being declared
//! dead keeps this claim held too), so every reader of the claim sees it
//! held from the take to the release, through a deletion phase of any
//! length; a heartbeat that finds the claim lost, or fails, stops the
//! deletion loop before its next row. It then waits, up to
//! `2 * stale_job_after_secs`, until no CREATING or FAILED backup row of the
//! store has a live owner (a heartbeat within `stale_job_after_secs` on the
//! catalog's clock, as the backend's `list_backup_rows_older_than` reads it; a
//! FAILED row's heartbeat may still be refreshed by a job that has not yet
//! seen its claim lost, and its prefix must not be deleted under a manifest
//! it may still write) and no restore job is RUNNING; it refuses and
//! releases the claim if they have not gone by then, printing the wait. A
//! CREATING row the wait saw no live owner for is deleted too: the row
//! first (`remove_dead_creating_backup_row`, on the catalog's clock, which
//! leaves a row whose heartbeat is young after all, and its objects with
//! it), then its prefix. A DELETING row is not
//! waited on: nothing heartbeats one, the reconciler finishes any it finds,
//! and the deletion is the same idempotent sequence purge runs on every row
//! (two deleters of one prefix do not fail each other, and the row's moves
//! are conditional), so purge finishes it itself; for the same reason a row
//! a DeleteBackup moved to DELETING or DELETED, or removed, between the
//! command's read and its turn is finished and counted as deleted, not left
//! (`extenddb_engine::maintenance::purge_row` states each state's step).
//! Every backup and restore
//! job checks the claim before it starts and refuses while it is held
//! (`LimitExceededException`; the engine's backup module states where in
//! the job's order the check runs). The wait ends early, with the claim
//! released and nothing deleted, when a heartbeat finds the claim lost, when
//! the rows cannot be read, or on `SIGINT` (Ctrl-C) or `SIGTERM`; the same
//! signal during the deletion ends the loop before its next row, never
//! inside one, so a stopped command leaves no half-deleted row and no claim
//! for the next command or server to wait out. Without `--confirm` the
//! command is a
//! read like `list` (no serve lock, nothing created): it prints what it
//! would delete, changes nothing and exits 0, with no store configured as
//! with one. Each deletion is logged under
//! the `extenddb::audit` target with the account and the ARN.
//!
//! On SQLite purge opens the database the way `serve` does, which takes the
//! serve lock: it refuses while a server runs on the file, and a server
//! refuses to start while it runs. Under that lock every job owner is dead
//! (one process serves the file, and this is it), and no reconciler runs
//! while the command holds the file, so before the wait the command runs
//! the reconciler's duties itself with a zero stale window, as the holder
//! of the maintenance claim (the cycle takes its held branch and does not
//! read the claim back through that window): a restore the stopped server
//! left RUNNING is failed and its target removed, a store-backed one too,
//! which the held branch fails rather than resumes (a record with no
//! recorded progress over a target that holds items is failed
//! `target_not_empty` and its target left for `remove-target`), a
//! target a FAILED restore left CREATING is removed, an in-catalog CREATING
//! backup it left is removed, and a DELETING row is finished; the wait and
//! the CREATING-row removal then also use a zero window, so a CREATING or
//! FAILED row whose heartbeat is young (the stopped server's job wrote it)
//! is neither waited on nor left, and the wait has nothing to wait for. The
//! server's own startup under the same lock clears a maintenance claim a
//! stopped command left, for the same reason, and the command's own take
//! of the claim takes over a row a killed command left. On the other
//! backends purge runs beside live servers; the maintenance claim is the
//! exclusion.
//!
//! Every command opens the catalog through the backend's factory, under its own
//! name (`extenddb backup list`, `extenddb backup purge`, `extenddb backup
//! remove-target`, `extenddb backup sync`), and none runs the startup recovery
//! a server runs (`ServerComponentsOptions::startup_recovery` off): that work
//! belongs to the servers the command runs beside, and a command that ran it
//! could exit with it half done. On PostgreSQL, MongoDB and Cassandra the
//! command holds a serving registration for its run, as a server does, so a
//! migration waits for it, and the registration carries the command's name,
//! which the migration's wait prints (PostgreSQL's session `application_name`,
//! MongoDB's `extenddb_servers` document, Cassandra's `extenddb_servers` row).
//! The command releases the registration when it ends, whatever the outcome, so
//! a migration does not wait for a row or document to expire. A catalog the
//! factory refuses (a migration running, the catalog at another migration than
//! this binary's) ends the command with the message and the exit code `serve`
//! gives for it: 6 while a migration runs, 3 for a catalog behind this binary.
//! A registration lost during the command does not stop it: no supervisor runs
//! in a command, so a command that outlasts a lost registration runs beside a
//! migration that started after the loss. The signal listener is up before the
//! open in every subcommand, so a Ctrl-C during the open or the identity read
//! ends the command once the open returns, with the registration released.
//!
//! `extenddb destroy` removes the databases and leaves the store untouched:
//! the store is the copy that survives destroy, and this command is the one
//! that deletes it.
//!
//! `backup remove-target --account <id> <table>` removes a restore target
//! DeleteTable refuses (a CREATING table) with its restore record, under the
//! maintenance claim like `purge`: a FAILED record's target whatever the
//! reason (`target_not_empty` included, the one state the reconciler leaves
//! for this command: on MongoDB and Cassandra a crash before the first
//! progress record, the whole first data file on MongoDB and the first
//! 10,000 items or 32 MiB on Cassandra, ends in it), a RUNNING record's
//! target whose owner is dead (no heartbeat within `stale_job_after_secs`;
//! on SQLite under the serve lock every owner is dead), and a legacy
//! record's target the same way. It refuses a RUNNING record whose owner is
//! live, a DONE record and a table no record names ("not a restore target";
//! an ACTIVE table is one or the other). It needs no store: it opens the
//! catalog as `serve` does (on SQLite under the serve lock) whether or not
//! `[backup.store]` is configured, prints the claim it took and the one line
//! of what it removed (`extenddb_engine::maintenance::remove_target` states
//! each step), and logs the removal under `extenddb::audit` with the
//! account, the table and the backup's ARN.
//!
//! `backup sync` rebuilds the catalog from the configured store: it gives an
//! AVAILABLE row to every backup prefix of the store that has a verified
//! manifest and no row, and reports the prefixes it skips with the reason
//! and the AVAILABLE rows whose manifest is gone (`extenddb_engine::sync`
//! states what it adopts and never adopts). It opens the catalog as `serve`
//! does (on SQLite under the serve lock) and needs the store healthy: an
//! unhealthy store refuses it, naming the problem, with no row changed. On
//! PostgreSQL, MongoDB and Cassandra every sync takes the maintenance claim
//! and heartbeats it, so no purge removes a row and deletes its bytes
//! between this sync's read and its insert (the claim re-checked with a
//! beat right before each insert, `--prune-missing`'s removal and
//! `--adopt-store`'s rewrite: `extenddb_engine::sync`'s module doc gives
//! the bound that remains); plain sync waits for no job (its
//! insert-if-absent orders it against a CreateBackup), and while it holds
//! the claim the jobs refuse, as during purge. `--prune-missing` and
//! `--adopt-store` also wait for running jobs as purge does, on SQLite
//! after the dead-owner resolutions purge runs, which fail a store-backed
//! restore a stopped server left RUNNING and remove its target. Plain sync
//! on SQLite runs no dead-owner resolution: the serve lock already keeps
//! every server and every other command off the file, so nothing changes
//! between its row read and its insert, and the rows a stopped server left
//! (a CREATING or DELETING backup, a RUNNING restore) are rows the sync
//! skips and leaves for the next server start, which resolves the backups
//! and resumes the restore. A first `SIGINT` or `SIGTERM` stops any
//! sync before its next prefix (or the wait at once), releases the claim
//! and exits 1. `--prune-missing` removes each AVAILABLE row whose manifest
//! is gone outright and deletes no object, and reports a row a restore is
//! reading or another writer moved. `--adopt-store` is the way a durable
//! catalog takes a root whose identity file names another catalog (a server
//! on an in-memory database adopts, at startup, a root another in-memory
//! catalog wrote): it rewrites the file's owner
//! (`extenddb_engine::store_identity::adopt`), logged under
//! `extenddb::audit`, syncs, then reads the file once more and fails naming
//! the owner if another catalog adopted the root meanwhile; on a root this
//! catalog already owns it says so and syncs. Every server of the catalog
//! the file names is to be stopped before the adoption: the previous owner
//! acts on the root until a later check that can read its catalog finds
//! the store owned by another catalog, except where its deletion fence
//! stops it first (a re-read of the file before each deletion, before
//! DeleteBackup's and purge's row change and before a CreateBackup's first
//! put, which leaves a gap of one read). The refusal and the output say so. Sync
//! exits 0 once it ran, skipped
//! prefixes included, and 1 on a refusal, a stop, a listing that failed, a
//! row `--prune-missing` left for a later run (a store or catalog call
//! failed, or a restore is reading the backup), or a prefix skipped for a
//! store or catalog error (the run is incomplete; a second run adopts it).
//! A row `--prune-missing` leaves because its manifest is back or another
//! writer moved it is reported, and the exit stays 0.
//!
//! Every `backup` command refuses an in-memory SQLite database (a
//! `[storage.sqlite] path` naming no file) before it opens the catalog: it
//! would open a new, empty catalog that ends with the command. A server on
//! such a database syncs with its store itself at every start.

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use extenddb_config as config;
use extenddb_engine::maintenance::{
    Interrupt, RemoveTarget, Stop, Wait, WaitOutcome, purge_rows, remove_target, wait_for_quiet,
};
use extenddb_engine::{BackupRuntime, ConfiguredStore, HeldClaim, StoreHealth, StoreProblem};
use extenddb_storage::StorageEngine;
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, JOB_HEARTBEAT_INTERVAL, JobStatus, StoreIdentity,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::hooks::ServingRegistrar;
use futures::TryStreamExt;

#[derive(Args)]
#[allow(clippy::doc_markdown)] // Clap help text, not rustdoc
pub struct BackupArgs {
    /// Path to configuration file (accepted before or after the subcommand)
    #[arg(short, long, default_value = "extenddb.toml", global = true)]
    config: String,

    #[command(subcommand)]
    command: BackupCommand,
}

#[derive(Subcommand)]
#[allow(clippy::doc_markdown)] // Clap help text, not rustdoc
enum BackupCommand {
    /// List the catalog's backup rows for the configured backup store. A read that runs
    /// beside a server, on an unhealthy store too.
    List {
        /// Only the FAILED rows (backups whose creation failed, kept for the operator)
        #[arg(long)]
        failed: bool,
    },
    /// Delete every backup of the configured backup store: each backup's objects and its
    /// catalog row. Leaves extenddb-store.json, exports/ and any prefix with no row.
    /// Takes the maintenance claim first and waits for running jobs to finish.
    Purge {
        /// Confirm the deletion (required; without it the command prints what it would delete
        /// and exits 0)
        #[arg(long)]
        confirm: bool,
    },
    /// Remove a restore target DeleteTable refuses (a CREATING table) and its restore record:
    /// the target of a FAILED restore (target_not_empty included) or of a RUNNING restore whose
    /// owner is dead. Takes the maintenance claim first. Refuses a RUNNING restore with a live
    /// owner, a DONE restore and a table no restore record names.
    RemoveTarget {
        /// The account the table belongs to
        #[arg(long)]
        account: String,
        /// The table's name
        table: String,
    },
    /// Give a catalog row to every backup of the configured backup store that has none: each
    /// prefix with a verified manifest (its sidecar matching, every data file present at its
    /// size, its ARN naming the prefix's account, table and backup id) gets an AVAILABLE row.
    /// Reports the prefixes it skips and the AVAILABLE rows whose manifest is gone. Refuses on
    /// an unhealthy store. Beside servers (PostgreSQL, MongoDB, Cassandra) it takes the
    /// maintenance claim; on SQLite it needs the server stopped.
    Sync {
        /// Make this catalog the owner of a store root whose extenddb-store.json names another
        /// catalog, then sync. Stop every server of the catalog the file names first. Takes the
        /// maintenance claim first and waits for running jobs.
        #[arg(long)]
        adopt_store: bool,
        /// Also remove the AVAILABLE rows whose manifest is gone from the store. Each row is
        /// removed outright and no object is deleted (the bytes may be on another host's root
        /// of the same path). Takes the maintenance claim first and waits for running jobs.
        #[arg(long)]
        prune_missing: bool,
    },
}

/// How often the waiting purge re-reads the rows.
const WAIT_POLL: Duration = Duration::from_secs(2);

/// The commands' names, as the catalog's other sessions see them: on
/// PostgreSQL, MongoDB and Cassandra every command registers as the
/// catalog's server does, so a migration waits for it, and the registration
/// carries this name, which the migration prints while it waits.
const LIST: &str = "extenddb backup list";
const PURGE: &str = "extenddb backup purge";
const REMOVE_TARGET: &str = "extenddb backup remove-target";
const SYNC: &str = "extenddb backup sync";

pub async fn run(args: BackupArgs, release: &'static str) -> anyhow::Result<()> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found; use --config <path> to specify its location.",
            args.config,
        );
    }
    let app_config = config::load(&args.config)?;
    // An in-memory database is a new catalog for each process that opens
    // it: a command would read an empty catalog, and a writing one would
    // record its work in a catalog that ends with it (an adopted root
    // owned by a catalog nothing can open again). A location that does not
    // parse is not in memory: its own error is printed, as `serve` does.
    if app_config.storage.backend == "sqlite" {
        match app_config.storage.database_location() {
            Ok(Some(_)) => {}
            Ok(None) => anyhow::bail!(
                "the SQLite database is in memory ([storage.sqlite] path names no file), so \
                 this command would open a new, empty catalog that ends with the command; a \
                 server on an in-memory database syncs its catalog with the configured backup \
                 store itself at every start"
            ),
            Err(e) => anyhow::bail!("{e}"),
        }
    }
    // The signal listener is up before the catalog is opened, in every
    // subcommand: a signal during the open or the identity read is acted on
    // once the open returns, with the serving registration released, where
    // a listener started after the open would leave the registration for
    // `migrate --yes` to wait out. During the purge the same handle ends the
    // wait at once and the deletion loop before its next row.
    let interrupt = listen_for_signals();
    if let BackupCommand::RemoveTarget { account, table } = &args.command {
        // The catalog as `serve` opens it; the store is not needed, so it is
        // described and never checked, and the default root and an identity
        // file are not created.
        let (catalog, _) = open_catalog(
            &app_config,
            &args.config,
            release,
            REMOVE_TARGET,
            Mode::WriteCatalogOnly,
        )
        .await?;
        if let Some(signal) = interrupt.signalled() {
            catalog.close().await;
            anyhow::bail!("{signal} received while opening the catalog; nothing was changed");
        }
        let done = remove_restore_target(&catalog, &app_config, account, table).await;
        catalog.close().await;
        return done;
    }
    let (command, mode) = match args.command {
        BackupCommand::List { .. } => (LIST, Mode::Read),
        BackupCommand::Purge { confirm: false } => (PURGE, Mode::Read),
        BackupCommand::Purge { confirm: true } => (PURGE, Mode::Write),
        BackupCommand::Sync { .. } => (SYNC, Mode::Sync),
        BackupCommand::RemoveTarget { .. } => unreachable!("handled above"),
    };
    let opening = open(&app_config, &args.config, release, command, mode).await?;
    if let Some(signal) = interrupt.signalled() {
        if let Opening::Store(opened) = opening {
            opened.close().await;
        }
        anyhow::bail!("{signal} received while opening the catalog; nothing was changed");
    }
    match (args.command, opening) {
        (BackupCommand::List { failed }, Opening::Store(opened)) => {
            let done = list(&opened, failed).await;
            opened.close().await;
            done
        }
        (BackupCommand::List { .. }, Opening::NoStore(reason)) => {
            println!("=== backups in the configured store: none ({reason}) ===");
            println!("  (none)");
            Ok(())
        }
        // The dry run is a read, as `list` is: no serve lock, nothing
        // created, the plan from the rows of the root; with no store, the
        // plan is empty and says why, as `list` does.
        (BackupCommand::Purge { confirm: false }, Opening::Store(opened)) => {
            let done = dry_run(&opened).await;
            opened.close().await;
            done
        }
        (BackupCommand::Purge { confirm: false }, Opening::NoStore(reason)) => {
            println!("=== extenddb backup purge ===");
            println!("Store:  none ({reason})");
            println!();
            println!("--- Backups to delete (each backup's objects and its catalog row):");
            println!("  (none)");
            println!();
            println!("Dry run: nothing was deleted.");
            Ok(())
        }
        (BackupCommand::Purge { confirm: true }, Opening::Store(opened)) => {
            let done = purge(&opened, &app_config, interrupt).await;
            opened.close().await;
            done
        }
        (BackupCommand::Purge { confirm: true }, Opening::NoStore(reason)) => {
            anyhow::bail!("{reason}")
        }
        (BackupCommand::RemoveTarget { .. }, _) => unreachable!("handled above"),
        (
            BackupCommand::Sync {
                adopt_store,
                prune_missing,
            },
            Opening::Store(opened),
        ) => {
            let done = sync(&opened, &app_config, adopt_store, prune_missing, interrupt).await;
            opened.close().await;
            done
        }
        (BackupCommand::Sync { .. }, Opening::NoStore(reason)) => anyhow::bail!("{reason}"),
    }
}

/// What `open` found: the configured store, or why there is none.
enum Opening {
    Store(Opened),
    NoStore(String),
}

/// How a command opens the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// A read beside a running server: no serve lock, no startup recovery,
    /// the identity file read and never written.
    Read,
    /// A write: on SQLite under the serve lock, as `serve` opens it, but
    /// without the startup recovery a server runs (on PostgreSQL the vector
    /// index rebuild, the control-plane recovery and the GSI queue workers,
    /// on MongoDB the key-repair pass, on Cassandra the control-plane
    /// transitions), which belongs to the servers the command runs beside
    /// and which the command must not exit with half done; the store's
    /// check may write the identity file of an empty root.
    Write,
    /// As `Write`, for `backup sync`: a defaulted root another catalog owns
    /// is configured, unhealthy, rather than left unconfigured with the
    /// warning (`extenddb_server::backup_store::Caller::SyncCommand`).
    Sync,
    /// As `serve` opens it, for a command that needs no store
    /// (`remove-target`): the store is described as `Read` leaves it, so
    /// neither the default root nor an identity file is created.
    WriteCatalogOnly,
}

/// The open catalog: the storage engine, the backup runtime (with the
/// configured store when there is one), the backend's name, and the serving
/// registration the command holds for its run.
struct Catalog {
    storage: Arc<dyn StorageEngine>,
    runtime: Arc<BackupRuntime>,
    backend: String,
    registration: Option<Arc<dyn ServingRegistrar>>,
}

impl Catalog {
    /// End the command's use of the catalog: release its serving
    /// registration.
    async fn close(self) {
        release_registration(self.registration).await;
    }
}

/// Open the catalog as `command` in `mode` and describe the configured
/// store without checking it; the second member is why no store is
/// configured, when none is. A catalog the factory refuses (a migration
/// running, or the catalog at another migration than this binary's) is an
/// [`extenddb_server::CatalogRefusal`], with the message and exit code
/// `serve` gives for the same status.
async fn open_catalog(
    app_config: &config::AppConfig,
    config_path: &str,
    release: &'static str,
    command: &'static str,
    mode: Mode,
) -> anyhow::Result<(
    Catalog,
    Option<extenddb_server::backup_store::NotConfigured>,
)> {
    let mut options = extenddb_storage::server_components::ServerComponentsOptions::default();
    options.release = release;
    options.max_concurrent_jobs = app_config.backup.max_concurrent_jobs;
    options.beside_server = mode == Mode::Read;
    options.startup_recovery = false;
    options.command = Some(command);
    let mut storage_config = app_config.storage.clone();
    storage_config.set_instance_id(&format!(
        "{}:backup-command",
        extenddb_server::backup_store::hostname()
    ));
    let components = extenddb_storage::create_server_components(
        storage_config.as_trait(),
        &app_config.server.region,
        options,
    )
    .await;
    let components = match components {
        Ok(components) => components,
        Err(extenddb_storage::server_components::BackendError::Catalog(error)) => {
            return Err(extenddb_server::catalog_refusal(
                &error,
                release,
                app_config,
                Some(config_path),
            )
            .await
            .into());
        }
        Err(e) => anyhow::bail!("cannot open the catalog: {e}"),
    };
    // The registration is the runtime hooks', which the command does not
    // otherwise keep: held here, it lasts until `Catalog::close`.
    let registration = components
        .runtime_hooks
        .as_ref()
        .and_then(|hooks| hooks.serving_registrar());
    let storage = components.engine;
    let backend = app_config.storage.backend.clone();
    let setup = extenddb_server::backup_store::configure_described(
        &app_config.backup,
        app_config.storage.database_file().as_deref(),
        &backend,
        &*storage,
        match mode {
            Mode::Read | Mode::WriteCatalogOnly => {
                extenddb_server::backup_store::Caller::ReadOnlyCommand
            }
            Mode::Write => extenddb_server::backup_store::Caller::Command,
            Mode::Sync => extenddb_server::backup_store::Caller::SyncCommand,
        },
    )
    .await;
    let setup = match setup {
        Ok(setup) => setup,
        Err(e) => {
            release_registration(registration).await;
            return Err(e);
        }
    };
    Ok((
        Catalog {
            storage,
            runtime: Arc::new(setup.runtime),
            backend,
            registration,
        },
        setup.not_configured,
    ))
}

/// The catalog with the configured store and the store's identity as the
/// command read it.
struct Opened {
    catalog: Catalog,
    identity: Identity,
}

/// The store's identity as the command read it.
enum Identity {
    /// The check passed, or the file was read and agrees with the rows.
    Known(StoreIdentity),
    /// The root has no identity file and the catalog no rows for it: a
    /// fresh root nothing has written to.
    Empty,
    /// The check failed (a listing still runs; a purge refuses).
    Problem(StoreProblem),
}

impl Opened {
    /// End the command's use of the catalog: release its serving
    /// registration.
    async fn close(self) {
        self.catalog.close().await;
    }

    fn store(&self) -> &ConfiguredStore {
        self.catalog
            .runtime
            .configured_store()
            .expect("open returns only with a configured store")
    }
}

async fn open(
    app_config: &config::AppConfig,
    config_path: &str,
    release: &'static str,
    command: &'static str,
    mode: Mode,
) -> anyhow::Result<Opening> {
    let (catalog, not_configured) =
        open_catalog(app_config, config_path, release, command, mode).await?;
    let Some(store) = catalog.runtime.configured_store() else {
        // Why there is none, as `configure_described` found it: the section
        // absent or disabled, the default root not yet created, or the
        // default root failed its check with no rows for it.
        let why = not_configured.expect("a runtime without a store carries the reason");
        catalog.close().await;
        return Ok(Opening::NoStore(why.to_string()));
    };
    let storage = &catalog.storage;
    let backend = &catalog.backend;
    // The server's startup leaves an S3 store's check to its reconciler;
    // the command runs it here. A read leaves the root as it finds it: the
    // identity file is read, never created, under the store's check bound as
    // the server's check is, so an endpoint that accepts and never answers
    // or a mount that has stopped answering costs the command the bound and
    // not the store's retries.
    let identity = match (mode, store.health()) {
        (Mode::Read | Mode::WriteCatalogOnly, _) => {
            match extenddb_engine::read_identity_bounded(store, &**storage).await {
                Ok(Some(identity)) => Identity::Known(identity),
                Ok(None) => Identity::Empty,
                Err(problem) => Identity::Problem(problem),
            }
        }
        (Mode::Write | Mode::Sync, StoreHealth::Checking) => {
            let outcome = extenddb_engine::check_configured_store(store, &**storage, backend).await;
            store.record_check(outcome.clone());
            match outcome {
                Ok(identity) => Identity::Known(identity),
                Err(problem) => Identity::Problem(problem),
            }
        }
        (Mode::Write | Mode::Sync, StoreHealth::Healthy) => match store.identity() {
            Some(identity) => Identity::Known(identity),
            None => Identity::Problem(StoreProblem::Catalog(
                "the store's id is unknown".to_owned(),
            )),
        },
        (Mode::Write | Mode::Sync, StoreHealth::Unhealthy(problem)) => Identity::Problem(problem),
    };
    Ok(Opening::Store(Opened { catalog, identity }))
}

/// How long a command waits for its registration's release: a row or
/// document whose delete does not answer in time expires on its own (45 s
/// after its last renewal on Cassandra and on MongoDB), and PostgreSQL's
/// session ends with the process.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// Release a command's serving registration, if it holds one, so a
/// migration that waits for the registrations does not wait for it to
/// expire. Bounded by [`RELEASE_TIMEOUT`].
async fn release_registration(registration: Option<Arc<dyn ServingRegistrar>>) {
    release_registration_within(registration, RELEASE_TIMEOUT).await;
}

async fn release_registration_within(
    registration: Option<Arc<dyn ServingRegistrar>>,
    bound: Duration,
) {
    if let Some(registration) = registration
        && tokio::time::timeout(bound, registration.release())
            .await
            .is_err()
    {
        tracing::warn!(
            "the command's serving registration was not released within {} s; it expires on \
             its own",
            bound.as_secs()
        );
    }
}

async fn rows_of(
    storage: &dyn StorageEngine,
    identity: &StoreIdentity,
    status: Option<BackupStatus>,
) -> anyhow::Result<Vec<BackupRow>> {
    storage
        .list_backup_rows(status, identity)
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("cannot list the catalog's backup rows: {e}"))
}

fn print_row(row: &BackupRow) {
    println!(
        "{:<9} {:<12} {:<32} {:>12} {:>10}  {}  store_root={} store_id={}{}",
        row.status,
        row.account_id,
        row.table_name,
        row.size_bytes,
        row.item_count,
        row.backup_arn,
        row.store.store_root,
        row.store.store_id,
        row.failure_reason
            .as_deref()
            .map(|r| format!("  failure_reason={r:?}"))
            .unwrap_or_default()
    );
}

/// Every row of the store's kind and root, whatever id it carries: the ids
/// the rows record, plus the file's when the check read one.
async fn rows_of_root(
    opened: &Opened,
    status: Option<BackupStatus>,
) -> anyhow::Result<(Vec<String>, Vec<BackupRow>)> {
    let store = opened.store();
    let mut ids = opened
        .catalog
        .storage
        .read_store_ids(store.kind, &store.root)
        .await
        .map_err(|e| anyhow::anyhow!("cannot read the catalog's store ids: {e}"))?;
    if let Identity::Known(identity) = &opened.identity
        && !ids.contains(&identity.store_id)
    {
        ids.push(identity.store_id.clone());
    }
    let mut rows = Vec::new();
    for id in &ids {
        let identity = StoreIdentity {
            store_kind: store.kind,
            store_root: store.root.clone(),
            store_id: id.clone(),
        };
        rows.extend(rows_of(&*opened.catalog.storage, &identity, status).await?);
    }
    Ok((ids, rows))
}

async fn list(opened: &Opened, failed: bool) -> anyhow::Result<()> {
    let status = failed.then_some(BackupStatus::Failed);
    let store = opened.store();
    let (ids, rows) = rows_of_root(opened, status).await?;
    match &opened.identity {
        Identity::Known(identity) => println!(
            "=== backups in the {} store {} (store id {}) ===",
            store.kind, store.root, identity.store_id
        ),
        Identity::Empty => println!(
            "=== backups in the {} store {} (the root has no {} yet; a server's first check \
             creates it) ===",
            store.kind,
            store.root,
            extenddb_engine::STORE_IDENTITY_KEY
        ),
        Identity::Problem(problem) => println!(
            "=== backups in the {} store {} (the store is unhealthy: {problem}; rows of every \
             id the catalog records for the root are listed) ===",
            store.kind, store.root
        ),
    }
    if ids.len() > 1 {
        println!(
            "  the catalog's rows for this root carry {} store ids: {}",
            ids.len(),
            ids.join(", ")
        );
    }
    if rows.is_empty() {
        println!("  (none)");
        return Ok(());
    }
    for row in &rows {
        print_row(row);
    }
    println!("{} row(s)", rows.len());
    Ok(())
}

/// The plan a purge prints before it deletes, and the dry run's whole
/// output: the store, its rows, and what stays.
async fn print_plan(opened: &Opened) -> anyhow::Result<()> {
    let configured = opened.store();
    println!("=== extenddb backup purge ===");
    match &opened.identity {
        Identity::Known(identity) => println!(
            "Store:  {} {} (store id {})",
            identity.store_kind, identity.store_root, identity.store_id
        ),
        Identity::Empty => println!(
            "Store:  {} {} (no {} yet)",
            configured.kind,
            configured.root,
            extenddb_engine::STORE_IDENTITY_KEY
        ),
        Identity::Problem(problem) => println!(
            "Store:  {} {} (unhealthy: {problem})",
            configured.kind, configured.root
        ),
    }
    let (_, planned) = rows_of_root(opened, None).await?;
    println!();
    println!("--- Backups to delete (each backup's objects and its catalog row):");
    if planned.is_empty() {
        println!("  (none)");
    }
    for row in &planned {
        print_row(row);
    }
    println!();
    println!(
        "Left in place: {}, exports/, and any prefix the catalog has no row for.",
        extenddb_engine::STORE_IDENTITY_KEY
    );
    println!();
    Ok(())
}

/// `backup purge` without `--confirm`: the plan, nothing changed, exit 0.
/// On an unhealthy store the plan still lists the rows, and the closing
/// line says that `--confirm` refuses until the store's check passes.
async fn dry_run(opened: &Opened) -> anyhow::Result<()> {
    print_plan(opened).await?;
    match &opened.identity {
        Identity::Problem(problem) => println!(
            "Dry run: nothing was deleted. The store is unhealthy ({}); --confirm refuses until \
             the store's check passes.",
            problem.category()
        ),
        Identity::Known(_) | Identity::Empty => println!(
            "Dry run: nothing was deleted. Run again with --confirm to permanently delete every \
             backup of the configured store listed above."
        ),
    }
    Ok(())
}

async fn purge(
    opened: &Opened,
    app_config: &config::AppConfig,
    mut interrupt: Interrupt,
) -> anyhow::Result<()> {
    let backup = &app_config.backup;
    let storage = &*opened.catalog.storage;
    let configured = opened.store();
    let identity = match &opened.identity {
        Identity::Known(identity) => identity,
        Identity::Empty => anyhow::bail!(
            "the {} backup store {} has no {} and the catalog has no rows for it: nothing to purge",
            configured.kind,
            configured.root,
            extenddb_engine::STORE_IDENTITY_KEY
        ),
        Identity::Problem(problem) => anyhow::bail!(
            "the {} backup store {} is unhealthy: {problem}; fix that before purging its backups",
            configured.kind,
            configured.root
        ),
    };
    let store = &*configured.store;
    let fence = configured.fence();
    print_plan(opened).await?;

    let stale_after = Duration::from_secs(backup.stale_job_after_secs);
    let holder = format!(
        "extenddb backup purge on {} (pid {})",
        extenddb_server::backup_store::hostname(),
        std::process::id()
    );
    // The signal listener has been up since before the open, so a signal
    // from here on ends the command with the claim released: at once during
    // the wait, before the next row during the deletion.
    let claim = storage
        .maintenance_claim(&holder, stale_after)
        .await
        .map_err(|e| match e {
            StorageError::AlreadyExists(m) => anyhow::anyhow!(
                "{m}; one maintenance command runs at a time, and a claim whose holder died \
                 expires after stale_job_after_secs ({} s)",
                backup.stale_job_after_secs
            ),
            other => anyhow::anyhow!("cannot take the maintenance claim: {other}"),
        })?;
    println!("Took the maintenance claim as {holder:?}.");
    let claim = HeldClaim::start(claim, JOB_HEARTBEAT_INTERVAL);

    // On SQLite the command holds the file alone: every job owner is dead
    // and no reconciler runs, so their resolution is this command's, and the
    // owner of any row is dead whatever its heartbeat says, so the window a
    // live owner is told apart by is zero. On the other backends purge runs
    // beside live servers, whose jobs heartbeat within `stale_after`.
    let owner_window = if opened.catalog.backend == "sqlite" {
        resolve_dead_owners(opened).await;
        Duration::ZERO
    } else {
        stale_after
    };

    // Wait for the jobs the claim excludes from starting to finish.
    let wait_limit = stale_after * 2;
    let started = Instant::now();
    let outcome = wait_for_quiet(
        storage,
        identity,
        &claim,
        Wait {
            owner_window,
            limit: wait_limit,
            poll: WAIT_POLL,
        },
        &mut interrupt,
        |remaining| {
            println!(
                "Waiting for running jobs to finish: {}",
                remaining.join("; ")
            )
        },
    )
    .await;
    match outcome {
        WaitOutcome::Quiet => {}
        WaitOutcome::Busy(remaining) => {
            release(claim).await;
            anyhow::bail!(
                "jobs were still running after waiting {} s (2 * stale_job_after_secs): {}. The \
                 maintenance claim is released; retry once they finish",
                started.elapsed().as_secs(),
                remaining.join("; ")
            );
        }
        WaitOutcome::Lost(reason) => {
            // The release is conditioned on the generation: another holder's
            // claim is left to it.
            release(claim).await;
            anyhow::bail!("stopped before any deletion: {reason}; retry once the claim is free");
        }
        WaitOutcome::CatalogError(e) => {
            release(claim).await;
            anyhow::bail!(
                "stopped before any deletion: {e}. The maintenance claim is released; retry once \
                 the catalog answers"
            );
        }
        WaitOutcome::Interrupted(signal) => {
            release(claim).await;
            anyhow::bail!(
                "stopped before any deletion: {signal} received while waiting. The maintenance \
                 claim is released"
            );
        }
    }

    // Re-read under the claim: the plan printed before it may be stale.
    let rows = match rows_of(storage, identity, None).await {
        Ok(rows) => rows,
        Err(e) => {
            release(claim).await;
            return Err(e);
        }
    };
    let outcome = purge_rows(
        storage,
        store,
        &fence,
        &rows,
        owner_window,
        &claim,
        &interrupt,
        |row| {
            tracing::info!(
                target: "extenddb::audit",
                action = "backup purge",
                account_id = %row.account_id,
                backup_arn = %row.backup_arn,
                store_root = %row.store.store_root,
                "deleted backup objects and catalog row"
            );
            println!("deleted {} ({})", row.backup_arn, row.status);
        },
    )
    .await;
    release(claim).await;
    let deleted = outcome.deleted.len();
    println!();
    println!(
        "=== extenddb backup purge complete: {deleted} backup(s) deleted, {} left ===",
        rows.len() - deleted
    );
    match outcome.stopped {
        Some(Stop::Lost(reason)) => {
            anyhow::bail!(
                "stopped before the remaining rows: {reason}; retry once the claim is free"
            )
        }
        Some(Stop::Interrupted(signal)) => anyhow::bail!(
            "stopped before the remaining rows: {signal} received. The maintenance claim is \
             released; run the command again for the rest"
        ),
        None => {}
    }
    if !outcome.skipped.is_empty() {
        for line in &outcome.skipped {
            println!("  left: {line}");
        }
        anyhow::bail!(
            "{} backup(s) could not be deleted; see above",
            outcome.skipped.len()
        );
    }
    Ok(())
}

/// The handle every subcommand reads a signal from, fed by a task that waits
/// for `SIGINT` (Ctrl-C) or `SIGTERM`. The first signal stops the command at
/// its next safe point (after the open, the wait at once, the deletion loop
/// before its next row) with its serving registration and, in a purge, the
/// claim released; a second one exits the process at once, the registration
/// then expiring on its own and the claim by the stale rule (on SQLite the
/// next server start clears it).
fn listen_for_signals() -> Interrupt {
    let (tx, interrupt) = Interrupt::new();
    tokio::spawn(async move {
        let name = signal_name().await;
        println!(
            "{name} received; finishing the current step and releasing what the command holds (a \
             second {name} exits at once)."
        );
        let _ = tx.send(Some(name));
        let again = signal_name().await;
        println!(
            "{again} received again; exiting now. The serving registration expires on its own and \
             a maintenance claim by the stale rule."
        );
        std::process::exit(130);
    });
    interrupt
}

#[cfg(unix)]
async fn signal_name() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    let ctrl_c = tokio::signal::ctrl_c();
    let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
        let _ = ctrl_c.await;
        return "SIGINT";
    };
    tokio::select! {
        _ = ctrl_c => "SIGINT",
        _ = sigterm.recv() => "SIGTERM",
    }
}

#[cfg(not(unix))]
async fn signal_name() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "Ctrl-C"
}

/// The reconciler's duties, run once with a zero stale window: on SQLite
/// under the serve lock every owner is dead. The command holds the
/// maintenance claim, and its state says so, so the cycle takes the held
/// branch without reading the claim (read through a zero window, the
/// command's own claim would read as held only within one instant of its
/// heartbeat). What was resolved is printed; a failure is printed and the
/// wait decides.
async fn resolve_dead_owners(opened: &Opened) {
    match extenddb_engine::reconciler::cycle(
        &opened.catalog.storage,
        &opened.catalog.runtime,
        Duration::ZERO,
        &opened.catalog.backend,
        &mut extenddb_engine::reconciler::ReconcilerState::for_maintenance_holder(),
    )
    .await
    {
        Ok(report) => {
            for table in &report.legacy_restores_removed {
                println!("removed the restore target {table}: its restore did not finish");
            }
            for table in &report.failed_targets_removed {
                println!("removed the restore target {table}: its restore had failed");
            }
            for table in &report.manifest_restores_failed_and_removed {
                println!(
                    "failed the restore into {table} and removed the table: its owner is dead, \
                     and a maintenance command resumes no restore"
                );
            }
            for (table, reason) in &report.manifest_restores_failed {
                println!("failed the restore into {table} ({reason}): its owner is dead");
            }
            for table in &report.targets_left_for_operator {
                println!(
                    "left the restore target {table}: its restore failed with target_not_empty; \
                     remove it with backup remove-target"
                );
            }
            for table in &report.targets_left_to_owner {
                println!(
                    "left the table {table}: it is no longer the restore target its record names"
                );
            }
            for arn in &report.legacy_backups_removed {
                println!("removed the in-catalog backup {arn}: its create did not finish");
            }
            for arn in &report.deletions_finished {
                println!("finished the deletion of {arn}");
            }
            for (arn, status) in &report.creating_resolved {
                println!(
                    "resolved the CREATING backup {arn} to {status}: its owner is dead{}",
                    match status {
                        extenddb_storage::backup_catalog::BackupStatus::Available =>
                            " and the store holds its complete manifest",
                        _ =>
                            " and the store holds no complete manifest; its objects are \
                              deleted once the row's heartbeat is stale",
                    }
                );
            }
            for arn in &report.creating_left_to_owner {
                println!(
                    "left the CREATING backup {arn}: another owner took it over before this \
                     command's write"
                );
            }
        }
        Err(e) => println!("could not resolve the jobs the stopped server left: {e}"),
    }
}

/// Release the claim; a failure is printed, since the claim then expires by
/// the stale rule and the command's own outcome is what the operator needs.
async fn release(claim: HeldClaim) {
    if let Err(e) = claim.release().await {
        println!("could not release the maintenance claim ({e}); it expires on its own");
    }
}

/// `backup remove-target --account <id> <table>` (module doc).
async fn remove_restore_target(
    catalog: &Catalog,
    app_config: &config::AppConfig,
    account_id: &str,
    table_name: &str,
) -> anyhow::Result<()> {
    let stale_after = Duration::from_secs(app_config.backup.stale_job_after_secs);
    let holder = format!(
        "extenddb backup remove-target on {} (pid {})",
        extenddb_server::backup_store::hostname(),
        std::process::id()
    );
    println!("=== extenddb backup remove-target ===");
    println!("Taking the maintenance claim as {holder:?}.");
    // On SQLite the command holds the file alone, so every job owner is
    // dead whatever its heartbeat says; on the other backends a RUNNING
    // record whose heartbeat is within `stale_after` has a live owner.
    let owner_window = if catalog.backend == "sqlite" {
        Duration::ZERO
    } else {
        stale_after
    };
    let removed = remove_target(
        &*catalog.storage,
        &RemoveTarget {
            account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
            holder,
            stale_after,
            owner_window,
            heartbeat_interval: JOB_HEARTBEAT_INTERVAL,
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let state = match (removed.status, removed.failure_reason.as_deref()) {
        (JobStatus::Running, _) => "RUNNING with a dead owner".to_owned(),
        (status, Some(reason)) => format!("{status} ({reason})"),
        (status, None) => status.to_string(),
    };
    let kind = match removed.kind {
        extenddb_storage::backup_catalog::RestoreKind::Manifest => "store-backed",
        extenddb_storage::backup_catalog::RestoreKind::Legacy => "in-catalog",
    };
    println!(
        "removed the restore target {table_name} (account {account_id}, table id {}) and its \
         record: its {kind} restore from {} was {state}",
        removed.table_id, removed.backup_arn
    );
    println!("The maintenance claim is released.");
    Ok(())
}

/// `backup sync [--adopt-store] [--prune-missing]` (module doc).
#[allow(clippy::too_many_lines)]
async fn sync(
    opened: &Opened,
    app_config: &config::AppConfig,
    adopt_store: bool,
    prune_missing: bool,
    mut interrupt: Interrupt,
) -> anyhow::Result<()> {
    let storage = &*opened.catalog.storage;
    let configured = opened.store();
    println!("=== extenddb backup sync ===");
    // The identity sync writes rows under, or the root to adopt.
    let mut adopting = false;
    let known = match &opened.identity {
        Identity::Known(identity) => {
            println!(
                "Store:  {} {} (store id {})",
                identity.store_kind, identity.store_root, identity.store_id
            );
            if adopt_store {
                println!(
                    "The root's {} already names this catalog: there is nothing to adopt.",
                    extenddb_engine::STORE_IDENTITY_KEY
                );
            }
            Some(identity.clone())
        }
        Identity::Problem(problem @ StoreProblem::ForeignCatalog { .. }) => {
            println!(
                "Store:  {} {} (owned by another catalog: {problem})",
                configured.kind, configured.root
            );
            if !adopt_store {
                anyhow::bail!(
                    "the {} backup store {} is owned by another catalog: {problem}. Sync changes \
                     no row of a root another catalog owns. Stop every server of the catalog the \
                     file names, then run with --adopt-store to make this catalog its owner: a \
                     running server of that catalog acts on the root until a later check that \
                     can read its catalog finds it owned by another catalog (its deletion fence refuses a deletion once the \
                     file names another catalog, but a write already past the fence's read \
                     lands){}",
                    configured.kind,
                    configured.root,
                    if configured.is_defaulted() {
                        ". The default root beside a SQLite database file is shared by every \
                         database file in its directory, and the adoption takes it from \
                         whichever catalog wrote the file"
                    } else {
                        ""
                    }
                );
            }
            adopting = true;
            None
        }
        Identity::Problem(problem) => anyhow::bail!(
            "the {} backup store {} is unhealthy: {problem}; sync changes no row while the store \
             is unhealthy",
            configured.kind,
            configured.root
        ),
        Identity::Empty => anyhow::bail!(
            "the {} backup store {} has no {}: its check did not run",
            configured.kind,
            configured.root,
            extenddb_engine::STORE_IDENTITY_KEY
        ),
    };

    // On SQLite the command holds the serve lock, which already excludes
    // every server and every other command, so plain sync takes no claim
    // and resolves nothing a stopped server left: its rows are skipped, and
    // the next server start resolves them (and resumes a store-backed
    // restore, which the dead-owner resolutions would fail). Elsewhere every sync takes the
    // maintenance claim, so no purge removes a row between this sync's row
    // read and its insert and then deletes the bytes it adopted; plain sync
    // waits for no job, since its insert-if-absent orders it against a
    // CreateBackup. The options wait for running jobs as purge does.
    let sqlite = opened.catalog.backend == "sqlite";
    let waits = adopt_store || prune_missing;
    let mut claim = None;
    if !sqlite || waits {
        let backup = &app_config.backup;
        let stale_after = Duration::from_secs(backup.stale_job_after_secs);
        let holder = format!(
            "extenddb backup sync on {} (pid {})",
            extenddb_server::backup_store::hostname(),
            std::process::id()
        );
        let taken = storage
            .maintenance_claim(&holder, stale_after)
            .await
            .map_err(|e| match e {
                StorageError::AlreadyExists(m) => anyhow::anyhow!(
                    "{m}; one maintenance command runs at a time, and a claim whose holder died \
                     expires after stale_job_after_secs ({} s)",
                    backup.stale_job_after_secs
                ),
                other => anyhow::anyhow!("cannot take the maintenance claim: {other}"),
            })?;
        println!("Took the maintenance claim as {holder:?}.");
        let held = HeldClaim::start(taken, JOB_HEARTBEAT_INTERVAL);
        if waits
            && let Err(refusal) = wait_for_jobs(
                opened,
                configured,
                known.as_ref(),
                &held,
                &mut interrupt,
                stale_after,
            )
            .await
        {
            release(held).await;
            return Err(refusal);
        }
        claim = Some(held);
    }

    let done = sync_held(
        opened,
        app_config,
        known,
        adopting,
        prune_missing,
        claim.as_ref(),
        &interrupt,
    )
    .await;
    if let Some(claim) = claim {
        release(claim).await;
    }
    done
}

/// The wait `--adopt-store` and `--prune-missing` run under the claim, as
/// purge's: on SQLite after the dead-owner resolutions with a zero window.
/// The refusal is the command's error, the claim still held.
async fn wait_for_jobs(
    opened: &Opened,
    configured: &ConfiguredStore,
    known: Option<&StoreIdentity>,
    claim: &HeldClaim,
    interrupt: &mut Interrupt,
    stale_after: Duration,
) -> anyhow::Result<()> {
    let storage = &*opened.catalog.storage;
    // The identity the wait reads rows of: the file's id, before the
    // adoption as after it.
    let wait_identity = match known {
        Some(identity) => identity.clone(),
        None => {
            let file = tokio::time::timeout(
                configured.check_timeout(),
                extenddb_engine::store_identity::read_identity_file(&*configured.store),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "the store's {} could not be read within {} s",
                    extenddb_engine::STORE_IDENTITY_KEY,
                    configured.check_timeout().as_secs()
                )
            })?
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "the root's {} is gone; run the command again",
                    extenddb_engine::STORE_IDENTITY_KEY
                )
            })?;
            StoreIdentity {
                store_kind: configured.kind,
                store_root: configured.root.clone(),
                store_id: file.store_id,
            }
        }
    };
    let owner_window = if opened.catalog.backend == "sqlite" {
        resolve_dead_owners(opened).await;
        Duration::ZERO
    } else {
        stale_after
    };
    let started = Instant::now();
    let outcome = wait_for_quiet(
        storage,
        &wait_identity,
        claim,
        Wait {
            owner_window,
            limit: stale_after * 2,
            poll: WAIT_POLL,
        },
        interrupt,
        |remaining| {
            println!(
                "Waiting for running jobs to finish: {}",
                remaining.join("; ")
            )
        },
    )
    .await;
    match outcome {
        WaitOutcome::Quiet => Ok(()),
        WaitOutcome::Busy(remaining) => Err(anyhow::anyhow!(
            "jobs were still running after waiting {} s (2 * stale_job_after_secs): {}. The \
             maintenance claim is released; retry once they finish",
            started.elapsed().as_secs(),
            remaining.join("; ")
        )),
        WaitOutcome::Lost(reason) => Err(anyhow::anyhow!(
            "stopped before any change: {reason}; retry once the claim is free"
        )),
        WaitOutcome::CatalogError(e) => Err(anyhow::anyhow!(
            "stopped before any change: {e}. The maintenance claim is released; retry once the \
             catalog answers"
        )),
        WaitOutcome::Interrupted(signal) => Err(anyhow::anyhow!(
            "stopped before any change: {signal} received while waiting. The maintenance claim \
             is released"
        )),
    }
}

/// The adoption, the sync and the prune, with the claim held (on
/// PostgreSQL, MongoDB and Cassandra) or under the serve lock (SQLite); the
/// caller releases the claim.
#[allow(clippy::too_many_lines)]
async fn sync_held(
    opened: &Opened,
    app_config: &config::AppConfig,
    known: Option<StoreIdentity>,
    adopting: bool,
    prune_missing: bool,
    claim: Option<&HeldClaim>,
    interrupt: &Interrupt,
) -> anyhow::Result<()> {
    let storage = &*opened.catalog.storage;
    let configured = opened.store();
    let store = &*configured.store;
    let catalog_id = storage
        .catalog_id()
        .await
        .map_err(|e| anyhow::anyhow!("the catalog could not be read: {e}"))?;
    let identity = match known {
        Some(identity) => identity,
        None => {
            debug_assert!(adopting);
            // The store's calls under the check's bound, as the server's
            // check runs them: a store that stops answering ends the
            // adoption at the call that did not finish, named in the
            // refusal, and the catalog's reads and the claim's beat run
            // beside the bound.
            let adopted = extenddb_engine::store_identity::adopt_configured(
                configured,
                storage,
                &opened.catalog.backend,
                claim,
            )
            .await
            .map_err(|refusal| {
                anyhow::anyhow!("--adopt-store did not adopt the root: {refusal}")
            })?;
            let identity = match adopted {
                extenddb_engine::store_identity::Adoption::Adopted {
                    identity,
                    previous_catalog_id,
                } => {
                    println!(
                        "Adopted the root: {} named catalog {previous_catalog_id} and now names \
                         this catalog (store id {}). A server of catalog {previous_catalog_id} \
                         that still runs acts on the root until a later check that can read its \
                         catalog finds it owned by another catalog; its deletion fence refuses a deletion and a \
                         CreateBackup once it re-reads the file. Stop every such server before \
                         an adoption.",
                        extenddb_engine::STORE_IDENTITY_KEY,
                        identity.store_id
                    );
                    identity
                }
                extenddb_engine::store_identity::Adoption::AlreadyOwned(identity) => {
                    println!(
                        "The root's {} names this catalog now: there is nothing to adopt.",
                        extenddb_engine::STORE_IDENTITY_KEY
                    );
                    identity
                }
            };
            configured.record_check(Ok(identity.clone()));
            identity
        }
    };
    if let Some(reason) = claim.and_then(HeldClaim::lost) {
        anyhow::bail!("stopped before the sync: {reason}; retry once the claim is free");
    }

    let stop = || {
        claim.and_then(HeldClaim::lost).or_else(|| {
            interrupt
                .signalled()
                .map(|signal| format!("{signal} received"))
        })
    };
    let report = extenddb_engine::sync::sync(
        storage,
        extenddb_engine::sync::SyncTarget {
            store,
            identity: &identity,
            metrics: None,
            region: Some(&app_config.server.region),
            stop: Some(&stop),
            claim,
        },
    )
    .await;
    let report = match report {
        Ok(report) => report,
        Err(extenddb_engine::sync::SyncError::Listing { error, partial }) => {
            print_sync_report(&partial, prune_missing, false);
            anyhow::bail!(
                "the store's listing failed: {error}. {} backup(s) listed before the failure \
                 were adopted, and no AVAILABLE row was examined for a missing manifest; run the \
                 command again",
                partial.adopted.len()
            );
        }
        Err(e) => anyhow::bail!("{e}"),
    };
    print_sync_report(&report, prune_missing, report.stopped.is_none());
    if let Some(reason) = &report.stopped {
        anyhow::bail!(
            "stopped: {reason}. {} backup(s) were adopted before the stop, and no AVAILABLE row \
             was examined for a missing manifest; run the command again for the rest",
            report.adopted.len()
        );
    }

    let mut counts = PruneCounts::default();
    let mut stopped = None;
    if prune_missing && !report.missing_manifests.is_empty() {
        let claim = claim.ok_or_else(|| {
            anyhow::anyhow!("--prune-missing runs only under the maintenance claim")
        })?;
        let (outcomes, stop_reason) = extenddb_engine::sync::prune_missing(
            storage,
            store,
            &configured.root,
            &report.missing_manifests,
            claim,
            &stop,
        )
        .await;
        println!();
        println!("--- --prune-missing (rows removed; no object is deleted):");
        for (arn, outcome) in &outcomes {
            println!("  {arn}: {outcome}");
        }
        counts = PruneCounts::of(&outcomes);
        stopped = stop_reason;
    }

    // The file once more: every sync started from a file naming this
    // catalog (the command's check read it, or the adoption wrote it), and
    // an adopter whose put landed during the sync is detected here, the
    // rows this sync inserted being then rows of a root this catalog does
    // not own.
    let via = configured.calls("identity file re-read");
    let file = extenddb_engine::store_identity::read_identity_file(&via).await;
    let file = (!via.timed_out()).then_some(file);
    owner_after_sync(file, &catalog_id, configured.check_timeout())?;

    println!();
    println!(
        "=== extenddb backup sync {}: {} backup(s) adopted, {} prefix(es) skipped, {} \
         AVAILABLE row(s) whose manifest is gone{} ===",
        run_word(&report, stopped.is_some()),
        report.adopted.len(),
        report.skipped.len(),
        report.missing_manifests.len(),
        match (prune_missing, &stopped) {
            (false, _) => String::new(),
            (true, None) => format!(" ({} removed)", counts.removed),
            (true, Some(_)) => format!(" ({} removed before the stop)", counts.removed),
        },
    );
    if let Some(reason) = stopped {
        anyhow::bail!("--prune-missing stopped before the remaining rows: {reason}");
    }
    counts.left_for_a_later_run()?;
    complete_run(&report)
}

/// The word of the summary line: `stopped` when the prune stopped before
/// its remaining rows, `incomplete` when a prefix was skipped for a store or
/// catalog error (the run then exits 1, [`complete_run`]), `complete`
/// otherwise.
fn run_word(report: &extenddb_engine::sync::SyncReport, stopped: bool) -> &'static str {
    if stopped {
        "stopped"
    } else if report.transient_skips() > 0 {
        "incomplete"
    } else {
        "complete"
    }
}

/// What `--prune-missing` did, counted over its outcomes.
#[derive(Debug, Default, PartialEq, Eq)]
struct PruneCounts {
    /// Rows removed.
    removed: usize,
    /// Rows left for a later run (`PruneOutcome::needs_another_run`).
    left: usize,
    /// Of those, rows a restore is reading.
    in_use: usize,
}

impl PruneCounts {
    fn of(outcomes: &[(String, extenddb_engine::sync::PruneOutcome)]) -> Self {
        let mut counts = Self::default();
        for (_, outcome) in outcomes {
            if *outcome == extenddb_engine::sync::PruneOutcome::Pruned {
                counts.removed += 1;
            } else if outcome.needs_another_run() {
                counts.left += 1;
                if matches!(outcome, extenddb_engine::sync::PruneOutcome::InUse(_)) {
                    counts.in_use += 1;
                }
            }
        }
        counts
    }

    /// The exit of a run that left a row for a later run: exit 1 with the
    /// error naming the count and, when a restore is reading one of the
    /// backups, when to run the command again; `Ok` when no row was left.
    fn left_for_a_later_run(&self) -> anyhow::Result<()> {
        if self.left == 0 {
            return Ok(());
        }
        anyhow::bail!(
            "{} row(s) whose manifest is gone were left for a later run (see above){}",
            self.left,
            if self.in_use > 0 {
                format!(
                    "; {} of them are being restored from: run the command again once the \
                     restore ends",
                    self.in_use
                )
            } else {
                "; run the command again once the store and the catalog answer".to_owned()
            }
        )
    }
}

/// The sync's last read of `extenddb-store.json` (`file`, `None` when the
/// read did not finish within `bound`): `Ok` when it still names this
/// catalog (`catalog_id`), and otherwise the error naming what it found, so
/// an adopter whose put landed during the sync is reported, whether the
/// sync adopted the root or started from a file that named this catalog.
fn owner_after_sync(
    file: Option<
        Result<
            Option<extenddb_engine::store_identity::StoreIdentityFile>,
            extenddb_engine::StoreProblem,
        >,
    >,
    catalog_id: &str,
    bound: Duration,
) -> anyhow::Result<()> {
    match file {
        Some(Ok(Some(file))) if file.catalog_id == catalog_id => Ok(()),
        Some(Ok(Some(file))) => anyhow::bail!(
            "after the sync, {} names catalog {} (store id {}): another catalog adopted the root \
             during the sync, and the rows this sync inserted are this catalog's rows of a root \
             it does not own",
            extenddb_engine::STORE_IDENTITY_KEY,
            file.catalog_id,
            file.store_id
        ),
        Some(Ok(None)) => anyhow::bail!(
            "after the sync, the root has no {}",
            extenddb_engine::STORE_IDENTITY_KEY
        ),
        Some(Err(problem)) => anyhow::bail!(
            "after the sync, {} could not be read: {problem}",
            extenddb_engine::STORE_IDENTITY_KEY
        ),
        None => anyhow::bail!(
            "after the sync, {} could not be read within {} s",
            extenddb_engine::STORE_IDENTITY_KEY,
            bound.as_secs()
        ),
    }
}

/// A sync that ran to its end is complete only when no prefix was skipped
/// for a store or catalog error; otherwise the command exits 1, since a
/// later run adopts those prefixes.
fn complete_run(report: &extenddb_engine::sync::SyncReport) -> anyhow::Result<()> {
    let transient = report.transient_skips();
    if transient > 0 {
        anyhow::bail!(
            "the run is incomplete: {transient} prefix(es) were skipped for a store or catalog \
             error (see above); run the command again once the store and the catalog answer"
        );
    }
    Ok(())
}

/// The report of a sync: what it adopted, what it skipped and why, the
/// AVAILABLE rows whose manifest is gone (`examined` is false when the run
/// ended before the end of the listing, which leaves them unexamined).
fn print_sync_report(
    report: &extenddb_engine::sync::SyncReport,
    prune_missing: bool,
    examined: bool,
) {
    println!();
    println!("--- Adopted (an AVAILABLE row inserted for each):");
    if report.adopted.is_empty() {
        println!("  (none)");
    }
    for adopted in &report.adopted {
        match &adopted.foreign {
            Some(note) => println!("  {} ({note})", adopted.backup_arn),
            None => println!("  {}", adopted.backup_arn),
        }
    }
    println!();
    println!("--- Skipped (not adopted):");
    if report.skipped.is_empty() {
        println!("  (none)");
    }
    for skipped in &report.skipped {
        println!("  {}: {}", skipped.prefix, skipped.reason);
    }
    println!();
    println!(
        "--- AVAILABLE rows whose manifest is not in the store (the rows were AVAILABLE when the \
         catalog was read):"
    );
    if !examined {
        println!("  (not examined)");
    } else if report.missing_manifests.is_empty() {
        println!("  (none)");
    }
    for key in &report.missing_manifests {
        println!("  {}  (account {})", key.backup_arn, key.account_id);
    }
    if !report.missing_manifests.is_empty() && !prune_missing {
        println!(
            "  (left AVAILABLE; run with --prune-missing to remove the rows, which deletes no \
             object)"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use extenddb_storage::hooks::{Reregistration, ServingRegistrar};

    use super::{
        PruneCounts, complete_run, owner_after_sync, release_registration,
        release_registration_within, run_word,
    };

    /// The identity file as a store holds it, naming `catalog_id`.
    fn file(catalog_id: &str) -> extenddb_engine::StoreIdentityFile {
        extenddb_engine::StoreIdentityFile {
            format_version: 1,
            store_id: "store-1".to_owned(),
            catalog_id: catalog_id.to_owned(),
            backend: "postgres".to_owned(),
            created_at_ms: 0,
            ephemeral_catalog: false,
        }
    }

    /// The adopting sync's last read: the file still naming this catalog
    /// passes; another catalog (an adopter whose put landed during the
    /// sync), no file, an unreadable file and a read past the bound each
    /// fail, the first naming the new owner.
    #[test]
    fn the_adopting_syncs_last_read_fails_unless_the_file_names_this_catalog() {
        let bound = Duration::from_secs(10);
        owner_after_sync(Some(Ok(Some(file("catalog-1")))), "catalog-1", bound).unwrap();
        let err = owner_after_sync(Some(Ok(Some(file("adopter")))), "catalog-1", bound)
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with(
                "after the sync, extenddb-store.json names catalog adopter (store id \
                             store-1): another catalog adopted the root during the sync"
            ),
            "{err}"
        );
        let err = owner_after_sync(Some(Ok(None)), "catalog-1", bound)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "after the sync, the root has no extenddb-store.json");
        let err = owner_after_sync(
            Some(Err(extenddb_engine::StoreProblem::Transport(
                "down".to_owned(),
            ))),
            "catalog-1",
            bound,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.starts_with("after the sync, extenddb-store.json could not be read: "),
            "{err}"
        );
        let err = owner_after_sync(None, "catalog-1", bound)
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "after the sync, extenddb-store.json could not be read within 10 s"
        );
    }

    /// A sync that skipped a prefix for a store or catalog error exits 1,
    /// naming the count, and its summary line reads `incomplete`; skips of
    /// any other reason leave it complete.
    #[test]
    fn a_sync_with_a_transient_skip_is_incomplete() {
        use extenddb_engine::sync::{SkipReason, SkippedPrefix, SyncReport};
        let skipped = |reason| SkippedPrefix {
            prefix: "123456789012/T/01700000000000-0000000a/".to_owned(),
            reason,
        };
        let lasting = || {
            vec![
                skipped(SkipReason::Incomplete("no manifest".to_owned())),
                skipped(SkipReason::RowExists("a row".to_owned())),
            ]
        };
        let complete = SyncReport {
            skipped: lasting(),
            ..SyncReport::default()
        };
        complete_run(&complete).unwrap();
        assert_eq!(run_word(&complete, false), "complete");
        assert_eq!(run_word(&complete, true), "stopped");
        for reason in [
            SkipReason::Store("get failed".to_owned()),
            SkipReason::Catalog("insert failed".to_owned()),
        ] {
            let mut skips = lasting();
            skips.push(skipped(reason));
            let report = SyncReport {
                skipped: skips,
                ..SyncReport::default()
            };
            let err = complete_run(&report).unwrap_err().to_string();
            assert!(
                err.starts_with(
                    "the run is incomplete: 1 prefix(es) were skipped for a store or \
                                 catalog error"
                ),
                "{err}"
            );
            assert_eq!(run_word(&report, false), "incomplete");
            assert_eq!(run_word(&report, true), "stopped");
        }
    }

    /// `--prune-missing`'s exit: a row left for a later run (a restore
    /// reading the backup, a failed store or catalog call) exits 1 naming
    /// the count and, for a restore, when to run the command again; rows
    /// removed, whose manifest is back, whose status changed or that are
    /// gone leave nothing for a later run and exit 0.
    #[test]
    fn a_prune_that_left_a_row_for_a_later_run_exits_1() {
        use extenddb_engine::sync::PruneOutcome;
        use extenddb_storage::backup_catalog::BackupStatus;
        let arn = |n: u32| format!("arn:aws:dynamodb:us-east-1:123456789012:table/T/backup/{n}");
        let settled = vec![
            (arn(1), PruneOutcome::Pruned),
            (arn(2), PruneOutcome::ManifestBack),
            (arn(3), PruneOutcome::Status(BackupStatus::Deleting)),
            (arn(4), PruneOutcome::Gone),
            (arn(5), PruneOutcome::Pruned),
        ];
        let counts = PruneCounts::of(&settled);
        assert_eq!(
            counts,
            PruneCounts {
                removed: 2,
                left: 0,
                in_use: 0
            }
        );
        counts.left_for_a_later_run().unwrap();
        assert_eq!(PruneCounts::default().left_for_a_later_run().ok(), Some(()));

        let mut with_restore = settled.clone();
        with_restore.push((arn(6), PruneOutcome::InUse("T-restored".to_owned())));
        let counts = PruneCounts::of(&with_restore);
        assert_eq!(
            counts,
            PruneCounts {
                removed: 2,
                left: 1,
                in_use: 1
            }
        );
        let err = counts.left_for_a_later_run().unwrap_err().to_string();
        assert_eq!(
            err,
            "1 row(s) whose manifest is gone were left for a later run (see above); 1 of them \
             are being restored from: run the command again once the restore ends"
        );

        let mut with_failure = settled;
        with_failure.push((
            arn(7),
            PruneOutcome::Failed("the manifest's head failed: down".to_owned()),
        ));
        with_failure.push((arn(8), PruneOutcome::InUse("T-r".to_owned())));
        let counts = PruneCounts::of(&with_failure);
        assert_eq!(
            counts,
            PruneCounts {
                removed: 2,
                left: 2,
                in_use: 1
            }
        );
        let err = counts.left_for_a_later_run().unwrap_err().to_string();
        assert!(
            err.starts_with("2 row(s) whose manifest is gone were left"),
            "{err}"
        );
        assert!(err.ends_with("once the restore ends"), "{err}");
        let counts = PruneCounts::of(&[(
            arn(9),
            PruneOutcome::Failed("the row could not be removed: down".to_owned()),
        )]);
        let err = counts.left_for_a_later_run().unwrap_err().to_string();
        assert_eq!(
            err,
            "1 row(s) whose manifest is gone were left for a later run (see above); run the \
             command again once the store and the catalog answer"
        );
    }

    /// A registrar that counts its releases, each taking `takes`.
    struct Counting {
        released: AtomicUsize,
        takes: Duration,
    }

    #[async_trait]
    impl ServingRegistrar for Counting {
        async fn lost(&self) -> String {
            std::future::pending().await
        }
        async fn register_again(&self) -> Reregistration {
            Reregistration::Registered
        }
        async fn release(&self) {
            tokio::time::sleep(self.takes).await;
            self.released.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A command's registration is released when the command ends, so a
    /// migration does not wait for its row or document to expire; a release
    /// that does not answer is abandoned at the bound.
    #[tokio::test]
    async fn a_commands_registration_is_released_within_the_bound() {
        let quick = Arc::new(Counting {
            released: AtomicUsize::new(0),
            takes: Duration::ZERO,
        });
        release_registration(Some(quick.clone() as Arc<dyn ServingRegistrar>)).await;
        assert_eq!(quick.released.load(Ordering::SeqCst), 1);

        let stalled = Arc::new(Counting {
            released: AtomicUsize::new(0),
            takes: Duration::from_secs(3600),
        });
        let bound = Duration::from_millis(100);
        let started = std::time::Instant::now();
        release_registration_within(Some(stalled.clone() as Arc<dyn ServingRegistrar>), bound)
            .await;
        assert_eq!(stalled.released.load(Ordering::SeqCst), 0);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );

        release_registration(None).await;
    }
}
