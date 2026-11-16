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
//! `backup migrate-legacy <arn>|--all` copies in-catalog backups into the
//! configured store and replaces each by its catalog row, under the
//! maintenance claim on every backend (on SQLite under the serve lock as
//! well); its module (`cmd_backup/migrate_legacy.rs`) states its output and
//! exit.
//!
//! Every command opens the catalog through the backend's factory, under its own
//! name (`extenddb backup list`, `extenddb backup purge`, `extenddb backup
//! remove-target`, `extenddb backup sync`, `extenddb backup migrate-legacy`),
//! and none runs the startup recovery
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
//! Every sync that listed the root to its end then reports the rows of the
//! stores other than the configured one: per store (kind, root, id) the
//! rows per status and each CREATING and DELETING row by ARN, which no
//! sync changes (`extenddb_engine::store_maintenance`'s module doc states
//! all three options below). `--move-store <old root>` re-points the rows
//! of a root whose tree was copied under the configured root: it needs the
//! configured store healthy, refuses the configured root itself and a root
//! with no rows (printing the stores the catalog knows), takes the claim
//! (on SQLite too) and waits for the jobs of the configured store and of
//! every store id of the old root, checks every row against the copy and
//! names each that stops the move, moves none while any does, and then
//! syncs the configured root; it exits 1 when a row stops it, is left
//! under the old root, or lands there during that sync (the closing report
//! of other stores names it). `--forget-store <root>` and `--forget-backup
//! <arn>` (repeatable, in order, up to the first refusal) remove rows
//! outright, delete no object (but for a DELETING row of the configured
//! store, whose deletion is finished first when the store's check passed),
//! and run on a store in any state and with no store configured, under the
//! claim on every backend; a configured root that cannot be opened (a
//! filesystem directory that does not exist) stops them before the claim,
//! as it stops plain sync, and `kind = "disabled"` runs them. They open the
//! store writing nothing to it ([`Mode::Forget`]). They exit 1 for a row they leave (a restore reading
//! it, a live owner, another spelling of the ARN, an in-catalog backup, a
//! DELETING row of an unhealthy configured store, a row of a configured
//! root that has no identity file under its rows, or on SQLite of the
//! default root found missing with rows for it). After `--forget-backup`
//! the configured store's check runs once and its outcome is printed: a
//! store left unhealthy by the removed row's missing manifest reads
//! healthy, or names the next row whose manifest is gone. The three
//! options conflict with each other, with `--adopt-store` and with
//! `--prune-missing`. Each row they change is logged under
//! `extenddb::audit`. Like sync's insert, each step of theirs that cannot be
//! undone is preceded by a beat of the claim (`HeldClaim::recheck`;
//! `extenddb_engine::store_maintenance`'s module doc lists the steps and the
//! bound that remains), and a claim found lost there stops the command,
//! exit 1, naming the step.
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
use extenddb_server::backup_store::NotConfigured;
use extenddb_storage::StorageEngine;
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, JOB_HEARTBEAT_INTERVAL, JobStatus, StoreIdentity, StoreKind,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::hooks::ServingRegistrar;
use futures::TryStreamExt;

mod migrate_legacy;

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
    /// Reports the prefixes it skips, the AVAILABLE rows whose manifest is gone and the rows of
    /// other stores. Refuses on an unhealthy store. Beside servers (PostgreSQL, MongoDB,
    /// Cassandra) it takes the maintenance claim; on SQLite it needs the server stopped.
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
        /// Move the rows of OLD_ROOT (its label as the catalog stores it, as backup list and
        /// the report of other stores print it) to the configured store, after the old root's
        /// tree was copied under the configured root. Reconfigure every server first. Checks
        /// each row against the copy and moves none while any row stops the move (all are
        /// named); then syncs. Takes the maintenance claim and waits for running jobs.
        #[arg(
            long,
            value_name = "OLD_ROOT",
            conflicts_with_all = ["adopt_store", "prune_missing", "forget_store", "forget_backup"]
        )]
        move_store: Option<String>,
        /// Remove every catalog row of ROOT, a root other than the configured one, outright:
        /// no tombstone is left and no object is deleted, so a later sync where the bytes are
        /// adopts them again. Runs on a store in any state and with no store configured; a
        /// configured root that cannot be opened (a directory that does not exist) stops it,
        /// and [backup.store] kind = "disabled" runs it. Takes the maintenance claim and waits
        /// for running jobs, as purge does; a row whose job writes a heartbeat after the wait is
        /// left and named.
        #[arg(
            long,
            value_name = "ROOT",
            conflicts_with_all = ["adopt_store", "prune_missing", "forget_backup"]
        )]
        forget_store: Option<String>,
        /// Remove the catalog row of the backup ARN outright, of any status and any store: no
        /// tombstone is left and no object is deleted, except that a DELETING row of the
        /// configured store has its deletion finished first; so a later sync where the bytes are
        /// adopts the backup again (DeleteBackup is the way to delete one). Repeat the option for
        /// more backups; they are removed in order, up to the first that is refused. Runs on a
        /// store in any state and with no store configured (a configured root that cannot be
        /// opened, such as a directory that does not exist, stops it, and [backup.store] kind =
        /// "disabled" runs it), and runs the store's check afterwards unless the root had no
        /// extenddb-store.json. Takes the maintenance claim and waits for
        /// running jobs, as purge does; a row whose job writes a heartbeat after the wait is
        /// left and named.
        #[arg(
            long,
            value_name = "ARN",
            action = clap::ArgAction::Append,
            conflicts_with_all = ["adopt_store", "prune_missing"]
        )]
        forget_backup: Vec<String>,
    },
    /// Copy in-catalog backups into the configured backup store, each under its own ARN, and
    /// replace each by its catalog row. Takes the maintenance claim first (every backup and
    /// restore job answers LimitExceededException until it ends) and waits for running jobs; on
    /// SQLite it needs the server stopped. Refuses a backup a 0.1.x release took (its backup id
    /// has the 13-digit shape a 0.1.x release issued) and one with an item the item check refuses
    /// or a definition the legacy restore refuses, keeping its in-catalog backup; restore such a
    /// backup and take a new one.
    /// With --all it then cleans up after deleted in-catalog backups: what a stopped run left
    /// under each one's prefix is deleted, and its record removed.
    #[command(group(clap::ArgGroup::new("backups").required(true).args(["arn", "all"])))]
    MigrateLegacy {
        /// The backup ARN of the in-catalog backup to migrate
        #[arg(value_name = "ARN", value_parser = migrate_legacy::backup_arn_arg)]
        arn: Option<String>,
        /// Migrate every in-catalog backup of every account, going on past a backup it refuses
        #[arg(long)]
        all: bool,
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
const MIGRATE_LEGACY: &str = "extenddb backup migrate-legacy";

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
    // The forget options run with no store configured too: the catalog is
    // kept open without one.
    let forgetting = matches!(
        &args.command,
        BackupCommand::Sync { forget_store, forget_backup, .. }
            if forget_store.is_some() || !forget_backup.is_empty()
    );
    let (command, mode) = match args.command {
        BackupCommand::List { .. } => (LIST, Mode::Read),
        BackupCommand::Purge { confirm: false } => (PURGE, Mode::Read),
        BackupCommand::Purge { confirm: true } => (PURGE, Mode::Write),
        BackupCommand::Sync { .. } if forgetting => (SYNC, Mode::Forget),
        BackupCommand::Sync { .. } => (SYNC, Mode::Sync),
        BackupCommand::MigrateLegacy { .. } => (MIGRATE_LEGACY, Mode::Write),
        BackupCommand::RemoveTarget { .. } => unreachable!("handled above"),
    };
    let opening = open(
        &app_config,
        &args.config,
        release,
        command,
        mode,
        forgetting,
    )
    .await?;
    if let Some(signal) = interrupt.signalled() {
        match opening {
            Opening::Store(opened) => opened.close().await,
            Opening::CatalogOnly(catalog, _) => catalog.close().await,
            Opening::NoStore(_) => {}
        }
        anyhow::bail!("{signal} received while opening the catalog; nothing was changed");
    }
    match (args.command, opening) {
        (
            BackupCommand::Sync {
                forget_store,
                forget_backup,
                ..
            },
            opening,
        ) if forgetting => {
            let (catalog, store) = match opening {
                Opening::Store(opened) => {
                    let Opened { catalog, identity } = opened;
                    (catalog, Ok(identity))
                }
                Opening::CatalogOnly(catalog, why) => (catalog, Err(why)),
                Opening::NoStore(why) => anyhow::bail!("{why}"),
            };
            let done = forget(
                &catalog,
                store.as_ref(),
                &app_config,
                forget_store.as_deref(),
                &forget_backup,
                interrupt,
            )
            .await;
            catalog.close().await;
            done
        }
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
                move_store,
                ..
            },
            Opening::Store(opened),
        ) => {
            let done = sync(
                &opened,
                &app_config,
                SyncOptions {
                    adopt_store,
                    prune_missing,
                    move_store: move_store.as_deref(),
                },
                interrupt,
            )
            .await;
            opened.close().await;
            done
        }
        (BackupCommand::Sync { .. }, Opening::NoStore(reason)) => anyhow::bail!("{reason}"),
        (BackupCommand::MigrateLegacy { arn, .. }, Opening::Store(opened)) => {
            let done = migrate_legacy::migrate_legacy(
                &opened,
                &app_config,
                release,
                arn.as_deref(),
                interrupt,
            )
            .await;
            opened.close().await;
            done
        }
        (BackupCommand::MigrateLegacy { .. }, Opening::NoStore(reason)) => {
            anyhow::bail!(
                "backup migrate-legacy needs a configured backup store to copy into, and this \
                 server has none ({reason})"
            )
        }
        (_, Opening::CatalogOnly(catalog, reason)) => {
            catalog.close().await;
            anyhow::bail!("{reason}")
        }
    }
}

/// What `open` found: the configured store, or why there is none (with the
/// catalog still open when the caller asked to keep it).
enum Opening {
    Store(Opened),
    NoStore(String),
    CatalogOnly(Catalog, NotConfigured),
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
    /// As `Write`, for `backup sync --forget-store` and `--forget-backup`,
    /// which write nothing to the root: the store is described as `Read`
    /// leaves it (no default root created, no `validate` and its probe
    /// file), the identity file is read and never written, and when it
    /// agrees with the rows the check that writes nothing
    /// (`check_configured_store_read_only`) runs once and is recorded, so
    /// a DELETING row of a healthy store can have its deletion finished.
    Forget,
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
) -> anyhow::Result<(Catalog, Option<NotConfigured>)> {
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
            Mode::Read | Mode::WriteCatalogOnly | Mode::Forget => {
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
    keep_without_store: bool,
) -> anyhow::Result<Opening> {
    let (catalog, not_configured) =
        open_catalog(app_config, config_path, release, command, mode).await?;
    let Some(store) = catalog.runtime.configured_store() else {
        // Why there is none, as `configure_described` found it: the section
        // absent or disabled, the default root not yet created, or the
        // default root failed its check with no rows for it.
        let why = not_configured.expect("a runtime without a store carries the reason");
        if keep_without_store {
            return Ok(Opening::CatalogOnly(catalog, why));
        }
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
        (Mode::Forget, _) => {
            match extenddb_engine::read_identity_bounded(store, &**storage).await {
                Ok(Some(identity)) => {
                    let check = extenddb_engine::check_configured_store_read_only(
                        store,
                        &**storage,
                        Some(&identity),
                    )
                    .await;
                    // A file gone since the read leaves the store checking:
                    // not healthy, so nothing is deleted.
                    match check {
                        Ok(Some(checked)) => {
                            store.record_check(Ok(checked));
                        }
                        Ok(None) => {}
                        Err(problem) => {
                            store.record_check(Err(problem));
                        }
                    }
                    Identity::Known(identity)
                }
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
    // The signal listener has been up since before the open, so a signal
    // from here on ends the command with the claim released: at once during
    // the wait, before the next row during the deletion.
    let claim = take_claim(storage, app_config, "purge").await?;

    // On SQLite the command holds the file alone: every job owner is dead
    // and no reconciler runs, so their resolution is this command's, and the
    // owner of any row is dead whatever its heartbeat says, so the window a
    // live owner is told apart by is zero. On the other backends purge runs
    // beside live servers, whose jobs heartbeat within `stale_after`.
    let owner_window = if opened.catalog.backend == "sqlite" {
        resolve_dead_owners(&opened.catalog).await;
        Duration::ZERO
    } else {
        stale_after
    };

    // Wait for the jobs the claim excludes from starting to finish.
    let wait_limit = stale_after * 2;
    let started = Instant::now();
    let outcome = wait_for_quiet(
        storage,
        std::slice::from_ref(identity),
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
async fn resolve_dead_owners(catalog: &Catalog) -> Vec<String> {
    resolve_dead_owners_with(
        catalog,
        extenddb_engine::reconciler::ReconcilerState::for_maintenance_holder(),
    )
    .await
}

/// [`resolve_dead_owners`] with the cycle's `state`; returns the ARNs of
/// the DELETING rows whose deletion the cycle finished.
async fn resolve_dead_owners_with(
    catalog: &Catalog,
    mut state: extenddb_engine::reconciler::ReconcilerState,
) -> Vec<String> {
    match extenddb_engine::reconciler::cycle(
        &catalog.storage,
        &catalog.runtime,
        Duration::ZERO,
        &catalog.backend,
        &mut state,
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
                // A removal of a backup, logged as every command's deletion
                // of one is.
                let account_id = extenddb_storage::util::check_backup_arn(arn)
                    .map(|parts| parts.account_id.to_owned())
                    .unwrap_or_default();
                tracing::info!(
                    target: "extenddb::audit",
                    action = "backup dead-owner resolution",
                    account_id = %account_id,
                    backup_arn = %arn,
                    "removed an in-catalog backup whose CreateBackup or DeleteBackup did not \
                     finish (its owner is dead)"
                );
                println!(
                    "removed the in-catalog backup {arn}: its CreateBackup or DeleteBackup did \
                     not finish"
                );
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
            report.deletions_finished
        }
        Err(e) => {
            println!("could not resolve the jobs the stopped server left: {e}");
            Vec::new()
        }
    }
}

/// Release the claim; a failure is printed, since the claim then expires by
/// the stale rule and the command's own outcome is what the operator needs.
/// `true` when the claim was released.
async fn release(claim: HeldClaim) -> bool {
    match claim.release().await {
        Ok(()) => true,
        Err(e) => {
            println!("could not release the maintenance claim ({e}); it expires on its own");
            false
        }
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

/// The options `sync` was given (`--forget-store` and `--forget-backup`
/// are [`forget`]'s).
#[derive(Clone, Copy)]
struct SyncOptions<'a> {
    adopt_store: bool,
    prune_missing: bool,
    move_store: Option<&'a str>,
}

/// `backup sync [--adopt-store] [--prune-missing] [--move-store <old root>]`
/// (module doc).
#[allow(clippy::too_many_lines)]
async fn sync(
    opened: &Opened,
    app_config: &config::AppConfig,
    options: SyncOptions<'_>,
    mut interrupt: Interrupt,
) -> anyhow::Result<()> {
    let SyncOptions {
        adopt_store,
        prune_missing,
        move_store,
    } = options;
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
             is unhealthy{}",
            configured.kind,
            configured.root,
            manifest_missing_hint(problem)
        ),
        Identity::Empty => anyhow::bail!(
            "the {} backup store {} has no {}: its check did not run",
            configured.kind,
            configured.root,
            extenddb_engine::STORE_IDENTITY_KEY
        ),
    };

    // The old root `--move-store` names: refused before the claim when it is
    // the configured root or has no rows.
    let moving = match move_store {
        Some(root) => {
            match extenddb_engine::store_maintenance::locate_root(
                storage,
                root,
                Some(&configured.root),
            )
            .await
            {
                Ok(located) => Some(located),
                Err(refusal) => return Err(root_refusal("--move-store", root, refusal)),
            }
        }
        None => None,
    };

    // On SQLite the command holds the serve lock, which already excludes
    // every server and every other command, so plain sync takes no claim
    // and resolves nothing a stopped server left: its rows are skipped, and
    // the next server start resolves them (and resumes a store-backed
    // restore, which the dead-owner resolutions would fail). Elsewhere every sync takes the
    // maintenance claim, so no purge removes a row between this sync's row
    // read and its insert and then deletes the bytes it adopted; plain sync
    // waits for no job, since its insert-if-absent orders it against a
    // CreateBackup. The options wait for running jobs as purge does,
    // `--move-store` for those of the old root's store ids too.
    let sqlite = opened.catalog.backend == "sqlite";
    let waits = adopt_store || prune_missing || moving.is_some();
    let mut claim = None;
    if !sqlite || waits {
        let backup = &app_config.backup;
        let stale_after = Duration::from_secs(backup.stale_job_after_secs);
        let held = take_claim(storage, app_config, "sync").await?;
        if waits {
            let mut identities = match &known {
                Some(identity) => vec![identity.clone()],
                None => match adopt_wait_identity(configured).await {
                    Ok(identity) => vec![identity],
                    Err(refusal) => {
                        release(held).await;
                        return Err(refusal);
                    }
                },
            };
            if let Some(located) = &moving {
                identities.extend(located.identities());
            }
            if let Err(refusal) = wait_for_jobs(
                &opened.catalog,
                &identities,
                &held,
                &mut interrupt,
                stale_after,
                false,
            )
            .await
            {
                release(held).await;
                return Err(refusal);
            }
        }
        claim = Some(held);
    }

    let done = sync_held(
        opened,
        app_config,
        known,
        adopting,
        prune_missing,
        moving.as_ref(),
        claim.as_ref(),
        &interrupt,
    )
    .await;
    if let Some(claim) = claim {
        release(claim).await;
    }
    done
}

/// Take the maintenance claim for `extenddb backup <command>` (`purge`,
/// `sync`, `migrate-legacy`) under `stale_job_after_secs`, print the
/// holder it was taken as, and start its heartbeat. A claim another holder
/// has is refused with that holder named.
pub(super) async fn take_claim(
    storage: &dyn StorageEngine,
    app_config: &config::AppConfig,
    command: &str,
) -> anyhow::Result<HeldClaim> {
    let backup = &app_config.backup;
    let stale_after = Duration::from_secs(backup.stale_job_after_secs);
    let holder = format!(
        "extenddb backup {command} on {} (pid {})",
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
    Ok(HeldClaim::start(taken, JOB_HEARTBEAT_INTERVAL))
}

/// The command's error for a root `--move-store` or `--forget-store`
/// (`option`) refused, the known stores printed first when the root has no
/// rows.
fn root_refusal(
    option: &str,
    root: &str,
    refusal: extenddb_engine::store_maintenance::RootRefusal,
) -> anyhow::Error {
    use extenddb_engine::store_maintenance::RootRefusal;
    match refusal {
        RootRefusal::ConfiguredRoot if option == "--move-store" => anyhow::anyhow!(
            "{option} {root}: that is the configured store's root, so nothing would move. Give \
             the root the rows were written under, as backup list printed it before the \
             configuration changed or as backup sync's report of other stores prints it"
        ),
        RootRefusal::ConfiguredRoot => anyhow::anyhow!(
            "{option} {root}: that is the configured store's root, which {option} refuses so \
             the store in use cannot lose its rows by a slip; extenddb backup purge deletes the \
             configured store's backups, and backup sync --prune-missing removes its AVAILABLE \
             rows whose manifest is gone"
        ),
        RootRefusal::NoRows(known) => {
            println!();
            println!("--- The stores the catalog holds backup rows for:");
            for line in known_store_lines(&known) {
                println!("{line}");
            }
            anyhow::anyhow!(
                "{option} {root}: the catalog has no backup row under that root (a root matches \
                 as the catalog stores it, byte for byte: a trailing / or an S3 root's \
                 ?endpoint= suffix makes another root); the roots it holds rows for are listed \
                 above"
            )
        }
        RootRefusal::Catalog(e) => anyhow::anyhow!("the catalog could not be read: {e}"),
    }
}

/// The identity `--adopt-store`'s wait reads rows of: the root's file id,
/// before the adoption as after it.
async fn adopt_wait_identity(configured: &ConfiguredStore) -> anyhow::Result<StoreIdentity> {
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
    Ok(StoreIdentity {
        store_kind: configured.kind,
        store_root: configured.root.clone(),
        store_id: file.store_id,
    })
}

/// The wait the options run under the claim, as purge's, over the rows of
/// `identities` and every RUNNING restore: on SQLite after the dead-owner
/// resolutions with a zero window, whose cycle runs the store's check
/// unless `writes_nothing` (the forget options, which record their own
/// check that writes nothing). Returns the DELETING rows whose deletion
/// that cycle finished. The refusal is the command's error, the claim
/// still held.
async fn wait_for_jobs(
    catalog: &Catalog,
    identities: &[StoreIdentity],
    claim: &HeldClaim,
    interrupt: &mut Interrupt,
    stale_after: Duration,
    writes_nothing: bool,
) -> anyhow::Result<Vec<String>> {
    let storage = &*catalog.storage;
    let mut finished = Vec::new();
    if catalog.backend == "sqlite" {
        let state = if writes_nothing {
            extenddb_engine::reconciler::ReconcilerState::for_maintenance_holder_without_store_check(
            )
        } else {
            extenddb_engine::reconciler::ReconcilerState::for_maintenance_holder()
        };
        finished = resolve_dead_owners_with(catalog, state).await;
    }
    let owner_window = owner_window(&catalog.backend, stale_after);
    let started = Instant::now();
    let outcome = wait_for_quiet(
        storage,
        identities,
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
        WaitOutcome::Quiet => Ok(finished),
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

/// The adoption, the move, the sync and the prune, with the claim held (on
/// PostgreSQL, MongoDB and Cassandra) or under the serve lock (SQLite); the
/// caller releases the claim.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn sync_held(
    opened: &Opened,
    app_config: &config::AppConfig,
    known: Option<StoreIdentity>,
    adopting: bool,
    prune_missing: bool,
    moving: Option<&extenddb_engine::store_maintenance::LocatedRoot>,
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
    if let Some(located) = moving {
        let owner_window = owner_window(
            &opened.catalog.backend,
            Duration::from_secs(app_config.backup.stale_job_after_secs),
        );
        let report = extenddb_engine::store_maintenance::move_store(
            storage,
            extenddb_engine::store_maintenance::MoveTarget {
                configured,
                identity: &identity,
                from_kind: located.kind,
                from_root: &located.root,
                owner_window,
                stop: &stop,
                claim,
            },
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "--move-store: the catalog's rows could not be read: {e}; no row changed"
            )
        })?;
        for line in move_report_lines(&report, located) {
            println!("{line}");
        }
        move_outcome(&report)?;
        println!();
        if report.gone.is_empty() {
            println!(
                "Every row of {} names the configured store now; syncing it.",
                located.root
            );
        } else {
            // The rows gone during the move name no store, or another
            // one: what the report established is that the old root has
            // no row left.
            println!(
                "No row is left under {} ({} moved, {} removed, or moved to another store, by \
                 another writer during the move); syncing the configured store.",
                located.root,
                report.moved.len(),
                report.gone.len()
            );
        }
    }
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
    // After a completed move, the run to repeat is plain sync: the old
    // root's rows are moved, and `--move-store` would refuse it as having no
    // rows.
    let again = run_again(moving.is_some());
    let report = match report {
        Ok(report) => report,
        Err(extenddb_engine::sync::SyncError::Listing { error, partial }) => {
            print_sync_report(&partial, prune_missing, false);
            anyhow::bail!(
                "the store's listing failed: {error}. {} backup(s) listed before the failure \
                 were adopted, and no AVAILABLE row was examined for a missing manifest; {again}",
                partial.adopted.len()
            );
        }
        Err(e) => anyhow::bail!("{e}"),
    };
    print_sync_report(&report, prune_missing, report.stopped.is_none());
    if let Some(reason) = &report.stopped {
        anyhow::bail!(
            "stopped: {reason}. {} backup(s) were adopted before the stop, and no AVAILABLE row \
             was examined for a missing manifest; {again} for the rest",
            report.adopted.len()
        );
    }
    // The rows of the other stores, read once the listing has ended.
    let others = extenddb_engine::store_maintenance::other_stores(storage, &identity).await;
    println!();
    let others_error = match &others {
        Ok(others) => {
            for line in other_stores_lines(others, &identity.store_root) {
                println!("{line}");
            }
            None
        }
        Err(e) => {
            println!("--- Rows of other stores: not read ({e})");
            Some(e.to_string())
        }
    };
    // After a move, a row of the old root in that report landed during the
    // move's sync (a CreateBackup of a server still configured with the old
    // root whose insert came after the move's re-list): named, and the
    // command exits 1.
    let landed = match (moving, &others) {
        (Some(located), Ok(others)) if old_root_listed(others, located) => {
            let rows = named_rows_of_root(storage, located).await;
            println!();
            println!("--- Rows that landed under the old root during the move:");
            match &rows {
                Ok(rows) if rows.is_empty() => println!("  (none listed now)"),
                Ok(rows) => {
                    for row in rows {
                        println!(
                            "  {} ({}, account {})",
                            row.backup_arn, row.status, row.account_id
                        );
                    }
                }
                Err(e) => println!("  (not read: {e})"),
            }
            Some(rows.map_or(0, |rows| rows.len()))
        }
        _ => None,
    };

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
    others_outcome(others_error.as_deref(), again)?;
    if let (Some(n), Some(located)) = (landed, moving) {
        anyhow::bail!(
            "a row landed under the old root during the move ({n} listed above): a CreateBackup \
             of a server still configured with {}. Reconfigure every server to the new root, \
             then, once the row's job has ended, copy its prefix under the new root (or remove \
             the row with --forget-backup) and run --move-store {} again",
            located.root,
            located.root
        );
    }
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

/// Whether the report of other stores lists a row of `located`'s root (an
/// entry count with no row behind it is not one).
fn old_root_listed(
    others: &[extenddb_engine::store_maintenance::OtherStore],
    located: &extenddb_engine::store_maintenance::LocatedRoot,
) -> bool {
    others.iter().any(|other| {
        other.store.store_kind == located.kind
            && other.store.store_root == located.root
            && !other.counts.is_empty()
    })
}

/// The rows of the old root, named for the operator.
async fn named_rows_of_root(
    storage: &dyn StorageEngine,
    located: &extenddb_engine::store_maintenance::LocatedRoot,
) -> Result<Vec<extenddb_engine::store_maintenance::NamedRow>, StorageError> {
    storage
        .list_backup_rows_by_root(located.kind, &located.root, None)
        .map_ok(|row| extenddb_engine::store_maintenance::NamedRow {
            status: row.status,
            backup_arn: row.backup_arn,
            account_id: row.account_id,
        })
        .try_collect()
        .await
}

/// The exit of a sync whose report of other stores failed (`error`, its
/// read's error): 1, since the report is part of the run; `again` is
/// [`run_again`]'s advice.
fn others_outcome(error: Option<&str>, again: &str) -> anyhow::Result<()> {
    match error {
        Some(e) => {
            anyhow::bail!("the rows of other stores could not be read: {e}; {again}")
        }
        None => Ok(()),
    }
}

/// The advice a sync that did not finish gives: run the command again, or,
/// after a completed `--move-store` (`moved`), plain `backup sync`, since
/// the old root's rows are moved and the option would refuse the root as
/// having no rows.
fn run_again(moved: bool) -> &'static str {
    if moved {
        "run extenddb backup sync (plain, without --move-store: the rows are moved, and the \
         option would refuse the old root as having no rows)"
    } else {
        "run the command again"
    }
}

/// The tail of plain sync's refusal on an unhealthy store: for a missing
/// manifest, the `--forget-backup` command for that backup.
fn manifest_missing_hint(problem: &StoreProblem) -> String {
    match problem {
        StoreProblem::ManifestMissing { backup_arn } => format!(
            "; check first that the root is the one that holds the backups: a root not shared \
             by every server (another host's directory at the same path) shows this too. When \
             that backup is gone for good, remove its row with extenddb backup sync \
             --forget-backup {backup_arn}, which runs on an unhealthy store and prints the \
             store's check afterwards"
        ),
        _ => String::new(),
    }
}

/// The window the options tell a live owner apart by: `stale_after` beside
/// servers; zero on SQLite, where the command holds the serve lock and no
/// job of another process can be alive.
fn owner_window(backend: &str, stale_after: Duration) -> Duration {
    if backend == "sqlite" {
        Duration::ZERO
    } else {
        stale_after
    }
}

/// The configured root's stores, one per store id the catalog records for
/// it (`ids`, the answer of `read_store_ids`): what the forget options wait
/// on when the configured store's identity is not known. A failed read
/// refuses the command before the claim, as the other catalog reads there
/// do, rather than leaving the wait with only the RUNNING restores to read.
fn configured_root_identities(
    kind: StoreKind,
    root: &str,
    ids: Result<Vec<String>, StorageError>,
) -> anyhow::Result<Vec<StoreIdentity>> {
    let ids = ids.map_err(|e| {
        anyhow::anyhow!(
            "the catalog's store ids for the configured root {root} could not be read: {e}; \
             nothing was changed"
        )
    })?;
    Ok(ids
        .into_iter()
        .map(|store_id| StoreIdentity {
            store_kind: kind,
            store_root: root.to_owned(),
            store_id,
        })
        .collect())
}

/// The stores the forget options wait on: the configured store when its
/// identity is known, otherwise every store id the catalog records for the
/// configured root (`configured_ids`), and every store id it records for
/// `--forget-store`'s root.
fn forget_wait_identities(
    known: Option<&StoreIdentity>,
    configured_ids: &[StoreIdentity],
    located: Option<&extenddb_engine::store_maintenance::LocatedRoot>,
) -> Vec<StoreIdentity> {
    let mut identities: Vec<StoreIdentity> = match known {
        Some(known) => vec![known.clone()],
        None => configured_ids.to_vec(),
    };
    if let Some(located) = located {
        identities.extend(located.identities());
    }
    identities
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

/// The counts of one store as a line's tail: `3 AVAILABLE, 1 DELETING`.
fn counts_text(counts: &[(BackupStatus, u64)]) -> String {
    counts
        .iter()
        .map(|(status, n)| format!("{n} {status}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The other-stores section of a sync's report: per store its counts per
/// status (on Cassandra, the status entries no row stands behind as such)
/// and its CREATING and DELETING rows, then the two options that act on
/// them. A store of `configured_root` under another store id is one the
/// two options refuse (the root is the configured one), so its line names
/// `--forget-backup` instead.
fn other_stores_lines(
    others: &[extenddb_engine::store_maintenance::OtherStore],
    configured_root: &str,
) -> Vec<String> {
    let mut lines = vec![
        "--- Rows of other stores (left as they are; sync reads and changes none of them):"
            .to_owned(),
    ];
    if others.is_empty() {
        lines.push("  (none)".to_owned());
        return lines;
    }
    let mut other_roots = false;
    for other in others {
        let mut tail = counts_text(&other.counts);
        if !other.entries.is_empty() {
            if !tail.is_empty() {
                tail.push_str("; ");
            }
            tail.push_str(&format!(
                "{} (status entries with no row of that status behind them, which readers skip; \
                 with no row of the root left, --forget-store <root> removes them)",
                counts_text(&other.entries)
            ));
        }
        if other.store.store_root == configured_root {
            tail.push_str(
                " (the configured root under another store id: --move-store and --forget-store \
                 refuse the configured root; remove such a row with extenddb backup sync \
                 --forget-backup <arn>)",
            );
        } else {
            other_roots = true;
        }
        lines.push(format!(
            "  {} {} (store id {}): {tail}",
            other.store.store_kind, other.store.store_root, other.store.store_id
        ));
        for row in &other.unresolved {
            lines.push(format!(
                "    {} {} (account {})",
                row.status, row.backup_arn, row.account_id
            ));
        }
    }
    if other_roots {
        lines.push(
            "  (to keep a root's backups, copy its tree under the configured root and run \
             extenddb backup sync --move-store <root>; to drop its rows, extenddb backup sync \
             --forget-store <root>)"
                .to_owned(),
        );
    }
    lines
}

/// The stores the catalog holds rows for, one line each, for a root that
/// has none.
fn known_store_lines(counts: &[extenddb_storage::backup_catalog::StoreRootCount]) -> Vec<String> {
    let stores = extenddb_engine::store_maintenance::group_counts(counts, None);
    if stores.is_empty() {
        return vec!["  (none)".to_owned()];
    }
    stores
        .iter()
        .map(|other| {
            format!(
                "  {} {} (store id {}): {}",
                other.store.store_kind,
                other.store.store_root,
                other.store.store_id,
                counts_text(&other.counts)
            )
        })
        .collect()
}

/// The report of `--move-store`.
fn move_report_lines(
    report: &extenddb_engine::store_maintenance::MoveReport,
    located: &extenddb_engine::store_maintenance::LocatedRoot,
) -> Vec<String> {
    let mut lines = vec![
        String::new(),
        format!(
            "--- --move-store {} ({}): {} row(s) under the old root, checked against the copy \
             under the configured root (the copy must be complete before the move)",
            located.root, located.kind, report.rows
        ),
    ];
    let mut section = |title: &str, items: Vec<String>| {
        lines.push(format!("--- {title}:"));
        if items.is_empty() {
            lines.push("  (none)".to_owned());
        }
        lines.extend(items.into_iter().map(|item| format!("  {item}")));
    };
    section(
        "Resolved (CREATING rows whose owner is dead, against the configured store)",
        report
            .resolved
            .iter()
            .map(|(arn, status)| format!("{arn}: now {status}"))
            .collect(),
    );
    section(
        "Finished (DELETING rows, their objects deleted under the configured root)",
        report
            .finished
            .iter()
            .map(|arn| format!("{arn}: now DELETED"))
            .collect(),
    );
    section(
        "Stopping the move (no row is moved while any is listed here)",
        report
            .stopping
            .iter()
            .map(|(row, why)| {
                format!(
                    "{} ({}, account {}): {why}",
                    row.backup_arn, row.status, row.account_id
                )
            })
            .collect(),
    );
    section(
        "Moved (each row now names the configured store)",
        report.moved.clone(),
    );
    if !report.gone.is_empty() {
        section(
            "Gone during the move (removed, or moved to another store, by another writer; \
             nothing moved)",
            report.gone.clone(),
        );
    }
    section(
        "Left under the old root",
        report
            .left
            .iter()
            .map(|row| {
                format!(
                    "{} ({}, account {})",
                    row.backup_arn, row.status, row.account_id
                )
            })
            .collect(),
    );
    lines.push(String::new());
    lines.push(format!(
        "=== extenddb backup sync --move-store {}: {} row(s) moved, {} resolved, {} \
         deletion(s) finished, {} row(s) stopping the move, {} gone during the move, {} left \
         under the old root ===",
        if report.complete() {
            "complete"
        } else {
            "stopped"
        },
        report.moved.len(),
        report.resolved.len(),
        report.finished.len(),
        report.stopping.len(),
        report.gone.len(),
        report.left.len()
    ));
    lines
}

/// The exit of `--move-store`: `Ok` when every row of the old root was
/// moved (the sync of the configured root follows), otherwise why not.
fn move_outcome(report: &extenddb_engine::store_maintenance::MoveReport) -> anyhow::Result<()> {
    if !report.stopping.is_empty() {
        anyhow::bail!(
            "--move-store moved no row: {} row(s) stop the move (see above). Resolve each and \
             run the command again; the CREATING rows resolved and the deletions finished are \
             kept",
            report.stopping.len()
        );
    }
    if let Some(reason) = &report.stopped {
        if report.rewritten == 0 {
            anyhow::bail!(
                "--move-store stopped before the rewrite: {reason}; no row was moved{}",
                relist_failure(report)
            );
        }
        anyhow::bail!(
            "--move-store stopped during the rewrite: {reason}{}",
            after_a_partial_rewrite(report)
        );
    }
    if let Some(e) = &report.rewrite_error {
        anyhow::bail!(
            "--move-store's rewrite failed: {e}{}",
            after_a_partial_rewrite(report)
        );
    }
    if let Some(e) = &report.relist_error {
        anyhow::bail!(
            "{e}; the rewrite counted {} row(s). Run extenddb backup sync, which lists any row \
             still under the old root in its report of other stores and syncs the configured \
             root; run --move-store again only if that report shows rows of the old root (with \
             none, it refuses the root as having no rows)",
            report.rewritten
        );
    }
    if !report.left.is_empty() {
        anyhow::bail!(
            "{} row(s) are still under the old root (see above): a CreateBackup of a server \
             still configured with that root, or a row another writer changed during the move. \
             Reconfigure every server to the new root, copy each such row's prefix under the new \
             root (or remove the row with --forget-backup), and run the command again",
            report.left.len()
        );
    }
    Ok(())
}

/// The listing's error after a stop or a failed rewrite, when the old
/// root's rows could not be listed again, with the sentence's separator.
fn relist_failure(report: &extenddb_engine::store_maintenance::MoveReport) -> String {
    report
        .relist_error
        .as_ref()
        .map_or_else(String::new, |e| format!("; {e}"))
}

/// What a stop or a failed rewrite leaves: the rows rewritten before it
/// (the count is what the rewrite returned, `moved` is empty when the
/// listing after it failed), and where the rows left under the old root
/// are.
fn after_a_partial_rewrite(report: &extenddb_engine::store_maintenance::MoveReport) -> String {
    format!(
        "; {} row(s) were rewritten{}, and the rows left under the old root {}: run the command \
         again for them",
        report.rewritten,
        relist_failure(report),
        if report.relist_error.is_some() {
            "could not be listed"
        } else {
            "are listed above"
        }
    )
}

/// The report of `--forget-store`.
fn forget_store_lines(
    report: &extenddb_engine::store_maintenance::ForgetStoreReport,
    located: &extenddb_engine::store_maintenance::LocatedRoot,
) -> Vec<String> {
    let mut lines = vec![
        String::new(),
        format!(
            "--- --forget-store {} ({}): rows removed outright; no object is deleted:",
            located.root, located.kind
        ),
    ];
    if report.rows.is_empty() {
        lines.push("  (none)".to_owned());
    }
    for (row, outcome) in &report.rows {
        lines.push(format!(
            "  {} ({}, account {}): {outcome}",
            row.backup_arn, row.status, row.account_id
        ));
    }
    let swept = report.entries_swept.unwrap_or_default();
    if swept.removed > 0 {
        lines.push(format!(
            "  ({} status entry(ies) of the root with no row of the root behind them removed: \
             entries a deleter wrote after a move of its row, and the earlier-status entries of \
             rows removed above)",
            swept.removed
        ));
    }
    if swept.repaired > 0 {
        lines.push(format!(
            "  ({} status entry(ies) of rows left under the root repaired: each was under an \
             earlier status than its row's, from a status change whose entry batch failed or \
             whose writer stopped before it, and is now under the row's status)",
            swept.repaired
        ));
    }
    if !report.entries_left.is_empty() {
        lines.push(format!(
            "  (the catalog still records store id(s) {} for the root: status entries written \
             after the root was listed, by a writer still configured with it; plain sync's \
             report of other stores shows them, and running the command again removes their \
             rows)",
            report.entries_left.join(", ")
        ));
    }
    // The rows the listing after the removals holds that the first listing
    // did not: each landed during the run, from a writer still configured
    // with the root, and is counted as left.
    let landed: Vec<&extenddb_engine::store_maintenance::NamedRow> = report
        .rows_left
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|left| {
            !report
                .rows
                .iter()
                .any(|(row, _)| row.backup_arn == left.backup_arn)
        })
        .collect();
    if !landed.is_empty() {
        lines.push(
            "--- Rows that landed under the root during the run (left; a writer is still \
                    configured with the root):"
                .to_owned(),
        );
        for row in landed {
            lines.push(format!(
                "  {} ({}, account {})",
                row.backup_arn, row.status, row.account_id
            ));
        }
    }
    lines.push(String::new());
    lines.push(format!(
        "=== extenddb backup sync --forget-store {}: {} row(s) removed, {} left ===",
        if report.complete() {
            "complete"
        } else if report.stopped.is_some() {
            "stopped"
        } else {
            "incomplete"
        },
        report.removed(),
        report.rows_left.as_ref().map_or_else(
            || "an unknown number".to_owned(),
            |left| left.len().to_string()
        )
    ));
    lines
}

/// The exit of `--forget-store`.
fn forget_store_outcome(
    report: &extenddb_engine::store_maintenance::ForgetStoreReport,
) -> anyhow::Result<()> {
    if let Some(reason) = &report.stopped {
        anyhow::bail!(
            "--forget-store stopped before the remaining rows: {reason}; run the command again \
             for the rest"
        );
    }
    if let Some(e) = &report.recheck_error {
        anyhow::bail!("{e}; run the command again to see what is left");
    }
    if !report.complete() {
        anyhow::bail!(
            "{} row(s) of the root are left (see above, each with why); run the command again \
             once their jobs have ended, and for a row in use once its restore has ended or its \
             target is removed with extenddb backup remove-target --account <id> <table>",
            report.rows_left.as_ref().map_or(0, Vec::len)
        );
    }
    Ok(())
}

/// The report of `--forget-backup`.
fn forget_backups_lines(
    report: &extenddb_engine::store_maintenance::ForgetBackupsReport,
    asked: &[String],
) -> Vec<String> {
    let mut lines = vec![
        String::new(),
        "--- --forget-backup (rows removed outright; no object is deleted, but a DELETING row \
         of the configured store has its deletion finished first):"
            .to_owned(),
    ];
    for (arn, outcome) in &report.done {
        lines.push(format!("  {arn}: {outcome}"));
    }
    for arn in asked.iter().skip(report.done.len()) {
        lines.push(format!("  {arn}: not reached"));
    }
    let removed = report
        .done
        .iter()
        .filter(|(_, o)| {
            matches!(
                o,
                extenddb_engine::store_maintenance::ForgetBackup::Row(_, forgotten)
                    if forgotten.is_removed()
            )
        })
        .count();
    lines.push(String::new());
    lines.push(format!(
        "=== extenddb backup sync --forget-backup {}: {removed} row(s) removed, {} not ===",
        if report.complete(asked.len()) {
            "complete"
        } else {
            "stopped"
        },
        asked.len() - report.done.iter().filter(|(_, o)| o.is_done()).count()
    ));
    lines
}

/// The exit of `--forget-backup`.
fn forget_backups_outcome(
    report: &extenddb_engine::store_maintenance::ForgetBackupsReport,
    asked: usize,
) -> anyhow::Result<()> {
    if let Some(reason) = &report.stopped {
        anyhow::bail!(
            "--forget-backup stopped before the remaining ARNs: {reason}; run the command again \
             for them"
        );
    }
    if !report.complete(asked) {
        let (arn, outcome) = report
            .done
            .last()
            .expect("an incomplete run without a stop ends at a refused ARN");
        anyhow::bail!(
            "--forget-backup did not remove the row of {arn} ({outcome}); the ARNs after it were \
             not reached"
        );
    }
    Ok(())
}

/// `backup sync --forget-store <root>` and `--forget-backup <arn>` (module
/// doc): on a store in any state, or with none configured (`identity` is
/// then why), under the maintenance claim on every backend.
#[allow(clippy::too_many_lines)]
async fn forget(
    catalog: &Catalog,
    identity: Result<&Identity, &NotConfigured>,
    app_config: &config::AppConfig,
    forget_store: Option<&str>,
    forget_backup: &[String],
    mut interrupt: Interrupt,
) -> anyhow::Result<()> {
    let storage = &*catalog.storage;
    let configured = catalog.runtime.configured_store();
    // The defaulted root missing (moved away, or a link to a volume that is
    // gone) while the catalog holds rows for it: its rows are refused as an
    // unmounted root's, and the header says so.
    let unmounted = match identity {
        Err(NotConfigured::DefaultRootMissingWithRows {
            label, store_ids, ..
        }) => Some((label.clone(), store_ids.clone())),
        _ => None,
    };
    println!("=== extenddb backup sync ===");
    // The check after the removals runs for `--forget-backup` alone, and
    // not when the root had no identity file as the command opened it
    // (`forget_check`), so the note says so only when it will run.
    let check_note =
        if forget_store.is_none() && !matches!(forget_check(identity), ForgetCheck::Skip) {
            "the removals read no store; the check after them does"
        } else {
            "the removals read no store"
        };
    let known = match (configured, identity) {
        (Some(store), Ok(Identity::Known(identity))) => {
            match store.health() {
                StoreHealth::Unhealthy(problem) => println!(
                    "Store:  {} {} (unhealthy: {problem}; {check_note})",
                    store.kind, store.root
                ),
                // The opening's read-only check gave no answer (the file
                // read, then gone before the check): not healthy, so a
                // DELETING row of this store is refused.
                StoreHealth::Checking => println!(
                    "Store:  {} {} (store id {}; check pending: the opening's check did not \
                     pass, so a DELETING row of this store is left; {check_note})",
                    store.kind, store.root, identity.store_id
                ),
                StoreHealth::Healthy => println!(
                    "Store:  {} {} (store id {})",
                    store.kind, store.root, identity.store_id
                ),
            }
            Some(identity.clone())
        }
        (Some(store), Ok(Identity::Problem(problem))) => {
            println!(
                "Store:  {} {} (unhealthy: {problem}; {check_note})",
                store.kind, store.root
            );
            None
        }
        (Some(store), Ok(Identity::Empty)) => {
            println!(
                "Store:  {} {} (no {})",
                store.kind,
                store.root,
                extenddb_engine::STORE_IDENTITY_KEY
            );
            None
        }
        (_, Err(_)) | (None, Ok(_)) => {
            match &unmounted {
                Some((root, ids)) => println!(
                    "Store:  none (the default root {root} is missing while the catalog holds \
                     rows for it, store id {}: a volume that is not mounted, or a directory moved \
                     away; this command refuses its rows and reads no store)",
                    ids.join(", ")
                ),
                None => println!(
                    "Store:  none ({}; the removals read no store)",
                    identity
                        .err()
                        .map_or_else(|| "not configured".to_owned(), ToString::to_string)
                ),
            }
            None
        }
    };
    // Every ARN's form before the claim: a typo costs no wait.
    let refused = extenddb_engine::store_maintenance::check_arns(forget_backup);
    if !refused.is_empty() {
        println!();
        println!("--- --forget-backup: ARNs refused before any change:");
        for (arn, why) in &refused {
            println!("  {arn:?}: {why}");
        }
        anyhow::bail!(
            "--forget-backup: {} ARN(s) given are not backup ARNs (see above); nothing was \
             changed",
            refused.len()
        );
    }
    let located = match forget_store {
        Some(root) => match extenddb_engine::store_maintenance::locate_root(
            storage,
            root,
            configured
                .map(|store| store.root.as_str())
                .or_else(|| unmounted.as_ref().map(|(root, _)| root.as_str())),
        )
        .await
        {
            Ok(located) => Some(located),
            Err(refusal) => return Err(root_refusal("--forget-store", root, refusal)),
        },
        None => None,
    };

    // The configured root's store ids when its identity is not known, so
    // the wait covers its rows too.
    let configured_ids = match (configured, &known) {
        (Some(store), None) => configured_root_identities(
            store.kind,
            &store.root,
            storage.read_store_ids(store.kind, &store.root).await,
        )?,
        _ => Vec::new(),
    };
    let stale_after = Duration::from_secs(app_config.backup.stale_job_after_secs);
    let claim = take_claim(storage, app_config, "sync").await?;
    let claimed_at = tokio::time::Instant::now();
    let identities = forget_wait_identities(known.as_ref(), &configured_ids, located.as_ref());
    let deletions_finished = match wait_for_jobs(
        catalog,
        &identities,
        &claim,
        &mut interrupt,
        stale_after,
        true,
    )
    .await
    {
        Ok(finished) => finished,
        Err(refusal) => {
            release(claim).await;
            return Err(refusal);
        }
    };
    let stop = || {
        claim.lost().or_else(|| {
            interrupt
                .signalled()
                .map(|signal| format!("{signal} received"))
        })
    };
    let on_wait = |remaining: Duration| {
        println!(
            "Waiting {} s before the first removal of a FAILED or DELETED row (or the \
             finished deletion of a DELETING one): a deletion a server's reconciler started before the claim was taken runs at most {} s, and its \
             last store call at most {} s more.",
            remaining.as_secs().max(1),
            extenddb_engine::reconciler::DUTY_DELETION_BOUND.as_secs(),
            extenddb_engine::S3_CHECK_TIMEOUT.as_secs()
        );
    };
    // The configured store's identity when its check passed (recorded at
    // the opening, writing nothing): a DELETING row of it has its deletion
    // finished before its removal.
    let healthy = configured.and_then(|store| store.identity().filter(|_| store.is_healthy()));
    let run = extenddb_engine::store_maintenance::ForgetRun {
        owner_window: owner_window(&catalog.backend, stale_after),
        removals_from: removals_from(&catalog.backend, claimed_at),
        on_wait: &on_wait,
        stop: &stop,
        claim: Some(&claim),
        configured: configured.map(
            |store| extenddb_engine::store_maintenance::ForgetConfigured {
                store,
                healthy: healthy.as_ref(),
                mismatch_row_ids: match identity {
                    Ok(Identity::Problem(problem)) => problem.mismatch_row_ids(),
                    _ => &[],
                },
                file_missing_with_rows: matches!(
                    identity,
                    Ok(Identity::Problem(StoreProblem::FileMissingWithRows(_)))
                ),
            },
        ),
        unmounted_root: unmounted.as_ref().map(|(root, _)| root.as_str()),
        deletions_finished: &deletions_finished,
    };
    let outcome = match &located {
        Some(located) => {
            let report = extenddb_engine::store_maintenance::forget_store(
                storage,
                located.kind,
                &located.root,
                run,
            )
            .await;
            match report {
                Ok(report) => {
                    for line in forget_store_lines(&report, located) {
                        println!("{line}");
                    }
                    forget_store_outcome(&report)
                }
                Err(e) => Err(anyhow::anyhow!(
                    "--forget-store: the root's rows could not be read: {e}; no row was removed"
                )),
            }
        }
        None => {
            let report = extenddb_engine::store_maintenance::forget_backups(
                storage,
                forget_backup,
                &app_config.server.region,
                run,
            )
            .await;
            for line in forget_backups_lines(&report, forget_backup) {
                println!("{line}");
            }
            forget_backups_outcome(&report, forget_backup.len())
        }
    };
    release(claim).await;

    // The store's check once more, as the next server check runs it but
    // writing nothing: a removed row may have been what kept the store
    // unhealthy.
    if let Some(store) = configured
        && forget_store.is_none()
    {
        println!();
        match forget_check(identity) {
            ForgetCheck::Skip => {
                let fresh = matches!(identity, Ok(Identity::Empty));
                for line in skipped_check_lines(&store.root, fresh) {
                    println!("{line}");
                }
            }
            ForgetCheck::Run(known) => {
                let check = extenddb_engine::check_configured_store_read_only(
                    store,
                    storage,
                    known.as_ref(),
                )
                .await;
                for line in store_check_lines(&check) {
                    println!("{line}");
                }
            }
        }
    }
    outcome
}

/// The instant before which a forget option removes no FAILED or DELETED
/// row: `FORGET_WAIT` after the claim was taken, beside servers; none on
/// SQLite, whose serve lock excludes every server.
fn removals_from(backend: &str, claimed_at: tokio::time::Instant) -> Option<tokio::time::Instant> {
    (backend != "sqlite").then(|| claimed_at + extenddb_engine::store_maintenance::FORGET_WAIT)
}

/// Whether the store's check runs after `--forget-backup`, and against
/// which identity.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ForgetCheck {
    /// The opening found no identity file under the root (with rows for
    /// it: an unmounted volume, say, whose rows the command refused; or
    /// none: a fresh root): the check is not run, so nothing is written to
    /// such a root.
    Skip,
    /// The read-only check, against the id the opening read when it read
    /// one.
    Run(Option<StoreIdentity>),
}

/// The check after `--forget-backup` for the store as the opening found
/// it.
fn forget_check(identity: Result<&Identity, &NotConfigured>) -> ForgetCheck {
    match identity {
        Ok(Identity::Known(identity)) => ForgetCheck::Run(Some(identity.clone())),
        Ok(Identity::Empty | Identity::Problem(StoreProblem::FileMissingWithRows(_))) => {
            ForgetCheck::Skip
        }
        Ok(Identity::Problem(_)) | Err(_) => ForgetCheck::Run(None),
    }
}

/// What `--forget-backup` prints when the opening found no identity file:
/// at a fresh root (`fresh`: no rows for it either), or at a root with rows
/// for it.
fn skipped_check_lines(root: &str, fresh: bool) -> Vec<String> {
    let key = extenddb_engine::STORE_IDENTITY_KEY;
    let opening = format!(
        "The configured store's check was not run: the root {root} had no {key} when the command \
         opened it, and this command writes nothing to a root."
    );
    if fresh {
        return vec![format!(
            "{opening} The root is fresh (the catalog has no rows for it either): a server's \
             first check creates its {key}."
        )];
    }
    vec![format!(
        "{opening} A server's check creates the file at a root the catalog has no rows for and \
         reports the store healthy, so mount the volume (or point [backup.store] at the root \
         that holds the backups) before the root's last row is gone."
    )]
}

/// The store's check after `--forget-backup`.
fn store_check_lines(check: &Result<Option<StoreIdentity>, StoreProblem>) -> Vec<String> {
    match check {
        Ok(Some(identity)) => vec![format!(
            "The configured store's check passes (store id {}); each server finds it so at its \
             next reconciler cycle.",
            identity.store_id
        )],
        Ok(None) => vec![format!(
            "The configured store's root has no {} and the catalog no rows for it; the check \
             writes nothing, and a server's next check creates the file.",
            extenddb_engine::STORE_IDENTITY_KEY
        )],
        Err(problem @ StoreProblem::ManifestMissing { backup_arn }) => vec![
            format!("The configured store's check fails: {problem}."),
            format!(
                "  If that backup is gone for good too, run extenddb backup sync \
                 --forget-backup {backup_arn}; then backup sync --prune-missing removes the \
                 remaining rows whose manifest is gone."
            ),
        ],
        Err(problem) => vec![format!("The configured store's check fails: {problem}.")],
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use extenddb_engine::StoreProblem;
    use extenddb_engine::store_maintenance::{
        ForgetBackup, ForgetBackupsReport, ForgetStoreReport, Forgotten, LocatedRoot, MoveReport,
        NamedRow, OtherStore,
    };
    use extenddb_storage::backup_catalog::{BackupStatus, StoreIdentity, StoreKind};
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

    /// `backup sync`'s options as clap parses them: the three store options
    /// conflict with each other, with `--adopt-store` and with
    /// `--prune-missing`; `--forget-backup` repeats, its ARNs in order.
    #[test]
    fn the_store_options_conflict_and_forget_backup_repeats() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(subcommand)]
            command: super::BackupCommand,
        }
        let parse = |args: &[&str]| {
            Cli::try_parse_from(std::iter::once("backup").chain(args.iter().copied()))
                .map(|cli| cli.command)
        };
        for refused in [
            &["sync", "--move-store", "/a", "--prune-missing"][..],
            &["sync", "--move-store", "/a", "--adopt-store"],
            &["sync", "--move-store", "/a", "--forget-store", "/b"],
            &["sync", "--move-store", "/a", "--forget-backup", "x"],
            &["sync", "--forget-store", "/b", "--forget-backup", "x"],
            &["sync", "--forget-store", "/b", "--prune-missing"],
            &["sync", "--forget-store", "/b", "--adopt-store"],
            &["sync", "--forget-backup", "x", "--prune-missing"],
            &["sync", "--forget-backup", "x", "--adopt-store"],
        ] {
            let Err(e) = parse(refused) else {
                panic!("{refused:?} parsed");
            };
            assert_eq!(
                e.kind(),
                clap::error::ErrorKind::ArgumentConflict,
                "{refused:?}"
            );
        }
        match parse(&["sync", "--forget-backup", "a", "--forget-backup", "b"]).unwrap() {
            super::BackupCommand::Sync {
                forget_backup,
                move_store: None,
                forget_store: None,
                adopt_store: false,
                prune_missing: false,
            } => assert_eq!(forget_backup, ["a", "b"]),
            _ => panic!("not a sync"),
        }
        match parse(&["sync", "--move-store", "s3://b/p?endpoint=http://h:1"]).unwrap() {
            super::BackupCommand::Sync {
                move_store: Some(root),
                ..
            } => assert_eq!(root, "s3://b/p?endpoint=http://h:1"),
            _ => panic!("not a move"),
        }
    }

    fn named(status: BackupStatus, arn: &str) -> NamedRow {
        NamedRow {
            status,
            backup_arn: arn.to_owned(),
            account_id: "123456789012".to_owned(),
        }
    }

    fn old_root() -> LocatedRoot {
        LocatedRoot {
            kind: StoreKind::Filesystem,
            root: "/srv/old".to_owned(),
            store_ids: vec!["store-0".to_owned()],
        }
    }

    /// The other-stores section: `(none)` without any; otherwise a line per
    /// store with its counts, its CREATING and DELETING rows below it, and
    /// the line naming the two options. A store of the configured root
    /// under another store id, which both options refuse, names
    /// `--forget-backup` on its own line instead, and the options' line is
    /// printed only when a root other than the configured one is listed.
    #[test]
    fn the_other_stores_section_counts_names_and_points_to_the_options() {
        assert_eq!(
            super::other_stores_lines(&[], "/srv/new"),
            [
                "--- Rows of other stores (left as they are; sync reads and changes none of them):",
                "  (none)",
            ]
        );
        let old = OtherStore {
            store: StoreIdentity {
                store_kind: StoreKind::Filesystem,
                store_root: "/srv/old".to_owned(),
                store_id: "store-0".to_owned(),
            },
            counts: vec![(BackupStatus::Available, 3), (BackupStatus::Deleting, 1)],
            unresolved: vec![named(BackupStatus::Deleting, "arn:d")],
            entries: Vec::new(),
        };
        let lines = super::other_stores_lines(std::slice::from_ref(&old), "/srv/new");
        assert_eq!(
            lines[1],
            "  filesystem /srv/old (store id store-0): 3 AVAILABLE, 1 DELETING"
        );
        assert_eq!(lines[2], "    DELETING arn:d (account 123456789012)");
        assert!(
            lines[3].contains("--move-store <root>") && lines[3].contains("--forget-store <root>"),
            "{}",
            lines[3]
        );
        assert_eq!(lines.len(), 4);

        // The configured root under another id, alone: no options' line.
        let same_root = OtherStore {
            store: StoreIdentity {
                store_kind: StoreKind::Filesystem,
                store_root: "/srv/new".to_owned(),
                store_id: "store-9".to_owned(),
            },
            counts: vec![(BackupStatus::Available, 2)],
            unresolved: Vec::new(),
            entries: Vec::new(),
        };
        let lines = super::other_stores_lines(std::slice::from_ref(&same_root), "/srv/new");
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(
            lines[1].starts_with(
                "  filesystem /srv/new (store id store-9): 2 AVAILABLE (the \
                                  configured root under another store id: --move-store and \
                                  --forget-store refuse the configured root; remove such a row \
                                  with extenddb backup sync --forget-backup <arn>)"
            ),
            "{}",
            lines[1]
        );
        assert!(!lines.iter().any(|l| l.contains("--move-store <root>")));

        // Beside another root: the options' line names that root's options.
        let lines = super::other_stores_lines(&[old, same_root], "/srv/new");
        assert_eq!(lines.len(), 5, "{lines:?}");
        assert!(lines[3].contains("--forget-backup <arn>"), "{}", lines[3]);
        assert!(lines[4].contains("--move-store <root>"), "{}", lines[4]);
    }

    /// The move exits 0 only when every row moved; a stopping row, a stop,
    /// a failed rewrite and a row left each exit 1 with their own sentence,
    /// and the summary line says which.
    #[test]
    fn the_move_exits_1_unless_every_row_moved() {
        let moved = MoveReport {
            rows: 2,
            moved: vec!["arn:1".to_owned(), "arn:2".to_owned()],
            rewritten: 2,
            ..MoveReport::default()
        };
        super::move_outcome(&moved).unwrap();
        let summary = super::move_report_lines(&moved, &old_root());
        assert_eq!(
            summary.last().unwrap(),
            "=== extenddb backup sync --move-store complete: 2 row(s) moved, 0 resolved, 0 \
             deletion(s) finished, 0 row(s) stopping the move, 0 gone during the move, 0 left \
             under the old root ==="
        );
        let gone = MoveReport {
            rows: 3,
            moved: vec!["arn:1".to_owned(), "arn:2".to_owned()],
            gone: vec!["arn:3".to_owned()],
            rewritten: 3,
            ..MoveReport::default()
        };
        assert!(
            super::move_report_lines(&gone, &old_root())
                .last()
                .unwrap()
                .contains(
                    "2 row(s) moved, 0 resolved, 0 deletion(s) finished, 0 row(s) stopping \
                           the move, 1 gone during the move, 0 left under the old root"
                ),
            "the summary line counts the rows gone during the move"
        );
        let stopping = MoveReport {
            stopping: vec![(named(BackupStatus::Available, "arn:1"), "why".to_owned())],
            ..MoveReport::default()
        };
        let err = super::move_outcome(&stopping).unwrap_err().to_string();
        assert!(
            err.starts_with("--move-store moved no row: 1 row(s) stop the move"),
            "{err}"
        );
        assert!(
            super::move_report_lines(&stopping, &old_root())
                .contains(&"  arn:1 (AVAILABLE, account 123456789012): why".to_owned())
        );
        for (report, says) in [
            (
                MoveReport {
                    stopped: Some("SIGINT received".to_owned()),
                    ..MoveReport::default()
                },
                "stopped before the rewrite: SIGINT received",
            ),
            (
                MoveReport {
                    rewrite_error: Some("lost".to_owned()),
                    ..MoveReport::default()
                },
                "rewrite failed: lost",
            ),
            (
                MoveReport {
                    left: vec![named(BackupStatus::Creating, "arn:3")],
                    ..MoveReport::default()
                },
                "1 row(s) are still under the old root",
            ),
        ] {
            let err = super::move_outcome(&report).unwrap_err().to_string();
            assert!(err.contains(says), "{err}");
            assert!(
                super::move_report_lines(&report, &old_root())
                    .last()
                    .unwrap()
                    .starts_with("=== extenddb backup sync --move-store stopped:")
            );
        }
    }

    /// A stop between two slices with the old root's listing then failing:
    /// `moved` is empty (the listing is what fills it) while the rewrite
    /// counted 500 keys, so the exit says that 500 rows were rewritten, not
    /// that no row was moved, and prints the listing's error; a stop
    /// before the first slice says no row was moved and prints it too. A
    /// failed rewrite with the listing failing says the same of its count.
    #[test]
    fn a_stop_with_a_failed_listing_counts_the_rewritten_rows_and_prints_the_listings_error() {
        let relist = "the old root's rows could not be read after the rewrite: connection reset";
        let stopped = MoveReport {
            rows: 1000,
            stopped: Some("the maintenance claim was lost".to_owned()),
            rewritten: 500,
            relist_error: Some(relist.to_owned()),
            ..MoveReport::default()
        };
        let err = super::move_outcome(&stopped).unwrap_err().to_string();
        assert_eq!(
            err,
            format!(
                "--move-store stopped during the rewrite: the maintenance claim was lost; 500 \
                 row(s) were rewritten; {relist}, and the rows left under the old root could not \
                 be listed: run the command again for them"
            )
        );
        let before = MoveReport {
            rows: 1000,
            stopped: Some("SIGTERM received".to_owned()),
            relist_error: Some(relist.to_owned()),
            ..MoveReport::default()
        };
        let err = super::move_outcome(&before).unwrap_err().to_string();
        assert_eq!(
            err,
            format!(
                "--move-store stopped before the rewrite: SIGTERM received; no row was moved; {relist}"
            )
        );
        let listed = MoveReport {
            rows: 1000,
            stopped: Some("SIGTERM received".to_owned()),
            moved: (0..500).map(|i| format!("arn:{i}")).collect(),
            rewritten: 500,
            ..MoveReport::default()
        };
        let err = super::move_outcome(&listed).unwrap_err().to_string();
        assert_eq!(
            err,
            "--move-store stopped during the rewrite: SIGTERM received; 500 row(s) were \
             rewritten, and the rows left under the old root are listed above: run the command \
             again for them"
        );
        let failed = MoveReport {
            rows: 1000,
            rewrite_error: Some("lost".to_owned()),
            rewritten: 500,
            relist_error: Some(relist.to_owned()),
            ..MoveReport::default()
        };
        let err = super::move_outcome(&failed).unwrap_err().to_string();
        assert_eq!(
            err,
            format!(
                "--move-store's rewrite failed: lost; 500 row(s) were rewritten; {relist}, and \
                 the rows left under the old root could not be listed: run the command again for \
                 them"
            )
        );
    }

    /// `--forget-store` exits 0 once the root lists no row, entries left
    /// behind (Cassandra) reported with the exit kept; a row left, a stop
    /// and a failed re-read exit 1.
    #[test]
    fn forget_store_exits_1_while_a_row_of_the_root_is_left() {
        let done = ForgetStoreReport {
            rows: vec![(
                named(BackupStatus::Failed, "arn:1"),
                Forgotten::Removed(BackupStatus::Failed),
            )],
            rows_left: Some(Vec::new()),
            entries_left: vec!["store-0".to_owned()],
            ..ForgetStoreReport::default()
        };
        super::forget_store_outcome(&done).unwrap();
        let lines = super::forget_store_lines(&done, &old_root());
        assert!(
            lines.iter().any(|l| l.contains("store id(s) store-0")),
            "{lines:?}"
        );
        assert_eq!(
            lines.last().unwrap(),
            "=== extenddb backup sync --forget-store complete: 1 row(s) removed, 0 left ==="
        );
        let left = ForgetStoreReport {
            rows: vec![(
                named(BackupStatus::Creating, "arn:2"),
                Forgotten::LiveOwner(BackupStatus::Creating),
            )],
            rows_left: Some(vec![
                named(BackupStatus::Creating, "arn:2"),
                named(BackupStatus::Available, "arn:3"),
            ]),
            ..ForgetStoreReport::default()
        };
        let err = super::forget_store_outcome(&left).unwrap_err().to_string();
        assert!(err.starts_with("2 row(s) of the root are left"), "{err}");
        assert!(
            err.contains("extenddb backup remove-target --account"),
            "{err}"
        );
        let lines = super::forget_store_lines(&left, &old_root());
        // The row the listing after the removals holds and the first did
        // not (arn:3) is named as landed; arn:2 is named above it, once.
        let landed = lines
            .iter()
            .position(|l| l.starts_with("--- Rows that landed under the root during the run"))
            .expect("the landed section");
        assert_eq!(
            lines[landed + 1],
            "  arn:3 (AVAILABLE, account 123456789012)"
        );
        assert_eq!(
            lines.iter().filter(|l| l.contains("arn:2")).count(),
            1,
            "{lines:?}"
        );
        assert_eq!(
            lines.last().unwrap(),
            "=== extenddb backup sync --forget-store incomplete: 0 row(s) removed, 2 left ==="
        );
        let swept = ForgetStoreReport {
            rows_left: Some(Vec::new()),
            entries_swept: Some(extenddb_storage::backup_catalog::StatusEntrySweep {
                removed: 2,
                repaired: 1,
            }),
            ..ForgetStoreReport::default()
        };
        let lines = super::forget_store_lines(&swept, &old_root());
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("  (2 status entry(ies) of the root"))
                && lines
                    .iter()
                    .any(|l| l.starts_with("  (1 status entry(ies) of rows left")),
            "{lines:?}"
        );
        let stopped = ForgetStoreReport {
            stopped: Some("SIGTERM received".to_owned()),
            ..ForgetStoreReport::default()
        };
        assert!(super::forget_store_outcome(&stopped).is_err());
        let unread = ForgetStoreReport {
            recheck_error: Some("the root's rows could not be read: down".to_owned()),
            ..ForgetStoreReport::default()
        };
        assert!(super::forget_store_outcome(&unread).is_err());
    }

    /// `--forget-backup` exits 0 when every ARN's row is gone, and 1 naming
    /// the ARN it stopped at (a legacy ARN included); the ARNs after it are
    /// listed as not reached.
    #[test]
    fn forget_backup_exits_1_naming_the_arn_it_stopped_at() {
        let asked = ["arn:1".to_owned(), "arn:2".to_owned(), "arn:3".to_owned()];
        let removed = |arn: &str| {
            (
                arn.to_owned(),
                ForgetBackup::Row(
                    BackupStatus::Available,
                    Forgotten::Removed(BackupStatus::Available),
                ),
            )
        };
        let all = ForgetBackupsReport {
            done: asked.iter().map(|a| removed(a)).collect(),
            stopped: None,
        };
        super::forget_backups_outcome(&all, 3).unwrap();
        // An ARN no row carries counts as done: a run given again after a
        // stop goes past the ARNs it removed.
        let again = ForgetBackupsReport {
            done: vec![
                ("arn:1".to_owned(), ForgetBackup::NoRow),
                removed("arn:2"),
                removed("arn:3"),
            ],
            stopped: None,
        };
        super::forget_backups_outcome(&again, 3).unwrap();
        let lines = super::forget_backups_lines(&again, &asked);
        assert!(
            lines.contains(&"  arn:1: no row carries this ARN; nothing to remove".to_owned()),
            "{lines:?}"
        );
        assert_eq!(
            lines.last().unwrap(),
            "=== extenddb backup sync --forget-backup complete: 2 row(s) removed, 0 not ==="
        );
        let refused = ForgetBackupsReport {
            done: vec![removed("arn:1"), ("arn:2".to_owned(), ForgetBackup::Legacy)],
            stopped: None,
        };
        let err = super::forget_backups_outcome(&refused, 3)
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with(
                "--forget-backup did not remove the row of arn:2 (an in-catalog backup"
            ),
            "{err}"
        );
        let lines = super::forget_backups_lines(&refused, &asked);
        assert!(
            lines.contains(&"  arn:3: not reached".to_owned()),
            "{lines:?}"
        );
        assert_eq!(
            lines.last().unwrap(),
            "=== extenddb backup sync --forget-backup stopped: 1 row(s) removed, 2 not ==="
        );
    }

    /// The store's check after `--forget-backup`: passing, or the problem;
    /// a manifest still missing names the next ARN to forget.
    #[test]
    fn the_check_after_forget_backup_names_the_next_backup_to_forget() {
        let ok = super::store_check_lines(&Ok(Some(StoreIdentity {
            store_kind: StoreKind::Filesystem,
            store_root: "/r".to_owned(),
            store_id: "store-1".to_owned(),
        })));
        assert!(ok[0].starts_with("The configured store's check passes (store id store-1)"));
        let empty = super::store_check_lines(&Ok(None));
        assert!(
            empty[0].contains("the check writes nothing, and a server's next check creates"),
            "{empty:?}"
        );
        let missing = super::store_check_lines(&Err(StoreProblem::ManifestMissing {
            backup_arn: "arn:next".to_owned(),
        }));
        assert!(
            missing[1].contains("--forget-backup arn:next"),
            "{missing:?}"
        );
    }

    /// The check after `--forget-backup` runs against the id the opening
    /// read, without one when the opening read none, and not at all when
    /// the opening found no identity file (`Empty`, which the forget
    /// options' opening reads for a fresh root since it writes nothing, or
    /// `FileMissingWithRows`); the line printed then says to mount the
    /// volume before the root's last row is gone.
    #[test]
    fn the_check_after_forget_backup_runs_only_where_the_opening_found_a_file() {
        use super::{ForgetCheck, Identity, forget_check};
        let identity = StoreIdentity {
            store_kind: StoreKind::Filesystem,
            store_root: "/r".to_owned(),
            store_id: "store-1".to_owned(),
        };
        assert_eq!(
            forget_check(Ok(&Identity::Known(identity.clone()))),
            ForgetCheck::Run(Some(identity))
        );
        assert_eq!(forget_check(Ok(&Identity::Empty)), ForgetCheck::Skip);
        assert_eq!(
            forget_check(Ok(&Identity::Problem(StoreProblem::FileMissingWithRows(
                vec!["store-1".to_owned()]
            )))),
            ForgetCheck::Skip
        );
        assert_eq!(
            forget_check(Ok(&Identity::Problem(StoreProblem::ManifestMissing {
                backup_arn: "arn:gone".to_owned()
            }))),
            ForgetCheck::Run(None)
        );
        assert_eq!(
            forget_check(Err(&super::NotConfigured::Absent)),
            ForgetCheck::Run(None)
        );
        let skipped = &super::skipped_check_lines("/mnt/b", false)[0];
        assert!(skipped.contains("/mnt/b had no"), "{skipped}");
        assert!(
            skipped.contains("before the root's last row is gone")
                && !skipped.contains("each server's next check reports the store"),
            "{skipped}"
        );
        let fresh = &super::skipped_check_lines("/mnt/c", true)[0];
        assert!(
            fresh.contains("/mnt/c had no")
                && fresh.contains("The root is fresh")
                && !fresh.contains("mount the volume"),
            "{fresh}"
        );
    }

    /// After a move, a row of the old root in the closing report of other
    /// stores makes the command exit 1; counts of status entries with no
    /// row behind them, or rows of another root, do not.
    #[test]
    fn a_row_of_the_old_root_in_the_closing_report_is_one_that_landed() {
        let root = old_root();
        let other = |root: &str, counts: Vec<(BackupStatus, u64)>, entries| OtherStore {
            store: StoreIdentity {
                store_kind: StoreKind::Filesystem,
                store_root: root.to_owned(),
                store_id: "store-0".to_owned(),
            },
            counts,
            unresolved: Vec::new(),
            entries,
        };
        assert!(super::old_root_listed(
            &[other(
                "/srv/old",
                vec![(BackupStatus::Creating, 1)],
                Vec::new()
            )],
            &root
        ));
        assert!(!super::old_root_listed(
            &[other(
                "/srv/old",
                Vec::new(),
                vec![(BackupStatus::Deleting, 1)]
            )],
            &root
        ));
        assert!(!super::old_root_listed(
            &[other(
                "/srv/elsewhere",
                vec![(BackupStatus::Available, 2)],
                Vec::new()
            )],
            &root
        ));
        let lines = super::other_stores_lines(
            &[other(
                "/srv/old",
                vec![(BackupStatus::Available, 1)],
                vec![(BackupStatus::Deleting, 1)],
            )],
            "/srv/new",
        );
        assert_eq!(
            lines[1],
            "  filesystem /srv/old (store id store-0): 1 AVAILABLE; 1 DELETING (status entries \
             with no row of that status behind them, which readers skip; with no row of the \
             root left, --forget-store <root> removes them)"
        );
    }

    /// The decisions the options take from the backend and the located
    /// root: the owner window (zero on SQLite under the serve lock), the
    /// wait before the first FAILED or DELETED removal (none on SQLite), and
    /// the stores the forget options wait on (the configured store's and
    /// the root's ids).
    #[test]
    fn the_options_take_the_window_the_wait_and_the_stores_from_the_backend() {
        let stale = std::time::Duration::from_secs(300);
        assert_eq!(
            super::owner_window("sqlite", stale),
            std::time::Duration::ZERO
        );
        for backend in ["postgres", "mongodb", "cassandra"] {
            assert_eq!(super::owner_window(backend, stale), stale, "{backend}");
        }
        let now = tokio::time::Instant::now();
        assert_eq!(super::removals_from("sqlite", now), None);
        assert_eq!(
            super::removals_from("postgres", now),
            Some(now + extenddb_engine::store_maintenance::FORGET_WAIT)
        );
        let configured = StoreIdentity {
            store_kind: StoreKind::Filesystem,
            store_root: "/srv/new".to_owned(),
            store_id: "store-1".to_owned(),
        };
        let root = old_root();
        assert_eq!(
            super::forget_wait_identities(Some(&configured), &[], Some(&root)),
            [configured.clone(), root.identities()[0].clone()]
        );
        assert_eq!(
            super::forget_wait_identities(None, &[], Some(&root)),
            root.identities()
        );
        assert_eq!(
            super::forget_wait_identities(Some(&configured), &[], None),
            [configured]
        );
        assert!(super::forget_wait_identities(None, &[], None).is_empty());
        let ids = [
            StoreIdentity {
                store_kind: StoreKind::Filesystem,
                store_root: "/srv/new".to_owned(),
                store_id: "store-1".to_owned(),
            },
            StoreIdentity {
                store_kind: StoreKind::Filesystem,
                store_root: "/srv/new".to_owned(),
                store_id: "store-2".to_owned(),
            },
        ];
        assert_eq!(
            super::forget_wait_identities(None, &ids, Some(&root)),
            [ids[0].clone(), ids[1].clone(), root.identities()[0].clone()],
            "the configured root's ids when its identity is not known"
        );
        assert_eq!(
            super::forget_wait_identities(Some(&ids[0]), &ids, None),
            [ids[0].clone()]
        );
    }

    /// The configured root's stores the forget options wait on when its
    /// identity is not known: one per id the catalog records, and a failed
    /// read of those ids refuses the command.
    #[test]
    fn a_failed_read_of_the_configured_roots_ids_refuses_the_forget() {
        let ids = super::configured_root_identities(
            StoreKind::Filesystem,
            "/srv/new",
            Ok(vec!["store-1".to_owned(), "store-2".to_owned()]),
        )
        .unwrap();
        assert_eq!(
            ids.iter().map(|i| i.store_id.as_str()).collect::<Vec<_>>(),
            ["store-1", "store-2"]
        );
        assert!(ids.iter().all(|i| i.store_root == "/srv/new"));
        let err = super::configured_root_identities(
            StoreKind::Filesystem,
            "/srv/new",
            Err(extenddb_storage::error::StorageError::Internal(
                "pool exhausted".to_owned(),
            )),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("store ids for the configured root /srv/new could not be read")
                && err.contains("pool exhausted")
                && err.ends_with("nothing was changed"),
            "{err}"
        );
    }

    /// A sync whose report of other stores failed exits 1; plain sync's
    /// refusal on a missing manifest names the backup's own
    /// `--forget-backup` command.
    #[test]
    fn a_failed_report_of_other_stores_exits_1_and_the_refusal_names_the_arn() {
        super::others_outcome(None, super::run_again(false)).unwrap();
        let err = super::others_outcome(Some("down"), super::run_again(false))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "the rows of other stores could not be read: down; run the command again"
        );
        // After a completed move the advice is plain sync: `--move-store`
        // would refuse the old root as having no rows.
        let err = super::others_outcome(Some("down"), super::run_again(true))
            .unwrap_err()
            .to_string();
        assert!(
            err.ends_with(
                "; run extenddb backup sync (plain, without --move-store: the rows are moved, \
                 and the option would refuse the old root as having no rows)"
            ),
            "{err}"
        );
        let hint = super::manifest_missing_hint(&StoreProblem::ManifestMissing {
            backup_arn: "arn:aws:dynamodb:us-east-1:1:table/T/backup/01".to_owned(),
        });
        assert!(
            hint.contains("--forget-backup arn:aws:dynamodb:us-east-1:1:table/T/backup/01,"),
            "{hint}"
        );
        assert!(super::manifest_missing_hint(&StoreProblem::Transport("x".to_owned())).is_empty());
    }
}
