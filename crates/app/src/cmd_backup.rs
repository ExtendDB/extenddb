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
//! store has a live owner (a heartbeat within `stale_job_after_secs`; a
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
//! the reconciler's duties itself with a zero stale window: a legacy restore
//! the stopped server left RUNNING is failed and its target removed, a
//! target a FAILED restore left CREATING is removed, an in-catalog CREATING
//! backup it left is removed, and a DELETING row is finished; the wait and
//! the CREATING-row removal then also use a zero window, so a CREATING or
//! FAILED row whose heartbeat is young (the stopped server's job wrote it)
//! is neither waited on nor left, and the wait has nothing to wait for. The
//! server's own startup under the same lock clears a maintenance claim a
//! stopped command left, for the same reason. On the other backends
//! purge runs beside live servers; the maintenance claim is the exclusion.
//!
//! `extenddb destroy` removes the databases and leaves the store untouched:
//! the store is the copy that survives destroy, and this command is the one
//! that deletes it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use extenddb_config as config;
use extenddb_engine::maintenance::{Interrupt, Wait, WaitOutcome, purge_row, wait_for_quiet};
use extenddb_engine::{BackupRuntime, ConfiguredStore, HeldClaim, StoreHealth, StoreProblem};
use extenddb_storage::StorageEngine;
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, JOB_HEARTBEAT_INTERVAL, StoreIdentity,
};
use extenddb_storage::error::StorageError;
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
}

/// How often the waiting purge re-reads the rows.
const WAIT_POLL: Duration = Duration::from_secs(2);

pub async fn run(args: BackupArgs, release: &'static str) -> anyhow::Result<()> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found; use --config <path> to specify its location.",
            args.config,
        );
    }
    let app_config = config::load(&args.config)?;
    match args.command {
        BackupCommand::List { failed } => match open(&app_config, release, Mode::Read).await? {
            Opening::Store(opened) => list(&opened, failed).await,
            Opening::NoStore(reason) => {
                println!("=== backups in the configured store: none ({reason}) ===");
                println!("  (none)");
                Ok(())
            }
        },
        BackupCommand::Purge { confirm: false } => {
            // The dry run is a read, as `list` is: no serve lock, nothing
            // created, the plan from the rows of the root; with no store,
            // the plan is empty and says why, as `list` does.
            match open(&app_config, release, Mode::Read).await? {
                Opening::Store(opened) => dry_run(&opened).await,
                Opening::NoStore(reason) => {
                    println!("=== extenddb backup purge ===");
                    println!("Store:  none ({reason})");
                    println!();
                    println!("--- Backups to delete (each backup's objects and its catalog row):");
                    println!("  (none)");
                    println!();
                    println!("Dry run: nothing was deleted.");
                    Ok(())
                }
            }
        }
        BackupCommand::Purge { confirm: true } => {
            match open(&app_config, release, Mode::Write).await? {
                Opening::Store(opened) => purge(&opened, &app_config).await,
                Opening::NoStore(reason) => anyhow::bail!("{reason}"),
            }
        }
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
    /// As `serve` opens it (on SQLite under the serve lock); the store's
    /// check may write the identity file of an empty root.
    Write,
}

/// The storage engine, the backup runtime with the configured store, and
/// the store's identity as the command read it.
struct Opened {
    storage: Arc<dyn StorageEngine>,
    runtime: Arc<BackupRuntime>,
    backend: String,
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
    fn store(&self) -> &ConfiguredStore {
        self.runtime
            .configured_store()
            .expect("open returns only with a configured store")
    }
}

async fn open(
    app_config: &config::AppConfig,
    release: &'static str,
    mode: Mode,
) -> anyhow::Result<Opening> {
    let mut options = extenddb_storage::server_components::ServerComponentsOptions::default();
    options.release = release;
    options.max_concurrent_jobs = app_config.backup.max_concurrent_jobs;
    options.beside_server = mode == Mode::Read;
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
    .await
    .map_err(|e| anyhow::anyhow!("cannot open the catalog: {e}"))?;
    let storage = components.engine;
    let backend = app_config.storage.backend.clone();
    let setup = extenddb_server::backup_store::configure_described(
        &app_config.backup,
        app_config.storage.database_file().as_deref(),
        &backend,
        &*storage,
        match mode {
            Mode::Read => extenddb_server::backup_store::Caller::ReadOnlyCommand,
            Mode::Write => extenddb_server::backup_store::Caller::Command,
        },
    )
    .await?;
    let runtime = Arc::new(setup.runtime);
    let Some(store) = runtime.configured_store() else {
        // Why there is none, as `configure_described` found it: the section
        // absent or disabled, the default root not yet created, or the
        // default root failed its check with no rows for it.
        let why = setup
            .not_configured
            .expect("a runtime without a store carries the reason");
        return Ok(Opening::NoStore(why.to_string()));
    };
    // The server's startup leaves an S3 store's check to its reconciler;
    // the command runs it here. A read leaves the root as it finds it: the
    // identity file is read, never created, under the store's check bound as
    // the server's check is, so an endpoint that accepts and never answers
    // or a mount that has stopped answering costs the command the bound and
    // not the store's retries.
    let identity = match (mode, store.health()) {
        (Mode::Read, _) => match extenddb_engine::read_identity_bounded(store, &*storage).await {
            Ok(Some(identity)) => Identity::Known(identity),
            Ok(None) => Identity::Empty,
            Err(problem) => Identity::Problem(problem),
        },
        (Mode::Write, StoreHealth::Checking) => {
            let outcome = extenddb_engine::check_configured_store(store, &*storage, &backend).await;
            store.record_check(outcome.clone());
            match outcome {
                Ok(identity) => Identity::Known(identity),
                Err(problem) => Identity::Problem(problem),
            }
        }
        (Mode::Write, StoreHealth::Healthy) => match store.identity() {
            Some(identity) => Identity::Known(identity),
            None => Identity::Problem(StoreProblem::Catalog(
                "the store's id is unknown".to_owned(),
            )),
        },
        (Mode::Write, StoreHealth::Unhealthy(problem)) => Identity::Problem(problem),
    };
    Ok(Opening::Store(Opened {
        storage,
        runtime,
        backend,
        identity,
    }))
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
        rows.extend(rows_of(&*opened.storage, &identity, status).await?);
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
async fn dry_run(opened: &Opened) -> anyhow::Result<()> {
    print_plan(opened).await?;
    println!(
        "Dry run: nothing was deleted. Run again with --confirm to permanently delete every \
         backup of the configured store listed above."
    );
    Ok(())
}

async fn purge(opened: &Opened, app_config: &config::AppConfig) -> anyhow::Result<()> {
    let backup = &app_config.backup;
    let storage = &*opened.storage;
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
    print_plan(opened).await?;

    let stale_after = Duration::from_secs(backup.stale_job_after_secs);
    let holder = format!(
        "extenddb backup purge on {} (pid {})",
        extenddb_server::backup_store::hostname(),
        std::process::id()
    );
    // The signal listener is up before the claim is taken, so a signal
    // from here on ends the command with the claim released: at once during
    // the wait, before the next row during the deletion.
    let mut interrupt = listen_for_signals();
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
    let owner_window = if opened.backend == "sqlite" {
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
    let mut deleted = 0usize;
    let mut skipped = Vec::new();
    let mut stopped = None;
    for row in &rows {
        if let Some(reason) = claim.lost() {
            stopped = Some(format!("{reason}; retry once the claim is free"));
            break;
        }
        if let Some(signal) = interrupt.signalled() {
            stopped = Some(format!(
                "{signal} received. The maintenance claim is released; run the command again \
                 for the rest"
            ));
            break;
        }
        if let Err(e) = purge_row(storage, store, row, owner_window).await {
            skipped.push(format!("{}: {e}", row.backup_arn));
            continue;
        }
        tracing::info!(
            target: "extenddb::audit",
            action = "backup purge",
            account_id = %row.account_id,
            backup_arn = %row.backup_arn,
            store_root = %row.store.store_root,
            "deleted backup objects and catalog row"
        );
        println!("deleted {} ({})", row.backup_arn, row.status);
        deleted += 1;
    }
    release(claim).await;
    println!();
    println!(
        "=== extenddb backup purge complete: {deleted} backup(s) deleted, {} left ===",
        rows.len() - deleted
    );
    if let Some(reason) = stopped {
        anyhow::bail!("stopped before the remaining rows: {reason}");
    }
    if !skipped.is_empty() {
        for line in &skipped {
            println!("  left: {line}");
        }
        anyhow::bail!(
            "{} backup(s) could not be deleted; see above",
            skipped.len()
        );
    }
    Ok(())
}

/// The handle the wait and the deletion loop read a signal from, fed by a
/// task that waits for `SIGINT` (Ctrl-C) or `SIGTERM`. The first signal
/// stops the command at its next safe point (the wait at once, the loop
/// before its next row) with the claim released; a second one exits the
/// process at once, the claim then expiring by the stale rule (on SQLite
/// the next server start clears it).
fn listen_for_signals() -> Interrupt {
    let (tx, interrupt) = Interrupt::new();
    tokio::spawn(async move {
        let name = signal_name().await;
        println!(
            "{name} received; finishing the current step and releasing the maintenance claim (a \
             second {name} exits at once)."
        );
        let _ = tx.send(Some(name));
        let again = signal_name().await;
        println!(
            "{again} received again; exiting now. The maintenance claim is left to expire by the \
             stale rule."
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
/// under the serve lock every owner is dead. What was resolved is printed;
/// a failure is printed and the wait decides.
async fn resolve_dead_owners(opened: &Opened) {
    match extenddb_engine::reconciler::cycle(
        &*opened.storage,
        &opened.runtime,
        Duration::ZERO,
        &opened.backend,
        &mut extenddb_engine::reconciler::ReconcilerState::default(),
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
