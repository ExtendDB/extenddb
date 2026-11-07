// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb backup migrate-legacy <arn>|--all`: the in-catalog backups
//! copied into the configured store and replaced there by their catalog
//! rows, under their own ARNs (`extenddb_engine::migrate_legacy` states
//! each step and every refusal).
//!
//! The command opens the catalog as `serve` does (on SQLite under the serve
//! lock, so the server is stopped for it) and needs the configured store
//! healthy, its identity known. It takes the maintenance claim on every
//! backend and heartbeats it for its whole run, so every backup and restore
//! job of the catalog answers `LimitExceededException` until it ends (a
//! second maintenance command fails at once), and it waits, as `purge`
//! does, until no CREATING or FAILED backup row of the store has a live
//! owner and no restore job is RUNNING (a legacy restore of a backup about
//! to be replaced among them). On SQLite it first runs the reconciler's
//! dead-owner resolutions itself and waits with a zero window, every owner
//! being dead under the serve lock.
//!
//! `<arn>` names one backup, as written or rebuilt in this server's region
//! (the spellings DescribeBackup resolves); it is checked as a backup ARN
//! before anything is opened (exit 2). `--all` takes every in-catalog
//! backup that is not DELETED, of every account, in creation order, and
//! goes on past a backup it refuses; then every DELETED one, whose copy a
//! stopped run left it deletes. Each backup gets one line: `migrated`,
//! `finished` (an earlier run had written its row, and what was left of the
//! in-catalog backup is now removed), already in the store, `deleted` (its
//! backup was deleted: nothing to migrate; with `--all` only a backup
//! whose left copy it deleted is named), `refused` (with the reason and the
//! way out; its in-catalog backup is kept where one exists), `skipped` (not
//! AVAILABLE, or deleted meanwhile) or `failed` (a store or catalog call; a
//! later run tries again), with notes under it (a copy deleted or left),
//! then a summary. Each migration, finish and deletion is logged under
//! `extenddb::audit` with the account and the ARN. A first `SIGINT` or
//! `SIGTERM` stops the run before the next backup, or inside a copy at the
//! writer's next step (the copy is deleted); a replacement already under
//! way finishes first, and the run then stops, whether or not another
//! backup follows. A lost claim, a store the writer finds unhealthy or
//! fenced, or a catalog call that fails stops it at once. A stopped run
//! releases the claim and exits 1. Otherwise the command exits 0 when every
//! backup it reached is in the store or deleted, and 1 after the others
//! ran.

use std::time::Duration;

use extenddb_core::validation::RestorableLimits;
use extenddb_engine::HeldClaim;
use extenddb_engine::maintenance::Interrupt;
use extenddb_engine::migrate_legacy::{BackupOutcome, Migration, Outcome, Selection, migrate};
use extenddb_storage::backup_catalog::JOB_HEARTBEAT_INTERVAL;
use extenddb_storage::error::StorageError;

use super::{Identity, Opened, config, release, wait_for_jobs};

/// `<arn>`'s check, before anything is opened: the shape of a backup ARN.
pub(super) fn backup_arn_arg(arn: &str) -> Result<String, String> {
    extenddb_storage::util::check_backup_arn(arn.trim())
        .map(|_| arn.trim().to_owned())
        .map_err(|e| format!("not a backup ARN: {e}"))
}

/// `backup migrate-legacy` (module doc). `arn` is `None` for `--all`;
/// `version` is this binary's, which the manifests name as their writer.
pub(super) async fn migrate_legacy(
    opened: &Opened,
    app_config: &config::AppConfig,
    version: &str,
    arn: Option<&str>,
    mut interrupt: Interrupt,
) -> anyhow::Result<()> {
    let storage = &*opened.catalog.storage;
    let configured = opened.store();
    println!("=== extenddb backup migrate-legacy ===");
    let identity = match &opened.identity {
        Identity::Known(identity) => {
            println!(
                "Store:  {} {} (store id {})",
                identity.store_kind, identity.store_root, identity.store_id
            );
            identity.clone()
        }
        Identity::Empty => anyhow::bail!(
            "the {} backup store {} has no {}: its check did not run",
            configured.kind,
            configured.root,
            extenddb_engine::STORE_IDENTITY_KEY
        ),
        Identity::Problem(problem) => anyhow::bail!(
            "the {} backup store {} is unhealthy: {problem}; nothing is migrated into a store \
             whose check fails",
            configured.kind,
            configured.root
        ),
    };

    let backup = &app_config.backup;
    let stale_after = Duration::from_secs(backup.stale_job_after_secs);
    let holder = format!(
        "extenddb backup migrate-legacy on {} (pid {})",
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
    println!(
        "Took the maintenance claim as {holder:?}: until the command ends every backup and \
         restore job of this catalog answers LimitExceededException."
    );
    let claim = HeldClaim::start(taken, JOB_HEARTBEAT_INTERVAL);
    if let Err(refusal) = wait_for_jobs(
        &opened.catalog,
        std::slice::from_ref(&identity),
        &claim,
        &mut interrupt,
        stale_after,
        false,
    )
    .await
    {
        release(claim).await;
        return Err(refusal);
    }

    let migration = Migration {
        configured,
        identity: &identity,
        claim: &claim,
        interrupt: &interrupt,
        limits: RestorableLimits::from(&app_config.limits),
        data_file_target_bytes: backup.data_file_target_bytes,
        region: &app_config.server.region,
        source_backend: &opened.catalog.backend,
        source_extenddb_version: version,
        metrics: None,
    };
    let selection = arn.map_or(Selection::All, Selection::Arn);
    let report = migrate(storage, &migration, selection, print_outcome).await;
    release(claim).await;

    let migrated = report.count(|o| matches!(o, Outcome::Migrated { .. }));
    let finished = report.count(|o| matches!(o, Outcome::Finished));
    let in_store = report.count(|o| matches!(o, Outcome::InStore));
    let deleted = report.count(|o| matches!(o, Outcome::Deleted));
    let refused = report.count(|o| matches!(o, Outcome::Refused(_) | Outcome::NotFound(_)));
    let skipped = report.count(|o| matches!(o, Outcome::Skipped(_)));
    let failed = report.count(|o| matches!(o, Outcome::Failed(_)));
    println!();
    if report.backups.is_empty() && report.stopped.is_none() {
        println!("No in-catalog backup to migrate.");
    }
    println!(
        "=== extenddb backup migrate-legacy {}: {migrated} migrated, {finished} finished, \
         {in_store} already in the store, {deleted} deleted, {refused} refused, {skipped} \
         skipped, {failed} failed ===",
        if report.stopped.is_some() {
            "stopped"
        } else {
            "complete"
        }
    );
    if let Some(reason) = &report.stopped {
        anyhow::bail!(
            "stopped before the remaining backups: {reason}. The maintenance claim is released; \
             run the command again for the rest"
        );
    }
    if !report.complete() {
        anyhow::bail!(
            "{} backup(s) were not migrated; see above",
            refused + skipped + failed
        );
    }
    Ok(())
}

/// One backup's lines, and its audit line when it wrote anything.
fn print_outcome(done: &BackupOutcome) {
    if let Some(what) = audited(&done.outcome) {
        tracing::info!(
            target: "extenddb::audit",
            action = "backup migrate-legacy",
            account_id = %done.account_id,
            backup_arn = %done.backup_arn,
            "{what}"
        );
    }
    for line in outcome_lines(done) {
        println!("{line}");
    }
}

/// What the audit line of `outcome` says, for an outcome that wrote: a
/// migration, a finish (the replacement call that removed what was left),
/// already in the store (the same call: on Cassandra it finishes a
/// conversion), and a deletion (a row's replacement call, or a left copy
/// deleted).
fn audited(outcome: &Outcome) -> Option<&'static str> {
    match outcome {
        Outcome::Migrated { .. } => {
            Some("copied an in-catalog backup into the store and replaced it by its catalog row")
        }
        Outcome::Finished => {
            Some("removed what was left of an in-catalog backup whose catalog row was written")
        }
        Outcome::InStore => Some(
            "finished the replacement of an in-catalog backup already in the store, if any was \
             left",
        ),
        Outcome::Deleted => Some(
            "removed what was left of a deleted backup: a copy a stopped run left, or an \
             in-catalog backup's leftovers",
        ),
        Outcome::Refused(_) | Outcome::NotFound(_) | Outcome::Skipped(_) | Outcome::Failed(_) => {
            None
        }
    }
}

/// The lines printed for one backup: its outcome, then its notes, indented.
fn outcome_lines(done: &BackupOutcome) -> Vec<String> {
    let BackupOutcome {
        account_id,
        backup_arn,
        outcome,
        notes,
    } = done;
    let first = match outcome {
        Outcome::Migrated {
            item_count,
            size_bytes,
        } => format!(
            "migrated {backup_arn} (account {account_id}): {item_count} items, {size_bytes} bytes"
        ),
        Outcome::Finished => format!(
            "finished {backup_arn} (account {account_id}): an earlier run had written its row; \
             what was left of the in-catalog backup is removed"
        ),
        Outcome::InStore => format!(
            "{backup_arn} (account {account_id}) is already in the store: nothing to migrate"
        ),
        Outcome::Deleted => format!(
            "deleted {backup_arn} (account {account_id}): the backup was deleted; nothing to \
             migrate"
        ),
        Outcome::Refused(reason) => format!(
            "refused {backup_arn} (account {account_id}): {reason}. Its in-catalog backup is kept"
        ),
        Outcome::NotFound(reason) => {
            format!("refused {backup_arn} (account {account_id}): {reason}")
        }
        Outcome::Skipped(reason) => {
            format!("skipped {backup_arn} (account {account_id}): {reason}")
        }
        Outcome::Failed(reason) => format!(
            "failed {backup_arn} (account {account_id}): {reason}. Its in-catalog backup is \
             kept; a later run tries again"
        ),
    };
    std::iter::once(first)
        .chain(notes.iter().map(|note| format!("  {note}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARN: &str =
        "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01791976430001-11110001";

    fn done(outcome: Outcome, notes: &[&str]) -> BackupOutcome {
        BackupOutcome {
            account_id: "123456789012".to_owned(),
            backup_arn: ARN.to_owned(),
            outcome,
            notes: notes.iter().map(|n| (*n).to_owned()).collect(),
        }
    }

    /// Each outcome's line, the notes under it indented, and the
    /// in-catalog suffix only where an in-catalog backup exists.
    #[test]
    fn each_outcome_prints_its_line_and_its_notes() {
        let lines = outcome_lines(&done(
            Outcome::Migrated {
                item_count: 3,
                size_bytes: 120,
            },
            &["deleted the copy an earlier run left under 1/Music/x/"],
        ));
        assert_eq!(
            lines,
            [
                format!("migrated {ARN} (account 123456789012): 3 items, 120 bytes"),
                "  deleted the copy an earlier run left under 1/Music/x/".to_owned(),
            ]
        );
        let refused = outcome_lines(&done(Outcome::Refused("why".to_owned()), &[]));
        assert!(
            refused[0].ends_with("why. Its in-catalog backup is kept"),
            "{refused:?}"
        );
        let not_found = outcome_lines(&done(Outcome::NotFound("none".to_owned()), &[]));
        assert_eq!(
            not_found,
            [format!("refused {ARN} (account 123456789012): none")]
        );
        let deleted = outcome_lines(&done(Outcome::Deleted, &["deleted the copy"]));
        assert!(
            deleted[0].starts_with(&format!("deleted {ARN}")),
            "{deleted:?}"
        );
        assert_eq!(deleted[1], "  deleted the copy");
        assert!(outcome_lines(&done(Outcome::InStore, &[]))[0].contains("is already in the store"));
    }

    /// The outcomes that wrote get an audit line, a finish of an ARN
    /// already in the store among them; refusals, skips and failures do not.
    #[test]
    fn every_outcome_that_wrote_is_audited() {
        for outcome in [
            Outcome::Migrated {
                item_count: 0,
                size_bytes: 0,
            },
            Outcome::Finished,
            Outcome::InStore,
            Outcome::Deleted,
        ] {
            assert!(audited(&outcome).is_some(), "{outcome:?}");
        }
        for outcome in [
            Outcome::Refused(String::new()),
            Outcome::NotFound(String::new()),
            Outcome::Skipped(String::new()),
            Outcome::Failed(String::new()),
        ] {
            assert!(audited(&outcome).is_none(), "{outcome:?}");
        }
    }

    /// `<arn>` is checked before anything is opened.
    #[test]
    fn the_arn_is_checked_as_a_backup_arn() {
        assert_eq!(backup_arn_arg(&format!(" {ARN} ")).as_deref(), Ok(ARN));
        assert!(backup_arn_arg("not-an-arn").is_err());
    }
}
