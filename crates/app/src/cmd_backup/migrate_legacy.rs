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
//! being dead under the serve lock; those resolutions remove an in-catalog
//! backup left CREATING (a dead create), as `purge`'s do, printing it and
//! logging the removal under `extenddb::audit`.
//!
//! `<arn>` names one backup, as written or rebuilt in this server's region
//! (the spellings DescribeBackup resolves); it is checked as a backup ARN
//! before anything is opened (exit 2). `--all` takes every in-catalog
//! backup that is not DELETED, of every account, in creation order, and
//! goes on past a backup it refuses; then every DELETED one, whose copy a
//! stopped run left it deletes and whose in-catalog record it removes once
//! nothing is left under its prefix. Each backup gets one line:
//! `migrated`, `finished` (an earlier run had written its row, and what was
//! left of the in-catalog backup is now removed), already in the store,
//! `deleted` (its backup was deleted: nothing to migrate; with `--all` only
//! a backup for which something was deleted or removed is named),
//! `refused` (with the reason and the way out), `skipped` (not AVAILABLE,
//! or deleted meanwhile) or `failed` (a store or catalog call; a later run
//! tries again), with notes under it (a copy deleted or left, a record
//! removed), then a summary. A `refused` or `failed` line says "Its
//! in-catalog backup is kept" only where one is: not for a deleted backup,
//! nor after a replacement call whose answer did not come. Each backup the
//! run deleted or replaced anything for is logged under `extenddb::audit`
//! with the account and the ARN, whatever its outcome (a refused, skipped
//! or failed backup whose earlier copy the run deleted, and on Cassandra
//! every finish, included). A first `SIGINT` or
//! `SIGTERM` stops the run before the next backup, or inside a copy at the
//! writer's next step (the objects the run recorded are deleted, and a note
//! under the backup's line says so); a replacement already under way finishes first, and the
//! run then stops, whether or not another backup follows. A second signal
//! exits at once with status 130 and leaves what a kill leaves: the copy in
//! progress, and on PostgreSQL, MongoDB and Cassandra the claim held until
//! it is stale. A lost claim (read before each backup, and re-checked
//! with a beat the command waits for before each step that cannot be
//! undone), a store the writer finds unhealthy or
//! fenced (the deletion fence runs beside the re-checks before the
//! manifest's put and the replacement, and before each deletion of a
//! copy), or a catalog call that fails stops it at once. A stopped run
//! releases the claim and exits 1, saying the claim is released only when
//! the release succeeded, and after a lost claim that the run no longer
//! holds it. Otherwise the command exits 0 when every
//! backup it reached is in the store or deleted, and 1 after the others
//! ran.

use std::time::Duration;

use extenddb_core::validation::RestorableLimits;
use extenddb_engine::maintenance::Interrupt;
use extenddb_engine::migrate_legacy::{
    BackupOutcome, MigrateReport, Migration, Outcome, Selection, migrate,
};

use super::{Identity, Opened, config, release, take_claim, wait_for_jobs};

/// `<arn>`'s check, before anything is opened: the shape of a backup ARN.
pub(super) fn backup_arn_arg(arn: &str) -> Result<String, String> {
    extenddb_storage::util::check_backup_arn(arn.trim())
        .map(|_| arn.trim().to_owned())
        .map_err(|e| e.to_string())
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
    let claim = take_claim(storage, app_config, "migrate-legacy").await?;
    println!(
        "Until the command ends every backup and restore job of this catalog answers \
         LimitExceededException."
    );
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
    let lost = claim.lost().is_some();
    let released = release(claim).await;

    println!();
    if report.backups.is_empty() && report.stopped.is_none() {
        println!("No in-catalog backup to migrate.");
    }
    println!("{}", summary_line(&report));
    match ending(&report, released, lost) {
        Some(error) => Err(anyhow::anyhow!(error)),
        None => Ok(()),
    }
}

/// The summary line: each outcome's count, `refused` counting an ARN
/// nothing holds ([`Outcome::NotFound`]) too.
fn summary_line(report: &MigrateReport) -> String {
    let migrated = report.count(|o| matches!(o, Outcome::Migrated { .. }));
    let finished = report.count(|o| matches!(o, Outcome::Finished));
    let in_store = report.count(|o| matches!(o, Outcome::InStore));
    let deleted = report.count(|o| matches!(o, Outcome::Deleted));
    let refused = report.count(|o| matches!(o, Outcome::Refused(_) | Outcome::NotFound(_)));
    let skipped = report.count(|o| matches!(o, Outcome::Skipped(_)));
    let failed = report.count(|o| matches!(o, Outcome::Failed(_)));
    format!(
        "=== extenddb backup migrate-legacy {}: {migrated} migrated, {finished} finished, \
         {in_store} already in the store, {deleted} deleted, {refused} refused, {skipped} \
         skipped, {failed} failed ===",
        if report.stopped.is_some() {
            "stopped"
        } else {
            "complete"
        }
    )
}

/// The command's error after the summary, `None` for exit 0: a stop (the
/// claim said released only when `released`, and after a lost claim, one
/// `lost` reports, said no longer held: another holder may have it), or
/// the count of backups not migrated (refused, not found, skipped or
/// failed).
fn ending(report: &MigrateReport, released: bool, lost: bool) -> Option<String> {
    if let Some(reason) = &report.stopped {
        return Some(if released && lost {
            format!(
                "stopped before the remaining backups: {reason}. This run no longer holds the \
                 maintenance claim; run the command again for the rest"
            )
        } else if lost {
            format!(
                "stopped before the remaining backups: {reason}. This run no longer holds the \
                 maintenance claim, and its release failed; run the command again for the \
                 rest, once the claim's heartbeat is older than stale_job_after_secs if no \
                 other command took it over"
            )
        } else if released {
            format!(
                "stopped before the remaining backups: {reason}. The maintenance claim is \
                 released; run the command again for the rest"
            )
        } else {
            format!(
                "stopped before the remaining backups: {reason}. The maintenance claim was not \
                 released; run the command again for the rest once its heartbeat is older than \
                 stale_job_after_secs"
            )
        });
    }
    if report.complete() {
        return None;
    }
    let not_migrated = report.count(|o| {
        matches!(
            o,
            Outcome::Refused(_) | Outcome::NotFound(_) | Outcome::Skipped(_) | Outcome::Failed(_)
        )
    });
    Some(format!(
        "{not_migrated} backup(s) were not migrated; see above"
    ))
}

/// One backup's lines, and its audit line when it wrote anything.
fn print_outcome(done: &BackupOutcome) {
    if let Some(what) = audited(done) {
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

/// What the audit line of `done` says, for every backup the run deleted or
/// replaced anything for ([`BackupOutcome::wrote`]), whatever its outcome:
/// a migration, a finish that removed what was left of an in-catalog
/// backup (a replacement call, MongoDB's stopped replacement finished, or
/// on Cassandra a converted row's item buckets deleted), a deletion (a copy
/// a stopped run left deleted, a deleted backup's in-catalog record
/// removed, or a stopped replacement finished), and for a backup refused,
/// skipped or failed after it, the deletion of a copy an earlier run left
/// under its prefix. An outcome that wrote nothing (an ARN already in the
/// store with nothing left, a deleted backup with nothing under its prefix
/// and no record to remove, a refusal before anything was deleted) has
/// none.
fn audited(done: &BackupOutcome) -> Option<&'static str> {
    if !done.wrote {
        return None;
    }
    Some(match done.outcome {
        Outcome::Migrated { .. } => {
            "copied an in-catalog backup into the store and replaced it by its catalog row"
        }
        Outcome::Finished | Outcome::InStore => {
            "removed what was left of an in-catalog backup whose catalog row was written"
        }
        Outcome::Deleted => {
            "removed what was left of a deleted backup: a copy a stopped run left, its \
             in-catalog record, or its in-catalog leftovers"
        }
        Outcome::Refused(_) | Outcome::NotFound(_) | Outcome::Skipped(_) | Outcome::Failed(_) => {
            "deleted the copy an earlier run left under the backup's prefix; the backup was not \
             migrated"
        }
    })
}

/// What a reason that already says the in-catalog backup is kept contains
/// (the deletion fence's stop), so the line does not say it twice.
const KEPT_SAID: &str = "in-catalog backup is kept";

/// The lines printed for one backup: its outcome, then its notes, indented.
fn outcome_lines(done: &BackupOutcome) -> Vec<String> {
    let BackupOutcome {
        account_id,
        backup_arn,
        outcome,
        notes,
        in_catalog_kept,
        wrote: _,
    } = done;
    let kept = if *in_catalog_kept {
        ". Its in-catalog backup is kept"
    } else {
        ""
    };
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
        Outcome::Refused(reason) => {
            format!("refused {backup_arn} (account {account_id}): {reason}{kept}")
        }
        Outcome::NotFound(reason) => {
            format!("refused {backup_arn} (account {account_id}): {reason}")
        }
        Outcome::Skipped(reason) => {
            format!("skipped {backup_arn} (account {account_id}): {reason}")
        }
        Outcome::Failed(reason) => {
            // A stop at the deletion fence says it in its own text.
            let kept = if reason.contains(KEPT_SAID) { "" } else { kept };
            format!(
                "failed {backup_arn} (account {account_id}): {reason}{kept}; a later run tries \
                 again"
            )
        }
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
            in_catalog_kept: true,
            wrote: false,
        }
    }

    /// `done` for a backup whose in-catalog backup is not known kept.
    fn not_kept(outcome: Outcome) -> BackupOutcome {
        BackupOutcome {
            in_catalog_kept: false,
            ..done(outcome, &[])
        }
    }

    /// `done` for a backup the run deleted or replaced anything for.
    fn wrote(outcome: Outcome) -> BackupOutcome {
        BackupOutcome {
            wrote: true,
            ..done(outcome, &[])
        }
    }

    /// Each outcome's line and the notes under it, indented.
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

    /// "Its in-catalog backup is kept" on a `refused` or `failed` line only
    /// where the outcome says it is: a refusal or a failure of an AVAILABLE
    /// backup, and not a deleted backup's (a failed deletion of its copy, a
    /// row holding its ARN) or a stop after a replacement call whose answer
    /// did not come. A failure whose reason says it already (the deletion
    /// fence's stop, reported as the backup's failure when it has notes)
    /// says it once.
    #[test]
    fn a_refused_or_failed_line_says_the_backup_is_kept_only_where_it_is() {
        let refused = outcome_lines(&done(Outcome::Refused("why".to_owned()), &[]));
        assert_eq!(
            refused,
            [format!(
                "refused {ARN} (account 123456789012): why. Its in-catalog backup is kept"
            )]
        );
        let failed = outcome_lines(&done(Outcome::Failed("the store failed".to_owned()), &[]));
        assert_eq!(
            failed,
            [format!(
                "failed {ARN} (account 123456789012): the store failed. Its in-catalog backup \
                 is kept; a later run tries again"
            )]
        );
        let copy_left = outcome_lines(&not_kept(Outcome::Failed(
            "the copy a stopped run left under 1/Music/x/ could not be deleted".to_owned(),
        )));
        assert_eq!(
            copy_left,
            [format!(
                "failed {ARN} (account 123456789012): the copy a stopped run left under \
                 1/Music/x/ could not be deleted; a later run tries again"
            )]
        );
        let after_the_replacement = outcome_lines(&not_kept(Outcome::Failed(
            "the replacement failed: lost".to_owned(),
        )));
        assert!(
            !after_the_replacement[0].contains("is kept"),
            "{after_the_replacement:?}"
        );
        let held = outcome_lines(&not_kept(Outcome::Refused(
            "a catalog row (AVAILABLE) holds the ARN".to_owned(),
        )));
        assert_eq!(
            held,
            [format!(
                "refused {ARN} (account 123456789012): a catalog row (AVAILABLE) holds the ARN"
            )]
        );
        let fence_stop = outcome_lines(&done(
            Outcome::Failed(format!(
                "the deletion fence refused the replacement of {ARN}: adopted; the copy under \
                 1/Music/x/ is left, its in-catalog backup is kept, and nothing more is written \
                 or deleted"
            )),
            &["deleted the copy an earlier run left under 1/Music/x/"],
        ));
        assert_eq!(
            fence_stop,
            [
                format!(
                    "failed {ARN} (account 123456789012): the deletion fence refused the \
                     replacement of {ARN}: adopted; the copy under 1/Music/x/ is left, its \
                     in-catalog backup is kept, and nothing more is written or deleted; a later \
                     run tries again"
                ),
                "  deleted the copy an earlier run left under 1/Music/x/".to_owned(),
            ]
        );
        assert_eq!(
            fence_stop[0].matches("in-catalog backup is kept").count(),
            1
        );
    }

    /// An outcome gets an audit line exactly when the run deleted or
    /// replaced anything for it, whatever the outcome: a refusal, a skip or
    /// a failure after the deletion of a copy an earlier run left gets one
    /// that names that deletion.
    #[test]
    fn an_audit_line_follows_a_write() {
        for outcome in [
            Outcome::Migrated {
                item_count: 0,
                size_bytes: 0,
            },
            Outcome::Finished,
            Outcome::InStore,
            Outcome::Deleted,
            Outcome::Refused(String::new()),
            Outcome::NotFound(String::new()),
            Outcome::Skipped(String::new()),
            Outcome::Failed(String::new()),
        ] {
            assert!(audited(&wrote(outcome.clone())).is_some(), "{outcome:?}");
            assert!(
                audited(&done(outcome.clone(), &[])).is_none(),
                "nothing written: {outcome:?}"
            );
        }
        assert_eq!(
            audited(&wrote(Outcome::Refused(String::new()))),
            Some(
                "deleted the copy an earlier run left under the backup's prefix; the backup was \
                 not migrated"
            )
        );
    }

    /// The summary counts each of the eight outcomes under its name
    /// (`refused` counting an ARN nothing holds too), and the error after
    /// it counts every backup not migrated; a stop says the claim is
    /// released only when it was.
    #[test]
    fn the_summary_counts_every_outcome() {
        let report = MigrateReport {
            backups: [
                Outcome::Migrated {
                    item_count: 1,
                    size_bytes: 1,
                },
                Outcome::Finished,
                Outcome::InStore,
                Outcome::Deleted,
                Outcome::Refused(String::new()),
                Outcome::NotFound(String::new()),
                Outcome::Skipped(String::new()),
                Outcome::Failed(String::new()),
            ]
            .into_iter()
            .map(|outcome| done(outcome, &[]))
            .collect(),
            stopped: None,
        };
        assert_eq!(
            summary_line(&report),
            "=== extenddb backup migrate-legacy complete: 1 migrated, 1 finished, 1 already in \
             the store, 1 deleted, 2 refused, 1 skipped, 1 failed ==="
        );
        assert_eq!(
            ending(&report, true, false).as_deref(),
            Some("4 backup(s) were not migrated; see above")
        );
        let not_found = MigrateReport {
            backups: vec![done(Outcome::NotFound(String::new()), &[])],
            stopped: None,
        };
        assert!(
            summary_line(&not_found).ends_with(
                ": 0 migrated, 0 finished, 0 already in the store, 0 deleted, 1 refused, 0 \
                 skipped, 0 failed ==="
            ),
            "{}",
            summary_line(&not_found)
        );
        assert_eq!(
            ending(&not_found, true, false).as_deref(),
            Some("1 backup(s) were not migrated; see above")
        );
        let in_store = MigrateReport {
            backups: vec![done(Outcome::InStore, &[])],
            stopped: None,
        };
        assert_eq!(ending(&in_store, true, false), None);
        let stopped = MigrateReport {
            backups: vec![],
            stopped: Some("SIGINT received".to_owned()),
        };
        assert!(summary_line(&stopped).contains(" stopped: 0 migrated"));
        let released = ending(&stopped, true, false).expect("a stop");
        assert!(
            released.contains("The maintenance claim is released"),
            "{released}"
        );
        let held = ending(&stopped, false, false).expect("a stop");
        assert!(!held.contains("is released"), "{held}");
        assert!(
            held.contains("The maintenance claim was not released"),
            "{held}"
        );
        // A lost claim, whose release finds nothing of this run's: the claim
        // may be another holder's, so it is not said to be free.
        let lost = ending(&stopped, true, true).expect("a stop");
        assert!(!lost.contains("is released"), "{lost}");
        assert!(
            lost.contains("This run no longer holds the maintenance claim"),
            "{lost}"
        );
        let lost_held = ending(&stopped, false, true).expect("a stop");
        assert!(
            lost_held
                .contains("This run no longer holds the maintenance claim, and its release failed")
                && lost_held.contains("stale_job_after_secs"),
            "{lost_held}"
        );
    }

    /// `<arn>` is checked before anything is opened, and the error says
    /// once what is wrong.
    #[test]
    fn the_arn_is_checked_as_a_backup_arn() {
        assert_eq!(backup_arn_arg(&format!(" {ARN} ")).as_deref(), Ok(ARN));
        let refused = backup_arn_arg("arn:aws:dynamodb:us-east-1:123456789012:table/t")
            .expect_err("a table ARN");
        assert_eq!(refused.matches("not a backup ARN").count(), 1, "{refused}");
        assert!(backup_arn_arg("not-an-arn").is_err());
    }
}
