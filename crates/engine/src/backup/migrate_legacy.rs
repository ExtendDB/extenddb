// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb backup migrate-legacy <arn>|--all`: in-catalog backups copied
//! into the configured store, each under its own ARN, and replaced there by
//! a catalog index row.
//!
//! [`migrate`] runs under the maintenance claim the command holds
//! ([`HeldClaim`]). `--all` takes every in-catalog backup that is not
//! DELETED from [`LegacyBackupReader::list_all`], in creation order, and
//! then every DELETED one from [`LegacyBackupReader::list_deleted`];
//! `<arn>` takes the in-catalog backup whose ARN is `<arn>` as written or
//! rebuilt in this server's region, the spellings DescribeBackup resolves,
//! through [`LegacyBackupReader::holder_status`]. Each backup that is not
//! DELETED goes through these steps in this order, and every refusal
//! before the first put reads nothing more and writes nothing:
//!
//! 1. The id: one with the 13-digit shape every 0.1.x release issued (with
//!    or without a suffix) is refused, since a store prefix carries only
//!    the 14-digit shape, whose id carries its creation instant. The way
//!    out is a restore of the backup and a new backup of the restored
//!    table, which has a new ARN.
//! 2. The table name: one the store-prefix rules refuse (`BackupArn`: over
//!    255 bytes, or dots only) is refused.
//! 3. A replacement that committed and stopped before its end (MongoDB's
//!    DELETING document marked `migrated_to_store`): a DELETING backup is
//!    first handed to [`LegacyBackupReader::finish_replacement`], which
//!    finishes it whatever the catalog index row holds ("finished" while
//!    the row is AVAILABLE, "deleted" when a DeleteBackup of the migrated
//!    backup came since).
//! 4. A catalog row of the backup's id: under either spelling of its ARN,
//!    then under any ARN and any account
//!    ([`BackupCatalog::row_holding_backup_id`]: the id alone is the key the
//!    inserts conflict on). One of the configured store under the
//!    stored spelling is an earlier run's (or a `backup sync` that adopted
//!    an earlier run's copy), and
//!    [`LegacyBackupReader::replace_with_index_row`] with that row removes
//!    what is left of the in-catalog backup ("finished" when the row is
//!    AVAILABLE, "deleted" when a DeleteBackup of the migrated backup came
//!    since), once the row's prefix has been read as the copy below is
//!    read back ([`verify_prefix`], the manifest's ARN, the item total
//!    against the in-catalog backup's count): a row whose objects are gone
//!    or short refuses the backup, naming the row, with the in-catalog
//!    backup kept; one of another store, or one under any other ARN, refuses
//!    the backup before anything under its prefix is deleted, since the
//!    prefix carries no region and may be that row's.
//! 5. The status: a CREATING or DELETING in-catalog backup is skipped and
//!    named.
//! 6. The open ([`LegacyBackupReader::items`]): the backup's name, its
//!    recorded source and its vector state. A backend that cannot read the
//!    backup (MongoDB's without an item collection) refuses it.
//! 7. The name: one the manifest refuses ([`is_valid_backup_name`]) is
//!    refused; the name survives with the ARN or not at all.
//! 8. The definition: [`source_table_from_legacy`], the table the legacy
//!    restore creates; it refuses what the legacy restore refuses (vector
//!    indexes, a multi-part base key) and a SQLite backup taken before
//!    catalog 0.0.4, whose vector state is unknown.
//!
//! Then the copy. Objects under the backup's prefix with no catalog row
//! for its id are what an earlier run left (it stopped before its
//! replacement), provided the manifest there, if any, names this backup
//! (one that names another, or does not read, refuses the backup with the
//! prefix left: on Cassandra, which reads no row by its id, it is what
//! tells another row's prefix); they are deleted through the deletion
//! fence first, by the prefix walk under the claim's deadline
//! ([`HeldClaim::acting_deadline`]), so every copy is written whole by one
//! run. The fence runs before the first put as CreateBackup's does. The
//! items stream through the backup job's writer
//! ([`writer::write_data_files`]), each checked with the item validator
//! under this instance's limits, into data files under the backup's
//! prefix; then the three documents, the fence and the claim's re-check,
//! and `extenddb-manifest.json`, whose `snapshot.kind` is
//! [`SnapshotDescriptor::KIND_LEGACY_IN_CATALOG`] with no marker, whose
//! item order is `unordered`, and whose `source_backend` and
//! `source_extenddb_version` name this binary. The prefix is then read back
//! through [`verify_prefix`] (the check `backup sync` and the dead-owner
//! duty apply), and the manifest's item total must equal both the items
//! written and the count the in-catalog backup records. Only then, after
//! the fence and the claim's re-check, is the row written, by
//! [`LegacyBackupReader::replace_with_index_row`]: AVAILABLE, unowned
//! (`owner_generation` 0, as `backup sync` writes one), in the configured
//! store's identity, with the manifest's name, size, item total and
//! `source_table`, and the id's instant as `created_at`. A refusal or
//! failure after the first put deletes the copy through the fence and
//! keeps the in-catalog backup; a read that failed or ended short of a
//! backup no longer AVAILABLE is skipped as deleted during the copy (a
//! status that cannot be re-read fails the backup, which is kept). An
//! index row of the id that the replacement meets (`AlreadyExists`, one
//! written since step 4 read none) refuses the backup and leaves the copy.
//! On Cassandra the items are read at `ALL` (the in-catalog create wrote
//! them at `ONE`), and a count the read does not reproduce names a repair
//! before the rerun rather than the legacy restore, which reads at `ONE`;
//! a conversion whose condition failed at every attempt
//! ([`Replaced::NotApplied`]) fails the backup with the copy deleted and
//! the in-catalog backup unchanged.
//!
//! Two runs can meet under one prefix (a backup job's prefix is its own:
//! its ARN is new): a run stopped past the stale window (SIGSTOP, a paused
//! VM) whose claim a second run took. So every object of a copy is written
//! with `put_if_absent`, and an object already under a key stops the run
//! with it left ([`Stop::Occupied`]: another run's object, or this run's
//! own put retried by the SDK after its answer was lost); and a run deletes
//! its own copy object by object, only the objects its writer recorded
//! ([`Pipeline::written`]: each object stored, and after a put whose
//! outcome is unknown the object `head` finds under the key, so a key the
//! head cannot read is left to the next run), each with
//! [`BackupStore::delete_written`](extenddb_backup_store::BackupStore::delete_written)
//! (S3 `If-Match` on the put's `ETag`; a filesystem root moves the file
//! aside and back unless its size and modification time are the put's),
//! reading the claim before each. The prefix walk runs only on a leftover
//! the steps above have shown no row's, and starts no removal at or after
//! the claim's deadline. What no check bounds is a call already sent when
//! the process stopped: it completes on resume. Such a put cannot replace
//! another run's object; it may add one, which stops the other run at that
//! key. Such a deletion of an object this run wrote can reach another
//! run's object at that key only on S3 when the two have the same bytes (the
//! same `ETag`), or on a filesystem root when the process stops again
//! between the move aside and the move back (the other file is then left
//! under a hidden name); a walk completes its step under way (one entry, or
//! one `DeleteObjects` request).
//!
//! The claim is read before each backup, and re-checked with a beat the
//! run waits for ([`HeldClaim::recheck`]) immediately before every step
//! that cannot be undone: the manifest's put, every deletion under a store
//! prefix (a copy an earlier run left, this run's copy, a deleted backup's
//! copy), each replacement call and the removal of a deleted backup's
//! record. Before the manifest's put and the replacement, and before each
//! deletion, the deletion fence (`check_catalog`: the root's identity file
//! still names this catalog) runs beside the re-check. A re-check or a
//! fence that fails stops the run with what it wrote left, and says so;
//! at a copy's deletion the stop keeps the backup's end as a note.
//!
//! A DELETED in-catalog backup (a DeleteBackup of the legacy ARN, which the
//! claim does not hold back) has nothing to migrate. A catalog row of its
//! id refuses it: under its own ARN and AVAILABLE, the deleted backup is in
//! the catalog again (a `backup sync` of an earlier release adopted a
//! stopped run's copy), and the operator deletes it again or keeps it;
//! under another ARN, the prefix may be that row's. Otherwise, behind the
//! fence, a copy a stopped run left under its prefix is deleted (when its
//! manifest, if any, names this backup), and once the store lists nothing
//! there the DELETED in-catalog record is removed
//! ([`LegacyBackupReader::remove_deleted`]): a DELETED in-catalog backup
//! whose id carries an instant is then one whose prefix is not yet known
//! clean ("deleted"; with `--all` a backup for which nothing was deleted or
//! removed is not reported). `backup sync` never adopts the prefix of an
//! in-catalog backup in any status, so a backup DeleteBackup answered
//! DELETED does not come back while its record is there, and its record
//! goes only once nothing is left to come back.
//!
//! A refusal is a property of one backup, or a race it lost (a DeleteBackup
//! or a restore that came first): it is reported and the next backup runs.
//! A lost claim, a signal, a store the writer finds unhealthy or fenced, a
//! failed listing of the in-catalog backups and a catalog call that fails
//! stop the run at once ([`MigrateReport::stopped`]). A signal is acted on
//! between backups and, inside the copy, at the writer's next step, the
//! copy deleted. Once the manifest is written the replacement is not
//! interrupted: it finishes, and a signal received meanwhile then stops the
//! run, whether or not another backup follows. A run that stops with a
//! copy under a prefix and no row for it (a lost claim, a crash) leaves the
//! in-catalog backup in place; one whose replacement's answer did not come
//! may have replaced it. The next run deletes the copy and writes it again
//! while the backup is AVAILABLE, or finishes the replacement whose row is
//! there. `backup sync` skips such a prefix while an in-catalog backup
//! holds the ARN, in any status.

use std::sync::{Mutex, PoisonError};

use extenddb_backup_store::ObjectMeta;
use extenddb_core::backup_format::{
    BackupArn, BackupDocuments, ItemOrder, ManifestHeader, SnapshotDescriptor,
    backup_creation_date_time, is_valid_backup_name,
};
use extenddb_core::metrics::MetricsCollector;
use extenddb_core::validation::{RestorableKeySchema, RestorableLimits};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, LegacyBackupKey, Replaced, StoreIdentity, UNOWNED_GENERATION,
    is_backup_not_found,
};
use extenddb_storage::backup_definition::source_table_from_legacy;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::ARN_PARTITION;
use extenddb_storage::{BackupCatalog, CancellationToken, LegacyBackupReader};
use futures::TryStreamExt;

use super::BackupArnSpellings;
use super::job::writer::{self, ClaimWatch, Pipeline, Stop};
use super::job::{KillPoint, kill_point, put_document};
use super::maintenance::{HeldClaim, Interrupt};
use super::reconciler::{StoreCalls, Verified, manifest_item_count, verify_prefix};
use super::runtime::ConfiguredStore;

/// The maintenance claim as the writer reads it at each put: confirmed by
/// [`HeldClaim::lost`]'s rule (a beat confirmed within two periods, or on
/// SQLite one on schedule parked in flight, and no loss recorded). The
/// manifest's put is preceded by a live re-check too.
impl ClaimWatch for HeldClaim {
    fn confirms_the_claim(&self) -> bool {
        HeldClaim::lost(self).is_none()
    }

    fn lost(&self) -> &CancellationToken {
        self.lost_signal()
    }

    fn reason(&self) -> String {
        HeldClaim::lost(self).unwrap_or_else(|| "the maintenance claim is lost".to_owned())
    }
}

/// What a migration runs with.
pub struct Migration<'a> {
    /// The configured store, healthy.
    pub configured: &'a ConfiguredStore,
    /// The configured store's identity, which the rows are written under.
    pub identity: &'a StoreIdentity,
    /// The maintenance claim the command holds.
    pub claim: &'a HeldClaim,
    /// The command's signal.
    pub interrupt: &'a Interrupt,
    /// The limits the item validator holds every item to.
    pub limits: RestorableLimits,
    /// `data_file_target_bytes`.
    pub data_file_target_bytes: u64,
    /// The server's region: `<arn>` is also looked up rebuilt in it, a
    /// backup of another region is noted, and the SSE description's key
    /// ARN is built in it, as the legacy DescribeBackup builds it.
    pub region: &'a str,
    /// The backend and the release the manifests name as their writer.
    pub source_backend: &'a str,
    pub source_extenddb_version: &'a str,
    /// The collector failed store calls are recorded in; `None` in a
    /// command.
    pub metrics: Option<&'a MetricsCollector>,
}

/// Which backups to migrate.
#[derive(Debug, Clone, Copy)]
pub enum Selection<'a> {
    /// The in-catalog backup this ARN names (as written, or rebuilt in the
    /// server's region).
    Arn(&'a str),
    /// Every in-catalog backup that is not DELETED, of every account.
    All,
}

/// What happened to one backup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Copied into the store and replaced by its row.
    Migrated { item_count: u64, size_bytes: i64 },
    /// A catalog row of the configured store already held the ARN (an
    /// earlier run's); what was left of the in-catalog backup is removed.
    Finished,
    /// `<arn>` names no in-catalog backup and an AVAILABLE catalog row of
    /// the configured store holds it: already in the store.
    InStore,
    /// The backup was deleted: a DELETED in-catalog backup (a copy a
    /// stopped run left is deleted, and its record removed once nothing is
    /// left under its prefix, in the notes), or a catalog row of the
    /// configured store that a DeleteBackup moved to DELETING or DELETED.
    /// Nothing to migrate.
    Deleted,
    /// Not migrated (or, for a deleted in-catalog backup, not cleaned), for
    /// a reason of its own, with nothing deleted under its prefix; the
    /// in-catalog backup, where one is AVAILABLE, is kept
    /// ([`BackupOutcome::in_catalog_kept`]). The text gives the reason and
    /// the way out.
    Refused(String),
    /// `<arn>` names no in-catalog backup and no catalog row of the
    /// configured store in a state to report; the text says what holds the
    /// ARN, if anything. Counted as refused.
    NotFound(String),
    /// Not AVAILABLE, or no longer AVAILABLE when its row was written; the
    /// text says which.
    Skipped(String),
    /// A store or catalog call for this backup failed, or the run stopped
    /// with notes for it; a later run tries again. Whether the in-catalog
    /// backup is kept is [`BackupOutcome::in_catalog_kept`].
    Failed(String),
}

/// One backup's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupOutcome {
    pub account_id: String,
    /// The ARN as the in-catalog backup stores it.
    pub backup_arn: String,
    pub outcome: Outcome,
    /// What else the operator is told about it: a copy an earlier run left
    /// and this one deleted, a copy that could not be deleted, a region
    /// other than the server's, a creation time that differs from the id's
    /// instant, a deleted backup's in-catalog record removed.
    pub notes: Vec<String>,
    /// Whether the in-catalog backup is known to be kept: `false` for a
    /// deleted one, for a run that stopped after a replacement call whose
    /// answer did not come (the backup may be in the store now), and once
    /// the claim is lost (a command that took it over may have replaced
    /// the backup). The
    /// command says "Its in-catalog backup is kept" only where this holds.
    pub in_catalog_kept: bool,
    /// Whether the run deleted or replaced anything for this backup: a
    /// copy put and its row written, what was left of the in-catalog backup
    /// removed, a copy a stopped run left deleted, a deleted backup's
    /// in-catalog record removed. The command logs an audit line only then.
    pub wrote: bool,
}

/// What a run did.
#[derive(Debug, Default)]
pub struct MigrateReport {
    /// Each backup the run reached, in order.
    pub backups: Vec<BackupOutcome>,
    /// Why the run stopped before the end, when it did.
    pub stopped: Option<String>,
}

impl MigrateReport {
    /// How many backups ended with an outcome `pick` keeps.
    #[must_use]
    pub fn count(&self, pick: impl Fn(&Outcome) -> bool) -> usize {
        self.backups.iter().filter(|b| pick(&b.outcome)).count()
    }

    /// Whether every backup the run reached is in the store (migrated,
    /// finished or already there) or deleted, and the run reached its end.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.stopped.is_none()
            && self.backups.iter().all(|b| {
                matches!(
                    b.outcome,
                    Outcome::Migrated { .. }
                        | Outcome::Finished
                        | Outcome::InStore
                        | Outcome::Deleted
                )
            })
    }
}

/// The way out named for a backup this release cannot copy but can
/// restore.
const CARRY_FORWARD: &str = "restore it (RestoreTableFromBackup) and take a new backup of the \
                             restored table, which has a new ARN";

/// One backup's end: an outcome, or a stop of the whole run.
enum Step {
    Done(Outcome),
    Stop(String),
}

/// What one backup's steps record beside its end.
#[derive(Default)]
struct Said {
    /// [`BackupOutcome::notes`].
    notes: Vec<String>,
    /// [`BackupOutcome::wrote`].
    wrote: bool,
    /// A replacement call whose answer did not come: the in-catalog backup
    /// may be gone.
    replacement_unknown: bool,
}

/// What the steps of one backup share.
struct Run<'m, 'a> {
    m: &'m Migration<'a>,
    /// Cancelled on the command's signal: the writer's shutdown token.
    shutdown: CancellationToken,
}

/// Migrate the backups `selection` names (module doc). `on_backup` is
/// called with each backup's outcome as it ends.
pub async fn migrate<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    m: &Migration<'_>,
    selection: Selection<'_>,
    mut on_backup: impl FnMut(&BackupOutcome),
) -> MigrateReport {
    let shutdown = CancellationToken::new();
    let watcher = tokio::spawn({
        let mut interrupt = m.interrupt.clone();
        let shutdown = shutdown.clone();
        async move {
            interrupt.wait().await;
            shutdown.cancel();
        }
    });
    let run = Run { m, shutdown };
    let mut report = MigrateReport::default();
    let keys = match selection {
        Selection::All => match all_keys(storage).await {
            Ok(keys) => keys,
            Err(stop) => {
                watcher.abort();
                report.stopped = Some(stop);
                return report;
            }
        },
        Selection::Arn(arn) => match named(storage, &run, arn).await {
            Ok(Named::Key(key)) => vec![key],
            Ok(Named::Done(done)) => {
                on_backup(&done);
                report.backups.push(done);
                vec![]
            }
            Err(stop) => {
                watcher.abort();
                report.stopped = Some(stop);
                return report;
            }
        },
    };
    let quiet = matches!(selection, Selection::All);
    for key in keys {
        if let Some(reason) = m.claim.lost() {
            report.stopped = Some(reason);
            break;
        }
        if let Some(signal) = m.interrupt.signalled() {
            report.stopped = Some(format!("{signal} received"));
            break;
        }
        let mut said = Said::default();
        let step = if key.status == BackupStatus::Deleted {
            clean_deleted(storage, &run, &key, &mut said).await
        } else {
            migrate_one(storage, &run, &key, &mut said).await
        };
        // Known kept only while the claim is held: a run that lost it may
        // have been taken over, and the taker may have replaced the backup.
        let in_catalog_kept = key.status != BackupStatus::Deleted
            && !said.replacement_unknown
            && m.claim.lost().is_none();
        match step {
            // A deleted backup `--all` found nothing to do for: not
            // reported.
            Step::Done(Outcome::Deleted)
                if quiet
                    && key.status == BackupStatus::Deleted
                    && !said.wrote
                    && said.notes.is_empty() => {}
            Step::Done(outcome) => {
                let done = BackupOutcome {
                    account_id: key.account_id,
                    backup_arn: key.backup_arn,
                    outcome,
                    notes: said.notes,
                    in_catalog_kept,
                    wrote: said.wrote,
                };
                on_backup(&done);
                report.backups.push(done);
            }
            Step::Stop(reason) => {
                // What the backup's steps noted before the stop (a copy
                // left) is the stop's to report.
                if !said.notes.is_empty() {
                    let done = BackupOutcome {
                        account_id: key.account_id,
                        backup_arn: key.backup_arn,
                        outcome: Outcome::Failed(reason.clone()),
                        notes: said.notes,
                        in_catalog_kept,
                        wrote: said.wrote,
                    };
                    on_backup(&done);
                    report.backups.push(done);
                }
                report.stopped = Some(reason);
                break;
            }
        }
    }
    // A signal the last backup's replacement ran through stops the run too.
    if report.stopped.is_none()
        && let Some(signal) = m.interrupt.signalled()
    {
        report.stopped = Some(format!("{signal} received"));
    }
    watcher.abort();
    report
}

/// `--all`'s backups: those [`LegacyBackupReader::list_all`] lists, then
/// the DELETED ones.
async fn all_keys<S: LegacyBackupReader + ?Sized>(
    storage: &S,
) -> Result<Vec<LegacyBackupKey>, String> {
    let mut keys = storage
        .list_all()
        .await
        .map_err(|e| format!("the in-catalog backups could not be listed: {e}"))?;
    keys.extend(
        storage
            .list_deleted()
            .await
            .map_err(|e| format!("the deleted in-catalog backups could not be listed: {e}"))?,
    );
    Ok(keys)
}

/// What `<arn>` resolved to.
enum Named {
    /// The in-catalog backup it names.
    Key(LegacyBackupKey),
    /// Nothing to copy: the outcome.
    Done(BackupOutcome),
}

/// The in-catalog backup `arn` names, in any status (the ARN as written,
/// or rebuilt in the server's region, of the ARN's account: the first
/// spelling [`LegacyBackupReader::holder_status`] answers for), or, when
/// none does, what holds the ARN: a replacement that stopped before its end
/// is finished, and a catalog row of the configured store reports the
/// backup in the store or deleted.
async fn named<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    arn: &str,
) -> Result<Named, String> {
    let parts = extenddb_storage::util::check_backup_arn(arn.trim())
        .map_err(|e| format!("{arn} is not a backup ARN: {e}"))?;
    let spellings = BackupArnSpellings::new(&parts, run.m.region);
    let account_id = parts.account_id.to_owned();
    let read_failed = |e: StorageError| format!("the catalog could not be read: {e}");
    let names: Vec<&str> = std::iter::once(spellings.canonical())
        .chain(spellings.as_written.as_deref())
        .collect();
    for &spelling in &names {
        if let Some(status) = storage
            .holder_status(&account_id, spelling)
            .await
            .map_err(read_failed)?
        {
            return Ok(Named::Key(LegacyBackupKey {
                account_id,
                backup_arn: spelling.to_owned(),
                status,
            }));
        }
    }
    let mut done = BackupOutcome {
        account_id: account_id.clone(),
        backup_arn: spellings.canonical().to_owned(),
        outcome: Outcome::NotFound(
            "no in-catalog backup and no catalog row has this ARN".to_owned(),
        ),
        notes: Vec::new(),
        in_catalog_kept: false,
        wrote: false,
    };
    for &spelling in &names {
        match finish_stopped_replacement(storage, run, &account_id, spelling).await {
            Ok(None) => {}
            Ok(Some(outcome)) => {
                done.backup_arn = spelling.to_owned();
                done.outcome = outcome;
                done.wrote = true;
                return Ok(Named::Done(done));
            }
            Err(stop) => return Err(stop),
        }
    }
    let held = super::read_catalog_row(storage, &spellings)
        .await
        .map_err(read_failed)?;
    if let Some((spelling, row)) = held {
        done.backup_arn = spelling.to_arn_string();
        done.outcome = if row.store == *run.m.identity {
            // On Cassandra a replacement that stopped after its conversion
            // left the item buckets; the call removes them. Elsewhere it
            // finds nothing left.
            let mut said = Said::default();
            let outcome = match finish(storage, run, &row, &mut said).await {
                Step::Done(Outcome::Finished) => Outcome::InStore,
                Step::Done(Outcome::Refused(why)) => Outcome::NotFound(why),
                Step::Done(other) => other,
                Step::Stop(stop) => return Err(stop),
            };
            done.wrote = said.wrote;
            outcome
        } else {
            Outcome::NotFound(format!(
                "it is a backup of another store ({} {}, store id {}), not an in-catalog backup",
                row.store.store_kind, row.store.store_root, row.store.store_id
            ))
        };
    }
    Ok(Named::Done(done))
}

/// [`LegacyBackupReader::finish_replacement`] of `backup_arn`, after the
/// claim's re-check: `None` when there was nothing to finish; otherwise the
/// outcome by what the catalog index now holds under the ARN, "finished"
/// for an AVAILABLE row and "deleted" for none or one a DeleteBackup of the
/// migrated backup moved on.
async fn finish_stopped_replacement<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    account_id: &str,
    backup_arn: &str,
) -> Result<Option<Outcome>, String> {
    run.m.claim.recheck().await?;
    let finished = storage
        .finish_replacement(account_id, backup_arn)
        .await
        .map_err(|e| {
            format!("the catalog could not finish the replacement of {backup_arn}: {e}")
        })?;
    if !finished {
        return Ok(None);
    }
    let row = match BackupArn::parse(backup_arn) {
        Ok(arn) => storage
            .read_backup_row(&arn)
            .await
            .map_err(|e| format!("the catalog could not be read: {e}"))?,
        Err(_) => None,
    };
    Ok(Some(match row {
        Some(row) if row.status == BackupStatus::Available => Outcome::Finished,
        _ => Outcome::Deleted,
    }))
}

/// The replacement with `row`, a row of the configured store that is
/// already in the catalog index: what is left of the in-catalog backup is
/// removed, and the outcome follows the row's status. AVAILABLE is
/// "finished"; DELETING or DELETED (a DeleteBackup of the migrated backup)
/// is "deleted"; a CREATING or FAILED row is refused without the call.
/// Before an AVAILABLE in-catalog backup is removed for an AVAILABLE row,
/// the row's prefix is read as the main path reads its own copy
/// ([`verify_prefix`], the manifest's ARN, and the item total against the
/// count the in-catalog backup records): a row whose objects are gone or
/// short (a `backup sync` without the in-catalog skip adopted a stopped
/// run's copy, or the objects went since) refuses the backup with the
/// in-catalog backup kept, since it is the only whole copy, and names the
/// row. The call runs after the deletion fence and the claim's re-check,
/// and `said.wrote` records whether it removed anything: an AVAILABLE
/// in-catalog backup left for it (on MongoDB a DELETING one too), and on
/// Cassandra every call that finds the row
/// converted (it writes the row's list and status entries again and
/// deletes the item buckets of a conversion that stopped after its
/// transaction).
async fn finish<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    row: &BackupRow,
    said: &mut Said,
) -> Step {
    let deleted = match row.status {
        BackupStatus::Available => false,
        BackupStatus::Deleting | BackupStatus::Deleted => true,
        other => {
            return Step::Done(Outcome::Refused(format!(
                "its catalog row of the configured store is {other}"
            )));
        }
    };
    let left = match storage
        .holder_status(&row.account_id, &row.backup_arn)
        .await
    {
        Ok(left) => left,
        Err(e) => return Step::Stop(format!("the catalog could not be read: {e}")),
    };
    if !deleted
        && left == Some(BackupStatus::Available)
        && let Err(step) = prefix_holds_the_backup(storage, run, row).await
    {
        return step;
    }
    if BackupArn::parse(&row.backup_arn).is_ok()
        && let Err(stop) = fenced_recheck(
            storage,
            run,
            &row.backup_arn,
            left.is_some(),
            "the replacement",
        )
        .await
    {
        return stop;
    }
    // On Cassandra the call finds the row converted whenever no in-catalog
    // backup is left, and deletes its item buckets. Elsewhere it removes an
    // AVAILABLE in-catalog backup, and on MongoDB a DELETING one, and
    // leaves any other.
    let cassandra = run.m.source_backend == "cassandra";
    let removes = match left {
        Some(BackupStatus::Available) => true,
        Some(BackupStatus::Deleting) => run.m.source_backend == "mongodb",
        _ => false,
    };
    match storage.replace_with_index_row(row).await {
        Ok(Replaced::Replaced | Replaced::AlreadyReplaced) => {
            said.wrote = removes || cassandra;
            Step::Done(if deleted {
                Outcome::Deleted
            } else {
                Outcome::Finished
            })
        }
        Ok(Replaced::NotAvailable(_)) if deleted => {
            said.wrote = cassandra;
            Step::Done(Outcome::Deleted)
        }
        Ok(Replaced::NotAvailable(what)) => Step::Done(Outcome::Skipped(format!(
            "its catalog row is no longer AVAILABLE ({what}); run the command again"
        ))),
        Ok(Replaced::NotApplied(what)) => Step::Done(Outcome::Failed(format!(
            "{what}; the in-catalog backup is unchanged, and the catalog row is left; run the \
             command again"
        ))),
        Ok(Replaced::InUse(table)) => Step::Done(Outcome::Refused(in_use(&table))),
        Err(StorageError::AlreadyExists(what)) => Step::Done(Outcome::Refused(what)),
        Err(e) => {
            said.replacement_unknown = true;
            Step::Stop(format!(
                "the catalog could not finish the replacement of {}: {e}",
                row.backup_arn
            ))
        }
    }
}

fn in_use(table: &str) -> String {
    format!(
        "a restore into {table} is reading it (the table is still CREATING); once that restore \
         ends, or its target is removed with extenddb backup remove-target, run the command again"
    )
}

/// The check before an AVAILABLE in-catalog backup is removed for an
/// AVAILABLE catalog row of its ARN: the row's prefix read as the main path
/// reads its own copy ([`verify_prefix`], the manifest's ARN, the item
/// total against the count the in-catalog backup records). `Err` is the
/// backup's end: a refusal naming the row for a prefix whose objects are
/// gone, short or another backup's, a failure for a read that failed. The
/// refusal says what the run after the row is forgotten does: for a prefix
/// gone or short it copies the backup again; for one holding another
/// backup's manifest it refuses the backup on that manifest and copies
/// nothing, so the operator is told to look at the prefix first.
async fn prefix_holds_the_backup<S: LegacyBackupReader + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    row: &BackupRow,
) -> Result<(), Step> {
    let arn = BackupArn::parse(&row.backup_arn).map_err(|e| {
        Step::Done(Outcome::Refused(format!(
            "its catalog row's ARN names no store prefix ({e})"
        )))
    })?;
    let prefix = arn.store_prefix();
    let row_named = |why: String, then: &str| {
        Step::Done(Outcome::Refused(format!(
            "the catalog row (AVAILABLE) under {} names the prefix {prefix}, which {why}, and \
             the in-catalog backup is the whole copy. Forget that row (extenddb \
             backup sync --forget-backup {}){then}",
            row.backup_arn, row.backup_arn
        )))
    };
    const COPIES_AGAIN: &str = " and run the command again, which copies the backup again";
    let recorded = storage
        .restore_source(&row.account_id, &row.backup_arn)
        .await
        .map_err(|e| Step::Stop(format!("the catalog could not be read: {e}")))?
        .item_count;
    let calls = StoreCalls {
        store: &*run.m.configured.store,
        metrics: run.m.metrics,
    };
    let manifest = match verify_prefix(calls, &prefix, None).await {
        Ok(Verified::Complete(manifest)) => manifest,
        Ok(Verified::Incomplete(why)) => {
            return Err(row_named(
                format!("does not read back as a complete backup ({why})"),
                COPIES_AGAIN,
            ));
        }
        Err(e) => {
            return Err(Step::Done(Outcome::Failed(format!(
                "the prefix {prefix} of its catalog row could not be read: {e}"
            ))));
        }
    };
    if manifest.backup_arn().ok().as_ref() != Some(&arn) {
        return Err(row_named(
            "holds a manifest naming another backup".to_owned(),
            ", look at what the prefix holds and delete it if it is not a backup, then run the \
             command again, which copies the backup again; while that manifest is there the \
             command refuses the backup on it and copies nothing",
        ));
    }
    let total = manifest_item_count(&manifest);
    if total != recorded {
        return Err(row_named(
            format!("holds a copy of {total} items where the in-catalog backup records {recorded}"),
            COPIES_AGAIN,
        ));
    }
    Ok(())
}

/// One backup (module doc). `notes` collects what the operator is told
/// beside the outcome.
#[allow(clippy::too_many_lines)]
async fn migrate_one<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    key: &LegacyBackupKey,
    said: &mut Said,
) -> Step {
    let m = run.m;
    let arn_text = key.backup_arn.as_str();
    // 1. The id, before anything is read.
    let Ok(parts) = extenddb_storage::util::check_backup_arn(arn_text) else {
        return Step::Done(Outcome::Refused(
            "its ARN does not parse as a backup ARN".to_owned(),
        ));
    };
    let Some(created_at) = backup_creation_date_time(parts.backup_id) else {
        return Step::Done(Outcome::Refused(format!(
            "its backup id {} has the 13-digit shape a 0.1.x release issued, and a store prefix \
             carries only the 14-digit shape; {CARRY_FORWARD}",
            parts.backup_id
        )));
    };
    // 2. The table name, by the store-prefix rules.
    let arn = match BackupArn::parse(arn_text) {
        Ok(arn) if arn.partition() == ARN_PARTITION => arn,
        Ok(arn) => {
            return Step::Done(Outcome::Refused(format!(
                "its ARN is in partition {}, which no request to this server can address",
                arn.partition()
            )));
        }
        Err(e) => {
            return Step::Done(Outcome::Refused(format!(
                "its table name cannot name a store prefix ({e}); restore it into a table with \
                 another name and take a new backup of that table, which has a new ARN"
            )));
        }
    };
    // 3. A replacement that committed and stopped before its end.
    if key.status == BackupStatus::Deleting {
        match finish_stopped_replacement(storage, run, &key.account_id, arn_text).await {
            Ok(Some(outcome)) => {
                said.wrote = true;
                return Step::Done(outcome);
            }
            Ok(None) => {}
            Err(stop) => return Step::Stop(stop),
        }
    }
    // 4. A catalog row of the id: under either spelling of the ARN, then
    // under any ARN and any account (the id alone is the key the inserts
    // conflict on; the prefix carries no region, so it may be that row's).
    let spellings = BackupArnSpellings::new(&parts, m.region);
    let held = match super::read_catalog_row(storage, &spellings).await {
        Ok(held) => held,
        Err(e) => return Step::Stop(format!("the catalog could not be read: {e}")),
    };
    if let Some((spelling, row)) = &held {
        if *spelling != arn {
            return Step::Done(Outcome::Refused(holds_its_id(&spelling.to_arn_string())));
        }
        if row.store == *m.identity {
            return finish(storage, run, row, said).await;
        }
        return Step::Done(Outcome::Refused(format!(
            "a catalog row of another store ({} {}, store id {}) holds its ARN",
            row.store.store_kind, row.store.store_root, row.store.store_id
        )));
    }
    match storage.row_holding_backup_id(arn.backup_id()).await {
        Ok(None) => {}
        Ok(Some(row)) => return Step::Done(Outcome::Refused(holds_its_id(&row.backup_arn))),
        Err(e) => return Step::Stop(format!("the catalog could not be read: {e}")),
    }
    // 5. The status.
    match key.status {
        BackupStatus::Available => {}
        BackupStatus::Creating => {
            return Step::Done(Outcome::Skipped(
                "it is CREATING (a create in flight, or one that stopped, which the reconciler \
                 removes)"
                    .to_owned(),
            ));
        }
        BackupStatus::Deleting => {
            return Step::Done(Outcome::Skipped(
                "it is DELETING (a DeleteBackup in flight, or one that stopped; DeleteBackup \
                 again finishes it)"
                    .to_owned(),
            ));
        }
        other => {
            return Step::Done(Outcome::Skipped(format!("it is {other}")));
        }
    }
    // 6. The open.
    let opened = match storage.items(&key.account_id, arn_text).await {
        Ok(opened) => opened,
        Err(e) if is_backup_not_found(&e) => {
            return Step::Done(Outcome::Skipped(
                "it is no longer AVAILABLE (deleted since it was listed)".to_owned(),
            ));
        }
        Err(StorageError::Unsupported(why)) => {
            return Step::Done(Outcome::Refused(format!(
                "{why}; the legacy restore cannot read it either"
            )));
        }
        Err(e) => {
            return Step::Stop(format!("the in-catalog backup could not be opened: {e}"));
        }
    };
    // 7. The name.
    if !is_valid_backup_name(&opened.backup_name) {
        return Step::Done(Outcome::Refused(format!(
            "its name {:?} is not a backup name the store format accepts (3 to 255 characters of \
             a-z, A-Z, 0-9, _, - and .); {CARRY_FORWARD}, under a valid name",
            opened.backup_name
        )));
    }
    // 8. The definition: what the legacy restore refuses (it has no way
    // out in this release), then what only the copy cannot state.
    if let Err(e) = opened.source.ensure_restorable() {
        return Step::Done(Outcome::Refused(format!(
            "{}; the legacy restore refuses it too",
            storage_text(&e)
        )));
    }
    // The SSE key ARN is built in this server's region, the region the
    // legacy DescribeBackup names it in (a backup's ARN may carry another).
    let mut source_table = match source_table_from_legacy(
        &opened.source,
        opened.vector_state,
        m.region,
        &key.account_id,
    ) {
        Ok(source_table) => source_table,
        Err(e) => {
            return Step::Done(Outcome::Refused(format!(
                "{}; {CARRY_FORWARD}",
                storage_text(&e)
            )));
        }
    };
    let key_schema = match RestorableKeySchema::resolve(
        &source_table.key_schema,
        &source_table.attribute_definitions,
    ) {
        Ok(key_schema) => key_schema,
        Err(e) => {
            return Step::Done(Outcome::Refused(format!(
                "its key schema cannot be checked: {e}"
            )));
        }
    };
    if (opened.source.backup_creation_date_time - created_at).abs() >= 0.001 {
        said.notes.push(format!(
            "its in-catalog creation time {} is not its id's instant {created_at}; the copy \
             records the id's",
            opened.source.backup_creation_date_time
        ));
    }
    if arn.region() != m.region {
        said.notes.push(format!(
            "its region {} is not this server's; requests find it by its ARN as written",
            arn.region()
        ));
    }

    let configured = m.configured;
    let prefix = arn.store_prefix();
    let calls = StoreCalls {
        store: &*configured.store,
        metrics: m.metrics,
    };
    // A copy an earlier run left (no row holds the id: step 4 found none
    // under any ARN). Its manifest, when it has one, must name this
    // backup: on Cassandra, which cannot read a row by its id, it is what
    // tells a prefix another row's backup holds.
    match calls.store.list(&prefix).try_next().await {
        Ok(None) => {}
        Ok(Some(_)) => {
            match calls.manifest_arn(&prefix).await {
                Ok(None) => {}
                Ok(Some(Ok(named))) if named == arn_text => {}
                Ok(Some(found)) => {
                    return Step::Done(Outcome::Refused(other_manifest(&prefix, &found)));
                }
                Err(e) => {
                    return Step::Done(Outcome::Failed(format!(
                        "the store could not read the manifest under {prefix}: {e}"
                    )));
                }
            }
            if let Err(problem) = configured.fence().check_catalog(storage).await {
                return Step::Stop(format!(
                    "the deletion fence refused the deletion of the copy an earlier run left \
                     under {prefix}: {problem}"
                ));
            }
            match delete_leftover(run, calls, &prefix).await {
                Ok(()) => {}
                Err(Leftover::Lost(why)) => {
                    return Step::Stop(format!(
                        "{why}; the copy an earlier run left under {prefix} is left"
                    ));
                }
                Err(Leftover::Failed(why)) => {
                    return Step::Done(Outcome::Failed(format!(
                        "the copy an earlier run left under {prefix} could not be deleted: {why}"
                    )));
                }
            }
            said.wrote = true;
            said.notes.push(format!(
                "deleted the copy an earlier run left under {prefix}"
            ));
        }
        Err(e) => {
            return Step::Done(Outcome::Failed(format!(
                "the store could not list {prefix}: {}",
                calls.failed("list", e)
            )));
        }
    }
    // The fence before the first put.
    if let Err(problem) = configured.fence().check_catalog(storage).await {
        return Step::Stop(format!(
            "the deletion fence refused the first write: {problem}"
        ));
    }

    // What this run puts under the prefix, for a deletion that reaches
    // only that.
    let written = Mutex::new(Vec::new());
    let pipeline = Pipeline {
        store: &*configured.store,
        configured,
        identity: m.identity,
        prefix: &prefix,
        arn: arn_text,
        heartbeat: m.claim,
        shutdown: &run.shutdown,
        metrics: m.metrics,
        target_bytes: m.data_file_target_bytes,
        key_prefix: &configured.key_prefix,
        bucket: &configured.bucket,
        written: Some(&written),
    };
    let recorded = opened.source.item_count;
    let files =
        match writer::write_data_files(&pipeline, opened.items, key_schema, m.limits, None).await {
            Ok(files) => files,
            Err(stop) => return stopped(storage, run, key, &prefix, &written, stop, said).await,
        };
    // The count check: a DeleteBackup that landed while the stream ran
    // (SQLite, MongoDB, Cassandra) leaves it short, and the re-read of the
    // backup's status tells that case; otherwise the recorded count is
    // wrong (on PostgreSQL, which reads one snapshot, only that).
    if i64::try_from(files.item_count).ok() != Some(recorded) {
        let n = files.item_count;
        let short = format!("the in-catalog backup records {recorded} items and its read gave {n}");
        if let Err(stop) = delete_copy(storage, run, &prefix, &written, said).await {
            return stop_noting(&short, stop, said);
        }
        return Step::Done(match reread_status(storage, key).await {
            Reread::Gone(outcome) => outcome,
            Reread::Failed(e) => Outcome::Failed(format!(
                "{short}, and whether it is still AVAILABLE could not be read ({e})"
            )),
            // The create wrote each item at ONE and the read was at ALL, so
            // a short read is a row no live replica holds; the legacy
            // restore reads at ONE and is no way out.
            Reread::Available if m.source_backend == "cassandra" => Outcome::Refused(format!(
                "the in-catalog backup records {recorded} items and its read at consistency ALL \
                 gave {n}; its create wrote each item at consistency ONE, so a replica that alone \
                 held some items may have lost them. Run nodetool repair on the account's \
                 keyspace and run the command again; the legacy restore reads at ONE and may \
                 restore fewer items"
            )),
            Reread::Available => Outcome::Refused(format!(
                "{short}; the recorded count is wrong, and the legacy restore, which does not \
                 check it, still restores the backup: {CARRY_FORWARD}"
            )),
        });
    }
    source_table.item_count = recorded;
    source_table.table_size_bytes = i64::try_from(files.item_bytes).unwrap_or(i64::MAX);
    let item_count = files.item_count;
    let header = ManifestHeader {
        backup_arn: arn.clone(),
        backup_name: opened.backup_name.clone(),
        source_backend: m.source_backend.to_owned(),
        source_extenddb_version: m.source_extenddb_version.to_owned(),
        snapshot: SnapshotDescriptor {
            kind: SnapshotDescriptor::KIND_LEGACY_IN_CATALOG.to_owned(),
            marker: String::new(),
        },
        item_order: ItemOrder::Unordered,
        source_table,
    };
    let docs = match BackupDocuments::build(
        header,
        files.entries,
        &configured.key_prefix,
        &configured.bucket,
    ) {
        Ok(docs) => docs,
        Err(e) => {
            let why = format!("building the manifest: {e}");
            return match delete_copy(storage, run, &prefix, &written, said).await {
                Ok(_) => Step::Done(Outcome::Failed(why)),
                Err(stop) => stop_noting(&why, stop, said),
            };
        }
    };
    for (name, bytes) in docs.documents_before_manifest() {
        if let Err(stop) = put_document(&pipeline, name, bytes).await {
            return stopped(storage, run, key, &prefix, &written, stop, said).await;
        }
    }
    kill_point(KillPoint::BeforeManifest, &run.shutdown).await;
    // The fence and a live beat before the manifest: once it is put,
    // `backup sync` and a rerun read the prefix as a whole copy. A root
    // another catalog owns since the first put, or a claim lost, leaves the
    // copy and the in-catalog backup, and stops the run.
    if let Err(stop) = fenced_recheck(storage, run, arn_text, true, "the manifest's put").await {
        return stop;
    }
    let (name, bytes) = docs.manifest_document();
    if let Err(stop) = put_document(&pipeline, name, bytes).await {
        return stopped(storage, run, key, &prefix, &written, stop, said).await;
    }
    kill_point(KillPoint::AfterManifest, &run.shutdown).await;

    // The re-read: what a restore, sync and the dead-owner duty will read.
    let manifest = match verify_prefix(calls, &prefix, None).await {
        Ok(Verified::Complete(manifest)) => manifest,
        Ok(Verified::Incomplete(why)) => {
            return deleting_the_copy(
                storage,
                run,
                &prefix,
                &written,
                Outcome::Failed(format!(
                    "the copy does not read back as a complete backup: {why}"
                )),
                said,
            )
            .await;
        }
        Err(e) => {
            return deleting_the_copy(
                storage,
                run,
                &prefix,
                &written,
                Outcome::Failed(format!("the copy could not be read back: {e}")),
                said,
            )
            .await;
        }
    };
    if manifest.backup_arn().ok().as_ref() != Some(&arn) {
        return deleting_the_copy(
            storage,
            run,
            &prefix,
            &written,
            Outcome::Failed("the copy's manifest names another backup".to_owned()),
            said,
        )
        .await;
    }
    let total = manifest_item_count(&manifest);
    if total != recorded {
        return deleting_the_copy(
            storage,
            run,
            &prefix,
            &written,
            Outcome::Refused(format!(
                "the copy's manifest counts {total} items and the in-catalog backup records \
                 {recorded}"
            )),
            said,
        )
        .await;
    }
    let source_table_details_json = match serde_json::to_value(&manifest.source_table) {
        Ok(value) => value,
        Err(e) => {
            return deleting_the_copy(
                storage,
                run,
                &prefix,
                &written,
                Outcome::Failed(format!("the source table does not serialize: {e}")),
                said,
            )
            .await;
        }
    };
    let row = BackupRow {
        backup_id: arn.backup_id().to_owned(),
        account_id: key.account_id.clone(),
        table_name: arn.table_name().to_owned(),
        backup_arn: arn_text.to_owned(),
        backup_name: manifest.backup_name.clone(),
        status: BackupStatus::Available,
        store: m.identity.clone(),
        size_bytes: manifest.backup_size_bytes,
        item_count: total,
        created_at,
        completed_at: None,
        failure_reason: None,
        source_table_details_json,
        owner_generation: UNOWNED_GENERATION,
        heartbeat_at: 0.0,
    };
    // The fence and a live beat before the replacement: a run that lost the
    // claim, or whose root another catalog owns, leaves the copy, which the
    // next run deletes (and rewrites while the backup is AVAILABLE).
    if let Err(stop) = fenced_recheck(storage, run, arn_text, true, "the replacement").await {
        return stop;
    }
    match storage.replace_with_index_row(&row).await {
        Ok(Replaced::Replaced) => {
            said.wrote = true;
            Step::Done(Outcome::Migrated {
                item_count,
                size_bytes: manifest.backup_size_bytes,
            })
        }
        Ok(Replaced::AlreadyReplaced) => {
            said.wrote = true;
            Step::Done(Outcome::Finished)
        }
        Ok(Replaced::NotAvailable(what)) => {
            deleting_the_copy(
                storage,
                run,
                &prefix,
                &written,
                Outcome::Skipped(format!(
                    "it was no longer AVAILABLE when its row was to be written ({what})"
                )),
                said,
            )
            .await
        }
        // The conversion's condition failed at every attempt (Cassandra): a
        // DeleteBackup or a restore's registration raced each one, and
        // nothing was written; the copy is deleted and a later run copies
        // the backup again.
        Ok(Replaced::NotApplied(what)) => {
            deleting_the_copy(
                storage,
                run,
                &prefix,
                &written,
                Outcome::Failed(format!(
                    "{what}; the in-catalog backup is unchanged, and a later run copies it again"
                )),
                said,
            )
            .await
        }
        Ok(Replaced::InUse(table)) => {
            deleting_the_copy(
                storage,
                run,
                &prefix,
                &written,
                Outcome::Refused(in_use(&table)),
                said,
            )
            .await
        }
        // Another row holds the id (one written since step 4 read none):
        // its bytes were under the prefix this run wrote over, so the copy
        // stays for the operator to look at.
        Err(StorageError::AlreadyExists(what)) => {
            said.notes.push(format!(
                "the copy under {prefix} is left: a catalog row of its id, written since this \
                 run read none, may hold that prefix; forget that row (extenddb backup sync \
                 --forget-backup) if it is not a backup"
            ));
            Step::Done(Outcome::Refused(what))
        }
        // The outcome is unknown: the row may be in, and the in-catalog
        // backup gone. The copy stays, and the next run finishes (a row
        // there) or deletes it (none). On Cassandra `--all` lists no
        // converted row, so the finish there is `<arn>`'s.
        Err(e) => {
            said.replacement_unknown = true;
            let next = if m.source_backend == "cassandra" {
                "extenddb backup migrate-legacy <arn> finishes the replacement (--all lists no \
                 converted row) or deletes the copy"
            } else {
                "the next run finishes the replacement or deletes the copy"
            };
            Step::Stop(format!(
                "the replacement failed: {e}; the copy under {prefix} is left, and {next}"
            ))
        }
    }
}

/// The deletion fence and a live beat before a step that cannot be undone
/// (the manifest's put, the replacement): `Err` is the run's stop, with the
/// copy under the backup's prefix and the in-catalog backup left as they
/// are. The fence's stop names the backup and, where one is there
/// (`in_catalog`), says its in-catalog backup is kept.
async fn fenced_recheck<S: BackupCatalog + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    backup_arn: &str,
    in_catalog: bool,
    step: &str,
) -> Result<(), Step> {
    let prefix = BackupArn::parse(backup_arn).map_or_else(
        |_| format!("the prefix of {backup_arn}"),
        |arn| arn.store_prefix(),
    );
    if let Err(problem) = run.m.configured.fence().check_catalog(storage).await {
        let kept = if in_catalog {
            ", its in-catalog backup is kept"
        } else {
            ""
        };
        return Err(Step::Stop(format!(
            "the deletion fence refused {step} of {backup_arn}: {problem}; the copy under \
             {prefix} is left{kept}, and nothing more is written or deleted"
        )));
    }
    run.m.claim.recheck().await.map_err(|why| {
        Step::Stop(format!(
            "{why}; this run writes and deletes nothing more under {prefix}: another \
             command may hold the claim and be writing there, and a later run deletes \
             what is left only when no catalog row holds the backup"
        ))
    })
}

/// The end of a backup whose copy is deleted first: `outcome` once
/// [`delete_copy`] ran (whole or in part, the notes say which), the run's
/// stop when it could not run (the claim lost, the fence refusing it), the
/// outcome it would have had kept as a note.
async fn deleting_the_copy<S: BackupCatalog + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    prefix: &str,
    written: &Mutex<Vec<ObjectMeta>>,
    outcome: Outcome,
    said: &mut Said,
) -> Step {
    match delete_copy(storage, run, prefix, written, said).await {
        Ok(_) => Step::Done(outcome),
        Err(stop) => stop_noting(&outcome_text(&outcome), stop, said),
    }
}

/// The run's stop at a copy's deletion that could not run, with the end
/// the backup would have had (`what`) kept as a note.
fn stop_noting(what: &str, stop: String, said: &mut Said) -> Step {
    said.notes.push(format!("before the stop: {what}"));
    Step::Stop(stop)
}

/// An outcome as a note names it.
fn outcome_text(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Refused(why) | Outcome::NotFound(why) => format!("refused ({why})"),
        Outcome::Skipped(why) => format!("skipped ({why})"),
        Outcome::Failed(why) => format!("failed ({why})"),
        other => format!("{other:?}"),
    }
}

/// What the re-read of the in-catalog backup's status found, after a read
/// that ended short or failed.
enum Reread {
    /// Still AVAILABLE: the read's end was the read's own.
    Available,
    /// No longer AVAILABLE: skipped, "deleted during the copy".
    Gone(Outcome),
    /// The status could not be read: whether the backup was deleted during
    /// the copy is unknown, and the backup fails with the reason.
    Failed(String),
}

async fn reread_status<S: LegacyBackupReader + ?Sized>(
    storage: &S,
    key: &LegacyBackupKey,
) -> Reread {
    match storage
        .holder_status(&key.account_id, &key.backup_arn)
        .await
    {
        Ok(Some(BackupStatus::Available)) => Reread::Available,
        Ok(status) => Reread::Gone(Outcome::Skipped(format!(
            "it was deleted during the copy (the in-catalog backup is {})",
            status.map_or_else(|| "gone".to_owned(), |s| s.to_string())
        ))),
        Err(e) => Reread::Failed(e.to_string()),
    }
}

/// The refusal of a backup whose id a catalog row under `held` holds.
fn holds_its_id(held: &str) -> String {
    format!(
        "a catalog row under {held} holds its backup id, and its store prefix may hold that \
         row's backup; nothing under it is deleted. Forget that row (extenddb backup sync \
         --forget-backup) if it is not a backup, then run the command again"
    )
}

/// The refusal of a prefix whose manifest does not name this backup.
fn other_manifest(prefix: &str, found: &Result<String, String>) -> String {
    let what = match found {
        Ok(named) => format!("names {named}"),
        Err(why) => format!("does not read ({why})"),
    };
    format!(
        "the manifest under {prefix} {what}: the prefix may hold another backup, and nothing \
         under it is deleted; look at it, and delete it if it is not a backup"
    )
}

/// A [`StorageError`]'s text without the variant's wrapper.
fn storage_text(e: &StorageError) -> String {
    match e {
        StorageError::Validation(text)
        | StorageError::Unsupported(text)
        | StorageError::Internal(text) => text.clone(),
        other => other.to_string(),
    }
}

/// The writer's stop (`write_data_files` or a document's put), as the
/// backup's end: a failure or a refused item ends this backup with its copy
/// deleted (a failure of a backup no longer AVAILABLE, a stream a
/// DeleteBackup ended, is a skip: "deleted during the copy"); a signal
/// stops the run with the copy deleted, and a note naming it; a lost claim,
/// a store found unhealthy or fenced, and an object another run wrote under
/// the prefix stop the run with no further store call.
async fn stopped<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    key: &LegacyBackupKey,
    prefix: &str,
    written: &Mutex<Vec<ObjectMeta>>,
    stop: Stop,
    said: &mut Said,
) -> Step {
    match stop {
        Stop::Refused(reason) => {
            deleting_the_copy(
                storage,
                run,
                prefix,
                written,
                Outcome::Refused(format!(
                    "{reason}; restore it (RestoreTableFromBackup), correct the items the log \
                     names, and take a new backup of the restored table, which has a new ARN"
                )),
                said,
            )
            .await
        }
        Stop::Fail(reason) => {
            if let Err(stop) = delete_copy(storage, run, prefix, written, said).await {
                return stop_noting(&format!("failed ({reason})"), stop, said);
            }
            Step::Done(match reread_status(storage, key).await {
                Reread::Gone(outcome) => outcome,
                Reread::Available => Outcome::Failed(reason),
                Reread::Failed(e) => Outcome::Failed(format!(
                    "{reason}; whether the in-catalog backup is still AVAILABLE could not be \
                     read ({e})"
                )),
            })
        }
        Stop::Cancelled => {
            match delete_copy(storage, run, prefix, written, said).await {
                Ok(true) => said.notes.push(format!(
                    "deleted the objects this run recorded under {prefix}; a put under way at \
                     the signal may still have stored its object, which the next run deletes"
                )),
                Ok(false) => {}
                Err(stop) => return Step::Stop(stop),
            }
            Step::Stop(match run.m.interrupt.signalled() {
                Some(signal) => format!("{signal} received during the copy"),
                None => "the copy was stopped".to_owned(),
            })
        }
        Stop::Occupied(why) => Step::Stop(format!(
            "{why}; this run writes and deletes nothing more under {prefix}, and a later run \
             deletes what is left only when no catalog row holds the backup"
        )),
        Stop::ClaimLost(why) => Step::Stop(format!(
            "{why}; this run writes and deletes nothing more under {prefix}: another \
             command may hold the claim and be writing there, and a later run deletes \
             what is left only when no catalog row holds the backup"
        )),
        Stop::StoreUnhealthy(why) => Step::Stop(why),
    }
}

/// Delete this run's copy: the objects its pipeline wrote
/// ([`Pipeline::written`]), the last written first (the manifest, when
/// there is one, before the files it names), each with
/// [`BackupStore::delete_written`](extenddb_backup_store::BackupStore::delete_written),
/// which leaves an object another run wrote under the key since. It runs
/// through the deletion fence, after the claim's re-check, and reads the
/// claim before each object, so a process stopped mid-deletion and resumed
/// past two periods removes nothing more than the object under way. No
/// prefix walk: what this run did not write is not this run's to delete.
/// `Ok(true)` when every object it wrote is gone, `Ok(false)` when the
/// store left some (a note says what); `Err` when the fence refused the
/// deletion or the claim is lost, before it or mid-way: the run stops with
/// the reason, as it does at the re-checks before the manifest and the
/// replacement, and a note says what is left.
async fn delete_copy<S: BackupCatalog + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    prefix: &str,
    written: &Mutex<Vec<ObjectMeta>>,
    said: &mut Said,
) -> Result<bool, String> {
    let notes = &mut said.notes;
    let left = |why: &str| {
        format!(
            "its copy under {prefix} was not deleted ({why}); backup sync does not adopt it, \
             and the next run of the command deletes it"
        )
    };
    let objects = std::mem::take(&mut *written.lock().unwrap_or_else(PoisonError::into_inner));
    if objects.is_empty() {
        return Ok(true);
    }
    let configured = run.m.configured;
    if let Err(problem) = configured.fence().check_catalog(storage).await {
        notes.push(left(&problem.to_string()));
        return Err(format!(
            "the deletion fence refused the deletion of the copy under {prefix}: {problem}; \
             nothing more is written or deleted"
        ));
    }
    if let Err(why) = run.m.claim.recheck().await {
        notes.push(left(&why));
        return Err(format!(
            "{why}; this run writes and deletes nothing more under {prefix}: another command \
             may hold the claim and be writing there, and a later run deletes what is left \
             only when no catalog row holds the backup"
        ));
    }
    let calls = StoreCalls {
        store: &*configured.store,
        metrics: run.m.metrics,
    };
    let mut whole = true;
    for (done, object) in objects.iter().rev().enumerate() {
        if let Some(why) = run.m.claim.lost() {
            notes.push(format!(
                "its copy under {prefix} was deleted in part ({why}): {} of its {} objects are \
                 left; backup sync does not adopt it, and the next run of the command deletes it",
                objects.len() - done,
                objects.len()
            ));
            return Err(format!(
                "{why}; this run writes and deletes nothing more under {prefix}: another command \
                 may hold the claim and be writing there, and a later run deletes what is left \
                 only when no catalog row holds the backup"
            ));
        }
        match calls.store.delete_written(object).await {
            Ok(true) => {}
            Ok(false) => {
                whole = false;
                notes.push(format!(
                    "{} holds an object this run did not write, which is left",
                    object.key
                ));
            }
            Err(e) => {
                notes.push(left(&calls.failed("delete_written", e).to_string()));
                return Ok(false);
            }
        }
    }
    Ok(whole)
}

/// Why [`delete_leftover`] stopped.
enum Leftover {
    /// The claim is lost: the run stops.
    Lost(String),
    /// The store failed the deletion: this backup fails.
    Failed(String),
}

/// Delete what a stopped run left under `prefix`, by the prefix walk: the
/// one deletion that reaches objects this run did not write, used only for
/// a prefix the caller has shown no catalog row holds and whose manifest,
/// if any, names this backup. Each pass follows a beat the run waits for
/// ([`HeldClaim::recheck`]) and starts no removal at or after
/// [`HeldClaim::acting_deadline`]
/// ([`delete_prefix_before`](extenddb_backup_store::BackupStore::delete_prefix_before)),
/// so a process stopped mid-walk and resumed past it removes nothing more
/// than the entry under way; a pass that reaches the deadline is followed
/// by another. A loss the timer records ends the walk at its next removal
/// (the walk's future is dropped).
async fn delete_leftover(
    run: &Run<'_, '_>,
    calls: StoreCalls<'_>,
    prefix: &str,
) -> Result<(), Leftover> {
    let claim = run.m.claim;
    loop {
        claim.recheck().await.map_err(Leftover::Lost)?;
        let deadline = claim.acting_deadline();
        let walk = tokio::select! {
            biased;
            () = claim.lost_signal().cancelled() => {
                return Err(Leftover::Lost(claim.lost().unwrap_or_else(|| {
                    "the maintenance claim is lost".to_owned()
                })));
            }
            walk = calls.store.delete_prefix_before(prefix, deadline) => walk,
        };
        match walk {
            Ok(done) if done.finished => return Ok(()),
            Ok(done) if done.deleted == 0 => {
                return Err(Leftover::Failed(
                    "the deletion removed nothing within two heartbeat periods of a confirmed \
                     beat"
                        .to_owned(),
                ));
            }
            Ok(_) => {}
            Err(e) => {
                return Err(Leftover::Failed(
                    calls.failed("delete_prefix", e).to_string(),
                ));
            }
        }
    }
}

/// A DELETED in-catalog backup (module doc). A catalog row of its id,
/// under either spelling of its ARN or under any ARN, refuses it: the
/// prefix may be that row's, and a row under its own ARN is a backup a
/// client deleted that is back in the catalog (a `backup sync` of an
/// earlier release adopted a stopped run's copy), which the operator
/// decides about. Otherwise, behind the fence: the copy a stopped run left
/// under its prefix is deleted, when its manifest (if any) names this
/// backup; and once the store lists nothing there, the DELETED in-catalog
/// record is removed ([`LegacyBackupReader::remove_deleted`]), so the
/// prefix is known clean and the record is not listed again. Each deletion
/// and the removal run after the claim's re-check. "deleted" either way,
/// with a note for what was deleted or removed. A backup whose id has the
/// 13-digit shape a 0.1.x release issued, or whose table name forms no
/// prefix, never had a copy, and is left as it is.
async fn clean_deleted<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    key: &LegacyBackupKey,
    said: &mut Said,
) -> Step {
    let m = run.m;
    let Ok(parts) = extenddb_storage::util::check_backup_arn(&key.backup_arn) else {
        return Step::Done(Outcome::Deleted);
    };
    if backup_creation_date_time(parts.backup_id).is_none() {
        return Step::Done(Outcome::Deleted);
    }
    let Ok(arn) = BackupArn::parse(&key.backup_arn) else {
        return Step::Done(Outcome::Deleted);
    };
    let prefix = arn.store_prefix();
    let calls = StoreCalls {
        store: &*m.configured.store,
        metrics: m.metrics,
    };
    let spellings = BackupArnSpellings::new(&parts, m.region);
    let held = match super::read_catalog_row(storage, &spellings).await {
        Ok(Some((_, row))) => Some((true, row)),
        Ok(None) => match storage.row_holding_backup_id(arn.backup_id()).await {
            Ok(row) => row.map(|row| (false, row)),
            Err(e) => return Step::Stop(format!("the catalog could not be read: {e}")),
        },
        Err(e) => return Step::Stop(format!("the catalog could not be read: {e}")),
    };
    if let Some((own_arn, row)) = held {
        return Step::Done(Outcome::Refused(row_holds_a_deleted_backup(own_arn, &row)));
    }
    if let Err(problem) = m.configured.fence().check_catalog(storage).await {
        return Step::Stop(format!(
            "the deletion fence refused the deletion of what a stopped run left under \
             {prefix}: {problem}"
        ));
    }
    match calls
        .prefix_has_objects(&prefix)
        .await
        .map_err(|e| format!("the store could not list {prefix}: {e}"))
    {
        Ok(false) => {}
        Ok(true) => {
            match calls.manifest_arn(&prefix).await {
                Ok(None) => {}
                Ok(Some(Ok(named))) if named == key.backup_arn => {}
                Ok(Some(found)) => {
                    return Step::Done(Outcome::Refused(other_manifest(&prefix, &found)));
                }
                Err(e) => {
                    return Step::Done(Outcome::Failed(format!(
                        "the store could not read the manifest under {prefix}: {e}"
                    )));
                }
            }
            match delete_leftover(run, calls, &prefix).await {
                Ok(()) => {}
                Err(Leftover::Lost(why)) => {
                    return Step::Stop(format!(
                        "{why}; the copy a stopped run left under {prefix} is left"
                    ));
                }
                Err(Leftover::Failed(why)) => {
                    return Step::Done(Outcome::Failed(format!(
                        "the copy a stopped run left under {prefix} could not be deleted: {why}; \
                         backup sync does not adopt it, and the next run of the command deletes it"
                    )));
                }
            }
            said.wrote = true;
            said.notes.push(format!(
                "deleted the copy a stopped run left under {prefix}: its in-catalog backup was \
                 deleted"
            ));
            match calls
                .prefix_has_objects(&prefix)
                .await
                .map_err(|e| format!("the store could not list {prefix}: {e}"))
            {
                Ok(false) => {}
                Ok(true) => {
                    return Step::Done(Outcome::Failed(format!(
                        "the store still lists objects under {prefix} after their deletion; the \
                         next run of the command deletes them"
                    )));
                }
                Err(why) => return Step::Done(Outcome::Failed(why)),
            }
        }
        Err(why) => return Step::Done(Outcome::Failed(why)),
    }
    if let Err(why) = m.claim.recheck().await {
        return Step::Stop(why);
    }
    match storage
        .remove_deleted(&key.account_id, &key.backup_arn)
        .await
    {
        Ok(true) => {
            said.wrote = true;
            said.notes.push(format!(
                "removed its in-catalog record: nothing is left under {prefix}"
            ));
        }
        Ok(false) => {}
        Err(e) => {
            return Step::Stop(format!(
                "the catalog could not remove the record of the deleted in-catalog backup \
                 {}: {e}",
                key.backup_arn
            ));
        }
    }
    Step::Done(Outcome::Deleted)
}

/// The refusal of a DELETED in-catalog backup whose id `row` holds, under
/// a spelling of the backup's own ARN (`own_arn`, the spellings
/// DescribeBackup resolves) or another ARN.
fn row_holds_a_deleted_backup(own_arn: bool, row: &BackupRow) -> String {
    if own_arn && row.status == BackupStatus::Available {
        format!(
            "a catalog row (AVAILABLE) under {} holds the ARN of this in-catalog backup, which a \
             DeleteBackup deleted: the backup is in the catalog again (a backup sync of an \
             earlier release adopted a copy a stopped run left). DeleteBackup the ARN again to \
             delete it, or keep it; nothing is deleted",
            row.backup_arn
        )
    } else {
        format!(
            "a catalog row ({}) under {} holds its backup id, and its store prefix may hold \
             that row's backup; nothing under it is deleted, and its in-catalog record is kept",
            row.status, row.backup_arn
        )
    }
}
