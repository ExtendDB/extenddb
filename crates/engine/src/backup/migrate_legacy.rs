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
//! 1. The id: one that carries no instant (the 13-digit and bare shapes
//!    every 0.1.x release issued) is refused, since a store prefix carries
//!    only the 14-digit shape. The way out is a restore of the backup and a
//!    new backup of the restored table, which has a new ARN.
//! 2. The table name: one the store-prefix rules refuse (`BackupArn`: over
//!    255 bytes, or dots only) is refused.
//! 3. A replacement that committed and stopped before its end (MongoDB's
//!    DELETING document marked `migrated_to_store`): a DELETING backup is
//!    first handed to [`LegacyBackupReader::finish_replacement`], which
//!    finishes it whatever the catalog index row holds ("finished").
//! 4. A catalog row of the backup's id under either spelling of its ARN:
//!    one of the configured store under the stored spelling is an earlier
//!    run's (or a `backup sync` that adopted an earlier run's copy), and
//!    [`LegacyBackupReader::replace_with_index_row`] with that row removes
//!    what is left of the in-catalog backup ("finished" when the row is
//!    AVAILABLE, "deleted" when a DeleteBackup of the migrated backup came
//!    since); one of another store, or one under the other spelling,
//!    refuses the backup, since its prefix may be that row's.
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
//! for it are what an earlier run left (it stopped before its
//! replacement): they are deleted through the deletion fence first, so
//! every copy is written whole by one run. The fence runs before the first
//! put as CreateBackup's does. The items stream through the backup job's
//! writer ([`writer::write_data_files`]), each checked with the item
//! validator under this instance's limits, into data files under the
//! backup's prefix; then the three documents, the claim check, and
//! `extenddb-manifest.json`, whose `snapshot.kind` is
//! [`SnapshotDescriptor::KIND_LEGACY_IN_CATALOG`] with no marker, whose
//! item order is `unordered`, and whose `source_backend` and
//! `source_extenddb_version` name this binary. The prefix is then read back
//! through [`verify_prefix`] (the check `backup sync` and the dead-owner
//! duty apply), and the manifest's item total must equal both the items
//! written and the count the in-catalog backup records. Only then, and with
//! the claim confirmed, is the row written, by
//! [`LegacyBackupReader::replace_with_index_row`]: AVAILABLE, unowned
//! (`owner_generation` 0, as `backup sync` writes one), in the configured
//! store's identity, with the manifest's name, size, item total and
//! `source_table`, and the id's instant as `created_at`. A refusal or
//! failure after the first put deletes the prefix through the fence and
//! keeps the in-catalog backup; an index row of the id that the replacement
//! meets (`AlreadyExists`) refuses the backup and leaves the prefix, whose
//! bytes may be that row's.
//!
//! A DELETED in-catalog backup (a DeleteBackup of the legacy ARN, which the
//! claim does not hold back) has nothing to migrate. A copy a stopped run
//! left under its prefix is deleted through the fence, unless a catalog
//! row of the id holds the prefix under either spelling ("deleted"; with
//! `--all` only a backup whose copy was found is reported). `backup sync`
//! never adopts such a prefix, so a backup DeleteBackup answered DELETED
//! does not come back.
//!
//! A refusal is a property of one backup, or a race it lost (a DeleteBackup
//! or a restore that came first): it is reported and the next backup runs.
//! A lost claim, a signal, a store the writer finds unhealthy or fenced, a
//! failed listing and a catalog call that fails stop the run at once
//! ([`MigrateReport::stopped`]). A signal is acted on between backups and,
//! inside the copy, at the writer's next step, the copy deleted. Once the
//! manifest is written the replacement is not interrupted: it finishes, and
//! a signal received meanwhile then stops the run, whether or not another
//! backup follows. A run that stops with a copy under a prefix and no row
//! for it (a lost claim, a crash, a replacement whose answer did not come)
//! leaves the in-catalog backup in place; the next run deletes the copy,
//! and writes it again while the backup is AVAILABLE. `backup sync` skips
//! such a prefix while an in-catalog backup holds the ARN, in any status.

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

/// The maintenance claim as the writer reads it: confirmed by
/// [`HeldClaim::lost`]'s rule (a beat confirmed within two periods, or one
/// in flight within its bound, and no loss recorded).
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
    /// The server's region: `<arn>` is also looked up rebuilt in it, and a
    /// backup of another region is noted.
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
    /// stopped run left is deleted, in the notes), or a catalog row of the
    /// configured store that a DeleteBackup moved to DELETING or DELETED.
    /// Nothing to migrate.
    Deleted,
    /// Not migrated, for a reason of its own; the in-catalog backup is
    /// kept. The text gives the reason and the way out.
    Refused(String),
    /// `<arn>` names no in-catalog backup and no catalog row of the
    /// configured store in a state to report; the text says what holds the
    /// ARN, if anything. Counted as refused.
    NotFound(String),
    /// Not AVAILABLE, or no longer AVAILABLE when its row was written; the
    /// text says which.
    Skipped(String),
    /// A store or catalog call for this backup failed; the in-catalog
    /// backup is kept and a later run tries again.
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
    /// instant.
    pub notes: Vec<String>,
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
        let mut notes = Vec::new();
        let step = if key.status == BackupStatus::Deleted {
            clean_deleted(storage, &run, &key, &mut notes).await
        } else {
            migrate_one(storage, &run, &key, &mut notes).await
        };
        match step {
            // A deleted backup `--all` found nothing under: not reported.
            Step::Done(Outcome::Deleted)
                if quiet && key.status == BackupStatus::Deleted && notes.is_empty() => {}
            Step::Done(outcome) => {
                let done = BackupOutcome {
                    account_id: key.account_id,
                    backup_arn: key.backup_arn,
                    outcome,
                    notes,
                };
                on_backup(&done);
                report.backups.push(done);
            }
            Step::Stop(reason) => {
                // What the backup's steps noted before the stop (a copy
                // left) is the stop's to report.
                if !notes.is_empty() {
                    let done = BackupOutcome {
                        account_id: key.account_id,
                        backup_arn: key.backup_arn,
                        outcome: Outcome::Failed(reason.clone()),
                        notes,
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
    };
    for &spelling in &names {
        if storage
            .finish_replacement(&account_id, spelling)
            .await
            .map_err(read_failed)?
        {
            done.backup_arn = spelling.to_owned();
            done.outcome = Outcome::Finished;
            return Ok(Named::Done(done));
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
            match finish(storage, &row).await {
                Step::Done(Outcome::Finished) => Outcome::InStore,
                Step::Done(Outcome::Refused(why)) => Outcome::NotFound(why),
                Step::Done(other) => other,
                Step::Stop(stop) => return Err(stop),
            }
        } else {
            Outcome::NotFound(format!(
                "it is a backup of another store ({} {}, store id {}), not an in-catalog backup",
                row.store.store_kind, row.store.store_root, row.store.store_id
            ))
        };
    }
    Ok(Named::Done(done))
}

/// The replacement with `row`, a row of the configured store that is
/// already in the catalog index: what is left of the in-catalog backup is
/// removed, and the outcome follows the row's status. AVAILABLE is
/// "finished"; DELETING or DELETED (a DeleteBackup of the migrated backup)
/// is "deleted"; a CREATING or FAILED row is refused without the call.
async fn finish<S: LegacyBackupReader + ?Sized>(storage: &S, row: &BackupRow) -> Step {
    let deleted = match row.status {
        BackupStatus::Available => false,
        BackupStatus::Deleting | BackupStatus::Deleted => true,
        other => {
            return Step::Done(Outcome::Refused(format!(
                "its catalog row of the configured store is {other}"
            )));
        }
    };
    match storage.replace_with_index_row(row).await {
        Ok(Replaced::Replaced | Replaced::AlreadyReplaced) if deleted => {
            Step::Done(Outcome::Deleted)
        }
        Ok(Replaced::Replaced | Replaced::AlreadyReplaced) => Step::Done(Outcome::Finished),
        Ok(Replaced::NotAvailable(_)) if deleted => Step::Done(Outcome::Deleted),
        Ok(Replaced::NotAvailable(what)) => Step::Done(Outcome::Skipped(format!(
            "its catalog row is no longer AVAILABLE ({what}); run the command again"
        ))),
        Ok(Replaced::InUse(table)) => Step::Done(Outcome::Refused(in_use(&table))),
        Err(StorageError::AlreadyExists(what)) => Step::Done(Outcome::Refused(what)),
        Err(e) => Step::Stop(format!(
            "the catalog could not finish the replacement of {}: {e}",
            row.backup_arn
        )),
    }
}

fn in_use(table: &str) -> String {
    format!(
        "a restore into {table} is reading it (the table is still CREATING); once that restore \
         ends, or its target is removed with extenddb backup remove-target, run the command again"
    )
}

/// One backup (module doc). `notes` collects what the operator is told
/// beside the outcome.
#[allow(clippy::too_many_lines)]
async fn migrate_one<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    key: &LegacyBackupKey,
    notes: &mut Vec<String>,
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
            "its backup id {} carries no instant (an id a 0.1.x release issued), and a store \
             prefix carries only the 14-digit shape; {CARRY_FORWARD}",
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
        match storage.finish_replacement(&key.account_id, arn_text).await {
            Ok(true) => return Step::Done(Outcome::Finished),
            Ok(false) => {}
            Err(e) => {
                return Step::Stop(format!(
                    "the catalog could not finish the replacement of {arn_text}: {e}"
                ));
            }
        }
    }
    // 4. A catalog row of the id under either spelling.
    let spellings = BackupArnSpellings::new(&parts, m.region);
    let held = match super::read_catalog_row(storage, &spellings).await {
        Ok(held) => held,
        Err(e) => return Step::Stop(format!("the catalog could not be read: {e}")),
    };
    if let Some((spelling, row)) = &held {
        if *spelling != arn {
            return Step::Done(Outcome::Refused(format!(
                "a catalog row under {} holds its backup id, and its store prefix may hold \
                 that row's backup; forget that row (extenddb backup sync --forget-backup) if \
                 it is not a backup, then run the command again",
                spelling.to_arn_string()
            )));
        }
        if row.store == *m.identity {
            return finish(storage, row).await;
        }
        return Step::Done(Outcome::Refused(format!(
            "a catalog row of another store ({} {}, store id {}) holds its ARN",
            row.store.store_kind, row.store.store_root, row.store.store_id
        )));
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
    let mut source_table = match source_table_from_legacy(
        &opened.source,
        opened.vector_state,
        arn.region(),
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
        notes.push(format!(
            "its in-catalog creation time {} is not its id's instant {created_at}; the copy \
             records the id's",
            opened.source.backup_creation_date_time
        ));
    }
    if arn.region() != m.region {
        notes.push(format!(
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
    // A copy an earlier run left (no row holds the prefix: the read above
    // found none under either spelling).
    match calls.store.list(&prefix).try_next().await {
        Ok(None) => {}
        Ok(Some(_)) => {
            if let Err(problem) = configured.fence().check_catalog(storage).await {
                return Step::Stop(format!(
                    "the deletion fence refused the deletion of the copy an earlier run left \
                     under {prefix}: {problem}"
                ));
            }
            if let Err(e) = calls.store.delete_prefix(&prefix).await {
                return Step::Done(Outcome::Failed(format!(
                    "the copy an earlier run left under {prefix} could not be deleted: {}",
                    calls.failed("delete_prefix", e)
                )));
            }
            notes.push(format!(
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
    };
    let recorded = opened.source.item_count;
    let files =
        match writer::write_data_files(&pipeline, opened.items, key_schema, m.limits, None).await {
            Ok(files) => files,
            Err(stop) => return stopped(storage, run, &prefix, stop, notes).await,
        };
    // The count check: a DeleteBackup that landed while the stream ran
    // (SQLite, MongoDB, Cassandra) leaves it short; on PostgreSQL, which
    // reads one snapshot, only a recorded count that is wrong does.
    if i64::try_from(files.item_count).ok() != Some(recorded) {
        let n = files.item_count;
        delete_copy(storage, run, &prefix, notes).await;
        return Step::Done(Outcome::Refused(format!(
            "the in-catalog backup records {recorded} items and its read gave {n} (a \
             DeleteBackup during the read ends it short; otherwise the recorded count is \
             wrong, and the legacy restore, which does not check it, still restores the \
             backup: {CARRY_FORWARD})"
        )));
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
            delete_copy(storage, run, &prefix, notes).await;
            return Step::Done(Outcome::Failed(format!("building the manifest: {e}")));
        }
    };
    for (name, bytes) in docs.documents_before_manifest() {
        if let Err(stop) = put_document(&pipeline, name, bytes).await {
            return stopped(storage, run, &prefix, stop, notes).await;
        }
    }
    kill_point(KillPoint::BeforeManifest, &run.shutdown).await;
    if !m.claim.confirms_the_claim() {
        return Step::Stop(ClaimWatch::reason(m.claim));
    }
    let (name, bytes) = docs.manifest_document();
    if let Err(stop) = put_document(&pipeline, name, bytes).await {
        return stopped(storage, run, &prefix, stop, notes).await;
    }
    kill_point(KillPoint::AfterManifest, &run.shutdown).await;

    // The re-read: what a restore, sync and the dead-owner duty will read.
    let manifest = match verify_prefix(calls, &prefix, None).await {
        Ok(Verified::Complete(manifest)) => manifest,
        Ok(Verified::Incomplete(why)) => {
            delete_copy(storage, run, &prefix, notes).await;
            return Step::Done(Outcome::Failed(format!(
                "the copy does not read back as a complete backup: {why}"
            )));
        }
        Err(e) => {
            delete_copy(storage, run, &prefix, notes).await;
            return Step::Done(Outcome::Failed(format!(
                "the copy could not be read back: {e}"
            )));
        }
    };
    if manifest.backup_arn().ok().as_ref() != Some(&arn) {
        delete_copy(storage, run, &prefix, notes).await;
        return Step::Done(Outcome::Failed(
            "the copy's manifest names another backup".to_owned(),
        ));
    }
    let total = manifest_item_count(&manifest);
    if total != recorded {
        delete_copy(storage, run, &prefix, notes).await;
        return Step::Done(Outcome::Refused(format!(
            "the copy's manifest counts {total} items and the in-catalog backup records \
             {recorded}"
        )));
    }
    let source_table_details_json = match serde_json::to_value(&manifest.source_table) {
        Ok(value) => value,
        Err(e) => {
            delete_copy(storage, run, &prefix, notes).await;
            return Step::Done(Outcome::Failed(format!(
                "the source table does not serialize: {e}"
            )));
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
    // The claim before the replacement: a run that lost it leaves the copy,
    // which the next run deletes (and rewrites while the backup is
    // AVAILABLE).
    if !m.claim.confirms_the_claim() {
        return Step::Stop(format!(
            "{}; the copy under {prefix} is left, and the next run deletes it",
            ClaimWatch::reason(m.claim)
        ));
    }
    match storage.replace_with_index_row(&row).await {
        Ok(Replaced::Replaced) => Step::Done(Outcome::Migrated {
            item_count,
            size_bytes: manifest.backup_size_bytes,
        }),
        Ok(Replaced::AlreadyReplaced) => Step::Done(Outcome::Finished),
        Ok(Replaced::NotAvailable(what)) => {
            delete_copy(storage, run, &prefix, notes).await;
            Step::Done(Outcome::Skipped(format!(
                "it was no longer AVAILABLE when its row was to be written ({what})"
            )))
        }
        Ok(Replaced::InUse(table)) => {
            delete_copy(storage, run, &prefix, notes).await;
            Step::Done(Outcome::Refused(in_use(&table)))
        }
        // Another row holds the id: the bytes under the prefix may be that
        // row's, so they stay.
        Err(StorageError::AlreadyExists(what)) => {
            notes.push(format!(
                "the objects under {prefix} are left: they may be the backup of the row that \
                 holds its id"
            ));
            Step::Done(Outcome::Refused(what))
        }
        // The outcome is unknown: the row may be in. The copy stays, and the
        // next run finishes (a row there) or deletes it (none).
        Err(e) => Step::Stop(format!(
            "the replacement failed: {e}; the copy under {prefix} is left, and the next run \
             finishes the replacement or deletes the copy"
        )),
    }
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
/// deleted; a signal stops the run with the copy deleted; a lost claim and
/// a store found unhealthy or fenced stop the run with no further store
/// call.
async fn stopped<S: BackupCatalog + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    prefix: &str,
    stop: Stop,
    notes: &mut Vec<String>,
) -> Step {
    match stop {
        Stop::Fail(reason) => {
            delete_copy(storage, run, prefix, notes).await;
            if writer::is_item_refusal(&reason) {
                Step::Done(Outcome::Refused(format!(
                    "{reason}; restore it (RestoreTableFromBackup), correct the items the log \
                     names, and take a new backup of the restored table, which has a new ARN"
                )))
            } else {
                Step::Done(Outcome::Failed(reason))
            }
        }
        Stop::Cancelled => {
            delete_copy(storage, run, prefix, notes).await;
            Step::Stop(match run.m.interrupt.signalled() {
                Some(signal) => format!("{signal} received during the copy"),
                None => "the copy was stopped".to_owned(),
            })
        }
        Stop::ClaimLost(why) => Step::Stop(format!(
            "{why}; the copy under {prefix} is left, and the next run deletes it"
        )),
        Stop::StoreUnhealthy(why) => Step::Stop(why),
    }
}

/// Delete the backup's copy through the deletion fence, noting what was
/// left when it could not be deleted.
async fn delete_copy<S: BackupCatalog + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    prefix: &str,
    notes: &mut Vec<String>,
) {
    let configured = run.m.configured;
    if let Err(problem) = configured.fence().check_catalog(storage).await {
        notes.push(format!(
            "its copy under {prefix} was not deleted ({problem}); backup sync does not adopt \
             it, and the next run of the command deletes it"
        ));
        return;
    }
    let calls = StoreCalls {
        store: &*configured.store,
        metrics: run.m.metrics,
    };
    if let Err(e) = calls.store.delete_prefix(prefix).await {
        notes.push(format!(
            "its copy under {prefix} could not be deleted ({}); backup sync does not adopt it, \
             and the next run of the command deletes it",
            calls.failed("delete_prefix", e)
        ));
    }
}

/// A DELETED in-catalog backup (module doc): the copy a stopped run left
/// under its prefix, which no catalog row holds under either spelling,
/// deleted through the fence. "deleted" either way; a note says what was
/// deleted. A backup whose id carries no instant, or whose table name
/// forms no prefix, never had a copy.
async fn clean_deleted<S: BackupCatalog + ?Sized>(
    storage: &S,
    run: &Run<'_, '_>,
    key: &LegacyBackupKey,
    notes: &mut Vec<String>,
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
    match calls.store.list(&prefix).try_next().await {
        Ok(None) => return Step::Done(Outcome::Deleted),
        Ok(Some(_)) => {}
        Err(e) => {
            return Step::Done(Outcome::Failed(format!(
                "the store could not list {prefix}: {}",
                calls.failed("list", e)
            )));
        }
    }
    let spellings = BackupArnSpellings::new(&parts, m.region);
    match super::read_catalog_row(storage, &spellings).await {
        Ok(None) => {}
        Ok(Some(_)) => return Step::Done(Outcome::Deleted),
        Err(e) => return Step::Stop(format!("the catalog could not be read: {e}")),
    }
    if let Err(problem) = m.configured.fence().check_catalog(storage).await {
        return Step::Stop(format!(
            "the deletion fence refused the deletion of the copy a stopped run left under \
             {prefix}: {problem}"
        ));
    }
    if let Err(e) = calls.store.delete_prefix(&prefix).await {
        return Step::Done(Outcome::Failed(format!(
            "the copy a stopped run left under {prefix} could not be deleted: {}; backup sync \
             does not adopt it, and the next run of the command deletes it",
            calls.failed("delete_prefix", e)
        )));
    }
    notes.push(format!(
        "deleted the copy a stopped run left under {prefix}: its in-catalog backup was deleted"
    ));
    Step::Done(Outcome::Deleted)
}
