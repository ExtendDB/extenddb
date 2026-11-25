// SPDX-License-Identifier: Apache-2.0

//! The options of `backup sync` that act on the rows of a root other than
//! the configured one: `--move-store <old root>` ([`move_store`]),
//! `--forget-store <root>` ([`forget_store`]) and `--forget-backup <arn>`
//! ([`forget_backups`]), and plain sync's report of the rows of the other
//! stores ([`other_stores`]). Each is written against the catalog's trait
//! and the configured store, so the engine's tests run them against a
//! double; the command takes the maintenance claim, waits for running jobs
//! and prints what they return.
//!
//! A root is given as its label, as the catalog stores it and as
//! `backup sync` prints it (the canonical directory of a filesystem store,
//! `s3://<bucket>/<prefix>` for S3 with its `?endpoint=` suffix when the
//! store has one), and matches byte for byte. [`locate_root`] finds the
//! kind of the rows under it (an S3 label starts with `s3://` and a
//! filesystem label is an absolute path, so the two never meet) and the
//! store ids the catalog records for it, which the command's wait reads
//! rows of besides the configured store's; a root with no rows is refused
//! with the stores the catalog knows ([`RootRefusal::NoRows`]), the help an
//! operator needs after a label that differs by a trailing `/` or by its
//! `?endpoint=` suffix. Both options refuse the configured root itself.
//!
//! [`move_store`] is the way a tree copied to a new root keeps its rows:
//! the operator copies the old root's tree under the configured root
//! first. It reads the old root's rows (each row's
//! `source_table_details_json` is dropped as it is read) and the configured
//! root's (account, backup id) pairs, so a failed read stops the move
//! before anything changed. The rows are grouped by (account, backup id),
//! and every old-root row of a group of more than one (with a row of the
//! configured root, or with another spelling of its ARN among the moved
//! rows, which only Cassandra holds) stops the move and is set aside before
//! any row is checked, so none of them is checked further: on Cassandra two
//! spellings of an ARN share one prefix, which a DELETING row's finished
//! deletion would otherwise empty. Every other row is checked against the
//! configured store, the store the copy is in, and every row that stops
//! the move is named in one pass:
//! - A CREATING row whose owner is dead is resolved AVAILABLE when the copy
//!   holds its complete backup (`reconciler::resolve_dead_creating_row`
//!   with `OnIncomplete::Leave`: the takeover on the catalog's clock, the
//!   manifest probe of the configured store, the AVAILABLE write). Any
//!   other probe result stops the move and writes no status: a copy without
//!   the complete backup proves nothing about the old root's prefix (a
//!   copy's order is arbitrary, so a copy without the manifest may come
//!   from a complete prefix), and a FAILED row would have the reconciler
//!   delete the copy, and any server still on the old root delete the
//!   original. The row is left CREATING for a reconciler on the old root
//!   to resolve there, or for `--forget-backup`. A row the backend does not
//!   hand over has a live owner, or another taker, and stops the move.
//! - A DELETING row whose copied prefix holds a verified manifest
//!   (`reconciler::verify_prefix`) stops the move. Otherwise its deletion
//!   is finished in the configured store behind the deletion fence
//!   (`finish_backup_delete`), which deletes what the copy brought of it
//!   and sets the row DELETED: that is the deletion the row's DeleteBackup
//!   asked for.
//! - A DELETED or FAILED row whose copied prefix holds a verified manifest
//!   stops the move, naming `--forget-backup`: moving the tombstone would
//!   have the reconciler delete a complete backup. Otherwise it moves, and
//!   the reconciler of the configured store cleans its leftovers there.
//! - An AVAILABLE row is probed in the configured store
//!   (`reconciler::probe_manifest`: the manifest and its sidecar, the
//!   digest, the reader's acceptance, the row's full ARN, every data file
//!   at its recorded size), and the manifest's size and item total must be
//!   the row's; anything else stops the move with the reason.
//!
//! The AVAILABLE resolutions and the finished deletions are kept when the
//! move stops; each is idempotent, and the next run sees the statuses they
//! wrote. With no row stopping, the fence re-reads the configured root's
//! identity file once (no row is moved into a root another catalog adopted
//! since the check), then `rewrite_store` moves the rows in calls of
//! [`REWRITE_SLICE`] keys, or of one key on a backend that rewrites key by
//! key (Cassandra), each row under the condition that it is still of the
//! old kind and root and not CREATING. Before each call the signal and the
//! claim's local state ([`HeldClaim::lost`], no write) are read, and before
//! each [`REWRITE_SLICE`] keys the claim is confirmed with a beat; then the
//! old root is listed once more: a row still
//! there (a CreateBackup of a server still configured with the old root
//! whose row landed after the wait, a row `rewrite_store` did not count, a
//! rewrite that stopped part way, the keys after a stop or a lost claim) is
//! reported and the command exits 1; a row the old root no longer lists is
//! read by its ARN (one read per such key, [`MOVED_ROW_READS`] at a time,
//! not a listing of the configured store) and counts as moved when it
//! names the configured store. Every row moved, resolved or finished is
//! logged under the `extenddb::audit` target. No object is copied by the
//! move, and none of the old root is touched.
//!
//! [`forget_store`] and [`forget_backups`] remove rows outright, leaving no
//! DELETED tombstone; the removals delete no object and read no store, so
//! they run on a store in any state and with none configured. A deletion
//! duty starts no deletion once it reads the maintenance claim held
//! (`reconciler`'s `Duty::fenced`), each deletion it started before ends by
//! [`super::reconciler::DUTY_DELETION_BOUND`] from its read of the claim
//! (the store checks the deadline before each removal), and beside servers
//! the command waits [`FORGET_WAIT`] from the claim before its first
//! removal of a FAILED or DELETED row (on SQLite the serve lock excludes
//! every server, and nothing waits). What a duty can still do to a removed
//! row's bytes is the step in flight at its deadline (one removal, or one
//! S3 request), and only a server process paused past the wait between its
//! last check and that step runs it after the removal: the assumption every
//! lease in the design shares. The exception is a DELETING row of the
//! configured store: the command finishes its deletion behind the fence
//! (`finish_backup_delete`, the deletion its DeleteBackup asked for) before
//! it removes the row, which needs the store's check, run at the command's
//! opening and writing nothing, to have passed; with the store unhealthy or
//! its id unknown the row is refused ([`Forgotten::DeletionPending`]), and
//! when the check refused on an identity mismatch that the row's own store
//! id takes part in, the refusal says so and names the way out, the same
//! command with no store configured ([`Forgotten::DeletionBehindMismatch`]).
//! On SQLite the command's own dead-owner pass may finish it first, under the
//! claim, and the removal of its DELETED row says so. A DELETING row of
//! another store keeps its bytes unless a server configured with that store
//! finishes the deletion (its reconciler, under the claim: the deletion the
//! row's DeleteBackup asked for), and a sync of that root after its return
//! with the bytes adopts the backup again. While the configured root has no
//! `extenddb-store.json` and the catalog holds rows for it (an unmounted
//! volume), every row of that root is refused
//! ([`Forgotten::RootWithoutFile`]): a server's next check would write a
//! new identity file at the empty root once its last row is gone. Each
//! status has its removal ([`forget_row`]): AVAILABLE through
//! `forget_available_backup_row` (the row's lock, the status check and the
//! in-use rule in one step: a row a CREATING restore target reads is left);
//! DELETED and FAILED through `remove_backup_row`; DELETING through
//! `set_backup_deleted` then `remove_backup_row`, the DELETED state between
//! the two met by no duty that starts after the claim, and by none that
//! listed the row before it (the row was DELETING then); CREATING through
//! `remove_dead_creating_backup_row` on the catalog's clock, a row left
//! CREATING having a live owner. A FAILED row
//! whose heartbeat is within the owner window (a job that has not yet seen
//! its claim lost may still write under the prefix) is left as having a
//! live owner. A removal that finds another status re-reads the row once
//! and applies that status's removal. `--forget-backup` takes the ARN as
//! the row carries it: a row under another spelling of the ARN (the
//! server's region) is named and left, an earlier release's in-catalog
//! backup is reported as one, with nothing changed, and an ARN no row and
//! no in-catalog backup carries counts as done (a run given again after a
//! stop goes past the ARNs it removed). The command checks every ARN's
//! form ([`check_arns`]) before it takes the claim. After `--forget-store`'s
//! removals, the root's status entries are swept under the claim
//! (`remove_orphan_status_entries`, each verified against its row; only
//! Cassandra has them): an entry whose row is gone or names another store
//! (a deleter whose entry batch landed after a move of its row) is
//! deleted, and one whose row is of the root under another status (a
//! status change whose entry batch failed, or whose writer died between
//! its row and its batch) is repaired, each counted; the store ids the
//! catalog still records for the root are then read once more and
//! reported.
//!
//! Every option confirms the maintenance claim with a beat of its own
//! ([`HeldClaim::recheck`]) right before each step that cannot be undone,
//! as sync does before its insert: the move before a dead CREATING row's
//! resolution, before a DELETING row's finished deletion and before each
//! [`REWRITE_SLICE`] keys of the rewrite; the forget options before each row's removal,
//! before a DELETING row's DELETED write or finished deletion and again
//! before the removal of the DELETED row it leaves, and `--forget-store` before the
//! sweep of the status entries. A claim found lost there stops the run with
//! the row as the steps before it left it, and the stop names the step. On
//! MongoDB and Cassandra, which take a claim over on the heartbeat's age
//! alone, a command stopped (SIGSTOP, a paused VM) past
//! `stale_job_after_secs` can lose its claim to another command; it then
//! stops before its next such step. What remains is a stop after the
//! re-check confirmed the claim and before the step's own call reached the
//! catalog or the store, or with that call already sent: the call lands
//! when the process resumes. The move's rewrite reads the claim's local
//! state before each call as well, and a command resumed past
//! `stale_job_after_secs` reads it lost at once (no beat confirmed it
//! within two periods, and every stale window is at least three), so what
//! it can write after the loss is the one call it had sent or was about to
//! send when it stopped: one key on Cassandra (its few requests), one
//! statement of up to [`REWRITE_SLICE`] keys on MongoDB. PostgreSQL's claim
//! is the session's advisory lock and SQLite's the serve lock, which no
//! other command takes while the holder's process lives.
//!
//! [`other_stores`] is the report plain `backup sync` prints after its
//! listing: per store other than the configured one, the rows per status
//! (`summarize_store_roots`; on Cassandra a count of status entries,
//! checked against the root's listing, and what no row of its status
//! stands behind is reported as status entries), each CREATING and
//! DELETING row by ARN (the
//! rows 4(c) leaves alone), so the operator can move them with
//! `--move-store` or drop them with `--forget-store`.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use extenddb_core::backup_format::BackupArn;
use extenddb_storage::backup_catalog::{
    BackupKey, BackupRow, BackupStatus, ForgetRow, StaleClock, StatusEntrySweep, StoreIdentity,
    StoreKind, StoreRootCount, is_backup_not_found,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupCatalog, LegacyBackupReader};
use futures::{StreamExt as _, TryStreamExt};

use super::ConfiguredStore;
use super::maintenance::HeldClaim;
use super::reconciler::{
    CreatingOutcome, OnIncomplete, Probe, StoreCalls, Verified, probe_manifest,
    resolve_dead_creating_row, verify_prefix,
};
use super::sync::Stop;

/// A row named in a report: its status, ARN and account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedRow {
    pub status: BackupStatus,
    pub backup_arn: String,
    pub account_id: String,
}

impl NamedRow {
    fn of(row: &BackupRow) -> Self {
        Self {
            status: row.status,
            backup_arn: row.backup_arn.clone(),
            account_id: row.account_id.clone(),
        }
    }
}

// --- the report of other stores ------------------------------------------

/// The rows the catalog holds for one store other than the configured one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtherStore {
    /// The store: kind, root and id.
    pub store: StoreIdentity,
    /// The rows per status, each status with at least one row, in
    /// [`BackupStatus::ALL`]'s order.
    pub counts: Vec<(BackupStatus, u64)>,
    /// The store's CREATING and DELETING rows, in that order and each in
    /// listing order.
    pub unresolved: Vec<NamedRow>,
    /// Per status, the status entries no listed row of that status stands
    /// behind, each status with at least one, in [`BackupStatus::ALL`]'s
    /// order: on a backend whose summary counts status entries
    /// (Cassandra), the summary's count less the store's rows of that
    /// status in the root's listing (`list_backup_rows_by_root`, which
    /// lists a row whose entry is under an earlier status under its own).
    /// Empty elsewhere.
    pub entries: Vec<(BackupStatus, u64)>,
}

/// The counts of `summarize_store_roots` (in its order) grouped by store,
/// `except` left out; no row is named.
#[must_use]
pub fn group_counts(counts: &[StoreRootCount], except: Option<&StoreIdentity>) -> Vec<OtherStore> {
    let mut stores: Vec<OtherStore> = Vec::new();
    for count in counts {
        if except == Some(&count.store) {
            continue;
        }
        match stores.last_mut() {
            Some(last) if last.store == count.store => {
                last.counts.push((count.status, count.rows));
            }
            _ => stores.push(OtherStore {
                store: count.store.clone(),
                counts: vec![(count.status, count.rows)],
                unresolved: Vec::new(),
                entries: Vec::new(),
            }),
        }
    }
    stores
}

/// Every store other than `configured` that the catalog holds rows for
/// (module doc), in [`StoreRootCount::sort`]'s order. A store of the
/// configured root under another id is another store too. On a backend
/// whose summary counts status entries (Cassandra), the store's rows are
/// read from its root's listing (`list_backup_rows_by_root`, kept to the
/// store's id), which lists each row under its own status whatever
/// partition its entry is in: `counts` holds those rows, and
/// [`OtherStore::entries`] the entries no row of their status stands
/// behind.
///
/// # Errors
///
/// The backend's error for the summary or a listing.
pub async fn other_stores<S: BackupCatalog + ?Sized>(
    storage: &S,
    configured: &StoreIdentity,
) -> Result<Vec<OtherStore>, StorageError> {
    let counts = storage.summarize_store_roots().await?;
    let mut stores = group_counts(&counts, Some(configured));
    if storage.summary_counts_status_entries() {
        for other in &mut stores {
            // The root's listing, which reads every status partition, so a
            // row whose entry is under an earlier status counts under its
            // own status; kept to the store's id.
            let (rows, unresolved) = storage
                .list_backup_rows_by_root(other.store.store_kind, &other.store.store_root, None)
                .try_fold(
                    (Vec::<(BackupStatus, u64)>::new(), Vec::new()),
                    |(mut rows, mut unresolved), row| {
                        if row.store == other.store {
                            match rows.iter_mut().find(|(s, _)| *s == row.status) {
                                Some((_, n)) => *n += 1,
                                None => rows.push((row.status, 1)),
                            }
                            if matches!(row.status, BackupStatus::Creating | BackupStatus::Deleting)
                            {
                                unresolved.push(NamedRow::of(&row));
                            }
                        }
                        std::future::ready(Ok((rows, unresolved)))
                    },
                )
                .await?;
            for (status, entries) in &other.counts {
                let listed = rows
                    .iter()
                    .find(|(s, _)| s == status)
                    .map_or(0, |(_, n)| *n);
                if *entries > listed {
                    other.entries.push((*status, entries - listed));
                }
            }
            other.counts = BackupStatus::ALL
                .iter()
                .filter_map(|status| rows.iter().find(|(s, _)| s == status).copied())
                .collect();
            other.unresolved = [BackupStatus::Creating, BackupStatus::Deleting]
                .iter()
                .flat_map(|status| unresolved.iter().filter(|r| r.status == *status).cloned())
                .collect();
        }
        return Ok(stores);
    }
    for other in &mut stores {
        for status in [BackupStatus::Creating, BackupStatus::Deleting] {
            if !other.counts.iter().any(|(s, n)| *s == status && *n > 0) {
                continue;
            }
            let mut rows = storage.list_backup_rows(Some(status), &other.store);
            while let Some(row) = rows.try_next().await? {
                other.unresolved.push(NamedRow::of(&row));
            }
        }
    }
    Ok(stores)
}

// --- the root an option names --------------------------------------------

/// The root an option names, as the catalog records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatedRoot {
    pub kind: StoreKind,
    pub root: String,
    /// The store ids the catalog records for the root (`read_store_ids`).
    pub store_ids: Vec<String>,
}

impl LocatedRoot {
    /// The root's stores, one per id: what the command's wait reads rows
    /// of besides the configured store's.
    #[must_use]
    pub fn identities(&self) -> Vec<StoreIdentity> {
        self.store_ids
            .iter()
            .map(|id| StoreIdentity {
                store_kind: self.kind,
                store_root: self.root.clone(),
                store_id: id.clone(),
            })
            .collect()
    }
}

/// Why [`locate_root`] refused a root.
#[derive(Debug)]
pub enum RootRefusal {
    /// The root is the configured store's own.
    ConfiguredRoot,
    /// The catalog records no row under the root, of either kind; the
    /// stores it does record rows for.
    NoRows(Vec<StoreRootCount>),
    /// The catalog could not be read.
    Catalog(StorageError),
}

/// The kind and store ids of the rows the catalog records under `root`
/// (module doc). `configured_root` is the configured store's root label,
/// when a store is configured: that root is refused.
///
/// # Errors
///
/// [`RootRefusal`].
pub async fn locate_root<S: BackupCatalog + ?Sized>(
    storage: &S,
    root: &str,
    configured_root: Option<&str>,
) -> Result<LocatedRoot, RootRefusal> {
    if configured_root == Some(root) {
        return Err(RootRefusal::ConfiguredRoot);
    }
    for kind in [StoreKind::Filesystem, StoreKind::S3] {
        let store_ids = storage
            .read_store_ids(kind, root)
            .await
            .map_err(RootRefusal::Catalog)?;
        if !store_ids.is_empty() {
            return Ok(LocatedRoot {
                kind,
                root: root.to_owned(),
                store_ids,
            });
        }
    }
    let known = storage
        .summarize_store_roots()
        .await
        .map_err(RootRefusal::Catalog)?;
    Err(RootRefusal::NoRows(known))
}

// --- the removals ----------------------------------------------------------

/// What [`forget_row`] did with one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Forgotten {
    /// The row is gone; it had this status. No object was deleted.
    Removed(BackupStatus),
    /// A DELETING row of the configured store: this command finished its
    /// deletion (its objects are deleted, the deletion its DeleteBackup
    /// asked for), then removed the row. Also a DELETED row whose deletion
    /// the command's own dead-owner pass finished (SQLite).
    RemovedAfterDeletion,
    /// Left: a DELETING row of the configured store whose deletion this
    /// command cannot finish, since the store's check did not pass (or the
    /// store's id is unknown).
    DeletionPending,
    /// Left: a DELETING row of the configured store that carries this
    /// store id, one of the row ids the store's check refused on as an
    /// identity mismatch (`IdMismatch`, `MultipleRowIds`), so the deletion
    /// waits on a check the row itself takes part in.
    DeletionBehindMismatch(String),
    /// Left: a row of the configured root, whose `extenddb-store.json` the
    /// command found missing while the catalog holds rows for the root (a
    /// volume that is not mounted, say), or of the defaulted root found
    /// missing with rows for it ([`ForgetRun::unmounted_root`]).
    RootWithoutFile,
    /// The row was gone before the removal.
    Gone,
    /// Left: a restore target registered from the backup is CREATING (its
    /// name, and the row's account).
    InUse(String, String),
    /// Left: a CREATING row whose owner is alive, or a FAILED row whose
    /// heartbeat is within the owner window (its job ended with the FAILED
    /// write, and may have been writing under the prefix until then).
    LiveOwner(BackupStatus),
    /// Left: the row changed under the removal again after the re-read (its
    /// status and store now).
    Changed(BackupStatus, StoreIdentity),
    /// Left: a catalog call failed (why).
    Failed(String),
    /// Not reached: the run stopped (why) during the wait before the
    /// first FAILED or DELETED removal, or before a DELETING row's
    /// finished deletion ([`FORGET_WAIT`]), the row as it was; or at the
    /// claim's re-check before a step ([`ForgetRun::claim`]), the row as
    /// the steps before it left it (the reason says which).
    Stopped(String),
}

impl Forgotten {
    /// Whether the row is still in the catalog: the command exits 1.
    #[must_use]
    pub fn is_left(&self) -> bool {
        !matches!(
            self,
            Self::Removed(_) | Self::RemovedAfterDeletion | Self::Gone
        )
    }

    /// Whether this run removed the row.
    #[must_use]
    pub fn is_removed(&self) -> bool {
        matches!(self, Self::Removed(_) | Self::RemovedAfterDeletion)
    }
}

impl std::fmt::Display for Forgotten {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Removed(status) => write!(
                f,
                "removed: the row ({status}) is gone, and no object was deleted"
            ),
            Self::RemovedAfterDeletion => f.write_str(
                "removed: the row is gone, and this command deleted its objects first (the row \
                 was DELETING in the configured store: the deletion its DeleteBackup asked for)",
            ),
            Self::DeletionPending => f.write_str(
                "left: a DELETING row of the configured store, whose deletion this command \
                 finishes before it removes the row, and the store's check did not pass; let a \
                 server finish the DeleteBackup, or run again once the store's check passes",
            ),
            Self::DeletionBehindMismatch(id) => write!(
                f,
                "left: a DELETING row of the configured store carrying store id {id}, one of the \
                 ids the store's identity check refuses, so neither this command nor a server \
                 finishes its deletion while that check fails; remove the row by running this \
                 command with [backup.store] kind = \"disabled\" (its bytes stay), then, with the \
                 store configured again, let backup sync adopt the backup once the check passes \
                 and DeleteBackup it again"
            ),
            Self::RootWithoutFile => write!(
                f,
                "left: the configured root has no {} while the catalog holds rows for it (a \
                 volume that is not mounted?); mount it, or point [backup.store] at the root \
                 that holds these backups, then run again",
                super::STORE_IDENTITY_KEY
            ),
            Self::Gone => f.write_str("the row was already gone"),
            Self::InUse(table, account) => write!(
                f,
                "left: the restore target {table} is CREATING from this backup: wait for its \
                 restore to end, or, when that restore failed or its owner is dead, remove the \
                 target with extenddb backup remove-target --account {account} {table}; then run \
                 again"
            ),
            Self::LiveOwner(BackupStatus::Failed) => f.write_str(
                "left: the FAILED row was written less than stale_job_after_secs ago (its \
                 heartbeat is within the window); run the command again once its heartbeat is \
                 older than stale_job_after_secs",
            ),
            Self::LiveOwner(status) => write!(
                f,
                "left: the {status} row has a live owner (its heartbeat is within \
                 stale_job_after_secs); run the command again once its job has ended"
            ),
            Self::Changed(status, store) => write!(
                f,
                "left: the row changed under the removal; it is now {status} in the {} store {} \
                 (store id {})",
                store.store_kind, store.store_root, store.store_id
            ),
            Self::Failed(why) => write!(f, "left: {why}"),
            Self::Stopped(why) => write!(f, "not reached: {why}"),
        }
    }
}

/// How long `--forget-store` and `--forget-backup` beside servers
/// (PostgreSQL, MongoDB, Cassandra) wait after taking the maintenance claim
/// before their first removal of a FAILED or DELETED row (and before
/// finishing the deletion of a DELETING row of the configured store, whose
/// DELETED row is removed next): a deletion duty
/// that read the claim free just before the command took it starts no
/// removal after [`super::reconciler::DUTY_DELETION_BOUND`] from that read
/// (the store checks the deadline before each one), and its step in flight
/// at the deadline, one removal or one S3 request, is answered or abandoned
/// within one call's bound ([`super::S3_CHECK_TIMEOUT`], the larger of the
/// two kinds', since the servers' stores need not be the command's). The
/// wait assumes the servers' processes keep running: one paused past it
/// between its last check of the deadline and a step it has already sent
/// runs that step when it resumes, the assumption every lease in the design
/// shares. On SQLite the serve lock excludes every server, and the command
/// does not wait.
pub const FORGET_WAIT: Duration =
    super::reconciler::DUTY_DELETION_BOUND.saturating_add(super::S3_CHECK_TIMEOUT);

/// How often the wait before the first FAILED or DELETED removal asks the
/// run's stop.
const FORGET_WAIT_STEP: Duration = Duration::from_secs(1);

/// What a run of `--forget-store` or `--forget-backup` removes rows under.
#[derive(Clone, Copy)]
pub struct ForgetRun<'a> {
    /// The window a live owner is told apart by: `stale_job_after` beside
    /// servers, zero on SQLite under the serve lock.
    pub owner_window: Duration,
    /// The instant before which no FAILED or DELETED row is removed: the
    /// claim's taking plus [`FORGET_WAIT`] beside servers, `None` on
    /// SQLite.
    pub removals_from: Option<tokio::time::Instant>,
    /// Told how long the run waits, once, when it does.
    pub on_wait: &'a (dyn Fn(Duration) + Sync),
    /// Asked before each row or ARN, and during the wait.
    pub stop: Stop<'a>,
    /// The maintenance claim the command holds, re-checked with a beat
    /// ([`HeldClaim::recheck`]) right before each removal, each
    /// `set_backup_deleted`, each finished deletion and the sweep of the
    /// root's status entries; a claim found lost there stops the run as
    /// `stop` does ([`Forgotten::Stopped`]). `None` re-checks nothing.
    pub claim: Option<&'a HeldClaim>,
    /// The configured store as the command opened it, when one is
    /// configured.
    pub configured: Option<ForgetConfigured<'a>>,
    /// A filesystem root the command found missing as it opened the
    /// store, with rows for it in the catalog: the defaulted root moved
    /// away, or a symbolic link to a volume that is gone. Every row of it
    /// is refused ([`Forgotten::RootWithoutFile`]), as the rows of a
    /// configured root without its identity file are.
    pub unmounted_root: Option<&'a str>,
    /// The DELETING rows whose deletion the command's own dead-owner pass
    /// finished before the removals (SQLite), by ARN: their DELETED rows'
    /// removal says the command deleted the objects.
    pub deletions_finished: &'a [String],
}

/// The configured store as a forget option meets its rows.
#[derive(Clone, Copy)]
pub struct ForgetConfigured<'a> {
    pub store: &'a ConfiguredStore,
    /// Its identity when the command's check of the store, which writes
    /// nothing, passed: a DELETING row of this store has its deletion
    /// finished behind the fence before its removal. `None` otherwise, and
    /// such a row is refused ([`Forgotten::DeletionPending`]).
    pub healthy: Option<&'a StoreIdentity>,
    /// The root's row ids when the opening's check failed on an identity
    /// mismatch (`IdMismatch`, `MultipleRowIds`), empty otherwise: a
    /// DELETING row carrying one of them is refused as
    /// [`Forgotten::DeletionBehindMismatch`], which names the way out.
    pub mismatch_row_ids: &'a [String],
    /// Whether the opening found no identity file under the root while the
    /// catalog holds rows for it: every row of the root is refused
    /// ([`Forgotten::RootWithoutFile`]), since a server's next check
    /// would write a new identity file at the root once its last row is
    /// gone.
    pub file_missing_with_rows: bool,
}

impl ForgetConfigured<'_> {
    /// Whether `row` is of the configured root (any id).
    fn holds(&self, row: &BackupRow) -> bool {
        row.store.store_kind == self.store.kind && row.store.store_root == self.store.root
    }
}

/// One run's removals: its [`ForgetRun`], the FAILED rows whose owner is
/// dead (read once per store), and whether the wait is over.
struct Removals<'a> {
    run: ForgetRun<'a>,
    failed_owners: FailedOwners,
    waited: bool,
}

impl<'a> Removals<'a> {
    fn new(run: ForgetRun<'a>) -> Self {
        Self {
            run,
            failed_owners: FailedOwners::new(run.owner_window),
            waited: run.removals_from.is_none(),
        }
    }

    /// The wait before the first FAILED or DELETED removal ([`FORGET_WAIT`]);
    /// `Err` is the stop that ended it.
    async fn wait_for_duties(&mut self) -> Result<(), String> {
        if self.waited {
            return Ok(());
        }
        let until = self
            .run
            .removals_from
            .expect("an unfinished wait has an end");
        let now = tokio::time::Instant::now();
        if until > now {
            (self.run.on_wait)(until - now);
        }
        loop {
            if let Some(reason) = (self.run.stop)() {
                return Err(reason);
            }
            let now = tokio::time::Instant::now();
            if now >= until {
                break;
            }
            tokio::time::sleep((until - now).min(FORGET_WAIT_STEP)).await;
        }
        self.waited = true;
        Ok(())
    }

    /// One beat of the run's claim right before `step`, a step that cannot
    /// be undone; `Err` is the loss as the outcome that stops the run, the
    /// row as the steps before it left it (`left`).
    async fn recheck(&self, step: &str, left: &str) -> Result<(), Forgotten> {
        match self.run.claim {
            Some(claim) => claim
                .recheck()
                .await
                .map_err(|reason| Forgotten::Stopped(format!("{reason} (before {step}; {left})"))),
            None => Ok(()),
        }
    }
}

/// The form of each ARN `--forget-backup` is given, before the command
/// takes the claim: the ARNs refused, each with why.
#[must_use]
pub fn check_arns(arns: &[String]) -> Vec<(String, String)> {
    arns.iter()
        .filter_map(|given| {
            let why = match extenddb_storage::util::check_backup_arn(given) {
                Err(e) => e.to_string(),
                Ok(_) => match BackupArn::parse(given) {
                    Err(e) => e.to_string(),
                    Ok(_) => return None,
                },
            };
            Some((given.clone(), why))
        })
        .collect()
}

/// The FAILED rows whose owner is dead, per store, read once per store:
/// a FAILED row the backend lists as older than the owner window on the
/// heartbeat clock.
struct FailedOwners {
    window: Duration,
    dead: HashMap<StoreIdentity, HashSet<String>>,
}

impl FailedOwners {
    fn new(window: Duration) -> Self {
        Self {
            window,
            dead: HashMap::new(),
        }
    }

    async fn is_dead<S: BackupCatalog + ?Sized>(
        &mut self,
        storage: &S,
        row: &BackupRow,
    ) -> Result<bool, StorageError> {
        if !self.dead.contains_key(&row.store) {
            let dead: HashSet<String> = storage
                .list_backup_rows_older_than(
                    BackupStatus::Failed,
                    &row.store,
                    StaleClock::Heartbeat,
                    self.window,
                )
                .map_ok(|r| r.backup_arn)
                .try_collect()
                .await?;
            self.dead.insert(row.store.clone(), dead);
        }
        Ok(self.dead[&row.store].contains(&row.backup_arn))
    }
}

/// Remove `row` outright by its status's removal (module doc); a removal
/// that finds another status re-reads the row once and applies that
/// status's.
async fn forget_row<S: BackupCatalog + ?Sized>(
    storage: &S,
    row: &BackupRow,
    removals: &mut Removals<'_>,
) -> Forgotten {
    let mut current = row.clone();
    for attempt in 0..2 {
        if let Some(done) = remove_by_status(storage, &current, removals).await {
            return done;
        }
        // The removal matched no row of that status: another writer moved
        // it, or (CREATING) its owner is alive. Read it once more.
        let arn = match BackupArn::parse(&row.backup_arn) {
            Ok(arn) => arn,
            Err(e) => return Forgotten::Failed(format!("the row's ARN does not parse: {e}")),
        };
        let now = match storage.read_backup_row(&arn).await {
            Ok(Some(now)) => lean(now),
            Ok(None) => return Forgotten::Gone,
            Err(e) => return Forgotten::Failed(format!("the row could not be read again: {e}")),
        };
        if now.store != row.store {
            return Forgotten::Changed(now.status, now.store);
        }
        if now.status == BackupStatus::Creating && current.status == BackupStatus::Creating {
            return Forgotten::LiveOwner(BackupStatus::Creating);
        }
        current = now;
        if attempt == 1 {
            break;
        }
    }
    Forgotten::Changed(current.status, current.store)
}

/// The removal of `row`'s status (module doc): `None` when it matched no
/// row of that status.
async fn remove_by_status<S: BackupCatalog + ?Sized>(
    storage: &S,
    row: &BackupRow,
    removals: &mut Removals<'_>,
) -> Option<Forgotten> {
    let owner_window = removals.run.owner_window;
    let key = row.key();
    let failed = |what: &str, e: StorageError| Some(Forgotten::Failed(format!("{what}: {e}")));
    let status = row.status;
    let configured = removals
        .run
        .configured
        .filter(|configured| configured.holds(row));
    let unmounted = removals.run.unmounted_root.is_some_and(|root| {
        row.store.store_kind == StoreKind::Filesystem && row.store.store_root == root
    });
    if unmounted || configured.is_some_and(|configured| configured.file_missing_with_rows) {
        return Some(Forgotten::RootWithoutFile);
    }
    match status {
        BackupStatus::Available => {
            if let Err(stopped) = removals
                .recheck("the AVAILABLE row's removal", "the row is unchanged")
                .await
            {
                return Some(stopped);
            }
            match storage.forget_available_backup_row(&key).await {
                Ok(ForgetRow::Removed) => Some(Forgotten::Removed(status)),
                Ok(ForgetRow::InUse(table)) => {
                    Some(Forgotten::InUse(table, row.account_id.clone()))
                }
                Ok(ForgetRow::Status(_)) => None,
                Err(e) if is_backup_not_found(&e) => Some(Forgotten::Gone),
                Err(e) => failed("the row could not be removed", e),
            }
        }
        BackupStatus::Creating => {
            if let Err(stopped) = removals
                .recheck("the CREATING row's removal", "the row is unchanged")
                .await
            {
                return Some(stopped);
            }
            match storage
                .remove_dead_creating_backup_row(&key, owner_window)
                .await
            {
                Ok(true) => Some(Forgotten::Removed(status)),
                Ok(false) => None,
                Err(e) => failed("the row could not be removed", e),
            }
        }
        BackupStatus::Failed | BackupStatus::Deleted => {
            if status == BackupStatus::Failed {
                match removals.failed_owners.is_dead(storage, row).await {
                    Ok(true) => {}
                    Ok(false) => return Some(Forgotten::LiveOwner(status)),
                    Err(e) => return failed("the row's owner could not be tested", e),
                }
            }
            if let Err(reason) = removals.wait_for_duties().await {
                return Some(Forgotten::Stopped(reason));
            }
            if let Err(stopped) = removals
                .recheck(
                    &format!("the {status} row's removal"),
                    "the row is unchanged",
                )
                .await
            {
                return Some(stopped);
            }
            match storage
                .remove_backup_row(&key, row.store.store_kind, &row.store.store_root)
                .await
            {
                Ok(true)
                    if status == BackupStatus::Deleted
                        && removals.run.deletions_finished.contains(&row.backup_arn) =>
                {
                    Some(Forgotten::RemovedAfterDeletion)
                }
                Ok(true) => Some(Forgotten::Removed(status)),
                Ok(false) => None,
                Err(e) => failed("the row could not be removed", e),
            }
        }
        BackupStatus::Deleting if configured.is_some() => {
            let configured = configured.expect("tested above");
            // A passed check means every row of the configured root carries
            // the file's id (`compare` refuses several ids and a mismatch),
            // so with the store healthy this row's store is the healthy one;
            // the comparison is kept so a row of another id is never
            // finished in this store whatever the check let through.
            if configured.healthy != Some(&row.store) {
                if configured.mismatch_row_ids.contains(&row.store.store_id) {
                    return Some(Forgotten::DeletionBehindMismatch(
                        row.store.store_id.clone(),
                    ));
                }
                return Some(Forgotten::DeletionPending);
            }
            finish_then_remove(storage, configured.store, row, removals).await
        }
        BackupStatus::Deleting => {
            if let Err(stopped) = removals
                .recheck("the DELETING row's DELETED write", "the row is unchanged")
                .await
            {
                return Some(stopped);
            }
            match storage.set_backup_deleted(&key).await {
                Ok(true) => {
                    if let Err(stopped) = removals
                        .recheck(
                            "the DELETED row's removal",
                            "the row is DELETED, and a second run removes it",
                        )
                        .await
                    {
                        return Some(stopped);
                    }
                    match storage
                        .remove_backup_row(&key, row.store.store_kind, &row.store.store_root)
                        .await
                    {
                        Ok(true) => Some(Forgotten::Removed(status)),
                        Ok(false) => None,
                        Err(e) => failed(
                            "the row was set DELETED and could not be removed (a second run \
                             removes it)",
                            e,
                        ),
                    }
                }
                Ok(false) => None,
                Err(e) => failed("the row could not be set DELETED", e),
            }
        }
    }
}

/// A DELETING row of the configured store (healthy, its id the row's):
/// after the wait for the duties, its deletion finished behind the fence
/// (`finish_backup_delete`, which deletes the prefix and sets the row
/// DELETED), then the row removed. The wait comes first, so a stop during
/// it leaves the row DELETING and its objects untouched. `None` when the
/// row was no longer DELETING.
async fn finish_then_remove<S: BackupCatalog + ?Sized>(
    storage: &S,
    configured: &ConfiguredStore,
    row: &BackupRow,
    removals: &mut Removals<'_>,
) -> Option<Forgotten> {
    let arn = match BackupArn::parse(&row.backup_arn) {
        Ok(arn) => arn,
        Err(e) => {
            return Some(Forgotten::Failed(format!(
                "the row's ARN does not parse: {e}"
            )));
        }
    };
    if let Err(reason) = removals.wait_for_duties().await {
        return Some(Forgotten::Stopped(reason));
    }
    if let Err(stopped) = removals
        .recheck(
            "the DELETING row's deletion in the configured store",
            "the row and its objects are unchanged",
        )
        .await
    {
        return Some(stopped);
    }
    let fence = configured.fence();
    match super::finish_backup_delete(storage, &configured.calls("deletion"), &fence, &arn).await {
        Ok(true) => {
            tracing::info!(
                target: "extenddb::audit",
                action = "backup sync --forget-backup",
                account_id = %row.account_id,
                backup_arn = %row.backup_arn,
                status = "DELETED",
                store_root = %row.store.store_root,
                "finished a DELETING row's deletion in the configured store before its removal"
            );
        }
        Ok(false) => return None,
        Err(e) => {
            return Some(Forgotten::Failed(format!(
                "its deletion in the configured store did not finish ({e}); the row is left \
                 DELETING"
            )));
        }
    }
    if let Err(stopped) = removals
        .recheck(
            "the DELETED row's removal",
            "its deletion is finished, the row is DELETED, and a second run removes it",
        )
        .await
    {
        return Some(stopped);
    }
    match storage
        .remove_backup_row(&row.key(), row.store.store_kind, &row.store.store_root)
        .await
    {
        Ok(true) => Some(Forgotten::RemovedAfterDeletion),
        Ok(false) => None,
        Err(e) => Some(Forgotten::Failed(format!(
            "the row's deletion is finished and the DELETED row could not be removed (a second \
             run removes it): {e}"
        ))),
    }
}

/// The audit line of a removal.
fn audit_removal(action: &str, row: &BackupRow, outcome: &Forgotten) {
    let (status, message) = match outcome {
        Forgotten::Removed(status) => (
            *status,
            "removed a backup row outright; no object was deleted",
        ),
        Forgotten::RemovedAfterDeletion => (
            row.status,
            "removed a backup row after this command finished its deletion",
        ),
        _ => return,
    };
    tracing::info!(
        target: "extenddb::audit",
        action = action,
        account_id = %row.account_id,
        backup_arn = %row.backup_arn,
        status = %status,
        store_kind = %row.store.store_kind,
        store_root = %row.store.store_root,
        store_id = %row.store.store_id,
        "{message}"
    );
}

/// What [`forget_store`] did.
#[derive(Debug, Default)]
pub struct ForgetStoreReport {
    /// Each row of the root and what was done with it, in listing order.
    pub rows: Vec<(NamedRow, Forgotten)>,
    /// Why the run stopped before the remaining rows, when it did.
    pub stopped: Option<String>,
    /// The rows the root still lists after the removals, named (each is
    /// either a row above that was left, or one that landed after the first
    /// listing, from a writer still configured with the root); `None` when
    /// the listing failed (its error in `recheck_error`).
    pub rows_left: Option<Vec<NamedRow>>,
    /// What the sweep of the root's status entries after the removals did
    /// (Cassandra; zero elsewhere): the entries with no row of the root
    /// behind them deleted, and those whose row is of the root under
    /// another status repaired. `None` when it did not run (a stop, or a
    /// listing that failed).
    pub entries_swept: Option<StatusEntrySweep>,
    /// The store ids the catalog still records for the root after the
    /// sweep, read when the root's listing after the removals returned no
    /// row: status entries written after that listing (a writer still
    /// configured with the root).
    pub entries_left: Vec<String>,
    /// Why the read after the removals failed, when it did.
    pub recheck_error: Option<String>,
}

impl ForgetStoreReport {
    /// The rows removed.
    #[must_use]
    pub fn removed(&self) -> usize {
        self.rows.iter().filter(|(_, f)| f.is_removed()).count()
    }

    /// Whether every row is gone: no row left, no stop, the read after the
    /// removals answered and listed nothing. Entries left behind do not
    /// count: no row stands behind them.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.stopped.is_none()
            && self.recheck_error.is_none()
            && self.rows_left.as_ref().is_some_and(Vec::is_empty)
            && !self.rows.iter().any(|(_, f)| f.is_left())
    }
}

/// `--forget-store <root>` (module doc): every row of `root` (of kind
/// `kind`, from [`locate_root`]) removed outright under `run`. Its stop is
/// asked before each row (the claim lost, a signal). Each removal is logged
/// under the `extenddb::audit` target.
///
/// # Errors
///
/// The backend's error for the root's listing; nothing was removed.
pub async fn forget_store<S: BackupCatalog + ?Sized>(
    storage: &S,
    kind: StoreKind,
    root: &str,
    run: ForgetRun<'_>,
) -> Result<ForgetStoreReport, StorageError> {
    let rows: Vec<BackupRow> = storage
        .list_backup_rows_by_root(kind, root, None)
        .map_ok(lean)
        .try_collect()
        .await?;
    let mut report = ForgetStoreReport::default();
    let mut removals = Removals::new(run);
    for row in &rows {
        if let Some(reason) = (run.stop)() {
            report.stopped = Some(reason);
            return Ok(report);
        }
        let outcome = forget_row(storage, row, &mut removals).await;
        if let Forgotten::Stopped(reason) = outcome {
            report.stopped = Some(reason);
            return Ok(report);
        }
        audit_removal("backup sync --forget-store", row, &outcome);
        report.rows.push((NamedRow::of(row), outcome));
    }
    match storage
        .list_backup_rows_by_root(kind, root, None)
        .map_ok(|row| NamedRow::of(&row))
        .try_collect::<Vec<NamedRow>>()
        .await
    {
        Ok(left) => report.rows_left = Some(left),
        Err(e) => report.recheck_error = Some(format!("the root's rows could not be read: {e}")),
    }
    // The root's status entries (Cassandra), under the claim: each verified
    // against its row as it is read, deleted or repaired. The claim is
    // confirmed by a beat first, as before each removal.
    if report.recheck_error.is_none()
        && let Some(claim) = run.claim
        && let Err(reason) = claim.recheck().await
    {
        report.stopped = Some(format!(
            "{reason} (before the sweep of the root's status entries, which did not run)"
        ));
        return Ok(report);
    }
    if report.recheck_error.is_none() {
        match storage.remove_orphan_status_entries(kind, root).await {
            Ok(sweep) => report.entries_swept = Some(sweep),
            Err(e) => {
                report.recheck_error =
                    Some(format!("the root's status entries could not be swept: {e}"));
            }
        }
    }
    if report.rows_left.as_ref().is_some_and(Vec::is_empty) && report.recheck_error.is_none() {
        match storage.read_store_ids(kind, root).await {
            Ok(ids) => report.entries_left = ids,
            Err(e) => {
                report.recheck_error = Some(format!("the root's store ids could not be read: {e}"));
            }
        }
    }
    Ok(report)
}

/// What `--forget-backup` did with one ARN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForgetBackup {
    /// The ARN's row, and what [`forget_row`] did with it.
    Row(BackupStatus, Forgotten),
    /// No row carries the ARN as given; a row under this other spelling of
    /// it does (its status), and was left.
    OtherSpelling(String, BackupStatus),
    /// The ARN is an earlier release's in-catalog backup: nothing changed.
    Legacy,
    /// No row and no in-catalog backup carries the ARN, under either
    /// spelling: there is nothing to remove, and the ARN counts as done.
    NoRow,
    /// The ARN is not one (why).
    Invalid(String),
    /// A catalog call failed (why); nothing changed.
    Failed(String),
}

impl ForgetBackup {
    /// Whether the ARN's row is gone (removed now, or already, or no row
    /// carries it): the run goes on to the next ARN; anything else stops it
    /// and exits 1.
    #[must_use]
    pub fn is_done(&self) -> bool {
        matches!(
            self,
            Self::Row(
                _,
                Forgotten::Removed(_) | Forgotten::RemovedAfterDeletion | Forgotten::Gone
            ) | Self::NoRow
        )
    }
}

impl std::fmt::Display for ForgetBackup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Row(_, outcome) => write!(f, "{outcome}"),
            Self::OtherSpelling(arn, status) => write!(
                f,
                "left: no row carries this ARN as written; the row of this backup is {arn} \
                 ({status}): run --forget-backup with that ARN"
            ),
            Self::Legacy => f.write_str(
                "an in-catalog backup; --forget-backup changes nothing; DeleteBackup removes it",
            ),
            Self::NoRow => f.write_str("no row carries this ARN; nothing to remove"),
            Self::Invalid(why) => write!(f, "refused: {why}"),
            Self::Failed(why) => write!(f, "left: {why}"),
        }
    }
}

/// What [`forget_backups`] did.
#[derive(Debug, Default)]
pub struct ForgetBackupsReport {
    /// Each ARN reached, as given, and its outcome, in the order given.
    pub done: Vec<(String, ForgetBackup)>,
    /// Why the run stopped before the remaining ARNs: a signal, the claim
    /// lost. A refusal stops the run too and is the last of `done`.
    pub stopped: Option<String>,
}

impl ForgetBackupsReport {
    /// Whether every ARN's row is gone.
    #[must_use]
    pub fn complete(&self, asked: usize) -> bool {
        self.stopped.is_none()
            && self.done.len() == asked
            && self.done.iter().all(|(_, outcome)| outcome.is_done())
    }
}

/// `--forget-backup <arn>`, once per ARN in the order given under `run`,
/// stopping at the first ARN whose row is not removed (module doc).
/// `region` is the server's, for the other spelling of the ARN. The run's
/// stop is asked before each ARN. Each removal is logged under the
/// `extenddb::audit` target.
pub async fn forget_backups<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    arns: &[String],
    region: &str,
    run: ForgetRun<'_>,
) -> ForgetBackupsReport {
    let mut report = ForgetBackupsReport::default();
    let mut removals = Removals::new(run);
    for given in arns {
        if let Some(reason) = (run.stop)() {
            report.stopped = Some(reason);
            return report;
        }
        let outcome = forget_backup(storage, given, region, &mut removals).await;
        if let ForgetBackup::Row(_, Forgotten::Stopped(reason)) = &outcome {
            report.stopped = Some(reason.clone());
            return report;
        }
        let done = outcome.is_done();
        report.done.push((given.clone(), outcome));
        if !done {
            break;
        }
    }
    report
}

async fn forget_backup<S: BackupCatalog + LegacyBackupReader + ?Sized>(
    storage: &S,
    given: &str,
    region: &str,
    removals: &mut Removals<'_>,
) -> ForgetBackup {
    let parts = match extenddb_storage::util::check_backup_arn(given) {
        Ok(parts) => parts,
        Err(e) => return ForgetBackup::Invalid(e.to_string()),
    };
    let arn = match BackupArn::parse(given) {
        Ok(arn) => arn,
        Err(e) => return ForgetBackup::Invalid(e.to_string()),
    };
    match storage.read_backup_row(&arn).await {
        Ok(Some(row)) => {
            let row = lean(row);
            let outcome = forget_row(storage, &row, removals).await;
            audit_removal("backup sync --forget-backup", &row, &outcome);
            return ForgetBackup::Row(row.status, outcome);
        }
        Ok(None) => {}
        Err(e) => return ForgetBackup::Failed(format!("the row could not be read: {e}")),
    }
    let spellings = super::BackupArnSpellings::new(&parts, region);
    for spelling in spellings.arns() {
        if *spelling == arn {
            continue;
        }
        match storage.read_backup_row(spelling).await {
            Ok(Some(row)) => return ForgetBackup::OtherSpelling(row.backup_arn, row.status),
            Ok(None) => {}
            Err(e) => return ForgetBackup::Failed(format!("the row could not be read: {e}")),
        }
    }
    let mut legacy = vec![spellings.canonical().to_owned()];
    if let Some(as_written) = &spellings.as_written {
        legacy.push(as_written.clone());
    }
    for spelling in legacy {
        match storage.describe(parts.account_id, &spelling).await {
            Ok(_) => return ForgetBackup::Legacy,
            Err(e) if is_backup_not_found(&e) => {}
            Err(e) => {
                return ForgetBackup::Failed(format!(
                    "the in-catalog backups could not be read: {e}"
                ));
            }
        }
    }
    ForgetBackup::NoRow
}

/// A row with its parsed `source_table_details_json` dropped: the options
/// keep the rows of a whole root, and read no column of that one.
fn lean(mut row: BackupRow) -> BackupRow {
    row.source_table_details_json = serde_json::Value::Null;
    row
}

// --- the move --------------------------------------------------------------

/// How many keys one `rewrite_store` call of the move covers, and how many
/// keys apart the move confirms its claim with a beat during the rewrite.
/// A backend that rewrites key by key (Cassandra,
/// `rewrite_store_runs_key_by_key`) is called with one key at a time. The
/// signal and the claim's local state are read before every call, so a
/// move whose claim another holder took during the rewrite (a command
/// stopped past `stale_job_after_secs` on MongoDB or Cassandra) writes no
/// call after the one it had sent or was about to send, with the keys not
/// yet rewritten left under the old root and listed there. One slice is
/// one statement on PostgreSQL and MongoDB, and one transaction on SQLite,
/// well inside PostgreSQL's batch and its statement bound.
pub const REWRITE_SLICE: usize = 500;

/// How many rows the move reads by ARN at a time after the rewrite, to
/// tell a moved row (it names the configured store) from one another
/// writer removed or moved elsewhere: one read per key the old root no
/// longer lists, in place of a listing of the configured store, whose rows
/// every backend reads whole.
pub const MOVED_ROW_READS: usize = 16;

/// What [`move_store`] moves rows from and to.
#[derive(Clone, Copy)]
pub struct MoveTarget<'a> {
    /// The configured store, healthy: the copy is under its root.
    pub configured: &'a ConfiguredStore,
    /// Its identity, the one the rows get.
    pub identity: &'a StoreIdentity,
    /// The old root, from [`locate_root`].
    pub from_kind: StoreKind,
    pub from_root: &'a str,
    /// The window a live owner is told apart by (`stale_job_after` beside
    /// servers, zero on SQLite under the serve lock).
    pub owner_window: Duration,
    /// Asked before each row, before the rewrite and before each call of
    /// the rewrite; a stop there leaves the rows of the earlier calls moved
    /// and the rest under the old root, as a claim lost there does.
    pub stop: Stop<'a>,
    /// The maintenance claim the command holds, re-checked with a beat
    /// ([`HeldClaim::recheck`]) right before each dead CREATING row's
    /// resolution, each DELETING row's finished deletion and each
    /// [`REWRITE_SLICE`] keys of the rewrite, and read without a beat
    /// ([`HeldClaim::lost`]) before every other call of the rewrite; a claim
    /// found lost there stops the move as `stop` does
    /// ([`MoveReport::stopped`]), with the rows of the calls before it
    /// moved. `None` re-checks nothing.
    pub claim: Option<&'a HeldClaim>,
}

/// What [`move_store`] did.
#[derive(Debug, Default)]
pub struct MoveReport {
    /// How many rows the old root listed.
    pub rows: usize,
    /// The CREATING rows resolved, with the status written (AVAILABLE: the
    /// move writes no other).
    pub resolved: Vec<(String, BackupStatus)>,
    /// The DELETING rows whose deletion was finished in the configured
    /// store (now DELETED).
    pub finished: Vec<String>,
    /// Every row that stops the move, with the reason; non-empty means no
    /// row was moved.
    pub stopping: Vec<(NamedRow, String)>,
    /// Why the move stopped before the rewrite, or between two of its
    /// calls, otherwise: a signal, the claim lost, the fence's refusal.
    /// A stop between calls leaves the earlier calls' rows moved
    /// ([`Self::moved`]) and the rest under the old root ([`Self::left`]).
    pub stopped: Option<String>,
    /// The rows moved, by ARN: rows the old root no longer lists and whose
    /// row, read by its ARN, now names the configured store (every key
    /// counted, when the old root's listing after the rewrite failed).
    pub moved: Vec<String>,
    /// The rows neither root lists after the rewrite: removed, or moved to
    /// another store, by another writer during the move (a tombstone duty
    /// of a server on the old root, a `--move-store` of another catalog's
    /// command); nothing was moved for them.
    pub gone: Vec<String>,
    /// What `rewrite_store` counted.
    pub rewritten: u64,
    /// The rewrite's error, when it failed (it may have moved some rows).
    pub rewrite_error: Option<String>,
    /// The rows the old root still lists after the rewrite.
    pub left: Vec<NamedRow>,
    /// Why the listing after the rewrite failed, when it did.
    pub relist_error: Option<String>,
}

impl MoveReport {
    /// Whether every row of the old root was moved.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.stopping.is_empty()
            && self.stopped.is_none()
            && self.rewrite_error.is_none()
            && self.relist_error.is_none()
            && self.left.is_empty()
    }
}

/// `--move-store <old root>` (module doc). The caller holds the maintenance
/// claim and has waited for the jobs of the configured store and of the old
/// root's store ids.
///
/// # Errors
///
/// The backend's error for the listing of the old root or of the
/// configured root, both read before any row is checked; nothing was
/// changed.
pub async fn move_store<S: BackupCatalog + ?Sized>(
    storage: &S,
    target: MoveTarget<'_>,
) -> Result<MoveReport, StorageError> {
    let mut rows: Vec<BackupRow> = storage
        .list_backup_rows_by_root(target.from_kind, target.from_root, None)
        .map_ok(lean)
        .try_collect()
        .await?;
    let mut report = MoveReport {
        rows: rows.len(),
        ..MoveReport::default()
    };
    // One backup id per account after the move, checked first, so no row of
    // a group that stops for it gets a resolution or a deletion (on
    // Cassandra two spellings of an ARN share one prefix). The rows are
    // grouped by (account, backup id) over the configured root's rows and
    // the old root's, and every old-root row of a group of more than one is
    // set aside before any row is checked.
    let mut groups: HashMap<(String, String), Vec<String>> = HashMap::new();
    let mut configured_rows = storage.list_backup_rows_by_root(
        target.identity.store_kind,
        &target.identity.store_root,
        None,
    );
    while let Some(row) = configured_rows.try_next().await? {
        groups
            .entry((row.account_id, row.backup_id))
            .or_default()
            .push(row.backup_arn);
    }
    drop(configured_rows);
    for row in &rows {
        groups
            .entry((row.account_id.clone(), row.backup_id.clone()))
            .or_default()
            .push(row.backup_arn.clone());
    }
    let mut two_rows: HashSet<String> = HashSet::new();
    for row in &rows {
        let group = &groups[&(row.account_id.clone(), row.backup_id.clone())];
        if group.len() < 2 {
            continue;
        }
        two_rows.insert(row.backup_arn.clone());
        let others: Vec<&str> = group
            .iter()
            .filter(|arn| **arn != row.backup_arn)
            .map(String::as_str)
            .collect();
        report.stopping.push((
            NamedRow::of(row),
            format!(
                "the move would give account {} two rows of backup id {}: {} holds it as well; \
                 forget one of them with --forget-backup",
                row.account_id,
                row.backup_id,
                others.join(", ")
            ),
        ));
    }
    let calls = StoreCalls {
        store: &*target.configured.store,
        metrics: None,
    };
    for row in &mut rows {
        if two_rows.contains(&row.backup_arn) {
            continue;
        }
        if let Some(reason) = (target.stop)() {
            report.stopped = Some(reason);
            return Ok(report);
        }
        if let Err(why) = check_row(storage, target, calls, row, &mut report).await {
            report.stopping.push((NamedRow::of(row), why));
        }
        if report.stopped.is_some() {
            return Ok(report);
        }
    }
    if !report.stopping.is_empty() {
        return Ok(report);
    }
    if let Some(reason) = (target.stop)() {
        report.stopped = Some(reason);
        return Ok(report);
    }
    if let Err(problem) = target.configured.fence().check_catalog(storage).await {
        report.stopped = Some(format!(
            "the configured store's fence refused the move: {problem}"
        ));
        return Ok(report);
    }
    // The rewrite in calls of REWRITE_SLICE keys, or of one key on a
    // backend that rewrites key by key (Cassandra). Before each call the
    // signal and the claim's local state are read, which writes nothing,
    // and before each REWRITE_SLICE keys the claim is confirmed with a
    // beat: a claim another holder took during the rewrite (a command
    // stopped past `stale_job_after_secs` on MongoDB or Cassandra) stops
    // it before the next call, and the keys not yet rewritten stay under
    // the old root, where the listing below reports them.
    let keys: Vec<BackupKey> = rows.iter().map(BackupRow::key).collect();
    let per_call = if storage.rewrite_store_runs_key_by_key() {
        1
    } else {
        REWRITE_SLICE
    };
    let mut sent = 0_usize;
    for call in keys.chunks(per_call) {
        let mut reason = (target.stop)().or_else(|| target.claim.and_then(HeldClaim::lost));
        if reason.is_none()
            && sent.is_multiple_of(REWRITE_SLICE)
            && let Some(claim) = target.claim
        {
            reason = claim.recheck().await.err();
        }
        if let Some(reason) = reason {
            report.stopped = Some(if sent == 0 {
                format!("{reason} (before the rewrite; no row was moved)")
            } else {
                format!(
                    "{reason} (before the rewrite of row {} of {}; the {} row(s) rewritten \
                     before it are moved, and the rest are left under the old root)",
                    sent + 1,
                    keys.len(),
                    report.rewritten
                )
            });
            break;
        }
        match storage
            .rewrite_store(target.from_kind, target.from_root, call, target.identity)
            .await
        {
            Ok(count) => report.rewritten += count,
            Err(e) => {
                report.rewrite_error = Some(e.to_string());
                break;
            }
        }
        sent += call.len();
    }
    let left: Result<Vec<NamedRow>, StorageError> = storage
        .list_backup_rows_by_root(target.from_kind, target.from_root, None)
        .map_ok(|row| NamedRow::of(&row))
        .try_collect()
        .await;
    match left {
        Ok(left) => {
            let still: HashSet<&str> = left.iter().map(|r| r.backup_arn.as_str()).collect();
            // A key the old root no longer lists is moved when its row now
            // names the configured store: one read per such key, up to
            // MOVED_ROW_READS at a time and taken in key order, so the
            // configured store's rows are not read whole here.
            let mut reads = futures::stream::iter(
                keys.iter()
                    .filter(|k| !still.contains(k.backup_arn.as_str()))
                    .map(|key| async move {
                        let now = match BackupArn::parse(&key.backup_arn) {
                            Ok(arn) => storage.read_backup_row(&arn).await,
                            Err(_) => Ok(None),
                        };
                        (key, now)
                    }),
            )
            .buffered(MOVED_ROW_READS);
            while let Some((key, now)) = reads.next().await {
                if report.relist_error.is_some() {
                    report.moved.push(key.backup_arn.clone());
                    continue;
                }
                match now {
                    Ok(Some(row)) if row.store == *target.identity => {
                        report.moved.push(key.backup_arn.clone());
                    }
                    Ok(_) => report.gone.push(key.backup_arn.clone()),
                    Err(e) => {
                        // The count is all there is for this key and the
                        // rest: each is taken as moved.
                        report.relist_error = Some(format!(
                            "the row of {} could not be read after the rewrite: {e}",
                            key.backup_arn
                        ));
                        report.moved.push(key.backup_arn.clone());
                    }
                }
            }
            drop(reads);
            report.left = left;
        }
        Err(e) => {
            report.relist_error = Some(format!(
                "the old root's rows could not be read after the rewrite: {e}"
            ));
            // The count is all there is; with every key counted, every
            // row moved.
            if report.rewrite_error.is_none() && report.rewritten == keys.len() as u64 {
                report.moved = keys.iter().map(|k| k.backup_arn.clone()).collect();
            }
        }
    }
    let moved: HashSet<&str> = report.moved.iter().map(String::as_str).collect();
    for row in rows
        .iter()
        .filter(|r| moved.contains(r.backup_arn.as_str()))
    {
        tracing::info!(
            target: "extenddb::audit",
            action = "backup sync --move-store",
            account_id = %row.account_id,
            backup_arn = %row.backup_arn,
            status = %row.status,
            old_store_root = %target.from_root,
            old_store_id = %row.store.store_id,
            store_kind = %target.identity.store_kind,
            store_root = %target.identity.store_root,
            store_id = %target.identity.store_id,
            "moved a backup row to the configured store; no object was copied or deleted"
        );
    }
    Ok(report)
}

/// Why a CREATING row the backend does not hand over stops the move.
const LIVE_CREATING: &str = "CREATING with a live owner (or another taker): wait for its \
                             CreateBackup to end and run the move again";

/// One beat of the move's claim ([`MoveTarget::claim`]) right before
/// `step`, a step that cannot be undone: `false` when the claim is lost,
/// with the reason in `report.stopped`, which stops the move.
async fn claim_held(target: MoveTarget<'_>, step: &str, report: &mut MoveReport) -> bool {
    let Some(claim) = target.claim else {
        return true;
    };
    match claim.recheck().await {
        Ok(()) => true,
        Err(reason) => {
            report.stopped = Some(format!("{reason} (before {step}; no row was moved)"));
            false
        }
    }
}

/// One row of the old root against the configured store (module doc):
/// `Err` is why it stops the move. A CREATING row resolved, and a DELETING
/// row finished, carry their new status in `row` from here on.
async fn check_row<S: BackupCatalog + ?Sized>(
    storage: &S,
    target: MoveTarget<'_>,
    calls: StoreCalls<'_>,
    row: &mut BackupRow,
    report: &mut MoveReport,
) -> Result<(), String> {
    let arn = BackupArn::parse(&row.backup_arn)
        .map_err(|e| format!("the row's ARN does not parse: {e}"))?;
    let prefix = arn.store_prefix();
    if row.status == BackupStatus::Creating {
        if !claim_held(
            target,
            &format!("the resolution of the CREATING row {}", row.backup_arn),
            report,
        )
        .await
        {
            return Ok(());
        }
        return match resolve_dead_creating_row(
            storage,
            calls,
            row,
            target.owner_window,
            OnIncomplete::Leave,
        )
        .await
        {
            CreatingOutcome::Resolved(status) => {
                tracing::info!(
                    target: "extenddb::audit",
                    action = "backup sync --move-store",
                    account_id = %row.account_id,
                    backup_arn = %row.backup_arn,
                    status = %status,
                    store_root = %row.store.store_root,
                    "resolved a CREATING row whose owner is dead against the configured store"
                );
                report.resolved.push((row.backup_arn.clone(), status));
                row.status = status;
                // The probe that resolved it is the AVAILABLE check.
                Ok(())
            }
            CreatingOutcome::Incomplete(why) => Err(format!(
                "CREATING, its owner dead, and the copy under the configured root holds no \
                 complete backup ({why}); the move writes no status for it, since the old root \
                 may hold the complete backup. Run a server configured with the old root (or, \
                 on SQLite, plain backup sync there), whose reconciler resolves the row against \
                 the old root, then copy the prefix again; or remove the row with \
                 --forget-backup"
            )),
            CreatingOutcome::NotHandedOver => Err(LIVE_CREATING.to_owned()),
            CreatingOutcome::LeftToOwner(why) => Err(format!(
                "CREATING, taken over by another owner during the move ({why})"
            )),
            CreatingOutcome::Failed(why) => Err(format!("CREATING: {why}")),
        };
    }
    // A DELETING or DELETED row's backup was deleted by a client: adopting
    // its copied prefix would bring it back, so the remedy offered is the
    // prefix's removal. A FAILED row's prefix may be what the operator
    // wants kept.
    let verified = |status: BackupStatus| {
        let remedy = match status {
            BackupStatus::Failed => {
                "Remove that prefix from the configured root, or remove the row with \
                 --forget-backup and let backup sync adopt the prefix"
            }
            _ => {
                "The backup was deleted: remove that prefix from the configured root and run \
                 the move again (removing the row with --forget-backup instead would have \
                 backup sync adopt the prefix, bringing back AVAILABLE a backup a client \
                 deleted, to be deleted again)"
            }
        };
        format!(
            "{status} whose prefix under the configured root holds a verified manifest: moving \
             it would have the reconciler delete a complete backup. {remedy}"
        )
    };
    match row.status {
        BackupStatus::Creating => unreachable!("returned above"),
        BackupStatus::Deleting => {
            match verify_prefix(calls, &prefix, None).await {
                Ok(Verified::Complete(_)) => return Err(verified(BackupStatus::Deleting)),
                Ok(Verified::Incomplete(_)) => {}
                Err(e) => return Err(format!("the configured store could not be read: {e}")),
            }
            if !claim_held(
                target,
                &format!(
                    "the deletion of the DELETING row {} in the configured store",
                    row.backup_arn
                ),
                report,
            )
            .await
            {
                return Ok(());
            }
            let fence = target.configured.fence();
            match super::finish_backup_delete(
                storage,
                &target.configured.calls("deletion"),
                &fence,
                &arn,
            )
            .await
            {
                Ok(true) => {
                    tracing::info!(
                        target: "extenddb::audit",
                        action = "backup sync --move-store",
                        account_id = %row.account_id,
                        backup_arn = %row.backup_arn,
                        status = "DELETED",
                        store_root = %target.identity.store_root,
                        "finished a DELETING row's deletion in the configured store"
                    );
                    report.finished.push(row.backup_arn.clone());
                    row.status = BackupStatus::Deleted;
                    Ok(())
                }
                Ok(false) => Err(
                    "DELETING: the row was no longer DELETING when its objects were gone"
                        .to_owned(),
                ),
                Err(e) => Err(format!(
                    "DELETING: its deletion in the configured store did not finish: {e}"
                )),
            }
        }
        BackupStatus::Deleted | BackupStatus::Failed => {
            match verify_prefix(calls, &prefix, None).await {
                Ok(Verified::Complete(_)) => Err(verified(row.status)),
                Ok(Verified::Incomplete(_)) => Ok(()),
                Err(e) => Err(format!("the configured store could not be read: {e}")),
            }
        }
        BackupStatus::Available => match probe_manifest(calls, &arn).await {
            Ok(Probe::Complete(fields)) => {
                if fields.size_bytes == Some(row.size_bytes)
                    && fields.item_count == Some(row.item_count)
                {
                    Ok(())
                } else {
                    Err(format!(
                        "the manifest under the configured root records {} bytes and {} items \
                         and the row {} bytes and {} items: another backup under the same ARN",
                        fields.size_bytes.unwrap_or_default(),
                        fields.item_count.unwrap_or_default(),
                        row.size_bytes,
                        row.item_count
                    ))
                }
            }
            Ok(Probe::Incomplete(why)) => Err(format!(
                "not a complete copy under the configured root: {why}; copy the prefix again \
                 (or remove the row with --forget-backup)"
            )),
            Err(e) => Err(format!("the configured store could not be read: {e}")),
        },
    }
}
