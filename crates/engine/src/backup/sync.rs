// SPDX-License-Identifier: Apache-2.0

//! `backup sync`: the catalog rebuilt from the store, for the backups the
//! catalog has no row for, and the report of the rows whose backup the store
//! no longer holds.
//!
//! The store is the source of truth and the row a cache (design 4(g)).
//! [`sync`] reads the rows first: every row of the configured kind and root,
//! of every status and every store id the catalog records for the root. It
//! keeps of each row only what it uses (the status, store id and table by
//! `(account_id, backup_id)`, and the key of each AVAILABLE row of the
//! configured store id) and drops the rest as the row is read; the
//! backends' `list_backup_rows` collect their whole result first, so the
//! read peaks above what is kept. It then lists the configured
//! store's whole root (the store trait has no delimiter listing) and handles
//! each `<account>/<table>/<backup_id>/` prefix ([`classify_store_key`]) as
//! the listing passes it: the store lists in key order, so the keys of one
//! prefix arrive together, and sync holds the objects of one prefix with
//! their sizes and the prefix of each manifest it listed, never the whole
//! listing. A filesystem store builds its own listing in memory
//! before it returns the first key (the store trait's doc gives its cost);
//! an S3 store lists a page at a time. Keys outside every backup prefix
//! (`extenddb-store.json` at the root) are passed over; the reserved
//! components (`exports/` first, `changelog` third) and three components
//! that are not a backup prefix (an id of an earlier build's shape, a
//! component no ARN can carry) are reported as skipped, never read.
//!
//! A prefix with a row of any status (CREATING, DELETING, DELETED, FAILED,
//! AVAILABLE) is skipped with the row's status as the reason, so sync never
//! modifies a row nor adopts a prefix a row names. A prefix with no row is
//! read and verified as the dead-owner duty verifies one
//! (`reconciler::verify_prefix`: the manifest and its sidecar within their
//! bounds, the digest, the reader's acceptance, every listed data file at
//! its recorded size, from the prefix's objects already listed), and the
//! manifest's ARN must name the listed account, table and backup id. A
//! manifest whose ARN is in another partition than [`ARN_PARTITION`] is
//! skipped: every request refuses such an ARN before it reads a row, so no
//! request could address the row. A manifest whose ARN is in another region
//! than this server's is adopted under this server's region's spelling of
//! the ARN (the manifest keeps the spelling it carries) and logged: the row
//! then carries the ARN this server issues, as the rows of the backups it
//! made do, so ListBackups names this server's region and DescribeBackup,
//! RestoreTableFromBackup and DeleteBackup find the backup under either
//! spelling, as they do for a backup this server made; the readers that
//! compare a manifest's ARN with the row's accept a difference in the
//! region alone (`BackupArn::names_same_backup`). Before the insert the row
//! is read under each spelling of the ARN (this server's region and the
//! manifest's), and a backup the manifest's spelling has a row for is
//! skipped: Cassandra keys its rows by the ARN and has no unique backup id,
//! so there the read is what keeps a second row of the backup id out of
//! the catalog under the manifest's spelling (a row of another root under
//! a third region's spelling is read by neither). A row of the same account
//! and id under another table that the rows read did not return (a row of
//! another root, or one whose list entries are missing) is not seen by it;
//! within one run, the row check keeps a second prefix of an id this run
//! adopted out. A row under the spelling the insert writes is left to the
//! insert. A
//! verified prefix gets an AVAILABLE row through
//! [`BackupCatalog::insert_backup_row`], unowned, in the configured store's
//! identity, with the manifest's name, size, item total and
//! `source_table`, the ARN in this server's region, and the id's instant
//! as `created_at`. An insert that
//! answers `false` found a row that landed since the rows were read (a
//! CreateBackup on another instance), a row of another root, account or
//! table holding the backup id, or, on Cassandra, a row whose list and
//! status entries are missing and that is not this insert's (a
//! CreateBackup's claim that stopped between its two writes); the row is
//! re-read to say which, and is left as it is. On Cassandra an earlier
//! insert of this same row that stopped between the row and its entries
//! (a sync killed there, or an entry batch that failed twice) is not in
//! the listings the rows are read from, so the next sync of the prefix
//! reaches the insert, which writes the entries and answers `true` (the
//! trait's doc), and the report lists the backup as adopted again. A failed
//! read of one prefix's manifest or sidecar ([`SkipReason::Store`]), or a
//! failed catalog call for one prefix ([`SkipReason::Catalog`]), is
//! reported for that prefix and sync goes on; a later run adopts it once
//! the call succeeds ([`SyncReport::transient_skips`]).
//!
//! A listing that fails stops sync: the rows inserted for the prefixes the
//! listing passed before the failure stay (each was verified), the prefix
//! it was in is not read (its keys may be cut short), and no
//! missing-manifest report is made, since the listing did not reach every
//! prefix. A stop asked through [`SyncTarget::stop`] (the command's
//! signal) is honoured before each prefix the same way.
//!
//! The report also names the AVAILABLE rows of the configured store whose
//! manifest the listing did not hold. [`prune_missing`] (`--prune-missing`,
//! under the maintenance claim) removes each such row outright through
//! [`BackupCatalog::forget_available_backup_row`], the row's lock, status
//! check and in-use rule with the removal, and deletes nothing in the
//! store: the bytes may be under another host's root of the same path, and
//! no DELETING or DELETED tombstone is left for a deletion duty to act on
//! them. A later sync where the bytes are adopts them again. Without
//! `--prune-missing` such rows stay AVAILABLE.
//!
//! [`sync`] neither lists nor changes the rows of the other stores (another
//! kind or root, or the configured root under another store id): a prefix
//! whose backup id such a row holds meets it at the read under the ARN's
//! spellings or at the insert, and is skipped naming that row's status,
//! kind, root and store id. The command reports
//! those rows as a whole once a listing has reached its end
//! ([`super::store_maintenance::other_stores`]: the rows per status of
//! each other store, and each CREATING and DELETING row by ARN), with the
//! two options that act on them, `--move-store` and `--forget-store`.
//!
//! The command takes the maintenance claim for every sync on PostgreSQL,
//! MongoDB and Cassandra, where it runs beside servers (design 4(f)), and
//! runs under the serve lock on SQLite; so no purge runs between its row
//! read and its insert while the claim is held. The claim is re-checked
//! with a beat ([`SyncTarget::claim`], `HeldClaim::recheck`) right before
//! each insert, and `--prune-missing` re-checks it right before each
//! removal: on MongoDB and Cassandra a command stopped (SIGSTOP, a paused
//! VM) past `stale_job_after_secs` can have its claim taken over by a
//! purge, and the beat finds it lost and stops the run before the write.
//! The bound that remains is a stop after the re-check confirmed the claim
//! and before the insert or removal reached the catalog, or with that call
//! already sent: it lands once the process resumes. A row it inserts is
//! AVAILABLE and unowned
//! (`owner_generation` 0); no reconciler duty reads an AVAILABLE row's
//! generation, so the row is handled as a created one.
//!
//! [`startup_sync`] is the sync a server whose catalog is an in-memory
//! SQLite database runs itself once its store is healthy: the catalog is
//! new at every start, so it is how such a server serves the backups of its
//! previous starts. The check that made the store healthy adopted a root an
//! earlier in-memory catalog owned (`store_identity`'s module doc). Each run
//! is bounded by [`STARTUP_SYNC_TIMEOUT`], and its store calls run as the
//! store's checks run theirs (`BoundedCalls`, under the same bound): on a
//! filesystem root the listing's walk holds the root's call slot and, once
//! abandoned at the bound, is marked in flight until it returns, so no
//! second walk starts beside the first: each cycle's check is refused at
//! once on the mark, naming the startup sync's call, and records the store
//! `unreachable` (CreateBackup, DeleteBackup and restores refused, the
//! deletions left waiting), and the sync, which runs only on a healthy
//! store, runs again on the first cycle after the walk ends. A run that
//! does not finish, or
//! that skipped a prefix for a store or catalog error, is left pending and
//! runs again in a later reconciler cycle. Every run lists from the first
//! key, so the startup sync completes only while listing the whole root
//! fits in the bound; past that it stays pending and is retried every
//! cycle, and the backups beyond what a run reaches stay unserved. Until a
//! run finishes, requests
//! for the earlier backups answer `BackupNotFoundException`; the log line
//! "startup sync of the in-memory catalog: N backup(s) adopted" ends that
//! window. Each run that lists the whole root ends with the line of
//! counts, and a run cut at the bound or stopped by an error logs one WARN
//! line instead. Until a run has logged its line of counts, a run logs each
//! prefix it skipped (WARN for a store or catalog error, INFO otherwise);
//! a later run of a sync left pending logs the transient skips at WARN
//! again and the rest at DEBUG, so a root of many prefixes with one that
//! fails costs one WARN line per cycle, not one line per prefix.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use extenddb_backup_store::{BackupStore, StoreError};
use extenddb_core::backup_format::{
    BackupArn, CHANGELOG_COMPONENT, EXPORTS_COMPONENT, EXTENDDB_MANIFEST_FILE, StoreKey,
    backup_creation_date_time, classify_store_key,
};
use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::BackupCatalog;
use extenddb_storage::backup_catalog::{
    BackupKey, BackupRow, BackupStatus, ForgetRow, StoreIdentity, UNOWNED_GENERATION,
    is_backup_not_found,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::ARN_PARTITION;
use futures::TryStreamExt;

use super::BackupRuntime;
use super::maintenance::HeldClaim;
use super::reconciler::{StoreCalls, Verified, manifest_item_count, verify_prefix};

/// The bound of one run of [`startup_sync`], before the server serves and in
/// each reconciler cycle, and of each of its store calls. A run abandoned
/// at the bound keeps the rows it inserted, and the next run skips those
/// prefixes on the row check, but every run lists the root from its first
/// key: a root whose listing alone takes longer than the bound is never
/// synced, and the sync stays pending and is retried every cycle. On both
/// store kinds the whole listing is collected before the first key reaches
/// the run (`BoundedCalls::list` collects the store's stream into a `Vec`
/// of `ObjectMeta`, one per object of the root, held in memory until the
/// run ends; the filesystem store also builds its listing whole before it
/// yields), so a listing that outlasts the bound reaches no prefix. On an
/// S3 store the abandoned listing's requests end with the dropped future.
/// On a filesystem store the listing is not stopped: the walk runs to its
/// end on the blocking pool, holding the listing it builds and the root's
/// call slot, marked in flight (module doc); until it ends, each cycle's
/// check is refused at once, naming the startup sync's call, and records
/// the store `unreachable`, so CreateBackup, DeleteBackup and restores are
/// refused, the deletions wait, and the sync, which runs only on a healthy
/// store, does not run; once the walk ends, the next cycle's check passes
/// and the sync runs again, from the first key. A root too large to list
/// within the bound is never synced by an in-memory server: the operator
/// gives it a smaller root or a durable catalog.
pub const STARTUP_SYNC_TIMEOUT: Duration = Duration::from_secs(60);

/// Asked before each prefix: why to stop, or `None` to go on.
pub type Stop<'a> = &'a (dyn Fn() -> Option<String> + Sync);

/// The store a sync reads and the identity its rows are written under.
#[derive(Clone, Copy)]
pub struct SyncTarget<'a> {
    /// The configured store.
    pub store: &'a dyn BackupStore,
    /// The configured store's identity, its id the identity file's.
    pub identity: &'a StoreIdentity,
    /// The collector failed store calls are recorded in; `None` in a
    /// command.
    pub metrics: Option<&'a MetricsCollector>,
    /// The server's region: an adopted row carries the manifest's ARN
    /// rebuilt in it (logged when the manifest's region differs), and the
    /// row is looked up under both spellings before the insert. `None`
    /// adopts the ARN as the manifest spells it and reads that spelling
    /// alone.
    pub region: Option<&'a str>,
    /// Asked before each prefix; `None` never stops.
    pub stop: Option<Stop<'a>>,
    /// The maintenance claim the command holds, re-checked with a beat
    /// ([`HeldClaim::recheck`]) right before each insert; a claim found lost
    /// there stops the run as `stop` does, the prefix not adopted. `None`
    /// (the startup sync of an in-memory catalog, which holds no claim)
    /// re-checks nothing.
    pub claim: Option<&'a HeldClaim>,
}

/// What a sync did.
#[derive(Debug, Default)]
pub struct SyncReport {
    /// The backups that got an AVAILABLE row, in listing order.
    pub adopted: Vec<AdoptedBackup>,
    /// The prefixes that did not, in listing order, with the reason.
    pub skipped: Vec<SkippedPrefix>,
    /// The keys of the AVAILABLE rows of the configured store whose
    /// manifest the listing does not hold; empty when the run stopped
    /// before the end of the listing.
    pub missing_manifests: Vec<BackupKey>,
    /// Why the run stopped before the end of the listing, when it did.
    pub stopped: Option<String>,
}

impl SyncReport {
    /// The prefixes skipped for a store or catalog error, which a later run
    /// may adopt.
    #[must_use]
    pub fn transient_skips(&self) -> usize {
        self.skipped
            .iter()
            .filter(|s| s.reason.is_transient())
            .count()
    }
}

/// A backup [`sync`] gave an AVAILABLE row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptedBackup {
    /// The ARN as the row carries it: in the server's region when one is
    /// given, otherwise as the manifest spells it.
    pub backup_arn: String,
    /// Set when the manifest's ARN names another region than the server's:
    /// the sentence the log carries, naming the manifest's spelling.
    pub foreign: Option<String>,
}

/// A prefix [`sync`] did not adopt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedPrefix {
    /// The prefix, as the store's keys spell it (`exports/` for that
    /// component).
    pub prefix: String,
    pub reason: SkipReason,
}

/// Why a prefix was not adopted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// Under a reserved component, the one named.
    Reserved(&'static str),
    /// Three components that are not a backup prefix; why.
    NotABackup(String),
    /// The catalog has a row for the account and backup id: its status,
    /// store id, and its table when that is not the prefix's.
    HasRow {
        status: BackupStatus,
        store_id: String,
        other_table: Option<String>,
    },
    /// A row the catalog's read had not returned holds the backup: what
    /// the read under the ARN's spellings, or the insert's re-read, found.
    RowExists(String),
    /// The prefix failed verification; which check.
    Incomplete(String),
    /// The manifest names another backup than the path.
    ArnMismatch(String),
    /// The manifest's ARN is in another partition: no request to this
    /// server can address it.
    OtherPartition(String),
    /// A read of the prefix's manifest or sidecar failed; the store's
    /// error.
    Store(String),
    /// A catalog call for the prefix (the read under the ARN's spellings,
    /// the insert) failed; the catalog's error.
    Catalog(String),
}

impl SkipReason {
    /// Whether a later run may adopt the prefix without anything changing
    /// in the store: a store or catalog call failed.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Store(_) | Self::Catalog(_))
    }
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reserved(component) if *component == EXPORTS_COMPONENT => {
                f.write_str("reserved for exports")
            }
            Self::Reserved(_) => f.write_str("reserved for the change-log archive"),
            Self::NotABackup(why) => write!(f, "not a backup prefix: {why}"),
            Self::HasRow {
                status,
                store_id,
                other_table: None,
            } => write!(
                f,
                "the catalog has a row for it ({status}, store id {store_id})"
            ),
            Self::HasRow {
                status,
                store_id,
                other_table: Some(table),
            } => write!(
                f,
                "the catalog has a row for its backup id under table {table} ({status}, store \
                 id {store_id}); a backup id names one backup"
            ),
            Self::RowExists(what) => f.write_str(what),
            Self::Incomplete(why) => write!(f, "not a complete backup: {why}"),
            Self::ArnMismatch(why) | Self::OtherPartition(why) => f.write_str(why),
            Self::Store(e) => write!(f, "the store could not be read: {e}"),
            Self::Catalog(e) => write!(f, "the catalog could not be read or written: {e}"),
        }
    }
}

/// Why a sync stopped before it reached the end of the listing.
#[derive(Debug)]
pub enum SyncError {
    /// The store's listing failed; `partial` is what the run did with the
    /// prefixes the listing passed before (no missing-manifest report).
    Listing {
        error: StoreError,
        partial: SyncReport,
    },
    /// The catalog's rows could not be read; nothing was listed or written.
    Catalog(StorageError),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Listing { error, partial } if partial.adopted.is_empty() => {
                write!(f, "the store's listing failed: {error}; no row was written")
            }
            Self::Listing { error, partial } => write!(
                f,
                "the store's listing failed: {error}; {} backup(s) listed before it were adopted",
                partial.adopted.len()
            ),
            Self::Catalog(e) => write!(
                f,
                "the catalog's rows could not be read: {e}; no row was written"
            ),
        }
    }
}

impl std::error::Error for SyncError {}

/// The prefix a listed key belongs to, and what it is.
enum Group {
    /// Under a reserved component.
    Reserved(&'static str),
    /// Under three components that are not a backup prefix.
    NotABackup(String),
    /// Under a backup prefix: its components and the objects listed so far
    /// with their sizes.
    Backup {
        account_id: String,
        table_name: String,
        backup_id: String,
        objects: HashMap<String, u64>,
    },
}

/// The prefix of `key` and an empty group for it; `None` for a key outside
/// every backup prefix.
fn group_of(key: &str) -> Option<(String, Group)> {
    match classify_store_key(key) {
        StoreKey::Outside => None,
        StoreKey::Reserved(component) => {
            Some((reserved_prefix(key, component), Group::Reserved(component)))
        }
        StoreKey::NotABackup { prefix, reason } => {
            Some((prefix.to_owned(), Group::NotABackup(reason)))
        }
        StoreKey::Backup {
            prefix,
            account_id,
            table_name,
            backup_id,
        } => Some((
            prefix.to_owned(),
            Group::Backup {
                account_id: account_id.to_owned(),
                table_name: table_name.to_owned(),
                backup_id: backup_id.to_owned(),
                objects: HashMap::new(),
            },
        )),
    }
}

/// The prefix a key under a reserved component is reported under:
/// `exports/`, or `<account>/<table>/changelog/`.
fn reserved_prefix(key: &str, component: &str) -> String {
    if component == EXPORTS_COMPONENT {
        return format!("{EXPORTS_COMPONENT}/");
    }
    let mut parts = key.splitn(3, '/');
    let account = parts.next().unwrap_or_default();
    let table = parts.next().unwrap_or_default();
    format!("{account}/{table}/{CHANGELOG_COMPONENT}/")
}

/// The rows of the configured kind and root, every status and every store
/// id the catalog records for the root, kept as only what sync uses: the
/// row check's status, store id and table by `(account_id, backup_id)`,
/// and the keys of the configured store id's AVAILABLE rows (the
/// missing-manifest candidates). Each row is dropped as it is read, its
/// parsed `source_table_details_json` with it.
async fn rows_of_root<S: BackupCatalog + ?Sized>(
    storage: &S,
    identity: &StoreIdentity,
) -> Result<(Known, Vec<BackupKey>), StorageError> {
    let mut ids = storage
        .read_store_ids(identity.store_kind, &identity.store_root)
        .await?;
    if !ids.contains(&identity.store_id) {
        ids.push(identity.store_id.clone());
    }
    let mut known = Known::new();
    let mut available = Vec::new();
    for id in ids {
        let of_id = StoreIdentity {
            store_id: id,
            ..identity.clone()
        };
        let mut listed = storage.list_backup_rows(None, &of_id);
        while let Some(row) = listed.try_next().await? {
            if row.status == BackupStatus::Available && row.store.store_id == identity.store_id {
                available.push(row.key());
            }
            let BackupRow {
                account_id,
                backup_id,
                table_name,
                status,
                store,
                ..
            } = row;
            known.insert(
                (account_id, backup_id),
                (status, store.store_id, table_name),
            );
        }
    }
    Ok((known, available))
}

/// What the row check knows of a row: its status, store id and table.
type Known = HashMap<(String, String), (BackupStatus, String, String)>;

/// Adopt the backups of the configured store that have no row (module doc).
///
/// # Errors
///
/// [`SyncError`]: the catalog's read failed (nothing written), or the
/// listing failed (the rows inserted before it stay).
pub async fn sync<S: BackupCatalog + ?Sized>(
    storage: &S,
    target: SyncTarget<'_>,
) -> Result<SyncReport, SyncError> {
    let calls = StoreCalls {
        store: target.store,
        metrics: target.metrics,
    };
    let identity = target.identity;
    let (mut known, available) = rows_of_root(storage, identity)
        .await
        .map_err(SyncError::Catalog)?;

    let mut report = SyncReport::default();
    // The prefixes whose manifest the listing held: one string per listed
    // manifest.
    let mut manifests: HashSet<String> = HashSet::new();
    let mut open: Option<(String, Group)> = None;
    let mut listing = calls.store.list("");
    loop {
        let next = match listing.try_next().await {
            Ok(next) => next,
            Err(e) => {
                return Err(SyncError::Listing {
                    error: calls.failed("list", e),
                    partial: report,
                });
            }
        };
        let Some(meta) = next else { break };
        let Some((prefix, group)) = group_of(&meta.key) else {
            continue;
        };
        if open.as_ref().is_none_or(|(open, _)| *open != prefix) {
            // The listing has passed the open prefix: every key of it is
            // listed.
            if let Some((done, group)) = open.take()
                && let Some(reason) =
                    finish_group(storage, calls, target, &mut known, &mut report, done, group).await
            {
                report.stopped = Some(reason);
                return Ok(report);
            }
            open = Some((prefix.clone(), group));
        }
        if let Some((_, Group::Backup { objects, .. })) = &mut open {
            if meta.key.len() == prefix.len() + EXTENDDB_MANIFEST_FILE.len()
                && meta.key.ends_with(EXTENDDB_MANIFEST_FILE)
            {
                manifests.insert(prefix);
            }
            objects.insert(meta.key, meta.size);
        }
    }
    if let Some((done, group)) = open.take()
        && let Some(reason) =
            finish_group(storage, calls, target, &mut known, &mut report, done, group).await
    {
        report.stopped = Some(reason);
        return Ok(report);
    }

    report.missing_manifests = available
        .into_iter()
        .filter(|key| {
            !BackupArn::parse(&key.backup_arn)
                .is_ok_and(|arn| manifests.contains(&arn.store_prefix()))
        })
        .collect();
    Ok(report)
}

/// One prefix the listing has passed: the stop, then the row check, then
/// the verification and the insert. Returns why the run stops, when the
/// stop asked it to (the prefix is then left unread).
async fn finish_group<S: BackupCatalog + ?Sized>(
    storage: &S,
    calls: StoreCalls<'_>,
    target: SyncTarget<'_>,
    known: &mut Known,
    report: &mut SyncReport,
    prefix: String,
    group: Group,
) -> Option<String> {
    if let Some(reason) = target.stop.and_then(|stop| stop()) {
        return Some(reason);
    }
    let (account_id, table_name, backup_id, objects) = match group {
        Group::Reserved(component) => {
            report.skipped.push(SkippedPrefix {
                prefix,
                reason: SkipReason::Reserved(component),
            });
            return None;
        }
        Group::NotABackup(why) => {
            report.skipped.push(SkippedPrefix {
                prefix,
                reason: SkipReason::NotABackup(why),
            });
            return None;
        }
        Group::Backup {
            account_id,
            table_name,
            backup_id,
            objects,
        } => (account_id, table_name, backup_id, objects),
    };
    // The row check, before anything under the prefix is read.
    let key = (account_id, backup_id);
    if let Some((status, store_id, row_table)) = known.get(&key) {
        report.skipped.push(SkippedPrefix {
            prefix,
            reason: SkipReason::HasRow {
                status: *status,
                store_id: store_id.clone(),
                other_table: (*row_table != table_name).then(|| row_table.clone()),
            },
        });
        return None;
    }
    let outcome = adopt_prefix(
        storage,
        calls,
        target,
        &prefix,
        (&key.0, &table_name, &key.1),
        &objects,
    )
    .await;
    match outcome {
        Ok(adopted) => {
            known.insert(
                key,
                (
                    BackupStatus::Available,
                    target.identity.store_id.clone(),
                    table_name,
                ),
            );
            report.adopted.push(adopted);
        }
        Err(NotAdopted::Skipped(reason)) => report.skipped.push(SkippedPrefix { prefix, reason }),
        Err(NotAdopted::Stopped(reason)) => return Some(reason),
    }
    None
}

/// Why [`adopt_prefix`] inserted no row.
enum NotAdopted {
    /// The prefix is reported as skipped, and the run goes on.
    Skipped(SkipReason),
    /// The claim was found lost right before the insert: the run stops,
    /// the prefix not adopted.
    Stopped(String),
}

impl From<SkipReason> for NotAdopted {
    fn from(reason: SkipReason) -> Self {
        Self::Skipped(reason)
    }
}

/// Verify one prefix with no row and insert its row.
async fn adopt_prefix<S: BackupCatalog + ?Sized>(
    storage: &S,
    calls: StoreCalls<'_>,
    target: SyncTarget<'_>,
    prefix: &str,
    (account_id, table_name, backup_id): (&str, &str, &str),
    objects: &HashMap<String, u64>,
) -> Result<AdoptedBackup, NotAdopted> {
    let manifest = match verify_prefix(calls, prefix, Some(objects)).await {
        Ok(Verified::Complete(manifest)) => manifest,
        Ok(Verified::Incomplete(why)) => return Err(SkipReason::Incomplete(why).into()),
        Err(e) => return Err(SkipReason::Store(e.to_string()).into()),
    };
    let arn = manifest
        .backup_arn()
        .map_err(|e| SkipReason::ArnMismatch(format!("the manifest's ARN is refused: {e}")))?;
    // The skip reasons are the report's; the caller logs or prints them
    // (the startup sync once per prefix, the command in its report).
    if arn.account_id() != account_id
        || arn.table_name() != table_name
        || arn.backup_id() != backup_id
    {
        return Err(SkipReason::ArnMismatch(format!(
            "the manifest names {}, whose account, table and backup id are not the prefix's",
            arn.to_arn_string()
        ))
        .into());
    }
    let backup_arn = arn.to_arn_string();
    if arn.partition() != ARN_PARTITION {
        return Err(SkipReason::OtherPartition(format!(
            "the manifest names {backup_arn}, in partition {}, and no request to this server \
             can address an ARN outside partition {ARN_PARTITION}",
            arn.partition()
        ))
        .into());
    }
    let created_at = backup_creation_date_time(backup_id).ok_or_else(|| {
        SkipReason::NotABackup(format!("backup id {backup_id:?} carries no instant"))
    })?;
    // The row's ARN: the manifest's rebuilt in this server's region, so the
    // row reads as one of a backup this server made (module doc); the
    // manifest's own spelling when no region is given or the region is
    // outside the ARN alphabet.
    let row_arn = target
        .region
        .and_then(|region| arn.in_region(region))
        .unwrap_or_else(|| arn.clone());
    let foreign = foreign_note(&arn, target.region);
    // A row under a spelling other than the row's: on Cassandra the insert
    // decides per spelling only (the trait's doc). A row under the row's
    // own spelling is left to the insert, which answers `false` for it, or,
    // on Cassandra, completes it when it is an earlier insert of this same
    // row that stopped before its list and status entries.
    match row_under_a_spelling(storage, &arn, target.region).await {
        Ok(Some((spelling, row))) if spelling != row_arn => {
            return Err(SkipReason::RowExists(describe_row(target, &row)).into());
        }
        Ok(_) => {}
        Err(e) => return Err(SkipReason::Catalog(e.to_string()).into()),
    }
    let source_table_details_json = serde_json::to_value(&manifest.source_table)
        .map_err(|e| SkipReason::Incomplete(format!("the source table does not serialize: {e}")))?;
    let row = BackupRow {
        backup_id: backup_id.to_owned(),
        account_id: account_id.to_owned(),
        table_name: table_name.to_owned(),
        backup_arn: row_arn.to_arn_string(),
        backup_name: manifest.backup_name.clone(),
        status: BackupStatus::Available,
        store: target.identity.clone(),
        size_bytes: manifest.backup_size_bytes,
        item_count: manifest_item_count(&manifest),
        created_at,
        completed_at: None,
        failure_reason: None,
        source_table_details_json,
        owner_generation: UNOWNED_GENERATION,
        heartbeat_at: 0.0,
    };
    // The insert is the step that cannot be undone: a row of a prefix a
    // purge holding a claim taken over from this command is deleting would
    // be AVAILABLE over deleted bytes.
    if let Some(claim) = target.claim
        && let Err(reason) = claim.recheck().await
    {
        return Err(NotAdopted::Stopped(format!(
            "{reason} (before the insert for {prefix}, which was not adopted)"
        )));
    }
    match storage.insert_backup_row(&row).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(
                SkipReason::RowExists(existing_row(storage, target, &row_arn).await).into(),
            );
        }
        Err(e) => return Err(SkipReason::Catalog(e.to_string()).into()),
    }
    match &foreign {
        Some(note) => tracing::warn!(
            "backup sync: adopted {} (account {account_id}), {note}; requests find it under \
             either spelling of its ARN",
            row.backup_arn
        ),
        None => tracing::info!(
            "backup sync: adopted {} (account {account_id})",
            row.backup_arn
        ),
    }
    Ok(AdoptedBackup {
        backup_arn: row.backup_arn,
        foreign,
    })
}

/// The row under `arn` rebuilt in `region` (the spelling every request tries
/// first) or under `arn` as the manifest writes it, in that order, with the
/// spelling it was found under.
async fn row_under_a_spelling<S: BackupCatalog + ?Sized>(
    storage: &S,
    arn: &BackupArn,
    region: Option<&str>,
) -> Result<Option<(BackupArn, BackupRow)>, StorageError> {
    let written = arn.to_arn_string();
    if let Some(region) = region
        && let Ok(parts) = extenddb_storage::util::check_backup_arn(&written)
    {
        let spellings = super::BackupArnSpellings::new(&parts, region);
        return super::read_catalog_row(storage, &spellings).await;
    }
    Ok(storage
        .read_backup_row(arn)
        .await?
        .map(|row| (arn.clone(), row)))
}

/// The sentence for a manifest whose ARN is in another region than the
/// server's, or `None`.
fn foreign_note(arn: &BackupArn, region: Option<&str>) -> Option<String> {
    region.filter(|region| arn.region() != *region).map(|_| {
        format!(
            "whose manifest spells its ARN in region {}, not this server's",
            arn.region()
        )
    })
}

/// What holds the backup id when the insert answered `false`: the row is
/// re-read by the ARN the insert wrote, so the report says which row it
/// met.
async fn existing_row<S: BackupCatalog + ?Sized>(
    storage: &S,
    target: SyncTarget<'_>,
    row_arn: &BackupArn,
) -> String {
    match storage.read_backup_row(row_arn).await {
        Ok(Some(row)) => describe_row(target, &row),
        Ok(None) => format!(
            "a row with backup id {} exists under another account, another table or another \
             spelling of the ARN, or an earlier release's in-catalog backup holds this ARN; it \
             is left as it is",
            row_arn.backup_id()
        ),
        Err(e) => format!("a row exists for it (its re-read failed: {e}); it is left as it is"),
    }
}

/// The report's sentence for a row the rows read at the start did not
/// return: one of another store, or one of this store that landed after
/// the read (a CreateBackup, say) or whose list entries a write did not
/// complete (a CreateBackup's claim on Cassandra). The row itself says
/// which store; the two cases of this store read the same, since telling
/// them apart would list every row of the row's status for each such
/// prefix.
fn describe_row(target: SyncTarget<'_>, row: &BackupRow) -> String {
    if row.store != *target.identity {
        return format!(
            "a row of another store holds it ({}; {} {}, store id {}); it is left as it is",
            row.status, row.store.store_kind, row.store.store_root, row.store.store_id
        );
    }
    format!(
        "a row for it ({}) exists that the catalog's read at the start did not return (it \
         landed after the read, or its list entries were never written); it is not a row this \
         sync's insert writes, so it is left as it is",
        row.status
    )
}

/// What [`prune_missing`] did with one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PruneOutcome {
    /// The row is gone; nothing in the store was deleted.
    Pruned,
    /// The manifest is under the prefix again; the row was left.
    ManifestBack,
    /// A restore target registered from the backup is CREATING (its name);
    /// the row was left.
    InUse(String),
    /// The row was not AVAILABLE any more (its status); left as it is.
    Status(BackupStatus),
    /// The row was gone.
    Gone,
    /// A store or catalog call failed (why); the row was left.
    Failed(String),
}

impl std::fmt::Display for PruneOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pruned => f.write_str("removed: the row is gone, and no object was deleted"),
            Self::ManifestBack => f.write_str("left: the manifest is under its prefix again"),
            Self::InUse(table) => write!(
                f,
                "left: the restore target {table} is being restored from it"
            ),
            Self::Status(status) => write!(f, "left: the row is {status} now"),
            Self::Gone => f.write_str("left: the row is gone"),
            Self::Failed(why) => write!(f, "left: {why}"),
        }
    }
}

impl PruneOutcome {
    /// Whether the row is still AVAILABLE with its manifest gone and a later
    /// run may remove it: a store or catalog call failed
    /// ([`Self::Failed`]), or a restore is reading the backup
    /// ([`Self::InUse`]). The command exits 1 when any row ends so.
    /// [`Self::ManifestBack`], [`Self::Status`] and [`Self::Gone`] leave
    /// nothing for a later run, and the command exits 0 for them.
    #[must_use]
    pub fn needs_another_run(&self) -> bool {
        matches!(self, Self::InUse(_) | Self::Failed(_))
    }
}

/// `--prune-missing`: for each AVAILABLE row whose manifest the listing did
/// not hold, `head` the manifest once more, then remove the row outright
/// ([`BackupCatalog::forget_available_backup_row`], which refuses a backup a
/// restore is reading and a row another writer moved). A `head` that fails
/// leaves the row ([`PruneOutcome::Failed`]): the store has not said whether
/// the manifest is back, so the row is kept and a later run decides.
/// Nothing in the store is deleted (module doc). The caller holds the
/// maintenance claim (`claim`), re-checked with a beat right before each
/// removal: a claim found lost there ends the loop with the reason, that
/// row left as it is. `stop`
/// is asked before each row and returns why to stop (the claim lost, a
/// signal), which ends the loop with the reason. Each removal is logged
/// under the `extenddb::audit` target, with `store_root` the configured
/// store's root.
pub async fn prune_missing<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: &dyn BackupStore,
    store_root: &str,
    rows: &[BackupKey],
    claim: &HeldClaim,
    stop: &dyn Fn() -> Option<String>,
) -> (Vec<(String, PruneOutcome)>, Option<String>) {
    let mut done = Vec::new();
    for row in rows {
        if let Some(reason) = stop() {
            return (done, Some(reason));
        }
        let outcome = match prune_one(storage, store, row, claim).await {
            Ok(outcome) => outcome,
            Err(reason) => return (done, Some(reason)),
        };
        if outcome == PruneOutcome::Pruned {
            tracing::info!(
                target: "extenddb::audit",
                action = "backup sync --prune-missing",
                account_id = %row.account_id,
                backup_arn = %row.backup_arn,
                store_root = %store_root,
                "removed an AVAILABLE row whose manifest is not under the root; no object was \
                 deleted"
            );
        }
        done.push((row.backup_arn.clone(), outcome));
    }
    (done, None)
}

/// One row of [`prune_missing`]; `Err` is the claim found lost right
/// before the removal, which ends the loop.
async fn prune_one<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: &dyn BackupStore,
    row: &BackupKey,
    claim: &HeldClaim,
) -> Result<PruneOutcome, String> {
    let arn = match BackupArn::parse(&row.backup_arn) {
        Ok(arn) => arn,
        Err(e) => {
            return Ok(PruneOutcome::Failed(format!(
                "the row's ARN does not parse: {e}"
            )));
        }
    };
    match store
        .head(&format!("{}{EXTENDDB_MANIFEST_FILE}", arn.store_prefix()))
        .await
    {
        Ok(Some(_)) => return Ok(PruneOutcome::ManifestBack),
        Ok(None) => {}
        Err(e) => {
            return Ok(PruneOutcome::Failed(format!(
                "the manifest's head failed: {e}"
            )));
        }
    }
    claim
        .recheck()
        .await
        .map_err(|reason| format!("{reason} (before the removal of {})", row.backup_arn))?;
    Ok(match storage.forget_available_backup_row(row).await {
        Ok(ForgetRow::Removed) => PruneOutcome::Pruned,
        Ok(ForgetRow::InUse(table)) => PruneOutcome::InUse(table),
        Ok(ForgetRow::Status(status)) => PruneOutcome::Status(status),
        Err(e) if is_backup_not_found(&e) => PruneOutcome::Gone,
        Err(e) => PruneOutcome::Failed(format!("the row could not be removed: {e}")),
    })
}

/// Hands a taken startup sync back when the run ends without listing the
/// whole root, or with a prefix a later run may adopt; dropped with its
/// future included.
struct Rearm<'a> {
    store: &'a super::ConfiguredStore,
    done: bool,
}

impl Drop for Rearm<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.store.retry_startup_sync();
        }
    }
}

/// The startup sync of a server whose catalog is an in-memory SQLite
/// database (module doc): once, when the configured store is healthy and
/// the sync is pending ([`super::ConfiguredStore::with_ephemeral_catalog`]),
/// bounded by [`STARTUP_SYNC_TIMEOUT`]. `None` when nothing ran (no store,
/// not pending, not healthy, or another caller is running it) and when a
/// run reached the bound (logged; the sync stays pending). A run that
/// stops before it listed the whole root (a listing or catalog failure, the
/// bound, or its future dropped at a shutdown), or that skipped a prefix for
/// a store or catalog error, leaves the sync pending for the next
/// reconciler cycle. The server calls it before it serves (the filesystem
/// store, whose check ran then) and every reconciler cycle calls it after
/// the store's check (the S3 store, whose first check runs there, and a sync
/// left pending).
pub async fn startup_sync<S: BackupCatalog + ?Sized>(
    storage: &S,
    runtime: &BackupRuntime,
) -> Option<Result<SyncReport, SyncError>> {
    let store = runtime.configured_store()?;
    let identity = store.identity().filter(|_| store.is_healthy())?;
    // The take is the one gate: of two callers that find the sync pending,
    // one runs it.
    if !store.take_startup_sync() {
        return None;
    }
    // Until a run has reported (listed the whole root and logged its line
    // of counts), the lasting skips are logged at INFO, so their reasons
    // reach the log when the first run was cut at its bound or ended in an
    // error.
    let first_report = !store.startup_sync_reported();
    let mut rearm = Rearm { store, done: false };
    // The store's calls as the checks run them (module doc): the listing's
    // walk under the run's bound, in the root's call slot, marked in flight
    // once abandoned.
    let via = store.calls_within("startup sync's store call", STARTUP_SYNC_TIMEOUT);
    let run = sync(
        storage,
        SyncTarget {
            store: &via,
            identity: &identity,
            metrics: runtime.metrics().map(|m| &**m),
            region: runtime.region(),
            stop: None,
            claim: None,
        },
    );
    // A store call abandoned at its bound or dropped with the run (a
    // filesystem call, whose thread still runs) is marked in flight under
    // its caller's name: the run's own call, or another caller's (a
    // deletion's, whose call held the slot the run waited for). Until it
    // returns, each cycle's check is refused and the store is unreachable,
    // so the sync runs again on the first cycle after it; the clause names
    // the caller as the refusal does. A run cut in a catalog call, or after
    // its last store call returned, marks nothing, and with no other
    // caller's call in flight the next cycle's check runs as usual.
    let in_flight = || {
        store
            .call_in_flight_caller()
            .map_or_else(String::new, |caller| {
                format!(
                    " (once the abandoned {caller} has returned; until then the store is \
                     unreachable)"
                )
            })
    };
    let Ok(outcome) = tokio::time::timeout(STARTUP_SYNC_TIMEOUT, run).await else {
        tracing::warn!(
            "backup store ({}) startup sync of the in-memory catalog did not finish within {} \
             s; the backups it adopted keep their rows, and a later reconciler cycle runs it \
             again from the first key of the root{}",
            store.kind,
            STARTUP_SYNC_TIMEOUT.as_secs(),
            in_flight()
        );
        return None;
    };
    match &outcome {
        Ok(report) => {
            let transient = report.transient_skips();
            rearm.done = transient == 0;
            let mut reserved = 0usize;
            for skipped in &report.skipped {
                if matches!(skipped.reason, SkipReason::Reserved(_)) {
                    reserved += 1;
                } else if skipped.reason.is_transient() {
                    tracing::warn!(
                        "backup store startup sync: {} not adopted: {}; the next reconciler \
                         cycle tries it again",
                        skipped.prefix,
                        skipped.reason
                    );
                } else if first_report {
                    tracing::info!(
                        "backup store startup sync: {} not adopted: {}",
                        skipped.prefix,
                        skipped.reason
                    );
                } else {
                    // An earlier run reported and named the prefix; the
                    // line of counts below says how many.
                    tracing::debug!(
                        "backup store startup sync: {} not adopted: {}",
                        skipped.prefix,
                        skipped.reason
                    );
                }
            }
            tracing::info!(
                "backup store ({}) startup sync of the in-memory catalog: {} backup(s) adopted, \
                 {} prefix(es) skipped ({reserved} of them reserved for exports or the \
                 change-log archive){}",
                store.kind,
                report.adopted.len(),
                report.skipped.len(),
                if transient == 0 {
                    String::new()
                } else {
                    format!(
                        "; {transient} prefix(es) met a store or catalog error, and the next \
                         reconciler cycle runs the sync again"
                    )
                }
            );
            store.record_startup_sync_report();
        }
        Err(e) => tracing::warn!(
            "backup store ({}) startup sync of the in-memory catalog stopped: {e}; a later \
             reconciler cycle runs it again{}",
            store.kind,
            in_flight()
        ),
    }
    Some(outcome)
}
