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
//! request could address the row. Another region is adopted and logged:
//! requests find the row by its ARN as written. Before the insert the row is
//! read under each spelling of the ARN (this server's region and the
//! manifest's), and a backup the other spelling has a row for is skipped:
//! Cassandra keys its rows by the ARN and has no unique backup id, so there
//! the read is what keeps a second row of the backup id out of the catalog
//! under the manifest's account, table and id. A row of the same account
//! and id under another table that the rows read did not return (a row of
//! another root, or one whose list entries are missing) is not seen by it;
//! within one run, the row check keeps a second prefix of an id this run
//! adopted out. A row under the manifest's own spelling is left to the
//! insert. A
//! verified prefix gets an AVAILABLE row through
//! [`BackupCatalog::insert_backup_row`], unowned, in the configured store's
//! identity, with the manifest's name, ARN, size, item total and
//! `source_table`, and the id's instant as `created_at`. An insert that
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
//! trait's doc). A failed
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
//! read and its insert. A row it inserts is AVAILABLE and unowned
//! (`owner_generation` 0); no reconciler duty reads an AVAILABLE row's
//! generation, so the row is handled as a created one.
//!
//! [`startup_sync`] is the sync a server whose catalog is an in-memory
//! SQLite database runs itself once its store is healthy: the catalog is
//! new at every start, so it is how such a server serves the backups of its
//! previous starts. The check that made the store healthy adopted a root an
//! earlier in-memory catalog owned (`store_identity`'s module doc). Each run
//! is bounded by [`STARTUP_SYNC_TIMEOUT`]; one that does not finish, or
//! that skipped a prefix for a store or catalog error, is left pending and
//! runs again in the next reconciler cycle. Every run lists from the first
//! key, so the startup sync completes only while listing the whole root
//! fits in the bound; past that it stays pending and is retried every
//! cycle, and the backups beyond what a run reaches stay unserved. Until a
//! run finishes, requests
//! for the earlier backups answer `BackupNotFoundException`; the log line
//! "startup sync of the in-memory catalog: N backup(s) adopted" ends that
//! window.

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
use super::reconciler::{StoreCalls, Verified, manifest_item_count, verify_prefix};

/// The bound of one run of [`startup_sync`], before the server serves and in
/// each reconciler cycle. A run abandoned at the bound keeps the rows it
/// inserted, and the next run skips those prefixes on the row check, but
/// every run lists the root from its first key: a root whose listing alone
/// takes longer than the bound is never synced, and the sync stays pending
/// and is retried every cycle (on a filesystem store the whole listing is
/// built before the first key, so no prefix is reached). On a filesystem
/// store an abandoned run's listing is not stopped: the walk runs to its end
/// on the blocking pool, holding the listing it builds, and the next
/// cycle's run starts another walk, so on a root whose walk outlasts the
/// cycle period the walks overlap.
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
    /// The server's region: an adopted ARN's region is compared with it
    /// (logged when they differ), and the row is looked up under the ARN
    /// rebuilt in it before the insert. `None` does neither.
    pub region: Option<&'a str>,
    /// Asked before each prefix; `None` never stops.
    pub stop: Option<Stop<'a>>,
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
    /// The ARN, as the manifest spells it.
    pub backup_arn: String,
    /// Set when the ARN's region is not the server's: the sentence the log
    /// carries.
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
        Err(reason) => report.skipped.push(SkippedPrefix { prefix, reason }),
    }
    None
}

/// Verify one prefix with no row and insert its row.
async fn adopt_prefix<S: BackupCatalog + ?Sized>(
    storage: &S,
    calls: StoreCalls<'_>,
    target: SyncTarget<'_>,
    prefix: &str,
    (account_id, table_name, backup_id): (&str, &str, &str),
    objects: &HashMap<String, u64>,
) -> Result<AdoptedBackup, SkipReason> {
    let manifest = match verify_prefix(calls, prefix, Some(objects)).await {
        Ok(Verified::Complete(manifest)) => manifest,
        Ok(Verified::Incomplete(why)) => return Err(SkipReason::Incomplete(why)),
        Err(e) => return Err(SkipReason::Store(e.to_string())),
    };
    let arn = manifest
        .backup_arn()
        .map_err(|e| SkipReason::ArnMismatch(format!("the manifest's ARN is refused: {e}")))?;
    if arn.account_id() != account_id
        || arn.table_name() != table_name
        || arn.backup_id() != backup_id
    {
        tracing::warn!(
            "backup sync: {prefix} holds a manifest naming {}, whose account, table and backup \
             id are not the prefix's; not adopted",
            arn.to_arn_string()
        );
        return Err(SkipReason::ArnMismatch(format!(
            "the manifest names {}, whose account, table and backup id are not the prefix's",
            arn.to_arn_string()
        )));
    }
    let backup_arn = arn.to_arn_string();
    if arn.partition() != ARN_PARTITION {
        tracing::warn!(
            "backup sync: {prefix} holds a manifest naming {backup_arn}, in partition {}; not \
             adopted, since every request refuses an ARN outside partition {ARN_PARTITION}",
            arn.partition()
        );
        return Err(SkipReason::OtherPartition(format!(
            "the manifest names {backup_arn}, in partition {}, and no request to this server \
             can address an ARN outside partition {ARN_PARTITION}",
            arn.partition()
        )));
    }
    let created_at = backup_creation_date_time(backup_id).ok_or_else(|| {
        SkipReason::NotABackup(format!("backup id {backup_id:?} carries no instant"))
    })?;
    // A row under the other spelling of the ARN: on Cassandra the insert
    // decides per spelling only (the trait's doc). A row under the
    // manifest's own spelling is left to the insert, which answers `false`
    // for it, or, on Cassandra, completes it when it is an earlier insert
    // of this same row that stopped before its list and status entries.
    match row_under_a_spelling(storage, &arn, target.region).await {
        Ok(Some((spelling, row))) if spelling != arn => {
            return Err(SkipReason::RowExists(
                describe_row(storage, target, &row).await,
            ));
        }
        Ok(_) => {}
        Err(e) => return Err(SkipReason::Catalog(e.to_string())),
    }
    let source_table_details_json = serde_json::to_value(&manifest.source_table)
        .map_err(|e| SkipReason::Incomplete(format!("the source table does not serialize: {e}")))?;
    let row = BackupRow {
        backup_id: backup_id.to_owned(),
        account_id: account_id.to_owned(),
        table_name: table_name.to_owned(),
        backup_arn: backup_arn.clone(),
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
    match storage.insert_backup_row(&row).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(SkipReason::RowExists(
                existing_row(storage, target, &arn).await,
            ));
        }
        Err(e) => return Err(SkipReason::Catalog(e.to_string())),
    }
    let foreign = foreign_note(&arn, target.region);
    match &foreign {
        Some(note) => tracing::warn!(
            "backup sync: adopted {backup_arn} (account {account_id}), {note}; requests find it \
             by its ARN as written"
        ),
        None => tracing::info!("backup sync: adopted {backup_arn} (account {account_id})"),
    }
    Ok(AdoptedBackup {
        backup_arn,
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

/// The sentence for an ARN in another region than the server's, or `None`.
fn foreign_note(arn: &BackupArn, region: Option<&str>) -> Option<String> {
    region
        .filter(|region| arn.region() != *region)
        .map(|_| format!("whose region {} is not this server's", arn.region()))
}

/// What holds the backup id when the insert answered `false`: re-read so
/// the report says which row it met.
async fn existing_row<S: BackupCatalog + ?Sized>(
    storage: &S,
    target: SyncTarget<'_>,
    arn: &BackupArn,
) -> String {
    match storage.read_backup_row(arn).await {
        Ok(Some(row)) => describe_row(storage, target, &row).await,
        Ok(None) => format!(
            "a row with backup id {} exists under another account, another table or another \
             spelling of the ARN, or an earlier release's in-catalog backup holds this ARN; it \
             is left as it is",
            arn.backup_id()
        ),
        Err(e) => format!("a row exists for it (its re-read failed: {e}); it is left as it is"),
    }
}

/// The report's sentence for a row the row check did not see.
async fn describe_row<S: BackupCatalog + ?Sized>(
    storage: &S,
    target: SyncTarget<'_>,
    row: &BackupRow,
) -> String {
    if row.store != *target.identity {
        return format!(
            "a row of another store holds it ({}; {} {}, store id {}); it is left as it is",
            row.status, row.store.store_kind, row.store.store_root, row.store.store_id
        );
    }
    let listed = storage
        .list_backup_rows(Some(row.status), target.identity)
        .try_any(|listed| std::future::ready(listed.backup_arn == row.backup_arn))
        .await;
    match listed {
        Ok(true) => format!(
            "a row for it ({}) landed after the catalog was read (a CreateBackup, say); it is \
             left as it is",
            row.status
        ),
        Ok(false) => format!(
            "the catalog holds a row for it ({}) that the catalog's listings do not return (a \
             write that ended before the row's list entries, such as a CreateBackup's claim); \
             it is not a row this sync's insert writes, so it is left as it is",
            row.status
        ),
        Err(e) => format!(
            "a row for it ({}) exists that the catalog's read did not return (its listing \
             failed: {e}); it is left as it is",
            row.status
        ),
    }
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
/// maintenance claim; `stop`
/// is asked before each row and returns why to stop (the claim lost, a
/// signal), which ends the loop with the reason. Each removal is logged
/// under the `extenddb::audit` target, with `store_root` the configured
/// store's root.
pub async fn prune_missing<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: &dyn BackupStore,
    store_root: &str,
    rows: &[BackupKey],
    stop: &dyn Fn() -> Option<String>,
) -> (Vec<(String, PruneOutcome)>, Option<String>) {
    let mut done = Vec::new();
    for row in rows {
        if let Some(reason) = stop() {
            return (done, Some(reason));
        }
        let outcome = prune_one(storage, store, row).await;
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

async fn prune_one<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: &dyn BackupStore,
    row: &BackupKey,
) -> PruneOutcome {
    let arn = match BackupArn::parse(&row.backup_arn) {
        Ok(arn) => arn,
        Err(e) => return PruneOutcome::Failed(format!("the row's ARN does not parse: {e}")),
    };
    match store
        .head(&format!("{}{EXTENDDB_MANIFEST_FILE}", arn.store_prefix()))
        .await
    {
        Ok(Some(_)) => return PruneOutcome::ManifestBack,
        Ok(None) => {}
        Err(e) => return PruneOutcome::Failed(format!("the manifest's head failed: {e}")),
    }
    match storage.forget_available_backup_row(row).await {
        Ok(ForgetRow::Removed) => PruneOutcome::Pruned,
        Ok(ForgetRow::InUse(table)) => PruneOutcome::InUse(table),
        Ok(ForgetRow::Status(status)) => PruneOutcome::Status(status),
        Err(e) if is_backup_not_found(&e) => PruneOutcome::Gone,
        Err(e) => PruneOutcome::Failed(format!("the row could not be removed: {e}")),
    }
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
    if !store.startup_sync_pending() {
        return None;
    }
    let identity = store.identity().filter(|_| store.is_healthy())?;
    if !store.take_startup_sync() {
        return None;
    }
    let mut rearm = Rearm { store, done: false };
    let run = sync(
        storage,
        SyncTarget {
            store: &*store.store,
            identity: &identity,
            metrics: runtime.metrics().map(|m| &**m),
            region: runtime.region(),
            stop: None,
        },
    );
    let Ok(outcome) = tokio::time::timeout(STARTUP_SYNC_TIMEOUT, run).await else {
        tracing::warn!(
            "backup store ({}) startup sync of the in-memory catalog did not finish within {} \
             s; the backups it adopted keep their rows, and the next reconciler cycle runs it \
             again from the first key of the root",
            store.kind,
            STARTUP_SYNC_TIMEOUT.as_secs()
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
                } else {
                    tracing::info!(
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
        }
        Err(e) => tracing::warn!(
            "backup store ({}) startup sync of the in-memory catalog stopped: {e}; the next \
             reconciler cycle runs it again",
            store.kind
        ),
    }
    Some(outcome)
}
