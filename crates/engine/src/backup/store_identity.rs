// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The store identity file and the checks that decide a configured store's
//! health.
//!
//! A store's root carries `extenddb-store.json` ([`STORE_IDENTITY_KEY`]),
//! holding a random store id and the `catalog_id` of the catalog that owns
//! the root; every backup row records the store id in `store_id`. Server
//! startup ([`establish`]) and every reconciler cycle ([`recheck`]) read the
//! file and check it against the catalog. The store is unhealthy when:
//!
//! - the file names another catalog (a root another deployment owns, or a
//!   copy of one; an operator adopts it on purpose, never a server by
//!   accident);
//! - its store id differs from the `store_id` of the catalog's rows for the
//!   configured kind and root (the root was replaced under the rows);
//! - the catalog has rows for the root and the root has no file (the root
//!   was emptied or re-pointed);
//! - those rows carry more than one `store_id`;
//! - the manifest probe fails: the newest AVAILABLE row's
//!   `extenddb-manifest.json` is not under the root. Instances that share a
//!   catalog must share one store root; host-local roots at one path give
//!   each instance different bytes under one `store_root`, which the ids
//!   alone cannot see, so the probe reads the root independently of the
//!   host;
//! - the store cannot be reached or read, or the file cannot be parsed.
//!
//! On a root with no file and a catalog with no rows for it, the server
//! creates the file with `put_if_absent`, then re-reads it and uses the id
//! it holds. Of two instances of one catalog starting against one empty root,
//! one creates the file and the other's create finds it present, and both
//! use the winner's id. An S3-compatible server that ignores `If-None-Match`
//! makes the create a plain put, last writer wins: the re-read catches a race
//! lost before it (the file holds another id than this instance wrote) and
//! marks the store unhealthy naming both ids; a race lost after it is caught
//! by the next check, which finds rows carrying an id the file does not.
//!
//! An unhealthy store is not a startup refusal: the caller logs the problem,
//! reports it on `/health`, refuses the operations that touch the store, and
//! runs the check again each cycle until it passes.
//!
//! A root whose file names another catalog changes owner only by
//! [`adopt`]: `extenddb backup sync --adopt-store`, under the maintenance
//! claim, for a root any other catalog owns; and a server whose catalog is
//! an in-memory SQLite database, for a root whose file another in-memory
//! catalog wrote. Such a catalog, and with it its `catalog_id`, is new at
//! every start, so the file it writes carries `ephemeral_catalog: true`
//! ([`StoreIdentityFile::ephemeral_catalog`]), and [`establish`] in such a
//! server adopts a root whose file carries the marker (the previous start's
//! catalog, or another in-memory server's), and only such a root: one a
//! durable catalog owns stays unhealthy there as anywhere. [`recheck`],
//! which runs once the identity is known, never adopts, so two in-memory
//! servers pointed at one root do not take it back and forth: the later
//! start owns it and the earlier finds it owned by another catalog. Until
//! that check, the previous owner acts on the root as its own, except that
//! its deletion fence (`super::StoreFence`) re-reads the file before each
//! deletion from the store and before a CreateBackup's first put, refuses
//! when the file names another catalog, and marks the store unhealthy; a
//! write already past the fence's read still lands. So every server of the
//! catalog a file names is to be stopped before `--adopt-store` takes its
//! root, and a new in-memory server started only after the old one has
//! exited. The
//! rewrite is a plain `put` (the store trait has no conditional replace),
//! so two adopters race and the last writer wins. Each re-reads the file
//! after its put, which detects another adopter's put that landed between
//! the two, and `backup sync --adopt-store` reads the file once more after
//! its sync, which detects one that landed during the sync; a put that
//! lands after both reads is found by the loser's next check, which finds
//! the store owned by another catalog. The audit line is written after the
//! re-read with what it found, and an intent line before the put, so a put
//! whose caller stopped waiting for it is on record too.

use extenddb_backup_store::{BackupStore, StoreError, byte_stream_from};
use extenddb_core::backup_format::{BackupArn, EXTENDDB_MANIFEST_FILE};
use extenddb_storage::BackupCatalog;
use extenddb_storage::backup_catalog::{StoreIdentity, StoreKind};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use super::maintenance::HeldClaim;

/// The key of the identity file, at the root, under no `<account>/` prefix.
pub const STORE_IDENTITY_KEY: &str = "extenddb-store.json";

/// The one version of the file's shape this build writes and reads.
pub const STORE_IDENTITY_FORMAT_VERSION: u32 = 1;

/// The most bytes a check reads of `extenddb-store.json`. The file this
/// build writes is under 300 bytes; an object larger than this under the
/// key is not an identity file, and is reported as unreadable without being
/// read into memory on every cycle.
pub const STORE_IDENTITY_MAX_BYTES: usize = 64 * 1024;

/// The contents of `extenddb-store.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreIdentityFile {
    pub format_version: u32,
    /// The random id every row of this store carries.
    pub store_id: String,
    /// The catalog that owns the root.
    pub catalog_id: String,
    /// The storage backend of the owning catalog, for the operator reading
    /// the file; nothing is checked against it.
    pub backend: String,
    /// When the file was created, epoch milliseconds on the creating host.
    pub created_at_ms: u64,
    /// Whether the owning catalog is an in-memory SQLite database, new at
    /// every start of its server (module doc). Absent in a file an earlier
    /// build wrote, which no in-memory catalog of that build could adopt
    /// again, so it reads as `false`.
    #[serde(default)]
    pub ephemeral_catalog: bool,
}

impl StoreIdentityFile {
    /// A new identity for `catalog_id`.
    fn new(catalog_id: &str, backend: &str, ephemeral_catalog: bool) -> Self {
        Self {
            format_version: STORE_IDENTITY_FORMAT_VERSION,
            store_id: uuid::Uuid::new_v4().to_string(),
            catalog_id: catalog_id.to_owned(),
            backend: backend.to_owned(),
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            ephemeral_catalog,
        }
    }

    /// Parse the file's bytes.
    ///
    /// # Errors
    ///
    /// [`StoreProblem::FileUnreadable`] for bytes that are not the documented
    /// shape, or carry a version this build does not read.
    pub fn parse(bytes: &[u8]) -> Result<Self, StoreProblem> {
        let file: Self = serde_json::from_slice(bytes)
            .map_err(|e| StoreProblem::FileUnreadable(format!("not a store identity file: {e}")))?;
        if file.format_version != STORE_IDENTITY_FORMAT_VERSION {
            return Err(StoreProblem::FileUnreadable(format!(
                "format_version {} is not {STORE_IDENTITY_FORMAT_VERSION}",
                file.format_version
            )));
        }
        if file.store_id.is_empty() || file.catalog_id.is_empty() {
            return Err(StoreProblem::FileUnreadable(
                "store_id and catalog_id must be non-empty".to_owned(),
            ));
        }
        Ok(file)
    }

    /// The file's bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("a plain struct serializes");
        bytes.push(b'\n');
        bytes
    }
}

/// Why a store is unhealthy. `Display` is the operator's line (ids, the
/// catalog id, the store's own error text, never a credential);
/// [`StoreProblem::public_reason`] is the part of it that names no root, and
/// [`StoreProblem::category`] alone is what `/health` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreProblem {
    /// The store could not be reached, read or written; the store's error.
    Transport(String),
    /// The store refused an operation: a root or a directory under it that
    /// another user can reach, a denied grant; the store's error.
    Denied(String),
    /// The store's `validate`, or a request of the identity check, failed for
    /// a reason that is neither a transport failure nor a denial (a bucket
    /// that does not exist, an I/O error on a filesystem root); the store's
    /// error. The configuration errors `validate` reports before the
    /// listener stop startup instead. One category whether `validate` or a
    /// later check's request found the condition, so a bucket deleted under
    /// a running server reads as it does at startup.
    Validate(String),
    /// `extenddb-store.json` is there but is not a readable identity file.
    FileUnreadable(String),
    /// The file names a catalog other than this one.
    ForeignCatalog {
        file_catalog_id: String,
        catalog_id: String,
    },
    /// The file's id is not the id the catalog's rows for the root carry.
    IdMismatch {
        file_id: String,
        row_ids: Vec<String>,
    },
    /// The catalog's rows for the root carry more than one id.
    MultipleRowIds(Vec<String>),
    /// The catalog has rows for the root and the root has no file.
    FileMissingWithRows(Vec<String>),
    /// This instance's create reported success and the re-read holds another
    /// id: the store did not honour the conditional put.
    CreateRaceLost { ours: String, file: String },
    /// The newest AVAILABLE row's manifest is not under the root.
    ManifestMissing { backup_arn: String },
    /// The catalog could not be read.
    Catalog(String),
}

impl StoreProblem {
    /// The problem's category, one or two words, for an API refusal.
    #[must_use]
    pub fn category(&self) -> &'static str {
        match self {
            Self::Transport(_) => "unreachable",
            Self::Denied(_) => "permission denied",
            Self::Validate(_) => "failed validation",
            Self::FileUnreadable(_) => "identity file unreadable",
            Self::ForeignCatalog { .. } => "owned by another catalog",
            Self::IdMismatch { .. } | Self::MultipleRowIds(_) | Self::CreateRaceLost { .. } => {
                "identity mismatch"
            }
            Self::FileMissingWithRows(_) => "identity file missing",
            Self::ManifestMissing { .. } => "manifest probe failed",
            Self::Catalog(_) => "catalog unreadable",
        }
    }

    /// The problem with the ids involved and never a root (a filesystem path
    /// or a bucket) or the store's own error text, which may carry one: the
    /// operator's line for the identity problems. Not the `/health` text,
    /// which is [`StoreProblem::category`].
    #[must_use]
    pub fn public_reason(&self) -> String {
        match self {
            Self::Transport(_) => "the store could not be reached".to_owned(),
            Self::Denied(_) => "the store refused an operation (permission denied)".to_owned(),
            Self::Validate(_) => "the store failed its validation".to_owned(),
            Self::FileUnreadable(_) => format!("{STORE_IDENTITY_KEY} could not be read"),
            Self::ForeignCatalog {
                file_catalog_id,
                catalog_id,
            } => format!(
                "{STORE_IDENTITY_KEY} names catalog {file_catalog_id}; this catalog is \
                 {catalog_id}"
            ),
            Self::IdMismatch { file_id, row_ids } => format!(
                "{STORE_IDENTITY_KEY} holds store id {file_id}; the catalog's rows for the root \
                 carry {}",
                row_ids.join(", ")
            ),
            Self::MultipleRowIds(ids) => format!(
                "the catalog's rows for the root carry more than one store id: {}",
                ids.join(", ")
            ),
            Self::FileMissingWithRows(ids) => format!(
                "the root has no {STORE_IDENTITY_KEY} but the catalog has rows for it (store id \
                 {})",
                ids.join(", ")
            ),
            Self::CreateRaceLost { ours, file } => format!(
                "this server created {STORE_IDENTITY_KEY} with store id {ours} but the file \
                 holds {file}; the store did not honour the conditional put"
            ),
            Self::ManifestMissing { backup_arn } => {
                format!("the manifest of {backup_arn} is not under the root")
            }
            Self::Catalog(_) => "the catalog could not be read".to_owned(),
        }
    }
}

impl std::fmt::Display for StoreProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "the store could not be reached: {e}"),
            Self::Denied(e) => write!(f, "the store refused an operation: {e}"),
            Self::Validate(e) => write!(f, "the store failed its validation: {e}"),
            Self::FileUnreadable(e) => write!(f, "{STORE_IDENTITY_KEY} could not be read: {e}"),
            Self::Catalog(e) => write!(f, "the catalog could not be read: {e}"),
            other => f.write_str(&other.public_reason()),
        }
    }
}

/// The problem a store's error is: a transport failure keeps its class with
/// the store error's whole cause chain (the SDK's top link is a bare
/// category such as "dispatch failure"; the cause is a link below it), a
/// denial is [`StoreProblem::Denied`], and every other refusal
/// ([`StoreProblem::Validate`]'s doc) is the same category `validate` gives
/// it.
impl From<StoreError> for StoreProblem {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::PermissionDenied(m) => Self::Denied(m),
            transport @ StoreError::Transport { .. } => Self::Transport(transport.to_string()),
            other => Self::Validate(other.to_string()),
        }
    }
}

/// Read the identity file, or `None` when the root has none. At most
/// [`STORE_IDENTITY_MAX_BYTES`] are read; a larger object is
/// [`StoreProblem::FileUnreadable`].
///
/// # Errors
///
/// The store's error as a [`StoreProblem`], or a file that does not parse.
pub async fn read_identity_file(
    store: &dyn BackupStore,
) -> Result<Option<StoreIdentityFile>, StoreProblem> {
    read_file(store).await
}

/// [`read_identity_file`].
async fn read_file(store: &dyn BackupStore) -> Result<Option<StoreIdentityFile>, StoreProblem> {
    let mut body = match store.get(STORE_IDENTITY_KEY).await {
        Ok(body) => body,
        Err(StoreError::NotFound) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut bytes = Vec::new();
    while let Some(chunk) = body.try_next().await? {
        if bytes.len() + chunk.len() > STORE_IDENTITY_MAX_BYTES {
            return Err(StoreProblem::FileUnreadable(format!(
                "larger than {STORE_IDENTITY_MAX_BYTES} bytes"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    StoreIdentityFile::parse(&bytes).map(Some)
}

/// The distinct ids the catalog's rows for the root carry, at most one.
async fn row_ids<C: BackupCatalog + ?Sized>(
    catalog: &C,
    kind: StoreKind,
    root: &str,
) -> Result<Vec<String>, StoreProblem> {
    let ids = catalog
        .read_store_ids(kind, root)
        .await
        .map_err(|e| StoreProblem::Catalog(e.to_string()))?;
    if ids.len() > 1 {
        return Err(StoreProblem::MultipleRowIds(ids));
    }
    Ok(ids)
}

/// The startup check: `validate` has passed; read the file, create it when
/// the root is empty of both file and rows, compare it with the rows and
/// with `catalog_id`, then probe the newest manifest. Returns the store's
/// identity, the rows' and the file's. `backend` and `ephemeral_catalog`
/// describe this catalog in a file the check writes; a catalog with
/// `ephemeral_catalog` set (an in-memory SQLite database) also adopts a
/// root whose file another such catalog wrote (module doc,
/// [`AdoptRule::EphemeralOwner`]).
///
/// # Errors
///
/// The problem that makes the store unhealthy (module doc).
pub async fn establish<C: BackupCatalog + ?Sized>(
    store: &dyn BackupStore,
    kind: StoreKind,
    root: &str,
    catalog: &C,
    backend: &str,
    ephemeral_catalog: bool,
) -> Result<StoreIdentity, StoreProblem> {
    let catalog_id = catalog
        .catalog_id()
        .await
        .map_err(|e| StoreProblem::Catalog(e.to_string()))?;
    let row_ids = row_ids(catalog, kind, root).await?;
    let file = match read_file(store).await? {
        Some(file) => file,
        None if !row_ids.is_empty() => {
            return Err(StoreProblem::FileMissingWithRows(row_ids));
        }
        None => {
            let ours = StoreIdentityFile::new(&catalog_id, backend, ephemeral_catalog);
            let created = store
                .put_if_absent(STORE_IDENTITY_KEY, byte_stream_from(ours.to_bytes()))
                .await?
                .is_some();
            // The key holds nothing the store reads as an object and refuses
            // the create (a FIFO or a socket at the path, say): nothing was
            // created, and nothing can be read.
            let file = read_file(store).await?.ok_or_else(|| {
                StoreProblem::Transport(format!(
                    "{STORE_IDENTITY_KEY} is neither readable nor creatable"
                ))
            })?;
            if created && file.store_id != ours.store_id {
                return Err(StoreProblem::CreateRaceLost {
                    ours: ours.store_id,
                    file: file.store_id,
                });
            }
            if created {
                tracing::info!(
                    "backup store ({kind}) root was empty; created {STORE_IDENTITY_KEY} with \
                     store id {}",
                    file.store_id
                );
            } else if file.catalog_id == catalog_id {
                tracing::info!(
                    "backup store ({kind}) root already carried {STORE_IDENTITY_KEY} (store id \
                     {}), created by another instance of this catalog",
                    file.store_id
                );
            } else {
                tracing::info!(
                    "backup store ({kind}) root already carried {STORE_IDENTITY_KEY} (store id \
                     {}), created by catalog {} while this check created it",
                    file.store_id,
                    file.catalog_id
                );
            }
            file
        }
    };
    if ephemeral_catalog && file.catalog_id != catalog_id && file.ephemeral_catalog {
        return match adopt(
            store,
            kind,
            root,
            catalog,
            backend,
            ephemeral_catalog,
            AdoptRule::EphemeralOwner,
            None,
        )
        .await
        {
            Ok(Adoption::Adopted { identity, .. } | Adoption::AlreadyOwned(identity)) => {
                Ok(identity)
            }
            Err(refusal) => Err(refusal.into_problem(&catalog_id)),
        };
    }
    let identity = compare(&file, &catalog_id, row_ids, kind, root)?;
    manifest_probe(store, &identity, catalog).await?;
    Ok(identity)
}

/// A read of the store's identity that writes nothing: the file, compared
/// with the catalog as [`establish`] compares it, without the create of a
/// missing file and without the manifest probe. `Ok(None)` for a root with
/// no file and no rows (a fresh root). For a command that reads beside a
/// server (`backup list`), which must leave the root as it found it.
///
/// # Errors
///
/// The problem that would make the store unhealthy (module doc).
pub async fn read_identity<C: BackupCatalog + ?Sized>(
    store: &dyn BackupStore,
    kind: StoreKind,
    root: &str,
    catalog: &C,
) -> Result<Option<StoreIdentity>, StoreProblem> {
    let catalog_id = catalog
        .catalog_id()
        .await
        .map_err(|e| StoreProblem::Catalog(e.to_string()))?;
    let row_ids = row_ids(catalog, kind, root).await?;
    match read_file(store).await? {
        Some(file) => compare(&file, &catalog_id, row_ids, kind, root).map(Some),
        None if !row_ids.is_empty() => Err(StoreProblem::FileMissingWithRows(row_ids)),
        None => Ok(None),
    }
}

/// Which owners [`adopt`] takes a root from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdoptRule {
    /// Any other catalog: `extenddb backup sync --adopt-store`, an
    /// operator's decision.
    Command,
    /// Only a catalog whose file carries `ephemeral_catalog`: the startup of
    /// a server whose own catalog is in memory (module doc).
    EphemeralOwner,
}

impl AdoptRule {
    /// The `action` of the audit line an adoption writes.
    fn action(self) -> &'static str {
        match self {
            Self::Command => "backup sync --adopt-store",
            Self::EphemeralOwner => "startup adoption by an in-memory catalog",
        }
    }
}

/// What [`adopt`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Adoption {
    /// The file named `previous_catalog_id` and now names this catalog; the
    /// store passed the check that follows (`compare`, the manifest probe).
    Adopted {
        identity: StoreIdentity,
        previous_catalog_id: String,
    },
    /// The file already named this catalog: nothing was written, and the
    /// store passed the check as [`establish`] runs it.
    AlreadyOwned(StoreIdentity),
}

/// Why [`adopt`] did not leave the root this catalog's and healthy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptRefusal {
    /// The store could not be read or written, the file could not be
    /// parsed, the catalog could not be read, or (the file already this
    /// catalog's) the check found another problem.
    Problem(StoreProblem),
    /// The root has no identity file: there is no owner to take it from.
    /// `row_ids` are the store ids this catalog's rows for the root carry:
    /// with none, the root is a fresh one that the next check (plain sync's
    /// included) gives a file; with some, the root lost its file under the
    /// rows.
    NoFile { row_ids: Vec<String> },
    /// The rule is [`AdoptRule::EphemeralOwner`] and the file's catalog is
    /// not an in-memory one.
    DurableOwner { file_catalog_id: String },
    /// This catalog's rows for the root carry an id other than the file's:
    /// the adopted root would be unhealthy at once (an identity mismatch).
    RowsCarryOtherIds {
        file_id: String,
        row_ids: Vec<String>,
    },
    /// After the put, the file names another catalog or holds another store
    /// id: a concurrent adopter wrote after this one (last writer wins).
    RaceLost {
        file_catalog_id: String,
        file_store_id: String,
    },
    /// The root, once this catalog's, would fail the check the server runs
    /// on it (the manifest probe: this catalog's newest AVAILABLE row of
    /// the root has no manifest under it); nothing was written, and the
    /// file still names `file_catalog_id`.
    Unusable {
        file_catalog_id: String,
        problem: StoreProblem,
    },
    /// The maintenance claim the command holds was found lost right before
    /// the rewrite (the reason); nothing was written.
    ClaimLost(String),
}

impl AdoptRefusal {
    /// The store problem the refusal leaves, for a check that records one:
    /// a root another catalog still owns is [`StoreProblem::ForeignCatalog`].
    #[must_use]
    pub fn into_problem(self, catalog_id: &str) -> StoreProblem {
        match self {
            Self::Problem(problem) | Self::Unusable { problem, .. } => problem,
            Self::NoFile { row_ids } if !row_ids.is_empty() => {
                StoreProblem::FileMissingWithRows(row_ids)
            }
            Self::NoFile { .. } => StoreProblem::FileUnreadable(format!(
                "{STORE_IDENTITY_KEY} was removed while the root was being adopted; the next \
                 check creates it"
            )),
            Self::DurableOwner { file_catalog_id }
            | Self::RaceLost {
                file_catalog_id, ..
            } => StoreProblem::ForeignCatalog {
                file_catalog_id,
                catalog_id: catalog_id.to_owned(),
            },
            Self::RowsCarryOtherIds { file_id, row_ids } => {
                StoreProblem::IdMismatch { file_id, row_ids }
            }
            Self::ClaimLost(reason) => StoreProblem::Catalog(reason),
        }
    }
}

impl std::fmt::Display for AdoptRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Problem(problem) => write!(f, "{problem}"),
            Self::NoFile { row_ids } if row_ids.is_empty() => write!(
                f,
                "the root has no {STORE_IDENTITY_KEY}, so no catalog owns it and there is \
                 nothing to adopt; plain sync creates the file"
            ),
            Self::NoFile { row_ids } => write!(
                f,
                "the root has no {STORE_IDENTITY_KEY}, and this catalog's rows for it carry \
                 store id {}: the root lost its file under the rows, and there is no owner to \
                 adopt it from",
                row_ids.join(", ")
            ),
            Self::DurableOwner { file_catalog_id } => write!(
                f,
                "{STORE_IDENTITY_KEY} names catalog {file_catalog_id}, which is not an \
                 in-memory catalog"
            ),
            Self::RowsCarryOtherIds { file_id, row_ids } => write!(
                f,
                "{STORE_IDENTITY_KEY} holds store id {file_id}, and this catalog's rows for the \
                 root carry {}: adopting the root would leave it unhealthy (an identity \
                 mismatch); nothing was written",
                row_ids.join(", ")
            ),
            Self::RaceLost {
                file_catalog_id,
                file_store_id,
            } => write!(
                f,
                "another writer replaced {STORE_IDENTITY_KEY} after this catalog's write: it \
                 now names catalog {file_catalog_id} with store id {file_store_id}"
            ),
            Self::Unusable {
                file_catalog_id,
                problem,
            } => write!(
                f,
                "the root would be unhealthy once this catalog owns it ({problem}); nothing was \
                 written, and {STORE_IDENTITY_KEY} still names catalog {file_catalog_id}"
            ),
            Self::ClaimLost(reason) => write!(
                f,
                "stopped before the rewrite of {STORE_IDENTITY_KEY}: {reason}; nothing was \
                 written"
            ),
        }
    }
}

/// Make this catalog the root's owner: run the check [`establish`] runs on
/// the root as it will be once this catalog owns it (`compare` with this
/// catalog's rows, the manifest probe of its newest AVAILABLE row), then
/// rewrite `extenddb-store.json`'s `catalog_id` (and `backend` and
/// `ephemeral_catalog`, which describe the owner) with a plain `put`,
/// keeping `store_id` and `created_at_ms`, and re-read the file. The check
/// reads only this catalog's rows and the store, which the rewrite does not
/// change, so its answer before the rewrite is its answer after it, and a
/// root this catalog could not use is refused with nothing written
/// ([`AdoptRefusal::Unusable`]). `rule` says which owners it takes the
/// root from. The rewrite is logged under the `extenddb::audit` target with
/// the old and the new catalog id. `claim`, the maintenance claim
/// `--adopt-store` holds, is re-checked with a beat right before the
/// rewrite, which is skipped when the claim is found lost
/// ([`AdoptRefusal::ClaimLost`]); the startup adoption of an in-memory
/// catalog holds none and passes `None`.
///
/// A file that already names this catalog is not written
/// ([`Adoption::AlreadyOwned`]). Nothing is written when the root has no
/// file, when the rule refuses the owner, when this catalog's rows for the
/// root carry an id other than the file's, or when the check fails.
///
/// # Errors
///
/// [`AdoptRefusal`], each case with what was and was not written.
#[allow(clippy::too_many_arguments)]
pub async fn adopt<C: BackupCatalog + ?Sized>(
    store: &dyn BackupStore,
    kind: StoreKind,
    root: &str,
    catalog: &C,
    backend: &str,
    ephemeral_catalog: bool,
    rule: AdoptRule,
    claim: Option<&HeldClaim>,
) -> Result<Adoption, AdoptRefusal> {
    let catalog_id = catalog
        .catalog_id()
        .await
        .map_err(|e| AdoptRefusal::Problem(StoreProblem::Catalog(e.to_string())))?;
    let row_ids = catalog
        .read_store_ids(kind, root)
        .await
        .map_err(|e| AdoptRefusal::Problem(StoreProblem::Catalog(e.to_string())))?;
    let file = read_file(store)
        .await
        .map_err(|problem| AdoptRefusal::Problem(at_step("the first read of", problem)))?
        .ok_or_else(|| AdoptRefusal::NoFile {
            row_ids: row_ids.clone(),
        })?;
    if file.catalog_id == catalog_id {
        if row_ids.len() > 1 {
            return Err(AdoptRefusal::Problem(StoreProblem::MultipleRowIds(row_ids)));
        }
        let identity =
            compare(&file, &catalog_id, row_ids, kind, root).map_err(AdoptRefusal::Problem)?;
        manifest_probe(store, &identity, catalog)
            .await
            .map_err(|problem| AdoptRefusal::Problem(at_step("the manifest probe of", problem)))?;
        return Ok(Adoption::AlreadyOwned(identity));
    }
    if rule == AdoptRule::EphemeralOwner && !file.ephemeral_catalog {
        return Err(AdoptRefusal::DurableOwner {
            file_catalog_id: file.catalog_id,
        });
    }
    if row_ids.iter().any(|id| *id != file.store_id) {
        return Err(AdoptRefusal::RowsCarryOtherIds {
            file_id: file.store_id,
            row_ids,
        });
    }
    let ours = StoreIdentityFile {
        catalog_id: catalog_id.clone(),
        backend: backend.to_owned(),
        ephemeral_catalog,
        ..file.clone()
    };
    // The check of the root as this catalog's, before anything is written:
    // the file's id with this catalog's rows (which carry it, or there are
    // none), and the manifest of this catalog's newest AVAILABLE row.
    let unusable = |problem: StoreProblem| match problem {
        // The probe's `head` failed in transport: the store's problem, not
        // the root's.
        StoreProblem::Transport(_) => {
            AdoptRefusal::Problem(at_step("the manifest probe before the rewrite of", problem))
        }
        problem => AdoptRefusal::Unusable {
            file_catalog_id: file.catalog_id.clone(),
            problem,
        },
    };
    let identity = compare(&ours, &catalog_id, row_ids.clone(), kind, root).map_err(&unusable)?;
    manifest_probe(store, &identity, catalog)
        .await
        .map_err(&unusable)?;
    if let Some(claim) = claim {
        claim.recheck().await.map_err(AdoptRefusal::ClaimLost)?;
    }
    tracing::info!(
        target: "extenddb::audit",
        action = rule.action(),
        outcome = "rewriting",
        store_kind = %kind,
        store_root = %root,
        store_id = %file.store_id,
        previous_catalog_id = %file.catalog_id,
        catalog_id = %catalog_id,
        "rewriting {STORE_IDENTITY_KEY} to name this catalog"
    );
    store
        .put(STORE_IDENTITY_KEY, byte_stream_from(ours.to_bytes()))
        .await
        .map_err(|e| {
            AdoptRefusal::Problem(at_step(
                "the rewrite (whose outcome is unknown; the audit log's intent line says it was \
                 started) of",
                e.into(),
            ))
        })?;
    let reread = read_file(store)
        .await
        .map_err(|problem| {
            AdoptRefusal::Problem(at_step("the re-read after the rewrite of", problem))
        })?
        .ok_or_else(|| AdoptRefusal::NoFile {
            row_ids: row_ids.clone(),
        })?;
    let won = reread.catalog_id == catalog_id && reread.store_id == file.store_id;
    tracing::info!(
        target: "extenddb::audit",
        action = rule.action(),
        outcome = if won { "adopted" } else { "race lost" },
        store_kind = %kind,
        store_root = %root,
        store_id = %file.store_id,
        previous_catalog_id = %file.catalog_id,
        catalog_id = %catalog_id,
        file_catalog_id = %reread.catalog_id,
        "rewrote {STORE_IDENTITY_KEY}; its re-read names catalog {}",
        reread.catalog_id
    );
    if !won {
        return Err(AdoptRefusal::RaceLost {
            file_catalog_id: reread.catalog_id,
            file_store_id: reread.store_id,
        });
    }
    tracing::info!(
        "backup store ({kind}) {root}: adopted from catalog {} (store id {})",
        file.catalog_id,
        identity.store_id
    );
    Ok(Adoption::Adopted {
        identity,
        previous_catalog_id: file.catalog_id,
    })
}

/// [`adopt`] as `backup sync --adopt-store` runs it, on `configured`'s
/// bounded calls ([`super::ConfiguredStore::calls`]): each store call under
/// the store's check bound, so a store that stops answering ends the
/// adoption at the call that did not finish, the refusal naming the step
/// ([`at_step`]), while the catalog's reads and the claim's beat run beside
/// the bound and are not cut by it.
///
/// # Errors
///
/// As [`adopt`].
pub async fn adopt_configured<C: BackupCatalog + ?Sized>(
    configured: &super::ConfiguredStore,
    catalog: &C,
    backend: &str,
    claim: Option<&HeldClaim>,
) -> Result<Adoption, AdoptRefusal> {
    adopt(
        &configured.calls("adoption's store call"),
        configured.kind,
        &configured.root,
        catalog,
        backend,
        false,
        AdoptRule::Command,
        claim,
    )
    .await
}

/// A store problem of [`adopt`] with the step it stopped at, for a store
/// that stopped answering (the command's bounded calls end the call that
/// did not finish, and the refusal then names it): the transport failure's
/// text gains `step` and the file's name; every other problem names its
/// condition already.
fn at_step(step: &str, problem: StoreProblem) -> StoreProblem {
    match problem {
        StoreProblem::Transport(text) => {
            StoreProblem::Transport(format!("{step} {STORE_IDENTITY_KEY}: {text}"))
        }
        other => other,
    }
}

/// The file against the catalog: it must name this catalog, and the rows
/// for the root, when there are any, must carry its id.
fn compare(
    file: &StoreIdentityFile,
    catalog_id: &str,
    row_ids: Vec<String>,
    kind: StoreKind,
    root: &str,
) -> Result<StoreIdentity, StoreProblem> {
    if file.catalog_id != catalog_id {
        return Err(StoreProblem::ForeignCatalog {
            file_catalog_id: file.catalog_id.clone(),
            catalog_id: catalog_id.to_owned(),
        });
    }
    if !row_ids.is_empty() && row_ids[0] != file.store_id {
        return Err(StoreProblem::IdMismatch {
            file_id: file.store_id.clone(),
            row_ids,
        });
    }
    Ok(StoreIdentity {
        store_kind: kind,
        store_root: root.to_owned(),
        store_id: file.store_id.clone(),
    })
}

/// The check every reconciler cycle runs on a store whose identity startup
/// settled: the file is still there, names this catalog and holds the
/// identity's id; the rows for the root carry no other id; the newest
/// manifest is under the root. A root that lost its file while the catalog
/// has no rows for it gets the file back, written with the identity's id
/// (`put_if_absent`, so a file another instance wrote first wins and is then
/// compared like any other). It never adopts (module doc).
///
/// # Errors
///
/// The problem that makes the store unhealthy (module doc).
pub async fn recheck<C: BackupCatalog + ?Sized>(
    store: &dyn BackupStore,
    identity: &StoreIdentity,
    catalog: &C,
    backend: &str,
    ephemeral_catalog: bool,
) -> Result<(), StoreProblem> {
    let catalog_id = catalog
        .catalog_id()
        .await
        .map_err(|e| StoreProblem::Catalog(e.to_string()))?;
    let row_ids = row_ids(catalog, identity.store_kind, &identity.store_root).await?;
    let file = match read_file(store).await? {
        Some(file) => file,
        None if !row_ids.is_empty() => {
            return Err(StoreProblem::FileMissingWithRows(row_ids));
        }
        None => {
            let ours = StoreIdentityFile {
                store_id: identity.store_id.clone(),
                ..StoreIdentityFile::new(&catalog_id, backend, ephemeral_catalog)
            };
            store
                .put_if_absent(STORE_IDENTITY_KEY, byte_stream_from(ours.to_bytes()))
                .await?;
            tracing::warn!(
                "backup store ({}) root had lost {STORE_IDENTITY_KEY}; rewrote it with store id \
                 {}",
                identity.store_kind,
                identity.store_id
            );
            read_file(store).await?.ok_or_else(|| {
                StoreProblem::Transport(format!(
                    "{STORE_IDENTITY_KEY} is neither readable nor creatable"
                ))
            })?
        }
    };
    if file.catalog_id != catalog_id {
        return Err(StoreProblem::ForeignCatalog {
            file_catalog_id: file.catalog_id,
            catalog_id,
        });
    }
    if file.store_id != identity.store_id {
        // The file changed under a running server: the root was replaced.
        return Err(StoreProblem::IdMismatch {
            file_id: file.store_id,
            row_ids: vec![identity.store_id.clone()],
        });
    }
    if let Some(other) = row_ids.iter().find(|id| **id != identity.store_id) {
        return Err(StoreProblem::IdMismatch {
            file_id: file.store_id,
            row_ids: vec![other.clone()],
        });
    }
    manifest_probe(store, identity, catalog).await
}

/// `head` the manifest of the newest AVAILABLE row of the store; a root with
/// no AVAILABLE row passes (there is nothing to probe).
///
/// # Errors
///
/// [`StoreProblem::ManifestMissing`] when the manifest is absent,
/// [`StoreProblem::Transport`] when the store cannot be read,
/// [`StoreProblem::Catalog`] when the rows cannot be listed.
pub async fn manifest_probe<C: BackupCatalog + ?Sized>(
    store: &dyn BackupStore,
    identity: &StoreIdentity,
    catalog: &C,
) -> Result<(), StoreProblem> {
    // One row, not the listing: the probe runs each reconciler cycle on
    // every instance and in each command's identity check.
    let Some(newest) = catalog
        .newest_available_backup_row(identity)
        .await
        .map_err(|e| StoreProblem::Catalog(e.to_string()))?
    else {
        return Ok(());
    };
    let arn = BackupArn::parse(&newest.backup_arn).map_err(|e| {
        StoreProblem::Catalog(format!("row {} carries a bad ARN: {e}", newest.backup_arn))
    })?;
    let key = format!("{}{EXTENDDB_MANIFEST_FILE}", arn.store_prefix());
    match store.head(&key).await? {
        Some(_) => Ok(()),
        None => Err(StoreProblem::ManifestMissing {
            backup_arn: newest.backup_arn.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_file_round_trips_and_refuses_other_shapes() {
        let file = StoreIdentityFile::new("cat-1", "postgres", false);
        assert_eq!(file.format_version, 1);
        assert_eq!(file.catalog_id, "cat-1");
        assert_eq!(file.backend, "postgres");
        assert!(!file.store_id.is_empty());
        let parsed = StoreIdentityFile::parse(&file.to_bytes()).unwrap();
        assert_eq!(parsed, file);
        // The marker round-trips, and a file without it (an earlier build's)
        // reads as a durable catalog's.
        let memory = StoreIdentityFile::new("cat-2", "sqlite", true);
        assert!(
            StoreIdentityFile::parse(&memory.to_bytes())
                .unwrap()
                .ephemeral_catalog
        );
        let earlier = serde_json::json!({
            "format_version": 1, "store_id": "s", "catalog_id": "c", "backend": "sqlite",
            "created_at_ms": 0
        });
        assert!(
            !StoreIdentityFile::parse(earlier.to_string().as_bytes())
                .unwrap()
                .ephemeral_catalog
        );

        let v2 = serde_json::json!({
            "format_version": 2, "store_id": "s", "catalog_id": "c", "backend": "b",
            "created_at_ms": 0
        });
        let err = StoreIdentityFile::parse(v2.to_string().as_bytes()).unwrap_err();
        assert!(matches!(&err, StoreProblem::FileUnreadable(m) if m.contains("format_version 2")));
        let empty = serde_json::json!({
            "format_version": 1, "store_id": "", "catalog_id": "c", "backend": "b",
            "created_at_ms": 0
        });
        assert!(matches!(
            StoreIdentityFile::parse(empty.to_string().as_bytes()),
            Err(StoreProblem::FileUnreadable(_))
        ));
        assert!(matches!(
            StoreIdentityFile::parse(b"not json"),
            Err(StoreProblem::FileUnreadable(_))
        ));
    }

    /// `public_reason` names ids and the catalog, never a root or the store's
    /// own error text (which may carry a path or a bucket); `Display` carries
    /// that text, and `/health` carries the category alone.
    #[test]
    fn the_public_reason_names_no_root() {
        let transport = StoreProblem::Transport("connect /srv/backups: refused".to_owned());
        assert!(!transport.public_reason().contains("/srv"));
        assert!(transport.to_string().contains("/srv/backups"));
        assert_eq!(transport.category(), "unreachable");
        let foreign = StoreProblem::ForeignCatalog {
            file_catalog_id: "other".to_owned(),
            catalog_id: "mine".to_owned(),
        };
        assert!(foreign.public_reason().contains("other"));
        assert!(foreign.public_reason().contains("mine"));
        assert_eq!(foreign.category(), "owned by another catalog");
        let probe = StoreProblem::ManifestMissing {
            backup_arn: "arn:x".to_owned(),
        };
        assert!(probe.public_reason().contains("arn:x"));
        assert_eq!(probe.category(), "manifest probe failed");
        let unreadable = StoreProblem::FileUnreadable("at /srv/x".to_owned());
        assert!(!unreadable.public_reason().contains("/srv"));
        let denied: StoreProblem =
            StoreError::PermissionDenied("/srv/backups/a has mode 0755".to_owned()).into();
        assert_eq!(denied.category(), "permission denied");
        assert!(!denied.public_reason().contains("/srv"));
        assert!(denied.to_string().contains("/srv/backups/a"));
        // A transport failure keeps its whole cause chain: the SDK's top
        // link names only the class, and the cause is the link below it.
        let chain: StoreProblem = StoreError::Transport {
            source: Box::new(TwoLinks),
        }
        .into();
        assert_eq!(chain.category(), "unreachable");
        assert!(chain.to_string().contains("dispatch failure"), "{chain}");
        assert!(chain.to_string().contains("connection refused"), "{chain}");
        // Any other store refusal is the category `validate` gives it, so a
        // bucket that disappears under a running server reads as a bucket
        // that was missing at startup.
        let missing: StoreProblem = StoreError::Other("NoSuchBucket".to_owned()).into();
        assert_eq!(missing.category(), "failed validation");
        assert!(missing.to_string().contains("NoSuchBucket"));
    }

    /// A two-link error chain: "dispatch failure" over "connection refused".
    #[derive(Debug)]
    struct TwoLinks;

    impl std::fmt::Display for TwoLinks {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("dispatch failure")
        }
    }

    impl std::error::Error for TwoLinks {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&CONNECTION_REFUSED)
        }
    }

    static CONNECTION_REFUSED: ConnectionRefused = ConnectionRefused;

    #[derive(Debug)]
    struct ConnectionRefused;

    impl std::fmt::Display for ConnectionRefused {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("connection refused")
        }
    }

    impl std::error::Error for ConnectionRefused {}

    #[test]
    fn compare_applies_the_three_rules() {
        let file = StoreIdentityFile {
            format_version: 1,
            store_id: "s-1".to_owned(),
            catalog_id: "c-1".to_owned(),
            backend: "sqlite".to_owned(),
            created_at_ms: 0,
            ephemeral_catalog: false,
        };
        let ok = compare(&file, "c-1", vec![], StoreKind::Filesystem, "/r").unwrap();
        assert_eq!(ok.store_id, "s-1");
        assert_eq!(ok.store_root, "/r");
        compare(
            &file,
            "c-1",
            vec!["s-1".to_owned()],
            StoreKind::Filesystem,
            "/r",
        )
        .unwrap();
        assert!(matches!(
            compare(&file, "c-2", vec![], StoreKind::Filesystem, "/r"),
            Err(StoreProblem::ForeignCatalog { .. })
        ));
        assert!(matches!(
            compare(
                &file,
                "c-1",
                vec!["s-9".to_owned()],
                StoreKind::S3,
                "s3://b/"
            ),
            Err(StoreProblem::IdMismatch { .. })
        ));
    }
}
