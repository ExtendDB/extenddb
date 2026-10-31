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

use extenddb_backup_store::{BackupStore, StoreError, byte_stream_from};
use extenddb_core::backup_format::{BackupArn, EXTENDDB_MANIFEST_FILE};
use extenddb_storage::BackupCatalog;
use extenddb_storage::backup_catalog::{BackupStatus, StoreIdentity, StoreKind};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

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
}

impl StoreIdentityFile {
    /// A new identity for `catalog_id`.
    fn new(catalog_id: &str, backend: &str) -> Self {
        Self {
            format_version: STORE_IDENTITY_FORMAT_VERSION,
            store_id: uuid::Uuid::new_v4().to_string(),
            catalog_id: catalog_id.to_owned(),
            backend: backend.to_owned(),
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
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
/// identity, the rows' and the file's.
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
            let ours = StoreIdentityFile::new(&catalog_id, backend);
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
            } else {
                tracing::info!(
                    "backup store ({kind}) root already carried {STORE_IDENTITY_KEY} (store id \
                     {}), created by another instance of this catalog",
                    file.store_id
                );
            }
            file
        }
    };
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
/// compared like any other).
///
/// # Errors
///
/// The problem that makes the store unhealthy (module doc).
pub async fn recheck<C: BackupCatalog + ?Sized>(
    store: &dyn BackupStore,
    identity: &StoreIdentity,
    catalog: &C,
    backend: &str,
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
                ..StoreIdentityFile::new(&catalog_id, backend)
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
    let rows: Vec<_> = catalog
        .list_backup_rows(Some(BackupStatus::Available), identity)
        .try_collect()
        .await
        .map_err(|e| StoreProblem::Catalog(e.to_string()))?;
    // Listings are ordered (`created_at`, `backup_arn`) ascending; the
    // newest row is the last.
    let Some(newest) = rows.last() else {
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
        let file = StoreIdentityFile::new("cat-1", "postgres");
        assert_eq!(file.format_version, 1);
        assert_eq!(file.catalog_id, "cat-1");
        assert_eq!(file.backend, "postgres");
        assert!(!file.store_id.is_empty());
        let parsed = StoreIdentityFile::parse(&file.to_bytes()).unwrap();
        assert_eq!(parsed, file);

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
