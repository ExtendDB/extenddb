// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup object store abstraction.
//!
//! One trait, [`BackupStore`], and two implementations: [`FilesystemStore`]
//! writes objects under a local root directory, [`S3Store`] writes them to an
//! S3 bucket through the official AWS SDK. Both are exercised by the same
//! conformance test module in `tests/conformance.rs`.
//!
//! Keys are `/`-separated component paths validated by [`key::validate_key`];
//! the same rules apply to both stores so a backup written by one store can be
//! read by the other. See `docs/design/15-backup-restore.md` for the full
//! design.

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

mod config;
mod error;
mod fs;
pub mod key;
mod s3;

pub use config::{BackupStoreConfig, BackupStoreKind, FilesystemStoreConfig, S3StoreConfig};
pub use error::StoreError;
pub use fs::FilesystemStore;
pub use s3::S3Store;

/// Byte stream flowing into and out of a store.
///
/// `put` consumes one, `get` returns one. Errors from the underlying source
/// propagate as [`StoreError`], which lets tests inject a mid-body failure to
/// exercise abort paths.
pub type ByteStream = BoxStream<'static, Result<Bytes, StoreError>>;

/// Build a [`ByteStream`] from an in-memory buffer.
pub fn byte_stream_from(bytes: impl Into<Bytes>) -> ByteStream {
    let bytes = bytes.into();
    Box::pin(futures::stream::once(async move { Ok(bytes) }))
}

/// Metadata for one stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Object key, relative to the store root (and, for S3, the configured
    /// bucket prefix).
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// Last modification time, when the store reports one.
    pub last_modified: Option<std::time::SystemTime>,
}

/// A destination for backup objects.
///
/// Implementations must be safe to share across tasks; the engine holds one
/// behind an `Arc<dyn BackupStore>` for the lifetime of the process.
pub trait BackupStore: Send + Sync {
    /// Store `body` under `key`, replacing any existing object.
    ///
    /// A failed put must leave nothing observable under `key`: the filesystem
    /// store writes to a temporary sibling and renames into place, the S3
    /// store aborts an in-flight multipart upload.
    ///
    /// One key-space divergence between the stores: the filesystem store
    /// cannot hold an object at `a` and another at `a/b` (a file and a
    /// directory cannot share a path), while S3 can. The backup layout keys
    /// objects only under per-backup prefixes, so it never produces that
    /// shape.
    fn put<'a>(&'a self, key: &'a str, body: ByteStream) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Stream the object stored under `key`.
    ///
    /// Returns [`StoreError::NotFound`] when no object exists.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ByteStream, StoreError>>;

    /// Metadata for the object under `key`, or `None` when no object exists.
    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>>;

    /// Stream metadata for every object whose key equals `prefix` or sits
    /// under `prefix/`, in lexicographic key order. An empty prefix lists
    /// every object. Prefix matching is component-aligned: prefix `a/b` does
    /// not match key `a/bc`.
    fn list<'a>(&'a self, prefix: &'a str) -> BoxStream<'a, Result<ObjectMeta, StoreError>>;

    /// Delete every object matched by `prefix` (same matching rule as
    /// [`BackupStore::list`]) and return how many objects were removed. The
    /// filesystem store also removes directories the deletion emptied, and
    /// its count includes any files under the prefix that listings hide
    /// (in-flight temporaries, symlinks), so the count can exceed the number
    /// of objects `list` reported. A prefix that matches nothing returns
    /// `Ok(0)`.
    fn delete_prefix<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<u64, StoreError>>;

    /// Startup validation. The filesystem store checks the root exists and is
    /// writable by writing and removing a probe file; the S3 store calls
    /// `HeadBucket`. Called once at server startup so a misconfigured store
    /// fails then, not at the first `CreateBackup`.
    fn validate(&self) -> BoxFuture<'_, Result<(), StoreError>>;
}

/// Open the store described by `config`.
///
/// Validates the per-kind section is present and its values are usable, then
/// constructs the store. S3 credentials come from the standard AWS SDK chain;
/// they are never part of the configuration.
///
/// # Errors
///
/// Returns an error when the section matching the selected store kind is
/// absent, a value fails validation (part size floor, zero concurrency,
/// malformed prefix), or the filesystem root is unusable.
pub async fn open(config: &BackupStoreConfig) -> Result<Arc<dyn BackupStore>, StoreError> {
    match config.store {
        BackupStoreKind::Filesystem => {
            let fs_config = config.filesystem.as_ref().ok_or_else(|| {
                StoreError::Other(
                    "[backup.filesystem] with a path is required when store = \"filesystem\""
                        .to_owned(),
                )
            })?;
            Ok(Arc::new(FilesystemStore::open(&fs_config.path).await?))
        }
        BackupStoreKind::S3 => {
            let s3_config = config.s3.as_ref().ok_or_else(|| {
                StoreError::Other(
                    "[backup.s3] with a bucket is required when store = \"s3\"".to_owned(),
                )
            })?;
            Ok(Arc::new(S3Store::open(s3_config).await?))
        }
    }
}
