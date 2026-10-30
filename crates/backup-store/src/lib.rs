// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup object store abstraction.
//!
//! One trait, [`BackupStore`], and two implementations: [`FilesystemStore`]
//! writes objects under a local root directory, [`S3Store`] writes them to an
//! S3 bucket through the official AWS SDK. Both are exercised by the same
//! conformance test module in `tests/conformance.rs`.
//!
//! Keys are `/`-separated component paths validated by [`key::validate_key`],
//! the upper bound of what both stores accept; the [`key`] module doc lists
//! where each store narrows it. The design overview
//! (`docs/design/15-backup-restore.md`, #396) states the rules the store's
//! callers honour; this crate's docs are the record of what the store does.
//!
//! The AWS SDK is built with aws-config's `sso` feature, so an SSO profile
//! can supply credentials; besides the SSO clients that feature links
//! `ring`, `hex` and `zeroize`, a second cryptography library beside the
//! `aws-lc-rs` the TLS stack uses.

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
pub use s3::{MAX_PART_SIZE_BYTES, MAX_STORE_PREFIX_LEN, MIN_PART_SIZE_BYTES, S3Store};

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
    /// Store-assigned entity tag, when the store has one. S3 reports the
    /// object's `ETag` with surrounding quotes removed (a multipart upload's
    /// `ETag` carries a `-<parts>` suffix and is not an MD5). The filesystem
    /// store has no equivalent and reports `None`.
    pub etag: Option<String>,
}

/// A destination for backup objects.
///
/// Implementations must be safe to share across tasks; the engine holds one
/// behind an `Arc<dyn BackupStore>` for the lifetime of the process.
pub trait BackupStore: Send + Sync {
    /// Store `body` under `key`, replacing any existing object. Returns the
    /// metadata of the stored object: `size` is the number of body bytes
    /// written and `etag` is the store's tag when it has one, so a caller
    /// writing a manifest can record both without a second `head` call.
    ///
    /// After an error the key holds nothing, the previous object, or the
    /// new complete object; never a partial one. The last case is a put that
    /// stored the object and then failed: the filesystem store's directory
    /// sync after the rename, or an S3 `CompleteMultipartUpload` that
    /// succeeded on the server and timed out on the client. The filesystem
    /// store writes to a temporary sibling and renames into place, the S3
    /// store aborts an in-flight multipart upload. `last_modified` may be
    /// `None` from `put` (S3 does not return it); `head` has it. A put that
    /// is dropped before it stores the object (a caller's timeout, a
    /// cancelled job), including while the filesystem store's open of its
    /// temporary is in flight, also leaves nothing: the filesystem store
    /// removes its temporary and the S3 store aborts its upload from a drop
    /// guard. The exception is runtime shutdown on the S3 store: the guard's
    /// abort is a task on the put's runtime, and a runtime that shuts down
    /// with puts in flight drops those tasks unpolled, so it sends no abort
    /// for them (an in-flight `CreateMultipartUpload` included). Their
    /// uploads, parts included, stay until a `delete_prefix` covering the
    /// key sweeps them or the bucket's lifecycle rule removes them; a caller
    /// that must leave nothing awaits its puts (or runs its failure path's
    /// `delete_prefix`) before the runtime shuts down. A put that overlaps a
    /// `delete_prefix` covering its key may fail or may leave its object
    /// (see [`BackupStore::delete_prefix`]).
    ///
    /// The keys each store accepts are listed in the [`key`] module doc.
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: ByteStream,
    ) -> BoxFuture<'a, Result<ObjectMeta, StoreError>>;

    /// Stream the object stored under `key`.
    ///
    /// Returns [`StoreError::NotFound`] when no object exists.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ByteStream, StoreError>>;

    /// Metadata for the object under `key`, or `None` when no object exists.
    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>>;

    /// Stream metadata for every object whose key equals `prefix` or sits
    /// under `prefix/`, in lexicographic key order. An empty prefix lists
    /// every object. Prefix matching is component-aligned: prefix `a/b` does
    /// not match key `a/bc`. Only keys that pass [`key::validate_key`] are
    /// reported: an entry the store did not write and could not address (an
    /// S3 console folder marker, a file name with a backslash) is skipped,
    /// so every key a listing yields can be passed to `get`, `head` and
    /// `delete_prefix`. The filesystem store collects the whole listing into
    /// one `Vec` before the stream yields its first entry (about 72 bytes plus
    /// the key per object); the S3 store's listing is paged.
    fn list<'a>(&'a self, prefix: &'a str) -> BoxStream<'a, Result<ObjectMeta, StoreError>>;

    /// Delete every object matched by `prefix` (same matching rule as
    /// [`BackupStore::list`]) and return a count of the objects in this
    /// call's own listing that it removed or found already gone. The
    /// filesystem store counts an entry only when this call removed it, so
    /// the counts of concurrent calls partition the objects; S3 counts every
    /// key its `DeleteObjects` requests named, since S3 reports an absent
    /// key as deleted, so the counts of concurrent S3 calls may overlap. Both
    /// stores remove entries under the prefix that listings hide (the
    /// filesystem store's in-flight temporaries, symlinks and names failing
    /// the key rule; S3 objects whose keys fail the key rule), so the count
    /// can exceed the number of objects `list` reported. The filesystem store
    /// also removes the prefix directory and every directory under it
    /// (directories above the prefix stay); the S3 store also aborts any
    /// in-progress multipart upload under the prefix, and returns `Ok` once
    /// the objects are gone even when that sweep's `ListMultipartUploads` or
    /// `AbortMultipartUpload` is denied (it logs a warning and leaves those
    /// uploads to the bucket's lifecycle rule; `validate` reports both
    /// grants). A prefix that matches
    /// nothing returns `Ok(0)`, and an entry that vanished between listing
    /// and removal is not an error, so two callers deleting the same prefix
    /// do not fail each other. A `put` that overlaps a `delete_prefix`
    /// covering its key may fail (with an error of any class, leaving no
    /// partial object) or may succeed and leave its complete object in
    /// place after the deletion returns; the store gives no ordering
    /// between them, and callers must not put under a prefix they are
    /// deleting.
    ///
    /// A `delete_prefix` whose future is dropped (a caller's timeout) stops
    /// at its next step, and what it removed stays removed: the S3 store at
    /// its next request (a `DeleteObjects` or `AbortMultipartUpload` already
    /// sent completes on the server), the filesystem store before its next
    /// entry removal (its walk runs on the blocking pool and checks a flag
    /// the dropped future sets).
    fn delete_prefix<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<u64, StoreError>>;

    /// Startup validation. The filesystem store applies the owner-only rule
    /// to the root and the ancestor rule to the directories above it (the
    /// `fs` module doc states both), checks that the root is still where
    /// `open` found it, and writes, flushes, syncs and removes a probe file
    /// (a probe a concurrent `delete_prefix("")` removed first counts as
    /// removed).
    /// The S3 store sends one request per grant it uses and names the
    /// missing grant on a denial; the requests and what they leave behind
    /// are documented on `validate` in `impl BackupStore for S3Store`.
    /// Called once at server startup so a misconfigured store fails then,
    /// not at the first `CreateBackup`.
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
/// absent, a value fails validation (an empty bucket, an S3 Express bucket
/// name, a part size outside 5 MiB to 5 GiB, zero concurrency, a malformed
/// prefix, an endpoint setting that breaks the rules in the `config` module
/// doc, the SDK's own and its credential providers' and IMDS client's
/// included), or the filesystem root is unusable.
pub async fn open(config: &BackupStoreConfig) -> Result<Arc<dyn BackupStore>, StoreError> {
    match config.kind {
        BackupStoreKind::Filesystem => {
            let fs_config = config.filesystem.as_ref().ok_or_else(|| {
                StoreError::Other(
                    "[backup.store.filesystem] with a path is required when kind = \"filesystem\""
                        .to_owned(),
                )
            })?;
            Ok(Arc::new(FilesystemStore::open(&fs_config.path).await?))
        }
        BackupStoreKind::S3 => {
            let s3_config = config.s3.as_ref().ok_or_else(|| {
                StoreError::Other(
                    "[backup.store.s3] with a bucket is required when kind = \"s3\"".to_owned(),
                )
            })?;
            Ok(Arc::new(S3Store::open(s3_config).await?))
        }
    }
}
