// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Input files and report files for bulk put operations.
//!
//! [`BulkSource`] enumerates and opens input files; [`BulkSink`] opens
//! report files that a [`ReportWriter`] renders page by page. This module
//! holds the filesystem implementations, [`FsSource`] and [`FsSink`]; the
//! S3 store's are in `bulk_put::s3`.
//!
//! Both filesystem implementations live inside the `import_export_io` jail. Every path
//! is composed from the account id of the authenticated identity and
//! checked lexically before any filesystem access. Enumeration descends
//! with `openat(O_DIRECTORY | O_NOFOLLOW)` directory handles from the
//! verified account handle and reads entries from those handles, so a
//! symlink is never followed into or out of the subtree and there is no
//! interval between examining a component and descending into it in which
//! a swapped symlink could be followed; the bucket existence check uses
//! the same primitives. Opening an input file or a report
//! directory starts from a
//! handle on the caller's account directory, verified against the
//! canonicalized, containment-checked account root, and walks the
//! remaining components with `openat(O_NOFOLLOW)`, so a component swapped
//! for a symlink at any point fails its own open instead of redirecting
//! the walk, and no path is resolved again after the account root. The
//! final input handle must be a regular file; `O_NONBLOCK` keeps a
//! swapped-in FIFO from blocking the open. Report rendering creates
//! missing directories with `mkdirat` on their parent's handle and holds
//! the final directory handle for the temp-file create, write, and rename.
//! The non-unix fallback is unhardened and the API is refused on those
//! targets by the request handler. Blocking filesystem work, encoding
//! included, runs on `spawn_blocking`, never on the async runtime.

use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;

use extenddb_storage::BoxedFuture as BoxFuture;
use extenddb_storage::{InputFile, ReportKind};

use crate::bulk_put::store_root::ensure_account_dir;

mod chain;
mod codec;
mod fs_source;
mod jail;
mod key_form;
mod location;
mod manifest;
mod reader;
mod sink;
mod walk;

#[cfg(test)]
mod contract;
#[cfg(test)]
mod tests;

pub(crate) use chain::{ChainLineReader, line_reader};
use jail::bucket_exists_blocking;
pub(crate) use key_form::{climbs_above_start, stored_form};
pub(crate) use location::{BucketRef, OutputSse, StoreFault, StoreUnavailable};
use reader::{etag_blocking, open_blocking};
pub(crate) use sink::ROTATION_THRESHOLD_BYTES;
use walk::list_blocking;

/// One enumerated input file, as stored on the operation record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceFile {
    pub path: String,
    pub size: u64,
    /// The entity tag the store listed for the object; `None` when the
    /// store derives it from the bytes instead, as the filesystem store
    /// does.
    pub etag: Option<String>,
}

/// A failure while enumerating or reading input files.
///
/// `TooManyFiles` comes only from `list`, when the walk passes one of the
/// bounds derived from the caller's file limit; the Start handler maps it
/// to the `max_bulk_input_files` `ValidationException`. `LineTooLong`,
/// `InvalidUtf8` and `Corrupt` follow the parse-level abandon path; `Store`
/// carries its own code and message; the runner maps the remaining three to
/// a `FAILED` operation with `InternalServerError`, so none of those
/// strings reaches a client.
#[derive(Debug)]
// The String and Io payloads are read through the derived Debug impl in the
// runner's log lines, which dead code analysis deliberately ignores.
#[allow(dead_code)]
pub(crate) enum SourceError {
    Jail(String),
    NotRegularFile,
    LineTooLong,
    /// A line whose bytes are not valid UTF-8 inside a JSON string literal.
    /// JSON text is UTF-8, so the line is not well-formed JSON; it is
    /// reported, not decoded lossily. Invalid bytes outside a string
    /// literal are a `Corrupt` line instead: the hosted parser tokenizes
    /// first and decodes string contents when it reaches them, so it
    /// reports "Unexpected token" for a bad byte between tokens and the
    /// UTF-8 message only for one inside a string (measured on the service).
    InvalidUtf8,
    /// The compressed stream is corrupt or ends before its trailer. The
    /// bytes are not readable input; the file is abandoned at that point
    /// as the hosted service does (measured on the service: a truncated gzip
    /// file counts one processed and failed row with the message
    /// "Unexpected token. Remainder of the file will not be processed."
    /// and the operation continues with the next file).
    Corrupt,
    /// `InputCompressionType` names a codec and the file does not begin
    /// with that codec's magic bytes: it is not a compressed object.
    /// Measured on the service for GZIP: the file counts one processed and
    /// failed row with the message "The S3 object could not be
    /// decompressed", the operation continues with the next file, and it
    /// ends FAILED with FailureCode ValidationException, "Some of the items
    /// failed validation checks and were not ingested".
    NotCompressed,
    /// `InputCompressionType` is `ZSTD`, the file begins with the zstd
    /// magic, and the frame does not decode (truncated or corrupt). The
    /// hosted service reports the file as one failed row with the message
    /// "The S3 object could not be decompressed", counts its bytes as
    /// processed input, and continues with the next file; the operation
    /// ends by its other rows (measured on the service: a truncated zstd frame
    /// beside a good file ends `COMPLETED_WITH_ERRORS`, where a plain file
    /// under `ZSTD` ends `FAILED` as under `GZIP`).
    DecodeFailed,
    /// The object at the key is no longer the one the operation listed: the
    /// S3 store's GET, sent with `If-Match` and the listed `ETag`, was
    /// answered 412. Measured on the service for an object overwritten
    /// after the listing and before its read: the file counts one processed
    /// and failed row with the message "The S3 object could not be imported
    /// because it was overwritten" and no `itemIndex`, none of its bytes are
    /// processed input, the manifest marks it `PARTIALLY_PROCESSED` with no
    /// range and the listed entity tag, the run continues with the next
    /// file, and the operation ends `FAILED` with `ValidationException`,
    /// "Some of the items failed validation checks and were not ingested".
    Overwritten,
    TooManyFiles,
    Io(std::io::Error),
    /// A location fault the store rendered (see [`StoreFault`]).
    Store(StoreFault),
}

/// How an input object is encoded, from the operation's
/// `InputCompressionType`. The reader decompresses while it reads; the
/// enumerated size, `ProcessedInputBytes` and the entity tag all describe
/// the stored (compressed) bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InputCodec {
    None,
    Gzip,
    Zstd,
}

impl InputCodec {
    /// Whether a decoder sits in the read chain.
    pub(crate) fn decodes(self) -> bool {
        !matches!(self, Self::None)
    }

    /// The codec's leading magic bytes, checked when the file is opened;
    /// a file that does not start with them is `NotCompressed`.
    fn magic(self) -> &'static [u8] {
        match self {
            Self::None => &[],
            Self::Gzip => &[0x1f, 0x8b],
            Self::Zstd => &[0x28, 0xb5, 0x2f, 0xfd],
        }
    }
}

/// A failure while rendering a report file. The runner maps `Store` to its
/// own code and message, and every other variant to a `FAILED` operation
/// by its class with a log entry; no path reaches a client.
#[derive(Debug)]
// The payloads are read through the derived Debug impl in the runner's log
// lines, which dead code analysis deliberately ignores.
#[allow(dead_code)]
pub(crate) enum SinkError {
    Jail(String),
    Symlink(String),
    Io(std::io::Error),
    /// A location fault the store rendered (see [`StoreFault`]).
    Store(StoreFault),
}

/// Where input files come from: [`FsSource`] on the filesystem store, the
/// S3 store's source in `bulk_put::s3`. `at` names the input bucket on
/// every call.
pub(crate) trait BulkSource: Send + Sync {
    /// The Start-time check of the input bucket: the expected owner first,
    /// then existence; a failure of either is a [`SourceError::Store`]
    /// fault. `prefix` is the request's `S3KeyPrefix`. The filesystem store
    /// creates the caller's account directory first, then answers an
    /// expected owner with `S3AccessDenied` (on this store the caller's
    /// account is the only owner a bucket directory can have) and a bucket
    /// directory that does not exist, or is not a directory, with
    /// `S3NoSuchBucket`.
    fn check_bucket(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>>;

    /// Enumerate regular files under `{root}/{account}/{bucket}` whose
    /// bucket-relative path starts with `prefix` (byte-wise, no decoding),
    /// lexical order, descending with `openat(O_DIRECTORY | O_NOFOLLOW)`
    /// handles from the verified account handle and reading entries from
    /// those handles, never descending into or including symlinks. The
    /// walk is bounded while it runs: a directory
    /// is entered only when its path is still compatible with `prefix`,
    /// collecting more than `max_files` matches stops the walk and returns
    /// `max_files + 1` entries, so a caller that finds more than
    /// `max_files` entries knows the bound was passed; visiting more than
    /// four times `max_files` directory entries that can hold a key under
    /// `prefix` is `TooManyFiles`, and entries beside the prefix are passed
    /// over without counting.
    /// Errors: bucket directory missing is `Ok` and empty; a jail violation
    /// is `Err`.
    fn list(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>>;

    /// Open one enumerated file, `file.path`, by the component walk: a
    /// verified handle on the caller's account directory, one
    /// `openat(O_DIRECTORY | O_NOFOLLOW)` per directory component, an
    /// `openat(O_NOFOLLOW | O_NONBLOCK)` of the final component, and an
    /// `fstat` that requires a regular file.
    /// The returned reader yields lines from at most `file.size` bytes of
    /// the file, the enumerated compressed size; bytes past it read as end
    /// of file. Under `Gzip` or `Zstd` the stream is decompressed while
    /// reading, every member or frame of a concatenated stream included.
    /// Runs the blocking parts on `spawn_blocking`.
    fn open(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>>;

    /// The entity tag of one input file for the manifest: the lowercase hex
    /// MD5 of the first `file.size` bytes, which is what S3 reports as the
    /// ETag of a single-part object (measured on the service: the manifest's
    /// `etag` equals the object's ETag). Resolved through the same jailed
    /// walk as `open`, so a swapped or symlinked file is refused, not
    /// hashed.
    fn etag(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>>;

    /// The number of physical lines in one input file, read the way the
    /// runner reads it (the enumerated size as the ceiling, decompressed
    /// under `Gzip` or `Zstd`). Used for the manifest of a file an earlier incarnation
    /// finished, whose count the current process never saw.
    fn line_count(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>>;

    /// The prefix a listing of `prefix` resolves to in this store (see
    /// [`key_form`]); `s3:ListBucket` is evaluated on it as well as on
    /// `prefix`. The default is `prefix` itself, which is what the
    /// filesystem store's walk matches byte-wise against the relative paths
    /// it finds; the S3 store against a configured endpoint resolves it.
    fn listed_prefix(&self, prefix: &str) -> String {
        prefix.to_owned()
    }

    /// Whether the service assigns the entity tags (S3), so the manifest
    /// never asks for the tag of a file listed without one: the object now
    /// at the key may be a replacement. The filesystem store hashes the
    /// bytes and answers `false`, the default.
    fn service_assigns_etags(&self) -> bool {
        false
    }
}

/// Async line source. `next_line` returns `Ok(None)` at end of file. Lines
/// longer than `max_line_bytes` are returned as `Err(LineTooLong)` without
/// buffering more than the cap; a line that is not valid UTF-8 is returned
/// as `Err(InvalidUtf8)` after it has been consumed.
pub(crate) trait LineReader: Send {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>>;
    /// Compressed bytes consumed so far (for `ProcessedInputBytes`).
    fn bytes_consumed(&self) -> u64;
    /// The offset of the end of the last line this reader delivered,
    /// within the stored object: the byte offset in the file for a plain
    /// object, and the compressed bytes consumed when a decoder sits in
    /// the chain. The runner decides from it whether an abandon leaves
    /// bytes of the file beyond the read that held the fault. The default
    /// is the raw consumed count, which a reader that tracks no per-line
    /// offset reports instead.
    fn last_line_end(&self) -> u64 {
        self.bytes_consumed()
    }
    /// The entity tag of the file, the lowercase hex MD5 of the bytes the
    /// reader consumed, once `next_line` has answered `Ok(None)` for end of
    /// file; `None` before that, and `None` after a fault, since an
    /// abandoned file was not read whole. Equal to what `BulkSource::etag`
    /// computes for the same file, so the runner records it at the
    /// checkpoint that finishes the file instead of hashing the file again
    /// when the manifest is rendered.
    fn etag(&self) -> Option<String> {
        None
    }
    /// The tag of the first answer the store accepted at the open, from a
    /// store whose tags the service assigns; `None` by default.
    fn opened_etag(&self) -> Option<String> {
        None
    }
}

/// Where reports go. `at` names the output bucket on every call; `sse` is
/// the encryption the output configuration asks for.
pub(crate) trait BulkSink: Send + Sync {
    /// The output location check: the expected owner first, then
    /// existence; a failure of either is a [`SinkError::Store`] fault. `key`
    /// is the object the operation is about to write. The runner asks this
    /// when the operation's first report record is produced and before the
    /// manifest is written. The filesystem store answers an expected owner
    /// with `S3AccessDenied` and a bucket directory that does not exist with
    /// `S3NoSuchBucket`, deciding existence with the same handle primitives
    /// the report walk uses (a symlink at the bucket name is never
    /// followed), and ignores `key` and `sse`.
    fn check_location(
        &self,
        at: &BucketRef<'_>,
        key: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<(), SinkError>>;

    /// The Start-time check of the output encryption settings for `key`; a
    /// failure is a [`SinkError::Store`] fault. The filesystem store records
    /// the encryption choice and checks nothing, which is the default.
    fn check_encryption(
        &self,
        _at: &BucketRef<'_>,
        _key: &str,
        _sse: &OutputSse,
    ) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async { Ok(()) })
    }

    /// Prepare one report file for writing: the lexical checks, a verified
    /// handle on the caller's account directory, a component walk that
    /// opens each directory with `openat(O_DIRECTORY | O_NOFOLLOW)`. The
    /// bucket directory is opened, never created (the caller stages it;
    /// the runner already failed the operation at the first report record
    /// when it was missing, so a missing one here is a crash-window last
    /// line of defence); the report
    /// tree beneath it is created with `mkdirat` on the parent handle.
    /// Then a sweep that unlinks every temp file an earlier incarnation
    /// left in the directory, and a temp file created with `create_new`
    /// relative to the final directory handle. The returned writer
    /// renders the file page by page and renames it into place on
    /// `finish`.
    fn open(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        kind: ReportKind,
        owner: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<Box<dyn ReportWriter>, SinkError>>;

    /// Write the operation's manifest, `manifest-files.json`, in the
    /// operation directory (`{account}:{operation_id}`, no shard suffix;
    /// measured on the service), one JSON object per line in `lines` order,
    /// uncompressed. Same lexical checks, directory walk, temp file and
    /// rename as a report; an existing manifest is replaced. Returns the
    /// key written.
    fn write_manifest(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        owner: &str,
        sse: &OutputSse,
        lines: Vec<Value>,
    ) -> BoxFuture<'_, Result<String, SinkError>>;

    /// The key a write of `key` lands under in this store (see
    /// [`key_form`]); `s3:PutObject` is evaluated on it as well as on
    /// `key`. The default is `key` itself.
    fn stored_key(&self, key: &str) -> String {
        key.to_owned()
    }
}

/// One report file being rendered. Dropping a writer without `finish`
/// removes the temp file, so a failed render leaves nothing behind.
pub(crate) trait ReportWriter: Send {
    /// Encode and append one page of records. Runs on the blocking pool.
    fn write_page(&mut self, records: Vec<Value>) -> BoxFuture<'_, Result<(), SinkError>>;
    /// Finish the gzip stream, fsync, rename over the final name, and
    /// return the object key as the hosted service spells it (with the
    /// inserted `/` after the prefix, doubled if the prefix ends in `/`).
    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<String, SinkError>>;
    /// The index of the report object the next `write_page` writes into:
    /// the open object's, or the next one's once the open one has rolled.
    /// The runner checks the caller's `s3:PutObject` on an object before the
    /// page that opens it.
    fn object_index(&self) -> u32;
    /// Test hook: lower the compressed byte count at which the writer
    /// rolls to the next report object, so a rotation test does not have
    /// to write 200 MiB. Wrappers that do not render leave it a no-op.
    #[cfg(test)]
    fn set_rotation_threshold(&mut self, _bytes: u64) {}
    /// Test hook: the compressed bytes written to the currently open
    /// report object so far, so a rotation test can set the threshold to
    /// exactly that figure and pin the boundary comparison. Zero when no
    /// object is open or the wrapper does not render.
    #[cfg(test)]
    fn open_object_bytes(&self) -> u64 {
        0
    }
}

/// Filesystem input source reading beneath the caller's account subtree of
/// the bulk store root.
pub(crate) struct FsSource {
    pub root: Arc<PathBuf>,
}

/// Filesystem report sink writing beneath the caller's account subtree of
/// the bulk store root.
pub(crate) struct FsSink {
    pub root: Arc<PathBuf>,
}
