// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Report file rendering: the report directory handle produced by the
//! write-side component walk, the streaming gzip report writer, and the
//! [`BulkSink`] implementation that drives them.

mod report_dir;

use std::path::PathBuf;
use std::sync::Arc;

use flate2::Compression;
use flate2::write::GzEncoder;

use crate::bulk_put::report;
use crate::import_export_io::account_scoped_roots;

use super::jail::{bucket_exists_blocking, check_component, check_prefix};
use super::key_form::walked_segments;
use super::manifest::write_manifest_blocking;
use super::*;
pub(super) use report_dir::ReportDirectory;

/// The name of the first report object of each kind; later objects are
/// `output-00001.json.gz` onward (see [`object_name`]).
#[cfg(test)]
pub(super) const FINAL_NAME: &str = "output-00000.json.gz";

/// The compressed byte count at which the writer closes the current report
/// object and the next page opens the next one: 200 MiB. Measured on the service: the hosted service closed `output-00000.json.gz` at
/// 211,529,545 bytes, 200 MiB plus the records already buffered when the
/// threshold tripped, and wrote the remaining 73.11 MiB to
/// `output-00001.json.gz`. The overall record and byte caps on a report
/// are separate and unchanged.
pub(crate) const ROTATION_THRESHOLD_BYTES: u64 = 200 * 1024 * 1024;

impl BulkSink for FsSink {
    fn check_location(
        &self,
        at: &BucketRef<'_>,
        _key: &str,
        _sse: &OutputSse,
    ) -> BoxFuture<'_, Result<(), SinkError>> {
        let root = Arc::clone(&self.root);
        let account = at.account.to_owned();
        let bucket = at.bucket.to_owned();
        let owner_mismatch = at.expected_owner.is_some();
        Box::pin(async move {
            // Ownership is decided before existence, as at Start: on this
            // store the caller's account is the only owner a bucket
            // directory can have.
            if owner_mismatch {
                return Err(SinkError::Store(StoreFault::access_denied()));
            }
            let exists = tokio::task::spawn_blocking(move || {
                bucket_exists_blocking(&[root], &account, &bucket)
            })
            .await
            .map_err(|e| SinkError::Io(std::io::Error::other(e)))?
            .map_err(|error| match error {
                SourceError::Jail(detail) => SinkError::Jail(detail),
                SourceError::Io(io) => SinkError::Io(io),
                other => SinkError::Io(std::io::Error::other(format!("{other:?}"))),
            })?;
            if exists {
                Ok(())
            } else {
                Err(SinkError::Store(StoreFault::no_such_bucket()))
            }
        })
    }

    fn open(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        kind: ReportKind,
        owner: &str,
        _sse: &OutputSse,
    ) -> BoxFuture<'_, Result<Box<dyn ReportWriter>, SinkError>> {
        let root = Arc::clone(&self.root);
        let account = at.account.to_owned();
        let bucket = at.bucket.to_owned();
        let prefix = prefix.to_owned();
        let operation_id = operation_id.to_owned();
        let owner = owner.to_owned();
        Box::pin(async move {
            let writer = tokio::task::spawn_blocking(move || {
                open_writer_blocking(
                    &[root],
                    &account,
                    &bucket,
                    &prefix,
                    &operation_id,
                    kind,
                    &owner,
                )
            })
            .await
            .map_err(|e| SinkError::Io(std::io::Error::other(e)))??;
            Ok(Box::new(writer) as Box<dyn ReportWriter>)
        })
    }

    fn write_manifest(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        owner: &str,
        _sse: &OutputSse,
        lines: Vec<Value>,
    ) -> BoxFuture<'_, Result<String, SinkError>> {
        let root = Arc::clone(&self.root);
        let account = at.account.to_owned();
        let bucket = at.bucket.to_owned();
        let prefix = prefix.to_owned();
        let operation_id = operation_id.to_owned();
        let owner = owner.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                write_manifest_blocking(
                    &[root],
                    &account,
                    &bucket,
                    &prefix,
                    &operation_id,
                    &owner,
                    &lines,
                )
            })
            .await
            .map_err(|e| SinkError::Io(std::io::Error::other(e)))?
        })
    }

    fn stored_key(&self, key: &str) -> String {
        stored_form(key, false)
    }
}

/// Rendering steps 1 to 4: lexical checks, base selection, the verified
/// account handle and the component walk to the report directory, and
/// `create_new` on the temp name relative to the walked handle. The
/// returned writer performs the writes and the rename.
fn open_writer_blocking(
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
    prefix: &str,
    operation_id: &str,
    kind: ReportKind,
    owner: &str,
) -> Result<FsReportWriter, SinkError> {
    // Step 1: lexical checks. Start enforces the same rules on the wire
    // fields; they are re-checked here so the sink is safe on its own terms
    // whatever the caller did.
    check_component(bucket).map_err(SinkError::Jail)?;
    check_prefix(prefix).map_err(SinkError::Jail)?;
    check_component(operation_id).map_err(SinkError::Jail)?;
    check_component(owner).map_err(SinkError::Jail)?;
    if roots.is_empty() {
        return Err(SinkError::Jail("no bulk store is configured".to_owned()));
    }
    let scoped =
        account_scoped_roots(roots, account).map_err(|e| SinkError::Jail(e.to_string()))?;

    // The account directory under the store root. Start created it through
    // `ensure_account_dir`; creating it again here covers a directory
    // removed while the operation ran or a resume on a process whose
    // startup pass has not seen the account yet.
    let existing = scoped.iter().find(|dir| {
        std::fs::symlink_metadata(dir)
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
    });
    let base = if let Some(dir) = existing {
        dir.clone()
    } else {
        let Some(first) = scoped.first() else {
            return Err(SinkError::Jail("no bulk store is configured".to_owned()));
        };
        std::fs::create_dir_all(first).map_err(SinkError::Io)?;
        first.clone()
    };

    // The component chain beneath the account directory. `check_prefix`
    // rejected `..`; an empty or `.` prefix component names the same
    // directory on the filesystem, matching the hosted key rule that a
    // doubled slash is one separator, so neither adds a walk step.
    let mut components: Vec<String> = vec![bucket.to_owned()];
    components.extend(walked_segments(prefix).map(str::to_owned));
    components.push("AWSDynamoDB".to_owned());
    components.push("BulkOperation".to_owned());
    components.push(format!("{account}:{operation_id}-0"));
    components.push("output".to_owned());
    components.push(report::kind_directory(kind).to_owned());

    // Steps 2 and 3: a verified handle on the account directory, the
    // component walk that opens or creates each directory, and the final
    // directory handle every later step operates against.
    let dir_handle = ReportDirectory::establish(&base, roots, account, &components)?;

    // Step 4 begins: sweep every temp file left in the directory, then
    // create this writer's first temp with create_new. The directory is
    // exclusive to one operation and kind, so every entry carrying the
    // temp marker, whatever its object index, belongs to a dead or stale
    // incarnation of this same operation: a dead runner's temp embeds an
    // owner id no successor recomputes and would otherwise sit in the
    // caller's export tree forever, and unlinking a live stale writer's
    // temp makes that writer's later rename fail with NOENT instead of
    // replacing this owner's report. The final objects do not carry the
    // marker and are untouched until this writer's finish installs its
    // own. create_new keeps refusing everything else, symlinks included,
    // without wedging the retry.
    dir_handle.remove_matching(&is_report_temp)?;
    let file = dir_handle.create_new(&temp_name(0, owner))?;
    // The report directory path, kept for the file access audit event
    // each rename emits. The walk operates on handles; this string exists
    // for the log only.
    let mut resolved_dir = base;
    for component in &components {
        resolved_dir.push(component);
    }
    Ok(FsReportWriter {
        dir: Arc::new(dir_handle),
        stream: Stream::Open(GzEncoder::new(
            CountingFile::new(file),
            Compression::default(),
        )),
        closed: 0,
        threshold: ROTATION_THRESHOLD_BYTES,
        owner: owner.to_owned(),
        first_key: report::report_key(prefix, account, operation_id, kind, 0),
        finished: false,
        account: account.to_owned(),
        resolved_dir,
    })
}

/// The name of report object `index` within its kind directory.
#[must_use]
pub(super) fn object_name(index: u32) -> String {
    format!("output-{index:05}.json.gz")
}

/// The temp name an object is rendered under before its rename.
fn temp_name(index: u32, owner: &str) -> String {
    format!("{}.tmp-{owner}", object_name(index))
}

/// Whether a directory entry is the temp of some report object.
fn is_report_temp(name: &str) -> bool {
    name.starts_with("output-") && name.contains(".json.gz.tmp-")
}

/// The index of a final report object name; `None` for anything else.
fn object_index(name: &str) -> Option<u32> {
    let digits = name.strip_prefix("output-")?.strip_suffix(".json.gz")?;
    if digits.len() != 5 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// A file that counts the compressed bytes the encoder hands it, so the
/// writer can measure the current object's size without a stat.
struct CountingFile {
    file: std::fs::File,
    written: u64,
}

impl CountingFile {
    fn new(file: std::fs::File) -> Self {
        Self { file, written: 0 }
    }
}

impl std::io::Write for CountingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.file.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Where the writer stands between pages.
enum Stream {
    /// Object `closed` is open under its temp name and receiving pages.
    Open(GzEncoder<CountingFile>),
    /// The last page took the current object past the threshold and it
    /// was closed; the next page creates object `closed`. Opening lazily
    /// means a roll on the final page never leaves an empty object.
    Rolled,
    /// A page failed or `finish` ran.
    Closed,
}

/// Finish the gzip stream of one object and sync its file.
fn close_object(encoder: GzEncoder<CountingFile>) -> Result<(), SinkError> {
    let counting = encoder.finish().map_err(SinkError::Io)?;
    counting.file.sync_all().map_err(SinkError::Io)
}

/// Streaming writer for the report objects of one kind. Created by
/// [`FsSink::open`] with the first temp file already in place. Pages are
/// gzip-encoded on the blocking pool as they arrive, the encoder moving
/// across `spawn_blocking` hops the way the line reader's inner reader
/// does. After each page the compressed bytes written to the current
/// object are compared with the threshold; a page that takes the object to
/// or past it closes that object (the page stays in it, the hosted
/// service's overshoot shape) and the next page opens
/// `output-0000N.json.gz` with N incremented. `finish` closes the open
/// object, renames every temp to its final name in index order, and
/// removes any higher-numbered object an earlier incarnation left. Rename
/// is atomic and replaces an earlier rendering of the same object, so a
/// re-render after a crash between rendering and the terminal transition
/// is idempotent.
struct FsReportWriter {
    dir: Arc<ReportDirectory>,
    stream: Stream,
    /// Objects closed so far; also the index of the open or next object.
    closed: u32,
    threshold: u64,
    owner: String,
    first_key: String,
    finished: bool,
    /// The two fields of the file access audit event each rename emits.
    account: String,
    resolved_dir: PathBuf,
}

impl ReportWriter for FsReportWriter {
    fn write_page(&mut self, records: Vec<Value>) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            if records.is_empty() {
                return Ok(());
            }
            let stream = std::mem::replace(&mut self.stream, Stream::Closed);
            let encoder = match stream {
                Stream::Open(encoder) => Some(encoder),
                Stream::Rolled => None,
                Stream::Closed => {
                    return Err(SinkError::Io(std::io::Error::other(
                        "report writer already failed or finished",
                    )));
                }
            };
            let dir = Arc::clone(&self.dir);
            let next_temp = temp_name(self.closed, &self.owner);
            let threshold = self.threshold;
            let result = tokio::task::spawn_blocking(move || {
                let mut encoder = match encoder {
                    Some(encoder) => encoder,
                    None => GzEncoder::new(
                        CountingFile::new(dir.create_new(&next_temp)?),
                        Compression::default(),
                    ),
                };
                report::encode_ndjson_page(&mut encoder, &records).map_err(SinkError::Io)?;
                if encoder.get_ref().written >= threshold {
                    close_object(encoder)?;
                    Ok(Stream::Rolled)
                } else {
                    Ok(Stream::Open(encoder))
                }
            })
            .await
            .map_err(|e| SinkError::Io(std::io::Error::other(e)))?;
            // On an error the encoder was dropped with its file handle
            // inside the closure; the temp files are removed when the
            // writer is dropped.
            let stream = result?;
            if matches!(stream, Stream::Rolled) {
                self.closed += 1;
            }
            self.stream = stream;
            Ok(())
        })
    }

    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<String, SinkError>> {
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut this = self;
                match std::mem::replace(&mut this.stream, Stream::Closed) {
                    Stream::Open(encoder) => {
                        close_object(encoder)?;
                        this.closed += 1;
                    }
                    Stream::Rolled => {}
                    Stream::Closed => {
                        return Err(SinkError::Io(std::io::Error::other(
                            "report writer already failed",
                        )));
                    }
                }
                for index in 0..this.closed {
                    let name = object_name(index);
                    this.dir.rename(&temp_name(index, &this.owner), &name)?;
                    // The audit event the import and export operations
                    // emit, on the rename that installs each object. Paths
                    // stay in the log and never reach a client.
                    tracing::info!(
                        account_id = %this.account,
                        operation = "StartBulkPutItems",
                        resolved_path = %this.resolved_dir.join(name).display(),
                        "import/export file access"
                    );
                }
                // An earlier incarnation that rolled further than this
                // render leaves higher-numbered objects the renames did
                // not replace; they would read as part of this report.
                let count = this.closed;
                this.dir
                    .remove_matching(&|name| object_index(name).is_some_and(|i| i >= count))?;
                this.finished = true;
                Ok(this.first_key.clone())
            })
            .await
            .map_err(|e| SinkError::Io(std::io::Error::other(e)))?
        })
    }

    fn object_index(&self) -> u32 {
        // The open object is `closed`; after a roll, `closed` is the next.
        self.closed
    }

    #[cfg(test)]
    fn set_rotation_threshold(&mut self, bytes: u64) {
        self.threshold = bytes;
    }

    #[cfg(test)]
    fn open_object_bytes(&self) -> u64 {
        match &self.stream {
            Stream::Open(encoder) => encoder.get_ref().written,
            Stream::Rolled | Stream::Closed => 0,
        }
    }
}

impl Drop for FsReportWriter {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // The failure path: close the open encoder's file handle, then
        // remove every object's temp, the closed ones and the open or
        // half-created one. Blocking calls are acceptable here because the
        // happy path never drops an unfinished writer.
        drop(std::mem::replace(&mut self.stream, Stream::Closed));
        for index in 0..=self.closed {
            let _ = self.dir.remove_leftover(&temp_name(index, &self.owner));
        }
    }
}
