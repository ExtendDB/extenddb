// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Step 5 of a restore: one verified data file decoded, validated and
//! handed to the backend's `bulk_load`.
//!
//! The decoder is synchronous (`std::io::Read`), so it runs on the blocking
//! pool: over the spooled file, or, on a filesystem store, over a
//! second `get` of the store bridged to `Read`, in both cases through
//! `ExtenddbManifest::reader`, which hashes the bytes again as it decodes
//! them: a spooled file's `.verified` marker says the download matched the
//! manifest, not that the file in the spool still does (a resume on the
//! same host reads what the spool holds now). On both paths the
//! items are handed to `bulk_load` as they decode and the digest is checked
//! at the end of the stream, so the items of a file replaced between the two
//! passes reach the backend (and, on a backend that commits per batch, the
//! CREATING target) before the end-of-stream check fails the file and the
//! failure path removes the target; the store root's and the spool's
//! owner-only modes are what keep the file from changing. Each item past the resume's `start_at` is
//! validated against the target's limits and key schema
//! (`validate_restorable_item`), then collected into chunks of at most
//! [`LOAD_CHUNK_ITEMS`] items or [`LOAD_CHUNK_BYTES`] of line text (the
//! bytes the decoder read for the items, the unit the backup side budgets
//! in), whichever comes first, and sent over a channel of two chunks to the
//! async side, which flattens them into the item stream of one `bulk_load`
//! call per file. The memory a file's load holds is therefore the decoded
//! items of at most `LOAD_CHUNK_BYTES` plus one line of line text per chunk
//! (the line that crosses the bound is kept in the chunk it was read for) at
//! four chunk positions (two in the channel, the one being filled, the one
//! being drained), the decoder's line (at most `MAX_LINE_BYTES`) and the
//! deserializer's intermediate value for it, and the backend's own batch.
//!
//! A decode or validation failure, the shutdown token and the heartbeat
//! (its loss, or a timer that no longer confirms the claim, both read at
//! every chunk boundary) end the stream with an `Err` item, which the
//! backend answers by failing the file (the design: nothing of a failed file
//! is recorded where the backend writes a file in one transaction); the
//! reason is kept here so the job reports it rather than the backend's
//! wrapping of it. A decoder that stops without ending the stream itself (a
//! panic on the blocking thread) fails the file too: a clean end of stream
//! the backend saw is not taken for a complete file. The join of the
//! decoder's thread after `bulk_load` returned has no bound of its own: on
//! the in-place path the thread reads the store through the bridge, and a
//! filesystem mount that stopped answering holds it as it holds the store's
//! own `get`, until the mount answers.

use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use extenddb_backup_store::BackupStore;
use extenddb_core::backup_format::{DataFileEntry, ExtenddbManifest};
use extenddb_core::types::Item;
use extenddb_core::validation::{RestorableKeySchema, RestorableLimits, validate_restorable_item};
use extenddb_storage::backup_catalog::RestoreTargetKey;
use extenddb_storage::error::StorageError;
use extenddb_storage::{BulkLoadFile, CancellationToken, JobClaim, RestoreEngine};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::mpsc;

use super::super::job::heartbeat::{ClaimCheck, HeartbeatHandle};
pub(crate) use super::download::Stop;

/// Items per chunk handed to the backend, at most.
pub(crate) const LOAD_CHUNK_ITEMS: usize = 1000;
/// Line bytes per chunk (the data file's text the decoder read for the
/// chunk's items), at most; a chunk closes at the first item that takes it
/// to this.
pub(crate) const LOAD_CHUNK_BYTES: u64 = 32 * 1024 * 1024;
/// Chunks the channel between the decoder and the backend holds.
const CHUNK_CHANNEL: usize = 2;

/// Where the file's bytes are read from.
pub(crate) enum Source {
    /// The spooled, verified file.
    Spool(PathBuf),
    /// The store again (a filesystem store, read in place).
    InPlace {
        store: Arc<dyn BackupStore>,
        key: String,
    },
}

/// One file's load.
pub(crate) struct FileLoad<'a> {
    pub manifest: &'a Arc<ExtenddbManifest>,
    pub entry: &'a DataFileEntry,
    pub file_index: u32,
    /// Items of the file an earlier run applied, decoded and skipped.
    pub start_at: u64,
    pub reapplying: bool,
    pub key_schema: &'a RestorableKeySchema,
    pub limits: RestorableLimits,
    pub shutdown: &'a CancellationToken,
    pub heartbeat: &'a HeartbeatHandle,
    /// The chunk's byte bound: [`LOAD_CHUNK_BYTES`], or a smaller bound a
    /// test set for its job.
    pub chunk_bytes: u64,
}

/// Decode `source` from `load.start_at` on, validate every item, and write
/// the file through `storage.bulk_load` (module doc). Returns the items the
/// backend applied.
pub(crate) async fn load_file<S: RestoreEngine + ?Sized>(
    storage: &S,
    target: &RestoreTargetKey,
    claim: &dyn JobClaim,
    load: FileLoad<'_>,
    source: Source,
) -> Result<u64, Stop> {
    let key = load.entry.key.clone();
    let read: Box<dyn Read + Send> = match source {
        Source::Spool(path) => {
            let file = std::fs::File::open(&path).map_err(|e| {
                Stop::Fail(format!(
                    "opening data file {key} in the spool ({}): {e}",
                    path.display()
                ))
            })?;
            Box::new(file)
        }
        Source::InPlace {
            store,
            key: store_key,
        } => {
            let body = store
                .get(&store_key)
                .await
                .map_err(|e| Stop::Fail(format!("reading data file {key} from the store: {e}")))?;
            let reader = tokio_util::io::StreamReader::new(body.map_err(std::io::Error::other));
            let bridge = tokio_util::io::SyncIoBridge::new_with_handle(
                reader,
                tokio::runtime::Handle::current(),
            );
            Box::new(bridge)
        }
    };
    let stop: Arc<Mutex<Option<Stop>>> = Arc::new(Mutex::new(None));
    let (tx, rx) = mpsc::channel::<Result<Vec<Item>, StorageError>>(CHUNK_CHANNEL);
    let feeder = Feeder {
        manifest: Arc::clone(load.manifest),
        entry: load.entry.clone(),
        start_at: load.start_at,
        key_schema: load.key_schema.clone(),
        limits: load.limits,
        shutdown: load.shutdown.clone(),
        claim: load.heartbeat.claim_check(),
        chunk_bytes: load.chunk_bytes,
        stop: Arc::clone(&stop),
        tx,
    };
    let decoder = tokio::task::spawn_blocking(move || feeder.run(read));
    let items = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (chunk, rx))
    })
    .flat_map(|chunk| match chunk {
        Ok(items) => futures::stream::iter(items.into_iter().map(Ok)).boxed(),
        Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
    })
    .boxed();
    let file = BulkLoadFile {
        file_index: load.file_index,
        start_at: load.start_at,
        reapplying: load.reapplying,
    };
    let result = tokio::select! {
        biased;
        () = load.heartbeat.lost().cancelled() => Err(None),
        result = storage.bulk_load(target, claim, file, items) => result.map_err(Some),
    };
    // The stream is dropped with the call, so a decoder blocked on the
    // channel returns; its thread is joined before the file is reported. A
    // decoder that did not return (it panicked) dropped the channel without
    // an `Err` item, so the backend saw a clean end of a short stream: the
    // file fails here, where a backend that writes per file rolls it back.
    if decoder.await.is_err() {
        return Err(Stop::Fail(format!(
            "data file {key}: the decoder stopped unexpectedly"
        )));
    }
    let stopped = stop.lock().unwrap_or_else(PoisonError::into_inner).take();
    match (result, stopped) {
        (Ok(applied), None) => Ok(applied),
        (Ok(_), Some(stop)) | (Err(_), Some(stop)) => Err(stop),
        (Err(None), None) => Err(Stop::ClaimLost(load.heartbeat.reason())),
        (Err(Some(e)), None) => Err(Stop::Fail(format!("data file {key}: {e}"))),
    }
}

/// The blocking side: the decoder, the validator and the chunking.
struct Feeder {
    manifest: Arc<ExtenddbManifest>,
    entry: DataFileEntry,
    start_at: u64,
    key_schema: RestorableKeySchema,
    limits: RestorableLimits,
    shutdown: CancellationToken,
    /// The heartbeat's pre-write check, read at every chunk boundary: the
    /// loss raised, or a timer that no longer confirms the claim.
    claim: ClaimCheck,
    chunk_bytes: u64,
    stop: Arc<Mutex<Option<Stop>>>,
    tx: mpsc::Sender<Result<Vec<Item>, StorageError>>,
}

impl Feeder {
    fn run(self, read: Box<dyn Read + Send>) {
        let key = self.entry.key.clone();
        let mut reader = match self.manifest.reader(&self.entry, read) {
            Ok(reader) => reader,
            Err(e) => {
                self.end(Stop::Fail(format!("data file {key}: {e}")));
                return;
            }
        };
        let mut index: u64 = 0;
        let mut chunk = Vec::new();
        // The line bytes of the items in `chunk`, from the decoder's count.
        let mut chunk_bytes: u64 = 0;
        let mut decoded_before = reader.bytes_decoded();
        loop {
            match reader.next_item() {
                Ok(Some(item)) => {
                    let line_bytes = reader.bytes_decoded() - decoded_before;
                    decoded_before = reader.bytes_decoded();
                    index += 1;
                    if index <= self.start_at {
                        continue;
                    }
                    if let Err(e) = validate_restorable_item(&item, &self.key_schema, &self.limits)
                    {
                        self.end(Stop::Fail(format!(
                            "data file {key}: item cannot be restored: {e}"
                        )));
                        return;
                    }
                    chunk_bytes += line_bytes;
                    chunk.push(item);
                    if chunk.len() >= LOAD_CHUNK_ITEMS || chunk_bytes >= self.chunk_bytes {
                        if self.shutdown.is_cancelled() {
                            self.end(Stop::Cancelled);
                            return;
                        }
                        if !self.claim.confirms_the_claim() {
                            self.end(Stop::ClaimLost(
                                "the claim was lost while the file was being decoded".to_owned(),
                            ));
                            return;
                        }
                        if self
                            .tx
                            .blocking_send(Ok(std::mem::take(&mut chunk)))
                            .is_err()
                        {
                            // The backend ended the call; nothing more to feed.
                            return;
                        }
                        chunk_bytes = 0;
                    }
                }
                Ok(None) => {
                    if !chunk.is_empty() {
                        let _ = self.tx.blocking_send(Ok(chunk));
                    }
                    return;
                }
                Err(e) => {
                    self.end(Stop::Fail(format!("data file {key}: {e}")));
                    return;
                }
            }
        }
    }

    /// Record why the stream ends and end it with an `Err` item.
    fn end(&self, stop: Stop) {
        let text = match &stop {
            Stop::Fail(reason) => reason.clone(),
            Stop::Cancelled => "the restore was stopped by the server's shutdown".to_owned(),
            Stop::ClaimLost(why) | Stop::LeftRunning(why) => why.clone(),
        };
        *self.stop.lock().unwrap_or_else(PoisonError::into_inner) = Some(stop);
        let _ = self.tx.blocking_send(Err(StorageError::Internal(text)));
    }
}
