// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The data-file pipeline of a backup job: items from the snapshot stream,
//! through the validator, into gzip data files, into the store.
//!
//! Three parts run at once. The feeder (a task of its own) reads the
//! snapshot stream, checks every item with
//! [`validate_restorable_item`] and sends the ones that pass over a bounded
//! item channel ([`ITEM_CHANNEL`] items). One encoder per data file runs on
//! the blocking pool: it takes items off that channel, writes them through
//! `DataFileEncoder` into a [`ChunkSink`] that hands [`CHUNK_BYTES`]-sized
//! chunks over a bounded chunk channel ([`CHUNK_CHANNEL`] chunks), and
//! closes the file once its uncompressed bytes reach
//! `data_file_target_bytes`, so a file exceeds the knob by less than one
//! line. The uploader (the job's own task) starts the store `put` of each
//! file when the file's first chunk arrives, with the chunk channel as the
//! body, so a file is never buffered whole; a file that gets no item (the
//! stream ended or stopped at a file boundary) is never started. The
//! memory the pipeline holds is the item channel's items, the chunk
//! channel's chunks and the encoder's own state, none of which grows with
//! the table or the file size; the feeder's measurement of an oversized
//! item's line (below) holds no line.
//!
//! The validator's rule: after the first refused item the job encodes and
//! uploads nothing more but keeps reading and checking the rest of the
//! stream, logs each refused item (key, attribute path, reason, backup ARN)
//! up to [`REFUSAL_LOG_CAP`] and counts the rest, and then fails with a
//! reason that gives the count and the first item, naming #618 only when
//! the first refusal is an invalid `N` value. An item whose data file line
//! would exceed the format's cap (`MAX_LINE_BYTES`) is refused the same way
//! (`RestorableItemReason::LineTooLong`): the feeder measures the line of
//! every item over the default item limit (`DEFAULT_MAX_ITEM_SIZE_BYTES`),
//! the largest item the cap is sized for, on the blocking pool and through
//! a writer that counts and stops at the cap, so the measurement holds no
//! line and an instance with a raised `max_item_size_bytes` names the item
//! instead of failing in the encoder with no item named, and an instance at
//! the default measures none. Such an item's validation runs on the
//! blocking pool with the measurement (its cost grows with the item: the
//! number rule over every member); an item within the default is validated
//! on the feeder's own task.
//!
//! Before every `put` the uploader checks the claim the writer runs under
//! ([`ClaimWatch::confirms_the_claim`]: the job's heartbeat,
//! [`HeartbeatHandle`], or for `backup migrate-legacy` the maintenance
//! claim's) and starts none otherwise; a lost claim or the shutdown token
//! ends the pipeline at its next await.
//! When the uploader fails on its own (a put refused, an encoder error) it
//! cancels a token the feeder selects on, so the feeder stops reading at
//! once rather than at the stream's next item and the FAILED write does not
//! wait on a slow or dead snapshot stream; the token is cancelled before
//! the uploader awaits the encoder, since an encoder waiting on the item
//! channel returns only once the feeder closes it; the feeder's own refusal
//! leaves that token alone, since the rest of the stream is still to be
//! checked.
//! When the pipeline returns, no `put` is in flight: a stop lets the
//! current put end (its body ends when the encoder drops the sink) or drops
//! it, and the store's own rule for a dropped put applies (the filesystem
//! store removes its temporary, the S3 store aborts the upload, and at
//! shutdown the failure path's `delete_prefix` sweeps what is left).

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::{Bytes, BytesMut};
use extenddb_backup_store::{BackupStore, ByteStream};
use extenddb_core::backup_format::{
    DataFileChecksums, DataFileEncoder, DataFileEntry, FormatError, KeyRange, KeyRangeTracker,
    MAX_LINE_BYTES, data_file_key, encode_item_line_len_bounded,
};
use extenddb_core::limits::DEFAULT_MAX_ITEM_SIZE_BYTES;
use extenddb_core::metrics::MetricsCollector;
use extenddb_core::types::{Item, item_size_bytes};
use extenddb_core::validation::{
    RestorableItemError, RestorableItemReason, RestorableKeySchema, RestorableLimits,
    validate_restorable_item,
};
use extenddb_storage::CancellationToken;
use extenddb_storage::backup_catalog::StoreIdentity;
use extenddb_storage::error::StorageError;
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio::sync::mpsc;

use super::super::runtime::{ConfiguredStore, StoreHealth};
use super::heartbeat::HeartbeatHandle;
use super::{KillPoint, kill_point};

/// Bytes per chunk handed from the encoder to the upload.
pub(crate) const CHUNK_BYTES: usize = 1024 * 1024;
/// Chunks the chunk channel holds before the encoder waits.
pub(crate) const CHUNK_CHANNEL: usize = 4;
/// Items the item channel holds before the feeder waits. Four, not one: a
/// one-item channel made a 1M-row backup on SQLite take 75% longer (12.4 s
/// to 21.6 s, measured in both orders under one host state) for about
/// 50 MiB less at the peak of the worst-shape run (design section 7).
pub(crate) const ITEM_CHANNEL: usize = 4;
/// Refused items logged one by one; the rest are counted.
pub(crate) const REFUSAL_LOG_CAP: u64 = 1000;

/// Why the job stops before the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Stop {
    /// The row is this job's and the failure path runs with this reason.
    Fail(String),
    /// The shutdown token fired: the failure path runs with reason
    /// `shutdown`.
    Cancelled,
    /// The claim is lost or its heartbeat did not return: no further store
    /// or catalog access.
    ClaimLost(String),
    /// The store, as the runtime holds it, is unhealthy or is no longer the
    /// store the row records ([`Pipeline::store_check`]): FAILED with this
    /// reason and no store call, the prefix's deletion included.
    StoreUnhealthy(String),
}

/// What the pipeline wrote.
#[derive(Debug, Default)]
pub(crate) struct DataFiles {
    /// Each file's manifest entry with the etag the store reported.
    pub entries: Vec<(DataFileEntry, Option<String>)>,
    /// Items in every file.
    pub item_count: u64,
    /// The DynamoDB sizes of those items, summed (the manifest's
    /// `source_table.table_size_bytes`).
    pub item_bytes: u64,
    /// Bytes the store reported stored, over every file.
    pub bytes_uploaded: u64,
}

/// The claim the pipeline writes under, as the uploader and the feeder read
/// it: the backup job's heartbeat ([`HeartbeatHandle`]), or the maintenance
/// claim `backup migrate-legacy` holds.
pub(crate) trait ClaimWatch: Send + Sync {
    /// The pre-put check: `true` while the claim is confirmed held.
    fn confirms_the_claim(&self) -> bool;
    /// Cancelled once the claim is known to be lost.
    fn lost(&self) -> &CancellationToken;
    /// Why the claim is lost, once it is.
    fn reason(&self) -> String;
}

impl ClaimWatch for HeartbeatHandle {
    fn confirms_the_claim(&self) -> bool {
        HeartbeatHandle::confirms_the_claim(self)
    }

    fn lost(&self) -> &CancellationToken {
        HeartbeatHandle::lost(self)
    }

    fn reason(&self) -> String {
        HeartbeatHandle::reason(self)
    }
}

/// What the uploader needs from the job.
pub(crate) struct Pipeline<'a, W: ?Sized = HeartbeatHandle> {
    pub store: &'a dyn BackupStore,
    /// The store as the runtime holds it: its health and identity, read
    /// before every put.
    pub configured: &'a ConfiguredStore,
    /// The store's identity as the row records it.
    pub identity: &'a StoreIdentity,
    /// The backup's store prefix (`BackupArn::store_prefix`).
    pub prefix: &'a str,
    /// The ARN, for the log lines.
    pub arn: &'a str,
    /// The claim every put is checked against.
    pub heartbeat: &'a W,
    pub shutdown: &'a CancellationToken,
    pub metrics: Option<&'a MetricsCollector>,
    /// `data_file_target_bytes`.
    pub target_bytes: u64,
    /// The store's configured key prefix and the summary's bucket name
    /// (`BackupDocuments::build`).
    pub key_prefix: &'a str,
    pub bucket: &'a str,
}

impl<W: ?Sized> Pipeline<'_, W> {
    /// The check before every put: the store is healthy as the runtime holds
    /// it (the reconciler's check sets it) and its identity is the one the
    /// row records. A store marked unhealthy mid-job, or whose identity file
    /// now names another store (a root swapped under the job), stops the
    /// job before the put of `name` with the store's reason
    /// ([`Stop::StoreUnhealthy`]): the objects a job would write into such a
    /// store are never the backup the row describes.
    pub(crate) fn store_check(&self, name: &str) -> Result<(), Stop> {
        match self.configured.health() {
            StoreHealth::Healthy => {}
            StoreHealth::Checking => {
                return Err(Stop::StoreUnhealthy(format!(
                    "the {} backup store has no passed check; {name} was not written",
                    self.configured.kind
                )));
            }
            StoreHealth::Unhealthy(problem) => {
                return Err(Stop::StoreUnhealthy(format!(
                    "the {} backup store is unhealthy ({problem}); {name} was not written",
                    self.configured.kind
                )));
            }
        }
        match self.configured.identity() {
            Some(identity) if identity == *self.identity => Ok(()),
            Some(identity) => Err(Stop::StoreUnhealthy(format!(
                "the {} backup store's id is {} and the row's is {}; {name} was not written",
                self.configured.kind, identity.store_id, self.identity.store_id
            ))),
            None => Err(Stop::StoreUnhealthy(format!(
                "the {} backup store's id is unknown; {name} was not written",
                self.configured.kind
            ))),
        }
    }
}

/// The words between the count and the first item in [`refusal_reason`].
const REFUSED_ITEMS: &str = " item(s) cannot be restored; first: ";

/// The reason the FAILED row carries for refused items (module doc).
fn refusal_reason(count: u64, first: &RestorableItemError) -> String {
    let issue = if matches!(first.reason, RestorableItemReason::InvalidNumber(_)) {
        " (#618)"
    } else {
        ""
    };
    format!("{count}{REFUSED_ITEMS}{first}{issue}")
}

/// Whether a [`Stop::Fail`] reason is the validator's refusal of items
/// ([`refusal_reason`]) rather than a store or encoder failure.
pub(crate) fn is_item_refusal(reason: &str) -> bool {
    reason
        .split_once(REFUSED_ITEMS)
        .is_some_and(|(count, _)| !count.is_empty() && count.bytes().all(|b| b.is_ascii_digit()))
}

/// What the feeder found by the end of the stream.
#[derive(Default)]
struct FeedEnd {
    refusals: Option<(u64, RestorableItemError)>,
    stream_error: Option<StorageError>,
    interrupted: Option<Stop>,
}

/// The feeder (module doc). `stop` is shared with the encoders: the feeder
/// sets it at the first refusal, and stops sending when another part set
/// it. `failed` is the uploader's: cancelled when the upload failed, so the
/// feeder returns at once.
struct Feeder {
    tx: mpsc::Sender<Item>,
    key: RestorableKeySchema,
    limits: RestorableLimits,
    stop: Arc<AtomicBool>,
    failed: CancellationToken,
    lost: CancellationToken,
    shutdown: CancellationToken,
    arn: String,
}

impl Feeder {
    async fn run(self, mut items: BoxStream<'static, Result<Item, StorageError>>) -> FeedEnd {
        let Self {
            tx,
            mut key,
            limits,
            stop,
            failed,
            lost,
            shutdown,
            arn,
        } = self;
        let mut tx = Some(tx);
        let mut end = FeedEnd::default();
        loop {
            let next = tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    end.interrupted = Some(Stop::Cancelled);
                    break;
                }
                () = lost.cancelled() => {
                    end.interrupted = Some(Stop::ClaimLost(String::new()));
                    break;
                }
                () = failed.cancelled() => break,
                next = items.next() => next,
            };
            let mut item = match next {
                None => break,
                Some(Ok(item)) => item,
                Some(Err(e)) => {
                    end.stream_error = Some(e);
                    break;
                }
            };
            let refusal = if item_size_bytes(&item) > DEFAULT_MAX_ITEM_SIZE_BYTES {
                // An item over the default item limit: the validator's walk
                // (the number rule over every member, CPU work proportional
                // to the item's size) and then the line's measurement (up
                // to the cap's bytes) run on the blocking pool as the
                // encoder does; the item and the key come back with the
                // result.
                let measured = tokio::task::spawn_blocking(move || {
                    let refusal = validate_restorable_item(&item, &key, &limits)
                        .err()
                        .or_else(|| line_too_long(&item, &key));
                    (item, key, refusal)
                })
                .await;
                match measured {
                    Ok((back, key_back, refusal)) => {
                        item = back;
                        key = key_back;
                        refusal
                    }
                    Err(e) => {
                        end.stream_error = Some(StorageError::Internal(format!(
                            "checking an item over the default item limit: {e}"
                        )));
                        break;
                    }
                }
            } else {
                validate_restorable_item(&item, &key, &limits).err()
            };
            if let Some(e) = refusal {
                match &mut end.refusals {
                    None => {
                        tracing::error!("backup {arn}: item cannot be restored: {e}");
                        end.refusals = Some((1, e));
                        stop.store(true, Ordering::Release);
                        tx = None;
                    }
                    Some((count, _)) => {
                        *count += 1;
                        if *count <= REFUSAL_LOG_CAP {
                            tracing::error!("backup {arn}: item cannot be restored: {e}");
                        }
                    }
                }
                continue;
            }
            if end.refusals.is_some() {
                continue;
            }
            if stop.load(Ordering::Acquire) {
                break;
            }
            let Some(sender) = &tx else { break };
            if sender.send(item).await.is_err() {
                break;
            }
        }
        if let Some((count, _)) = &end.refusals
            && *count > REFUSAL_LOG_CAP
        {
            tracing::error!(
                "backup {arn}: {} more item(s) cannot be restored (not listed)",
                count - REFUSAL_LOG_CAP
            );
        }
        drop(items);
        end
    }
}

/// The line-cap rule (module doc) for an item over the default item limit:
/// its line is measured against `MAX_LINE_BYTES` through a counting writer
/// that holds no line ([`encode_item_line_len_bounded`]), and one whose
/// line would exceed the cap is refused with `LineTooLong`. The feeder
/// measures no item within the default limit: the cap is sized to hold
/// every line such an item can expand to (`MAX_LINE_BYTES`'s doc, pinned by
/// the format's own test). An item the serializer refuses is left to the
/// encoder, which reports it.
fn line_too_long(item: &Item, key: &RestorableKeySchema) -> Option<RestorableItemError> {
    match encode_item_line_len_bounded(item, MAX_LINE_BYTES) {
        Err(FormatError::LineTooLong { cap }) => Some(RestorableItemError {
            key: key.shown_key(item),
            path: String::new(),
            reason: RestorableItemReason::LineTooLong { cap },
        }),
        Ok(_) | Err(_) => None,
    }
}

const STOPPED: &str = "the pipeline stopped";

/// The encoder's sink: chunks of [`CHUNK_BYTES`] over the chunk channel, the
/// remainder on `flush`. A write after the pipeline stopped, or after the
/// upload ended, fails and poisons the encoder. Both failures are
/// `BrokenPipe`, never `Interrupted`: the encoder retries `Interrupted` on
/// every path (it is std's "try again" kind), and a stopped pipeline does
/// not come back, so an `Interrupted` here would spin the compressor's
/// trailer write for as long as the drain lasts.
struct ChunkSink {
    tx: mpsc::Sender<Bytes>,
    buf: BytesMut,
    stop: Arc<AtomicBool>,
}

impl ChunkSink {
    fn send(&self, chunk: Bytes) -> std::io::Result<()> {
        self.tx
            .blocking_send(chunk)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "the upload ended"))
    }
}

impl Write for ChunkSink {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.stop.load(Ordering::Acquire) {
            return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, STOPPED));
        }
        self.buf.extend_from_slice(data);
        while self.buf.len() >= CHUNK_BYTES {
            let chunk = self.buf.split_to(CHUNK_BYTES).freeze();
            self.send(chunk)?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            let chunk = self.buf.split().freeze();
            self.send(chunk)?;
        }
        Ok(())
    }
}

/// How one encoder ended.
enum FileEnd {
    /// No item reached the encoder: nothing was written.
    Empty,
    /// The file reached `data_file_target_bytes` and closed; the stream goes
    /// on.
    Closed {
        checksums: DataFileChecksums,
        range: Option<KeyRange>,
        /// The DynamoDB sizes of the file's items, summed.
        item_bytes: u64,
    },
    /// The stream ended inside this file, which closed.
    Ended {
        checksums: DataFileChecksums,
        range: Option<KeyRange>,
        item_bytes: u64,
    },
}

/// What an encoder hands back: the item channel and the tracker for the
/// next file, and how it ended.
type Encoded = (
    mpsc::Receiver<Item>,
    Option<KeyRangeTracker>,
    Result<FileEnd, FormatError>,
);

/// One data file's encoder (module doc), on the blocking pool.
fn encode_file(
    mut items: mpsc::Receiver<Item>,
    sink: ChunkSink,
    mut tracker: Option<KeyRangeTracker>,
    target_bytes: u64,
) -> Encoded {
    let stop = Arc::clone(&sink.stop);
    let mut encoder = DataFileEncoder::new(sink);
    let mut item_bytes: u64 = 0;
    let result = loop {
        if stop.load(Ordering::Acquire) {
            break Err(FormatError::Encode(STOPPED.to_owned()));
        }
        let Some(item) = items.blocking_recv() else {
            if encoder.item_count() == 0 {
                break Ok(FileEnd::Empty);
            }
            break close(&mut encoder, &mut tracker).map(|(checksums, range)| FileEnd::Ended {
                checksums,
                range,
                item_bytes,
            });
        };
        if let Some(tracker) = &mut tracker
            && let Err(e) = tracker.push(&item)
        {
            break Err(e);
        }
        if let Err(e) = encoder.write_item(&item) {
            break Err(e);
        }
        item_bytes += item_size_bytes(&item) as u64;
        if encoder.uncompressed_bytes() >= target_bytes {
            break close(&mut encoder, &mut tracker).map(|(checksums, range)| FileEnd::Closed {
                checksums,
                range,
                item_bytes,
            });
        }
    };
    // A finished encoder's drop writes nothing; an open one's drop takes
    // the sink out first, so no trailer reaches the upload.
    drop(encoder);
    (items, tracker, result)
}

/// Close the file: the trailer and the remainder go to the sink, the
/// tracker starts the next file.
fn close(
    encoder: &mut DataFileEncoder<ChunkSink>,
    tracker: &mut Option<KeyRangeTracker>,
) -> Result<(DataFileChecksums, Option<KeyRange>), FormatError> {
    let checksums = encoder.try_finish()?;
    Ok((
        checksums,
        tracker.as_mut().and_then(KeyRangeTracker::next_file),
    ))
}

/// The pipeline (module doc). `tracker` is `Some` for a key-ordered backup.
pub(crate) async fn write_data_files<W: ClaimWatch + ?Sized>(
    p: &Pipeline<'_, W>,
    items: BoxStream<'static, Result<Item, StorageError>>,
    key: RestorableKeySchema,
    limits: RestorableLimits,
    mut tracker: Option<KeyRangeTracker>,
) -> Result<DataFiles, Stop> {
    let stop = Arc::new(AtomicBool::new(false));
    let failed = CancellationToken::new();
    let (item_tx, item_rx) = mpsc::channel(ITEM_CHANNEL);
    let feeder = tokio::spawn(
        Feeder {
            tx: item_tx,
            key,
            limits,
            stop: Arc::clone(&stop),
            failed: failed.clone(),
            lost: p.heartbeat.lost().clone(),
            shutdown: p.shutdown.clone(),
            arn: p.arn.to_owned(),
        }
        .run(items),
    );
    let mut item_rx = Some(item_rx);
    let mut files = DataFiles::default();
    let mut index: u32 = 1;
    let uploaded: Result<(), Stop> = loop {
        let (chunk_tx, mut chunk_rx) = mpsc::channel::<Bytes>(CHUNK_CHANNEL);
        let sink = ChunkSink {
            tx: chunk_tx,
            buf: BytesMut::with_capacity(CHUNK_BYTES),
            stop: Arc::clone(&stop),
        };
        let rx = item_rx.take().expect("the previous encoder returned it");
        let file_tracker = tracker.take();
        let target = p.target_bytes;
        let encoder =
            tokio::task::spawn_blocking(move || encode_file(rx, sink, file_tracker, target));

        // The file's first chunk, or the channel closing with none.
        let first = tokio::select! {
            biased;
            () = p.shutdown.cancelled() => Err(Stop::Cancelled),
            () = p.heartbeat.lost().cancelled() => Err(Stop::ClaimLost(p.heartbeat.reason())),
            chunk = chunk_rx.recv() => Ok(chunk),
        };
        let first = match first {
            Ok(Some(first)) => first,
            Ok(None) => {
                let (_, _, end) = match encoder.await {
                    Ok(encoded) => encoded,
                    Err(e) => break Err(Stop::Fail(format!("the encoder task failed: {e}"))),
                };
                break match end {
                    Ok(FileEnd::Empty) => Ok(()),
                    Ok(_) => Err(Stop::Fail(format!(
                        "data file {index} closed without a chunk"
                    ))),
                    Err(_) if stop.load(Ordering::Acquire) => Ok(()),
                    Err(e) => Err(Stop::Fail(format!("encoding data file {index}: {e}"))),
                };
            }
            Err(why) => {
                stop.store(true, Ordering::Release);
                drop(chunk_rx);
                // The feeder first (module doc): an encoder waiting on the
                // item channel returns only when the feeder closes it, and
                // the feeder waits on the stream's next item until told.
                failed.cancel();
                let _ = encoder.await;
                break Err(why);
            }
        };

        let name = data_file_key(index);
        if let Err(why) = p.store_check(&name) {
            stop.store(true, Ordering::Release);
            drop(chunk_rx);
            failed.cancel();
            let _ = encoder.await;
            break Err(why);
        }
        if !p.heartbeat.confirms_the_claim() {
            stop.store(true, Ordering::Release);
            drop(chunk_rx);
            failed.cancel();
            let _ = encoder.await;
            break Err(Stop::ClaimLost(format!(
                "no heartbeat confirmed the claim recently enough for data file {index}"
            )));
        }
        let key = format!("{}{name}", p.prefix);
        let body: ByteStream = Box::pin(futures::stream::once(async move { Ok(first) }).chain(
            futures::stream::unfold(chunk_rx, |mut rx| async move {
                rx.recv().await.map(|chunk| (Ok(chunk), rx))
            }),
        ));
        let put = tokio::select! {
            biased;
            () = p.shutdown.cancelled() => Err(Stop::Cancelled),
            () = p.heartbeat.lost().cancelled() => Err(Stop::ClaimLost(p.heartbeat.reason())),
            result = p.store.put(&key, body) => result.map_err(|e| {
                if let Some(metrics) = p.metrics {
                    metrics.record_backup_store_error("put");
                }
                Stop::Fail(format!("writing {name}: {e}"))
            }),
        };
        let meta = match put {
            Ok(meta) => meta,
            Err(why) => {
                stop.store(true, Ordering::Release);
                // Before the encoder (module doc): a put that fails mid-file
                // leaves the encoder in `blocking_recv`, which returns only
                // once the feeder, told here, closes the channel.
                failed.cancel();
                let _ = encoder.await;
                break Err(why);
            }
        };
        // The put ended because the encoder dropped the sink, so the encoder
        // has returned or is about to.
        let (rx, file_tracker, end) = match encoder.await {
            Ok(encoded) => encoded,
            Err(e) => break Err(Stop::Fail(format!("the encoder task failed: {e}"))),
        };
        item_rx = Some(rx);
        tracker = file_tracker;
        let (checksums, range, item_bytes, last) = match end {
            Ok(FileEnd::Closed {
                checksums,
                range,
                item_bytes,
            }) => (checksums, range, item_bytes, false),
            Ok(FileEnd::Ended {
                checksums,
                range,
                item_bytes,
            }) => (checksums, range, item_bytes, true),
            Ok(FileEnd::Empty) => {
                break Err(Stop::Fail(format!(
                    "data file {index} sent a chunk and then reported no item"
                )));
            }
            Err(_) if stop.load(Ordering::Acquire) => break Ok(()),
            Err(e) => break Err(Stop::Fail(format!("encoding data file {index}: {e}"))),
        };
        // The store's size is the manifest's: the reconciler's dead-owner
        // probe refuses a data file whose stored size differs from the
        // recorded one, so a live job must not commit a manifest that probe
        // would call incomplete.
        if meta.size != checksums.compressed_bytes {
            break Err(Stop::Fail(format!(
                "the store reports {} bytes for {name} and the encoder wrote {}",
                meta.size, checksums.compressed_bytes
            )));
        }
        files.item_count += checksums.item_count;
        files.item_bytes += item_bytes;
        files.bytes_uploaded += meta.size;
        files.entries.push((
            DataFileEntry::from_checksums(name, &checksums, range),
            meta.etag,
        ));
        if index == 1 {
            kill_point(KillPoint::AfterDataFile, p.shutdown).await;
        }
        if last {
            break Ok(());
        }
        index += 1;
    };
    stop.store(true, Ordering::Release);
    if uploaded.is_err() {
        // The uploader's own failure: the feeder has nothing left to check.
        failed.cancel();
    }
    drop(item_rx);
    let end = match feeder.await {
        Ok(end) => end,
        Err(e) => return Err(Stop::Fail(format!("the snapshot reader task failed: {e}"))),
    };
    if let Err(why @ (Stop::Cancelled | Stop::ClaimLost(_) | Stop::StoreUnhealthy(_))) = uploaded {
        return Err(why);
    }
    match end.interrupted {
        Some(Stop::Cancelled) => return Err(Stop::Cancelled),
        Some(Stop::ClaimLost(_)) => return Err(Stop::ClaimLost(p.heartbeat.reason())),
        Some(Stop::Fail(_) | Stop::StoreUnhealthy(_)) | None => {}
    }
    match (end.refusals, end.stream_error) {
        // A stream that fails after a refusal (the pass that checks the rest
        // reads the whole table) keeps the refusals in the reason, counted
        // over the items read before the failure, so `backup list --failed`
        // still names the first refused item.
        (Some((count, first)), Some(e)) => {
            return Err(Stop::Fail(format!(
                "{}; the snapshot then failed: {e}",
                refusal_reason(count, &first)
            )));
        }
        (None, Some(e)) => return Err(Stop::Fail(format!("reading the snapshot: {e}"))),
        (Some((count, first)), None) => {
            return Err(Stop::Fail(refusal_reason(count, &first)));
        }
        (None, None) => {}
    }
    uploaded.map(|()| files)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use extenddb_core::types::AttributeValue;

    use super::*;

    fn stopped_sink() -> ChunkSink {
        let (tx, _rx) = mpsc::channel(CHUNK_CHANNEL);
        ChunkSink {
            tx,
            buf: BytesMut::with_capacity(CHUNK_BYTES),
            stop: Arc::new(AtomicBool::new(true)),
        }
    }

    /// A write after the stop is `BrokenPipe`, the kind nothing retries.
    #[test]
    fn a_write_after_the_stop_is_a_broken_pipe() {
        let e = stopped_sink().write(b"x").unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(e.to_string(), STOPPED);
    }

    /// An encoder holding items when the pipeline stops fails its close at
    /// once instead of retrying the trailer write for the whole drain: the
    /// shutdown of a job in the middle of a large table ends within the
    /// server's drain window.
    #[test]
    fn closing_an_encoder_after_the_stop_fails_at_once() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut encoder = DataFileEncoder::new(stopped_sink());
            let item = Item::from([("pk".to_owned(), AttributeValue::S("a".to_owned()))]);
            // The gzip header is the first write to reach the sink.
            let written = encoder.write_item(&item);
            let closed = encoder.try_finish().map(|_| ());
            drop(encoder);
            let _ = done_tx.send((written, closed));
        });
        let (written, closed) = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the close returned within the window");
        let e = written.and(closed).unwrap_err();
        assert!(e.to_string().contains(STOPPED), "{e}");
    }
}
