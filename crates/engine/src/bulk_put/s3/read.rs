// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Reading one input object: sequential ranged GETs of 8 MiB, ending at
//! the enumerated size, behind a blocking `Read` the shared decode chain
//! consumes on the blocking pool. Every GET carries `If-Match` with the
//! `ETag` the listing gave at Start, the first range of an open and of a
//! resumed open included, so a read never takes bytes of an object other
//! than the one listed and one read never mixes two versions. A 412 means
//! the listed object is no longer at the key: the line reader reports the
//! file as overwritten (`SourceError::Overwritten`), as the service does
//! for an object overwritten after its listing (measured on the service).
//! For a record the listing gave no `ETag`, the first range goes without
//! `If-Match` and later ranges carry the first answer's `ETag`, and a first
//! answer that gives a size other than the listed one is reported as
//! overwritten too: the total of its `Content-Range`, or for an answer
//! without one that carries the whole object, its length. A replacement of
//! the same size is then caught within one read but not across a resume,
//! whose first range again takes the object as it is. A fault on the first
//! range is reported with `after_first_byte` false, on a later one true. A
//! body shorter than its `Content-Length`, or shorter than the range when
//! the object does not end there, is the server's failure, never an end of
//! stream: under a codec an early end would read as a corrupt stream.
//!
//! The ranged reader runs on the blocking pool, which a dropped future
//! does not stop. Each opened object therefore has a stop signal: the open
//! holds its sending half until it returns and then hands it to the line
//! reader. Dropping the open, the line reader, or a `next_line` call before
//! it answers fires the signal, and the ranged reader races every GET, its
//! retries and their waits against it, so a read the caller gave up on
//! drops the request in flight, sends no further one, and the blocking
//! fill returns at once.

use std::io::{BufRead, Read};
use std::sync::Arc;

use aws_sdk_s3::operation::RequestId;
use tokio::sync::watch;

use extenddb_storage::BoxedFuture as BoxFuture;
use extenddb_storage::InputFile;

use super::error::{Outcome, Tail, classify};
use super::retry::Backoff;
use super::{Location, S3Clients, source_error, unquoted};
use crate::bulk_put::source::{
    ChainLineReader, InputCodec, LineReader, SourceError, StoreFault, StoreUnavailable, line_reader,
};

/// Bytes asked for per ranged GET.
pub(super) const GET_PART_BYTES: u64 = 8 * 1024 * 1024;

/// One object being read.
struct Object {
    clients: Arc<S3Clients>,
    location: Location,
    key: String,
}

/// The bytes one GET gave.
struct Part {
    data: Vec<u8>,
    /// The response's `ETag`, quoted as S3 sends it.
    etag: Option<String>,
    request_id: Option<String>,
    /// The object ends with these bytes.
    last: bool,
    /// The object's size as the answer's `Content-Range` gives it, or the
    /// body's length for an answer without the header that carries the
    /// whole object; `None` otherwise and with a `*` total.
    total: Option<u64>,
}

/// Why a GET gave no bytes.
enum Miss {
    /// The range starts at or past the object's end (416).
    PastEnd,
    Fault(SourceError),
}

/// The server's failure reading `object`, marked `transient` when it outlasted the retries.
fn internal(object: &Object, detail: &str, transient: bool) -> Miss {
    tracing::error!(
        account_id = %object.location.account,
        bucket = %object.location.bucket,
        key = %object.key,
        detail,
        "S3 read failed"
    );
    let text = format!("S3 read failed: {detail}");
    Miss::Fault(SourceError::Io(if transient {
        StoreUnavailable::io(text)
    } else {
        std::io::Error::other(text)
    }))
}

/// `bytes {first}-{last}/{total}` from a `Content-Range` header; the total
/// is `None` when the service sends `*`.
fn content_range(header: &str) -> Option<(u64, Option<u64>)> {
    let (span, total) = header.strip_prefix("bytes ")?.split_once('/')?;
    let (first, _) = span.split_once('-')?;
    Some((first.parse().ok()?, total.parse().ok()))
}

/// A GET's 412: the only precondition a GET sends is `If-Match` with the
/// listed (or first) `ETag`, so the object at the key is no longer the one
/// the operation listed. The store renders it as a store fault, which
/// crosses the decode chain like any other, and the line reader hands it
/// to the runner as `SourceError::Overwritten`, at the open and at a later
/// range alike, so the runner records the file as overwritten, as the
/// service does.
fn listed_object_gone(error: SourceError) -> SourceError {
    match error {
        SourceError::Store(fault) if fault.code == "S3PreconditionFailed" => {
            SourceError::Overwritten
        }
        other => other,
    }
}

/// GET bytes `start..=end` of the object. Every GET carries a `Range`, an
/// object listed at size 0 included (`bytes=0-0`, which S3 answers 416 for
/// an empty object, read as end of file), so what a GET keeps is bounded by
/// the range whatever the object holds when it is read. The body is read
/// to at most one byte past the range, enough to tell a longer body from
/// one that fits, and the rest is never read. A body cut mid-stream is
/// requested again for the same range with the same `If-Match`, under the
/// bound of the call's own retries (`retry`).
async fn get_range(
    object: &Object,
    start: u64,
    end: u64,
    if_match: Option<&str>,
    after_first_byte: bool,
) -> Result<Part, Miss> {
    let mut backoff = object.clients.backoff();
    loop {
        let cut = match fetch(object, start, end, if_match, after_first_byte, &mut backoff).await {
            Fetched::Done(result) => return result,
            Fetched::Cut(detail) => detail,
        };
        let Some(wait) = backoff.next_wait() else {
            return Err(internal(object, &cut, true));
        };
        tracing::warn!(
            account_id = %object.location.account,
            bucket = %object.location.bucket,
            key = %object.key,
            range = %format!("bytes={start}-{end}"),
            retry = backoff.retries(),
            wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            detail = %cut,
            "S3 read body was cut; the range is requested again after the wait"
        );
        tokio::time::sleep(wait).await;
    }
}

/// One GET of a range: its outcome, or a body cut before the range ended.
enum Fetched {
    Done(Result<Part, Miss>),
    Cut(String),
}

async fn fetch(
    object: &Object,
    start: u64,
    end: u64,
    if_match: Option<&str>,
    after_first_byte: bool,
    backoff: &mut Backoff,
) -> Fetched {
    let range = format!("bytes={start}-{end}");
    let sent = object
        .clients
        .call_within(
            &object.location.bucket,
            backoff,
            &|client: aws_sdk_s3::Client| {
                client
                    .get_object()
                    .bucket(&object.location.bucket)
                    .key(&object.key)
                    .range(range.clone())
                    .set_if_match(if_match.map(str::to_owned))
                    .set_expected_bucket_owner(object.location.owner.clone())
                    .send()
            },
        )
        .await;
    let output = match sent {
        Ok(output) => output,
        Err(error) => {
            let miss = if matches!(
                classify(&error, Tail::AmazonS3, after_first_byte),
                Outcome::PastEnd
            ) {
                Miss::PastEnd
            } else {
                Miss::Fault(source_error(
                    &error,
                    &object.location,
                    &object.key,
                    after_first_byte,
                    "read",
                ))
            };
            return Fetched::Done(Err(miss));
        }
    };
    let etag = output.e_tag().map(str::to_owned);
    let request_id = output.request_id().map(str::to_owned);
    let range_header = output.content_range().map(str::to_owned);
    let requested = end - start + 1;
    let mut data = match bounded_body(output.body, requested.saturating_add(1)).await {
        Ok(data) => data,
        Err(detail) => return Fetched::Cut(detail),
    };
    let got = data.len() as u64;
    let endpoint_fault = |detail: &str| Fetched::Done(Err(internal(object, detail, false)));
    let range_total = match range_header.as_deref().and_then(content_range) {
        Some((_, total)) => total,
        // A whole-object answer gives the object's size by its length.
        None if start == 0 && got <= requested => Some(got),
        None => None,
    };
    let last = match range_header.as_deref().and_then(content_range) {
        Some((first, _)) if first != start => {
            return endpoint_fault("Content-Range does not start at the range asked for");
        }
        Some((_, total)) => {
            if got > requested {
                data.truncate(usize::try_from(requested).unwrap_or(usize::MAX));
            } else if got < requested && total != Some(start + got) {
                // Only the object's end may cut a range short.
                return Fetched::Cut("body shorter than the range asked for".to_owned());
            }
            total.is_some_and(|total| start + data.len() as u64 >= total)
        }
        // A service that ignores Range answers the whole object; that
        // places the bytes only for a read from the start, and only when
        // the whole object fits in the range asked for.
        None if start == 0 && got <= requested => true,
        None if start == 0 => {
            return endpoint_fault(
                "an answer without Content-Range is longer than the range asked for",
            );
        }
        None => return endpoint_fault("ranged GET answered without Content-Range"),
    };
    Fetched::Done(Ok(Part {
        data,
        etag,
        request_id,
        last,
        total: range_total,
    }))
}

/// At most `limit` bytes of a response body; the rest is never read. The
/// SDK fails the body of a response that ends before its Content-Length,
/// and its stalled-stream protection a body that stops; both errors land
/// here, as the detail of a cut body.
async fn bounded_body(
    mut body: aws_sdk_s3::primitives::ByteStream,
    limit: u64,
) -> Result<Vec<u8>, String> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut data = Vec::new();
    while data.len() < limit {
        match body.next().await {
            None => break,
            Some(Ok(chunk)) => {
                let room = limit - data.len();
                data.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Some(Err(error)) => return Err(format!("response body: {error}")),
        }
    }
    Ok(data)
}

/// Resolves once the object's reads are to stop: the signal was fired, or
/// its sending half was dropped.
async fn stopped(mut stop: watch::Receiver<bool>) {
    let _ = stop.wait_for(|stopped| *stopped).await;
}

/// The fault of a read stopped by its signal. Nobody reads it: the caller
/// that would have has gone.
fn stopped_read() -> Miss {
    Miss::Fault(SourceError::Io(std::io::Error::other(
        "S3 read stopped: its reader was dropped",
    )))
}

/// Fires the stop signal when dropped, unless disarmed first.
struct StopOnDrop<'a>(Option<&'a watch::Sender<bool>>);

impl StopOnDrop<'_> {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(stop) = self.0 {
            stop.send_replace(true);
        }
    }
}

/// The blocking `Read` over the object's ranged GETs, run by the decode
/// chain on the blocking pool; each GET is driven on the runtime through
/// `Handle::block_on`, raced against the object's stop signal.
struct RangedObject {
    object: Arc<Object>,
    runtime: tokio::runtime::Handle,
    stop: watch::Receiver<bool>,
    size: u64,
    next: u64,
    buffer: Vec<u8>,
    pos: usize,
    /// The `ETag` sent as `If-Match` on every later range: the listed one,
    /// or the first answer's for a record listed without one.
    if_match: Option<String>,
    done: bool,
}

impl RangedObject {
    fn accept(&mut self, start: u64, result: Result<Part, Miss>) -> Result<(), SourceError> {
        self.pos = 0;
        match result {
            Ok(part) => {
                self.next = start.saturating_add(part.data.len() as u64);
                self.done = part.last || part.data.is_empty();
                self.buffer = part.data;
                Ok(())
            }
            Err(Miss::PastEnd) => {
                self.done = true;
                self.buffer = Vec::new();
                Ok(())
            }
            Err(Miss::Fault(error)) => Err(error),
        }
    }
}

impl Read for RangedObject {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.buffer.len() {
            if self.done || self.next >= self.size {
                return Ok(0);
            }
            let start = self.next;
            let part_bytes = self.object.clients.part_bytes();
            let end = start.saturating_add(part_bytes).min(self.size) - 1;
            let object = Arc::clone(&self.object);
            let if_match = self.if_match.clone();
            let stop = self.stop.clone();
            // The signal is polled first, so a read stopped before this
            // range sends nothing; once it fires, the GET in flight or the
            // wait before its retry is dropped.
            let result = self.runtime.block_on(async move {
                tokio::select! {
                    biased;
                    () = stopped(stop) => Err(stopped_read()),
                    result = get_range(&object, start, end, if_match.as_deref(), true) => result,
                }
            });
            // A rendered fault travels to the chain inside the I/O error,
            // which hands it on as `SourceError::Store`.
            self.accept(start, result).map_err(|error| match error {
                SourceError::Store(fault) => std::io::Error::other(fault),
                SourceError::Io(io) => io,
                other => std::io::Error::other(format!("{other:?}")),
            })?;
        }
        let n = buf.len().min(self.buffer.len() - self.pos);
        buf[..n].copy_from_slice(&self.buffer[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// The line reader of one S3 object: the shared chain, reporting at end of
/// file the `ETag` of the read (the first range's, which the service's
/// manifest records for a file read to its end) instead of the digest of
/// the bytes.
pub(super) struct S3LineReader {
    chain: ChainLineReader,
    etag: String,
    /// The sending half of the object's stop signal; dropped with the
    /// reader, it stops the object's reads.
    stop: watch::Sender<bool>,
}

impl LineReader for S3LineReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            // A call dropped before it answers stops the object's reads:
            // the blocking fill it started stays with the decode chain, and
            // the next call awaits that fill, which ends with the stop as
            // its fault after the lines it read.
            let armed = StopOnDrop(Some(&self.stop));
            let line = self
                .chain
                .next_line(max_line_bytes)
                .await
                .map_err(listed_object_gone);
            armed.disarm();
            line
        })
    }

    fn bytes_consumed(&self) -> u64 {
        self.chain.bytes_consumed()
    }

    fn last_line_end(&self) -> u64 {
        self.chain.last_line_end()
    }

    fn etag(&self) -> Option<String> {
        self.chain.etag().map(|_| self.etag.clone())
    }

    fn opened_etag(&self) -> Option<String> {
        Some(self.etag.clone())
    }
}

fn enumerated_size(file: &InputFile) -> u64 {
    u64::try_from(file.size).unwrap_or(0)
}

/// Open one enumerated object: fetch its first range, then build the
/// decode chain over the ranged reader.
pub(super) async fn open(
    clients: Arc<S3Clients>,
    location: Location,
    file: &InputFile,
    compression: InputCodec,
) -> Result<S3LineReader, SourceError> {
    let size = enumerated_size(file);
    // Held by this future until it returns and then by the line reader,
    // so a dropped open stops the reads it started too.
    let (stop, stop_signal) = watch::channel(false);
    let object = Arc::new(Object {
        clients,
        location,
        key: file.path.clone(),
    });
    // An object listed at size 0 is fetched too, as one byte: a missing,
    // denied or replaced object is then refused at its open as any other
    // object is, and the read keeps none of the bytes.
    let first_end = size.min(object.clients.part_bytes()).max(1) - 1;
    let listed = file.etag.as_deref().map(|etag| format!("\"{etag}\""));
    let first = get_range(&object, 0, first_end, listed.as_deref(), false).await;
    // Without a listed `ETag` the listed size is what the operation knows of
    // the object, so a first answer that gives another size is an object
    // replaced since the listing, recorded as for a 412.
    if listed.is_none()
        && let Ok(Part {
            total: Some(total), ..
        }) = &first
        && *total != size
    {
        return Err(SourceError::Overwritten);
    }
    let (read_etag, request_id) = match &first {
        Ok(part) => (part.etag.clone(), part.request_id.clone()),
        Err(_) => (None, None),
    };
    let mut ranged = RangedObject {
        object: Arc::clone(&object),
        runtime: tokio::runtime::Handle::current(),
        stop: stop_signal,
        size,
        next: 0,
        buffer: Vec::new(),
        pos: 0,
        if_match: listed.or_else(|| read_etag.clone()),
        done: false,
    };
    ranged.accept(0, first).map_err(listed_object_gone)?;
    // The audit event the import and export operations emit, with the
    // object read. Keys stay in the log and never reach a client.
    tracing::info!(
        account_id = %object.location.account,
        operation = "StartBulkPutItems",
        object = %format!("s3://{}/{}", object.location.bucket, object.key),
        request_id = request_id.as_deref().unwrap_or("null"),
        "import/export file access"
    );
    let etag = read_etag
        .as_deref()
        .map(unquoted)
        .or_else(|| file.etag.clone())
        .unwrap_or_default();
    let chain =
        tokio::task::spawn_blocking(move || line_reader(Box::new(ranged), size, compression))
            .await
            .map_err(|e| SourceError::Io(std::io::Error::other(e)))??;
    Ok(S3LineReader { chain, etag, stop })
}

/// The entity tag for the manifest: the `ETag` listed at Start, or, for a
/// record without one, the `ETag` of a GET of the first byte. Only the
/// header is used and the body is not read, so a service that ignores
/// `Range` and answers the whole object gives the same tag.
pub(super) async fn etag(
    clients: &Arc<S3Clients>,
    location: &Location,
    file: &InputFile,
) -> Result<String, SourceError> {
    if let Some(etag) = &file.etag {
        return Ok(etag.clone());
    }
    let object = Object {
        clients: Arc::clone(clients),
        location: location.clone(),
        key: file.path.clone(),
    };
    let mut backoff = object.clients.backoff();
    let sent = object
        .clients
        .call_within(
            &object.location.bucket,
            &mut backoff,
            &|client: aws_sdk_s3::Client| {
                client
                    .get_object()
                    .bucket(&object.location.bucket)
                    .key(&object.key)
                    .range("bytes=0-0")
                    .set_expected_bucket_owner(object.location.owner.clone())
                    .send()
            },
        )
        .await;
    match sent {
        Ok(output) => Ok(output.e_tag().map(unquoted).unwrap_or_default()),
        Err(error) if matches!(classify(&error, Tail::AmazonS3, false), Outcome::PastEnd) => {
            Ok(String::new())
        }
        Err(error) => Err(source_error(
            &error,
            &object.location,
            &object.key,
            false,
            "read",
        )),
    }
}

/// The number of physical lines in one object, read the way the runner
/// reads it.
pub(super) async fn line_count(
    clients: Arc<S3Clients>,
    location: Location,
    file: &InputFile,
    compression: InputCodec,
) -> Result<u64, SourceError> {
    let reader = open(clients, location, file, compression).await?;
    // The stop signal stays with this future, so a dropped count stops
    // the reads of the blocking task below.
    let S3LineReader { chain, stop, .. } = reader;
    let counted = tokio::task::spawn_blocking(move || {
        let Some(mut inner) = chain.into_inner() else {
            return Ok(0);
        };
        let mut count: u64 = 0;
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match inner.read_until(b'\n', &mut buf) {
                Ok(0) => break,
                Ok(_) => count += 1,
                Err(e) => {
                    return Err(
                        StoreFault::of_io(&e).map_or(SourceError::Io(e), SourceError::Store)
                    );
                }
            }
        }
        Ok(count)
    })
    .await
    .map_err(|e| SourceError::Io(std::io::Error::other(e)))?;
    drop(stop);
    counted
}
