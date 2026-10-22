// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data file encoding and decoding.
//!
//! A data file is gzip-compressed newline-delimited JSON, one
//! `{"Item": <DynamoDB JSON item>}` object per line, the encoding Amazon
//! `DynamoDB` writes for `DYNAMODB_JSON` exports and the same line encoding
//! the engine's `ExportTableToPointInTime` handler produces (a
//! `serde_json::json!({"Item": item})` wrapper serialized compactly, then a
//! newline), so the two agree byte for byte.
//!
//! Checksums: `sha256` and `md5` are computed over the compressed bytes, the
//! object as stored, which is what `extenddb-manifest.json` records per data
//! file and what `manifest-files.json` records as `md5Checksum`. Item and
//! uncompressed byte counts are computed over the line stream.
//!
//! Memory: [`DataFileEncoder`] writes through any [`Write`] and hashes the
//! compressed bytes as they leave the compressor, so a caller streaming to a
//! file or an upload holds the gzip window and one serialized line, not the
//! file. [`DataFileDecoder`] reads from any [`Read`], hashes the compressed
//! bytes as they are consumed, and checks the digests when the stream ends.
//! A caller that must not act on any item before the digests are known (a
//! restore) runs the bytes through [`CompressedHasher`] first, keeping a copy
//! on local disk, and decodes from that copy; [`DataFileDecoder::verified`]
//! does both steps for a slice already in memory.

use std::io::{BufRead, BufReader, Read, Write};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use flate2::Compression;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::FormatError;
use crate::types::Item;

/// Maximum length of a single decompressed data-file line, in bytes.
///
/// One line holds one item wrapped as `{"Item": <DynamoDB JSON item>}` plus a
/// trailing newline. A `DynamoDB` item is capped at 400 KB. The `DYNAMODB_JSON`
/// wire form inflates that: every binary (`B`, `BS`) value is base64 (about
/// 4/3), attribute names and the `{"S":...}` / `{"N":...}` type tags repeat per
/// value, and the `{"Item":...}` wrapper adds a fixed frame. Four megabytes is
/// an order of magnitude above the worst realistic expansion of a 400 KB item,
/// so it never rejects a legitimate line, while it bounds the memory a single
/// `next_item` call can allocate from a malicious or corrupt gzip stream (a
/// gzip member can expand about 1030 to 1, so an unbounded read of a small
/// object can otherwise allocate gigabytes). A line longer than the cap
/// (newline included) yields [`FormatError::LineTooLong`] before the whole
/// line is materialized; a line of exactly the cap is accepted.
///
/// A data file has no blank lines: every line is one item. A blank line is
/// a [`FormatError::Json`] like any other unparseable line.
pub const MAX_LINE_BYTES: usize = 4 * 1024 * 1024;

/// Lowercase hex encoding of a byte slice.
pub(super) fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Strip a single trailing `\n` and an optional preceding `\r` from a line.
fn strip_trailing_newline(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// The single-key wrapper each data file line holds.
#[derive(Deserialize)]
struct ItemLine {
    #[serde(rename = "Item")]
    item: Item,
}

/// Borrowing form of [`ItemLine`] for the writer, so an item is serialized in
/// place rather than cloned into a `serde_json::Value`.
#[derive(Serialize)]
struct ItemLineRef<'a> {
    #[serde(rename = "Item")]
    item: &'a Item,
}

/// Checksums and counts produced while encoding a data file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileChecksums {
    /// Number of items encoded.
    pub item_count: u64,
    /// Lowercase hex sha256 of the compressed bytes.
    pub sha256_hex: String,
    /// Base64 MD5 of the compressed bytes, the `md5Checksum` encoding
    /// `manifest-files.json` uses.
    pub md5_base64: String,
    /// Size of the compressed output.
    pub compressed_bytes: u64,
    /// Total size of the uncompressed lines, newlines included.
    pub uncompressed_bytes: u64,
}

/// What a decoder should verify. Only the members that are `Some` are
/// checked, so a caller holding just the `md5Checksum` from a
/// `manifest-files.json` line can still verify what it has.
#[derive(Debug, Clone, Default)]
pub struct DataFileExpectations {
    /// Expected lowercase hex sha256 of the compressed bytes.
    pub sha256_hex: Option<String>,
    /// Expected base64 MD5 of the compressed bytes.
    pub md5_base64: Option<String>,
    /// Expected item count.
    pub item_count: Option<u64>,
    /// Expected uncompressed byte count.
    pub uncompressed_bytes: Option<u64>,
}

impl From<&DataFileChecksums> for DataFileExpectations {
    fn from(checksums: &DataFileChecksums) -> Self {
        Self {
            sha256_hex: Some(checksums.sha256_hex.clone()),
            md5_base64: Some(checksums.md5_base64.clone()),
            item_count: Some(checksums.item_count),
            uncompressed_bytes: Some(checksums.uncompressed_bytes),
        }
    }
}

/// Digest of a data file's compressed bytes: what the manifests record and
/// what a reader verifies before trusting the file's contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressedDigest {
    /// Lowercase hex sha256 of the compressed bytes.
    pub sha256_hex: String,
    /// Base64 MD5 of the compressed bytes, the `md5Checksum` encoding
    /// `manifest-files.json` uses.
    pub md5_base64: String,
    /// Number of compressed bytes.
    pub bytes: u64,
}

/// Incremental hasher over compressed bytes. Feed it the bytes of a data
/// file as they arrive (from a download, a file read, or the compressor) and
/// call [`CompressedHasher::finish`] for the digest.
#[derive(Default)]
pub struct CompressedHasher {
    sha256: Sha256,
    md5: Md5,
    bytes: u64,
}

impl CompressedHasher {
    /// A hasher with nothing fed yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed bytes.
    pub fn update(&mut self, bytes: &[u8]) {
        self.sha256.update(bytes);
        self.md5.update(bytes);
        self.bytes += bytes.len() as u64;
    }

    /// Finish and return the digest.
    #[must_use]
    pub fn finish(self) -> CompressedDigest {
        CompressedDigest {
            sha256_hex: hex_encode(&self.sha256.finalize()),
            md5_base64: BASE64.encode(self.md5.finalize()),
            bytes: self.bytes,
        }
    }

    /// Digest of a slice, in one call.
    #[must_use]
    pub fn digest(bytes: &[u8]) -> CompressedDigest {
        let mut hasher = Self::new();
        hasher.update(bytes);
        hasher.finish()
    }
}

impl DataFileExpectations {
    /// Check the digest fields that are set against a computed digest.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::ChecksumMismatch`] naming the first digest that
    /// differs.
    pub fn verify_digest(&self, digest: &CompressedDigest) -> Result<(), FormatError> {
        if let Some(expected) = &self.sha256_hex
            && expected != &digest.sha256_hex
        {
            return Err(FormatError::ChecksumMismatch {
                subject: "data file sha256",
                expected: expected.clone(),
                computed: digest.sha256_hex.clone(),
            });
        }
        if let Some(expected) = &self.md5_base64
            && expected != &digest.md5_base64
        {
            return Err(FormatError::ChecksumMismatch {
                subject: "data file md5",
                expected: expected.clone(),
                computed: digest.md5_base64.clone(),
            });
        }
        Ok(())
    }
}

/// A [`Write`] that hashes everything written through it before passing it
/// on. The compressor writes into this, so the digest covers the bytes as
/// stored.
///
/// `inner` is taken out by [`DataFileEncoder::into_inner`], and by the
/// encoder's `Drop` when the encoder is poisoned; the compressor's own `Drop`
/// then writes into nothing and its `BrokenPipe` is ignored, which is the
/// documented flate2 behaviour for a dropped encoder.
struct HashingWriter<W: Write> {
    inner: Option<W>,
    hasher: CompressedHasher,
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let Some(inner) = self.inner.as_mut() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "sink already taken",
            ));
        };
        let written = inner.write(buf)?;
        // `Write::write` may return `Ok(0)` for a sink that accepts no more
        // bytes. flate2's header and trailer writers loop until their bytes
        // are accepted, so passing `Ok(0)` through would spin forever; std's
        // `write_all` turns it into `WriteZero` for the same reason.
        if written == 0 && !buf.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "sink accepted no bytes",
            ));
        }
        self.hasher.update(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.inner.as_mut() {
            Some(inner) => inner.flush(),
            None => Ok(()),
        }
    }
}

/// A [`Read`] that hashes everything read through it. The decompressor reads
/// from this, so the digest covers exactly the compressed bytes consumed.
struct HashingReader<R: Read> {
    inner: R,
    hasher: CompressedHasher,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        Ok(read)
    }
}

/// Streaming encoder: serializes items as `{"Item": ...}` lines into a gzip
/// stream written to `W`, hashing the compressed output as it goes.
///
/// The gzip level is fixed at [`GZIP_LEVEL`] so two runs of the same build
/// encoding the same items produce the same bytes and checksums (compression
/// is not part of the format contract; the output also depends on the flate2
/// backend and version, which `Cargo.lock` pins).
///
/// A write error poisons the encoder: part of a line may have entered the
/// compressor, so the stream is damaged and no later write or `finish` can
/// produce a valid file. Every later call returns [`FormatError::Encode`];
/// the caller discards the file and starts it again from its first item.
/// Dropping a poisoned encoder writes nothing more into the sink: the sink
/// is taken out first, so the compressor's own `Drop` has nowhere to flush
/// its buffered data and trailer. A caller whose sink needs cleanup (an
/// upload to abort) takes it back with [`Self::into_inner`] before dropping.
pub struct DataFileEncoder<W: Write> {
    encoder: GzEncoder<HashingWriter<W>>,
    item_count: u64,
    uncompressed_bytes: u64,
    state: EncoderState,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EncoderState {
    /// Accepting items.
    Open,
    /// `try_finish` succeeded; the trailer is written and no item may follow.
    Finished,
    /// A write or finish failed; the stream is damaged.
    Poisoned,
}

/// gzip compression level the encoder uses: flate2's default, pinned so two
/// runs of one build produce identical bytes for identical items. Measured
/// once on a 16-core Xeon with 1M synthetic 318-byte `DynamoDB` JSON items
/// (318 MB, localhost, single thread) using the `gzip` CLI (zlib), not the
/// `miniz_oxide` backend flate2 uses here, so the absolute figures transfer
/// only approximately: level 6 ran at about 50 MB/s with a 21% output ratio;
/// level 1 was 1.6x faster at 25%; level 9 was 4x slower for 20%. Ratios
/// depend on the data; the level choice depends only on the ratios between
/// levels.
pub const GZIP_LEVEL: u32 = 6;

const POISONED: &str = "encoder poisoned by an earlier write error; discard the file";
const FINISHED: &str = "encoder already finished; no item may follow the trailer";

impl<W: Write> DataFileEncoder<W> {
    /// Start a data file written to `sink`.
    #[must_use]
    pub fn new(sink: W) -> Self {
        Self {
            encoder: GzEncoder::new(
                HashingWriter {
                    inner: Some(sink),
                    hasher: CompressedHasher::new(),
                },
                Compression::new(GZIP_LEVEL),
            ),
            item_count: 0,
            uncompressed_bytes: 0,
            state: EncoderState::Open,
        }
    }

    fn check_open(&self) -> Result<(), FormatError> {
        match self.state {
            EncoderState::Open => Ok(()),
            EncoderState::Finished => Err(FormatError::Encode(FINISHED.to_owned())),
            EncoderState::Poisoned => Err(FormatError::Encode(POISONED.to_owned())),
        }
    }

    /// Append one item as a line.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when the item fails to serialize (nothing
    /// has been written; the encoder stays usable) and [`FormatError::Encode`]
    /// when the compressor or sink fails, after which the encoder is poisoned,
    /// or when the encoder is already finished or poisoned.
    pub fn write_item(&mut self, item: &Item) -> Result<(), FormatError> {
        self.check_open()?;
        // The exact line encoding the engine's export handler produces.
        let mut line = serde_json::to_vec(&ItemLineRef { item })?;
        line.push(b'\n');
        if let Err(e) = self.encoder.write_all(&line) {
            self.state = EncoderState::Poisoned;
            return Err(FormatError::Encode(e.to_string()));
        }
        self.item_count += 1;
        self.uncompressed_bytes += line.len() as u64;
        Ok(())
    }

    /// Whether a write or finish error has made this file unusable. False
    /// after a successful [`Self::try_finish`]; see [`Self::is_finished`].
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.state == EncoderState::Poisoned
    }

    /// Whether [`Self::try_finish`] has succeeded, so the file is complete
    /// and accepts no more items.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.state == EncoderState::Finished
    }

    /// Number of items written so far.
    #[must_use]
    pub fn item_count(&self) -> u64 {
        self.item_count
    }

    /// Uncompressed bytes written so far, newlines included. A writer
    /// targeting a per-file size compares this against its limit.
    #[must_use]
    pub fn uncompressed_bytes(&self) -> u64 {
        self.uncompressed_bytes
    }

    /// Finish the gzip stream and flush the sink, keeping the encoder so the
    /// sink can be recovered with [`Self::into_inner`] on failure (an upload
    /// handle that must be aborted, for example). Returns the checksums a
    /// manifest records. After a successful `try_finish` the encoder accepts
    /// no more items.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Encode`] when the compressor or sink fails, or
    /// when the encoder is already finished or poisoned. The encoder is
    /// poisoned after a failure.
    pub fn try_finish(&mut self) -> Result<DataFileChecksums, FormatError> {
        self.check_open()?;
        let result = self
            .encoder
            .try_finish()
            .and_then(|()| self.encoder.get_mut().flush());
        if let Err(e) = result {
            self.state = EncoderState::Poisoned;
            return Err(FormatError::Encode(e.to_string()));
        }
        // No item may follow the trailer.
        self.state = EncoderState::Finished;
        let digest = std::mem::take(&mut self.encoder.get_mut().hasher).finish();
        Ok(DataFileChecksums {
            item_count: self.item_count,
            sha256_hex: digest.sha256_hex,
            md5_base64: digest.md5_base64,
            compressed_bytes: digest.bytes,
            uncompressed_bytes: self.uncompressed_bytes,
        })
    }

    /// Give back the sink, finished or not. After a failed `write_item` or
    /// `try_finish` the sink holds a partial file the caller must discard.
    ///
    /// # Panics
    ///
    /// Never: the sink is present until this call consumes the encoder.
    #[must_use]
    pub fn into_inner(mut self) -> W {
        self.encoder
            .get_mut()
            .inner
            .take()
            .expect("the sink is taken exactly once, here")
    }

    /// [`Self::try_finish`] then [`Self::into_inner`], for callers whose sink
    /// needs no cleanup on failure (a `Vec<u8>`, a file removed by path).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Encode`] when the compressor or sink fails; the
    /// sink is dropped without being written to again.
    pub fn finish(mut self) -> Result<(W, DataFileChecksums), FormatError> {
        let checksums = self.try_finish()?;
        Ok((self.into_inner(), checksums))
    }
}

impl<W: Write> Drop for DataFileEncoder<W> {
    /// A poisoned encoder gives the compressor nothing to write into, so its
    /// `Drop` cannot push the buffered data and trailer of a damaged stream
    /// into a sink the caller has already decided to abandon. An open or
    /// finished encoder is left to flate2's default behaviour.
    fn drop(&mut self) {
        if self.state == EncoderState::Poisoned {
            drop(self.encoder.get_mut().inner.take());
        }
    }
}

/// Encode items into an in-memory gzip data file, returning the compressed
/// bytes and the checksums a manifest records for them. Convenience over
/// [`DataFileEncoder`] for callers that hold a whole file anyway (tests, small
/// files); a backup job streams through the encoder instead.
///
/// # Errors
///
/// Returns [`FormatError::Json`] when an item fails to serialize and
/// [`FormatError::Encode`] when compression fails.
pub fn encode_data_file<I>(items: I) -> Result<(Vec<u8>, DataFileChecksums), FormatError>
where
    I: IntoIterator<Item = Item>,
{
    let mut encoder = DataFileEncoder::new(Vec::new());
    for item in items {
        encoder.write_item(&item)?;
    }
    encoder.finish()
}

/// Streaming decoder for a data file read from any [`Read`].
///
/// Items stream back one line at a time. The compressed bytes are hashed as
/// they are consumed and the digests, item count and uncompressed byte count
/// are checked when the stream ends, so the last `next_item` call returns the
/// verification result. Until then every item is unverified: a caller that
/// needs the digests checked before it acts on any item uses
/// [`CompressedHasher`] over a local copy first, or [`Self::verified`] when
/// the file is already in memory.
///
/// Items are checked for JSON shape only. An `N` value that is not a valid
/// `DynamoDB` number (`"abc"`, `"1E999"`, `"1_000"`) decodes as written; a
/// restore runs each item through the service's validation layer before
/// writing it, the same path `PutItem` takes.
///
/// Any error ends the stream: every later `next_item` returns
/// [`FormatError::Decode`] naming the earlier failure, never `Ok(None)`, so a
/// consumer that drops errors cannot mistake a corrupt file for a short one.
/// The [`Iterator`] impl is fused on top of that: it yields the first error
/// once and then `None`, so error-dropping iterator idioms terminate.
pub struct DataFileDecoder<R: Read> {
    reader: BufReader<MultiGzDecoder<HashingReader<R>>>,
    expectations: DataFileExpectations,
    items_decoded: u64,
    bytes_decoded: u64,
    finished: bool,
    /// Set by the first error; replayed by every later call.
    failed: Option<&'static str>,
}

impl<'a> DataFileDecoder<&'a [u8]> {
    /// Verify the compressed-byte checksums against an in-memory file, then
    /// open the stream over it. The digests are checked here, before any item
    /// is returned; the end-of-stream checks still run.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::ChecksumMismatch`] when a checksum in
    /// `expectations` does not match `compressed`.
    pub fn verified(
        compressed: &'a [u8],
        expectations: DataFileExpectations,
    ) -> Result<Self, FormatError> {
        expectations.verify_digest(&CompressedHasher::digest(compressed))?;
        Ok(Self::new(compressed, expectations))
    }
}

impl<R: Read> DataFileDecoder<R> {
    /// Open a stream over `compressed`. Nothing is verified until the stream
    /// ends; see the type docs.
    pub fn new(compressed: R, expectations: DataFileExpectations) -> Self {
        Self {
            reader: BufReader::new(MultiGzDecoder::new(HashingReader {
                inner: compressed,
                hasher: CompressedHasher::new(),
            })),
            expectations,
            items_decoded: 0,
            bytes_decoded: 0,
            finished: false,
            failed: None,
        }
    }

    /// Decode the next item, or run the end-of-stream checks and return
    /// `Ok(None)`.
    ///
    /// The line is read through a bounded loop that refuses to materialize
    /// more than [`MAX_LINE_BYTES`], so a malicious or corrupt gzip member
    /// cannot drive unbounded memory. When
    /// [`DataFileExpectations::uncompressed_bytes`] is set it is also enforced
    /// incrementally: the running total is checked against the expectation as
    /// each line is consumed, so an oversized stream is rejected before the
    /// whole of it is read rather than only at clean end of stream.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Decode`] when decompression fails,
    /// [`FormatError::LineTooLong`] when a single line reaches the cap,
    /// [`FormatError::Json`] when a line does not parse, and at end of stream
    /// [`FormatError::ChecksumMismatch`], [`FormatError::ItemCountMismatch`]
    /// or [`FormatError::ByteCountMismatch`] when the consumed bytes differ
    /// from the expectations (the byte count is also enforced mid-stream once
    /// it is exceeded).
    pub fn next_item(&mut self) -> Result<Option<Item>, FormatError> {
        if let Some(reason) = self.failed {
            return Err(FormatError::Decode(format!(
                "decoder failed earlier: {reason}"
            )));
        }
        if self.finished {
            return Ok(None);
        }
        let result = self.next_item_inner();
        if result.is_err() {
            self.finished = true;
            self.failed = Some(match &result {
                Err(FormatError::ChecksumMismatch { .. }) => "checksum mismatch",
                Err(FormatError::ItemCountMismatch { .. }) => "item count mismatch",
                Err(FormatError::ByteCountMismatch { .. }) => "byte count mismatch",
                Err(FormatError::LineTooLong { .. }) => "line too long",
                Err(FormatError::Json(_)) => "unparseable line",
                _ => "decode error",
            });
        }
        result
    }

    fn next_item_inner(&mut self) -> Result<Option<Item>, FormatError> {
        let mut line: Vec<u8> = Vec::new();
        let read = self.read_capped_line(&mut line)?;
        if read == 0 {
            self.finished = true;
            return self.finish_checks().map(|()| None);
        }
        self.bytes_decoded += read as u64;
        // Enforce the recorded uncompressed size incrementally: once the
        // running total passes the expectation the stream is oversized, so
        // reject it here rather than continuing to read.
        if let Some(expected) = self.expectations.uncompressed_bytes
            && self.bytes_decoded > expected
        {
            return Err(FormatError::ByteCountMismatch {
                expected,
                actual: self.bytes_decoded,
            });
        }
        let text = std::str::from_utf8(strip_trailing_newline(&line))
            .map_err(|e| FormatError::Decode(e.to_string()))?;
        let parsed: ItemLine = serde_json::from_str(text)?;
        self.items_decoded += 1;
        Ok(Some(parsed.item))
    }

    /// End-of-stream verification: digests first, then counts. By the time
    /// the decompressor reports end of stream it has consumed every byte of
    /// the source (`MultiGzDecoder` parses any trailing bytes as a further
    /// member and fails on garbage), so the digest covers the whole object.
    fn finish_checks(&mut self) -> Result<(), FormatError> {
        let hashing = self.reader.get_mut().get_mut();
        let digest = std::mem::take(&mut hashing.hasher).finish();
        self.expectations.verify_digest(&digest)?;
        if let Some(expected) = self.expectations.item_count
            && expected != self.items_decoded
        {
            return Err(FormatError::ItemCountMismatch {
                expected,
                actual: self.items_decoded,
            });
        }
        if let Some(expected) = self.expectations.uncompressed_bytes
            && expected != self.bytes_decoded
        {
            return Err(FormatError::ByteCountMismatch {
                expected,
                actual: self.bytes_decoded,
            });
        }
        Ok(())
    }

    /// Read one newline-terminated line into `line`, appending at most
    /// [`MAX_LINE_BYTES`] bytes (newline included) before erroring.
    ///
    /// Returns the number of bytes read, `0` at end of stream. The read walks
    /// the `BufReader`'s own buffer with `fill_buf` / `consume`, so peak
    /// allocation is the growing line (capped) plus the reader's fixed
    /// buffer, never the whole compressed member expanded at once.
    fn read_capped_line(&mut self, line: &mut Vec<u8>) -> Result<usize, FormatError> {
        loop {
            let available = self
                .reader
                .fill_buf()
                .map_err(|e| FormatError::Decode(e.to_string()))?;
            if available.is_empty() {
                return Ok(line.len());
            }
            if let Some(nl) = available.iter().position(|&b| b == b'\n') {
                let want = nl + 1;
                if line.len() + want > MAX_LINE_BYTES {
                    return Err(FormatError::LineTooLong {
                        cap: MAX_LINE_BYTES,
                    });
                }
                line.extend_from_slice(&available[..want]);
                self.reader.consume(want);
                return Ok(line.len());
            }
            let take = available.len();
            if line.len() + take > MAX_LINE_BYTES {
                return Err(FormatError::LineTooLong {
                    cap: MAX_LINE_BYTES,
                });
            }
            line.extend_from_slice(available);
            self.reader.consume(take);
        }
    }
}

impl<R: Read> Iterator for DataFileDecoder<R> {
    type Item = Result<Item, FormatError>;

    /// Fused: once an error has been yielded, `None`. `next_item` keeps
    /// returning the error for callers that use it directly.
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed.is_some() {
            return None;
        }
        self.next_item().transpose()
    }
}

impl<R: Read> std::iter::FusedIterator for DataFileDecoder<R> {}

/// Decode a whole in-memory data file into a `Vec`, verifying the digests
/// before the first item and the counts at the end. For tests and files known
/// to be small; a restore streams through [`DataFileDecoder`] instead and
/// never holds a file's items in memory.
///
/// # Errors
///
/// Propagates every error [`DataFileDecoder`] can produce.
pub fn decode_data_file(
    compressed: &[u8],
    expectations: DataFileExpectations,
) -> Result<Vec<Item>, FormatError> {
    let mut decoder = DataFileDecoder::verified(compressed, expectations)?;
    let mut items = Vec::new();
    while let Some(item) = decoder.next_item()? {
        items.push(item);
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::types::AttributeValue;

    fn item(entries: Vec<(&str, AttributeValue)>) -> Item {
        entries
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect()
    }

    fn string_set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|s| (*s).to_owned()).collect()
    }

    fn byte_set(values: &[&[u8]]) -> BTreeSet<Vec<u8>> {
        values.iter().map(|b| b.to_vec()).collect()
    }

    /// A map nested to the given depth, ending in a string leaf.
    fn nested_map(depth: usize) -> AttributeValue {
        let mut value = AttributeValue::S("leaf".to_owned());
        for level in (0..depth).rev() {
            let mut map = BTreeMap::new();
            map.insert(format!("level{level}"), value);
            value = AttributeValue::M(map);
        }
        value
    }

    /// Items covering every `DynamoDB` attribute type, with numbers at the
    /// 38-significant-digit maximum and in negative form. Number strings are
    /// written in the normalized form the `N` deserializer produces, so a
    /// round trip compares equal.
    fn all_types_items() -> Vec<Item> {
        vec![
            item(vec![
                ("pk", AttributeValue::S("item-1".to_owned())),
                (
                    "n_38_digits",
                    AttributeValue::N("99999999999999999999999999999999999999".to_owned()),
                ),
                (
                    "n_negative",
                    AttributeValue::N("-12345678901234567890.123456789".to_owned()),
                ),
                (
                    "n_large_magnitude",
                    AttributeValue::N(format!("1{}", "0".repeat(100))),
                ),
                ("n_small", AttributeValue::N("0.001".to_owned())),
                ("b", AttributeValue::B(vec![0xde, 0xad, 0xbe, 0xef])),
                ("bool_true", AttributeValue::Bool(true)),
                ("null", AttributeValue::Null),
            ]),
            item(vec![
                ("pk", AttributeValue::S("item-2".to_owned())),
                ("ss", AttributeValue::SS(string_set(&["a", "b", "c"]))),
                (
                    "ns",
                    AttributeValue::NS(
                        ["-1", "0.5", "42"]
                            .iter()
                            .map(|s| (*s).to_owned())
                            .collect(),
                    ),
                ),
                (
                    "bs",
                    AttributeValue::BS(byte_set(&[b"one".as_slice(), b"two".as_slice()])),
                ),
                (
                    "l",
                    AttributeValue::L(vec![
                        AttributeValue::N("1".to_owned()),
                        AttributeValue::S("x".to_owned()),
                        AttributeValue::Bool(false),
                        AttributeValue::Null,
                        AttributeValue::L(vec![]),
                    ]),
                ),
                ("m_depth_32", nested_map(32)),
            ]),
        ]
    }

    #[test]
    fn encode_decode_round_trip_all_types() {
        let items = all_types_items();
        let (compressed, checksums) = encode_data_file(items.clone()).unwrap();
        assert_eq!(checksums.item_count, 2);
        assert_eq!(checksums.compressed_bytes, compressed.len() as u64);
        assert!(checksums.uncompressed_bytes > checksums.compressed_bytes / 4);

        let decoded = decode_data_file(&compressed, (&checksums).into()).unwrap();
        assert_eq!(decoded, items);
    }

    #[test]
    fn exponent_form_numbers_decode_to_normalized_values() {
        // Exponent and shorthand forms are valid on the wire; the N
        // deserializer normalizes them, so a file holding them decodes to the
        // plain forms.
        let line = concat!(
            r#"{"Item":{"e_neg":{"N":"-2.5e-2"},"e_pos":{"N":"1.5E3"},"#,
            r#""pk":{"S":"exp"}}}"#,
            "\n",
        );
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(line.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let decoded = decode_data_file(&compressed, DataFileExpectations::default()).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].get("e_pos"),
            Some(&AttributeValue::N("1500".to_owned()))
        );
        assert_eq!(
            decoded[0].get("e_neg"),
            Some(&AttributeValue::N("-0.025".to_owned()))
        );
    }

    #[test]
    fn line_encoding_matches_export_handler_shape() {
        // One item, then decompress and compare against the exact string the
        // engine's export handler would write for it.
        let one = item(vec![
            ("a", AttributeValue::N("1".to_owned())),
            ("pk", AttributeValue::S("x".to_owned())),
        ]);
        let (compressed, _) = encode_data_file(vec![one.clone()]).unwrap();

        let mut reader = BufReader::new(MultiGzDecoder::new(compressed.as_slice()));
        let mut text = String::new();
        std::io::Read::read_to_string(&mut reader, &mut text).unwrap();

        let wrapper = serde_json::json!({ "Item": one });
        let expected = format!("{}\n", serde_json::to_string(&wrapper).unwrap());
        assert_eq!(text, expected);
        assert_eq!(
            text,
            "{\"Item\":{\"a\":{\"N\":\"1\"},\"pk\":{\"S\":\"x\"}}}\n"
        );
    }

    #[test]
    fn empty_file_round_trip() {
        let (compressed, checksums) = encode_data_file(Vec::<Item>::new()).unwrap();
        assert_eq!(checksums.item_count, 0);
        assert_eq!(checksums.uncompressed_bytes, 0);
        let decoded = decode_data_file(&compressed, (&checksums).into()).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn tampered_compressed_bytes_fail_sha256() {
        let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let last = compressed.len() - 1;
        compressed[last] ^= 0xff;
        let err = decode_data_file(&compressed, (&checksums).into()).unwrap_err();
        assert!(matches!(
            err,
            FormatError::ChecksumMismatch {
                subject: "data file sha256",
                ..
            }
        ));
    }

    #[test]
    fn tampered_compressed_bytes_fail_md5_when_only_md5_given() {
        let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        compressed[0] ^= 0x01;
        let expectations = DataFileExpectations {
            md5_base64: Some(checksums.md5_base64),
            ..Default::default()
        };
        let err = decode_data_file(&compressed, expectations).unwrap_err();
        assert!(matches!(
            err,
            FormatError::ChecksumMismatch {
                subject: "data file md5",
                ..
            }
        ));
    }

    #[test]
    fn wrong_item_count_is_detected() {
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let expectations = DataFileExpectations {
            item_count: Some(checksums.item_count + 1),
            ..Default::default()
        };
        let err = decode_data_file(&compressed, expectations).unwrap_err();
        assert!(matches!(
            err,
            FormatError::ItemCountMismatch {
                expected: 3,
                actual: 2
            }
        ));
    }

    #[test]
    fn wrong_uncompressed_byte_count_is_detected() {
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let expectations = DataFileExpectations {
            uncompressed_bytes: Some(checksums.uncompressed_bytes + 1),
            ..Default::default()
        };
        let err = decode_data_file(&compressed, expectations).unwrap_err();
        assert!(matches!(err, FormatError::ByteCountMismatch { .. }));
    }

    #[test]
    fn decoder_streams_via_iterator() {
        let items = all_types_items();
        let (compressed, checksums) = encode_data_file(items.clone()).unwrap();
        let decoder = DataFileDecoder::verified(&compressed, (&checksums).into()).unwrap();
        let streamed: Result<Vec<Item>, FormatError> = decoder.collect();
        assert_eq!(streamed.unwrap(), items);
    }

    #[test]
    fn truncated_gzip_stream_is_an_error() {
        let (compressed, _) = encode_data_file(all_types_items()).unwrap();
        let truncated = &compressed[..compressed.len() / 2];
        let err = decode_data_file(truncated, DataFileExpectations::default()).unwrap_err();
        assert!(matches!(err, FormatError::Decode(_)));
    }

    #[test]
    fn malformed_line_is_an_error() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"{\"NotItem\":{}}\n").unwrap();
        let compressed = encoder.finish().unwrap();
        let err = decode_data_file(&compressed, DataFileExpectations::default()).unwrap_err();
        assert!(matches!(err, FormatError::Json(_)));
    }

    #[test]
    fn md5_checksum_encoding_matches_manifest_files_shape() {
        // 16 MD5 bytes encode to 24 base64 characters with padding, the
        // shape manifest-files.json carries.
        let (_, checksums) = encode_data_file(all_types_items()).unwrap();
        assert_eq!(checksums.md5_base64.len(), 24);
        assert!(checksums.md5_base64.ends_with("=="));
        assert_eq!(checksums.sha256_hex.len(), 64);
    }

    /// Read the process high-water resident set size in kibibytes from
    /// `/proc/self/status` (`VmHWM`).
    #[cfg(target_os = "linux")]
    fn peak_rss_kib() -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                let kib = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap();
                return kib;
            }
        }
        panic!("VmHWM not found in /proc/self/status");
    }

    #[test]
    fn single_oversized_line_hits_the_cap_without_materializing_it() {
        // A gzip whose one line is far larger than the cap: highly
        // compressible repeated bytes with no newline until the very end.
        // Uncompressed this is 64 MiB; compressed it is tiny. A decoder that
        // read the whole line into memory would allocate the full 64 MiB.
        let uncompressed_len = 64 * 1024 * 1024;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        let block = vec![b'a'; 1024 * 1024];
        let mut written = 0usize;
        while written < uncompressed_len {
            encoder.write_all(&block).unwrap();
            written += block.len();
        }
        // No trailing newline: the whole thing is one line.
        let compressed = encoder.finish().unwrap();
        assert!(
            compressed.len() < 512 * 1024,
            "fixture should be small compressed, was {}",
            compressed.len()
        );

        #[cfg(target_os = "linux")]
        let before = peak_rss_kib();

        let err = decode_data_file(&compressed, DataFileExpectations::default()).unwrap_err();
        assert!(
            matches!(err, FormatError::LineTooLong { cap } if cap == MAX_LINE_BYTES),
            "expected LineTooLong, got {err:?}"
        );

        // On Linux, prove the decoder never materialized the 64 MiB line:
        // peak RSS growth across the call must stay well under it. The cap is
        // 4 MiB plus the reader buffer, so a generous ceiling of 32 MiB
        // growth still fails loudly if the whole line were buffered.
        #[cfg(target_os = "linux")]
        {
            let after = peak_rss_kib();
            let growth_kib = after.saturating_sub(before);
            println!(
                "bomb test peak RSS: before={before} KiB, after={after} KiB, growth={growth_kib} KiB"
            );
            assert!(
                growth_kib < 32 * 1024,
                "peak RSS grew {growth_kib} KiB decoding a 64 MiB bomb; \
                 the line was not bounded"
            );
        }
    }

    #[test]
    fn oversized_stream_is_rejected_before_full_read_when_byte_count_is_known() {
        // Many small lines whose running total will exceed a deliberately
        // low expectation; the decoder must stop mid-stream, not at EOF.
        let items: Vec<Item> = (0..1000)
            .map(|i| item(vec![("pk", AttributeValue::S(format!("item-{i}")))]))
            .collect();
        let (compressed, _) = encode_data_file(items).unwrap();
        let expectations = DataFileExpectations {
            uncompressed_bytes: Some(64),
            ..Default::default()
        };
        let mut decoder = DataFileDecoder::verified(&compressed, expectations).unwrap();
        let mut err = None;
        // Pull items until the incremental byte-count check fires.
        loop {
            match decoder.next_item() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert!(
            matches!(
                err,
                Some(FormatError::ByteCountMismatch { expected: 64, .. })
            ),
            "expected a mid-stream ByteCountMismatch, got {err:?}"
        );
    }

    #[test]
    fn multi_member_gzip_decodes_all_members() {
        // Two independent gzip members concatenated: MultiGzDecoder must read
        // both, GzDecoder would stop after the first.
        let first = all_types_items();
        let second = vec![
            item(vec![("pk", AttributeValue::S("m2-a".to_owned()))]),
            item(vec![("pk", AttributeValue::S("m2-b".to_owned()))]),
        ];
        let (mut member_one, _) = encode_data_file(first.clone()).unwrap();
        let (member_two, _) = encode_data_file(second.clone()).unwrap();
        member_one.extend_from_slice(&member_two);

        let decoded = decode_data_file(&member_one, DataFileExpectations::default()).unwrap();
        let mut expected = first;
        expected.extend(second);
        assert_eq!(decoded, expected);
    }

    /// The cap is exclusive: a line of exactly `MAX_LINE_BYTES` (newline
    /// included) is read and then fails to parse as an item; one byte more is
    /// `LineTooLong` before the line is materialized.
    #[test]
    fn line_cap_boundary() {
        let build = |len: usize| {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            let mut line = vec![b'x'; len - 1];
            line.push(b'\n');
            encoder.write_all(&line).unwrap();
            encoder.finish().unwrap()
        };
        let at_cap = build(MAX_LINE_BYTES);
        let err = decode_data_file(&at_cap, DataFileExpectations::default()).unwrap_err();
        assert!(
            matches!(err, FormatError::Json(_)),
            "at the cap the line is read: {err}"
        );
        let over = build(MAX_LINE_BYTES + 1);
        let err = decode_data_file(&over, DataFileExpectations::default()).unwrap_err();
        assert!(matches!(err, FormatError::LineTooLong { .. }), "{err}");
    }

    /// A sink that only exposes `Write`, like a file, and hands back at most
    /// 7 bytes per call so the hashing writer's short-write handling is
    /// exercised.
    struct CountingSink(Vec<u8>, usize);
    impl Write for CountingSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let n = buf.len().min(7);
            self.0.extend_from_slice(&buf[..n]);
            self.1 += 1;
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn streaming_encoder_matches_in_memory_encoder() {
        let items = all_types_items();
        let (expected_bytes, expected) = encode_data_file(items.clone()).unwrap();
        let mut encoder = DataFileEncoder::new(CountingSink(Vec::new(), 0));
        for item in &items {
            encoder.write_item(item).unwrap();
        }
        assert_eq!(encoder.item_count(), items.len() as u64);
        assert_eq!(encoder.uncompressed_bytes(), expected.uncompressed_bytes);
        let (sink, checksums) = encoder.finish().unwrap();
        assert_eq!(sink.0, expected_bytes);
        assert_eq!(checksums, expected);
        assert!(sink.1 > 1, "short writes were exercised");
    }

    #[test]
    fn encoder_output_is_deterministic() {
        let (a, ca) = encode_data_file(all_types_items()).unwrap();
        let (b, cb) = encode_data_file(all_types_items()).unwrap();
        assert_eq!(a, b);
        assert_eq!(ca, cb);
    }

    #[test]
    fn compressed_hasher_incremental_equals_one_shot() {
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let mut hasher = CompressedHasher::new();
        for chunk in compressed.chunks(13) {
            hasher.update(chunk);
        }
        let digest = hasher.finish();
        assert_eq!(digest, CompressedHasher::digest(&compressed));
        assert_eq!(digest.sha256_hex, checksums.sha256_hex);
        assert_eq!(digest.md5_base64, checksums.md5_base64);
        assert_eq!(digest.bytes, checksums.compressed_bytes);
        DataFileExpectations::from(&checksums)
            .verify_digest(&digest)
            .unwrap();
    }

    #[test]
    fn streaming_decoder_verifies_at_end_of_stream() {
        let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        // Flip a byte inside the gzip payload (past the 10-byte header, before
        // the 8-byte trailer): the stream still decompresses something, but
        // the digest over the stored bytes changes.
        let n = compressed.len();
        compressed[n / 2] ^= 0x01;
        let mut decoder = DataFileDecoder::new(
            std::io::Cursor::new(compressed),
            DataFileExpectations::from(&checksums),
        );
        // Items are unverified until the end: the decoder either yields
        // items, fails to decompress, or fails the digest check at EOF. It
        // must not report clean completion.
        let mut saw_error = false;
        loop {
            match decoder.next_item() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {
                    saw_error = true;
                    break;
                }
            }
        }
        assert!(saw_error, "a tampered stream must not complete cleanly");
    }

    /// Bytes after the gzip member are parsed as a further member by
    /// `MultiGzDecoder`, so garbage is a decode error (not a digest mismatch:
    /// the digest check never runs because the stream did not end cleanly).
    #[test]
    fn trailing_bytes_after_gzip_member_are_a_decode_error() {
        for garbage in [
            &b"x"[..],
            &b"garbage after the gzip member"[..],
            &[0u8; 8][..],
        ] {
            let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
            compressed.extend_from_slice(garbage);
            let mut decoder = DataFileDecoder::new(
                std::io::Cursor::new(compressed),
                DataFileExpectations::from(&checksums),
            );
            let mut n = 0;
            let err = loop {
                match decoder.next_item() {
                    Ok(Some(_)) => n += 1,
                    Ok(None) => panic!("trailing bytes must not decode cleanly"),
                    Err(e) => break e,
                }
            };
            assert!(matches!(err, FormatError::Decode(_)), "{err}");
            assert_eq!(n, all_types_items().len(), "the valid member decoded first");
            // The error is sticky.
            assert!(matches!(decoder.next_item(), Err(FormatError::Decode(_))));
        }
    }

    #[test]
    fn decoder_errors_are_sticky() {
        let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let n = compressed.len();
        compressed[n / 2] ^= 0x01;
        let mut decoder = DataFileDecoder::new(
            std::io::Cursor::new(compressed),
            DataFileExpectations::from(&checksums),
        );
        while let Ok(Some(_)) = decoder.next_item() {}
        for _ in 0..3 {
            let err = decoder
                .next_item()
                .expect_err("an error must not turn into Ok(None)");
            assert!(matches!(err, FormatError::Decode(_)), "{err}");
            assert!(err.to_string().contains("failed earlier"), "{err}");
        }
    }

    /// The iterator yields the first error once and then ends, so a consumer
    /// that drops errors terminates instead of spinning on the sticky error;
    /// a consumer that collects into a `Result` still gets the error.
    #[test]
    fn iterator_is_fused_after_an_error() {
        let tampered = || {
            let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
            let n = compressed.len();
            compressed[n - 1] ^= 0x01;
            DataFileDecoder::new(
                std::io::Cursor::new(compressed),
                DataFileExpectations::from(&checksums),
            )
        };
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let count = tampered().filter_map(Result::ok).count();
            let _ = tx.send(count);
        });
        let count = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("an error-dropping consumer must terminate");
        assert!(count <= all_types_items().len());

        let collected: Result<Vec<Item>, FormatError> = tampered().collect();
        assert!(collected.is_err(), "collect still surfaces the error");

        let mut decoder = tampered();
        let mut errors = 0;
        for r in decoder.by_ref() {
            if r.is_err() {
                errors += 1;
            }
        }
        assert_eq!(errors, 1, "the error is yielded exactly once");
        assert!(decoder.next().is_none());
        assert!(decoder.next_item().is_err(), "next_item stays sticky");
    }

    /// Flaky sink: fails the `fail_on`th write, counts every write attempt.
    struct Flaky {
        buf: Vec<u8>,
        calls: usize,
        fail_on: usize,
    }
    impl Write for Flaky {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            if self.calls == self.fail_on {
                return Err(std::io::Error::other("boom"));
            }
            self.buf.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Incompressible payloads so the compressor flushes to the sink during
    /// `write_item` rather than only at finish.
    fn incompressible_items() -> Vec<Item> {
        (0..4u64)
            .map(|i| {
                let mut x: u64 = 0x9e37_79b9_7f4a_7c15 ^ i;
                let blob: String = (0..300_000)
                    .map(|_| {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        char::from(b'a' + u8::try_from(x % 26).unwrap())
                    })
                    .collect();
                item(vec![("pk", AttributeValue::S(blob))])
            })
            .collect()
    }

    #[test]
    fn encoder_is_poisoned_after_a_sink_error() {
        let big = incompressible_items();
        let mut encoder = DataFileEncoder::new(Flaky {
            buf: Vec::new(),
            calls: 0,
            fail_on: 2,
        });
        let mut first_err = None;
        for it in &big {
            if let Err(e) = encoder.write_item(it) {
                first_err = Some(e);
                break;
            }
        }
        assert!(
            matches!(first_err, Some(FormatError::Encode(_))),
            "{first_err:?}"
        );
        assert!(encoder.is_poisoned());
        assert!(!encoder.is_finished());
        // A retry of the item and a finish are both refused.
        let err = encoder.write_item(&big[0]).unwrap_err();
        assert!(err.to_string().contains("poisoned"), "{err}");
        let err = encoder.try_finish().unwrap_err();
        assert!(err.to_string().contains("poisoned"), "{err}");
        // The sink can still be recovered for cleanup.
        let sink = encoder.into_inner();
        assert!(sink.calls >= 2);
    }

    /// Dropping a poisoned encoder must not push the compressor's buffered
    /// data and trailer into the sink: the caller has decided to abandon the
    /// file, and for an upload sink that would be tens of kilobytes of
    /// multipart data after the abort decision.
    #[test]
    fn dropping_a_poisoned_encoder_writes_nothing_more() {
        use std::sync::{Arc, Mutex};
        struct Shared(Arc<Mutex<Flaky>>);
        impl Write for Shared {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().write(b)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let sink = Arc::new(Mutex::new(Flaky {
            buf: Vec::new(),
            calls: 0,
            fail_on: 2,
        }));
        let mut encoder = DataFileEncoder::new(Shared(Arc::clone(&sink)));
        for it in &incompressible_items() {
            if encoder.write_item(it).is_err() {
                break;
            }
        }
        assert!(encoder.is_poisoned());
        let calls_before = sink.lock().unwrap().calls;
        drop(encoder);
        assert_eq!(
            sink.lock().unwrap().calls,
            calls_before,
            "drop of a poisoned encoder wrote into the sink"
        );
    }

    #[test]
    fn sink_accepting_no_bytes_is_an_error_not_a_hang() {
        struct Zero;
        impl Write for Zero {
            fn write(&mut self, _b: &[u8]) -> std::io::Result<usize> {
                Ok(0)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut encoder = DataFileEncoder::new(Zero);
            let r = encoder.write_item(&all_types_items()[0]);
            let _ = tx.send(r.map_err(|e| e.to_string()));
        });
        let outcome = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("write_item returned within the deadline");
        let err = outcome.expect_err("Ok(0) sink is an error");
        assert!(err.contains("accepted no bytes"), "{err}");
    }

    #[test]
    fn try_finish_keeps_the_sink_and_refuses_further_items() {
        let mut encoder = DataFileEncoder::new(Vec::new());
        for it in all_types_items() {
            encoder.write_item(&it).unwrap();
        }
        let checksums = encoder.try_finish().unwrap();
        assert!(encoder.is_finished());
        assert!(!encoder.is_poisoned(), "a clean finish is not a failure");
        let err = encoder.write_item(&all_types_items()[0]).unwrap_err();
        assert!(err.to_string().contains("finished"), "{err}");
        let bytes = encoder.into_inner();
        assert_eq!(bytes.len() as u64, checksums.compressed_bytes);
        let (expected_bytes, expected) = encode_data_file(all_types_items()).unwrap();
        assert_eq!(bytes, expected_bytes);
        assert_eq!(checksums, expected);
    }

    #[test]
    fn streaming_decoder_clean_stream_completes_with_counts_checked() {
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let mut decoder = DataFileDecoder::new(
            std::io::Cursor::new(compressed),
            DataFileExpectations::from(&checksums),
        );
        let mut n = 0;
        while let Some(_item) = decoder.next_item().unwrap() {
            n += 1;
        }
        assert_eq!(n, all_types_items().len());
        // A second call after completion stays at the end.
        assert!(decoder.next_item().unwrap().is_none());
    }

    #[test]
    fn verified_decoder_rejects_before_first_item() {
        let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let n = compressed.len();
        compressed[n / 2] ^= 0x01;
        let err = DataFileDecoder::verified(&compressed, DataFileExpectations::from(&checksums))
            .err()
            .expect("digest checked before any item");
        assert!(matches!(err, FormatError::ChecksumMismatch { .. }), "{err}");
    }
}
