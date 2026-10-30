// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Data file encoding and decoding.
//!
//! A data file is gzip-compressed newline-delimited JSON, one
//! `{"Item": <DynamoDB JSON item>}` object per line, the encoding Amazon
//! `DynamoDB` writes for `DYNAMODB_JSON` exports. [`encode_item_line`] is the
//! one encoding of that line; the engine's `ExportTableToPointInTime` writer
//! calls it, so the two paths agree byte for byte by construction.
//!
//! The reader contract, which v1 freezes: the container is a gzip (RFC
//! 1952) stream; several gzip members concatenated are accepted and their
//! decompressed bytes are one stream, so a line may span a member boundary
//! (the reader is flate2's `MultiGzDecoder`; a reader built on a
//! single-member decoder, Go's `gzip.Reader` with `Multistream(false)`,
//! would truncate such a file). The writer emits one member. A line ends in
//! `\n`, with one exception: the last line's `\n` may be absent, and that
//! line is an item like any other, counted in `uncompressed_bytes` without a
//! newline (the writer always ends the last line with one). The object has
//! the one member `Item` (an unknown member is a
//! [`FormatError::Json`]); the item names each top-level attribute once (a
//! repeated name is a [`FormatError::Json`], as `ImportTable` refuses it;
//! nested maps are read by the wire `AttributeValue` deserializer, which
//! this format leaves alone, so a repeated name inside an `M` is last-wins);
//! a repeated type descriptor at the top of an attribute value
//! (`{"S":"x","S":"y"}`) is refused on this path, because the line is
//! parsed from text and the deserializer sees both keys, while the request
//! path, which parses a body to a `serde_json::Value` first, keeps the last
//! (`S("y")`); the writer emits neither shape, so no written file is
//! affected. There are no blank lines. JSON whitespace around the object is
//! JSON's own rule and is accepted, so a `\r` before the newline (a CRLF
//! file) parses; it is a byte of the line and counts toward
//! `uncompressed_bytes`. The writer emits neither. The lines are UTF-8
//! without a byte order mark.
//!
//! A second reader of the same line format exists outside this module: the
//! engine's `ImportTable` reader for `DYNAMODB_JSON` input
//! (`read_dynamodb_json` in `crates/engine/src/import_export_io.rs`), which
//! reads `ExtendDB`'s own exports. It is laxer than the contract above and is
//! not bound to it: it skips blank and whitespace-only lines and trims
//! Unicode whitespace around each line; it reads an object without an `Item`
//! member as a bare item, and ignores other members beside `Item`; it keeps
//! the last of two top-level attributes with one name (and the last of two
//! `Item` members); it has no line cap, and reads the whole file into
//! memory through a `serde_json::Value` and a clone of the item; its errors
//! name the line but not the cause. Nothing here changes it; a restore reads
//! data files only through [`DataFileDecoder`], which
//! [`super::ExtenddbManifest::reader`] wraps.
//!
//! Checksums: `sha256` and `md5` are computed over the compressed bytes, the
//! object as stored, which is what `extenddb-manifest.json` records per data
//! file and what `manifest-files.json` records as `md5Checksum`, and so is
//! the compressed size. Item and uncompressed byte counts are computed over
//! the line stream.
//!
//! Memory: [`DataFileEncoder`] writes through any [`Write`] and hashes the
//! compressed bytes as they leave the compressor, so a caller streaming to a
//! file or an upload holds the gzip window and one serialized line (a buffer
//! as long as the longest line written so far), not the file. [`DataFileDecoder`] reads from any [`Read`], hashes and counts the
//! compressed bytes as they are consumed, refuses the source as soon as it
//! yields more bytes than recorded, and checks the digests when the stream
//! ends. Decoding a line materializes its item, which takes up to about 128
//! times the line's size; see [`DataFileDecoder`] for the bound.
//! A caller that must not act on any item before the digests are known (a
//! restore) runs the bytes through [`CompressedHasher`] first, keeping a copy
//! on local disk, and decodes from that copy without hashing it again
//! ([`super::DataFileEntry::decoder_prehashed`],
//! [`super::ExtenddbManifest::reader_prehashed`]);
//! [`DataFileDecoder::verified`] does both steps for a slice already in
//! memory, hashing it once.

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
use super::error::{JsonError, truncated};
use super::strict_item::item_without_duplicates;
use crate::types::Item;

/// Longest line, newline included, a data file may carry; the encoder refuses
/// a longer item and the decoder refuses a longer line (a hostile or corrupt
/// gzip object can otherwise allocate gigabytes). A line longer than the cap
/// yields [`FormatError::LineTooLong`] before the whole line is materialized;
/// a line of exactly the cap is accepted.
///
/// Sizing: the cap must hold the line of every item the default item limit
/// (409,600 bytes, 400 KB as `DynamoDB` sizes items) admits, or
/// `CreateBackup` would refuse a table the service accepted. The largest
/// expansion from item size to line bytes is a list of number sets. A number
/// with one or two significant digits costs 2 bytes of item size
/// ([`crate::types::item_size_bytes`]), and the longest such normalized
/// string is 133 characters (`1.1E-130` is `0.`, 129 zeros, `11`), which a
/// set writes as `"...",`: 136 bytes of line for 2 bytes of item, 68x. A set
/// member has no per-member size overhead, and members are distinct only
/// within one set, so a list of sets repeats the same 90 two-digit values
/// at `E-130` in every set. Measured with `item_size_bytes` and
/// [`encode_item_line`] at the default limit (the test
/// `largest_expansion_at_the_default_item_limit_fits_the_cap` builds it):
///
/// | item | item bytes | line bytes | ratio |
/// |---|---|---|---|
/// | list of 2,262 `NS` of the 90 values `1.0E-130`..`9.9E-130` | 409,429 | 27,686,918 | 67.62 |
/// | list of 1,134 `NS` of 180 values (`E-130` and `E-129`) | 409,381 | 27,648,092 | 67.54 |
/// | list of 136,531 `N` `1.1E-130` | 409,600 | 19,387,440 | 47.33 |
/// | list of 136,531 `N` `1E-130` (132 characters) | 409,600 | 19,250,909 | 47.00 |
/// | list of 102,398 `N` `-1E-130` (a sign costs a byte) | 409,599 | 14,540,554 | 35.50 |
/// | list of 409,593 empty strings | 409,600 | 3,686,375 | 9.00 |
/// | list of one-member `SS` / list of `S`, each of one control character (`\u0001`) | 409,599 | 3,686,366 / 3,071,978 | 9.00 / 7.50 |
/// | list of one-member `BS`, each of one byte | 409,599 | 3,276,774 | 8.00 |
/// | list of maps (`{"": NULL}`, `{}`) | 409,597 | 1,706,663 / 921,620 | 4.17 / 2.25 |
///
/// The cap, 32 MiB (33,554,432 bytes), holds the worst case with a 1.21x
/// margin. It is exceeded from an operator item limit
/// (`max_item_size_bytes`) of about 496,000 bytes at the measured 67.62x,
/// 493,000 at the 68x bound; a deployment that raises the limit above that
/// can have items whose line exceeds the cap, and the encoder refuses them
/// at backup time rather than letting the restore find them.
///
/// The cap bounds the line, not the item a line decodes to; see
/// [`DataFileDecoder`] for the decoder's peak memory.
///
/// A data file has no blank lines: every line is one item. A blank line is
/// a [`FormatError::Json`] like any other unparseable line.
pub const MAX_LINE_BYTES: usize = 32 * 1024 * 1024;

/// Serialize one item as the data-file line `{"Item": ...}` plus a newline:
/// the same bytes the engine's `ExportTableToPointInTime` writer produces, so
/// the two paths share one encoding by construction. The line is not checked
/// against [`MAX_LINE_BYTES`]; [`DataFileEncoder::write_item`] does that.
/// [`encode_item_line_into`] writes the same bytes into a buffer the caller
/// keeps.
///
/// # Errors
///
/// Returns the serializer's error when the item cannot be serialized.
pub fn encode_item_line(item: &Item) -> Result<Vec<u8>, serde_json::Error> {
    // `serde_json::to_vec`'s initial capacity, so a short line costs one
    // allocation as it did through `to_vec`.
    let mut line = Vec::with_capacity(128);
    encode_item_line_into(item, &mut line)?;
    Ok(line)
}

/// [`encode_item_line`] into `line`, which is cleared first, so a caller
/// encoding many items reuses one allocation. This is the one encoding of a
/// line; [`encode_item_line`] and [`DataFileEncoder::write_item`] call it.
/// After an error `line` holds a partial line.
///
/// # Errors
///
/// Returns the serializer's error when the item cannot be serialized.
pub fn encode_item_line_into(item: &Item, line: &mut Vec<u8>) -> Result<(), serde_json::Error> {
    line.clear();
    serde_json::to_writer(&mut *line, &ItemLineRef { item })?;
    line.push(b'\n');
    Ok(())
}

/// Lowercase hex encoding of a byte slice.
pub(super) fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Strip the single trailing `\n` from a line. A `\r` before it is JSON
/// whitespace and left to the parser (module doc).
fn strip_trailing_newline(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

/// The single-key wrapper each data file line holds. The item refuses a
/// repeated top-level attribute name (module docs).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ItemLine {
    #[serde(rename = "Item", deserialize_with = "item_without_duplicates")]
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
    /// Expected size of the compressed bytes. The decoder refuses the source
    /// as soon as it has yielded one byte more, so a file recorded as small
    /// cannot stream a long tail of unverified items before the digest
    /// check; a shorter source is refused at end of stream.
    pub compressed_bytes: Option<u64>,
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
            compressed_bytes: Some(checksums.compressed_bytes),
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
    /// Check the size and digest fields that are set against a computed
    /// digest: the compressed size first, then sha256, then md5.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::CompressedSizeMismatch`] when the size differs,
    /// and [`FormatError::ChecksumMismatch`] naming the first digest that
    /// differs (the expected value is shown bounded: it may come from an
    /// unvalidated `manifest-files.json` line).
    pub fn verify_digest(&self, digest: &CompressedDigest) -> Result<(), FormatError> {
        self.verify_compressed_size(digest.bytes)?;
        if let Some(expected) = &self.sha256_hex
            && expected != &digest.sha256_hex
        {
            return Err(FormatError::ChecksumMismatch {
                subject: "data file sha256",
                expected: truncated(expected),
                computed: digest.sha256_hex.clone(),
            });
        }
        if let Some(expected) = &self.md5_base64
            && expected != &digest.md5_base64
        {
            return Err(FormatError::ChecksumMismatch {
                subject: "data file md5",
                expected: truncated(expected),
                computed: digest.md5_base64.clone(),
            });
        }
        Ok(())
    }

    /// Check the compressed size, when it is set, against `bytes`.
    fn verify_compressed_size(&self, bytes: u64) -> Result<(), FormatError> {
        match self.compressed_bytes {
            Some(expected) if expected != bytes => Err(FormatError::CompressedSizeMismatch {
                expected,
                actual: bytes,
            }),
            _ => Ok(()),
        }
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
        // `Interrupted` is the one error std defines as "retry"; `write_all`
        // already retries it on the item path, so retry it here too and the
        // compressor's header and trailer writes see the same sink contract
        // (`flush` below does the same).
        let written = loop {
            match inner.write(buf) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                other => break other?,
            }
        };
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
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        loop {
            match inner.flush() {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                other => return other,
            }
        }
    }
}

/// A [`Read`] that counts, and unless the digest was verified before the
/// read (`hasher` is `None`) hashes, everything read through it. The
/// decompressor reads from this, so the digest covers exactly the compressed
/// bytes consumed.
///
/// With a `limit` (the recorded compressed size) it asks the source for at
/// most one byte past the limit, and once it has that byte it fails every
/// read and sets `overrun`; the decoder turns that into
/// [`FormatError::CompressedSizeMismatch`]. One byte past is what tells a
/// longer object from one that ends at the limit.
struct HashingReader<R: Read> {
    inner: R,
    hasher: Option<CompressedHasher>,
    /// Compressed bytes read so far, hashed or not.
    bytes: u64,
    limit: Option<u64>,
    overrun: bool,
}

const OVERRUN: &str = "data file is longer than its recorded compressed size";

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.overrun {
            return Err(std::io::Error::other(OVERRUN));
        }
        let buf = match self.limit {
            Some(limit) => {
                let room = limit.saturating_sub(self.bytes).saturating_add(1);
                let len = usize::try_from(room).map_or(buf.len(), |room| room.min(buf.len()));
                &mut buf[..len]
            }
            None => buf,
        };
        // Retried here, as the encoder retries it on writes: flate2 passes an
        // `Interrupted` from its source through `fill_buf`, and the decoder's
        // errors are sticky, so a signal during a read would otherwise turn
        // a good file into a reported corrupt one.
        let read = loop {
            match self.inner.read(buf) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                other => break other?,
            }
        };
        if let Some(hasher) = &mut self.hasher {
            hasher.update(&buf[..read]);
        }
        self.bytes += read as u64;
        if let Some(limit) = self.limit
            && self.bytes > limit
        {
            self.overrun = true;
            return Err(std::io::Error::other(OVERRUN));
        }
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
/// Only a successful [`Self::try_finish`] (or [`Self::finish`]) writes the
/// gzip trailer. Dropping an encoder in any other state (fresh, open with
/// items written, poisoned) writes nothing more into the sink: the sink is
/// taken out first, so the compressor's own `Drop` has nowhere to flush its
/// buffered data and trailer. Otherwise an encoder abandoned on a source
/// error or a cancellation would complete a valid gzip file of the items so
/// far, indistinguishable from a whole file without its manifest entry. A
/// caller whose sink needs cleanup (an upload to abort) takes it back with
/// [`Self::into_inner`] before dropping.
pub struct DataFileEncoder<W: Write> {
    encoder: GzEncoder<HashingWriter<W>>,
    /// The line being written, reused from one item to the next: its
    /// capacity is the longest line written so far.
    line: Vec<u8>,
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
/// runs of one build produce identical bytes for identical items.
///
/// The encoder's own rate at this level, measured on one shared 16-core Xeon
/// Platinum 8124M (no SHA extensions), release build, one thread, writing to
/// `io::sink()`, sha256 and md5 included, item generation subtracted: about
/// 33 MB/s of line bytes for 1M synthetic 318-byte items (compression ratio
/// 0.20) and about 15 MB/s for 1,000 items at the 400 KB limit (a 475 KB line
/// of text, a 5,000-element `L` of `N` and a 500-entry `M`; ratio 0.26). A
/// backup job encodes on one thread, so this is its rate: about 8.5 hours per
/// TB of small-item lines. Not held constant: the host ran other builds (load
/// 1.5 to 2 during these runs), the items are synthetic, and the rate depends
/// on the data's compressibility and on the flate2 backend (`miniz_oxide`
/// here; a different backend changes the output bytes as well as the rate).
/// The level choice rests on ratios between levels, measured with the `gzip`
/// CLI (zlib) on the 318-byte items: level 1 was 1.6x faster than level 6 at
/// a 25% output ratio against 21%, and level 9 was 4x slower for 20%.
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
            line: Vec::new(),
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
    /// Returns [`FormatError::Json`] when the item fails to serialize and
    /// [`FormatError::LineTooLong`] when its line exceeds [`MAX_LINE_BYTES`]
    /// (in both cases nothing has been written and the encoder stays usable),
    /// and [`FormatError::Encode`] when the compressor or sink fails, after
    /// which the encoder is poisoned, or when the encoder is already finished
    /// or poisoned.
    pub fn write_item(&mut self, item: &Item) -> Result<(), FormatError> {
        self.check_open()?;
        encode_item_line_into(item, &mut self.line)?;
        let line = &self.line;
        // The decoder refuses a line over the cap, so a file that contains
        // one would verify on write and fail on restore. Refuse it here, with
        // the encoder left usable (nothing has entered the compressor).
        if line.len() > MAX_LINE_BYTES {
            return Err(FormatError::LineTooLong {
                cap: MAX_LINE_BYTES,
            });
        }
        if let Err(e) = self.encoder.write_all(line) {
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

    /// Uncompressed bytes written so far, newlines included.
    ///
    /// The rotation rule a writer with a per-file size target
    /// (`data_file_target_bytes`) applies: after each [`Self::write_item`],
    /// the file closes once this has reached the target. The line that
    /// reaches it is in the file, so a file exceeds the target by less than
    /// one line, at most [`MAX_LINE_BYTES`], and every file holds at least
    /// one item.
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
    /// An encoder that did not finish gives the compressor nothing to write
    /// into, so its `Drop` cannot push buffered data and a trailer into a
    /// sink the caller abandoned (type doc). A finished encoder has nothing
    /// left to write.
    fn drop(&mut self) {
        if self.state != EncoderState::Finished {
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
/// Items stream back one line at a time. The compressed bytes are hashed and
/// counted as they are consumed and the digests, item count and uncompressed
/// byte count are checked when the stream ends, so the last `next_item` call
/// returns the verification result. Until then every item is unverified: a
/// caller that needs the digests checked before it acts on any item uses
/// [`CompressedHasher`] over a local copy first and decodes the copy with
/// [`super::DataFileEntry::decoder_prehashed`], or [`Self::verified`] when
/// the file is already in memory. Two sizes are enforced while streaming,
/// so a file recorded as small cannot stream a long tail of items before
/// the digest refuses it: the compressed size (the source is refused once
/// it yields one byte more than recorded) and the uncompressed size (checked
/// after each line).
///
/// Memory (the one statement of the decode-memory bound; the other docs and
/// tests point here): the line is read through a loop capped at
/// [`MAX_LINE_BYTES`], so the line buffer never exceeds the cap; one buffer
/// is reused for every line and keeps the longest line's capacity. Parsing the
/// line then materializes its item: the wire `AttributeValue` deserializer
/// builds a `serde_json::Value` of each top-level attribute's value and
/// converts it, consuming it level by level, so the peak is the `Value` plus
/// the item being built, whatever the nesting depth. Peak heap growth while
/// decoding one line, per line byte (requested bytes counted by a global
/// allocator, release build, `serde_json` 1.0.150 without `preserve_order`,
/// which is the default `extenddb` binary and `cargo test -p extenddb-core`;
/// a build that includes the MongoDB backend enables `preserve_order`
/// through `bson`, replaces `serde_json`'s `BTreeMap` with `indexmap`, and
/// measures about two thirds of each figure, 51 for the flat list, so the
/// table is the larger of the two; the `VmHWM` growth on Linux x86-64 with
/// glibc malloc is within 4% of it):
///
/// | line | peak per line byte |
/// |---|---|
/// | list of empty strings (`{"S":""},`), flat | 76 |
/// | the same list inside 30 more `L` wrappers (32 levels, `DynamoDB`'s nesting limit) | 76 |
/// | the same list inside 60 more (past the limit, so only a forged line) | 76 |
/// | list of one-member maps with empty names chained 30 deep (`{"M":{"":{"M":...}}}`), the worst item found | 115 |
/// | objects with empty names nested to `serde_json`'s recursion limit (`{"":{"":...0}}`), not an item: refused, but only after the `Value` is built | 127 |
///
/// So one line costs at most about 128 bytes per line byte, about 4 GiB at
/// the cap; a list of empty strings exactly at the cap raises `VmHWM` by
/// 2.44 GiB (the `data_file_decode_peak` probe). At the default item limit
/// the largest measured is the 409,593-empty-string item (a 3.69 MB line,
/// 280 MB); the longest line, the 27.7 MB number-set list, takes 92 MB. A
/// line near the cap comes from a raised `max_item_size_bytes` or a forged
/// file. The figures depend on the allocator and on the `serde_json` and
/// `AttributeValue` code, not on the format. The request path (`PutItem`
/// and every other operation that reads an item) uses the same
/// deserializer, so its cost no longer grows with nesting depth either.
///
/// Items are checked for JSON shape only. An `N` value that is not a valid
/// `DynamoDB` number (`"abc"`, `"1E999"`, `"1_000"`) decodes as written. The
/// decoder checks the file against its format and its recorded sizes,
/// counts and digests, and [`super::ExtenddbManifest::reader`] adds the key
/// range check; neither applies the target's item limits (size, nesting
/// depth, number validity, key lengths), which the restore applies to each
/// item before writing it.
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
    /// The current line, cleared before each item and reused, as the
    /// encoder reuses its buffer: it keeps the capacity of the longest line
    /// read so far (at most [`MAX_LINE_BYTES`]) for the decoder's lifetime.
    line: Vec<u8>,
}

impl<'a> DataFileDecoder<&'a [u8]> {
    /// Verify the compressed size and checksums against an in-memory file,
    /// then open the stream over it. They are checked here, before any item
    /// is returned; the end-of-stream checks still run.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::CompressedSizeMismatch`] or
    /// [`FormatError::ChecksumMismatch`] when the size or a checksum in
    /// `expectations` does not match `compressed`.
    pub fn verified(
        compressed: &'a [u8],
        expectations: DataFileExpectations,
    ) -> Result<Self, FormatError> {
        expectations.verify_digest(&CompressedHasher::digest(compressed))?;
        Ok(Self::prehashed(compressed, expectations))
    }
}

impl<R: Read> DataFileDecoder<R> {
    /// Open a stream over `compressed`. Nothing is verified until the stream
    /// ends; see the type docs. The gzip header is read from the source here
    /// (flate2 parses it in the constructor), so `new` may block on `R`; a
    /// header error surfaces on the first [`Self::next_item`].
    pub fn new(compressed: R, expectations: DataFileExpectations) -> Self {
        Self::open(compressed, expectations, Some(CompressedHasher::new()))
    }

    /// Open a stream over `compressed` whose digests the caller has already
    /// verified against `expectations` (with [`CompressedHasher`] and
    /// [`DataFileExpectations::verify_digest`]): the bytes are not hashed
    /// again and the digests are not compared, while the compressed size,
    /// the uncompressed size, the item count and every line are checked as
    /// [`Self::new`] checks them. [`super::DataFileEntry::decoder_prehashed`]
    /// states when that is safe; [`Self::verified`] meets the condition by
    /// hashing the slice it then decodes.
    pub(super) fn prehashed(compressed: R, expectations: DataFileExpectations) -> Self {
        Self::open(compressed, expectations, None)
    }

    fn open(
        compressed: R,
        expectations: DataFileExpectations,
        hasher: Option<CompressedHasher>,
    ) -> Self {
        Self {
            reader: BufReader::new(MultiGzDecoder::new(HashingReader {
                inner: compressed,
                hasher,
                bytes: 0,
                limit: expectations.compressed_bytes,
                overrun: false,
            })),
            expectations,
            items_decoded: 0,
            bytes_decoded: 0,
            finished: false,
            failed: None,
            line: Vec::new(),
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
    /// [`FormatError::Json`] when a line does not parse,
    /// [`FormatError::CompressedSizeMismatch`] as soon as the source yields
    /// more bytes than recorded, and at end of stream
    /// [`FormatError::CompressedSizeMismatch`],
    /// [`FormatError::ChecksumMismatch`], [`FormatError::ItemCountMismatch`]
    /// or [`FormatError::ByteCountMismatch`] when the consumed bytes differ
    /// from the expectations (the uncompressed byte count is also enforced
    /// mid-stream once it is exceeded).
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
                Err(FormatError::CompressedSizeMismatch { .. }) => "compressed size mismatch",
                Err(FormatError::ItemCountMismatch { .. }) => "item count mismatch",
                Err(FormatError::ByteCountMismatch { .. }) => "byte count mismatch",
                Err(FormatError::LineTooLong { .. }) => "line too long",
                Err(FormatError::Json(_)) => "unparseable line",
                _ => "decode error",
            });
        }
        result
    }

    /// Whether the compressed bytes are being hashed: false for a
    /// [`Self::prehashed`] stream, and after the end-of-stream checks.
    #[cfg(test)]
    fn hashing(&self) -> bool {
        self.reader.get_ref().get_ref().hasher.is_some()
    }

    /// The error for a failed read: [`FormatError::CompressedSizeMismatch`]
    /// when the source ran past its recorded size, otherwise
    /// [`FormatError::Decode`].
    fn source_error(&self, e: &std::io::Error) -> FormatError {
        let source = self.reader.get_ref().get_ref();
        match source.limit {
            Some(expected) if source.overrun => FormatError::CompressedSizeMismatch {
                expected,
                actual: source.bytes,
            },
            _ => FormatError::Decode(e.to_string()),
        }
    }

    fn next_item_inner(&mut self) -> Result<Option<Item>, FormatError> {
        // The buffer is taken out for the read, which borrows the reader,
        // and put back whatever the read's outcome.
        let mut line = std::mem::take(&mut self.line);
        line.clear();
        let read = self.read_capped_line(&mut line);
        self.line = line;
        let read = read?;
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
        // Errors in the line name it (1-based); positions in them are within
        // the line, which is parsed on its own.
        let line_number = self.items_decoded + 1;
        let text = std::str::from_utf8(strip_trailing_newline(&self.line)).map_err(|e| {
            FormatError::Decode(format!(
                "data file line {line_number}: {e} (a byte index within the line)"
            ))
        })?;
        let parsed: ItemLine = serde_json::from_str(text)
            .map_err(|e| FormatError::Json(JsonError::on_data_file_line(e, line_number)))?;
        self.items_decoded += 1;
        Ok(Some(parsed.item))
    }

    /// End-of-stream verification: compressed size and digests (the size
    /// alone for a [`Self::prehashed`] stream) first, then counts. By the time
    /// the decompressor reports end of stream it has consumed every byte of
    /// the source (`MultiGzDecoder` parses any trailing bytes as a further
    /// member and fails on garbage), so the digest covers the whole object.
    fn finish_checks(&mut self) -> Result<(), FormatError> {
        let source = self.reader.get_mut().get_mut();
        match source.hasher.take() {
            Some(hasher) => self.expectations.verify_digest(&hasher.finish())?,
            None => self.expectations.verify_compressed_size(source.bytes)?,
        }
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

    /// Read one line into `line`, through its newline or, for a last line
    /// without one, to the end of the stream (module docs), appending at
    /// most [`MAX_LINE_BYTES`] bytes (newline included) before erroring.
    ///
    /// Returns the number of bytes read, `0` at end of stream. The read walks
    /// the `BufReader`'s own buffer with `fill_buf` / `consume`, so reading
    /// holds the growing line (capped) plus the reader's fixed buffers, never
    /// the whole compressed member expanded at once. This bounds the read
    /// only: parsing the line afterwards materializes its item, which can
    /// take many times the line's size (type docs).
    fn read_capped_line(&mut self, line: &mut Vec<u8>) -> Result<usize, FormatError> {
        loop {
            let available = match self.reader.fill_buf() {
                Ok(available) => available,
                Err(e) => return Err(self.source_error(&e)),
            };
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

    /// Fused: once the decoder has failed, `None`. The first error is
    /// yielded once by whichever of `next` or [`Self::next_item`] meets it;
    /// `next_item` keeps returning it for callers that use it directly, so a
    /// caller mixing the two can see it from `next_item` and then `None`
    /// from `next`.
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed.is_some() {
            return None;
        }
        self.next_item().transpose()
    }
}

impl<R: Read> std::iter::FusedIterator for DataFileDecoder<R> {}

/// Decode a whole in-memory data file into a `Vec`, verifying the size and
/// digests before the first item and the counts at the end. For tests and files known
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

    /// Fewer items than recorded and more items than recorded are both
    /// refused at end of stream.
    #[test]
    fn wrong_item_count_is_detected() {
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        assert_eq!(checksums.item_count, 2);
        for (recorded, expected_err) in [(3, (3, 2)), (1, (1, 2)), (0, (0, 2))] {
            let expectations = DataFileExpectations {
                item_count: Some(recorded),
                ..Default::default()
            };
            let err = decode_data_file(&compressed, expectations).unwrap_err();
            assert!(
                matches!(
                    err,
                    FormatError::ItemCountMismatch { expected, actual }
                        if (expected, actual) == expected_err
                ),
                "recorded {recorded}: {err}"
            );
        }
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

    // The line-bomb test reads the process-wide `VmHWM`, so it lives in its
    // own integration test binary (`tests/data_file_line_bomb.rs`) where no
    // other test can raise the high-water mark first.

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
        let mut yielded = 0;
        // Pull items until the incremental byte-count check fires.
        loop {
            match decoder.next_item() {
                Ok(Some(_)) => yielded += 1,
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
        // The end-of-stream check reports the same variant from the same
        // input, so the count is what shows the check ran mid-stream: each
        // line is about 30 bytes, so the third line passes 64.
        assert!(yielded < 10, "{yielded} of 1000 items streamed first");
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
        // A line of `len` bytes, the last a newline or (an unterminated final
        // line) not.
        let build = |len: usize, newline: bool| {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            let mut line = vec![b'x'; len];
            if newline {
                line[len - 1] = b'\n';
            }
            encoder.write_all(&line).unwrap();
            encoder.finish().unwrap()
        };
        for newline in [true, false] {
            let at_cap = build(MAX_LINE_BYTES, newline);
            let err = decode_data_file(&at_cap, DataFileExpectations::default()).unwrap_err();
            assert!(
                matches!(err, FormatError::Json(_)),
                "newline {newline}: at the cap the line is read: {err}"
            );
            let over = build(MAX_LINE_BYTES + 1, newline);
            let err = decode_data_file(&over, DataFileExpectations::default()).unwrap_err();
            assert!(
                matches!(err, FormatError::LineTooLong { .. }),
                "newline {newline}: {err}"
            );
        }
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
    /// With the compressed size known, the extra bytes are refused as a size
    /// mismatch before that (the file is longer than recorded); this test
    /// leaves the size out to reach the decompressor's own check.
    #[test]
    fn trailing_bytes_after_gzip_member_are_a_decode_error() {
        for garbage in [
            &b"x"[..],
            &b"garbage after the gzip member"[..],
            &[0u8; 8][..],
        ] {
            let (mut compressed, checksums) = encode_data_file(all_types_items()).unwrap();
            compressed.extend_from_slice(garbage);
            let recorded = checksums.compressed_bytes;
            let err = DataFileDecoder::new(
                std::io::Cursor::new(compressed.clone()),
                DataFileExpectations::from(&checksums),
            )
            .collect::<Result<Vec<Item>, _>>()
            .unwrap_err();
            assert!(
                matches!(
                    err,
                    FormatError::CompressedSizeMismatch { expected, actual }
                        if expected == recorded && actual == recorded + 1
                ),
                "{err}"
            );
            let mut decoder = DataFileDecoder::new(
                std::io::Cursor::new(compressed),
                DataFileExpectations {
                    compressed_bytes: None,
                    ..DataFileExpectations::from(&checksums)
                },
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

    /// The encoder and the decoder share the line cap: a line of exactly the
    /// cap round-trips, one byte more is refused at write time with the
    /// encoder still usable.
    #[test]
    fn encoder_refuses_a_line_over_the_cap() {
        let overhead = encode_item_line(&item(vec![("pk", AttributeValue::S(String::new()))]))
            .unwrap()
            .len();
        let exactly = item(vec![(
            "pk",
            AttributeValue::S("x".repeat(MAX_LINE_BYTES - overhead)),
        )]);
        let over = item(vec![(
            "pk",
            AttributeValue::S("x".repeat(MAX_LINE_BYTES - overhead + 1)),
        )]);
        assert_eq!(encode_item_line(&exactly).unwrap().len(), MAX_LINE_BYTES);
        let mut encoder = DataFileEncoder::new(Vec::new());
        let err = encoder.write_item(&over).unwrap_err();
        assert!(matches!(err, FormatError::LineTooLong { .. }), "{err}");
        assert!(!encoder.is_poisoned());
        encoder.write_item(&exactly).unwrap();
        let (bytes, checksums) = encoder.finish().unwrap();
        assert_eq!(checksums.item_count, 1);
        let decoded = decode_data_file(&bytes, DataFileExpectations::from(&checksums)).unwrap();
        assert_eq!(decoded, vec![exactly]);
    }

    /// The streaming decoder checks the digest at end of stream: a file whose
    /// items are identical but whose compressed bytes differ (another gzip
    /// level) yields every item and then `ChecksumMismatch`, never `Ok(None)`.
    #[test]
    fn streaming_decoder_digest_check_fires_at_end_of_stream() {
        let items = all_types_items();
        let (_, level6) = encode_data_file(items.clone()).unwrap();
        let mut sink = Vec::new();
        {
            let mut gz = GzEncoder::new(&mut sink, Compression::new(1));
            for it in &items {
                gz.write_all(&encode_item_line(it).unwrap()).unwrap();
            }
            gz.finish().unwrap();
        }
        assert_ne!(sink.len() as u64, level6.compressed_bytes, "levels differ");
        let mut decoder = DataFileDecoder::new(
            std::io::Cursor::new(sink),
            DataFileExpectations {
                sha256_hex: Some(level6.sha256_hex.clone()),
                md5_base64: None,
                compressed_bytes: None,
                item_count: Some(level6.item_count),
                uncompressed_bytes: Some(level6.uncompressed_bytes),
            },
        );
        let mut n = 0;
        let err = loop {
            match decoder.next_item() {
                Ok(Some(_)) => n += 1,
                Ok(None) => panic!("a digest mismatch must not complete cleanly"),
                Err(e) => break e,
            }
        };
        assert_eq!(n, items.len(), "every item streams before the digest check");
        assert!(
            matches!(
                err,
                FormatError::ChecksumMismatch {
                    subject: "data file sha256",
                    ..
                }
            ),
            "{err}"
        );
    }

    /// `DataFileEntry::decoder` is the restore's entry point: it validates the
    /// key and carries every expectation from the manifest entry.
    #[test]
    fn data_file_entry_decoder_carries_every_expectation() {
        use super::super::{DataFileEntry, data_file_key};
        let items = all_types_items();
        let (bytes, c) = encode_data_file(items.clone()).unwrap();
        let entry = DataFileEntry {
            key: data_file_key(1),
            item_count: c.item_count,
            sha256: c.sha256_hex.clone(),
            md5: c.md5_base64.clone(),
            bytes: c.compressed_bytes,
            uncompressed_bytes: c.uncompressed_bytes,
            key_range: None,
        };
        let decoded: Vec<Item> = entry
            .decoder(std::io::Cursor::new(bytes.clone()))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(decoded, items);

        let run = |e: &DataFileEntry| -> FormatError {
            e.decoder(std::io::Cursor::new(bytes.clone()))
                .unwrap()
                .collect::<Result<Vec<Item>, _>>()
                .unwrap_err()
        };
        let mut e = entry.clone();
        e.sha256 = "0".repeat(64);
        assert!(matches!(
            run(&e),
            FormatError::ChecksumMismatch {
                subject: "data file sha256",
                ..
            }
        ));
        let mut e = entry.clone();
        e.md5 = "AAAAAAAAAAAAAAAAAAAAAA==".to_owned();
        assert!(matches!(
            run(&e),
            FormatError::ChecksumMismatch {
                subject: "data file md5",
                ..
            }
        ));
        for bytes in [c.compressed_bytes - 1, c.compressed_bytes + 1] {
            let mut e = entry.clone();
            e.bytes = bytes;
            assert!(
                matches!(
                    run(&e),
                    FormatError::CompressedSizeMismatch { expected, .. } if expected == bytes
                ),
                "{bytes}"
            );
        }
        let mut e = entry.clone();
        e.item_count += 1;
        assert!(matches!(run(&e), FormatError::ItemCountMismatch { .. }));
        let mut e = entry.clone();
        e.uncompressed_bytes -= 1;
        assert!(matches!(run(&e), FormatError::ByteCountMismatch { .. }));
        let mut e = entry;
        e.key = "data/../x.json.gz".to_owned();
        assert!(matches!(
            e.decoder(std::io::Cursor::new(bytes)).err(),
            Some(FormatError::InvalidDataFileKey { .. })
        ));
    }

    /// The byte count is enforced while streaming, not only at the end: an
    /// oversized stream fails after a few items, not after all of them.
    #[test]
    fn oversized_stream_fails_before_it_is_fully_read() {
        let items: Vec<Item> = (0..1000u32)
            .map(|i| item(vec![("pk", AttributeValue::N(i.to_string()))]))
            .collect();
        let (bytes, c) = encode_data_file(items).unwrap();
        let mut decoder = DataFileDecoder::new(
            std::io::Cursor::new(bytes),
            DataFileExpectations {
                sha256_hex: None,
                md5_base64: None,
                compressed_bytes: None,
                item_count: None,
                uncompressed_bytes: Some(64),
            },
        );
        let mut yielded = 0;
        let err = loop {
            match decoder.next_item() {
                Ok(Some(_)) => yielded += 1,
                Ok(None) => panic!("must not complete"),
                Err(e) => break e,
            }
        };
        assert!(
            matches!(err, FormatError::ByteCountMismatch { expected: 64, .. }),
            "{err}"
        );
        assert!(
            yielded < 10,
            "{yielded} items streamed before the cap fired"
        );
        assert_eq!(c.item_count, 1000);
    }

    /// A failure in `try_finish` poisons the encoder like a failed write.
    #[test]
    fn try_finish_failure_poisons_the_encoder() {
        // Fail the last write: the gzip trailer. Count writes on a plain
        // encoding first to know which call that is.
        let probe = {
            let mut e = DataFileEncoder::new(Flaky {
                buf: Vec::new(),
                calls: 0,
                fail_on: usize::MAX,
            });
            e.write_item(&all_types_items()[0]).unwrap();
            e.try_finish().unwrap();
            e.into_inner().calls
        };
        let mut encoder = DataFileEncoder::new(Flaky {
            buf: Vec::new(),
            calls: 0,
            fail_on: probe,
        });
        encoder.write_item(&all_types_items()[0]).unwrap();
        let err = encoder.try_finish().unwrap_err();
        assert!(matches!(err, FormatError::Encode(_)), "{err}");
        assert!(encoder.is_poisoned());
        assert!(!encoder.is_finished());
        assert!(encoder.try_finish().is_err());
        let _ = encoder.into_inner();
    }

    /// `Interrupted` from the sink is retried on every path, so where it
    /// lands does not decide whether a file must be re-encoded.
    /// The worst expansion the cap's doc names (a list of number sets, 90
    /// two-digit values at `E-130` in each, 2 bytes of item size and 136
    /// bytes of line per member) at the default item limit fits the cap, so
    /// the constant and the default limit cannot drift apart silently. The
    /// next shape in the doc's table, the list of single numbers, is checked
    /// to expand less; the other rows are not checked here.
    #[test]
    fn largest_expansion_at_the_default_item_limit_fits_the_cap() {
        use crate::types::item_size_bytes;
        use crate::validation::number::validate_and_normalize_number;
        let limit = crate::limits::LimitsConfig::default().max_item_size_bytes;
        let set: BTreeSet<String> = (10..100)
            .map(|m| validate_and_normalize_number(&format!("{}.{}E-130", m / 10, m % 10)).unwrap())
            .collect();
        assert_eq!(set.len(), 90);
        assert!(
            set.iter().all(|v| v.len() >= 132),
            "the longest two-digit forms"
        );
        // As many sets as fit: "pk" + "p" + "l" + the list's 3 bytes, then
        // each set costs its members plus 1 as a list element.
        let element = crate::types::attribute_value_size(&AttributeValue::NS(set.clone())) + 1;
        let fits = (limit - 7) / element;
        let build = |n: usize, value: &AttributeValue| {
            let mut it = item(vec![("pk", AttributeValue::S("p".into()))]);
            it.insert("l".to_owned(), AttributeValue::L(vec![value.clone(); n]));
            it
        };
        let worst = build(fits, &AttributeValue::NS(set));
        let size = item_size_bytes(&worst);
        assert!(
            size <= limit && size + element > limit,
            "{size} fills the limit"
        );
        let line = encode_item_line(&worst).unwrap().len();
        assert!(
            line <= MAX_LINE_BYTES,
            "{line} bytes > cap {MAX_LINE_BYTES}"
        );
        // The doc's figures: 409,429 item bytes, 27,686,918 line bytes.
        assert_eq!((size, line), (409_429, 27_686_918));
        // The list of single numbers, the next shape in the table, expands
        // less.
        let n = validate_and_normalize_number("1.1E-130").unwrap();
        let numbers = build((limit - 7) / 3, &AttributeValue::N(n));
        assert!(item_size_bytes(&numbers) <= limit);
        let numbers_line = encode_item_line(&numbers).unwrap().len();
        assert!(numbers_line < line, "{numbers_line}");
    }

    /// Dropping an encoder that did not finish writes nothing more, whether
    /// it is fresh or has items in its buffer: an abandoned encoder must not
    /// complete a valid gzip file in the sink.
    #[test]
    fn dropping_an_unfinished_encoder_writes_nothing_more() {
        use std::sync::{Arc, Mutex};
        struct Counting(Arc<Mutex<(usize, Vec<u8>)>>);
        impl Write for Counting {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                let mut g = self.0.lock().unwrap();
                g.0 += 1;
                g.1.extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        for items in [0usize, 50] {
            let sink = Arc::new(Mutex::new((0, Vec::new())));
            let mut encoder = DataFileEncoder::new(Counting(Arc::clone(&sink)));
            for it in all_types_items().into_iter().cycle().take(items) {
                encoder.write_item(&it).unwrap();
            }
            let before = sink.lock().unwrap().0;
            drop(encoder);
            let (after, bytes) = sink.lock().unwrap().clone();
            assert_eq!(after, before, "{items} items: drop wrote into the sink");
            assert!(
                decode_data_file(&bytes, DataFileExpectations::default()).is_err(),
                "{items} items: the sink must not hold a complete file"
            );
        }
    }

    /// `Interrupted` is retried at the last sink write (the trailer, which
    /// std's `write_all` does not cover) and on `flush`; the output is the
    /// same bytes as an uninterrupted run.
    #[test]
    fn interrupted_trailer_write_and_flush_are_retried() {
        struct Interrupting {
            buf: Vec<u8>,
            writes: usize,
            interrupt_write: usize,
            flush_interrupts: usize,
        }
        impl Write for Interrupting {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.writes += 1;
                if self.writes == self.interrupt_write {
                    return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
                }
                self.buf.extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                if self.flush_interrupts > 0 {
                    self.flush_interrupts -= 1;
                    return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
                }
                Ok(())
            }
        }
        let (expected, checksums) = encode_data_file(all_types_items()).unwrap();
        // Count the writes of a plain run, then interrupt the last one.
        let mut probe = Interrupting {
            buf: Vec::new(),
            writes: 0,
            interrupt_write: usize::MAX,
            flush_interrupts: 0,
        };
        {
            let mut encoder = DataFileEncoder::new(&mut probe);
            for it in all_types_items() {
                encoder.write_item(&it).unwrap();
            }
            encoder.try_finish().unwrap();
        }
        let last = probe.writes;
        let mut encoder = DataFileEncoder::new(Interrupting {
            buf: Vec::new(),
            writes: 0,
            interrupt_write: last,
            flush_interrupts: 3,
        });
        for it in all_types_items() {
            encoder.write_item(&it).unwrap();
        }
        let (sink, got) = encoder.finish().unwrap();
        assert_eq!(sink.buf, expected);
        assert_eq!(got, checksums);
    }

    /// A source that returns `Interrupted` is read through, as `read_to_end`
    /// would; the decoder's sticky error is for real failures.
    #[test]
    fn interrupted_source_is_retried() {
        struct Interrupting<'a> {
            data: &'a [u8],
            calls: usize,
        }
        impl Read for Interrupting<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                self.calls += 1;
                if self.calls.is_multiple_of(2) {
                    return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
                }
                let n = buf.len().min(self.data.len()).min(7);
                buf[..n].copy_from_slice(&self.data[..n]);
                self.data = &self.data[n..];
                Ok(n)
            }
        }
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        let decoder = DataFileDecoder::new(
            Interrupting {
                data: &compressed,
                calls: 0,
            },
            (&checksums).into(),
        );
        let items: Vec<Item> = decoder.collect::<Result<_, _>>().unwrap();
        assert_eq!(items, all_types_items());
    }

    /// The reader contract: an unknown member beside `Item` is refused; a
    /// CRLF line is JSON whitespace and parses, its `\r` counted as a byte.
    #[test]
    fn unknown_members_are_refused_and_crlf_is_whitespace() {
        let crlf = b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\r\n";
        let mut sink = Vec::new();
        {
            let mut gz = GzEncoder::new(&mut sink, Compression::new(GZIP_LEVEL));
            gz.write_all(crlf).unwrap();
            gz.finish().unwrap();
        }
        let expectations = DataFileExpectations {
            uncompressed_bytes: Some(crlf.len() as u64),
            ..DataFileExpectations::default()
        };
        assert_eq!(decode_data_file(&sink, expectations).unwrap().len(), 1);
        for body in [
            &b"{\"Item\":{\"pk\":{\"S\":\"a\"}},\"Extra\":1}\n"[..],
            &b"{\"Extra\":1,\"Item\":{\"pk\":{\"S\":\"a\"}}}\n"[..],
        ] {
            let mut sink = Vec::new();
            {
                let mut gz = GzEncoder::new(&mut sink, Compression::new(GZIP_LEVEL));
                gz.write_all(body).unwrap();
                gz.finish().unwrap();
            }
            let err = decode_data_file(&sink, DataFileExpectations::default()).unwrap_err();
            assert!(matches!(err, FormatError::Json(_)), "{body:?}: {err}");
        }
    }

    /// The end-of-stream checks run on a zero-item stream too.
    #[test]
    fn empty_file_with_wrong_expectations_fails() {
        let (compressed, checksums) = encode_data_file(Vec::<Item>::new()).unwrap();
        let mut count = DataFileExpectations::from(&checksums);
        count.item_count = Some(5);
        assert!(matches!(
            decode_data_file(&compressed, count).unwrap_err(),
            FormatError::ItemCountMismatch { .. }
        ));
        let mut bytes = DataFileExpectations::from(&checksums);
        bytes.uncompressed_bytes = Some(5);
        assert!(matches!(
            decode_data_file(&compressed, bytes).unwrap_err(),
            FormatError::ByteCountMismatch { .. }
        ));
        let mut digest = DataFileExpectations::from(&checksums);
        digest.sha256_hex = Some("0".repeat(64));
        assert!(matches!(
            decode_data_file(&compressed, digest).unwrap_err(),
            FormatError::ChecksumMismatch { .. }
        ));
    }

    /// A sink whose `flush` fails (a full disk under a `BufWriter`) fails the
    /// finish and poisons the encoder; it is not a clean finish.
    #[test]
    fn failing_flush_poisons_the_encoder() {
        struct FlushFails(Vec<u8>);
        impl Write for FlushFails {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::Error::other("disk full"))
            }
        }
        let mut encoder = DataFileEncoder::new(FlushFails(Vec::new()));
        for it in all_types_items() {
            encoder.write_item(&it).unwrap();
        }
        let err = encoder.try_finish().unwrap_err();
        assert!(matches!(err, FormatError::Encode(_)), "{err}");
        assert!(encoder.is_poisoned());
    }

    #[test]
    fn interrupted_sink_is_retried() {
        struct Interrupting {
            buf: Vec<u8>,
            interrupts_left: usize,
        }
        impl Write for Interrupting {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                if self.interrupts_left > 0 {
                    self.interrupts_left -= 1;
                    return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
                }
                self.buf.extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (expected, checksums) = encode_data_file(all_types_items()).unwrap();
        let mut encoder = DataFileEncoder::new(Interrupting {
            buf: Vec::new(),
            interrupts_left: 5,
        });
        for it in all_types_items() {
            encoder.write_item(&it).unwrap();
        }
        let (sink, got) = encoder.finish().unwrap();
        assert_eq!(sink.buf, expected);
        assert_eq!(got, checksums);
    }

    /// Invalid UTF-8 inside a line is a decode error, not a lossy item.
    #[test]
    fn invalid_utf8_line_is_a_decode_error() {
        let mut sink = Vec::new();
        {
            let mut gz = GzEncoder::new(&mut sink, Compression::new(GZIP_LEVEL));
            gz.write_all(b"{\"Item\":{\"pk\":{\"S\":\"\xff\xfe\"}}}\n")
                .unwrap();
            gz.finish().unwrap();
        }
        let mut decoder =
            DataFileDecoder::new(std::io::Cursor::new(sink), DataFileExpectations::default());
        let err = decoder.next_item().unwrap_err();
        assert!(matches!(err, FormatError::Decode(_)), "{err}");
    }

    /// A final line without a newline is accepted: the service's export
    /// files end with one, but a reader that refused its absence would turn a
    /// hand-concatenated file into a missing item.
    #[test]
    fn unterminated_final_line_is_an_item() {
        let items = all_types_items();
        let mut sink = Vec::new();
        {
            let mut gz = GzEncoder::new(&mut sink, Compression::new(GZIP_LEVEL));
            let mut all = Vec::new();
            for it in &items {
                all.extend(encode_item_line(it).unwrap());
            }
            all.pop();
            gz.write_all(&all).unwrap();
            gz.finish().unwrap();
        }
        let decoded: Vec<Item> =
            DataFileDecoder::new(std::io::Cursor::new(sink), DataFileExpectations::default())
                .collect::<Result<_, _>>()
                .unwrap();
        assert_eq!(decoded, items);
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

    /// Items that compress well but not trivially, so the gzip level changes
    /// the output (at a handful of short items levels 6 and 9 agree).
    fn semi_compressible_items() -> Vec<Item> {
        (0..2000u32)
            .map(|i| {
                item(vec![
                    ("pk", AttributeValue::S(format!("item-{i:05}"))),
                    ("v", AttributeValue::N(((i * 7919) % 10007).to_string())),
                    (
                        "s",
                        AttributeValue::S("lorem ipsum dolor sit amet ".repeat((i % 17) as usize)),
                    ),
                ])
            })
            .collect()
    }

    /// The encoder's bytes for a fixed input are pinned: a change of
    /// `GZIP_LEVEL`, a timestamp or name in the gzip header, or another
    /// compressor backend changes the digest. Two runs of one build already
    /// agree (`encoder_output_is_deterministic`); this pins the bytes across
    /// builds, as `Cargo.lock` pins the `flate2` and `miniz_oxide` versions.
    #[test]
    fn encoder_output_bytes_are_pinned() {
        let (bytes, checksums) = encode_data_file(semi_compressible_items()).unwrap();
        // The gzip header: magic, deflate, no flags, mtime 0, XFL 0, OS 255
        // (flate2's defaults; level 6 sets no XFL bit).
        assert_eq!(&bytes[..10], &[0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff]);
        assert_eq!(
            (checksums.compressed_bytes, checksums.sha256_hex.as_str()),
            (PINNED_LEN, PINNED_SHA256),
            "the encoder's output changed"
        );
    }

    const PINNED_LEN: u64 = 17_507;
    const PINNED_SHA256: &str = "b5d05ccbf2851b5103069dab1870b41470025bda0af1b91f4ba5eaf4a57bad32";

    /// A file recorded as 1 compressed byte is refused before it yields more
    /// than a handful of items, however large it is and whatever the other
    /// expectations say (here none but the size, and an uncompressed size of
    /// `u64::MAX`).
    #[test]
    fn compressed_size_is_enforced_while_reading() {
        /// Counts the bytes it hands out and the reads asked of it.
        struct CountingSource {
            inner: std::io::Cursor<Vec<u8>>,
            stats: std::rc::Rc<std::cell::Cell<(u64, u64)>>,
        }
        impl Read for CountingSource {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let n = self.inner.read(buf)?;
                let (bytes, reads) = self.stats.get();
                self.stats.set((bytes + n as u64, reads + 1));
                Ok(n)
            }
        }
        let items = incompressible_items();
        let (bytes, c) = encode_data_file(items.clone()).unwrap();
        assert!(c.compressed_bytes > 256 * 1024, "{}", c.compressed_bytes);
        for (recorded, max_yielded) in [(1, 0), (c.compressed_bytes / 3, 2)] {
            let stats = std::rc::Rc::new(std::cell::Cell::new((0, 0)));
            let source = CountingSource {
                inner: std::io::Cursor::new(bytes.clone()),
                stats: stats.clone(),
            };
            let mut decoder = DataFileDecoder::new(
                source,
                DataFileExpectations {
                    compressed_bytes: Some(recorded),
                    uncompressed_bytes: Some(u64::MAX),
                    ..DataFileExpectations::default()
                },
            );
            let mut yielded = 0;
            let err = loop {
                match decoder.next_item() {
                    Ok(Some(_)) => yielded += 1,
                    Ok(None) => panic!("{recorded}: must not complete"),
                    Err(e) => break e,
                }
            };
            assert!(
                matches!(
                    err,
                    FormatError::CompressedSizeMismatch { expected, actual }
                        if expected == recorded && actual == recorded + 1
                ),
                "{recorded}: {err}"
            );
            assert!(
                yielded <= max_yielded,
                "{recorded}: {yielded} of {} items streamed first",
                items.len()
            );
            // The source handed out exactly one byte past the record.
            let (handed_out, reads) = stats.get();
            assert_eq!(handed_out, recorded + 1, "{recorded}");
            // Sticky, and the reader asked the source for no more.
            assert!(decoder.next_item().is_err());
            assert_eq!(stats.get(), (handed_out, reads), "{recorded}");
        }
    }

    /// Once [`HashingReader`] has seen a byte past its limit it fails every
    /// read without asking its source again (its own contract, below the
    /// decoder's stickiness).
    #[test]
    fn hashing_reader_fails_every_read_after_an_overrun() {
        struct Source(u64);
        impl Read for Source {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                self.0 += 1;
                buf.fill(0);
                Ok(buf.len())
            }
        }
        let mut reader = HashingReader {
            inner: Source(0),
            hasher: Some(CompressedHasher::new()),
            bytes: 0,
            limit: Some(10),
            overrun: false,
        };
        let mut buf = [0u8; 64];
        let err = reader.read(&mut buf).unwrap_err();
        assert_eq!(err.to_string(), OVERRUN);
        assert_eq!((reader.inner.0, reader.bytes), (1, 11));
        for _ in 0..3 {
            assert_eq!(reader.read(&mut buf).unwrap_err().to_string(), OVERRUN);
        }
        assert_eq!((reader.inner.0, reader.bytes), (1, 11));
    }

    /// The streaming loop, not just `verified`: many small items, a file far
    /// larger than the recorded size, and the error arrives after a few
    /// items of a thousand.
    #[test]
    fn compressed_size_overrun_stops_a_long_stream_early() {
        let items: Vec<Item> = (0..20_000u32)
            .map(|i| {
                let mut x = u64::from(i).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                x ^= x >> 29;
                item(vec![("pk", AttributeValue::S(format!("{x:016x}{i}")))])
            })
            .collect();
        let (bytes, c) = encode_data_file(items).unwrap();
        let recorded = c.compressed_bytes / 4;
        let mut decoder = DataFileDecoder::new(
            std::io::Cursor::new(bytes),
            DataFileExpectations {
                compressed_bytes: Some(recorded),
                ..DataFileExpectations::default()
            },
        );
        let mut yielded = 0u64;
        let err = loop {
            match decoder.next_item() {
                Ok(Some(_)) => yielded += 1,
                Ok(None) => panic!("must not complete"),
                Err(e) => break e,
            }
        };
        assert!(
            matches!(err, FormatError::CompressedSizeMismatch { expected, .. } if expected == recorded),
            "{err}"
        );
        assert!(
            yielded < c.item_count / 2,
            "{yielded} of {} items streamed before the size check",
            c.item_count
        );
    }

    /// A source shorter than recorded is refused at end of stream, a source
    /// of exactly the recorded size passes, and `verified` checks the size
    /// before the first item.
    #[test]
    fn compressed_size_mismatch_at_end_of_stream_and_in_verified() {
        let (bytes, c) = encode_data_file(all_types_items()).unwrap();
        let with = |compressed_bytes| DataFileExpectations {
            compressed_bytes: Some(compressed_bytes),
            ..DataFileExpectations::default()
        };
        let streamed = |e: DataFileExpectations| {
            DataFileDecoder::new(std::io::Cursor::new(bytes.clone()), e)
                .collect::<Result<Vec<Item>, _>>()
        };
        assert_eq!(streamed(with(c.compressed_bytes)).unwrap().len(), 2);
        let err = streamed(with(c.compressed_bytes + 1)).unwrap_err();
        assert!(
            matches!(
                err,
                FormatError::CompressedSizeMismatch { expected, actual }
                    if expected == c.compressed_bytes + 1 && actual == c.compressed_bytes
            ),
            "{err}"
        );
        let err = streamed(with(c.compressed_bytes - 1)).unwrap_err();
        assert!(
            matches!(err, FormatError::CompressedSizeMismatch { .. }),
            "{err}"
        );
        for recorded in [c.compressed_bytes - 1, c.compressed_bytes + 1] {
            let err = DataFileDecoder::verified(&bytes, with(recorded))
                .err()
                .expect("size checked before any item");
            assert!(
                matches!(err, FormatError::CompressedSizeMismatch { .. }),
                "{err}"
            );
        }
        assert_eq!(
            DataFileExpectations::from(&c).compressed_bytes,
            Some(c.compressed_bytes)
        );
    }

    /// A data line that names one top-level attribute twice is refused, as
    /// `ImportTable` refuses it, whichever value comes first; a repeated
    /// name inside a nested map is the wire deserializer's (last wins), not
    /// this format's.
    #[test]
    fn repeated_top_level_attribute_is_refused() {
        let gz = |body: &[u8]| {
            let mut sink = Vec::new();
            let mut enc = GzEncoder::new(&mut sink, Compression::new(GZIP_LEVEL));
            enc.write_all(body).unwrap();
            enc.finish().unwrap();
            sink
        };
        for body in [
            &b"{\"Item\":{\"pk\":{\"S\":\"a\"},\"pk\":{\"S\":\"b\"}}}\n"[..],
            &b"{\"Item\":{\"pk\":{\"S\":\"a\"},\"v\":{\"N\":\"1\"},\"v\":{\"S\":\"x\"}}}\n"[..],
        ] {
            let err = decode_data_file(&gz(body), DataFileExpectations::default()).unwrap_err();
            assert!(matches!(err, FormatError::Json(_)), "{err}");
            assert!(err.to_string().contains("more than once"), "{err}");
        }
        let nested = b"{\"Item\":{\"pk\":{\"S\":\"a\"},\"m\":{\"M\":{\"x\":{\"S\":\"1\"},\"x\":{\"S\":\"2\"}}}}}\n";
        let items = decode_data_file(&gz(nested), DataFileExpectations::default()).unwrap();
        assert_eq!(
            items[0].get("m"),
            Some(&AttributeValue::M(
                [("x".to_owned(), AttributeValue::S("2".to_owned()))]
                    .into_iter()
                    .collect()
            ))
        );
    }

    /// serde's messages quote the input without a bound; the `Json`
    /// variant's message is bounded whatever the line holds.
    #[test]
    fn json_errors_on_data_lines_are_bounded() {
        let long = "Q".repeat(1_000_000);
        for body in [
            format!("{{\"Item\":{{\"pk\":{{\"{long}\":\"a\"}}}}}}\n"),
            format!("{{\"Item\":{{\"pk\":{{\"SS\":[\"{long}\",\"{long}\"]}}}}}}\n"),
            format!("{{\"{long}\":1}}\n"),
            format!(
                "{{\"Item\":{{\"pk\":{{\"S\":\"a\"}},\"{}\":{{\"S\":\"a\"}},\"{}\":{{\"S\":\"b\"}}}}}}\n",
                "\u{1}".repeat(500),
                "\u{1}".repeat(500)
            ),
        ] {
            let mut sink = Vec::new();
            {
                let mut gz = GzEncoder::new(&mut sink, Compression::new(GZIP_LEVEL));
                gz.write_all(body.as_bytes()).unwrap();
                gz.finish().unwrap();
            }
            let err = decode_data_file(&sink, DataFileExpectations::default()).unwrap_err();
            assert!(matches!(err, FormatError::Json(_)), "{err}");
            let text = err.to_string();
            assert!(text.len() < 200, "{} bytes: {}", text.len(), &text[..200]);
            assert!(!text.contains('\u{1}'), "control characters are escaped");
        }
    }

    /// A recorded digest comes from an unvalidated `manifest-files.json`
    /// line, so the mismatch message carries a bounded, escaped rendering of
    /// it: a 1 MB expectation full of control characters (a newline, an ANSI
    /// escape) gives a short message with none in it.
    #[test]
    fn checksum_mismatch_bounds_and_escapes_the_expected_value() {
        let hostile = "x\n\u{1b}[2J\u{1b}[31mFAKE LOG LINE\r\u{7}".repeat(40_000);
        assert!(hostile.len() > 1_000_000);
        let (compressed, checksums) = encode_data_file(all_types_items()).unwrap();
        for expectations in [
            DataFileExpectations {
                md5_base64: Some(hostile.clone()),
                ..Default::default()
            },
            DataFileExpectations {
                sha256_hex: Some(hostile.clone()),
                ..Default::default()
            },
        ] {
            let err = DataFileDecoder::verified(&compressed, expectations)
                .err()
                .expect("a mismatch");
            assert!(matches!(err, FormatError::ChecksumMismatch { .. }), "{err}");
            let text = err.to_string();
            assert!(text.len() < 300, "{} bytes", text.len());
            assert!(
                !text.chars().any(char::is_control),
                "control characters are escaped: {text:?}"
            );
            assert!(text.contains(r#""x\n\u{1b}[2J"#), "{text}");
        }
        // The computed value is the decoder's own and stays as it is.
        let err = DataFileDecoder::verified(
            &compressed,
            DataFileExpectations {
                md5_base64: Some("AAAAAAAAAAAAAAAAAAAAAA==".to_owned()),
                ..Default::default()
            },
        )
        .err()
        .expect("a mismatch");
        assert_eq!(
            err.to_string(),
            format!(
                "data file md5 checksum mismatch: expected \"AAAAAAAAAAAAAAAAAAAAAA==\", \
                 computed {}",
                checksums.md5_base64
            )
        );
    }

    /// `Debug` is how `unwrap`, `expect` and a `tracing` `?err` field print
    /// an error, so it is bounded like `Display`: a 1 MB attribute
    /// descriptor that serde quotes in full.
    #[test]
    fn json_error_debug_is_bounded() {
        let long = "Q".repeat(1_000_000);
        for body in [
            format!("{{\"Item\":{{\"pk\":{{\"{long}\":\"a\"}}}}}}\n"),
            format!("{{\"{long}\":1}}\n"),
        ] {
            let mut sink = Vec::new();
            {
                let mut gz = GzEncoder::new(&mut sink, Compression::new(GZIP_LEVEL));
                gz.write_all(body.as_bytes()).unwrap();
                gz.finish().unwrap();
            }
            let err = decode_data_file(&sink, DataFileExpectations::default()).unwrap_err();
            assert!(matches!(err, FormatError::Json(_)), "{err}");
            let debug = format!("{err:?}");
            assert!(
                debug.len() < 300,
                "{} bytes: {}",
                debug.len(),
                &debug[..300]
            );
            assert!(debug.contains("QQQQ"), "{debug}");
        }
        // A manifest's error (no data-file line) is bounded the same way.
        let err = FormatError::from(
            serde_json::from_str::<ItemLine>(&format!("{{\"{long}\":1}}"))
                .err()
                .unwrap(),
        );
        assert!(format!("{err:?}").len() < 300);
        assert!(err.to_string().len() < 300);
    }

    /// Each line is parsed on its own, so serde's position is within the
    /// line; the error names the line in the file, 1-based, so an operator
    /// can find the item in a large file.
    #[test]
    fn line_errors_name_the_line_in_the_file() {
        let good = encode_item_line(&item(vec![("pk", AttributeValue::S("a".into()))])).unwrap();
        let gzip = |lines: &[&[u8]]| {
            let mut sink = Vec::new();
            let mut gz = GzEncoder::new(&mut sink, Compression::new(GZIP_LEVEL));
            for line in lines {
                gz.write_all(line).unwrap();
            }
            gz.finish().unwrap();
            sink
        };
        let bad_json = br#"{"Item":{"pk":{"Q":"a"}}}
"#;
        let err = decode_data_file(
            &gzip(&[&good, &good, bad_json, &good]),
            DataFileExpectations::default(),
        )
        .unwrap_err();
        let FormatError::Json(json) = &err else {
            panic!("{err}")
        };
        assert_eq!(json.data_file_line(), Some(3));
        assert_eq!((json.line(), json.column()), (1, 23));
        assert_eq!(
            err.to_string(),
            "JSON error: data file line 3: unknown AttributeValue type descriptor: Q \
             at column 23 of the line"
        );
        // Invalid UTF-8 on line 2.
        let bad_utf8 = b"{\"Item\":{\"pk\":{\"S\":\"\xff\"}}}\n";
        let err = decode_data_file(&gzip(&[&good, bad_utf8]), DataFileExpectations::default())
            .unwrap_err();
        assert!(matches!(err, FormatError::Decode(_)), "{err}");
        assert!(
            err.to_string()
                .starts_with("data file decode error: data file line 2: invalid utf-8"),
            "{err}"
        );
        // A blank first line is line 1.
        let err = decode_data_file(&gzip(&[b"\n"]), DataFileExpectations::default()).unwrap_err();
        assert!(err.to_string().contains("data file line 1: "), "{err}");
        // A descriptor name ending in ` at line 3 column 4` sets serde's
        // position; no column is shown for it, and the name is kept whole.
        let forged = br#"{"Item":{"a":{"L":[{"X at line 3 column 4":1}]}}}
"#;
        let err =
            decode_data_file(&gzip(&[&good, forged]), DataFileExpectations::default()).unwrap_err();
        let FormatError::Json(json) = &err else {
            panic!("{err}")
        };
        assert_eq!((json.line(), json.column()), (3, 4));
        assert_eq!(
            err.to_string(),
            "JSON error: data file line 2: unknown AttributeValue type descriptor: X \
             at line 3 column 4"
        );
    }

    /// A prehashed decoder does not hash the bytes and does not compare the
    /// digests (a digest that does not match is not noticed), and enforces
    /// the compressed size, the uncompressed size and the item count as the
    /// hashing decoder does. `verified` hashes once, before the stream.
    #[test]
    fn prehashed_decoder_skips_the_digest_and_keeps_every_other_check() {
        let items = all_types_items();
        let (bytes, checksums) = encode_data_file(items.clone()).unwrap();
        let entry = crate::backup_format::DataFileEntry::from_checksums(
            crate::backup_format::data_file_key(1),
            &checksums,
            None,
        );
        let decode = |entry: &crate::backup_format::DataFileEntry| {
            let mut decoder = entry.decoder_prehashed(&bytes[..]).unwrap();
            let mut out = Vec::new();
            loop {
                assert!(!decoder.hashing(), "a prehashed decoder hashes");
                match decoder.next_item() {
                    Ok(Some(item)) => out.push(item),
                    Ok(None) => return Ok(out),
                    Err(e) => return Err(e),
                }
            }
        };
        assert_eq!(decode(&entry).unwrap(), items);
        // Wrong digests: not compared, so the file reads.
        let mut wrong_digests = entry.clone();
        wrong_digests.sha256 = "0".repeat(64);
        wrong_digests.md5 = "AAAAAAAAAAAAAAAAAAAAAA==".to_owned();
        assert_eq!(decode(&wrong_digests).unwrap(), items);
        // The hashing decoder refuses the same entry.
        assert!(matches!(
            wrong_digests
                .decoder(&bytes[..])
                .unwrap()
                .collect::<Result<Vec<_>, _>>(),
            Err(FormatError::ChecksumMismatch { .. })
        ));
        // Every other expectation is still enforced.
        let mut short = entry.clone();
        short.bytes -= 1;
        assert!(matches!(
            decode(&short),
            Err(FormatError::CompressedSizeMismatch { .. })
        ));
        let mut long = entry.clone();
        long.bytes += 1;
        assert!(matches!(
            decode(&long),
            Err(FormatError::CompressedSizeMismatch { .. })
        ));
        let mut count = entry.clone();
        count.item_count += 1;
        assert!(matches!(
            decode(&count),
            Err(FormatError::ItemCountMismatch { .. })
        ));
        let mut size = entry.clone();
        size.uncompressed_bytes -= 1;
        assert!(matches!(
            decode(&size),
            Err(FormatError::ByteCountMismatch { .. })
        ));
        // One byte more than the stream holds is found at the end of the
        // stream, where the hashing decoder compares its digests; the
        // incremental check cannot see it.
        let mut size = entry.clone();
        size.uncompressed_bytes += 1;
        let mut decoder = size.decoder_prehashed(&bytes[..]).unwrap();
        for _ in 0..items.len() {
            decoder.next_item().unwrap().unwrap();
        }
        assert!(matches!(
            decoder.next_item(),
            Err(FormatError::ByteCountMismatch {
                expected,
                actual
            }) if expected == entry.uncompressed_bytes + 1 && actual == entry.uncompressed_bytes
        ));
        // `verified` checks the digest itself, then streams without hashing.
        let decoder =
            DataFileDecoder::verified(&bytes, DataFileExpectations::from(&checksums)).unwrap();
        assert!(!decoder.hashing());
        assert!(
            DataFileDecoder::verified(&bytes, wrong_digests.expectations()).is_err(),
            "verified compares the digest"
        );
        // The hashing decoder hashes until its end-of-stream checks.
        let decoder = entry.decoder(&bytes[..]).unwrap();
        assert!(decoder.hashing());
    }

    /// The encoder reuses one line buffer: a short line after a long one is
    /// written whole and alone, and `encode_item_line_into` clears the
    /// buffer it is given.
    #[test]
    fn encoder_line_buffer_is_cleared_between_items() {
        let long = item(vec![("pk", AttributeValue::S("x".repeat(500)))]);
        let short = item(vec![("pk", AttributeValue::S("y".to_owned()))]);
        let (bytes, checksums) = encode_data_file([long.clone(), short.clone()]).unwrap();
        assert_eq!(
            decode_data_file(&bytes, DataFileExpectations::from(&checksums)).unwrap(),
            vec![long.clone(), short.clone()]
        );
        let expected =
            encode_item_line(&long).unwrap().len() + encode_item_line(&short).unwrap().len();
        assert_eq!(checksums.uncompressed_bytes, expected as u64);
        let mut line = b"left over".to_vec();
        encode_item_line_into(&short, &mut line).unwrap();
        assert_eq!(line, encode_item_line(&short).unwrap());
    }
}
