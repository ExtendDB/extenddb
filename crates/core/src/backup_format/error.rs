// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Error type for backup format encoding, decoding, and verification.
//!
//! Every message that echoes an untrusted field (a manifest member, a data
//! file key, an attribute name or value, serde's own text about the input)
//! carries at most a bounded slice of it: [`truncated`] for fields this module
//! formats, and [`JsonError`]'s `Display` and `Debug` for serde's messages,
//! which quote the input (an unknown `item_order` variant, a duplicate set
//! member list) without a bound of their own.

use std::fmt::Write as _;

use thiserror::Error;

/// Longest form of one untrusted field an error message carries: the bytes
/// of its `{:?}` rendering, escapes included and the two quotes not.
pub(super) const SHOWN_FIELD_BYTES: usize = 80;

/// The prefix kept of a field longer than [`SHOWN_FIELD_BYTES`], in the same
/// measure; the rest is replaced by its length, so the `{:?}` rendering of
/// the result stays within `SHOWN_FIELD_BYTES + 2` bytes.
const SHOWN_PREFIX_BYTES: usize = 48;

/// Longest serde message [`FormatError::Json`] displays, before its line and
/// column.
const JSON_MESSAGE_BYTES: usize = 120;

/// An untrusted string for an error message: the string itself when an
/// upper bound of its `{:?}` rendering is at most [`SHOWN_FIELD_BYTES`]
/// bytes, otherwise a prefix and the total length
/// (`data/00000000... <2000010 bytes>`). Call sites format the result with
/// `{:?}`. The rendering is measured, not the raw length, because escaping
/// expands a control character up to tenfold (`\u{7f}` for one byte). The
/// measure is each character's `char::escape_debug`, which also escapes `'`
/// where `str`'s `Debug` does not, so a string of apostrophes is cut at 80
/// bytes although it would render in fewer.
pub(super) fn truncated(s: &str) -> String {
    let mut shown = 0;
    let mut prefix = 0;
    for c in s.chars() {
        // `char::escape_debug` escapes at least what `str`'s `Debug` does
        // (it also escapes `'`), so this is an upper bound; it is measured
        // in bytes, since an unescaped character can take up to four.
        shown += c.escape_debug().map(char::len_utf8).sum::<usize>();
        if shown > SHOWN_FIELD_BYTES {
            return format!("{}... <{} bytes>", &s[..prefix], s.len());
        }
        if shown <= SHOWN_PREFIX_BYTES {
            prefix += c.len_utf8();
        }
    }
    s.to_owned()
}

/// A message that quotes untrusted text without escaping it (serde's): at
/// most `max` bytes, control characters escaped, the remainder replaced by
/// its length.
fn bounded_text(s: &str, max: usize) -> String {
    let mut out = String::new();
    for c in s.chars() {
        let before = out.len();
        if c.is_control() {
            let _ = write!(out, "{}", c.escape_debug());
        } else {
            out.push(c);
        }
        if out.len() > max {
            out.truncate(before);
            let _ = write!(out, "... <{} bytes>", s.len());
            break;
        }
    }
    out
}

/// A `serde_json` error with its message bounded: the payload of
/// [`FormatError::Json`].
///
/// serde's message quotes the input without a bound, so neither `Display`
/// nor `Debug` prints it whole: both carry at most [`JSON_MESSAGE_BYTES`] of
/// it, control characters escaped, then the position. The inner error is not
/// exposed (nor as `source()`), so a reporter that walks the chain or
/// debug-prints the error (`unwrap`, `expect`, a `tracing` `?err` field)
/// cannot print it whole either; [`Self::line`] and [`Self::column`] give the
/// position.
///
/// An error in a data-file line names the line: [`Self::data_file_line`] is
/// its 1-based number in the file, and serde's position is within that line
/// (each line is parsed on its own). serde takes a position from the end of
/// a message raised inside the deserializer (` at line 3 column 4`), and a
/// type descriptor or attribute name quoted at the end of such a message can
/// carry that text, so serde's line is not always 1. The column is shown,
/// as `at column N of the line`, only when serde's line is 1; otherwise the
/// message is shown as serde rendered it, with no position of this type's.
pub struct JsonError {
    inner: serde_json::Error,
    data_file_line: Option<u64>,
}

impl JsonError {
    /// An error in line `line` (1-based) of a data file.
    pub(super) fn on_data_file_line(inner: serde_json::Error, line: u64) -> Self {
        Self {
            inner,
            data_file_line: Some(line),
        }
    }

    /// serde's 1-based line within the document, or 0 when serde gave no
    /// position. Within a data-file line it is 1 unless the message carried
    /// a position of its own (type docs).
    #[must_use]
    pub fn line(&self) -> usize {
        self.inner.line()
    }

    /// serde's 1-based column within the line, or 0 when serde gave no
    /// position.
    #[must_use]
    pub fn column(&self) -> usize {
        self.inner.column()
    }

    /// The 1-based number of the data-file line that failed to parse, or
    /// `None` for a document that is not a data file (a manifest).
    #[must_use]
    pub fn data_file_line(&self) -> Option<u64> {
        self.data_file_line
    }

    /// serde's message bounded, without the position unless `keep_position`.
    fn bounded_message(&self, keep_position: bool) -> String {
        let full = self.inner.to_string();
        let message = if self.inner.line() == 0 || keep_position {
            full.as_str()
        } else {
            let position = format!(" at line {} column {}", self.line(), self.column());
            full.strip_suffix(&position).unwrap_or(&full)
        };
        bounded_text(message, JSON_MESSAGE_BYTES)
    }
}

impl From<serde_json::Error> for JsonError {
    fn from(inner: serde_json::Error) -> Self {
        Self {
            inner,
            data_file_line: None,
        }
    }
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.data_file_line, self.line()) {
            (None, 0) => write!(f, "{}", self.bounded_message(false)),
            (None, line) => write!(
                f,
                "{} at line {line} column {}",
                self.bounded_message(false),
                self.column()
            ),
            (Some(n), 1) => write!(
                f,
                "data file line {n}: {} at column {} of the line",
                self.bounded_message(false),
                self.column()
            ),
            (Some(n), _) => write!(f, "data file line {n}: {}", self.bounded_message(true)),
        }
    }
}

impl std::fmt::Debug for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonError")
            .field("message", &self.bounded_message(false))
            .field("line", &self.line())
            .field("column", &self.column())
            .field("data_file_line", &self.data_file_line)
            .finish_non_exhaustive()
    }
}

/// Errors produced while reading, writing, or verifying the backup format.
#[derive(Debug, Error)]
pub enum FormatError {
    /// The manifest declares a format version this build does not read:
    /// 0, or later than `supported`. A build reads every version from 1 to
    /// the one it writes.
    #[error(
        "unsupported backup format version {found}; this build reads format versions 1 to {supported}"
    )]
    UnsupportedFormatVersion {
        /// Version found in the manifest.
        found: u32,
        /// The latest version this build reads (`CURRENT_FORMAT_VERSION`);
        /// it reads every version from 1 to this one.
        supported: u32,
    },

    /// A checksum did not match the expected value.
    #[error("{subject} checksum mismatch: expected {expected:?}, computed {computed}")]
    ChecksumMismatch {
        /// What was checksummed, for the error message.
        subject: &'static str,
        /// The recorded checksum, cut by [`truncated`] where it is untrusted;
        /// displayed with `{:?}`, so a control character in it is escaped.
        expected: String,
        /// The checksum computed from the bytes at hand.
        computed: String,
    },

    /// The number of items decoded differs from the number recorded.
    #[error("item count mismatch: expected {expected}, decoded {actual}")]
    ItemCountMismatch {
        /// The recorded item count.
        expected: u64,
        /// The number of items actually decoded.
        actual: u64,
    },

    /// The uncompressed byte count differs from the number recorded.
    #[error("uncompressed byte count mismatch: expected {expected}, decoded {actual}")]
    ByteCountMismatch {
        /// The recorded uncompressed byte count.
        expected: u64,
        /// The number of bytes actually decoded.
        actual: u64,
    },

    /// The number of compressed bytes read from a data file differs from the
    /// size recorded for it. Raised as soon as the reader has consumed more
    /// than the recorded size (then `actual` is the count when it stopped,
    /// one more than `expected`), and at end of stream when it consumed
    /// fewer.
    #[error("compressed size mismatch: expected {expected} bytes, read {actual}")]
    CompressedSizeMismatch {
        /// The recorded compressed size.
        expected: u64,
        /// The number of compressed bytes read.
        actual: u64,
    },

    /// A backup ARN did not have the expected shape.
    #[error("invalid backup ARN: {0}")]
    InvalidArn(String),

    /// JSON serialization or deserialization failed: a manifest that does
    /// not parse, or a data file line that is not one `{"Item": ...}` object
    /// (the message then names the line).
    ///
    /// The message carries serde's text cut to a bound (serde quotes the
    /// input, unbounded), in `Display` and `Debug` alike; see [`JsonError`].
    /// The manifest's nested members reuse the request wire types, whose
    /// enum deserializers word their errors as the API does, so a bad value
    /// in one of them reads like a request validation error (`1 validation
    /// error detected: Value 'MANHATTAN' at 'distanceFunction' failed to
    /// satisfy constraint ...`, `'keySchema.1.member.keyType'`): request-path
    /// text about a stored document, in a restore log. Decoupling the types
    /// would change the format's authority; the text is left as is.
    #[error("JSON error: {0}")]
    Json(JsonError),

    /// A single decompressed data-file line exceeded the maximum allowed
    /// length, so decoding stopped before materializing it in full.
    #[error(
        "data file line exceeded the {cap}-byte maximum length; \
         refusing to decompress further to bound memory"
    )]
    LineTooLong {
        /// The maximum line length, in bytes.
        cap: usize,
    },

    /// A `DataFileEntry.key` did not have the required
    /// `data/<name>.json.gz` shape.
    #[error("invalid data file key {key:?}: {reason}")]
    InvalidDataFileKey {
        /// The offending key.
        key: String,
        /// Why the key was rejected.
        reason: &'static str,
    },

    /// A `DataFileEntry` other than its key is malformed: a digest of the
    /// wrong shape, a zero size or item count, or a `key_range` missing from
    /// a key-ordered manifest or present in an unordered one.
    #[error("invalid data file entry {key:?}: {reason}")]
    InvalidDataFileEntry {
        /// The entry's key.
        key: String,
        /// Why the entry was rejected.
        reason: &'static str,
    },

    /// A key-ordered data file's items do not ascend strictly (refused by
    /// the writer's `KeyRangeTracker` and the reader's `KeyRangeCheck`), or
    /// do not match the file's `key_range`: the first or last item is not
    /// the recorded endpoint (refused by `KeyRangeCheck`).
    #[error("data file items break key order: {0}")]
    KeyOrderViolation(String),

    /// Writing to the gzip encoder failed.
    #[error("data file encode error: {0}")]
    Encode(String),

    /// Gzip decompression failed or the stream was malformed.
    #[error("data file decode error: {0}")]
    Decode(String),

    /// The key schema has no order the format can define (multi-part keys,
    /// or a key attribute without a definition).
    #[error("key schema has no format-defined order: {0}")]
    UnorderableKeySchema(String),

    /// An item's key attributes do not match the key schema.
    #[error("invalid key: {0}")]
    InvalidKey(String),

    /// A manifest is inconsistent: its identity fields disagree, its data
    /// file list has overlapping or unsorted key ranges or a size total that
    /// does not match, or its checksum sidecar is not in the one accepted
    /// shape.
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),
}

impl From<serde_json::Error> for FormatError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e.into())
    }
}
