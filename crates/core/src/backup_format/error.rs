// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Error type for backup format encoding, decoding, and verification.

use thiserror::Error;

/// Errors produced while reading, writing, or verifying the backup format.
#[derive(Debug, Error)]
pub enum FormatError {
    /// The manifest declares a format version this build does not understand.
    #[error(
        "unsupported backup format version {found}; this build supports format version {supported}"
    )]
    UnsupportedFormatVersion {
        /// Version found in the manifest.
        found: u32,
        /// Version this build supports.
        supported: u32,
    },

    /// A checksum did not match the expected value.
    #[error("{subject} checksum mismatch: expected {expected}, computed {computed}")]
    ChecksumMismatch {
        /// What was checksummed, for the error message.
        subject: &'static str,
        /// The recorded checksum.
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

    /// A backup ARN did not have the expected shape.
    #[error("invalid backup ARN: {0}")]
    InvalidArn(String),

    /// JSON serialization or deserialization failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

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

    /// Writing to the gzip encoder failed.
    #[error("data file encode error: {0}")]
    Encode(String),

    /// Gzip decompression failed or the stream was malformed.
    #[error("data file decode error: {0}")]
    Decode(String),
}
