// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Portable backup format: pure types, serde, and checksum helpers.
//!
//! A backup is a store prefix holding the `ExtendDB` manifest
//! (`extenddb-manifest.json`) and its checksum sidecar
//! (`extenddb-manifest.sha256`), the two manifests Amazon `DynamoDB` writes for
//! `ExportTableToPointInTime` with `DYNAMODB_JSON` output
//! (`manifest-summary.json` and `manifest-files.json`), and gzip data files of
//! newline-delimited `{"Item": ...}` objects under a `data/` prefix. Because
//! the `DynamoDB`-shaped pieces follow the service's own export layout, the
//! `data/` prefix is a valid `ImportTable` source for the real service, and a
//! real export is restorable here.
//!
//! Write order: data files, `manifest-files.json`, `manifest-summary.json`,
//! `extenddb-manifest.sha256`, then `extenddb-manifest.json` last. The
//! manifest's presence is the commit record; a prefix with a manifest but no
//! sidecar, or whose sidecar does not match, is corrupt and is refused.
//!
//! The store key space is case-sensitive, as `DynamoDB` table names are
//! (`Music` and `music` are different tables). A filesystem store on a
//! case-insensitive filesystem (macOS APFS default volumes) cannot hold
//! backups of both; the store documents that constraint, the format does not
//! work around it.
//!
//! The `DynamoDB` export layout is documented at
//! <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html>,
//! which is the source of truth for the field sets and encodings in
//! [`ExportSummaryManifest`] and [`ExportFileEntry`].
//!
//! This module performs no I/O and has no storage dependency: callers hand it
//! bytes and iterators, and it hands back bytes, types, and checksums.

mod data_file;
mod ddb_export;
mod error;
pub mod key_order;
mod layout;
mod manifest;

#[cfg(test)]
mod golden_tests;

pub use data_file::{
    CompressedDigest, CompressedHasher, DataFileChecksums, DataFileDecoder, DataFileEncoder,
    DataFileExpectations, GZIP_LEVEL, MAX_LINE_BYTES, decode_data_file, encode_data_file,
};
pub use ddb_export::{
    DYNAMODB_JSON_OUTPUT_FORMAT, EXPORT_SUMMARY_VERSION, ExportFileEntry, ExportSummaryManifest,
    parse_files_manifest, write_files_manifest,
};
pub use error::FormatError;
pub use key_order::{KeyOrder, hash_key_text};
pub use layout::{
    BackupArn, DATA_PREFIX, EXTENDDB_MANIFEST_FILE, FILES_MANIFEST_FILE, MANIFEST_CHECKSUM_FILE,
    SUMMARY_MANIFEST_FILE, data_file_key,
};
pub use manifest::{
    CURRENT_FORMAT_VERSION, DataFileEntry, ExtenddbManifest, ItemOrder, KeyRange,
    SnapshotDescriptor, SourceTableSchema, manifest_checksum, verify_manifest_checksum,
};
