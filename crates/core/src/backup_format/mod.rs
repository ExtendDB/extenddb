// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Portable backup format: pure types, serde, and checksum helpers.
//!
//! A backup is a store prefix holding the `ExtendDB` manifest
//! (`extenddb-manifest.json`), the two manifests Amazon `DynamoDB` writes for
//! `ExportTableToPointInTime` with `DYNAMODB_JSON` output
//! (`manifest-summary.json` and `manifest-files.json`), and gzip data files of
//! newline-delimited `{"Item": ...}` objects under a `data/` prefix. Because
//! the `DynamoDB`-shaped pieces follow the service's own export layout, the
//! `data/` prefix is a valid `ImportTable` source for the real service, and a
//! real export is restorable here.
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
mod layout;
mod manifest;

#[cfg(test)]
mod golden_tests;

pub use data_file::{
    DataFileChecksums, DataFileDecoder, DataFileExpectations, MAX_LINE_BYTES, decode_data_file,
    encode_data_file,
};
pub use ddb_export::{
    DYNAMODB_JSON_OUTPUT_FORMAT, EXPORT_SUMMARY_VERSION, ExportFileEntry, ExportSummaryManifest,
    parse_files_manifest, write_files_manifest,
};
pub use error::FormatError;
pub use layout::{
    BackupArnParts, DATA_PREFIX, EXTENDDB_MANIFEST_FILE, FILES_MANIFEST_FILE,
    SUMMARY_MANIFEST_FILE, backup_prefix, data_file_key, parse_backup_arn,
};
pub use manifest::{
    CURRENT_FORMAT_VERSION, DataFileEntry, ExtenddbManifest, SnapshotDescriptor, SourceTableSchema,
};
