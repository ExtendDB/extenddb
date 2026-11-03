// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Portable backup format: pure types, serde, and checksum helpers.
//!
//! A backup is a store prefix holding the `ExtendDB` manifest
//! (`extenddb-manifest.json`) and its checksum sidecar
//! (`extenddb-manifest.sha256`), the two manifests Amazon `DynamoDB` writes for
//! `ExportTableToPointInTime` with `DYNAMODB_JSON` output
//! (`manifest-summary.json` and `manifest-files.json`), and gzip data files of
//! newline-delimited `{"Item": ...}` objects under a `data/` prefix. The
//! `DynamoDB`-shaped pieces follow the shapes of the service's own export
//! layout for the two manifests and the data files; the service also writes
//! `manifest-summary.checksum`, `manifest-summary.md5`,
//! `manifest-files.checksum` and an empty `_started` object, which
//! `ExtendDB` does not. The `data/` prefix is an `ImportTable` source for the
//! real service by construction (the file encoding is the documented one),
//! not verified against the service; a live check is Phase 2 in the design.
//! `ImportTable`'s own limits apply to it: at most 50,000 objects under the
//! prefix and items of at most 400 KB, so a backup of more than 50,000 data
//! files, or of a table whose operator-raised `max_item_size_bytes` let
//! larger items in, is not importable there. The other direction holds
//! only in part: [`DataFileDecoder`] reads a data file of a `DynamoDB`
//! export, but a backup needs an `extenddb-manifest.json`, which an export
//! does not have, [`DataFileEntry::validate_key`] refuses the service's
//! data-file names, and the service writes an empty data file for an empty
//! partition where this format has no empty data file, so a `DynamoDB`
//! export is not a backup this module can restore.
//!
//! A backup job writes the data files through [`DataFileEncoder`], with one
//! [`KeyRangeTracker`] over all of them when the backup is key-ordered
//! ([`KeyRangeTracker::next_file`] between files), then the documents
//! [`BackupDocuments::build`] returns, in the write order its type docs
//! state; the manifest is written last and is the commit record. A restore
//! reads the manifest with [`verify_manifest_checksum`] and
//! [`ExtenddbManifest::from_json_bytes`], and each data file through
//! [`ExtenddbManifest::reader`] or, after hashing it itself,
//! [`ExtenddbManifest::reader_prehashed`].
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
//!
//! The design overview (`docs/design/15-backup-restore.md`, #396) states the
//! rules the format's writers and readers honour; this module's docs and the
//! testdata README are the record of the format itself.

mod data_file;
mod ddb_export;
mod documents;
mod error;
// `key_order` is public as a module and its two items are re-exported
// below: #395 names the module in docs (`backup_format::key_order`) and
// uses the re-exports in code; both paths stay.
pub mod key_order;
mod layout;
mod manifest;
mod strict_item;

#[cfg(test)]
mod golden_tests;

pub use data_file::{
    CompressedDigest, CompressedHasher, DataFileChecksums, DataFileDecoder, DataFileEncoder,
    DataFileExpectations, GZIP_LEVEL, MAX_LINE_BYTES, decode_data_file, encode_data_file,
    encode_item_line, encode_item_line_into,
};
pub use ddb_export::{
    DYNAMODB_JSON_OUTPUT_FORMAT, EXPORT_SUMMARY_VERSION, ExportFileEntry, ExportSummaryManifest,
    parse_files_manifest,
};
pub use documents::{BackupDocuments, ManifestHeader};
pub use error::{FormatError, JsonError};
pub use key_order::{KeyOrder, hash_key_text};
pub use layout::{
    BACKUP_ID_MILLIS_DIGITS, BackupArn, BackupArnError, BackupArnParts, DATA_PREFIX,
    EXTENDDB_MANIFEST_FILE, FILES_MANIFEST_FILE, MANIFEST_CHECKSUM_FILE, MAX_BACKUP_ARN_LEN,
    SUMMARY_MANIFEST_FILE, backup_creation_date_time, backup_id_epoch_millis, check_backup_arn,
    data_file_key, format_backup_arn, format_backup_id, is_backup_id, parse_backup_arn,
    trim_backup_arn,
};
pub use manifest::{
    CURRENT_FORMAT_VERSION, DataFileEntry, DataFileReader, ExtenddbManifest, ItemOrder, KeyRange,
    KeyRangeCheck, KeyRangeTracker, SnapshotDescriptor, SourceTableSchema, check_format_version,
    verify_manifest_checksum,
};
