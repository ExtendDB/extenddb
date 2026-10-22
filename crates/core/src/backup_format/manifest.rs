// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `extenddb-manifest.json` types: the `ExtendDB` extension to the
//! `DynamoDB` export layout, and the commit record of a backup.
//!
//! The manifest is written last; its presence marks a complete backup. It
//! carries everything a restore needs that the `DynamoDB` export manifests do
//! not: the full source table definition, the snapshot descriptor, the item
//! order guarantee, and a sha256 per data file.
//!
//! Integrity of the manifest itself is a sidecar object,
//! `extenddb-manifest.sha256`, in `sha256sum` format: the lowercase hex
//! sha256 of the exact bytes of `extenddb-manifest.json`, two spaces, the
//! file name, newline. The writer computes it from the bytes it is about to
//! store and writes the sidecar before the manifest; a reader hashes the
//! bytes it fetched and compares, and an operator can run
//! `sha256sum -c extenddb-manifest.sha256`. Hashing stored bytes avoids
//! defining a canonical JSON form: an earlier revision hashed a re-serialized
//! struct with the checksum field blanked, which tied the digest to one JSON
//! library's float formatting and field order and would have failed for any
//! field an older reader did not model.
//!
//! Optional members of [`SourceTableSchema`] are always serialized, as `null`
//! when absent, for human readers and non-Rust tools; index lists are plain
//! arrays, empty when the table has none. This build's reader accepts a
//! missing member as `null` (`#[serde(default)]`), so "the writer did not
//! know this field" is signalled by `format_version`, not by absence.
//! `vector_indexes` is the one member whose `null` carries meaning, see its
//! docs. Nested members reuse the `DynamoDB` wire types, whose own optional
//! members are omitted when absent, as on the wire; the checked-in golden
//! manifest under `testdata/extenddb-manifest/` pins the exact bytes so a
//! change to one of those shared types fails a test here.
//!
//! Casing: top-level and `source_table` member names are `snake_case`;
//! `item_order` values are `snake_case`; members of the nested wire types
//! and their enum values (`PROVISIONED`, `HASH`) keep the `DynamoDB` API
//! spelling. Frozen with format version 1.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::FormatError;
use super::data_file::hex_encode;
use super::key_order::KeyOrder;
use super::layout::{BackupArn, DATA_PREFIX, EXTENDDB_MANIFEST_FILE};
use crate::types::{
    AttributeDefinition, BillingMode, GsiInput, Item, KeySchemaElement, LsiInput,
    OnDemandThroughput, ProvisionedThroughput, SseDescription, VectorIndexSpecification,
};

/// The backup format version this build reads and writes.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// How the source backend took its consistent snapshot.
///
/// `kind` names the mechanism (for example `postgres_repeatable_read`) and
/// `marker` records the backend-specific position, kept for diagnostics and
/// for a later change-log archive to anchor against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDescriptor {
    /// The snapshot mechanism, named by the backend that took it.
    pub kind: String,
    /// Backend-specific snapshot position.
    pub marker: String,
}

/// Order of items within and across a backup's data files.
///
/// A restore may exploit `KeyOrdered` (SQLite inserts into a fresh table 3 to
/// 4 times faster when rows arrive in primary-key order; measured with the
/// bundled SQLite on a 1M-item fixture, ratio only) but must produce the same
/// table either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemOrder {
    /// Items ascend in the order [`super::key_order::KeyOrder`] defines (hash
    /// key by its text encoding, bytewise; range key `S`/`B` bytewise, `N`
    /// numeric), strictly within each file and from one file to the next;
    /// every [`DataFileEntry::key_range`] is set and the ranges do not
    /// overlap. Only hash-only and single-range-key tables can be ordered.
    KeyOrdered,
    /// No ordering guarantee.
    Unordered,
}

/// The source table definition captured at backup time.
///
/// Field types reuse the existing wire types, so the nested JSON matches what
/// the `DynamoDB` API itself carries (`PascalCase` members inside each element).
/// Timestamps are `f64` epoch seconds, the same representation
/// [`crate::types::BackupDetails`] uses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceTableSchema {
    /// Name of the source table.
    pub table_name: String,
    /// Unique id of the source table.
    pub table_id: String,
    /// ARN of the source table.
    pub table_arn: String,
    /// When the source table was created, as epoch seconds.
    pub table_creation_date_time: f64,
    /// Base table key schema.
    pub key_schema: Vec<KeySchemaElement>,
    /// Attribute definitions for all key attributes.
    pub attribute_definitions: Vec<AttributeDefinition>,
    /// Billing mode at backup time.
    pub billing_mode: BillingMode,
    /// Provisioned throughput; `null` when the table is on demand.
    #[serde(default)]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    /// On-demand throughput limits; `null` when none are configured.
    #[serde(default)]
    pub on_demand_throughput: Option<OnDemandThroughput>,
    /// Table class; `null` when the source reported none.
    #[serde(default)]
    pub table_class: Option<String>,
    /// Server-side encryption as `DescribeTable` reports it; `null` when the
    /// source reported none. A restore maps it to an `SSESpecification`.
    #[serde(default)]
    pub sse_description: Option<SseDescription>,
    /// Global secondary indexes defined on the source table; empty when none.
    #[serde(default)]
    pub global_secondary_indexes: Vec<GsiInput>,
    /// Local secondary indexes defined on the source table; empty when none.
    #[serde(default)]
    pub local_secondary_indexes: Vec<LsiInput>,
    /// Vector indexes, as creation specifications. Three states: `null`, the
    /// source backend has no vector support (a target with vector support
    /// restores the table without vector indexes); `[]`, the source supports
    /// vector search and the table had no vector indexes; a non-empty list,
    /// which a target without vector support refuses to restore rather than
    /// dropping them.
    #[serde(default)]
    pub vector_indexes: Option<Vec<VectorIndexSpecification>>,
    /// Item count at backup time.
    pub item_count: i64,
    /// Table size in bytes at backup time.
    pub table_size_bytes: i64,
}

/// First and last base-table key in a data file, as `DynamoDB` JSON items
/// holding exactly the key attributes (enforced by
/// [`ExtenddbManifest::validate_data_files`]). Required when the backup is
/// [`ItemOrder::KeyOrdered`] and refused when it is unordered; lets a restore
/// reject overlapping files (a duplicate primary key across files) before
/// downloading any of them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyRange {
    /// Key of the first item in the file.
    pub first: Item,
    /// Key of the last item in the file.
    pub last: Item,
}

/// One data file recorded in the manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFileEntry {
    /// Store key of the file, relative to the backup prefix
    /// (for example `data/000001.json.gz`).
    pub key: String,
    /// Number of items in the file.
    pub item_count: u64,
    /// Lowercase hex sha256 of the file's compressed bytes.
    pub sha256: String,
    /// Base64 MD5 of the file's compressed bytes; the same value the
    /// `manifest-files.json` line for this file carries as `md5Checksum`.
    pub md5: String,
    /// Size of the file's compressed bytes.
    pub bytes: u64,
    /// Size of the file's uncompressed lines, newlines included. A decoder
    /// enforces it incrementally, so a corrupt or hostile file is rejected
    /// before it is fully expanded.
    pub uncompressed_bytes: u64,
    /// Key range covered by the file. Required for a key-ordered backup and
    /// must be `null` for an unordered one; both enforced by
    /// [`ExtenddbManifest::validate_data_files`].
    #[serde(default)]
    pub key_range: Option<KeyRange>,
}

impl DataFileEntry {
    /// Validate that `key` names a data file within the backup's `data/`
    /// prefix and nowhere else.
    ///
    /// The key must be [`DATA_PREFIX`] followed by exactly one path segment
    /// that matches the data-file naming pattern (`<digits>.json.gz`). This
    /// rejects `..`, absolute paths, and any extra directory level, so a
    /// malicious manifest cannot point the restore path at a store key outside
    /// the backup. Validation lives in the format layer, the natural home for
    /// the layout rule, independent of any store-level sandboxing.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] when the key is not a
    /// single data-file entry under the prefix.
    pub fn validate_key(&self) -> Result<(), FormatError> {
        let reject = |reason: &'static str| FormatError::InvalidDataFileKey {
            key: self.key.clone(),
            reason,
        };

        let name = self
            .key
            .strip_prefix(DATA_PREFIX)
            .ok_or_else(|| reject("must start with the data/ prefix"))?;

        if name.is_empty() {
            return Err(reject("names no file under data/"));
        }
        // Exactly one segment: no further path separators, no traversal, no
        // absolute component.
        if name.contains('/') {
            return Err(reject("must hold exactly one path segment under data/"));
        }
        if name.contains('\\') {
            return Err(reject("must not contain a backslash"));
        }
        if name == "." || name == ".." || name.contains("..") {
            return Err(reject("must not contain a path-traversal component"));
        }
        // The data-file naming pattern: one or more digits then .json.gz.
        let digits = name
            .strip_suffix(".json.gz")
            .ok_or_else(|| reject("must end with .json.gz"))?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(reject("file stem must be all digits"));
        }
        Ok(())
    }

    /// The expectations a decoder enforces for this file: every digest and
    /// count the entry records.
    #[must_use]
    pub fn expectations(&self) -> super::data_file::DataFileExpectations {
        super::data_file::DataFileExpectations {
            sha256_hex: Some(self.sha256.clone()),
            md5_base64: Some(self.md5.clone()),
            item_count: Some(self.item_count),
            uncompressed_bytes: Some(self.uncompressed_bytes),
        }
    }

    /// Streaming decoder over this entry's data file read from `compressed`,
    /// with the entry's digests and counts as expectations. Validates the key
    /// first. This is the restore path's entry point; the digests are checked
    /// when the stream ends, so a caller that must know them earlier hashes a
    /// local copy with [`crate::backup_format::CompressedHasher`] first.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] when the key is not a
    /// single data-file entry under the prefix.
    pub fn decoder<R: std::io::Read>(
        &self,
        compressed: R,
    ) -> Result<super::data_file::DataFileDecoder<R>, FormatError> {
        self.validate_key()?;
        Ok(super::data_file::DataFileDecoder::new(
            compressed,
            self.expectations(),
        ))
    }

    /// Decode this entry's in-memory data file into a `Vec`, validating the
    /// key first and verifying the digests before the first item. For tests
    /// and files known to be small; see [`Self::decoder`] for the streaming
    /// form.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] when the key is not a
    /// single data-file entry under the prefix, and any error
    /// [`crate::backup_format::decode_data_file`] can produce.
    pub fn decode(&self, compressed: &[u8]) -> Result<Vec<Item>, FormatError> {
        self.validate_key()?;
        super::data_file::decode_data_file(compressed, self.expectations())
    }
}

/// The `extenddb-manifest.json` document.
///
/// Serde leaves unknown fields tolerated (no `deny_unknown_fields`), so a
/// newer writer can add members without breaking an older reader; the hard
/// gate is [`Self::check_supported`], which refuses a `format_version` this
/// build does not understand. Because the integrity check hashes stored bytes
/// (see the module docs), adding an optional field does not by itself require
/// a version bump; changing the meaning of an existing field or adding a
/// field a reader must understand to restore correctly does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtenddbManifest {
    /// Format version; see [`CURRENT_FORMAT_VERSION`].
    pub format_version: u32,
    /// ARN of the backup, in the shape
    /// `arn:<partition>:dynamodb:<region>:<account>:table/<table>/backup/<id>`.
    pub backup_arn: String,
    /// Name given to the backup at creation.
    pub backup_name: String,
    /// When the backup was requested, as epoch seconds, the representation
    /// [`crate::types::BackupDetails`] uses. The snapshot the data files hold
    /// was taken at this time (see the design doc on request-time snapshots).
    pub backup_creation_date_time: f64,
    /// Total size of the backup's data files, in compressed bytes.
    pub backup_size_bytes: i64,
    /// The storage backend that produced the backup.
    pub source_backend: String,
    /// The `ExtendDB` version that produced the backup.
    pub source_extenddb_version: String,
    /// How the consistent snapshot was taken.
    pub snapshot: SnapshotDescriptor,
    /// Order of items in the data files.
    pub item_order: ItemOrder,
    /// The source table definition.
    pub source_table: SourceTableSchema,
    /// The data files that make up the backup, in the order a restore applies
    /// them. This order is authoritative; a store listing is never used to
    /// order files (zero-padding in [`super::data_file_key`] runs out at a
    /// million files).
    pub data_files: Vec<DataFileEntry>,
}

/// The content of the `extenddb-manifest.sha256` sidecar for a stored
/// manifest: `<lowercase hex sha256>  extenddb-manifest.json\n`, the
/// `sha256sum` output format.
#[must_use]
pub fn manifest_checksum(manifest_bytes: &[u8]) -> String {
    format!(
        "{}  {EXTENDDB_MANIFEST_FILE}\n",
        hex_encode(&Sha256::digest(manifest_bytes))
    )
}

/// Compare fetched manifest bytes against the sidecar's content. The digest
/// is the first whitespace-delimited token, compared case-insensitively, so a
/// bare digest and `sha256sum` output are both accepted.
///
/// # Errors
///
/// Returns [`FormatError::ChecksumMismatch`] when the digests differ or the
/// sidecar is empty.
pub fn verify_manifest_checksum(manifest_bytes: &[u8], sidecar: &str) -> Result<(), FormatError> {
    let expected = sidecar
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let computed = hex_encode(&Sha256::digest(manifest_bytes));
    if expected == computed {
        Ok(())
    } else {
        Err(FormatError::ChecksumMismatch {
            subject: "manifest",
            expected,
            computed,
        })
    }
}

/// The members a reader needs before it can decide whether to parse the
/// rest: the version gate runs on this so a document from a later version
/// fails with [`FormatError::UnsupportedFormatVersion`] rather than with a
/// parse error about a member this build does not know.
#[derive(Deserialize)]
struct VersionOnly {
    format_version: u32,
}

impl ExtenddbManifest {
    /// Serialize to the bytes stored as `extenddb-manifest.json`. The writer
    /// hashes exactly these bytes for the sidecar.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when serialization fails.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, FormatError> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Parse stored manifest bytes: the version gate first, then the full
    /// document, then the identity check ([`Self::validate_identity`]) and
    /// the data-file list checks ([`Self::validate_data_files`]). Callers
    /// verify the sidecar checksum against the same bytes first; see
    /// [`verify_manifest_checksum`].
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::UnsupportedFormatVersion`] when the version is
    /// not [`CURRENT_FORMAT_VERSION`] (checked before anything else is
    /// parsed), [`FormatError::Json`] when the bytes do not parse, and the
    /// errors of [`Self::validate_identity`] and
    /// [`Self::validate_data_files`].
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        // The gate reads only `format_version`, so a document from a later
        // version fails with that reason instead of a deserialization error
        // about a member this build does not know.
        let version: VersionOnly = serde_json::from_slice(bytes)?;
        if version.format_version != CURRENT_FORMAT_VERSION {
            return Err(FormatError::UnsupportedFormatVersion {
                found: version.format_version,
                supported: CURRENT_FORMAT_VERSION,
            });
        }
        let manifest: Self = serde_json::from_slice(bytes)?;
        manifest.validate_identity()?;
        manifest.validate_data_files()?;
        Ok(manifest)
    }

    /// The parsed backup ARN.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidArn`] when `backup_arn` is malformed.
    pub fn backup_arn(&self) -> Result<BackupArn, FormatError> {
        BackupArn::parse(&self.backup_arn)
    }

    /// Check that the manifest names one table: `backup_arn` parses, its
    /// table name equals `source_table.table_name`, and
    /// `source_table.table_arn` is a table ARN for the same account and
    /// table. `backup_arn` is the authoritative identity; the source-table
    /// fields are checked against it so a restore never has two answers to
    /// "which table is this". The table ARN's region is not compared: it is
    /// recorded when the table is created and the backup ARN carries the
    /// region configured when the backup is taken.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidArn`] when `backup_arn` is malformed,
    /// [`FormatError::InvalidManifest`] when the source-table fields disagree
    /// with it.
    pub fn validate_identity(&self) -> Result<(), FormatError> {
        let arn = self.backup_arn()?;
        if arn.table_name() != self.source_table.table_name {
            return Err(FormatError::InvalidManifest(format!(
                "backup_arn names table {:?} but source_table.table_name is {:?}",
                arn.table_name(),
                self.source_table.table_name
            )));
        }
        let table_arn = &self.source_table.table_arn;
        let fields: Vec<&str> = table_arn.splitn(6, ':').collect();
        let matches = match fields.as_slice() {
            ["arn", _partition, "dynamodb", _region, account, resource] => {
                *account == arn.account_id()
                    && resource
                        .strip_prefix("table/")
                        .is_some_and(|name| name == arn.table_name())
            }
            _ => false,
        };
        if !matches {
            return Err(FormatError::InvalidManifest(format!(
                "source_table.table_arn {table_arn:?} is not a table ARN for account {} and \
                 table {:?} named by backup_arn",
                arn.account_id(),
                arn.table_name()
            )));
        }
        Ok(())
    }

    /// The key order of the source table, when it has one.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::UnorderableKeySchema`] for a multi-part key.
    pub fn key_order(&self) -> Result<KeyOrder, FormatError> {
        KeyOrder::resolve(
            &self.source_table.key_schema,
            &self.source_table.attribute_definitions,
        )
    }

    /// Refuse a manifest whose format version this build does not understand.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::UnsupportedFormatVersion`] when `format_version`
    /// differs from [`CURRENT_FORMAT_VERSION`].
    pub fn check_supported(&self) -> Result<(), FormatError> {
        if self.format_version == CURRENT_FORMAT_VERSION {
            Ok(())
        } else {
            Err(FormatError::UnsupportedFormatVersion {
                found: self.format_version,
                supported: CURRENT_FORMAT_VERSION,
            })
        }
    }

    /// Validate the data-file list: every key passes the layout rule and
    /// appears once; `backup_size_bytes` equals the sum of the files' sizes;
    /// for a key-ordered manifest every file carries a `key_range` whose
    /// `first` and `last` are key-only items of the schema's shape with
    /// `first <= last`, and the ranges ascend without overlap from one file
    /// to the next; for an unordered manifest no file carries a `key_range`
    /// (there is no order for it to describe, and a reader must not be
    /// handed ranges it cannot check).
    ///
    /// Callers run this when a manifest is parsed, and the decode path runs
    /// the same per-entry [`DataFileEntry::validate_key`] again before it
    /// resolves a key, so a key that slips past one check is caught at the
    /// other. Within-file order and duplicates are checked by the restore
    /// while it streams the file; this check covers what the manifest alone
    /// can prove.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidDataFileKey`] for a bad or duplicate key, a
    /// missing range on a key-ordered manifest, or a range on an unordered
    /// one; [`FormatError::InvalidManifest`] for a size mismatch or a range
    /// violation; [`FormatError::InvalidKey`] for a malformed range endpoint;
    /// [`FormatError::UnorderableKeySchema`] for a key-ordered manifest whose
    /// schema has no order.
    pub fn validate_data_files(&self) -> Result<(), FormatError> {
        let mut seen = std::collections::BTreeSet::new();
        let mut total: u64 = 0;
        for entry in &self.data_files {
            entry.validate_key()?;
            if !seen.insert(entry.key.as_str()) {
                return Err(FormatError::InvalidDataFileKey {
                    key: entry.key.clone(),
                    reason: "listed more than once",
                });
            }
            if self.item_order == ItemOrder::Unordered && entry.key_range.is_some() {
                return Err(FormatError::InvalidDataFileKey {
                    key: entry.key.clone(),
                    reason: "unordered manifest entry has a key_range",
                });
            }
            total = total.saturating_add(entry.bytes);
        }
        if i64::try_from(total).ok() != Some(self.backup_size_bytes) {
            return Err(FormatError::InvalidManifest(format!(
                "backup_size_bytes is {} but the data files total {total}",
                self.backup_size_bytes
            )));
        }
        if self.item_order == ItemOrder::KeyOrdered {
            let order = self.key_order()?;
            let mut previous_last: Option<&Item> = None;
            for entry in &self.data_files {
                let range =
                    entry
                        .key_range
                        .as_ref()
                        .ok_or_else(|| FormatError::InvalidDataFileKey {
                            key: entry.key.clone(),
                            reason: "key_ordered manifest entry has no key_range",
                        })?;
                order.validate_key_item(&range.first)?;
                order.validate_key_item(&range.last)?;
                if order.compare(&range.first, &range.last)? == std::cmp::Ordering::Greater {
                    return Err(FormatError::InvalidManifest(format!(
                        "{}: key_range first is after last",
                        entry.key
                    )));
                }
                if let Some(prev) = previous_last
                    && order.compare(prev, &range.first)? != std::cmp::Ordering::Less
                {
                    return Err(FormatError::InvalidManifest(format!(
                        "{}: key_range overlaps or does not follow the previous file",
                        entry.key
                    )));
                }
                previous_last = Some(&range.last);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyType, Projection, ProjectionType, ScalarAttributeType};

    #[allow(clippy::too_many_lines)]
    fn sample_manifest() -> ExtenddbManifest {
        ExtenddbManifest {
            format_version: CURRENT_FORMAT_VERSION,
            backup_arn:
                "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01489602797149-73d8d5bc"
                    .to_owned(),
            backup_name: "music-nightly".to_owned(),
            backup_creation_date_time: 1_757_500_000.123,
            backup_size_bytes: 61_728,
            source_backend: "postgres".to_owned(),
            source_extenddb_version: "0.1.11".to_owned(),
            snapshot: SnapshotDescriptor {
                kind: "postgres_repeatable_read".to_owned(),
                marker: "1234:1234:".to_owned(),
            },
            source_table: SourceTableSchema {
                table_name: "Music".to_owned(),
                table_id: "12345a12-abcd-123a-ab12-1234abc12345".to_owned(),
                table_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/Music".to_owned(),
                table_creation_date_time: 1_757_400_000.0,
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "Artist".to_owned(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "SongTitle".to_owned(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "Artist".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "SongTitle".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "Genre".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                billing_mode: BillingMode::Provisioned,
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 5,
                }),
                on_demand_throughput: None,
                table_class: Some("STANDARD".to_owned()),
                sse_description: Some(SseDescription {
                    status: "ENABLED".to_owned(),
                    sse_type: Some(crate::types::SseType::KMS),
                    kms_master_key_arn: Some(
                        "arn:aws:kms:us-east-1:123456789012:key/abcd".to_owned(),
                    ),
                }),
                global_secondary_indexes: vec![GsiInput {
                    index_name: "GenreIndex".to_owned(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "Genre".to_owned(),
                        key_type: KeyType::Hash,
                    }],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: Some(ProvisionedThroughput {
                        read_capacity_units: 1,
                        write_capacity_units: 1,
                    }),
                }],
                local_secondary_indexes: vec![LsiInput {
                    index_name: "TitleIndex".to_owned(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "Artist".to_owned(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "SongTitle".to_owned(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::KeysOnly,
                        non_key_attributes: None,
                    },
                }],
                vector_indexes: Some(vec![VectorIndexSpecification {
                    index_name: "embeddings".to_owned(),
                    dimensions: 1024,
                    distance_function: crate::types::DistanceFunction::Cosine,
                    vector_attribute: crate::types::VectorAttribute {
                        attribute_name: "vec".to_owned(),
                    },
                    search_schema: None,
                    projection: None,
                }]),
                item_count: 100_000,
                table_size_bytes: 123_456,
            },
            item_order: ItemOrder::Unordered,
            data_files: vec![DataFileEntry {
                key: "data/000001.json.gz".to_owned(),
                item_count: 50_000,
                sha256: "0".repeat(64),
                md5: "1B2M2Y8AsgTpgAmY7PhCfg==".to_owned(),
                bytes: 61_728,
                uncompressed_bytes: 300_000,
                key_range: None,
            }],
        }
    }

    #[test]
    fn manifest_serde_round_trip() {
        let manifest = sample_manifest();
        let bytes = manifest.to_json_bytes().unwrap();
        let parsed = ExtenddbManifest::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn snapshot_descriptor_serde_round_trip() {
        let snapshot = SnapshotDescriptor {
            kind: "sqlite_wal_read_txn".to_owned(),
            marker: String::new(),
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        let parsed: SnapshotDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, snapshot);
    }

    #[test]
    fn source_table_schema_serde_round_trip() {
        let schema = sample_manifest().source_table;
        let json = serde_json::to_string(&schema).unwrap();
        let parsed: SourceTableSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, schema);
    }

    #[test]
    fn data_file_entry_serde_round_trip() {
        let entry = DataFileEntry {
            key: "data/000042.json.gz".to_owned(),
            item_count: 7,
            sha256: "ab".repeat(32),
            md5: "cd".repeat(11),
            bytes: 1234,
            uncompressed_bytes: 5678,
            key_range: Some(KeyRange {
                first: [(
                    "pk".to_owned(),
                    crate::types::AttributeValue::S("a".to_owned()),
                )]
                .into_iter()
                .collect(),
                last: [(
                    "pk".to_owned(),
                    crate::types::AttributeValue::S("z".to_owned()),
                )]
                .into_iter()
                .collect(),
            }),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: DataFileEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, entry);
    }

    #[test]
    fn absent_optional_members_serialize_as_null() {
        let mut manifest = sample_manifest();
        manifest.source_table.on_demand_throughput = None;
        manifest.source_table.vector_indexes = None;
        manifest.source_table.table_class = None;
        manifest.source_table.sse_description = None;
        manifest.source_table.global_secondary_indexes.clear();
        let value = serde_json::to_value(&manifest).unwrap();
        let table = &value["source_table"];
        assert!(table["on_demand_throughput"].is_null());
        assert!(table["vector_indexes"].is_null());
        assert!(table["table_class"].is_null());
        assert!(table["sse_description"].is_null());
        assert_eq!(table["global_secondary_indexes"], serde_json::json!([]));
        assert!(value["data_files"][0]["key_range"].is_null());
    }

    #[test]
    fn vector_indexes_distinguish_unsupported_from_none() {
        let mut manifest = sample_manifest();
        manifest.source_table.vector_indexes = Some(vec![]);
        let bytes = manifest.to_json_bytes().unwrap();
        let parsed = ExtenddbManifest::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.source_table.vector_indexes, Some(vec![]));
        manifest.source_table.vector_indexes = None;
        let bytes = manifest.to_json_bytes().unwrap();
        let parsed = ExtenddbManifest::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed.source_table.vector_indexes, None);
    }

    fn key(artist: &str, title: &str) -> Item {
        [
            (
                "Artist".to_owned(),
                crate::types::AttributeValue::S(artist.to_owned()),
            ),
            (
                "SongTitle".to_owned(),
                crate::types::AttributeValue::S(title.to_owned()),
            ),
        ]
        .into_iter()
        .collect()
    }

    fn second_file(first: Item, last: Item) -> DataFileEntry {
        DataFileEntry {
            key: "data/000002.json.gz".to_owned(),
            item_count: 1,
            sha256: "1".repeat(64),
            md5: "x".to_owned(),
            bytes: 100,
            uncompressed_bytes: 200,
            key_range: Some(KeyRange { first, last }),
        }
    }

    #[test]
    fn key_ordered_manifest_requires_valid_non_overlapping_key_ranges() {
        let mut manifest = sample_manifest();
        manifest.item_order = ItemOrder::KeyOrdered;
        let err = manifest.validate_data_files().unwrap_err();
        assert!(
            matches!(err, FormatError::InvalidDataFileKey { .. }),
            "{err}"
        );

        // Endpoints must be key-only items of the schema's shape.
        manifest.data_files[0].key_range = Some(KeyRange {
            first: Item::new(),
            last: Item::new(),
        });
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidKey(_))
        ));
        let mut with_extra = key("a", "a");
        with_extra.insert(
            "Genre".to_owned(),
            crate::types::AttributeValue::S("x".into()),
        );
        manifest.data_files[0].key_range = Some(KeyRange {
            first: with_extra,
            last: key("a", "b"),
        });
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidKey(_))
        ));

        // first must not be after last.
        manifest.data_files[0].key_range = Some(KeyRange {
            first: key("b", "a"),
            last: key("a", "a"),
        });
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidManifest(_))
        ));

        // Valid single file.
        manifest.data_files[0].key_range = Some(KeyRange {
            first: key("a", "a"),
            last: key("m", "z"),
        });
        manifest.validate_data_files().unwrap();
        assert_eq!(
            serde_json::to_value(&manifest).unwrap()["item_order"],
            serde_json::json!("key_ordered")
        );

        // A second file must start strictly after the first ends.
        manifest
            .data_files
            .push(second_file(key("m", "z"), key("z", "z")));
        manifest.backup_size_bytes += 100;
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidManifest(_))
        ));
        manifest.data_files[1].key_range = Some(KeyRange {
            first: key("n", "a"),
            last: key("z", "z"),
        });
        manifest.validate_data_files().unwrap();
        // Files out of order are rejected too.
        manifest.data_files.swap(0, 1);
        manifest.data_files[0].key = "data/000001.json.gz".to_owned();
        manifest.data_files[1].key = "data/000002.json.gz".to_owned();
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidManifest(_))
        ));
    }

    #[test]
    fn duplicate_data_file_keys_are_rejected() {
        let mut manifest = sample_manifest();
        let dup = manifest.data_files[0].clone();
        manifest.data_files.push(dup);
        manifest.backup_size_bytes *= 2;
        let err = manifest.validate_data_files().unwrap_err();
        assert!(matches!(
            err,
            FormatError::InvalidDataFileKey {
                reason: "listed more than once",
                ..
            }
        ));
    }

    #[test]
    fn backup_size_must_equal_the_sum_of_data_files() {
        let mut manifest = sample_manifest();
        manifest.backup_size_bytes += 1;
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidManifest(_))
        ));
    }

    #[test]
    fn version_gate_runs_before_the_full_parse() {
        // A later version with a shape this build cannot parse must still be
        // reported as an unsupported version, not as a JSON error.
        let bytes = br#"{"format_version": 7, "something_new": {"x": 1}}"#;
        let err = ExtenddbManifest::from_json_bytes(bytes).unwrap_err();
        assert!(
            matches!(err, FormatError::UnsupportedFormatVersion { found: 7, .. }),
            "{err}"
        );
    }

    #[test]
    fn malformed_backup_arn_is_rejected_on_load() {
        let mut manifest = sample_manifest();
        manifest.backup_arn =
            "arn:aws:dynamodb:us-east-1:evil:1234:table/Music/backup/1".to_owned();
        let err =
            ExtenddbManifest::from_json_bytes(&manifest.to_json_bytes().unwrap()).unwrap_err();
        assert!(matches!(err, FormatError::InvalidArn(_)), "{err}");
    }

    /// `backup_arn` is the identity; the source-table name and ARN must agree
    /// with it.
    #[test]
    fn source_table_must_match_backup_arn() {
        let good = sample_manifest();
        good.validate_identity().unwrap();

        let mut other_name = good.clone();
        other_name.source_table.table_name = "Other".to_owned();
        let err =
            ExtenddbManifest::from_json_bytes(&other_name.to_json_bytes().unwrap()).unwrap_err();
        assert!(matches!(err, FormatError::InvalidManifest(_)), "{err}");
        assert!(err.to_string().contains("source_table.table_name"), "{err}");

        for bad_arn in [
            "arn:aws:dynamodb:us-east-1:123456789012:table/Other",
            "arn:aws:dynamodb:us-east-1:999999999999:table/Music",
            "arn:aws:s3:us-east-1:123456789012:table/Music",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/index/x",
            "Music",
        ] {
            let mut m = good.clone();
            m.source_table.table_arn = bad_arn.to_owned();
            let err = m.validate_identity().unwrap_err();
            assert!(
                matches!(err, FormatError::InvalidManifest(_)),
                "{bad_arn}: {err}"
            );
            assert!(err.to_string().contains("table_arn"), "{err}");
        }
        // The table ARN's region is recorded at table creation and is not
        // compared.
        let mut other_region = good;
        other_region.source_table.table_arn =
            "arn:aws:dynamodb:eu-west-1:123456789012:table/Music".to_owned();
        other_region.validate_identity().unwrap();
    }

    #[test]
    fn unordered_manifest_must_not_carry_key_ranges() {
        let mut manifest = sample_manifest();
        assert_eq!(manifest.item_order, ItemOrder::Unordered);
        manifest.data_files[0].key_range = Some(KeyRange {
            first: Item::new(),
            last: Item::new(),
        });
        let err = manifest.validate_data_files().unwrap_err();
        assert!(
            matches!(
                err,
                FormatError::InvalidDataFileKey {
                    reason: "unordered manifest entry has a key_range",
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        let manifest = sample_manifest();
        let mut value = serde_json::to_value(&manifest).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("added_in_a_later_version".to_owned(), serde_json::json!(42));
        let parsed: ExtenddbManifest = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn check_supported_accepts_current_version() {
        let manifest = sample_manifest();
        manifest.check_supported().unwrap();
    }

    #[test]
    fn check_supported_rejects_version_two() {
        let mut manifest = sample_manifest();
        manifest.format_version = 2;
        let err = manifest.check_supported().unwrap_err();
        match err {
            FormatError::UnsupportedFormatVersion { found, supported } => {
                assert_eq!(found, 2);
                assert_eq!(supported, CURRENT_FORMAT_VERSION);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn sidecar_checksum_round_trip() {
        let bytes = sample_manifest().to_json_bytes().unwrap();
        let sidecar = manifest_checksum(&bytes);
        // sha256sum format: 64 hex, two spaces, the file name, newline.
        assert!(
            sidecar.ends_with("  extenddb-manifest.json\n"),
            "{sidecar:?}"
        );
        assert_eq!(sidecar.len(), 64 + 2 + EXTENDDB_MANIFEST_FILE.len() + 1);
        verify_manifest_checksum(&bytes, &sidecar).unwrap();
        // A bare digest, with or without a newline, and upper-case hex are
        // accepted too.
        let digest = sidecar.split_whitespace().next().unwrap();
        verify_manifest_checksum(&bytes, digest).unwrap();
        verify_manifest_checksum(&bytes, &format!("{digest}\n")).unwrap();
        verify_manifest_checksum(&bytes, &digest.to_ascii_uppercase()).unwrap();
        assert!(verify_manifest_checksum(&bytes, "").is_err());
    }

    #[test]
    fn sidecar_checksum_detects_tampered_bytes() {
        let bytes = sample_manifest().to_json_bytes().unwrap();
        let sidecar = manifest_checksum(&bytes);
        let mut tampered = bytes.clone();
        let pos = tampered.windows(5).position(|w| w == b"50000").unwrap();
        tampered[pos] = b'7';
        let err = verify_manifest_checksum(&tampered, &sidecar).unwrap_err();
        assert!(matches!(
            err,
            FormatError::ChecksumMismatch {
                subject: "manifest",
                ..
            }
        ));
    }

    #[test]
    fn sidecar_checksum_is_over_stored_bytes_not_a_canonical_form() {
        // Re-serializing with different whitespace changes the digest: the
        // sidecar binds the stored object, nothing else.
        let manifest = sample_manifest();
        let compact = manifest.to_json_bytes().unwrap();
        let pretty = serde_json::to_vec_pretty(&manifest).unwrap();
        assert_ne!(manifest_checksum(&compact), manifest_checksum(&pretty));
        assert!(verify_manifest_checksum(&compact, &manifest_checksum(&pretty)).is_err());
        verify_manifest_checksum(&pretty, &manifest_checksum(&pretty)).unwrap();
    }

    #[test]
    fn from_json_bytes_runs_version_gate_and_key_check() {
        let mut manifest = sample_manifest();
        manifest.format_version = 2;
        let err =
            ExtenddbManifest::from_json_bytes(&manifest.to_json_bytes().unwrap()).unwrap_err();
        assert!(matches!(err, FormatError::UnsupportedFormatVersion { .. }));
        let mut manifest = sample_manifest();
        manifest.data_files[0].key = "../escape.json.gz".to_owned();
        let err =
            ExtenddbManifest::from_json_bytes(&manifest.to_json_bytes().unwrap()).unwrap_err();
        assert!(matches!(err, FormatError::InvalidDataFileKey { .. }));
    }

    #[test]
    fn valid_data_file_keys_pass() {
        for key in [
            "data/000001.json.gz",
            "data/42.json.gz",
            "data/1000000.json.gz",
        ] {
            let entry = DataFileEntry {
                key: key.to_owned(),
                item_count: 1,
                sha256: "0".repeat(64),
                md5: String::new(),
                bytes: 1,
                uncompressed_bytes: 1,
                key_range: None,
            };
            entry
                .validate_key()
                .unwrap_or_else(|e| panic!("{key:?} rejected: {e}"));
        }
    }

    #[test]
    fn data_file_key_rejects_bad_shapes() {
        for key in [
            "../x",                     // traversal above the prefix
            "/etc/passwd",              // absolute path, no prefix
            "other/f.json.gz",          // wrong prefix
            "data/../secret.json.gz",   // traversal inside the prefix
            "data/sub/000001.json.gz",  // extra directory level
            "data/",                    // no file
            "data/000001.json",         // wrong suffix
            "data/000001.txt.gz",       // wrong suffix
            "data/abc.json.gz",         // non-digit stem
            "data/.json.gz",            // empty stem
            "data\\000001.json.gz",     // backslash separator
            "data/000001.json.gz.evil", // trailing suffix
        ] {
            let entry = DataFileEntry {
                key: key.to_owned(),
                item_count: 1,
                sha256: "0".repeat(64),
                md5: String::new(),
                bytes: 1,
                uncompressed_bytes: 1,
                key_range: None,
            };
            assert!(
                matches!(
                    entry.validate_key(),
                    Err(FormatError::InvalidDataFileKey { .. })
                ),
                "expected rejection for {key:?}"
            );
        }
    }

    #[test]
    fn validate_data_files_reports_first_bad_key() {
        let mut manifest = sample_manifest();
        manifest.data_files[0].key = "../escape.json.gz".to_owned();
        assert!(matches!(
            manifest.validate_data_files(),
            Err(FormatError::InvalidDataFileKey { .. })
        ));
    }

    #[test]
    fn entry_decode_rejects_traversal_key_before_reading_bytes() {
        let entry = DataFileEntry {
            key: "../escape.json.gz".to_owned(),
            item_count: 0,
            sha256: "0".repeat(64),
            md5: String::new(),
            bytes: 0,
            uncompressed_bytes: 0,
            key_range: None,
        };
        // Bytes are irrelevant: the key check fires first.
        let err = entry.decode(b"not even gzip").unwrap_err();
        assert!(matches!(err, FormatError::InvalidDataFileKey { .. }));
    }
}
