// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `extenddb-manifest.json` types: the `ExtendDB` extension to the
//! `DynamoDB` export layout, and the commit record of a backup.
//!
//! The manifest is written last; its presence marks a complete backup. It
//! carries everything a restore needs that the `DynamoDB` export manifests do
//! not: the full source table definition, the snapshot descriptor, and a
//! sha256 per data file.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::FormatError;
use super::data_file::hex_encode;
use super::layout::DATA_PREFIX;
use crate::types::{
    AttributeDefinition, BillingMode, GsiInput, KeySchemaElement, LsiInput, OnDemandThroughput,
    ProvisionedThroughput, SseDescription, VectorIndexDescription,
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
    /// Provisioned throughput, present when the table is PROVISIONED.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    /// On-demand throughput limits, when configured.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub on_demand_throughput: Option<OnDemandThroughput>,
    /// Table class, when the source reported one.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub table_class: Option<String>,
    /// Server-side encryption settings, when the source reported them.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sse_specification: Option<SseDescription>,
    /// Global secondary indexes defined on the source table.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub global_secondary_indexes: Option<Vec<GsiInput>>,
    /// Local secondary indexes defined on the source table.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub local_secondary_indexes: Option<Vec<LsiInput>>,
    /// Vector indexes defined on the source table. Present when the source
    /// backend supports vector search; a restore target without vector
    /// support refuses the restore rather than dropping them.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub vector_indexes: Option<Vec<VectorIndexDescription>>,
    /// Item count at backup time.
    pub item_count: i64,
    /// Table size in bytes at backup time.
    pub table_size_bytes: i64,
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
    /// Size of the file's compressed bytes.
    pub bytes: u64,
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

    /// Decode this entry's data file, validating the key again first.
    ///
    /// The key layout rule is enforced here as well as at parse time, so a
    /// caller that decodes an entry always crosses the same check regardless
    /// of whether it ran [`ExtenddbManifest::validate_data_files`]. The
    /// entry's recorded `sha256` and `item_count` are enforced against the
    /// bytes.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] when the key is not a
    /// single data-file entry under the prefix, and any error
    /// [`crate::backup_format::decode_data_file`] can produce.
    pub fn decode(&self, compressed: &[u8]) -> Result<Vec<crate::types::Item>, FormatError> {
        self.validate_key()?;
        let expectations = super::data_file::DataFileExpectations {
            sha256_hex: Some(self.sha256.clone()),
            item_count: Some(self.item_count),
            ..Default::default()
        };
        super::data_file::decode_data_file(compressed, expectations)
    }
}

/// The `extenddb-manifest.json` document.
///
/// Serde leaves unknown fields tolerated (no `deny_unknown_fields`), so a
/// newer writer can add members without breaking an older reader; the hard
/// gate is [`Self::check_supported`], which refuses a `format_version` this
/// build does not understand.
///
/// # Adding a field requires a format version bump
///
/// [`Self::verify_manifest_sha256`] recomputes the digest from the
/// re-serialized struct, and deserialization drops any field this build does
/// not model. So a future writer that adds a new checksummed field emits a
/// document whose `manifest_sha256` covers that field, but an older reader
/// re-serializes without it and computes a different digest, failing
/// verification. Any additive manifest field must therefore raise
/// [`CURRENT_FORMAT_VERSION`], so an older build refuses the manifest at the
/// version gate rather than reporting a spurious checksum mismatch.
///
/// # The canonical form behind `manifest_sha256`
///
/// The digest is taken over `serde_json`'s compact serialization of this
/// struct with `manifest_sha256` blanked. That serialization includes
/// `serde_json`'s shortest-round-trip `f64` formatting for
/// `backup_creation_date_time` and [`SourceTableSchema::table_creation_date_time`].
/// Rust-to-Rust verification is stable because both [`Self::seal`] and
/// [`Self::verify_manifest_sha256`] re-serialize through the same formatter,
/// so the digest never depends on the original document bytes. The constraint
/// binds any non-`serde_json` verifier (for example a Python or other-language
/// tool): it must reproduce that float formatting byte for byte, or compute a
/// different digest for the identical logical manifest. The timestamp stays an
/// `f64` here deliberately, the representation [`crate::types::BackupDetails`]
/// uses on the wire, so the manifest reuses it without a lossy conversion;
/// storing a canonical string instead would fork that representation across
/// the type layer and the restore path, so the float-formatting constraint is
/// documented rather than removed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtenddbManifest {
    /// Format version; see [`CURRENT_FORMAT_VERSION`].
    pub format_version: u32,
    /// ARN of the backup, in the shape
    /// `arn:aws:dynamodb:<region>:<account>:table/<table>/backup/<id>`.
    pub backup_arn: String,
    /// Name given to the backup at creation.
    pub backup_name: String,
    /// When the backup was created, as epoch seconds, the representation
    /// [`crate::types::BackupDetails`] uses.
    pub backup_creation_date_time: f64,
    /// Total size of the backup's data files, in compressed bytes.
    pub backup_size_bytes: i64,
    /// The storage backend that produced the backup.
    pub source_backend: String,
    /// The `ExtendDB` version that produced the backup.
    pub source_extenddb_version: String,
    /// How the consistent snapshot was taken.
    pub snapshot: SnapshotDescriptor,
    /// The source table definition.
    pub source_table: SourceTableSchema,
    /// The data files that make up the backup.
    pub data_files: Vec<DataFileEntry>,
    /// Lowercase hex sha256 of the manifest's canonical JSON, computed with
    /// this field set to the empty string. See
    /// [`Self::compute_manifest_sha256`].
    pub manifest_sha256: String,
}

impl ExtenddbManifest {
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

    /// Validate every data-file key against the layout rule.
    ///
    /// Callers run this when a manifest is parsed, and the decode path runs
    /// the same per-entry [`DataFileEntry::validate_key`] again before it
    /// resolves a key, so a key that slips past one check is caught at the
    /// other.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidDataFileKey`] for the first offending
    /// key.
    pub fn validate_data_files(&self) -> Result<(), FormatError> {
        for entry in &self.data_files {
            entry.validate_key()?;
        }
        Ok(())
    }

    /// Compute the manifest checksum.
    ///
    /// The canonical form is the compact `serde_json` serialization of the
    /// manifest, in struct declaration order, with `manifest_sha256` set to
    /// the empty string. The checksum is the lowercase hex sha256 of those
    /// bytes.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when serialization fails.
    pub fn compute_manifest_sha256(&self) -> Result<String, FormatError> {
        let mut unsealed = self.clone();
        unsealed.manifest_sha256 = String::new();
        let canonical = serde_json::to_string(&unsealed)?;
        Ok(hex_encode(&Sha256::digest(canonical.as_bytes())))
    }

    /// Compute the checksum and store it in `manifest_sha256`.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when serialization fails.
    pub fn seal(&mut self) -> Result<(), FormatError> {
        self.manifest_sha256 = self.compute_manifest_sha256()?;
        Ok(())
    }

    /// Verify that `manifest_sha256` matches the manifest's contents.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::ChecksumMismatch`] when the stored checksum does
    /// not match the recomputed one, and [`FormatError::Json`] when
    /// serialization fails.
    pub fn verify_manifest_sha256(&self) -> Result<(), FormatError> {
        let computed = self.compute_manifest_sha256()?;
        if computed == self.manifest_sha256 {
            Ok(())
        } else {
            Err(FormatError::ChecksumMismatch {
                subject: "manifest",
                expected: self.manifest_sha256.clone(),
                computed,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyType, Projection, ProjectionType, ScalarAttributeType};

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
                sse_specification: Some(SseDescription {
                    status: "ENABLED".to_owned(),
                    sse_type: Some(crate::types::SseType::KMS),
                    kms_master_key_arn: Some(
                        "arn:aws:kms:us-east-1:123456789012:key/abcd".to_owned(),
                    ),
                }),
                global_secondary_indexes: Some(vec![GsiInput {
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
                }]),
                local_secondary_indexes: Some(vec![LsiInput {
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
                }]),
                vector_indexes: Some(vec![VectorIndexDescription {
                    index_name: "embeddings".to_owned(),
                    vector_attribute: crate::types::VectorAttribute {
                        attribute_name: "vec".to_owned(),
                    },
                    dimensions: 1024,
                    search_schema: None,
                    distance_function: crate::types::DistanceFunction::Cosine,
                    index_status: crate::types::IndexStatus::Active,
                    backfilling: None,
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn:
                        "arn:aws:dynamodb:us-east-1:123456789012:table/Music/index/embeddings"
                            .to_owned(),
                    projection: Some(Projection {
                        projection_type: ProjectionType::KeysOnly,
                        non_key_attributes: None,
                    }),
                }]),
                item_count: 100_000,
                table_size_bytes: 123_456,
            },
            data_files: vec![DataFileEntry {
                key: "data/000001.json.gz".to_owned(),
                item_count: 50_000,
                sha256: "0".repeat(64),
                bytes: 61_728,
            }],
            manifest_sha256: String::new(),
        }
    }

    #[test]
    fn manifest_serde_round_trip() {
        let mut manifest = sample_manifest();
        manifest.seal().unwrap();
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: ExtenddbManifest = serde_json::from_str(&json).unwrap();
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
            bytes: 1234,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: DataFileEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, entry);
    }

    #[test]
    fn absent_optional_members_stay_absent() {
        let mut manifest = sample_manifest();
        manifest.source_table.on_demand_throughput = None;
        manifest.source_table.vector_indexes = None;
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(!json.contains("on_demand_throughput"));
        assert!(!json.contains("vector_indexes"));
    }

    #[test]
    fn unknown_fields_are_tolerated() {
        let mut manifest = sample_manifest();
        manifest.seal().unwrap();
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
    fn seal_then_verify_round_trip() {
        let mut manifest = sample_manifest();
        manifest.seal().unwrap();
        assert_eq!(manifest.manifest_sha256.len(), 64);
        manifest.verify_manifest_sha256().unwrap();
    }

    #[test]
    fn compute_is_independent_of_stored_checksum() {
        let mut manifest = sample_manifest();
        let before = manifest.compute_manifest_sha256().unwrap();
        manifest.seal().unwrap();
        let after = manifest.compute_manifest_sha256().unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn verify_detects_tampered_field() {
        let mut manifest = sample_manifest();
        manifest.seal().unwrap();
        manifest.backup_size_bytes += 1;
        let err = manifest.verify_manifest_sha256().unwrap_err();
        assert!(matches!(
            err,
            FormatError::ChecksumMismatch {
                subject: "manifest",
                ..
            }
        ));
    }

    #[test]
    fn verify_detects_tampered_data_file_entry() {
        let mut manifest = sample_manifest();
        manifest.seal().unwrap();
        manifest.data_files[0].sha256 = "f".repeat(64);
        assert!(manifest.verify_manifest_sha256().is_err());
    }

    #[test]
    fn verify_rejects_empty_checksum_on_populated_manifest() {
        let manifest = sample_manifest();
        assert!(manifest.verify_manifest_sha256().is_err());
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
                bytes: 1,
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
                bytes: 1,
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
            bytes: 0,
        };
        // Bytes are irrelevant: the key check fires first.
        let err = entry.decode(b"not even gzip").unwrap_err();
        assert!(matches!(err, FormatError::InvalidDataFileKey { .. }));
    }
}
