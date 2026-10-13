// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Golden-file tests against the checked-in `DynamoDB` export under
//! `testdata/ddb-export/` and the `ExtendDB` manifest under
//! `testdata/extenddb-manifest/`.
//!
//! The export fixture is constructed, not captured from the live service: it
//! follows the documented S3 export output format (member names, the
//! `2020-06-30` summary version, timestamps, the explicit `null` for
//! `s3SseKmsKeyId`) and was generated with this crate's own encoder, so what
//! it proves is that the reader accepts a document of that shape and that the
//! writer's output does not drift, not that it matches bytes the service
//! wrote. The README in that directory records the details.

use std::path::PathBuf;

use super::{
    DataFileExpectations, ExportSummaryManifest, decode_data_file, parse_files_manifest,
    write_files_manifest,
};
use crate::types::AttributeValue;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/ddb-export")
}

fn read_fixture(name: &str) -> Vec<u8> {
    let path = fixture_dir().join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

#[test]
fn golden_summary_manifest_parses() {
    let raw = String::from_utf8(read_fixture("manifest-summary.json")).unwrap();
    let summary = ExportSummaryManifest::from_json(&raw).unwrap();
    assert_eq!(summary.version, "2020-06-30");
    assert_eq!(
        summary.export_arn,
        "arn:aws:dynamodb:us-east-1:123456789012:table/ProductCatalog/export/01693685827463-2d8752fd"
    );
    assert_eq!(summary.table_id, "12345a12-abcd-123a-ab12-1234abc12345");
    assert_eq!(summary.s3_sse_algorithm, "AES256");
    assert_eq!(summary.s3_sse_kms_key_id, None);
    assert_eq!(summary.item_count, 3);
    assert_eq!(summary.output_format, "DYNAMODB_JSON");
    assert_eq!(summary.export_type, None);

    // The serializer reproduces the document byte for byte.
    assert_eq!(summary.to_json().unwrap(), raw);
}

#[test]
fn golden_files_manifest_parses_and_reserializes_byte_for_byte() {
    let raw = String::from_utf8(read_fixture("manifest-files.json")).unwrap();
    let entries = parse_files_manifest(&raw).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].item_count, 3);
    assert_eq!(entries[0].md5_checksum, "1jUj2wzOC+iuxJN+LKy6Aw==");
    assert_eq!(
        entries[0].data_file_s3_key,
        "AWSDynamoDB/01693685827463-2d8752fd/data/ka2sswm5ha4uejfqcmnjcbg6ru.json.gz"
    );

    assert_eq!(write_files_manifest(&entries).unwrap(), raw);
}

#[test]
fn golden_data_file_decodes_with_manifest_checksum() {
    let files_raw = String::from_utf8(read_fixture("manifest-files.json")).unwrap();
    let entry = &parse_files_manifest(&files_raw).unwrap()[0];

    let compressed = read_fixture("data/ka2sswm5ha4uejfqcmnjcbg6ru.json.gz");
    let expectations = DataFileExpectations {
        md5_base64: Some(entry.md5_checksum.clone()),
        item_count: Some(entry.item_count),
        ..Default::default()
    };
    let items = decode_data_file(&compressed, expectations).unwrap();
    assert_eq!(items.len(), 3);

    assert_eq!(
        items[0].get("Id"),
        Some(&AttributeValue::N("103".to_owned()))
    );
    assert_eq!(
        items[0].get("Title"),
        Some(&AttributeValue::S("Book 103 Title".to_owned()))
    );
    assert_eq!(
        items[0].get("Authors"),
        Some(&AttributeValue::SS(
            ["Author1", "Author2"]
                .iter()
                .map(|s| (*s).to_owned())
                .collect()
        ))
    );
    assert_eq!(
        items[0].get("InPublication"),
        Some(&AttributeValue::Bool(false))
    );
    assert_eq!(
        items[1].get("Binary"),
        Some(&AttributeValue::B(vec![0xde, 0xad, 0xbe, 0xef]))
    );
    match items[2].get("Meta") {
        Some(AttributeValue::M(map)) => {
            assert_eq!(map.get("a"), Some(&AttributeValue::Null));
            assert_eq!(
                map.get("list"),
                Some(&AttributeValue::L(vec![
                    AttributeValue::N("1".to_owned()),
                    AttributeValue::S("x".to_owned()),
                ]))
            );
        }
        other => panic!("unexpected Meta value: {other:?}"),
    }
}

#[test]
fn golden_data_file_reencodes_to_identical_lines() {
    // Decoding the fixture and re-encoding its items must reproduce the same
    // uncompressed lines: the fixture's line encoding and the encoder's agree
    // byte for byte. (The gzip container itself is not compared, since
    // compressor output is implementation-defined.)
    use std::io::Read;

    let compressed = read_fixture("data/ka2sswm5ha4uejfqcmnjcbg6ru.json.gz");
    let items = decode_data_file(&compressed, DataFileExpectations::default()).unwrap();

    let (reencoded, checksums) = super::encode_data_file(items).unwrap();
    assert_eq!(checksums.item_count, 3);

    let mut fixture_lines = String::new();
    flate2::read::GzDecoder::new(compressed.as_slice())
        .read_to_string(&mut fixture_lines)
        .unwrap();
    let mut reencoded_lines = String::new();
    flate2::read::GzDecoder::new(reencoded.as_slice())
        .read_to_string(&mut reencoded_lines)
        .unwrap();
    assert_eq!(reencoded_lines, fixture_lines);
    assert_eq!(checksums.uncompressed_bytes, fixture_lines.len() as u64);
}

/// The frozen `extenddb-manifest.json` for format version 1. The nested
/// members reuse the `DynamoDB` wire types, so a change to one of those types
/// (a rename, a `skip_serializing_if` toggle, a new required member) changes
/// the on-disk format without a version bump and without any other test
/// noticing; this fixture makes such a change fail here. Regenerate it only
/// with a format version bump.
mod extenddb_manifest_golden {
    use super::super::{ExtenddbManifest, manifest_checksum, verify_manifest_checksum};

    const MANIFEST: &[u8] = include_bytes!("../../testdata/extenddb-manifest/v1.json");
    const SIDECAR: &str = include_str!("../../testdata/extenddb-manifest/v1.sha256");

    #[test]
    fn golden_manifest_parses_and_reserializes_byte_for_byte() {
        let manifest = ExtenddbManifest::from_json_bytes(MANIFEST).expect("golden parses");
        let again = manifest.to_json_bytes().unwrap();
        assert_eq!(
            std::str::from_utf8(&again).unwrap(),
            std::str::from_utf8(MANIFEST).unwrap(),
            "the on-disk shape of format version 1 changed; bump the version"
        );
    }

    #[test]
    fn golden_sidecar_verifies() {
        verify_manifest_checksum(MANIFEST, SIDECAR).unwrap();
        assert_eq!(manifest_checksum(MANIFEST), SIDECAR);
    }

    #[test]
    fn golden_manifest_covers_every_optional_member() {
        // The fixture sets every top-level optional member of the source
        // table so a serialization change to any of them is caught. Nested
        // optionals inside the index and vector shapes (an LSI's
        // `NonKeyAttributes`, the vector index's `SearchSchema` and
        // `Projection`) are absent in the fixture; the byte-for-byte test
        // still catches a `skip_serializing_if` change on them but not a
        // rename.
        let manifest = ExtenddbManifest::from_json_bytes(MANIFEST).unwrap();
        let t = &manifest.source_table;
        assert!(t.provisioned_throughput.is_some());
        assert!(t.on_demand_throughput.is_some());
        assert!(t.table_class.is_some());
        assert!(t.sse_description.is_some());
        assert!(!t.global_secondary_indexes.is_empty());
        assert!(!t.local_secondary_indexes.is_empty());
        assert!(t.vector_indexes.as_ref().is_some_and(|v| !v.is_empty()));
        assert!(manifest.data_files.iter().all(|f| f.key_range.is_some()));
    }
}

#[cfg(test)]
mod generate_golden_manifest {
    #![allow(clippy::too_many_lines)]
    //! Run with `cargo test -p extenddb-core write_golden_manifest --
    //! --ignored` to (re)generate the fixture. Only when the format version
    //! is bumped.
    use super::super::{
        DataFileEntry, ExtenddbManifest, ItemOrder, KeyRange, SnapshotDescriptor,
        SourceTableSchema, manifest_checksum,
    };
    use crate::types::{
        AttributeDefinition, AttributeValue, BillingMode, DistanceFunction, GsiInput, Item,
        KeySchemaElement, KeyType, LsiInput, OnDemandThroughput, Projection, ProjectionType,
        ProvisionedThroughput, ScalarAttributeType, SseDescription, SseType, VectorAttribute,
        VectorIndexSpecification,
    };

    fn key(a: &str, t: &str) -> Item {
        [
            ("Artist".to_owned(), AttributeValue::S(a.to_owned())),
            ("SongTitle".to_owned(), AttributeValue::S(t.to_owned())),
        ]
        .into_iter()
        .collect()
    }

    fn golden() -> ExtenddbManifest {
        ExtenddbManifest {
            format_version: 1,
            backup_arn:
                "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01757500000123-0badcafe"
                    .to_owned(),
            backup_name: "music-nightly".to_owned(),
            backup_creation_date_time: 1_757_500_000.123,
            backup_size_bytes: 61_728 + 100,
            source_backend: "postgres".to_owned(),
            source_extenddb_version: "0.1.13".to_owned(),
            snapshot: SnapshotDescriptor {
                kind: "postgres_repeatable_read".to_owned(),
                marker: "1234:1234:".to_owned(),
            },
            item_order: ItemOrder::KeyOrdered,
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
                on_demand_throughput: Some(OnDemandThroughput {
                    max_read_request_units: Some(100),
                    max_write_request_units: Some(50),
                }),
                table_class: Some("STANDARD".to_owned()),
                sse_description: Some(SseDescription {
                    status: "ENABLED".to_owned(),
                    sse_type: Some(SseType::KMS),
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
                        projection_type: ProjectionType::Include,
                        non_key_attributes: Some(vec!["Year".to_owned()]),
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
                    distance_function: DistanceFunction::Cosine,
                    vector_attribute: VectorAttribute {
                        attribute_name: "vec".to_owned(),
                    },
                    search_schema: None,
                    projection: None,
                }]),
                item_count: 100_000,
                table_size_bytes: 123_456,
            },
            data_files: vec![
                DataFileEntry {
                    key: "data/000001.json.gz".to_owned(),
                    item_count: 50_000,
                    sha256: "0".repeat(64),
                    md5: "1B2M2Y8AsgTpgAmY7PhCfg==".to_owned(),
                    bytes: 61_728,
                    uncompressed_bytes: 300_000,
                    key_range: Some(KeyRange {
                        first: key("A", "a"),
                        last: key("M", "z"),
                    }),
                },
                DataFileEntry {
                    key: "data/000002.json.gz".to_owned(),
                    item_count: 50_000,
                    sha256: "1".repeat(64),
                    md5: "1B2M2Y8AsgTpgAmY7PhCfg==".to_owned(),
                    bytes: 100,
                    uncompressed_bytes: 300_000,
                    key_range: Some(KeyRange {
                        first: key("N", "a"),
                        last: key("Z", "z"),
                    }),
                },
            ],
        }
    }

    /// Regenerates the fixture from `golden()`. Ignored by default so an
    /// ordinary `cargo test` never rewrites checked-in files; run it with
    /// `cargo test -p extenddb-core write_golden_manifest -- --ignored` after
    /// an intentional format change, then review the diff.
    #[test]
    #[ignore = "rewrites the checked-in fixture; run explicitly"]
    fn write_golden_manifest() {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/extenddb-manifest");
        let bytes = golden().to_json_bytes().unwrap();
        std::fs::write(dir.join("v1.json"), &bytes).unwrap();
        std::fs::write(dir.join("v1.sha256"), manifest_checksum(&bytes)).unwrap();
    }
}
