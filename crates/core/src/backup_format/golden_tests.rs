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

use super::ddb_export::write_files_manifest;
use super::{DataFileExpectations, ExportSummaryManifest, decode_data_file, parse_files_manifest};
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
    assert_eq!(summary.s3_sse_algorithm.as_deref(), Some("AES256"));
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

/// The frozen `extenddb-manifest.json` for format version 1, three times:
/// `v1.json` with every optional member set and every list non-empty,
/// `v1-minimal.json` with every optional member absent (written as `null`)
/// and every list empty, and `v1-unordered.json` with the shapes the other
/// two lack (an unordered backup's `"key_range":null` files, and the absent
/// state of every optional nested member `v1.json` sets: `OnDemandThroughput`
/// with both limits `null`, an `SseDescription` without `SSEType` and
/// `KMSMasterKeyArn`, a GSI without `ProvisionedThroughput`, a
/// `StreamSpecification` without `StreamViewType`, a `time_to_live` without
/// `AttributeName`; and a vector index's `SearchSchema` and `Projection`, an
/// LSI's `INCLUDE` projection with `NonKeyAttributes`, which `v1.json`
/// lacks). The nested members reuse the `DynamoDB`
/// wire types, so a change to one of those types (a rename, a
/// `skip_serializing_if` toggle, a new required member) changes the on-disk
/// format without any other test noticing; these fixtures make such a
/// change fail here, in the present and in the absent state. A new optional
/// member that is omitted when absent leaves every fixture's bytes as they
/// were, and so does a new or renamed enum value no fixture uses;
/// `nested_types_have_the_pinned_members` and `enum_values_are_pinned` fail
/// those instead. They are
/// permanent read tests: every later build parses them, and they are never
/// regenerated once v1 is released (see the testdata README and
/// `ExtenddbManifest`'s evolution rule).
mod extenddb_manifest_golden {
    use super::super::manifest::manifest_checksum;
    use super::super::{ExtenddbManifest, ItemOrder, verify_manifest_checksum};
    use crate::types::{ProjectionType, SearchSchemaElementType, TimeToLiveStatus};

    const MANIFEST: &[u8] = include_bytes!("../../testdata/extenddb-manifest/v1.json");
    const SIDECAR: &str = include_str!("../../testdata/extenddb-manifest/v1.sha256");
    const MINIMAL: &[u8] = include_bytes!("../../testdata/extenddb-manifest/v1-minimal.json");
    const MINIMAL_SIDECAR: &str =
        include_str!("../../testdata/extenddb-manifest/v1-minimal.sha256");
    const UNORDERED: &[u8] = include_bytes!("../../testdata/extenddb-manifest/v1-unordered.json");
    const UNORDERED_SIDECAR: &str =
        include_str!("../../testdata/extenddb-manifest/v1-unordered.sha256");

    #[test]
    fn golden_unordered_manifest_parses_and_reserializes_byte_for_byte() {
        let manifest =
            ExtenddbManifest::from_json_bytes(UNORDERED).expect("unordered golden parses");
        let again = manifest.to_json_bytes().unwrap();
        assert_eq!(
            std::str::from_utf8(&again).unwrap(),
            std::str::from_utf8(UNORDERED).unwrap(),
            "the on-disk shape of format version 1 changed; bump the version"
        );
        verify_manifest_checksum(UNORDERED, UNORDERED_SIDECAR.as_bytes()).unwrap();
        assert_eq!(manifest_checksum(UNORDERED), UNORDERED_SIDECAR);
    }

    #[test]
    fn golden_unordered_manifest_has_the_shapes_the_others_lack() {
        let manifest = ExtenddbManifest::from_json_bytes(UNORDERED).unwrap();
        assert_eq!(manifest.item_order, ItemOrder::Unordered);
        assert!(manifest.data_files.len() > 1);
        assert!(manifest.data_files.iter().all(|f| f.key_range.is_none()));
        let t = &manifest.source_table;
        let on_demand = t.on_demand_throughput.as_ref().unwrap();
        assert!(on_demand.max_read_request_units.is_none());
        assert!(on_demand.max_write_request_units.is_none());
        let sse = t.sse_description.as_ref().unwrap();
        assert!(sse.sse_type.is_none());
        assert!(sse.kms_master_key_arn.is_none());
        let gsi = &t.global_secondary_indexes[0];
        assert!(gsi.provisioned_throughput.is_none());
        let vector = &t.vector_indexes.as_ref().unwrap()[0];
        let schema = vector.search_schema.as_ref().unwrap();
        assert!(
            schema
                .iter()
                .any(|e| e.element_type == SearchSchemaElementType::Hash)
        );
        assert!(
            schema
                .iter()
                .any(|e| e.element_type == SearchSchemaElementType::InlineFilter)
        );
        assert!(
            vector
                .projection
                .as_ref()
                .unwrap()
                .non_key_attributes
                .is_some()
        );
        let lsi = &t.local_secondary_indexes[0];
        assert_eq!(lsi.projection.projection_type, ProjectionType::Include);
        assert!(lsi.projection.non_key_attributes.is_some());
        let stream = t.stream_specification.as_ref().unwrap();
        assert!(!stream.stream_enabled);
        assert!(stream.stream_view_type.is_none());
        let ttl = t.time_to_live.as_ref().unwrap();
        assert_eq!(ttl.time_to_live_status, TimeToLiveStatus::Disabled);
        assert!(ttl.attribute_name.is_none());
        // The members are in the bytes as written: the nested wire types'
        // absent members as `null` (`OnDemandThroughput`) or omitted (the
        // others).
        let text = std::str::from_utf8(UNORDERED).unwrap();
        for member in [
            "\"item_order\":\"unordered\"",
            "\"key_range\":null",
            "\"on_demand_throughput\":{\"MaxReadRequestUnits\":null,\"MaxWriteRequestUnits\":null}",
            "\"sse_description\":{\"Status\":\"DISABLED\"}",
            "\"Projection\":{\"ProjectionType\":\"KEYS_ONLY\"}}]",
            "\"SearchSchema\":[",
            "\"SearchSchemaElementType\":\"HASH\"",
            "\"SearchSchemaElementType\":\"INLINE_FILTER\"",
            "\"NonKeyAttributes\":[",
            "\"ProjectionType\":\"INCLUDE\"",
            "\"provisioned_throughput\":null",
            "\"stream_specification\":{\"StreamEnabled\":false}",
            // The wire type omits an absent `AttributeName`.
            "\"time_to_live\":{\"TimeToLiveStatus\":\"DISABLED\"}",
        ] {
            assert!(text.contains(member), "{member}");
        }
    }

    #[test]
    fn golden_minimal_manifest_parses_and_reserializes_byte_for_byte() {
        let manifest = ExtenddbManifest::from_json_bytes(MINIMAL).expect("minimal golden parses");
        let again = manifest.to_json_bytes().unwrap();
        assert_eq!(
            std::str::from_utf8(&again).unwrap(),
            std::str::from_utf8(MINIMAL).unwrap(),
            "the on-disk shape of format version 1 with absent members changed; bump the version"
        );
        verify_manifest_checksum(MINIMAL, MINIMAL_SIDECAR.as_bytes()).unwrap();
        assert_eq!(manifest_checksum(MINIMAL), MINIMAL_SIDECAR);
    }

    #[test]
    fn golden_minimal_manifest_has_every_optional_member_absent() {
        let manifest = ExtenddbManifest::from_json_bytes(MINIMAL).unwrap();
        let t = &manifest.source_table;
        assert!(t.provisioned_throughput.is_none());
        assert!(t.on_demand_throughput.is_none());
        assert!(t.table_class.is_none());
        assert!(t.sse_description.is_none());
        assert!(t.vector_indexes.is_none());
        assert!(t.stream_specification.is_none());
        assert!(t.time_to_live.is_none());
        assert!(t.global_secondary_indexes.is_empty());
        assert!(t.local_secondary_indexes.is_empty());
        assert!(manifest.data_files.is_empty());
        // The members are in the bytes as `null` / `[]`, not omitted.
        let text = std::str::from_utf8(MINIMAL).unwrap();
        for member in [
            "\"provisioned_throughput\":null",
            "\"on_demand_throughput\":null",
            "\"table_class\":null",
            "\"sse_description\":null",
            "\"vector_indexes\":null",
            "\"stream_specification\":null",
            "\"time_to_live\":null",
            "\"global_secondary_indexes\":[]",
            "\"local_secondary_indexes\":[]",
            "\"data_files\":[]",
        ] {
            assert!(text.contains(member), "{member}");
        }
    }

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
        verify_manifest_checksum(MANIFEST, SIDECAR.as_bytes()).unwrap();
        assert_eq!(manifest_checksum(MANIFEST), SIDECAR);
    }

    #[test]
    fn golden_manifest_covers_every_optional_member() {
        // The fixture sets every top-level optional member of the source
        // table so a serialization change to any of them is caught. Nested
        // optionals inside the index and vector shapes (an LSI's
        // `NonKeyAttributes`, the vector index's `SearchSchema` and
        // `Projection`) are absent here and present in `v1-unordered.json`,
        // so a rename of any of them fails that fixture; the nested
        // optionals set here (`MaxReadRequestUnits`, `MaxWriteRequestUnits`,
        // `SSEType`, `KMSMasterKeyArn`, a GSI's `ProvisionedThroughput`,
        // `StreamViewType`, `AttributeName`) are absent there.
        let manifest = ExtenddbManifest::from_json_bytes(MANIFEST).unwrap();
        let t = &manifest.source_table;
        assert!(t.provisioned_throughput.is_some());
        assert!(t.on_demand_throughput.is_some());
        assert!(t.table_class.is_some());
        assert!(t.sse_description.is_some());
        assert!(!t.global_secondary_indexes.is_empty());
        assert!(!t.local_secondary_indexes.is_empty());
        assert!(t.vector_indexes.as_ref().is_some_and(|v| !v.is_empty()));
        let stream = t.stream_specification.as_ref().unwrap();
        assert!(stream.stream_enabled);
        assert!(stream.stream_view_type.is_some());
        let ttl = t.time_to_live.as_ref().unwrap();
        assert_eq!(ttl.time_to_live_status, TimeToLiveStatus::Enabled);
        assert!(ttl.attribute_name.is_some());
        assert!(manifest.data_files.iter().all(|f| f.key_range.is_some()));
        let text = std::str::from_utf8(MANIFEST).unwrap();
        for member in [
            "\"stream_specification\":{\"StreamEnabled\":true,\"StreamViewType\":\"NEW_AND_OLD_IMAGES\"}",
            "\"time_to_live\":{\"TimeToLiveStatus\":\"ENABLED\",\"AttributeName\":\"expires_at\"}",
        ] {
            assert!(text.contains(member), "{member}");
        }
    }

    /// Unknown members are tolerated at every level of the document, not
    /// only at the top: a later writer can add a member to the top level,
    /// to `snapshot`, to `source_table`, to any nested wire type (a key
    /// schema element, an attribute definition, a throughput, an index, a
    /// projection, a vector index and its search schema, the stream and TTL
    /// descriptions), to a `data_files[]` entry and to a `key_range`, and
    /// this reader parses the document to the same value. Every JSON object
    /// in the two fixtures that cover all of those shapes gets one injected
    /// member in turn, except the key items under `key_range.first` and
    /// `.last`, where a new name is a new key attribute (refused as a wrong
    /// key shape, correctly) and a new descriptor is a second type (refused
    /// by the `AttributeValue` deserializer, correctly).
    #[test]
    fn unknown_members_are_tolerated_at_every_level() {
        fn objects(value: &serde_json::Value, path: &str, out: &mut Vec<String>) {
            match value {
                serde_json::Value::Object(map) => {
                    out.push(path.to_owned());
                    for (k, v) in map {
                        objects(v, &format!("{path}/{k}"), out);
                    }
                }
                serde_json::Value::Array(items) => {
                    for (i, v) in items.iter().enumerate() {
                        objects(v, &format!("{path}/{i}"), out);
                    }
                }
                _ => {}
            }
        }
        fn inject(value: &mut serde_json::Value, path: &str) {
            let mut cursor = value;
            for segment in path.split('/').filter(|s| !s.is_empty()) {
                cursor = match cursor {
                    serde_json::Value::Object(map) => map.get_mut(segment).unwrap(),
                    serde_json::Value::Array(items) => {
                        &mut items[segment.parse::<usize>().unwrap()]
                    }
                    other => panic!("{path}: {other}"),
                };
            }
            cursor
                .as_object_mut()
                .unwrap()
                .insert("added_in_a_later_version".to_owned(), serde_json::json!(42));
        }
        let mut injected = 0;
        for fixture in [MANIFEST, UNORDERED] {
            let original = ExtenddbManifest::from_json_bytes(fixture).unwrap();
            let doc: serde_json::Value = serde_json::from_slice(fixture).unwrap();
            let mut paths = Vec::new();
            objects(&doc, "", &mut paths);
            for path in paths {
                if path.contains("/key_range/first") || path.contains("/key_range/last") {
                    continue;
                }
                let mut doc = doc.clone();
                inject(&mut doc, &path);
                let parsed = ExtenddbManifest::from_json_bytes(&serde_json::to_vec(&doc).unwrap())
                    .unwrap_or_else(|e| panic!("unknown member at {path:?}: {e}"));
                assert_eq!(parsed, original, "unknown member at {path:?}");
                injected += 1;
            }
        }
        // The levels the contract names are among those visited.
        let covered = |needle: &str| {
            let doc: serde_json::Value = serde_json::from_slice(MANIFEST).unwrap();
            let mut paths = Vec::new();
            objects(&doc, "", &mut paths);
            let doc: serde_json::Value = serde_json::from_slice(UNORDERED).unwrap();
            objects(&doc, "", &mut paths);
            paths.iter().any(|p| p.ends_with(needle))
        };
        for level in [
            "",
            "/snapshot",
            "/source_table",
            "/source_table/key_schema/0",
            "/source_table/attribute_definitions/0",
            "/source_table/provisioned_throughput",
            "/source_table/on_demand_throughput",
            "/source_table/sse_description",
            "/source_table/global_secondary_indexes/0",
            "/source_table/global_secondary_indexes/0/Projection",
            "/source_table/global_secondary_indexes/0/ProvisionedThroughput",
            "/source_table/local_secondary_indexes/0",
            "/source_table/local_secondary_indexes/0/Projection",
            "/source_table/vector_indexes/0",
            "/source_table/vector_indexes/0/VectorAttribute",
            "/source_table/vector_indexes/0/SearchSchema/0",
            "/source_table/vector_indexes/0/Projection",
            "/source_table/stream_specification",
            "/source_table/time_to_live",
            "/data_files/0",
            "/data_files/0/key_range",
        ] {
            assert!(covered(level), "no object at {level:?} in the fixtures");
        }
        assert!(injected >= 30, "{injected}");
    }
}

#[cfg(test)]
mod generate_golden_manifest {
    #![allow(clippy::too_many_lines)]
    //! Run with `EXTENDDB_WRITE_GOLDEN=1 cargo test -p extenddb-core
    //! write_golden_manifest -- --ignored` to write the fixtures of the
    //! current format version, `v{CURRENT_FORMAT_VERSION}*.json`. Once a
    //! version is released its fixtures are never rewritten; a version bump
    //! writes new ones beside them.
    use super::super::manifest::manifest_checksum;
    use super::super::{
        CURRENT_FORMAT_VERSION, DataFileEntry, ExtenddbManifest, ItemOrder, KeyRange,
        SnapshotDescriptor, SourceTableSchema,
    };
    use crate::types::{
        AttributeDefinition, AttributeValue, BillingMode, DistanceFunction, GsiInput, Item,
        KeySchemaElement, KeyType, LsiInput, OnDemandThroughput, Projection, ProjectionType,
        ProvisionedThroughput, ScalarAttributeType, SearchSchemaElement, SearchSchemaElementType,
        SseDescription, SseType, StreamSpecification, StreamViewType, TimeToLiveDescription,
        TimeToLiveStatus, VectorAttribute, VectorIndexSpecification,
    };

    fn key(a: &str, t: &str) -> Item {
        [
            ("Artist".to_owned(), AttributeValue::S(a.to_owned())),
            ("SongTitle".to_owned(), AttributeValue::S(t.to_owned())),
        ]
        .into_iter()
        .collect()
    }

    pub(super) fn golden() -> ExtenddbManifest {
        ExtenddbManifest {
            format_version: CURRENT_FORMAT_VERSION,
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
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                time_to_live: Some(TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Enabled,
                    attribute_name: Some("expires_at".to_owned()),
                }),
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

    /// The minimal manifest: an empty hash-only table with every optional
    /// member absent and every list empty.
    fn minimal() -> ExtenddbManifest {
        ExtenddbManifest {
            format_version: CURRENT_FORMAT_VERSION,
            backup_arn:
                "arn:aws:dynamodb:us-east-1:123456789012:table/Empty/backup/01757500000123-0badcafe"
                    .to_owned(),
            backup_name: "empty".to_owned(),
            backup_creation_date_time: 1_757_500_000.123,
            backup_size_bytes: 0,
            source_backend: "sqlite".to_owned(),
            source_extenddb_version: "0.1.13".to_owned(),
            snapshot: SnapshotDescriptor {
                kind: "sqlite_wal_read_transaction".to_owned(),
                marker: String::new(),
            },
            item_order: ItemOrder::Unordered,
            source_table: SourceTableSchema {
                table_name: "Empty".to_owned(),
                table_id: "00000000-0000-4000-8000-000000000000".to_owned(),
                table_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/Empty".to_owned(),
                table_creation_date_time: 1_757_400_000.0,
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: BillingMode::PayPerRequest,
                provisioned_throughput: None,
                on_demand_throughput: None,
                table_class: None,
                sse_description: None,
                global_secondary_indexes: vec![],
                local_secondary_indexes: vec![],
                vector_indexes: None,
                stream_specification: None,
                time_to_live: None,
                item_count: 0,
                table_size_bytes: 0,
            },
            data_files: vec![],
        }
    }

    /// An unordered backup (MongoDB) of an on-demand table: data files
    /// without key ranges, an `OnDemandThroughput` with both limits absent,
    /// an `SseDescription` without `SSEType` or `KMSMasterKeyArn`, a GSI
    /// without `ProvisionedThroughput`, a vector index with a search schema
    /// and a projection, an LSI with an `INCLUDE` projection, a disabled
    /// stream without `StreamViewType` and a disabled TTL without
    /// `AttributeName`: the absent states of the nested members `v1.json`
    /// sets.
    pub(super) fn unordered() -> ExtenddbManifest {
        let element = |name: &str, key_type| KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        };
        let file = |index: u32, bytes: u64| DataFileEntry {
            key: crate::backup_format::data_file_key(index),
            item_count: 1_000,
            sha256: "2".repeat(64),
            md5: "1B2M2Y8AsgTpgAmY7PhCfg==".to_owned(),
            bytes,
            uncompressed_bytes: 64_000,
            key_range: None,
        };
        ExtenddbManifest {
            format_version: CURRENT_FORMAT_VERSION,
            backup_arn:
                "arn:aws:dynamodb:eu-west-1:123456789012:table/Events/backup/01757500000123-0badcafe"
                    .to_owned(),
            backup_name: "events-weekly".to_owned(),
            backup_creation_date_time: 1_757_500_000.123,
            backup_size_bytes: 9_000 + 8_000,
            source_backend: "mongodb".to_owned(),
            source_extenddb_version: "0.1.14".to_owned(),
            snapshot: SnapshotDescriptor {
                kind: "mongodb_snapshot_read_concern".to_owned(),
                marker: "1757500000:7".to_owned(),
            },
            item_order: ItemOrder::Unordered,
            source_table: SourceTableSchema {
                table_name: "Events".to_owned(),
                table_id: "6f1c2a0e-1111-4222-8333-444455556666".to_owned(),
                table_arn: "arn:aws:dynamodb:eu-west-1:123456789012:table/Events".to_owned(),
                table_creation_date_time: 1_757_400_000.5,
                key_schema: vec![
                    element("tenant", KeyType::Hash),
                    element("at", KeyType::Range),
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "tenant".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "at".to_owned(),
                        attribute_type: ScalarAttributeType::N,
                    },
                    AttributeDefinition {
                        attribute_name: "kind".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                billing_mode: BillingMode::PayPerRequest,
                provisioned_throughput: None,
                // Both limits absent: the wire type writes each as `null`.
                on_demand_throughput: Some(OnDemandThroughput {
                    max_read_request_units: None,
                    max_write_request_units: None,
                }),
                table_class: Some("STANDARD_INFREQUENT_ACCESS".to_owned()),
                // A description without `SSEType` or `KMSMasterKeyArn`: the
                // wire type omits both.
                sse_description: Some(SseDescription {
                    status: "DISABLED".to_owned(),
                    sse_type: None,
                    kms_master_key_arn: None,
                }),
                // The GSI of an on-demand table has no `ProvisionedThroughput`.
                global_secondary_indexes: vec![GsiInput {
                    index_name: "ByKindGlobal".to_owned(),
                    key_schema: vec![element("kind", KeyType::Hash)],
                    projection: Projection {
                        projection_type: ProjectionType::KeysOnly,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                }],
                local_secondary_indexes: vec![LsiInput {
                    index_name: "ByKind".to_owned(),
                    key_schema: vec![
                        element("tenant", KeyType::Hash),
                        element("kind", KeyType::Range),
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::Include,
                        non_key_attributes: Some(vec!["payload".to_owned(), "at".to_owned()]),
                    },
                }],
                vector_indexes: Some(vec![VectorIndexSpecification {
                    index_name: "similar".to_owned(),
                    dimensions: 384,
                    distance_function: DistanceFunction::Euclidean,
                    vector_attribute: VectorAttribute {
                        attribute_name: "embedding".to_owned(),
                    },
                    search_schema: Some(vec![
                        SearchSchemaElement {
                            attribute_name: "tenant".to_owned(),
                            element_type: SearchSchemaElementType::Hash,
                        },
                        SearchSchemaElement {
                            attribute_name: "kind".to_owned(),
                            element_type: SearchSchemaElementType::InlineFilter,
                        },
                    ]),
                    projection: Some(Projection {
                        projection_type: ProjectionType::Include,
                        non_key_attributes: Some(vec!["payload".to_owned()]),
                    }),
                }]),
                // A disabled stream, as `DescribeTable` reports it: no
                // `StreamViewType`.
                stream_specification: Some(StreamSpecification {
                    stream_enabled: false,
                    stream_view_type: None,
                }),
                // TTL reported without an attribute name, as `DescribeTimeToLive`
                // reports a table whose TTL is disabled.
                time_to_live: Some(TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Disabled,
                    attribute_name: None,
                }),
                item_count: 2_000,
                table_size_bytes: 128_000,
            },
            data_files: vec![file(1, 9_000), file(2, 8_000)],
        }
    }

    /// Writes the current version's fixtures from `golden()`, `minimal()`
    /// and `unordered()`. Ignored by default so an ordinary `cargo test`
    /// never writes checked-in files; run it with
    /// `EXTENDDB_WRITE_GOLDEN=1 cargo test -p extenddb-core write_golden_manifest -- --ignored`
    /// after a version bump, then review the diff. It refuses to overwrite a
    /// fixture that exists: a released version's fixtures are never
    /// rewritten, only added beside (`ExtenddbManifest`'s evolution rule).
    /// Before format version 1 is released, a change to its shape is made
    /// by writing into another directory (`EXTENDDB_WRITE_GOLDEN=<existing
    /// directory>`; the checked-in files are `include_bytes!`'d, so they
    /// cannot be deleted first) and moving the files over the old ones.
    #[test]
    #[ignore = "writes the checked-in fixtures; run explicitly"]
    fn write_golden_manifest() {
        use std::io::Write as _;
        // `--ignored` and `--include-ignored` would otherwise write the
        // checked-in fixtures as a side effect of an unrelated run.
        let Some(target) = std::env::var_os("EXTENDDB_WRITE_GOLDEN") else {
            eprintln!("set EXTENDDB_WRITE_GOLDEN=1 to write the fixtures");
            return;
        };
        let target = std::path::PathBuf::from(target);
        let dir = if target.is_dir() {
            target
        } else {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/extenddb-manifest")
        };
        let v = format!("v{CURRENT_FORMAT_VERSION}");
        for (name, manifest) in [
            (v.clone(), golden()),
            (format!("{v}-minimal"), minimal()),
            (format!("{v}-unordered"), unordered()),
        ] {
            let bytes = manifest.to_json_bytes().unwrap();
            for (path, content) in [
                (dir.join(format!("{name}.json")), bytes.clone()),
                (
                    dir.join(format!("{name}.sha256")),
                    manifest_checksum(&bytes).into_bytes(),
                ),
            ] {
                // `create_new` fails if the file exists: the fixtures of a
                // version are written once.
                let mut file = std::fs::File::create_new(&path).unwrap_or_else(|e| {
                    panic!(
                        "refusing to write {}: {e}; a released version's fixtures are never \
                         rewritten (write into another directory with \
                         EXTENDDB_WRITE_GOLDEN=<dir> and move the files over only if version \
                         {CURRENT_FORMAT_VERSION} has not been released)",
                        path.display()
                    )
                });
                file.write_all(&content).unwrap();
            }
        }
    }

    /// The writer's constructors and the checked-in fixtures of the current
    /// version are two copies of one shape; this binds them, so an edit to
    /// `golden()`, `minimal()` or `unordered()` that the writer would turn
    /// into different fixture bytes fails here first. Written against
    /// version 1; when the version is bumped the three constructors move to
    /// the new version's shape and the comparison moves with them.
    #[test]
    fn writer_constructors_match_the_checked_in_fixtures() {
        const V1: &[u8] = include_bytes!("../../testdata/extenddb-manifest/v1.json");
        const V1_MINIMAL: &[u8] =
            include_bytes!("../../testdata/extenddb-manifest/v1-minimal.json");
        const V1_UNORDERED: &[u8] =
            include_bytes!("../../testdata/extenddb-manifest/v1-unordered.json");
        assert_eq!(
            CURRENT_FORMAT_VERSION, 1,
            "the constructors now describe version {CURRENT_FORMAT_VERSION}: compare them with \
             its fixtures here and keep the v1 files as parse tests"
        );
        for (name, manifest, fixture) in [
            ("v1.json", golden(), V1),
            ("v1-minimal.json", minimal(), V1_MINIMAL),
            ("v1-unordered.json", unordered(), V1_UNORDERED),
        ] {
            let bytes = manifest.to_json_bytes().unwrap();
            assert_eq!(
                std::str::from_utf8(&bytes).unwrap(),
                std::str::from_utf8(fixture).unwrap(),
                "{name}: the writer's constructor and the checked-in fixture differ"
            );
        }
    }
}

/// The members each type of the manifest writes, and the values each enum
/// in it reads and writes, pinned per type. The golden fixtures pin the
/// bytes of their inputs, but a new optional member that is omitted when
/// absent (`skip_serializing_if`) leaves those bytes unchanged, and an enum
/// value no fixture uses (a renamed `AES256`, a new `TimeToLiveStatus`)
/// changes no fixture either; this test fails both, until the list here is
/// changed, which is a format version bump (`ExtenddbManifest`'s evolution
/// rule).
#[cfg(test)]
mod member_names {
    use serde::de::{self, DeserializeOwned, Deserializer, Visitor};
    use serde::{Serialize, forward_to_deserialize_any};

    use super::super::{
        DataFileEntry, ExtenddbManifest, ItemOrder, KeyRange, SnapshotDescriptor, SourceTableSchema,
    };
    use super::generate_golden_manifest::{golden, unordered};
    use crate::types::{
        AttributeDefinition, AttributeValue, BillingMode, DistanceFunction, GsiInput,
        KeySchemaElement, KeyType, LsiInput, OnDemandThroughput, Projection, ProjectionType,
        ProvisionedThroughput, ScalarAttributeType, SearchSchemaElement, SearchSchemaElementType,
        SseDescription, SseType, StreamSpecification, StreamViewType, TimeToLiveDescription,
        TimeToLiveStatus, VectorAttribute, VectorIndexSpecification,
    };

    /// A deserializer that records what a derived `Deserialize` passes to
    /// `deserialize_struct` (the member names: every member the type reads,
    /// whether or not it is written when absent) or to `deserialize_enum`
    /// (the variant names it accepts), and reads nothing.
    #[derive(Default)]
    struct FieldsProbe {
        fields: Option<&'static [&'static str]>,
        variants: Option<&'static [&'static str]>,
    }

    impl<'de> Deserializer<'de> for &mut FieldsProbe {
        type Error = de::value::Error;
        fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
            Err(de::Error::custom("not a struct or an enum"))
        }
        fn deserialize_struct<V: Visitor<'de>>(
            self,
            _name: &'static str,
            fields: &'static [&'static str],
            _visitor: V,
        ) -> Result<V::Value, Self::Error> {
            self.fields = Some(fields);
            Err(de::Error::custom("probe"))
        }
        fn deserialize_enum<V: Visitor<'de>>(
            self,
            _name: &'static str,
            variants: &'static [&'static str],
            _visitor: V,
        ) -> Result<V::Value, Self::Error> {
            self.variants = Some(variants);
            Err(de::Error::custom("probe"))
        }
        forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
            bytes byte_buf option unit unit_struct newtype_struct seq tuple
            tuple_struct map identifier ignored_any
        }
    }

    fn fields_read<T: DeserializeOwned>() -> Vec<&'static str> {
        let mut probe = FieldsProbe::default();
        let _ = T::deserialize(&mut probe);
        probe
            .fields
            .expect("a derived struct deserializer")
            .to_vec()
    }

    /// A derived enum reads exactly the values `expected`, and each of them
    /// parses and writes back as itself.
    fn check_enum<T: Serialize + DeserializeOwned>(name: &str, expected: &[&str]) {
        let mut probe = FieldsProbe::default();
        let _ = T::deserialize(&mut probe);
        let variants = probe.variants.expect("a derived enum deserializer");
        assert_eq!(
            variants, expected,
            "{name}: the values read changed; that is a format version bump"
        );
        for wire in expected {
            let value: T = serde_json::from_value(serde_json::json!(wire))
                .unwrap_or_else(|e| panic!("{name}: {wire:?} does not parse: {e}"));
            assert_eq!(
                serde_json::to_value(&value).unwrap(),
                serde_json::json!(wire),
                "{name}: {wire:?} is written differently"
            );
        }
    }

    /// An enum with a hand-written `Deserialize`, which the probe cannot
    /// see: every row's value writes as its wire string and the string
    /// parses as the value, the strings are distinct, and a spelling in
    /// another case is refused. The rows come from an exhaustive `match`
    /// with no wildcard at the call site, so a new variant does not compile
    /// until it has a row.
    fn check_wire_table<T>(name: &str, rows: &[(T, &str)])
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let mut seen = std::collections::BTreeSet::new();
        for (value, wire) in rows {
            assert!(seen.insert(*wire), "{name}: {wire:?} is listed twice");
            assert_eq!(
                serde_json::to_value(value).unwrap(),
                serde_json::json!(wire),
                "{name}: {value:?} is written differently; that is a format version bump"
            );
            let parsed: T = serde_json::from_value(serde_json::json!(wire))
                .unwrap_or_else(|e| panic!("{name}: {wire:?} does not parse: {e}"));
            assert_eq!(&parsed, value, "{name}: {wire:?} parses as another value");
            let other_case = wire.to_lowercase();
            if other_case != *wire {
                assert!(
                    serde_json::from_value::<T>(serde_json::json!(other_case)).is_err(),
                    "{name}: {other_case:?} is accepted"
                );
            }
        }
    }

    /// `AttributeValue`, whose wire form is one object with the type
    /// descriptor as its one member (`{"S":"a"}`), not a string: every
    /// row's value writes as an object whose one member is its descriptor
    /// and reads back as itself, the descriptors are distinct, a descriptor
    /// in lower case is refused, and the descriptors the reader accepts,
    /// among every string of one to four ASCII upper-case letters (the
    /// alphabet and the lengths of `DynamoDB`'s descriptors), are exactly
    /// the rows'. The rows come from an exhaustive `match` with no wildcard
    /// at the call site, so a new variant does not compile until it has a
    /// row, and a descriptor the reader starts to accept fails the last
    /// check.
    fn check_descriptor_table(name: &str, rows: &[(AttributeValue, &str)]) {
        let mut descriptors = std::collections::BTreeSet::new();
        for (value, descriptor) in rows {
            assert!(
                descriptors.insert(*descriptor),
                "{name}: {descriptor:?} is listed twice"
            );
            let written = serde_json::to_value(value).unwrap();
            let members: Vec<&String> = written.as_object().unwrap().keys().collect();
            assert_eq!(
                members,
                [descriptor],
                "{name}: {value:?} is written under another descriptor; that is a format version bump"
            );
            let parsed: AttributeValue = serde_json::from_value(written.clone())
                .unwrap_or_else(|e| panic!("{name}: {written} does not parse: {e}"));
            assert_eq!(&parsed, value, "{name}: {written} parses as another value");
            let other_case = serde_json::json!({ descriptor.to_lowercase(): written[*descriptor] });
            assert!(
                serde_json::from_value::<AttributeValue>(other_case.clone()).is_err(),
                "{name}: {other_case} is accepted"
            );
        }
        // A descriptor the reader knows fails on its payload or parses;
        // one it does not know fails on the descriptor, whatever the
        // payload.
        let unknown = |candidate: &str| {
            serde_json::from_value::<AttributeValue>(serde_json::json!({ candidate: null }))
                .err()
                .is_some_and(|e| {
                    e.to_string()
                        .starts_with("unknown AttributeValue type descriptor")
                })
        };
        let mut accepted = std::collections::BTreeSet::new();
        let mut candidate = String::new();
        for length in 1..=4 {
            let mut digits = vec![0u8; length];
            loop {
                candidate.clear();
                candidate.extend(digits.iter().map(|d| char::from(b'A' + d)));
                if !unknown(&candidate) {
                    accepted.insert(candidate.clone());
                }
                let mut i = 0;
                while i < length && digits[i] == 25 {
                    digits[i] = 0;
                    i += 1;
                }
                if i == length {
                    break;
                }
                digits[i] += 1;
            }
        }
        let accepted: Vec<&str> = accepted.iter().map(String::as_str).collect();
        let rows: Vec<&str> = descriptors.into_iter().collect();
        assert_eq!(
            accepted, rows,
            "{name}: the descriptors read changed; that is a format version bump"
        );
    }

    /// The enums the manifest carries, each with its wire values. `ItemOrder`
    /// and the derived wire enums are read by the probe; `KeyType`,
    /// `ScalarAttributeType` and `DistanceFunction` have hand-written
    /// `Deserialize` impls and are pinned by tables, and so is
    /// `AttributeValue`, the value type of every `KeyRange` endpoint and of
    /// every data file line, with its ten type descriptors. Every enum-typed
    /// member of the types `nested_types_have_the_pinned_members` lists is
    /// here (`SseDescription.Status` and `table_class` are strings).
    #[test]
    fn enum_values_are_pinned() {
        check_enum::<ItemOrder>("ItemOrder", &["key_ordered", "unordered"]);
        check_enum::<BillingMode>("BillingMode", &["PROVISIONED", "PAY_PER_REQUEST"]);
        check_enum::<ProjectionType>("ProjectionType", &["ALL", "KEYS_ONLY", "INCLUDE"]);
        check_enum::<StreamViewType>(
            "StreamViewType",
            &["KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES"],
        );
        check_enum::<SseType>("SseType", &["AES256", "KMS"]);
        check_enum::<TimeToLiveStatus>(
            "TimeToLiveStatus",
            &["ENABLED", "ENABLING", "DISABLED", "DISABLING"],
        );
        check_enum::<SearchSchemaElementType>(
            "SearchSchemaElementType",
            &["HASH", "INLINE_FILTER"],
        );

        let key_type = |v: KeyType| match v {
            KeyType::Hash => "HASH",
            KeyType::Range => "RANGE",
        };
        check_wire_table(
            "KeyType",
            &[KeyType::Hash, KeyType::Range].map(|v| (v, key_type(v))),
        );
        let scalar = |v: ScalarAttributeType| match v {
            ScalarAttributeType::S => "S",
            ScalarAttributeType::N => "N",
            ScalarAttributeType::B => "B",
        };
        check_wire_table(
            "ScalarAttributeType",
            &[
                ScalarAttributeType::S,
                ScalarAttributeType::N,
                ScalarAttributeType::B,
            ]
            .map(|v| (v, scalar(v))),
        );
        let distance = |v: DistanceFunction| match v {
            DistanceFunction::Cosine => "COSINE",
            DistanceFunction::Euclidean => "EUCLIDEAN",
            DistanceFunction::DotProduct => "DOT_PRODUCT",
        };
        check_wire_table(
            "DistanceFunction",
            &[
                DistanceFunction::Cosine,
                DistanceFunction::Euclidean,
                DistanceFunction::DotProduct,
            ]
            .map(|v| (v, distance(v))),
        );
        let descriptor = |v: &AttributeValue| match v {
            AttributeValue::S(_) => "S",
            AttributeValue::N(_) => "N",
            AttributeValue::B(_) => "B",
            AttributeValue::SS(_) => "SS",
            AttributeValue::NS(_) => "NS",
            AttributeValue::BS(_) => "BS",
            AttributeValue::Bool(_) => "BOOL",
            AttributeValue::Null => "NULL",
            AttributeValue::L(_) => "L",
            AttributeValue::M(_) => "M",
        };
        let rows: Vec<(AttributeValue, &str)> = [
            AttributeValue::S("a".to_owned()),
            AttributeValue::N("1".to_owned()),
            AttributeValue::B(vec![0xde, 0xad]),
            AttributeValue::SS(["a".to_owned()].into_iter().collect()),
            AttributeValue::NS(["1".to_owned()].into_iter().collect()),
            AttributeValue::BS([vec![0xde]].into_iter().collect()),
            AttributeValue::Bool(true),
            AttributeValue::Null,
            AttributeValue::L(vec![AttributeValue::N("1".to_owned())]),
            AttributeValue::M(
                [("k".to_owned(), AttributeValue::S("v".to_owned()))]
                    .into_iter()
                    .collect(),
            ),
        ]
        .into_iter()
        .map(|v| {
            let d = descriptor(&v);
            (v, d)
        })
        .collect();
        check_descriptor_table("AttributeValue", &rows);
        // A value outside each table is refused: the hand-written readers
        // have no fallback.
        assert!(serde_json::from_value::<KeyType>(serde_json::json!("SORT")).is_err());
        assert!(serde_json::from_value::<ScalarAttributeType>(serde_json::json!("SS")).is_err());
        assert!(
            serde_json::from_value::<DistanceFunction>(serde_json::json!("MANHATTAN")).is_err()
        );
    }

    /// The top-level member names of `value`'s JSON, in the order written,
    /// each with whether its value is `null`.
    fn members_written<T: Serialize>(value: &T) -> Vec<(String, bool)> {
        struct Keys;
        impl<'de> Visitor<'de> for Keys {
            type Value = Vec<(String, bool)>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an object")
            }
            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    let value: serde_json::Value = map.next_value()?;
                    out.push((key, value.is_null()));
                }
                Ok(out)
            }
        }
        let text = serde_json::to_string(value).unwrap();
        serde_json::Deserializer::from_str(&text)
            .deserialize_map(Keys)
            .unwrap()
    }

    /// `value` has every member set: it writes exactly `expected`, in that
    /// order, none of them `null`; and the type reads exactly `expected`.
    fn check<T: Serialize + DeserializeOwned>(name: &str, value: &T, expected: &[&str]) {
        let written = members_written(value);
        let names: Vec<&str> = written.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names, expected,
            "{name}: the members written changed; that is a format version bump"
        );
        for (member, null) in &written {
            assert!(!null, "{name}.{member} is not set in the value checked");
        }
        assert_eq!(
            fields_read::<T>(),
            expected,
            "{name}: the members read changed; that is a format version bump"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)] // one pinned list per type
    fn nested_types_have_the_pinned_members() {
        let full = golden();
        let other = unordered();
        let t = &full.source_table;
        let file = &full.data_files[0];
        let vector = &other.source_table.vector_indexes.as_ref().unwrap()[0];
        check::<ExtenddbManifest>(
            "ExtenddbManifest",
            &full,
            &[
                "format_version",
                "backup_arn",
                "backup_name",
                "backup_creation_date_time",
                "backup_size_bytes",
                "source_backend",
                "source_extenddb_version",
                "snapshot",
                "item_order",
                "source_table",
                "data_files",
            ],
        );
        check::<SnapshotDescriptor>("SnapshotDescriptor", &full.snapshot, &["kind", "marker"]);
        check::<SourceTableSchema>(
            "SourceTableSchema",
            t,
            &[
                "table_name",
                "table_id",
                "table_arn",
                "table_creation_date_time",
                "key_schema",
                "attribute_definitions",
                "billing_mode",
                "provisioned_throughput",
                "on_demand_throughput",
                "table_class",
                "sse_description",
                "global_secondary_indexes",
                "local_secondary_indexes",
                "vector_indexes",
                "stream_specification",
                "time_to_live",
                "item_count",
                "table_size_bytes",
            ],
        );
        check::<DataFileEntry>(
            "DataFileEntry",
            file,
            &[
                "key",
                "item_count",
                "sha256",
                "md5",
                "bytes",
                "uncompressed_bytes",
                "key_range",
            ],
        );
        check::<KeyRange>(
            "KeyRange",
            file.key_range.as_ref().unwrap(),
            &["first", "last"],
        );
        check::<KeySchemaElement>(
            "KeySchemaElement",
            &t.key_schema[0],
            &["AttributeName", "KeyType"],
        );
        check::<AttributeDefinition>(
            "AttributeDefinition",
            &t.attribute_definitions[0],
            &["AttributeName", "AttributeType"],
        );
        check::<ProvisionedThroughput>(
            "ProvisionedThroughput",
            t.provisioned_throughput.as_ref().unwrap(),
            &["ReadCapacityUnits", "WriteCapacityUnits"],
        );
        check::<OnDemandThroughput>(
            "OnDemandThroughput",
            t.on_demand_throughput.as_ref().unwrap(),
            &["MaxReadRequestUnits", "MaxWriteRequestUnits"],
        );
        check::<SseDescription>(
            "SseDescription",
            t.sse_description.as_ref().unwrap(),
            &["Status", "SSEType", "KMSMasterKeyArn"],
        );
        let gsi = &t.global_secondary_indexes[0];
        check::<GsiInput>(
            "GsiInput",
            gsi,
            &[
                "IndexName",
                "KeySchema",
                "Projection",
                "ProvisionedThroughput",
            ],
        );
        check::<Projection>(
            "Projection",
            &gsi.projection,
            &["ProjectionType", "NonKeyAttributes"],
        );
        check::<LsiInput>(
            "LsiInput",
            &other.source_table.local_secondary_indexes[0],
            &["IndexName", "KeySchema", "Projection"],
        );
        check::<VectorIndexSpecification>(
            "VectorIndexSpecification",
            vector,
            &[
                "IndexName",
                "Dimensions",
                "DistanceFunction",
                "VectorAttribute",
                "SearchSchema",
                "Projection",
            ],
        );
        check::<VectorAttribute>(
            "VectorAttribute",
            &vector.vector_attribute,
            &["AttributeName"],
        );
        check::<SearchSchemaElement>(
            "SearchSchemaElement",
            &vector.search_schema.as_ref().unwrap()[0],
            &["AttributeName", "SearchSchemaElementType"],
        );
        check::<StreamSpecification>(
            "StreamSpecification",
            t.stream_specification.as_ref().unwrap(),
            &["StreamEnabled", "StreamViewType"],
        );
        check::<TimeToLiveDescription>(
            "TimeToLiveDescription",
            t.time_to_live.as_ref().unwrap(),
            &["TimeToLiveStatus", "AttributeName"],
        );
    }
}
