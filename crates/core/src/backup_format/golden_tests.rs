// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Golden-file tests against the checked-in `DynamoDB` export under
//! `testdata/ddb-export/`. See the README in that directory for the fixture's
//! provenance.

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
