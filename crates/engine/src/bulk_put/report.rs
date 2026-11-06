// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Report encoding and record construction for bulk put operations.
//!
//! Each report file is gzip-compressed newline-delimited JSON, one record
//! per failed or condition-failed row. The object key layout and the record
//! shapes reproduce the hosted error report, so tooling written for that
//! report parses this output unchanged.

use std::io::Write;

use extenddb_core::types::Item;
use extenddb_storage::ReportKind;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The members of one report record, declared in the service's order
/// (`itemLocation`, `errorMessages`, `newImage`, `oldImage`). `serde_json`'s
/// `Map` is a `BTreeMap`, so a `Value` serializes its keys sorted; the
/// records travel as `Value` through the runner, so they are round-tripped
/// through this struct at serialization time to restore the service order.
/// Enabling `serde_json`'s `preserve_order` feature would reorder every map
/// the workspace serializes, so it is not used.
#[derive(Serialize, Deserialize)]
struct OrderedReportRecord {
    #[serde(rename = "itemLocation")]
    item_location: OrderedItemLocation,
    #[serde(rename = "errorMessages")]
    error_messages: Vec<String>,
    #[serde(rename = "newImage", skip_serializing_if = "Option::is_none")]
    new_image: Option<Value>,
    #[serde(rename = "oldImage", skip_serializing_if = "Option::is_none")]
    old_image: Option<Value>,
}

/// The members of `itemLocation`, declared in the service's order
/// (`inputSourceArn`, then `itemIndex` when present).
#[derive(Serialize, Deserialize)]
struct OrderedItemLocation {
    #[serde(rename = "inputSourceArn")]
    input_source_arn: String,
    #[serde(rename = "itemIndex", skip_serializing_if = "Option::is_none")]
    item_index: Option<i64>,
}

/// The members of one manifest line, declared in the service's order
/// (`inputSourceArn`, `fileProcessingStatus`, `processedRanges`, `etag`).
#[derive(Serialize, Deserialize)]
struct OrderedManifestLine {
    #[serde(rename = "inputSourceArn")]
    input_source_arn: String,
    #[serde(rename = "fileProcessingStatus")]
    file_processing_status: String,
    #[serde(rename = "processedRanges")]
    processed_ranges: Vec<Value>,
    #[serde(rename = "etag")]
    etag: String,
}

/// Serialize one report record with its members in the service's order,
/// round-tripping the `Value` through [`OrderedReportRecord`]. A record that
/// does not match the shape falls back to the `Value`'s own serialization
/// (the byte count is the same either way; only the key order differs).
fn ordered_record_json(record: &Value) -> String {
    match serde_json::from_value::<OrderedReportRecord>(record.clone()) {
        Ok(ordered) => serde_json::to_string(&ordered).unwrap_or_else(|_| record.to_string()),
        Err(_) => record.to_string(),
    }
}

/// Serialize one manifest line with its members in the service's order,
/// round-tripping the `Value` through [`OrderedManifestLine`]. The manifest
/// writer calls this so the written bytes carry the service member order.
pub(crate) fn write_manifest_line<W: Write>(
    writer: &mut W,
    line: &Value,
) -> Result<(), std::io::Error> {
    let bytes = match serde_json::from_value::<OrderedManifestLine>(line.clone()) {
        Ok(ordered) => serde_json::to_string(&ordered).unwrap_or_else(|_| line.to_string()),
        Err(_) => line.to_string(),
    };
    writer.write_all(bytes.as_bytes())
}

/// The message every condition-failure record carries, the one `PutItem`
/// returns for the same outcome.
const CONDITION_FAILED_MESSAGE: &str = "The conditional request failed";

/// Gzip-encode one page of records into `encoder` as newline-delimited
/// JSON, one record per line. The encoder holds the gzip stream state
/// across pages; the caller finishes it once after the last page, so the
/// whole report is never held in memory.
pub(crate) fn encode_ndjson_page<W: Write>(
    encoder: &mut GzEncoder<W>,
    records: &[Value],
) -> Result<(), std::io::Error> {
    for record in records {
        encoder.write_all(ordered_record_json(record).as_bytes())?;
        encoder.write_all(b"\n")?;
    }
    Ok(())
}

/// The serialized length in bytes of one report record, the figure the
/// per-operation report byte cap counts. The newline terminator the encoder
/// appends is not included.
#[must_use]
pub(crate) fn record_bytes(record: &Value) -> u64 {
    u64::try_from(record.to_string().len()).unwrap_or(u64::MAX)
}

/// The directory name each report kind renders under.
#[must_use]
pub(crate) fn kind_directory(kind: ReportKind) -> &'static str {
    match kind {
        ReportKind::Error => "errors",
        ReportKind::ConditionCheckFailure => "condition-check-failures",
    }
}

/// The configured output prefix as it precedes `AWSDynamoDB/` in every
/// output key (measured on the service): an empty prefix
/// contributes nothing, so the key starts at `AWSDynamoDB/` with no
/// leading slash; a prefix ending in `/` is used as it is; any other
/// prefix gets one `/` appended. A trailing slash is never doubled.
fn prefix_segment(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_owned()
    } else {
        format!("{prefix}/")
    }
}

/// The manifest's key: the operation directory itself, with no `-0` shard
/// suffix (measured on the service), and the same prefix rule as a report.
#[must_use]
pub(crate) fn manifest_key(prefix: &str, account: &str, operation_id: &str) -> String {
    let prefix = prefix_segment(prefix);
    format!("{prefix}AWSDynamoDB/BulkOperation/{account}:{operation_id}/manifest-files.json")
}

/// Processing status of one input file as the manifest names it. Four
/// values, each measured on the service: the two a cancel shows, the one
/// written for an input object rejected before any read, and the one
/// written for an object whose read ended abnormally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileProcessingStatus {
    /// Read to its end with no abandon.
    FullyProcessed,
    /// Abandoned at a parse-level fault, or cut short by a cancel or a
    /// failure.
    PartiallyProcessed,
    /// Never opened: the operation ended before it reached this file.
    NotProcessed,
    /// The read ended abnormally because the object itself was gone or
    /// unreadable; no processed range is reported.
    NotAvailable,
}

/// One manifest line (measured on the service): the file's S3 ARN, its status,
/// the processed row index ranges (`[[0, last]]`, or `[]` when no row was
/// counted), and the file's entity tag.
#[must_use]
pub(crate) fn manifest_line(
    bucket: &str,
    path: &str,
    status: FileProcessingStatus,
    last_counted_row: Option<u64>,
    etag: &str,
) -> Value {
    let ranges: Vec<Value> = match last_counted_row {
        Some(last) => vec![json!([0, last])],
        None => Vec::new(),
    };
    json!({
        "inputSourceArn": format!("arn:aws:s3:::{bucket}/{path}"),
        "fileProcessingStatus": match status {
            FileProcessingStatus::FullyProcessed => "FULLY_PROCESSED",
            FileProcessingStatus::PartiallyProcessed => "PARTIALLY_PROCESSED",
            FileProcessingStatus::NotProcessed => "NOT_PROCESSED",
            FileProcessingStatus::NotAvailable => "NOT_AVAILABLE",
        },
        "processedRanges": ranges,
        "etag": etag,
    })
}

/// The object key of report object `index` of one kind, spelled as the
/// hosted service spells it, with the prefix rule of `prefix_segment`. The
/// first object is `output-00000.json.gz`; the writer rolls to
/// `output-00001.json.gz` and onward once the compressed bytes of an object
/// reach 200 MiB.
#[must_use]
pub(crate) fn report_key(
    prefix: &str,
    account: &str,
    operation_id: &str,
    kind: ReportKind,
    index: u32,
) -> String {
    let kind_dir = kind_directory(kind);
    let prefix = prefix_segment(prefix);
    format!(
        "{prefix}AWSDynamoDB/BulkOperation/{account}:{operation_id}-0/output/{kind_dir}/output-{index:05}.json.gz"
    )
}

/// One error-report record, for a parse-level or item-level fault.
///
/// `item_index` is absent on the parse-level abandon record and present on
/// item-level records. `new_image` presence follows the fault class and the
/// runner decides it: measured present on missing-key faults, absent on
/// type-mismatch, oversized, and parse-level records.
#[must_use]
pub(crate) fn error_record(
    bucket: &str,
    path: &str,
    item_index: Option<i64>,
    message: &str,
    new_image: Option<&Item>,
) -> Value {
    let mut record = json!({
        "itemLocation": item_location(bucket, path, item_index),
        "errorMessages": [message],
    });
    if let Some(item) = new_image {
        record["newImage"] = wrapped_item(item);
    }
    record
}

/// One condition-failure record: the fixed message, the submitted item as
/// `newImage`, and, when the stored item could be read, that item as
/// `oldImage`.
#[must_use]
pub(crate) fn condition_record(
    bucket: &str,
    path: &str,
    item_index: Option<i64>,
    new_image: &Item,
    old_image: Option<&Item>,
) -> Value {
    let mut record = json!({
        "itemLocation": item_location(bucket, path, item_index),
        "errorMessages": [CONDITION_FAILED_MESSAGE],
        "newImage": wrapped_item(new_image),
    });
    if let Some(item) = old_image {
        record["oldImage"] = wrapped_item(item);
    }
    record
}

/// `itemLocation` names the source row: the input file spelled as an S3
/// object ARN over the bucket-relative path, plus the zero-based line index
/// when the fault is attributable to one line.
fn item_location(bucket: &str, path: &str, item_index: Option<i64>) -> Value {
    let mut location = json!({
        "inputSourceArn": format!("arn:aws:s3:::{bucket}/{path}"),
    });
    if let Some(index) = item_index {
        location["itemIndex"] = json!(index);
    }
    location
}

/// `newImage` and `oldImage` wrap the item in `{"Item": ...}` (measured).
fn wrapped_item(item: &Item) -> Value {
    // A map with string keys cannot fail to serialize; Null appears only if
    // that invariant broke, and the record stays well-formed either way.
    let value = serde_json::to_value(item).unwrap_or(Value::Null);
    json!({"Item": value})
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::AttributeValue;
    use flate2::Compression;

    const ACCOUNT: &str = "111122223333";
    const OP_ID: &str = "01789060589767-da9ddd4f";

    fn item(key: &str, value: &str) -> Item {
        let mut item = Item::new();
        item.insert(key.to_owned(), AttributeValue::S(value.to_owned()));
        item
    }

    #[test]
    fn report_key_without_a_trailing_slash_inserts_one_separator() {
        let key = report_key("loads/reports", ACCOUNT, OP_ID, ReportKind::Error, 0);
        assert_eq!(
            key,
            format!(
                "loads/reports/AWSDynamoDB/BulkOperation/{ACCOUNT}:{OP_ID}-0/output/errors/output-00000.json.gz"
            )
        );
    }

    #[test]
    fn report_key_with_a_trailing_slash_keeps_one_separator() {
        // Measured on the service: `out-d3/` yields `out-d3/AWSDynamoDB`,
        // not a doubled slash.
        let key = report_key(
            "loads/reports/",
            ACCOUNT,
            OP_ID,
            ReportKind::ConditionCheckFailure,
            0,
        );
        assert_eq!(
            key,
            format!(
                "loads/reports/AWSDynamoDB/BulkOperation/{ACCOUNT}:{OP_ID}-0/output/condition-check-failures/output-00000.json.gz"
            )
        );
    }

    #[test]
    fn later_report_objects_are_numbered_with_five_digits() {
        // The second object of a kind is `output-00001.json.gz`.
        assert_eq!(
            report_key("out", ACCOUNT, OP_ID, ReportKind::ConditionCheckFailure, 1),
            format!(
                "out/AWSDynamoDB/BulkOperation/{ACCOUNT}:{OP_ID}-0/output/condition-check-failures/output-00001.json.gz"
            )
        );
    }

    #[test]
    fn keys_without_a_prefix_start_at_the_service_directory() {
        // Measured on the service: no output prefix puts both objects
        // at the bucket root with no leading slash.
        assert_eq!(
            report_key("", ACCOUNT, OP_ID, ReportKind::Error, 0),
            format!(
                "AWSDynamoDB/BulkOperation/{ACCOUNT}:{OP_ID}-0/output/errors/output-00000.json.gz"
            )
        );
        assert_eq!(
            manifest_key("", ACCOUNT, OP_ID),
            format!("AWSDynamoDB/BulkOperation/{ACCOUNT}:{OP_ID}/manifest-files.json")
        );
        assert_eq!(
            manifest_key("run/", ACCOUNT, OP_ID),
            format!("run/AWSDynamoDB/BulkOperation/{ACCOUNT}:{OP_ID}/manifest-files.json")
        );
    }

    #[test]
    fn encode_ndjson_page_round_trips_across_pages() {
        let first = vec![serde_json::json!({"errorMessages": ["a"]})];
        let second = vec![serde_json::json!({"errorMessages": ["b"], "n": 1})];
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encode_ndjson_page(&mut encoder, &first).unwrap();
        encode_ndjson_page(&mut encoder, &second).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut decoder = flate2::read::GzDecoder::new(compressed.as_slice());
        let mut plain = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut plain).unwrap();
        let parsed: Vec<Value> = plain
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(parsed, [first, second].concat());
        assert_eq!(plain.matches('\n').count(), 2, "one terminator per record");
    }

    #[test]
    fn record_bytes_is_the_serialized_length() {
        // `{"a":1}` serializes to seven bytes.
        assert_eq!(record_bytes(&serde_json::json!({"a": 1})), 7);
        // `{}` serializes to two.
        assert_eq!(record_bytes(&serde_json::json!({})), 2);
    }

    #[test]
    fn error_record_carries_index_and_image_only_when_given() {
        let full = error_record(
            "loads",
            "orders/part-000.json",
            Some(2),
            "One or more parameter values were invalid: Missing the key sk in the item",
            Some(&item("pk", "a")),
        );
        assert_eq!(
            full["itemLocation"]["inputSourceArn"],
            "arn:aws:s3:::loads/orders/part-000.json"
        );
        assert_eq!(full["itemLocation"]["itemIndex"], 2);
        assert_eq!(
            full["errorMessages"],
            serde_json::json!([
                "One or more parameter values were invalid: Missing the key sk in the item"
            ])
        );
        assert_eq!(full["newImage"]["Item"]["pk"]["S"], "a");
        assert!(full.get("oldImage").is_none());

        let abandon = error_record("loads", "orders/part-000.json", None, "m", None);
        assert!(abandon["itemLocation"].get("itemIndex").is_none());
        assert!(abandon.get("newImage").is_none());
    }

    #[test]
    fn condition_record_carries_the_fixed_message_and_both_images() {
        let record = condition_record(
            "loads",
            "part-000.json",
            Some(5),
            &item("pk", "new"),
            Some(&item("pk", "old")),
        );
        assert_eq!(
            record["errorMessages"],
            serde_json::json!(["The conditional request failed"])
        );
        assert_eq!(record["newImage"]["Item"]["pk"]["S"], "new");
        assert_eq!(record["oldImage"]["Item"]["pk"]["S"], "old");
        assert_eq!(record["itemLocation"]["itemIndex"], 5);

        let missing_old = condition_record("loads", "p.json", Some(0), &item("pk", "new"), None);
        assert!(missing_old.get("oldImage").is_none());
    }

    #[test]
    fn an_error_record_serializes_in_service_member_order() {
        let record = error_record(
            "loads",
            "part-000.json",
            Some(2),
            "boom",
            Some(&item("pk", "a")),
        );
        assert_eq!(
            ordered_record_json(&record),
            r#"{"itemLocation":{"inputSourceArn":"arn:aws:s3:::loads/part-000.json","itemIndex":2},"errorMessages":["boom"],"newImage":{"Item":{"pk":{"S":"a"}}}}"#
        );
    }

    #[test]
    fn a_condition_record_serializes_in_service_member_order() {
        let record = condition_record(
            "loads",
            "part-000.json",
            Some(5),
            &item("pk", "new"),
            Some(&item("pk", "old")),
        );
        assert_eq!(
            ordered_record_json(&record),
            r#"{"itemLocation":{"inputSourceArn":"arn:aws:s3:::loads/part-000.json","itemIndex":5},"errorMessages":["The conditional request failed"],"newImage":{"Item":{"pk":{"S":"new"}}},"oldImage":{"Item":{"pk":{"S":"old"}}}}"#
        );
    }

    #[test]
    fn a_manifest_line_serializes_in_service_member_order() {
        let line = manifest_line(
            "loads",
            "orders/a.json",
            FileProcessingStatus::FullyProcessed,
            Some(5),
            "etag123",
        );
        let mut buffer: Vec<u8> = Vec::new();
        write_manifest_line(&mut buffer, &line).unwrap();
        assert_eq!(
            String::from_utf8(buffer).unwrap(),
            r#"{"inputSourceArn":"arn:aws:s3:::loads/orders/a.json","fileProcessingStatus":"FULLY_PROCESSED","processedRanges":[[0,5]],"etag":"etag123"}"#
        );
    }

    #[test]
    fn encode_ndjson_page_emits_records_in_service_member_order() {
        let record = error_record("loads", "p.json", Some(1), "boom", Some(&item("pk", "a")));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encode_ndjson_page(&mut encoder, std::slice::from_ref(&record)).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut decoder = flate2::read::GzDecoder::new(compressed.as_slice());
        let mut plain = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut plain).unwrap();
        assert_eq!(
            plain,
            "{\"itemLocation\":{\"inputSourceArn\":\"arn:aws:s3:::loads/p.json\",\"itemIndex\":1},\"errorMessages\":[\"boom\"],\"newImage\":{\"Item\":{\"pk\":{\"S\":\"a\"}}}}\n"
        );
    }
}
