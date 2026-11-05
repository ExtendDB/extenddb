// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Row parsing acceptance.

// Measured on the service: a top-level key outside the record shape abandons
// the file with its own message, the bad row counted processed and failed,
// the rows after it never read.
#[tokio::test]
async fn a_row_with_an_unknown_top_level_key_abandons_the_file() {
    let h = Harness::new().await;
    let extra =
        "{\"Item\": {\"pk\": {\"S\": \"k1\"}, \"sk\": {\"S\": \"s\"}}, \"Unknown\": 1}".to_owned();
    h.write_input("part-000.json", &file_of(&[row(0), extra, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k0").await.is_some());
    assert!(h.get("k1").await.is_none());
    assert!(
        h.get("k2").await.is_none(),
        "the row after the abandon is never read"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::UNKNOWN_FIELD_MESSAGE])
    );
    assert!(report[0]["itemLocation"].get("itemIndex").is_none());
}

// Measured on the service: 32 and 33 levels fail the row alone with the short
// message "Nesting Levels have exceeded supported limits" and the submitted
// image, and the file continues; 31 levels is stored.
#[tokio::test]
async fn a_row_nested_past_the_item_limit_fails_alone_with_the_short_message_and_image() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[
            row_nested(0, 31),
            row_nested(1, 32),
            row_nested(2, 33),
            row(3),
        ]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 4);
    assert_eq!(record.failed_item_count, 2);
    assert!(h.get("k0").await.is_some(), "31 levels is stored");
    assert!(h.get("k1").await.is_none());
    assert!(h.get("k2").await.is_none());
    assert!(h.get("k3").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2);
    for (record, index) in report.iter().zip([1, 2]) {
        assert_eq!(
            record["errorMessages"],
            json!(["Nesting Levels have exceeded supported limits"])
        );
        assert_eq!(record["itemLocation"]["itemIndex"], json!(index));
        assert_eq!(
            record["newImage"]["Item"]["pk"],
            json!({"S": format!("k{index}")})
        );
    }
}

// Measured on the service: 64 levels abandons the file with the long nesting
// message and no item index; the rows after it are never read.
#[tokio::test]
async fn a_row_nested_sixty_four_levels_abandons_the_file() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_nested(1, 64), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k0").await.is_some());
    assert!(h.get("k2").await.is_none());
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::NESTING_ABANDON_MESSAGE])
    );
    assert!(report[0]["itemLocation"].get("itemIndex").is_none());
    assert!(report[0].get("newImage").is_none());
}

#[tokio::test]
async fn a_non_parseable_line_abandons_the_file_with_the_unexpected_token_message() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), "this is not json {{{".to_owned(), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k2").await.is_none(), "the file is abandoned");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::CORRUPT_STREAM_MESSAGE]),
        "measured on the service: text that is not JSON is an unexpected token"
    );
}

/// A row whose attribute value decoded but failed a rule (an empty string
/// set) fails alone with the `PutItem` message and the file continues; a
/// repeated set member is not a fault and the row is written with the set
/// deduplicated (both measured on the service).
#[tokio::test]
async fn a_row_with_an_invalid_set_fails_alone_and_the_file_continues() {
    let h = Harness::new().await;
    let rows = [
        row(0),
        r#"{"Item": {"pk": {"S": "k1"}, "sk": {"S": "s"}, "ss": {"SS": []}}}"#.to_owned(),
        row(2),
        r#"{"Item": {"pk": {"S": "k3"}, "sk": {"S": "s"}, "ss": {"SS": ["a", "a"]}}}"#.to_owned(),
        row(4),
    ];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 4);
    let k3 = h.get("k3").await.expect("the deduplicated row is written");
    assert_eq!(
        k3.get("ss"),
        Some(&AttributeValue::SS(["a".to_owned()].into_iter().collect()))
    );
    assert!(
        h.get("k4").await.is_some(),
        "the row after the faults is written"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!(["One or more parameter values were invalid: An string set  may not be empty"])
    );
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(1));
}

/// A number set with numerically equal members written
/// differently (`NS ["1", "1.0"]`) is deduplicated by number and the row is
/// written with the set collapsed, where the service stores it as `NS ["1"]`
/// with the operation COMPLETED (measured on the service). The deserializer
/// normalizes each member and refuses a set whose members collapse to fewer,
/// so without the number-aware dedupe the row would fail; the bulk dedupe
/// keeps the first member and the set stores as the single normalized value.
#[tokio::test]
async fn a_number_set_with_numerically_equal_members_is_written_deduplicated() {
    let h = Harness::new().await;
    let rows = [
        row(0),
        r#"{"Item": {"pk": {"S": "k1"}, "sk": {"S": "s"}, "ns": {"NS": ["1", "1.0"]}}}"#.to_owned(),
        row(2),
    ];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(h.count_items().await, 3);
    let k1 = h
        .get("k1")
        .await
        .expect("the deduplicated number set is written");
    assert_eq!(
        k1.get("ns"),
        Some(&AttributeValue::NS(["1".to_owned()].into_iter().collect())),
        "NS [1, 1.0] stores as the single normalized member 1"
    );
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none(),
        "no row failed"
    );
}

/// Number set members store in their
/// normalized forms. NS ["1.0", "1"] stores as NS ["1"] (the two collapse
/// to one), and NS ["1.0", "2"] stores as NS ["1", "2"] (distinct numbers
/// each normalized); the operation COMPLETED with no failed row. Measured
/// on the service.
#[tokio::test]
async fn number_set_members_store_in_their_normalized_forms() {
    let h = Harness::new().await;
    let rows = [
        r#"{"Item": {"pk": {"S": "a"}, "sk": {"S": "s"}, "ns": {"NS": ["1.0", "1"]}}}"#.to_owned(),
        r#"{"Item": {"pk": {"S": "b"}, "sk": {"S": "s"}, "ns": {"NS": ["1.0", "2"]}}}"#.to_owned(),
    ];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.failed_item_count, 0);
    let a = h.get("a").await.expect("row a written");
    assert_eq!(
        a.get("ns"),
        Some(&AttributeValue::NS(["1".to_owned()].into_iter().collect())),
        "NS [1.0, 1] stores as the single normalized member [1]"
    );
    let b = h.get("b").await.expect("row b written");
    assert_eq!(
        b.get("ns"),
        Some(&AttributeValue::NS(
            ["1".to_owned(), "2".to_owned()].into_iter().collect()
        )),
        "NS [1.0, 2] stores as the normalized members [1, 2]"
    );
}

/// The item size counts a set as submitted, repeated members included,
/// though the set is stored with one copy of each member. Measured on the
/// service: an `SS` of two identical 250,000-byte strings (about 250 KB
/// once deduplicated) fails the row with the item size message, and so
/// does a 410,000-byte string beside `SS ["a", "a"]`. The row holding the
/// string once is ExtendDB's control: the same set within the limit is
/// written.
#[tokio::test]
async fn repeated_set_members_count_toward_the_item_size_as_submitted() {
    let h = Harness::new().await;
    let big = "x".repeat(250_000);
    let wide = "y".repeat(410_000);
    let rows = [
        row(0),
        format!(
            r#"{{"Item": {{"pk": {{"S": "k1"}}, "sk": {{"S": "s"}}, "ss": {{"SS": ["{big}", "{big}"]}}}}}}"#
        ),
        format!(
            r#"{{"Item": {{"pk": {{"S": "k2"}}, "sk": {{"S": "s"}}, "ss": {{"SS": ["{big}"]}}}}}}"#
        ),
        format!(
            r#"{{"Item": {{"pk": {{"S": "k3"}}, "sk": {{"S": "s"}}, "v": {{"S": "{wide}"}}, "ss": {{"SS": ["a", "a"]}}}}}}"#
        ),
        row(4),
    ];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(record.failed_item_count, 2);
    assert!(
        h.get("k1").await.is_none(),
        "the doubled member is counted twice"
    );
    let k2 = h.get("k2").await.expect("the single member is written");
    assert_eq!(
        k2.get("ss"),
        Some(&AttributeValue::SS([big].into_iter().collect()))
    );
    assert!(h.get("k3").await.is_none());
    assert!(h.get("k4").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2);
    for (record, index) in report.iter().zip([1, 3]) {
        assert_eq!(record["errorMessages"], json!([OVERSIZED]));
        assert_eq!(record["itemLocation"]["itemIndex"], json!(index));
        assert!(
            record.get("newImage").is_none(),
            "oversized records carry no newImage"
        );
    }
}

/// A missing-key fault carries the item as `newImage` only when the item as
/// submitted is within the item size limit, the size the service counts: a
/// row with no `sk` whose `SS` holds two identical 250,000-byte strings gets
/// the record without an image, and the same row with the string once gets
/// it with the image.
#[tokio::test]
async fn a_key_fault_over_the_limit_only_through_repeats_carries_no_image() {
    let h = Harness::new().await;
    let big = "x".repeat(250_000);
    let rows = [
        format!(r#"{{"Item": {{"pk": {{"S": "k0"}}, "ss": {{"SS": ["{big}", "{big}"]}}}}}}"#),
        format!(r#"{{"Item": {{"pk": {{"S": "k1"}}, "ss": {{"SS": ["{big}"]}}}}}}"#),
    ];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.failed_item_count, 2);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2);
    for record in &report {
        let message = record["errorMessages"][0].as_str().unwrap_or_default();
        assert!(message.contains("Missing the key sk"), "{message}");
    }
    assert!(
        report[0].get("newImage").is_none(),
        "the submitted item is over the limit"
    );
    assert!(
        report[1].get("newImage").is_some(),
        "the submitted item is within the limit"
    );
}

/// A string or attribute name containing U+0000 fails the row alone with
/// a fixed message; the storage write never sees the row and the rows
/// after it are written.
#[tokio::test]
async fn a_row_with_nul_in_a_string_fails_alone_and_the_file_continues() {
    let h = Harness::new().await;
    let rows = [
        row(0),
        r#"{"Item": {"pk": {"S": "k1"}, "sk": {"S": "s"}, "v": {"S": "a\u0000b"}}}"#.to_owned(),
        r#"{"Item": {"pk": {"S": "k2"}, "sk": {"S": "s"}, "x\u0000y": {"N": "1"}}}"#.to_owned(),
        row(3),
    ];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 4);
    assert_eq!(record.failed_item_count, 2);
    assert_eq!(h.count_items().await, 2);
    assert!(h.get("k1").await.is_none());
    assert!(h.get("k2").await.is_none());
    assert!(
        h.get("k3").await.is_some(),
        "the row after the faults is written"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2);
    for (i, record) in report.iter().enumerate() {
        assert_eq!(
            record["errorMessages"],
            json!([crate::bulk_put::rows::NUL_IN_STRING_MESSAGE])
        );
        assert_eq!(record["itemLocation"]["itemIndex"], json!(i + 1));
    }
}

/// A line that is not valid UTF-8 is not well-formed JSON: the file is
/// abandoned at that line with the parse-level message, and the bytes are
/// never decoded lossily into a written row.
#[tokio::test]
async fn a_line_that_is_not_valid_utf8_abandons_the_file_and_writes_nothing_altered() {
    let h = Harness::new().await;
    let mut bytes = file_of(&[row(0)]);
    bytes.extend_from_slice(br#"{"Item": {"pk": {"S": "k1"}, "sk": {"S": "s"}, "v": {"S": ""#);
    bytes.extend_from_slice(&[0xff, 0xfe]);
    bytes.extend_from_slice(b"\"}}}\n");
    bytes.extend_from_slice(&file_of(&[row(2)]));
    h.write_input("part-000.json", &bytes);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 1);
    assert!(
        h.get("k1").await.is_none(),
        "the undecodable row is not written"
    );
    assert!(
        h.get("k2").await.is_none(),
        "the rest of the file is not read"
    );
    assert_eq!(
        Some(record.processed_input_bytes),
        record.total_input_bytes,
        "ProcessedInputBytes advances to the end of the abandoned file"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::INVALID_UTF8_MESSAGE]),
        "measured on the service"
    );
    assert!(report[0]["itemLocation"].get("itemIndex").is_none());
}
