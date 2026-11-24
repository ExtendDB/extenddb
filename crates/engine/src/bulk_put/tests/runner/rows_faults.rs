// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row faults measured on the hosted service, driven end to end
//! through the runner: the key texts a bad key attribute answers, the file
//! abandon a record fault produces, and the two item shapes the service
//! accepts.

use super::*;

/// A row whose hash key is a `NULL` value.
fn row_null_hash_key() -> String {
    "{\"Item\": {\"pk\": {\"NULL\": true}, \"sk\": {\"S\": \"s\"}}}".to_owned()
}

/// A row whose hash key is an empty string.
fn row_empty_hash_key() -> String {
    "{\"Item\": {\"pk\": {\"S\": \"\"}, \"sk\": {\"S\": \"s\"}}}".to_owned()
}

/// A row whose sort key carries an `N` value that is not a number.
fn row_non_numeric_sort_key() -> String {
    "{\"Item\": {\"pk\": {\"S\": \"k1\"}, \"sk\": {\"N\": \"not-a-number\"}}}".to_owned()
}

// A NULL hash key fails the row alone with the type-mismatch text
// and the file continues.
#[tokio::test]
async fn a_null_hash_key_fails_the_row_with_the_type_mismatch_text() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_null_hash_key(), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k0").await.is_some());
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([
            "One or more parameter values were invalid: Type mismatch for key pk expected: S \
             actual: NULL"
        ])
    );
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(1));
}

// An empty-string hash key fails the row alone with the key text,
// and the record carries the submitted image.
#[tokio::test]
async fn an_empty_string_hash_key_fails_the_row_with_the_key_text_and_the_image() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_empty_hash_key(), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([
            "One or more parameter values are not valid. The AttributeValue for a key attribute \
             cannot contain an empty string value. Key: pk"
        ])
    );
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(1));
    assert_eq!(report[0]["newImage"]["Item"]["sk"], json!({"S": "s"}));
}

// The non-numeric sort key abandons the file. The rows before it
// are written and counted, the abandon counts one failed row with no item
// index, the rows after it are never read, and the manifest marks the file
// partially processed up to the row before it.
#[tokio::test]
async fn a_non_numeric_sort_key_abandons_the_file_and_keeps_the_rows_before_it() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_non_numeric_sort_key(), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k0").await.is_some());
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
        json!([crate::bulk_put::rows::INVALID_NUMERIC_MESSAGE])
    );
    assert!(report[0]["itemLocation"].get("itemIndex").is_none());
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 0]]));
}

// The oversized values map abandons the file the same way, with
// its own message, and the rows before it are written.
#[tokio::test]
async fn an_oversized_expression_values_map_abandons_the_file() {
    let h = Harness::new().await;
    let value = "a".repeat(crate::bulk_put::rows::MAX_EXPRESSION_VALUES_BYTES + 100);
    let oversized = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"k1\"}}, \"sk\": {{\"S\": \"s\"}}}}, \
         \"ConditionExpression\": \"#v = :v\", \
         \"ExpressionAttributeNames\": {{\"#v\": \"v\"}}, \
         \"ExpressionAttributeValues\": {{\":v\": {{\"S\": \"{value}\"}}}}}}"
    );
    h.write_input("part-000.json", &file_of(&[row(0), oversized, row(2)]));
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
        json!([crate::bulk_put::rows::VALUES_TOO_LARGE_MESSAGE])
    );
    assert!(report[0]["itemLocation"].get("itemIndex").is_none());
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 0]]));
}

// An item of 400 attributes and an attribute name of 256
// bytes are written, with no failure. No ExtendDB bound refuses either.
#[tokio::test]
async fn an_item_of_four_hundred_attributes_and_a_long_attribute_name_are_written() {
    let h = Harness::new().await;
    let mut wide = String::from("{\"Item\": {\"pk\": {\"S\": \"k0\"}, \"sk\": {\"S\": \"s\"}");
    for index in 0..398 {
        wide.push_str(&format!(", \"a{index}\": {{\"S\": \"v\"}}"));
    }
    wide.push_str("}}");
    let long_name = "n".repeat(256);
    let named = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"k1\"}}, \"sk\": {{\"S\": \"s\"}}, \
         \"{long_name}\": {{\"S\": \"v\"}}}}}}"
    );
    h.write_input("part-000.json", &file_of(&[wide, named]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 0);
    let written = h.get("k0").await.expect("the wide row is written");
    assert_eq!(written.len(), 400);
    let named = h.get("k1").await.expect("the long-name row is written");
    assert!(named.contains_key(&long_name));
}

/// One byte over the attribute name limit.
const LONG_NAME_BYTES: usize = 65_536;

/// The `PutItem` text for an item attribute name over the limit.
const NAME_TOO_LARGE: &str =
    "1 validation error detected: Attribute name is too large, must be less than 65536 bytes";

// A row whose condition maps `#n` to a 65,536-byte name fails with the text
// `PutItem` answers for the same condition, and is not written.
#[tokio::test]
async fn a_condition_name_over_the_limit_fails_the_row_with_the_put_item_text() {
    let h = Harness::new().await;
    let long_name = "n".repeat(LONG_NAME_BYTES);
    let named = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"c\"}}, \"sk\": {{\"S\": \"s\"}}}}, \
         \"ConditionExpression\": \"attribute_not_exists(#n)\", \
         \"ExpressionAttributeNames\": {{\"#n\": \"{long_name}\"}}}}"
    );
    h.write_input("part-000.json", &file_of(&[row(0), named, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(
        h.get("c").await.is_none(),
        "the conditional row is not written"
    );
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([format!("{NAME_TOO_LARGE} for key #n")])
    );
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(1));
}

// A row whose only fault is a 65,536-byte attribute name, and a row with
// that name and no `sk`, get the same record: the `PutItem` name text and
// no image. The name check runs ahead of the key checks, so the second row
// does not report its missing key.
#[tokio::test]
async fn a_long_attribute_name_fails_the_row_ahead_of_a_missing_key() {
    let h = Harness::new().await;
    let long_name = "n".repeat(LONG_NAME_BYTES);
    let named = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"k1\"}}, \"sk\": {{\"S\": \"s\"}}, \
         \"{long_name}\": {{\"S\": \"v\"}}}}}}"
    );
    let named_without_sk =
        format!("{{\"Item\": {{\"pk\": {{\"S\": \"k2\"}}, \"{long_name}\": {{\"S\": \"v\"}}}}}}");
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), named, named_without_sk, row(3)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 4);
    assert_eq!(record.failed_item_count, 2);
    assert!(
        h.get("k1").await.is_none(),
        "the long-name row is not written"
    );
    assert!(h.get("k3").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2);
    for (entry, item_index) in report.iter().zip([1, 2]) {
        assert_eq!(entry["errorMessages"], json!([NAME_TOO_LARGE]));
        assert_eq!(entry["itemLocation"]["itemIndex"], json!(item_index));
        assert!(
            entry.get("newImage").is_none(),
            "row {item_index}: the name fault carries no image"
        );
    }
}

/// A row against key `c` whose condition is `attribute_not_exists(pk)`
/// padded with spaces to `len` bytes.
fn row_with_condition_of(len: usize) -> String {
    let base = "attribute_not_exists(pk)";
    let expression = format!("{base}{}", " ".repeat(len - base.len()));
    assert_eq!(expression.len(), len);
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"c\"}}, \"sk\": {{\"S\": \"s\"}}}}, \
         \"ConditionExpression\": \"{expression}\"}}"
    )
}

//  (measured on the service): the condition text alone is bound
// at 4,096 bytes on the bulk path as on `PutItem`; 4,096 is written and
// 4,097 fails the row with the size text and the byte count. The record
// carries the message without the `Invalid ConditionExpression: ` prefix
// the single-item path puts on it.
#[tokio::test]
async fn a_condition_over_four_kib_fails_the_row_with_the_unprefixed_size_text() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_with_condition_of(4097), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(
        h.get("c").await.is_none(),
        "the conditional row is not written"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!(["Expression size has exceeded the maximum allowed size; expression size: 4097"])
    );
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(1));
}

#[tokio::test]
async fn a_condition_of_exactly_four_kib_is_written() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&[row_with_condition_of(4096)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED", "a condition of 4,096 bytes");
    assert_eq!(record.failed_item_count, 0);
    assert!(h.get("c").await.is_some());
}

//  (measured on the service): a syntax error in the condition fails the
// row with the parser text and no prefix. The hosted parser's text is
// `Syntax error; token: "=", near: "= = :v"`; the shared parser words its
// syntax faults differently on every path, which is outside this feature,
// so the test holds the prefix rule and not the wording.
#[tokio::test]
async fn a_condition_syntax_error_fails_the_row_with_the_unprefixed_parser_text() {
    let h = Harness::new().await;
    let bad = "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}}, \
               \"ConditionExpression\": \"attribute_not_exists(pk) OR #v = = :v\", \
               \"ExpressionAttributeNames\": {\"#v\": \"v\"}, \
               \"ExpressionAttributeValues\": {\":v\": {\"S\": \"x\"}}}"
        .to_owned();
    h.write_input("part-000.json", &file_of(&[row(0), bad, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failed_item_count, 1);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    let names = std::collections::HashMap::from([("#v".to_owned(), "v".to_owned())]);
    let values =
        std::collections::HashMap::from([(":v".to_owned(), AttributeValue::S("x".to_owned()))]);
    let put_item = resolve_condition(
        Some("attribute_not_exists(pk) OR #v = = :v"),
        Some(&names),
        Some(&values),
        None,
        None,
        &LimitsConfig::default(),
    )
    .expect_err("the condition expression is invalid")
    .to_string();
    let expected = put_item
        .strip_prefix("Invalid ConditionExpression: ")
        .expect("the single-item path prefixes the condition fault");
    assert_eq!(report[0]["errorMessages"], json!([expected]));
}
