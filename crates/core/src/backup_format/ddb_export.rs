// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The two manifests Amazon `DynamoDB` writes for `ExportTableToPointInTime`
//! with `DYNAMODB_JSON` output: `manifest-summary.json` (one JSON document)
//! and `manifest-files.json` (JSON lines, one object per data file).
//!
//! Field sets and encodings follow the service's documented output format:
//! <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html>.
//! Notably: `version` is the string `"2020-06-30"` for full exports,
//! timestamps are ISO 8601 strings, `s3SseKmsKeyId` is serialized as an
//! explicit `null` when absent, `exportType` appears only on incremental
//! exports, and each files-manifest line carries `itemCount`, `md5Checksum`
//! (base64 of the data file's MD5), `etag`, and `dataFileS3Key`.

use serde::{Deserialize, Serialize};

use super::FormatError;

/// The `version` value the service writes for full exports.
pub const EXPORT_SUMMARY_VERSION: &str = "2020-06-30";

/// The `outputFormat` value for `DynamoDB` JSON exports.
pub const DYNAMODB_JSON_OUTPUT_FORMAT: &str = "DYNAMODB_JSON";

/// The `manifest-summary.json` document.
///
/// Unknown fields are tolerated so a newer service revision (for example the
/// incremental-export `exportFromTime`/`exportToTime`/`outputView` members)
/// still parses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportSummaryManifest {
    /// Manifest schema version, `"2020-06-30"` for full exports.
    pub version: String,
    /// ARN of the export.
    pub export_arn: String,
    /// When the export started, ISO 8601.
    pub start_time: String,
    /// When the export finished, ISO 8601.
    pub end_time: String,
    /// ARN of the exported table.
    pub table_arn: String,
    /// Unique id of the exported table.
    pub table_id: String,
    /// Point in time the export represents, ISO 8601.
    pub export_time: String,
    /// Destination bucket.
    pub s3_bucket: String,
    /// Destination prefix.
    pub s3_prefix: String,
    /// Server-side encryption algorithm of the written objects.
    pub s3_sse_algorithm: String,
    /// KMS key id when SSE-KMS was used; the service writes an explicit
    /// `null` otherwise, so this member is never skipped.
    pub s3_sse_kms_key_id: Option<String>,
    /// Key of the `manifest-files.json` object.
    pub manifest_files_s3_key: String,
    /// Billed size of the export.
    pub billed_size_bytes: i64,
    /// Total item count across all data files.
    pub item_count: i64,
    /// Output format, `"DYNAMODB_JSON"` here.
    pub output_format: String,
    /// Present only for incremental exports, where the service writes
    /// `"INCREMENTAL_EXPORT"`; absent for full exports.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub export_type: Option<String>,
}

impl ExportSummaryManifest {
    /// Serialize to the compact JSON document the file holds.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when serialization fails.
    pub fn to_json(&self) -> Result<String, FormatError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Parse a `manifest-summary.json` document.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Json`] when the document does not parse.
    pub fn from_json(s: &str) -> Result<Self, FormatError> {
        Ok(serde_json::from_str(s)?)
    }
}

/// One line of `manifest-files.json`: a single exported data file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportFileEntry {
    /// Number of items in the data file.
    pub item_count: u64,
    /// Base64 MD5 of the data file's bytes.
    pub md5_checksum: String,
    /// S3 `ETag` of the data file object.
    pub etag: String,
    /// Key of the data file object.
    pub data_file_s3_key: String,
}

/// Serialize a files manifest: one compact JSON object per line, each line
/// terminated with a newline, matching the JSON lines format the service
/// writes.
///
/// # Errors
///
/// Returns [`FormatError::Json`] when serialization fails.
pub fn write_files_manifest(entries: &[ExportFileEntry]) -> Result<String, FormatError> {
    let mut out = String::new();
    for entry in entries {
        out.push_str(&serde_json::to_string(entry)?);
        out.push('\n');
    }
    Ok(out)
}

/// Parse a `manifest-files.json` document (JSON lines). Blank lines are
/// tolerated, including the absence of a trailing newline.
///
/// # Errors
///
/// Returns [`FormatError::Json`] when any non-blank line does not parse.
pub fn parse_files_manifest(s: &str) -> Result<Vec<ExportFileEntry>, FormatError> {
    let mut entries = Vec::new();
    for line in s.lines() {
        if line.trim().is_empty() {
            continue;
        }
        entries.push(serde_json::from_str(line)?);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_summary() -> ExportSummaryManifest {
        ExportSummaryManifest {
            version: EXPORT_SUMMARY_VERSION.to_owned(),
            export_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/ProductCatalog/export/01234567890123-a1b2c3d4".to_owned(),
            start_time: "2020-11-04T07:28:34.028Z".to_owned(),
            end_time: "2020-11-04T07:33:43.897Z".to_owned(),
            table_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/ProductCatalog".to_owned(),
            table_id: "12345a12-abcd-123a-ab12-1234abc12345".to_owned(),
            export_time: "2020-11-04T07:28:34.028Z".to_owned(),
            s3_bucket: "ddb-productcatalog-export".to_owned(),
            s3_prefix: "2020-Nov".to_owned(),
            s3_sse_algorithm: "AES256".to_owned(),
            s3_sse_kms_key_id: None,
            manifest_files_s3_key: "AWSDynamoDB/01693685827463-2d8752fd/manifest-files.json".to_owned(),
            billed_size_bytes: 0,
            item_count: 8,
            output_format: DYNAMODB_JSON_OUTPUT_FORMAT.to_owned(),
            export_type: None,
        }
    }

    #[test]
    fn summary_serde_round_trip() {
        let summary = sample_summary();
        let json = summary.to_json().unwrap();
        let parsed = ExportSummaryManifest::from_json(&json).unwrap();
        assert_eq!(parsed, summary);
    }

    #[test]
    fn summary_uses_documented_member_names() {
        let json = sample_summary().to_json().unwrap();
        for member in [
            "\"version\"",
            "\"exportArn\"",
            "\"startTime\"",
            "\"endTime\"",
            "\"tableArn\"",
            "\"tableId\"",
            "\"exportTime\"",
            "\"s3Bucket\"",
            "\"s3Prefix\"",
            "\"s3SseAlgorithm\"",
            "\"s3SseKmsKeyId\"",
            "\"manifestFilesS3Key\"",
            "\"billedSizeBytes\"",
            "\"itemCount\"",
            "\"outputFormat\"",
        ] {
            assert!(json.contains(member), "missing member {member} in {json}");
        }
    }

    #[test]
    fn summary_writes_explicit_null_kms_key() {
        let json = sample_summary().to_json().unwrap();
        assert!(json.contains("\"s3SseKmsKeyId\":null"));
    }

    #[test]
    fn summary_omits_export_type_for_full_exports() {
        let json = sample_summary().to_json().unwrap();
        assert!(!json.contains("exportType"));
    }

    #[test]
    fn summary_carries_export_type_for_incremental_exports() {
        let mut summary = sample_summary();
        summary.export_type = Some("INCREMENTAL_EXPORT".to_owned());
        let json = summary.to_json().unwrap();
        assert!(json.contains("\"exportType\":\"INCREMENTAL_EXPORT\""));
        let parsed = ExportSummaryManifest::from_json(&json).unwrap();
        assert_eq!(parsed, summary);
    }

    #[test]
    fn summary_parses_documented_example() {
        // The full-export example from the documented output format.
        let doc = r#"{
   "version": "2020-06-30",
   "exportArn": "arn:aws:dynamodb:us-east-1:123456789012:table/ProductCatalog/export/01234567890123-a1b2c3d4",
   "startTime": "2020-11-04T07:28:34.028Z",
   "endTime": "2020-11-04T07:33:43.897Z",
   "tableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/ProductCatalog",
   "tableId": "12345a12-abcd-123a-ab12-1234abc12345",
   "exportTime": "2020-11-04T07:28:34.028Z",
   "s3Bucket": "ddb-productcatalog-export",
   "s3Prefix": "2020-Nov",
   "s3SseAlgorithm": "AES256",
   "s3SseKmsKeyId": null,
   "manifestFilesS3Key": "AWSDynamoDB/01693685827463-2d8752fd/manifest-files.json",
   "billedSizeBytes": 0,
   "itemCount": 8,
   "outputFormat": "DYNAMODB_JSON"
}"#;
        let parsed = ExportSummaryManifest::from_json(doc).unwrap();
        assert_eq!(parsed, sample_summary());
    }

    #[test]
    fn summary_tolerates_incremental_only_members() {
        // Incremental exports add members full-export readers do not model.
        let doc = r#"{
 "version": "2023-08-01",
 "exportArn": "arn:aws:dynamodb:us-east-1:123456789012:table/t/export/x",
 "startTime": "2023-09-19T04:20:18.000Z",
 "endTime": "2023-09-19T04:40:24.780Z",
 "tableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/t",
 "tableId": "b116b490-6460-4d4a-9a6b-5d360abf4fb3",
 "exportFromTime": "2023-09-18T17:00:00.000Z",
 "exportToTime": "2023-09-19T04:00:00.000Z",
 "exportTime": "2023-09-19T04:00:00.000Z",
 "s3Bucket": "b",
 "s3Prefix": "p",
 "s3SseAlgorithm": "AES256",
 "s3SseKmsKeyId": null,
 "manifestFilesS3Key": "p/AWSDynamoDB/x/manifest-files.json",
 "billedSizeBytes": 20901239349,
 "itemCount": 169928274,
 "outputFormat": "DYNAMODB_JSON",
 "outputView": "NEW_AND_OLD_IMAGES",
 "exportType": "INCREMENTAL_EXPORT"
}"#;
        let parsed = ExportSummaryManifest::from_json(doc).unwrap();
        assert_eq!(parsed.export_type.as_deref(), Some("INCREMENTAL_EXPORT"));
        assert_eq!(parsed.item_count, 169_928_274);
    }

    #[test]
    fn file_entry_serde_round_trip() {
        let entry = ExportFileEntry {
            item_count: 8,
            md5_checksum: "sQMSpEILNgoQmarvDFonGQ==".to_owned(),
            etag: "af83d6f217c19b8b0fff8023d8ca4716-1".to_owned(),
            data_file_s3_key: "AWSDynamoDB/01693685827463-2d8752fd/data/asdl123dasas.json.gz"
                .to_owned(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: ExportFileEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, entry);
    }

    #[test]
    fn files_manifest_write_parse_round_trip() {
        let entries = vec![
            ExportFileEntry {
                item_count: 8,
                md5_checksum: "sQMSpEILNgoQmarvDFonGQ==".to_owned(),
                etag: "af83d6f217c19b8b0fff8023d8ca4716-1".to_owned(),
                data_file_s3_key: "data/000001.json.gz".to_owned(),
            },
            ExportFileEntry {
                item_count: 0,
                md5_checksum: "1B2M2Y8AsgTpgAmY7PhCfg==".to_owned(),
                etag: "d41d8cd98f00b204e9800998ecf8427e".to_owned(),
                data_file_s3_key: "data/000002.json.gz".to_owned(),
            },
        ];
        let text = write_files_manifest(&entries).unwrap();
        assert_eq!(text.lines().count(), 2);
        assert!(text.ends_with('\n'));
        let parsed = parse_files_manifest(&text).unwrap();
        assert_eq!(parsed, entries);
    }

    #[test]
    fn files_manifest_parses_documented_example_line() {
        // The files-manifest example from the documented output format,
        // reflowed onto one line as the service writes it.
        let line = r#"{"itemCount":8,"md5Checksum":"sQMSpEILNgoQmarvDFonGQ==","etag":"af83d6f217c19b8b0fff8023d8ca4716-1","dataFileS3Key":"AWSDynamoDB/01693685827463-2d8752fd/data/asdl123dasas.json.gz"}"#;
        let parsed = parse_files_manifest(line).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].item_count, 8);
        assert_eq!(parsed[0].md5_checksum, "sQMSpEILNgoQmarvDFonGQ==");
        // Writing back reproduces the line byte for byte, plus the newline.
        let written = write_files_manifest(&parsed).unwrap();
        assert_eq!(written, format!("{line}\n"));
    }

    #[test]
    fn files_manifest_tolerates_blank_lines() {
        let text =
            "\n{\"itemCount\":1,\"md5Checksum\":\"m\",\"etag\":\"e\",\"dataFileS3Key\":\"k\"}\n\n";
        let parsed = parse_files_manifest(text).unwrap();
        assert_eq!(parsed.len(), 1);
    }
}
