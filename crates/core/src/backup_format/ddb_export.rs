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
//!
//! `dataFileS3Key` is the object's full key in the bucket, not a path
//! relative to the manifest. For a full export to a prefix the service
//! writes `<s3Prefix>/AWSDynamoDB/<export id>/data/<file>` (measured on
//! `DynamoDB`). The documentation's full-export example differs from that:
//! it has `s3Prefix` `2020-Nov` and keys starting at
//! `AWSDynamoDB/<export id>/data/<file>`, without the prefix (its
//! incremental-export example carries the prefix). The checked-in fixture
//! under `testdata/ddb-export/` copies the documentation's full-export
//! example, so it pins the field set and encodings, not the key shape. An
//! `ExtendDB` writer records the full store key,
//! `<prefix>/data/<file>`, where `<prefix>` is the backup prefix
//! `<account>/<table>/<backup id>` ([`super::BackupArn::store_prefix`]),
//! preceded by the store's configured key prefix when it has one;
//! [`super::BackupDocuments::build`] derives it from the backup ARN and the
//! store's key prefix, once for both manifests. `ImportTable` does not read this manifest (it lists the prefix it
//! is given), so the field is informational; `etag` is optional here because
//! a filesystem store has none.
//!
//! `manifest-summary.json` for an `ExtendDB` backup, which
//! [`super::BackupDocuments::build`] derives from the
//! [`super::ExtenddbManifest`] (the summary has no reader in `ExtendDB`; it
//! is written for tools that read the service's export layout). `<prefix>`
//! below is the backup's key prefix in the store with every leading and
//! trailing `/` dropped ([`trim_key_prefix`]):
//!
//! | member | value |
//! |---|---|
//! | `version` | `2020-06-30` ([`EXPORT_SUMMARY_VERSION`]) |
//! | `exportArn` | the backup ARN (`backup_arn`); there is no export |
//! | `startTime`, `endTime`, `exportTime` | the snapshot instant, `backup_creation_date_time`, in RFC 3339 UTC with millisecond precision (`2025-09-10T10:26:40.123Z`); the backup is a snapshot, so the three instants are one |
//! | `tableArn`, `tableId` | `source_table.table_arn`, `source_table.table_id` |
//! | `s3Bucket` | the bucket name on an S3 store; the root path on a filesystem store |
//! | `s3Prefix` | `<prefix>`: the backup prefix, `<account>/<table>/<backup id>`, preceded by the store's configured key prefix when it has one |
//! | `s3SseAlgorithm` | `null`: the store's encryption is the store's configuration, not the format's; the service writes `AES256` or `aws:kms` |
//! | `s3SseKmsKeyId` | `null` |
//! | `manifestFilesS3Key` | `<prefix>/manifest-files.json`; `manifest-files.json` when `<prefix>` is empty |
//! | `billedSizeBytes` | `backup_size_bytes` (the compressed size; nothing is billed) |
//! | `itemCount` | the sum of the data files' `item_count` |
//! | `outputFormat` | `DYNAMODB_JSON` ([`DYNAMODB_JSON_OUTPUT_FORMAT`]) |
//! | `exportType` | absent (a full export) |

use serde::{Deserialize, Serialize};

use super::FormatError;
use super::layout::{DATA_PREFIX, FILES_MANIFEST_FILE, backup_id_epoch_millis};
use super::manifest::{DataFileEntry, ExtenddbManifest};

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
    /// Server-side encryption algorithm of the written objects (`AES256` or
    /// `aws:kms` from the service); `null` for an `ExtendDB` backup (module
    /// docs). Never skipped: the member is present in every document.
    pub s3_sse_algorithm: Option<String>,
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
    /// The summary an `ExtendDB` backup writes, with the member values the
    /// module docs table gives: `bucket` is the bucket name (S3) or root
    /// path (filesystem), `prefix` the backup's key prefix in the store
    /// (every leading and trailing `/` dropped, [`trim_key_prefix`]). The
    /// instants are
    /// taken from the backup id in `manifest.backup_arn`, which
    /// [`ExtenddbManifest::validate`] has checked equals
    /// `backup_creation_date_time`.
    ///
    /// # Errors
    ///
    /// [`FormatError::InvalidArn`] when `backup_arn` does not parse,
    /// [`FormatError::InvalidManifest`] when its id carries no instant or
    /// the item counts overflow; neither happens for a manifest that passed
    /// [`ExtenddbManifest::validate`] unless its counts sum past `i64::MAX`.
    pub(super) fn for_backup(
        manifest: &ExtenddbManifest,
        bucket: &str,
        prefix: &str,
    ) -> Result<Self, FormatError> {
        let arn = manifest.backup_arn()?;
        let millis = backup_id_epoch_millis(arn.backup_id()).ok_or_else(|| {
            FormatError::InvalidManifest(format!(
                "backup id {:?} does not carry the backup's creation instant",
                arn.backup_id()
            ))
        })?;
        let instant = rfc3339_millis(millis)?;
        let item_count = manifest
            .data_files
            .iter()
            .try_fold(0u64, |sum, f| sum.checked_add(f.item_count))
            .and_then(|sum| i64::try_from(sum).ok())
            .ok_or_else(|| {
                FormatError::InvalidManifest("data file item counts overflow".to_owned())
            })?;
        let prefix = trim_key_prefix(prefix);
        Ok(Self {
            version: EXPORT_SUMMARY_VERSION.to_owned(),
            export_arn: manifest.backup_arn.clone(),
            start_time: instant.clone(),
            end_time: instant.clone(),
            table_arn: manifest.source_table.table_arn.clone(),
            table_id: manifest.source_table.table_id.clone(),
            export_time: instant,
            s3_bucket: bucket.to_owned(),
            s3_prefix: prefix.to_owned(),
            s3_sse_algorithm: None,
            s3_sse_kms_key_id: None,
            manifest_files_s3_key: store_key(prefix, FILES_MANIFEST_FILE),
            billed_size_bytes: manifest.backup_size_bytes,
            item_count,
            output_format: DYNAMODB_JSON_OUTPUT_FORMAT.to_owned(),
            export_type: None,
        })
    }

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
    /// `ETag` of the data file object as the store reported it, without
    /// quotes. The service always writes one; an `ExtendDB` writer omits the
    /// member when the store (filesystem) has no `ETag`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub etag: Option<String>,
    /// Key of the data file object.
    pub data_file_s3_key: String,
}

impl ExportFileEntry {
    /// The `manifest-files.json` line for a data file the
    /// `extenddb-manifest.json` records as `entry`: `itemCount` and
    /// `md5Checksum` are the entry's, so the two manifests cannot disagree;
    /// `dataFileS3Key` is `store_key_prefix` (every leading and trailing
    /// `/` dropped, [`trim_key_prefix`]) followed by `/` and the entry's key
    /// (`<prefix>/data/<file>`, module docs), and `etag` is what the store
    /// reported for the object, `None` on a store without one.
    #[must_use]
    pub(super) fn from_data_file(
        entry: &DataFileEntry,
        store_key_prefix: &str,
        etag: Option<String>,
    ) -> Self {
        debug_assert!(entry.key.starts_with(DATA_PREFIX), "{}", entry.key);
        Self {
            item_count: entry.item_count,
            md5_checksum: entry.md5.clone(),
            etag,
            data_file_s3_key: store_key(store_key_prefix, &entry.key),
        }
    }
}

/// A store key prefix as the export manifests carry it: every leading and
/// trailing `/` dropped, so `a//` is `a`, `/a/` is `a` and `//` is empty.
/// `s3Prefix`, `manifestFilesS3Key` and `dataFileS3Key` all start from this
/// form, and [`super::BackupDocuments::build`] states how it meets the
/// store's own normalization of a configured prefix.
pub(super) fn trim_key_prefix(prefix: &str) -> &str {
    prefix.trim_matches('/')
}

/// The store key of `relative` (a key relative to the backup prefix) under
/// `prefix`: `<prefix>/<relative>` with `prefix` in [`trim_key_prefix`]'s
/// form, and `relative` alone when that is empty. Both export manifests
/// build their keys with it.
fn store_key(prefix: &str, relative: &str) -> String {
    let prefix = trim_key_prefix(prefix);
    if prefix.is_empty() {
        relative.to_owned()
    } else {
        format!("{prefix}/{relative}")
    }
}

/// `millis` since the Unix epoch as RFC 3339 UTC with millisecond precision
/// (`2025-09-10T10:26:40.123Z`), the form the summary manifest's instants
/// take.
fn rfc3339_millis(millis: u64) -> Result<String, FormatError> {
    let nanos = i128::from(millis) * 1_000_000;
    let instant = time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|e| FormatError::InvalidManifest(format!("backup instant {millis} ms: {e}")))?;
    let format = time::format_description::parse(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
    )
    .expect("a constant format description parses");
    instant
        .format(&format)
        .map_err(|e| FormatError::InvalidManifest(format!("backup instant {millis} ms: {e}")))
}

/// Serialize a files manifest: one compact JSON object per line, each line
/// terminated with a newline, matching the JSON lines format the service
/// writes.
///
/// # Errors
///
/// Returns [`FormatError::Json`] when serialization fails.
pub(super) fn write_files_manifest(entries: &[ExportFileEntry]) -> Result<String, FormatError> {
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
            s3_sse_algorithm: Some("AES256".to_owned()),
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

    /// `s3SseAlgorithm` is present as `null` when absent, never skipped,
    /// and parses back.
    #[test]
    fn summary_writes_explicit_null_sse_algorithm() {
        let mut summary = sample_summary();
        summary.s3_sse_algorithm = None;
        let json = summary.to_json().unwrap();
        assert!(json.contains("\"s3SseAlgorithm\":null"), "{json}");
        assert_eq!(ExportSummaryManifest::from_json(&json).unwrap(), summary);
    }

    const V1: &[u8] = include_bytes!("../../testdata/extenddb-manifest/v1.json");

    /// Every member of the summary an `ExtendDB` backup writes is the value
    /// the module docs table gives, derived from the manifest.
    #[test]
    fn summary_for_backup_follows_the_documented_table() {
        let manifest = ExtenddbManifest::from_json_bytes(V1).unwrap();
        let summary = ExportSummaryManifest::for_backup(
            &manifest,
            "backups-bucket",
            "123456789012/Music/01757500000123-0badcafe/",
        )
        .unwrap();
        assert_eq!(summary.version, "2020-06-30");
        assert_eq!(summary.export_arn, manifest.backup_arn);
        // 1757500000.123 s is 2025-09-10T10:26:40.123Z.
        assert_eq!(summary.start_time, "2025-09-10T10:26:40.123Z");
        assert_eq!(summary.end_time, summary.start_time);
        assert_eq!(summary.export_time, summary.start_time);
        assert_eq!(summary.table_arn, manifest.source_table.table_arn);
        assert_eq!(summary.table_id, manifest.source_table.table_id);
        assert_eq!(summary.s3_bucket, "backups-bucket");
        assert_eq!(
            summary.s3_prefix,
            "123456789012/Music/01757500000123-0badcafe"
        );
        assert_eq!(summary.s3_sse_algorithm, None);
        assert_eq!(summary.s3_sse_kms_key_id, None);
        assert_eq!(
            summary.manifest_files_s3_key,
            "123456789012/Music/01757500000123-0badcafe/manifest-files.json"
        );
        assert_eq!(summary.billed_size_bytes, manifest.backup_size_bytes);
        assert_eq!(summary.billed_size_bytes, 61_828);
        assert_eq!(summary.item_count, 100_000);
        assert_eq!(
            u64::try_from(summary.item_count).unwrap(),
            manifest
                .data_files
                .iter()
                .map(|f| f.item_count)
                .sum::<u64>()
        );
        assert_eq!(summary.output_format, "DYNAMODB_JSON");
        assert_eq!(summary.export_type, None);
        // Without a trailing slash on the prefix the keys are the same.
        let again = ExportSummaryManifest::for_backup(
            &manifest,
            "backups-bucket",
            "123456789012/Music/01757500000123-0badcafe",
        )
        .unwrap();
        assert_eq!(again, summary);
        // An empty prefix (a store rooted at the backup) gives the bare
        // file name, as `ExportFileEntry::from_data_file` gives the bare
        // data file key, not a key with a leading `/`.
        for empty in ["", "/"] {
            let bare =
                ExportSummaryManifest::for_backup(&manifest, "backups-bucket", empty).unwrap();
            assert_eq!(bare.s3_prefix, "");
            assert_eq!(bare.manifest_files_s3_key, "manifest-files.json");
            let line = ExportFileEntry::from_data_file(&manifest.data_files[0], empty, None);
            assert_eq!(line.data_file_s3_key, manifest.data_files[0].key);
        }
        // The written document has every member, `exportType` excepted.
        let json = summary.to_json().unwrap();
        assert!(json.contains("\"s3SseAlgorithm\":null"), "{json}");
        assert!(!json.contains("exportType"), "{json}");
        assert_eq!(ExportSummaryManifest::from_json(&json).unwrap(), summary);
    }

    /// The instant formatter: whole seconds keep three fraction digits, and
    /// the epoch and a far instant format.
    #[test]
    fn rfc3339_millis_has_millisecond_precision() {
        assert_eq!(rfc3339_millis(0).unwrap(), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            rfc3339_millis(1_757_500_000_000).unwrap(),
            "2025-09-10T10:26:40.000Z"
        );
        assert_eq!(
            rfc3339_millis(1_757_500_000_123).unwrap(),
            "2025-09-10T10:26:40.123Z"
        );
        assert_eq!(
            rfc3339_millis(99_999_999_999_999).unwrap(),
            "5138-11-16T09:46:39.999Z"
        );
    }

    /// The `manifest-files.json` line is derived from the manifest's entry,
    /// so `md5Checksum` and `itemCount` agree with `md5` and `item_count` by
    /// construction, and `dataFileS3Key` is the full store key.
    #[test]
    fn file_entry_from_data_file_agrees_with_the_manifest_entry() {
        let manifest = ExtenddbManifest::from_json_bytes(V1).unwrap();
        let prefix = "123456789012/Music/01757500000123-0badcafe/";
        for (entry, etag) in manifest
            .data_files
            .iter()
            .zip([Some("etag-1".to_owned()), None])
        {
            let line = ExportFileEntry::from_data_file(entry, prefix, etag.clone());
            assert_eq!(line.item_count, entry.item_count);
            assert_eq!(line.md5_checksum, entry.md5);
            assert_eq!(line.etag, etag);
            assert_eq!(
                line.data_file_s3_key,
                format!("123456789012/Music/01757500000123-0badcafe/{}", entry.key)
            );
            assert!(line.data_file_s3_key.ends_with(".json.gz"));
            // The same key whether or not the prefix ends in `/`, and with a
            // store key prefix in front of the backup prefix.
            let no_slash =
                ExportFileEntry::from_data_file(entry, prefix.trim_end_matches('/'), None);
            assert_eq!(no_slash.data_file_s3_key, line.data_file_s3_key);
            let rooted = ExportFileEntry::from_data_file(entry, &format!("root/{prefix}"), None);
            assert_eq!(
                rooted.data_file_s3_key,
                format!("root/{}", line.data_file_s3_key)
            );
            let bare = ExportFileEntry::from_data_file(entry, "", None);
            assert_eq!(bare.data_file_s3_key, entry.key);
        }
        // The counts the two manifests carry are the same numbers.
        let lines: Vec<ExportFileEntry> = manifest
            .data_files
            .iter()
            .map(|e| ExportFileEntry::from_data_file(e, prefix, None))
            .collect();
        let summary = ExportSummaryManifest::for_backup(&manifest, "b", prefix).unwrap();
        assert_eq!(
            lines.iter().map(|l| l.item_count).sum::<u64>(),
            u64::try_from(summary.item_count).unwrap()
        );
        let text = write_files_manifest(&lines).unwrap();
        assert_eq!(parse_files_manifest(&text).unwrap(), lines);
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
            etag: Some("af83d6f217c19b8b0fff8023d8ca4716-1".to_owned()),
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
                etag: Some("af83d6f217c19b8b0fff8023d8ca4716-1".to_owned()),
                data_file_s3_key: "data/000001.json.gz".to_owned(),
            },
            ExportFileEntry {
                item_count: 0,
                md5_checksum: "1B2M2Y8AsgTpgAmY7PhCfg==".to_owned(),
                etag: Some("d41d8cd98f00b204e9800998ecf8427e".to_owned()),
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

    /// Every leading and trailing `/` of the prefix is dropped once, in one
    /// place, so `s3Prefix`, `manifestFilesS3Key` and `dataFileS3Key` start
    /// from the same prefix: `a//` is `a`, `/a/` is `a`, and `//` is the
    /// empty prefix.
    #[test]
    fn prefix_with_repeated_trailing_slashes_gives_one_prefix_everywhere() {
        let manifest = ExtenddbManifest::from_json_bytes(V1).unwrap();
        let entry = &manifest.data_files[0];
        for (prefix, trimmed) in [
            ("a//", "a"),
            ("a/b///", "a/b"),
            ("//", ""),
            ("a", "a"),
            ("/a/", "a"),
            ("//a/b", "a/b"),
            ("/", ""),
        ] {
            let summary = ExportSummaryManifest::for_backup(&manifest, "b", prefix).unwrap();
            let line = ExportFileEntry::from_data_file(entry, prefix, None);
            assert_eq!(summary.s3_prefix, trimmed, "{prefix}");
            let (files_key, data_key) = if trimmed.is_empty() {
                (FILES_MANIFEST_FILE.to_owned(), entry.key.clone())
            } else {
                (
                    format!("{trimmed}/{FILES_MANIFEST_FILE}"),
                    format!("{trimmed}/{}", entry.key),
                )
            };
            assert_eq!(summary.manifest_files_s3_key, files_key, "{prefix}");
            assert_eq!(line.data_file_s3_key, data_key, "{prefix}");
        }
    }
}
