// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Types for `ImportTable` and `ExportTableToPointInTime` operations.
//!
//! extenddb uses `FileSource` (local filesystem) instead of `S3BucketSource`.
//! Import reads from a local path; export writes to a local path.

use serde::{Deserialize, Serialize};

use super::{AttributeDefinition, BillingMode, GsiInput, KeySchemaElement, ProvisionedThroughput};

/// Input format for import operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputFormat {
    /// `DynamoDB` JSON format (`{"Item": {"pk": {"S": "val"}, ...}}`).
    #[serde(rename = "DYNAMODB_JSON")]
    DynamoDbJson,
    /// Amazon Ion text format.
    #[serde(rename = "ION")]
    Ion,
    /// Comma-separated values.
    #[serde(rename = "CSV")]
    Csv,
}

/// Export format (CSV is not supported for export).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportFormat {
    /// `DynamoDB` JSON format.
    #[serde(rename = "DYNAMODB_JSON")]
    DynamoDbJson,
    /// Amazon Ion text format.
    #[serde(rename = "ION")]
    Ion,
}

/// CSV-specific import options.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CsvOptions {
    /// Column delimiter (default: comma).
    #[serde(rename = "Delimiter", default = "default_csv_delimiter")]
    pub delimiter: String,
    /// Column header names. If absent, the first row is used as headers.
    #[serde(rename = "HeaderList", skip_serializing_if = "Option::is_none")]
    pub header_list: Option<Vec<String>>,
}

fn default_csv_delimiter() -> String {
    ",".to_owned()
}

/// Format-specific import options.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InputFormatOptions {
    /// CSV options (only relevant when `InputFormat` is `CSV`).
    #[serde(rename = "Csv", skip_serializing_if = "Option::is_none")]
    pub csv: Option<CsvOptions>,
}

/// extenddb-specific file source (replaces `S3BucketSource`).
///
/// Points to a local filesystem path containing the data to import.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileSource {
    /// Local filesystem path to the import data file or directory.
    #[serde(rename = "Path")]
    pub path: String,
}

/// Table creation parameters for import.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableCreationParameters {
    /// Table name.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Attribute definitions.
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,
    /// Key schema.
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    /// Billing mode.
    #[serde(rename = "BillingMode", skip_serializing_if = "Option::is_none")]
    pub billing_mode: Option<BillingMode>,
    /// Provisioned throughput.
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    /// Global secondary indexes.
    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_secondary_indexes: Option<Vec<GsiInput>>,
}

/// Input for `ImportTable`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportTableInput {
    /// extenddb-specific: local filesystem source (replaces `S3BucketSource`).
    #[serde(rename = "FileSource")]
    pub file_source: FileSource,
    /// Format of the source data.
    #[serde(rename = "InputFormat")]
    pub input_format: InputFormat,
    /// Format-specific options.
    #[serde(rename = "InputFormatOptions", skip_serializing_if = "Option::is_none")]
    pub input_format_options: Option<InputFormatOptions>,
    /// Table creation parameters.
    #[serde(rename = "TableCreationParameters")]
    pub table_creation_parameters: TableCreationParameters,
}

/// Import status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImportStatus {
    /// Import is in progress.
    #[serde(rename = "IN_PROGRESS")]
    InProgress,
    /// Import completed successfully.
    #[serde(rename = "COMPLETED")]
    Completed,
    /// Import failed.
    #[serde(rename = "FAILED")]
    Failed,
}

/// Description of an import operation.
#[derive(Debug, Clone, Serialize)]
pub struct ImportTableDescription {
    /// ARN of the import.
    #[serde(rename = "ImportArn")]
    pub import_arn: String,
    /// Current status.
    #[serde(rename = "ImportStatus")]
    pub import_status: ImportStatus,
    /// ARN of the target table.
    #[serde(rename = "TableArn")]
    pub table_arn: String,
    /// Table ID.
    #[serde(rename = "TableId", skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,
    /// Source file path.
    #[serde(rename = "FileSource")]
    pub file_source: FileSource,
    /// Input format.
    #[serde(rename = "InputFormat")]
    pub input_format: InputFormat,
    /// Table creation parameters.
    #[serde(rename = "TableCreationParameters")]
    pub table_creation_parameters: TableCreationParameters,
    /// Number of errors.
    #[serde(rename = "ErrorCount")]
    pub error_count: i64,
    /// Number of items processed.
    #[serde(rename = "ProcessedItemCount")]
    pub processed_item_count: i64,
    /// Number of items imported.
    #[serde(rename = "ImportedItemCount")]
    pub imported_item_count: i64,
    /// Start time (epoch seconds).
    #[serde(rename = "StartTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<f64>,
    /// End time (epoch seconds).
    #[serde(rename = "EndTime", skip_serializing_if = "Option::is_none")]
    pub end_time: Option<f64>,
    /// Failure code (if failed).
    #[serde(rename = "FailureCode", skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    /// Failure message (if failed).
    #[serde(rename = "FailureMessage", skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
}

/// Output for `ImportTable`.
#[derive(Debug, Clone, Serialize)]
pub struct ImportTableOutput {
    /// Description of the import.
    #[serde(rename = "ImportTableDescription")]
    pub import_table_description: ImportTableDescription,
}

/// Input for `ExportTableToPointInTime`.
///
/// Accepts both the extenddb-specific `FilePath` field and the standard `DynamoDB`
/// fields (`S3Bucket`, `S3Prefix`, `ClientToken`, etc.). When `S3Prefix` is
/// provided, it is used as the local filesystem path. `S3Bucket`,
/// `S3BucketOwner`, `ExportTime`, and `ClientToken` are accepted but ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct ExportTableToPointInTimeInput {
    /// ARN of the table to export.
    #[serde(rename = "TableArn")]
    pub table_arn: String,
    /// extenddb-specific: local filesystem path to write export data.
    #[serde(rename = "FilePath")]
    file_path: Option<String>,
    /// Standard `DynamoDB` field — used as file path when `FilePath` is absent.
    #[serde(rename = "S3Prefix")]
    s3_prefix: Option<String>,
    /// Standard `DynamoDB` field — accepted but ignored.
    #[serde(rename = "S3Bucket")]
    _s3_bucket: Option<String>,
    /// Standard `DynamoDB` field — accepted but ignored.
    #[serde(rename = "S3BucketOwner")]
    _s3_bucket_owner: Option<String>,
    /// Standard `DynamoDB` field — accepted but ignored.
    #[serde(rename = "ExportTime")]
    _export_time: Option<f64>,
    /// Standard `DynamoDB` field — accepted but ignored.
    #[serde(rename = "ClientToken")]
    _client_token: Option<String>,
    /// Standard `DynamoDB` field — accepted but ignored.
    #[serde(rename = "ExportType")]
    _export_type: Option<String>,
    /// Standard `DynamoDB` field — accepted but ignored.
    #[serde(rename = "IncrementalExportSpecification")]
    _incremental_export_spec: Option<serde_json::Value>,
    /// Export format (default: `DYNAMODB_JSON`).
    #[serde(rename = "ExportFormat")]
    pub export_format: Option<ExportFormat>,
}

impl ExportTableToPointInTimeInput {
    /// Resolve the output file path from either `FilePath` or `S3Prefix`.
    ///
    /// # Errors
    ///
    /// Returns an error message if neither field is provided.
    pub fn resolve_file_path(&self) -> Result<&str, &'static str> {
        if let Some(ref p) = self.file_path {
            return Ok(p.as_str());
        }
        if let Some(ref p) = self.s3_prefix {
            return Ok(p.as_str());
        }
        Err("Either FilePath or S3Prefix must be provided")
    }
}

/// Export status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportStatus {
    /// Export completed successfully.
    #[serde(rename = "COMPLETED")]
    Completed,
    /// Export failed.
    #[serde(rename = "FAILED")]
    Failed,
}

/// Description of an export operation.
#[derive(Debug, Clone, Serialize)]
pub struct ExportDescription {
    /// ARN of the export.
    #[serde(rename = "ExportArn")]
    pub export_arn: String,
    /// Current status.
    #[serde(rename = "ExportStatus")]
    pub export_status: ExportStatus,
    /// ARN of the source table.
    #[serde(rename = "TableArn")]
    pub table_arn: String,
    /// Table ID.
    #[serde(rename = "TableId", skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,
    /// Export format.
    #[serde(rename = "ExportFormat")]
    pub export_format: ExportFormat,
    /// Number of items exported.
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    /// Size in bytes.
    #[serde(rename = "BilledSizeBytes")]
    pub billed_size_bytes: i64,
    /// Start time (epoch seconds).
    #[serde(rename = "StartTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<f64>,
    /// End time (epoch seconds).
    #[serde(rename = "EndTime", skip_serializing_if = "Option::is_none")]
    pub end_time: Option<f64>,
    /// Failure code (if failed).
    #[serde(rename = "FailureCode", skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    /// Failure message (if failed).
    #[serde(rename = "FailureMessage", skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
}

/// Output for `ExportTableToPointInTime`.
#[derive(Debug, Clone, Serialize)]
pub struct ExportTableToPointInTimeOutput {
    /// Description of the export.
    #[serde(rename = "ExportDescription")]
    pub export_description: ExportDescription,
}
