// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Types for bulk operations: `StartBulkPutItems`, `DescribeBulkOperation`,
//! `ListBulkOperations`, and `CancelBulkOperation`.
//!
//! Member names follow the API model. On the S3 store `S3Bucket` and
//! `S3KeyPrefix` name a bucket and a key prefix; on the filesystem store
//! they name a directory under the caller's account subtree of the root and
//! a path prefix within it.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Input format for bulk put operations.
///
/// Unlike `ImportTable`'s `InputFormat`, this enum has no CSV member; the
/// model defines a separate two-member set for bulk operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BulkOperationInputFormat {
    /// `DynamoDB` JSON format (`{"Item": {"pk": {"S": "val"}, ...}}`).
    #[serde(rename = "DYNAMODB_JSON")]
    DynamodbJson,
    /// Amazon Ion text format.
    #[serde(rename = "ION")]
    Ion,
}

impl<'de> Deserialize<'de> for BulkOperationInputFormat {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "DYNAMODB_JSON" => Ok(Self::DynamodbJson),
            "ION" => Ok(Self::Ion),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'inputFormat' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [DYNAMODB_JSON, ION]"
            ))),
        }
    }
}

/// Compression applied to bulk input files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum InputCompressionType {
    /// Gzip-compressed input files.
    #[serde(rename = "GZIP")]
    Gzip,
    /// Zstandard-compressed input files, decoded while reading.
    #[serde(rename = "ZSTD")]
    Zstd,
    /// Uncompressed input files. An absent `InputCompressionType` is read as
    /// this value.
    #[serde(rename = "NONE")]
    None,
}

impl<'de> Deserialize<'de> for InputCompressionType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "GZIP" => Ok(Self::Gzip),
            "ZSTD" => Ok(Self::Zstd),
            "NONE" => Ok(Self::None),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'inputCompressionType' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [GZIP, ZSTD, NONE]"
            ))),
        }
    }
}

/// Performance tier for a bulk operation. Recorded and returned; a single
/// node makes no behavioural distinction between the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BulkOperationPerformanceTier {
    /// Default tier.
    #[serde(rename = "STANDARD")]
    Standard,
    /// Accelerated tier.
    #[serde(rename = "ACCELERATED")]
    Accelerated,
}

impl<'de> Deserialize<'de> for BulkOperationPerformanceTier {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "STANDARD" => Ok(Self::Standard),
            "ACCELERATED" => Ok(Self::Accelerated),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'performanceTier' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [STANDARD, ACCELERATED]"
            ))),
        }
    }
}

/// Report record kinds an operation writes to its output location.
///
/// The model names this shape `OutputTypeEnum` and the list member
/// `OutputType`; the wire member name is what clients see, so the type is
/// named after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OutputType {
    /// Rows that failed a parse-level or item-level check.
    #[serde(rename = "ERROR")]
    Error,
    /// Rows whose condition evaluated false at the write.
    #[serde(rename = "CONDITION_CHECK_FAILURE")]
    ConditionCheckFailure,
}

impl<'de> Deserialize<'de> for OutputType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "ERROR" => Ok(Self::Error),
            "CONDITION_CHECK_FAILURE" => Ok(Self::ConditionCheckFailure),
            // The value sits inside a list, so the service would report a
            // positional path here; a serde deserializer cannot know its
            // index. Same limitation as `DistanceFunction` in table.rs.
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'outputType' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [ERROR, CONDITION_CHECK_FAILURE]"
            ))),
        }
    }
}

/// Server-side encryption algorithm for the report location. Recorded, no
/// effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum S3SseAlgorithm {
    /// AES-256 encryption.
    #[serde(rename = "AES256")]
    Aes256,
    /// KMS-managed key encryption.
    #[serde(rename = "KMS")]
    Kms,
}

impl<'de> Deserialize<'de> for S3SseAlgorithm {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "AES256" => Ok(Self::Aes256),
            "KMS" => Ok(Self::Kms),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 's3SseAlgorithm' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [AES256, KMS]"
            ))),
        }
    }
}

/// Status of a bulk operation.
///
/// The transitions are `IN_PROGRESS` to any of `COMPLETED`,
/// `COMPLETED_WITH_ERRORS`, `FAILED`, or `CANCELLING`, and `CANCELLING` to
/// `CANCELLED` only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BulkOperationStatus {
    /// The operation is running.
    #[serde(rename = "IN_PROGRESS")]
    InProgress,
    /// Every row processed, no failed items. Condition failures do not
    /// count as failed items.
    #[serde(rename = "COMPLETED")]
    Completed,
    /// Every row processed, one or more failed items.
    #[serde(rename = "COMPLETED_WITH_ERRORS")]
    CompletedWithErrors,
    /// A cancel was accepted and the operation is stopping.
    #[serde(rename = "CANCELLING")]
    Cancelling,
    /// The operation stopped after a cancel.
    #[serde(rename = "CANCELLED")]
    Cancelled,
    /// The operation could not continue.
    #[serde(rename = "FAILED")]
    Failed,
}

impl<'de> Deserialize<'de> for BulkOperationStatus {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "IN_PROGRESS" => Ok(Self::InProgress),
            "COMPLETED" => Ok(Self::Completed),
            "COMPLETED_WITH_ERRORS" => Ok(Self::CompletedWithErrors),
            "CANCELLING" => Ok(Self::Cancelling),
            "CANCELLED" => Ok(Self::Cancelled),
            "FAILED" => Ok(Self::Failed),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'bulkOperationStatus' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [IN_PROGRESS, COMPLETED, COMPLETED_WITH_ERRORS, CANCELLING, CANCELLED, FAILED]"
            ))),
        }
    }
}

impl BulkOperationStatus {
    /// Whether this status is terminal.
    ///
    /// Every poll loop and every state check must treat all four terminal
    /// statuses alike, including `COMPLETED_WITH_ERRORS`.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::CompletedWithErrors | Self::Failed | Self::Cancelled
        )
    }
}

/// Input file location and format for `StartBulkPutItems`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InputS3Configuration {
    /// Directory name under the configured import root, resolved per
    /// account.
    #[serde(rename = "S3Bucket")]
    pub s3_bucket: String,
    /// Prefix over bucket-relative file paths. Empty or absent means every
    /// file in the bucket directory.
    #[serde(rename = "S3KeyPrefix", skip_serializing_if = "Option::is_none")]
    pub s3_key_prefix: Option<String>,
    /// Format of the input files.
    #[serde(rename = "InputFormat")]
    pub input_format: BulkOperationInputFormat,
    /// Compression applied to the input files; absent is read as `NONE`.
    #[serde(
        rename = "InputCompressionType",
        skip_serializing_if = "Option::is_none"
    )]
    pub input_compression_type: Option<InputCompressionType>,
    /// Owning account id; when present it must equal the caller's account.
    #[serde(rename = "S3BucketOwner", skip_serializing_if = "Option::is_none")]
    pub s3_bucket_owner: Option<String>,
}

/// Report location and record selection for `StartBulkPutItems`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutputS3Configuration {
    /// Which report record kinds are written. The model requires the member;
    /// the handler rejects an empty list.
    #[serde(rename = "OutputType")]
    pub output_type: Vec<OutputType>,
    /// Directory name under the configured export root. Absent means no
    /// report is written; counters are still maintained.
    #[serde(rename = "S3Bucket", skip_serializing_if = "Option::is_none")]
    pub s3_bucket: Option<String>,
    /// Prefix under which the report tree is written.
    #[serde(rename = "S3KeyPrefix", skip_serializing_if = "Option::is_none")]
    pub s3_key_prefix: Option<String>,
    /// Owning account id; when present it must equal the caller's account.
    #[serde(rename = "S3BucketOwner", skip_serializing_if = "Option::is_none")]
    pub s3_bucket_owner: Option<String>,
    /// Server-side encryption algorithm. Recorded, no effect.
    #[serde(rename = "S3SseAlgorithm", skip_serializing_if = "Option::is_none")]
    pub s3_sse_algorithm: Option<S3SseAlgorithm>,
    /// KMS key for server-side encryption. Recorded, no effect.
    #[serde(rename = "S3SseKmsKeyId", skip_serializing_if = "Option::is_none")]
    pub s3_sse_kms_key_id: Option<String>,
}

/// Input for `StartBulkPutItems`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StartBulkPutItemsInput {
    /// A bare table name or a table ARN in the caller's account and region.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Idempotency token. A repeated token with the same parameters replays
    /// the existing operation.
    #[serde(rename = "ClientToken", skip_serializing_if = "Option::is_none")]
    pub client_token: Option<String>,
    /// Input file location and format.
    #[serde(rename = "InputS3Configuration")]
    pub input_s3_configuration: InputS3Configuration,
    /// Report location and record selection.
    #[serde(rename = "OutputS3Configuration")]
    pub output_s3_configuration: OutputS3Configuration,
    /// Write rate cap for the operation. The model constrains it to
    /// `>= 1`; the Start handler enforces the range in a later phase, so
    /// the wire type is signed and an out-of-range value reaches the
    /// handler as a value rather than dying in deserialization, like
    /// `Limit` on `QueryInput`. Recorded and returned.
    #[serde(
        rename = "MaxWriteRequestUnits",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_write_request_units: Option<i64>,
    /// Performance tier; absent means `STANDARD`.
    #[serde(rename = "PerformanceTier", skip_serializing_if = "Option::is_none")]
    pub performance_tier: Option<BulkOperationPerformanceTier>,
}

/// Bytes written to the table or to one secondary index.
///
/// The wire value is a structure holding one member, not a bare number.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct BulkOperationBytes {
    /// Bytes written, rounded per item to the next 1 KB with a 1 KB minimum.
    #[serde(rename = "BulkPutBytes")]
    pub bulk_put_bytes: u64,
}

/// Bytes written to one vector index.
///
/// The wire value is a structure holding one member, not a bare number.
/// Vector index bytes are metered in their own unit and are not included in
/// the top-level `BulkPutBytes`.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct BulkOperationVectorBytes {
    /// Vector write bytes for the index.
    #[serde(rename = "BulkPutVectorBytes")]
    pub bulk_put_vector_bytes: u64,
}

/// Usage entry for one table in a terminal bulk operation description.
#[derive(Debug, Clone, Serialize)]
pub struct BulkOperationUsage {
    /// Bare name of the table. The model types the member `TableArn`, but
    /// the service emits the bare table name here (measured on the
    /// service), not the ARN.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Total bytes written to the base table.
    #[serde(rename = "BulkPutBytes")]
    pub bulk_put_bytes: u64,
    /// Base table bytes as a nested structure; the same figure as
    /// `BulkPutBytes`.
    #[serde(rename = "Table")]
    pub table: BulkOperationBytes,
    /// Bytes written to each global secondary index. An index no row
    /// projects into has no entry, so a map with no entries is omitted
    /// (measured on the service).
    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub global_secondary_indexes: HashMap<String, BulkOperationBytes>,
    /// Bytes written to each local secondary index. An empty map is omitted
    /// (measured on the service).
    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub local_secondary_indexes: HashMap<String, BulkOperationBytes>,
    /// Vector write bytes for each vector index. An empty map is omitted
    /// (measured on the service).
    #[serde(rename = "VectorIndexes", skip_serializing_if = "HashMap::is_empty")]
    pub vector_indexes: HashMap<String, BulkOperationVectorBytes>,
}

/// One entry of a description's `TableDetails`: the target table's name
/// and id.
#[derive(Debug, Clone, Serialize)]
pub struct BulkOperationTableDetail {
    /// Bare name of the target table.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Id of the target table.
    #[serde(rename = "TableId")]
    pub table_id: String,
}

/// Description of a bulk operation.
#[derive(Debug, Clone, Serialize)]
pub struct BulkOperationDescription {
    /// ARN of the operation.
    #[serde(rename = "BulkOperationArn")]
    pub bulk_operation_arn: String,
    /// Current status.
    #[serde(rename = "BulkOperationStatus")]
    pub bulk_operation_status: BulkOperationStatus,
    /// Name and id of the target table. One entry; the model types the
    /// member as a list. Present on the Start response and on every
    /// Describe (measured on the service).
    #[serde(rename = "TableDetails")]
    pub table_details: Vec<BulkOperationTableDetail>,
    /// ARN of the target table. Modelled, never emitted by the service
    /// (measured on the service), so always `None` on the wire.
    #[serde(rename = "TableArn", skip_serializing_if = "Option::is_none")]
    pub table_arn: Option<String>,
    /// Id of the target table. Modelled, never emitted by the service
    /// (measured on the service); the id travels in `TableDetails`.
    #[serde(rename = "TableId", skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,
    /// Client token the operation was started with.
    #[serde(rename = "ClientToken", skip_serializing_if = "Option::is_none")]
    pub client_token: Option<String>,
    /// Input configuration as submitted.
    #[serde(
        rename = "InputS3Configuration",
        skip_serializing_if = "Option::is_none"
    )]
    pub input_s3_configuration: Option<InputS3Configuration>,
    /// Output configuration as submitted.
    #[serde(
        rename = "OutputS3Configuration",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_s3_configuration: Option<OutputS3Configuration>,
    /// Start time (epoch seconds).
    #[serde(rename = "StartTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<f64>,
    /// End time (epoch seconds), present once terminal.
    #[serde(rename = "EndTime", skip_serializing_if = "Option::is_none")]
    pub end_time: Option<f64>,
    /// Rows read and attempted, including failed rows and condition
    /// failures.
    #[serde(rename = "ProcessedItemCount", skip_serializing_if = "Option::is_none")]
    pub processed_item_count: Option<u64>,
    /// Bytes of input consumed so far.
    #[serde(
        rename = "ProcessedInputBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub processed_input_bytes: Option<u64>,
    /// Sum of the enumerated sizes of all matched input files, fixed at
    /// start.
    #[serde(rename = "TotalInputBytes", skip_serializing_if = "Option::is_none")]
    pub total_input_bytes: Option<u64>,
    /// Usage entries, present once terminal; one entry for the table.
    #[serde(rename = "BulkOperationUsage", skip_serializing_if = "Option::is_none")]
    pub bulk_operation_usage: Option<Vec<BulkOperationUsage>>,
    /// Rows that failed a parse-level or item-level check.
    #[serde(rename = "FailedItemCount", skip_serializing_if = "Option::is_none")]
    pub failed_item_count: Option<u64>,
    /// Exception type name explaining a `FAILED` status.
    #[serde(rename = "FailureCode", skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    /// Message explaining a `FAILED` status.
    #[serde(rename = "FailureMessage", skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
    /// Write rate cap the operation was started with. Signed like the
    /// input field it echoes; the model types both as the same `long`.
    #[serde(
        rename = "MaxWriteRequestUnits",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_write_request_units: Option<i64>,
    /// Performance tier the operation runs at.
    #[serde(rename = "PerformanceTier", skip_serializing_if = "Option::is_none")]
    pub performance_tier: Option<BulkOperationPerformanceTier>,
}

/// Output for `StartBulkPutItems`.
#[derive(Debug, Clone, Serialize)]
pub struct StartBulkPutItemsOutput {
    /// Description of the started operation; status is always
    /// `IN_PROGRESS`.
    #[serde(rename = "BulkOperationDescription")]
    pub bulk_operation_description: BulkOperationDescription,
}

/// Input for `DescribeBulkOperation`.
#[derive(Debug, Clone, Deserialize)]
pub struct DescribeBulkOperationInput {
    /// ARN of the operation to describe.
    #[serde(rename = "BulkOperationArn")]
    pub bulk_operation_arn: String,
}

/// Output for `DescribeBulkOperation`.
#[derive(Debug, Clone, Serialize)]
pub struct DescribeBulkOperationOutput {
    /// Description of the operation.
    #[serde(rename = "BulkOperationDescription")]
    pub bulk_operation_description: BulkOperationDescription,
}

/// Input for `ListBulkOperations`.
#[derive(Debug, Clone, Deserialize)]
pub struct ListBulkOperationsInput {
    /// Page size. The model constrains it to 1 to 25 inclusive; absent
    /// means 10 (measured). The List handler enforces the range, so the
    /// wire type is signed and an out-of-range value reaches the handler as
    /// a value rather than dying in deserialization, like `Limit` on
    /// `QueryInput`.
    #[serde(rename = "MaxResults")]
    pub max_results: Option<i64>,
    /// Pagination cursor from a previous response.
    #[serde(rename = "NextToken")]
    pub next_token: Option<String>,
}

/// Summary of one operation in a `ListBulkOperations` page.
#[derive(Debug, Clone, Serialize)]
pub struct BulkOperationSummary {
    /// ARN of the operation.
    #[serde(rename = "BulkOperationArn")]
    pub bulk_operation_arn: String,
    /// Current status.
    #[serde(rename = "BulkOperationStatus")]
    pub bulk_operation_status: BulkOperationStatus,
    /// Start time (epoch seconds).
    #[serde(rename = "StartTime")]
    pub start_time: f64,
    /// End time (epoch seconds), present once terminal.
    #[serde(rename = "EndTime", skip_serializing_if = "Option::is_none")]
    pub end_time: Option<f64>,
}

/// Output for `ListBulkOperations`.
#[derive(Debug, Clone, Serialize)]
pub struct ListBulkOperationsOutput {
    /// One page of summaries, newest first.
    #[serde(rename = "BulkOperationsSummaryList")]
    pub bulk_operations_summary_list: Vec<BulkOperationSummary>,
    /// Cursor for the next page; absent on the last page.
    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

/// Input for `CancelBulkOperation`.
#[derive(Debug, Clone, Deserialize)]
pub struct CancelBulkOperationInput {
    /// ARN of the operation to cancel.
    #[serde(rename = "BulkOperationArn")]
    pub bulk_operation_arn: String,
}

/// Output for `CancelBulkOperation`. The model requires both members and
/// defines no others.
#[derive(Debug, Clone, Serialize)]
pub struct CancelBulkOperationOutput {
    /// ARN of the operation.
    #[serde(rename = "BulkOperationArn")]
    pub bulk_operation_arn: String,
    /// Status after the cancel, normally `CANCELLING`.
    #[serde(rename = "BulkOperationStatus")]
    pub bulk_operation_status: BulkOperationStatus,
}

#[cfg(test)]
#[path = "bulk_operation_tests/mod.rs"]
mod bulk_operation_tests;
