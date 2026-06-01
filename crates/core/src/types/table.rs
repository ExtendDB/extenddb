// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde::{Deserialize, Serialize};

use super::key_schema::{AttributeDefinition, KeySchemaElement};

// --- Enums ---

/// Billing mode for a Virtual `DynamoDB` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BillingMode {
    /// Provisioned capacity with explicit RCU/WCU.
    Provisioned,
    /// On-demand capacity — pay per request.
    PayPerRequest,
}

/// Current status of a Virtual `DynamoDB` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TableStatus {
    /// Table is being created.
    Creating,
    /// Table is ready for use.
    Active,
    /// Table is being deleted.
    Deleting,
    /// Table is being updated.
    Updating,
}

/// Projection type for a secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProjectionType {
    /// All attributes are projected.
    All,
    /// Only key attributes are projected.
    KeysOnly,
    /// Key attributes plus specified non-key attributes are projected.
    Include,
}

/// View type for Virtual `DynamoDB` Streams records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamViewType {
    /// Only key attributes.
    KeysOnly,
    /// The entire item after modification.
    NewImage,
    /// The entire item before modification.
    OldImage,
    /// Both old and new images.
    NewAndOldImages,
}

/// Server-side encryption type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseType {
    /// Amazon S3-managed encryption.
    AES256,
    /// AWS KMS-managed encryption.
    KMS,
}

// --- Structs ---

/// Provisioned throughput settings for a table or GSI (input).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvisionedThroughput {
    #[serde(rename = "ReadCapacityUnits")]
    pub read_capacity_units: i64,
    #[serde(rename = "WriteCapacityUnits")]
    pub write_capacity_units: i64,
}

/// Provisioned throughput description returned in responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvisionedThroughputDescription {
    #[serde(rename = "ReadCapacityUnits")]
    pub read_capacity_units: i64,
    #[serde(rename = "WriteCapacityUnits")]
    pub write_capacity_units: i64,
    #[serde(rename = "NumberOfDecreasesToday")]
    pub number_of_decreases_today: i64,
    #[serde(
        rename = "LastIncreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_increase_date_time: Option<f64>,
    #[serde(
        rename = "LastDecreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_decrease_date_time: Option<f64>,
}

/// Index projection configuration — which attributes are copied into the index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    #[serde(rename = "ProjectionType")]
    pub projection_type: ProjectionType,
    #[serde(rename = "NonKeyAttributes", skip_serializing_if = "Option::is_none")]
    pub non_key_attributes: Option<Vec<String>>,
}

/// Virtual `DynamoDB` Streams configuration for a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSpecification {
    #[serde(rename = "StreamEnabled")]
    pub stream_enabled: bool,
    #[serde(rename = "StreamViewType", skip_serializing_if = "Option::is_none")]
    pub stream_view_type: Option<StreamViewType>,
}

/// Server-side encryption description.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SseDescription {
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "SSEType", skip_serializing_if = "Option::is_none")]
    pub sse_type: Option<SseType>,
}

/// Summary of the table's billing mode and last update timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BillingModeSummary {
    #[serde(rename = "BillingMode")]
    pub billing_mode: BillingMode,
    #[serde(
        rename = "LastUpdateToPayPerRequestDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_update_to_pay_per_request_date_time: Option<f64>,
}

/// A key-value tag attached to a Virtual `DynamoDB` resource.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tag {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Value")]
    pub value: String,
}

/// Global secondary index description returned in responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GsiDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(rename = "IndexStatus")]
    pub index_status: String,
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughputDescription>,
    #[serde(rename = "IndexSizeBytes")]
    pub index_size_bytes: i64,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
}

/// Local secondary index description returned in responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LsiDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(rename = "IndexSizeBytes")]
    pub index_size_bytes: i64,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
}

/// Global secondary index definition for `CreateTable` requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GsiInput {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

/// Local secondary index definition for `CreateTable` requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LsiInput {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
}

/// Full description of a Virtual `DynamoDB` table, returned by `CreateTable`,
/// `DeleteTable`, and `DescribeTable`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDescription {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(rename = "TableStatus")]
    pub table_status: TableStatus,
    #[serde(rename = "CreationDateTime")]
    pub creation_date_time: f64,
    #[serde(rename = "TableSizeBytes")]
    pub table_size_bytes: i64,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "TableArn")]
    pub table_arn: String,
    #[serde(rename = "TableId")]
    pub table_id: String,
    #[serde(rename = "ProvisionedThroughput")]
    pub provisioned_throughput: ProvisionedThroughputDescription,
    #[serde(rename = "BillingModeSummary", skip_serializing_if = "Option::is_none")]
    pub billing_mode_summary: Option<BillingModeSummary>,
    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_secondary_indexes: Option<Vec<GsiDescription>>,
    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub local_secondary_indexes: Option<Vec<LsiDescription>>,
    #[serde(
        rename = "StreamSpecification",
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(rename = "LatestStreamArn", skip_serializing_if = "Option::is_none")]
    pub latest_stream_arn: Option<String>,
    #[serde(rename = "LatestStreamLabel", skip_serializing_if = "Option::is_none")]
    pub latest_stream_label: Option<String>,
    #[serde(rename = "DeletionProtectionEnabled")]
    pub deletion_protection_enabled: bool,
    #[serde(rename = "SSEDescription", skip_serializing_if = "Option::is_none")]
    pub sse_description: Option<SseDescription>,
    #[serde(rename = "TableClassSummary", skip_serializing_if = "Option::is_none")]
    pub table_class_summary: Option<serde_json::Value>,
}

/// `CreateTable` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CreateTableInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(rename = "BillingMode")]
    pub billing_mode: Option<BillingMode>,
    #[serde(rename = "ProvisionedThroughput")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(rename = "GlobalSecondaryIndexes")]
    pub global_secondary_indexes: Option<Vec<GsiInput>>,
    #[serde(rename = "LocalSecondaryIndexes")]
    pub local_secondary_indexes: Option<Vec<LsiInput>>,
    #[serde(rename = "StreamSpecification")]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(rename = "SSESpecification")]
    pub sse_specification: Option<serde_json::Value>,
    #[serde(rename = "Tags")]
    pub tags: Option<Vec<Tag>>,
    #[serde(rename = "DeletionProtectionEnabled")]
    pub deletion_protection_enabled: Option<bool>,
    #[serde(rename = "TableClass")]
    pub table_class: Option<String>,
}

/// `CreateTable` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CreateTableOutput {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

/// `DeleteTable` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DeleteTableInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
}

/// `DeleteTable` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeleteTableOutput {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

/// `DescribeTable` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DescribeTableInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
}

/// `DescribeTable` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DescribeTableOutput {
    #[serde(rename = "Table")]
    pub table: TableDescription,
}

/// `ListTables` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ListTablesInput {
    #[serde(rename = "Limit")]
    pub limit: Option<i32>,
    #[serde(rename = "ExclusiveStartTableName")]
    pub exclusive_start_table_name: Option<String>,
}

/// `ListTables` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ListTablesOutput {
    #[serde(rename = "TableNames")]
    pub table_names: Vec<String>,
    #[serde(
        rename = "LastEvaluatedTableName",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_table_name: Option<String>,
}

// --- UpdateTable ---

/// A single GSI update action within an `UpdateTable` request.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GlobalSecondaryIndexUpdate {
    #[serde(rename = "Create", skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateGsiAction>,
    #[serde(rename = "Update", skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateGsiAction>,
    #[serde(rename = "Delete", skip_serializing_if = "Option::is_none")]
    pub delete: Option<DeleteGsiAction>,
}

/// Create a new GSI on an existing table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CreateGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

/// Delete an existing GSI from a table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DeleteGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

/// Update provisioned throughput on an existing GSI.
///
/// Recognized by the deserializer but not yet implemented — the engine
/// returns a clear "not yet supported" error.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdateGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

/// `UpdateTable` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdateTableInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "BillingMode")]
    pub billing_mode: Option<BillingMode>,
    #[serde(rename = "ProvisionedThroughput")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(rename = "DeletionProtectionEnabled")]
    pub deletion_protection_enabled: Option<bool>,
    #[serde(rename = "GlobalSecondaryIndexUpdates")]
    pub global_secondary_index_updates: Option<Vec<GlobalSecondaryIndexUpdate>>,
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    #[serde(rename = "StreamSpecification")]
    pub stream_specification: Option<StreamSpecification>,
}

/// `UpdateTable` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UpdateTableOutput {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

// --- TTL ---

/// TTL status for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeToLiveStatus {
    /// TTL is being enabled.
    Enabling,
    /// TTL is enabled.
    Enabled,
    /// TTL is being disabled.
    Disabling,
    /// TTL is disabled.
    Disabled,
}

/// TTL description returned by `DescribeTimeToLive` and `UpdateTimeToLive`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TimeToLiveDescription {
    #[serde(rename = "TimeToLiveStatus")]
    pub time_to_live_status: TimeToLiveStatus,
    #[serde(rename = "AttributeName", skip_serializing_if = "Option::is_none")]
    pub attribute_name: Option<String>,
}

/// `DescribeTimeToLive` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DescribeTimeToLiveInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
}

/// `DescribeTimeToLive` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DescribeTimeToLiveOutput {
    #[serde(rename = "TimeToLiveDescription")]
    pub time_to_live_description: TimeToLiveDescription,
}

/// `UpdateTimeToLive` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdateTimeToLiveInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "TimeToLiveSpecification")]
    pub time_to_live_specification: TimeToLiveSpecification,
}

/// TTL specification for `UpdateTimeToLive`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TimeToLiveSpecification {
    #[serde(rename = "Enabled")]
    pub enabled: bool,
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
}

/// `UpdateTimeToLive` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UpdateTimeToLiveOutput {
    #[serde(rename = "TimeToLiveSpecification")]
    pub time_to_live_specification: TimeToLiveSpecificationOutput,
}

/// TTL specification in responses (uses status enum instead of bool).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TimeToLiveSpecificationOutput {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "Enabled")]
    pub enabled: bool,
}

// --- Tags ---

/// `TagResource` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TagResourceInput {
    #[serde(rename = "ResourceArn")]
    pub resource_arn: String,
    #[serde(rename = "Tags")]
    pub tags: Vec<Tag>,
}

/// `UntagResource` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UntagResourceInput {
    #[serde(rename = "ResourceArn")]
    pub resource_arn: String,
    #[serde(rename = "TagKeys")]
    pub tag_keys: Vec<String>,
}

/// `ListTagsOfResource` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ListTagsOfResourceInput {
    #[serde(rename = "ResourceArn")]
    pub resource_arn: String,
    #[serde(rename = "NextToken")]
    pub next_token: Option<String>,
}

/// `ListTagsOfResource` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ListTagsOfResourceOutput {
    #[serde(rename = "Tags")]
    pub tags: Vec<Tag>,
    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

// --- DescribeLimits ---

/// `DescribeLimits` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DescribeLimitsOutput {
    #[serde(rename = "AccountMaxReadCapacityUnits")]
    pub account_max_read_capacity_units: i64,
    #[serde(rename = "AccountMaxWriteCapacityUnits")]
    pub account_max_write_capacity_units: i64,
    #[serde(rename = "TableMaxReadCapacityUnits")]
    pub table_max_read_capacity_units: i64,
    #[serde(rename = "TableMaxWriteCapacityUnits")]
    pub table_max_write_capacity_units: i64,
}
