// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DynamoDB` Streams types — stream records, shard iterators, and API input/output.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::attribute_value::AttributeValue;
use super::table::StreamViewType;

/// Event type for a stream record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamEventName {
    Insert,
    Modify,
    Remove,
}

/// Shard iterator type for `GetShardIterator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ShardIteratorType {
    TrimHorizon,
    Latest,
    AtSequenceNumber,
    AfterSequenceNumber,
}

/// Stream status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamStatus {
    Enabling,
    Enabled,
    Disabling,
    Disabled,
}

/// The DynamoDB-specific portion of a stream record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecordData {
    #[serde(rename = "ApproximateCreationDateTime")]
    pub approximate_creation_date_time: i64,
    #[serde(rename = "Keys")]
    pub keys: BTreeMap<String, AttributeValue>,
    #[serde(rename = "NewImage", skip_serializing_if = "Option::is_none")]
    pub new_image: Option<BTreeMap<String, AttributeValue>>,
    #[serde(rename = "OldImage", skip_serializing_if = "Option::is_none")]
    pub old_image: Option<BTreeMap<String, AttributeValue>>,
    #[serde(rename = "SequenceNumber")]
    pub sequence_number: String,
    #[serde(rename = "SizeBytes")]
    pub size_bytes: i64,
    #[serde(rename = "StreamViewType")]
    pub stream_view_type: StreamViewType,
}

/// Identity of the principal that triggered a stream record.
///
/// For TTL-originated deletions, `DynamoDB` sets `Type` to `"Service"` and
/// `PrincipalId` to `"dynamodb.amazonaws.com"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserIdentity {
    #[serde(rename = "Type")]
    pub identity_type: String,
    #[serde(rename = "PrincipalId")]
    pub principal_id: String,
}

/// A full stream record as returned by `GetRecords`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    #[serde(rename = "eventID")]
    pub event_id: String,
    #[serde(rename = "eventName")]
    pub event_name: StreamEventName,
    #[serde(rename = "eventVersion")]
    pub event_version: String,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    #[serde(rename = "dynamodb")]
    pub dynamodb: StreamRecordData,
    #[serde(rename = "userIdentity", skip_serializing_if = "Option::is_none")]
    pub user_identity: Option<UserIdentity>,
}

/// Shard description within a stream.
#[derive(Debug, Clone, Serialize)]
pub struct Shard {
    #[serde(rename = "ShardId")]
    pub shard_id: String,
    #[serde(rename = "ParentShardId", skip_serializing_if = "Option::is_none")]
    pub parent_shard_id: Option<String>,
    #[serde(rename = "SequenceNumberRange")]
    pub sequence_number_range: SequenceNumberRange,
}

/// Sequence number range for a shard.
#[derive(Debug, Clone, Serialize)]
pub struct SequenceNumberRange {
    #[serde(rename = "StartingSequenceNumber")]
    pub starting_sequence_number: String,
    #[serde(
        rename = "EndingSequenceNumber",
        skip_serializing_if = "Option::is_none"
    )]
    pub ending_sequence_number: Option<String>,
}

/// Stream description returned by `DescribeStream`.
#[derive(Debug, Clone, Serialize)]
pub struct StreamDescription {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "StreamLabel")]
    pub stream_label: String,
    #[serde(rename = "StreamStatus")]
    pub stream_status: StreamStatus,
    #[serde(rename = "StreamViewType")]
    pub stream_view_type: StreamViewType,
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<super::key_schema::KeySchemaElement>,
    #[serde(rename = "Shards")]
    pub shards: Vec<Shard>,
    #[serde(
        rename = "LastEvaluatedShardId",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_shard_id: Option<String>,
}

/// A stream summary returned by `ListStreams`.
#[derive(Debug, Clone, Serialize)]
pub struct StreamSummary {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "StreamLabel")]
    pub stream_label: String,
    #[serde(rename = "TableName")]
    pub table_name: String,
}

// --- API Input/Output types ---

/// `DescribeStream` request.
#[derive(Debug, Deserialize)]
pub struct DescribeStreamInput {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "Limit", default)]
    pub limit: Option<i64>,
    #[serde(rename = "ExclusiveStartShardId", default)]
    pub exclusive_start_shard_id: Option<String>,
}

/// `DescribeStream` response.
#[derive(Debug, Serialize)]
pub struct DescribeStreamOutput {
    #[serde(rename = "StreamDescription")]
    pub stream_description: StreamDescription,
}

/// `ListStreams` request.
#[derive(Debug, Deserialize)]
pub struct ListStreamsInput {
    #[serde(rename = "TableName", default)]
    pub table_name: Option<String>,
    #[serde(rename = "Limit", default)]
    pub limit: Option<i64>,
    #[serde(rename = "ExclusiveStartStreamArn", default)]
    pub exclusive_start_stream_arn: Option<String>,
}

/// `ListStreams` response.
#[derive(Debug, Serialize)]
pub struct ListStreamsOutput {
    #[serde(rename = "Streams")]
    pub streams: Vec<StreamSummary>,
    #[serde(
        rename = "LastEvaluatedStreamArn",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_stream_arn: Option<String>,
}

/// `GetShardIterator` request.
#[derive(Debug, Deserialize)]
pub struct GetShardIteratorInput {
    #[serde(rename = "StreamArn")]
    pub stream_arn: String,
    #[serde(rename = "ShardId")]
    pub shard_id: String,
    #[serde(rename = "ShardIteratorType")]
    pub shard_iterator_type: ShardIteratorType,
    #[serde(rename = "SequenceNumber", default)]
    pub sequence_number: Option<String>,
}

/// `GetShardIterator` response.
#[derive(Debug, Serialize)]
pub struct GetShardIteratorOutput {
    #[serde(rename = "ShardIterator", skip_serializing_if = "Option::is_none")]
    pub shard_iterator: Option<String>,
}

/// `GetRecords` request.
#[derive(Debug, Deserialize)]
pub struct GetRecordsInput {
    #[serde(rename = "ShardIterator")]
    pub shard_iterator: String,
    #[serde(rename = "Limit", default)]
    pub limit: Option<i64>,
}

/// `GetRecords` response.
#[derive(Debug, Serialize)]
pub struct GetRecordsOutput {
    #[serde(rename = "Records")]
    pub records: Vec<StreamRecord>,
    #[serde(rename = "NextShardIterator", skip_serializing_if = "Option::is_none")]
    pub next_shard_iterator: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_identity_serializes_correctly() {
        let identity = UserIdentity {
            identity_type: "Service".to_owned(),
            principal_id: "dynamodb.amazonaws.com".to_owned(),
        };
        let json = serde_json::to_value(&identity).unwrap();
        assert_eq!(json["Type"], "Service");
        assert_eq!(json["PrincipalId"], "dynamodb.amazonaws.com");
    }

    #[test]
    fn stream_record_with_user_identity() {
        let record = StreamRecord {
            event_id: "test".to_owned(),
            event_name: StreamEventName::Remove,
            event_version: "1.1".to_owned(),
            event_source: "aws:dynamodb".to_owned(),
            aws_region: "us-east-1".to_owned(),
            dynamodb: StreamRecordData {
                approximate_creation_date_time: 0,
                keys: BTreeMap::new(),
                new_image: None,
                old_image: None,
                sequence_number: "1".to_owned(),
                size_bytes: 0,
                stream_view_type: super::super::table::StreamViewType::NewAndOldImages,
            },
            user_identity: Some(UserIdentity {
                identity_type: "Service".to_owned(),
                principal_id: "dynamodb.amazonaws.com".to_owned(),
            }),
        };
        let json = serde_json::to_value(&record).unwrap();
        let ui = &json["userIdentity"];
        assert_eq!(ui["Type"], "Service");
        assert_eq!(ui["PrincipalId"], "dynamodb.amazonaws.com");
    }

    #[test]
    fn stream_record_without_user_identity_omits_field() {
        let record = StreamRecord {
            event_id: "test".to_owned(),
            event_name: StreamEventName::Insert,
            event_version: "1.1".to_owned(),
            event_source: "aws:dynamodb".to_owned(),
            aws_region: "us-east-1".to_owned(),
            dynamodb: StreamRecordData {
                approximate_creation_date_time: 0,
                keys: BTreeMap::new(),
                new_image: None,
                old_image: None,
                sequence_number: "1".to_owned(),
                size_bytes: 0,
                stream_view_type: super::super::table::StreamViewType::NewAndOldImages,
            },
            user_identity: None,
        };
        let json = serde_json::to_value(&record).unwrap();
        assert!(json.get("userIdentity").is_none());
    }
}
