// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Types for `DynamoDB` backup and point-in-time recovery operations.
//!
//! Member names follow the `DynamoDB` 2012-08-10 service model as shipped in
//! botocore 1.33.13 (`botocore/data/dynamodb/2012-08-10/service-2.json.gz`,
//! shapes `BackupDescription`, `SourceTableDetails`,
//! `SourceTableFeatureDetails`, `GlobalSecondaryIndexInfo`,
//! `LocalSecondaryIndexInfo`, `BackupSummary`).

use serde::{Deserialize, Serialize};

use super::key_schema::KeySchemaElement;
use super::table::{
    OnDemandThroughput, Projection, ProvisionedThroughput, SseDescription, StreamSpecification,
    TimeToLiveDescription,
};

/// Backup details returned by `CreateBackup` and `DescribeBackup`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BackupDetails {
    pub backup_arn: String,
    pub backup_name: String,
    pub backup_status: String,
    pub backup_type: String,
    pub backup_size_bytes: i64,
    pub backup_creation_date_time: f64,
}

/// Full backup description including source table info.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BackupDescription {
    pub backup_details: BackupDetails,
    pub source_table_details: SourceTableDetails,
    /// Filled by the engine from the backup's recorded definition; backends
    /// leave it `None`. Absent for a backup recorded without a definition
    /// (catalog 0.0.3 and earlier) and for a backup that is not AVAILABLE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_table_feature_details: Option<SourceTableFeatureDetails>,
}

/// Source table details stored with a backup.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SourceTableDetails {
    pub table_name: String,
    pub table_id: String,
    pub table_arn: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub item_count: i64,
    pub table_size_bytes: i64,
    pub billing_mode: Option<String>,
    pub table_creation_date_time: f64,
    /// Required in the service shape. The engine always fills it on
    /// `DescribeBackup` (`{0, 0}` for an on-demand table, measured on the
    /// service); a backend may leave it `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    /// Not in the botocore 1.33.13 model; newer models carry it. Emitted only
    /// when the backup recorded one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_demand_throughput: Option<OnDemandThroughput>,
}

/// Features of the source table a backup recorded, for `DescribeBackup`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SourceTableFeatureDetails {
    #[serde(
        rename = "LocalSecondaryIndexes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub local_secondary_indexes: Option<Vec<LocalSecondaryIndexInfo>>,
    #[serde(
        rename = "GlobalSecondaryIndexes",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub global_secondary_indexes: Option<Vec<GlobalSecondaryIndexInfo>>,
    /// The service model names this member `StreamDescription` but gives it
    /// the `StreamSpecification` shape (`StreamEnabled`, `StreamViewType`).
    #[serde(
        rename = "StreamDescription",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_description: Option<StreamSpecification>,
    #[serde(
        rename = "TimeToLiveDescription",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub time_to_live_description: Option<TimeToLiveDescription>,
    #[serde(
        rename = "SSEDescription",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub sse_description: Option<SseDescription>,
}

impl SourceTableFeatureDetails {
    /// True when no member is set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.local_secondary_indexes.is_none()
            && self.global_secondary_indexes.is_none()
            && self.stream_description.is_none()
            && self.time_to_live_description.is_none()
            && self.sse_description.is_none()
    }
}

/// A global secondary index as `DescribeBackup` reports it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobalSecondaryIndexInfo {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(
        rename = "ProvisionedThroughput",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

/// A local secondary index as `DescribeBackup` reports it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalSecondaryIndexInfo {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
}

/// Summary of a backup for `ListBackups`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BackupSummary {
    pub backup_arn: String,
    pub backup_name: String,
    pub table_name: String,
    /// The source table's id, as `SourceTableDetails.TableId` reports it.
    #[serde(default)]
    pub table_id: String,
    pub table_arn: String,
    pub backup_status: String,
    pub backup_type: String,
    pub backup_size_bytes: i64,
    pub backup_creation_date_time: f64,
}

/// Continuous backups description for `DescribeContinuousBackups`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ContinuousBackupsDescription {
    pub continuous_backups_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point_in_time_recovery_description: Option<PointInTimeRecoveryDescription>,
}

/// PITR status.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PointInTimeRecoveryDescription {
    pub point_in_time_recovery_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub earliest_restorable_date_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_restorable_date_time: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyType, ProjectionType, TimeToLiveStatus};

    #[test]
    fn feature_details_use_the_service_member_names() {
        let details = SourceTableFeatureDetails {
            local_secondary_indexes: Some(vec![LocalSecondaryIndexInfo {
                index_name: "l".to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                }],
                projection: Projection {
                    projection_type: ProjectionType::All,
                    non_key_attributes: None,
                },
            }]),
            global_secondary_indexes: Some(vec![GlobalSecondaryIndexInfo {
                index_name: "g".to_owned(),
                key_schema: vec![],
                projection: Projection {
                    projection_type: ProjectionType::KeysOnly,
                    non_key_attributes: None,
                },
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 0,
                    write_capacity_units: 0,
                }),
            }]),
            stream_description: Some(StreamSpecification {
                stream_enabled: true,
                stream_view_type: Some(crate::types::StreamViewType::NewImage),
            }),
            time_to_live_description: Some(TimeToLiveDescription {
                time_to_live_status: TimeToLiveStatus::Enabled,
                attribute_name: Some("ttl".to_owned()),
            }),
            sse_description: None,
        };
        let v = serde_json::to_value(&details).unwrap();
        assert_eq!(v["LocalSecondaryIndexes"][0]["IndexName"], "l");
        assert_eq!(
            v["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"]["ReadCapacityUnits"],
            0
        );
        assert_eq!(v["StreamDescription"]["StreamViewType"], "NEW_IMAGE");
        assert_eq!(v["TimeToLiveDescription"]["TimeToLiveStatus"], "ENABLED");
        assert_eq!(v["TimeToLiveDescription"]["AttributeName"], "ttl");
        assert!(v.get("SSEDescription").is_none());
    }

    #[test]
    fn summary_carries_table_id() {
        let s = BackupSummary {
            table_id: "tid".to_owned(),
            ..Default::default()
        };
        assert_eq!(serde_json::to_value(&s).unwrap()["TableId"], "tid");
    }
}
