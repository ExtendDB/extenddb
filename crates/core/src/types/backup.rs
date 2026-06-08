// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Types for `DynamoDB` backup and point-in-time recovery operations.

use serde::{Deserialize, Serialize};

/// Backup details returned by `CreateBackup` and `DescribeBackup`.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BackupDescription {
    pub backup_details: BackupDetails,
    pub source_table_details: SourceTableDetails,
}

/// Source table details stored with a backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SourceTableDetails {
    pub table_name: String,
    pub table_id: String,
    pub table_arn: String,
    pub key_schema: Vec<super::key_schema::KeySchemaElement>,
    pub item_count: i64,
    pub table_size_bytes: i64,
    pub billing_mode: Option<String>,
    pub table_creation_date_time: f64,
}

/// Summary of a backup for `ListBackups`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BackupSummary {
    pub backup_arn: String,
    pub backup_name: String,
    pub table_name: String,
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
