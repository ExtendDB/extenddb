// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BackupEngine` implementation for the DynamoDB-at-home backend — honest stubs.
//!
//! DynamoDB has a native backup and PITR API. This v1 backend does not implement
//! it. Each method names the DynamoDB API call it would map to.

use futures::future::BoxFuture;

use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, ContinuousBackupsDescription, TableDescription,
};
use extenddb_storage::BackupEngine;
use extenddb_storage::error::StorageError;

use crate::DynamoEngine;

impl BackupEngine for DynamoEngine {
    fn create_backup(
        &self,
        _account_id: &str,
        _table_name: &str,
        _backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        // Maps to DynamoDB CreateBackup.
        Box::pin(async {
            Err(StorageError::Internal(
                "Backups are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB CreateBackup."
                    .into(),
            ))
        })
    }

    fn describe_backup(
        &self,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        // Maps to DynamoDB DescribeBackup.
        Box::pin(async {
            Err(StorageError::Internal(
                "Backups are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB DescribeBackup."
                    .into(),
            ))
        })
    }

    fn list_backups(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        // Maps to DynamoDB ListBackups.
        Box::pin(async {
            Err(StorageError::Internal(
                "Backups are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB ListBackups."
                    .into(),
            ))
        })
    }

    fn delete_backup(
        &self,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        // Maps to DynamoDB DeleteBackup.
        Box::pin(async {
            Err(StorageError::Internal(
                "Backups are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB DeleteBackup."
                    .into(),
            ))
        })
    }

    fn restore_table_from_backup(
        &self,
        _account_id: &str,
        _target_table_name: &str,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        // Maps to DynamoDB RestoreTableFromBackup.
        Box::pin(async {
            Err(StorageError::Internal(
                "Backups are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB RestoreTableFromBackup."
                    .into(),
            ))
        })
    }

    fn describe_continuous_backups(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        // Maps to DynamoDB DescribeContinuousBackups.
        Box::pin(async {
            Err(StorageError::Internal(
                "Backups are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB DescribeContinuousBackups."
                    .into(),
            ))
        })
    }

    fn update_continuous_backups(
        &self,
        _account_id: &str,
        _table_name: &str,
        _pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        // Maps to DynamoDB UpdateContinuousBackups.
        Box::pin(async {
            Err(StorageError::Internal(
                "Backups are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB UpdateContinuousBackups."
                    .into(),
            ))
        })
    }

    fn restore_table_to_point_in_time(
        &self,
        _account_id: &str,
        _source_table_name: &str,
        _target_table_name: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        // Maps to DynamoDB RestoreTableToPointInTime.
        Box::pin(async {
            Err(StorageError::Internal(
                "Backups are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB RestoreTableToPointInTime."
                    .into(),
            ))
        })
    }
}
