// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup, snapshot, and restore trait implementations for `BigtableEngine`.
//!
//! Cloud Bigtable does not provide cross-row point-in-time snapshot reads on
//! the data plane, so `CreateBackup` (and `open_table_snapshot`) are refused
//! with [`StorageError::Unsupported`]. All read/query/delete/restore lookups
//! (`ListBackups`, `DescribeBackup`, `DeleteBackup`, `RestoreTableFromBackup`)
//! answer as for a catalog with no backups: `ListBackups` returns an empty
//! list, and `DescribeBackup`, `DeleteBackup`, and `RestoreTableFromBackup`
//! return `BackupNotFoundException`.

use std::time::Duration;

use extenddb_core::backup_format::BackupArn;
use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, CreateTableInput, Item, TableDescription,
};
use extenddb_storage::backup_catalog::{
    AbandonedLegacyBackup, BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart,
    ForgetRow, JobClaim, JobPermit, JobStatus, LegacyBackupItems, LegacyBackupKey,
    LegacyBackupReader, MaintenanceClaim, Replaced, RestoreJobRow, RestoreRegistered,
    RestoreRegistrationRow, RestoreTargetKey, StaleClock, StoreIdentity, StoreKind, StoreRootCount,
    TimeBounds, backup_not_found,
};
use extenddb_storage::backup_definition::{BackupRestoreSource, RestoreRequest};
use extenddb_storage::error::StorageError;
use extenddb_storage::restore_engine::{BulkLoadFile, Finished, RestoreEngine};
use extenddb_storage::{SnapshotEngine, TableSnapshot};
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};

use crate::BigtableEngine;
use crate::catalog::keys;

/// Canonical message returned when `CreateBackup` is invoked on Bigtable.
pub const CREATE_BACKUP_UNSUPPORTED_REASON: &str =
    "CreateBackup is not supported by this storage backend";

fn create_unsupported<T>() -> BoxFuture<'static, Result<T, StorageError>> {
    Box::pin(async {
        Err(StorageError::Unsupported(
            CREATE_BACKUP_UNSUPPORTED_REASON.to_owned(),
        ))
    })
}

fn missing_backup<T>(arn: &str) -> BoxFuture<'static, Result<T, StorageError>> {
    let err = backup_not_found(arn);
    Box::pin(async move { Err(err) })
}

impl SnapshotEngine for BigtableEngine {
    fn open_table_snapshot(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<TableSnapshot, StorageError>> {
        create_unsupported()
    }
}

impl LegacyBackupReader for BigtableEngine {
    fn create(
        &self,
        _account_id: &str,
        _table_name: &str,
        _backup_name: &str,
        _permit: JobPermit,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        create_unsupported()
    }

    fn describe(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        missing_backup(backup_arn)
    }

    fn list(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn delete(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        missing_backup(backup_arn)
    }

    fn restore_source(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupRestoreSource, StorageError>> {
        missing_backup(backup_arn)
    }

    fn restore(
        &self,
        _account_id: &str,
        request: RestoreRequest,
        _permit: JobPermit,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        missing_backup(&request.backup_arn)
    }

    fn abandoned_creating_backups(
        &self,
        _stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<AbandonedLegacyBackup>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_all(&self) -> BoxFuture<'_, Result<Vec<LegacyBackupKey>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_deleted(&self) -> BoxFuture<'_, Result<Vec<LegacyBackupKey>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn holder_status<'a>(
        &'a self,
        _account_id: &'a str,
        _backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<Option<BackupStatus>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn remove_deleted<'a>(
        &'a self,
        _account_id: &'a str,
        _backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async { Ok(false) })
    }

    fn items(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<LegacyBackupItems, StorageError>> {
        missing_backup(backup_arn)
    }

    fn replace_with_index_row<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Replaced, StorageError>> {
        missing_backup(&row.backup_arn)
    }
}

impl BackupCatalog for BigtableEngine {
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move {
            let cat = self.cat();
            let setting_key = keys::setting("catalog_id");
            if let Some(val) = cat.get(&setting_key).await?
                && let Some(id) = val.get("value").and_then(serde_json::Value::as_str)
            {
                return Ok(id.to_owned());
            }
            let fresh = uuid::Uuid::new_v4().to_string();
            let _ = cat
                .put_if_absent(
                    &setting_key,
                    &serde_json::json!({ "key": "catalog_id", "value": fresh }),
                )
                .await?;
            let stored = cat
                .get(&setting_key)
                .await?
                .and_then(|v| {
                    v.get("value")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or(fresh);
            Ok(stored)
        })
    }

    fn claim_backup<'a>(
        &'a self,
        _row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Box<dyn JobClaim>, StorageError>> {
        create_unsupported()
    }

    fn insert_backup_row<'a>(
        &'a self,
        _row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        create_unsupported()
    }

    fn list_backup_rows<'a>(
        &'a self,
        _status: Option<BackupStatus>,
        _store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        Box::pin(stream::empty())
    }

    fn list_backup_rows_by_root<'a>(
        &'a self,
        _store_kind: StoreKind,
        _store_root: &'a str,
        _status: Option<BackupStatus>,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        Box::pin(stream::empty())
    }

    fn newest_available_backup_row<'a>(
        &'a self,
        _store: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn list_account_backup_rows<'a>(
        &'a self,
        _account_id: &'a str,
        _table_name: Option<&'a str>,
        _bounds: &'a TimeBounds,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        Box::pin(stream::empty())
    }

    fn read_backup_row<'a>(
        &'a self,
        _arn: &'a BackupArn,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn row_holding_backup_id<'a>(
        &'a self,
        _backup_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn start_backup_delete<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<DeleteStart, StorageError>> {
        missing_backup(&key.backup_arn)
    }

    fn set_backup_deleted<'a>(
        &'a self,
        _key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async { Ok(false) })
    }

    fn remove_backup_row<'a>(
        &'a self,
        _key: &'a BackupKey,
        _store_kind: StoreKind,
        _store_root: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async { Ok(false) })
    }

    fn forget_available_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<ForgetRow, StorageError>> {
        missing_backup(&key.backup_arn)
    }

    fn rewrite_store<'a>(
        &'a self,
        _from_kind: StoreKind,
        _from_root: &'a str,
        _keys: &'a [BackupKey],
        _to: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async { Ok(0) })
    }

    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        _key: &'a BackupKey,
        _stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async { Ok(false) })
    }

    fn claim_dead_backup<'a>(
        &'a self,
        _key: &'a BackupKey,
        _stale_after: Duration,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn list_backup_rows_older_than<'a>(
        &'a self,
        _status: BackupStatus,
        _store: &'a StoreIdentity,
        _clock: StaleClock,
        _age: Duration,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        Box::pin(stream::empty())
    }

    fn backup_in_use<'a>(
        &'a self,
        _key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn remove_restore_target<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async { Ok(false) })
    }

    fn list_dead_restore_jobs(
        &self,
        _stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_running_restore_jobs(&self) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fail_restore_job<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
        _owner_generation: u64,
        _heartbeat_at: f64,
        _reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async { Ok(false) })
    }

    fn claim_restore<'a>(
        &'a self,
        _account_id: &'a str,
        _input: CreateTableInput,
        registration: &'a RestoreRegistrationRow,
    ) -> BoxFuture<'a, Result<RestoreRegistered, StorageError>> {
        missing_backup(&registration.backup.backup_arn)
    }

    fn claim_restore_dead<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
        _stale_after: Duration,
        _spool_host: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn read_restore_job<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Option<RestoreJobRow>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn list_restore_jobs<'a>(
        &'a self,
        _status: Option<JobStatus>,
    ) -> BoxFuture<'a, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn read_store_ids<'a>(
        &'a self,
        _store_kind: StoreKind,
        _store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn summarize_store_roots(&self) -> BoxFuture<'_, Result<Vec<StoreRootCount>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn maintenance_claim<'a>(
        &'a self,
        _holder: &'a str,
        _stale_after: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn MaintenanceClaim>, StorageError>> {
        create_unsupported()
    }

    fn check_maintenance_claim(
        &self,
        _stale_after: Duration,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        Box::pin(async { Ok(false) })
    }
}

impl RestoreEngine for BigtableEngine {
    fn bulk_load<'a>(
        &'a self,
        _target: &'a RestoreTargetKey,
        _claim: &'a dyn JobClaim,
        _file: BulkLoadFile,
        _items: BoxStream<'a, Result<Item, StorageError>>,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        create_unsupported()
    }

    fn finish_restore<'a>(
        &'a self,
        _target: &'a RestoreTargetKey,
        _claim: &'a dyn JobClaim,
        _item_count: u64,
        _size_bytes: u64,
    ) -> BoxFuture<'a, Result<Finished, StorageError>> {
        create_unsupported()
    }

    fn count_restore_target<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async { Ok(0) })
    }

    fn restore_target_ready<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async { Ok(false) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::backup_catalog::is_backup_not_found;
    use futures::StreamExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn create_backup_is_unsupported_and_lookup_ops_act_as_empty_catalog() {
        let config = crate::config::BigtableStorageConfig {
            project_id: "test-proj".to_owned(),
            instance_id: "test-inst".to_owned(),
            emulator_host: Some("127.0.0.1:8086".to_owned()),
            ..crate::config::BigtableStorageConfig::default()
        };
        let client = Arc::new(
            crate::data::client::BigtableClient::connect(&config)
                .await
                .unwrap(),
        );
        let engine = BigtableEngine::new(client.clone(), client, 60);

        assert!(matches!(
            engine.open_table_snapshot("123456789012", "tbl").await,
            Err(StorageError::Unsupported(_))
        ));
        assert!(matches!(
            LegacyBackupReader::create(&engine, "123456789012", "tbl", "bk", JobPermit::detached())
                .await,
            Err(StorageError::Unsupported(_))
        ));

        let summaries = LegacyBackupReader::list(&engine, "123456789012", None)
            .await
            .expect("ListBackups succeeds with empty list");
        assert!(summaries.is_empty());

        let bounds = TimeBounds::default();
        let mut rows_stream =
            BackupCatalog::list_account_backup_rows(&engine, "123456789012", None, &bounds);
        assert!(rows_stream.next().await.is_none());

        let arn = BackupArn::parse(
            "arn:aws:dynamodb:us-east-1:123456789012:table/tbl/backup/01700000000000-01234567",
        )
        .unwrap();
        assert!(
            BackupCatalog::read_backup_row(&engine, &arn)
                .await
                .unwrap()
                .is_none()
        );

        let desc_err = LegacyBackupReader::describe(&engine, "123456789012", &arn.to_arn_string())
            .await
            .unwrap_err();
        assert!(is_backup_not_found(&desc_err), "{desc_err:?}");

        let del_err = LegacyBackupReader::delete(&engine, "123456789012", &arn.to_arn_string())
            .await
            .unwrap_err();
        assert!(is_backup_not_found(&del_err), "{del_err:?}");

        let restore_err =
            LegacyBackupReader::restore_source(&engine, "123456789012", &arn.to_arn_string())
                .await
                .unwrap_err();
        assert!(is_backup_not_found(&restore_err), "{restore_err:?}");
    }
}
