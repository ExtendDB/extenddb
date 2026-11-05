// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl BulkOperationStore for FaultingStorage {
    fn start_bulk_operation(
        &self,
        account_id: &str,
        req: NewBulkOperation,
        account_limit: u32,
    ) -> BoxFuture<'_, Result<StartOutcome, StorageError>> {
        self.inner
            .start_bulk_operation(account_id, req, account_limit)
    }

    fn get_bulk_operation(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<Option<BulkOperationRecord>, StorageError>> {
        let doctored = self.doctor_status.clone();
        let call = self.get_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let fail_now = self.fail_get_on_call == Some(call);
        let fut = self.inner.get_bulk_operation(account_id, operation_id);
        Box::pin(async move {
            if fail_now {
                return Err(StorageError::Internal(
                    "injected read failure at the lease re-read".to_owned(),
                ));
            }
            let mut record = fut.await?;
            if let (Some(status), Some(r)) = (doctored, record.as_mut()) {
                r.status = status;
            }
            Ok(record)
        })
    }

    fn list_bulk_operations(
        &self,
        account_id: &str,
        limit: u32,
        after: Option<ListCursor>,
    ) -> BoxFuture<'_, BulkOperationListResult> {
        self.inner.list_bulk_operations(account_id, limit, after)
    }

    fn count_non_terminal(&self, account_id: &str) -> BoxFuture<'_, Result<u64, StorageError>> {
        self.inner.count_non_terminal(account_id)
    }

    fn count_non_terminal_for_table(
        &self,
        account_id: &str,
        table_id: &str,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        self.inner
            .count_non_terminal_for_table(account_id, table_id)
    }

    fn count_terminal_born_since(
        &self,
        account_id: &str,
        since_ms: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        self.inner.count_terminal_born_since(account_id, since_ms)
    }

    fn request_cancel(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<CancelOutcome, StorageError>> {
        self.inner.request_cancel(account_id, operation_id)
    }

    fn claim_runnable(
        &self,
        owner: &str,
        lease: Duration,
        max: usize,
        exclude_accounts: &[String],
        exclude_operations: &[String],
    ) -> BoxFuture<'_, Result<Vec<BulkOperationRecord>, StorageError>> {
        self.inner
            .claim_runnable(owner, lease, max, exclude_accounts, exclude_operations)
    }

    fn checkpoint(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        lease: Duration,
        progress: Progress,
    ) -> BoxFuture<'_, Result<LeaseState, StorageError>> {
        if self.fail_checkpoint_once && !self.checkpoint_fail_fired.swap(true, Ordering::SeqCst) {
            return Box::pin(async {
                Err(StorageError::Transient(
                    "injected checkpoint failure".to_owned(),
                ))
            });
        }
        if let Some(notify) = &self.pend_checkpoint
            && !self.checkpoint_pend_fired.swap(true, Ordering::SeqCst)
        {
            let notify = Arc::clone(notify);
            return Box::pin(async move {
                notify.notify_one();
                std::future::pending::<()>().await;
                unreachable!("pending never resolves")
            });
        }
        let lease = self.checkpoint_lease.unwrap_or(lease);
        self.inner
            .checkpoint(account_id, operation_id, owner, lease, progress)
    }

    fn report_records_page(
        &self,
        account_id: &str,
        operation_id: &str,
        kind: ReportKind,
        after: Option<(i64, i64)>,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<ReportRecord>, StorageError>> {
        self.inner
            .report_records_page(account_id, operation_id, kind, after, limit)
    }

    fn finish(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        outcome: Terminal,
    ) -> BoxFuture<'_, Result<FinishState, StorageError>> {
        if let Some(notify) = &self.pend_finish
            && !self.finish_fired.swap(true, Ordering::SeqCst)
        {
            let notify = Arc::clone(notify);
            return Box::pin(async move {
                notify.notify_one();
                std::future::pending::<()>().await;
                unreachable!("pending never resolves")
            });
        }
        self.inner.finish(account_id, operation_id, owner, outcome)
    }

    fn list_purgeable_before(
        &self,
        cutoff: SystemTime,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        self.inner.list_purgeable_before(cutoff, limit)
    }

    fn purge_operations(
        &self,
        pairs: &[(String, String)],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let fut = self.inner.purge_operations(pairs);
        let calls = Arc::clone(&self.purge_calls);
        Box::pin(async move {
            let result = fut.await;
            calls.fetch_add(1, Ordering::SeqCst);
            result
        })
    }
}

impl TableEngine for FaultingStorage {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        self.inner.create_table(account_id, input)
    }

    fn delete_table(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        self.inner.delete_table(account_id, input)
    }

    fn describe_table(
        &self,
        account_id: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        self.inner.describe_table(account_id, input)
    }

    fn list_tables(
        &self,
        account_id: &str,
        input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        self.inner.list_tables(account_id, input)
    }

    fn update_table(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        self.inner.update_table(account_id, input)
    }

    fn table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        self.inner.table_key_info(account_id, table_name)
    }

    fn index_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        self.inner.index_info(account_id, table_name, index_name)
    }

    fn index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        self.inner.index_info_by_table_id(table_id, index_name)
    }
}

impl MetadataEngine for FaultingStorage {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        self.inner.describe_ttl(account_id, table_name)
    }

    fn update_ttl(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner
            .update_ttl(account_id, table_name, attribute_name, enabled)
    }

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.tag_resource(arn, tags)
    }

    fn untag_resource(
        &self,
        arn: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.untag_resource(arn, tag_keys)
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        self.inner.list_tags(arn)
    }

    fn tables_with_ttl(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        self.inner.tables_with_ttl(account_id)
    }

    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        self.inner.all_tables_with_ttl()
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        self.inner.all_tables_with_ttl_index_ready()
    }

    fn create_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner
            .create_ttl_index(account_id, table_name, ttl_attribute)
    }

    fn drop_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner
            .drop_ttl_index(account_id, table_name, ttl_attribute)
    }

    fn find_expired_items_indexed(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        self.inner
            .find_expired_items_indexed(account_id, table_name, ttl_attribute, limit)
    }

    fn refresh_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.refresh_table_size(account_id, table_name)
    }

    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        self.inner.list_active_table_names(account_id)
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        self.inner.all_active_tables()
    }
}

impl StreamEngine for FaultingStorage {
    fn write_stream_record(
        &self,
        account_id: &str,
        record: &StreamRecord,
        shard_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner
            .write_stream_record(account_id, record, shard_id, table_name)
    }

    fn get_stream_records(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        self.inner
            .get_stream_records(account_id, stream_arn, shard_id, after_sequence, limit)
    }

    fn describe_stream(
        &self,
        account_id: &str,
        input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        self.inner.describe_stream(account_id, input)
    }

    fn list_streams(
        &self,
        account_id: &str,
        table_name: Option<&str>,
        limit: i64,
        exclusive_start_stream_arn: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult> {
        self.inner
            .list_streams(account_id, table_name, limit, exclusive_start_stream_arn)
    }

    fn cleanup_expired_stream_records(
        &self,
        retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        self.inner.cleanup_expired_stream_records(retention_hours)
    }

    fn assign_shard(
        &self,
        account_id: &str,
        table_name: &str,
        partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        self.inner
            .assign_shard(account_id, table_name, partition_key)
    }

    fn next_sequence_number(&self, shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        self.inner.next_sequence_number(shard_id)
    }

    fn validate_shard(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.validate_shard(account_id, stream_arn, shard_id)
    }

    fn latest_sequence_number(
        &self,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        self.inner.latest_sequence_number(shard_id)
    }
}

impl extenddb_storage::BackupCatalog for FaultingStorage {
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>> {
        extenddb_storage::BackupCatalog::catalog_id(&*self.inner)
    }

    fn claim_backup<'a>(
        &'a self,
        row: &'a extenddb_storage::backup_catalog::BackupRow,
    ) -> BoxFuture<'a, Result<Box<dyn extenddb_storage::JobClaim>, StorageError>> {
        extenddb_storage::BackupCatalog::claim_backup(&*self.inner, row)
    }

    fn list_backup_rows<'a>(
        &'a self,
        status: Option<extenddb_storage::backup_catalog::BackupStatus>,
        store: &'a extenddb_storage::backup_catalog::StoreIdentity,
    ) -> futures::stream::BoxStream<
        'a,
        Result<extenddb_storage::backup_catalog::BackupRow, StorageError>,
    > {
        extenddb_storage::BackupCatalog::list_backup_rows(&*self.inner, status, store)
    }

    fn list_account_backup_rows<'a>(
        &'a self,
        account_id: &'a str,
        table_name: Option<&'a str>,
        bounds: &'a extenddb_storage::backup_catalog::TimeBounds,
    ) -> futures::stream::BoxStream<
        'a,
        Result<extenddb_storage::backup_catalog::BackupRow, StorageError>,
    > {
        extenddb_storage::BackupCatalog::list_account_backup_rows(
            &*self.inner,
            account_id,
            table_name,
            bounds,
        )
    }

    fn read_backup_row<'a>(
        &'a self,
        arn: &'a extenddb_core::backup_format::BackupArn,
    ) -> BoxFuture<'a, Result<Option<extenddb_storage::backup_catalog::BackupRow>, StorageError>>
    {
        extenddb_storage::BackupCatalog::read_backup_row(&*self.inner, arn)
    }

    fn start_backup_delete<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::BackupKey,
    ) -> BoxFuture<'a, Result<extenddb_storage::backup_catalog::DeleteStart, StorageError>> {
        extenddb_storage::BackupCatalog::start_backup_delete(&*self.inner, key)
    }

    fn set_backup_deleted<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::set_backup_deleted(&*self.inner, key)
    }

    fn remove_backup_row<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::remove_backup_row(&*self.inner, key)
    }

    fn backup_in_use<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::BackupKey,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        extenddb_storage::BackupCatalog::backup_in_use(&*self.inner, key)
    }

    fn remove_restore_target<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::remove_restore_target(&*self.inner, key)
    }

    fn list_dead_restore_jobs(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::backup_catalog::RestoreJobRow>, StorageError>>
    {
        extenddb_storage::BackupCatalog::list_dead_restore_jobs(&*self.inner, stale_after)
    }

    fn fail_restore_job<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
        owner_generation: u64,
        heartbeat_at: f64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::fail_restore_job(
            &*self.inner,
            key,
            owner_generation,
            heartbeat_at,
            reason,
        )
    }

    fn check_maintenance_claim(&self) -> BoxFuture<'_, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::check_maintenance_claim(&*self.inner)
    }
}

impl extenddb_storage::LegacyBackupReader for FaultingStorage {
    fn create(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
        permit: extenddb_storage::JobPermit,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDetails, StorageError>> {
        extenddb_storage::LegacyBackupReader::create(
            &*self.inner,
            account_id,
            table_name,
            backup_name,
            permit,
        )
    }

    fn describe(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDescription, StorageError>> {
        extenddb_storage::LegacyBackupReader::describe(&*self.inner, account_id, backup_arn)
    }

    fn list(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<extenddb_core::types::BackupSummary>, StorageError>> {
        extenddb_storage::LegacyBackupReader::list(&*self.inner, account_id, table_name)
    }

    fn delete(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDescription, StorageError>> {
        extenddb_storage::LegacyBackupReader::delete(&*self.inner, account_id, backup_arn)
    }

    fn restore_source(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<extenddb_storage::BackupRestoreSource, StorageError>> {
        extenddb_storage::LegacyBackupReader::restore_source(&*self.inner, account_id, backup_arn)
    }

    fn restore(
        &self,
        account_id: &str,
        request: extenddb_storage::RestoreRequest,
        permit: extenddb_storage::JobPermit,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        extenddb_storage::LegacyBackupReader::restore(&*self.inner, account_id, request, permit)
    }

    fn abandoned_creating_backups(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<
        '_,
        Result<Vec<extenddb_storage::backup_catalog::AbandonedLegacyBackup>, StorageError>,
    > {
        extenddb_storage::LegacyBackupReader::abandoned_creating_backups(&*self.inner, stale_after)
    }
}

impl extenddb_storage::SnapshotEngine for FaultingStorage {
    fn open_table_snapshot(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<extenddb_storage::snapshot::TableSnapshot, StorageError>> {
        self.inner.open_table_snapshot(account_id, table_name)
    }
}

impl WorkerStore for FaultingStorage {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        self.inner.process_control_plane_transitions()
    }
}
