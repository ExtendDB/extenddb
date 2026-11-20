// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

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

    fn newest_available_backup_row<'a>(
        &'a self,
        store: &'a extenddb_storage::backup_catalog::StoreIdentity,
    ) -> BoxFuture<'a, Result<Option<extenddb_storage::backup_catalog::BackupRow>, StorageError>>
    {
        extenddb_storage::BackupCatalog::newest_available_backup_row(&*self.inner, store)
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
        store_kind: extenddb_storage::backup_catalog::StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::remove_backup_row(
            &*self.inner,
            key,
            store_kind,
            store_root,
        )
    }

    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::remove_dead_creating_backup_row(
            &*self.inner,
            key,
            stale_after,
        )
    }

    fn claim_dead_backup<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Option<Box<dyn extenddb_storage::JobClaim>>, StorageError>> {
        extenddb_storage::BackupCatalog::claim_dead_backup(&*self.inner, key, stale_after)
    }

    fn list_backup_rows_older_than<'a>(
        &'a self,
        status: extenddb_storage::backup_catalog::BackupStatus,
        store: &'a extenddb_storage::backup_catalog::StoreIdentity,
        clock: extenddb_storage::backup_catalog::StaleClock,
        age: Duration,
    ) -> futures::stream::BoxStream<
        'a,
        Result<extenddb_storage::backup_catalog::BackupRow, StorageError>,
    > {
        extenddb_storage::BackupCatalog::list_backup_rows_older_than(
            &*self.inner,
            status,
            store,
            clock,
            age,
        )
    }

    fn sweep_restore_registrations(&self) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        extenddb_storage::BackupCatalog::sweep_restore_registrations(&*self.inner)
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

    fn list_running_restore_jobs(
        &self,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::backup_catalog::RestoreJobRow>, StorageError>>
    {
        extenddb_storage::BackupCatalog::list_running_restore_jobs(&*self.inner)
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

    fn read_store_ids<'a>(
        &'a self,
        store_kind: extenddb_storage::backup_catalog::StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        extenddb_storage::BackupCatalog::read_store_ids(&*self.inner, store_kind, store_root)
    }

    fn maintenance_claim<'a>(
        &'a self,
        holder: &'a str,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn extenddb_storage::MaintenanceClaim>, StorageError>> {
        extenddb_storage::BackupCatalog::maintenance_claim(&*self.inner, holder, stale_after)
    }

    fn check_maintenance_claim(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::check_maintenance_claim(&*self.inner, stale_after)
    }

    fn insert_backup_row<'a>(
        &'a self,
        row: &'a extenddb_storage::backup_catalog::BackupRow,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::BackupCatalog::insert_backup_row(&*self.inner, row)
    }

    fn list_backup_rows_by_root<'a>(
        &'a self,
        store_kind: extenddb_storage::backup_catalog::StoreKind,
        store_root: &'a str,
        status: Option<extenddb_storage::backup_catalog::BackupStatus>,
    ) -> futures::stream::BoxStream<
        'a,
        Result<extenddb_storage::backup_catalog::BackupRow, StorageError>,
    > {
        extenddb_storage::BackupCatalog::list_backup_rows_by_root(
            &*self.inner,
            store_kind,
            store_root,
            status,
        )
    }

    fn row_holding_backup_id<'a>(
        &'a self,
        backup_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<extenddb_storage::backup_catalog::BackupRow>, StorageError>>
    {
        extenddb_storage::BackupCatalog::row_holding_backup_id(&*self.inner, backup_id)
    }

    fn forget_available_backup_row<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::BackupKey,
    ) -> BoxFuture<'a, Result<extenddb_storage::backup_catalog::ForgetRow, StorageError>> {
        extenddb_storage::BackupCatalog::forget_available_backup_row(&*self.inner, key)
    }

    fn rewrite_store<'a>(
        &'a self,
        from_kind: extenddb_storage::backup_catalog::StoreKind,
        from_root: &'a str,
        keys: &'a [extenddb_storage::backup_catalog::BackupKey],
        to: &'a extenddb_storage::backup_catalog::StoreIdentity,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        extenddb_storage::BackupCatalog::rewrite_store(&*self.inner, from_kind, from_root, keys, to)
    }

    fn claim_restore<'a>(
        &'a self,
        account_id: &'a str,
        input: extenddb_core::types::CreateTableInput,
        registration: &'a extenddb_storage::backup_catalog::RestoreRegistrationRow,
    ) -> BoxFuture<'a, Result<extenddb_storage::backup_catalog::RestoreRegistered, StorageError>>
    {
        extenddb_storage::BackupCatalog::claim_restore(
            &*self.inner,
            account_id,
            input,
            registration,
        )
    }

    fn claim_restore_dead<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
        stale_after: Duration,
        spool_host: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<Box<dyn extenddb_storage::JobClaim>>, StorageError>> {
        extenddb_storage::BackupCatalog::claim_restore_dead(
            &*self.inner,
            key,
            stale_after,
            spool_host,
        )
    }

    fn read_restore_job<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Option<extenddb_storage::backup_catalog::RestoreJobRow>, StorageError>>
    {
        extenddb_storage::BackupCatalog::read_restore_job(&*self.inner, key)
    }

    fn list_restore_jobs<'a>(
        &'a self,
        status: Option<extenddb_storage::backup_catalog::JobStatus>,
    ) -> BoxFuture<'a, Result<Vec<extenddb_storage::backup_catalog::RestoreJobRow>, StorageError>>
    {
        extenddb_storage::BackupCatalog::list_restore_jobs(&*self.inner, status)
    }

    fn summarize_store_roots(
        &self,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::backup_catalog::StoreRootCount>, StorageError>>
    {
        extenddb_storage::BackupCatalog::summarize_store_roots(&*self.inner)
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

    fn list_all(
        &self,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::backup_catalog::LegacyBackupKey>, StorageError>>
    {
        extenddb_storage::LegacyBackupReader::list_all(&*self.inner)
    }

    fn list_deleted(
        &self,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::backup_catalog::LegacyBackupKey>, StorageError>>
    {
        extenddb_storage::LegacyBackupReader::list_deleted(&*self.inner)
    }

    fn holder_status<'a>(
        &'a self,
        account_id: &'a str,
        backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<Option<extenddb_storage::backup_catalog::BackupStatus>, StorageError>>
    {
        extenddb_storage::LegacyBackupReader::holder_status(&*self.inner, account_id, backup_arn)
    }

    fn remove_deleted<'a>(
        &'a self,
        account_id: &'a str,
        backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::LegacyBackupReader::remove_deleted(&*self.inner, account_id, backup_arn)
    }

    fn items(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<extenddb_storage::backup_catalog::LegacyBackupItems, StorageError>>
    {
        extenddb_storage::LegacyBackupReader::items(&*self.inner, account_id, backup_arn)
    }

    fn replace_with_index_row<'a>(
        &'a self,
        row: &'a extenddb_storage::backup_catalog::BackupRow,
    ) -> BoxFuture<'a, Result<extenddb_storage::backup_catalog::Replaced, StorageError>> {
        extenddb_storage::LegacyBackupReader::replace_with_index_row(&*self.inner, row)
    }
}

impl extenddb_storage::RestoreEngine for FaultingStorage {
    fn bulk_load<'a>(
        &'a self,
        target: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
        claim: &'a dyn extenddb_storage::JobClaim,
        file: extenddb_storage::BulkLoadFile,
        items: futures::stream::BoxStream<'a, Result<Item, StorageError>>,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        extenddb_storage::RestoreEngine::bulk_load(&*self.inner, target, claim, file, items)
    }

    fn finish_restore<'a>(
        &'a self,
        target: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
        claim: &'a dyn extenddb_storage::JobClaim,
        item_count: u64,
        size_bytes: u64,
    ) -> BoxFuture<'a, Result<extenddb_storage::Finished, StorageError>> {
        extenddb_storage::RestoreEngine::finish_restore(
            &*self.inner,
            target,
            claim,
            item_count,
            size_bytes,
        )
    }

    fn count_restore_target<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        extenddb_storage::RestoreEngine::count_restore_target(&*self.inner, key)
    }

    fn restore_target_ready<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        extenddb_storage::RestoreEngine::restore_target_ready(&*self.inner, key)
    }

    fn read_restore_progress<'a>(
        &'a self,
        key: &'a extenddb_storage::backup_catalog::RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Vec<(u32, u64)>, StorageError>> {
        extenddb_storage::RestoreEngine::read_restore_progress(&*self.inner, key)
    }

    fn sweep_restore_progress<'a>(
        &'a self,
        running: &'a [String],
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        extenddb_storage::RestoreEngine::sweep_restore_progress(&*self.inner, running)
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
