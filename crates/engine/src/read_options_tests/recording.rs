// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A storage double for the read-options tests: records every `get_item`,
//! `query` and `scan` call with the [`ReadOptions`] it received and answers
//! with no items. `table_key_info`, `index_info` and `describe_table` answer
//! from the fixtures; every other method is unreachable from the handlers
//! under test and says so.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use extenddb_storage::BoxedFuture as BoxFuture;

use super::fixtures::{ACCOUNT, EVENTS, ORDERS, USERS, gsi_info, lsi_info, table_info};
use extenddb_core::backup_format::BackupArn;
use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, CreateTableInput, DeleteTableInput,
    DescribeStreamInput, DescribeTableInput, IndexInfo, Item, ListTablesInput, ListTablesOutput,
    StreamDescription, StreamRecord, TableDescription, TableKeyInfo, Tag, TimeToLiveDescription,
    UpdateTableInput,
};
use extenddb_storage::backup_catalog::{
    AbandonedLegacyBackup, BackupKey, BackupRow, BackupStatus, DeleteStart, ForgetRow, JobClaim,
    JobStatus, LegacyBackupItems, LegacyBackupKey, MaintenanceClaim, Replaced, RestoreJobRow,
    RestoreRegistered, RestoreRegistrationRow, RestoreTargetKey, StaleClock, StoreIdentity,
    StoreKind, StoreRootCount, TimeBounds,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupCatalog, BackupRestoreSource, BulkLoadFile, DataEngine, Finished, IdempotencyKey,
    ItemPairResult, JobPermit, LegacyBackupReader, MetadataEngine, QueryResult, ReadOptions,
    RestoreEngine, RestoreRequest, SnapshotEngine, StreamCapture, StreamEngine, StreamListResult,
    StreamRecordsResult, TableEngine, TableSnapshot, TransactGetOp, TransactWriteOp, TtlTableInfo,
    WorkerStore,
};
use futures::stream::BoxStream;

/// One read that reached storage.
#[derive(Debug, Clone)]
pub(super) struct ReadCall {
    pub(super) op: &'static str,
    pub(super) table: String,
    pub(super) index_name: Option<String>,
    pub(super) opts: ReadOptions,
}

/// A storage engine that records the read calls it receives.
///
/// `table_key_info`, `index_info` (through `index_info_by_table_id`),
/// `describe_table` (the export handler describes the table before it scans
/// it) and the three read methods have behaviour; everything else is
/// unreachable from the handlers under test and panics if called.
pub(super) struct RecordingStorage {
    tables: HashMap<String, TableKeyInfo>,
    indexes: Vec<IndexInfo>,
    reads: Mutex<Vec<ReadCall>>,
}

impl RecordingStorage {
    pub(super) fn new() -> Arc<Self> {
        let mut tables = HashMap::new();
        for name in [ORDERS, USERS, EVENTS] {
            tables.insert(name.to_owned(), table_info(name));
        }
        Arc::new(Self {
            tables,
            indexes: vec![gsi_info(), lsi_info()],
            reads: Mutex::new(Vec::new()),
        })
    }

    fn record(
        &self,
        op: &'static str,
        key_info: &TableKeyInfo,
        index_name: Option<&str>,
        opts: ReadOptions,
    ) {
        self.reads.lock().unwrap().push(ReadCall {
            op,
            table: key_info.table_name.clone(),
            index_name: index_name.map(str::to_owned),
            opts,
        });
    }

    pub(super) fn reads(&self) -> Vec<ReadCall> {
        self.reads.lock().unwrap().clone()
    }
}

impl TableEngine for RecordingStorage {
    fn create_table(
        &self,
        _account_id: &str,
        _input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn delete_table(
        &self,
        _account_id: &str,
        _input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn describe_table(
        &self,
        _account_id: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        // The export handler describes the table before it scans it.
        let description = TableDescription {
            table_name: input.table_name,
            ..TableDescription::default()
        };
        Box::pin(async move { Ok(description) })
    }

    fn list_tables(
        &self,
        _account_id: &str,
        _input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn update_table(
        &self,
        _account_id: &str,
        _input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        let result = if account_id == ACCOUNT {
            self.tables
                .get(table_name)
                .cloned()
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))
        } else {
            Err(StorageError::TableNotFound(table_name.to_owned()))
        };
        Box::pin(async move { result })
    }

    fn index_info(
        &self,
        _account_id: &str,
        _table_name: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        self.index_info_by_table_id("", index_name)
    }

    fn index_info_by_table_id(
        &self,
        _table_id: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let result = self
            .indexes
            .iter()
            .find(|idx| idx.index_name == index_name)
            .cloned()
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()));
        Box::pin(async move { result })
    }
}

impl DataEngine for RecordingStorage {
    fn put_item(
        &self,
        _key_info: &TableKeyInfo,
        _item: Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        _key: &Item,
        opts: ReadOptions,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        self.record("GetItem", key_info, None, opts);
        Box::pin(async { Ok(None) })
    }

    fn delete_item(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn update_item(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
        _actions: &[UpdateAction],
        _return_old: bool,
        _return_new: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult> {
        unimplemented!("not reached by read handlers")
    }

    fn query(
        &self,
        key_info: &TableKeyInfo,
        _key_condition: &KeyCondition,
        _maps: &ExpressionMaps,
        _forward: bool,
        _limit: Option<i64>,
        _exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
        opts: ReadOptions,
    ) -> BoxFuture<'_, QueryResult> {
        self.record("Query", key_info, index_name, opts);
        Box::pin(async { Ok((Vec::new(), None)) })
    }

    fn scan(
        &self,
        key_info: &TableKeyInfo,
        _limit: Option<i64>,
        _exclusive_start_key: Option<&Item>,
        _segment: Option<i64>,
        _total_segments: Option<i64>,
        index_name: Option<&str>,
        opts: ReadOptions,
    ) -> BoxFuture<'_, QueryResult> {
        self.record("Scan", key_info, index_name, opts);
        Box::pin(async { Ok((Vec::new(), None)) })
    }

    fn scan_key_in_segment(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
        _segment: i64,
        _total_segments: i64,
        _index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn transact_get_items(
        &self,
        _ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn transact_write_items(
        &self,
        _ops: &[TransactWriteOp<'_>],
        _idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        _max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unimplemented!("not reached by read handlers")
    }
}

impl MetadataEngine for RecordingStorage {
    fn describe_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn update_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
        _attribute_name: &str,
        _enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn tag_resource(&self, _arn: &str, _tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn untag_resource(
        &self,
        _arn: &str,
        _tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_tags(&self, _arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn tables_with_ttl(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn create_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn drop_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn find_expired_items_indexed(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn refresh_table_size(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_active_table_names(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }
}

impl StreamEngine for RecordingStorage {
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn get_stream_records(
        &self,
        _account_id: &str,
        _stream_arn: &str,
        _shard_id: &str,
        _after_sequence: Option<&str>,
        _limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        unimplemented!("not reached by read handlers")
    }

    fn describe_stream(
        &self,
        _account_id: &str,
        _input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_streams(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
        _limit: i64,
        _exclusive_start_stream_arn: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult> {
        unimplemented!("not reached by read handlers")
    }

    fn cleanup_expired_stream_records(
        &self,
        _retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn assign_shard(
        &self,
        _account_id: &str,
        _table_name: &str,
        _partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn validate_shard(
        &self,
        _account_id: &str,
        _stream_arn: &str,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn latest_sequence_number(
        &self,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn first_sequence_number_at_or_after(
        &self,
        _shard_id: &str,
        _at_ms: i64,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }
}

impl WorkerStore for RecordingStorage {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }
}

impl BackupCatalog for RecordingStorage {
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn claim_backup<'a>(
        &'a self,
        _row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Box<dyn JobClaim>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_backup_rows<'a>(
        &'a self,
        _status: Option<BackupStatus>,
        _store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_account_backup_rows<'a>(
        &'a self,
        _account_id: &'a str,
        _table_name: Option<&'a str>,
        _bounds: &'a TimeBounds,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn read_backup_row<'a>(
        &'a self,
        _arn: &'a BackupArn,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn start_backup_delete<'a>(
        &'a self,
        _key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<DeleteStart, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn set_backup_deleted<'a>(
        &'a self,
        _key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn remove_backup_row<'a>(
        &'a self,
        _key: &'a BackupKey,
        _store_kind: StoreKind,
        _store_root: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn insert_backup_row<'a>(
        &'a self,
        _row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_backup_rows_by_root<'a>(
        &'a self,
        _store_kind: StoreKind,
        _store_root: &'a str,
        _status: Option<BackupStatus>,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn newest_available_backup_row<'a>(
        &'a self,
        _store: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn row_holding_backup_id<'a>(
        &'a self,
        _backup_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn forget_available_backup_row<'a>(
        &'a self,
        _key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<ForgetRow, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn rewrite_store<'a>(
        &'a self,
        _from_kind: StoreKind,
        _from_root: &'a str,
        _keys: &'a [BackupKey],
        _to: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        _key: &'a BackupKey,
        _stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn claim_dead_backup<'a>(
        &'a self,
        _key: &'a BackupKey,
        _stale_after: Duration,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_backup_rows_older_than<'a>(
        &'a self,
        _status: BackupStatus,
        _store: &'a StoreIdentity,
        _clock: StaleClock,
        _age: Duration,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn backup_in_use<'a>(
        &'a self,
        _key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn remove_restore_target<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_dead_restore_jobs(
        &self,
        _stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_running_restore_jobs(&self) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn fail_restore_job<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
        _owner_generation: u64,
        _heartbeat_at: f64,
        _reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn claim_restore<'a>(
        &'a self,
        _account_id: &'a str,
        _input: CreateTableInput,
        _registration: &'a RestoreRegistrationRow,
    ) -> BoxFuture<'a, Result<RestoreRegistered, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn claim_restore_dead<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
        _stale_after: Duration,
        _spool_host: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn read_restore_job<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Option<RestoreJobRow>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_restore_jobs<'a>(
        &'a self,
        _status: Option<JobStatus>,
    ) -> BoxFuture<'a, Result<Vec<RestoreJobRow>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn read_store_ids<'a>(
        &'a self,
        _store_kind: StoreKind,
        _store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn summarize_store_roots(&self) -> BoxFuture<'_, Result<Vec<StoreRootCount>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn maintenance_claim<'a>(
        &'a self,
        _holder: &'a str,
        _stale_after: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn MaintenanceClaim>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn check_maintenance_claim(
        &self,
        _stale_after: Duration,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }
}

impl RestoreEngine for RecordingStorage {
    fn bulk_load<'a>(
        &'a self,
        _target: &'a RestoreTargetKey,
        _claim: &'a dyn JobClaim,
        _file: BulkLoadFile,
        _items: BoxStream<'a, Result<Item, StorageError>>,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn finish_restore<'a>(
        &'a self,
        _target: &'a RestoreTargetKey,
        _claim: &'a dyn JobClaim,
        _item_count: u64,
        _size_bytes: u64,
    ) -> BoxFuture<'a, Result<Finished, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn count_restore_target<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn restore_target_ready<'a>(
        &'a self,
        _key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }
}

impl LegacyBackupReader for RecordingStorage {
    fn create(
        &self,
        _account_id: &str,
        _table_name: &str,
        _backup_name: &str,
        _permit: JobPermit,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn describe(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn delete(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn restore_source(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupRestoreSource, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn restore(
        &self,
        _account_id: &str,
        _request: RestoreRequest,
        _permit: JobPermit,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn abandoned_creating_backups(
        &self,
        _stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<AbandonedLegacyBackup>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_all(&self) -> BoxFuture<'_, Result<Vec<LegacyBackupKey>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn list_deleted(&self) -> BoxFuture<'_, Result<Vec<LegacyBackupKey>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn holder_status<'a>(
        &'a self,
        _account_id: &'a str,
        _backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<Option<BackupStatus>, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn remove_deleted<'a>(
        &'a self,
        _account_id: &'a str,
        _backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn items(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<LegacyBackupItems, StorageError>> {
        unimplemented!("not reached by read handlers")
    }

    fn replace_with_index_row<'a>(
        &'a self,
        _row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Replaced, StorageError>> {
        unimplemented!("not reached by read handlers")
    }
}

impl SnapshotEngine for RecordingStorage {
    fn open_table_snapshot(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<TableSnapshot, StorageError>> {
        unimplemented!("not reached by read handlers")
    }
}
