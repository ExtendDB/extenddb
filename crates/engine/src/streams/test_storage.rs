// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A storage double for the stream handler tests. `validate_shard` accepts
//! every shard, `get_stream_records` answers an empty page and
//! `describe_stream` answers a description with no shards, counting its
//! calls. Every other method is unreachable from the stream handlers under
//! test and panics if called.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, ContinuousBackupsDescription,
    CreateTableInput, DeleteTableInput, DescribeStreamInput, DescribeTableInput, IndexInfo, Item,
    ListTablesInput, ListTablesOutput, StreamDescription, StreamRecord, StreamStatus,
    StreamViewType, TableDescription, TableKeyInfo, Tag, TimeToLiveDescription, UpdateTableInput,
};
use extenddb_storage::BoxedFuture as BoxFuture;
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupEngine, BackupRestoreSource, DataEngine, IdempotencyKey, ItemPairResult, MetadataEngine,
    QueryResult, RestoreRequest, StreamCapture, StreamEngine, StreamListResult,
    StreamRecordsResult, TableEngine, TransactGetOp, TransactWriteOp, TtlTableInfo, WorkerStore,
};

use crate::OperationContext;

/// The account the context's requests run as.
pub(super) const ACCOUNT: &str = "000000000000";

/// A stream ARN in [`ACCOUNT`]; the double accepts any shard of it.
pub(super) const STREAM_ARN: &str =
    "arn:aws:dynamodb:us-east-1:000000000000:table/Orders/stream/2026-10-22T00:00:00.000";

/// A storage engine with just enough stream behaviour for the handlers.
pub(super) struct StreamStorage {
    /// How many `describe_stream` calls reached the double.
    describe_calls: AtomicUsize,
}

impl StreamStorage {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            describe_calls: AtomicUsize::new(0),
        })
    }

    /// How many `describe_stream` calls reached storage.
    pub(super) fn describe_calls(&self) -> usize {
        self.describe_calls.load(Ordering::Relaxed)
    }
}

/// An operation context over `storage` for [`ACCOUNT`].
pub(super) fn context(storage: &Arc<StreamStorage>) -> OperationContext {
    let empty: Vec<Arc<PathBuf>> = Vec::new();
    OperationContext {
        storage: Arc::clone(storage) as Arc<dyn extenddb_storage::StorageEngine>,
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from(ACCOUNT),
        import_paths: Arc::from(empty.clone()),
        export_paths: Arc::from(empty),
        authorized_key_info: HashMap::new(),
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
    }
}

impl TableEngine for StreamStorage {
    fn create_table(
        &self,
        _account_id: &str,
        _input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn delete_table(
        &self,
        _account_id: &str,
        _input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn describe_table(
        &self,
        _account_id: &str,
        _input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn list_tables(
        &self,
        _account_id: &str,
        _input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn update_table(
        &self,
        _account_id: &str,
        _input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn table_key_info(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn index_info(
        &self,
        _account_id: &str,
        _table_name: &str,
        _index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn index_info_by_table_id(
        &self,
        _table_id: &str,
        _index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }
}

impl DataEngine for StreamStorage {
    fn put_item(
        &self,
        _key_info: &TableKeyInfo,
        _item: Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn get_item(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
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
        unimplemented!("not reached by the stream handlers")
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
        unimplemented!("not reached by the stream handlers")
    }

    fn query(
        &self,
        _key_info: &TableKeyInfo,
        _key_condition: &KeyCondition,
        _maps: &ExpressionMaps,
        _forward: bool,
        _limit: Option<i64>,
        _exclusive_start_key: Option<&Item>,
        _index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        unimplemented!("not reached by the stream handlers")
    }

    fn scan(
        &self,
        _key_info: &TableKeyInfo,
        _limit: Option<i64>,
        _exclusive_start_key: Option<&Item>,
        _segment: Option<i64>,
        _total_segments: Option<i64>,
        _index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        unimplemented!("not reached by the stream handlers")
    }

    fn scan_key_in_segment(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
        _segment: i64,
        _total_segments: i64,
        _index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn transact_get_items(
        &self,
        _ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn transact_write_items(
        &self,
        _ops: &[TransactWriteOp<'_>],
        _idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        _max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }
}

impl MetadataEngine for StreamStorage {
    fn describe_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn update_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
        _attribute_name: &str,
        _enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn tag_resource(&self, _arn: &str, _tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn untag_resource(
        &self,
        _arn: &str,
        _tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn list_tags(&self, _arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn tables_with_ttl(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn create_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn drop_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn find_expired_items_indexed(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn refresh_table_size(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn list_active_table_names(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }
}

impl StreamEngine for StreamStorage {
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn get_stream_records(
        &self,
        _account_id: &str,
        _shard_id: &str,
        _after_sequence: Option<&str>,
        _limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        Box::pin(async { Ok((Vec::new(), None)) })
    }

    fn describe_stream(
        &self,
        _account_id: &str,
        input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        self.describe_calls.fetch_add(1, Ordering::Relaxed);
        let description = StreamDescription {
            stream_arn: input.stream_arn.clone(),
            stream_label: String::new(),
            stream_status: StreamStatus::Enabled,
            stream_view_type: StreamViewType::NewImage,
            table_name: "Orders".to_owned(),
            key_schema: Vec::new(),
            shards: Vec::new(),
            last_evaluated_shard_id: None,
        };
        Box::pin(async move { Ok(description) })
    }

    fn list_streams(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
        _limit: i64,
        _exclusive_start_stream_arn: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult> {
        unimplemented!("not reached by the stream handlers")
    }

    fn cleanup_expired_stream_records(
        &self,
        _retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn assign_shard(
        &self,
        _account_id: &str,
        _table_name: &str,
        _partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn validate_shard(
        &self,
        _account_id: &str,
        _stream_arn: &str,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async { Ok(()) })
    }

    fn latest_sequence_number(
        &self,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }
}

impl WorkerStore for StreamStorage {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }
}

impl BackupEngine for StreamStorage {
    fn create_backup(
        &self,
        _account_id: &str,
        _table_name: &str,
        _backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn describe_backup(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn list_backups(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn delete_backup(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn backup_restore_source(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupRestoreSource, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn restore_table_from_backup(
        &self,
        _account_id: &str,
        _request: RestoreRequest,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn describe_continuous_backups(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }

    fn update_continuous_backups(
        &self,
        _account_id: &str,
        _table_name: &str,
        _pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        unimplemented!("not reached by the stream handlers")
    }
}
