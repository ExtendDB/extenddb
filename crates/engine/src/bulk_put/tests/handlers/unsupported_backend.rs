// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// The MongoDB and Cassandra backends refuse every bulk operation with
// ValidationException "Bulk operations are not supported on this storage
// backend". This double stands in for such a backend: it
// delegates describe_table to a real engine (so Start passes the table
// lookup) and refuses every BulkOperationStore method, so every handler
// path reaches the refusal the client sees.
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{
    CreateTableInput, DeleteTableInput, DescribeStreamInput, DescribeTableInput, IndexInfo, Item,
    ListTablesInput, ListTablesOutput, StreamDescription, StreamRecord, TableDescription,
    TableKeyInfo, Tag, TimeToLiveDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BoxedFuture as BoxFuture, BulkOperationListResult, BulkOperationRecord, BulkOperationStore,
    BulkRowMarker, CancelOutcome, DataEngine, FinishState, IdempotencyKey, InputFile,
    ItemPairResult, LeaseState, ListCursor, MetadataEngine, NewBulkOperation, Progress,
    QueryResult, ReportKind, ReportRecord, RowOutcome, StartOutcome, StorageEngine, StreamCapture,
    StreamEngine, StreamListResult, StreamRecordsResult, TableEngine, Terminal, TransactGetOp,
    TransactWriteOp, TtlTableInfo, WorkerStore,
};

/// The refusal message both unsupported backends answer. A toggle lets
/// a test turn the refusal into an internal error.
pub(super) const UNSUPPORTED_MESSAGE: &str =
    "Bulk operations are not supported on this storage backend";

fn refuse<T: Send + 'static>() -> BoxFuture<'static, Result<T, StorageError>> {
    Box::pin(async { Err(StorageError::Validation(UNSUPPORTED_MESSAGE.to_owned())) })
}

/// A storage engine whose bulk store refuses every operation, wrapping a
/// real engine for the catalog lookup Start performs before it. When
/// `creating_index` is set, the description it answers marks that
/// global secondary index `CREATING`, the state a backfill leaves it in.
pub(super) struct BulkUnsupportedStorage {
    pub(super) inner: Arc<dyn StorageEngine>,
    pub(super) creating_index: Option<String>,
}

mod backup;

impl TableEngine for BulkUnsupportedStorage {
    fn create_table(
        &self,
        _account_id: &str,
        _input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!()
    }
    fn delete_table(
        &self,
        _account_id: &str,
        _input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!()
    }
    fn describe_table(
        &self,
        account_id: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let described = self.inner.describe_table(account_id, input);
        let creating = self.creating_index.clone();
        Box::pin(async move {
            let mut table = described.await?;
            if let Some(name) = creating {
                for index in table.global_secondary_indexes.iter_mut().flatten() {
                    if index.index_name == name {
                        index.index_status = "CREATING".to_owned();
                    }
                }
            }
            Ok(table)
        })
    }
    fn list_tables(
        &self,
        _account_id: &str,
        _input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        unimplemented!()
    }
    fn update_table(
        &self,
        _account_id: &str,
        _input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!()
    }
    fn table_key_info(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        unimplemented!()
    }
    fn index_info(
        &self,
        _account_id: &str,
        _table_name: &str,
        _index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        unimplemented!()
    }
    fn index_info_by_table_id(
        &self,
        _table_id: &str,
        _index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        unimplemented!()
    }
}

impl DataEngine for BulkUnsupportedStorage {
    fn put_items_batch(
        &self,
        _key_info: &TableKeyInfo,
        _rows: Vec<extenddb_storage::BatchRow>,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::BatchRowResult>, StorageError>> {
        refuse()
    }
    #[allow(clippy::too_many_arguments)]
    fn put_item(
        &self,
        _key_info: &TableKeyInfo,
        _item: Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
        _marker: Option<&BulkRowMarker>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        unimplemented!()
    }
    fn bulk_row_outcome(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _file_index: i64,
        _row_index: i64,
    ) -> BoxFuture<'_, Result<Option<RowOutcome>, StorageError>> {
        unimplemented!()
    }
    fn delete_bulk_row_outcomes(
        &self,
        _account_id: &str,
        _operation_id: &str,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unimplemented!()
    }
    fn get_item(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        unimplemented!()
    }
    #[allow(clippy::too_many_arguments)]
    fn delete_item(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        unimplemented!()
    }
    #[allow(clippy::too_many_arguments)]
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
        unimplemented!()
    }
    #[allow(clippy::too_many_arguments)]
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
        unimplemented!()
    }
    #[allow(clippy::too_many_arguments)]
    fn scan(
        &self,
        _key_info: &TableKeyInfo,
        _limit: Option<i64>,
        _exclusive_start_key: Option<&Item>,
        _segment: Option<i64>,
        _total_segments: Option<i64>,
        _index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult> {
        unimplemented!()
    }
    fn scan_key_in_segment(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
        _segment: i64,
        _total_segments: i64,
        _index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        unimplemented!()
    }
    fn transact_get_items(
        &self,
        _ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        unimplemented!()
    }
    fn transact_write_items(
        &self,
        _ops: &[TransactWriteOp<'_>],
        _idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn cleanup_expired_idempotency_tokens(
        &self,
        _max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unimplemented!()
    }
}

impl MetadataEngine for BulkUnsupportedStorage {
    fn describe_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        unimplemented!()
    }
    fn update_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
        _attribute_name: &str,
        _enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn tag_resource(&self, _arn: &str, _tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn untag_resource(
        &self,
        _arn: &str,
        _tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn list_tags(&self, _arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        unimplemented!()
    }
    fn tables_with_ttl(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unimplemented!()
    }
    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        unimplemented!()
    }
    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        unimplemented!()
    }
    fn create_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn drop_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn find_expired_items_indexed(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        unimplemented!()
    }
    fn refresh_table_size(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn list_active_table_names(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        unimplemented!()
    }
    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unimplemented!()
    }
}

impl StreamEngine for BulkUnsupportedStorage {
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn get_stream_records(
        &self,
        _account_id: &str,
        _stream_arn: &str,
        _shard_id: &str,
        _after_sequence: Option<&str>,
        _limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        unimplemented!()
    }
    fn describe_stream(
        &self,
        _account_id: &str,
        _input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        unimplemented!()
    }
    fn list_streams(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
        _limit: i64,
        _exclusive_start_stream_arn: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult> {
        unimplemented!()
    }
    fn cleanup_expired_stream_records(
        &self,
        _retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        unimplemented!()
    }
    fn assign_shard(
        &self,
        _account_id: &str,
        _table_name: &str,
        _partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        unimplemented!()
    }
    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        unimplemented!()
    }
    fn validate_shard(
        &self,
        _account_id: &str,
        _stream_arn: &str,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        unimplemented!()
    }
    fn latest_sequence_number(
        &self,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        unimplemented!()
    }
}

impl WorkerStore for BulkUnsupportedStorage {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        unimplemented!()
    }
}

impl BulkOperationStore for BulkUnsupportedStorage {
    fn start_bulk_operation(
        &self,
        _account_id: &str,
        _req: NewBulkOperation,
        _account_limit: u32,
    ) -> BoxFuture<'_, Result<StartOutcome, StorageError>> {
        refuse()
    }
    fn get_bulk_operation(
        &self,
        _account_id: &str,
        _operation_id: &str,
    ) -> BoxFuture<'_, Result<Option<BulkOperationRecord>, StorageError>> {
        refuse()
    }
    fn count_non_terminal(&self, _account_id: &str) -> BoxFuture<'_, Result<u64, StorageError>> {
        refuse()
    }
    fn record_input_files(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _owner: &str,
        _lease: Duration,
        _files: Vec<InputFile>,
        _total_input_bytes: i64,
    ) -> BoxFuture<'_, Result<LeaseState, StorageError>> {
        refuse()
    }
    fn list_bulk_operations(
        &self,
        _account_id: &str,
        _limit: u32,
        _after: Option<ListCursor>,
    ) -> BoxFuture<'_, BulkOperationListResult> {
        refuse()
    }
    fn request_cancel(
        &self,
        _account_id: &str,
        _operation_id: &str,
    ) -> BoxFuture<'_, Result<CancelOutcome, StorageError>> {
        refuse()
    }
    fn claim_runnable(
        &self,
        _owner: &str,
        _lease: Duration,
        _max: usize,
        _exclude_accounts: &[String],
        _exclude_operations: &[String],
    ) -> BoxFuture<'_, Result<Vec<BulkOperationRecord>, StorageError>> {
        refuse()
    }
    fn checkpoint(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _owner: &str,
        _lease: Duration,
        _progress: Progress,
    ) -> BoxFuture<'_, Result<LeaseState, StorageError>> {
        refuse()
    }
    fn report_records_page(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _kind: ReportKind,
        _after: Option<(i64, i64)>,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<ReportRecord>, StorageError>> {
        refuse()
    }
    fn finish(
        &self,
        _account_id: &str,
        _operation_id: &str,
        _owner: &str,
        _outcome: Terminal,
    ) -> BoxFuture<'_, Result<FinishState, StorageError>> {
        refuse()
    }
    fn list_purgeable_before(
        &self,
        _cutoff: SystemTime,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        refuse()
    }
    fn purge_operations(
        &self,
        _pairs: &[(String, String)],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        refuse()
    }
}
