// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! A storage double whose per-key data calls fail with a chosen service error
//! for a chosen set of partition key values and succeed for every other key,
//! recording the writes it applied.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use extenddb_core::error::{
    DynamoDbError, PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE, ThrottlingReason,
};
use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BackupDescription, BackupDetails, BackupSummary,
    ContinuousBackupsDescription, CreateTableInput, DeleteTableInput, DescribeStreamInput,
    DescribeTableInput, IndexInfo, Item, KeySchemaElement, KeyType, ListTablesInput,
    ListTablesOutput, ScalarAttributeType, StreamDescription, StreamRecord, TableDescription,
    TableKeyInfo, Tag, TimeToLiveDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupEngine, BackupRestoreSource, BoxedFuture, DataEngine, IdempotencyKey, ItemPairResult,
    MetadataEngine, QueryResult, RestoreRequest, StreamCapture, StreamEngine, StreamListResult,
    StreamRecordsResult, TableEngine, TransactGetOp, TransactWriteOp, TtlTableInfo, WorkerStore,
};

pub const ACCOUNT: &str = "123456789012";
pub const REGION: &str = "us-east-1";

/// One applied write, as the double saw it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Applied {
    Put(String),
    Delete(String),
}

/// A storage double whose per-key calls fail with a chosen service error for
/// a chosen set of partition key values and succeed for every other key.
pub struct FailingStore {
    /// Tables the double knows, with whether each has a local secondary index.
    tables: HashMap<String, bool>,
    /// Items served by `get_item`, by partition key value.
    items: BTreeMap<String, Item>,
    /// Partition key values whose data calls fail with `error`.
    failing: HashSet<String>,
    /// The error returned for a failing key.
    error: DynamoDbError,
    /// Writes that reached the backend, in call order.
    applied: Mutex<Vec<Applied>>,
}

impl FailingStore {
    pub fn new(error: DynamoDbError) -> Self {
        Self {
            tables: HashMap::from([("T".to_owned(), true), ("U".to_owned(), false)]),
            items: BTreeMap::new(),
            failing: HashSet::new(),
            error,
            applied: Mutex::new(Vec::new()),
        }
    }

    pub fn throttling() -> Self {
        Self::new(throttle_error("TableReadProvisionedThroughputExceeded"))
    }

    /// A double that throttles writes, carrying the write reason.
    pub fn throttling_writes() -> Self {
        Self::new(throttle_error("TableWriteProvisionedThroughputExceeded"))
    }

    pub fn with_items(mut self, pks: &[&str]) -> Self {
        for pk in pks {
            self.items.insert((*pk).to_owned(), item(pk));
        }
        self
    }

    pub fn with_large_item(mut self, pk: &str) -> Self {
        self.items.insert(pk.to_owned(), large_item(pk));
        self
    }

    pub fn failing_on(mut self, pks: &[&str]) -> Self {
        self.failing = pks.iter().map(|p| (*p).to_owned()).collect();
        self
    }

    fn fails(&self, key_or_item: &Item) -> bool {
        self.failing.contains(&pk_of(key_or_item))
    }

    pub fn applied(&self) -> Vec<Applied> {
        self.applied.lock().expect("applied lock").clone()
    }
}

pub fn throttle_error(reason: &str) -> DynamoDbError {
    DynamoDbError::provisioned_throughput_exceeded(
        PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
        vec![ThrottlingReason {
            reason: reason.to_owned(),
            resource: format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/T"),
        }],
    )
}

pub fn pk_of(item: &Item) -> String {
    match item.get("pk") {
        Some(AttributeValue::S(s)) => s.clone(),
        other => panic!("item without string pk: {other:?}"),
    }
}

pub fn key(pk: &str) -> Item {
    BTreeMap::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

pub fn item(pk: &str) -> Item {
    let mut it = key(pk);
    it.insert("v".to_owned(), AttributeValue::N("1".to_owned()));
    it
}

/// An item just over 4 KB, so a consistent read of it costs two capacity
/// units and a size-blind charge of one per item is told apart.
pub fn large_item(pk: &str) -> Item {
    let mut it = key(pk);
    it.insert("v".to_owned(), AttributeValue::S("x".repeat(4_100)));
    it
}

pub fn key_info(table: &str, has_lsi: bool) -> TableKeyInfo {
    TableKeyInfo {
        table_name: table.to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: format!("id-{table}"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        base_key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }],
        has_lsi,
        global_secondary_indexes: Vec::new(),
        local_secondary_indexes: Vec::new(),
        stream_specification: None,
        vector_indexes: Vec::new(),
    }
}

impl TableEngine for FailingStore {
    fn create_table(
        &self,
        _account_id: &str,
        _input: CreateTableInput,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn delete_table(
        &self,
        _account_id: &str,
        _input: DeleteTableInput,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn describe_table(
        &self,
        _account_id: &str,
        _input: DescribeTableInput,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn list_tables(
        &self,
        _account_id: &str,
        _input: ListTablesInput,
    ) -> BoxedFuture<'_, Result<ListTablesOutput, StorageError>> {
        unimplemented!("not exercised")
    }
    fn update_table(
        &self,
        _account_id: &str,
        _input: UpdateTableInput,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn table_key_info(
        &self,
        _account_id: &str,
        table_name: &str,
    ) -> BoxedFuture<'_, Result<TableKeyInfo, StorageError>> {
        let result = match self.tables.get(table_name) {
            Some(has_lsi) => Ok(key_info(table_name, *has_lsi)),
            None => Err(StorageError::TableNotFound(table_name.to_owned())),
        };
        Box::pin(async move { result })
    }
    fn index_info(
        &self,
        _account_id: &str,
        _table_name: &str,
        _index_name: &str,
    ) -> BoxedFuture<'_, Result<IndexInfo, StorageError>> {
        unimplemented!("not exercised")
    }
    fn index_info_by_table_id(
        &self,
        _table_id: &str,
        _index_name: &str,
    ) -> BoxedFuture<'_, Result<IndexInfo, StorageError>> {
        unimplemented!("not exercised")
    }
}

impl DataEngine for FailingStore {
    fn put_item(
        &self,
        _key_info: &TableKeyInfo,
        item: Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxedFuture<'_, Result<Option<Item>, StorageError>> {
        let result = if self.fails(&item) {
            Err(StorageError::Service(self.error.clone()))
        } else {
            self.applied
                .lock()
                .expect("applied lock")
                .push(Applied::Put(pk_of(&item)));
            Ok(None)
        };
        Box::pin(async move { result })
    }

    fn get_item(
        &self,
        _key_info: &TableKeyInfo,
        key: &Item,
    ) -> BoxedFuture<'_, Result<Option<Item>, StorageError>> {
        let result = if self.fails(key) {
            Err(StorageError::Service(self.error.clone()))
        } else {
            Ok(self.items.get(&pk_of(key)).cloned())
        };
        Box::pin(async move { result })
    }

    fn delete_item(
        &self,
        _key_info: &TableKeyInfo,
        key: &Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxedFuture<'_, Result<Option<Item>, StorageError>> {
        let result = if self.fails(key) {
            Err(StorageError::Service(self.error.clone()))
        } else {
            self.applied
                .lock()
                .expect("applied lock")
                .push(Applied::Delete(pk_of(key)));
            Ok(None)
        };
        Box::pin(async move { result })
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
    ) -> BoxedFuture<'_, ItemPairResult> {
        unimplemented!("not exercised")
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
    ) -> BoxedFuture<'_, QueryResult> {
        unimplemented!("not exercised")
    }

    fn scan(
        &self,
        _key_info: &TableKeyInfo,
        _limit: Option<i64>,
        _exclusive_start_key: Option<&Item>,
        _segment: Option<i64>,
        _total_segments: Option<i64>,
        _index_name: Option<&str>,
    ) -> BoxedFuture<'_, QueryResult> {
        unimplemented!("not exercised")
    }

    fn scan_key_in_segment(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
        _segment: i64,
        _total_segments: i64,
        _index_name: Option<&str>,
    ) -> BoxedFuture<'_, Result<bool, StorageError>> {
        unimplemented!("not exercised")
    }

    fn transact_get_items(
        &self,
        _ops: &[TransactGetOp<'_>],
    ) -> BoxedFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        unimplemented!("not exercised")
    }

    fn transact_write_items(
        &self,
        _ops: &[TransactWriteOp<'_>],
        _idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        _max_age_seconds: i64,
    ) -> BoxedFuture<'_, Result<u64, StorageError>> {
        unimplemented!("not exercised")
    }
}

impl MetadataEngine for FailingStore {
    fn describe_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxedFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn update_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
        _attribute_name: &str,
        _enabled: bool,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }
    fn tag_resource(&self, _arn: &str, _tags: &[Tag]) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }
    fn untag_resource(
        &self,
        _arn: &str,
        _tag_keys: &[String],
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }
    fn list_tags(&self, _arn: &str) -> BoxedFuture<'_, Result<Vec<Tag>, StorageError>> {
        unimplemented!("not exercised")
    }
    fn tables_with_ttl(
        &self,
        _account_id: &str,
    ) -> BoxedFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unimplemented!("not exercised")
    }
    fn all_tables_with_ttl(&self) -> BoxedFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        unimplemented!("not exercised")
    }
    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxedFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        unimplemented!("not exercised")
    }
    fn create_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }
    fn drop_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }
    fn find_expired_items_indexed(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
        _limit: usize,
    ) -> BoxedFuture<'_, Result<Vec<Item>, StorageError>> {
        unimplemented!("not exercised")
    }
    fn refresh_table_size(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }
    fn list_active_table_names(
        &self,
        _account_id: &str,
    ) -> BoxedFuture<'_, Result<Vec<String>, StorageError>> {
        unimplemented!("not exercised")
    }
    fn all_active_tables(&self) -> BoxedFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        unimplemented!("not exercised")
    }
}

impl StreamEngine for FailingStore {
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }
    fn get_stream_records(
        &self,
        _account_id: &str,
        _shard_id: &str,
        _after_sequence: Option<&str>,
        _limit: i64,
    ) -> BoxedFuture<'_, StreamRecordsResult> {
        unimplemented!("not exercised")
    }
    fn describe_stream(
        &self,
        _account_id: &str,
        _input: &DescribeStreamInput,
    ) -> BoxedFuture<'_, Result<StreamDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn list_streams(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
        _limit: i64,
        _exclusive_start_stream_arn: Option<&str>,
    ) -> BoxedFuture<'_, StreamListResult> {
        unimplemented!("not exercised")
    }
    fn cleanup_expired_stream_records(
        &self,
        _retention_hours: i64,
    ) -> BoxedFuture<'_, Result<u64, StorageError>> {
        unimplemented!("not exercised")
    }
    fn assign_shard(
        &self,
        _account_id: &str,
        _table_name: &str,
        _partition_key: &str,
    ) -> BoxedFuture<'_, Result<String, StorageError>> {
        unimplemented!("not exercised")
    }
    fn next_sequence_number(
        &self,
        _shard_id: &str,
    ) -> BoxedFuture<'_, Result<String, StorageError>> {
        unimplemented!("not exercised")
    }
    fn validate_shard(
        &self,
        _account_id: &str,
        _stream_arn: &str,
        _shard_id: &str,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unimplemented!("not exercised")
    }
    fn latest_sequence_number(
        &self,
        _shard_id: &str,
    ) -> BoxedFuture<'_, Result<Option<String>, StorageError>> {
        unimplemented!("not exercised")
    }
}

impl BackupEngine for FailingStore {
    fn create_backup(
        &self,
        _account_id: &str,
        _table_name: &str,
        _backup_name: &str,
    ) -> BoxedFuture<'_, Result<BackupDetails, StorageError>> {
        unimplemented!("not exercised")
    }
    fn describe_backup(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn list_backups(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
    ) -> BoxedFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        unimplemented!("not exercised")
    }
    fn delete_backup(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn backup_restore_source(
        &self,
        _account_id: &str,
        _backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupRestoreSource, StorageError>> {
        unimplemented!("not exercised")
    }
    fn restore_table_from_backup(
        &self,
        _account_id: &str,
        _request: RestoreRequest,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn describe_continuous_backups(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxedFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        unimplemented!("not exercised")
    }
    fn update_continuous_backups(
        &self,
        _account_id: &str,
        _table_name: &str,
        _pitr_enabled: bool,
    ) -> BoxedFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        unimplemented!("not exercised")
    }
}

impl WorkerStore for FailingStore {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxedFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        unimplemented!("not exercised")
    }
}
