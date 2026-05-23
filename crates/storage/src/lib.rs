// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage trait definitions for extenddb.
//!
//! Defines `TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`,
//! and `WorkerStore` traits using RPITIT for async methods. Account-scoped
//! methods receive `account_id` from the authenticated identity.

pub mod authorization_store;
pub mod bootstrapper;
pub mod config;
pub mod diagnostics;
pub mod diagnostics_store;
pub mod error;
pub mod hooks;
pub mod management_store;
pub mod operations;
pub mod server_components;
pub mod settings_store;
pub mod transact;

pub use transact::{TransactGetOp, TransactWriteOp};

pub use server_components::{
    BackendError, ServerComponents, ServerComponentsFactory, ServerComponentsRegistration,
    create_server_components,
};

pub use hooks::{ServerRuntimeHooks, WorkerContext};

/// Pluggable lookup for `TableKeyInfo`.
///
/// Allows the engine layer to consult an in-memory cache transparently
/// instead of calling `StorageEngine::table_key_info` directly. The server
/// crate provides a SWR-cached implementation; tests and embedded uses can
/// pass `None` to fall back to direct storage lookups.
///
/// Defined here (rather than in `extenddb-engine`) so the cache wrapper in
/// `extenddb-server` can implement it without creating a circular crate
/// dependency.
pub trait TableKeyInfoLookup: Send + Sync {
    fn lookup<'a>(
        &'a self,
        account_id: &'a str,
        table_name: &'a str,
    ) -> futures::future::BoxFuture<
        'a,
        Result<extenddb_core::types::TableKeyInfo, error::StorageError>,
    >;
}

pub mod util;

use std::sync::Arc;

use futures::future::BoxFuture;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{
    CreateTableInput, DeleteTableInput, DescribeStreamInput, DescribeTableInput, IndexInfo, Item,
    ListTablesInput, ListTablesOutput, StreamDescription, StreamRecord, StreamSummary,
    StreamViewType, TableDescription, TableKeyInfo, Tag, TimeToLiveDescription, UpdateTableInput,
    UserIdentity,
};

use error::StorageError;

// Type aliases for complex return types used in trait methods.
/// Result of an update/put/delete that may return old and/or new item images.
pub type ItemPairResult = Result<(Option<Item>, Option<Item>), StorageError>;
/// Result of a query or scan: items plus an optional last-evaluated-key for pagination.
pub type QueryResult = Result<(Vec<Item>, Option<Item>), StorageError>;
/// TTL table info: `(account_id, table_name, ttl_attribute)`.
pub type TtlTableInfo = (String, String, String);
/// Stream records result: records plus an optional next shard iterator.
pub type StreamRecordsResult = Result<(Vec<StreamRecord>, Option<String>), StorageError>;
/// Stream list result: summaries plus an optional next exclusive start ARN.
pub type StreamListResult = Result<(Vec<StreamSummary>, Option<String>), StorageError>;

/// Parameters for capturing a stream record within a data write transaction.
///
/// When present, the storage backend inserts the stream record in the same
/// transaction as the data write, guaranteeing atomicity.
#[derive(Debug, Clone)]
pub struct StreamCapture {
    /// Which images to include in the stream record.
    pub view_type: StreamViewType,
    /// Optional user identity (set for TTL-originated deletions).
    pub user_identity: Option<UserIdentity>,
    /// AWS region for the stream record.
    pub region: Arc<str>,
}

/// Table lifecycle operations.
///
/// All methods receive `account_id` to scope operations to a single account.
/// This enables multi-account isolation: different accounts can have tables
/// with the same name without conflict.
pub trait TableEngine: Send + Sync {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>>;

    fn delete_table(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>>;

    fn describe_table(
        &self,
        account_id: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>>;

    fn list_tables(
        &self,
        account_id: &str,
        input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>>;

    /// Modify table settings (billing mode, throughput, deletion protection).
    fn update_table(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>>;

    /// Fetch key schema and attribute definitions for an ACTIVE table.
    ///
    /// Lighter than `describe_table` — returns only the metadata needed
    /// by data operations for validation and key extraction.
    fn table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>>;

    /// Fetch metadata for a secondary index on an ACTIVE table.
    ///
    /// Returns the index key schema, projection, and type (GSI/LSI).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::IndexNotFound`] if the index does not exist.
    /// Returns [`StorageError::TableNotFound`] if the table does not exist.
    fn index_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>>;

    /// Fetch metadata for a secondary index using a known `table_id`.
    ///
    /// Saves one catalog roundtrip vs `index_info` when the caller already
    /// has `TableKeyInfo` (P118 optimization #4). Backends that don't override
    /// this will fall back to the standard `index_info` path.
    fn index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>>;
}

/// Item-level data operations.
///
/// All methods receive a `TableKeyInfo` from the engine layer, which has
/// already validated the table exists and is ACTIVE. Storage backends do
/// not re-fetch catalog metadata for data operations.
///
/// `account_id` is carried inside `TableKeyInfo` for data operations,
/// so these methods do not need a separate `account_id` parameter.
pub trait DataEngine: Send + Sync {
    /// Write an item to a table, replacing any existing item with the same key.
    ///
    /// If `condition` is `Some`, evaluates the condition against the existing item
    /// inside a transaction. Returns `StorageError::ConditionFailed` if the
    /// condition evaluates to false.
    ///
    /// When `stream` is `Some`, the stream record is inserted in the same
    /// transaction as the data write, guaranteeing atomicity.
    ///
    /// Returns the previous item if `return_old` is true and an item existed.
    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>>;

    /// Read a single item by primary key.
    ///
    /// Returns `None` if the item does not exist (not an error).
    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>>;

    /// Delete a single item by primary key.
    ///
    /// If `condition` is `Some`, evaluates the condition against the existing item
    /// inside a transaction. Returns `StorageError::ConditionFailed` if the
    /// condition evaluates to false.
    ///
    /// When `stream` is `Some`, the stream record is inserted in the same
    /// transaction as the data write, guaranteeing atomicity.
    ///
    /// Returns the deleted item if `return_old` is true and an item existed.
    fn delete_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>>;

    /// Update an item by primary key using update actions.
    ///
    /// UpdateItem is an upsert: if the item doesn't exist, a new item is created
    /// containing the key attributes plus the SET values.
    ///
    /// If `condition` is `Some`, evaluates the condition against the existing item
    /// (or empty item for new) inside a transaction.
    ///
    /// When `stream` is `Some`, the stream record is inserted in the same
    /// transaction as the data write, guaranteeing atomicity.
    ///
    /// Returns the item (old or new) based on `ReturnValues` semantics.
    /// The caller specifies which snapshots to capture via `return_old` and `return_new`.
    #[allow(clippy::too_many_arguments)]
    fn update_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        actions: &[UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, ItemPairResult>;

    /// Query items by partition key with optional sort key condition.
    ///
    /// Returns items matching the key condition, ordered by sort key.
    /// `forward` controls sort order (`true` = ascending, `false` = descending).
    /// `limit` caps the number of items read (before filtering).
    /// `exclusive_start_key` enables pagination.
    /// `index_name` routes the query to a secondary index table.
    ///
    /// Returns `(items, last_evaluated_key)`. If `last_evaluated_key` is `Some`,
    /// there are more items to read.
    #[allow(clippy::too_many_arguments)]
    fn query(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult>;

    /// Scan all items in a table or index.
    ///
    /// Returns items in storage order. `limit` caps the number of items read
    /// (before filtering). `exclusive_start_key` enables pagination.
    /// `segment` and `total_segments` enable parallel scan.
    /// `index_name` routes the scan to a secondary index table.
    ///
    /// Returns `(items, last_evaluated_key)`.
    #[allow(clippy::too_many_arguments)]
    fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, QueryResult>;

    /// Execute multiple get operations in a single consistent snapshot.
    ///
    /// Returns one `Option<Item>` per request, in the same order as `ops`.
    /// All reads see the same database snapshot (serializable isolation).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Internal`] on transaction or query failure.
    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>>;

    /// Execute multiple write operations atomically in a single transaction.
    ///
    /// All operations succeed or all are rolled back. Returns `Ok(())` on
    /// success. On condition check failure, returns
    /// `StorageError::TransactionCanceled` with per-item cancellation reasons.
    ///
    /// When `stream` is `Some`, stream records for each write operation are
    /// inserted in the same transaction as the data writes.
    ///
    /// When `token` is `Some`, the idempotency token is checked and stored
    /// in the same transaction as the writes, guaranteeing atomicity.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::TransactionCanceled`] if any condition fails.
    /// Returns [`StorageError::Internal`] on transaction or query failure.
    /// Returns [`StorageError::IdempotentReplay`] if the token matches a previous request.
    /// Returns [`StorageError::IdempotentMismatch`] if the token exists with different ops.
    #[allow(clippy::too_many_arguments)]
    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        token: Option<(&str, &str)>,
    ) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Delete idempotency tokens older than the given age in seconds.
    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>>;
}

/// TTL, tag, and table-size management operations.
///
/// Methods that operate on table-scoped resources receive `account_id`.
/// Tag methods use ARN (which embeds account_id) so they don't need it separately.
pub trait MetadataEngine: Send + Sync {
    /// Return the TTL configuration for a table.
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>>;

    /// Enable or disable TTL on a table attribute.
    fn update_ttl(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Add or overwrite tags on a resource.
    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Remove tags by key from a resource.
    fn untag_resource(
        &self,
        arn: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>>;

    /// List all tags for a resource.
    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>>;

    /// List all table names that have TTL enabled, with their TTL attribute.
    fn tables_with_ttl(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>>;

    /// List all tables with TTL enabled across all accounts: `(account_id, table_name, ttl_attribute)`.
    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>>;

    /// List all tables with TTL enabled AND index ready: `(account_id, table_name, ttl_attribute)`.
    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>>;

    /// Create the TTL expression index concurrently for a table.
    /// Sets `ttl_index_ready = TRUE` on success.
    fn create_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Drop the TTL expression index for a table.
    /// Sets `ttl_index_ready = FALSE`.
    fn drop_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Find expired items using the TTL index (ordered scan with LIMIT).
    fn find_expired_items_indexed(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>>;

    /// Recompute and store `table_size_bytes` and `item_count` for a table.
    fn refresh_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>>;

    /// List all active table names (for background workers).
    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>>;

    /// List all active tables across all accounts: `(account_id, table_name)`.
    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>>;
}

/// DynamoDB Streams record storage and retrieval.
pub trait StreamEngine: Send + Sync {
    /// Write a stream record atomically (called within the data write transaction).
    fn write_stream_record(
        &self,
        account_id: &str,
        record: &StreamRecord,
        shard_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Read stream records from a shard starting after a sequence number.
    fn get_stream_records(
        &self,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult>;

    /// Describe a stream (shard list, status, view type).
    fn describe_stream(
        &self,
        account_id: &str,
        input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>>;

    /// List streams, optionally filtered by table name.
    fn list_streams(
        &self,
        account_id: &str,
        table_name: Option<&str>,
        limit: i64,
        exclusive_start_stream_arn: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult>;

    /// Delete stream records older than the retention period.
    fn cleanup_expired_stream_records(
        &self,
        retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>>;

    /// Assign a shard for a given partition key (hash-based).
    fn assign_shard(
        &self,
        account_id: &str,
        table_name: &str,
        partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>>;

    /// Generate the next sequence number for a shard.
    fn next_sequence_number(&self, shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>>;

    /// Validate that a shard exists for the given stream ARN.
    ///
    /// Returns `Ok(())` if the shard exists and belongs to the stream.
    /// Returns `Err(StorageError::TableNotFound)` if the stream or shard does not exist.
    fn validate_shard(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>>;

    /// Return the latest sequence number in a shard, or `None` if the shard is empty.
    ///
    /// Used by `GetShardIterator` with `LATEST` to resolve the current position
    /// so that only records written after the iterator was created are returned.
    fn latest_sequence_number(
        &self,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>>;
}

/// Background worker operations that require storage access.
///
/// Covers control-plane transition processing and other periodic maintenance
/// tasks that were previously methods on the concrete `PostgresEngine`.
pub trait WorkerStore: Send + Sync {
    /// Process pending control-plane transitions (CREATING → ACTIVE,
    /// DELETING → deleted). Returns a list of `(table_name, description)`
    /// for each transition that fired.
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>>;
}

/// Backup and point-in-time recovery operations.
pub trait BackupEngine: Send + Sync {
    /// Create a backup of a table, snapshotting all items.
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDetails, StorageError>>;

    /// Describe a backup by ARN.
    fn describe_backup(
        &self,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDescription, StorageError>>;

    /// List backups for a table.
    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<extenddb_core::types::BackupSummary>, StorageError>>;

    /// Delete a backup by ARN.
    fn delete_backup(
        &self,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::BackupDescription, StorageError>>;

    /// Restore a table from a backup.
    fn restore_table_from_backup(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>>;

    /// Describe continuous backups / PITR status for a table.
    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::ContinuousBackupsDescription, StorageError>>;

    /// Update continuous backups (enable/disable PITR).
    fn update_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
        pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<extenddb_core::types::ContinuousBackupsDescription, StorageError>>;

    /// Restore a table to a point in time.
    // TODO(cleanup): This method is unreachable — the engine handler returns
    // ValidationException("not yet supported") before calling storage. Remove
    // when real PITR is implemented or during the next storage trait cleanup.
    fn restore_table_to_point_in_time(
        &self,
        account_id: &str,
        source_table_name: &str,
        target_table_name: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>>;
}

/// Supertrait combining all DynamoDB operation traits.
///
/// All storage backends must implement this to provide a complete
/// DynamoDB-compatible API. This trait has NO additional methods beyond
/// the trait bounds — backend-specific concerns belong in ServerRuntimeHooks.
pub trait StorageEngine:
    TableEngine + DataEngine + MetadataEngine + StreamEngine + BackupEngine + WorkerStore + Send + Sync
{
}

// Blanket implementation: any type implementing all 6 traits is a StorageEngine
impl<T> StorageEngine for T where
    T: TableEngine
        + DataEngine
        + MetadataEngine
        + StreamEngine
        + BackupEngine
        + WorkerStore
        + Send
        + Sync
{
}

/// Supertrait combining all catalog/management operation traits.
///
/// All storage backends must implement this to provide management API
/// functionality (accounts, users, groups, roles, policies, settings, metrics).
pub trait CatalogStore:
    management_store::ManagementStore
    + management_store::AdminStore
    + management_store::SettingsStore
    + management_store::MetricsStore
    + management_store::RateLimitStore
    + authorization_store::AuthorizationStore
    + Send
    + Sync
{
    /// Get the cached encryption key (if available).
    ///
    /// Returns None if encryption key is not cached. This is used by
    /// cmd_serve to construct the auth provider without re-querying
    /// the settings table.
    fn cached_encryption_key(&self) -> Option<String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that CatalogStore is dyn-compatible (object-safe).
    ///
    /// This test ensures all catalog traits use BoxFuture instead of RPITIT,
    /// allowing us to use `Arc<dyn CatalogStore>` in the factory pattern.
    #[test]
    fn catalog_store_is_dyn_compatible() {
        // This function just needs to compile - it's never called
        fn _assert_dyn(_: Arc<dyn CatalogStore>) {}
    }
}
