// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Operation dispatch and business logic for extenddb.
//!
//! This crate sits between the HTTP server and the storage layer. It validates
//! inputs, dispatches operations by name, and translates storage errors into
//! `DynamoDB`-format error responses.

mod backup;
mod batch_get_item;
mod batch_write_item;
pub mod capacity_helpers;
mod create_table;
mod delete_item;
mod delete_table;
mod describe_endpoints;
mod describe_limits;
mod describe_table;
mod expected;
mod expression_helpers;
mod get_item;
mod import_export;
mod import_export_io;
mod index_helpers;
mod legacy_filter;
mod list_tables;
mod put_item;
mod query;
mod read_helpers;
mod scan;
pub mod stream_capture;
mod streams;
mod tagging;
mod transact_get_items;
mod transact_write_helpers;
mod transact_write_items;
mod ttl;
mod update_item;
mod update_table;

pub use batch_get_item::handle_batch_get_item;
pub use batch_write_item::handle_batch_write_item;
pub use create_table::handle_create_table;
pub use delete_item::handle_delete_item;
pub use delete_table::handle_delete_table;
pub use describe_endpoints::handle_describe_endpoints;
pub use describe_limits::handle_describe_limits;
pub use describe_table::handle_describe_table;
pub use get_item::handle_get_item;
pub use import_export::{handle_export_table, handle_import_table};
pub use list_tables::handle_list_tables;
pub use put_item::handle_put_item;
pub use query::handle_query;
pub use scan::handle_scan;
pub use streams::{
    handle_describe_stream, handle_get_records, handle_get_shard_iterator, handle_list_streams,
};
pub use tagging::{handle_list_tags_of_resource, handle_tag_resource, handle_untag_resource};
pub use transact_get_items::handle_transact_get_items;
pub use transact_write_items::handle_transact_write_items;
pub use ttl::{handle_describe_time_to_live, handle_update_time_to_live};
pub use update_item::handle_update_item;
pub use update_table::handle_update_table;

use std::path::PathBuf;
use std::sync::Arc;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;

/// Check whether an operation name is recognized by the dispatch table.
///
/// Real DynamoDB validates the operation name before checking authentication.
/// An unknown operation with no auth headers returns `UnknownOperationException`,
/// not `MissingAuthenticationToken`. The server layer calls this before auth.
#[must_use]
pub fn is_known_operation(operation: &str) -> bool {
    matches!(
        operation,
        "CreateTable"
            | "DeleteTable"
            | "DescribeTable"
            | "ListTables"
            | "UpdateTable"
            | "DescribeEndpoints"
            | "DescribeLimits"
            | "PutItem"
            | "GetItem"
            | "DeleteItem"
            | "UpdateItem"
            | "Query"
            | "Scan"
            | "BatchGetItem"
            | "BatchWriteItem"
            | "TransactGetItems"
            | "TransactWriteItems"
            | "DescribeTimeToLive"
            | "UpdateTimeToLive"
            | "TagResource"
            | "UntagResource"
            | "ListTagsOfResource"
            | "DescribeStream"
            | "ListStreams"
            | "GetShardIterator"
            | "GetRecords"
            | "ImportTable"
            | "ExportTableToPointInTime"
            | "CreateBackup"
            | "DescribeBackup"
            | "ListBackups"
            | "DeleteBackup"
            | "RestoreTableFromBackup"
            | "DescribeContinuousBackups"
            | "UpdateContinuousBackups"
            | "RestoreTableToPointInTime"
    )
}
use serde::Serialize;

/// Serialize an operation output to JSON, logging and sanitizing any serialization failure.
///
/// All operation handlers should use this instead of raw `serde_json::to_value`
/// with inline `.map_err(...)`. This ensures internal error details never leak
/// to the client.
pub(crate) fn serialize_output(
    output: &impl Serialize,
) -> Result<serde_json::Value, DynamoDbError> {
    serde_json::to_value(output).map_err(|e| {
        tracing::error!(internal_error = %e, "failed to serialize operation output");
        DynamoDbError::InternalServerError("Internal server error".to_owned())
    })
}

/// Convert a `StorageError` into a sanitized `DynamoDbError`, logging internal details.
///
/// Use this for storage calls that don't go through `storage_err_to_dynamo`
/// (e.g., tagging, metadata operations that return `StorageError` directly).
pub(crate) fn sanitize_storage_error(e: extenddb_storage::error::StorageError) -> DynamoDbError {
    tracing::error!(internal_error = %e, "storage internal error");
    DynamoDbError::InternalServerError("Internal server error".to_owned())
}

/// Map a serde deserialization error to the appropriate DynamoDB error type.
///
/// Enum validation errors (produced by custom Deserialize impls) contain
/// "validation error detected" and should be returned as `ValidationException`.
/// All other deserialization failures are `SerializationException`.
pub(crate) fn deserialize_error(e: serde_json::Error) -> DynamoDbError {
    let msg = e.to_string();
    if msg.contains("validation error detected")
        || msg.contains("may not be empty")
        || msg.contains("contains duplicates")
        || msg.contains("must not be empty")
        || msg.contains("contains invalid key")
        || msg.contains("Syntax error; key")
    {
        DynamoDbError::ValidationException(msg)
    } else {
        DynamoDbError::SerializationException(format!(
            "Start of structure or map found where not expected: {e}"
        ))
    }
}

/// Pre-validate enum fields in a JSON body and return a combined error if multiple are invalid.
///
/// DynamoDB reports all invalid enum fields together rather than stopping at the first.
/// Each entry is `(json_field_name, api_field_name, valid_values)`.
pub(crate) fn validate_enum_fields(
    body: &serde_json::Value,
    fields: &[(&str, &str, &[&str])],
) -> Result<(), DynamoDbError> {
    let obj = match body.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };
    let mut errors: Vec<String> = Vec::new();
    for &(json_name, api_name, valid) in fields {
        if let Some(val) = obj.get(json_name) {
            if let Some(s) = val.as_str() {
                if !valid.contains(&s) {
                    errors.push(format!(
                        "Value '{s}' at '{api_name}' failed to satisfy constraint: \
                         Member must satisfy enum value set: [{}]",
                        valid.join(", ")
                    ));
                }
            }
        }
    }
    if errors.is_empty() {
        return Ok(());
    }
    let count = errors.len();
    let msg = format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        errors.join("; ")
    );
    Err(DynamoDbError::ValidationException(msg))
}
///
/// Populated by engine handlers so the server layer can record capacity,
/// returned item counts, and returned byte counts without parsing the JSON
/// response body.
#[derive(Debug, Default)]
pub struct DispatchMetrics {
    /// Consumed read capacity units (full item size, rounded up to 4 KB).
    pub read_capacity_units: f64,
    /// Consumed write capacity units (full item size, rounded up to 1 KB).
    pub write_capacity_units: f64,
    /// Number of items returned to the client.
    pub returned_item_count: u64,
    /// Total bytes of items returned to the client (pre-projection scanned size,
    /// consistent with how DynamoDB charges capacity on scanned data).
    pub returned_bytes: u64,
    /// GSI/LSI name when the operation targets a secondary index.
    /// Used to attribute metrics to the specific index.
    pub index_name: Option<String>,
}

/// Result of an engine dispatch: the JSON response body plus sideband metrics.
#[must_use]
pub struct DispatchResult {
    /// The serialized JSON response body.
    pub body: serde_json::Value,
    /// Sideband metrics for the metrics collector.
    pub metrics: DispatchMetrics,
}

impl DispatchResult {
    /// Create a result with no sideband metrics (control-plane operations).
    pub(crate) fn body_only(body: serde_json::Value) -> Self {
        Self {
            body,
            metrics: DispatchMetrics::default(),
        }
    }
}

/// Context passed to every operation handler.
/// Fix #9: region and `account_id` use Arc<str> to avoid per-request cloning.
///
/// # Catalog Query Discipline (P118)
///
/// For single-table item operations (`GetItem`, `PutItem`, `DeleteItem`,
/// `UpdateItem`, `Query`, `Scan`), the auth layer pre-fetches `TableKeyInfo`
/// and stores it in `pre_fetched_key_info`. Engine handlers MUST use this
/// pre-fetched value instead of calling `storage.table_key_info()` directly.
/// New per-request catalog roundtrips require justification in the discussion
/// file and principal reviewer approval.
pub struct OperationContext {
    pub storage: Arc<dyn extenddb_storage::StorageEngine>,
    pub limits: Arc<LimitsConfig>,
    pub region: Arc<str>,
    pub account_id: Arc<str>,
    /// Allowed directories for import file operations. Empty means imports
    /// are disabled (secure default).
    pub import_paths: Arc<[Arc<PathBuf>]>,
    /// Allowed directories for export file operations. Empty means exports
    /// are disabled (secure default).
    pub export_paths: Arc<[Arc<PathBuf>]>,
    /// Pre-fetched `TableKeyInfo` from the auth layer (P118 optimization #2).
    /// Populated for single-table item-level operations; `None` for table-level
    /// and batch/transact operations.
    pub pre_fetched_key_info: Option<extenddb_core::types::TableKeyInfo>,
    /// Auth/authz cache handles. Used by control-plane and tagging operations
    /// to issue write-through cache invalidations after the underlying state
    /// changes (e.g. `TagResource` invalidates the resource-tags cache).
    pub auth_cache: extenddb_auth::AuthCacheRegistry,
    /// Optional cached `TableKeyInfo` lookup. When set, batch / transact /
    /// multi-table engine handlers route through this instead of calling
    /// `storage.table_key_info` directly. When unset (e.g. unit tests), the
    /// engine falls back to direct storage lookups.
    pub table_key_info_lookup: Option<Arc<dyn extenddb_storage::TableKeyInfoLookup>>,
}

impl OperationContext {
    /// Return pre-fetched `TableKeyInfo` if available and matching the requested
    /// table, otherwise fetch from storage. This is the single entry point for
    /// obtaining `TableKeyInfo` in engine handlers (P118 Catalog Query Discipline).
    pub(crate) async fn table_key_info(
        &self,
        table_name: &str,
    ) -> Result<extenddb_core::types::TableKeyInfo, extenddb_storage::error::StorageError> {
        if let Some(ref ki) = self.pre_fetched_key_info {
            if ki.table_name == table_name && *ki.account_id == *self.account_id {
                return Ok(ki.clone());
            }
        }
        if let Some(ref lookup) = self.table_key_info_lookup {
            return lookup.lookup(&self.account_id, table_name).await;
        }
        self.storage
            .table_key_info(&self.account_id, table_name)
            .await
    }
}

/// Dispatch an operation by name.
///
/// Returns the JSON response body and sideband metrics for the metrics collector.
pub async fn dispatch(
    operation: &str,
    body: serde_json::Value,
    ctx: &OperationContext,
    server_addr: &str,
) -> Result<DispatchResult, DynamoDbError> {
    match operation {
        "CreateTable" => handle_create_table(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "DeleteTable" => handle_delete_table(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "DescribeTable" => handle_describe_table(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "ListTables" => handle_list_tables(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "UpdateTable" => handle_update_table(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "DescribeEndpoints" => {
            handle_describe_endpoints(server_addr).map(DispatchResult::body_only)
        }
        "DescribeLimits" => handle_describe_limits(&ctx.limits).map(DispatchResult::body_only),
        "PutItem" => handle_put_item(body, ctx).await,
        "GetItem" => handle_get_item(body, ctx).await,
        "DeleteItem" => handle_delete_item(body, ctx).await,
        "UpdateItem" => handle_update_item(body, ctx).await,
        "Query" => handle_query(body, ctx).await,
        "Scan" => handle_scan(body, ctx).await,
        "BatchGetItem" => handle_batch_get_item(body, ctx).await,
        "BatchWriteItem" => handle_batch_write_item(body, ctx).await,
        "TransactGetItems" => handle_transact_get_items(body, ctx).await,
        "TransactWriteItems" => handle_transact_write_items(body, ctx).await,
        "DescribeTimeToLive" => handle_describe_time_to_live(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "UpdateTimeToLive" => handle_update_time_to_live(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "TagResource" => handle_tag_resource(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "UntagResource" => handle_untag_resource(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "ListTagsOfResource" => handle_list_tags_of_resource(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "DescribeStream" => handle_describe_stream(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "ListStreams" => handle_list_streams(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "GetShardIterator" => handle_get_shard_iterator(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "GetRecords" => handle_get_records(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "ImportTable" => handle_import_table(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "ExportTableToPointInTime" => handle_export_table(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "CreateBackup" => backup::handle_create_backup(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "DescribeBackup" => backup::handle_describe_backup(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "ListBackups" => backup::handle_list_backups(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "DeleteBackup" => backup::handle_delete_backup(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "RestoreTableFromBackup" => backup::handle_restore_table_from_backup(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "DescribeContinuousBackups" => backup::handle_describe_continuous_backups(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "UpdateContinuousBackups" => backup::handle_update_continuous_backups(body, ctx)
            .await
            .map(DispatchResult::body_only),
        "RestoreTableToPointInTime" => backup::handle_restore_table_to_point_in_time(body, ctx)
            .await
            .map(DispatchResult::body_only),
        _ => Err(DynamoDbError::UnknownOperationException(String::new())),
    }
}
