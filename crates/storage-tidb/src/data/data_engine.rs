// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Thin `DataEngine` trait implementation that delegates to `impl TidbEngine`
//! methods in sibling modules.

use std::future::Future;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{IndexInfo, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BatchWriteOp, DataEngine, ExportTableItemsSummary, IdempotencyClaim, ItemExportSink,
    StreamCapture, TransactGetOp, TransactWriteOp,
};
use futures::future::BoxFuture;

use super::{native_index_name, physical_data_table_name};
use crate::TidbEngine;
use crate::tidb_util::{
    is_index_not_found_tidb_storage_error_for_index_on_table,
    is_table_not_found_tidb_storage_error_for_table, retry_tidb_idempotent_operation,
};

impl DataEngine for TidbEngine {
    fn put_item<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&'a Expr>,
        maps: &'a ExpressionMaps,
        stream: Option<&'a StreamCapture>,
    ) -> BoxFuture<'a, Result<Option<Item>, StorageError>> {
        data_table_future(
            key_info,
            self.put_item_impl(key_info, item, return_old, condition, maps, stream),
        )
    }

    fn get_item<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        key: &'a Item,
        consistent_read: bool,
    ) -> BoxFuture<'a, Result<Option<Item>, StorageError>> {
        retrying_data_table_future("get_item", key_info, move || {
            self.get_item_impl(key_info, key, consistent_read)
        })
    }

    fn batch_get_items<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        keys: &'a [Item],
        consistent_read: bool,
    ) -> BoxFuture<'a, Result<Vec<Item>, StorageError>> {
        retrying_data_table_future("batch_get_items", key_info, move || {
            self.batch_get_items_impl(key_info, keys, consistent_read)
        })
    }

    fn batch_write_items<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        ops: &'a [BatchWriteOp<'a>],
        stream: Option<&'a StreamCapture>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        data_table_future(key_info, self.batch_write_items_impl(key_info, ops, stream))
    }

    fn delete_item<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        key: &'a Item,
        return_old: bool,
        condition: Option<&'a Expr>,
        maps: &'a ExpressionMaps,
        stream: Option<&'a StreamCapture>,
    ) -> BoxFuture<'a, Result<Option<Item>, StorageError>> {
        data_table_future(
            key_info,
            self.delete_item_impl(key_info, key, return_old, condition, maps, stream),
        )
    }

    fn update_item<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        key: &'a Item,
        actions: &'a [UpdateAction],
        return_old: bool,
        return_new: bool,
        condition: Option<&'a Expr>,
        maps: &'a ExpressionMaps,
        stream: Option<&'a StreamCapture>,
    ) -> BoxFuture<'a, Result<(Option<Item>, Option<Item>), StorageError>> {
        data_table_future(
            key_info,
            self.update_item_impl(
                key_info, key, actions, return_old, return_new, condition, maps, stream,
            ),
        )
    }

    fn query<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        key_condition: &'a KeyCondition,
        maps: &'a ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&'a Item>,
        index: Option<&'a IndexInfo>,
        consistent_read: bool,
    ) -> BoxFuture<'a, Result<(Vec<Item>, Option<Item>), StorageError>> {
        retrying_read_table_future("query", key_info, index, move || {
            self.query_impl(
                key_info,
                key_condition,
                maps,
                forward,
                limit,
                exclusive_start_key,
                index,
                consistent_read,
            )
        })
    }

    fn scan<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&'a Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index: Option<&'a IndexInfo>,
        consistent_read: bool,
    ) -> BoxFuture<'a, Result<(Vec<Item>, Option<Item>), StorageError>> {
        retrying_read_table_future("scan", key_info, index, move || {
            self.scan_impl(
                key_info,
                limit,
                exclusive_start_key,
                segment,
                total_segments,
                index,
                consistent_read,
            )
        })
    }

    fn export_table_items<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        export_time_epoch: Option<f64>,
        max_items: u64,
        sink: &'a mut dyn ItemExportSink,
    ) -> BoxFuture<'a, Result<ExportTableItemsSummary, StorageError>> {
        data_table_future(
            key_info,
            self.export_table_items_impl(key_info, export_time_epoch, max_items, sink),
        )
    }

    fn refresh_table_statistics<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        data_table_future(key_info, self.refresh_table_statistics_impl(key_info))
    }

    fn transact_get_items<'a>(
        &'a self,
        ops: &'a [TransactGetOp<'a>],
    ) -> BoxFuture<'a, Result<Vec<Option<Item>>, StorageError>> {
        retrying_transact_get_future(ops, move || self.transact_get_items_impl(ops))
    }

    fn transact_write_items<'a>(
        &'a self,
        ops: &'a [TransactWriteOp<'a>],
        idempotency: Option<IdempotencyClaim<'a>>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        transact_write_future(ops, self.transact_write_items_impl(ops, idempotency))
    }
}

fn data_table_future<'a, T, F>(
    key_info: &'a TableKeyInfo,
    future: F,
) -> BoxFuture<'a, Result<T, StorageError>>
where
    T: Send + 'a,
    F: Future<Output = Result<T, StorageError>> + Send + 'a,
{
    Box::pin(async move { normalize_data_table_error(key_info, future.await) })
}

fn retrying_data_table_future<'a, T, F, Fut>(
    operation: &'static str,
    key_info: &'a TableKeyInfo,
    mut make_future: F,
) -> BoxFuture<'a, Result<T, StorageError>>
where
    T: Send + 'a,
    F: FnMut() -> Fut + Send + 'a,
    Fut: Future<Output = Result<T, StorageError>> + Send + 'a,
{
    Box::pin(async move {
        let result = retry_tidb_idempotent_operation(operation, &mut make_future).await;
        normalize_data_table_error(key_info, result)
    })
}

fn retrying_read_table_future<'a, T, F, Fut>(
    operation: &'static str,
    key_info: &'a TableKeyInfo,
    index: Option<&'a IndexInfo>,
    mut make_future: F,
) -> BoxFuture<'a, Result<T, StorageError>>
where
    T: Send + 'a,
    F: FnMut() -> Fut + Send + 'a,
    Fut: Future<Output = Result<T, StorageError>> + Send + 'a,
{
    Box::pin(async move {
        let result = retry_tidb_idempotent_operation(operation, &mut make_future).await;
        normalize_read_table_error(key_info, index, result)
    })
}

fn retrying_transact_get_future<'a, T, F, Fut>(
    ops: &'a [TransactGetOp<'a>],
    mut make_future: F,
) -> BoxFuture<'a, Result<T, StorageError>>
where
    T: Send + 'a,
    F: FnMut() -> Fut + Send + 'a,
    Fut: Future<Output = Result<T, StorageError>> + Send + 'a,
{
    Box::pin(async move {
        let result = retry_tidb_idempotent_operation("transact_get_items", &mut make_future).await;
        normalize_multi_data_table_error(ops.iter().map(|op| op.key_info), result)
    })
}

fn transact_write_future<'a, T, F>(
    ops: &'a [TransactWriteOp<'a>],
    future: F,
) -> BoxFuture<'a, Result<T, StorageError>>
where
    T: Send + 'a,
    F: Future<Output = Result<T, StorageError>> + Send + 'a,
{
    Box::pin(async move {
        normalize_multi_data_table_error(ops.iter().map(transact_write_key_info), future.await)
    })
}

fn normalize_data_table_error<T>(
    key_info: &TableKeyInfo,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    match result {
        Ok(value) => Ok(value),
        Err(error) if is_missing_physical_data_table(&error, key_info) => {
            Err(StorageError::TableNotFound(key_info.table_name.clone()))
        }
        Err(error) => Err(error),
    }
}

fn normalize_read_table_error<T>(
    key_info: &TableKeyInfo,
    index: Option<&IndexInfo>,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            if is_missing_physical_data_table(&error, key_info) {
                return Err(StorageError::TableNotFound(key_info.table_name.clone()));
            }
            if let Some(index) = index
                && is_missing_native_index(&error, key_info, index)
            {
                return Err(StorageError::IndexNotFound(index.index_name.clone()));
            }
            Err(error)
        }
    }
}

fn normalize_multi_data_table_error<'a, T>(
    tables: impl IntoIterator<Item = &'a TableKeyInfo>,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            for table in tables {
                if is_missing_physical_data_table(&error, table) {
                    return Err(StorageError::TableNotFound(table.table_name.clone()));
                }
            }
            Err(error)
        }
    }
}

fn is_missing_physical_data_table(error: &StorageError, key_info: &TableKeyInfo) -> bool {
    is_table_not_found_tidb_storage_error_for_table(
        error,
        &physical_data_table_name(&key_info.table_id),
    )
}

fn is_missing_native_index(
    error: &StorageError,
    key_info: &TableKeyInfo,
    index: &IndexInfo,
) -> bool {
    is_index_not_found_tidb_storage_error_for_index_on_table(
        error,
        &native_index_name(&index.index_id),
        &physical_data_table_name(&key_info.table_id),
    )
}

fn transact_write_key_info<'a>(op: &'a TransactWriteOp<'_>) -> &'a TableKeyInfo {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
    }
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{
        AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, KeyType, Projection,
        ProjectionType, ScalarAttributeType,
    };

    use super::*;

    fn key_info(table_name: &str, table_id: &str) -> TableKeyInfo {
        TableKeyInfo {
            table_name: table_name.to_owned(),
            account_id: "acct".to_owned(),
            table_id: table_id.to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            }],
            secondary_index_key_schemas: Vec::new(),
            has_lsi: false,
            stream_specification: None,
            stream_label: None,
        }
    }

    fn index_info(index_name: &str, index_id: &str) -> IndexInfo {
        IndexInfo {
            index_name: index_name.to_owned(),
            index_id: index_id.to_owned(),
            index_type: IndexType::Gsi,
            key_schema: vec![KeySchemaElement {
                attribute_name: "gpk".to_owned(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        }
    }

    #[test]
    fn data_table_error_normalization_maps_matching_physical_table() {
        let table = key_info("orders", "tableid");
        let result: Result<(), StorageError> = Err(StorageError::Internal(
            "ERROR 1146 (42S02): Table 'extenddb_data._ddb_tableid' doesn't exist".to_owned(),
        ));

        let error = normalize_data_table_error(&table, result).unwrap_err();

        assert!(matches!(error, StorageError::TableNotFound(name) if name == "orders"));
    }

    #[test]
    fn data_table_error_normalization_leaves_internal_tables_internal() {
        let table = key_info("orders", "tableid");
        let result: Result<(), StorageError> = Err(StorageError::Internal(
            "ERROR 1146 (42S02): Table 'extenddb_data.stream_records' doesn't exist".to_owned(),
        ));

        let error = normalize_data_table_error(&table, result).unwrap_err();

        assert!(matches!(error, StorageError::Internal(_)));
    }

    #[test]
    fn multi_table_error_normalization_uses_matching_api_table() {
        let orders = key_info("orders", "ordersid");
        let users = key_info("users", "usersid");
        let result: Result<(), StorageError> = Err(StorageError::Internal(
            "ERROR 1146 (42S02): Table 'extenddb_data._ddb_usersid' doesn't exist".to_owned(),
        ));

        let error = normalize_multi_data_table_error([&orders, &users], result).unwrap_err();

        assert!(matches!(error, StorageError::TableNotFound(name) if name == "users"));
    }

    #[test]
    fn read_error_normalization_maps_matching_native_index() {
        let table = key_info("orders", "tableid");
        let index = index_info("by_customer", "idx-1");
        let result: Result<(), StorageError> = Err(StorageError::Internal(
            "ERROR 1176 (42000): Key 'idx_idx1' doesn't exist in table '_ddb_tableid'".to_owned(),
        ));

        let error = normalize_read_table_error(&table, Some(&index), result).unwrap_err();

        assert!(matches!(error, StorageError::IndexNotFound(name) if name == "by_customer"));
    }

    #[test]
    fn read_error_normalization_leaves_other_native_indexes_internal() {
        let table = key_info("orders", "tableid");
        let index = index_info("by_customer", "idx-1");
        let result: Result<(), StorageError> = Err(StorageError::Internal(
            "ERROR 1176 (42000): Key 'idx_idx2' doesn't exist in table '_ddb_tableid'".to_owned(),
        ));

        let error = normalize_read_table_error(&table, Some(&index), result).unwrap_err();

        assert!(matches!(error, StorageError::Internal(_)));
    }

    #[tokio::test]
    async fn retrying_data_table_future_reenters_tidb_retryable_read() {
        let table = key_info("orders", "tableid");
        let mut attempts = 0;

        let result = retrying_data_table_future("test_retrying_read", &table, || {
            attempts += 1;
            let attempt = attempts;
            async move {
                if attempt == 1 {
                    Err(StorageError::Internal(
                        "ERROR 8028 (HY000): Information schema is changed. [try again later]"
                            .to_owned(),
                    ))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 2);
    }

    #[tokio::test]
    async fn retrying_read_table_future_normalizes_after_retries() {
        let table = key_info("orders", "tableid");
        let index = index_info("by_customer", "idx-1");
        let mut attempts = 0;

        let result: Result<(), StorageError> =
            retrying_read_table_future("test_retrying_index_read", &table, Some(&index), || {
                attempts += 1;
                let attempt = attempts;
                async move {
                    if attempt == 1 {
                        Err(StorageError::Internal(
                            "ERROR 9007 (HY000): Write conflict".to_owned(),
                        ))
                    } else {
                        Err(StorageError::Internal(
                            "ERROR 1176 (42000): Key 'idx_idx1' doesn't exist in table '_ddb_tableid'"
                                .to_owned(),
                        ))
                    }
                }
            })
            .await;

        let error = result.unwrap_err();

        assert!(matches!(error, StorageError::IndexNotFound(name) if name == "by_customer"));
        assert_eq!(attempts, 2);
    }
}
