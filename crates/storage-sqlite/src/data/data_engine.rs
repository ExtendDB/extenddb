// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DataEngine` trait implementation for `SqliteEngine`.
//!
//! Each method clones its borrowed arguments into owned values and delegates to
//! the corresponding `*_impl` method inside a boxed future. The transact-write
//! path rebuilds borrowed `TransactWriteOp`s from owned components inside the
//! future, since the trait's borrowed ops cannot outlive the call.

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{Item, ReturnValuesOnConditionCheckFailure, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, IdempotencyKey, StreamCapture, TransactGetOp, TransactWriteOp};
use futures::future::BoxFuture;

use crate::store::SqliteEngine;

impl DataEngine for SqliteEngine {
    /// Declares vector support by handing over the implementation. `Some(self)`
    /// only compiles because `SqliteEngine` implements `VectorSearchEngine`, so
    /// this cannot claim a capability the backend does not have.
    fn as_vector_search(&self) -> Option<&dyn extenddb_storage::VectorSearchEngine> {
        Some(self)
    }

    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
        marker: Option<&extenddb_storage::BulkRowMarker>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        let marker = marker.cloned();
        Box::pin(async move {
            self.put_item_impl(
                &key_info,
                item,
                return_old,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
                marker.as_ref(),
            )
            .await
        })
    }

    fn put_items_batch(
        &self,
        key_info: &TableKeyInfo,
        rows: Vec<extenddb_storage::BatchRow>,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::BatchRowResult>, StorageError>> {
        let key_info = key_info.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.put_items_batch_impl(&key_info, rows, stream.as_ref())
                .await
        })
    }

    fn bulk_row_outcome(
        &self,
        account_id: &str,
        operation_id: &str,
        file_index: i64,
        row_index: i64,
    ) -> BoxFuture<'_, Result<Option<extenddb_storage::RowOutcome>, StorageError>> {
        let account_id = account_id.to_owned();
        let operation_id = operation_id.to_owned();
        Box::pin(async move {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT outcome FROM bulk_row_outcomes \
                 WHERE account_id = ? AND operation_id = ? \
                   AND file_index = ? AND row_index = ?",
            )
            .bind(&account_id)
            .bind(&operation_id)
            .bind(file_index)
            .bind(row_index)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            row.map(|(outcome,)| {
                extenddb_storage::RowOutcome::parse(&outcome).ok_or_else(|| {
                    StorageError::Internal(format!("unknown bulk row outcome '{outcome}'"))
                })
            })
            .transpose()
        })
    }

    fn bulk_write_takeover(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        cursor: (i64, i64),
    ) -> BoxFuture<'_, Result<u32, StorageError>> {
        let account_id = account_id.to_owned();
        let operation_id = operation_id.to_owned();
        let owner = owner.to_owned();
        Box::pin(async move {
            // Under the engine write lock, so a batch of the earlier owner
            // either committed before this or reads the new owner.
            let _writer = self.write_lock.lock().await;
            let stalled: i64 = sqlx::query_scalar(
                "INSERT INTO bulk_write_owners \
                 (account_id, operation_id, owner, cursor_file, cursor_row, stalled) \
                 VALUES (?, ?, ?, ?, ?, 0) \
                 ON CONFLICT (account_id, operation_id) DO UPDATE SET \
                 owner = excluded.owner, \
                 stalled = CASE WHEN bulk_write_owners.cursor_file = excluded.cursor_file \
                   AND bulk_write_owners.cursor_row = excluded.cursor_row \
                   THEN bulk_write_owners.stalled + 1 ELSE 0 END, \
                 cursor_file = excluded.cursor_file, cursor_row = excluded.cursor_row \
                 RETURNING stalled",
            )
            .bind(&account_id)
            .bind(&operation_id)
            .bind(&owner)
            .bind(cursor.0)
            .bind(cursor.1)
            .fetch_one(&self.pool)
            .await
            .map_err(crate::sqlite_util::map_sqlx_err)?;
            Ok(u32::try_from(stalled).unwrap_or(u32::MAX))
        })
    }

    fn bulk_rows_recorded(
        &self,
        account_id: &str,
        operation_id: &str,
        file_index: i64,
        from_row: i64,
        limit: i64,
    ) -> BoxFuture<'_, Result<Vec<extenddb_storage::RecordedRow>, StorageError>> {
        let account_id = account_id.to_owned();
        let operation_id = operation_id.to_owned();
        Box::pin(async move {
            let rows: Vec<(i64, String, Option<i64>, Option<String>)> = sqlx::query_as(
                "SELECT row_index, outcome, old_size, old_image FROM bulk_row_outcomes \
                 WHERE account_id = ? AND operation_id = ? AND file_index = ? \
                   AND row_index >= ? ORDER BY row_index LIMIT ?",
            )
            .bind(&account_id)
            .bind(&operation_id)
            .bind(file_index)
            .bind(from_row)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(crate::sqlite_util::map_sqlx_err)?;
            rows.into_iter()
                .map(|(row_index, outcome, old_size, old_image)| {
                    let outcome =
                        extenddb_storage::RowOutcome::parse(&outcome).ok_or_else(|| {
                            StorageError::Internal(format!("unknown bulk row outcome '{outcome}'"))
                        })?;
                    let old_image = old_image
                        .map(|text| serde_json::from_str(&text))
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    Ok(extenddb_storage::RecordedRow {
                        row_index,
                        outcome,
                        old_size,
                        old_image,
                    })
                })
                .collect()
        })
    }

    fn delete_bulk_row_outcomes(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        let account_id = account_id.to_owned();
        let operation_id = operation_id.to_owned();
        Box::pin(async move {
            // D1: every writer holds the engine write lock. A single DELETE
            // is its own transaction.
            let _writer = self.write_lock.lock().await;
            let result = sqlx::query(
                "DELETE FROM bulk_row_outcomes WHERE account_id = ? AND operation_id = ?",
            )
            .bind(&account_id)
            .bind(&operation_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query("DELETE FROM bulk_write_owners WHERE account_id = ? AND operation_id = ?")
                .bind(&account_id)
                .bind(&operation_id)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(result.rows_affected())
        })
    }

    fn get_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        Box::pin(async move { self.get_item_impl(&key_info, &key).await })
    }

    fn delete_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.delete_item_impl(
                &key_info,
                &key,
                return_old,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
        })
    }

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
    ) -> BoxFuture<'_, Result<(Option<Item>, Option<Item>), StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        let actions = actions.to_vec();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.update_item_impl(
                &key_info,
                &key,
                &actions,
                return_old,
                return_new,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
        })
    }

    fn query(
        &self,
        key_info: &TableKeyInfo,
        key_condition: &KeyCondition,
        maps: &ExpressionMaps,
        forward: bool,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<(Vec<Item>, Option<Item>), StorageError>> {
        let key_info = key_info.clone();
        let key_condition = key_condition.clone();
        let maps = maps.clone();
        let exclusive_start_key = exclusive_start_key.cloned();
        let index_name = index_name.map(str::to_owned);
        Box::pin(async move {
            self.query_impl(
                &key_info,
                &key_condition,
                &maps,
                forward,
                limit,
                exclusive_start_key.as_ref(),
                index_name.as_deref(),
            )
            .await
        })
    }

    fn scan(
        &self,
        key_info: &TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<(Vec<Item>, Option<Item>), StorageError>> {
        let key_info = key_info.clone();
        let exclusive_start_key = exclusive_start_key.cloned();
        let index_name = index_name.map(str::to_owned);
        Box::pin(async move {
            self.scan_impl(
                &key_info,
                limit,
                exclusive_start_key.as_ref(),
                segment,
                total_segments,
                index_name.as_deref(),
            )
            .await
        })
    }

    fn scan_key_in_segment(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        segment: i64,
        total_segments: i64,
        index_name: Option<&str>,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        let key_info = key_info.clone();
        let key = key.clone();
        let index_name = index_name.map(str::to_owned);
        Box::pin(async move {
            self.scan_key_in_segment_impl(
                &key_info,
                &key,
                segment,
                total_segments,
                index_name.as_deref(),
            )
            .await
        })
    }

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        let owned: Vec<(TableKeyInfo, Item)> = ops
            .iter()
            .map(|op| (op.key_info.clone(), op.key.clone()))
            .collect();
        Box::pin(async move {
            let borrowed: Vec<TransactGetOp> = owned
                .iter()
                .map(|(key_info, key)| TransactGetOp { key_info, key })
                .collect();
            self.transact_get_items_impl(&borrowed).await
        })
    }

    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let owned: Vec<OwnedWriteOp> = ops.iter().map(OwnedWriteOp::from_op).collect();
        let idempotency = idempotency.map(|k| {
            (
                k.account_id.to_owned(),
                k.token.to_owned(),
                k.fingerprint.to_owned(),
            )
        });
        Box::pin(async move {
            let borrowed: Vec<TransactWriteOp> = owned.iter().map(OwnedWriteOp::as_op).collect();
            self.transact_write_items_impl(
                &borrowed,
                idempotency.as_ref().map(|(a, t, f)| IdempotencyKey {
                    account_id: a,
                    token: t,
                    fingerprint: f,
                }),
            )
            .await
        })
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        max_age_seconds: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            self.cleanup_expired_idempotency_tokens_impl(max_age_seconds)
                .await
        })
    }
}

/// Owned form of a `TransactWriteOp`, so the borrowed ops can be reconstructed
/// inside the `'static` future.
enum OwnedWriteOp {
    Put {
        key_info: TableKeyInfo,
        item: Item,
        condition: Option<Expr>,
        maps: ExpressionMaps,
        rv: ReturnValuesOnConditionCheckFailure,
        stream: Option<StreamCapture>,
    },
    Delete {
        key_info: TableKeyInfo,
        key: Item,
        condition: Option<Expr>,
        maps: ExpressionMaps,
        rv: ReturnValuesOnConditionCheckFailure,
        stream: Option<StreamCapture>,
    },
    Update {
        key_info: TableKeyInfo,
        key: Item,
        actions: Vec<UpdateAction>,
        condition: Option<Expr>,
        maps: ExpressionMaps,
        rv: ReturnValuesOnConditionCheckFailure,
        stream: Option<StreamCapture>,
    },
    ConditionCheck {
        key_info: TableKeyInfo,
        key: Item,
        condition: Expr,
        maps: ExpressionMaps,
        rv: ReturnValuesOnConditionCheckFailure,
    },
}

impl OwnedWriteOp {
    fn from_op(op: &TransactWriteOp<'_>) -> Self {
        match op {
            TransactWriteOp::Put {
                key_info,
                item,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => Self::Put {
                key_info: (*key_info).clone(),
                item: (*item).clone(),
                condition: condition.cloned(),
                maps: (*maps).clone(),
                rv: *return_values_on_ccf,
                stream: stream.clone(),
            },
            TransactWriteOp::Delete {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => Self::Delete {
                key_info: (*key_info).clone(),
                key: (*key).clone(),
                condition: condition.cloned(),
                maps: (*maps).clone(),
                rv: *return_values_on_ccf,
                stream: stream.clone(),
            },
            TransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                return_values_on_ccf,
                stream,
            } => Self::Update {
                key_info: (*key_info).clone(),
                key: (*key).clone(),
                actions: actions.to_vec(),
                condition: condition.cloned(),
                maps: (*maps).clone(),
                rv: *return_values_on_ccf,
                stream: stream.clone(),
            },
            TransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf,
            } => Self::ConditionCheck {
                key_info: (*key_info).clone(),
                key: (*key).clone(),
                condition: (*condition).clone(),
                maps: (*maps).clone(),
                rv: *return_values_on_ccf,
            },
        }
    }

    fn as_op(&self) -> TransactWriteOp<'_> {
        match self {
            Self::Put {
                key_info,
                item,
                condition,
                maps,
                rv,
                stream,
            } => TransactWriteOp::Put {
                key_info,
                item,
                condition: condition.as_ref(),
                maps,
                return_values_on_ccf: *rv,
                stream: stream.clone(),
            },
            Self::Delete {
                key_info,
                key,
                condition,
                maps,
                rv,
                stream,
            } => TransactWriteOp::Delete {
                key_info,
                key,
                condition: condition.as_ref(),
                maps,
                return_values_on_ccf: *rv,
                stream: stream.clone(),
            },
            Self::Update {
                key_info,
                key,
                actions,
                condition,
                maps,
                rv,
                stream,
            } => TransactWriteOp::Update {
                key_info,
                key,
                actions,
                condition: condition.as_ref(),
                maps,
                return_values_on_ccf: *rv,
                stream: stream.clone(),
            },
            Self::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                rv,
            } => TransactWriteOp::ConditionCheck {
                key_info,
                key,
                condition,
                maps,
                return_values_on_ccf: *rv,
            },
        }
    }
}
