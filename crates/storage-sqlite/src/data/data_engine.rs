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
    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> BoxFuture<'_, Result<Option<Item>, StorageError>> {
        let key_info = key_info.clone();
        let condition = condition.cloned();
        let maps = maps.clone();
        let stream = stream.cloned();
        Box::pin(async move {
            self.put_item_impl(
                &key_info,
                item,
                return_old,
                condition.as_ref(),
                &maps,
                stream.as_ref(),
            )
            .await
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
