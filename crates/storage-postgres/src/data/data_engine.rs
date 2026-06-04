// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Thin `DataEngine` trait implementation that delegates to `impl PostgresEngine`
//! methods in sibling modules.

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, StreamCapture, TransactGetOp, TransactWriteOp};
use futures::future::BoxFuture;

use crate::PostgresEngine;

impl DataEngine for PostgresEngine {
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
        let index_name = index_name.map(|s| s.to_string());
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
        let index_name = index_name.map(|s| s.to_string());
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

    fn scan_full_table_snapshot<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        page_size: i64,
        on_page: extenddb_storage::SnapshotPageHandler<'a>,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        let key_info = key_info.clone();
        Box::pin(async move {
            self.scan_full_table_snapshot_impl(&key_info, page_size, on_page)
                .await
        })
    }

    fn transact_get_items(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> BoxFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        // Clone ops to owned data to satisfy lifetime requirements
        let owned_ops: Vec<_> = ops
            .iter()
            .map(|op| (op.key_info.clone(), op.key.clone()))
            .collect();
        Box::pin(async move {
            // Reconstruct borrowed ops from owned data
            let borrowed_ops: Vec<TransactGetOp> = owned_ops
                .iter()
                .map(|(key_info, key)| TransactGetOp { key_info, key })
                .collect();
            self.transact_get_items_impl(&borrowed_ops).await
        })
    }

    fn transact_write_items(
        &self,
        ops: &[TransactWriteOp<'_>],
        token: Option<(&str, &str)>,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // Clone ops to owned data - unavoidable due to lifetime constraints
        let owned_ops: Vec<_> = ops
            .iter()
            .map(|op| match op {
                TransactWriteOp::Put {
                    key_info,
                    item,
                    condition,
                    maps,
                    return_values_on_ccf,
                    stream,
                } => (
                    0u8,
                    (*key_info).clone(),
                    (*item).clone(),
                    None,
                    Vec::new(),
                    condition.cloned(),
                    (*maps).clone(),
                    *return_values_on_ccf,
                    stream.clone(),
                ),
                TransactWriteOp::Delete {
                    key_info,
                    key,
                    condition,
                    maps,
                    return_values_on_ccf,
                    stream,
                } => (
                    1u8,
                    (*key_info).clone(),
                    (*key).clone(),
                    None,
                    Vec::new(),
                    condition.cloned(),
                    (*maps).clone(),
                    *return_values_on_ccf,
                    stream.clone(),
                ),
                TransactWriteOp::Update {
                    key_info,
                    key,
                    actions,
                    condition,
                    maps,
                    return_values_on_ccf,
                    stream,
                } => (
                    2u8,
                    (*key_info).clone(),
                    (*key).clone(),
                    None,
                    actions.to_vec(),
                    condition.cloned(),
                    (*maps).clone(),
                    *return_values_on_ccf,
                    stream.clone(),
                ),
                TransactWriteOp::ConditionCheck {
                    key_info,
                    key,
                    condition,
                    maps,
                    return_values_on_ccf,
                } => (
                    3u8,
                    (*key_info).clone(),
                    (*key).clone(),
                    Some((*condition).clone()),
                    Vec::new(),
                    None,
                    (*maps).clone(),
                    *return_values_on_ccf,
                    None,
                ),
            })
            .collect();
        let token = token.map(|(a, b)| (a.to_string(), b.to_string()));

        Box::pin(async move {
            // Reconstruct borrowed ops from owned data
            let borrowed_ops: Vec<TransactWriteOp> = owned_ops
                .iter()
                .map(
                    |(
                        tag,
                        key_info,
                        item_or_key,
                        cond_check,
                        actions,
                        condition,
                        maps,
                        rv,
                        stream,
                    )| {
                        match tag {
                            0 => TransactWriteOp::Put {
                                key_info,
                                item: item_or_key,
                                condition: condition.as_ref(),
                                maps,
                                return_values_on_ccf: *rv,
                                stream: stream.clone(),
                            },
                            1 => TransactWriteOp::Delete {
                                key_info,
                                key: item_or_key,
                                condition: condition.as_ref(),
                                maps,
                                return_values_on_ccf: *rv,
                                stream: stream.clone(),
                            },
                            2 => TransactWriteOp::Update {
                                key_info,
                                key: item_or_key,
                                actions,
                                condition: condition.as_ref(),
                                maps,
                                return_values_on_ccf: *rv,
                                stream: stream.clone(),
                            },
                            3 => TransactWriteOp::ConditionCheck {
                                key_info,
                                key: item_or_key,
                                condition: cond_check.as_ref().unwrap(),
                                maps,
                                return_values_on_ccf: *rv,
                            },
                            _ => unreachable!(),
                        }
                    },
                )
                .collect();
            self.transact_write_items_impl(
                &borrowed_ops,
                token.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
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
