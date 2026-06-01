// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Thin `DataEngine` trait implementation that delegates to `impl PostgresEngine`
//! methods in sibling modules.

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{IndexInfo, Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, StreamCapture, TransactGetOp, TransactWriteOp};
use futures::future::BoxFuture;

use crate::PostgresEngine;

impl DataEngine for PostgresEngine {
    fn put_item<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&'a Expr>,
        maps: &'a ExpressionMaps,
        stream: Option<&'a StreamCapture>,
    ) -> BoxFuture<'a, Result<Option<Item>, StorageError>> {
        Box::pin(self.put_item_impl(key_info, item, return_old, condition, maps, stream))
    }

    fn get_item<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        key: &'a Item,
        _consistent_read: bool,
    ) -> BoxFuture<'a, Result<Option<Item>, StorageError>> {
        Box::pin(self.get_item_impl(key_info, key))
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
        Box::pin(self.delete_item_impl(key_info, key, return_old, condition, maps, stream))
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
        Box::pin(self.update_item_impl(
            key_info, key, actions, return_old, return_new, condition, maps, stream,
        ))
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
        _consistent_read: bool,
    ) -> BoxFuture<'a, Result<(Vec<Item>, Option<Item>), StorageError>> {
        Box::pin(self.query_impl(
            key_info,
            key_condition,
            maps,
            forward,
            limit,
            exclusive_start_key,
            index,
        ))
    }

    fn scan<'a>(
        &'a self,
        key_info: &'a TableKeyInfo,
        limit: Option<i64>,
        exclusive_start_key: Option<&'a Item>,
        segment: Option<i64>,
        total_segments: Option<i64>,
        index: Option<&'a IndexInfo>,
        _consistent_read: bool,
    ) -> BoxFuture<'a, Result<(Vec<Item>, Option<Item>), StorageError>> {
        Box::pin(self.scan_impl(
            key_info,
            limit,
            exclusive_start_key,
            segment,
            total_segments,
            index,
        ))
    }

    fn transact_get_items<'a>(
        &'a self,
        ops: &'a [TransactGetOp<'a>],
    ) -> BoxFuture<'a, Result<Vec<Option<Item>>, StorageError>> {
        Box::pin(self.transact_get_items_impl(ops))
    }

    fn transact_write_items<'a>(
        &'a self,
        ops: &'a [TransactWriteOp<'a>],
        token: Option<(&'a str, &'a str)>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(self.transact_write_items_impl(ops, token))
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
