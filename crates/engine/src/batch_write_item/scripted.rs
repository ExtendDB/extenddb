// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `DataEngine` double for the `write_batch` tests: it answers each write
//! from a script and records the writes it received.

use std::collections::VecDeque;
use std::sync::Mutex;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::{
    BoxedFuture, IdempotencyKey, ItemPairResult, QueryResult, StreamCapture, TransactGetOp,
    TransactWriteOp,
};

use super::*;

/// Answers the writes it receives from `script`, in order, and succeeds once
/// the script is used up. Records `table/pk` for every write received.
pub(super) struct Scripted {
    script: Mutex<VecDeque<Result<(), StorageError>>>,
    writes: Mutex<Vec<String>>,
}

impl Scripted {
    pub(super) fn new(script: Vec<Result<(), StorageError>>) -> Self {
        Self {
            script: Mutex::new(script.into()),
            writes: Mutex::new(Vec::new()),
        }
    }

    /// `table/pk` of every write received, in order.
    pub(super) fn writes(&self) -> Vec<String> {
        self.writes.lock().unwrap().clone()
    }

    fn answer(&self, key_info: &TableKeyInfo, item: &Item) -> Result<Option<Item>, StorageError> {
        let pk = match item.get("pk") {
            Some(AttributeValue::S(s)) => s.clone(),
            other => panic!("the double only takes string partition keys, got {other:?}"),
        };
        self.writes
            .lock()
            .unwrap()
            .push(format!("{}/{pk}", key_info.table_name));
        let next = self.script.lock().unwrap().pop_front();
        next.unwrap_or(Ok(())).map(|()| None)
    }
}

fn unused<T: Send + 'static>() -> BoxedFuture<'static, Result<T, StorageError>> {
    Box::pin(async { Err(StorageError::Internal("not used by these tests".to_owned())) })
}

impl DataEngine for Scripted {
    fn put_item(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxedFuture<'_, Result<Option<Item>, StorageError>> {
        Box::pin(std::future::ready(self.answer(key_info, &item)))
    }

    fn delete_item(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
        _return_old: bool,
        _condition: Option<&Expr>,
        _maps: &ExpressionMaps,
        _stream: Option<&StreamCapture>,
    ) -> BoxedFuture<'_, Result<Option<Item>, StorageError>> {
        Box::pin(std::future::ready(self.answer(key_info, key)))
    }

    fn get_item(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
    ) -> BoxedFuture<'_, Result<Option<Item>, StorageError>> {
        unused()
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
        unused()
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
        unused()
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
        unused()
    }

    fn scan_key_in_segment(
        &self,
        _key_info: &TableKeyInfo,
        _key: &Item,
        _segment: i64,
        _total_segments: i64,
        _index_name: Option<&str>,
    ) -> BoxedFuture<'_, Result<bool, StorageError>> {
        unused()
    }

    fn transact_get_items(
        &self,
        _ops: &[TransactGetOp<'_>],
    ) -> BoxedFuture<'_, Result<Vec<Option<Item>>, StorageError>> {
        unused()
    }

    fn transact_write_items(
        &self,
        _ops: &[TransactWriteOp<'_>],
        _idempotency: Option<IdempotencyKey<'_>>,
    ) -> BoxedFuture<'_, Result<(), StorageError>> {
        unused()
    }

    fn cleanup_expired_idempotency_tokens(
        &self,
        _max_age_seconds: i64,
    ) -> BoxedFuture<'_, Result<u64, StorageError>> {
        unused()
    }
}
