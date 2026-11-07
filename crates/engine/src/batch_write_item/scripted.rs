// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `DataEngine` double for the `write_batch` tests, and the fixtures that run
//! `write_batch` on it: it answers each write from a script and records the
//! writes it received.

use std::collections::VecDeque;
use std::sync::Mutex;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use extenddb_storage::{
    BoxedFuture, IdempotencyKey, ItemPairResult, QueryResult, StreamCapture, TransactGetOp,
    TransactWriteOp,
};
use serde_json::{Value, json};

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

// --- fixtures ---------------------------------------------------------------

/// A hash-only table keyed by a string `pk`.
pub(super) fn table(name: &str) -> TableKeyInfo {
    let key_schema = vec![KeySchemaElement {
        attribute_name: "pk".to_owned(),
        key_type: KeyType::Hash,
    }];
    TableKeyInfo {
        table_name: name.to_owned(),
        account_id: "123456789012".to_owned(),
        table_id: format!("id-{name}"),
        key_schema: key_schema.clone(),
        base_key_schema: key_schema,
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }],
        has_lsi: false,
        global_secondary_indexes: vec![],
        local_secondary_indexes: vec![],
        stream_specification: None,
        vector_indexes: vec![],
    }
}

/// One successful lookup for each of `names`.
pub(super) fn found(names: &[&str]) -> Vec<Result<TableKeyInfo, StorageError>> {
    names.iter().map(|n| Ok(table(n))).collect()
}

pub(super) fn put(pk: &str) -> Value {
    json!({"PutRequest": {"Item": {"pk": {"S": pk}}}})
}

/// A put whose partition key is a number, against a table keyed by a string.
pub(super) fn put_with_wrong_key_type() -> Value {
    json!({"PutRequest": {"Item": {"pk": {"N": "1"}}}})
}

/// Run `write_batch` on `request_items`, taking the tables in the order of
/// `order` with one entry of `lookups` for each.
pub(super) async fn run(
    data: &Scripted,
    request_items: Value,
    order: &[&str],
    lookups: Vec<Result<TableKeyInfo, StorageError>>,
) -> Result<DispatchResult, DynamoDbError> {
    run_body(
        data,
        json!({ "RequestItems": request_items }),
        order,
        lookups,
    )
    .await
}

/// Like [`run`], for a request body that carries more than `RequestItems`.
pub(super) async fn run_body(
    data: &Scripted,
    body: Value,
    order: &[&str],
    lookups: Vec<Result<TableKeyInfo, StorageError>>,
) -> Result<DispatchResult, DynamoDbError> {
    let input: BatchWriteItemInput = serde_json::from_value(body).unwrap();
    let tables: Vec<(&String, &Vec<WriteRequest>)> = order
        .iter()
        .map(|name| input.request_items.get_key_value(*name).unwrap())
        .collect();
    let region: Arc<str> = Arc::from("us-east-1");
    write_batch(
        data,
        &LimitsConfig::default(),
        &region,
        &input,
        &tables,
        lookups,
    )
    .await
}

/// The error of a batch that must have been rejected.
pub(super) fn rejected(result: Result<DispatchResult, DynamoDbError>) -> DynamoDbError {
    match result {
        Err(e) => e,
        Ok(r) => panic!("the batch must be rejected, got {}", r.body),
    }
}

/// The body of a batch that must have been answered with a 200.
pub(super) fn answered(result: Result<DispatchResult, DynamoDbError>) -> DispatchResult {
    result.unwrap_or_else(|e| panic!("the batch must answer 200: {e:?}"))
}
