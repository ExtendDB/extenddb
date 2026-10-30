// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pins that the request's `ConsistentRead` reaches storage as
//! [`ReadOptions::consistent_read`] on every read path: `GetItem`, `Query`,
//! `Scan` and `BatchGetItem` (per table, `false` told apart from absent);
//! that a local secondary index read carries the client's value on `Query`
//! and `Scan`; that a global secondary index read reaches storage with
//! `consistent_read: false` because the handler refuses `ConsistentRead:
//! true` on a GSI before storage is called, on `Query` and on `Scan`; and
//! that the export scan asks for a consistent read.
//!
//! The storage double in [`recording`] records every read call with the
//! options it received and answers with no items; the handlers run end to end
//! above it.

mod fixtures;
mod recording;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;

use crate::OperationContext;
use fixtures::{ACCOUNT, EVENTS, GSI, LSI, ORDERS, USERS};
use recording::{ReadCall, RecordingStorage};

fn context(storage: &Arc<RecordingStorage>) -> OperationContext {
    context_with_export_paths(storage, Vec::new())
}

fn context_with_export_paths(
    storage: &Arc<RecordingStorage>,
    export_paths: Vec<Arc<PathBuf>>,
) -> OperationContext {
    let empty: Vec<Arc<PathBuf>> = Vec::new();
    OperationContext {
        storage: Arc::clone(storage) as Arc<dyn extenddb_storage::StorageEngine>,
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from(ACCOUNT),
        import_paths: Arc::from(empty),
        export_paths: Arc::from(export_paths),
        authorized_key_info: HashMap::new(),
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
    }
}

/// Adds `ConsistentRead` to `body` when the test names a value; `None` leaves
/// the member absent, which is the third shape a client can send.
fn with_consistent_read(mut body: Value, consistent_read: Option<bool>) -> Value {
    if let Some(flag) = consistent_read {
        body["ConsistentRead"] = Value::Bool(flag);
    }
    body
}

fn get_item_body() -> Value {
    json!({"TableName": ORDERS, "Key": {"pk": {"S": "order-1"}}})
}

fn query_body() -> Value {
    json!({
        "TableName": ORDERS,
        "KeyConditionExpression": "pk = :v",
        "ExpressionAttributeValues": {":v": {"S": "order-1"}}
    })
}

fn scan_body() -> Value {
    json!({"TableName": ORDERS})
}

fn gsi_query_body() -> Value {
    json!({
        "TableName": ORDERS,
        "IndexName": GSI,
        "KeyConditionExpression": "category = :s",
        "ExpressionAttributeValues": {":s": {"S": "open"}}
    })
}

fn gsi_scan_body() -> Value {
    json!({"TableName": ORDERS, "IndexName": GSI})
}

fn lsi_query_body() -> Value {
    json!({
        "TableName": EVENTS,
        "IndexName": LSI,
        "KeyConditionExpression": "pk = :v",
        "ExpressionAttributeValues": {":v": {"S": "device-1"}}
    })
}

fn lsi_scan_body() -> Value {
    json!({"TableName": EVENTS, "IndexName": LSI})
}

/// The three shapes a client can send and what each must reach storage as.
const SENT_AND_EXPECTED: [(Option<bool>, bool); 3] =
    [(Some(true), true), (Some(false), false), (None, false)];

/// Runs one handler over a fresh recording storage and returns the single
/// read it made.
async fn single_read(op: &str, body: Value) -> ReadCall {
    let storage = RecordingStorage::new();
    let ctx = context(&storage);
    let result = match op {
        "GetItem" => crate::get_item::handle_get_item(body, &ctx).await,
        "Query" => crate::query::handle_query(body, &ctx).await,
        "Scan" => crate::scan::handle_scan(body, &ctx).await,
        other => panic!("no handler for {other}"),
    };
    let _ = result.unwrap_or_else(|e| panic!("{op} failed before storage: {e:?}"));
    let reads = storage.reads();
    assert_eq!(reads.len(), 1, "{op} made {} storage reads", reads.len());
    reads.into_iter().next().unwrap()
}

#[tokio::test]
async fn get_item_consistent_read_true_reaches_storage() {
    let call = single_read("GetItem", with_consistent_read(get_item_body(), Some(true))).await;
    assert_eq!(call.op, "GetItem");
    assert_eq!(call.table, ORDERS);
    assert!(call.opts.consistent_read);
}

#[tokio::test]
async fn get_item_consistent_read_false_and_absent_reach_storage_as_false() {
    for sent in [Some(false), None] {
        let call = single_read("GetItem", with_consistent_read(get_item_body(), sent)).await;
        assert!(
            !call.opts.consistent_read,
            "ConsistentRead {sent:?} reached storage as consistent"
        );
    }
}

#[tokio::test]
async fn query_consistent_read_reaches_storage() {
    for (sent, expected) in SENT_AND_EXPECTED {
        let call = single_read("Query", with_consistent_read(query_body(), sent)).await;
        assert_eq!(call.op, "Query");
        assert_eq!(call.index_name, None);
        assert_eq!(
            call.opts.consistent_read, expected,
            "ConsistentRead {sent:?} reached storage as {}",
            call.opts.consistent_read
        );
    }
}

#[tokio::test]
async fn scan_consistent_read_reaches_storage() {
    for (sent, expected) in SENT_AND_EXPECTED {
        let call = single_read("Scan", with_consistent_read(scan_body(), sent)).await;
        assert_eq!(call.op, "Scan");
        assert_eq!(call.index_name, None);
        assert_eq!(
            call.opts.consistent_read, expected,
            "ConsistentRead {sent:?} reached storage as {}",
            call.opts.consistent_read
        );
    }
}

#[tokio::test]
async fn batch_get_item_carries_consistent_read_per_table() {
    // Orders asks for a consistent read; Users sends `false` in one round and
    // leaves the member absent in the other. Both Users shapes must reach
    // storage as `false`, so a handler that treats "present" as "true" fails.
    for users_sent in [Some(false), None] {
        let storage = RecordingStorage::new();
        let ctx = context(&storage);
        let body = json!({
            "RequestItems": {
                ORDERS: {
                    "Keys": [{"pk": {"S": "order-1"}}, {"pk": {"S": "order-2"}}],
                    "ConsistentRead": true
                },
                USERS: with_consistent_read(
                    json!({"Keys": [{"id": {"S": "user-1"}}]}),
                    users_sent
                )
            }
        });
        let _ = crate::batch_get_item::handle_batch_get_item(body, &ctx)
            .await
            .expect("BatchGetItem failed before storage");

        let reads = storage.reads();
        assert_eq!(reads.len(), 3, "one storage read per key: {reads:?}");
        let orders: Vec<&ReadCall> = reads.iter().filter(|r| r.table == ORDERS).collect();
        let users: Vec<&ReadCall> = reads.iter().filter(|r| r.table == USERS).collect();
        assert_eq!(orders.len(), 2);
        assert_eq!(users.len(), 1);
        assert!(
            orders
                .iter()
                .all(|r| r.op == "GetItem" && r.opts.consistent_read),
            "Orders keys must reach storage consistent: {orders:?}"
        );
        assert!(
            users
                .iter()
                .all(|r| r.op == "GetItem" && !r.opts.consistent_read),
            "Users ConsistentRead {users_sent:?} must reach storage as false: {users:?}"
        );
    }
}

#[tokio::test]
async fn gsi_query_reaches_storage_eventually_consistent() {
    for sent in [Some(false), None] {
        let call = single_read("Query", with_consistent_read(gsi_query_body(), sent)).await;
        assert_eq!(call.op, "Query");
        assert_eq!(call.index_name.as_deref(), Some(GSI));
        assert!(
            !call.opts.consistent_read,
            "a GSI read must reach storage with consistent_read: false"
        );
    }
}

#[tokio::test]
async fn gsi_query_with_consistent_read_is_refused_before_storage() {
    let storage = RecordingStorage::new();
    let ctx = context(&storage);
    let result =
        crate::query::handle_query(with_consistent_read(gsi_query_body(), Some(true)), &ctx).await;
    match result {
        Err(DynamoDbError::ValidationException(msg)) => {
            assert_eq!(
                msg,
                "Consistent reads are not supported on global secondary indexes"
            );
        }
        Err(other) => panic!("expected ValidationException, got {other:?}"),
        Ok(_) => panic!("ConsistentRead: true on a GSI must be refused"),
    }
    assert!(
        storage.reads().is_empty(),
        "the refusal must happen before any storage read: {:?}",
        storage.reads()
    );
}

#[tokio::test]
async fn gsi_scan_reaches_storage_eventually_consistent() {
    for sent in [Some(false), None] {
        let call = single_read("Scan", with_consistent_read(gsi_scan_body(), sent)).await;
        assert_eq!(call.op, "Scan");
        assert_eq!(call.index_name.as_deref(), Some(GSI));
        assert!(
            !call.opts.consistent_read,
            "a GSI scan must reach storage with consistent_read: false"
        );
    }
}

#[tokio::test]
async fn gsi_scan_with_consistent_read_is_refused_before_storage() {
    let storage = RecordingStorage::new();
    let ctx = context(&storage);
    let result =
        crate::scan::handle_scan(with_consistent_read(gsi_scan_body(), Some(true)), &ctx).await;
    match result {
        Err(DynamoDbError::ValidationException(msg)) => {
            assert_eq!(
                msg,
                "Consistent reads are not supported on global secondary indexes"
            );
        }
        Err(other) => panic!("expected ValidationException, got {other:?}"),
        Ok(_) => panic!("ConsistentRead: true on a GSI scan must be refused"),
    }
    assert!(
        storage.reads().is_empty(),
        "the refusal must happen before any storage read: {:?}",
        storage.reads()
    );
}

/// A local secondary index is served strongly consistent on request, so the
/// client's value goes through unchanged on both Query and Scan.
#[tokio::test]
async fn lsi_query_and_scan_carry_the_clients_consistent_read() {
    for (op, body) in [("Query", lsi_query_body()), ("Scan", lsi_scan_body())] {
        for (sent, expected) in SENT_AND_EXPECTED {
            let call = single_read(op, with_consistent_read(body.clone(), sent)).await;
            assert_eq!(call.op, op);
            assert_eq!(call.index_name.as_deref(), Some(LSI));
            assert_eq!(
                call.opts.consistent_read, expected,
                "{op} on an LSI with ConsistentRead {sent:?} reached storage as {}",
                call.opts.consistent_read
            );
        }
    }
}

/// A temp directory removed when the test ends: on success, on an assertion
/// failure and on a panic inside the handler.
struct ExportRoot(PathBuf);

impl ExportRoot {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "extenddb-read-options-export-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&root).expect("export root");
        // The path jail compares canonical paths, as the server does for its
        // configured roots; a temp dir behind a symlink must be resolved first.
        Self(root.canonicalize().expect("canonical export root"))
    }
}

impl Drop for ExportRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// An export reflects every write acknowledged before the export time, so the
/// engine asks storage for a consistent read on every page.
#[tokio::test]
async fn export_scan_asks_for_a_consistent_read() {
    let root = ExportRoot::new();
    let storage = RecordingStorage::new();
    let ctx = context_with_export_paths(&storage, vec![Arc::new(root.0.clone())]);
    let file_path = root.0.join(ACCOUNT).join("orders.json");
    let body = json!({
        "TableArn": format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/{ORDERS}"),
        "FilePath": file_path.to_string_lossy(),
    });
    let _ = crate::import_export::handle_export_table(body, &ctx)
        .await
        .expect("export failed before storage");

    let reads = storage.reads();
    assert!(!reads.is_empty(), "the export made no storage read");
    assert!(
        reads
            .iter()
            .all(|r| r.op == "Scan" && r.table == ORDERS && r.opts.consistent_read),
        "every export page must be a consistent scan of the table: {reads:?}"
    );
}
