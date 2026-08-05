// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Concurrency regression tests.
//!
//! Many clients issuing read-then-write operations in parallel must never
//! surface `InternalServerError` (500). This guards the SQLite write path,
//! where a deferred `BEGIN` whose read snapshot is invalidated by another
//! connection pool (the catalog/credential pool shares the same database file)
//! returns `SQLITE_BUSY_SNAPSHOT` (517) — which `busy_timeout` cannot retry.
//! Opening every write transaction with `BEGIN IMMEDIATE` takes the reserved
//! write lock up front, eliminating that race. Backend-agnostic: passes against
//! real DynamoDB as well.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};

const TASKS: usize = 16;
const PER_TASK: usize = 64;

async fn make_simple_table(c: &aws_sdk_dynamodb::Client, name: &str) {
    let _ = c
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .send()
        .await;
    wait_for_active(c, name).await;
}

/// Concurrent conditional puts (read-then-write) followed by parallel deletes
/// across many tasks — the Rust analogue of the Python `test_parallel_deletes`
/// that exposed the 517-as-500 regression under load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_writes_then_deletes_no_internal_error() {
    let c = client();
    let table = format!("ConcurrencyRegression_{}", ts());
    make_simple_table(c, &table).await;

    // Phase 1 — concurrent conditional puts on distinct keys. Each put reads
    // (attribute_not_exists) then writes inside one transaction.
    let mut handles = Vec::new();
    for t in 0..TASKS {
        let c = c.clone();
        let table = table.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_TASK {
                let key = format!("t{t}-{i}");
                c.put_item()
                    .table_name(&table)
                    .item("pk", s(&key))
                    .item("v", n(i as i64))
                    .condition_expression("attribute_not_exists(pk)")
                    .send()
                    .await
                    .unwrap_or_else(|e| panic!("put {key} failed: {}", err_msg(&e)));
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Phase 2 — parallel deletes of every key across all tasks.
    let mut handles = Vec::new();
    for t in 0..TASKS {
        let c = c.clone();
        let table = table.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_TASK {
                let key = format!("t{t}-{i}");
                c.delete_item()
                    .table_name(&table)
                    .key("pk", s(&key))
                    .send()
                    .await
                    .unwrap_or_else(|e| panic!("delete {key} failed: {}", err_msg(&e)));
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // All keys must be gone.
    let scan = c
        .scan()
        .table_name(&table)
        .send()
        .await
        .expect("scan failed");
    assert_eq!(scan.count(), 0, "all items should have been deleted");

    let _ = c.delete_table().table_name(&table).send().await;
}
