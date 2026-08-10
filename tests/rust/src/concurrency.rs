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

/// Concurrent unconditional `UpdateItem` upserts creating the SAME new item must
/// all succeed, and each update expression must apply on top of whatever the
/// previous writer committed.
///
/// The row lock taken by the read side serialises writers to an item that already
/// exists, but it cannot lock a row that does not exist yet, so two writers could
/// both take the insert path and the loser was answered with
/// `ConditionalCheckFailedException`. An `UpdateItem` carrying no condition is an
/// upsert and must never produce that error.
///
/// Asserting the merge, not merely the absence of the error, is deliberate and is
/// what makes this test discriminating. Measured against real DynamoDB on
/// 2026-08-10: four writers each setting a different attribute on one brand-new key
/// yield an item holding all four. A fix that resolved the conflict by overwriting
/// with the loser's own computed item would keep every call succeeding while
/// silently dropping the other writers' attributes, and would pass a test that only
/// counted errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_unconditional_upserts_to_one_new_item_all_succeed_and_merge() {
    let c = client();
    let table = format!("ConcurrentUpsert_{}", ts());
    make_simple_table(c, &table).await;

    // Several trials: the race window is small, so one trial can miss it.
    for trial in 0..5 {
        let key = format!("same-key-{trial}");
        let attrs = ["a", "b", "c", "d"];
        let mut handles = Vec::new();
        for (i, attr) in attrs.iter().enumerate() {
            let c = c.clone();
            let table = table.clone();
            let key = key.clone();
            let attr = (*attr).to_string();
            handles.push(tokio::spawn(async move {
                c.update_item()
                    .table_name(&table)
                    .key("pk", s(&key))
                    .update_expression(format!("SET #{attr} = :v"))
                    .expression_attribute_names(format!("#{attr}"), &attr)
                    .expression_attribute_values(":v", s(&format!("writer-{i}")))
                    .send()
                    .await
                    .map_err(|e| format!("{:?}", e.into_service_error()))
            }));
        }

        for h in handles {
            let r = h.await.expect("task panicked");
            assert!(
                r.is_ok(),
                "trial {trial}: an unconditional UpdateItem creating a new item failed: {:?}",
                r.err()
            );
        }

        let got = c
            .get_item()
            .table_name(&table)
            .key("pk", s(&key))
            .consistent_read(true)
            .send()
            .await
            .expect("get_item")
            .item
            .expect("item must exist after four successful upserts");
        for attr in attrs {
            assert!(
                got.contains_key(attr),
                "trial {trial}: attribute '{attr}' was lost; every writer's expression must be \
                 applied on top of the previous winner, got keys {:?}",
                got.keys().collect::<Vec<_>>()
            );
        }
    }
}

/// Conditional creates keep their existing semantics under the create-race retry.
///
/// Exactly one `attribute_not_exists(pk)` writer may win a race to create the same
/// key, and `attribute_exists(pk)` must never win against a key that never existed.
///
/// This is a regression guard, not a bug demonstration, and the distinction is worth
/// recording: it passes both before and after the retry was introduced. Re-evaluating
/// the condition against the race winner produces the same answers, because an
/// `attribute_exists` writer fails against its empty base before ever reaching the
/// insert, and an `attribute_not_exists` writer that loses the race still fails when
/// re-evaluated against the winner. The value of this test is proving the retry did
/// not perturb either outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_one_racing_conditional_create_wins() {
    let c = client();
    let table = format!("ConcurrentCondUpsert_{}", ts());
    make_simple_table(c, &table).await;

    for (expr, expected_winners) in [("attribute_not_exists(pk)", 1), ("attribute_exists(pk)", 0)] {
        let key = format!("cond-{}-{}", expected_winners, ts());
        let mut handles = Vec::new();
        for i in 0..4 {
            let c = c.clone();
            let table = table.clone();
            let key = key.clone();
            let expr = expr.to_string();
            handles.push(tokio::spawn(async move {
                c.update_item()
                    .table_name(&table)
                    .key("pk", s(&key))
                    .update_expression("SET #v = :v")
                    .condition_expression(&expr)
                    .expression_attribute_names("#v", "value")
                    .expression_attribute_values(":v", s(&format!("w{i}")))
                    .send()
                    .await
                    .is_ok()
            }));
        }
        let mut winners = 0;
        for h in handles {
            if h.await.expect("task panicked") {
                winners += 1;
            }
        }
        assert_eq!(
            winners, expected_winners,
            "'{expr}' racing to create one new key: expected {expected_winners} writer(s) to \
             succeed, got {winners}"
        );
    }
}

/// Concurrent writers to the SAME attribute serialize into a linear history.
///
/// "All succeed" is necessary but not sufficient: six writers each doing
/// `SET v = :their-value` on one new key could all return 200 while updates were
/// applied on stale bases and silently lost. This test proves a total order
/// exists, which is the actual contract (DynamoDB serializes writes per item;
/// which writer ends up last is scheduling-dependent, but the history is linear).
///
/// The proof uses `ReturnValues: ALL_OLD`. Each writer observes the committed
/// item it replaced, so a clean serialization of N writers over one new key must
/// produce:
///   * exactly one writer that observed no previous item (it created the key),
///   * every other writer observing some OTHER writer's value, each observed at
///     most once (two writers seeing the same old value means one applied to a
///     stale base and the earlier write was lost),
///   * a final value equal to the one written value nobody observed as old.
/// Together these force the observations into one chain:
///   none -> w_a -> w_b -> ... -> final.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_attribute_updates_serialize_into_a_linear_history() {
    let c = client();
    let table = format!("SameAttrChain_{}", ts());
    make_simple_table(c, &table).await;

    const WRITERS: usize = 6;
    // Several trials: the race window is small, so one trial can miss it.
    for trial in 0..5 {
        let key = format!("chain-key-{trial}");
        let mut handles = Vec::new();
        for i in 0..WRITERS {
            let c = c.clone();
            let table = table.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                c.update_item()
                    .table_name(&table)
                    .key("pk", s(&key))
                    .update_expression("SET v = :v")
                    .expression_attribute_values(":v", s(&format!("writer-{i}")))
                    .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
                    .send()
                    .await
                    .map(|out| {
                        out.attributes
                            .and_then(|a| a.get("v").and_then(|v| v.as_s().ok().cloned()))
                    })
                    .map_err(|e| format!("{:?}", e.into_service_error()))
            }));
        }

        let mut observed_old: Vec<Option<String>> = Vec::new();
        for h in handles {
            let r = h.await.expect("task panicked");
            observed_old.push(r.unwrap_or_else(|e| {
                panic!("trial {trial}: a same-attribute UpdateItem failed: {e}")
            }));
        }

        // Exactly one writer created the key (observed no prior item).
        let creators = observed_old.iter().filter(|o| o.is_none()).count();
        assert_eq!(
            creators, 1,
            "trial {trial}: exactly one writer must observe an absent item; {creators} did. \
             More than one means a write was applied as a fresh create over an existing \
             committed value, losing it. Observed: {observed_old:?}"
        );

        // No two writers observed the same predecessor.
        let mut seen = std::collections::HashSet::new();
        for old in observed_old.iter().flatten() {
            assert!(
                seen.insert(old.clone()),
                "trial {trial}: two writers observed the same old value '{old}', so one of \
                 them applied to a stale base and the write between them was lost. \
                 Observed: {observed_old:?}"
            );
        }

        // The final value is the one written value that nobody observed as old.
        let final_v = c
            .get_item()
            .table_name(&table)
            .key("pk", s(&key))
            .consistent_read(true)
            .send()
            .await
            .expect("get_item")
            .item
            .and_then(|i| i.get("v").and_then(|v| v.as_s().ok().cloned()))
            .expect("item must hold v after successful updates");
        assert!(
            final_v.starts_with("writer-"),
            "trial {trial}: final value '{final_v}' is not any writer's value"
        );
        assert!(
            !seen.contains(&final_v),
            "trial {trial}: final value '{final_v}' was also observed as someone's old \
             value, so the history has a fork rather than a single chain. \
             Observed: {observed_old:?}"
        );
        // Chain accounting: 1 creator + (N-1) observed predecessors + 1 final tail
        // must cover all N written values exactly once.
        assert_eq!(
            seen.len() + 1,
            WRITERS,
            "trial {trial}: {} distinct predecessors + final tail != {WRITERS} writers; \
             a value vanished from the history. Observed: {observed_old:?}, final: {final_v}",
            seen.len()
        );
    }
}
