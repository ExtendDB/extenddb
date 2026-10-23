// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Same-item write contention through `TransactWriteItems`.
//!
//! DynamoDB serialises transactional writes to one item: when N transactions
//! race a conditional Put (`attribute_not_exists`) on a key that does not
//! exist yet, exactly one wins and every other cancels with
//! `ConditionalCheckFailed`. No racer may 500, silently overwrite the winner,
//! or leave a secondary index inconsistent with the base table.
//!
//! These tests guard the loss modes of that race on backends where a locking
//! read cannot lock a row that does not exist yet:
//! - a loser overwriting the winner without its condition ever failing;
//! - a loser's synchronous index maintenance colliding with the winner's
//!   committed index row and surfacing as `InternalServerError`;
//! - a loser whose condition PASSES against the winner cancelling instead of
//!   overwriting (the condition must be re-evaluated, not assumed failed);
//! - a transactional Update losing its expression instead of re-applying it
//!   on top of the winner's item. Four writers setting four different
//!   attributes on a new key must yield an item holding all four, the merge
//!   semantics measured against the real service on the non-transactional
//!   UpdateItem path (see the rationale comment in
//!   `crates/storage-postgres/src/data/update_item.rs`).
//!
//! Measured against the real service (us-east-1, 2026-08-31: 8
//! barrier-synchronized writers, 6 rounds): exactly one `attribute_not_exists`
//! Put wins every round, and losers split by timing. Those overlapping the
//! winner's in-flight transaction cancel with a `TransactionConflict` reason
//! ("Transaction is ongoing for the item", no Item); those arriving after the
//! winner committed cancel with `ConditionalCheckFailed` ("The conditional
//! request failed", ALL_OLD Item = the winner's item). ExtendDB serializes
//! same-item transactions instead of conflict-cancelling the overlap window,
//! so its losers deterministically take the post-commit CCF shape. The
//! all-losers-are-CCF and all-racers-succeed assertions below are therefore
//! ExtendDB-only, and these tests skip on real DynamoDB, where simultaneous
//! passing-condition puts and unconditional updates also conflict-cancel
//! rather than all succeeding (clients converge to the ExtendDB outcome only
//! after retrying). A transactional Delete of a nonexistent item is a
//! measured no-op success on the real service.

use std::sync::Arc;

use crate::test_base::*;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::retry::RetryConfig;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::{
    TransactWriteItemsError, TransactWriteItemsOutput,
};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    LocalSecondaryIndex, Projection, ProjectionType, Put, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, TransactWriteItem, Update,
};
use std::collections::HashMap;
use tokio::sync::Barrier;

const WRITERS: usize = 8;
const ROUNDS: usize = 20;

/// A client with SDK retries disabled: a 500 surfaced by any racer must fail
/// the test, not be laundered into a clean retry that lands after the winner
/// commits.
fn no_retry_client() -> Client {
    let conf = client()
        .config()
        .to_builder()
        .retry_config(RetryConfig::disabled())
        .build();
    Client::from_conf(conf)
}

async fn make_hash_table(c: &Client, name: &str) {
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

/// Composite-key table with an LSI, so index maintenance runs synchronously
/// inside the write transaction on every backend and configuration.
async fn make_lsi_table(c: &Client, name: &str) {
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
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
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
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("lsk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .local_secondary_indexes(
            LocalSecondaryIndex::builder()
                .index_name("by_lsk")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("pk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("lsk")
                        .key_type(KeyType::Range)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await;
    wait_for_active(c, name).await;
}

/// A loser's typed cancellation, asserted: single-op TransactionCanceled with
/// a `ConditionalCheckFailed` reason carrying the ALL_OLD item. Returns that
/// item so callers can prove the condition was re-evaluated against the
/// committed winner rather than assumed failed.
fn assert_ccf_cancellation(
    err: SdkError<TransactWriteItemsError>,
) -> HashMap<String, AttributeValue> {
    let msg = err_msg(&err);
    let service_err = err.into_service_error();
    let TransactWriteItemsError::TransactionCanceledException(tce) = &service_err else {
        panic!("loser must cancel with TransactionCanceledException, got: {msg}");
    };
    let reasons = tce.cancellation_reasons();
    assert_eq!(reasons.len(), 1, "single-op transaction has one reason");
    assert_eq!(
        reasons[0].code(),
        Some("ConditionalCheckFailed"),
        "loser's reason must be ConditionalCheckFailed: {msg}"
    );
    reasons[0]
        .item()
        .cloned()
        .expect("ReturnValuesOnConditionCheckFailure=ALL_OLD must carry the winner's item")
}

/// One round of the create race: `WRITERS` concurrent single-Put transactions,
/// all `attribute_not_exists(pk)` with RVOCCF ALL_OLD, all targeting the same
/// nonexistent key. Returns (winner task ids, ALL_OLD items from losers).
async fn race_conditional_puts(
    c: &Client,
    table: &str,
    key: &str,
    with_sort_key: bool,
) -> (Vec<usize>, Vec<HashMap<String, AttributeValue>>) {
    let barrier = Arc::new(Barrier::new(WRITERS));
    let mut handles = Vec::new();
    for i in 0..WRITERS {
        let c = c.clone();
        let table = table.to_owned();
        let key = key.to_owned();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut put = Put::builder()
                .table_name(&table)
                .item("pk", s(&key))
                .item("writer", n(i as i64))
                .condition_expression("attribute_not_exists(pk)")
                .return_values_on_condition_check_failure(
                    ReturnValuesOnConditionCheckFailure::AllOld,
                );
            if with_sort_key {
                // Fixed sort and index keys: every racer targets the same row.
                put = put.item("sk", s("row")).item("lsk", s("idx"));
            }
            let res: Result<TransactWriteItemsOutput, _> = c
                .transact_write_items()
                .transact_items(
                    TransactWriteItem::builder()
                        .put(put.build().unwrap())
                        .build(),
                )
                .send()
                .await;
            (i, res)
        }));
    }
    let mut winners = Vec::new();
    let mut loser_items = Vec::new();
    for h in handles {
        let (i, res) = h.await.unwrap();
        match res {
            Ok(_) => winners.push(i),
            Err(e) => loser_items.push(assert_ccf_cancellation(e)),
        }
    }
    (winners, loser_items)
}

/// Every loser's ALL_OLD item must be the winner's committed item: the proof
/// that the condition was re-evaluated against real state, not assumed failed.
fn assert_losers_saw_winner(
    round: usize,
    winners: &[usize],
    loser_items: &[HashMap<String, AttributeValue>],
) {
    assert_eq!(
        winners.len(),
        1,
        "round {round}: exactly one attribute_not_exists put may succeed, got {winners:?}"
    );
    assert_eq!(loser_items.len(), WRITERS - 1);
    for item in loser_items {
        assert_eq!(
            item.get("writer"),
            Some(&n(winners[0] as i64)),
            "round {round}: loser's ALL_OLD item must be the winner's committed item"
        );
    }
}

/// Conditional transactional puts racing on a fresh key of a plain table:
/// exactly one winner per round, losers cancel having seen the winner's item,
/// and the stored item is the winner's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_conditional_transact_puts_one_winner() {
    if is_real_dynamodb() {
        return; // See module docs: live service may conflict-cancel instead.
    }
    let c = no_retry_client();
    let table = format!("TxPutCreateRace_{}", ts());
    make_hash_table(&c, &table).await;

    for round in 0..ROUNDS {
        let key = format!("k{round}");
        let (winners, loser_items) = race_conditional_puts(&c, &table, &key, false).await;
        assert_losers_saw_winner(round, &winners, &loser_items);
        let got = c
            .get_item()
            .table_name(&table)
            .key("pk", s(&key))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        let item = got.item().expect("winner's item must exist");
        assert_eq!(
            item.get("writer"),
            Some(&n(winners[0] as i64)),
            "round {round}: stored item must be the winner's, not a silent overwrite"
        );
    }

    let _ = c.delete_table().table_name(&table).send().await;
}

/// The same race on a table with an LSI: synchronous index maintenance must
/// not collide on the winner's committed index row (no 500), and the index
/// must hold exactly the winner's row afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_conditional_transact_puts_index_stays_consistent() {
    if is_real_dynamodb() {
        return; // See module docs: live service may conflict-cancel instead.
    }
    let c = no_retry_client();
    let table = format!("TxPutCreateRaceLsi_{}", ts());
    make_lsi_table(&c, &table).await;

    for round in 0..ROUNDS {
        let key = format!("k{round}");
        let (winners, loser_items) = race_conditional_puts(&c, &table, &key, true).await;
        assert_losers_saw_winner(round, &winners, &loser_items);
        // The base row is the winner's.
        let got = c
            .get_item()
            .table_name(&table)
            .key("pk", s(&key))
            .key("sk", s("row"))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        let item = got.item().expect("winner's item must exist");
        assert_eq!(item.get("writer"), Some(&n(winners[0] as i64)));
        // The LSI holds exactly one row for the key, and it is the winner's.
        let q = c
            .query()
            .table_name(&table)
            .index_name("by_lsk")
            .key_condition_expression("pk = :p")
            .expression_attribute_values(":p", s(&key))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        assert_eq!(
            q.count(),
            1,
            "round {round}: index must hold exactly the winner's row"
        );
        assert_eq!(q.items()[0].get("writer"), Some(&n(winners[0] as i64)));
    }

    let _ = c.delete_table().table_name(&table).send().await;
}

/// A condition that PASSES against the winner must overwrite, not cancel:
/// racers guard on an attribute no writer ever sets, so the condition holds
/// against both the empty pre-image and any winner's item. Every racer must
/// succeed; a fix that maps a lost create race straight to
/// ConditionalCheckFailed without re-evaluating would cancel the losers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_transact_puts_condition_passing_against_winner_overwrites() {
    if is_real_dynamodb() {
        return; // See module docs: live service may conflict-cancel instead.
    }
    let c = no_retry_client();
    let table = format!("TxPutCreateRacePass_{}", ts());
    make_hash_table(&c, &table).await;

    for round in 0..ROUNDS {
        let key = format!("k{round}");
        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut handles = Vec::new();
        for i in 0..WRITERS {
            let c = c.clone();
            let table = table.clone();
            let key = key.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                c.transact_write_items()
                    .transact_items(
                        TransactWriteItem::builder()
                            .put(
                                Put::builder()
                                    .table_name(&table)
                                    .item("pk", s(&key))
                                    .item("writer", n(i as i64))
                                    .condition_expression("attribute_not_exists(never_set)")
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                    )
                    .send()
                    .await
            }));
        }
        let mut succeeded = 0;
        for h in handles {
            let res = h.await.unwrap();
            assert!(
                res.is_ok(),
                "round {round}: a condition passing against the winner must overwrite, \
                 not cancel: {}",
                err_msg(&res.unwrap_err())
            );
            succeeded += 1;
        }
        assert_eq!(succeeded, WRITERS);
        // The stored item is one of the racers' (last committer wins).
        let got = c
            .get_item()
            .table_name(&table)
            .key("pk", s(&key))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        let writer = got.item().expect("item must exist").get("writer").cloned();
        let is_valid = (0..WRITERS).any(|i| writer == Some(n(i as i64)));
        assert!(
            is_valid,
            "round {round}: stored writer must be one of the racers"
        );
    }

    let _ = c.delete_table().table_name(&table).send().await;
}

/// Conditional transactional Updates racing to create: same one-winner
/// contract as the Put arm, through the Update arm's re-evaluation path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_conditional_transact_updates_one_winner() {
    if is_real_dynamodb() {
        return; // See module docs: live service may conflict-cancel instead.
    }
    let c = no_retry_client();
    let table = format!("TxUpdCondCreateRace_{}", ts());
    make_hash_table(&c, &table).await;

    for round in 0..ROUNDS {
        let key = format!("k{round}");
        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut handles = Vec::new();
        for i in 0..WRITERS {
            let c = c.clone();
            let table = table.clone();
            let key = key.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let res = c
                    .transact_write_items()
                    .transact_items(
                        TransactWriteItem::builder()
                            .update(
                                Update::builder()
                                    .table_name(&table)
                                    .key("pk", s(&key))
                                    .update_expression("SET writer = :v")
                                    .condition_expression("attribute_not_exists(pk)")
                                    .expression_attribute_values(":v", n(i as i64))
                                    .return_values_on_condition_check_failure(
                                        ReturnValuesOnConditionCheckFailure::AllOld,
                                    )
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                    )
                    .send()
                    .await;
                (i, res)
            }));
        }
        let mut winners = Vec::new();
        let mut loser_items = Vec::new();
        for h in handles {
            let (i, res) = h.await.unwrap();
            match res {
                Ok(_) => winners.push(i),
                Err(e) => loser_items.push(assert_ccf_cancellation(e)),
            }
        }
        assert_losers_saw_winner(round, &winners, &loser_items);
    }

    let _ = c.delete_table().table_name(&table).send().await;
}

/// Unconditional transactional Updates racing on a fresh key must all succeed
/// and merge: each loser re-applies its expression on top of the winner's
/// item, so four writers setting four different attributes yield an item
/// holding all four (the merge semantics measured against the real service on
/// the non-transactional UpdateItem path).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_transact_updates_on_new_item_merge_attributes() {
    if is_real_dynamodb() {
        return; // See module docs: live service may conflict-cancel instead.
    }
    const UPDATERS: usize = 4;
    let c = no_retry_client();
    let table = format!("TxUpdateCreateRace_{}", ts());
    make_hash_table(&c, &table).await;

    for round in 0..ROUNDS {
        let key = format!("k{round}");
        let barrier = Arc::new(Barrier::new(UPDATERS));
        let mut handles = Vec::new();
        for i in 0..UPDATERS {
            let c = c.clone();
            let table = table.clone();
            let key = key.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                c.transact_write_items()
                    .transact_items(
                        TransactWriteItem::builder()
                            .update(
                                Update::builder()
                                    .table_name(&table)
                                    .key("pk", s(&key))
                                    .update_expression(format!("SET a{i} = :v"))
                                    .expression_attribute_values(":v", n(i as i64))
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                    )
                    .send()
                    .await
            }));
        }
        for h in handles {
            let res = h.await.unwrap();
            assert!(
                res.is_ok(),
                "unconditional transactional update must succeed: {}",
                err_msg(&res.unwrap_err())
            );
        }
        let got = c
            .get_item()
            .table_name(&table)
            .key("pk", s(&key))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        let item = got.item().expect("item must exist");
        for i in 0..UPDATERS {
            assert_eq!(
                item.get(&format!("a{i}")),
                Some(&n(i as i64)),
                "round {round}: attribute a{i} lost; losers must re-apply on the winner's item"
            );
        }
    }

    let _ = c.delete_table().table_name(&table).send().await;
}
