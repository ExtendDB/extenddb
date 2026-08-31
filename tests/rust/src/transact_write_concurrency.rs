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
//! - an unconditional transactional Update losing its expression instead of
//!   re-applying it on top of the winner's item (four writers setting four
//!   different attributes on a new key must yield an item holding all four,
//!   as measured against the real service).
//!
//! Backend-agnostic: passes against real DynamoDB as well.

use std::sync::Arc;

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, LocalSecondaryIndex, Projection,
    ProjectionType, Put, ScalarAttributeType, TransactWriteItem, Update,
};
use tokio::sync::Barrier;

const WRITERS: usize = 8;
const ROUNDS: usize = 20;

async fn make_hash_table(c: &aws_sdk_dynamodb::Client, name: &str) {
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
async fn make_lsi_table(c: &aws_sdk_dynamodb::Client, name: &str) {
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

/// One round of the create race: `writers` concurrent single-Put transactions,
/// all `attribute_not_exists(pk)`, all targeting the same nonexistent key.
/// Returns (winner task ids, loser error strings).
async fn race_conditional_puts(
    c: &aws_sdk_dynamodb::Client,
    table: &str,
    key: &str,
    with_sort_key: bool,
) -> (Vec<usize>, Vec<String>) {
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
                .condition_expression("attribute_not_exists(pk)");
            if with_sort_key {
                // Fixed sort and index keys: every racer targets the same row.
                put = put.item("sk", s("row")).item("lsk", s("idx"));
            }
            let res = c
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
    let mut losses = Vec::new();
    for h in handles {
        let (i, res) = h.await.unwrap();
        match res {
            Ok(_) => winners.push(i),
            Err(e) => {
                let code = err_code(&e).unwrap_or("<none>").to_owned();
                let msg = err_msg(&e);
                assert_ne!(
                    code, "InternalServerError",
                    "racer must not surface a 500: {msg}"
                );
                assert_eq!(
                    code, "TransactionCanceledException",
                    "loser must cancel, got {code}: {msg}"
                );
                assert!(
                    msg.contains("ConditionalCheckFailed"),
                    "cancellation must carry ConditionalCheckFailed: {msg}"
                );
                losses.push(msg);
            }
        }
    }
    (winners, losses)
}

/// Conditional transactional puts racing on a fresh key of a plain table:
/// exactly one winner per round, and the stored item is the winner's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_conditional_transact_puts_one_winner() {
    let c = client();
    let table = format!("TxPutCreateRace_{}", ts());
    make_hash_table(c, &table).await;

    for round in 0..ROUNDS {
        let key = format!("k{round}");
        let (winners, _losses) = race_conditional_puts(c, &table, &key, false).await;
        assert_eq!(
            winners.len(),
            1,
            "round {round}: exactly one attribute_not_exists put may succeed, got {winners:?}"
        );
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
    let c = client();
    let table = format!("TxPutCreateRaceLsi_{}", ts());
    make_lsi_table(c, &table).await;

    for round in 0..ROUNDS {
        let key = format!("k{round}");
        let (winners, _losses) = race_conditional_puts(c, &table, &key, true).await;
        assert_eq!(
            winners.len(),
            1,
            "round {round}: exactly one attribute_not_exists put may succeed, got {winners:?}"
        );
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

/// Unconditional transactional Updates racing on a fresh key must all succeed
/// and merge: each loser re-applies its expression on top of the winner's
/// item, so four writers setting four different attributes yield an item
/// holding all four (measured against the real service).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_transact_updates_on_new_item_merge_attributes() {
    const UPDATERS: usize = 4;
    let c = client();
    let table = format!("TxUpdateCreateRace_{}", ts());
    make_hash_table(c, &table).await;

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
