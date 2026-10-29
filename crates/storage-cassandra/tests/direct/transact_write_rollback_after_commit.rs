// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for a cancelled transaction that touches a key an
//! earlier transaction committed.
//!
//! A committing Put or Update clears `created_to_prepare`. Rollback deletes a
//! row that still carries the flag and has `prepared_txn_id` set to the
//! cancelled transaction, so a committed row that kept the flag loses its item
//! when a later transaction that prepared the key is cancelled.
//!
//! `commit_put_or_update` clears the flag in one statement for a table with a
//! sort key and in another for a table without one, so the scenario runs on
//! both.

use extenddb_core::expression::{Expr, ExpressionMaps, PathElement};
use extenddb_core::types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TransactWriteOp};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{TestTable, setup_engine};
use crate::transact_write_stream_records::{item_with, key_for};
use crate::transact_write_stream_tables::with_sort;

/// `attribute_exists(id)`, which fails against a key that is absent.
fn attribute_exists_id() -> Expr {
    Expr::Function {
        name: "attribute_exists".to_owned(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".to_owned())])],
    }
}

/// A transaction commits an item. A second transaction overwrites the same key
/// and carries a condition on another key that fails, so it prepares the key,
/// is cancelled, and rolls back. The item the first transaction committed is
/// still stored afterwards, with its first value, and the other key was never
/// written.
///
/// `item` and `key` build the item and the key for an id, with the sort key the
/// table needs, so the scenario runs on a table of either key shape.
async fn cancelled_transaction_keeps_a_committed_item(
    engine: &CassandraEngine,
    table: &TestTable,
    item: impl Fn(&str, &str) -> Item,
    key: impl Fn(&str) -> Item,
) {
    let maps = ExpressionMaps::default();

    let committed = item("aaa-committed", "from-the-first-transaction");
    let first = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item: &committed,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    engine.transact_write_items(&first, None).await.unwrap();

    let overwrite = item("aaa-committed", "from-the-cancelled-transaction");
    let absent = item("zzz-absent", "never-written");
    let exists = attribute_exists_id();
    let second = vec![
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &overwrite,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &absent,
            condition: Some(&exists),
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
    ];
    let result = engine.transact_write_items(&second, None).await;
    assert!(
        matches!(result, Err(StorageError::TransactionCanceled(_))),
        "expected cancellation, got {result:?}"
    );

    let stored = engine
        .get_item(&table.key_info, &key("aaa-committed"))
        .await
        .unwrap();
    assert_eq!(
        stored.as_ref().and_then(|found| found.get("val")),
        Some(&AttributeValue::S("from-the-first-transaction".to_owned())),
        "the cancelled transaction changed or deleted the committed item: {stored:?}"
    );
    assert!(
        engine
            .get_item(&table.key_info, &key("zzz-absent"))
            .await
            .unwrap()
            .is_none(),
        "the cancelled transaction wrote a second item"
    );
}

/// The scenario on a table with a hash key only.
#[tokio::test]
async fn test_transact_write_cancelled_transaction_keeps_an_item_an_earlier_transaction_committed()
{
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnRollbackKeepsCommitted", false).await;
    cancelled_transaction_keeps_a_committed_item(&engine, &table, item_with, key_for).await;
}

/// The scenario on a table with a string sort key, which commits through its
/// own statement.
#[tokio::test]
async fn test_transact_write_cancelled_transaction_keeps_a_committed_sort_key_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnRollbackKeepsCommittedSk", true).await;
    let sort = AttributeValue::S("s1".to_owned());
    cancelled_transaction_keeps_a_committed_item(
        &engine,
        &table,
        |id, val| with_sort(item_with(id, val), &sort),
        |id| with_sort(key_for(id), &sort),
    )
    .await;
}
