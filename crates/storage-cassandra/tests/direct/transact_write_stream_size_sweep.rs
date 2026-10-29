// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration test for the item sizes around Cassandra's batch limit.
//!
//! A table with one GSI and a `NEW_AND_OLD_IMAGES` stream takes transactions of
//! 16, 20, 24, 28 and 32 KB items. On the default `batch_size_fail_threshold`
//! (50 KiB), a transacted insert fails once the item reaches about 28 KB and a
//! transacted overwrite once it reaches about 20 KB, when the record shares the
//! batch that carries the index entry.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{ReturnValuesOnConditionCheckFailure, StreamViewType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, IdempotencyKey, TransactWriteOp};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{TestTable, setup_engine};
use crate::transact_write_stream_batch_limit::{
    big_item, events_for, ledger_entries, records_of, seed, transacted_put,
};
use crate::transact_write_stream_records::{capture, item_with, key_for};

/// One transaction with a big Put first and a small Put second, both recorded,
/// under a client token. When the first op's batch is rejected, the second op
/// is never committed and the token still answers as a replay.
async fn transacted_pair(
    engine: &CassandraEngine,
    key_info: &TableKeyInfo,
    kb: usize,
) -> Result<(), StorageError> {
    let maps = ExpressionMaps::default();
    let big = big_item(&format!("pair-big-{kb}"), kb * 1_000, "new");
    let small = item_with(&format!("pair-small-{kb}"), "tiny");
    let view = capture(StreamViewType::NewAndOldImages);
    let put = |item| TransactWriteOp::Put {
        key_info,
        item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(view.clone()),
    };
    let ops = vec![put(&big), put(&small)];
    let token = format!("token-pair-{kb}");
    let idempotency = IdempotencyKey {
        account_id: &key_info.account_id,
        token: &token,
        fingerprint: "fingerprint-sweep",
    };
    engine.transact_write_items(&ops, Some(idempotency)).await
}

/// An insert, an overwrite and a two-op transaction each commit with their
/// records at 16, 20, 24, 28 and 32 KB. Every size is attempted before the test
/// reports, so a failure lists each shape and size that was rejected.
#[tokio::test]
async fn test_transact_write_records_at_every_item_size_around_the_batch_limit() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "TxnStreamSweep", "StatusIndex", "status").await;
    let key_info = &table.key_info;

    let mut rejected = Vec::new();
    let mut expected: Vec<(String, &str)> = Vec::new();
    for kb in [16_usize, 20, 24, 28, 32] {
        for (kind, event) in [("insert", "Insert"), ("overwrite", "Modify")] {
            let id = format!("{kind}-{kb}");
            if kind == "overwrite" {
                seed(&engine, key_info, &id, kb * 1_000, "old").await;
            }
            let result = transacted_put(&engine, key_info, &id, kb * 1_000, "new").await;
            println!("size sweep: {kind} {kb} KB: {result:?}");
            if result.is_err() {
                rejected.push(format!("{kind} {kb} KB"));
            }
            expected.push((id, event));
        }

        let result = transacted_pair(&engine, key_info, kb).await;
        println!("size sweep: pair {kb} KB: {result:?}");
        if result.is_err() {
            rejected.push(format!("pair {kb} KB"));
        }
        expected.push((format!("pair-big-{kb}"), "Insert"));
        expected.push((format!("pair-small-{kb}"), "Insert"));
    }
    assert!(rejected.is_empty(), "rejected transactions: {rejected:?}");

    let keys: Vec<&str> = expected.iter().map(|(id, _)| id.as_str()).collect();
    let records = records_of(&engine, key_info, &keys).await;
    for (id, event) in &expected {
        assert_eq!(events_for(&records, id), vec![*event], "records for {id}");
    }
    for kb in [16_usize, 20, 24, 28, 32] {
        let small = key_for(&format!("pair-small-{kb}"));
        assert!(
            engine.get_item(key_info, &small).await.unwrap().is_some(),
            "the second op of the {kb} KB pair did not commit"
        );
    }
    assert_eq!(ledger_entries(&engine, key_info).await, 0);
}
