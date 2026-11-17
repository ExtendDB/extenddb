// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration test for when a transacted mutation takes its stream
//! sequence number.
//!
//! A sequence number starts with the millisecond at which it was taken. A
//! consumer that has read a later record of a shard can miss an
//! earlier-numbered record that lands after it, so the number has to be taken
//! once the item's write has landed. The write time of the stored item is the
//! time the lightweight transaction committed, which gives the lower bound.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{ReturnValuesOnConditionCheckFailure, StreamViewType};
use extenddb_storage::{DataEngine, TransactWriteOp};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{TestTable, setup_engine};
use crate::transact_write_stream_records::{
    capture, item_with, key_for, records_for_keys, set_val_action,
};

/// The millisecond at which Cassandra stored the current `item_data` of `id`.
async fn item_write_ms(engine: &CassandraEngine, table: &TestTable, id: &str) -> i64 {
    let keyspace = format!("extenddb_ttl_test_account_{}", table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace('-', "_"));
    let query =
        format!("SELECT WRITETIME(item_data) AS written FROM {keyspace}.{data_table} WHERE pk = ?");
    let result = engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!(id))
        .await
        .unwrap();
    let row = result
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default()
        .into_iter()
        .next()
        .unwrap();
    let micros: i64 = row.get_r_by_name("written").unwrap();
    micros / 1_000
}

/// The millisecond a sequence number was taken at: its first 13 digits.
fn sequence_ms(sequence_number: &str) -> i64 {
    sequence_number[..13].parse().unwrap()
}

/// Twenty transacted Puts and twenty transacted Updates each record a sequence
/// number whose millisecond is not earlier than the millisecond at which their
/// item was written.
///
/// Each transaction holds one op, so the gap between taking the number and
/// writing the item is the commit phase of that one op.
#[tokio::test]
async fn test_transact_write_sequence_number_is_taken_after_the_item_is_written() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamSequence", false).await;
    let maps = ExpressionMaps::default();
    let view = capture(StreamViewType::NewAndOldImages);

    let mut ids = Vec::new();
    for i in 0..20 {
        let id = format!("seq-put-{i}");
        let item = item_with(&id, "created");
        let ops = vec![TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        }];
        engine.transact_write_items(&ops, None).await.unwrap();
        ids.push(id);
    }
    for i in 0..20 {
        let id = format!("seq-upd-{i}");
        let item = item_with(&id, "before");
        engine
            .put_item(&table.key_info, item, false, None, &maps, None)
            .await
            .unwrap();
        let key = key_for(&id);
        let (actions, update_maps) = set_val_action("after");
        let ops = vec![TransactWriteOp::Update {
            key_info: &table.key_info,
            key: &key,
            actions: &actions,
            condition: None,
            maps: &update_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        }];
        engine.transact_write_items(&ops, None).await.unwrap();
        ids.push(id);
    }

    let keys: Vec<&str> = ids.iter().map(String::as_str).collect();
    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &keys,
    )
    .await;
    assert_eq!(records.len(), ids.len(), "one record per key: {records:?}");

    let mut early = Vec::new();
    for id in &ids {
        let (sequence_number, _, _) = records
            .iter()
            .find(|(_, _, data)| data["dynamodb"]["Keys"]["id"]["S"] == id.as_str())
            .unwrap();
        let taken = sequence_ms(sequence_number);
        let written = item_write_ms(&engine, &table, id).await;
        if taken < written {
            early.push(format!("{id}: {} ms early", written - taken));
        }
    }
    println!(
        "sequence number earlier than the item's write in {} of {}",
        early.len(),
        ids.len()
    );
    assert!(
        early.is_empty(),
        "{} of {} sequence numbers were taken before the item was written: {early:?}",
        early.len(),
        ids.len()
    );
}
