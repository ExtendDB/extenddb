// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for stream records on tables with a sort key or an
//! index, and for the batch that carries a record with its index rows.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeValue, Item, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamViewType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TransactWriteOp};
use extenddb_storage_cassandra::CassandraEngine;
use extenddb_storage_cassandra::data::index::fetch_indexes_for_table;

use crate::helpers::{TestTable, setup_engine};
use crate::transact_write_stream_records::{
    capture, item_with, key_for, one_stream_record_per_mutation, records_for_keys,
};

/// `item` with the sort key `sort`.
fn with_sort(mut item: Item, sort: &AttributeValue) -> Item {
    item.insert("sort".to_owned(), sort.clone());
    item
}

/// `item` with the attribute the test tables' GSI is keyed on.
fn with_status(mut item: Item) -> Item {
    item.insert("status".to_owned(), AttributeValue::S("open".to_owned()));
    item
}

/// A transaction with a Put, an Update and a Delete records one record per
/// mutation on tables with a string, a number and a binary sort key, and every
/// record carries both key attributes.
#[tokio::test]
async fn test_transact_write_records_one_stream_record_per_mutation_with_a_sort_key() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let sorts = [
        (
            "TxnStreamSortS",
            ScalarAttributeType::S,
            AttributeValue::S("s1".to_owned()),
        ),
        (
            "TxnStreamSortN",
            ScalarAttributeType::N,
            AttributeValue::N("1".to_owned()),
        ),
        (
            "TxnStreamSortB",
            ScalarAttributeType::B,
            AttributeValue::B(vec![1, 2, 3, 4]),
        ),
    ];
    for (name, sort_type, sort) in sorts {
        let table = TestTable::with_sort_key_type(&engine, name, sort_type).await;
        let records = one_stream_record_per_mutation(
            &engine,
            &table,
            |id, val| with_sort(item_with(id, val), &sort),
            |id| with_sort(key_for(id), &sort),
        )
        .await;
        for (_, _, record) in &records {
            let keys = record["dynamodb"]["Keys"].as_object().unwrap();
            assert!(
                keys.len() == 2 && keys.contains_key("id") && keys.contains_key("sort"),
                "{name}: the record's keys are {keys:?}"
            );
        }
    }
}

/// Pin the table's GSI to synchronous propagation, the way a delay of 0 does in
/// production, so its index row goes into the same batch as the record.
async fn pin_gsi_sync(engine: &CassandraEngine, table: &TestTable) {
    let pin = format!(
        "UPDATE {}.indexes SET propagation_delay_ms = 0 \
         WHERE table_id = '{}' AND index_name = 'StatusIndex'",
        engine.catalog_keyspace(),
        table.key_info.table_id
    );
    engine.session_arc().query(pin).await.unwrap();
}

/// A transaction with a Put, an Update and a Delete records one record per
/// mutation on a table with a GSI, and the Put writes its index row in the batch
/// that carries its record.
#[tokio::test]
async fn test_transact_write_records_one_stream_record_per_mutation_with_a_gsi() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "TxnStreamAllVerbsGsi", "StatusIndex", "status").await;
    pin_gsi_sync(&engine, &table).await;

    one_stream_record_per_mutation(
        &engine,
        &table,
        |id, val| with_status(item_with(id, val)),
        key_for,
    )
    .await;

    let (rows, _) = engine
        .scan(&table.key_info, None, None, None, None, Some("StatusIndex"))
        .await
        .unwrap();
    assert!(
        rows.iter()
            .any(|row| row.get("id") == Some(&AttributeValue::S("put-1".to_owned()))),
        "no index row for the Put: {rows:?}"
    );
}

/// One transacted Put of a small item with a status, so it has an index row,
/// and a `NEW_AND_OLD_IMAGES` capture.
async fn transacted_put_with_status(
    engine: &CassandraEngine,
    table: &TestTable,
    id: &str,
) -> Result<(), StorageError> {
    let maps = ExpressionMaps::default();
    let item = with_status(item_with(id, "small"));
    let ops = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item: &item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture(StreamViewType::NewAndOldImages)),
    }];
    engine.transact_write_items(&ops, None).await
}

/// The error text of a failed transaction, which has to come from the batch
/// that carries the secondary effects.
fn batch_error(result: Result<(), StorageError>) -> String {
    match result {
        Err(StorageError::Internal(message)) => {
            assert!(
                message.contains("commit_secondary_effects batch"),
                "the error did not come from the secondary-effects batch: {message}"
            );
            message
        }
        other => panic!("expected an Internal error from the batch, got {other:?}"),
    }
}

/// A transacted Put whose index statement is rejected writes no stream record.
///
/// The record of a small item shares the batch with the index rows, and
/// Cassandra applies none of the statements of a batch it rejects. The index's
/// table is removed so the server rejects the index statement.
#[tokio::test]
async fn test_transact_write_failing_index_statement_prevents_the_record() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table =
        TestTable::with_gsi(&engine, "TxnStreamFailingIndex", "StatusIndex", "status").await;
    pin_gsi_sync(&engine, &table).await;
    let indexes = fetch_indexes_for_table(
        &table.key_info.table_id,
        &engine.session_arc(),
        &engine.catalog_keyspace(),
    )
    .await
    .unwrap();
    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    engine
        .drop_index_data_table(&keyspace, &indexes[0].index_id)
        .await
        .unwrap();

    let result = transacted_put_with_status(&engine, &table, "fail-idx").await;
    batch_error(result);

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["fail-idx"],
    )
    .await;
    assert!(
        records.is_empty(),
        "the index statement failed, so no record may be written: {records:?}"
    );
}

/// A transacted Put whose record statement is rejected writes no index row.
///
/// The same property from the other side: the index rows and the record of a
/// small item are one batch. The account's `stream_records` table is removed so
/// the server rejects the record statement.
#[tokio::test]
async fn test_transact_write_failing_record_statement_prevents_the_index_rows() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table =
        TestTable::with_gsi(&engine, "TxnStreamFailingRecord", "StatusIndex", "status").await;
    pin_gsi_sync(&engine, &table).await;
    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    engine
        .session_arc()
        .query(format!("DROP TABLE {keyspace}.stream_records"))
        .await
        .unwrap();

    let result = transacted_put_with_status(&engine, &table, "fail-rec").await;
    batch_error(result);

    let (rows, _) = engine
        .scan(&table.key_info, None, None, None, None, Some("StatusIndex"))
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "the record statement failed, so no index row may be written: {rows:?}"
    );
}
