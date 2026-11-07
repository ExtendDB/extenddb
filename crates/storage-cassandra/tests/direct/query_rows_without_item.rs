// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query over data rows that hold no item.
//!
//! A partition whose items have all been deleted answers a partition read
//! with one row of its static columns, and a TTL claim or a transaction
//! prepare on an absent key writes a row with only `prepared_txn_id` set.
//! Neither row is an item. A page may come back short because of them, but
//! while items remain it carries a LastEvaluatedKey, so a reader paging on
//! from the last item reads every item.

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::DataEngine;
use extenddb_storage_cassandra::CassandraEngine;
use std::collections::BTreeMap;

use crate::helpers::{TestTable, setup_engine};

fn key(sk: &str) -> Item {
    BTreeMap::from([
        ("id".to_string(), AttributeValue::S("p".to_string())),
        ("sort".to_string(), AttributeValue::S(sk.to_string())),
    ])
}

async fn put(engine: &CassandraEngine, table: &TestTable, sk: &str) {
    let mut item = key(sk);
    item.insert("v".to_string(), AttributeValue::S("x".to_string()));
    engine
        .put_item(
            &table.key_info,
            item,
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("put_item");
}

/// Writes the row a TTL claim on the absent key `sk` leaves: no `item_data`.
async fn claim_row(engine: &CassandraEngine, table: &TestTable, sk: &str) {
    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace('-', "_"));
    engine
        .session_arc()
        .query_with_values(
            format!(
                "UPDATE {keyspace}.{data_table} SET prepared_txn_id = uuid(), \
                 prepared_txn_timestamp = 1 WHERE pk = ? AND sk_s = ?"
            ),
            cdrs_tokio::query_values!("p", sk),
        )
        .await
        .expect("claim row");
}

async fn query(
    engine: &CassandraEngine,
    table: &TestTable,
    forward: bool,
    limit: Option<i64>,
    start: Option<&Item>,
) -> (Vec<String>, Option<Item>) {
    let condition = KeyCondition {
        pk_path: vec![PathElement::Attribute("id".to_string())],
        pk_value: Expr::Placeholder(":p".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };
    let mut maps = ExpressionMaps::default();
    maps.values
        .insert(":p".to_string(), AttributeValue::S("p".to_string()));
    let (items, last_key) = engine
        .query(
            &table.key_info,
            &condition,
            &maps,
            forward,
            limit,
            start,
            None,
        )
        .await
        .expect("query");
    let sort_keys = items
        .iter()
        .map(|item| match item.get("sort") {
            Some(AttributeValue::S(s)) => s.clone(),
            other => panic!("sort key {other:?}"),
        })
        .collect();
    (sort_keys, last_key)
}

/// Pages through the partition with `limit` and returns every sort key read.
async fn read_all(
    engine: &CassandraEngine,
    table: &TestTable,
    forward: bool,
    limit: i64,
) -> Vec<String> {
    let mut all = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..100 {
        let (page, last_key) = query(engine, table, forward, Some(limit), start.as_ref()).await;
        all.extend(page);
        match last_key {
            Some(k) => start = Some(k),
            None => return all,
        }
    }
    panic!("more than 100 pages");
}

#[tokio::test]
async fn test_query_of_an_emptied_partition() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryEmptied", true).await;
    for sk in ["a", "b"] {
        put(&engine, &table, sk).await;
    }
    for sk in ["a", "b"] {
        engine
            .delete_item(
                &table.key_info,
                &key(sk),
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("delete_item");
    }
    for (forward, limit) in [
        (true, None),
        (true, Some(1)),
        (false, None),
        (false, Some(1)),
    ] {
        assert_eq!(
            query(&engine, &table, forward, limit, None).await,
            (Vec::new(), None),
            "forward {forward}, limit {limit:?}"
        );
    }
}

#[tokio::test]
async fn test_query_pages_past_rows_without_an_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryClaimRows", true).await;
    let live: Vec<String> = (0..10).map(|i| format!("s{i:02}")).collect();
    for sk in &live {
        put(&engine, &table, sk).await;
    }
    // Two before every item, two between s04 and s05, two after every item.
    for sk in ["a0", "a1", "s04a", "s04b", "t0", "t1"] {
        claim_row(&engine, &table, sk).await;
    }
    let reversed: Vec<String> = live.iter().rev().cloned().collect();

    // The first page holds the first item even when the rows before it
    // fill the LIMIT.
    let (page, last_key) = query(&engine, &table, true, Some(1), None).await;
    assert_eq!(page, ["s00"]);
    assert_eq!(last_key, Some(key("s00")));
    let (page, last_key) = query(&engine, &table, false, Some(1), None).await;
    assert_eq!(page, ["s09"]);
    assert_eq!(last_key, Some(key("s09")));

    // No Limit.
    assert_eq!(
        query(&engine, &table, true, None, None).await,
        (live.clone(), None)
    );
    assert_eq!(
        query(&engine, &table, false, None, None).await,
        (reversed.clone(), None)
    );

    // Every item, whatever the page size.
    for limit in [1, 2, 3, 4, 5, 9, 10, 11] {
        assert_eq!(
            read_all(&engine, &table, true, limit).await,
            live,
            "limit {limit}"
        );
        assert_eq!(
            read_all(&engine, &table, false, limit).await,
            reversed,
            "limit {limit}"
        );
    }
}

#[tokio::test]
async fn test_query_past_a_row_without_an_item_in_a_large_partition() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use futures::StreamExt;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryClaimRowLarge", true).await;
    let live: Vec<String> = (0..3000).map(|i| format!("s{i:04}")).collect();
    futures::stream::iter(&live)
        .for_each_concurrent(32, |sk| put(&engine, &table, sk))
        .await;
    claim_row(&engine, &table, "a0").await;

    // The page comes from the first rows read, whatever the size of the
    // partition behind them.
    let (page, last_key) = query(&engine, &table, true, Some(1), None).await;
    assert_eq!(page, ["s0000"]);
    assert_eq!(last_key, Some(key("s0000")));
    let (page, last_key) = query(&engine, &table, true, Some(100), None).await;
    assert_eq!(page, live[..100]);
    assert_eq!(last_key, Some(key("s0099")));
    assert_eq!(read_all(&engine, &table, true, 1000).await, live);
}

#[tokio::test]
async fn test_query_past_a_long_run_of_rows_without_an_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use futures::StreamExt;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryClaimRowRun", true).await;
    let claims: Vec<String> = (0..3000).map(|i| format!("a{i:04}")).collect();
    futures::stream::iter(&claims)
        .for_each_concurrent(32, |sk| claim_row(&engine, &table, sk))
        .await;
    for sk in ["s0", "s1"] {
        put(&engine, &table, sk).await;
    }

    assert_eq!(
        query(&engine, &table, true, Some(1), None).await,
        (vec!["s0".to_string()], Some(key("s0")))
    );
    assert_eq!(read_all(&engine, &table, true, 1).await, ["s0", "s1"]);
    assert_eq!(
        query(&engine, &table, false, Some(5), None).await,
        (vec!["s1".to_string(), "s0".to_string()], Some(key("s0")))
    );
}
