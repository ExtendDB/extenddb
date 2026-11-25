// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Query over data rows that hold no item.
//!
//! A row without `item_data` holds no item. Such rows are the static-only
//! row that a partition whose items have all been deleted answers a
//! partition read with, the row a TTL claim on an absent key writes (only
//! `prepared_txn_id` and `prepared_txn_timestamp` set), and the row a
//! transaction prepare leaves when its `UPDATE ... IF prepared_txn_id =
//! null` lands on an item deleted after the transaction read it. A page may
//! come back short because of them, but while items remain it carries a
//! LastEvaluatedKey, so a reader paging on from the last item reads every
//! item. A run of such rows is read on after its last row, in reads of at
//! most 1,000 rows (or the read's own LIMIT, if larger), and no row is read
//! twice. The GSI backfill reads the rest of a partition after a page that
//! ends inside it.

use cdrs_tokio::types::value::Value;
use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::DataEngine;
use extenddb_storage_cassandra::CassandraEngine;
use futures::StreamExt;

use crate::helpers::{TestTable, claim_row, data_table_metric, key, put, setup_engine};

/// The key of the item with sort key `sk` in partition `p`.
fn at(sk: &str) -> Item {
    key("p", Some(AttributeValue::S(sk.to_string())))
}

/// Writes the row a TTL claim on the absent key `sk` of partition `p` leaves.
async fn claim(engine: &CassandraEngine, table: &TestTable, sk: &str) {
    claim_row(engine, table, "p", Some(Value::from(sk))).await;
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

/// The single-partition reads Cassandra has made of `table` (the count of
/// its `system_views.local_read_latency`). The table is the test's own, so
/// a change in the count is the test's reads.
async fn partition_reads(engine: &CassandraEngine, table: &TestTable) -> i64 {
    data_table_metric::<i64>(engine, table, "local_read_latency", "count")
        .await
        .unwrap_or(0)
}

#[tokio::test]
async fn test_query_of_an_emptied_partition() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryEmptied", true).await;
    for sk in ["a", "b"] {
        put(&engine, &table, at(sk)).await;
    }
    for sk in ["a", "b"] {
        engine
            .delete_item(
                &table.key_info,
                &at(sk),
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
        put(&engine, &table, at(sk)).await;
    }
    // Two before every item, two between s04 and s05, two after every item.
    for sk in ["a0", "a1", "s04a", "s04b", "t0", "t1"] {
        claim(&engine, &table, sk).await;
    }
    let reversed: Vec<String> = live.iter().rev().cloned().collect();

    // The first page holds the first item even when the rows before it
    // fill the LIMIT.
    let (page, last_key) = query(&engine, &table, true, Some(1), None).await;
    assert_eq!(page, ["s00"]);
    assert_eq!(last_key, Some(at("s00")));
    let (page, last_key) = query(&engine, &table, false, Some(1), None).await;
    assert_eq!(page, ["s09"]);
    assert_eq!(last_key, Some(at("s09")));

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
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryClaimRowLarge", true).await;
    let live: Vec<String> = (0..3000).map(|i| format!("s{i:04}")).collect();
    futures::stream::iter(&live)
        .for_each_concurrent(32, |sk| put(&engine, &table, at(sk)))
        .await;
    claim(&engine, &table, "a0").await;

    // The page comes from the first rows read, whatever the size of the
    // partition behind them.
    let (page, last_key) = query(&engine, &table, true, Some(1), None).await;
    assert_eq!(page, ["s0000"]);
    assert_eq!(last_key, Some(at("s0000")));
    let (page, last_key) = query(&engine, &table, true, Some(100), None).await;
    assert_eq!(page, live[..100]);
    assert_eq!(last_key, Some(at("s0099")));
    assert_eq!(read_all(&engine, &table, true, 1000).await, live);
}

/// 3,000 rows without an item ahead of two items. With Limit 1, the reads
/// take 2, 4, 8 and so on up to 512 rows, then 1,000, and the 11th read
/// reaches the items. A read again from the start with a doubled LIMIT takes
/// 13 reads, and reads that stay at 2 rows take 1,500.
#[tokio::test]
async fn test_query_past_a_long_run_of_rows_without_an_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryClaimRowRun", true).await;
    let claims: Vec<String> = (0..3000).map(|i| format!("a{i:04}")).collect();
    futures::stream::iter(&claims)
        .for_each_concurrent(32, |sk| claim(&engine, &table, sk))
        .await;
    for sk in ["s0", "s1"] {
        put(&engine, &table, at(sk)).await;
    }

    let before = partition_reads(&engine, &table).await;
    assert_eq!(
        query(&engine, &table, true, Some(1), None).await,
        (vec!["s0".to_string()], Some(at("s0")))
    );
    let reads = partition_reads(&engine, &table).await - before;
    assert!(reads <= 11, "limit 1: {reads} reads");
    assert_eq!(read_all(&engine, &table, true, 1).await, ["s0", "s1"]);
    assert_eq!(
        query(&engine, &table, false, Some(5), None).await,
        (vec!["s1".to_string(), "s0".to_string()], Some(at("s0")))
    );
}

/// Writes `claims` rows without an item, then 300 items, in partition `p`
/// of a new table, reads it with Limit 128, and returns the most rows one
/// read of the data table returned (the `max` of its
/// `system_views.rows_per_read` histogram).
async fn max_rows_per_read_at_limit_128(
    engine: &CassandraEngine,
    name: &str,
    claims: usize,
) -> f64 {
    let table = TestTable::new(engine, name, true).await;
    let claim_keys: Vec<String> = (0..claims).map(|i| format!("a{i:04}")).collect();
    let live: Vec<String> = (0..300).map(|i| format!("s{i:04}")).collect();
    futures::stream::iter(&claim_keys)
        .for_each_concurrent(32, |sk| claim(engine, &table, sk))
        .await;
    futures::stream::iter(&live)
        .for_each_concurrent(32, |sk| put(engine, &table, at(sk)))
        .await;

    let reads_before = partition_reads(engine, &table).await;
    let counted_before = data_table_metric::<i64>(engine, &table, "rows_per_read", "count")
        .await
        .unwrap_or(0);
    assert_eq!(
        query(engine, &table, true, Some(128), None).await,
        (live[..128].to_vec(), Some(at("s0127"))),
        "{claims} rows without an item"
    );
    let reads = partition_reads(engine, &table).await - reads_before;
    // Cassandra adds each read to the rows_per_read histogram a moment after
    // the read; wait for all of them.
    let mut counted = counted_before;
    for _ in 0..50 {
        counted = data_table_metric::<i64>(engine, &table, "rows_per_read", "count")
            .await
            .unwrap_or(0);
        if counted >= counted_before + reads {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        counted >= counted_before + reads,
        "rows_per_read counted {} of {reads} reads",
        counted - counted_before
    );
    data_table_metric::<f64>(engine, &table, "rows_per_read", "max")
        .await
        .unwrap_or(0.0)
}

/// Rows without an item ahead of 300 items, read with Limit 128. With 1,030
/// such rows the reads take 129, 258 and 516 rows, then up to 1,000, which
/// reaches the items. With 3,500, the reads after the read of 516 rows take
/// up to 1,000 rows each. No read returns more than 1,000 rows. A read again
/// from the start with a doubled LIMIT ends with a read that returns every
/// row of the partition (1,330 and 3,800). Reads that double without the cap
/// of 1,000 rows return 1,865 rows in the fifth read of the second shape.
#[tokio::test]
async fn test_query_past_a_run_of_rows_without_an_item_reads_1000_rows_at_most() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    for (name, claims) in [
        ("QueryClaimRowOvershoot", 1030),
        ("QueryClaimRowLongRun", 3500),
    ] {
        let max = max_rows_per_read_at_limit_128(&engine, name, claims).await;
        // The histogram rounds a count up: a read of 1,000 rows shows as
        // 1,109, and one of 1,330 rows as 1,331.
        assert!(max > 0.0, "no read counted, so the check proves nothing");
        assert!(
            max <= 1_109.0,
            "{claims} rows without an item: one read returned {max} rows"
        );
    }
}

/// 2,100 rows without an item, each followed by 110 deleted rows, ahead of
/// two items. With Limit 1 the reads take 2, 4, 8 and so on up to 512 rows,
/// and pass at most 56,320 deleted rows. The read of 1,000 rows passes about
/// 110,000, more than Cassandra's `tombstone_failure_threshold` (100,000 by
/// default), and Cassandra refuses it. The read runs again with 500 rows,
/// and the later reads stay at 500 rows, so the Query answers the first item.
#[tokio::test]
async fn test_query_reads_again_with_half_the_rows_when_cassandra_refuses_a_read() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryClaimRowsTombstones", true).await;
    let t = crate::helpers::data_table(&engine, &table);
    let session = engine.session_arc();
    let runs: Vec<usize> = (0..2100).collect();
    futures::stream::iter(&runs)
        .for_each_concurrent(8, |i| {
            let (session, t) = (session.clone(), t.clone());
            async move {
                // Test-generated ASCII keys, so literals are safe here.
                let mut cql = format!(
                    "BEGIN UNLOGGED BATCH UPDATE {t} SET prepared_txn_id = uuid(), \
                     prepared_txn_timestamp = 1 WHERE pk = 'p' AND sk_s = 'c{i:04}'; "
                );
                for j in 0..110 {
                    cql.push_str(&format!(
                        "DELETE FROM {t} WHERE pk = 'p' AND sk_s = 'c{i:04}_{j:03}'; "
                    ));
                }
                cql.push_str("APPLY BATCH");
                session
                    .query(cql)
                    .await
                    .expect("claim row and deleted rows");
            }
        })
        .await;
    for sk in ["s0", "s1"] {
        put(&engine, &table, at(sk)).await;
    }
    assert_eq!(
        query(&engine, &table, true, Some(1), None).await,
        (vec!["s0".to_string()], Some(at("s0")))
    );
}

/// A GSI added by UpdateTable to a table with one partition of 600 items
/// and one of 1,000 rows without an item ahead of the items `z1` and `z2`.
/// The backfill reads 500 rows a page. After a page that ends inside a
/// partition it reads the rest of that partition, so the index goes ACTIVE
/// with all 602 items. A backfill that moved its token cursor past the
/// partition of a full page wrote 500.
#[tokio::test]
async fn test_a_gsi_backfill_reads_the_rest_of_a_partition() {
    use extenddb_storage::TableEngine as _;
    use std::sync::Arc;
    use std::time::Duration;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = Arc::new(
        CassandraEngine::new(&crate::helpers::test_config(), "us-east-1")
            .await
            .expect("engine"),
    );
    let table = TestTable::new(&engine, "QueryBackfillPaging", true).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("activate the table");

    let indexed = |pk: &str, sk: String| {
        let mut item = key(pk, Some(AttributeValue::S(sk)));
        item.insert("status".to_string(), AttributeValue::S("x".to_string()));
        item
    };
    let big: Vec<String> = (0..600).map(|i| format!("s{i:03}")).collect();
    futures::stream::iter(&big)
        .for_each_concurrent(32, |sk| put(&engine, &table, indexed("big", sk.clone())))
        .await;
    let claims: Vec<String> = (0..1000).map(|i| format!("a{i:04}")).collect();
    futures::stream::iter(&claims)
        .for_each_concurrent(32, |sk| {
            claim_row(&engine, &table, "claimed", Some(Value::from(sk.as_str())))
        })
        .await;
    for sk in ["z1", "z2"] {
        put(&engine, &table, indexed("claimed", sk.to_string())).await;
    }

    let shutdown = extenddb_storage::CancellationToken::new();
    let worker = extenddb_storage_cassandra::workers::spawn_gsi_backfill_recovery_for_tests(
        engine.clone(),
        &table.key_info.account_id,
        Duration::from_secs(120),
        Duration::from_secs(60),
        shutdown.clone(),
    );
    engine
        .update_table(
            &table.key_info.account_id,
            crate::index::status_index_creation(&table.key_info.table_name),
        )
        .await
        .expect("add the index");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let index_id = loop {
        let (status, _, index_id) =
            crate::index::index_state(&engine, &table.key_info.table_id, "StatusIndex").await;
        if status == "ACTIVE" {
            break index_id;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the index is {status} after 120 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), worker)
        .await
        .expect("the worker returns within 30 s of its token")
        .expect("the worker does not panic");
    let account_keyspace = engine.account_keyspace(&table.key_info.account_id);
    assert_eq!(
        crate::index::index_rows(&engine, &account_keyspace, &index_id).await,
        602
    );
}
