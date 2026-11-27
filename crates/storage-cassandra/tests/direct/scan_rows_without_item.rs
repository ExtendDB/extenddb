// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scan over data rows that hold no item.
//!
//! A partition whose items have all been deleted answers a read with one row
//! of its static columns, and a TTL claim on an absent key writes a row with
//! no `item_data`. Neither row is an item. A page may come back short because
//! of them, but while items remain it carries a LastEvaluatedKey, so a reader
//! paging on from the last item reads every item. With a Limit under 1,000, a
//! long run of such rows is read in reads of at most 1,000 rows each, and no
//! read goes back to the start of the run.

use cdrs_tokio::types::value::Value;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::DataEngine;
use extenddb_storage_cassandra::CassandraEngine;
use futures::StreamExt;
use std::collections::BTreeSet;

use crate::helpers::{
    TestTable, claim_row, data_table_metric, emptied_partition, key, put, scan_all, setup_engine,
};
use crate::scan::ids;

/// The key of item `id`: sort key "a" on a table with a sort key.
fn key_a(table: &TestTable, id: &str) -> Item {
    let sort = (table.key_info.key_schema.len() > 1).then(|| AttributeValue::S("a".to_string()));
    key(id, sort)
}

async fn put_and_delete(engine: &CassandraEngine, table: &TestTable, id: &str) {
    put(engine, table, key_a(table, id)).await;
    engine
        .delete_item(
            &table.key_info,
            &key_a(table, id),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("delete_item");
}

/// Pages through the table (or one segment of it) with `limit` and returns
/// every partition key read, in the order read.
async fn read_all(
    engine: &CassandraEngine,
    table: &TestTable,
    limit: Option<i64>,
    segment: Option<(i64, i64)>,
) -> Vec<String> {
    ids(&scan_all(engine, table, limit, segment).await)
}

/// 30 live partitions among 300 emptied ones and 20 claim rows, so runs of
/// rows without an item sit between the items in token order.
async fn fill(engine: &CassandraEngine, table: &TestTable) -> BTreeSet<String> {
    let live: Vec<String> = (0..30).map(|i| format!("live{i:03}")).collect();
    let emptied: Vec<String> = (0..300).map(|i| format!("gone{i:03}")).collect();
    let claims: Vec<String> = (0..20).map(|i| format!("claim{i:03}")).collect();
    let claim_sk = (table.key_info.key_schema.len() > 1).then(|| Value::from("a"));
    futures::stream::iter(&live)
        .for_each_concurrent(32, |id| put(engine, table, key_a(table, id)))
        .await;
    futures::stream::iter(&emptied)
        .for_each_concurrent(32, |id| put_and_delete(engine, table, id))
        .await;
    futures::stream::iter(&claims)
        .for_each_concurrent(32, |id| claim_row(engine, table, id, claim_sk.clone()))
        .await;
    live.into_iter().collect()
}

async fn assert_scans_read_every_item(engine: &CassandraEngine, table: &TestTable) {
    let live = fill(engine, table).await;
    for limit in [None, Some(1), Some(2), Some(7), Some(50)] {
        let read = read_all(engine, table, limit, None).await;
        assert_eq!(read.len(), live.len(), "limit {limit:?}: {read:?}");
        assert_eq!(
            read.into_iter().collect::<BTreeSet<_>>(),
            live,
            "limit {limit:?}"
        );
    }
    for limit in [None, Some(1), Some(3)] {
        let mut read = read_all(engine, table, limit, Some((0, 2))).await;
        read.extend(read_all(engine, table, limit, Some((1, 2))).await);
        assert_eq!(
            read.len(),
            live.len(),
            "segments, limit {limit:?}: {read:?}"
        );
        assert_eq!(
            read.into_iter().collect::<BTreeSet<_>>(),
            live,
            "segments, limit {limit:?}"
        );
    }
}

#[tokio::test]
async fn test_scan_past_partitions_without_an_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanRowsWithoutItem", true).await;
    assert_scans_read_every_item(&engine, &table).await;
}

#[tokio::test]
async fn test_scan_of_a_hash_only_table_past_claim_rows() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanHashOnlyClaims", false).await;
    assert_scans_read_every_item(&engine, &table).await;
}

#[tokio::test]
async fn test_scan_of_a_table_with_no_item_left() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanNoItemLeft", true).await;
    let emptied: Vec<String> = (0..50).map(|i| format!("gone{i:03}")).collect();
    futures::stream::iter(&emptied)
        .for_each_concurrent(32, |id| put_and_delete(&engine, &table, id))
        .await;
    for limit in [None, Some(1), Some(7)] {
        let (items, last_key) = engine
            .scan(&table.key_info, limit, None, None, None, None)
            .await
            .expect("scan");
        assert!(items.is_empty(), "limit {limit:?}: {:?}", ids(&items));
        assert_eq!(last_key, None, "limit {limit:?}");
    }
}

/// Writes `n` emptied partitions with CQL, as DeleteItem leaves them on a
/// table with a sort key: the static delete timestamp and a deleted row.
async fn emptied_partitions(engine: &CassandraEngine, table: &TestTable, n: usize) {
    let ids: Vec<String> = (0..n).map(|i| format!("gone{i:06}")).collect();
    let deleted = [Value::from("a")];
    futures::stream::iter(&ids)
        .for_each_concurrent(8, |id| emptied_partition(engine, table, id, &deleted))
        .await;
}

/// A run of claim rows inside one partition, ahead of an item in the same
/// partition: the scan reads on within the partition (query 1), not past it.
#[tokio::test]
async fn test_scan_past_claim_rows_inside_a_partition() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanClaimRun", true).await;
    for i in 0..40 {
        claim_row(&engine, &table, "p", Some(Value::from(format!("c{i:03}")))).await;
    }
    put(
        &engine,
        &table,
        key("p", Some(AttributeValue::S("z".to_string()))),
    )
    .await;
    for limit in [None, Some(1), Some(3)] {
        assert_eq!(
            read_all(&engine, &table, limit, None).await,
            ["p"],
            "limit {limit:?}"
        );
    }
}

/// Checks Cassandra's `system_views.tombstones_per_read` histogram for
/// `table`: no read counted 1,500 tombstones or more. The histogram rounds
/// up: a read of 1,001 rows shows as about 1,109.
async fn assert_no_read_of_1500_tombstones(engine: &CassandraEngine, table: &TestTable) {
    let max = data_table_metric::<f64>(engine, table, "tombstones_per_read", "max")
        .await
        .unwrap_or(0.0);
    assert!(
        max > 0.0,
        "no tombstones counted, so the check proves nothing"
    );
    assert!(max < 1_500.0, "one read counted {max} tombstones");
}

/// The range reads Cassandra has made of `table` (its
/// `system_views.local_scan_latency` count, which a refused read adds to as
/// well). The table is the test's own, so a change in the count is the
/// test's reads.
pub(crate) async fn range_reads(engine: &CassandraEngine, table: &TestTable) -> i64 {
    data_table_metric::<i64>(engine, table, "local_scan_latency", "count")
        .await
        .unwrap_or(0)
}

/// A run of emptied partitions longer than one bounded read, between items.
/// A paged Scan reads it in statements of at most 1,000 rows each, so no
/// single read of the data table counts more tombstones than that; a scan
/// that read the run again from its start with a growing LIMIT, or let the
/// read size grow without a cap, would read the whole run in one statement.
/// Each read takes twice the rows of the one before, so each pass reads the
/// run in at most 100 range reads; with reads that stay at the first read's
/// 2 rows, the Limit 1 pass takes about 2,500.
#[tokio::test]
async fn test_scan_past_a_long_run_of_emptied_partitions() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanEmptiedRun", true).await;
    emptied_partitions(&engine, &table, 5_000).await;
    let live: BTreeSet<String> = (0..5).map(|i| format!("live{i}")).collect();
    for id in &live {
        put(&engine, &table, key_a(&table, id)).await;
    }
    for limit in [Some(1), Some(2), Some(1000)] {
        let before = range_reads(&engine, &table).await;
        let read = read_all(&engine, &table, limit, None).await;
        let reads = range_reads(&engine, &table).await - before;
        assert_eq!(read.len(), live.len(), "limit {limit:?}: {read:?}");
        assert_eq!(
            read.into_iter().collect::<BTreeSet<_>>(),
            live,
            "limit {limit:?}"
        );
        assert!(reads <= 100, "limit {limit:?}: {reads} range reads");
    }
    assert_no_read_of_1500_tombstones(&engine, &table).await;
    // No Limit reads up to 1,000,001 rows in one statement, by design.
    let read = read_all(&engine, &table, None, None).await;
    assert_eq!(read.into_iter().collect::<BTreeSet<_>>(), live, "no limit");
}

/// A table with more emptied partitions than Cassandra's
/// `tombstone_failure_threshold` (100,000 by default). Each read of the run
/// stays below it, so the scan answers no item and no key, and no read
/// counts 1,500 tombstones or more.
#[tokio::test]
async fn test_scan_of_a_table_emptied_past_the_tombstone_threshold() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanEmptiedPastThreshold", true).await;
    emptied_partitions(&engine, &table, 101_000).await;
    for limit in [Some(1000), Some(128)] {
        let (items, last_key) = engine
            .scan(&table.key_info, limit, None, None, None, None)
            .await
            .expect("scan of an emptied table");
        assert!(items.is_empty(), "limit {limit:?}");
        assert_eq!(last_key, None, "limit {limit:?}");
    }
    assert_no_read_of_1500_tombstones(&engine, &table).await;
}
