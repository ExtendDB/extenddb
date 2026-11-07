// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scan over data rows that hold no item.
//!
//! A partition whose items have all been deleted answers a read with one row
//! of its static columns, and a TTL claim on an absent key writes a row with
//! no `item_data`. Neither row is an item. A page may come back short because
//! of them, but while items remain it carries a LastEvaluatedKey, so a reader
//! paging on from the last item reads every item. A long run of such rows is
//! read in reads of at most about 1,000 rows, never from its start again.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::DataEngine;
use extenddb_storage_cassandra::CassandraEngine;
use futures::StreamExt;
use std::collections::{BTreeMap, BTreeSet};

use crate::helpers::{TestTable, setup_engine};

fn key(table: &TestTable, id: &str) -> Item {
    let mut key = BTreeMap::from([("id".to_string(), AttributeValue::S(id.to_string()))]);
    if table.key_info.key_schema.len() > 1 {
        key.insert("sort".to_string(), AttributeValue::S("a".to_string()));
    }
    key
}

async fn put(engine: &CassandraEngine, table: &TestTable, id: &str) {
    let mut item = key(table, id);
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

async fn put_and_delete(engine: &CassandraEngine, table: &TestTable, id: &str) {
    put(engine, table, id).await;
    engine
        .delete_item(
            &table.key_info,
            &key(table, id),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("delete_item");
}

/// Writes the row a TTL claim on the absent key `id` leaves: no `item_data`.
async fn claim_row(engine: &CassandraEngine, table: &TestTable, id: &str) {
    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace('-', "_"));
    let session = engine.session_arc();
    if table.key_info.key_schema.len() > 1 {
        session
            .query_with_values(
                format!(
                    "UPDATE {keyspace}.{data_table} SET prepared_txn_id = uuid(), \
                     prepared_txn_timestamp = 1 WHERE pk = ? AND sk_s = ?"
                ),
                cdrs_tokio::query_values!(id, "a"),
            )
            .await
            .expect("claim row");
    } else {
        session
            .query_with_values(
                format!(
                    "UPDATE {keyspace}.{data_table} SET prepared_txn_id = uuid(), \
                     prepared_txn_timestamp = 1 WHERE pk = ?"
                ),
                cdrs_tokio::query_values!(id),
            )
            .await
            .expect("claim row");
    }
}

fn ids(items: &[Item]) -> Vec<String> {
    items
        .iter()
        .map(|item| match item.get("id") {
            Some(AttributeValue::S(s)) => s.clone(),
            other => panic!("partition key {other:?}"),
        })
        .collect()
}

/// Pages through the table (or one segment of it) with `limit` and returns
/// every partition key read, in the order read.
async fn read_all(
    engine: &CassandraEngine,
    table: &TestTable,
    limit: Option<i64>,
    segment: Option<(i64, i64)>,
) -> Vec<String> {
    let mut all = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..1000 {
        let (items, last_key) = engine
            .scan(
                &table.key_info,
                limit,
                start.as_ref(),
                segment.map(|(s, _)| s),
                segment.map(|(_, t)| t),
                None,
            )
            .await
            .expect("scan");
        all.extend(ids(&items));
        match last_key {
            Some(k) => start = Some(k),
            None => return all,
        }
    }
    panic!("more than 1000 pages");
}

/// 30 live partitions among 300 emptied ones and 20 claim rows, so runs of
/// rows without an item sit between the items in token order.
async fn fill(engine: &CassandraEngine, table: &TestTable) -> BTreeSet<String> {
    let live: Vec<String> = (0..30).map(|i| format!("live{i:03}")).collect();
    let emptied: Vec<String> = (0..300).map(|i| format!("gone{i:03}")).collect();
    let claims: Vec<String> = (0..20).map(|i| format!("claim{i:03}")).collect();
    futures::stream::iter(&live)
        .for_each_concurrent(32, |id| put(engine, table, id))
        .await;
    futures::stream::iter(&emptied)
        .for_each_concurrent(32, |id| put_and_delete(engine, table, id))
        .await;
    futures::stream::iter(&claims)
        .for_each_concurrent(32, |id| claim_row(engine, table, id))
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

/// The CQL table behind `table`.
fn data_table(engine: &CassandraEngine, table: &TestTable) -> String {
    format!(
        "{}.items_{}",
        engine.account_keyspace(&table.key_info.account_id),
        table.key_info.table_id.replace('-', "_")
    )
}

/// Writes `n` emptied partitions with CQL, as DeleteItem leaves them on a
/// table with a sort key: the static delete timestamp and a deleted row.
async fn emptied_partitions(engine: &CassandraEngine, table: &TestTable, n: usize) {
    let cql = format!(
        "BEGIN UNLOGGED BATCH \
         UPDATE {t} SET partition_max_delete_timestamp = 1 WHERE pk = ?; \
         DELETE FROM {t} WHERE pk = ? AND sk_s = 'a'; \
         APPLY BATCH",
        t = data_table(engine, table)
    );
    let session = engine.session_arc();
    let ids: Vec<String> = (0..n).map(|i| format!("gone{i:06}")).collect();
    futures::stream::iter(&ids)
        .for_each_concurrent(8, |id| {
            let (session, cql) = (session.clone(), cql.clone());
            async move {
                session
                    .query_with_values(cql, cdrs_tokio::query_values!(id.as_str(), id.as_str()))
                    .await
                    .expect("emptied partition");
            }
        })
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
    let t = data_table(&engine, &table);
    let session = engine.session_arc();
    for i in 0..40 {
        session
            .query_with_values(
                format!(
                    "UPDATE {t} SET prepared_txn_id = uuid(), prepared_txn_timestamp = 1 \
                     WHERE pk = 'p' AND sk_s = ?"
                ),
                cdrs_tokio::query_values!(format!("c{i:03}")),
            )
            .await
            .expect("claim row");
    }
    let mut item = BTreeMap::from([
        ("id".to_string(), AttributeValue::S("p".to_string())),
        ("sort".to_string(), AttributeValue::S("z".to_string())),
    ]);
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
    for limit in [None, Some(1), Some(3)] {
        assert_eq!(
            read_all(&engine, &table, limit, None).await,
            ["p"],
            "limit {limit:?}"
        );
    }
}

/// The most tombstones Cassandra has counted in one read of `table`'s data
/// table, from its `system_views.tombstones_per_read` histogram.
async fn max_tombstones_per_read(engine: &CassandraEngine, table: &TestTable) -> f64 {
    use cdrs_tokio::types::IntoRustByName;

    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace('-', "_"));
    let result = engine
        .session_arc()
        .query_with_values(
            "SELECT max FROM system_views.tombstones_per_read \
             WHERE keyspace_name = ? AND table_name = ?",
            cdrs_tokio::query_values!(keyspace.as_str(), data_table.as_str()),
        )
        .await
        .expect("tombstones_per_read");
    let rows = result
        .response_body()
        .expect("tombstones_per_read body")
        .into_rows()
        .unwrap_or_default();
    let max: Option<f64> = rows
        .first()
        .map(|row| row.get_by_name("max").expect("max column"))
        .unwrap_or_default();
    max.unwrap_or(0.0)
}

/// A run of emptied partitions longer than one bounded read, between items.
/// A paged Scan reads it in statements of at most about 1,000 rows each, so
/// no single read of the data table counts more tombstones than that; a scan
/// that read the run again from its start with a growing LIMIT, or let the
/// read size grow without a cap, would read the whole run in one statement.
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
        put(&engine, &table, id).await;
    }
    for limit in [Some(1), Some(2), Some(1000)] {
        let read = read_all(&engine, &table, limit, None).await;
        assert_eq!(read.len(), live.len(), "limit {limit:?}: {read:?}");
        assert_eq!(
            read.into_iter().collect::<BTreeSet<_>>(),
            live,
            "limit {limit:?}"
        );
    }
    let max = max_tombstones_per_read(&engine, &table).await;
    assert!(
        max > 0.0,
        "no tombstones counted, so the check proves nothing"
    );
    // The histogram rounds up: a read of 1,001 rows shows as about 1,109.
    assert!(max < 1_500.0, "one read counted {max} tombstones");
    // No Limit reads up to 1,000,001 rows in one statement, by design.
    let read = read_all(&engine, &table, None, None).await;
    assert_eq!(read.into_iter().collect::<BTreeSet<_>>(), live, "no limit");
}

/// A table with more emptied partitions than Cassandra's
/// `tombstone_failure_threshold` (100,000 by default). Each read of the run
/// stays below it, so the scan answers no item and no key.
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
}
