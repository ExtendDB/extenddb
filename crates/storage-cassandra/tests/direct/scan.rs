// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for the Scan operation.
//!
//! These exercise `DataEngine::scan` against the Cassandra backend directly:
//! full base-table scans (hash-only and composite key), token-based key
//! pagination, parallel-scan token-range segments, and index scans.
//!
//! Scan order in Cassandra follows the token ring (not attribute value order),
//! so pagination/segment tests assert on the *set* of returned items (coverage
//! and absence of duplicates) rather than a specific ordering.

use extenddb_core::types::AttributeValue;
use extenddb_storage::DataEngine;
use std::collections::BTreeMap;
use std::collections::HashSet;

use crate::helpers::{TestTable, setup_engine};

/// Insert `count` items into a hash-only table with ids `item-000..`.
async fn put_pk_only_items(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    table: &TestTable,
    count: usize,
) {
    for i in 0..count {
        let mut item = BTreeMap::new();
        item.insert(
            "id".to_string(),
            AttributeValue::S(format!("item-{:03}", i)),
        );
        item.insert("value".to_string(), AttributeValue::N(i.to_string()));
        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }
}

/// Collect the `id` attribute (string) from a list of items.
fn ids(items: &[BTreeMap<String, AttributeValue>]) -> Vec<String> {
    items
        .iter()
        .map(|item| match item.get("id") {
            Some(AttributeValue::S(s)) => s.clone(),
            other => panic!("expected string id, got {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn test_scan_empty_table() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanEmptyTable", false).await;

    let (items, last_key) = engine
        .scan(&table.key_info, None, None, None, None, None)
        .await
        .expect("scan should succeed");

    assert!(items.is_empty(), "empty table should return no items");
    assert!(
        last_key.is_none(),
        "empty table should have no pagination key"
    );
}

#[tokio::test]
async fn test_scan_pk_only_all() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanPkOnlyTable", false).await;

    put_pk_only_items(&engine, &table, 10).await;

    let (items, last_key) = engine
        .scan(&table.key_info, None, None, None, None, None)
        .await
        .expect("scan should succeed");

    assert_eq!(items.len(), 10, "should return all 10 items");
    assert!(
        last_key.is_none(),
        "no pagination key when all items returned"
    );

    let unique: HashSet<String> = ids(&items).into_iter().collect();
    assert_eq!(unique.len(), 10, "all returned ids should be unique");
}

#[tokio::test]
async fn test_scan_composite_key_all() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanCompositeTable", true).await;

    // 3 partitions, 4 sort keys each = 12 items.
    for p in 0..3 {
        for s in 0..4 {
            let mut item = BTreeMap::new();
            item.insert("id".to_string(), AttributeValue::S(format!("pk-{p}")));
            item.insert("sort".to_string(), AttributeValue::S(format!("sk-{s:02}")));
            item.insert(
                "data".to_string(),
                AttributeValue::N((p * 10 + s).to_string()),
            );
            engine
                .put_item(
                    &table.key_info,
                    item,
                    false,
                    None,
                    &Default::default(),
                    None,
                )
                .await
                .expect("put_item should succeed");
        }
    }

    let (items, last_key) = engine
        .scan(&table.key_info, None, None, None, None, None)
        .await
        .expect("scan should succeed");

    assert_eq!(items.len(), 12, "should return all 12 items");
    assert!(last_key.is_none());

    // Every (pk, sk) pair should appear exactly once.
    let pairs: HashSet<(String, String)> = items
        .iter()
        .map(|item| {
            let id = match item.get("id") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => panic!("expected id"),
            };
            let sort = match item.get("sort") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => panic!("expected sort"),
            };
            (id, sort)
        })
        .collect();
    assert_eq!(pairs.len(), 12, "all 12 (pk, sk) pairs should be present");
}

#[tokio::test]
async fn test_scan_limit_returns_last_key() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanLimitTable", false).await;

    put_pk_only_items(&engine, &table, 10).await;

    let (items, last_key) = engine
        .scan(&table.key_info, Some(4), None, None, None, None)
        .await
        .expect("scan should succeed");

    assert_eq!(items.len(), 4, "should return exactly the limit");
    assert!(
        last_key.is_some(),
        "more items remain, so a LastEvaluatedKey is expected"
    );
}

#[tokio::test]
async fn test_scan_pagination_pk_only() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanPagePkTable", false).await;

    let total = 10usize;
    put_pk_only_items(&engine, &table, total).await;

    // Walk all pages with a small limit and verify full, duplicate-free coverage.
    let mut seen: HashSet<String> = HashSet::new();
    let mut start_key: Option<BTreeMap<String, AttributeValue>> = None;
    let mut pages = 0;

    loop {
        let (items, last_key) = engine
            .scan(
                &table.key_info,
                Some(3),
                start_key.as_ref(),
                None,
                None,
                None,
            )
            .await
            .expect("scan page should succeed");

        for id in ids(&items) {
            assert!(seen.insert(id.clone()), "duplicate id across pages: {id}");
        }

        pages += 1;
        assert!(pages <= total + 2, "pagination did not terminate");

        match last_key {
            Some(k) => start_key = Some(k),
            None => break,
        }
    }

    assert_eq!(seen.len(), total, "all items should be seen exactly once");
}

#[tokio::test]
async fn test_scan_pagination_composite_key() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanPageCompositeTable", true).await;

    // 4 partitions x 5 sort keys = 20 items, forcing both pagination queries
    // (finish-current-partition and next-partitions).
    let mut expected: HashSet<(String, String)> = HashSet::new();
    for p in 0..4 {
        for s in 0..5 {
            let id = format!("pk-{p}");
            let sort = format!("sk-{s:02}");
            expected.insert((id.clone(), sort.clone()));
            let mut item = BTreeMap::new();
            item.insert("id".to_string(), AttributeValue::S(id));
            item.insert("sort".to_string(), AttributeValue::S(sort));
            item.insert(
                "data".to_string(),
                AttributeValue::N((p * 10 + s).to_string()),
            );
            engine
                .put_item(
                    &table.key_info,
                    item,
                    false,
                    None,
                    &Default::default(),
                    None,
                )
                .await
                .expect("put_item should succeed");
        }
    }

    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut start_key: Option<BTreeMap<String, AttributeValue>> = None;
    let mut pages = 0;

    loop {
        let (items, last_key) = engine
            .scan(
                &table.key_info,
                Some(3),
                start_key.as_ref(),
                None,
                None,
                None,
            )
            .await
            .expect("scan page should succeed");

        for item in &items {
            let id = match item.get("id") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => panic!("expected id"),
            };
            let sort = match item.get("sort") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => panic!("expected sort"),
            };
            assert!(
                seen.insert((id.clone(), sort.clone())),
                "duplicate (pk, sk) across pages: {id}/{sort}"
            );
        }

        pages += 1;
        assert!(pages <= 30, "pagination did not terminate");

        match last_key {
            Some(k) => start_key = Some(k),
            None => break,
        }
    }

    assert_eq!(seen, expected, "all (pk, sk) pairs covered exactly once");
}

#[tokio::test]
async fn test_scan_parallel_segments() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanSegmentsTable", false).await;

    let total_items = 30usize;
    put_pk_only_items(&engine, &table, total_items).await;

    let total_segments = 4i64;
    let mut seen: HashSet<String> = HashSet::new();

    for segment in 0..total_segments {
        let (items, last_key) = engine
            .scan(
                &table.key_info,
                None,
                None,
                Some(segment),
                Some(total_segments),
                None,
            )
            .await
            .expect("segment scan should succeed");

        // No limit was set, so each segment should be fully drained in one call.
        assert!(
            last_key.is_none(),
            "segment {segment} should be fully drained"
        );

        for id in ids(&items) {
            assert!(
                seen.insert(id.clone()),
                "id {id} appeared in more than one segment"
            );
        }
    }

    assert_eq!(
        seen.len(),
        total_items,
        "union of all segments should cover every item exactly once"
    );
}

#[tokio::test]
async fn test_scan_gsi() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "ScanGsiTable", "StatusIndex", "status").await;

    // Make the GSI synchronous so writes land in the index immediately.
    let catalog_keyspace = engine.catalog_keyspace();
    let update_delay = format!(
        "UPDATE {}.indexes SET propagation_delay_ms = 0 WHERE table_id = ? AND index_name = ?",
        catalog_keyspace
    );
    engine
        .session()
        .query_with_values(
            &update_delay,
            cdrs_tokio::query_values!(table.key_info.table_id.as_str(), "StatusIndex"),
        )
        .await
        .expect("failed to set GSI to synchronous");

    let items_data = vec![
        ("item1", "active"),
        ("item2", "pending"),
        ("item3", "active"),
        ("item4", "inactive"),
    ];
    for (id, status) in items_data {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert("status".to_string(), AttributeValue::S(status.to_string()));
        item.insert("data".to_string(), AttributeValue::S(format!("data-{id}")));
        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Scan the index table itself (all index entries).
    let (items, last_key) = engine
        .scan(&table.key_info, None, None, None, None, Some("StatusIndex"))
        .await
        .expect("index scan should succeed");

    assert_eq!(
        items.len(),
        4,
        "index scan should return all 4 projected items"
    );
    assert!(last_key.is_none());

    let unique: HashSet<String> = ids(&items).into_iter().collect();
    assert_eq!(unique.len(), 4, "all 4 index entries should be unique");
}

#[tokio::test]
async fn test_scan_gsi_pagination() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "ScanGsiPageTable", "StatusIndex", "status").await;

    let catalog_keyspace = engine.catalog_keyspace();
    let update_delay = format!(
        "UPDATE {}.indexes SET propagation_delay_ms = 0 WHERE table_id = ? AND index_name = ?",
        catalog_keyspace
    );
    engine
        .session()
        .query_with_values(
            &update_delay,
            cdrs_tokio::query_values!(table.key_info.table_id.as_str(), "StatusIndex"),
        )
        .await
        .expect("failed to set GSI to synchronous");

    // Several distinct status values (distinct index partitions) plus repeats
    // (same index partition, distinct base keys) to exercise both pagination
    // queries on the index table.
    let total = 12usize;
    let statuses = [
        "active", "pending", "active", "inactive", "active", "pending",
    ];
    for i in 0..total {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S(format!("item-{i:02}")));
        item.insert(
            "status".to_string(),
            AttributeValue::S(statuses[i % statuses.len()].to_string()),
        );
        item.insert("data".to_string(), AttributeValue::N(i.to_string()));
        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut start_key: Option<BTreeMap<String, AttributeValue>> = None;
    let mut pages = 0;

    loop {
        let (items, last_key) = engine
            .scan(
                &table.key_info,
                Some(5),
                start_key.as_ref(),
                None,
                None,
                Some("StatusIndex"),
            )
            .await
            .expect("index scan page should succeed");

        for id in ids(&items) {
            assert!(
                seen.insert(id.clone()),
                "duplicate id across index pages: {id}"
            );
        }

        pages += 1;
        assert!(pages <= total + 2, "index pagination did not terminate");

        // The storage layer returns an index-keys-only LastEvaluatedKey; the
        // engine normally enriches it with the base table key. Mimic that here
        // so the next page can resume within the correct index partition.
        match last_key {
            Some(mut k) => {
                let last = items.last().expect("non-empty page has a last item");
                if let Some(id) = last.get("id") {
                    k.insert("id".to_string(), id.clone());
                }
                start_key = Some(k);
            }
            None => break,
        }
    }

    assert_eq!(seen.len(), total, "all index items seen exactly once");
}

// ─────────────────────────────────────────────────────────────────────────────
// Sort-key type coverage (N, B) and LSI scans.
//
// The base-table scan tests above use String sort keys. These exercise the
// numeric (`sk_n`) and binary (`sk_b`) clustering columns — including the
// numeric literal path used by pagination's "finish current partition" query —
// plus a scan over a Local Secondary Index table.
// ─────────────────────────────────────────────────────────────────────────────

use extenddb_core::types::ScalarAttributeType;

#[tokio::test]
async fn test_scan_numeric_sort_key_all() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table =
        TestTable::with_sort_key_type(&engine, "ScanNumSkTable", ScalarAttributeType::N).await;

    // 3 partitions x 4 numeric sort keys = 12 items.
    let mut expected: HashSet<(String, String)> = HashSet::new();
    for p in 0..3 {
        for s in 0..4 {
            let id = format!("pk-{p}");
            let sort = (s * 100).to_string();
            expected.insert((id.clone(), sort.clone()));
            let mut item = BTreeMap::new();
            item.insert("id".to_string(), AttributeValue::S(id));
            item.insert("sort".to_string(), AttributeValue::N(sort));
            engine
                .put_item(
                    &table.key_info,
                    item,
                    false,
                    None,
                    &Default::default(),
                    None,
                )
                .await
                .expect("put_item should succeed");
        }
    }

    let (items, last_key) = engine
        .scan(&table.key_info, None, None, None, None, None)
        .await
        .expect("scan should succeed");

    assert_eq!(items.len(), 12, "should return all 12 numeric-SK items");
    assert!(last_key.is_none());

    let pairs: HashSet<(String, String)> = items
        .iter()
        .map(|item| {
            let id = match item.get("id") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => panic!("expected id"),
            };
            let sort = match item.get("sort") {
                Some(AttributeValue::N(n)) => n.clone(),
                _ => panic!("expected numeric sort"),
            };
            (id, sort)
        })
        .collect();
    assert_eq!(pairs, expected, "all numeric (pk, sk) pairs present");
}

#[tokio::test]
async fn test_scan_numeric_sort_key_pagination() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table =
        TestTable::with_sort_key_type(&engine, "ScanNumSkPageTable", ScalarAttributeType::N).await;

    // 4 partitions x 5 numeric sort keys = 20 items, forcing the finish-partition
    // (`sk_n > <literal>`) and next-partitions queries during pagination.
    let mut expected: HashSet<(String, String)> = HashSet::new();
    for p in 0..4 {
        for s in 0..5 {
            let id = format!("pk-{p}");
            let sort = (s * 10).to_string();
            expected.insert((id.clone(), sort.clone()));
            let mut item = BTreeMap::new();
            item.insert("id".to_string(), AttributeValue::S(id));
            item.insert("sort".to_string(), AttributeValue::N(sort));
            engine
                .put_item(
                    &table.key_info,
                    item,
                    false,
                    None,
                    &Default::default(),
                    None,
                )
                .await
                .expect("put_item should succeed");
        }
    }

    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut start_key: Option<BTreeMap<String, AttributeValue>> = None;
    let mut pages = 0;

    loop {
        let (items, last_key) = engine
            .scan(
                &table.key_info,
                Some(3),
                start_key.as_ref(),
                None,
                None,
                None,
            )
            .await
            .expect("scan page should succeed");

        for item in &items {
            let id = match item.get("id") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => panic!("expected id"),
            };
            let sort = match item.get("sort") {
                Some(AttributeValue::N(n)) => n.clone(),
                _ => panic!("expected numeric sort"),
            };
            assert!(
                seen.insert((id.clone(), sort.clone())),
                "duplicate (pk, sk) across pages: {id}/{sort}"
            );
        }

        pages += 1;
        assert!(pages <= 30, "pagination did not terminate");

        match last_key {
            Some(k) => start_key = Some(k),
            None => break,
        }
    }

    assert_eq!(
        seen, expected,
        "all numeric (pk, sk) pairs covered exactly once"
    );
}

#[tokio::test]
async fn test_scan_binary_sort_key_all() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table =
        TestTable::with_sort_key_type(&engine, "ScanBinSkTable", ScalarAttributeType::B).await;

    // 2 partitions x 3 binary sort keys = 6 items.
    let mut expected: HashSet<(String, Vec<u8>)> = HashSet::new();
    for p in 0..2u8 {
        for s in 0..3u8 {
            let id = format!("pk-{p}");
            let sort = vec![p, s, 0xAB];
            expected.insert((id.clone(), sort.clone()));
            let mut item = BTreeMap::new();
            item.insert("id".to_string(), AttributeValue::S(id));
            item.insert("sort".to_string(), AttributeValue::B(sort));
            engine
                .put_item(
                    &table.key_info,
                    item,
                    false,
                    None,
                    &Default::default(),
                    None,
                )
                .await
                .expect("put_item should succeed");
        }
    }

    let (items, last_key) = engine
        .scan(&table.key_info, None, None, None, None, None)
        .await
        .expect("scan should succeed");

    assert_eq!(items.len(), 6, "should return all 6 binary-SK items");
    assert!(last_key.is_none());

    let pairs: HashSet<(String, Vec<u8>)> = items
        .iter()
        .map(|item| {
            let id = match item.get("id") {
                Some(AttributeValue::S(s)) => s.clone(),
                _ => panic!("expected id"),
            };
            let sort = match item.get("sort") {
                Some(AttributeValue::B(b)) => b.clone(),
                _ => panic!("expected binary sort"),
            };
            (id, sort)
        })
        .collect();
    assert_eq!(pairs, expected, "all binary (pk, sk) pairs present");
}

#[tokio::test]
async fn test_scan_lsi() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    // Base key: (id S, sort N); LSI key: (id S, priority N). LSIs are always
    // synchronous, so index rows are written on put_item.
    let table = TestTable::with_lsi(&engine, "ScanLsiTable", "LsiPriority", "priority").await;

    let rows = vec![
        ("user1", 1000, 3),
        ("user1", 2000, 1),
        ("user2", 1500, 2),
        ("user2", 2500, 5),
    ];
    for (id, sort, priority) in &rows {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S((*id).to_string()));
        item.insert("sort".to_string(), AttributeValue::N(sort.to_string()));
        item.insert(
            "priority".to_string(),
            AttributeValue::N(priority.to_string()),
        );
        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    let (items, last_key) = engine
        .scan(&table.key_info, None, None, None, None, Some("LsiPriority"))
        .await
        .expect("LSI scan should succeed");

    assert_eq!(
        items.len(),
        4,
        "LSI scan should return all 4 projected items"
    );
    assert!(last_key.is_none());

    // Each item should carry the base + index key attributes.
    for item in &items {
        assert!(item.contains_key("id"), "projected item missing id");
        assert!(
            item.contains_key("priority"),
            "projected item missing priority"
        );
    }
}
