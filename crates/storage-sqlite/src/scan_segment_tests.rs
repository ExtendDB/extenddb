// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A parallel Scan returns every item of a partition key from one segment, on
//! a table and on a GSI, and every item from one segment, with and without
//! `Limit`, as DynamoDB does (measured in us-east-1, 13 and 14 Nov 2026). The
//! start-key check answers with the rule the scan predicate applies.

use std::collections::BTreeMap;

use extenddb_core::types::{AttributeValue, Item, TableKeyInfo, extract_key};
use extenddb_storage::{DataEngine, TableEngine};
use serde_json::json;

use crate::sqlite_util::scan_segment;
use crate::store::SqliteEngine;

const ACCOUNT: &str = "000000000000";
const TABLE: &str = "scan-segments";
const GSI: &str = "byGroup";

/// Items per partition key: `p0` holds 5, `p1` 3, `p2` 2 and `p3` 1.
const PARTITIONS: [(&str, usize); 4] = [("p0", 5), ("p1", 3), ("p2", 2), ("p3", 1)];
const ITEM_COUNT: usize = 11;

/// The segment counts of the measured Scans on the table and on the GSI.
const TABLE_SEGMENT_COUNTS: [i64; 6] = [1, 2, 3, 4, 11, 13];
const GSI_SEGMENT_COUNTS: [i64; 5] = [1, 2, 3, 6, 8];

async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    // Tables start ACTIVE and GSIs are maintained in the write transaction:
    // no control-plane or index worker runs inside a unit test.
    for (key, value) in [
        ("control_plane_delay_seconds", "0"),
        ("index_propagation_delay_ms", "0"),
    ] {
        sqlx::query("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&engine.pool)
            .await
            .expect("setting");
    }
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    engine
}

/// A table keyed on `pk` S HASH and `sk` S RANGE with 11 items in four
/// partitions, and a GSI keyed on `gpk` S with projection ALL: `g0` for the
/// even items and `g1` for the odd ones. Returns the table's key info.
async fn table(engine: &SqliteEngine) -> TableKeyInfo {
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
        "TableName": TABLE,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [{
            "IndexName": GSI,
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .expect("input");
    engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create table");
    let key_info = engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("key info");
    let mut n = 0;
    for (pk, count) in PARTITIONS {
        for i in 0..count {
            let item: Item = serde_json::from_value(json!({
                "pk": {"S": pk},
                "sk": {"S": format!("s{i}")},
                "gpk": {"S": format!("g{}", n % 2)},
                "v": {"N": n.to_string()}
            }))
            .expect("item");
            engine
                .put_item(
                    &key_info,
                    item,
                    false,
                    None,
                    &extenddb_core::expression::ExpressionMaps::default(),
                    None,
                )
                .await
                .expect("put");
            n += 1;
        }
    }
    assert_eq!(n, ITEM_COUNT);
    key_info
}

/// The key info the engine hands storage for a Scan of the GSI: the index key
/// schema over the table's base key schema.
fn gsi_key_info(table: &TableKeyInfo) -> TableKeyInfo {
    let gsi = table
        .global_secondary_indexes
        .iter()
        .find(|idx| idx.index_name == GSI)
        .expect("gsi");
    TableKeyInfo {
        key_schema: gsi.key_schema.clone(),
        base_key_schema: table.key_schema.clone(),
        ..table.clone()
    }
}

/// The `ExclusiveStartKey` the engine builds from the last item of a page:
/// the scanned key schema's attributes, plus the base key on an index.
fn start_key(item: &Item, key_info: &TableKeyInfo) -> Item {
    let mut key = extract_key(item, &key_info.key_schema);
    key.extend(extract_key(item, &key_info.base_key_schema));
    key
}

fn text(item: &Item, attr: &str) -> String {
    match item.get(attr) {
        Some(AttributeValue::S(s)) => s.clone(),
        other => panic!("{attr} is {other:?}"),
    }
}

/// Every item of one segment, page by page as the engine reads them.
async fn segment_items(
    engine: &SqliteEngine,
    key_info: &TableKeyInfo,
    index: Option<&str>,
    segment: i64,
    total: i64,
    limit: Option<i64>,
) -> Vec<Item> {
    let mut items = Vec::new();
    let mut start: Option<Item> = None;
    for _ in 0..=ITEM_COUNT {
        let (page, last) = engine
            .scan(
                key_info,
                limit,
                start.as_ref(),
                Some(segment),
                Some(total),
                index,
            )
            .await
            .expect("scan");
        if let Some(limit) = limit {
            assert!(page.len() <= usize::try_from(limit).expect("limit"));
        }
        items.extend(page);
        match last {
            Some(_) => start = items.last().map(|item| start_key(item, key_info)),
            None => return items,
        }
    }
    panic!("segment {segment} of {total} did not end after {ITEM_COUNT} pages");
}

/// One case per measured Scan, with and without a page limit: the Scan
/// target, its segment count and the `Limit`.
fn cases() -> Vec<(Option<&'static str>, i64, Option<i64>)> {
    let mut cases = Vec::new();
    for limit in [None, Some(2)] {
        cases.extend(TABLE_SEGMENT_COUNTS.map(|total| (None, total, limit)));
        cases.extend(GSI_SEGMENT_COUNTS.map(|total| (Some(GSI), total, limit)));
    }
    cases
}

#[tokio::test]
async fn every_partition_key_comes_back_from_one_segment() {
    let engine = engine().await;
    let table = table(&engine).await;
    let gsi = gsi_key_info(&table);
    for (index, total, limit) in cases() {
        let label = format!("index {index:?}, {total} segments, limit {limit:?}");
        let key_info = if index.is_some() { &gsi } else { &table };
        let pk_attr = &key_info.key_schema[0].attribute_name;
        let mut segments = Vec::new();
        for segment in 0..total {
            segments.push(segment_items(&engine, key_info, index, segment, total, limit).await);
        }

        // Every item once across the segments, by its base key.
        let mut seen: BTreeMap<(String, String), i64> = BTreeMap::new();
        // Every partition key value from one segment.
        let mut segment_of: BTreeMap<String, i64> = BTreeMap::new();
        for (segment, items) in (0..).zip(&segments) {
            for item in items {
                let base_key = (text(item, "pk"), text(item, "sk"));
                assert_eq!(
                    seen.insert(base_key.clone(), segment),
                    None,
                    "{label}: {base_key:?} came back twice"
                );
                let pk = text(item, pk_attr);
                let first = *segment_of.entry(pk.clone()).or_insert(segment);
                assert_eq!(
                    first, segment,
                    "{label}: {pk_attr} {pk} came back from segments {first} and {segment}"
                );
            }
        }
        assert_eq!(seen.len(), ITEM_COUNT, "{label}: every item once");

        // The start-key check applies the same rule: an item's key belongs
        // to the segment that returned it and to no other.
        for (segment, items) in (0..).zip(&segments) {
            for item in items {
                let key = start_key(item, key_info);
                for other in 0..total {
                    let in_segment = engine
                        .scan_key_in_segment(key_info, &key, other, total, index)
                        .await
                        .expect("membership");
                    assert_eq!(in_segment, other == segment, "{label}: {key:?} in {other}");
                }
            }
        }
    }
}

#[tokio::test]
async fn sql_function_answers_the_rule() {
    let engine = engine().await;
    for (pk, total) in [
        ("p0", 1),
        ("p0", 2),
        ("g1", 13),
        ("k\u{e9}y", 1_000_000),
        ("", 7),
    ] {
        let answer: i64 = sqlx::query_scalar("SELECT extenddb_scan_segment(?, ?)")
            .bind(pk)
            .bind(total)
            .fetch_one(&engine.pool)
            .await
            .expect("function");
        assert_eq!(
            answer,
            scan_segment(pk.as_bytes(), total),
            "{pk} in {total}"
        );
        assert!((0..total).contains(&answer), "{pk} in {total}: {answer}");
    }
    let null: Option<i64> = sqlx::query_scalar("SELECT extenddb_scan_segment(NULL, 4)")
        .fetch_one(&engine.pool)
        .await
        .expect("null");
    assert_eq!(null, None);
}
