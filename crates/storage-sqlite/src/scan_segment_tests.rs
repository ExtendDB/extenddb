// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A parallel Scan returns every item of a partition key from one segment, on
//! a table and on a GSI, and every item from one segment, with and without
//! `Limit`, as DynamoDB does (measured in us-east-1 on 13 and 14 Nov 2026, and
//! with `Limit` 1 on 17 Nov 2026). The start-key check answers with the rule
//! the scan predicate applies.

use std::collections::BTreeMap;

use extenddb_core::types::{AttributeValue, Item, TableKeyInfo, extract_key};
use extenddb_storage::util::composite_pk_to_text;
use extenddb_storage::{DataEngine, TableEngine};
use serde_json::json;

use crate::restore_job_tests::{ACCOUNT, engine};
use crate::store::SqliteEngine;

const TABLE: &str = "scan-segments";
const GSI: &str = "byGroup";
/// The GSI with two HASH attributes, whose `pk` column holds the composite
/// key text.
const TWO_HASH_GSI: &str = "byPair";

/// Items per partition key: `p0` holds 5, `p1` 3, `p2` 2 and `p3` 1.
const PARTITIONS: [(&str, usize); 4] = [("p0", 5), ("p1", 3), ("p2", 2), ("p3", 1)];
const ITEM_COUNT: usize = 11;

/// The segment counts of the measured Scans on the table and on the GSI.
const TABLE_SEGMENT_COUNTS: [i64; 6] = [1, 2, 3, 4, 11, 13];
const GSI_SEGMENT_COUNTS: [i64; 5] = [1, 2, 3, 6, 8];

/// A table keyed on `pk` S HASH and `sk` S RANGE with 11 items in four
/// partitions, a GSI keyed on `gpk` S with projection ALL (`g0` for the even
/// items and `g1` for the odd ones), and a GSI keyed on `ga` S and `gb` N,
/// two HASH attributes. Returns the table's key info.
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
            {"AttributeName": "gpk", "AttributeType": "S"},
            {"AttributeName": "ga", "AttributeType": "S"},
            {"AttributeName": "gb", "AttributeType": "N"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [
            {
                "IndexName": GSI,
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": TWO_HASH_GSI,
                "KeySchema": [
                    {"AttributeName": "ga", "KeyType": "HASH"},
                    {"AttributeName": "gb", "KeyType": "HASH"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }
        ]
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
                "ga": {"S": (["a", "b", "c"][n % 3])},
                "gb": {"N": (n % 2).to_string()},
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

/// The key info the engine hands storage for a Scan of the index `name`: the
/// index key schema over the table's base key schema.
fn index_key_info(table: &TableKeyInfo, name: &str) -> TableKeyInfo {
    let gsi = table
        .global_secondary_indexes
        .iter()
        .find(|idx| idx.index_name == name)
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
/// target, its segment count and the `Limit`. The last case is the GSI with
/// two HASH attributes, without a page limit.
fn cases() -> Vec<(Option<&'static str>, i64, Option<i64>)> {
    let mut cases = Vec::new();
    for limit in [None, Some(2)] {
        cases.extend(TABLE_SEGMENT_COUNTS.map(|total| (None, total, limit)));
        cases.extend(GSI_SEGMENT_COUNTS.map(|total| (Some(GSI), total, limit)));
    }
    cases.push((Some(TWO_HASH_GSI), 3, None));
    cases
}

#[tokio::test]
async fn every_partition_key_comes_back_from_one_segment() {
    let engine = engine().await;
    let table = table(&engine).await;
    for (index, total, limit) in cases() {
        let label = format!("index {index:?}, {total} segments, limit {limit:?}");
        let key_info = index.map_or_else(|| table.clone(), |name| index_key_info(&table, name));
        let mut segments = Vec::new();
        for segment in 0..total {
            segments.push(segment_items(&engine, &key_info, index, segment, total, limit).await);
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
                let pk = composite_pk_to_text(item, &key_info.key_schema).expect("pk text");
                let first = *segment_of.entry(pk.clone()).or_insert(segment);
                assert_eq!(
                    first, segment,
                    "{label}: partition key {pk} came back from segments {first} and {segment}"
                );
            }
        }
        assert_eq!(seen.len(), ITEM_COUNT, "{label}: every item once");

        // The start-key check applies the same rule: an item's key belongs
        // to the segment that returned it and to no other.
        for (segment, items) in (0..).zip(&segments) {
            for item in items {
                let key = start_key(item, &key_info);
                for other in 0..total {
                    let in_segment = engine
                        .scan_key_in_segment(&key_info, &key, other, total, index)
                        .await
                        .expect("membership");
                    assert_eq!(in_segment, other == segment, "{label}: {key:?} in {other}");
                }
            }
        }
    }

    // A NULL pk, which no row has, answers NULL as SQLite's own functions do.
    let null: Option<i64> = sqlx::query_scalar("SELECT extenddb_scan_segment(NULL, 4)")
        .fetch_one(&engine.pool)
        .await
        .expect("null");
    assert_eq!(null, None);
}
