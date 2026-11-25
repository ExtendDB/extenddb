// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scan past rows that hold no item, by sort key type, and past emptied
//! partitions that each hold many deleted rows.
//!
//! A full read of rows that hold no item reads on after its last row, with
//! the row's sort key read back from Cassandra and bound again; the typed
//! tests check that this works for number, binary and string sort keys. A
//! read through partitions with many deleted rows can reach Cassandra's
//! `tombstone_failure_threshold` (100,000 by default); the scan then reads
//! again with fewer rows, the first read of a storage call included, and the
//! later reads of the call stay at that size.

use bigdecimal::BigDecimal;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::decimal::Decimal;
use cdrs_tokio::types::value::Value;
use extenddb_core::types::{AttributeValue, ScalarAttributeType};
use extenddb_storage::DataEngine;
use extenddb_storage_cassandra::CassandraEngine;
use futures::StreamExt;
use std::str::FromStr;

use crate::helpers::{TestTable, claim_row, emptied_partition, key, put, scan_all, setup_engine};
use crate::scan::ids;
use crate::scan_rows_without_item::range_reads;

#[derive(Clone, Debug)]
enum Sk {
    S(&'static str),
    N(&'static str),
    B(Vec<u8>),
}

impl Sk {
    fn attr(&self) -> AttributeValue {
        match self {
            Sk::S(s) => AttributeValue::S((*s).to_string()),
            Sk::N(n) => AttributeValue::N((*n).to_string()),
            Sk::B(b) => AttributeValue::B(b.clone()),
        }
    }

    fn bind(&self) -> Value {
        match self {
            Sk::S(s) => Value::from(*s),
            Sk::N(n) => {
                let (unscaled, scale) = BigDecimal::from_str(n)
                    .expect("decimal")
                    .as_bigint_and_exponent();
                Value::from(Decimal::new(unscaled, i32::try_from(scale).expect("scale")))
            }
            Sk::B(b) => Value::from(Blob::new(b.clone())),
        }
    }
}

/// Numbers are stored in a normal form, so they are compared by value.
fn canon(v: &AttributeValue) -> String {
    match v {
        AttributeValue::N(n) => BigDecimal::from_str(n)
            .expect("number")
            .normalized()
            .to_string(),
        other => format!("{other:?}"),
    }
}

/// Partition `p` holding `rows` in sort key order (a claim row or an item
/// each), and 40 emptied partitions. Every page size reads exactly the items
/// of `p`, once each and in order.
async fn check_typed(ty: ScalarAttributeType, name: &str, rows: Vec<(Sk, bool)>, gone: Sk) {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_sort_key_type(&engine, name, ty).await;
    for (sk, is_item) in &rows {
        if *is_item {
            put(&engine, &table, key("p", Some(sk.attr()))).await;
        } else {
            claim_row(&engine, &table, "p", Some(sk.bind())).await;
        }
    }
    for i in 0..40 {
        emptied_partition(&engine, &table, &format!("gone{i:02}"), &[gone.bind()]).await;
    }
    let want: Vec<String> = rows
        .iter()
        .filter(|(_, is_item)| *is_item)
        .map(|(sk, _)| canon(&sk.attr()))
        .collect();
    for limit in [Some(1), Some(2), Some(3), Some(7), None] {
        let items = scan_all(&engine, &table, limit, None).await;
        let got: Vec<String> = items
            .iter()
            .map(|item| canon(item.get("sort").expect("sort")))
            .collect();
        assert_eq!(got, want, "limit {limit:?}");
    }
}

#[tokio::test]
async fn test_scan_past_claim_rows_with_a_number_sort_key() {
    let rows = vec![
        (Sk::N("-123456789012345678901234567890.123456789"), false),
        (Sk::N("-99999.5"), false),
        (Sk::N("-99999"), true),
        (Sk::N("-0.000000000000000000000000000001"), false),
        (Sk::N("0"), true),
        (Sk::N("0.1"), false),
        (Sk::N("0.10000000000000000000000000000000000001"), false),
        (Sk::N("0.2"), true),
        (Sk::N("1.000000000000000000000000000000000001"), false),
        (Sk::N("1E+5"), false),
        (Sk::N("1E+10"), true),
        (Sk::N("9.9999999999999999999999999999999999998E+125"), false),
        (Sk::N("9.9999999999999999999999999999999999999E+125"), false),
    ];
    check_typed(ScalarAttributeType::N, "ScanClaimNumber", rows, Sk::N("5")).await;
}

#[tokio::test]
async fn test_scan_past_claim_rows_with_a_binary_sort_key() {
    let b = |bytes: &[u8]| Sk::B(bytes.to_vec());
    let rows = vec![
        (b(&[0x00]), false),
        (b(&[0x00, 0x00]), false),
        (b(&[0x00, 0x01]), true),
        (b(&[0x7f, 0xff]), false),
        (b(&[0x80]), true),
        (b(&[0x80, 0x00]), false),
        (b(&[0xab; 1024]), false),
        (b(&[0xfe]), true),
        (b(&[0xff, 0x00]), false),
        (b(&[0xff, 0xff, 0x01]), true),
        (b(&[0xff, 0xff, 0xff]), false),
    ];
    check_typed(ScalarAttributeType::B, "ScanClaimBinary", rows, b(&[0x01])).await;
}

#[tokio::test]
async fn test_scan_past_claim_rows_with_a_string_sort_key() {
    let rows = vec![
        (Sk::S("A"), false),
        (Sk::S("a"), true),
        (Sk::S("\u{e9}"), false),
        (Sk::S("\u{65e5}\u{672c}"), false),
        (Sk::S("\u{1f600}"), true),
        (Sk::S("\u{1f601}"), false),
    ];
    check_typed(ScalarAttributeType::S, "ScanClaimString", rows, Sk::S("a")).await;
}

/// The sort keys `s000` onwards of the `n` deleted rows of an emptied
/// partition.
fn deleted_rows(n: usize) -> Vec<Value> {
    (0..n).map(|j| Value::from(format!("s{j:03}"))).collect()
}

async fn token_of(engine: &CassandraEngine, pk: &str) -> i64 {
    let rows = engine
        .session_arc()
        .query_with_values(
            "SELECT token(?) AS t FROM system.local",
            cdrs_tokio::query_values!(pk),
        )
        .await
        .expect("token")
        .response_body()
        .expect("body")
        .into_rows()
        .expect("rows");
    IntoRustByName::<i64>::get_by_name(&rows[0], "t")
        .expect("decode")
        .expect("not null")
}

/// `n` partition keys `<prefix><i>`, in token order.
async fn keys_in_token_order(engine: &CassandraEngine, prefix: &str, n: usize) -> Vec<String> {
    let mut tokens = Vec::with_capacity(n);
    for i in 0..n {
        let pk = format!("{prefix}{i:04}");
        tokens.push((token_of(engine, &pk).await, pk));
    }
    tokens.sort();
    tokens.into_iter().map(|(_, pk)| pk).collect()
}

/// 2,100 emptied partitions that each held 110 rows, and one item in the
/// partition with the highest token. A read of 1,000 such rows counts about
/// 110,000 tombstones, past the threshold; a read of 500 counts about 55,000.
/// A Scan with a small Limit reads through all of them to the item.
#[tokio::test]
async fn test_scan_past_emptied_partitions_with_many_deleted_rows() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanHeavyEmptied", true).await;
    let mut pks = keys_in_token_order(&engine, "heavy", 2101).await;
    let live = pks.pop().expect("partition keys");
    let deleted = deleted_rows(110);
    futures::stream::iter(&pks)
        .for_each_concurrent(8, |pk| emptied_partition(&engine, &table, pk, &deleted))
        .await;
    put(
        &engine,
        &table,
        key(&live, Some(AttributeValue::S("a".to_string()))),
    )
    .await;
    for limit in [Some(1), Some(128)] {
        let items = scan_all(&engine, &table, limit, None).await;
        assert_eq!(ids(&items), std::slice::from_ref(&live), "limit {limit:?}");
    }
}

/// Five items at the lowest tokens, then 300 emptied partitions that each
/// held 400 rows. The engine reads a page in storage calls: the first, with
/// Limit 128, reads the 5 items and 124 emptied partitions and returns the
/// fifth item's key. The second, with Limit 1,024, starts after that item.
/// Its reads of 1,025, 512 and 256 rows pass more than 100,000 tombstones,
/// and Cassandra refuses each of them. Each refused read runs again with half
/// the rows, and the later reads of the call stay at 128 rows. So the call
/// answers no item and no key in 6 range reads: the 3 refused ones, then 128,
/// 128 and 44 rows. Later reads that grew back to 256 rows would take the
/// last 172 partitions in one read, for 5 range reads.
#[tokio::test]
async fn test_scan_after_items_past_partitions_with_many_deleted_rows() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "ScanHeavyAfterItems", true).await;
    let mut gone = keys_in_token_order(&engine, "p", 305).await;
    let live: Vec<String> = gone.drain(..5).collect();
    let sort = || Some(AttributeValue::S("a".to_string()));
    for pk in &live {
        put(&engine, &table, key(pk, sort())).await;
    }
    let deleted = deleted_rows(400);
    futures::stream::iter(&gone)
        .for_each_concurrent(8, |pk| emptied_partition(&engine, &table, pk, &deleted))
        .await;

    let (items, last_key) = engine
        .scan(&table.key_info, Some(128), None, None, None, None)
        .await
        .expect("scan, Limit 128");
    assert_eq!(ids(&items), live);
    assert_eq!(last_key, Some(key(&live[4], sort())));
    let before = range_reads(&engine, &table).await;
    let (items, last_key) = engine
        .scan(
            &table.key_info,
            Some(1024),
            last_key.as_ref(),
            None,
            None,
            None,
        )
        .await
        .expect("scan after the fifth item, Limit 1024");
    let reads = range_reads(&engine, &table).await - before;
    assert_eq!((ids(&items), last_key), (Vec::<String>::new(), None));
    assert_eq!(reads, 6, "range reads of the call after the fifth item");

    // CreateBackup, export and the TTL work read with Limit 1,000 from the
    // start. Cassandra refuses that first read of 1,001 rows and its retry
    // of 500. A read of 250 rows passes 245 emptied partitions (98,000
    // tombstones) and returns the 5 items with the fifth item's key.
    let (items, last_key) = engine
        .scan(&table.key_info, Some(1000), None, None, None, None)
        .await
        .expect("scan from the start, Limit 1000");
    assert_eq!(ids(&items), live);
    assert_eq!(last_key, Some(key(&live[4], sort())));
}
