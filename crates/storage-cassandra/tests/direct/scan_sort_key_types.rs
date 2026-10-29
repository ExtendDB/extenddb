// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scan past rows that hold no item, by sort key type, and past emptied
//! partitions that each hold many deleted rows.
//!
//! A full read of rows that hold no item reads on after its last row, with
//! the row's sort key read back from Cassandra and bound again; the typed
//! tests check that this works for number, binary and string sort keys. A
//! read through partitions with many deleted rows can reach Cassandra's
//! `tombstone_failure_threshold` (100,000 by default) at 1,000 rows; the scan
//! then reads again with fewer rows.

use bigdecimal::BigDecimal;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::decimal::Decimal;
use cdrs_tokio::types::value::Value;
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item, ScalarAttributeType};
use extenddb_storage::DataEngine;
use extenddb_storage_cassandra::CassandraEngine;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::helpers::{TestTable, setup_engine};

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

    fn column(&self) -> &'static str {
        match self {
            Sk::S(_) => "sk_s",
            Sk::N(_) => "sk_n",
            Sk::B(_) => "sk_b",
        }
    }
}

fn data_table(engine: &CassandraEngine, table: &TestTable) -> String {
    format!(
        "{}.items_{}",
        engine.account_keyspace(&table.key_info.account_id),
        table.key_info.table_id.replace('-', "_")
    )
}

/// The row a TTL claim on the absent key (`pk`, `sk`) leaves: no `item_data`.
async fn claim(engine: &CassandraEngine, table: &TestTable, pk: &str, sk: &Sk) {
    let t = data_table(engine, table);
    engine
        .session_arc()
        .query_with_values(
            format!(
                "UPDATE {t} SET prepared_txn_id = uuid(), prepared_txn_timestamp = 1 \
                 WHERE pk = ? AND {} = ?",
                sk.column()
            ),
            cdrs_tokio::query_values!(pk, sk.bind()),
        )
        .await
        .expect("claim row");
}

/// A partition as DeleteItem leaves it once its only item is gone.
async fn emptied(engine: &CassandraEngine, table: &TestTable, pk: &str, sk: &Sk) {
    let t = data_table(engine, table);
    engine
        .session_arc()
        .query_with_values(
            format!(
                "BEGIN UNLOGGED BATCH \
                 UPDATE {t} SET partition_max_delete_timestamp = 1 WHERE pk = ?; \
                 DELETE FROM {t} WHERE pk = ? AND {} = ?; \
                 APPLY BATCH",
                sk.column()
            ),
            cdrs_tokio::query_values!(pk, pk, sk.bind()),
        )
        .await
        .expect("emptied partition");
}

async fn put_at(engine: &CassandraEngine, table: &TestTable, pk: &str, sk: &Sk) {
    let item: Item = BTreeMap::from([
        ("id".to_string(), AttributeValue::S(pk.to_string())),
        ("sort".to_string(), sk.attr()),
        ("v".to_string(), AttributeValue::S("x".to_string())),
    ]);
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

/// Pages a Scan to the end and returns the items in the order read.
async fn read_all(engine: &CassandraEngine, table: &TestTable, limit: Option<i64>) -> Vec<Item> {
    let (mut all, mut start): (Vec<Item>, Option<Item>) = (Vec::new(), None);
    for _ in 0..3000 {
        let (items, last_key) = engine
            .scan(&table.key_info, limit, start.as_ref(), None, None, None)
            .await
            .expect("scan");
        all.extend(items);
        match last_key {
            Some(k) => start = Some(k),
            None => return all,
        }
    }
    panic!("more than 3000 pages");
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
            put_at(&engine, &table, "p", sk).await;
        } else {
            claim(&engine, &table, "p", sk).await;
        }
    }
    for i in 0..40 {
        emptied(&engine, &table, &format!("gone{i:02}"), &gone).await;
    }
    let want: Vec<String> = rows
        .iter()
        .filter(|(_, is_item)| *is_item)
        .map(|(sk, _)| canon(&sk.attr()))
        .collect();
    for limit in [Some(1), Some(2), Some(3), Some(7), None] {
        let items = read_all(&engine, &table, limit).await;
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
    let t = data_table(&engine, &table);
    let session = engine.session_arc();
    let mut tokens: Vec<(i64, String)> = Vec::new();
    for i in 0..2101 {
        let pk = format!("heavy{i:04}");
        tokens.push((token_of(&engine, &pk).await, pk));
    }
    tokens.sort();
    let (_, live) = tokens.pop().expect("partition keys");
    futures::stream::iter(tokens.iter().map(|(_, pk)| pk))
        .for_each_concurrent(8, |pk| {
            let (session, t) = (session.clone(), t.clone());
            async move {
                // Test-generated ASCII keys, so literals are safe here.
                let mut cql = format!(
                    "BEGIN UNLOGGED BATCH UPDATE {t} SET partition_max_delete_timestamp = 1 \
                     WHERE pk = '{pk}'; "
                );
                for j in 0..110 {
                    cql.push_str(&format!(
                        "DELETE FROM {t} WHERE pk = '{pk}' AND sk_s = 's{j:03}'; "
                    ));
                }
                cql.push_str("APPLY BATCH");
                session.query(cql).await.expect("emptied partition");
            }
        })
        .await;
    put_at(&engine, &table, &live, &Sk::S("a")).await;
    for limit in [Some(1), Some(128)] {
        let items = read_all(&engine, &table, limit).await;
        let ids: Vec<&AttributeValue> = items.iter().filter_map(|i| i.get("id")).collect();
        assert_eq!(ids, [&AttributeValue::S(live.clone())], "limit {limit:?}");
    }
}
