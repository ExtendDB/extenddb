use super::*;
use extenddb_core::expression::{parse_key_condition, tokenize};
use extenddb_core::types::{AttributeValue, CreateTableInput, IndexType};
use extenddb_storage::DataEngine;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};

const ACCOUNT: &str = "000000000000";
const TABLE: &str = "lsi-plan";

async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 2, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    sqlx::query(
        "INSERT INTO accounts (account_id, account_name) VALUES ('000000000000', 'default')",
    )
    .execute(&engine.pool)
    .await
    .expect("account");
    // Tables become ACTIVE at create time: no transition poller runs here.
    sqlx::query(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0')",
    )
    .execute(&engine.pool)
    .await
    .expect("settings");
    engine
}

/// A (pk S, sk S) table with an LSI on (pk, lsk S) and a GSI on (gpk S,
/// gsk S), and 300 items in partition `p` sharing 30 index sort key values.
async fn table(engine: &SqliteEngine) -> TableKeyInfo {
    let input: CreateTableInput = serde_json::from_value(json!({
        "TableName": TABLE,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "lsk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
            {"AttributeName": "gsk", "AttributeType": "S"}
        ],
        "LocalSecondaryIndexes": [{
            "IndexName": "lsi1",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi1",
            "KeySchema": [
                {"AttributeName": "gpk", "KeyType": "HASH"},
                {"AttributeName": "gsk", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create table");
    let key_info = engine
        .fetch_table_key_info(ACCOUNT, TABLE)
        .await
        .expect("key info");
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    for i in 0..300 {
        let s = |v: String| AttributeValue::S(v);
        let item: Item = BTreeMap::from([
            ("pk".to_owned(), s("p".to_owned())),
            ("sk".to_owned(), s(format!("s{i:04}"))),
            ("lsk".to_owned(), s(format!("l{:02}", i % 30))),
            ("gpk".to_owned(), s("g".to_owned())),
            ("gsk".to_owned(), s(format!("l{:02}", i % 30))),
        ]);
        engine
            .put_item(&key_info, item, false, None, &maps, None)
            .await
            .expect("put an item");
    }
    key_info
}

/// The table's key info as the engine passes it for a Query on `index`,
/// with the index table name and whether the index is an LSI.
async fn index_query_target(
    engine: &SqliteEngine,
    base: &TableKeyInfo,
    index: &str,
) -> (TableKeyInfo, String, bool) {
    let info = engine
        .fetch_index_info_by_table_id(&base.table_id, index)
        .await
        .expect("index info");
    let key_info = TableKeyInfo {
        key_schema: info.key_schema.clone(),
        base_key_schema: base.key_schema.clone(),
        ..base.clone()
    };
    (
        key_info,
        index_table_name(&info.index_id),
        info.index_type == IndexType::Lsi,
    )
}

fn partition(attr: &str, value: &str) -> (KeyCondition, ExpressionMaps) {
    condition(&format!("{attr} = :v"), &[("v", value)])
}

/// A key condition and its value map; `values` name the placeholders
/// without the colon, as the engine passes them.
fn condition(expr: &str, values: &[(&str, &str)]) -> (KeyCondition, ExpressionMaps) {
    let tokens = tokenize(expr).expect("tokenize");
    let cond = parse_key_condition(&tokens).expect("parse");
    let values = values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), AttributeValue::S((*value).to_owned())))
        .collect();
    (cond, ExpressionMaps::new(HashMap::new(), values))
}

fn s_key(pairs: &[(&str, &str)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), AttributeValue::S((*v).to_owned())))
        .collect()
}

/// `EXPLAIN QUERY PLAN` detail lines for a statement and its binds.
async fn plan(engine: &SqliteEngine, sql: &str, binds: Vec<BoundValue>) -> Vec<String> {
    let explain = format!("EXPLAIN QUERY PLAN {sql}");
    let mut query = sqlx::query_as::<_, (i64, i64, i64, String)>(&explain);
    for bound in binds {
        query = crate::data::bind_bound!(query, bound);
    }
    query
        .fetch_all(&engine.pool)
        .await
        .expect("explain the statement")
        .into_iter()
        .map(|(_, _, _, detail)| detail)
        .collect()
}

/// Plan the Query statement for one page and assert it is a walk of the
/// ordering index with no sort step.
#[allow(clippy::too_many_arguments)]
async fn assert_index_walk(
    engine: &SqliteEngine,
    label: &str,
    key_info: &TableKeyInfo,
    ddb_table: &str,
    is_lsi: bool,
    cond: &KeyCondition,
    maps: &ExpressionMaps,
    forward: bool,
    start: Option<&Item>,
) {
    let (sql, binds) = query_statement(
        ddb_table,
        is_lsi,
        key_info,
        cond,
        maps,
        forward,
        Some(100),
        start,
        true,
    )
    .expect("build the statement")
    .expect("an index Query always reads");
    let plan = plan(engine, &sql, binds).await;
    let shown = format!("{sql}\n  {}", plan.join("\n  "));
    assert!(
        !plan.iter().any(|l| l.contains("TEMP B-TREE")),
        "{label}: the plan sorts in a temporary B-tree:\n{shown}"
    );
    assert!(
        plan.iter()
            .any(|l| l.contains("USING INDEX") && l.contains("idx_order_")),
        "{label}: the plan does not walk the ordering index:\n{shown}"
    );
}

#[tokio::test]
async fn lsi_query_walks_the_ordering_index_without_a_sort() {
    let engine = engine().await;
    let base = table(&engine).await;
    let (lsi, lsi_table, is_lsi) = index_query_target(&engine, &base, "lsi1").await;
    assert!(is_lsi);
    // The partition alone, one index sort key value, and a range of them.
    let shapes = [
        ("pk only", partition("pk", "p")),
        (
            "lsk = :l",
            condition("pk = :v AND lsk = :l", &[("v", "p"), ("l", "l07")]),
        ),
        (
            "lsk BETWEEN",
            condition(
                "pk = :v AND lsk BETWEEN :a AND :b",
                &[("v", "p"), ("a", "l05"), ("b", "l10")],
            ),
        ),
    ];
    // A start key inside a group of rows sharing an index sort key value.
    let start = s_key(&[("pk", "p"), ("lsk", "l07"), ("sk", "s0127")]);
    for (shape, (cond, maps)) in &shapes {
        for forward in [true, false] {
            for start in [None, Some(&start)] {
                let label = format!("LSI {shape} forward={forward} resumed={}", start.is_some());
                assert_index_walk(
                    &engine, &label, &lsi, &lsi_table, true, cond, maps, forward, start,
                )
                .await;
            }
        }
    }
}

#[tokio::test]
async fn gsi_query_walks_the_ordering_index_without_a_sort() {
    let engine = engine().await;
    let base = table(&engine).await;
    let (gsi, gsi_table, is_lsi) = index_query_target(&engine, &base, "gsi1").await;
    assert!(!is_lsi);
    let (cond, maps) = partition("gpk", "g");
    let start = s_key(&[("gpk", "g"), ("gsk", "l07"), ("pk", "p"), ("sk", "s0127")]);
    for forward in [true, false] {
        for start in [None, Some(&start)] {
            let label = format!("GSI forward={forward} resumed={}", start.is_some());
            assert_index_walk(
                &engine, &label, &gsi, &gsi_table, false, &cond, &maps, forward, start,
            )
            .await;
        }
    }
}

/// A GSI with RANGE attributes `a`, `b` and `c` orders on all three and
/// then the base key, the columns of its `idx_order_*` after `pk`:
/// `(pk, sk_s, sk2_s, sk3_s, base_pk, base_sk_s)`. Its pages walk that
/// index for the partition alone, for conditions on a prefix of the RANGE
/// attributes, and for equalities on all of them, and each shape's pages
/// return the items its condition selects in that order.
#[tokio::test]
async fn multi_range_gsi_query_walks_the_ordering_index_without_a_sort() {
    const MULTI: &str = "multi-range-plan";
    let engine = engine().await;
    // Zero delay, so each write puts its GSI row in the same transaction and
    // the pages below read it without a GSI worker.
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '0') \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .execute(&engine.pool)
    .await
    .expect("synchronous propagation");
    let input: CreateTableInput = serde_json::from_value(json!({
        "TableName": MULTI,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "h", "AttributeType": "S"},
            {"AttributeName": "a", "AttributeType": "S"},
            {"AttributeName": "b", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "gsi3",
            "KeySchema": [
                {"AttributeName": "h", "KeyType": "HASH"},
                {"AttributeName": "a", "KeyType": "RANGE"},
                {"AttributeName": "b", "KeyType": "RANGE"},
                {"AttributeName": "c", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create table");
    let base = engine
        .fetch_table_key_info(ACCOUNT, MULTI)
        .await
        .expect("key info");
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    // 300 items in index partition `g` from 7 base partitions, 5 items per
    // (a, b, c) value.
    for i in 0..300 {
        let s = |v: String| AttributeValue::S(v);
        let item: Item = BTreeMap::from([
            ("pk".to_owned(), s(format!("p{}", i % 7))),
            ("sk".to_owned(), s(format!("s{i:04}"))),
            ("h".to_owned(), s("g".to_owned())),
            ("a".to_owned(), s(format!("a{}", i % 3))),
            ("b".to_owned(), s(format!("b{}", i % 5))),
            ("c".to_owned(), s(format!("c{}", i % 4))),
        ]);
        engine
            .put_item(&base, item, false, None, &maps, None)
            .await
            .expect("put an item");
    }
    let (gsi, gsi_table, is_lsi) = index_query_target(&engine, &base, "gsi3").await;
    assert!(!is_lsi);
    // The engine assigns the clauses of a key condition on a multi-RANGE
    // index to their key attributes before storage sees it.
    let classified = |expr: &str, values: &[(&str, &str)]| {
        let (mut cond, maps) = condition(expr, values);
        cond.resolve_key_schema(&gsi.key_schema, &maps.names)
            .expect("a key condition the service serves");
        (cond, maps)
    };
    let shapes = [
        ("h only", classified("h = :h", &[("h", "g")])),
        (
            "a = :a",
            classified("h = :h AND a = :a", &[("h", "g"), ("a", "a1")]),
        ),
        (
            "a = :a AND b BETWEEN",
            classified(
                "h = :h AND a = :a AND b BETWEEN :lo AND :hi",
                &[("h", "g"), ("a", "a1"), ("lo", "b1"), ("hi", "b3")],
            ),
        ),
        (
            "a, b and c equal",
            classified(
                "h = :h AND a = :a AND b = :b AND c = :c",
                &[("h", "g"), ("a", "a1"), ("b", "b2"), ("c", "c3")],
            ),
        ),
    ];
    // Item 127: a1, b2, c3, base key (p1, s0127).
    let start = s_key(&[
        ("h", "g"),
        ("a", "a1"),
        ("b", "b2"),
        ("c", "c3"),
        ("pk", "p1"),
        ("sk", "s0127"),
    ]);
    for (shape, (cond, maps)) in &shapes {
        for forward in [true, false] {
            for start in [None, Some(&start)] {
                let label = format!(
                    "multi-RANGE GSI {shape} forward={forward} resumed={}",
                    start.is_some()
                );
                assert_index_walk(
                    &engine, &label, &gsi, &gsi_table, false, cond, maps, forward, start,
                )
                .await;
            }
        }
    }
    // The pages of each shape, read with Limit 7 in both directions, hold the
    // items the condition selects in the index order. A condition applied to
    // the wrong RANGE column walks the same index, so only the rows show it.
    let keep: [fn(usize) -> bool; 4] = [
        |_| true,
        |i| i % 3 == 1,
        |i| i % 3 == 1 && (1..=3).contains(&(i % 5)),
        |i| i % 3 == 1 && i % 5 == 2 && i % 4 == 3,
    ];
    let text = |item: &Item, attr: &str| match item.get(attr) {
        Some(AttributeValue::S(v)) => v.clone(),
        other => panic!("{attr}: {other:?}"),
    };
    for ((shape, (cond, maps)), keep) in shapes.iter().zip(keep) {
        let mut want: Vec<[String; 5]> = (0..300)
            .filter(|&i| keep(i))
            .map(|i| {
                [
                    format!("a{}", i % 3),
                    format!("b{}", i % 5),
                    format!("c{}", i % 4),
                    format!("p{}", i % 7),
                    format!("s{i:04}"),
                ]
            })
            .collect();
        want.sort();
        for forward in [true, false] {
            let mut got = Vec::new();
            let mut start: Option<Item> = None;
            loop {
                let (items, last) = engine
                    .query_impl(
                        &gsi,
                        cond,
                        maps,
                        forward,
                        Some(7),
                        start.as_ref(),
                        Some("gsi3"),
                    )
                    .await
                    .expect("query a page");
                got.extend(
                    items
                        .iter()
                        .map(|item| ["a", "b", "c", "pk", "sk"].map(|attr| text(item, attr))),
                );
                if last.is_none() {
                    break;
                }
                let tail = items.last().expect("a page with a key has items");
                start = Some(s_key(&[
                    ("h", "g"),
                    ("a", &text(tail, "a")),
                    ("b", &text(tail, "b")),
                    ("c", &text(tail, "c")),
                    ("pk", &text(tail, "pk")),
                    ("sk", &text(tail, "sk")),
                ]));
            }
            let expected: Vec<[String; 5]> = if forward {
                want.clone()
            } else {
                want.iter().rev().cloned().collect()
            };
            assert_eq!(
                got, expected,
                "multi-RANGE GSI {shape} forward={forward}: rows"
            );
        }
    }
}

/// The pages of an LSI Query in both directions: ordered by the index sort
/// key, then the base sort key, with each page resuming after the last row
/// of the one before, including inside a group of equal index sort keys.
#[tokio::test]
async fn lsi_query_pages_follow_the_index_order_in_both_directions() {
    let engine = engine().await;
    let base = table(&engine).await;
    let (lsi, _, _) = index_query_target(&engine, &base, "lsi1").await;
    let (cond, maps) = partition("pk", "p");
    let mut want: Vec<(String, String)> = (0..300)
        .map(|i| (format!("l{:02}", i % 30), format!("s{i:04}")))
        .collect();
    want.sort();
    for forward in [true, false] {
        let mut got = Vec::new();
        let mut start: Option<Item> = None;
        loop {
            let (items, last) = engine
                .query_impl(
                    &lsi,
                    &cond,
                    &maps,
                    forward,
                    Some(7),
                    start.as_ref(),
                    Some("lsi1"),
                )
                .await
                .expect("query a page");
            let text = |item: &Item, attr: &str| match item.get(attr) {
                Some(AttributeValue::S(v)) => v.clone(),
                other => panic!("{attr}: {other:?}"),
            };
            got.extend(items.iter().map(|i| (text(i, "lsk"), text(i, "sk"))));
            if last.is_none() {
                break;
            }
            // The engine adds the base sort key to an index page's key.
            let tail = items.last().expect("a page with a key has items");
            start = Some(s_key(&[
                ("pk", "p"),
                ("lsk", &text(tail, "lsk")),
                ("sk", &text(tail, "sk")),
            ]));
        }
        let mut expected = want.clone();
        if !forward {
            expected.reverse();
        }
        assert_eq!(got, expected, "forward={forward}");
    }
}
