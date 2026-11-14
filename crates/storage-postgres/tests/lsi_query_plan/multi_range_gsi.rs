// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The plan PostgreSQL chooses for a Query on a GSI with several RANGE
//! attributes.
//!
//! The ordering index of such an index table holds every RANGE column in
//! key-schema order, then the base key: `(pk, sk_s, sk2_s, sk3_s, base_pk,
//! base_sk_s)` for RANGE attributes `a`, `b` and `c` of type S. A Query orders
//! by those columns after `pk` and resumes with a row comparison over the same
//! columns, so a page is a walk of the index for the partition alone, for
//! conditions on a prefix of the RANGE attributes and for equalities on all of
//! them, in both directions.

use super::*;
use extenddb_core::types::GsiInput;

const MULTI: &str = "t_multi_range_plan";
const MULTI_ITEMS: usize = 3_000;
/// One (a, b, c) value per residue of the item number modulo 60 (3, 5 and 4
/// are pairwise coprime), so 50 items share each value.
const MULTI_GROUP_PAGE: usize = 10;

/// An index row in index order: `a`, `b`, `c`, then the base key.
type Row = (String, String, String, String, String);

fn row(i: usize) -> Row {
    (
        format!("a{}", i % 3),
        format!("b{}", i % 5),
        format!("c{}", i % 4),
        format!("p{}", i % 7),
        sk(i),
    )
}

/// A (pk S, sk S) table with a GSI on HASH `h` and RANGE `a`, `b`, `c`.
fn multi_range_input() -> CreateTableInput {
    CreateTableInput {
        table_name: MULTI.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: ["pk", "sk", "h", "a", "b", "c"]
            .into_iter()
            .map(s_attr)
            .collect(),
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "gsi3".to_owned(),
            key_schema: vec![
                key("h", KeyType::Hash),
                key("a", KeyType::Range),
                key("b", KeyType::Range),
                key("c", KeyType::Range),
            ],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        ..Default::default()
    }
}

/// Create the table with `MULTI_ITEMS` items in index partition `g`, spread
/// over seven base partitions. Returns the key info a Query on the GSI is
/// given, the SQL name of the GSI table and the name of its ordering index.
async fn multi_range_table(s: &Scratch) -> (TableKeyInfo, String, String) {
    s.engine
        .create_table(ACCOUNT, multi_range_input())
        .await
        .expect("create the table");
    let base = s
        .engine
        .table_key_info(ACCOUNT, MULTI)
        .await
        .expect("read the key info");
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    for i in 0..MULTI_ITEMS {
        let (a, b, c, pk, sk) = row(i);
        let item: Item = BTreeMap::from([
            ("pk".to_owned(), sv(&pk)),
            ("sk".to_owned(), sv(&sk)),
            ("h".to_owned(), sv("g")),
            ("a".to_owned(), sv(&a)),
            ("b".to_owned(), sv(&b)),
            ("c".to_owned(), sv(&c)),
        ]);
        s.engine
            .put_item(&base, item, false, None, &maps, None)
            .await
            .expect("put an item");
    }
    // A GSI is written from the `gsi_pending` queue after the base write
    // commits; the engine's workers drain it.
    let mut drained = false;
    for _ in 0..1_500 {
        let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM gsi_pending")
            .fetch_one(&s.db)
            .await
            .expect("count pending rows");
        if pending == 0 {
            drained = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(drained, "the GSI propagation queue did not drain");
    let index = s
        .engine
        .index_info(ACCOUNT, MULTI, "gsi3")
        .await
        .expect("read the index info");
    let bare_table = format!("_ddb_{}", index.index_id);
    let sql_table = format!("\"{bare_table}\"");
    sqlx::query(&format!("ANALYZE {sql_table}"))
        .execute(&s.db)
        .await
        .expect("analyze the index table");
    // PostgreSQL shortens the generated index name to 63 bytes, so the
    // ordering index is found by its columns.
    let ordering_index: String = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes WHERE tablename = $1 \
           AND strpos(indexdef, '(pk, sk_s, sk2_s, sk3_s, base_pk, base_sk_s)') > 0",
    )
    .bind(&bare_table)
    .fetch_one(&s.db)
    .await
    .expect("the GSI table has an ordering index over every RANGE column");
    let key_info = TableKeyInfo {
        key_schema: index.key_schema.clone(),
        base_key_schema: base.key_schema.clone(),
        ..base
    };
    (key_info, sql_table, ordering_index)
}

/// A key condition on the GSI with its clauses assigned to the key attributes,
/// as the engine passes it to storage.
fn classified(
    key_info: &TableKeyInfo,
    expr: &str,
    values: &[(&str, &str)],
) -> (KeyCondition, ExpressionMaps) {
    let (mut cond, maps) = condition(expr, values);
    cond.resolve_key_schema(&key_info.key_schema, &maps.names)
        .expect("a key condition the service serves");
    (cond, maps)
}

/// The page a Query with Limit `limit` returns from `rows`, which are in
/// forward index order.
fn expected_rows(rows: &[Row], forward: bool, cursor: Option<&Row>, limit: usize) -> Vec<Row> {
    let mut page: Vec<Row> = rows
        .iter()
        .filter(|r| match cursor {
            None => true,
            Some(c) if forward => *r > c,
            Some(c) => *r < c,
        })
        .cloned()
        .collect();
    if !forward {
        page.reverse();
    }
    page.truncate(limit);
    page
}

fn assert_ordering_index_walk(label: &str, statement: &str, plan: &[String], index: &str) {
    let shown = format!("{statement}\n{}", plan.join("\n"));
    eprintln!("{label}:\n{shown}\n");
    assert!(
        sort_nodes(plan).is_empty(),
        "{label}: the plan sorts the rows the index scan reads:\n{shown}"
    );
    assert!(
        plan.iter()
            .any(|l| l.contains("Index Scan") && l.contains(index)),
        "{label}: the plan does not scan the ordering index {index}:\n{shown}"
    );
    assert!(
        !plan.iter().any(|l| l.trim_start().starts_with("Filter:")),
        "{label}: the plan filters rows after the index scan instead of seeking on them:\n{shown}"
    );
}

/// One shape of key condition: its label, the condition, the rows it selects
/// in forward index order, the page Limit, and the statement's binds for a
/// first page in placeholder order.
struct Shape {
    label: &'static str,
    cond: (KeyCondition, ExpressionMaps),
    rows: Vec<Row>,
    limit: usize,
    binds: Vec<&'static str>,
}

#[tokio::test]
async fn multi_range_gsi_query_plans_as_an_index_walk_in_both_directions() {
    let test = "multi_range_gsi_query_plans_as_an_index_walk_in_both_directions";
    if base_conn().is_none() {
        return skip(test);
    }
    let mut s = scratch().await;
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
        .fetch_one(&s.db)
        .await
        .expect("read the server version");
    if version < 160_000 {
        eprintln!(
            "SKIP {test}: EXPLAIN (GENERIC_PLAN) needs PostgreSQL 16 or later, the server \
             reports server_version_num {version}."
        );
        s.cleanup().await;
        return;
    }
    s.engine = s.engine.start_gsi_workers();
    let (key_info, sql_table, ordering_index) = multi_range_table(&s).await;

    let mut order: Vec<Row> = (0..MULTI_ITEMS).map(row).collect();
    order.sort();
    let select = |keep: &dyn Fn(&Row) -> bool| -> Vec<Row> {
        order.iter().filter(|r| keep(r)).cloned().collect()
    };
    // Item 127 is (a1, b2, c3) in base partition p1: a start key inside the
    // group of 50 items that share it.
    let cursor = row(127);
    assert_eq!(
        (cursor.0.as_str(), cursor.1.as_str(), cursor.2.as_str()),
        ("a1", "b2", "c3")
    );
    let start: Item = BTreeMap::from([
        ("h".to_owned(), sv("g")),
        ("a".to_owned(), sv(&cursor.0)),
        ("b".to_owned(), sv(&cursor.1)),
        ("c".to_owned(), sv(&cursor.2)),
        ("pk".to_owned(), sv(&cursor.3)),
        ("sk".to_owned(), sv(&cursor.4)),
    ]);
    // A resumed page binds the start key's RANGE values and then its base
    // key after the key condition's values.
    let cursor_binds = [
        cursor.0.as_str(),
        cursor.1.as_str(),
        cursor.2.as_str(),
        cursor.3.as_str(),
        cursor.4.as_str(),
    ];

    let shapes = [
        Shape {
            label: "h only",
            cond: classified(&key_info, "h = :h", &[("h", "g")]),
            rows: order.clone(),
            limit: PAGE,
            binds: vec!["g"],
        },
        Shape {
            label: "a = :a",
            cond: classified(&key_info, "h = :h AND a = :a", &[("h", "g"), ("a", "a1")]),
            rows: select(&|r| r.0 == "a1"),
            limit: PAGE,
            binds: vec!["g", "a1"],
        },
        // The BETWEEN condition binds first, then the equality on `a`.
        Shape {
            label: "a = :a AND b BETWEEN",
            cond: classified(
                &key_info,
                "h = :h AND a = :a AND b BETWEEN :lo AND :hi",
                &[("h", "g"), ("a", "a1"), ("lo", "b1"), ("hi", "b3")],
            ),
            rows: select(&|r| r.0 == "a1" && r.1.as_str() >= "b1" && r.1.as_str() <= "b3"),
            limit: PAGE,
            binds: vec!["g", "b1", "b3", "a1"],
        },
        Shape {
            label: "a, b and c equal",
            cond: classified(
                &key_info,
                "h = :h AND a = :a AND b = :b AND c = :c",
                &[("h", "g"), ("a", "a1"), ("b", "b2"), ("c", "c3")],
            ),
            rows: select(&|r| r.0 == "a1" && r.1 == "b2" && r.2 == "c3"),
            limit: MULTI_GROUP_PAGE,
            binds: vec!["g", "a1", "b2", "c3"],
        },
    ];

    for shape in &shapes {
        let (cond, maps) = &shape.cond;
        for forward in [true, false] {
            for resumed in [false, true] {
                let label = format!(
                    "multi-RANGE GSI {}, {} page, forward={forward}",
                    shape.label,
                    if resumed { "resumed" } else { "first" }
                );
                let start_key = resumed.then_some(&start);
                let limit = i64::try_from(shape.limit).expect("a page limit fits in i64");
                let (items, _) = s
                    .engine
                    .query(
                        &key_info,
                        cond,
                        maps,
                        forward,
                        Some(limit),
                        start_key,
                        Some("gsi3"),
                    )
                    .await
                    .expect("query a page of the GSI");
                let text = |item: &Item, attr: &str| match item.get(attr) {
                    Some(AttributeValue::S(v)) => v.clone(),
                    other => panic!("{attr}: {other:?}"),
                };
                let got: Vec<Row> = items
                    .iter()
                    .map(|i| {
                        (
                            text(i, "a"),
                            text(i, "b"),
                            text(i, "c"),
                            text(i, "pk"),
                            text(i, "sk"),
                        )
                    })
                    .collect();
                let want = expected_rows(
                    &shape.rows,
                    forward,
                    resumed.then_some(&cursor),
                    shape.limit,
                );
                assert_eq!(got, want, "{label}: rows");
                let statement = last_statement(&s.db, &sql_table).await;
                let generic = generic_plan(&s.db, &statement).await;
                assert_ordering_index_walk(
                    &format!("{label}, generic plan"),
                    &statement,
                    &generic,
                    &ordering_index,
                );
                let mut values = shape.binds.clone();
                if resumed {
                    values.extend(cursor_binds);
                }
                let custom = custom_plan(&s.db, &statement, &values).await;
                assert_ordering_index_walk(
                    &format!("{label}, custom plan"),
                    &statement,
                    &custom,
                    &ordering_index,
                );
            }
        }
    }

    s.cleanup().await;
}
