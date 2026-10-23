// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for the plan PostgreSQL chooses for a Query on a local
//! secondary index.
//!
//! Every index table whose index has a sort key has an ordering index on
//! `(pk, sk, base_pk, base_sk)`. A Query whose `ORDER BY` is that index minus
//! the constant `pk` is a walk of the index that stops at the page limit. One
//! that skips a column of it gets a sort node over the index scan: an
//! `Incremental Sort` when the key condition leaves several index sort key
//! values, and a top-N `Sort` when `lsk = :l` leaves one. The rows come back in
//! the same order either way, so the wire suites cannot tell the two apart,
//! and these tests read the plan.
//!
//! Each test takes the statement the engine sent from `pg_stat_activity` and
//! explains that statement. It checks both plans PostgreSQL can run a prepared
//! statement with: the custom plan for the bound values, used for the first
//! executions on a connection, and the generic plan it may switch to after
//! them.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_lsip_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases and read other sessions'
//! rows in `pg_stat_activity`. Without it every test here reports a skip and
//! passes, the same convention the wire suites use. PostgreSQL added
//! `EXPLAIN (GENERIC_PLAN)` in version 16; on an older server the test reports
//! a skip as well.

use std::collections::{BTreeMap, HashMap};

use extenddb_core::expression::{ExpressionMaps, KeyCondition, parse_key_condition, tokenize};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, LsiInput, Projection, ProjectionType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_lsi_plan";
/// Items in the queried partition, and how many of them share each index
/// sort key value.
const ITEMS: usize = 3_000;
const GROUP: usize = 50;
/// The Query Limit of a page, larger than a group so a page crosses from one
/// index sort key value to the next.
const PAGE: usize = 100;
/// The Query Limit of a page whose key condition fixes one index sort key
/// value (`lsk = :l`): smaller than a group, so the page ends inside it. When
/// the Limit covers the whole group, PostgreSQL may read the group through a
/// bitmap scan and sort it, which it estimates cheaper than an ordered index
/// scan that reads the same rows.
const GROUP_PAGE: usize = 10;

struct Scratch {
    engine: PostgresEngine,
    db: PgPool,
    admin: PgPool,
    db_name: String,
}

impl Scratch {
    async fn cleanup(self) {
        let Scratch {
            engine,
            db,
            admin,
            db_name,
        } = self;
        drop(engine);
        db.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
        ))
        .execute(&admin)
        .await
        .expect("drop the scratch database");
        admin.close().await;
    }
}

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Every `.sql` file under `migrations/` and `data_migrations/`, each
/// directory in filename order, so a new migration is picked up here without
/// editing this file.
fn shipped_migrations() -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for dir in ["migrations", "data_migrations"] {
        let mut files: Vec<_> = std::fs::read_dir(root.join(dir))
            .expect("read the migrations directory")
            .map(|e| e.expect("directory entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect();
        files.sort();
        for f in files {
            out.push(std::fs::read_to_string(&f).expect("read a migration file"));
        }
    }
    out
}

fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_lsip_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");
    let url = format!("{base}/{db_name}");
    let db = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    // One scratch database stands in for both the catalog and the data
    // database, so every shipped migration file from both directories is
    // applied to it, in filename order.
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&db)
        .await
        .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&db)
        .await
        .expect("seed the account row");
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database");
    Scratch {
        engine,
        db,
        admin,
        db_name,
    }
}

fn s_attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn sv(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

/// A (pk S, sk S) table with an LSI on (pk, lsk S).
fn lsi_input() -> CreateTableInput {
    CreateTableInput {
        table_name: TABLE.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: vec![s_attr("pk"), s_attr("sk"), s_attr("lsk")],
        billing_mode: Some(BillingMode::PayPerRequest),
        local_secondary_indexes: Some(vec![LsiInput {
            index_name: "lsi1".to_owned(),
            key_schema: vec![key("pk", KeyType::Hash), key("lsk", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        }]),
        ..Default::default()
    }
}

fn lsk(i: usize) -> String {
    format!("l{:03}", i % (ITEMS / GROUP))
}

fn sk(i: usize) -> String {
    format!("s{i:05}")
}

/// Create the table and fill partition `p` with `ITEMS` items, `GROUP` per
/// index sort key value, plus a smaller partition `q`. Returns the key info a
/// Query on the LSI is given and the SQL name of the LSI table.
async fn lsi_table(s: &Scratch) -> (TableKeyInfo, String) {
    s.engine
        .create_table(ACCOUNT, lsi_input())
        .await
        .expect("create the table");
    let base = s
        .engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("read the key info");
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    for (pk, count) in [("p", ITEMS), ("q", ITEMS / 10)] {
        for i in 0..count {
            let item: Item = BTreeMap::from([
                ("pk".to_owned(), sv(pk)),
                ("sk".to_owned(), sv(&sk(i))),
                ("lsk".to_owned(), sv(&lsk(i))),
            ]);
            s.engine
                .put_item(&base, item, false, None, &maps, None)
                .await
                .expect("put an item");
        }
    }
    let index = s
        .engine
        .index_info(ACCOUNT, TABLE, "lsi1")
        .await
        .expect("read the index info");
    let sql_table = format!("\"_ddb_{}\"", index.index_id);
    sqlx::query(&format!("ANALYZE {sql_table}"))
        .execute(&s.db)
        .await
        .expect("analyze the index table");
    // The key info the engine passes for an index Query: the index key schema,
    // with the base table's key schema for the tie-breaker columns.
    let key_info = TableKeyInfo {
        key_schema: index.key_schema.clone(),
        base_key_schema: base.key_schema.clone(),
        ..base
    };
    (key_info, sql_table)
}

/// A key condition on the LSI and its value map. `values` are the
/// `:name` placeholders, given without the colon: the engine strips it before
/// building the maps.
fn condition(expr: &str, values: &[(&str, &str)]) -> (KeyCondition, ExpressionMaps) {
    let tokens = tokenize(expr).expect("tokenize the key condition");
    let cond = parse_key_condition(&tokens).expect("parse the key condition");
    let values = values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), sv(value)))
        .collect();
    (cond, ExpressionMaps::new(HashMap::new(), values))
}

/// The text of the most recent Query statement another session of this
/// database ran on `sql_table`.
async fn last_statement(db: &PgPool, sql_table: &str) -> String {
    sqlx::query_scalar(
        "SELECT query FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() \
           AND strpos(query, $1) > 0 \
         ORDER BY query_start DESC LIMIT 1",
    )
    .bind(format!("SELECT item_data FROM {sql_table}"))
    .fetch_optional(db)
    .await
    .expect("read pg_stat_activity")
    .expect("the engine's Query statement is visible in pg_stat_activity")
}

/// The plan PostgreSQL would cache for `statement` after its first executions.
/// Sent over the simple query protocol: the extended protocol would make the
/// statement's `$n` placeholders parameters of the `EXPLAIN` itself.
async fn generic_plan(db: &PgPool, statement: &str) -> Vec<String> {
    let sql = format!("EXPLAIN (GENERIC_PLAN, COSTS OFF) {statement}");
    sqlx::raw_sql(&sql)
        .fetch_all(db)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| row.get::<String, _>(0))
        .collect()
}

/// The plan for `statement` with `values` bound, as the first executions get.
async fn custom_plan(db: &PgPool, statement: &str, values: &[&str]) -> Vec<String> {
    let mut conn = db.acquire().await.expect("a connection");
    let args = values
        .iter()
        .map(|v| format!("'{}'", v.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    // Simple query protocol, for the same reason as in `generic_plan`.
    let sql = format!(
        "PREPARE lsi_page AS {statement}; \
         EXPLAIN (COSTS OFF) EXECUTE lsi_page({args}); \
         DEALLOCATE lsi_page"
    );
    sqlx::raw_sql(&sql)
        .fetch_all(&mut *conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| row.get::<String, _>(0))
        .collect()
}

/// The sort nodes of a plan: `Sort` and `Incremental Sort`.
fn sort_nodes(plan: &[String]) -> Vec<&str> {
    plan.iter()
        .map(|l| l.trim_start().trim_start_matches("->").trim())
        .filter(|node| {
            *node == "Sort" || node.starts_with("Sort  (") || node.starts_with("Incremental Sort")
        })
        .collect()
}

fn assert_index_walk(label: &str, statement: &str, plan: &[String]) {
    let shown = format!("{statement}\n{}", plan.join("\n"));
    eprintln!("{label}:\n{shown}\n");
    assert!(
        sort_nodes(plan).is_empty(),
        "{label}: the plan sorts the rows the index scan reads:\n{shown}"
    );
    assert!(
        plan.iter()
            .any(|l| l.contains("Index Scan") && l.contains("_pk_sk_s_base_pk_base_sk_s_idx")),
        "{label}: the plan does not scan the ordering index:\n{shown}"
    );
    // Every predicate, the cursor included, is an index condition. A cursor
    // the index cannot seek on comes back as a Filter under the scan, and the
    // scan then reads and discards rows before the page starts.
    assert!(
        !plan.iter().any(|l| l.trim_start().starts_with("Filter:")),
        "{label}: the plan filters rows after the index scan instead of seeking on them:\n{shown}"
    );
}

/// One Query page through the engine; returns the page's (lsk, sk) keys and
/// the statement the engine sent.
async fn page(
    s: &Scratch,
    key_info: &TableKeyInfo,
    sql_table: &str,
    (cond, maps): &(KeyCondition, ExpressionMaps),
    forward: bool,
    start: Option<&Item>,
    limit: usize,
) -> (Vec<(String, String)>, String) {
    let limit = i64::try_from(limit).expect("a page limit fits in i64");
    let (items, _) = s
        .engine
        .query(
            key_info,
            cond,
            maps,
            forward,
            Some(limit),
            start,
            Some("lsi1"),
        )
        .await
        .expect("query a page of the LSI");
    let text = |item: &Item, attr: &str| match item.get(attr) {
        Some(AttributeValue::S(v)) => v.clone(),
        other => panic!("{attr}: {other:?}"),
    };
    let keys = items
        .iter()
        .map(|i| (text(i, "lsk"), text(i, "sk")))
        .collect();
    (keys, last_statement(&s.db, sql_table).await)
}

/// The page a Query with Limit `limit` returns from `rows`, which are in
/// forward index order: the first `limit` rows past `cursor` in the Query's
/// direction, or from the first row in that direction when there is no
/// cursor.
fn expected_page(
    rows: &[(String, String)],
    forward: bool,
    cursor: Option<&(String, String)>,
    limit: usize,
) -> Vec<(String, String)> {
    let mut page: Vec<(String, String)> = rows
        .iter()
        .filter(|row| match cursor {
            None => true,
            Some(c) if forward => *row > c,
            Some(c) => *row < c,
        })
        .cloned()
        .collect();
    if !forward {
        page.reverse();
    }
    page.truncate(limit);
    page
}

/// Query one page, compare its rows with `expected`, and check that both
/// plans of the statement the engine sent walk the ordering index. `values`
/// are the statement's binds in placeholder order, for the custom plan.
#[allow(clippy::too_many_arguments)]
async fn check_page(
    s: &Scratch,
    key_info: &TableKeyInfo,
    sql_table: &str,
    label: &str,
    cond: &(KeyCondition, ExpressionMaps),
    forward: bool,
    start: Option<&Item>,
    limit: usize,
    expected: &[(String, String)],
    values: &[&str],
) {
    let (keys, statement) = page(s, key_info, sql_table, cond, forward, start, limit).await;
    assert_eq!(keys, expected, "{label}: rows");
    let generic = generic_plan(&s.db, &statement).await;
    assert_index_walk(&format!("{label}, generic plan"), &statement, &generic);
    let custom = custom_plan(&s.db, &statement, values).await;
    assert_index_walk(&format!("{label}, custom plan"), &statement, &custom);
}

#[tokio::test]
async fn lsi_query_plans_as_an_index_walk_in_both_directions() {
    let test = "lsi_query_plans_as_an_index_walk_in_both_directions";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
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
    let (key_info, sql_table) = lsi_table(&s).await;

    // Every item of partition p in index order: index sort key, then base
    // sort key.
    let mut order: Vec<(String, String)> = (0..ITEMS).map(|i| (lsk(i), sk(i))).collect();
    order.sort();

    // A start key in the middle of a group of items sharing an index sort key.
    let middle = order.len() / 2 + GROUP / 2;
    let cursor = order[middle].clone();
    let (start_lsk, start_sk) = (cursor.0.as_str(), cursor.1.as_str());
    let start: Item = BTreeMap::from([
        ("pk".to_owned(), sv("p")),
        ("lsk".to_owned(), sv(start_lsk)),
        ("sk".to_owned(), sv(start_sk)),
    ]);

    // Key conditions: the partition alone; one index sort key value, the
    // start key's group; a range of index sort key values around it.
    let whole = condition("pk = :v", &[("v", "p")]);
    let one_value = condition("pk = :v AND lsk = :l", &[("v", "p"), ("l", start_lsk)]);
    let group: Vec<(String, String)> = order
        .iter()
        .filter(|(l, _)| l == start_lsk)
        .cloned()
        .collect();
    let (low, high) = (lsk(20), lsk(40));
    assert!(low.as_str() < start_lsk && start_lsk < high.as_str());
    let range = condition(
        "pk = :v AND lsk BETWEEN :a AND :b",
        &[("v", "p"), ("a", low.as_str()), ("b", high.as_str())],
    );
    let in_range: Vec<(String, String)> = order
        .iter()
        .filter(|(l, _)| *l >= low && *l <= high)
        .cloned()
        .collect();

    for forward in [true, false] {
        check_page(
            &s,
            &key_info,
            &sql_table,
            &format!("first page, forward={forward}"),
            &whole,
            forward,
            None,
            PAGE,
            &expected_page(&order, forward, None, PAGE),
            &["p"],
        )
        .await;
        check_page(
            &s,
            &key_info,
            &sql_table,
            &format!("lsk = :l, first page, forward={forward}"),
            &one_value,
            forward,
            None,
            GROUP_PAGE,
            &expected_page(&group, forward, None, GROUP_PAGE),
            &["p", start_lsk],
        )
        .await;
    }

    for forward in [true, false] {
        // The binds the engine sends for a resumed LSI page: the partition
        // key, the key condition's sort key values, the start key's index
        // sort key, the partition key again as the base partition key, then
        // the start key's base sort key.
        let label = format!("resumed page, forward={forward}");
        let expected = expected_page(&order, forward, Some(&cursor), PAGE);
        check_page(
            &s,
            &key_info,
            &sql_table,
            &label,
            &whole,
            forward,
            Some(&start),
            PAGE,
            &expected,
            &["p", start_lsk, "p", start_sk],
        )
        .await;
        check_page(
            &s,
            &key_info,
            &sql_table,
            &format!("lsk = :l, resumed page, forward={forward}"),
            &one_value,
            forward,
            Some(&start),
            GROUP_PAGE,
            &expected_page(&group, forward, Some(&cursor), GROUP_PAGE),
            &["p", start_lsk, start_lsk, "p", start_sk],
        )
        .await;
        check_page(
            &s,
            &key_info,
            &sql_table,
            &format!("lsk BETWEEN, resumed page, forward={forward}"),
            &range,
            forward,
            Some(&start),
            PAGE,
            &expected_page(&in_range, forward, Some(&cursor), PAGE),
            &["p", low.as_str(), high.as_str(), start_lsk, "p", start_sk],
        )
        .await;

        // The statement does not read the start key's partition key: the
        // cursor binds the queried partition key as base_pk, which keeps the
        // page inside partition p, so a start key naming partition q returns
        // the same page. DynamoDB refuses such a start key with a
        // ValidationException; that check belongs to request validation.
        let mut elsewhere = start.clone();
        elsewhere.insert("pk".to_owned(), sv("q"));
        let (keys_elsewhere, _) = page(
            &s,
            &key_info,
            &sql_table,
            &whole,
            forward,
            Some(&elsewhere),
            PAGE,
        )
        .await;
        assert_eq!(
            keys_elsewhere, expected,
            "{label}: a start key naming partition q moved the page boundary"
        );
    }

    s.cleanup().await;
}
