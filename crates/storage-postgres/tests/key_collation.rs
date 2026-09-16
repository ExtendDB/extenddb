// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for the collation of the PostgreSQL key columns.
//!
//! Amazon DynamoDB orders string keys by UTF-8 byte value. The query builders
//! express that with `COLLATE "C"` on every string key predicate, and the wire
//! suites check the resulting order on every backend. What only this layer can
//! check is that PostgreSQL serves those predicates from the key indexes: that
//! needs the columns themselves declared `COLLATE "C"`, which is visible in the
//! catalog and in `EXPLAIN` output, neither of which is reachable over the wire.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_coll_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::{BTreeMap, HashMap};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, GsiInput, Item,
    KeySchemaElement, KeyType, ListTablesInput, Projection, ProjectionType, ScalarAttributeType,
};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

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

fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_coll_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    for sql in [
        include_str!("../migrations/001_schema.sql"),
        include_str!("../migrations/002_vector_indexes.sql"),
        include_str!("../data_migrations/001_data_schema.sql"),
        include_str!("../data_migrations/002_gsi_pending.sql"),
        include_str!("../data_migrations/003_idempotency_account_scope.sql"),
        include_str!("../data_migrations/004_vector_index_state.sql"),
    ] {
        sqlx::raw_sql(sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&db)
        .await
        .expect("pin the control-plane delay to zero");
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'index_propagation_delay_ms'")
        .execute(&db)
        .await
        .expect("pin the propagation delay to zero");
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

/// A (pk S, sk S) table with a (gpk S, gsk S) GSI.
fn composite_input(table: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: table.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: vec![s_attr("pk"), s_attr("sk"), s_attr("gpk"), s_attr("gsk")],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "gsi1".to_owned(),
            key_schema: vec![key("gpk", KeyType::Hash), key("gsk", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        ..Default::default()
    }
}

fn hash_only_input(table: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: table.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash)],
        attribute_definitions: vec![s_attr("pk")],
        billing_mode: Some(BillingMode::PayPerRequest),
        ..Default::default()
    }
}

async fn table_id(db: &PgPool, table: &str) -> String {
    sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(table)
        .fetch_one(db)
        .await
        .expect("look up the table id")
}

async fn index_id(db: &PgPool, table_id: &str, index: &str) -> String {
    sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1 AND index_name = $2")
        .bind(table_id)
        .bind(index)
        .fetch_one(db)
        .await
        .expect("look up the index id")
}

/// Column name to collation name for every text column of a data table.
async fn text_collations(db: &PgPool, sql_table: &str) -> BTreeMap<String, String> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT a.attname, c.collname \
         FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation \
         WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped \
           AND a.atttypid = 'text'::regtype",
    )
    .bind(format!("\"_ddb_{sql_table}\""))
    .fetch_all(db)
    .await
    .expect("read the column collations")
    .into_iter()
    .collect()
}

#[tokio::test]
async fn text_key_columns_are_declared_in_byte_order_collation() {
    let test = "text_key_columns_are_declared_in_byte_order_collation";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;

    s.engine
        .create_table(ACCOUNT, composite_input("t_coll"))
        .await
        .expect("create the composite table");
    s.engine
        .create_table(ACCOUNT, hash_only_input("t_hash"))
        .await
        .expect("create the hash-only table");

    let composite = table_id(&s.db, "t_coll").await;
    let gsi = index_id(&s.db, &composite, "gsi1").await;
    let hash_only = table_id(&s.db, "t_hash").await;

    let base = text_collations(&s.db, &composite).await;
    assert_eq!(
        base.get("pk").map(String::as_str),
        Some("C"),
        "base pk: {base:?}"
    );
    assert_eq!(
        base.get("sk_s").map(String::as_str),
        Some("C"),
        "base sk_s: {base:?}"
    );

    let index = text_collations(&s.db, &gsi).await;
    for col in ["pk", "sk_s", "base_pk", "base_sk_s"] {
        assert_eq!(
            index.get(col).map(String::as_str),
            Some("C"),
            "gsi {col}: {index:?}"
        );
    }

    let hash = text_collations(&s.db, &hash_only).await;
    assert_eq!(
        hash.get("pk").map(String::as_str),
        Some("C"),
        "hash-only pk: {hash:?}"
    );

    s.cleanup().await;
}

/// The plan for a statement, as the text lines `EXPLAIN` prints.
async fn explain(db: &PgPool, sql: &str) -> Vec<String> {
    sqlx::query_scalar::<_, String>(&format!("EXPLAIN (COSTS OFF) {sql}"))
        .fetch_all(db)
        .await
        .expect("explain the statement")
}

/// Every line of `plan` that is an index condition, joined, with the filter
/// lines alongside so a failure prints both.
fn index_conds(plan: &[String]) -> String {
    plan.iter()
        .map(|l| l.trim())
        .filter(|l| l.starts_with("Index Cond") || l.starts_with("Filter"))
        .collect::<Vec<_>>()
        .join(" | ")
}

#[tokio::test]
async fn resuming_inside_a_partition_seeks_on_the_sort_key() {
    let test = "resuming_inside_a_partition_seeks_on_the_sort_key";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;

    s.engine
        .create_table(ACCOUNT, composite_input("t_seek"))
        .await
        .expect("create the table");
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, "t_seek")
        .await
        .expect("read the key info");
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());
    for i in 0..2_000 {
        let item: Item = BTreeMap::from([
            ("pk".to_owned(), AttributeValue::S("hot".to_owned())),
            ("sk".to_owned(), AttributeValue::S(format!("s{i:05}"))),
            ("gpk".to_owned(), AttributeValue::S(format!("g{}", i % 4))),
            ("gsk".to_owned(), AttributeValue::S(format!("k{i:05}"))),
        ]);
        s.engine
            .put_item(&key_info, item, false, None, &maps, None)
            .await
            .expect("put an item");
    }
    let tid = table_id(&s.db, "t_seek").await;
    let gid = index_id(&s.db, &tid, "gsi1").await;
    sqlx::query(&format!("ANALYZE \"_ddb_{tid}\""))
        .execute(&s.db)
        .await
        .expect("analyze the data table");
    sqlx::query(&format!("ANALYZE \"_ddb_{gid}\""))
        .execute(&s.db)
        .await
        .expect("analyze the index table");

    // The cursor shapes the query builders emit for string sort keys, with the
    // collation qualifiers they carry. Whether `sk_s` shows up in an Index Cond
    // or in a Filter is decided by the column's declared collation; whether the
    // GSI cursor is one seek is decided by its row-comparison form.
    let query_resume = format!(
        "SELECT item_data FROM \"_ddb_{tid}\" WHERE pk = 'hot' AND sk_s COLLATE \"C\" > 's01000' \
         ORDER BY sk_s COLLATE \"C\" LIMIT 129"
    );
    let scan_resume = format!(
        "SELECT item_data FROM \"_ddb_{tid}\" WHERE (pk, sk_s COLLATE \"C\") > ('hot', 's01000') \
         ORDER BY pk, sk_s COLLATE \"C\" LIMIT 129"
    );
    let gsi_resume = format!(
        "SELECT item_data FROM \"_ddb_{gid}\" WHERE pk = 'g1' AND \
         (sk_s COLLATE \"C\", base_pk COLLATE \"C\", base_sk_s COLLATE \"C\") > ('k01000', 'hot', 's01000') \
         ORDER BY sk_s COLLATE \"C\", base_pk COLLATE \"C\", base_sk_s COLLATE \"C\" LIMIT 129"
    );

    for (label, sql) in [
        ("query resume", &query_resume),
        ("scan resume", &scan_resume),
        ("gsi resume", &gsi_resume),
    ] {
        let plan = explain(&s.db, sql).await;
        let conds = index_conds(&plan);
        assert!(
            plan.iter()
                .any(|l| l.contains("Index Scan") || l.contains("Index Only Scan")),
            "{label}: no index scan in plan:\n{}",
            plan.join("\n")
        );
        assert!(
            plan.iter()
                .any(|l| l.trim().starts_with("Index Cond") && l.contains("sk_s")),
            "{label}: sk_s is not in an Index Cond ({conds}):\n{}",
            plan.join("\n")
        );
        assert!(
            !plan
                .iter()
                .any(|l| l.trim().starts_with("Filter") && l.contains("sk_s")),
            "{label}: sk_s is still applied as a Filter ({conds}):\n{}",
            plan.join("\n")
        );
    }

    s.cleanup().await;
}

#[tokio::test]
async fn list_tables_pages_in_byte_order_without_skipping() {
    let test = "list_tables_pages_in_byte_order_without_skipping";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;

    // `a_z` sorts before `ab` by bytes and after it under en_US.utf8. The page
    // filter and the page order must agree or `ab` is never returned.
    for name in ["lt_a_z", "lt_ab", "lt_b"] {
        s.engine
            .create_table(ACCOUNT, hash_only_input(name))
            .await
            .expect("create a table");
    }

    let mut seen = Vec::new();
    let mut start: Option<String> = None;
    loop {
        let out = s
            .engine
            .list_tables(
                ACCOUNT,
                ListTablesInput {
                    exclusive_start_table_name: start.clone(),
                    limit: Some(1),
                },
            )
            .await
            .expect("list a page");
        seen.extend(out.table_names.iter().cloned());
        match out.last_evaluated_table_name {
            Some(next) => start = Some(next),
            None => break,
        }
    }
    assert_eq!(seen, vec!["lt_a_z", "lt_ab", "lt_b"]);

    s.cleanup().await;
}
