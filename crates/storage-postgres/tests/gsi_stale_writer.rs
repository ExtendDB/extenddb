// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! A write that read the catalog before a GSI existed and wrote its item after
//! the GSI's backfill had passed it.
//!
//! The catalog and the data tables are separate databases, so nothing in the
//! write's transaction can tell it about the new index. The write notices that it
//! was slow and queues a repair. This file uses a test hook that makes every write
//! pause after its catalog read, which is process-wide, so it is a test binary of
//! its own.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_gsis_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType, TableKeyInfo, UpdateTableInput,
};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_gsi_online_build";

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
    let db_name = format!("eddb_gsis_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    // One scratch database stands in for both the catalog and the data database.
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
    .expect("open a PostgresEngine on the scratch database")
    .start_gsi_workers();
    Scratch {
        engine,
        db,
        admin,
        db_name,
    }
}

async fn create_table(s: &Scratch) -> TableKeyInfo {
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: TABLE.to_owned(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".to_owned(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_owned(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".to_owned(),
                        attribute_type: ScalarAttributeType::N,
                    },
                ],
                billing_mode: Some(BillingMode::PayPerRequest),
                ..Default::default()
            },
        )
        .await
        .expect("create the table");
    s.engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("read the key info")
}

/// Add GSI `gidx` keyed on `g` (S) and, when `range`, `r` (N).
async fn add_gsi(s: &Scratch, range: bool) {
    let mut defs = vec![serde_json::json!({"AttributeName": "g", "AttributeType": "S"})];
    let mut schema = vec![serde_json::json!({"AttributeName": "g", "KeyType": "HASH"})];
    if range {
        defs.push(serde_json::json!({"AttributeName": "r", "AttributeType": "N"}));
        schema.push(serde_json::json!({"AttributeName": "r", "KeyType": "RANGE"}));
    }
    let input: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": TABLE,
        "AttributeDefinitions": defs,
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "gidx",
            "KeySchema": schema,
            "Projection": {"ProjectionType": "ALL"},
        }}],
    }))
    .expect("build the UpdateTable input");
    s.engine
        .update_table(ACCOUNT, input)
        .await
        .expect("add the GSI");
}

fn item(n: u32, g: Option<&str>, r: Option<u32>) -> Item {
    let mut it = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(format!("p{}", n % 5))),
        ("sk".to_owned(), AttributeValue::N(n.to_string())),
    ]);
    if let Some(g) = g {
        it.insert("g".to_owned(), AttributeValue::S(g.to_owned()));
    }
    if let Some(r) = r {
        it.insert("r".to_owned(), AttributeValue::N(r.to_string()));
    }
    it
}

async fn put(s: &Scratch, key_info: &TableKeyInfo, it: Item) {
    s.engine
        .put_item(key_info, it, false, None, &ExpressionMaps::default(), None)
        .await
        .expect("put");
}

async fn ids(db: &PgPool) -> (String, String, String) {
    sqlx::query_as(
        "SELECT t.table_id, i.index_id, i.index_status FROM tables t \
         JOIN indexes i ON i.table_id = t.table_id \
         WHERE t.table_name = $1 AND i.index_name = 'gidx'",
    )
    .bind(TABLE)
    .fetch_one(db)
    .await
    .expect("look up the ids")
}

/// `(base_pk, base_sk, g)` for every row of the index.
async fn index_rows(db: &PgPool, index_id: &str) -> BTreeSet<(String, String, String)> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(&format!(
        "SELECT base_pk, base_sk_n::text, pk FROM \"_ddb_{index_id}\""
    ))
    .fetch_all(db)
    .await
    .expect("read the index table");
    rows.into_iter().collect()
}

/// `(pk, sk, g)` for every base item carrying `g`.
async fn expected_rows(db: &PgPool, table_id: &str) -> BTreeSet<(String, String, String)> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(&format!(
        "SELECT pk, sk_n::text, item_data->'g'->>'S' FROM \"_ddb_{table_id}\" \
         WHERE item_data ? 'g'"
    ))
    .fetch_all(db)
    .await
    .expect("read the base table");
    rows.into_iter().collect()
}

async fn drive(range_cond: bool) {
    let s = scratch().await;
    let key_info = create_table(&s).await;
    for n in 0..10 {
        put(&s, &key_info, item(n, Some("before"), None)).await;
    }

    extenddb_storage_postgres::pause_writes_after_index_read_for_tests(Duration::from_millis(
        3_000,
    ));
    // Writes that read "no index" now and write after the GSI is ACTIVE: a new
    // item (an autocommit insert), an overwrite that changes the index key, and
    // a delete, the last two through a transaction because of their conditions.
    let ki = key_info.clone();
    let maps = ExpressionMaps::default();
    let cond = extenddb_core::expression::parse_condition(
        &extenddb_core::expression::tokenize("attribute_exists(pk)").expect("tokenize"),
    )
    .expect("parse");
    let writes = async {
        let a = s
            .engine
            .put_item(&ki, item(100, Some("late"), None), false, None, &maps, None);
        let b = s.engine.put_item(
            &ki,
            item(1, Some("changed"), None),
            false,
            range_cond.then_some(&cond),
            &maps,
            None,
        );
        let mut key2 = item(2, None, None);
        key2.remove("g");
        let c = s
            .engine
            .delete_item(&ki, &key2, false, range_cond.then_some(&cond), &maps, None);
        let (a, b, c) = tokio::join!(a, b, c);
        a.expect("late put");
        b.expect("late overwrite");
        c.expect("late delete");
    };
    let build = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        add_gsi(&s, false).await;
        extenddb_storage_postgres::pause_writes_after_index_read_for_tests(Duration::ZERO);
    };
    tokio::join!(writes, build);

    let (table_id, index_id, status) = ids(&s.db).await;
    assert_eq!(status, "ACTIVE");
    // Give the repair rows time to apply.
    for _ in 0..100 {
        if index_rows(&s.db, &index_id).await == expected_rows(&s.db, &table_id).await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await,
        "the writes that missed the index were repaired"
    );
    s.cleanup().await;
}

#[tokio::test]
async fn writes_that_missed_a_new_gsi_are_repaired() {
    let test = "writes_that_missed_a_new_gsi_are_repaired";
    if base_conn().is_none() {
        return skip(test);
    }
    // Unconditional writes on a table with no index take the autocommit path;
    // conditional ones take a transaction. One test for both, run in sequence,
    // because the pause is process-wide.
    drive(false).await;
    drive(true).await;
}
