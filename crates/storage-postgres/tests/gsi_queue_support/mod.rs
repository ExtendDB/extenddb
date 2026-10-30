// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Scratch database and table helpers shared by the GSI queue storage-level
//! tests (`gsi_queue_fairness.rs`, `gsi_queue_many_tables.rs`).
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind,
//! named `eddb_gsiq_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it each test reports
//! a skip and passes, the same convention the wire suites use.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, GsiInput, Item,
    KeySchemaElement, KeyType, Projection, ProjectionType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub const ACCOUNT: &str = "123456789012";
pub const REGION: &str = "us-east-1";

pub struct Scratch {
    pub engine: PostgresEngine,
    pub db: PgPool,
    pub admin: PgPool,
    pub db_name: String,
}

impl Scratch {
    pub async fn cleanup(self) {
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

pub fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Every `.sql` file under `migrations/` and `data_migrations/`, each directory
/// in filename order.
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

/// A scratch database with every shipped migration applied, the control-plane
/// delay pinned to zero, one account, and an engine with its queue workers
/// started.
pub async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_gsiq_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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

pub async fn set_delay_ms(db: &PgPool, ms: u64) {
    sqlx::query("UPDATE settings SET value = $1 WHERE key = 'index_propagation_delay_ms'")
        .bind(ms.to_string())
        .execute(db)
        .await
        .expect("set the propagation delay");
}

fn s_attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    }
}

fn hash_key(name: &str) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type: KeyType::Hash,
    }
}

/// Create `name`, keyed on `pk`, with one GSI `gidx` keyed on `g`.
pub async fn table_with_gsi(s: &Scratch, name: &str) -> TableKeyInfo {
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: name.to_owned(),
                key_schema: vec![hash_key("pk")],
                attribute_definitions: vec![s_attr("pk"), s_attr("g")],
                billing_mode: Some(BillingMode::PayPerRequest),
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "gidx".to_owned(),
                    key_schema: vec![hash_key("g")],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                }]),
                ..Default::default()
            },
        )
        .await
        .expect("create the table");
    s.engine
        .table_key_info(ACCOUNT, name)
        .await
        .expect("read the key info")
}

pub fn item(pk: &str, g: &str) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("g".to_owned(), AttributeValue::S(g.to_owned())),
    ])
}

pub async fn put(s: &Scratch, key_info: &TableKeyInfo, pk: &str, g: &str) {
    let maps = ExpressionMaps::default();
    s.engine
        .put_item(key_info, item(pk, g), false, None, &maps, None)
        .await
        .expect("put the item");
}

pub async fn pending(db: &PgPool, table_id: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM gsi_pending WHERE table_id = $1")
        .bind(table_id)
        .fetch_one(db)
        .await
        .expect("count the table's pending rows")
}

/// Pending rows over several tables at once.
pub async fn pending_in(db: &PgPool, table_ids: &[String]) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM gsi_pending WHERE table_id = ANY($1)")
        .bind(table_ids)
        .fetch_one(db)
        .await
        .expect("count the tables' pending rows")
}

pub async fn index_rows(db: &PgPool, table_id: &str) -> i64 {
    let index_id: String = sqlx::query_scalar(
        "SELECT index_id FROM indexes WHERE table_id = $1 AND index_name = 'gidx'",
    )
    .bind(table_id)
    .fetch_one(db)
    .await
    .expect("look up the index id");
    sqlx::query_scalar(&format!("SELECT count(*) FROM \"_ddb_{index_id}\""))
        .fetch_one(db)
        .await
        .expect("count the index rows")
}

/// The `(new_item, index_context)` of one row the engine enqueued for `table`,
/// read back and then removed from the queue: a payload the workers apply as they
/// would any write. The caller sets a propagation delay long enough (60 s) for the
/// row to still be there, and removes it so its `ready_at` does not floor rows
/// enqueued after it.
pub async fn template_row(
    s: &Scratch,
    table: &TableKeyInfo,
) -> (serde_json::Value, serde_json::Value) {
    put(s, table, "template", "g0").await;
    let row: (serde_json::Value, serde_json::Value) =
        sqlx::query_as("SELECT new_item, index_context FROM gsi_pending WHERE table_id = $1")
            .bind(&table.table_id)
            .fetch_one(&s.db)
            .await
            .expect("read the template row");
    sqlx::query("DELETE FROM gsi_pending WHERE table_id = $1")
        .bind(&table.table_id)
        .execute(&s.db)
        .await
        .expect("remove the template row");
    row
}

/// Put `count` rows for `table` on the queue, each a distinct `pk`
/// (`<prefix><n>`), copied from `template`, ready `ready_in` from now (zero for
/// ready at once). `partition` pins every row to one worker; `None` spreads them
/// over the four.
///
/// The base items the rows describe are written to the table as well. A
/// secondary index row's apply writes the item as the base table holds it at
/// apply time, not the image the row carries, so a row with no base item behind
/// it applies as a delete and leaves no index row.
pub async fn enqueue_rows(
    s: &Scratch,
    table: &TableKeyInfo,
    template: &(serde_json::Value, serde_json::Value),
    prefix: &str,
    count: i64,
    partition: Option<i32>,
    ready_in: Duration,
) {
    sqlx::query(&format!(
        "INSERT INTO \"_ddb_{}\" (pk, item_data) \
         SELECT $2 || g::text, jsonb_set($1::jsonb, '{{pk,S}}', to_jsonb($2 || g::text)) \
         FROM generate_series(1, $3) g",
        table.table_id
    ))
    .bind(&template.0)
    .bind(prefix)
    .bind(count)
    .execute(&s.db)
    .await
    .expect("write the base items");
    sqlx::query(
        "INSERT INTO gsi_pending \
         (table_id, worker_partition, old_item, new_item, index_context, ready_at) \
         SELECT $1, COALESCE($5, (g % 4)::int), NULL, \
                jsonb_set($2::jsonb, '{pk,S}', to_jsonb($6 || g::text)), $3::jsonb, \
                NOW() + make_interval(secs => $7) \
         FROM generate_series(1, $4) g",
    )
    .bind(&table.table_id)
    .bind(&template.0)
    .bind(&template.1)
    .bind(count)
    .bind(partition)
    .bind(prefix)
    .bind(ready_in.as_secs_f64())
    .execute(&s.db)
    .await
    .expect("enqueue the rows");
}
