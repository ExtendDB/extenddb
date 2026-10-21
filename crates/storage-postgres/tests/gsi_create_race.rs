// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level test for creating a global secondary index while concurrent
//! writers change the base table.
//!
//! `UpdateTable` creates the index, seeds it from the base table, and publishes
//! it. While that runs, four tasks overwrite existing rows (moving the index
//! key), insert new rows, and delete rows. When the index is `ACTIVE` and the
//! propagation queue has drained, the index table must equal the projection of
//! the base table, row for row: no row the backfill copied and a writer then
//! changed may keep its old image, no row a writer moved may leave its old key
//! behind, and a row the scan meets twice must not fail the creation.
//!
//! The scenario runs ten times. It is written to fail before the fix: on the
//! base revision the creation publishes the index at once with no hold, so a
//! write that lands while the scan is running is dropped or races the scan, and
//! the index is left disagreeing with the base table.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_gsir_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it the test reports
//! a skip and passes, the same convention the other suites use.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGsiAction, CreateTableInput,
    GlobalSecondaryIndexUpdate, Item, KeySchemaElement, KeyType, Projection, ProjectionType,
    ScalarAttributeType, TableKeyInfo, UpdateTableInput,
};
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
/// Base rows seeded before the index is created.
const SEED_ROWS: i64 = 3000;
/// Distinct index-key values among the seeded rows.
const SEED_GROUPS: i64 = 97;

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

/// Every `.sql` file under `migrations/` and `data_migrations/`, each directory
/// in filename order, so a new migration is picked up here without editing this
/// file.
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

fn skip() {
    eprintln!(
        "SKIP gsi_create_race: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no \
         PostgreSQL to build a scratch catalog in."
    );
}

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_gsir_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    // One scratch database stands in for both the catalog and the data database,
    // so every shipped migration from both directories is applied to it.
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    // CreateTable completes inline with no control-plane worker.
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&db)
        .await
        .expect("pin the control-plane delay to zero");
    // A small, non-zero propagation delay keeps writes on the asynchronous queue,
    // which is the path the race lives on.
    sqlx::query("UPDATE settings SET value = '5' WHERE key = 'index_propagation_delay_ms'")
        .execute(&db)
        .await
        .expect("set a small propagation delay");
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

fn item_with(pk: &str, g1: &str) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("g1".to_owned(), AttributeValue::S(g1.to_owned())),
        ("pad".to_owned(), AttributeValue::S("x".repeat(300))),
    ])
}

fn key_only(pk: &str) -> Item {
    BTreeMap::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

async fn table_id(db: &PgPool, table: &str) -> String {
    sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(table)
        .fetch_one(db)
        .await
        .expect("look up the table id")
}

async fn gsi_index_id(db: &PgPool, tid: &str) -> String {
    sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1 AND index_name = 'gsi1'")
        .bind(tid)
        .fetch_one(db)
        .await
        .expect("look up the index id")
}

/// `(pk, g1)` of every base row that carries `g1`, sorted. This is the KEYS_ONLY
/// projection the index must hold.
async fn base_projection(db: &PgPool, tid: &str) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT item_data->'pk'->>'S', item_data->'g1'->>'S' FROM \"_ddb_{tid}\" \
         WHERE item_data ? 'g1'"
    ))
    .fetch_all(db)
    .await
    .expect("read the base projection");
    rows.sort();
    rows
}

/// `(pk, g1)` of every index row, sorted.
async fn index_rows(db: &PgPool, iid: &str) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT item_data->'pk'->>'S', item_data->'g1'->>'S' FROM \"_ddb_{iid}\""
    ))
    .fetch_all(db)
    .await
    .expect("read the index rows");
    rows.sort();
    rows
}

async fn queue_depth(db: &PgPool, tid: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM gsi_pending WHERE table_id = $1")
        .bind(tid)
        .fetch_one(db)
        .await
        .expect("count the queued rows")
}

async fn hold_count(db: &PgPool, tid: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM vector_index_holds WHERE table_id = $1")
        .bind(tid)
        .fetch_one(db)
        .await
        .expect("count the holds")
}

/// One writer: a mix of overwrites that move the index key, inserts of new rows,
/// and deletes, until `stop` is set. It first waits until the index's catalog
/// row is visible, so every write it makes observes the index being created and
/// maintains it through the queue. A write that ran before the catalog row was
/// visible would miss the index through the catalog read window that every write
/// path has, which is a separate matter from the creation race under test.
async fn writer(
    engine: &PostgresEngine,
    db: &PgPool,
    key_info: &TableKeyInfo,
    tid: &str,
    stop: &AtomicBool,
    seed: i64,
) {
    let maps = ExpressionMaps::default();
    // Wait for the index's catalog row to appear, or for the creation to end.
    while !stop.load(Ordering::Relaxed) && !gsi_visible(db, tid).await {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let mut i = seed;
    while !stop.load(Ordering::Relaxed) {
        let kind = i % 10;
        if kind < 6 {
            // Overwrite an existing row with a new index key (a key move).
            let pk = format!("k{}", i % SEED_ROWS);
            let item = item_with(&pk, &format!("h{}", i % 13));
            let _ = engine
                .put_item(key_info, item, false, None, &maps, None)
                .await;
        } else if kind < 8 {
            // Insert a new row, one that the scan may meet, miss, or meet twice.
            let pk = format!("ins{seed}_{i}");
            let item = item_with(&pk, &format!("g{}", i % SEED_GROUPS));
            let _ = engine
                .put_item(key_info, item, false, None, &maps, None)
                .await;
        } else {
            // Delete an existing row.
            let pk = format!("k{}", i % SEED_ROWS);
            let _ = engine
                .delete_item(key_info, &key_only(&pk), false, None, &maps, None)
                .await;
        }
        i += 1;
    }
}

/// Whether the GSI's catalog row exists yet, whatever its status.
async fn gsi_visible(db: &PgPool, tid: &str) -> bool {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM indexes WHERE table_id = $1 AND index_name = 'gsi1'",
    )
    .bind(tid)
    .fetch_one(db)
    .await
    .unwrap_or(0);
    n > 0
}

/// Wait until the propagation queue is empty and no hold is held for the table,
/// then settle once more. Returns false on timeout.
async fn wait_for_drain(db: &PgPool, tid: &str) -> bool {
    for _ in 0..600 {
        if queue_depth(db, tid).await == 0 && hold_count(db, tid).await == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if queue_depth(db, tid).await == 0 && hold_count(db, tid).await == 0 {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn run_once(s: &Scratch, iter: usize) -> Result<(), String> {
    let table = format!("gsi_race_{iter}");
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: table.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("create table: {e}"))?;
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, &table)
        .await
        .map_err(|e| format!("key info: {e}"))?;
    let tid = table_id(&s.db, &table).await;

    // Seed the base table directly, so the setup is one statement rather than
    // thousands of writes. No index exists yet, so no maintenance is due.
    sqlx::query(&format!(
        "INSERT INTO \"_ddb_{tid}\" (pk, item_data) \
         SELECT 'k'||g, jsonb_build_object( \
             'pk', jsonb_build_object('S', 'k'||g), \
             'g1', jsonb_build_object('S', 'g'||(g % {SEED_GROUPS})), \
             'pad', jsonb_build_object('S', repeat('x', 300))) \
         FROM generate_series(0, {}) g",
        SEED_ROWS - 1
    ))
    .execute(&s.db)
    .await
    .map_err(|e| format!("seed: {e}"))?;

    let stop = AtomicBool::new(false);
    let create_input = UpdateTableInput {
        table_name: table.clone(),
        billing_mode: None,
        table_throughput_mode: None,
        provisioned_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: Some(vec![GlobalSecondaryIndexUpdate {
            create: Some(CreateGsiAction {
                index_name: "gsi1".to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "g1".to_owned(),
                    key_type: KeyType::Hash,
                }],
                projection: Projection {
                    projection_type: ProjectionType::KeysOnly,
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
            }),
            update: None,
            delete: None,
        }]),
        attribute_definitions: Some(vec![AttributeDefinition {
            attribute_name: "g1".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }]),
        stream_specification: None,
        table_class: None,
        on_demand_throughput: None,
        vector_index_updates: None,
    };

    let create = async {
        let r = s.engine.update_table(ACCOUNT, create_input).await;
        stop.store(true, Ordering::Relaxed);
        r
    };
    let writers = async {
        tokio::join!(
            writer(&s.engine, &s.db, &key_info, &tid, &stop, 1),
            writer(&s.engine, &s.db, &key_info, &tid, &stop, 777),
            writer(&s.engine, &s.db, &key_info, &tid, &stop, 1_500),
            writer(&s.engine, &s.db, &key_info, &tid, &stop, 2_600),
        )
    };
    let (create_result, _) = tokio::join!(create, writers);
    create_result.map_err(|e| format!("UpdateTable creating the GSI failed: {e}"))?;

    if !wait_for_drain(&s.db, &tid).await {
        return Err(format!(
            "propagation did not drain: queue={}, holds={}",
            queue_depth(&s.db, &tid).await,
            hold_count(&s.db, &tid).await
        ));
    }

    let iid = gsi_index_id(&s.db, &tid).await;
    let base = base_projection(&s.db, &tid).await;
    let index = index_rows(&s.db, &iid).await;

    if base != index {
        use std::collections::BTreeSet;
        let b: BTreeSet<_> = base.iter().cloned().collect();
        let i: BTreeSet<_> = index.iter().cloned().collect();
        let missing = b.difference(&i).count();
        let stale = i.difference(&b).count();
        return Err(format!(
            "index does not equal the base projection: base={}, index={}, missing from index={}, \
             stale in index={}",
            base.len(),
            index.len(),
            missing,
            stale
        ));
    }
    eprintln!(
        "iteration {iter}: base={} index={} equal",
        base.len(),
        index.len()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gsi_creation_converges_under_concurrent_writers() {
    if base_conn().is_none() {
        return skip();
    }
    let s = scratch().await;
    for iter in 0..10 {
        if let Err(e) = run_once(&s, iter).await {
            panic!("iteration {iter}: {e}");
        }
    }
    s.cleanup().await;
}

/// Create a base table keyed on `pk` (HASH) with a `g1` attribute, so a GSI on
/// `g1` can be manufactured over it. Returns the table id.
async fn create_base_table(s: &Scratch, table: &str) -> String {
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: table.to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                ..Default::default()
            },
        )
        .await
        .expect("create the base table");
    table_id(&s.db, table).await
}

/// Insert a catalog row for a GSI stuck mid-creation: `index_status = 'CREATING'`
/// on `g1` (HASH), KEYS_ONLY. Returns the index id.
async fn insert_creating_gsi(db: &PgPool, tid: &str, index_name: &str) -> String {
    let iid = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO indexes \
         (table_id, index_name, index_id, index_type, key_schema, projection, index_status) \
         VALUES ($1, $2, $3, 'GSI', $4, $5, 'CREATING')",
    )
    .bind(tid)
    .bind(index_name)
    .bind(&iid)
    .bind(serde_json::json!([{"AttributeName": "g1", "KeyType": "HASH"}]))
    .bind(serde_json::json!({"ProjectionType": "KEYS_ONLY"}))
    .execute(db)
    .await
    .expect("insert a CREATING GSI catalog row");
    iid
}

/// Finding 1. The orphan-hold sweep runs against the live set of building
/// indexes. A GSI under creation holds a row in `vector_index_holds` and a
/// `CREATING` row in `indexes`, and its backfill can outlive the sweep's age
/// bound. The sweep must leave that hold in place; releasing it would let the
/// propagation queue drain onto the half-built index.
///
/// The hold here is aged past the one-minute bound by hand, so the test does not
/// wait, and the sweep is driven through the reconciler path the stuck-build
/// worker and startup both use.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creating_gsi_hold_survives_the_orphan_sweep() {
    if base_conn().is_none() {
        return skip();
    }
    let s = scratch().await;
    let tid = create_base_table(&s, "gsi_hold_sweep").await;
    let iid = insert_creating_gsi(&s.db, &tid, "gsi_slow").await;
    sqlx::query(
        "INSERT INTO vector_index_holds (table_id, index_id, created_at) \
         VALUES ($1, $2, NOW() - INTERVAL '2 minutes')",
    )
    .bind(&tid)
    .bind(&iid)
    .execute(&s.db)
    .await
    .expect("insert an aged hold for the creating GSI");

    extenddb_storage_postgres::reconcile_incomplete_vector_indexes(&s.engine)
        .await
        .expect("run the reconciler path that performs the orphan sweep");

    let holds: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM vector_index_holds WHERE table_id = $1 AND index_id = $2",
    )
    .bind(&tid)
    .bind(&iid)
    .fetch_one(&s.db)
    .await
    .expect("count the hold");
    assert_eq!(
        holds, 1,
        "the orphan sweep released a live GSI creation's hold"
    );
    s.cleanup().await;
}
