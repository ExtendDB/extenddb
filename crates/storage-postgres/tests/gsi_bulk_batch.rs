// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Bulk put batches against a table that `UpdateTable` is adding a GSI to.
//!
//! A batch reads the catalog once for all its rows, so a GSI whose `CREATING`
//! row commits after that read is unknown to every row the batch writes. The
//! batch handles it the way a single write does: when it is slow enough that
//! the backfill may have passed it, it re-reads the catalog before its commit
//! and queues a repair for each row it wrote and each GSI it did not know.
//!
//! - Several bulk writers and one `PutItem` writer run while `UpdateTable` adds
//!   a GSI over a seeded table. Once the queue drains, the index must equal the
//!   projection of the base table. Ten iterations.
//! - A batch is held after its catalog read until the build has finished, so its
//!   first base-table write comes seconds after the read. This uses the
//!   process-wide write pause the stale-writer tests use, which is why every
//!   test here takes `SERIAL` first.
//! - A batch and a single put that carry the key info cached before the GSI was
//!   added are validated against the attribute definitions read with the
//!   indexes, so a wrong-typed new index key is refused.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_gsib_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the other suites use.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType, TableKeyInfo, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BatchRow, BatchRowResult, DataEngine, TableEngine};
use extenddb_storage_postgres::{
    PauseWrites, PostgresConfig, PostgresEngine, pause_writes_for_tests,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
/// Base rows seeded before the index is created.
const SEED_ROWS: u64 = 3000;
/// Rows per bulk batch in the concurrent test.
const BATCH_ROWS: u64 = 25;

/// The write pause is process-wide, so the tests in this binary run one at a
/// time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

/// A scratch database standing in for both the catalog and the data database,
/// with a 5 ms propagation delay so index maintenance goes through the queue.
async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_gsib_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    // The shipped migrations create neither row, so each is inserted.
    for (key, value) in [
        ("control_plane_delay_seconds", "0"),
        ("index_propagation_delay_ms", "5"),
    ] {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .bind(value)
        .execute(&db)
        .await
        .expect("pin a setting");
    }
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&db)
        .await
        .expect("seed the account row");
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            pool_size: 16,
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

/// A hash-key table `table`, seeded with `seed` items `k0..` that carry `g1`.
/// Returns its key info, read before any GSI exists, and its table id.
async fn seeded_table(s: &Scratch, table: &str, seed: u64) -> (TableKeyInfo, String) {
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
        .expect("create the table");
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, table)
        .await
        .expect("read the key info");
    let tid = key_info.table_id.clone();
    if seed > 0 {
        // One statement rather than thousands of writes. No index exists yet, so
        // no maintenance is due.
        sqlx::query(&format!(
            "INSERT INTO \"_ddb_{tid}\" (pk, item_data) \
             SELECT 'k'||g, jsonb_build_object( \
                 'pk', jsonb_build_object('S', 'k'||g), \
                 'g1', jsonb_build_object('S', 'g'||(g % 97)), \
                 'pad', jsonb_build_object('S', repeat('x', 200))) \
             FROM generate_series(0, {}) g",
            seed - 1
        ))
        .execute(&s.db)
        .await
        .expect("seed the base table");
    }
    (key_info, tid)
}

/// Add GSI `gsi1` on `g1` (S), keys only.
async fn add_gsi(s: &Scratch, table: &str) -> Result<(), StorageError> {
    let input: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": table,
        "AttributeDefinitions": [{"AttributeName": "g1", "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "g1", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "KEYS_ONLY"},
        }}],
    }))
    .expect("build the UpdateTable input");
    s.engine.update_table(ACCOUNT, input).await.map(|_| ())
}

fn item(pk: &str, g1: Option<AttributeValue>) -> Item {
    let mut it = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("pad".to_owned(), AttributeValue::S("y".repeat(200))),
    ]);
    if let Some(g) = g1 {
        it.insert("g1".to_owned(), g);
    }
    it
}

fn gs(v: impl Into<String>) -> Option<AttributeValue> {
    Some(AttributeValue::S(v.into()))
}

fn row(item: Item) -> BatchRow {
    BatchRow {
        item,
        condition: None,
        marker: None,
        return_old: false,
    }
}

async fn gsi_index_id(db: &PgPool, tid: &str) -> String {
    sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1 AND index_name = 'gsi1'")
        .bind(tid)
        .fetch_one(db)
        .await
        .expect("look up the index id")
}

/// `(pk, g1)` of every base row that carries `g1`: the keys-only projection the
/// index must hold.
async fn base_projection(db: &PgPool, tid: &str) -> BTreeSet<(String, String)> {
    let rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT item_data->'pk'->>'S', item_data->'g1'->>'S' FROM \"_ddb_{tid}\" \
         WHERE item_data ? 'g1'"
    ))
    .fetch_all(db)
    .await
    .expect("read the base projection");
    rows.into_iter().collect()
}

/// `(pk, g1)` of every index row.
async fn index_rows(db: &PgPool, iid: &str) -> BTreeSet<(String, String)> {
    let rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT item_data->'pk'->>'S', item_data->'g1'->>'S' FROM \"_ddb_{iid}\""
    ))
    .fetch_all(db)
    .await
    .expect("read the index rows");
    rows.into_iter().collect()
}

async fn queue_depth(db: &PgPool, tid: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM gsi_pending WHERE table_id = $1")
        .bind(tid)
        .fetch_one(db)
        .await
        .expect("count the queued rows")
}

/// Wait for the propagation queue to empty, twice in a row 50 ms apart.
async fn wait_for_drain(db: &PgPool, tid: &str) -> bool {
    for _ in 0..600 {
        if queue_depth(db, tid).await == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if queue_depth(db, tid).await == 0 {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Compare the index with the base projection once the queue has drained.
async fn index_matches(s: &Scratch, tid: &str) -> Result<usize, String> {
    if !wait_for_drain(&s.db, tid).await {
        return Err(format!(
            "propagation did not drain: queue={}",
            queue_depth(&s.db, tid).await
        ));
    }
    let iid = gsi_index_id(&s.db, tid).await;
    let base = base_projection(&s.db, tid).await;
    let index = index_rows(&s.db, &iid).await;
    if base == index {
        return Ok(base.len());
    }
    let missing: Vec<_> = base.difference(&index).take(5).collect();
    let stale: Vec<_> = index.difference(&base).take(5).collect();
    Err(format!(
        "index does not equal the base projection: base={}, index={}, missing from index={} \
         (first {missing:?}), stale in index={} (first {stale:?})",
        base.len(),
        index.len(),
        base.difference(&index).count(),
        index.difference(&base).count(),
    ))
}

/// One bulk writer: batches of `BATCH_ROWS` rows that move existing items to a
/// new index key, insert new items, and overwrite items without `g1` (which
/// removes them from the index), until `stop` is set. Keys repeat across
/// writers in different orders, so batches deadlock with each other and take
/// the retry path. Returns the number of batches written.
async fn bulk_writer(
    engine: &PostgresEngine,
    key_info: &TableKeyInfo,
    stop: &AtomicBool,
    seed: u64,
) -> Result<u64, String> {
    let mut i = seed;
    let mut batches = 0;
    while !stop.load(Ordering::Relaxed) {
        let mut rows = Vec::new();
        let mut keys = BTreeSet::new();
        for _ in 0..BATCH_ROWS {
            i += 1;
            let existing = format!("k{}", (i * 7919) % SEED_ROWS);
            let it = match i % 10 {
                0..=5 => item(&existing, gs(format!("h{}", i % 13))),
                6 | 7 => item(&format!("b{seed}_{i}"), gs(format!("g{}", i % 97))),
                _ => item(&existing, None),
            };
            let pk = it["pk"].clone();
            if keys.insert(format!("{pk:?}")) {
                rows.push(row(it));
            }
        }
        let n = rows.len();
        let results = engine
            .put_items_batch(key_info, rows, None)
            .await
            .map_err(|e| format!("bulk batch: {e}"))?;
        if results.len() != n
            || results
                .iter()
                .any(|r| !matches!(r, BatchRowResult::Written { .. }))
        {
            return Err(format!("bulk batch answered {results:?}"));
        }
        batches += 1;
    }
    Ok(batches)
}

/// One `PutItem` writer beside the bulk writers, over the same keys.
async fn single_writer(engine: &PostgresEngine, key_info: &TableKeyInfo, stop: &AtomicBool) {
    let maps = ExpressionMaps::default();
    let mut i: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        i += 1;
        let it = item(
            &format!("k{}", (i * 104_729) % SEED_ROWS),
            gs(format!("p{}", i % 7)),
        );
        engine
            .put_item(key_info, it, false, None, &maps, None, None)
            .await
            .expect("put");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_batches_during_a_gsi_build_leave_the_index_equal_to_the_table() {
    let test = "bulk_batches_during_a_gsi_build_leave_the_index_equal_to_the_table";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    let s = scratch().await;
    for iter in 0..10 {
        let table = format!("gsib_{iter}");
        let (key_info, tid) = seeded_table(&s, &table, SEED_ROWS).await;
        let stop = AtomicBool::new(false);
        let build = async {
            // Let the writers get going first, so batches straddle the catalog
            // commit.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let r = add_gsi(&s, &table).await;
            stop.store(true, Ordering::Relaxed);
            r
        };
        let writers = async {
            tokio::join!(
                bulk_writer(&s.engine, &key_info, &stop, 1),
                bulk_writer(&s.engine, &key_info, &stop, 1_000_003),
                bulk_writer(&s.engine, &key_info, &stop, 2_000_029),
                single_writer(&s.engine, &key_info, &stop),
            )
        };
        let (built, (a, b, c, ())) = tokio::join!(build, writers);
        built.unwrap_or_else(|e| panic!("iteration {iter}: UpdateTable failed: {e}"));
        let batches: u64 = [a, b, c]
            .into_iter()
            .map(|r| r.unwrap_or_else(|e| panic!("iteration {iter}: {e}")))
            .sum();
        match index_matches(&s, &tid).await {
            Ok(rows) => eprintln!("iteration {iter}: {batches} batches, {rows} index rows equal"),
            Err(e) => panic!("iteration {iter}: {e}"),
        }
    }
    s.cleanup().await;
}

/// A batch that read the catalog before the GSI existed and wrote after the
/// build had finished: the backfill passed its rows before they changed, and
/// the barrier did not wait for it because it had not yet written to the base
/// table. Only the repair it queues before its commit brings the index level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_held_past_a_gsi_build_queues_a_repair_for_every_row() {
    let test = "a_batch_held_past_a_gsi_build_queues_a_repair_for_every_row";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    let s = scratch().await;
    let table = "gsib_held";
    let (key_info, tid) = seeded_table(&s, table, 40).await;

    // Moved to a new index key, removed from the index, and new.
    let mut rows: Vec<BatchRow> = (0..10)
        .map(|n| row(item(&format!("k{n}"), gs("late"))))
        .collect();
    rows.extend((10..15).map(|n| row(item(&format!("k{n}"), None))));
    rows.extend((0..10).map(|n| row(item(&format!("new{n}"), gs("new")))));
    let n = rows.len();

    let hold = Duration::from_millis(3_000);
    pause_writes_for_tests(PauseWrites::AfterIndexRead, hold);
    let started = Instant::now();
    let batch = s.engine.put_items_batch(&key_info, rows, None);
    let build = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let r = add_gsi(&s, table).await;
        let built_after = started.elapsed();
        pause_writes_for_tests(PauseWrites::AfterIndexRead, Duration::ZERO);
        r.map(|()| built_after)
    };
    let (results, built_after) = tokio::join!(batch, build);
    let built_after = built_after.expect("add the GSI");
    assert!(
        built_after < hold,
        "the build took {built_after:?}, longer than the {hold:?} hold, so the batch did not \
         write after it"
    );
    let results = results.expect("the held batch");
    assert_eq!(results.len(), n);
    assert!(
        results
            .iter()
            .all(|r| matches!(r, BatchRowResult::Written { .. })),
        "{results:?}"
    );
    if let Err(e) = index_matches(&s, &tid).await {
        panic!("the rows the held batch wrote were not repaired: {e}");
    }
    s.cleanup().await;
}

/// A write carrying the key info cached before `UpdateTable` added a GSI is
/// checked against the definitions read with the indexes: a number where the
/// new index key is declared a string is refused, by a batch and by a put, and
/// the batch writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_with_key_info_older_than_a_new_gsi_is_validated_against_its_definitions() {
    let test = "a_write_with_key_info_older_than_a_new_gsi_is_validated_against_its_definitions";
    if base_conn().is_none() {
        return skip(test);
    }
    let _serial = SERIAL.lock().await;
    let s = scratch().await;
    let table = "gsib_types";
    let (stale, tid) = seeded_table(&s, table, 0).await;
    add_gsi(&s, table).await.expect("add the GSI");
    let wrong = || Some(AttributeValue::N("7".to_owned()));

    let batch = s
        .engine
        .put_items_batch(
            &stale,
            vec![row(item("ok", gs("fine"))), row(item("bad", wrong()))],
            None,
        )
        .await;
    assert!(
        matches!(batch, Err(StorageError::Validation(_))),
        "a batch row with a wrong-typed index key: {batch:?}"
    );
    let put = s
        .engine
        .put_item(
            &stale,
            item("bad", wrong()),
            false,
            None,
            &ExpressionMaps::default(),
            None,
            None,
        )
        .await;
    assert!(
        matches!(put, Err(StorageError::Validation(_))),
        "a put with a wrong-typed index key: {put:?}"
    );
    let written: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"_ddb_{tid}\""))
        .fetch_one(&s.db)
        .await
        .expect("count the base rows");
    assert_eq!(written, 0, "the refused batch wrote nothing");
    s.cleanup().await;
}

// The ledger and write owner cases.
#[path = "gsi_bulk_batch/ledger.rs"]
mod ledger;
