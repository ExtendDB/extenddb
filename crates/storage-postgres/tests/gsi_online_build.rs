// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for adding a GSI to a table that is taking writes.
//!
//! The wire test (`tests/test_gsi_online_backfill.py`) shows the end result
//! under real concurrency. These pin down the parts that concurrency only hits
//! sometimes:
//!
//! - a queued index update applied after a newer one for the same item, which a
//!   second server process, or a backfill that wrote the newer state first, can
//!   both cause;
//! - a GSI left `CREATING` by a crash, which the recovery sweep has to finish.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_gsib_*`, so the state that failed can be inspected.
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

/// `(base_pk, base_sk, g, item_data)` for every row of the index: the key
/// columns and the projected item, so a row with the right key but a stale
/// projection shows up as a difference.
async fn index_rows(db: &PgPool, index_id: &str) -> BTreeSet<(String, String, String, String)> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT base_pk, base_sk_n::text, pk, item_data::text FROM \"_ddb_{index_id}\""
    ))
    .fetch_all(db)
    .await
    .expect("read the index table");
    rows.into_iter().collect()
}

/// `(pk, sk, g, item_data)` for every base item carrying `g`; the GSIs in this
/// file project ALL, so the index row's item is the base item.
async fn expected_rows(db: &PgPool, table_id: &str) -> BTreeSet<(String, String, String, String)> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT pk, sk_n::text, item_data->'g'->>'S', item_data::text FROM \"_ddb_{table_id}\" \
         WHERE item_data ? 'g'"
    ))
    .fetch_all(db)
    .await
    .expect("read the base table");
    rows.into_iter().collect()
}

/// Wait for the GSI to become ACTIVE. UpdateTable publishes it before returning
/// unless the build hit a transient failure, in which case the recovery worker
/// (not running in these tests) would; a short wait tells the two apart.
async fn wait_active(db: &PgPool) -> String {
    let mut status = String::new();
    for _ in 0..100 {
        status = ids(db).await.2;
        if status == "ACTIVE" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    status
}

async fn wait_queue_empty(db: &PgPool) {
    for _ in 0..500 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM gsi_pending")
            .fetch_one(db)
            .await
            .expect("count pending rows");
        if n == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the propagation queue did not drain");
}

/// Two queued updates for one item, applied newest first. The index must end at
/// the item's state, not at the last row applied. Replaying the older row's
/// image left that image in the index: a second row for the item, or the old
/// value in place of the new one.
#[tokio::test]
async fn queue_rows_applied_out_of_order_converge_to_the_item() {
    let test = "queue_rows_applied_out_of_order_converge_to_the_item";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    add_gsi(&s, false).await;
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .unwrap_or(key_info);
    let (table_id, index_id, _) = ids(&s.db).await;

    // Hold both rows back so their order can be chosen.
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '600000') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&s.db)
    .await
    .expect("set a long propagation delay");
    put(&s, &key_info, item(1, Some("old"), None)).await;
    put(&s, &key_info, item(1, Some("new"), None)).await;
    let pending: Vec<i64> = sqlx::query_scalar("SELECT id FROM gsi_pending ORDER BY id")
        .fetch_all(&s.db)
        .await
        .expect("read pending ids");
    assert_eq!(pending.len(), 2, "both writes queued");

    // The newer row first, as a peer process's worker that skipped the locked
    // older row would apply it.
    sqlx::query("UPDATE gsi_pending SET ready_at = NOW() WHERE id = $1")
        .bind(pending[1])
        .execute(&s.db)
        .await
        .expect("release the newer row");
    let mut left = 2;
    for _ in 0..500 {
        left = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gsi_pending")
            .fetch_one(&s.db)
            .await
            .expect("count");
        if left == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(left, 1, "the newer row applied on its own first");
    sqlx::query("UPDATE gsi_pending SET ready_at = NOW()")
        .execute(&s.db)
        .await
        .expect("release the older row");
    wait_queue_empty(&s.db).await;

    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await,
        "the index holds the item's current state only"
    );
    s.cleanup().await;
}

/// A queued update that lands after the backfill already wrote the same index
/// row. Applying the row's image with a plain INSERT failed with a duplicate key,
/// which rolled the queue row back to be retried forever and stalled every row
/// behind it in its partition.
#[tokio::test]
async fn queue_row_for_a_row_the_backfill_wrote_is_applied() {
    let test = "queue_row_for_a_row_the_backfill_wrote_is_applied";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '600000') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&s.db)
    .await
    .expect("set a long propagation delay");
    add_gsi(&s, false).await;
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .unwrap_or(key_info);
    let (table_id, index_id, _) = ids(&s.db).await;

    // Queue an update, then write its row into the index directly, as a backfill
    // reading the item after the write would.
    put(&s, &key_info, item(2, Some("v"), None)).await;
    sqlx::query(&format!(
        "INSERT INTO \"_ddb_{index_id}\" (pk, base_pk, base_sk_n, item_data) \
         SELECT item_data->'g'->>'S', pk, sk_n, item_data FROM \"_ddb_{table_id}\""
    ))
    .execute(&s.db)
    .await
    .expect("write the row the backfill would have");
    sqlx::query("UPDATE gsi_pending SET ready_at = NOW()")
        .execute(&s.db)
        .await
        .expect("release the row");
    wait_queue_empty(&s.db).await;
    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await
    );
    s.cleanup().await;
}

/// A synchronous GSI write that finds index rows for its item already there,
/// with no old item to say so. Two ways to get there: the backfill wrote the row
/// while the put waited for the item's lock (same index key; a plain INSERT
/// failed the put with a duplicate key), and a write that predated the index
/// deleted the item without removing its row (a different index key; the put
/// inserted next to it and the item was indexed twice).
#[tokio::test]
async fn sync_write_over_an_existing_index_row_succeeds() {
    let test = "sync_write_over_an_existing_index_row_succeeds";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&s.db)
    .await
    .expect("make index propagation synchronous");
    add_gsi(&s, false).await;
    let (table_id, index_id, _) = ids(&s.db).await;
    sqlx::query(&format!(
        "INSERT INTO \"_ddb_{index_id}\" (pk, base_pk, base_sk_n, item_data) \
         VALUES ('v', 'p3', 3, '{{}}'::jsonb), ('gone', 'p4', 4, '{{}}'::jsonb)"
    ))
    .execute(&s.db)
    .await
    .expect("write the rows a backfill and a stale delete would have left");
    put(&s, &key_info, item(3, Some("v"), None)).await;
    put(&s, &key_info, item(4, Some("new"), None)).await;
    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await
    );
    s.cleanup().await;
}

/// Puts, deletes and conditional puts from several writers while the GSI is
/// built, with propagation synchronous and then asynchronous.
async fn concurrent_build(delay_ms: u32) {
    let s = scratch().await;
    let key_info = create_table(&s).await;
    // Several backfill pages' worth, so pages run while writers hold their rows.
    for n in 0..1200 {
        put(
            &s,
            &key_info,
            item(n, Some(&format!("g{}", n % 4)), Some(n)),
        )
        .await;
    }
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(delay_ms.to_string())
    .execute(&s.db)
    .await
    .expect("set the propagation delay");

    let stop = std::sync::atomic::AtomicBool::new(false);
    let maps = ExpressionMaps::default();
    let exists = extenddb_core::expression::parse_condition(
        &extenddb_core::expression::tokenize("attribute_exists(pk)").expect("tokenize"),
    )
    .expect("parse");
    let writer = |seed: u32| {
        let (s, key_info, stop, maps, exists) = (&s, &key_info, &stop, &maps, &exists);
        async move {
            let mut i = 0u32;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                i += 1;
                let n = (seed * 7919 + i * 31) % 1300;
                let result = match i % 4 {
                    0 => s
                        .engine
                        .delete_item(key_info, &item(n, None, None), false, None, maps, None)
                        .await
                        .map(|_| ()),
                    1 => s
                        .engine
                        .put_item(key_info, item(n, None, Some(i)), false, None, maps, None)
                        .await
                        .map(|_| ()),
                    2 => match s
                        .engine
                        .put_item(
                            key_info,
                            item(n, Some(&format!("c{}", i % 3)), Some(i)),
                            false,
                            Some(exists),
                            maps,
                            None,
                        )
                        .await
                    {
                        Err(extenddb_storage::error::StorageError::ConditionFailed(_)) => Ok(()),
                        other => other.map(|_| ()),
                    },
                    _ => s
                        .engine
                        .put_item(
                            key_info,
                            item(n, Some(&format!("w{}", i % 5)), Some(i)),
                            false,
                            None,
                            maps,
                            None,
                        )
                        .await
                        .map(|_| ()),
                };
                result.expect("a write during the build");
            }
        }
    };
    let build = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        add_gsi(&s, false).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    };
    tokio::join!(writer(1), writer(2), writer(3), writer(4), build);

    let (table_id, index_id, _) = ids(&s.db).await;
    assert_eq!(wait_active(&s.db).await, "ACTIVE");
    wait_queue_empty(&s.db).await;
    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await,
        "delay {delay_ms} ms"
    );
    s.cleanup().await;
}

#[tokio::test]
async fn concurrent_writes_during_a_build_leave_the_index_equal_to_the_table() {
    let test = "concurrent_writes_during_a_build_leave_the_index_equal_to_the_table";
    if base_conn().is_none() {
        return skip(test);
    }
    concurrent_build(0).await;
    concurrent_build(10).await;
}

/// A write carrying the key info cached before UpdateTable added a GSI with a
/// sort key on a new attribute. The engine drops that cache only when the
/// UpdateTable returns, so writes during the build carry definitions that do not
/// know the attribute; the index row was written without its sort key column.
#[tokio::test]
async fn write_with_key_info_older_than_a_new_gsi_indexes_its_sort_key() {
    let test = "write_with_key_info_older_than_a_new_gsi_indexes_its_sort_key";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let stale = create_table(&s).await;
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&s.db)
    .await
    .expect("make index propagation synchronous");
    add_gsi(&s, true).await;
    let (_, index_id, _) = ids(&s.db).await;
    put(&s, &stale, item(9, Some("x"), Some(7))).await;
    let (rows, missing): (i64, i64) = sqlx::query_as(&format!(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE sk_n IS NULL) FROM \"_ddb_{index_id}\""
    ))
    .fetch_one(&s.db)
    .await
    .expect("count index rows");
    assert_eq!((rows, missing), (1, 0), "one index row, with its sort key");
    s.cleanup().await;
}

/// A synchronous write to a table with two synchronous GSIs, one of whose data
/// tables is gone (a GSI delete, or a failed build's removal, that the write's
/// catalog read predates). The write succeeds and the surviving index is right.
/// The two GSIs share one savepoint, so the first one's rows are rolled back
/// with the second's failure and have to be written again.
#[tokio::test]
async fn sync_write_survives_a_gsi_whose_table_is_gone() {
    let test = "sync_write_survives_a_gsi_whose_table_is_gone";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&s.db)
    .await
    .expect("make index propagation synchronous");
    add_gsi(&s, false).await;
    let (table_id, kept_id, _) = ids(&s.db).await;
    // A second GSI on the same attribute, whose data table then disappears
    // without its catalog row: what a write that read the catalog just before a
    // GSI delete committed sees.
    let input: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": TABLE,
        "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "gone",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }}],
    }))
    .expect("build the UpdateTable input");
    s.engine
        .update_table(ACCOUNT, input)
        .await
        .expect("add the second GSI");
    let gone_id: String =
        sqlx::query_scalar("SELECT index_id FROM indexes WHERE index_name = 'gone'")
            .fetch_one(&s.db)
            .await
            .expect("second index id");
    sqlx::query(&format!("DROP TABLE \"_ddb_{gone_id}\""))
        .execute(&s.db)
        .await
        .expect("drop the second index's table behind the catalog's back");

    // The catalog returns the two GSIs in no fixed order. Whichever runs first,
    // the final rows must be right; when the kept one runs first its rows are
    // rolled back with the other's failure and written again.
    put(&s, &key_info, item(1, Some("a"), None)).await;
    put(&s, &key_info, item(1, Some("b"), None)).await;
    put(&s, &key_info, item(2, Some("c"), None)).await;
    assert_eq!(
        index_rows(&s.db, &kept_id).await,
        expected_rows(&s.db, &table_id).await,
        "the surviving index holds the items' current state"
    );
    s.cleanup().await;
}

/// A GSI a crash left `CREATING`, with part of its rows written and some of them
/// stale, is finished by the recovery sweep: complete, current, and `ACTIVE`.
#[tokio::test]
async fn recovery_finishes_a_gsi_left_creating() {
    let test = "recovery_finishes_a_gsi_left_creating";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    for n in 0..40 {
        put(
            &s,
            &key_info,
            item(n, Some(&format!("g{}", n % 3)), Some(n)),
        )
        .await;
    }
    add_gsi(&s, true).await;
    let (table_id, index_id, status) = ids(&s.db).await;
    assert_eq!(status, "ACTIVE");

    // What a build that died partway leaves: CREATING, some rows missing, one
    // stale, and one for an item that no longer exists.
    sqlx::query("UPDATE indexes SET index_status = 'CREATING' WHERE index_id = $1")
        .bind(&index_id)
        .execute(&s.db)
        .await
        .expect("mark CREATING");
    let idx = format!("\"_ddb_{index_id}\"");
    let base = format!("\"_ddb_{table_id}\"");
    sqlx::query(&format!("DELETE FROM {idx} WHERE base_sk_n >= 20"))
        .execute(&s.db)
        .await
        .expect("drop half the rows");
    sqlx::query(&format!(
        "UPDATE {idx} SET pk = 'stale', item_data = '{{}}'::jsonb WHERE base_sk_n = 3"
    ))
    .execute(&s.db)
    .await
    .expect("make one row stale");
    sqlx::query(&format!("DELETE FROM {base} WHERE sk_n = 5"))
        .execute(&s.db)
        .await
        .expect("remove one base item behind the index's back");

    // The build that ran inside UpdateTable held the ownership lock on a
    // connection of its own, which the server releases when it notices that
    // connection closed. Until then the sweep skips the index, as it would for a
    // build still running, so give it a few passes.
    let mut finished = 0;
    for _ in 0..50 {
        finished = extenddb_storage_postgres::reconcile_creating_gsis(&s.engine)
            .await
            .expect("run the recovery sweep");
        if finished > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(finished, 1);
    let (_, _, status) = ids(&s.db).await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await
    );
    let range_ok: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {idx} WHERE sk_n IS NULL"))
            .fetch_one(&s.db)
            .await
            .expect("check the sort key column");
    assert_eq!(range_ok, 0, "every row carries the index sort key");
    s.cleanup().await;
}
