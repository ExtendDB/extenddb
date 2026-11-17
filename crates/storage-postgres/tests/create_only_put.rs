// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Create-only puts: a `PutItem` whose condition is `attribute_not_exists` on a
//! key attribute. With nothing else to do in a transaction, the write is one
//! autocommit `INSERT ... ON CONFLICT DO NOTHING`; these tests hold it to the
//! answers of the transaction path.
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
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_create_only";
/// A table keyed on `pk` alone; the put has a separate statement for it.
const HASH_TABLE: &str = "t_create_only_hash";

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

/// The server's base URL, or `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED`
/// set to any value but `0`, a job that must reach a server fails here
/// instead.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        let required =
            std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
        assert!(
            !required,
            "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED says \
             this job must reach a PostgreSQL server"
        );
        return None;
    }
    Some(conn.trim_end_matches('/').to_owned())
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
    let db_name = format!("eddb_cput_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
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

/// A table keyed on `pk` (S) alone.
async fn create_hash_table(s: &Scratch) -> TableKeyInfo {
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: HASH_TABLE.to_owned(),
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
        .expect("create the hash table");
    s.engine
        .table_key_info(ACCOUNT, HASH_TABLE)
        .await
        .expect("read the key info")
}

/// An item keyed on `pk` and, on the range table, on `sk` 1, with `v` as its
/// value.
fn keyed(range: bool, pk: &str, v: &str) -> Item {
    let mut it = key(range, pk);
    it.insert("v".to_owned(), AttributeValue::S(v.to_owned()));
    it
}

/// The key of `keyed(range, pk, _)`.
fn key(range: bool, pk: &str) -> Item {
    let mut it = BTreeMap::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))]);
    if range {
        it.insert("sk".to_owned(), AttributeValue::N("1".to_owned()));
    }
    it
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

/// Wait for the GSI to become ACTIVE. `UpdateTable` publishes it before returning
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

fn cond(expr: &str) -> extenddb_core::expression::Expr {
    extenddb_core::expression::parse_condition(
        &extenddb_core::expression::tokenize(expr).expect("tokenize"),
    )
    .expect("parse")
}

async fn put_if(
    s: &Scratch,
    key_info: &TableKeyInfo,
    it: Item,
    expr: &str,
    maps: &ExpressionMaps,
) -> Result<Option<Item>, extenddb_storage::error::StorageError> {
    let c = cond(expr);
    s.engine
        .put_item(key_info, it, false, Some(&c), maps, None)
        .await
}

async fn stored(s: &Scratch, key_info: &TableKeyInfo, n: u32) -> Option<Item> {
    s.engine
        .get_item(key_info, &item(n, None, None))
        .await
        .expect("get")
}

/// On a table keyed on `pk` and `sk` and on one keyed on `pk` alone, which have
/// their own statements, for each form of the condition.
#[tokio::test]
async fn create_only_put_inserts_when_absent_and_reports_the_item_when_present() {
    let test = "create_only_put_inserts_when_absent_and_reports_the_item_when_present";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let mut named = ExpressionMaps::default();
    named.names.insert("k".to_owned(), "pk".to_owned());
    let forms = [
        ("attribute_not_exists(pk)", &maps),
        ("attribute_not_exists(#k)", &named),
        ("NOT attribute_exists(pk)", &maps),
        ("attribute_not_exists(sk)", &maps),
        (
            "attribute_not_exists(pk) AND attribute_not_exists(sk)",
            &maps,
        ),
        (
            "attribute_not_exists(pk) OR attribute_not_exists(sk)",
            &maps,
        ),
    ];
    let tables = [
        (create_table(&s).await, true),
        (create_hash_table(&s).await, false),
    ];
    for (key_info, range) in &tables {
        for (n, (expr, maps)) in forms.iter().enumerate() {
            if !range && expr.contains("sk") {
                continue;
            }
            let pk = format!("k{n}");
            let first = keyed(*range, &pk, "first");
            put_if(&s, key_info, first.clone(), expr, maps)
                .await
                .unwrap_or_else(|e| panic!("{expr}: first put: {e:?}"));
            match put_if(&s, key_info, keyed(*range, &pk, "second"), expr, maps).await {
                Err(extenddb_storage::error::StorageError::ConditionFailed(Some(old))) => {
                    assert_eq!(old, first, "{expr}: the failure names the stored item");
                }
                other => {
                    panic!("{expr}: second put: expected ConditionFailed(Some), got {other:?}")
                }
            }
            let stored = s
                .engine
                .get_item(key_info, &key(*range, &pk))
                .await
                .expect("get");
            assert_eq!(stored, Some(first), "{expr} (range: {range}): unchanged");
        }
    }
    s.cleanup().await;
}

/// `attribute_not_exists` on a non-key attribute is not create-only: an
/// existing item without the attribute passes and is overwritten.
#[tokio::test]
async fn not_exists_on_a_non_key_attribute_still_overwrites() {
    let test = "not_exists_on_a_non_key_attribute_still_overwrites";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    let maps = ExpressionMaps::default();
    put(&s, &key_info, item(1, None, None)).await;
    let next = item(1, Some("g"), None);
    put_if(
        &s,
        &key_info,
        next.clone(),
        "attribute_not_exists(g)",
        &maps,
    )
    .await
    .expect("passes: the stored item has no g");
    assert_eq!(stored(&s, &key_info, 1).await, Some(next));
    s.cleanup().await;
}

/// Concurrent create-only puts of one key: exactly one wins, and every loser
/// reports the winner's item.
#[tokio::test]
async fn concurrent_create_only_puts_have_one_winner() {
    let test = "concurrent_create_only_puts_have_one_winner";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    let maps = ExpressionMaps::default();
    for round in 0..20u32 {
        let n = 100 + round;
        let attempts = (0..8u32).map(|w| {
            let it = item(n, Some(&format!("w{w}")), None);
            put_if(&s, &key_info, it, "attribute_not_exists(pk)", &maps)
        });
        let results = futures::future::join_all(attempts).await;
        let winners = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(winners, 1, "round {round}: {results:?}");
        let winner = stored(&s, &key_info, n).await.expect("the winner's item");
        for r in &results {
            match r {
                Ok(_) => {}
                Err(extenddb_storage::error::StorageError::ConditionFailed(Some(old))) => {
                    assert_eq!(old, &winner, "round {round}");
                }
                Err(e) => panic!("round {round}: {e:?}"),
            }
        }
    }
    s.cleanup().await;
}

/// On a table with a GSI the put keeps the transaction path, and the index
/// gets its row.
#[tokio::test]
async fn create_only_put_on_an_indexed_table_maintains_the_index() {
    let test = "create_only_put_on_an_indexed_table_maintains_the_index";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    add_gsi(&s, false).await;
    assert_eq!(wait_active(&s.db).await, "ACTIVE");
    let maps = ExpressionMaps::default();
    for n in 0..10 {
        put_if(
            &s,
            &key_info,
            item(n, Some("g"), None),
            "attribute_not_exists(pk)",
            &maps,
        )
        .await
        .expect("create");
    }
    wait_queue_empty(&s.db).await;
    let (table_id, index_id, _) = ids(&s.db).await;
    assert_eq!(
        index_rows(&s.db, &index_id).await,
        expected_rows(&s.db, &table_id).await
    );
    s.cleanup().await;
}

// The tests below take the arm where the item is gone again when the put reads
// it after a lost insert. A BEFORE INSERT trigger parks the put's insert on an
// advisory lock, the gate, while an outside transaction commits a winner. A
// second session, the locker, then asks for the table's ACCESS EXCLUSIVE lock,
// which queues behind the parked insert. When the gate opens, the insert loses
// to the committed winner at once and ends. The locker takes the table, and the
// put's read, a plain SELECT that no row lock would delay, queues behind it. The
// locker deletes the winner and commits, so the read finds no item.

/// The advisory lock key of the gate.
const GATE: i64 = 0x4372_4f6e;

/// The key of the item the gate tests race on.
const RACE_PK: &str = "race";

/// Open a connection to the scratch database outside its pools. Returns it and
/// its backend pid.
async fn connect(s: &Scratch) -> (PgConnection, i32) {
    let base = base_conn().expect("caller checks base_conn() first");
    let mut conn = PgConnection::connect(&format!("{base}/{}", s.db_name))
        .await
        .expect("connect to the scratch database");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .expect("read the backend pid");
    (conn, pid)
}

/// Wait until `pid` waits for a lock of kind `event`. With `blocker`, wait
/// instead until any backend waits for such a lock that `blocker` holds. Fails
/// after 5 s, so the caller can report what the put did instead.
async fn wait_for_lock_wait(
    db: &PgPool,
    pid: Option<i32>,
    blocker: Option<i32>,
    event: &str,
) -> Result<(), String> {
    for _ in 0..500 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND wait_event = $3 \
             AND ($1::int IS NULL OR pid = $1) \
             AND ($2::int IS NULL OR $2 = ANY(pg_blocking_pids(pid)))",
        )
        .bind(pid)
        .bind(blocker)
        .bind(event)
        .fetch_one(db)
        .await
        .expect("read pg_stat_activity");
        if waiting > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "no wait on a {event} lock (pid {pid:?}, blocker {blocker:?})"
    ))
}

/// The data table of `table`, quoted.
async fn data_table(db: &PgPool, table: &str) -> String {
    let id: String =
        sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
            .bind(ACCOUNT)
            .bind(table)
            .fetch_one(db)
            .await
            .expect("look up the table id");
    format!("\"_ddb_{id}\"")
}

/// A table of the given shape, its data table, and the session that holds the
/// gate, closed. Every insert into the table passes the gate first, unless its
/// transaction sets `test.bypass_gate`.
async fn gated_table(s: &Scratch, range: bool) -> (TableKeyInfo, String, PgConnection, i32) {
    let (key_info, name) = if range {
        (create_table(s).await, TABLE)
    } else {
        (create_hash_table(s).await, HASH_TABLE)
    };
    let data = data_table(&s.db, name).await;
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION pass_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           IF current_setting('test.bypass_gate', true) = 'on' THEN RETURN NEW; END IF; \
           PERFORM pg_advisory_lock_shared({GATE}); \
           PERFORM pg_advisory_unlock_shared({GATE}); \
           RETURN NEW; \
         END $$; \
         CREATE TRIGGER pass_gate BEFORE INSERT ON {data} \
           FOR EACH ROW EXECUTE FUNCTION pass_gate();"
    ))
    .execute(&s.db)
    .await
    .expect("install the gate trigger");
    let (mut gate, gate_pid) = connect(s).await;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(GATE)
        .execute(&mut gate)
        .await
        .expect("close the gate");
    (key_info, data, gate, gate_pid)
}

/// With the put's insert parked at the gate, commit a winner `v`, and delete
/// it between the put's lost insert and its read. With `rearm`, the gate closes
/// again behind the insert, so the put's next insert parks too.
async fn lose_to_a_deleted_winner(
    s: &Scratch,
    data: &str,
    range: bool,
    gate: &mut PgConnection,
    v: &str,
    rearm: bool,
) -> Result<(), String> {
    let mut tx = s.db.begin().await.expect("begin the winner");
    sqlx::query("SET LOCAL test.bypass_gate = 'on'")
        .execute(&mut *tx)
        .await
        .expect("bypass the gate");
    let sql = if range {
        format!("INSERT INTO {data} (pk, sk_n, item_data) VALUES ($1, 1, $2)")
    } else {
        format!("INSERT INTO {data} (pk, item_data) VALUES ($1, $2)")
    };
    sqlx::query(&sql)
        .bind(RACE_PK)
        .bind(serde_json::to_value(keyed(range, RACE_PK, v)).expect("serialize the winner"))
        .execute(&mut *tx)
        .await
        .expect("insert the winner");
    tx.commit().await.expect("commit the winner");

    let (mut locker, locker_pid) = connect(s).await;
    sqlx::query("BEGIN")
        .execute(&mut locker)
        .await
        .expect("begin the locker");
    let lock_sql = format!("LOCK TABLE {data} IN ACCESS EXCLUSIVE MODE");
    let lock = sqlx::query(&lock_sql).execute(&mut locker);
    let open = async {
        // The locker queues behind the parked insert; then the gate opens.
        wait_for_lock_wait(&s.db, Some(locker_pid), None, "relation").await?;
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(GATE)
            .execute(&mut *gate)
            .await
            .expect("open the gate");
        if rearm {
            // Granted only after the parked insert has passed the gate.
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(GATE)
                .execute(&mut *gate)
                .await
                .expect("close the gate again");
        }
        Ok::<(), String>(())
    };
    let (lock_result, gate) = tokio::join!(lock, open);
    gate?;
    lock_result.expect("lock the table");
    // The put's read waits for the locker.
    wait_for_lock_wait(&s.db, None, Some(locker_pid), "relation").await?;
    let sql = if range {
        format!("DELETE FROM {data} WHERE pk = $1 AND sk_n = 1")
    } else {
        format!("DELETE FROM {data} WHERE pk = $1")
    };
    sqlx::query(&sql)
        .bind(RACE_PK)
        .execute(&mut locker)
        .await
        .expect("delete the winner");
    sqlx::query("COMMIT")
        .execute(&mut locker)
        .await
        .expect("commit the delete");
    Ok(())
}

/// Release every advisory lock `gate` holds. A driver that stops early calls
/// this, so that a parked insert goes on and the put can report its result.
async fn open_gate_fully(gate: &mut PgConnection) {
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(gate)
        .await
        .expect("open the gate");
}

async fn deleted_winner_is_retried(test: &str, range: bool) {
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let (key_info, data, mut gate, gate_pid) = gated_table(&s, range).await;
    let maps = ExpressionMaps::default();
    let create_only = cond("attribute_not_exists(pk)");
    let put = s.engine.put_item(
        &key_info,
        keyed(range, RACE_PK, "loser"),
        false,
        Some(&create_only),
        &maps,
        None,
    );
    let driver = async {
        let steps = async {
            wait_for_lock_wait(&s.db, None, Some(gate_pid), "advisory").await?;
            lose_to_a_deleted_winner(&s, &data, range, &mut gate, "winner", false).await
        }
        .await;
        open_gate_fully(&mut gate).await;
        steps
    };
    let (result, steps) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(put, driver) })
            .await
            .expect("the put finished");
    if let Err(e) = steps {
        panic!("{e}; the put returned {result:?}");
    }
    assert_eq!(result.expect("the retried insert succeeds"), None);
    let stored = s
        .engine
        .get_item(&key_info, &key(range, RACE_PK))
        .await
        .expect("get");
    assert_eq!(stored, Some(keyed(range, RACE_PK, "loser")));
    s.cleanup().await;
}

#[tokio::test]
async fn a_create_only_put_whose_blocking_item_is_deleted_creates_the_item() {
    deleted_winner_is_retried(
        "a_create_only_put_whose_blocking_item_is_deleted_creates_the_item",
        false,
    )
    .await;
}

#[tokio::test]
async fn a_create_only_put_whose_blocking_item_is_deleted_creates_the_item_on_a_range_table() {
    deleted_winner_is_retried(
        "a_create_only_put_whose_blocking_item_is_deleted_creates_the_item_on_a_range_table",
        true,
    )
    .await;
}

/// Five lost inserts in a row, each to an item deleted before the read: the
/// put gives up, as on the transaction path, and writes nothing.
#[tokio::test]
async fn a_create_only_put_that_keeps_losing_gives_up_after_five_inserts() {
    let test = "a_create_only_put_that_keeps_losing_gives_up_after_five_inserts";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let (key_info, data, mut gate, gate_pid) = gated_table(&s, true).await;
    let maps = ExpressionMaps::default();
    let create_only = cond("attribute_not_exists(pk)");
    let put = s.engine.put_item(
        &key_info,
        keyed(true, RACE_PK, "loser"),
        false,
        Some(&create_only),
        &maps,
        None,
    );
    let driver = async {
        let steps = async {
            for k in 1..=5 {
                wait_for_lock_wait(&s.db, None, Some(gate_pid), "advisory").await?;
                lose_to_a_deleted_winner(&s, &data, true, &mut gate, &format!("w{k}"), k < 5)
                    .await?;
            }
            Ok::<(), String>(())
        }
        .await;
        open_gate_fully(&mut gate).await;
        steps
    };
    let (result, steps) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(put, driver) })
            .await
            .expect("the put finished");
    if let Err(e) = steps {
        panic!("{e}; the put returned {result:?}");
    }
    match result {
        Err(extenddb_storage::error::StorageError::Internal(m)) => {
            assert!(m.contains("after 5 attempts"), "{m}");
        }
        other => panic!("expected Internal after 5 inserts, got {other:?}"),
    }
    let stored = s
        .engine
        .get_item(&key_info, &key(true, RACE_PK))
        .await
        .expect("get");
    assert_eq!(stored, None, "the put that gave up wrote nothing");
    s.cleanup().await;
}

/// The read after a lost insert does not wait for a write in progress on the
/// item, as the transaction path's locking read does; with
/// `create_only_puts` off, the put takes that path again.
#[tokio::test]
async fn the_switch_sends_a_create_only_put_back_to_the_transaction_path() {
    let test = "the_switch_sends_a_create_only_put_back_to_the_transaction_path";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    let maps = ExpressionMaps::default();
    let first = item(1, Some("first"), None);
    put(&s, &key_info, first.clone()).await;
    let data = data_table(&s.db, TABLE).await;

    // An outside transaction holds the item's row lock.
    let (mut locker, locker_pid) = connect(&s).await;
    sqlx::query("BEGIN")
        .execute(&mut locker)
        .await
        .expect("begin the locker");
    sqlx::query(&format!(
        "SELECT item_data FROM {data} WHERE pk = 'p1' AND sk_n = 1 FOR UPDATE"
    ))
    .execute(&mut locker)
    .await
    .expect("lock the item");

    let condition = cond("attribute_not_exists(pk)");
    let expect_failure = |r: Result<Option<Item>, extenddb_storage::error::StorageError>| match r {
        Err(extenddb_storage::error::StorageError::ConditionFailed(Some(old))) => {
            assert_eq!(old, first);
        }
        other => panic!("expected ConditionFailed(Some), got {other:?}"),
    };

    // On (the default): answered while the lock is still held.
    let on = s.engine.put_item(
        &key_info,
        item(1, Some("second"), None),
        false,
        Some(&condition),
        &maps,
        None,
    );
    expect_failure(
        tokio::time::timeout(Duration::from_secs(5), on)
            .await
            .expect("the create-only put does not wait for the row lock"),
    );

    // Off: the locking read waits for the outside transaction.
    let off_engine = s.engine.clone().with_create_only_puts(false);
    let off = off_engine.put_item(
        &key_info,
        item(1, Some("second"), None),
        false,
        Some(&condition),
        &maps,
        None,
    );
    let release = async {
        let waited = wait_for_lock_wait(&s.db, None, Some(locker_pid), "transactionid").await;
        sqlx::query("COMMIT")
            .execute(&mut locker)
            .await
            .expect("commit the locker");
        waited
    };
    let (result, waited) = tokio::join!(off, release);
    waited.unwrap_or_else(|e| panic!("{e}: the put did not wait for the row lock"));
    expect_failure(result);
    s.cleanup().await;
}

/// One measured shape of the micro-benchmark below.
struct Shape {
    name: &'static str,
    condition: Option<&'static str>,
    /// Put on keys that exist, so the condition fails.
    existing: bool,
}

const BENCH_SHAPES: [Shape; 5] = [
    Shape {
        name: "create-only, new item",
        condition: Some("attribute_not_exists(pk)"),
        existing: false,
    },
    Shape {
        name: "same answer, transaction path, new item",
        condition: Some("attribute_not_exists(pk) AND attribute_not_exists(zz)"),
        existing: false,
    },
    Shape {
        name: "create-only, item exists",
        condition: Some("attribute_not_exists(pk)"),
        existing: true,
    },
    Shape {
        name: "same answer, transaction path, item exists",
        condition: Some("attribute_not_exists(pk) AND attribute_not_exists(zz)"),
        existing: true,
    },
    Shape {
        name: "unconditional, new item (reference)",
        condition: None,
        existing: false,
    },
];

/// Micro-benchmark of the create-only put against the transaction path.
///
/// `attribute_not_exists(pk) AND attribute_not_exists(zz)` gives the same
/// answer as `attribute_not_exists(pk)` (every stored item has `pk`), but its
/// second term is not on a key attribute, so it keeps the transaction path. The
/// two conditions therefore measure the two paths in one binary, on one
/// database. Shapes run interleaved, round after round, so drift on the host
/// affects them alike.
///
/// Ignored by default: numbers from a local server are only meaningful
/// relative to each other on the same machine, so this runs by hand around a
/// change, never in CI:
///
/// ```text
/// EXTENDDB_TEST_PG_CONNECTION_STRING=postgresql://postgres:pg@127.0.0.1:5432 \
///   cargo test --release -p extenddb-storage-postgres --test create_only_put -- \
///   --ignored --test-threads=1 --nocapture bench_
/// ```
///
/// Reported for each shape: the latency of one put at a time (p50, p90, p99),
/// and the throughput with `IN_FLIGHT` puts in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "micro-benchmark; run by hand with --ignored"]
async fn bench_create_only_put_against_the_transaction_path() {
    const ROUNDS: u32 = 5;
    const SERIAL_PUTS: u32 = 400;
    const IN_FLIGHT: u32 = 8;
    const PARALLEL_PUTS: u32 = 2_000;
    const EXISTING: u32 = 1_000;

    let test = "bench_create_only_put_against_the_transaction_path";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    let maps = ExpressionMaps::default();
    let conditions: Vec<Option<extenddb_core::expression::Expr>> = BENCH_SHAPES
        .iter()
        .map(|shape| shape.condition.map(cond))
        .collect();

    // Keys that exist, for the shapes whose condition fails.
    let value = "x".repeat(200);
    let value = value.as_str();
    let existing_item = |n: u32| keyed(true, &format!("exists-{n}"), value);
    for n in 0..EXISTING {
        put(&s, &key_info, existing_item(n)).await;
    }
    let mut next_key = 0u64;
    let mut fresh_item = move || {
        next_key += 1;
        keyed(true, &format!("new-{next_key}"), value)
    };

    let mut latencies: Vec<Vec<u128>> = vec![Vec::new(); BENCH_SHAPES.len()];
    let mut throughputs: Vec<Vec<f64>> = vec![Vec::new(); BENCH_SHAPES.len()];
    for _ in 0..ROUNDS {
        for (i, shape) in BENCH_SHAPES.iter().enumerate() {
            let condition = conditions[i].as_ref();
            let pick = |fresh: &mut dyn FnMut() -> Item, n: u32| {
                if shape.existing {
                    existing_item(n % EXISTING)
                } else {
                    fresh()
                }
            };
            let check = |r: Result<Option<Item>, extenddb_storage::error::StorageError>| match (
                shape.existing,
                r,
            ) {
                (false, Ok(_))
                | (true, Err(extenddb_storage::error::StorageError::ConditionFailed(Some(_)))) => {}
                (_, other) => panic!("{}: unexpected answer {other:?}", shape.name),
            };

            for n in 0..SERIAL_PUTS {
                let it = pick(&mut fresh_item, n);
                let start = std::time::Instant::now();
                let r = s
                    .engine
                    .put_item(&key_info, it, false, condition, &maps, None)
                    .await;
                latencies[i].push(start.elapsed().as_micros());
                check(r);
            }

            let items: Vec<Item> = (0..PARALLEL_PUTS)
                .map(|n| pick(&mut fresh_item, n))
                .collect();
            let mut chunks: Vec<Vec<Item>> = (0..IN_FLIGHT).map(|_| Vec::new()).collect();
            for (n, it) in items.into_iter().enumerate() {
                chunks[n % IN_FLIGHT as usize].push(it);
            }
            let start = std::time::Instant::now();
            let workers = chunks.into_iter().map(|chunk| {
                let s = &s;
                let key_info = &key_info;
                let maps = &maps;
                async move {
                    for it in chunk {
                        check(
                            s.engine
                                .put_item(key_info, it, false, condition, maps, None)
                                .await,
                        );
                    }
                }
            });
            futures::future::join_all(workers).await;
            throughputs[i].push(f64::from(PARALLEL_PUTS) / start.elapsed().as_secs_f64());
        }
    }

    print_report(
        ROUNDS,
        SERIAL_PUTS,
        PARALLEL_PUTS,
        IN_FLIGHT,
        latencies,
        throughputs,
    );
    s.cleanup().await;
}

/// Print the micro-benchmark's table: per shape, the latency percentiles of
/// the puts made one at a time, and the median throughput of the rounds.
fn print_report(
    rounds: u32,
    serial: u32,
    parallel: u32,
    in_flight: u32,
    mut latencies: Vec<Vec<u128>>,
    mut throughputs: Vec<Vec<f64>>,
) {
    println!(
        "\n{rounds} rounds; {serial} puts one at a time and {parallel} with {in_flight} in \
         flight, per shape and round\n"
    );
    println!(
        "{:<44} {:>8} {:>8} {:>8} {:>16}",
        "shape", "p50 us", "p90 us", "p99 us", "puts/s (median)"
    );
    for (i, shape) in BENCH_SHAPES.iter().enumerate() {
        let lat = &mut latencies[i];
        lat.sort_unstable();
        let pick = |q: usize| lat[(lat.len() - 1) * q / 100];
        let tput = &mut throughputs[i];
        tput.sort_by(f64::total_cmp);
        println!(
            "{:<44} {:>8} {:>8} {:>8} {:>16.0}",
            shape.name,
            pick(50),
            pick(90),
            pick(99),
            tput[tput.len() / 2]
        );
    }
}
