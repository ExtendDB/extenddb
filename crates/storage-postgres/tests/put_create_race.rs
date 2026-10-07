// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for a `PutItem` that races another writer to create
//! the same item.
//!
//! Amazon DynamoDB applies such puts one after another: the later put
//! overwrites the item, after its own condition is checked against it. These
//! tests hold the competing create open in an outside transaction, so the put
//! deterministically loses the insert and has to order itself after the winner.
//! The four gate tests, on hash and on hash and range tables, also delete the
//! winner before the put re-reads it, so the put retries its insert, and they
//! bound those retries. The last test runs the race on a table with an LSI and
//! a stream, and checks that both get the winner as the old image.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_putr_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::BTreeMap;
use std::time::Duration;

use extenddb_core::expression::{self, Expr, ExpressionMaps};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, LsiInput, Projection, ProjectionType, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, StreamRecord, StreamSpecification, StreamViewType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, StreamCapture, TableEngine, TransactWriteOp};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_put_create_race";

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
    let db_name = format!("eddb_putr_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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

fn s_key(name: &str, key_type: KeyType) -> (KeySchemaElement, AttributeDefinition) {
    (
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        },
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
    )
}

/// Create the test table, keyed on `pk` and, when `range` is set, also on `sk`.
async fn table(s: &Scratch, range: bool) -> TableKeyInfo {
    let mut keys = vec![s_key("pk", KeyType::Hash)];
    if range {
        keys.push(s_key("sk", KeyType::Range));
    }
    let (key_schema, attribute_definitions) = keys.into_iter().unzip();
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: TABLE.to_owned(),
                key_schema,
                attribute_definitions,
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

fn item(range: bool, v: &str) -> Item {
    let mut item = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S("c".to_owned())),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ]);
    if range {
        item.insert("sk".to_owned(), AttributeValue::S("1".to_owned()));
    }
    item
}

fn condition(text: &str) -> Expr {
    let tokens = expression::tokenize(text).expect("tokenize");
    expression::parse_condition(&tokens).expect("parse")
}

async fn data_table(db: &PgPool) -> String {
    let id: String =
        sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
            .bind(ACCOUNT)
            .bind(TABLE)
            .fetch_one(db)
            .await
            .expect("look up the table id");
    format!("\"_ddb_{id}\"")
}

/// Begin an outside transaction that has created `winner` but not committed: a
/// create in flight. Returns it and its backend pid.
async fn create_in_flight(
    db: &PgPool,
    table: &str,
    winner: &Item,
) -> (Transaction<'static, Postgres>, i32) {
    let mut creator = db.begin().await.expect("begin the outside transaction");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *creator)
        .await
        .expect("read the backend pid");
    let data = serde_json::to_value(winner).expect("serialize the winner");
    let sql = if winner.contains_key("sk") {
        format!("INSERT INTO {table} (pk, sk_s, item_data) VALUES ('c', '1', $1)")
    } else {
        format!("INSERT INTO {table} (pk, item_data) VALUES ('c', $1)")
    };
    sqlx::query(&sql)
        .bind(data)
        .execute(&mut *creator)
        .await
        .expect("insert the winner");
    (creator, pid)
}

/// Wait until a backend waits for a lock of kind `event` (`transactionid` or
/// `advisory`) that the backend `blocker` holds. Fails after 5 s, so the caller
/// can report what the put did instead.
async fn wait_until_blocked_by(db: &PgPool, blocker: i32, event: &str) -> Result<(), String> {
    for _ in 0..500 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND wait_event = $2 AND $1 = ANY(pg_blocking_pids(pid))",
        )
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
        "no backend waited on the {event} lock of backend {blocker}"
    ))
}

/// Run `put` (ReturnValues ALL_OLD) while the winner's create is in flight,
/// commit the create once the put waits on it, and return the put's result.
async fn put_after_winner(
    s: &Scratch,
    key_info: &TableKeyInfo,
    range: bool,
    condition: Option<&Expr>,
) -> Result<Option<Item>, StorageError> {
    let table = data_table(&s.db).await;
    let (creator, creator_pid) = create_in_flight(&s.db, &table, &item(range, "winner")).await;
    let maps = ExpressionMaps::default();
    let put = s
        .engine
        .put_item(key_info, item(range, "loser"), true, condition, &maps, None);
    let commit = async {
        let waited = wait_until_blocked_by(&s.db, creator_pid, "transactionid").await;
        creator.commit().await.expect("commit the create");
        waited
    };
    let (result, waited) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(put, commit) })
            .await
            .expect("the put finished");
    if let Err(e) = waited {
        panic!("{e}; the put returned {result:?}");
    }
    result
}

async fn stored_v(s: &Scratch) -> String {
    let table = data_table(&s.db).await;
    sqlx::query_scalar(&format!(
        "SELECT item_data->'v'->>'S' FROM {table} WHERE pk = 'c'"
    ))
    .fetch_one(&s.db)
    .await
    .expect("read the stored item")
}

async fn overwrites_the_winner(test: &str, range: bool) {
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = table(&s, range).await;
    let old = put_after_winner(&s, &key_info, range, None)
        .await
        .expect("the put succeeds");
    assert_eq!(
        old,
        Some(item(range, "winner")),
        "ALL_OLD returns the winner"
    );
    assert_eq!(
        stored_v(&s).await,
        "loser",
        "the later put overwrote the winner"
    );
    s.cleanup().await;
}

#[tokio::test]
async fn a_put_that_loses_the_create_race_overwrites_the_winner() {
    overwrites_the_winner(
        "a_put_that_loses_the_create_race_overwrites_the_winner",
        false,
    )
    .await;
}

#[tokio::test]
async fn a_put_that_loses_the_create_race_overwrites_the_winner_on_a_range_table() {
    overwrites_the_winner(
        "a_put_that_loses_the_create_race_overwrites_the_winner_on_a_range_table",
        true,
    )
    .await;
}

#[tokio::test]
async fn a_lost_create_race_checks_the_condition_against_the_winner() {
    // The condition holds for both the missing item and the winner, so the
    // put succeeds after the winner instead of failing on the lost insert.
    let test = "a_lost_create_race_checks_the_condition_against_the_winner";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = table(&s, false).await;
    let cond = condition("attribute_not_exists(gone)");
    let old = put_after_winner(&s, &key_info, false, Some(&cond))
        .await
        .expect("the condition holds for the winner too");
    assert_eq!(old, Some(item(false, "winner")));
    assert_eq!(stored_v(&s).await, "loser");
    s.cleanup().await;
}

#[tokio::test]
async fn a_lost_create_race_fails_a_condition_the_winner_breaks() {
    // `attribute_not_exists(pk)` holds for the missing item but not for the
    // winner: the put fails its condition and returns the winner it saw.
    let test = "a_lost_create_race_fails_a_condition_the_winner_breaks";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = table(&s, false).await;
    let cond = condition("attribute_not_exists(pk)");
    match put_after_winner(&s, &key_info, false, Some(&cond)).await {
        Err(StorageError::ConditionFailed(old)) => {
            assert_eq!(old, Some(item(false, "winner")));
        }
        other => panic!("expected ConditionFailed, got {other:?}"),
    }
    assert_eq!(stored_v(&s).await, "winner", "the failed put wrote nothing");
    s.cleanup().await;
}

// The tests below take the arm where the winner is gone again when the put
// re-reads it. A BEFORE INSERT trigger parks the put's insert on an advisory
// lock, the gate. While the insert is parked, an outside transaction commits a
// winner, and a second one, the locker, locks it FOR UPDATE. When the gate
// opens, the insert loses at once: the winner is committed, and a row lock does
// not make an insert wait. The put's locking re-read then waits on the locker,
// which deletes the winner and commits, so the re-read returns no row.

/// The advisory lock key of the gate.
const GATE: i64 = 0x5075_7452;

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

/// Make every insert into `table` pass the gate first, unless its transaction
/// sets `test.bypass_gate`. Passing takes the gate shared and releases it, so
/// an insert parks while a session holds the gate exclusively.
async fn install_gate(s: &Scratch, table: &str) {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION pass_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           IF current_setting('test.bypass_gate', true) = 'on' THEN RETURN NEW; END IF; \
           PERFORM pg_advisory_lock_shared({GATE}); \
           PERFORM pg_advisory_unlock_shared({GATE}); \
           RETURN NEW; \
         END $$; \
         CREATE TRIGGER pass_gate BEFORE INSERT ON {table} \
           FOR EACH ROW EXECUTE FUNCTION pass_gate();"
    ))
    .execute(&s.db)
    .await
    .expect("install the gate trigger");
}

/// Commit the item with `v` as its value, past the gate.
async fn commit_winner(s: &Scratch, table: &str, range: bool, v: &str) {
    let mut tx = s.db.begin().await.expect("begin the winner");
    sqlx::query("SET LOCAL test.bypass_gate = 'on'")
        .execute(&mut *tx)
        .await
        .expect("bypass the gate");
    let sql = if range {
        format!("INSERT INTO {table} (pk, sk_s, item_data) VALUES ('c', '1', $1)")
    } else {
        format!("INSERT INTO {table} (pk, item_data) VALUES ('c', $1)")
    };
    sqlx::query(&sql)
        .bind(serde_json::to_value(item(range, v)).expect("serialize the winner"))
        .execute(&mut *tx)
        .await
        .expect("insert the winner");
    tx.commit().await.expect("commit the winner");
}

/// With the put's insert parked at the gate, make it lose to a winner `v` that
/// is deleted before the put re-reads it. With `rearm`, the gate closes again
/// behind the insert, so the put's next insert parks too.
async fn lose_to_a_vanishing_winner(
    s: &Scratch,
    table: &str,
    range: bool,
    gate: &mut PgConnection,
    v: &str,
    rearm: bool,
) -> Result<(), String> {
    commit_winner(s, table, range, v).await;
    let (mut locker, locker_pid) = connect(s).await;
    sqlx::query("BEGIN")
        .execute(&mut locker)
        .await
        .expect("begin the locker");
    let locked: Option<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT item_data FROM {table} WHERE pk = 'c' FOR UPDATE"
    ))
    .fetch_optional(&mut locker)
    .await
    .expect("lock the winner");
    assert!(locked.is_some(), "the locker holds the committed winner");
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
    wait_until_blocked_by(&s.db, locker_pid, "transactionid").await?;
    sqlx::query(&format!("DELETE FROM {table} WHERE pk = 'c'"))
        .execute(&mut locker)
        .await
        .expect("delete the winner");
    sqlx::query("COMMIT")
        .execute(&mut locker)
        .await
        .expect("commit the delete");
    Ok(())
}

/// Install the gate on a fresh table, keyed on `pk` and, when `range` is set,
/// also on `sk`, and close the gate. Returns the table's key info, its data
/// table, and the session that holds the gate.
async fn gated_table(s: &Scratch, range: bool) -> (TableKeyInfo, String, PgConnection, i32) {
    let key_info = table(s, range).await;
    let data = data_table(&s.db).await;
    install_gate(s, &data).await;
    let (mut gate, gate_pid) = connect(s).await;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(GATE)
        .execute(&mut gate)
        .await
        .expect("close the gate");
    (key_info, data, gate, gate_pid)
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
    let put = s
        .engine
        .put_item(&key_info, item(range, "loser"), true, None, &maps, None);
    let driver = async {
        let steps = async {
            wait_until_blocked_by(&s.db, gate_pid, "advisory").await?;
            lose_to_a_vanishing_winner(&s, &data, range, &mut gate, "winner", false).await
        }
        .await;
        open_gate_fully(&mut gate).await;
        steps
    };
    let (result, driven) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(put, driver) })
            .await
            .expect("the put finished");
    if let Err(e) = driven {
        panic!("{e}; the put returned {result:?}");
    }
    assert_eq!(
        result.expect("the retried insert succeeds"),
        None,
        "the winner is gone, so there is no old image"
    );
    assert_eq!(stored_v(&s).await, "loser");
    s.cleanup().await;
}

#[tokio::test]
async fn a_put_whose_create_race_winner_is_deleted_creates_the_item() {
    deleted_winner_is_retried(
        "a_put_whose_create_race_winner_is_deleted_creates_the_item",
        false,
    )
    .await;
}

#[tokio::test]
async fn a_put_whose_create_race_winner_is_deleted_creates_the_item_on_a_range_table() {
    deleted_winner_is_retried(
        "a_put_whose_create_race_winner_is_deleted_creates_the_item_on_a_range_table",
        true,
    )
    .await;
}

async fn create_race_churn_gives_up(test: &str, range: bool) {
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let (key_info, data, mut gate, gate_pid) = gated_table(&s, range).await;
    let maps = ExpressionMaps::default();
    let put = s
        .engine
        .put_item(&key_info, item(range, "loser"), true, None, &maps, None);
    let driver = async {
        let steps = async {
            for k in 1..=4 {
                wait_until_blocked_by(&s.db, gate_pid, "advisory").await?;
                lose_to_a_vanishing_winner(&s, &data, range, &mut gate, &format!("w{k}"), true)
                    .await?;
            }
            // The fifth insert loses to a winner that stays: the put gives up.
            wait_until_blocked_by(&s.db, gate_pid, "advisory").await?;
            commit_winner(&s, &data, range, "w5").await;
            Ok::<(), String>(())
        }
        .await;
        open_gate_fully(&mut gate).await;
        steps
    };
    let (result, driven) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(put, driver) })
            .await
            .expect("the put finished");
    if let Err(e) = driven {
        panic!("{e}; the put returned {result:?}");
    }
    match result {
        Err(StorageError::Internal(m)) => assert!(m.contains("after 5 attempts"), "{m}"),
        other => panic!("expected Internal after 5 inserts, got {other:?}"),
    }
    assert_eq!(
        stored_v(&s).await,
        "w5",
        "the put that gave up wrote nothing"
    );
    s.cleanup().await;
}

#[tokio::test]
async fn a_put_that_keeps_losing_the_create_race_gives_up_after_five_inserts() {
    create_race_churn_gives_up(
        "a_put_that_keeps_losing_the_create_race_gives_up_after_five_inserts",
        false,
    )
    .await;
}

#[tokio::test]
async fn a_put_that_keeps_losing_the_create_race_gives_up_after_five_inserts_on_a_range_table() {
    create_race_churn_gives_up(
        "a_put_that_keeps_losing_the_create_race_gives_up_after_five_inserts_on_a_range_table",
        true,
    )
    .await;
}

const LSI_TABLE: &str = "t_put_race_lsi_stream";

/// Create a (pk, sk) table with LSI `lsi1` on `lsk` and a stream.
async fn lsi_stream_table(s: &Scratch) -> TableKeyInfo {
    // The scratch database holds the catalog and data schemas together, so it
    // has the catalog's copy of `stream_shards`. Drop its foreign key to
    // `tables`, which the data database does not have.
    sqlx::query("ALTER TABLE stream_shards DROP CONSTRAINT stream_shards_table_id_fkey")
        .execute(&s.db)
        .await
        .expect("match the data schema");
    let (pk, pk_def) = s_key("pk", KeyType::Hash);
    let (sk, sk_def) = s_key("sk", KeyType::Range);
    let (lsk, lsk_def) = s_key("lsk", KeyType::Range);
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: LSI_TABLE.to_owned(),
                key_schema: vec![pk.clone(), sk],
                attribute_definitions: vec![pk_def, sk_def, lsk_def],
                billing_mode: Some(BillingMode::PayPerRequest),
                local_secondary_indexes: Some(vec![LsiInput {
                    index_name: "lsi1".to_owned(),
                    key_schema: vec![pk, lsk],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                }]),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                ..Default::default()
            },
        )
        .await
        .expect("create the table");
    s.engine
        .table_key_info(ACCOUNT, LSI_TABLE)
        .await
        .expect("read the key info")
}

fn range_item(pk: &str, extra: &[(&str, &str)]) -> Item {
    let mut item = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("sk".to_owned(), AttributeValue::S("1".to_owned())),
    ]);
    for (name, value) in extra {
        item.insert((*name).to_owned(), AttributeValue::S((*value).to_owned()));
    }
    item
}

#[tokio::test]
async fn a_lost_create_race_gives_the_lsi_and_the_stream_the_winner() {
    // A write transaction creates `c`, with its LSI row and stream record, and
    // then waits on `d`, which an outside transaction holds. A PutItem of `c`
    // meanwhile loses the insert and waits for that create. Once it commits,
    // the put must replace the winner everywhere: one LSI row for the put's
    // `lsk`, none for the winner's, and a MODIFY record whose old image is the
    // winner.
    let test = "a_lost_create_race_gives_the_lsi_and_the_stream_the_winner";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = lsi_stream_table(&s).await;
    s.engine
        .put_item(&key_info, range_item("d", &[]), false, None, &maps, None)
        .await
        .expect("seed d");
    let table_id: String =
        sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
            .bind(ACCOUNT)
            .bind(LSI_TABLE)
            .fetch_one(&s.db)
            .await
            .expect("look up the table id");
    let index_id: String = sqlx::query_scalar(
        "SELECT index_id FROM indexes WHERE table_id = $1 AND index_name = 'lsi1'",
    )
    .bind(&table_id)
    .fetch_one(&s.db)
    .await
    .expect("look up the index id");
    let (table, lsi) = (
        format!("\"_ddb_{table_id}\""),
        format!("\"_ddb_{index_id}\""),
    );

    let mut holder = s.db.begin().await.expect("begin the outside transaction");
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .expect("read the backend pid");
    sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'd' FOR UPDATE"))
        .execute(&mut *holder)
        .await
        .expect("lock d");

    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: REGION.into(),
    };
    let winner = range_item("c", &[("lsk", "l-winner"), ("v", "winner")]);
    let loser = range_item("c", &[("lsk", "l-loser"), ("v", "loser")]);
    let new_d = range_item("d", &[("v", "creator")]);
    let stream_put = |item| TransactWriteOp::Put {
        key_info: &key_info,
        item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture.clone()),
    };
    let creator_ops = [stream_put(&winner), stream_put(&new_d)];

    let creator = s.engine.transact_write_items(&creator_ops, None);
    let put = async {
        // The creator has inserted c and waits on d.
        wait_until_blocked_by(&s.db, holder_pid, "transactionid").await?;
        Ok::<_, String>(
            s.engine
                .put_item(&key_info, loser.clone(), true, None, &maps, Some(&capture))
                .await,
        )
    };
    let release = async {
        // The put waits on the creator's insert of c before d is released.
        let blocked = async {
            for _ in 0..500 {
                let n: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
                     AND wait_event_type = 'Lock' AND wait_event = 'transactionid' \
                     AND NOT ($1 = ANY(pg_blocking_pids(pid)))",
                )
                .bind(holder_pid)
                .fetch_one(&s.db)
                .await
                .expect("read pg_stat_activity");
                if n > 0 {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err("the put never waited on the creator's insert".to_owned())
        }
        .await;
        holder.rollback().await.expect("release d");
        blocked
    };
    let (created, put_result, released) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(creator, put, release)
    })
    .await
    .expect("both writes finished");
    created.expect("the create commits");
    let put_result = put_result.unwrap_or_else(|e| panic!("{e}"));
    if let Err(e) = released {
        panic!("{e}; the put returned {put_result:?}");
    }
    assert_eq!(
        put_result.expect("the put commits after the winner"),
        Some(winner.clone()),
        "ALL_OLD returns the winner"
    );

    let lsks: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT item_data->'lsk'->>'S' FROM {lsi} WHERE pk = 'c'"
    ))
    .fetch_all(&s.db)
    .await
    .expect("read the LSI rows");
    assert_eq!(lsks, ["l-loser"], "only the put's LSI row is left");
    let v: String = sqlx::query_scalar(&format!(
        "SELECT item_data->'v'->>'S' FROM {table} WHERE pk = 'c'"
    ))
    .fetch_one(&s.db)
    .await
    .expect("read c");
    assert_eq!(v, "loser");

    let records: Vec<(serde_json::Value,)> = sqlx::query_as(
        "SELECT record_data FROM stream_records WHERE table_id = $1 ORDER BY sequence_number",
    )
    .bind(&table_id)
    .fetch_all(&s.db)
    .await
    .expect("read the stream records");
    let c_events: Vec<(String, Option<Item>)> = records
        .into_iter()
        .map(|(data,)| serde_json::from_value::<StreamRecord>(data).expect("a stream record"))
        .filter(|r| r.dynamodb.keys.get("pk") == Some(&AttributeValue::S("c".to_owned())))
        .map(|r| (format!("{:?}", r.event_name), r.dynamodb.old_image))
        .collect();
    assert_eq!(
        c_events,
        [
            ("Insert".to_owned(), None),
            ("Modify".to_owned(), Some(winner.clone()))
        ],
        "the create, then the put that replaced the winner"
    );

    s.cleanup().await;
}
