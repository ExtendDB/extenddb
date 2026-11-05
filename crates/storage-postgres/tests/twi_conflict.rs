// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for lock conflicts inside `TransactWriteItems`.
//!
//! Amazon DynamoDB cancels a write transaction that loses a conflict with a
//! `TransactionConflict` reason on the contended item. These tests force the
//! PostgreSQL side of that: a real deadlock with an outside lock holder, and
//! many transactions that take the same items in opposite request orders. They
//! also pin that a check or delete of a missing item holds its key until
//! commit, so a concurrent create orders around it instead of slipping by.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_twic_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use extenddb_core::expression::{self, Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, GsiInput, Item,
    KeySchemaElement, KeyType, LsiInput, Projection, ProjectionType,
    ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamRecord, StreamSpecification,
    StreamViewType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, IdempotencyKey, StreamCapture, TableEngine, TransactWriteOp};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_twi_conflict";

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
    let db_name = format!("eddb_twic_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    .expect("open a PostgresEngine on the scratch database");
    Scratch {
        engine,
        db,
        admin,
        db_name,
    }
}

/// Create a hash-key table holding items `a` and `b`, and return its key info.
async fn seeded_table(s: &Scratch, maps: &ExpressionMaps) -> TableKeyInfo {
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: TABLE.to_owned(),
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
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("read the key info");
    for pk in ["a", "b"] {
        s.engine
            .put_item(&key_info, item(pk, "seed"), false, None, maps, None)
            .await
            .expect("seed an item");
    }
    key_info
}

fn item(pk: &str, v: &str) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ])
}

fn put<'a>(
    key_info: &'a TableKeyInfo,
    item: &'a Item,
    maps: &'a ExpressionMaps,
) -> TransactWriteOp<'a> {
    TransactWriteOp::Put {
        key_info,
        item,
        condition: None,
        maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }
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

#[tokio::test]
async fn a_deadlock_cancels_with_transaction_conflict_on_the_waiting_item() {
    let test = "a_deadlock_cancels_with_transaction_conflict_on_the_waiting_item";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = seeded_table(&s, &maps).await;
    let table = data_table(&s.db).await;

    // An outside transaction holds `b`. The write transaction locks `a`, then
    // waits on `b`. The outside transaction then asks for `a`, which closes the
    // cycle. The write transaction's deadlock check runs first (see
    // `outside_transaction`), so PostgreSQL aborts it.
    let (mut holder, holder_pid) = outside_transaction(&s.db).await;
    sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'b' FOR UPDATE"))
        .execute(&mut *holder)
        .await
        .expect("lock b");

    let (new_a, new_b) = (item("a", "twi"), item("b", "twi"));
    // Request order is b, a: the reason must land on b's request position.
    let ops = [put(&key_info, &new_b, &maps), put(&key_info, &new_a, &maps)];
    let twi = s.engine.transact_write_items(&ops, None);
    let close_cycle = async {
        wait_for_lock_waiters(&s.db, holder_pid, 1).await;
        sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'a' FOR UPDATE"))
            .execute(&mut *holder)
            .await
            .expect("the outside transaction gets a once the write transaction aborts");
    };
    // Bounded, so a transaction that never closes the cycle fails the test
    // instead of hanging it.
    let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(twi, close_cycle)
    })
    .await
    .expect("the write transaction finished");
    holder.rollback().await.expect("release the outside locks");

    match result {
        Err(StorageError::TransactionCanceled(reasons)) => {
            let codes: Vec<&str> = reasons.iter().map(|r| r.code.as_str()).collect();
            assert_eq!(codes, ["TransactionConflict", "None"], "{reasons:?}");
            assert_eq!(
                reasons[0].message.as_deref(),
                Some("Transaction is ongoing for the item")
            );
            assert_eq!(reasons[1].message, None);
        }
        other => panic!("expected a TransactionConflict cancellation, got {other:?}"),
    }
    // The canceled transaction applied nothing.
    for pk in ["a", "b"] {
        let v: String = sqlx::query_scalar(&format!(
            "SELECT item_data->'v'->>'S' FROM {table} WHERE pk = $1"
        ))
        .bind(pk)
        .fetch_one(&s.db)
        .await
        .expect("read the item");
        assert_eq!(v, "seed", "item {pk}");
    }

    s.cleanup().await;
}

#[tokio::test]
async fn a_deadlock_after_an_empty_index_key_answers_the_empty_key() {
    // An Update member that writes an empty value to an index key from the
    // stored item records that fault once its statement is applied (an empty
    // literal would be found before any row is locked), and the member after
    // it in execution order then deadlocks. The request answers the empty
    // key's ValidationException, as it does without the conflict, and no
    // internal cancellation code reaches the caller.
    let test = "a_deadlock_after_an_empty_index_key_answers_the_empty_key";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = gsi_table(&s).await;
    let plain = ExpressionMaps::default();
    let mut seed_a = item("a", "seed");
    seed_a.insert("e".to_owned(), AttributeValue::S(String::new()));
    for seed in [seed_a, item("b", "seed")] {
        s.engine
            .put_item(&key_info, seed, false, None, &plain, None)
            .await
            .expect("seed an item");
    }
    let table = data_table(&s.db).await;

    // An outside transaction holds `b`. The ops run in key order, so the
    // Update of `a` runs first: it locks `a`, applies `SET a1 = e` and
    // records the empty key. The Put of `b` then waits on `b`, and the
    // outside transaction asks for `a`, which closes the cycle. The write
    // transaction's deadlock check runs first (see `outside_transaction`), so
    // PostgreSQL aborts it.
    let (mut holder, holder_pid) = outside_transaction(&s.db).await;
    sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'b' FOR UPDATE"))
        .execute(&mut *holder)
        .await
        .expect("lock b");

    let tokens = expression::tokenize("SET a1 = e").expect("tokenize");
    let actions = expression::parse_update(&tokens).expect("parse");
    let (key_a, new_b) = (key("a"), item("b", "twi"));
    // Request order is b, a; the empty key decides wherever it is.
    let ops = [
        put(&key_info, &new_b, &plain),
        TransactWriteOp::Update {
            key_info: &key_info,
            key: &key_a,
            actions: &actions,
            condition: None,
            maps: &plain,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
    ];
    let twi = s.engine.transact_write_items(&ops, None);
    let close_cycle = async {
        wait_for_lock_waiters(&s.db, holder_pid, 1).await;
        sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'a' FOR UPDATE"))
            .execute(&mut *holder)
            .await
            .expect("the outside transaction gets a once the write transaction aborts");
    };
    // Bounded, so a transaction that never closes the cycle fails the test
    // instead of hanging it.
    let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(twi, close_cycle)
    })
    .await
    .expect("the write transaction finished");
    holder.rollback().await.expect("release the outside locks");

    match result {
        Err(StorageError::Validation(msg)) => assert_eq!(
            msg,
            "One or more parameter values are not valid. The update expression attempted to \
             update a secondary index key to a value that is not supported. The AttributeValue \
             for a key attribute cannot contain an empty string value."
        ),
        other => panic!("expected the empty key's ValidationException, got {other:?}"),
    }
    // The canceled transaction applied nothing.
    for pk in ["a", "b"] {
        let (v, a1): (String, Option<String>) = sqlx::query_as(&format!(
            "SELECT item_data->'v'->>'S', item_data->'a1'->>'S' FROM {table} WHERE pk = $1"
        ))
        .bind(pk)
        .fetch_one(&s.db)
        .await
        .expect("read the item");
        assert_eq!(v, "seed", "item {pk}");
        assert_eq!(a1, None, "item {pk}");
    }

    s.cleanup().await;
}

#[tokio::test]
async fn opposite_request_orders_do_not_deadlock() {
    let test = "opposite_request_orders_do_not_deadlock";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = seeded_table(&s, &maps).await;

    // Eight writers, half naming the items as (a, b) and half as (b, a). Taken
    // in request order these deadlock within a few rounds.
    let writers = (0..8).map(|w| {
        let (engine, key_info, maps) = (&s.engine, &key_info, &maps);
        async move {
            for round in 0..50 {
                let tag = format!("w{w}-r{round}");
                let (new_a, new_b) = (item("a", &tag), item("b", &tag));
                let ops = if w % 2 == 0 {
                    [put(key_info, &new_a, maps), put(key_info, &new_b, maps)]
                } else {
                    [put(key_info, &new_b, maps), put(key_info, &new_a, maps)]
                };
                engine
                    .transact_write_items(&ops, None)
                    .await
                    .unwrap_or_else(|e| panic!("writer {w} round {round}: {e:?}"));
            }
        }
    });
    futures::future::join_all(writers).await;

    // The last commit is some writer's last round, and it wrote both items.
    let table = data_table(&s.db).await;
    let mut tags = Vec::new();
    for pk in ["a", "b"] {
        let v: String = sqlx::query_scalar(&format!(
            "SELECT item_data->'v'->>'S' FROM {table} WHERE pk = $1"
        ))
        .bind(pk)
        .fetch_one(&s.db)
        .await
        .expect("read the item");
        tags.push(v);
    }
    assert_eq!(tags[0], tags[1], "a and b come from different transactions");
    assert!(
        tags[0].ends_with("-r49"),
        "last commit was not a final round: {}",
        tags[0]
    );

    s.cleanup().await;
}

/// Create a hash-key table with GSIs `gi1` on `a1` and `gi2` on `a2`.
async fn gsi_table(s: &Scratch) -> TableKeyInfo {
    let s_attr = |name: &str| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    };
    let hash = |name: &str| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type: KeyType::Hash,
    };
    let gsi = |index: &str, attr: &str| GsiInput {
        index_name: index.to_owned(),
        key_schema: vec![hash(attr)],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    };
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: TABLE.to_owned(),
                key_schema: vec![hash("pk")],
                attribute_definitions: vec![s_attr("pk"), s_attr("a1"), s_attr("a2")],
                billing_mode: Some(BillingMode::PayPerRequest),
                global_secondary_indexes: Some(vec![gsi("gi1", "a1"), gsi("gi2", "a2")]),
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

/// An item whose `attr` index key is the empty string.
fn empty_key(pk: &str, attr: &str) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        (attr.to_owned(), AttributeValue::S(String::new())),
    ])
}

#[tokio::test]
async fn the_earliest_invalid_op_in_request_order_is_reported() {
    // The ops run in key order, but Amazon DynamoDB names the first invalid
    // item of the request: [z, a] names z's index, [a, z] names a's.
    let test = "the_earliest_invalid_op_in_request_order_is_reported";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = gsi_table(&s).await;
    let (z, a) = (empty_key("z", "a2"), empty_key("a", "a1"));

    for (ops, expected) in [
        (
            [put(&key_info, &z, &maps), put(&key_info, &a, &maps)],
            "IndexName: gi2, IndexKey: a2",
        ),
        (
            [put(&key_info, &a, &maps), put(&key_info, &z, &maps)],
            "IndexName: gi1, IndexKey: a1",
        ),
    ] {
        match s.engine.transact_write_items(&ops, None).await {
            Err(StorageError::Validation(msg)) => {
                assert!(msg.ends_with(expected), "{msg}");
            }
            other => panic!("expected a ValidationException naming {expected}, got {other:?}"),
        }
    }

    s.cleanup().await;
}

#[tokio::test]
async fn an_invalid_first_op_answers_without_waiting_on_later_locks() {
    // Once the earliest invalid op is known, the rest must not run: here the
    // next op's row is held by an outside transaction.
    let test = "an_invalid_first_op_answers_without_waiting_on_later_locks";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = gsi_table(&s).await;
    let b = item("b", "seed");
    s.engine
        .put_item(&key_info, b.clone(), false, None, &maps, None)
        .await
        .expect("seed b");
    let table = data_table(&s.db).await;
    let mut holder = s.db.begin().await.expect("begin the outside transaction");
    sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'b' FOR UPDATE"))
        .execute(&mut *holder)
        .await
        .expect("lock b");

    let a = empty_key("a", "a1");
    let ops = [put(&key_info, &a, &maps), put(&key_info, &b, &maps)];
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        s.engine.transact_write_items(&ops, None),
    )
    .await
    .expect("the transaction answered without waiting on b");
    holder.rollback().await.expect("release b");
    match result {
        Err(StorageError::Validation(msg)) => {
            assert!(msg.ends_with("IndexName: gi1, IndexKey: a1"), "{msg}");
        }
        other => panic!("expected a ValidationException, got {other:?}"),
    }

    s.cleanup().await;
}

#[tokio::test]
async fn an_empty_literal_first_answers_without_waiting_on_later_locks() {
    // An Update member's empty index-key literal decides the request when no
    // member before it writes an empty value from the item. With it first no
    // member runs, so the request must not wait on the next op's row, held
    // here by an outside transaction.
    let test = "an_empty_literal_first_answers_without_waiting_on_later_locks";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = gsi_table(&s).await;
    let b = item("b", "seed");
    s.engine
        .put_item(&key_info, b.clone(), false, None, &maps, None)
        .await
        .expect("seed b");
    let table = data_table(&s.db).await;
    let mut holder = s.db.begin().await.expect("begin the outside transaction");
    sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'b' FOR UPDATE"))
        .execute(&mut *holder)
        .await
        .expect("lock b");

    let empty = ExpressionMaps::new(
        std::collections::HashMap::new(),
        std::collections::HashMap::from([("e".to_owned(), AttributeValue::S(String::new()))]),
    );
    let tokens = expression::tokenize("SET a1 = :e").expect("tokenize");
    let actions = expression::parse_update(&tokens).expect("parse");
    let key_a = key("a");
    let ops = [
        TransactWriteOp::Update {
            key_info: &key_info,
            key: &key_a,
            actions: &actions,
            condition: None,
            maps: &empty,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        put(&key_info, &b, &maps),
    ];
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        s.engine.transact_write_items(&ops, None),
    )
    .await
    .expect("the transaction answered without waiting on b");
    holder.rollback().await.expect("release b");
    match result {
        Err(StorageError::Validation(msg)) => assert_eq!(
            msg,
            "One or more parameter values are not valid. The update expression attempted to \
             update a secondary index key to a value that is not supported. The AttributeValue \
             for a key attribute cannot contain an empty string value."
        ),
        other => panic!("expected the empty literal's ValidationException, got {other:?}"),
    }

    s.cleanup().await;
}

/// The condition `attribute_not_exists(pk)`.
fn not_exists() -> Expr {
    let tokens = expression::tokenize("attribute_not_exists(pk)").expect("tokenize");
    expression::parse_condition(&tokens).expect("parse")
}

fn key(pk: &str) -> Item {
    BTreeMap::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

/// Begin an outside transaction that has inserted `pk` but not committed: a
/// create in flight. Returns it and its backend pid.
async fn create_in_flight(
    db: &PgPool,
    table: &str,
    pk: &str,
) -> (Transaction<'static, Postgres>, i32) {
    let mut creator = db.begin().await.expect("begin the outside transaction");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *creator)
        .await
        .expect("read the backend pid");
    sqlx::query(&format!(
        "INSERT INTO {table} (pk, item_data) VALUES ($1, jsonb_build_object('pk', jsonb_build_object('S', $1::text)))"
    ))
    .bind(pk)
    .execute(&mut *creator)
    .await
    .expect("insert the item");
    (creator, pid)
}

/// Begin the outside transaction of a deadlock test, with its deadlock check
/// later than the write transaction's, and return it with its backend pid.
///
/// PostgreSQL aborts the transaction whose deadlock check finds the cycle, and
/// each waiter checks once, `deadlock_timeout` after it starts to wait. The
/// write transaction waits first, but with the default 1 s on both sides the
/// two checks are milliseconds apart, and under load the outside
/// transaction's can run first and abort it instead. With 5 s here, the write
/// transaction's check runs 1 s into its wait, with the cycle closed, and the
/// outside transaction gets its lock when the write transaction aborts.
/// `SET LOCAL` ends with the transaction, so the pooled connection keeps the
/// server default. The role must be allowed to set `deadlock_timeout` (a
/// superuser, as in CI).
async fn outside_transaction(db: &PgPool) -> (sqlx::Transaction<'static, sqlx::Postgres>, i32) {
    let mut holder = db.begin().await.expect("begin the outside transaction");
    sqlx::query("SET LOCAL deadlock_timeout = '5s'")
        .execute(&mut *holder)
        .await
        .expect("set the outside transaction's deadlock timeout");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .expect("read the backend pid");
    (holder, pid)
}

/// Wait until `n` backends other than `own_pid` wait on a lock.
async fn wait_for_lock_waiters(db: &PgPool, own_pid: i32, n: i64) {
    for _ in 0..500 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' AND pid <> $1",
        )
        .bind(own_pid)
        .fetch_one(db)
        .await
        .expect("read pg_stat_activity");
        if waiting >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{n} backends never waited on a lock");
}

#[tokio::test]
async fn a_check_of_a_missing_item_waits_for_its_concurrent_create() {
    // The item does not exist yet, but an outside transaction is creating it.
    // The check must wait for that create to commit and then fail, instead of
    // passing on an absence that is about to end.
    let test = "a_check_of_a_missing_item_waits_for_its_concurrent_create";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = seeded_table(&s, &maps).await;
    let table = data_table(&s.db).await;
    let (creator, creator_pid) = create_in_flight(&s.db, &table, "c").await;

    let (cond, c) = (not_exists(), key("c"));
    let ops = [TransactWriteOp::ConditionCheck {
        key_info: &key_info,
        key: &c,
        condition: &cond,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
    }];
    let twi = s.engine.transact_write_items(&ops, None);
    let commit = async {
        wait_for_lock_waiters(&s.db, creator_pid, 1).await;
        creator.commit().await.expect("commit the create");
    };
    let (result, ()) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(twi, commit) })
            .await
            .expect("the write transaction finished");

    match result {
        Err(StorageError::TransactionCanceled(reasons)) => {
            let codes: Vec<&str> = reasons.iter().map(|r| r.code.as_str()).collect();
            assert_eq!(codes, ["ConditionalCheckFailed"], "{reasons:?}");
        }
        other => panic!("expected a ConditionalCheckFailed cancellation, got {other:?}"),
    }

    s.cleanup().await;
}

#[tokio::test]
async fn a_delete_of_a_missing_item_waits_for_its_concurrent_create() {
    // Same race for a Delete: it must order after the create and remove the
    // new item, not skip it as missing while the create commits around it.
    let test = "a_delete_of_a_missing_item_waits_for_its_concurrent_create";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = seeded_table(&s, &maps).await;
    let table = data_table(&s.db).await;
    let (creator, creator_pid) = create_in_flight(&s.db, &table, "c").await;

    let c = key("c");
    let ops = [TransactWriteOp::Delete {
        key_info: &key_info,
        key: &c,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let twi = s.engine.transact_write_items(&ops, None);
    let commit = async {
        wait_for_lock_waiters(&s.db, creator_pid, 1).await;
        creator.commit().await.expect("commit the create");
    };
    let (result, ()) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(twi, commit) })
            .await
            .expect("the write transaction finished");
    result.expect("the delete commits");

    let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE pk = 'c'"))
        .fetch_one(&s.db)
        .await
        .expect("count c");
    assert_eq!(left, 0, "the delete ran after the create, so c is gone");

    s.cleanup().await;
}

#[tokio::test]
async fn a_plain_put_waits_for_a_check_of_the_missing_item() {
    // A transaction checks that `0c` is missing, then waits on `b`, which an
    // outside transaction holds. A non-transactional PutItem of `0c` meanwhile
    // must wait for the transaction, which still relies on `0c` being absent.
    let test = "a_plain_put_waits_for_a_check_of_the_missing_item";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = seeded_table(&s, &maps).await;
    let table = data_table(&s.db).await;

    let mut holder = s.db.begin().await.expect("begin the outside transaction");
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .expect("read the backend pid");
    sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'b' FOR UPDATE"))
        .execute(&mut *holder)
        .await
        .expect("lock b");

    // `0c` sorts before `b`, so the check runs first and then the Put waits.
    let (cond, c, new_b) = (not_exists(), key("0c"), item("b", "twi"));
    let ops = [
        put(&key_info, &new_b, &maps),
        TransactWriteOp::ConditionCheck {
            key_info: &key_info,
            key: &c,
            condition: &cond,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        },
    ];
    let twi = s.engine.transact_write_items(&ops, None);
    let plain_put = async {
        wait_for_lock_waiters(&s.db, holder_pid, 1).await;
        // Readers neither see the reserved key nor wait for it.
        let read = tokio::time::timeout(Duration::from_secs(5), async {
            let got = s.engine.get_item(&key_info, &key("0c")).await;
            let rows: i64 =
                sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE pk = '0c'"))
                    .fetch_one(&s.db)
                    .await
                    .expect("count 0c");
            (got, rows)
        })
        .await
        .expect("a read of the reserved key does not wait");
        assert_eq!(read.0.expect("GetItem of 0c"), None, "GetItem sees no item");
        assert_eq!(read.1, 0, "no reader sees the placeholder row");
        s.engine
            .put_item(&key_info, item("0c", "plain"), false, None, &maps, None)
            .await
    };
    let release = async {
        // Both the transaction and the plain put wait.
        wait_for_lock_waiters(&s.db, holder_pid, 2).await;
        holder.rollback().await.expect("release b");
    };
    let (twi_result, put_result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(twi, plain_put, release)
    })
    .await
    .expect("both writes finished");
    twi_result.expect("the check passes: 0c was missing when it ran");
    put_result.expect("the plain put commits after the transaction");

    let v: String = sqlx::query_scalar(&format!(
        "SELECT item_data->'v'->>'S' FROM {table} WHERE pk = '0c'"
    ))
    .fetch_one(&s.db)
    .await
    .expect("read 0c");
    assert_eq!(v, "plain");

    s.cleanup().await;
}

const LSI_TABLE: &str = "t_twi_lsi_stream";

/// Create a (pk, sk) table with LSI `lsi1` on `lsk` and a stream.
async fn lsi_stream_table(s: &Scratch) -> TableKeyInfo {
    // The scratch database holds the catalog and data schemas together, so it
    // has the catalog's copy of `stream_shards`. Drop its foreign key to
    // `tables`, which the data database does not have.
    sqlx::query("ALTER TABLE stream_shards DROP CONSTRAINT stream_shards_table_id_fkey")
        .execute(&s.db)
        .await
        .expect("match the data schema");
    let s_attr = |name: &str| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    };
    let key = |name: &str, key_type: KeyType| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    };
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: LSI_TABLE.to_owned(),
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
async fn a_delete_that_loses_its_reservation_removes_the_winner_everywhere() {
    // A write transaction creates `c`, with its LSI row and stream record, and
    // then waits on `d`, which an outside transaction holds. A Delete of the
    // missing `c` meanwhile waits for that create. Once it commits, the Delete
    // must remove the winner: one REMOVE record whose old image is the winner,
    // and no LSI row left behind.
    let test = "a_delete_that_loses_its_reservation_removes_the_winner_everywhere";
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
    let winner = range_item("c", &[("lsk", "l"), ("v", "winner")]);
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
    let c = range_item("c", &[]);
    let delete_ops = [TransactWriteOp::Delete {
        key_info: &key_info,
        key: &c,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture.clone()),
    }];

    let creator = s.engine.transact_write_items(&creator_ops, None);
    let deleter = async {
        // The creator has inserted c and waits on d.
        wait_for_lock_waiters(&s.db, holder_pid, 1).await;
        s.engine.transact_write_items(&delete_ops, None).await
    };
    let release = async {
        // The Delete now waits on the creator's insert of c.
        wait_for_lock_waiters(&s.db, holder_pid, 2).await;
        holder.rollback().await.expect("release d");
    };
    let (created, deleted, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(creator, deleter, release)
    })
    .await
    .expect("both transactions finished");
    created.expect("the create commits");
    deleted.expect("the delete commits after it");

    for t in [&table, &lsi] {
        let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {t} WHERE pk = 'c'"))
            .fetch_one(&s.db)
            .await
            .expect("count c");
        assert_eq!(left, 0, "c is gone from {t}");
    }
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
            ("Remove".to_owned(), Some(winner.clone()))
        ],
        "the create, then the delete of the winner"
    );

    s.cleanup().await;
}

#[tokio::test]
async fn a_conflict_cancellation_keeps_the_token_used_and_the_same_request_runs_again() {
    // Measured on the service (2026-11-05, Q4a and Q4b): a request canceled
    // with TransactionConflict keeps its ClientRequestToken used. Another
    // request with the token answers IdempotentParameterMismatchException and
    // writes nothing, and the same request sent again with the token runs.
    let test = "a_conflict_cancellation_keeps_the_token_used_and_the_same_request_runs_again";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = seeded_table(&s, &maps).await;
    let table = data_table(&s.db).await;
    let key = IdempotencyKey {
        account_id: ACCOUNT,
        token: "twi-conflict-token",
        fingerprint: "fp-conflict",
    };

    // The deadlock of a_deadlock_cancels_with_transaction_conflict_on_the_waiting_item,
    // with a token.
    let (mut holder, holder_pid) = outside_transaction(&s.db).await;
    sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'b' FOR UPDATE"))
        .execute(&mut *holder)
        .await
        .expect("lock b");
    let (new_a, new_b) = (item("a", "twi"), item("b", "twi"));
    let ops = [put(&key_info, &new_b, &maps), put(&key_info, &new_a, &maps)];
    let twi = s.engine.transact_write_items(&ops, Some(key));
    let close_cycle = async {
        wait_for_lock_waiters(&s.db, holder_pid, 1).await;
        sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'a' FOR UPDATE"))
            .execute(&mut *holder)
            .await
            .expect("the outside transaction gets a once the write transaction aborts");
    };
    // Bounded, so a transaction that never closes the cycle fails the test
    // instead of hanging it.
    let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(twi, close_cycle)
    })
    .await
    .expect("the write transaction finished");
    holder.rollback().await.expect("release the outside locks");
    match result {
        Err(StorageError::TransactionCanceled(reasons)) => {
            let codes: Vec<&str> = reasons.iter().map(|r| r.code.as_str()).collect();
            assert_eq!(codes, ["TransactionConflict", "None"], "{reasons:?}");
        }
        other => panic!("expected a TransactionConflict cancellation, got {other:?}"),
    }

    // Q4a: another request with the token.
    let new_q = item("q", "other");
    let other = [put(&key_info, &new_q, &maps)];
    let other_key = IdempotencyKey {
        fingerprint: "fp-other",
        ..key
    };
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        s.engine.transact_write_items(&other, Some(other_key)),
    )
    .await
    .expect("the other request finished");
    assert!(
        matches!(result, Err(StorageError::IdempotentMismatch)),
        "{result:?}"
    );
    let written: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE pk = 'q'"))
        .fetch_one(&s.db)
        .await
        .expect("count q");
    assert_eq!(written, 0, "the other request wrote q");

    // Q4b: the same request again, with nothing held, runs and commits.
    tokio::time::timeout(
        Duration::from_secs(30),
        s.engine.transact_write_items(&ops, Some(key)),
    )
    .await
    .expect("the same request finished")
    .expect("the same request runs again and commits");
    for pk in ["a", "b"] {
        let v: String = sqlx::query_scalar(&format!(
            "SELECT item_data->'v'->>'S' FROM {table} WHERE pk = $1"
        ))
        .bind(pk)
        .fetch_one(&s.db)
        .await
        .expect("read the item");
        assert_eq!(v, "twi", "item {pk}");
    }

    s.cleanup().await;
}

#[tokio::test]
async fn a_cancelled_token_is_used_for_ten_minutes_from_its_last_cancellation() {
    // Measured on the service (2026-11-05, Q1a to Q1c): a cancelled token is
    // still used 9 minutes after its request and free 11 minutes after it,
    // and a cancellation of the same request sent again starts the window
    // again. The test moves the stored created_at back instead of waiting.
    let test = "a_cancelled_token_is_used_for_ten_minutes_from_its_last_cancellation";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = seeded_table(&s, &maps).await;
    let table = data_table(&s.db).await;
    let token = "twi-window-token";
    let key = IdempotencyKey {
        account_id: ACCOUNT,
        token,
        fingerprint: "fp-ccf",
    };
    let age = |minutes: i32| {
        let db = s.db.clone();
        async move {
            sqlx::query(
                "UPDATE idempotency_tokens SET created_at = NOW() - make_interval(mins => $1) \
                 WHERE token = $2",
            )
            .bind(minutes)
            .bind(token)
            .execute(&db)
            .await
            .expect("move created_at back");
        }
    };
    // A Put of `a` with attribute_not_exists(pk): `a` exists, so it fails.
    let condition = not_exists();
    let ccf_item = item("a", "ccf");
    let ccf = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &ccf_item,
        condition: Some(&condition),
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let cancelled = |result: Result<(), StorageError>| {
        matches!(&result, Err(StorageError::TransactionCanceled(r))
            if r.len() == 1 && r[0].code == "ConditionalCheckFailed")
    };

    assert!(cancelled(
        s.engine.transact_write_items(&ccf, Some(key)).await
    ));
    // Q1c: 6 minutes later the same request is cancelled again, which starts
    // the window again.
    age(6).await;
    assert!(cancelled(
        s.engine.transact_write_items(&ccf, Some(key)).await
    ));
    let seconds: f64 = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM NOW() - created_at)::float8 FROM idempotency_tokens \
         WHERE token = $1",
    )
    .bind(token)
    .fetch_one(&s.db)
    .await
    .expect("read created_at");
    assert!(
        seconds < 60.0,
        "created_at is {seconds} s old after the second cancellation"
    );

    // Q1b: 9 minutes after the last cancellation the token is still used.
    let new_q = item("q", "other");
    let other = [put(&key_info, &new_q, &maps)];
    let other_key = IdempotencyKey {
        fingerprint: "fp-other",
        ..key
    };
    age(9).await;
    let result = s.engine.transact_write_items(&other, Some(other_key)).await;
    assert!(
        matches!(result, Err(StorageError::IdempotentMismatch)),
        "{result:?}"
    );
    // Q1a: 11 minutes after it the token is free.
    age(11).await;
    s.engine
        .transact_write_items(&other, Some(other_key))
        .await
        .expect("a token 11 minutes past its cancellation is free");
    let written: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE pk = 'q'"))
        .fetch_one(&s.db)
        .await
        .expect("count q");
    assert_eq!(written, 1);

    s.cleanup().await;
}

/// Rounds of the two tests below. Most rounds send the second request while
/// the first is still running.
const RACE_ROUNDS: usize = 20;

/// The update `SET n = n + :one` and its maps.
fn increment() -> (Vec<UpdateAction>, ExpressionMaps) {
    let tokens = expression::tokenize("SET n = n + :one").expect("tokenize");
    let actions = expression::parse_update(&tokens).expect("parse");
    let maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("one".to_owned(), AttributeValue::N("1".to_owned()))]),
    );
    (actions, maps)
}

/// Seed the item `cnt` with `n` = 0.
async fn seed_counter(s: &Scratch, key_info: &TableKeyInfo) {
    let cnt = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S("cnt".to_owned())),
        ("n".to_owned(), AttributeValue::N("0".to_owned())),
    ]);
    s.engine
        .put_item(key_info, cnt, false, None, &ExpressionMaps::default(), None)
        .await
        .expect("seed cnt");
}

/// The stored `n` of `cnt`.
async fn counter(db: &PgPool, table: &str) -> i64 {
    let n: String = sqlx::query_scalar(&format!(
        "SELECT item_data->'n'->>'N' FROM {table} WHERE pk = 'cnt'"
    ))
    .fetch_one(db)
    .await
    .expect("read n");
    n.parse().expect("n is an integer")
}

/// Send `ops` with `key` twice at the same moment, released by a barrier, and
/// return both answers. Bounded, so a send that never finishes fails the test
/// instead of hanging it.
async fn send_twice_at_once(
    engine: &PostgresEngine,
    ops: &[TransactWriteOp<'_>],
    key: IdempotencyKey<'_>,
) -> [Result<(), StorageError>; 2] {
    let barrier = &tokio::sync::Barrier::new(2);
    let send = move || async move {
        barrier.wait().await;
        engine.transact_write_items(ops, Some(key)).await
    };
    let (first, second) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(send(), send())
    })
    .await
    .expect("both sends finished");
    [first, second]
}

/// One send applied the request and the other answered the replay or
/// TransactionInProgress.
fn applied_by_one(answers: &[Result<(), StorageError>; 2]) -> bool {
    let applied = answers.iter().filter(|a| a.is_ok()).count();
    let waited = answers
        .iter()
        .filter(|a| {
            matches!(
                a,
                Err(StorageError::IdempotentReplay | StorageError::TransactionInProgress)
            )
        })
        .count();
    applied == 1 && waited == 1
}

#[tokio::test]
async fn two_sends_of_one_request_with_one_token_apply_it_once() {
    // Measured on the service (2026-11-05): two sends of one request with one
    // fresh token at the same moment applied it once in all 20 rounds. One
    // send answered OK and the other TransactionInProgressException, or both
    // answered OK. Here the second send's token insert waits on the first
    // send's token row, and the second send answers the replay once the first
    // commits (#851).
    let test = "two_sends_of_one_request_with_one_token_apply_it_once";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let plain = ExpressionMaps::default();
    let key_info = seeded_table(&s, &plain).await;
    seed_counter(&s, &key_info).await;
    let table = data_table(&s.db).await;
    let (actions, maps) = increment();
    let cnt = key("cnt");
    let ops = [TransactWriteOp::Update {
        key_info: &key_info,
        key: &cnt,
        actions: &actions,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];

    let mut failed = Vec::new();
    for round in 0..RACE_ROUNDS {
        let token = format!("twi-race-{round}");
        let key = IdempotencyKey {
            account_id: ACCOUNT,
            token: &token,
            fingerprint: "fp-increment",
        };
        let before = counter(&s.db, &table).await;
        let answers = send_twice_at_once(&s.engine, &ops, key).await;
        let grew = counter(&s.db, &table).await - before;
        if grew != 1 || !applied_by_one(&answers) {
            failed.push(format!(
                "round {round}: n grew by {grew}, answers {answers:?}"
            ));
        }
    }
    assert!(
        failed.is_empty(),
        "{} of {RACE_ROUNDS} rounds:\n{}",
        failed.len(),
        failed.join("\n")
    );

    s.cleanup().await;
}

#[tokio::test]
async fn two_sends_that_run_a_cancelled_request_again_apply_it_once() {
    // A request cancelled after the request-level key checks keeps its token,
    // and the same request sent again with it runs again (measured
    // 2026-11-05). Here the second run commits, and it is sent twice at the
    // same moment. The second send waits on the first send's token row and
    // answers the replay once the first commits. It does not answer the
    // mismatch, which is the answer for a different request.
    let test = "two_sends_that_run_a_cancelled_request_again_apply_it_once";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let plain = ExpressionMaps::default();
    let key_info = seeded_table(&s, &plain).await;
    seed_counter(&s, &key_info).await;
    let table = data_table(&s.db).await;
    let (actions, maps) = increment();
    let tokens = expression::tokenize("attribute_exists(go)").expect("tokenize");
    let gate = expression::parse_condition(&tokens).expect("parse");
    let cnt = key("cnt");
    let ops = [TransactWriteOp::Update {
        key_info: &key_info,
        key: &cnt,
        actions: &actions,
        condition: Some(&gate),
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let set_go = |sql: String| {
        let db = s.db.clone();
        async move {
            sqlx::query(&sql)
                .execute(&db)
                .await
                .expect("change go on cnt");
        }
    };

    let mut failed = Vec::new();
    for round in 0..RACE_ROUNDS {
        let token = format!("twi-rerun-race-{round}");
        let key = IdempotencyKey {
            account_id: ACCOUNT,
            token: &token,
            fingerprint: "fp-gated-increment",
        };
        // Without `go` the condition fails, and the token is kept cancelled.
        set_go(format!(
            "UPDATE {table} SET item_data = item_data - 'go' WHERE pk = 'cnt'"
        ))
        .await;
        let first = tokio::time::timeout(
            Duration::from_secs(30),
            s.engine.transact_write_items(&ops, Some(key)),
        )
        .await
        .expect("the first send finished");
        assert!(
            matches!(&first, Err(StorageError::TransactionCanceled(r))
                if r.len() == 1 && r[0].code == "ConditionalCheckFailed"),
            "round {round}: {first:?}"
        );
        // With `go` the same request commits when it runs again.
        set_go(format!(
            "UPDATE {table} SET item_data = item_data || \
             jsonb_build_object('go', jsonb_build_object('S', 'y')) WHERE pk = 'cnt'"
        ))
        .await;
        let before = counter(&s.db, &table).await;
        let answers = send_twice_at_once(&s.engine, &ops, key).await;
        let grew = counter(&s.db, &table).await - before;
        if grew != 1 || !applied_by_one(&answers) {
            failed.push(format!(
                "round {round}: n grew by {grew}, answers {answers:?}"
            ));
        }
    }
    assert!(
        failed.is_empty(),
        "{} of {RACE_ROUNDS} rounds:\n{}",
        failed.len(),
        failed.join("\n")
    );

    s.cleanup().await;
}
