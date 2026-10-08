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

use std::collections::BTreeMap;
use std::time::Duration;

use extenddb_core::expression::{self, Expr, ExpressionMaps};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, GsiInput, Item,
    KeySchemaElement, KeyType, LsiInput, Projection, ProjectionType,
    ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamRecord, StreamSpecification,
    StreamViewType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, StreamCapture, TableEngine, TransactWriteOp};
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
    // cycle. The write transaction waited first, so its deadlock check runs
    // first and PostgreSQL aborts it.
    let mut holder = s.db.begin().await.expect("begin the outside transaction");
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .expect("read the backend pid");
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
