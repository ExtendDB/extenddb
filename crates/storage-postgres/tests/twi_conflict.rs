// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for lock conflicts inside `TransactWriteItems`.
//!
//! Amazon DynamoDB cancels a write transaction that loses a conflict with a
//! `TransactionConflict` reason on the contended item. These tests force the
//! PostgreSQL side of that: a real deadlock with an outside lock holder, and
//! many transactions that take the same items in opposite request orders.
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

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, GsiInput, Item,
    KeySchemaElement, KeyType, Projection, ProjectionType, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine, TransactWriteOp};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

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

/// Wait until some backend other than `own_pid` waits on a row lock.
async fn wait_for_lock_waiter(db: &PgPool, own_pid: i32) {
    for _ in 0..500 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' AND pid <> $1",
        )
        .bind(own_pid)
        .fetch_one(db)
        .await
        .expect("read pg_stat_activity");
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the transaction never waited on the held lock");
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
        wait_for_lock_waiter(&s.db, holder_pid).await;
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
