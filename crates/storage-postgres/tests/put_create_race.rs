// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for a `PutItem` that races another writer to create
//! the same item.
//!
//! Amazon DynamoDB applies such puts one after another: the later put
//! overwrites the item, after its own condition is checked against it. These
//! tests hold the competing create open in an outside transaction, so the put
//! deterministically loses the insert and has to order itself after the winner.
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
    KeyType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};

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

/// Wait until a backend other than `own_pid` waits on a lock.
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
    panic!("the put never waited on the create in flight");
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
        wait_for_lock_waiter(&s.db, creator_pid).await;
        creator.commit().await.expect("commit the create");
    };
    let (result, ()) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(put, commit) })
            .await
            .expect("the put finished");
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
