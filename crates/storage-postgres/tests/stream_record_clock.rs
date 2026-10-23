// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A stream record's `created_at` column and its `ApproximateCreationDateTime`
//! come from one clock read.
//!
//! The column default (`NOW()`) is the transaction start time. A write that
//! waits inside its transaction, on a row lock here, stores a `created_at`
//! that is earlier than the record's own timestamp by the length of the wait.
//! The retention sweep filters on `created_at`, so the two values must agree.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. Each test builds
//! its own scratch database and drops it afterwards.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType, StreamSpecification, StreamViewType, TableKeyInfo, UserIdentity,
};
use extenddb_storage::{DataEngine, StreamCapture, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_stream_clock";

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

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_sclk_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
        .max_connections(3)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    sqlx::query("ALTER TABLE stream_shards DROP CONSTRAINT IF EXISTS stream_shards_table_id_fkey")
        .execute(&db)
        .await
        .expect("match the two-database layout");
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

fn item(pk: &str, v: &str) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("v".to_owned(), AttributeValue::S(v.to_owned())),
    ])
}

async fn stream_table(s: &Scratch) -> TableKeyInfo {
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
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewImage),
                }),
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

/// Stored `(created_at as epoch seconds, ApproximateCreationDateTime)` for
/// every record in the scratch database.
async fn stored_times(db: &PgPool) -> Vec<(i64, i64)> {
    let rows: Vec<(f64, serde_json::Value)> = sqlx::query_as(
        "SELECT EXTRACT(EPOCH FROM created_at)::float8, record_data FROM stream_records \
         ORDER BY sequence_number",
    )
    .fetch_all(db)
    .await
    .expect("read the stream records");
    rows.into_iter()
        .map(|(created, data)| {
            let approx = data["dynamodb"]["ApproximateCreationDateTime"]
                .as_i64()
                .expect("ApproximateCreationDateTime is an integer");
            #[allow(clippy::cast_possible_truncation)]
            let created_secs = created.floor() as i64;
            (created_secs, approx)
        })
        .collect()
}

#[tokio::test]
async fn created_at_matches_the_record_timestamp_when_the_write_waited() {
    let test = "created_at_matches_the_record_timestamp_when_the_write_waited";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let maps = ExpressionMaps::default();
    let key_info = stream_table(&s).await;
    let capture = StreamCapture {
        view_type: StreamViewType::NewImage,
        user_identity: None,
        region: Arc::from(REGION),
    };
    // Seed so the write below takes the locked update path.
    s.engine
        .put_item(
            &key_info,
            item("k", "seed"),
            false,
            None,
            &maps,
            Some(&capture),
        )
        .await
        .expect("seed the item");
    let table = data_table(&s.db).await;

    // An outside transaction holds the row. The write begins its own
    // transaction (fixing NOW()), waits on the lock for longer than a second,
    // then builds its record once the lock is released.
    let mut holder = s.db.begin().await.expect("begin the outside transaction");
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .expect("read the backend pid");
    sqlx::query(&format!("SELECT 1 FROM {table} WHERE pk = 'k' FOR UPDATE"))
        .execute(&mut *holder)
        .await
        .expect("lock the row");

    let write = s.engine.put_item(
        &key_info,
        item("k", "late"),
        false,
        None,
        &maps,
        Some(&capture),
    );
    let release = async {
        wait_for_lock_waiters(&s.db, holder_pid, 1).await;
        // Past the next second boundary and then some, so NOW() and the
        // record's own clock read fall in different seconds.
        tokio::time::sleep(Duration::from_millis(2_100)).await;
        holder.commit().await.expect("release the row");
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(write, release)
    })
    .await
    .expect("the write finished");
    result.expect("the write succeeded");

    // A service-identity deletion, the TTL worker's shape, through the same
    // helper.
    s.engine
        .delete_item(
            &key_info,
            &BTreeMap::from([("pk".to_owned(), AttributeValue::S("k".to_owned()))]),
            false,
            None,
            &maps,
            Some(&StreamCapture {
                view_type: StreamViewType::NewImage,
                user_identity: Some(UserIdentity {
                    identity_type: "Service".to_owned(),
                    principal_id: "dynamodb.amazonaws.com".to_owned(),
                }),
                region: Arc::from(REGION),
            }),
        )
        .await
        .expect("the TTL-shaped delete succeeded");

    let times = stored_times(&s.db).await;
    assert_eq!(times.len(), 3, "seed, late, and remove records: {times:?}");
    for (created_secs, approx) in &times {
        assert_eq!(
            created_secs, approx,
            "created_at second must equal ApproximateCreationDateTime: {times:?}"
        );
    }
    // And the waited record is at least two seconds after the seed.
    assert!(
        times[1].1 - times[0].1 >= 2,
        "the late record carries the post-wait time: {times:?}"
    );

    s.cleanup().await;
}
