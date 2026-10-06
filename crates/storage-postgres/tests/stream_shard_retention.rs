// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A deleted table's stream shards are removed once its stream has aged out.
//!
//! DeleteTable keeps a table's shards and records so the stream stays readable
//! for the retention window, as on the service. The records were always
//! trimmed by the retention sweep; the shards were not, and every deleted
//! stream-enabled table left its four shard rows behind for good. The sweep
//! now removes the shards of a table that is gone from the catalog once none
//! of its records remain, and leaves every live table's shards alone.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. Each test builds
//! its own scratch database and drops it afterwards.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DeleteTableInput, KeySchemaElement,
    KeyType, ScalarAttributeType, StreamSpecification, StreamViewType,
};
use extenddb_storage::{StreamEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

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

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_shrd_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    // One scratch database stands in for both the catalog and the data
    // database. The catalog schema's `stream_shards` carries a foreign key to
    // `tables` that cannot exist in a real deployment (the two live in
    // different databases) and that the engine's insert order violates; drop
    // it so the scratch layout matches production.
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

fn stream_table(name: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: name.to_owned(),
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
    }
}

async fn shard_count(db: &PgPool, table_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM stream_shards WHERE table_id = $1")
        .bind(table_id)
        .fetch_one(db)
        .await
        .expect("count shards")
}

#[tokio::test]
async fn shards_of_a_deleted_table_go_once_its_records_are_gone() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;

    let gone = s
        .engine
        .create_table(ACCOUNT, stream_table("t_gone"))
        .await
        .expect("create the table to delete");
    let live = s
        .engine
        .create_table(ACCOUNT, stream_table("t_live"))
        .await
        .expect("create the table that stays");
    let gone_id = gone.table_id.clone();
    let live_id = live.table_id.clone();
    assert_eq!(shard_count(&s.db, &gone_id).await, 4);
    assert_eq!(shard_count(&s.db, &live_id).await, 4);

    s.engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: "t_gone".to_owned(),
            },
        )
        .await
        .expect("delete the table");
    assert_eq!(
        shard_count(&s.db, &gone_id).await,
        4,
        "DeleteTable keeps the shards so the stream stays readable"
    );

    // One record still inside the retention window keeps the shards.
    let shard: String = sqlx::query_scalar(
        "SELECT shard_id FROM stream_shards WHERE table_id = $1 ORDER BY shard_id LIMIT 1",
    )
    .bind(&gone_id)
    .fetch_one(&s.db)
    .await
    .expect("one shard id");
    sqlx::query(
        "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, record_data, created_at) \
         VALUES ($1, '000000000000000000001', $2, 'INSERT', '{}'::jsonb, NOW() + interval '1 hour')",
    )
    .bind(&shard)
    .bind(&gone_id)
    .execute(&s.db)
    .await
    .expect("seed a record that is still inside the window");

    // A retention of zero hours makes everything already written eligible.
    s.engine
        .cleanup_expired_stream_records(0)
        .await
        .expect("sweep");
    assert_eq!(
        shard_count(&s.db, &gone_id).await,
        4,
        "shards stay while a record of theirs remains"
    );
    assert_eq!(shard_count(&s.db, &live_id).await, 4);

    sqlx::query("DELETE FROM stream_records WHERE table_id = $1")
        .bind(&gone_id)
        .execute(&s.db)
        .await
        .expect("age the record out");
    s.engine
        .cleanup_expired_stream_records(0)
        .await
        .expect("sweep");
    assert_eq!(
        shard_count(&s.db, &gone_id).await,
        0,
        "the deleted table's shards are removed"
    );
    assert_eq!(
        shard_count(&s.db, &live_id).await,
        4,
        "a live table's shards are never swept, even with no records"
    );

    s.cleanup().await;
}
