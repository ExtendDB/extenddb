// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

//! The scratch database and request helpers the stream disable tests share.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); the tests skip otherwise. Each
//! test builds its own scratch database and drops it afterwards.

pub use extenddb_core::expression::ExpressionMaps;
pub use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DescribeStreamInput, DescribeTableInput,
    KeySchemaElement, KeyType, ScalarAttributeType, StreamDescription, StreamSpecification,
    StreamStatus, StreamViewType, TableDescription,
};
pub use extenddb_storage::error::StorageError;
pub use extenddb_storage::{DataEngine, StreamCapture, StreamEngine, TableEngine};
pub use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
pub use sqlx::PgPool;
pub use sqlx::postgres::PgPoolOptions;

pub const ACCOUNT: &str = "123456789012";
pub const REGION: &str = "us-east-1";

pub struct Scratch {
    pub engine: PostgresEngine,
    pub db: PgPool,
    pub admin: PgPool,
    pub db_name: String,
}

impl Scratch {
    pub async fn cleanup(self) {
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

pub fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Every `.sql` file under `migrations/` and `data_migrations/`, each
/// directory in filename order.
pub fn shipped_migrations() -> Vec<String> {
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

pub async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_sdis_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    // One scratch database stands in for both the catalog and the data
    // database; drop the foreign key that cannot exist across the two.
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

pub fn table_input(name: &str, stream: bool) -> CreateTableInput {
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
        stream_specification: stream.then_some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        }),
        ..Default::default()
    }
}

/// Finish the control-plane transition the worker would run.
pub async fn activate(s: &Scratch, name: &str) {
    sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_name = $1")
        .bind(name)
        .execute(&s.db)
        .await
        .expect("activate");
}

pub async fn set_stream(
    s: &Scratch,
    name: &str,
    spec: serde_json::Value,
) -> Result<TableDescription, StorageError> {
    let input = serde_json::json!({"TableName": name, "StreamSpecification": spec});
    let out = s
        .engine
        .update_table(ACCOUNT, serde_json::from_value(input).expect("input"))
        .await;
    activate(s, name).await;
    out
}

pub async fn describe_table(s: &Scratch, name: &str) -> TableDescription {
    s.engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: name.to_owned(),
            },
        )
        .await
        .expect("describe table")
}

pub async fn describe_stream(s: &Scratch, arn: &str) -> StreamDescription {
    s.engine
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn: arn.to_owned(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("describe stream")
}

pub async fn put(s: &Scratch, name: &str, pk: &str, view_type: StreamViewType) {
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, name)
        .await
        .expect("key info");
    let item = serde_json::from_value(serde_json::json!({"pk": {"S": pk}})).expect("item");
    let capture = StreamCapture {
        view_type,
        user_identity: None,
        region: REGION.into(),
    };
    s.engine
        .put_item(
            &key_info,
            item,
            false,
            None,
            &ExpressionMaps::default(),
            Some(&capture),
        )
        .await
        .expect("put");
}

pub async fn last_record(s: &Scratch, shard: &str) -> Option<String> {
    sqlx::query_scalar("SELECT MAX(sequence_number) FROM stream_records WHERE shard_id = $1")
        .bind(shard)
        .fetch_one(&s.db)
        .await
        .expect("last record")
}

pub async fn record_count(s: &Scratch) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM stream_records")
        .fetch_one(&s.db)
        .await
        .expect("count records")
}

pub async fn open_shard_ids(s: &Scratch, table_id: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT shard_id FROM stream_shards \
         WHERE table_id = $1 AND ending_sequence_number IS NULL ORDER BY shard_id",
    )
    .bind(table_id)
    .fetch_all(&s.db)
    .await
    .expect("open shard ids")
}
