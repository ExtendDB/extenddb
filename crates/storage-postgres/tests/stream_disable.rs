// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Disabling a table's stream closes its shards, and enabling it again
//! creates a new stream, against the rows measured on the service on
//! 2026-10-24.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. Each test builds
//! its own scratch database and drops it afterwards.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DescribeStreamInput, DescribeTableInput,
    KeySchemaElement, KeyType, ScalarAttributeType, StreamDescription, StreamSpecification,
    StreamStatus, StreamViewType, TableDescription,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, StreamCapture, StreamEngine, TableEngine};
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
/// directory in filename order.
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

fn table_input(name: &str, stream: bool) -> CreateTableInput {
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
async fn activate(s: &Scratch, name: &str) {
    sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_name = $1")
        .bind(name)
        .execute(&s.db)
        .await
        .expect("activate");
}

async fn set_stream(
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

async fn describe_table(s: &Scratch, name: &str) -> TableDescription {
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

async fn describe_stream(s: &Scratch, arn: &str) -> StreamDescription {
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

async fn put(s: &Scratch, name: &str, pk: &str, view_type: StreamViewType) {
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

async fn last_record(s: &Scratch, shard: &str) -> Option<String> {
    sqlx::query_scalar("SELECT MAX(sequence_number) FROM stream_records WHERE shard_id = $1")
        .bind(shard)
        .fetch_one(&s.db)
        .await
        .expect("last record")
}

async fn record_count(s: &Scratch) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM stream_records")
        .fetch_one(&s.db)
        .await
        .expect("count records")
}

#[tokio::test]
async fn disable_closes_every_shard_and_re_enable_creates_a_new_stream() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("t482", true))
        .await
        .expect("create");
    activate(&s, "t482").await;
    for pk in ["k0", "k1", "k2"] {
        put(&s, "t482", pk, StreamViewType::NewAndOldImages).await;
    }
    let enabled = describe_table(&s, "t482").await;
    let old_arn = enabled.latest_stream_arn.clone().expect("stream arn");

    let disabled = set_stream(&s, "t482", serde_json::json!({"StreamEnabled": false}))
        .await
        .expect("disable");
    let mut disabled = disabled;
    disabled.prepare_for_response();
    assert!(disabled.stream_specification.is_none());
    assert_eq!(
        disabled.latest_stream_arn.as_deref(),
        Some(old_arn.as_str())
    );
    let mut table = describe_table(&s, "t482").await;
    table.prepare_for_response();
    assert!(table.stream_specification.is_none());
    assert_eq!(table.latest_stream_label, enabled.latest_stream_label);

    let closed = describe_stream(&s, &old_arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert_eq!(closed.stream_view_type, StreamViewType::NewAndOldImages);
    assert_eq!(closed.shards.len(), 4);
    for shard in &closed.shards {
        let range = &shard.sequence_number_range;
        let expected = last_record(&s, &shard.shard_id)
            .await
            .unwrap_or_else(|| range.starting_sequence_number.clone());
        assert_eq!(
            range.ending_sequence_number.as_deref(),
            Some(expected.as_str())
        );
    }

    // A write still carrying the enabled view type records nothing.
    put(&s, "t482", "stale", StreamViewType::NewAndOldImages).await;
    assert_eq!(record_count(&s).await, 3);

    match set_stream(&s, "t482", serde_json::json!({"StreamEnabled": false})).await {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table has no stream to disable: TableName: t482");
        }
        other => panic!("{other:?}"),
    }

    let reenabled = set_stream(
        &s,
        "t482",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .expect("re-enable");
    let new_arn = reenabled.latest_stream_arn.clone().expect("new arn");
    assert_ne!(new_arn, old_arn);
    let new = describe_stream(&s, &new_arn).await;
    assert!(matches!(new.stream_status, StreamStatus::Enabled));
    assert_eq!(new.stream_view_type, StreamViewType::KeysOnly);
    assert!(
        new.shards
            .iter()
            .all(|n| !closed.shards.iter().any(|o| o.shard_id == n.shard_id))
    );
    match set_stream(
        &s,
        "t482",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}),
    )
    .await
    {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table already has an enabled stream: TableName: t482");
        }
        other => panic!("{other:?}"),
    }

    put(&s, "t482", "after", StreamViewType::KeysOnly).await;
    assert_eq!(record_count(&s).await, 4);
    let old_again = describe_stream(&s, &old_arn).await;
    assert!(matches!(old_again.stream_status, StreamStatus::Disabled));
    for (a, b) in old_again.shards.iter().zip(&closed.shards) {
        assert_eq!(a.shard_id, b.shard_id);
        assert_eq!(
            a.sequence_number_range.ending_sequence_number,
            b.sequence_number_range.ending_sequence_number
        );
    }

    let (listed, _) = s
        .engine
        .list_streams(ACCOUNT, Some("t482"), 100, None)
        .await
        .expect("list");
    let arns: Vec<&str> = listed.iter().map(|x| x.stream_arn.as_str()).collect();
    assert_eq!(arns, [new_arn.as_str(), old_arn.as_str()]);
    match s
        .engine
        .validate_shard(ACCOUNT, &old_arn, &new.shards[0].shard_id)
        .await
    {
        Err(StorageError::TableNotFound(m)) => {
            assert_eq!(m, "Requested resource not found: Shard does not exist");
        }
        other => panic!("{other:?}"),
    }
    s.cleanup().await;
}

#[tokio::test]
async fn disable_on_a_table_that_never_had_a_stream_is_refused() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("nostream", false))
        .await
        .expect("create");
    activate(&s, "nostream").await;
    match set_stream(&s, "nostream", serde_json::json!({"StreamEnabled": false})).await {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table has no stream to disable: TableName: nostream");
        }
        other => panic!("{other:?}"),
    }
    // An UpdateTable that enables a stream and is then refused for an index
    // change in the same call leaves no shards and no label behind.
    let input = serde_json::json!({
        "TableName": "nostream",
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"},
        "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "missing"}}],
    });
    let refused = s
        .engine
        .update_table(ACCOUNT, serde_json::from_value(input).expect("input"))
        .await;
    assert!(refused.is_err(), "{refused:?}");
    let shards: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM stream_shards")
        .fetch_one(&s.db)
        .await
        .expect("count shards");
    assert_eq!(shards, 0);
    assert!(
        describe_table(&s, "nostream")
            .await
            .latest_stream_arn
            .is_none()
    );
    s.cleanup().await;
}

/// A stream-enabled table created under v0.1.13 or earlier has four shard
/// rows named `shardId-{table_name}-{index:016}`. Its stream stays
/// describable, readable and listed after the upgrade, keeps its view type
/// once disabled, and is replaced by a labelled stream on re-enable.
#[tokio::test]
async fn a_stream_from_before_v0_1_14_survives_the_upgrade() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("legacy", false))
        .await
        .expect("create");
    activate(&s, "legacy").await;
    let table_id = describe_table(&s, "legacy").await.table_id;
    // What a pre-v0.1.14 release wrote: the label and specification on the
    // catalog row, four name-based open shards in the data database.
    let label = "2026-10-20T10:00:00.000";
    sqlx::query(
        "UPDATE tables SET stream_label = $1, stream_specification = $2 \
         WHERE account_id = $3 AND table_name = 'legacy'",
    )
    .bind(label)
    .bind(serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}))
    .bind(ACCOUNT)
    .execute(&s.db)
    .await
    .expect("seed the catalog row");
    for i in 0..4 {
        sqlx::query(
            "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
             VALUES ($1, $2, $3)",
        )
        .bind(format!("shardId-legacy-{i:016}"))
        .bind(&table_id)
        .bind(format!("{:021}", 0))
        .execute(&s.db)
        .await
        .expect("seed a legacy shard");
    }
    let arn = describe_table(&s, "legacy")
        .await
        .latest_stream_arn
        .expect("the seeded stream arn");
    assert!(arn.ends_with(label));

    put(&s, "legacy", "k", StreamViewType::NewImage).await;
    let open = describe_stream(&s, &arn).await;
    assert!(matches!(open.stream_status, StreamStatus::Enabled));
    assert_eq!(open.stream_view_type, StreamViewType::NewImage);
    let ids: Vec<&str> = open.shards.iter().map(|x| x.shard_id.as_str()).collect();
    assert_eq!(
        ids,
        (0..4)
            .map(|i| format!("shardId-legacy-{i:016}"))
            .collect::<Vec<_>>()
    );
    s.engine
        .validate_shard(ACCOUNT, &arn, "shardId-legacy-0000000000000001")
        .await
        .expect("a legacy shard belongs to the stream");
    assert_eq!(record_count(&s).await, 1);
    let (listed, _) = s
        .engine
        .list_streams(ACCOUNT, Some("legacy"), 100, None)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].stream_arn, arn);

    set_stream(&s, "legacy", serde_json::json!({"StreamEnabled": false}))
        .await
        .expect("disable");
    let closed = describe_stream(&s, &arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert_eq!(
        closed.stream_view_type,
        StreamViewType::NewImage,
        "a disabled legacy stream keeps its view type"
    );
    assert!(
        closed
            .shards
            .iter()
            .all(|x| x.sequence_number_range.ending_sequence_number.is_some())
    );
    // The backend keeps the disabled specification (with the view type) and
    // the engine omits it from every response.
    let stored = describe_table(&s, "legacy").await.stream_specification;
    assert_eq!(
        stored,
        Some(StreamSpecification {
            stream_enabled: false,
            stream_view_type: Some(StreamViewType::NewImage),
        })
    );
    let mut rendered = describe_table(&s, "legacy").await;
    rendered.prepare_for_response();
    assert!(rendered.stream_specification.is_none());
    put(&s, "legacy", "dark", StreamViewType::NewImage).await;
    assert_eq!(
        record_count(&s).await,
        1,
        "a write after the disable records nothing"
    );

    let re = set_stream(
        &s,
        "legacy",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .expect("re-enable");
    let new_arn = re.latest_stream_arn.expect("new arn");
    assert_ne!(new_arn, arn);
    let new = describe_stream(&s, &new_arn).await;
    assert!(matches!(new.stream_status, StreamStatus::Enabled));
    assert_eq!(new.shards.len(), 4);
    assert!(new.shards.iter().all(|x| x.shard_id.len() == 61));
    // The enable closed the four legacy rows: none is still open.
    let open_legacy: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM stream_shards \
         WHERE shard_id LIKE 'shardId-legacy-%' AND ending_sequence_number IS NULL",
    )
    .fetch_one(&s.db)
    .await
    .expect("count open legacy shards");
    assert_eq!(
        open_legacy, 0,
        "the enable closes the legacy stream's shards"
    );
    s.cleanup().await;
}
