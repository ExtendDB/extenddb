// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `get_stream_records` reads a shard only under the stream its iterator names.
//!
//! The catalog read is keyed on the caller's account and the table name the
//! ARN carries, the data read on that table's id, and the stream label is
//! decided in Rust from the table's current label. Each test here removes one
//! of those from the picture (same label on two tables, same name in two
//! accounts, a new label on the same shards) so that one predicate alone
//! decides the answer.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. Each test builds
//! its own scratch database and drops it afterwards.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::stream_arn;
use extenddb_storage::{StreamEngine, StreamRecordsResult, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "111111111111";
const OTHER_ACCOUNT: &str = "222222222222";
const REGION: &str = "us-east-1";
const LABEL: &str = "2026-01-01T00:00:00.000";
const LATER_LABEL: &str = "2026-02-02T00:00:00.000";

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

    /// A stream-enabled table whose label is pinned to `label`. Returns its
    /// table id and the id of its first shard.
    async fn stream_table(&self, account: &str, name: &str, label: &str) -> (String, String) {
        let desc = self
            .engine
            .create_table(account, stream_table(name))
            .await
            .expect("create a stream-enabled table");
        sqlx::query("UPDATE tables SET stream_label = $1 WHERE table_id = $2")
            .bind(label)
            .bind(&desc.table_id)
            .execute(&self.db)
            .await
            .expect("pin the stream label");
        let shard: String = sqlx::query_scalar(
            "SELECT shard_id FROM stream_shards WHERE table_id = $1 ORDER BY shard_id LIMIT 1",
        )
        .bind(&desc.table_id)
        .fetch_one(&self.db)
        .await
        .expect("one shard id");
        (desc.table_id, shard)
    }

    async fn write_records(&self, shard_id: &str, table_id: &str, seqs: &[u64]) {
        for seq in seqs {
            sqlx::query(
                "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, \
                 record_data, created_at) VALUES ($1, $2, $3, 'INSERT', $4::jsonb, NOW())",
            )
            .bind(shard_id)
            .bind(format!("{seq:021}"))
            .bind(table_id)
            .bind(record_json(*seq))
            .execute(&self.db)
            .await
            .expect("seed a record");
        }
    }

    async fn relabel(&self, table_id: &str, label: &str) {
        sqlx::query("UPDATE tables SET stream_label = $1 WHERE table_id = $2")
            .bind(label)
            .bind(table_id)
            .execute(&self.db)
            .await
            .expect("relabel");
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
    let db_name = format!("eddb_sown_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    // database; the catalog schema's foreign key from `stream_shards` to
    // `tables` cannot exist in a real deployment, so it is dropped.
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
    for account in [ACCOUNT, OTHER_ACCOUNT] {
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
            .bind(account)
            .bind(format!("acct-{account}"))
            .execute(&db)
            .await
            .expect("seed the account row");
    }
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
            stream_view_type: Some(StreamViewType::KeysOnly),
        }),
        ..Default::default()
    }
}

fn record_json(seq: u64) -> String {
    serde_json::json!({
        "eventID": format!("e{seq}"),
        "eventName": "INSERT",
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": REGION,
        "dynamodb": {
            "ApproximateCreationDateTime": 1_700_000_000,
            "Keys": { "pk": { "S": format!("k{seq}") } },
            "SequenceNumber": format!("{seq:021}"),
            "SizeBytes": 10,
            "StreamViewType": "KEYS_ONLY"
        }
    })
    .to_string()
}

fn assert_stream_not_found(result: StreamRecordsResult, arn: &str) {
    let StorageError::TableNotFound(expected) = extenddb_storage::util::stream_not_found(arn)
    else {
        unreachable!("stream_not_found is a TableNotFound error")
    };
    match result {
        Err(StorageError::TableNotFound(msg)) => assert_eq!(msg, expected),
        other => panic!("expected the stream not-found error, got {other:?}"),
    }
}

/// Two tables of one account share a stream label: a shard of table `b`
/// under table `a`'s ARN is refused, so the table name alone decides.
#[tokio::test]
async fn another_tables_shard_with_the_same_label_is_not_found() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    let (a_id, shard_a) = s.stream_table(ACCOUNT, "a", LABEL).await;
    let (b_id, shard_b) = s.stream_table(ACCOUNT, "b", LABEL).await;
    s.write_records(&shard_a, &a_id, &[1]).await;
    // Inserted out of order so the read's ORDER BY, not insertion order,
    // produces the sequence order the control asserts.
    s.write_records(&shard_b, &b_id, &[2, 1]).await;
    let arn_a = stream_arn(REGION, ACCOUNT, "a", LABEL);
    let arn_b = stream_arn(REGION, ACCOUNT, "b", LABEL);

    // Both directions, so whichever table a lookup without the name would
    // pick, one of them reads a shard that is not its own.
    assert_stream_not_found(
        s.engine
            .get_stream_records(ACCOUNT, &arn_a, &shard_b, None, 10)
            .await,
        &arn_a,
    );
    assert_stream_not_found(
        s.engine
            .get_stream_records(ACCOUNT, &arn_b, &shard_a, None, 10)
            .await,
        &arn_b,
    );
    assert_stream_not_found(
        s.engine
            .get_stream_records(ACCOUNT, &arn_a, "shardId-forged-0000000000000000", None, 10)
            .await,
        &arn_a,
    );
    let (records, _) = s
        .engine
        .get_stream_records(ACCOUNT, &arn_a, &shard_a, None, 10)
        .await
        .expect("own shard reads");
    assert_eq!(records.len(), 1);
    let (records, last) = s
        .engine
        .get_stream_records(ACCOUNT, &arn_b, &shard_b, None, 10)
        .await
        .expect("own shard reads in sequence order");
    assert_eq!(records[0].dynamodb.sequence_number, format!("{:021}", 1));
    assert_eq!(records[1].dynamodb.sequence_number, format!("{:021}", 2));
    assert_eq!(last.as_deref(), Some(format!("{:021}", 2).as_str()));
    s.cleanup().await;
}

/// Two accounts own a table of the same name and label: the caller's ARN
/// resolves in the caller's account only, so the other account's shard is
/// refused under either spelling of the account in the ARN.
#[tokio::test]
async fn another_accounts_same_named_stream_is_not_found() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    let (own_id, own_shard) = s.stream_table(ACCOUNT, "t", LABEL).await;
    let (other_id, other_shard) = s.stream_table(OTHER_ACCOUNT, "t", LABEL).await;
    s.write_records(&own_shard, &own_id, &[1]).await;
    s.write_records(&other_shard, &other_id, &[1]).await;

    // From both callers, so whichever table a lookup without the account
    // would pick, one caller reads the other account's shard.
    for (caller, foreign_shard, own) in [
        (ACCOUNT, &other_shard, &own_shard),
        (OTHER_ACCOUNT, &own_shard, &other_shard),
    ] {
        for arn in [
            stream_arn(REGION, ACCOUNT, "t", LABEL),
            stream_arn(REGION, OTHER_ACCOUNT, "t", LABEL),
        ] {
            assert_stream_not_found(
                s.engine
                    .get_stream_records(caller, &arn, foreign_shard, None, 10)
                    .await,
                &arn,
            );
            // The control: the caller's own shard reads under either spelling
            // of the account, since storage binds the caller's account and
            // parses only the table name and label out of the ARN.
            let (records, _) = s
                .engine
                .get_stream_records(caller, &arn, own, None, 10)
                .await
                .expect("own shard reads");
            assert_eq!(records.len(), 1);
        }
    }
    s.cleanup().await;
}

/// The table takes a new stream label while keeping its shard rows: the old
/// ARN's iterator is refused and the new ARN reads every record.
#[tokio::test]
async fn an_earlier_label_on_the_same_shards_is_not_found() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    let (table_id, shard) = s.stream_table(ACCOUNT, "t", LABEL).await;
    s.write_records(&shard, &table_id, &[1]).await;
    let old_arn = stream_arn(REGION, ACCOUNT, "t", LABEL);
    s.engine
        .get_stream_records(ACCOUNT, &old_arn, &shard, None, 10)
        .await
        .expect("current label reads");

    s.relabel(&table_id, LATER_LABEL).await;
    s.write_records(&shard, &table_id, &[2]).await;
    assert_stream_not_found(
        s.engine
            .get_stream_records(ACCOUNT, &old_arn, &shard, None, 10)
            .await,
        &old_arn,
    );
    let new_arn = stream_arn(REGION, ACCOUNT, "t", LATER_LABEL);
    let (records, _) = s
        .engine
        .get_stream_records(ACCOUNT, &new_arn, &shard, None, 10)
        .await
        .expect("new label reads");
    assert_eq!(records.len(), 2);
    s.cleanup().await;
}
