// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The page `get_stream_records` returns is ordered by sequence number
//! whatever order the records were written in.
//!
//! The statements in `stream_records_sql.rs` put `ORDER BY sequence_number
//! LIMIT n` inside the records subquery. Without that `ORDER BY` the subquery
//! returns whichever rows the scan produces first; on a shard written in
//! sequence order that happens to be the right page, so the ordered-page test
//! in `stream_shard_ownership.rs` cannot see the clause go missing. This test
//! writes the records with the highest sequence number first, so a subquery
//! that reads in physical order returns the wrong page.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType,
};
use extenddb_storage::util::stream_arn;
use extenddb_storage::{StreamEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "111111111111";
const REGION: &str = "us-east-1";
const LABEL: &str = "2026-01-01T00:00:00.000";

/// The server's base URL, or `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED`
/// set to any value but `0`, a job that must reach a server fails here
/// instead.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.is_empty() {
        let required =
            std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
        assert!(
            !required,
            "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED says \
             this job must reach a PostgreSQL server"
        );
        return None;
    }
    Some(conn)
}

fn shipped_migrations() -> Vec<String> {
    let mut out = Vec::new();
    for dir in ["migrations", "data_migrations"] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(dir);
        let mut files: Vec<_> = std::fs::read_dir(&path)
            .expect("list the migration directory")
            .map(|e| e.expect("read a directory entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "sql"))
            .collect();
        files.sort();
        for f in files {
            out.push(std::fs::read_to_string(&f).expect("read a migration file"));
        }
    }
    out
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

#[tokio::test]
async fn a_page_is_in_sequence_order_when_the_records_were_written_in_reverse() {
    let Some(base) = base_conn() else {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
    let db_name = format!("eddb_sord_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    sqlx::query("ALTER TABLE stream_shards DROP CONSTRAINT IF EXISTS stream_shards_table_id_fkey")
        .execute(&db)
        .await
        .expect("match the two-database layout");
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&db)
    .await
    .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{ACCOUNT}"))
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

    let desc = engine
        .create_table(ACCOUNT, stream_table("t"))
        .await
        .expect("create a stream-enabled table");
    let table_id = desc.table_id.clone();
    sqlx::query("UPDATE tables SET stream_label = $1 WHERE table_id = $2")
        .bind(LABEL)
        .bind(&table_id)
        .execute(&db)
        .await
        .expect("pin the stream label");
    let shard: String = sqlx::query_scalar(
        "SELECT shard_id FROM stream_shards WHERE table_id = $1 ORDER BY shard_id LIMIT 1",
    )
    .bind(&table_id)
    .fetch_one(&db)
    .await
    .expect("one shard id");

    // Highest sequence number first, one row at a time, so the physical order
    // of the heap is the reverse of the sequence order.
    for seq in (1..=10u64).rev() {
        sqlx::query(
            "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, \
             record_data, created_at) VALUES ($1, $2, $3, 'INSERT', $4::jsonb, NOW())",
        )
        .bind(&shard)
        .bind(format!("{seq:021}"))
        .bind(&table_id)
        .bind(record_json(seq))
        .execute(&db)
        .await
        .expect("seed one record");
    }
    sqlx::query("ANALYZE stream_records")
        .execute(&db)
        .await
        .expect("analyze the records");

    let arn = stream_arn(REGION, ACCOUNT, "t", LABEL);
    let want =
        |from: u64| -> Vec<String> { (from..from + 3).map(|n| format!("{n:021}")).collect() };

    let (first, last) = engine
        .get_stream_records(ACCOUNT, &arn, &shard, None, 3)
        .await
        .expect("the first page reads");
    let got: Vec<String> = first
        .iter()
        .map(|r| r.dynamodb.sequence_number.clone())
        .collect();
    assert_eq!(
        got,
        want(1),
        "the first page is the three lowest sequence numbers"
    );
    assert_eq!(last.as_deref(), Some("000000000000000000003"));

    let (second, _) = engine
        .get_stream_records(ACCOUNT, &arn, &shard, Some("000000000000000000002"), 3)
        .await
        .expect("the page after sequence 2 reads");
    let got: Vec<String> = second
        .iter()
        .map(|r| r.dynamodb.sequence_number.clone())
        .collect();
    assert_eq!(got, want(3), "the page after sequence 2 is 3, 4, 5");

    drop(engine);
    db.close().await;
    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
    ))
    .execute(&admin)
    .await
    .expect("remove the scratch database");
    admin.close().await;
}
