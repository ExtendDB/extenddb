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
//! The same file pins the cost of a page. The record read is a lateral
//! subquery with its own `ORDER BY` and `LIMIT`, and `EXPLAIN (ANALYZE)` of the
//! statements the engine runs shows the scans of `stream_records` visiting no
//! more rows than the page holds, however many records follow the bound.
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
use extenddb_storage_postgres::stream_records_sql::{
    STREAM_RECORDS_AFTER, STREAM_RECORDS_FROM_START,
};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

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

    /// Records numbered 1 to `total` in one statement, then `ANALYZE`, so the
    /// planner sees the statistics a shard that has been written for a while
    /// has.
    async fn write_run(&self, shard_id: &str, table_id: &str, total: u64) {
        let seqs: Vec<String> = (1..=total).map(|n| format!("{n:021}")).collect();
        let docs: Vec<String> = (1..=total).map(record_json).collect();
        sqlx::query(
            "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, \
             record_data, created_at) \
             SELECT $1, t.seq, $2, 'INSERT', t.doc::jsonb, NOW() \
             FROM unnest($3::text[], $4::text[]) AS t(seq, doc)",
        )
        .bind(shard_id)
        .bind(table_id)
        .bind(&seqs)
        .bind(&docs)
        .execute(&self.db)
        .await
        .expect("seed a run of records");
        sqlx::query("ANALYZE stream_records")
            .execute(&self.db)
            .await
            .expect("analyze the records");
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
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
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

fn assert_stream_not_found(result: StreamRecordsResult) {
    match result {
        Err(StorageError::TableNotFound(msg)) => {
            assert_eq!(msg, extenddb_storage::util::STREAM_DOES_NOT_EXIST);
        }
        other => panic!("expected the stream does not exist error, got {other:?}"),
    }
}

/// The plan `EXPLAIN (ANALYZE, FORMAT JSON)` reports for `statement`, run on
/// `conn`.
async fn plan_of(conn: &mut sqlx::PgConnection, statement: &str) -> serde_json::Value {
    let row = sqlx::raw_sql(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {statement}"))
        .fetch_one(conn)
        .await
        .expect("explain the statement");
    let document: serde_json::Value = row.try_get(0).expect("the plan document");
    document[0]["Plan"].clone()
}

/// The rows the plan's scans of `stream_records` visited: the rows each scan
/// returned plus the rows a filter or an index recheck discarded, over all
/// loops of the scan.
fn stream_records_rows_visited(plan: &serde_json::Value) -> f64 {
    let mut visited = 0.0;
    let mut nodes = vec![plan];
    while let Some(node) = nodes.pop() {
        if node["Relation Name"] == "stream_records" {
            let per_loop: f64 = [
                "Actual Rows",
                "Rows Removed by Filter",
                "Rows Removed by Index Recheck",
            ]
            .iter()
            .filter_map(|key| node[*key].as_f64())
            .sum();
            visited += per_loop * node["Actual Loops"].as_f64().unwrap_or(1.0);
        }
        if let Some(children) = node["Plans"].as_array() {
            nodes.extend(children);
        }
    }
    visited
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
    );
    assert_stream_not_found(
        s.engine
            .get_stream_records(ACCOUNT, &arn_b, &shard_a, None, 10)
            .await,
    );
    assert_stream_not_found(
        s.engine
            .get_stream_records(ACCOUNT, &arn_a, "shardId-forged-0000000000000000", None, 10)
            .await,
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

/// Five pages of one shard, read one after the other, return every record once
/// in sequence order with none skipped or repeated at a page boundary, and the
/// read after the last page finds the shard idle.
#[tokio::test]
async fn five_pages_read_every_record_once_in_sequence_order() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    const PAGE: i64 = 20;
    let s = scratch().await;
    let (table_id, shard) = s.stream_table(ACCOUNT, "t", LABEL).await;
    s.write_run(&shard, &table_id, 100).await;
    let arn = stream_arn(REGION, ACCOUNT, "t", LABEL);

    let mut after: Option<String> = None;
    let mut seen = Vec::new();
    for page in 1..=5 {
        let (records, last) = s
            .engine
            .get_stream_records(ACCOUNT, &arn, &shard, after.as_deref(), PAGE)
            .await
            .expect("a page reads");
        assert_eq!(records.len(), 20, "page {page}");
        seen.extend(records.iter().map(|r| r.dynamodb.sequence_number.clone()));
        after = last;
    }
    let expected: Vec<String> = (1..=100).map(|n| format!("{n:021}")).collect();
    assert_eq!(seen, expected);

    let (records, last) = s
        .engine
        .get_stream_records(ACCOUNT, &arn, &shard, after.as_deref(), PAGE)
        .await
        .expect("the read after the last page");
    assert!(records.is_empty());
    assert_eq!(last, None);
    s.cleanup().await;
}

/// A shard of the stream with nothing to read answers an empty page, not the
/// not-found error, with and without a sequence bound: the join keeps the
/// shard row when no record follows the bound.
#[tokio::test]
async fn an_idle_shard_reads_an_empty_page_with_and_without_a_bound() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    let (table_id, shard) = s.stream_table(ACCOUNT, "t", LABEL).await;
    let arn = stream_arn(REGION, ACCOUNT, "t", LABEL);
    let bound = |n: u64| Some(format!("{n:021}"));

    // No record in the shard yet, then a bound that follows no record.
    for after in [None, bound(5)] {
        let (records, last) = s
            .engine
            .get_stream_records(ACCOUNT, &arn, &shard, after.as_deref(), 10)
            .await
            .expect("a shard without records reads");
        assert!(records.is_empty());
        assert_eq!(last, None);
    }

    // Records 1 and 2 are written: a bound on the last record reads nothing,
    // and a bound before it reads that record.
    s.write_records(&shard, &table_id, &[1, 2]).await;
    let (records, last) = s
        .engine
        .get_stream_records(ACCOUNT, &arn, &shard, bound(2).as_deref(), 10)
        .await
        .expect("a bound on the last record reads");
    assert!(records.is_empty());
    assert_eq!(last, None);
    let (records, last) = s
        .engine
        .get_stream_records(ACCOUNT, &arn, &shard, bound(1).as_deref(), 10)
        .await
        .expect("a bound before the last record reads");
    assert_eq!(records.len(), 1);
    assert_eq!(last, bound(2));
    s.cleanup().await;
}

/// A page costs the page size, not the records that follow the bound. The
/// statements run under `EXPLAIN (ANALYZE)`, in the plan chosen for the
/// parameters and in the generic plan a reused prepared statement may settle
/// on, from the start of a shard of five pages and after its second page. The
/// scans of `stream_records` may visit no more rows than the page holds.
#[tokio::test]
async fn a_page_visits_at_most_the_page_size_of_the_shards_records() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    const PAGE: u32 = 200;
    let s = scratch().await;
    let (table_id, shard) = s.stream_table(ACCOUNT, "t", LABEL).await;
    s.write_run(&shard, &table_id, 5 * u64::from(PAGE)).await;
    let after = format!("{:021}", 2 * PAGE);
    let cases = [
        (
            "from the start",
            STREAM_RECORDS_FROM_START,
            format!("('{shard}', '{table_id}', {PAGE})"),
        ),
        (
            "after two pages",
            STREAM_RECORDS_AFTER,
            format!("('{shard}', '{table_id}', '{after}', {PAGE})"),
        ),
    ];

    let mut conn = s.db.acquire().await.expect("a connection");
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&mut *conn)
            .await
            .expect("set the plan mode");
        for (name, statement, arguments) in &cases {
            sqlx::raw_sql(&format!("PREPARE page_probe AS {statement}"))
                .execute(&mut *conn)
                .await
                .expect("prepare the statement");
            let plan = plan_of(&mut conn, &format!("EXECUTE page_probe{arguments}")).await;
            sqlx::raw_sql("DEALLOCATE page_probe")
                .execute(&mut *conn)
                .await
                .expect("deallocate the statement");

            assert_eq!(
                plan["Actual Rows"].as_f64(),
                Some(f64::from(PAGE)),
                "{mode}, {name}: the statement returns a full page:\n{plan:#}"
            );
            let visited = stream_records_rows_visited(&plan);
            assert!(
                visited <= f64::from(PAGE),
                "{mode}, {name}: the scans of stream_records visited {visited} rows for a \
                 page of {PAGE}:\n{plan:#}"
            );
        }
    }
    drop(conn);
    s.cleanup().await;
}
