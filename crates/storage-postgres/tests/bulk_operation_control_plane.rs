// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage-level tests for the `PostgreSQL` bulk operation catalog schema and
//! the start transaction's conflict recovery.
//!
//! Each test builds its own throwaway database, applies the shipped
//! migrations to it, and drops it when it passes. A failing test leaves its
//! database behind on purpose, named `eddb_bulk_pg_*`, so the state that
//! failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component, pointing at a server whose role may create and drop databases.
//! Without it every test here reports a skip and passes, the convention
//! `vector_control_plane.rs` set.

use extenddb_storage::{BulkOperationStore, InputFile, NewBulkOperation, StartOutcome};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

/// One scratch environment at a time across this test binary, so the engine
/// pool's ten-connection floor cannot exhaust a shared server.
async fn environment_permit() -> tokio::sync::OwnedSemaphorePermit {
    static ENVIRONMENTS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    std::sync::Arc::clone(
        ENVIRONMENTS.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(1))),
    )
    .acquire_owned()
    .await
    .expect("the environment semaphore is never closed")
}

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

fn skip(test: &str) {
    // In the CI job that provides PostgreSQL, an unset connection string is a
    // configuration error, not a skip: the summary line of a skipped run reads
    // the same as a real pass (cargo captures the message below), so the test
    // fails loudly instead. That job sets EXTENDDB_TEST_PG_REQUIRED; every
    // other job (plain `cargo test --workspace`, the MongoDB and Cassandra
    // legs) has no PostgreSQL and skips like the rest of this crate's suites.
    assert!(
        std::env::var("EXTENDDB_TEST_PG_REQUIRED").map_or(true, |v| v.is_empty()),
        "{test}: EXTENDDB_TEST_PG_REQUIRED is set but EXTENDDB_TEST_PG_CONNECTION_STRING is \
         not; the PostgreSQL bulk store tests would silently pass without a database"
    );
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

/// A throwaway database with every shipped migration applied.
struct Scratch {
    pool: PgPool,
    admin: PgPool,
    db_name: String,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Scratch {
    /// Drop the scratch database. Called only on the success path, so a
    /// failure leaves the database for inspection.
    async fn cleanup(self) {
        let Scratch {
            pool,
            admin,
            db_name,
            _permit,
        } = self;
        pool.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
        ))
        .execute(&admin)
        .await
        .expect("drop the scratch database");
        admin.close().await;
    }
}

/// Build a scratch database and apply the shipped migrations, catalog and
/// data, included from the crate rather than restated here: a test that
/// built its own idea of the schema could pass against a shape no deployment
/// has.
async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let permit = environment_permit().await;
    let db_name = format!("eddb_bulk_pg_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&format!("{base}/{db_name}"))
        .await
        .expect("connect to the scratch database");

    // The shipped catalog and data migration files, in name order, on one
    // database that stands in for both.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for dir in ["migrations", "data_migrations"] {
        let mut files: Vec<_> = std::fs::read_dir(root.join(dir))
            .expect("read a migrations directory")
            .map(|entry| entry.expect("a directory entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect();
        files.sort();
        for file in files {
            let sql = std::fs::read_to_string(&file).expect("read a migration file");
            sqlx::raw_sql(&sql)
                .execute(&pool)
                .await
                .unwrap_or_else(|e| panic!("apply {}: {e}", file.display()));
        }
    }

    Scratch {
        pool,
        admin,
        db_name,
        _permit: permit,
    }
}

/// Insert a `bulk_operations` row with the given identity and status, leaving
/// the defaulted columns to the schema. Runs on any executor so the step 5
/// test can use it inside an uncommitted transaction.
async fn insert_operation<'e, E>(
    executor: E,
    account_id: &str,
    operation_id: &str,
    table_id: &str,
    status: &str,
    token_hash: Option<&str>,
    fingerprint: &str,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "INSERT INTO bulk_operations (\
             account_id, operation_id, table_id, table_name, table_arn, status, \
             client_token, client_token_hash, request_fingerprint, \
             input_config, output_config, input_files, \
             start_time_ms, total_input_bytes, usage, end_time_ms) \
         VALUES ($1, $2, $3, 'orders', $4, $5, $6, $6, $7, \
             '{}'::jsonb, '{}'::jsonb, '[]'::jsonb, 1, 0, '{}'::jsonb, \
             CASE WHEN $5 IN ('IN_PROGRESS', 'CANCELLING') THEN NULL ELSE 2 END)",
    )
    .bind(account_id)
    .bind(operation_id)
    .bind(table_id)
    .bind(format!(
        "arn:aws:dynamodb:{REGION}:{account_id}:table/orders"
    ))
    .bind(status)
    .bind(token_hash)
    .bind(fingerprint)
    .execute(executor)
    .await
    .map(|_| ())
}

/// A start request against the given table with the given token hash.
fn new_operation(
    operation_id: &str,
    table_id: &str,
    token_hash: &str,
    fingerprint: &str,
) -> NewBulkOperation {
    NewBulkOperation {
        operation_id: operation_id.to_owned(),
        table_id: table_id.to_owned(),
        table_name: "orders".to_owned(),
        table_arn: format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/orders"),
        client_token: Some("token".to_owned()),
        client_token_hash: Some(token_hash.to_owned()),
        request_fingerprint: fingerprint.to_owned(),
        input_config: serde_json::json!({}),
        output_config: serde_json::json!({}),
        input_files: vec![InputFile {
            path: "data/part-000.json".to_owned(),
            size: 10,
            rows: None,
            etag: None,
            abandon: None,
        }],
        max_write_request_units: None,
        performance_tier: None,
        start_time_ms: 1,
        total_input_bytes: Some(10),
        initial_failure: None,
        run_context: None,
    }
}

/// The shipped migration files create the five bulk tables.
#[tokio::test]
async fn the_shipped_migrations_create_the_bulk_tables() {
    if base_conn().is_none() {
        skip("the_shipped_migrations_create_the_bulk_tables");
        return;
    }
    let scratch = scratch().await;

    for table in [
        "bulk_operations",
        "bulk_operation_input_files",
        "bulk_operation_report_records",
        "bulk_row_outcomes",
        "bulk_write_owners",
    ] {
        let (exists,): (bool,) = sqlx::query_as("SELECT to_regclass('public.' || $1) IS NOT NULL")
            .bind(table)
            .fetch_one(&scratch.pool)
            .await
            .expect("check a migrated table");
        assert!(exists, "{table} must exist after the migrations");
    }

    scratch.cleanup().await;
}

/// The partial unique index admits one non-terminal row per table: a second
/// one is rejected by the index by name, while a terminal row and another
/// table's row both pass.
#[tokio::test]
async fn the_table_guard_rejects_a_second_non_terminal_row() {
    if base_conn().is_none() {
        skip("the_table_guard_rejects_a_second_non_terminal_row");
        return;
    }
    let scratch = scratch().await;

    insert_operation(
        &scratch.pool,
        ACCOUNT,
        "op-1",
        "table-1",
        "IN_PROGRESS",
        None,
        "fp-1",
    )
    .await
    .expect("the first non-terminal row inserts");

    let second = insert_operation(
        &scratch.pool,
        ACCOUNT,
        "op-2",
        "table-1",
        "CANCELLING",
        None,
        "fp-2",
    )
    .await
    .expect_err("a second non-terminal row for the same table must be rejected");
    assert!(
        second.to_string().contains("idx_bulk_ops_table_guard"),
        "the rejection must come from the table guard index: {second}"
    );

    // Terminal rows are outside the index predicate, so history accumulates.
    insert_operation(
        &scratch.pool,
        ACCOUNT,
        "op-3",
        "table-1",
        "COMPLETED",
        None,
        "fp-3",
    )
    .await
    .expect("a terminal row for the same table inserts");
    // The guard scopes by table, not by account.
    insert_operation(
        &scratch.pool,
        ACCOUNT,
        "op-4",
        "table-2",
        "IN_PROGRESS",
        None,
        "fp-4",
    )
    .await
    .expect("a non-terminal row for another table inserts");

    scratch.cleanup().await;
}

/// Step 5 of the start transaction: a same-token insert that hits the token
/// index returns `IdempotentReplay`, not an error, and the transaction is not
/// aborted.
///
/// The advisory lock serialises starts within an account, so two API starts
/// cannot race into step 4. To force the token conflict past the step 2
/// lookup, the conflicting row is inserted by a raw transaction that holds no
/// advisory lock and commits only after the start is observed blocked on the
/// in-flight index entry. The start's step 2 then saw nothing, its insert
/// conflicts on commit, and the outcome proves the transaction survived to
/// run step 5's lookup: a plain insert in its place would answer with a
/// unique violation instead.
#[tokio::test]
async fn a_same_token_conflict_at_insert_recovers_to_idempotent_replay() {
    if base_conn().is_none() {
        skip("a_same_token_conflict_at_insert_recovers_to_idempotent_replay");
        return;
    }
    let base = base_conn().expect("checked above");
    let scratch = scratch().await;

    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: format!("{base}/{}", scratch.db_name),
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database");

    // The conflicting row: same account, same token hash, same fingerprint,
    // a different table, so the only index the start can hit is the token's.
    // The start's own table exists: the start transaction refuses a table
    // whose row is absent.
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, 'control-plane')")
        .bind(ACCOUNT)
        .execute(&scratch.pool)
        .await
        .expect("seed the account");
    sqlx::query(
        "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, \
             table_status, table_arn, table_id) \
         VALUES ($1, 'orders', '[]', '[]', 'ACTIVE', $2, 'table-2')",
    )
    .bind(ACCOUNT)
    .bind(format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/orders"))
    .execute(&scratch.pool)
    .await
    .expect("seed the start's table");
    let mut raw_tx = scratch
        .pool
        .begin()
        .await
        .expect("begin the raw transaction");
    insert_operation(
        &mut *raw_tx,
        ACCOUNT,
        "op-first",
        "table-1",
        "IN_PROGRESS",
        Some("hash-1"),
        "fp-1",
    )
    .await
    .expect("insert the uncommitted same-token row");

    let start = engine.start_bulk_operation(
        ACCOUNT,
        new_operation("op-second", "table-2", "hash-1", "fp-1"),
        10,
    );

    let db_name = scratch.db_name.clone();
    let admin = scratch.admin.clone();
    let release = async move {
        // Commit the raw transaction only once the start is blocked on the
        // in-flight index entry, which pins the conflict to step 4: an
        // earlier commit would let step 2 answer and the test would not
        // exercise the recovery it exists for.
        let mut waited = 0u32;
        loop {
            let (blocked,): (bool,) = sqlx::query_as(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                 WHERE datname = $1 AND wait_event_type = 'Lock' \
                   AND query LIKE 'INSERT INTO bulk_operations%')",
            )
            .bind(&db_name)
            .fetch_one(&admin)
            .await
            .expect("poll pg_stat_activity");
            if blocked {
                break;
            }
            waited += 1;
            assert!(waited < 200, "the start never blocked on the token index");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        raw_tx.commit().await.expect("commit the raw transaction");
    };

    let (outcome, ()) = tokio::join!(start, release);
    match outcome.expect("the same-token conflict must not surface as an error") {
        StartOutcome::IdempotentReplay(record) => {
            assert_eq!(record.operation_id, "op-first");
            assert_eq!(record.request_fingerprint, "fp-1");
        }
        StartOutcome::Created(_) => panic!("the token index did not fire"),
        StartOutcome::IdempotentMismatch => panic!("fingerprints are equal; mismatch is wrong"),
        StartOutcome::TableBusy => {
            panic!("a same-token start must never be answered as a table conflict")
        }
        StartOutcome::AccountLimitReached => panic!("the limit of 10 cannot be reached here"),
        StartOutcome::FailedStartLimit => panic!("this start carries no initial failure"),
        StartOutcome::IndexOperationInProgress => panic!("the table has no index"),
    }

    drop(engine);
    scratch.cleanup().await;
}
