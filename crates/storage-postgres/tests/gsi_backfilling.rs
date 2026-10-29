// SPDX-License-Identifier: Apache-2.0
//! A GSI added by `UpdateTable` walks the service's two `CREATING` phases.
//!
//! Measured 2026-10-26 (see `extenddb_storage::gsi_backfill`): `Backfilling`
//! is `false` and a read is told the table has no such index until the
//! backfill starts, then `true` with the backfilling sentence until the index
//! is `ACTIVE`, when the member is gone and reads are served. The table's
//! `gsi_creating_phase_delay_ms` hold lengthens both phases so the walk is
//! observable on a three-item table.
//!
//! Each test builds its own throwaway database from the shipped migrations and
//! drops it when it passes; a failing test leaves it behind, named
//! `eddb_gsibf_*`. Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL
//! with no database component, pointing at a server whose role may create and
//! drop databases. Without it each test reports a skip and passes.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DescribeTableInput,
    KeySchemaElement, KeyType, ScalarAttributeType, TableDescription, TableKeyInfo,
    UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const TABLE: &str = "t_gsi_backfilling";
const BACKFILLING: &str = "Cannot read from backfilling global secondary index: gidx";

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
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

/// A scratch database holding one three-item table, an engine on it running
/// the propagation workers, and the table's phase hold.
struct Scratch {
    admin: sqlx::PgPool,
    db: sqlx::PgPool,
    db_name: String,
    engine: PostgresEngine,
    key_info: TableKeyInfo,
}

impl Scratch {
    async fn new(base: &str, hold_ms: u32) -> Self {
        let db_name = format!("eddb_gsibf_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
        // One scratch database stands in for both the catalog and the data database.
        for sql in shipped_migrations() {
            sqlx::raw_sql(&sql)
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
            "us-east-1",
        )
        .await
        .expect("open a PostgresEngine on the scratch database")
        .start_gsi_workers();

        engine
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
        let key_info = engine
            .table_key_info(ACCOUNT, TABLE)
            .await
            .expect("read the key info");
        for n in 0..3 {
            let item = BTreeMap::from([
                ("pk".to_owned(), AttributeValue::S(format!("p{n}"))),
                ("g".to_owned(), AttributeValue::S("v".to_owned())),
            ]);
            engine
                .put_item(
                    &key_info,
                    item,
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
                .expect("put");
        }

        sqlx::query("INSERT INTO settings (key, value) VALUES ($1, $2)")
            .bind(extenddb_core::settings_keys::gsi_creating_phase_delay_key(
                TABLE,
            ))
            .bind(hold_ms.to_string())
            .execute(&db)
            .await
            .expect("set the phase hold");

        Self {
            admin,
            db,
            db_name,
            engine,
            key_info,
        }
    }

    /// Start `UpdateTable` adding `gidx` in its own task. It returns once the
    /// index is ACTIVE on this backend, or once a passing build failure has
    /// left it to the recovery pass.
    fn add_gsi(&self) -> tokio::task::JoinHandle<Result<TableDescription, StorageError>> {
        let input: UpdateTableInput = serde_json::from_value(serde_json::json!({
            "TableName": TABLE,
            "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "gidx",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }}],
        }))
        .expect("build the UpdateTable input");
        let updater = self.engine.clone();
        tokio::spawn(async move { updater.update_table(ACCOUNT, input).await })
    }

    /// What DescribeTable says about `gidx`: its status and its serialised
    /// `Backfilling` member, or `None` before the catalog row is visible.
    async fn gsi_state(&self) -> Option<(String, Option<serde_json::Value>)> {
        let desc = self
            .engine
            .describe_table(
                ACCOUNT,
                DescribeTableInput {
                    table_name: TABLE.to_owned(),
                },
            )
            .await
            .expect("describe the table");
        let gsi = desc
            .global_secondary_indexes?
            .into_iter()
            .find(|g| g.index_name == "gidx")?;
        let wire = serde_json::to_value(&gsi).expect("serialise the GSI description");
        Some((gsi.index_status, wire.get("Backfilling").cloned()))
    }

    /// The phase of `gidx`: 1 is `Backfilling` false, 2 is true, 3 is ACTIVE.
    ///
    /// Reads the index between two DescribeTable calls and checks the read
    /// gets the answer the description implies. `None` when the index is not
    /// visible yet, or when the two calls disagree, so a phase change between
    /// them cannot pair one phase's description with the other's refusal.
    async fn phase(&self) -> Option<u8> {
        let before = self.gsi_state().await?;
        let read = self
            .engine
            .index_info_by_table_id(&self.key_info.table_id, "gidx")
            .await;
        if self.gsi_state().await.as_ref() != Some(&before) {
            return None;
        }
        Some(match before {
            (status, Some(serde_json::Value::Bool(false))) if status == "CREATING" => {
                assert!(
                    matches!(read, Err(StorageError::IndexNotFound(ref n)) if n == "gidx"),
                    "before the backfill a read is told there is no such index: {read:?}"
                );
                1
            }
            (status, Some(serde_json::Value::Bool(true))) if status == "CREATING" => {
                assert!(
                    matches!(read, Err(StorageError::Validation(ref m)) if m == BACKFILLING),
                    "during the backfill a read gets the backfilling sentence: {read:?}"
                );
                2
            }
            (status, None) if status == "ACTIVE" => {
                read.expect("an ACTIVE index resolves");
                3
            }
            other => panic!("a state the service does not report: {other:?}"),
        })
    }

    async fn cleanup(self) {
        drop(self.engine);
        self.db.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            self.db_name
        ))
        .execute(&self.admin)
        .await
        .expect("drop the scratch database");
        self.admin.close().await;
    }
}

#[tokio::test]
async fn a_gsi_added_by_update_table_walks_both_creating_phases() {
    let Some(base) = base_conn() else {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
    let s = Scratch::new(&base, 1500).await;
    let update = s.add_gsi();

    let mut seen: Vec<u8> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while seen.last() != Some(&3) {
        assert!(Instant::now() < deadline, "no ACTIVE state; seen {seen:?}");
        if let Some(phase) = s.phase().await
            && seen.last() != Some(&phase)
        {
            assert!(
                seen.last().is_none_or(|&last| last < phase),
                "phase {phase} after {seen:?}"
            );
            seen.push(phase);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(seen, vec![1, 2, 3], "each phase is observed, in order");
    update
        .await
        .expect("the UpdateTable task")
        .expect("UpdateTable succeeds");

    s.cleanup().await;
}

/// A build that fails for a passing reason leaves its index CREATING for the
/// recovery pass. The index stays in its second phase through the wait and the
/// retry, until the retry publishes it.
///
/// The failure is a real one: the test locks the index's catalog row so the
/// flip to ACTIVE waits on it, then cancels the waiting statement, which is
/// `SQLSTATE 57014`, a failure the build treats as passing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_passing_build_failure_keeps_the_index_backfilling_until_the_retry_publishes_it() {
    let Some(base) = base_conn() else {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
    let s = Scratch::new(&base, 1500).await;
    let update = s.add_gsi();

    let deadline = Instant::now() + Duration::from_secs(30);
    while s.phase().await != Some(2) {
        assert!(Instant::now() < deadline, "the backfill never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut lock = s.db.begin().await.expect("begin the lock transaction");
    sqlx::query("SELECT 1 FROM indexes WHERE index_name = 'gidx' FOR UPDATE")
        .execute(&mut *lock)
        .await
        .expect("lock the index row");
    let flip = loop {
        let waiting: Option<(i32,)> = sqlx::query_as(
            "SELECT pid FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
               AND query LIKE 'UPDATE indexes SET index_status = ''ACTIVE''%'",
        )
        .fetch_optional(&s.db)
        .await
        .expect("read pg_stat_activity");
        if let Some((pid,)) = waiting {
            break pid;
        }
        assert!(
            Instant::now() < deadline,
            "the flip to ACTIVE never waited on the lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(flip)
        .fetch_one(&s.db)
        .await
        .expect("cancel the flip");
    assert!(cancelled, "the flip's backend was signalled");
    lock.rollback().await.expect("release the index row");

    // UpdateTable succeeds and reports the index it leaves to the recovery pass.
    let returned = update
        .await
        .expect("the UpdateTable task")
        .expect("a passing build failure is not an UpdateTable failure");
    let gsi = returned
        .global_secondary_indexes
        .unwrap_or_default()
        .into_iter()
        .find(|g| g.index_name == "gidx")
        .expect("gidx in the UpdateTable response");
    assert_eq!(
        (gsi.index_status.as_str(), gsi.backfilling),
        ("CREATING", Some(true)),
        "the UpdateTable response after the failure"
    );
    for _ in 0..10 {
        assert_eq!(
            s.phase().await,
            Some(2),
            "waiting for the retry, the index is still backfilling"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The pass the server's recovery worker runs every minute.
    let engine = s.engine.clone();
    let retry =
        tokio::spawn(
            async move { extenddb_storage_postgres::reconcile_creating_gsis(&engine).await },
        );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match s.phase().await {
            Some(3) => break,
            Some(2) | None => {}
            Some(phase) => panic!("phase {phase} during the retry"),
        }
        assert!(
            Instant::now() < deadline,
            "the retry never published the index"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        retry
            .await
            .expect("the recovery task")
            .expect("the recovery pass"),
        1,
        "the retry published the index"
    );

    s.cleanup().await;
}
