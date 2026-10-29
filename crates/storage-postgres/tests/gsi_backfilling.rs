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
//! drop databases. Without it the test reports a skip and passes.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DescribeTableInput,
    KeySchemaElement, KeyType, ScalarAttributeType, UpdateTableInput,
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

/// What DescribeTable says about `gidx`: its status and its serialised
/// `Backfilling` member, or `None` before the catalog row is visible.
async fn gsi_state(engine: &PostgresEngine) -> Option<(String, Option<serde_json::Value>)> {
    let desc = engine
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

#[tokio::test]
async fn a_gsi_added_by_update_table_walks_both_creating_phases() {
    let Some(base) = base_conn() else {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
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
        .max_connections(2)
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

    let pk = |name: &str, key_type| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    };
    let def = |name: &str| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    };
    engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: TABLE.to_owned(),
                key_schema: vec![pk("pk", KeyType::Hash)],
                attribute_definitions: vec![def("pk")],
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

    sqlx::query("INSERT INTO settings (key, value) VALUES ($1, '1500')")
        .bind(extenddb_core::settings_keys::gsi_creating_phase_delay_key(
            TABLE,
        ))
        .execute(&db)
        .await
        .expect("set the phase hold");

    // UpdateTable returns once the index is ACTIVE on this backend, so it runs
    // in its own task while this one watches DescribeTable.
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
    let updater = engine.clone();
    let update = tokio::spawn(async move { updater.update_table(ACCOUNT, input).await });

    // Phases seen, in order: 1 is Backfilling false, 2 is true, 3 is ACTIVE.
    // A reading counts only when DescribeTable agrees before and after the
    // read, so a phase change between the two calls cannot pair one phase's
    // description with the other's refusal.
    let mut seen: Vec<u8> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while seen.last() != Some(&3) {
        assert!(Instant::now() < deadline, "no ACTIVE state; seen {seen:?}");
        let before = gsi_state(&engine).await;
        let read = engine
            .index_info_by_table_id(&key_info.table_id, "gidx")
            .await;
        if before.is_none() || gsi_state(&engine).await != before {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let phase = match before.expect("checked above") {
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
        };
        if seen.last() != Some(&phase) {
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
