// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage-level tests for what the control plane answers while a table is
//! `DELETING`.
//!
//! Amazon DynamoDB answers a second `DeleteTable` and an `UpdateTable` in that
//! window with `ResourceInUseException` and the sentence `Attempt to change a
//! resource which is still in use: Table is being deleted: <name>` (measured
//! 2026-10-24). The window is produced here by `delete_table` itself: with a
//! control-plane delay of one second or more the row is marked `DELETING` with
//! a scheduled transition, and these tests run no poller to finish it.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_delw_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.
//!
//! The data-plane tests below pin the other half of the window: DynamoDB serves
//! `PutItem`, `GetItem`, `Query` and `Scan` on a `DELETING` table until the
//! table is gone, then answers `ResourceNotFoundException` (measured
//! 2026-10-25). Two `DeleteTable` calls on one `ACTIVE` table get one `200` and
//! one `ResourceInUseException` with the `Table is being deleted` sentence.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteTableInput, Item,
    KeySchemaElement, KeyType, ReturnValuesOnConditionCheckFailure, ScalarAttributeType,
    TableKeyInfo, TableStatus, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine, TransactGetOp, TransactWriteOp};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

#[path = "deleting_window/ttl.rs"]
mod ttl;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_deleting_window";

struct Scratch {
    engine: PostgresEngine,
    catalog: PgPool,
    admin: PgPool,
    db_name: String,
}

impl Scratch {
    /// Drop the scratch database. Called only on the success path, so a failure
    /// leaves the database for inspection.
    async fn cleanup(self) {
        let Scratch {
            engine,
            catalog,
            admin,
            db_name,
        } = self;
        drop(engine);
        catalog.close().await;
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

fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

/// A scratch database with the shipped schema, a zero control-plane delay so
/// `create_table` completes inline, and an engine on it.
async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_delw_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();

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
    let catalog = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&catalog)
            .await
            .expect("apply a shipped migration");
    }
    set_delay(&catalog, "0").await;
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'index_propagation_delay_ms'")
        .execute(&catalog)
        .await
        .expect("pin the propagation delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&catalog)
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
        catalog,
        admin,
        db_name,
    }
}

async fn set_delay(catalog: &PgPool, seconds: &str) {
    sqlx::query("UPDATE settings SET value = $1 WHERE key = 'control_plane_delay_seconds'")
        .bind(seconds)
        .execute(catalog)
        .await
        .expect("set the control-plane delay");
}

async fn status_of(catalog: &PgPool, table: &str) -> Option<String> {
    sqlx::query_scalar("SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(table)
        .fetch_optional(catalog)
        .await
        .expect("read the table status")
}

fn update_input(table: &str) -> UpdateTableInput {
    UpdateTableInput {
        table_name: table.to_owned(),
        billing_mode: None,
        provisioned_throughput: None,
        deletion_protection_enabled: Some(true),
        global_secondary_index_updates: None,
        attribute_definitions: None,
        stream_specification: None,
        table_class: None,
        on_demand_throughput: None,
        table_throughput_mode: None,
        vector_index_updates: None,
    }
}

/// Create `table` with a single string hash key, completing inline (delay 0).
async fn create_table_named(s: &Scratch, table: &str) {
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: table.to_owned(),
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
    assert_eq!(
        status_of(&s.catalog, table).await.as_deref(),
        Some("ACTIVE")
    );
}

/// Delete `TABLE` with a delay long enough that the row stays `DELETING` for
/// the rest of the test: the window held open.
async fn start_deleting(s: &Scratch) {
    set_delay(&s.catalog, "600").await;
    let desc = s
        .engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("the first DeleteTable");
    assert_eq!(desc.table_status, TableStatus::Deleting);
    assert_eq!(
        status_of(&s.catalog, TABLE).await.as_deref(),
        Some("DELETING"),
        "the row must be DELETING for the rest of the test"
    );
}

/// Create `TABLE`, then delete it with the window held open.
async fn open_window(s: &Scratch) {
    create_table_named(s, TABLE).await;
    start_deleting(s).await;
}

fn item(pk: &str) -> Item {
    BTreeMap::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

fn query_pk(pk: &str) -> (KeyCondition, ExpressionMaps) {
    let condition = KeyCondition {
        pk_path: vec![PathElement::Attribute("pk".to_owned())],
        pk_value: Expr::Placeholder(":pk".to_owned()),
        extra_pk_conditions: Vec::new(),
        sk_condition: None,
        extra_sk_conditions: Vec::new(),
    };
    let maps = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([(":pk".to_owned(), AttributeValue::S(pk.to_owned()))]),
    );
    (condition, maps)
}

/// Create `TABLE`, write the item `pre`, and open the window. Returns the key
/// info read while the row was still ACTIVE, the snapshot a cache would hold.
async fn open_window_with_item(s: &Scratch) -> TableKeyInfo {
    create_table_named(s, TABLE).await;
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("key info of the ACTIVE table");
    s.engine
        .put_item(
            &key_info,
            item("pre"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("seed the pre-existing item");
    start_deleting(s).await;
    key_info
}

/// A second DeleteTable during the window reports the table as being deleted,
/// not as absent, and leaves the row in place for the deletion under way.
#[tokio::test]
async fn a_second_delete_table_during_the_window_is_table_being_deleted() {
    if base_conn().is_none() {
        return skip("a_second_delete_table_during_the_window_is_table_being_deleted");
    }
    let s = scratch().await;
    open_window(&s).await;

    let err = s
        .engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect_err("DeleteTable on a DELETING table");
    assert!(
        matches!(&err, StorageError::TableBeingDeleted(name) if name == TABLE),
        "expected TableBeingDeleted({TABLE:?}), got {err:?}"
    );
    assert_eq!(
        status_of(&s.catalog, TABLE).await.as_deref(),
        Some("DELETING")
    );

    s.cleanup().await;
}

/// UpdateTable during the window reports the table as being deleted, not with
/// the sentence for the other non-ACTIVE statuses, and changes nothing.
#[tokio::test]
async fn update_table_during_the_window_is_table_being_deleted() {
    if base_conn().is_none() {
        return skip("update_table_during_the_window_is_table_being_deleted");
    }
    let s = scratch().await;
    open_window(&s).await;

    let err = s
        .engine
        .update_table(ACCOUNT, update_input(TABLE))
        .await
        .expect_err("UpdateTable on a DELETING table");
    assert!(
        matches!(&err, StorageError::TableBeingDeleted(name) if name == TABLE),
        "expected TableBeingDeleted({TABLE:?}), got {err:?}"
    );
    let protected: bool = sqlx::query_scalar(
        "SELECT deletion_protection_enabled FROM tables WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .fetch_one(&s.catalog)
    .await
    .expect("read the flag");
    assert!(!protected, "a refused UpdateTable must not have written");

    s.cleanup().await;
}

/// A CREATING table keeps the existing answer: the new sentence is for
/// DELETING only.
#[tokio::test]
async fn update_table_on_a_creating_table_keeps_table_not_active() {
    if base_conn().is_none() {
        return skip("update_table_on_a_creating_table_keeps_table_not_active");
    }
    let s = scratch().await;
    open_window(&s).await;
    sqlx::query(
        "UPDATE tables SET table_status = 'CREATING' WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .execute(&s.catalog)
    .await
    .expect("mark CREATING");

    let err = s
        .engine
        .update_table(ACCOUNT, update_input(TABLE))
        .await
        .expect_err("UpdateTable on a CREATING table");
    assert!(
        matches!(&err, StorageError::TableNotActive(name) if name == TABLE),
        "expected TableNotActive({TABLE:?}), got {err:?}"
    );

    s.cleanup().await;
}

/// During the window the key-info read finds the table, and PutItem, GetItem
/// (the pre-existing item and the one just written), Query and Scan are
/// served, as the service serves them.
#[tokio::test]
async fn data_plane_is_served_during_the_window() {
    if base_conn().is_none() {
        return skip("data_plane_is_served_during_the_window");
    }
    let s = scratch().await;
    open_window_with_item(&s).await;
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("a DELETING table is found by the data plane");

    s.engine
        .put_item(
            &key_info,
            item("w1"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("PutItem during the window");
    let got = s
        .engine
        .get_item(&key_info, &item("pre"))
        .await
        .expect("GetItem of the pre-existing item");
    assert_eq!(got, Some(item("pre")));
    let got = s
        .engine
        .get_item(&key_info, &item("w1"))
        .await
        .expect("GetItem of the window write");
    assert_eq!(got, Some(item("w1")));
    let (condition, maps) = query_pk("pre");
    let (rows, _) = s
        .engine
        .query(&key_info, &condition, &maps, true, None, None, None)
        .await
        .expect("Query during the window");
    assert_eq!(rows, vec![item("pre")]);
    let (rows, _) = s
        .engine
        .scan(&key_info, Some(1), None, None, None, None)
        .await
        .expect("Scan during the window");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        status_of(&s.catalog, TABLE).await.as_deref(),
        Some("DELETING")
    );

    s.cleanup().await;
}

/// Once the data table is dropped while the row still reads DELETING, a
/// request that reaches storage, here with the key info read before the
/// delete (the snapshot a cache would hold), answers the table as not found
/// rather than as an internal error.
#[tokio::test]
async fn a_request_after_the_data_drop_is_table_not_found() {
    if base_conn().is_none() {
        return skip("a_request_after_the_data_drop_is_table_not_found");
    }
    let s = scratch().await;
    let key_info = open_window_with_item(&s).await;
    sqlx::query(&format!("DROP TABLE \"_ddb_{}\"", key_info.table_id))
        .execute(&s.catalog)
        .await
        .expect("drop the data table under the DELETING row");

    let err = s
        .engine
        .put_item(
            &key_info,
            item("w1"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect_err("PutItem after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {err:?}"
    );
    let err = s
        .engine
        .get_item(&key_info, &item("pre"))
        .await
        .expect_err("GetItem after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {err:?}"
    );
    // The transaction paths run outside the single-request statements, so
    // they are funnelled at their own entry points.
    let pre = item("pre");
    let err = s
        .engine
        .transact_get_items(&[TransactGetOp {
            key_info: &key_info,
            key: &pre,
        }])
        .await
        .expect_err("TransactGetItems after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {err:?}"
    );
    let maps = ExpressionMaps::default();
    let w2 = item("w2");
    let err = s
        .engine
        .transact_write_items(
            &[TransactWriteOp::Put {
                key_info: &key_info,
                item: &w2,
                condition: None,
                maps: &maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }],
            None,
        )
        .await
        .expect_err("TransactWriteItems after the data drop");
    assert!(
        matches!(&err, StorageError::TableNotFound(name) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {err:?}"
    );

    s.cleanup().await;
}

/// Two concurrent DeleteTable calls on one ACTIVE table: the first locks the
/// row `FOR UPDATE` and marks it DELETING; the second, blocked on that lock,
/// re-reads the row after the commit, finds it excluded by the status filter,
/// and answers TableBeingDeleted from the unfiltered lookup. Ten rounds, a
/// fresh table each; one row per table, DELETING, and its data table present.
#[tokio::test]
async fn two_concurrent_delete_tables_have_one_winner() {
    if base_conn().is_none() {
        return skip("two_concurrent_delete_tables_have_one_winner");
    }
    let s = scratch().await;
    let engine = Arc::new(s.engine.clone());

    for round in 0..10 {
        let table = format!("t_pair_{round}");
        set_delay(&s.catalog, "0").await;
        create_table_named(&s, &table).await;
        set_delay(&s.catalog, "600").await;

        let tasks: Vec<_> = (0..2)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let table = table.clone();
                tokio::spawn(async move {
                    engine
                        .delete_table(ACCOUNT, DeleteTableInput { table_name: table })
                        .await
                })
            })
            .collect();
        let mut ok = 0;
        let mut being_deleted = 0;
        for task in tasks {
            match task.await.expect("task") {
                Ok(_) => ok += 1,
                Err(StorageError::TableBeingDeleted(name)) if name == table => being_deleted += 1,
                Err(other) => panic!("round {round}: unexpected {other:?}"),
            }
        }
        assert_eq!((ok, being_deleted), (1, 1), "round {round}");

        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT table_status, table_id FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(ACCOUNT)
        .bind(&table)
        .fetch_all(&s.catalog)
        .await
        .expect("rows");
        assert_eq!(rows.len(), 1, "round {round}: one catalog row");
        assert_eq!(rows[0].0, "DELETING", "round {round}");
        let present: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(format!("\"_ddb_{}\"", rows[0].1))
            .fetch_one(&s.catalog)
            .await
            .expect("to_regclass");
        assert!(
            present.is_some(),
            "round {round}: the data table is still there"
        );
    }

    drop(engine);
    s.cleanup().await;
}
