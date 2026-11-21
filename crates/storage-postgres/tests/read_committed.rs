// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The sessions this backend serves from run at READ COMMITTED whatever
//! `default_transaction_isolation` the database, the role or the connection
//! string sets.
//!
//! The test migrates a scratch catalog and data database, then sets
//! REPEATABLE READ as their default (`ALTER DATABASE`), which every session
//! opened after it starts with. A trigger records, in `isolation_seen`, the
//! `transaction_isolation` of each statement that writes a probed table,
//! inside the transaction the store itself opened. See `migrations_support`
//! for the server this test needs.

mod migrations_support;

use std::collections::BTreeMap;

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType,
};
use extenddb_storage::migrations::MigrationId;
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine, PostgresStorageConfig};
use migrations_support::{ACCOUNT, Scratch, base_conn, skip};
use sqlx::{Connection, PgConnection, PgPool};

const REGION: &str = "us-east-1";
const TABLE: &str = "t_read_committed";
const POOL_SIZE: u32 = 10;

/// The probe: `isolation_seen` and the trigger function that fills it.
const PROBE: &str = "CREATE TABLE isolation_seen (tbl text NOT NULL, level text NOT NULL); \
     CREATE FUNCTION record_isolation() RETURNS trigger LANGUAGE plpgsql AS $$ \
     BEGIN \
       INSERT INTO isolation_seen VALUES (TG_TABLE_NAME, current_setting('transaction_isolation')); \
       RETURN NULL; \
     END $$";

/// A migrated catalog and data database, each with the probe, whose default
/// isolation is REPEATABLE READ from here on. Returns the scratch and a pool
/// on each database.
async fn repeatable_read_scratch(base: &str) -> (Scratch, PgPool, PgPool) {
    let s = Scratch::new(base, "pgrc").await;
    s.migrate_through(MigrationId::LATEST).await;
    let (catalog, data) = (s.catalog().await, s.data().await);
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, 'read committed')")
        .bind(ACCOUNT)
        .execute(&catalog)
        .await
        .expect("seed the account row");
    // No control plane runs here, so a table goes ACTIVE in CreateTable.
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&catalog)
    .await
    .expect("pin the control-plane delay to zero");
    // The server's stores read the encryption key `init` writes.
    let key = extenddb_storage::bootstrapper::helpers::generate_encryption_key()
        .expect("generate the encryption key");
    sqlx::query("INSERT INTO settings (key, value) VALUES ('encryption_key', $1)")
        .bind(&key)
        .execute(&catalog)
        .await
        .expect("seed the encryption key");
    for (pool, name) in [(&catalog, &s.catalog_name), (&data, &s.data_name)] {
        sqlx::raw_sql(PROBE)
            .execute(pool)
            .await
            .expect("create the probe");
        sqlx::query(&format!(
            "ALTER DATABASE \"{name}\" SET default_transaction_isolation = 'repeatable read'"
        ))
        .execute(pool)
        .await
        .expect("set the default isolation level");
        // A session that sets nothing starts with the database's default, so
        // what the stores read below is their own setting.
        let mut fresh = PgConnection::connect(&format!("{base}/{name}"))
            .await
            .expect("open a session");
        let level: String = sqlx::query_scalar("SHOW default_transaction_isolation")
            .fetch_one(&mut fresh)
            .await
            .expect("read the default");
        assert_eq!(level, "repeatable read", "the default of {name}");
        fresh.close().await.expect("close the session");
    }
    (s, catalog, data)
}

/// Record the isolation of each statement that writes `table`.
async fn probe(pool: &PgPool, table: &str) {
    sqlx::raw_sql(&format!(
        "CREATE TRIGGER record_isolation AFTER INSERT OR UPDATE OR DELETE ON \"{table}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION record_isolation()"
    ))
    .execute(pool)
    .await
    .expect("attach the probe");
}

/// Assert that `row` wrote `table` and that each of its statements ran at
/// READ COMMITTED, and clear what the probe recorded.
async fn assert_read_committed(pool: &PgPool, table: &str, row: &str) {
    let mut levels: Vec<String> =
        sqlx::query_scalar("DELETE FROM isolation_seen WHERE tbl = $1 RETURNING level")
            .bind(table)
            .fetch_all(pool)
            .await
            .expect("read the probe");
    levels.sort();
    levels.dedup();
    assert_eq!(
        levels,
        ["read committed"],
        "{row}: the levels of the statements that wrote {table}"
    );
}

fn item(pk: &str) -> Item {
    BTreeMap::from([("pk".to_owned(), AttributeValue::S(pk.to_owned()))])
}

/// Each store of a server, and the stores of the settings and account
/// commands, writes at READ COMMITTED; a job's claim session and every
/// connection of the engine's data pool start at it.
#[tokio::test]
async fn every_store_writes_at_read_committed_under_a_repeatable_read_default() {
    let Some(base) = base_conn() else {
        return skip("every_store_writes_at_read_committed_under_a_repeatable_read_default");
    };
    let (s, catalog, data) = repeatable_read_scratch(&base).await;
    let config = PostgresStorageConfig {
        connection_string: s.catalog_url(),
        pool_size: POOL_SIZE,
        catalog_pool_size: None,
    };
    let mut options = ServerComponentsOptions::default();
    options.startup_recovery = false;
    let started =
        (extenddb_storage_postgres::backend().server_components)(&config, REGION, options)
            .await
            .unwrap_or_else(|e| panic!("start the server's stores: {e}"));
    let engine = &started.engine;
    for table in ["tables", "accounts", "settings"] {
        probe(&catalog, table).await;
    }

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
    assert_read_committed(&catalog, "tables", "CreateTable, the engine's catalog pool").await;

    let key_info = engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("read the key info");
    let data_table = format!("_ddb_{}", key_info.table_id);
    probe(&data, &data_table).await;
    let maps = ExpressionMaps::default();
    let tokens = expression::tokenize("attribute_not_exists(pk)").expect("tokenize");
    let condition = expression::parse_condition(&tokens).expect("parse");
    engine
        .put_item(&key_info, item("a"), false, Some(&condition), &maps, None)
        .await
        .expect("the conditional put");
    assert_read_committed(
        &data,
        &data_table,
        "a conditional PutItem, a transaction on the engine's data pool",
    )
    .await;
    engine
        .put_item(&key_info, item("b"), false, None, &maps, None)
        .await
        .expect("the put");
    assert_read_committed(
        &data,
        &data_table,
        "a PutItem outside a transaction, on the engine's data pool",
    )
    .await;

    started
        .catalog_store
        .create_account("111122223333", "server store")
        .await
        .expect("create an account through the server's catalog store");
    assert_read_committed(&catalog, "accounts", "the server's catalog store").await;

    let management = (extenddb_storage_postgres::backend().management_store)(&s.catalog_url())
        .await
        .unwrap_or_else(|e| panic!("open the management store: {e}"));
    management
        .create_account("444455556666", "account command")
        .await
        .expect("create an account through the management store");
    assert_read_committed(&catalog, "accounts", "the management store").await;

    let settings = (extenddb_storage_postgres::backend().settings_store)(&s.catalog_url())
        .await
        .unwrap_or_else(|e| panic!("open the settings store: {e}"));
    settings
        .set_setting("read_committed_probe", "1")
        .await
        .expect("set a setting");
    assert_read_committed(&catalog, "settings", "the settings store").await;

    let claim = extenddb_storage_postgres::claim_session_settings(
        &catalog,
        "read-committed-probe",
        &["default_transaction_isolation"],
    )
    .await
    .expect("open a claim session")
    .expect("no other session holds the probe's lock");
    assert_eq!(claim, ["read committed"], "a job's claim session");

    // Every connection of the data pool, not only the first one. The
    // server's engine is a trait object without `data_pool()`, so this
    // opens a `PostgresEngine` of its own on the same databases.
    let pool_engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: s.catalog_url(),
            pool_size: POOL_SIZE,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch databases");
    let mut held = Vec::new();
    for _ in 0..POOL_SIZE {
        held.push(
            pool_engine
                .data_pool()
                .acquire()
                .await
                .expect("take a connection"),
        );
    }
    let mut levels = Vec::new();
    for conn in &mut held {
        let mut tx = conn.begin().await.expect("begin");
        let level: String = sqlx::query_scalar("SHOW transaction_isolation")
            .fetch_one(&mut *tx)
            .await
            .expect("read the level");
        tx.rollback().await.expect("roll back");
        levels.push(level);
    }
    assert_eq!(
        levels,
        vec!["read committed"; POOL_SIZE as usize],
        "the level of a transaction on each of the {POOL_SIZE} connections of the engine's \
         data pool"
    );

    catalog.close().await;
    data.close().await;
}
