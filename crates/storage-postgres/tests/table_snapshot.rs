// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` on `PostgreSQL`: the shared consistency check from
//! `extenddb_storage::snapshot_conformance` against real scratch databases,
//! plus the PostgreSQL-specific marker and key-order checks.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL with no database
//! name); skips otherwise, and fails instead when `EXTENDDB_TEST_PG_REQUIRED`
//! is set. Each test creates its own catalog and data databases and drops
//! them when its `Scratch` is dropped, on success or panic (a killed test
//! binary leaves them behind). Most tests run on both layouts the engine
//! supports (`on_both_layouts!`): a separate data database, and one database
//! shared by catalog and data, where the open uses one transaction.

use extenddb_storage::snapshot::paging::OpenStage;
use extenddb_storage::snapshot_conformance::{self, PageSize};
use extenddb_storage::{SnapshotEngine, TableEngine};
use extenddb_storage_postgres::{
    PostgresConfig, PostgresEngine, apply_all_migrations, data_table_name, index_table_name,
    vector_table_name,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};

const ACCOUNT: &str = "123456789012";
/// SQL backend: ordered keys, numbers to the service's 38 digits. With a
/// separate data database an open at `AfterDefinition` holds the `tables` row
/// `FOR SHARE`, so an `UpdateTable` issued then waits for it; on one shared
/// database the open holds no row lock and nothing waits.
const fn expectations(shared: bool) -> snapshot_conformance::Expectations {
    snapshot_conformance::Expectations {
        key_ordered: true,
        numeric_key_digits: 38,
        update_table_waits_at: if shared {
            &[]
        } else {
            &[OpenStage::AfterDefinition]
        },
    }
}
const REGION: &str = "us-east-1";

/// Runs `async fn $name(shared: bool)` as two tests,
/// `$name::separate_data_database` and `$name::one_shared_database`, each
/// skipped without `PostgreSQL`.
macro_rules! on_both_layouts {
    ($name:ident) => {
        mod $name {
            #[tokio::test]
            async fn separate_data_database() {
                if super::base_conn().is_none() {
                    eprintln!("SKIP {}: no PostgreSQL", module_path!());
                    return;
                }
                super::$name(false).await;
            }

            #[tokio::test]
            async fn one_shared_database() {
                if super::base_conn().is_none() {
                    eprintln!("SKIP {}: no PostgreSQL", module_path!());
                    return;
                }
                super::$name(true).await;
            }
        }
    };
}

struct Scratch {
    engine: PostgresEngine,
    catalog: PgPool,
    base: String,
    names: [String; 2],
    /// No separate data database: catalog and data in `names[0]`.
    shared: bool,
    /// Every test here opens a snapshot or touches the paging statics, so
    /// each holds the snapshot hooks lock for its whole body.
    hooks: snapshot_conformance::HooksGuard,
}

impl Drop for Scratch {
    /// Drops the two scratch databases on success or panic (not when the
    /// test binary is killed).
    /// Runs on its own thread and runtime with a fresh connection, because
    /// the test runtime's pools cannot be awaited from `Drop`; `WITH (FORCE)`
    /// terminates the sessions those pools still hold.
    fn drop(&mut self) {
        let base = self.base.clone();
        let names = self.names.clone();
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("cleanup runtime");
                rt.block_on(async move {
                    let Ok(mut conn) = PgConnection::connect(&format!("{base}/postgres")).await
                    else {
                        eprintln!("scratch cleanup: could not connect; {names:?} left behind");
                        return;
                    };
                    for name in &names {
                        if let Err(e) =
                            sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
                                .execute(&mut conn)
                                .await
                        {
                            eprintln!("scratch cleanup: drop {name}: {e}");
                        }
                    }
                    let _ = conn.close().await;
                });
            });
        });
    }
}

impl Scratch {
    /// The database the data tables live in.
    fn data_url(&self) -> String {
        let name = if self.shared {
            &self.names[0]
        } else {
            &self.names[1]
        };
        format!("{}/{name}", self.base)
    }

    /// The data database's name, for `pg_stat_activity` filters.
    fn data_name(&self) -> &str {
        if self.shared {
            &self.names[0]
        } else {
            &self.names[1]
        }
    }

    /// A pool on the scratch data database, for probes the engine does not
    /// expose.
    async fn data_pool(&self) -> PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.data_url())
            .await
            .expect("data pool")
    }
}

/// The server's base URL from `EXTENDDB_TEST_PG_CONNECTION_STRING`, or
/// `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED` set (CI sets it beside
/// the connection string) a missing or empty URL is a failure, so a typo in
/// the variable's name cannot turn the step green by skipping every test.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        assert!(
            std::env::var_os("EXTENDDB_TEST_PG_REQUIRED").is_none(),
            "EXTENDDB_TEST_PG_REQUIRED is set but EXTENDDB_TEST_PG_CONNECTION_STRING is not"
        );
        return None;
    }
    Some(conn.trim().trim_end_matches('/').to_owned())
}

async fn scratch() -> Scratch {
    scratch_with(false).await
}

async fn scratch_with(shared: bool) -> Scratch {
    scratch_opts(shared, false)
        .await
        .expect("no optional extension requested")
}

/// `shared`: no separate data database; data migrations go on the catalog
/// database and the engine runs catalog and data on one pool. `pgvector`:
/// install the extension on the data database before the engine probes it;
/// `None` (after printing why) when the server does not offer it.
async fn scratch_opts(shared: bool, pgvector: bool) -> Option<Scratch> {
    let hooks = snapshot_conformance::hooks_lock().await;
    let base = base_conn().expect("caller checks base_conn() first");
    let stem = format!("eddb_snap_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let names = [format!("{stem}_c"), format!("{stem}_d")];
    if !create_databases(&base, &names, pgvector).await {
        return None;
    }
    let catalog_url = format!("{base}/{}", names[0]);
    let data_url = if shared {
        catalog_url.clone()
    } else {
        format!("{base}/{}", names[1])
    };
    let catalog = PgPoolOptions::new()
        .max_connections(2)
        .connect(&catalog_url)
        .await
        .expect("catalog pool");
    migrate(&catalog, &data_url, pgvector).await;
    configure_catalog(&catalog, (!shared).then_some(&data_url)).await;
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{stem}"))
        .execute(&catalog)
        .await
        .expect("account");
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: catalog_url,
            pool_size: 8,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("engine");
    Some(Scratch {
        engine,
        catalog,
        base,
        names,
        shared,
        hooks,
    })
}

/// Create the two scratch databases; `false` (after printing why, with
/// nothing created) when `pgvector` is wanted and the server does not offer
/// it.
async fn create_databases(base: &str, names: &[String; 2], pgvector: bool) -> bool {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to postgres");
    if pgvector {
        let available: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = 'vector')",
        )
        .fetch_one(&admin)
        .await
        .expect("list the available extensions");
        if !available {
            eprintln!("this PostgreSQL server does not offer the pgvector extension");
            admin.close().await;
            return false;
        }
    }
    for name in names {
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&admin)
            .await
            .expect("create scratch database");
    }
    admin.close().await;
    true
}

/// Apply every catalog and data migration the crate ships, as `extenddb
/// migrate --yes` does (the data ones on `data_url`, which is the catalog
/// database on the shared layout), and pgvector if asked. The table lives in
/// the crate (`migrations/mod.rs`), so a migration added there is applied
/// here without this file knowing.
async fn migrate(catalog: &PgPool, data_url: &str, pgvector: bool) {
    let data = PgPoolOptions::new()
        .max_connections(2)
        .connect(data_url)
        .await
        .expect("data pool");
    apply_all_migrations(catalog, &data)
        .await
        .expect("apply the crate's migrations");
    if pgvector {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(&data)
            .await
            .expect("install pgvector on the data database");
    }
    data.close().await;
}

/// Zero the control-plane and propagation delays, and point the engine at
/// `separate_data_url` when there is one. The rows are written, not only
/// updated: migration 1 seeds no setting, and the tests below that change a
/// delay update the row written here.
async fn configure_catalog(catalog: &PgPool, separate_data_url: Option<&String>) {
    for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, '0') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .execute(catalog)
        .await
        .expect("setting");
    }
    if let Some(data_url) = separate_data_url {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(data_url)
        .execute(catalog)
        .await
        .expect("data connection setting");
    }
}

async fn snapshot_is_consistent_under_concurrent_writes(shared: bool) {
    let s = scratch_with(shared).await;
    snapshot_conformance::run(
        &s.hooks,
        &s.engine,
        ACCOUNT,
        "snap_src",
        50,
        expectations(shared),
        || async {
            s.engine
                .process_control_plane_transitions()
                .await
                .expect("pump");
        },
    )
    .await;
    let catalog = s.catalog.clone();
    snapshot_conformance::run_refusals(
        &s.hooks,
        &s.engine,
        ACCOUNT,
        "snap_refused",
        || async {
            s.engine
                .process_control_plane_transitions()
                .await
                .expect("pump");
        },
        |table| async move {
            sqlx::query("UPDATE tables SET table_status = 'UPDATING' WHERE table_name = $1")
                .bind(table)
                .execute(&catalog)
                .await
                .expect("flip status");
        },
    )
    .await;
}
on_both_layouts!(snapshot_is_consistent_under_concurrent_writes);

async fn create_active(s: &Scratch, input: extenddb_core::types::CreateTableInput) {
    let name = input.table_name.clone();
    s.engine.create_table(ACCOUNT, input).await.expect("create");
    snapshot_conformance::wait_active(&s.engine, ACCOUNT, &name, || async {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
    })
    .await;
}

async fn key_info(s: &Scratch, table: &str) -> extenddb_core::types::TableKeyInfo {
    let desc = s
        .engine
        .describe_table(
            ACCOUNT,
            extenddb_core::types::DescribeTableInput {
                table_name: table.to_owned(),
            },
        )
        .await
        .expect("describe");
    extenddb_core::types::TableKeyInfo {
        table_name: table.to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: desc.table_id.clone(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        ..Default::default()
    }
}

/// A composite-key table with an `S` range key and no index.
fn string_range_input(name: &str) -> extenddb_core::types::CreateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("create input")
}

/// Strings `en_US` and `C` order differently.
const COLLATION_KEYS: [&str; 10] = ["b", "B", "_x", "a", "ab", "Z", "-", "10", "9", "\u{e9}"];

/// Put one item per key in `COLLATION_KEYS`, as the hash key (`pk`, with `sk`
/// `N` 1) or as the range key (`sk` `S`, under one `pk`).
async fn put_collation_keys(s: &Scratch, table: &str, as_range: bool) {
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;
    let ki = key_info(s, table).await;
    for key in COLLATION_KEYS {
        let item: Item = if as_range {
            [
                ("pk".to_owned(), AttributeValue::S("p".to_owned())),
                ("sk".to_owned(), AttributeValue::S(key.to_owned())),
            ]
        } else {
            [
                ("pk".to_owned(), AttributeValue::S(key.to_owned())),
                ("sk".to_owned(), AttributeValue::N("1".to_owned())),
            ]
        }
        .into_iter()
        .collect();
        s.engine
            .put_item(&ki, item, false, None, &ExpressionMaps::default(), None)
            .await
            .expect("put");
    }
}

/// The key text the stream yielded, in stream order: `pk`, or `sk` for a
/// range-key table.
async fn stream_keys(
    snapshot: extenddb_storage::TableSnapshot,
    attribute: &'static str,
) -> Vec<String> {
    use extenddb_core::types::AttributeValue;
    use futures::StreamExt;
    snapshot
        .items
        .map(move |r| match r.expect("item").get(attribute) {
            Some(AttributeValue::S(v)) => v.clone(),
            _ => panic!("{attribute} is not a string"),
        })
        .collect()
        .await
}

/// A table created before the key columns were declared `COLLATE "C"` keeps
/// the database collation. Ordering it bytewise would be a full sort before
/// the first row, so it streams unordered and complete, whichever text key
/// column (`pk`, or `sk_s` for an `S` range key) has the database collation.
/// A table with "C" key columns on the same database is still ordered,
/// bytewise. Keys chosen so `en_US` and `C` disagree.
async fn legacy_collation_table_streams_unordered_and_complete(shared: bool) {
    let s = scratch_with(shared).await;
    let data = s.data_pool().await;
    // On a server whose default collation is C or POSIX the recollated
    // columns below would still sort bytewise and the test would show
    // nothing.
    let datcollate: String = sqlx::query_scalar(
        "SELECT datcollate::text FROM pg_database WHERE datname = current_database()",
    )
    .fetch_one(&data)
    .await
    .expect("datcollate");
    assert!(
        !matches!(datcollate.as_str(), "C" | "POSIX"),
        "the scratch data database must have a non-C default collation, got {datcollate}"
    );

    create_active(&s, snapshot_conformance::table_input("legacy_pk")).await;
    create_active(&s, string_range_input("legacy_sk")).await;
    create_active(&s, string_range_input("current")).await;
    put_collation_keys(&s, "legacy_pk", false).await;
    put_collation_keys(&s, "legacy_sk", true).await;
    put_collation_keys(&s, "current", true).await;
    // What a pre-#361 table looks like: a key column on the database
    // collation. One table per column the check reads.
    for (table, column) in [("legacy_pk", "pk"), ("legacy_sk", "sk_s")] {
        let relation = data_table_name(&key_info(&s, table).await.table_id);
        sqlx::query(&format!(
            "ALTER TABLE {relation} ALTER COLUMN {column} TYPE TEXT COLLATE \"default\""
        ))
        .execute(&data)
        .await
        .expect("recollate");
    }
    data.close().await;

    let mut expected: Vec<String> = COLLATION_KEYS.iter().map(|k| (*k).to_owned()).collect();
    expected.sort();
    for (table, attribute) in [("legacy_pk", "pk"), ("legacy_sk", "sk")] {
        let snapshot = s
            .engine
            .open_table_snapshot(ACCOUNT, table)
            .await
            .expect("open");
        assert!(
            !snapshot.key_ordered,
            "{table}: a key column without collation \"C\" streams unordered"
        );
        let mut streamed = stream_keys(snapshot, attribute).await;
        streamed.sort();
        assert_eq!(streamed, expected, "{table}: every item streams");
    }

    let snapshot = s
        .engine
        .open_table_snapshot(ACCOUNT, "current")
        .await
        .expect("open");
    assert!(snapshot.key_ordered, "\"C\" key columns stream ordered");
    assert_eq!(
        stream_keys(snapshot, "sk").await,
        expected,
        "stream is not in bytewise order"
    );
}
on_both_layouts!(legacy_collation_table_streams_unordered_and_complete);

/// The snapshot bound is half the pool, with a separate data database or one
/// shared pool: five snapshots holding their data connections on a
/// ten-connection pool, the sixth open refused at once with `LimitExceeded`,
/// item reads and `DescribeTable` still served, and a dropped snapshot's
/// permit free again.
async fn half_the_pool_holds_snapshots(shared: bool) {
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;
    use futures::StreamExt;
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("many")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "many", 20).await;
    let ki = key_info(&s, "many").await;
    // Each stream reads one item of a five-row page, so its transaction and
    // data connection stay open.
    let _page = PageSize::set(5);
    // The engine's pool is MIN_POOL_SIZE (10) here.
    let mut held = Vec::new();
    for i in 0..5 {
        let mut snapshot = s
            .engine
            .open_table_snapshot(ACCOUNT, "many")
            .await
            .unwrap_or_else(|e| panic!("open {i}: {e}"));
        snapshot.items.next().await.expect("an item").expect("ok");
        held.push(snapshot);
    }
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.engine.open_table_snapshot(ACCOUNT, "many"),
    )
    .await
    .expect("an open over the bound is refused at once, not queued")
    .expect_err("the sixth open is over the bound");
    assert!(
        matches!(err, extenddb_storage::error::StorageError::LimitExceeded(_)),
        "{err}"
    );
    let key: Item = [
        ("pk".to_owned(), AttributeValue::S("p000".to_owned())),
        ("sk".to_owned(), AttributeValue::N("0".to_owned())),
    ]
    .into_iter()
    .collect();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.engine.get_item(&ki, &key),
    )
    .await
    .expect("GetItem is not starved by the open snapshots")
    .expect("get_item")
    .expect("the item exists");
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.engine.describe_table(
            ACCOUNT,
            extenddb_core::types::DescribeTableInput {
                table_name: "many".to_owned(),
            },
        ),
    )
    .await
    .expect("DescribeTable is not starved by the open snapshots")
    .expect("describe");
    drop(held.pop());
    s.engine
        .open_table_snapshot(ACCOUNT, "many")
        .await
        .expect("a dropped snapshot frees its permit");
}

#[tokio::test]
async fn five_snapshots_on_a_separate_data_pool_leave_half_of_it() {
    if base_conn().is_none() {
        eprintln!("SKIP five_snapshots_on_a_separate_data_pool_leave_half_of_it: no PostgreSQL");
        return;
    }
    half_the_pool_holds_snapshots(false).await;
}

#[tokio::test]
async fn five_snapshots_on_one_shared_pool_leave_half_of_it() {
    if base_conn().is_none() {
        eprintln!("SKIP five_snapshots_on_one_shared_pool_leave_half_of_it: no PostgreSQL");
        return;
    }
    half_the_pool_holds_snapshots(true).await;
}

/// Hold `ACCESS SHARE` on one data relation from another session, run
/// `DeleteTable`, and check it took the asynchronous path (it returns at once
/// and the row is `DELETING`, not gone); then release the lock and check the
/// worker drops `relations` and removes the row.
async fn delete_with_relation_locked(s: &Scratch, table: &str, locked: &str, relations: &[String]) {
    let mut holder = PgConnection::connect(&s.data_url()).await.expect("holder");
    sqlx::query("BEGIN")
        .execute(&mut holder)
        .await
        .expect("begin");
    sqlx::query(&format!("LOCK TABLE {locked} IN ACCESS SHARE MODE"))
        .execute(&mut holder)
        .await
        .expect("lock the relation");
    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.engine.delete_table(
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: table.to_owned(),
            },
        ),
    )
    .await
    .expect("DeleteTable must not wait behind a reader of an index data table")
    .expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    let status: Option<String> =
        sqlx::query_scalar("SELECT table_status FROM tables WHERE table_name = $1")
            .bind(table)
            .fetch_optional(&s.catalog)
            .await
            .expect("status");
    assert_eq!(
        status.as_deref(),
        Some("DELETING"),
        "with {locked} in use the row stays until the worker's drop"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut holder)
        .await
        .expect("release");
    holder.close().await.expect("close holder");

    let data = s.data_pool().await;
    for _ in 0..100 {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
        let mut all_gone = true;
        for relation in relations {
            let gone: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NULL")
                .bind(relation)
                .fetch_one(&data)
                .await
                .expect("probe");
            all_gone &= gone;
        }
        let row: Option<String> =
            sqlx::query_scalar("SELECT table_status FROM tables WHERE table_name = $1")
                .bind(table)
                .fetch_optional(&s.catalog)
                .await
                .expect("status");
        if all_gone && row.is_none() {
            data.close().await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{relations:?} and the catalog row of {table} were not all removed");
}

/// `DeleteTable` probes every relation its synchronous drop touches, not only
/// the base data table: a session holding `ACCESS SHARE` on a GSI data table
/// alone (a Query or Scan with `IndexName`, the backfill) sends it down the
/// asynchronous path at once, rather than into a drop that would wait out its
/// lock timeout behind the reader.
async fn delete_table_with_a_gsi_data_table_in_use_takes_the_async_path(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("gsilock")).await;
    let table_id = key_info(&s, "gsilock").await.table_id;
    let index_id: String = sqlx::query_scalar(
        "SELECT i.index_id FROM indexes i JOIN tables t ON t.table_id = i.table_id \
         WHERE t.table_name = 'gsilock' AND i.index_name = 'gi'",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("gi row");
    let gsi_table = index_table_name(&index_id);
    delete_with_relation_locked(
        &s,
        "gsilock",
        &gsi_table,
        &[data_table_name(&table_id), gsi_table.clone()],
    )
    .await;
}
on_both_layouts!(delete_table_with_a_gsi_data_table_in_use_takes_the_async_path);

/// The same for a vector-index data table, on a server with pgvector.
async fn delete_table_with_a_vector_data_table_in_use_takes_the_async_path(shared: bool) {
    let Some(s) = scratch_opts(shared, true).await else {
        eprintln!("SKIP: no pgvector on this server, so no vector-index data table");
        return;
    };
    assert!(
        s.engine.vector_capable(),
        "pgvector installed on the data database"
    );
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "veclock",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{
            "IndexName": "vi",
            "VectorAttribute": {"AttributeName": "emb"},
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .expect("create input");
    create_active(&s, input).await;
    let table_id = key_info(&s, "veclock").await.table_id;
    let index_id: String = sqlx::query_scalar(
        "SELECT v.index_id FROM vector_indexes v JOIN tables t ON t.table_id = v.table_id \
         WHERE t.table_name = 'veclock'",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("vector index row");
    let vector_table = vector_table_name(&index_id);
    let data = s.data_pool().await;
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(&vector_table)
        .fetch_one(&data)
        .await
        .expect("probe");
    data.close().await;
    assert!(exists, "the vector index has a data table to lock");
    delete_with_relation_locked(
        &s,
        "veclock",
        &vector_table,
        &[data_table_name(&table_id), vector_table.clone()],
    )
    .await;
}
on_both_layouts!(delete_table_with_a_vector_data_table_in_use_takes_the_async_path);

/// A row the stream cannot parse ends it at the error, as on every backend,
/// and gives up the snapshot's permit at the error rather than at the next
/// poll or the drop: with the errored stream still held, a full bound of five
/// opens succeeds.
async fn an_unparseable_row_ends_the_stream_and_frees_its_permit(shared: bool) {
    use futures::StreamExt as _;
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("corrupt")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "corrupt", 20).await;
    let table_id = key_info(&s, "corrupt").await.table_id;
    let data = s.data_pool().await;
    // Valid JSON that is not an item, keyed between p005 and p006: the
    // seventh row, on the second (full) page of five, so the error is not
    // also the end of the table.
    sqlx::query(&format!(
        "INSERT INTO {} (pk, sk_n, item_data) VALUES ('p005a', 1, '{{\"pk\": 5}}')",
        data_table_name(&table_id)
    ))
    .execute(&data)
    .await
    .expect("insert an unparseable row");
    data.close().await;
    let _page = PageSize::set(5);
    let mut snapshot = s
        .engine
        .open_table_snapshot(ACCOUNT, "corrupt")
        .await
        .expect("open");
    let mut items = 0;
    loop {
        match snapshot.items.next().await {
            Some(Ok(_)) => items += 1,
            Some(Err(_)) => break,
            None => panic!("the stream ended without reporting the unparseable row"),
        }
    }
    assert_eq!(
        items, 5,
        "the first page, then the error for the page holding the unparseable row"
    );
    // Before the stream is polled again (its end drops the state, and the
    // permit with it), the permit must already be free.
    let mut held = Vec::new();
    for i in 0..5 {
        held.push(
            s.engine
                .open_table_snapshot(ACCOUNT, "corrupt")
                .await
                .unwrap_or_else(|e| panic!("open {i} with the errored stream held: {e}")),
        );
    }
    assert!(
        snapshot.items.next().await.is_none(),
        "nothing follows the first error"
    );
}
on_both_layouts!(an_unparseable_row_ends_the_stream_and_frees_its_permit);

/// A GSI row whose data table is missing is left out of the description,
/// and so are the attribute definitions only it used: `CreateTable` would
/// refuse a definition no key names.
async fn gsi_without_a_data_table_is_left_out_with_its_attributes(shared: bool) {
    let s = scratch_with(shared).await;
    s.engine
        .create_table(ACCOUNT, snapshot_conformance::table_input("half"))
        .await
        .expect("create");
    snapshot_conformance::wait_active(&s.engine, ACCOUNT, "half", || async {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
    })
    .await;
    s.engine
        .update_table(
            ACCOUNT,
            serde_json::from_value(serde_json::json!({
                "TableName": "half",
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}],
                "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "gi2",
                    "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
                }}]
            }))
            .expect("update input"),
        )
        .await
        .expect("add gi2");
    for _ in 0..50 {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
    }
    let index_id: String = sqlx::query_scalar(
        "SELECT i.index_id FROM indexes i JOIN tables t ON t.table_id = i.table_id \
         WHERE t.table_name = 'half' AND i.index_name = 'gi2'",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("gi2 row");
    let data = s.data_pool().await;
    // An index the snapshot cannot see whole: drop its data table.
    sqlx::query(&format!("DROP TABLE {}", index_table_name(&index_id)))
        .execute(&data)
        .await
        .expect("drop the GSI data table");
    data.close().await;

    let snapshot = s
        .engine
        .open_table_snapshot(ACCOUNT, "half")
        .await
        .expect("open");
    let gsis: Vec<_> = snapshot
        .description
        .global_secondary_indexes
        .iter()
        .flatten()
        .map(|g| g.index_name.clone())
        .collect();
    assert!(!gsis.contains(&"gi2".to_owned()), "{gsis:?}");
    let attrs: Vec<_> = snapshot
        .description
        .attribute_definitions
        .iter()
        .map(|d| d.attribute_name.clone())
        .collect();
    assert!(
        !attrs.contains(&"h".to_owned()),
        "unused definition left in: {attrs:?}"
    );
    drop(snapshot);
}
on_both_layouts!(gsi_without_a_data_table_is_left_out_with_its_attributes);

/// `DeleteTable` while a snapshot stream holds the data table never leaves
/// the catalog row gone with the data table orphaned: it takes the
/// asynchronous path, returns at once, and the worker finishes once the
/// stream is done, even if the caller gave up.
async fn delete_table_during_a_snapshot_does_not_orphan_the_data_table(shared: bool) {
    use futures::StreamExt as _;
    let s = scratch_with(shared).await;
    s.engine
        .create_table(ACCOUNT, snapshot_conformance::table_input("victim"))
        .await
        .expect("create");
    snapshot_conformance::wait_active(&s.engine, ACCOUNT, "victim", || async {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
    })
    .await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "victim", 20).await;
    let table_id: String =
        sqlx::query_scalar("SELECT table_id FROM tables WHERE table_name = 'victim'")
            .fetch_one(&s.catalog)
            .await
            .expect("table id");
    let page = PageSize::set(5);
    let mut snapshot = s
        .engine
        .open_table_snapshot(ACCOUNT, "victim")
        .await
        .expect("open");
    // Read one page so the cursor is mid-table and the lock is held.
    snapshot.items.next().await.expect("an item").expect("ok");
    drop(page);

    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        s.engine.delete_table(
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: "victim".to_owned(),
            },
        ),
    )
    .await
    .expect("DeleteTable must not wait behind the stream")
    .expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    let status: Option<String> =
        sqlx::query_scalar("SELECT table_status FROM tables WHERE table_name = 'victim'")
            .fetch_optional(&s.catalog)
            .await
            .expect("status");
    assert_eq!(
        status.as_deref(),
        Some("DELETING"),
        "the row stays until the drop"
    );

    // The stream still reads its snapshot to the end.
    while let Some(item) = snapshot.items.next().await {
        item.expect("stream item");
    }
    drop(snapshot);
    let data = s.data_pool().await;
    for _ in 0..100 {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
        let gone: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NULL")
            .bind(data_table_name(&table_id))
            .fetch_one(&data)
            .await
            .expect("probe");
        let row: Option<String> =
            sqlx::query_scalar("SELECT table_status FROM tables WHERE table_name = 'victim'")
                .fetch_optional(&s.catalog)
                .await
                .expect("status");
        if gone && row.is_none() {
            data.close().await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the data table and the catalog row were not both removed");
}
on_both_layouts!(delete_table_during_a_snapshot_does_not_orphan_the_data_table);

/// The two SQLSTATEs that mean the server discarded the snapshot map to
/// `SnapshotExpired`; a third does not. The errors are raised by the server
/// so the test exercises the real `sqlx::Error::Database` shape.
#[tokio::test]
async fn sqlstate_mapping_is_pinned() {
    if base_conn().is_none() {
        eprintln!("SKIP sqlstate_mapping_is_pinned: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    for (code, expired) in [("40001", true), ("72000", true), ("57014", false)] {
        let err = sqlx::query(&format!(
            "DO $$ BEGIN RAISE EXCEPTION 'probe' USING ERRCODE = '{code}'; END $$"
        ))
        .execute(&s.catalog)
        .await
        .expect_err("raised");
        let mapped = extenddb_storage_postgres::map_fetch_error(&err);
        assert_eq!(
            matches!(
                mapped,
                extenddb_storage::error::StorageError::SnapshotExpired(_)
            ),
            expired,
            "{code}: {mapped:?}"
        );
    }
}

async fn snapshot_marker_and_key_order(shared: bool) {
    let s = scratch_with(shared).await;
    s.engine
        .create_table(ACCOUNT, snapshot_conformance::table_input("ordered"))
        .await
        .expect("create");
    snapshot_conformance::wait_active(&s.engine, ACCOUNT, "ordered", || async {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
    })
    .await;
    let snapshot = s
        .engine
        .open_table_snapshot(ACCOUNT, "ordered")
        .await
        .expect("open");
    assert!(snapshot.key_ordered);
    assert_eq!(
        snapshot.info.kind,
        extenddb_storage::SnapshotInfo::KIND_POSTGRES_REPEATABLE_READ
    );
    // pg_current_snapshot() text: "xmin:xmax:xip_list".
    assert!(
        snapshot.info.marker.split(':').count() == 3,
        "marker {:?}",
        snapshot.info.marker
    );
    drop(snapshot);

    // A table that is not ACTIVE is refused before any data read.
    sqlx::query("UPDATE tables SET table_status = 'UPDATING' WHERE table_name = 'ordered'")
        .execute(&s.catalog)
        .await
        .expect("flip status");
    let err = s
        .engine
        .open_table_snapshot(ACCOUNT, "ordered")
        .await
        .expect_err("refused");
    assert!(
        matches!(
            err,
            extenddb_storage::error::StorageError::TableNotActive(_)
        ),
        "{err}"
    );
}
on_both_layouts!(snapshot_marker_and_key_order);

/// A hash-only table with no index, for tests that need many of them.
fn plain_input(name: &str) -> extenddb_core::types::CreateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("create input")
}

fn delete_input(table: &str) -> extenddb_core::types::DeleteTableInput {
    extenddb_core::types::DeleteTableInput {
        table_name: table.to_owned(),
    }
}

/// `DeleteTable` under a 5 s timeout, so a delete that waits behind a reader
/// or the pool fails the test instead of hanging it.
async fn delete_within_5s(
    s: &Scratch,
    table: &str,
) -> Result<extenddb_core::types::TableDescription, extenddb_storage::error::StorageError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.engine.delete_table(ACCOUNT, delete_input(table)),
    )
    .await
    .unwrap_or_else(|_| panic!("DeleteTable of {table} did not return within 5 s"))
}

async fn catalog_status(s: &Scratch, table: &str) -> Option<String> {
    sqlx::query_scalar("SELECT table_status FROM tables WHERE table_name = $1")
        .bind(table)
        .fetch_optional(&s.catalog)
        .await
        .expect("status")
}

/// Twenty concurrent `DeleteTable`s of twenty tables on a ten-connection
/// engine all succeed within 5 s. Each holds one catalog connection with its
/// row `FOR UPDATE` while it probes for readers; on one shared pool the probe
/// must run on that connection, or `pool_size` deletes hold every
/// connection, each waiting for a second, until the acquire timeout.
async fn concurrent_delete_tables_do_not_exhaust_the_pool(shared: bool) {
    let s = scratch_with(shared).await;
    let tables: Vec<String> = (0..20).map(|i| format!("many{i:02}")).collect();
    for table in &tables {
        create_active(&s, plain_input(table)).await;
    }
    let started = std::time::Instant::now();
    let results = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        futures::future::join_all(
            tables
                .iter()
                .map(|t| s.engine.delete_table(ACCOUNT, delete_input(t))),
        ),
    )
    .await
    .expect("twenty concurrent DeleteTables finish within 5 s");
    eprintln!("twenty DeleteTables took {:?}", started.elapsed());
    for (table, result) in tables.iter().zip(results) {
        result.unwrap_or_else(|e| panic!("DeleteTable {table}: {e}"));
    }
}
on_both_layouts!(concurrent_delete_tables_do_not_exhaust_the_pool);

/// An open held at `AfterDefinition` while nine `DeleteTable`s of the same
/// table are issued, then resumed: the open and every delete return, none
/// waiting out the pool's acquire timeout. On the separate layout the
/// deletes wait on the row the open holds `FOR SHARE` with catalog
/// connections while the open needs only a data connection, and the open
/// then streams every item; the first delete finds the table in use and
/// leaves it `DELETING`, and the other eight find it `DELETING` and return
/// its description. On the shared layout the open holds no row lock and, at
/// this pause, no lock on the data table yet, so the first delete drops the
/// table at once, the other eight find it gone, and the open reports
/// `TableNotFound`.
async fn an_open_held_with_deletes_queued_completes(shared: bool) {
    use futures::StreamExt as _;
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("queued")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "queued", 20).await;
    let mut pause = extenddb_storage::snapshot::paging::arm_open_pause(
        extenddb_storage::snapshot::paging::OpenStage::AfterDefinition,
    );
    let reached = pause.take_reached();
    let open = s.engine.open_table_snapshot(ACCOUNT, "queued");
    let controller = async {
        reached.await.expect("the open reached the pause");
        let deletes = futures::future::join_all(
            (0..9).map(|_| s.engine.delete_table(ACCOUNT, delete_input("queued"))),
        );
        let resume = async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            pause.resume();
        };
        let (results, ()) = tokio::join!(deletes, resume);
        results
    };
    let started = std::time::Instant::now();
    let (opened, deletes) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        tokio::join!(open, controller)
    })
    .await
    .expect("the open and the deletes finish well inside the 30 s acquire timeout");
    eprintln!("open and nine deletes took {:?}", started.elapsed());
    let mut returned = 0;
    for result in deletes {
        match result {
            Ok(d) => {
                assert_eq!(d.table_status, extenddb_core::types::TableStatus::Deleting);
                returned += 1;
            }
            Err(extenddb_storage::error::StorageError::TableNotFound(_)) => {}
            Err(e) => panic!("a queued DeleteTable failed: {e}"),
        }
    }
    let (want, status) = if shared {
        (1, None)
    } else {
        (9, Some("DELETING"))
    };
    assert_eq!(
        returned, want,
        "DeleteTables that returned the table's description"
    );
    assert_eq!(catalog_status(&s, "queued").await.as_deref(), status);
    if shared {
        let err = opened.expect_err("the table was dropped before the open's LOCK");
        assert!(
            matches!(err, extenddb_storage::error::StorageError::TableNotFound(_)),
            "{err}"
        );
    } else {
        let snapshot = opened.expect("the held open completes");
        let items: Vec<_> = snapshot.items.collect().await;
        assert_eq!(
            items.len(),
            20,
            "the open streams the table as of its snapshot"
        );
        assert!(items.iter().all(Result::is_ok));
    }
}
on_both_layouts!(an_open_held_with_deletes_queued_completes);

/// A restored table's snapshot description carries the same
/// `restore_summary` as `describe_table`'s (both read the `table_restores`
/// row). The record is written directly, as `RestoreTableFromBackup` leaves
/// it.
async fn a_restored_table_describes_its_restore(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("restored")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "restored", 5).await;
    sqlx::query(
        "INSERT INTO table_restores \
         (table_id, source_backup_arn, source_table_arn, restore_date_time) \
         SELECT table_id, $1, $2, '2026-10-01T12:00:00.250Z' FROM tables \
         WHERE table_name = 'restored'",
    )
    .bind("arn:aws:dynamodb:us-east-1:123456789012:table/src/backup/01759320000250-0a1b2c3d")
    .bind("arn:aws:dynamodb:us-east-1:123456789012:table/src")
    .execute(&s.catalog)
    .await
    .expect("restore record");
    snapshot_conformance::assert_snapshot_describes_the_table(
        &s.hooks, &s.engine, ACCOUNT, "restored", true,
    )
    .await;
}
on_both_layouts!(a_restored_table_describes_its_restore);

/// The pid of the one backend whose last statement was the stream's `FETCH`.
async fn stream_backend(s: &Scratch, data: &PgPool) -> i32 {
    sqlx::query_scalar(
        "SELECT pid FROM pg_stat_activity WHERE datname = $1 \
         AND state = 'idle in transaction' AND query LIKE 'FETCH%'",
    )
    .bind(s.data_name())
    .fetch_one(data)
    .await
    .expect("the stream's backend is idle in its transaction after a FETCH")
}

/// End the stream's backend from another session, as a server restart or
/// an administrator would.
async fn terminate_stream_backend(s: &Scratch) {
    let data = s.data_pool().await;
    let pid = stream_backend(s, &data).await;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&data)
        .await
        .expect("terminate");
    assert!(terminated, "pg_terminate_backend({pid})");
    data.close().await;
}

/// How a snapshot's data transaction ends after a `DeleteTable` found the
/// table in use.
#[derive(Debug, Clone, Copy)]
enum Release {
    /// The stream is dropped with rows left.
    Dropped,
    /// The stream is read to its end and kept.
    Drained,
    /// The stream's backend is terminated and the stream yields the error.
    FetchError,
    /// The open is cancelled while held at `BeforeItems`.
    OpenCancelled,
}

/// A held snapshot or an open in progress, whichever `Release` needs.
enum Holder<'a> {
    Stream(Box<extenddb_storage::TableSnapshot>),
    Open(
        futures::future::BoxFuture<
            'a,
            Result<extenddb_storage::TableSnapshot, extenddb_storage::error::StorageError>,
        >,
        extenddb_storage::snapshot::paging::OpenPause,
    ),
}

/// The active poll of the poller in the wake tests: long enough that a drop
/// retried by the timer instead of a wake comes well after the tests' 3 s
/// bound.
const WAKE_TEST_ACTIVE_POLL: std::time::Duration = std::time::Duration::from_secs(10);

/// Wait (up to 15 s) until `engine`'s control plane has made more than
/// `after` drop attempts, and return the count.
async fn wait_for_drop_attempt(engine: &PostgresEngine, after: u64) -> u64 {
    for _ in 0..150 {
        let attempts = engine.control_plane_drop_attempts_for_tests();
        if attempts > after {
            return attempts;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the control plane made no drop attempt after the first {after} within 15 s");
}

/// Wait (up to 3 s from `released_at`) until `relations` are gone and
/// `table` has no catalog row; `what` names the release in the output.
async fn removed_within_3s(
    s: &Scratch,
    table: &str,
    relations: &[String],
    released_at: std::time::Instant,
    what: &str,
) {
    let data = s.data_pool().await;
    while released_at.elapsed() < std::time::Duration::from_secs(3) {
        let mut all_gone = true;
        for relation in relations {
            let gone: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NULL")
                .bind(relation)
                .fetch_one(&data)
                .await
                .expect("probe");
            all_gone &= gone;
        }
        if all_gone && catalog_status(s, table).await.is_none() {
            eprintln!(
                "{what}: table removed {:?} after the release",
                released_at.elapsed()
            );
            data.close().await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("{what}: {table} was not removed within 3 s of the release");
}

/// `DeleteTable` while a snapshot holds the table leaves it `DELETING`. The
/// control-plane poller (the one `serve` runs, with its active poll set to
/// `WAKE_TEST_ACTIVE_POLL`) makes its first drop attempt, which finds the
/// table held and skips it, and pauses before the next. The snapshot's
/// transaction then ends, by `how`, during that pause: the wake the release
/// sends has the poller retry at once, and the table is gone within 3 s.
/// Without the wake the next attempt comes about 9.5 s after the release.
/// `Dropped`, `FetchError` and `OpenCancelled` end the transaction through
/// `HeldSnapshot`'s drop, `Drained` through its `finish`.
/// `a_plain_reader_past_the_active_window_delays_the_drop_only_until_it_releases`
/// pins the poller's behaviour for a reader that sends no wake.
async fn release_wakes_the_active_poller(shared: bool, how: Release) {
    use futures::StreamExt as _;
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("woken")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "woken", 20).await;
    let table_id = key_info(&s, "woken").await.table_id;
    let _page = PageSize::set(5);
    let holder = if let Release::OpenCancelled = how {
        let mut pause = extenddb_storage::snapshot::paging::arm_open_pause(
            extenddb_storage::snapshot::paging::OpenStage::BeforeItems,
        );
        let reached = pause.take_reached();
        let mut open = s.engine.open_table_snapshot(ACCOUNT, "woken");
        tokio::select! {
            r = &mut open => panic!("the open finished while it should be paused: {r:?}"),
            r = reached => r.expect("the open reached BeforeItems"),
        }
        Holder::Open(open, pause)
    } else {
        let mut snapshot = s
            .engine
            .open_table_snapshot(ACCOUNT, "woken")
            .await
            .expect("open");
        snapshot.items.next().await.expect("an item").expect("ok");
        Holder::Stream(Box::new(snapshot))
    };

    let deleted = delete_within_5s(&s, "woken").await.expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    // Started after DeleteTable, so the wakes CreateTable and DeleteTable
    // left are one stored permit: the poller starts its active window at
    // once, and nothing else wakes it before the release.
    let _poller = s
        .engine
        .spawn_control_plane_poller_with_active_poll_for_tests(WAKE_TEST_ACTIVE_POLL);
    let attempts = wait_for_drop_attempt(&s.engine, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        s.engine.control_plane_drop_attempts_for_tests(),
        attempts,
        "{how:?}: precondition: the poller is in the pause after its first attempt"
    );
    assert_eq!(
        catalog_status(&s, "woken").await.as_deref(),
        Some("DELETING"),
        "{how:?}: the poller cannot drop the table while the snapshot holds it"
    );

    // Kept alive until the end, so only the release under test can wake the
    // poller (a dropped stream would wake it through its drop).
    let mut kept = None;
    match (how, holder) {
        (Release::Dropped, Holder::Stream(snapshot)) => drop(snapshot),
        (Release::Drained, Holder::Stream(mut snapshot)) => {
            while let Some(item) = snapshot.items.next().await {
                item.expect("stream item");
            }
            kept = Some(snapshot);
        }
        (Release::FetchError, Holder::Stream(mut snapshot)) => {
            terminate_stream_backend(&s).await;
            loop {
                match snapshot.items.next().await {
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                    None => panic!("the stream ended without the FETCH error"),
                }
            }
            kept = Some(snapshot);
        }
        (Release::OpenCancelled, Holder::Open(open, pause)) => {
            drop(open);
            drop(pause);
        }
        (how, _) => unreachable!("holder built for {how:?}"),
    }
    let released_at = std::time::Instant::now();
    removed_within_3s(
        &s,
        "woken",
        &[data_table_name(&table_id)],
        released_at,
        &format!("{how:?}"),
    )
    .await;
    drop(kept);
}

/// A reader that is not a snapshot stream (a plain transaction holding
/// `ACCESS SHARE`, as `pg_dump` or a client read do) sends the poller no
/// wake when it releases. The poller
/// therefore stays active for as long as a `DELETING` row's drop is deferred
/// because the table is held: with the reader holding the table for 10 s,
/// well past the poller's active window of 5 s (`control_plane_delay_seconds`
/// is 0; each attempt in the window finds the table held and skips it at
/// once, so the window's attempts end with it), the row is `DELETING`
/// throughout and the table is gone within 3 s of the release. A poller that went idle at
/// the end of its window would drop it at its 60 s idle pass instead, and
/// this test's 3 s bound fails it.
///
/// The poller is started after the `DeleteTable`, so the wakes `CreateTable`
/// and `DeleteTable` left are one stored permit and one window. Started
/// before, the `DeleteTable` wake would arrive during the first window and be
/// stored for a second one right after it, and a poller that went idle would
/// still be active at about 10 s from two chained windows; the hold would
/// have to outlast both to tell the two apart.
async fn a_plain_reader_past_the_active_window_delays_the_drop_only_until_it_releases(
    shared: bool,
) {
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("read")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "read", 5).await;
    let relation = data_table_name(&key_info(&s, "read").await.table_id);
    let mut holder = PgConnection::connect(&s.data_url()).await.expect("holder");
    sqlx::query("BEGIN")
        .execute(&mut holder)
        .await
        .expect("begin");
    sqlx::query(&format!("LOCK TABLE {relation} IN ACCESS SHARE MODE"))
        .execute(&mut holder)
        .await
        .expect("hold the data table as a plain reader");

    let deleted = delete_within_5s(&s, "read").await.expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting,
        "a held table takes the asynchronous path"
    );
    let deleted_at = std::time::Instant::now();
    // The production poller, woken at once by the permit the DeleteTable
    // (and the CreateTable) left; its active window is 5 s from now.
    let _poller = s.engine.spawn_control_plane_poller_for_tests();
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    assert_eq!(
        catalog_status(&s, "read").await.as_deref(),
        Some("DELETING"),
        "the poller cannot drop the table while the reader holds it"
    );

    sqlx::query("ROLLBACK")
        .execute(&mut holder)
        .await
        .expect("release");
    holder.close().await.expect("close holder");
    let released_at = std::time::Instant::now();
    let data = s.data_pool().await;
    while released_at.elapsed() < std::time::Duration::from_secs(3) {
        let gone: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NULL")
            .bind(&relation)
            .fetch_one(&data)
            .await
            .expect("probe");
        if gone && catalog_status(&s, "read").await.is_none() {
            eprintln!(
                "plain reader: table removed {:?} after the release ({:?} after DeleteTable)",
                released_at.elapsed(),
                deleted_at.elapsed()
            );
            data.close().await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!(
        "the table was not removed within 3 s of a plain reader's release, {:?} after \
         DeleteTable: the poller went idle while the drop was deferred",
        deleted_at.elapsed()
    );
}
on_both_layouts!(a_plain_reader_past_the_active_window_delays_the_drop_only_until_it_releases);

async fn dropping_a_stream_wakes_the_active_poller(shared: bool) {
    release_wakes_the_active_poller(shared, Release::Dropped).await;
}
on_both_layouts!(dropping_a_stream_wakes_the_active_poller);

async fn draining_a_stream_wakes_the_active_poller(shared: bool) {
    release_wakes_the_active_poller(shared, Release::Drained).await;
}
on_both_layouts!(draining_a_stream_wakes_the_active_poller);

async fn a_fetch_error_wakes_the_active_poller(shared: bool) {
    release_wakes_the_active_poller(shared, Release::FetchError).await;
}
on_both_layouts!(a_fetch_error_wakes_the_active_poller);

async fn cancelling_an_open_wakes_the_active_poller(shared: bool) {
    release_wakes_the_active_poller(shared, Release::OpenCancelled).await;
}
on_both_layouts!(cancelling_an_open_wakes_the_active_poller);

/// The in-catalog backup holds the data table's `ACCESS SHARE` from its
/// `LOCK TABLE` until it has copied the items (`HeldSnapshot::finish`). Held
/// there (its catalog `INSERT INTO backups` waits on a session holding
/// `backups` in `SHARE` mode), a `DeleteTable` leaves the row `DELETING`;
/// the backup is then let go during the active poller's pause after its
/// first drop attempt, and its end wakes the poller, which drops the table
/// within 3 s rather than at its next timed attempt about 9.5 s later.
async fn the_in_catalog_backup_wakes_the_active_poller(shared: bool) {
    use extenddb_storage::BackupEngine as _;
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("backed")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "backed", 20).await;
    let relation = data_table_name(&key_info(&s, "backed").await.table_id);
    let catalog_url = format!("{}/{}", s.base, s.names[0]);
    let mut blocker = PgConnection::connect(&catalog_url).await.expect("blocker");
    sqlx::query("BEGIN")
        .execute(&mut blocker)
        .await
        .expect("begin");
    sqlx::query("LOCK TABLE backups IN SHARE MODE")
        .execute(&mut blocker)
        .await
        .expect("hold backups");

    let backup = s.engine.create_backup(ACCOUNT, "backed", "held");
    let controller = async {
        let mut watcher = PgConnection::connect(&format!("{}/postgres", s.base))
            .await
            .expect("watcher");
        let mut waiting = 0;
        for _ in 0..100 {
            waiting = sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = $1 \
                 AND wait_event_type = 'Lock' AND query LIKE 'INSERT INTO backups%'",
            )
            .bind(&s.names[0])
            .fetch_one(&mut watcher)
            .await
            .expect("waiters");
            if waiting == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        watcher.close().await.expect("close watcher");
        assert_eq!(
            waiting, 1,
            "precondition: the backup holds the data table and waits on backups"
        );
        let deleted = delete_within_5s(&s, "backed").await.expect("delete");
        assert_eq!(
            deleted.table_status,
            extenddb_core::types::TableStatus::Deleting,
            "the backup's lock sends DeleteTable down the asynchronous path"
        );
        let poller = s
            .engine
            .spawn_control_plane_poller_with_active_poll_for_tests(WAKE_TEST_ACTIVE_POLL);
        let attempts = wait_for_drop_attempt(&s.engine, 0).await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_eq!(
            s.engine.control_plane_drop_attempts_for_tests(),
            attempts,
            "precondition: the poller is in the pause after its first attempt"
        );
        assert_eq!(
            catalog_status(&s, "backed").await.as_deref(),
            Some("DELETING"),
            "the poller cannot drop the table while the backup holds it"
        );
        sqlx::query("ROLLBACK")
            .execute(&mut blocker)
            .await
            .expect("let the backup go");
        (poller, std::time::Instant::now())
    };
    let (backed_up, (poller, released_at)) = tokio::join!(backup, controller);
    backed_up.expect("the backup completes");
    removed_within_3s(&s, "backed", &[relation], released_at, "in-catalog backup").await;
    drop(poller);
    blocker.close().await.expect("close blocker");
}
on_both_layouts!(the_in_catalog_backup_wakes_the_active_poller);

/// A `FETCH` that fails (the backend ended under the stream) ends the stream
/// at the error and frees its permit and connection at once: with the
/// errored stream still held a full bound of five opens succeeds. The
/// `SnapshotExpired` codes take this branch.
async fn a_fetch_error_ends_the_stream_and_frees_its_permit(shared: bool) {
    use futures::StreamExt as _;
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("ended")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "ended", 20).await;
    let _page = PageSize::set(5);
    let mut snapshot = s
        .engine
        .open_table_snapshot(ACCOUNT, "ended")
        .await
        .expect("open");
    snapshot.items.next().await.expect("an item").expect("ok");
    terminate_stream_backend(&s).await;
    let mut items = 1;
    loop {
        match snapshot.items.next().await {
            Some(Ok(_)) => items += 1,
            Some(Err(e)) => {
                eprintln!("FETCH error after {items} items: {e}");
                break;
            }
            None => panic!("the stream ended without reporting the FETCH error"),
        }
    }
    assert_eq!(items, 5, "the first page, then the error");
    let mut held = Vec::new();
    for i in 0..5 {
        held.push(
            s.engine
                .open_table_snapshot(ACCOUNT, "ended")
                .await
                .unwrap_or_else(|e| panic!("open {i} with the errored stream held: {e}")),
        );
    }
    assert!(
        snapshot.items.next().await.is_none(),
        "nothing follows the first error"
    );
}
on_both_layouts!(a_fetch_error_ends_the_stream_and_frees_its_permit);

/// `DeleteTable` while an open is held at `BeforeItems` (definition read,
/// snapshot pinned, cursor not declared) takes the asynchronous path: the
/// open already holds `ACCESS SHARE` on the data table, so the drop cannot
/// remove it under the open, and the open then streams every item.
async fn delete_table_at_before_items_takes_the_async_path(shared: bool) {
    use futures::StreamExt as _;
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("early")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "early", 20).await;
    let mut pause = extenddb_storage::snapshot::paging::arm_open_pause(
        extenddb_storage::snapshot::paging::OpenStage::BeforeItems,
    );
    let reached = pause.take_reached();
    let mut open = s.engine.open_table_snapshot(ACCOUNT, "early");
    tokio::select! {
        r = &mut open => panic!("the open finished while it should be paused: {r:?}"),
        r = reached => r.expect("the open reached BeforeItems"),
    }
    let deleted = delete_within_5s(&s, "early").await.expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert_eq!(
        catalog_status(&s, "early").await.as_deref(),
        Some("DELETING"),
        "the row stays until the worker's drop"
    );
    pause.resume();
    let snapshot = open.await.expect("the held open completes");
    let items: Vec<_> = snapshot.items.collect().await;
    assert_eq!(items.len(), 20, "the open streams every item");
    assert!(items.iter().all(Result::is_ok));
}
on_both_layouts!(delete_table_at_before_items_takes_the_async_path);

/// A stream commits its transaction at the fetch that shows the end (a short
/// page), not when the stream next yields `None` or is dropped: with every
/// item read and the stream still held, its backend is idle (out of its
/// transaction, holding no lock on the data table) and a full bound of five
/// opens succeeds. Twenty-two items at a page of five: the fifth fetch
/// returns two rows, so the end is known before the last item is yielded
/// (after `None` the stream's state is dropped, which would release
/// everything whether or not the last page did).
async fn a_drained_stream_ends_its_transaction(shared: bool) {
    use futures::StreamExt as _;
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("drained")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "drained", 22).await;
    let table_id = key_info(&s, "drained").await.table_id;
    let page = PageSize::set(5);
    let mut snapshot = s
        .engine
        .open_table_snapshot(ACCOUNT, "drained")
        .await
        .expect("open");
    snapshot.items.next().await.expect("an item").expect("ok");
    let data = s.data_pool().await;
    let pid = stream_backend(&s, &data).await;
    for _ in 1..22 {
        snapshot
            .items
            .next()
            .await
            .expect("an item")
            .expect("stream item");
    }
    drop(page);
    let (state, locks): (Option<String>, i64) = sqlx::query_as(
        "SELECT (SELECT state FROM pg_stat_activity WHERE pid = $1), \
         (SELECT count(*) FROM pg_locks WHERE pid = $1 AND relation = to_regclass($2))",
    )
    .bind(pid)
    .bind(data_table_name(&table_id))
    .fetch_one(&data)
    .await
    .expect("backend state");
    data.close().await;
    assert_eq!(
        state.as_deref(),
        Some("idle"),
        "the drained stream's backend left its transaction"
    );
    assert_eq!(locks, 0, "and holds no lock on the data table");
    let mut held = Vec::new();
    for i in 0..5 {
        held.push(
            s.engine
                .open_table_snapshot(ACCOUNT, "drained")
                .await
                .unwrap_or_else(|e| panic!("open {i} with the drained stream held: {e}")),
        );
    }
    assert!(
        snapshot.items.next().await.is_none(),
        "twenty-two items, then the end"
    );
}
on_both_layouts!(a_drained_stream_ends_its_transaction);

/// With all but one of the engine's ten connections held (nine
/// `DeleteTable`s of another table, each waiting on that table's row, which
/// an outside session holds `FOR UPDATE`), an open still completes: on the
/// shared layout it needs one connection, and with a separate data database
/// one catalog and one data connection from different pools. An open that
/// took two connections from one pool would wait out the acquire timeout.
async fn an_open_needs_one_connection_per_pool(shared: bool) {
    use futures::StreamExt as _;
    let s = scratch_with(shared).await;
    create_active(&s, plain_input("blocked")).await;
    create_active(&s, snapshot_conformance::table_input("opened")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "opened", 20).await;
    let mut holder = PgConnection::connect(&format!("{}/{}", s.base, s.names[0]))
        .await
        .expect("holder");
    sqlx::query("BEGIN")
        .execute(&mut holder)
        .await
        .expect("begin");
    sqlx::query("SELECT 1 FROM tables WHERE table_name = 'blocked' FOR UPDATE")
        .execute(&mut holder)
        .await
        .expect("hold the row");
    let deletes = futures::future::join_all(
        (0..9).map(|_| s.engine.delete_table(ACCOUNT, delete_input("blocked"))),
    );
    let started = std::time::Instant::now();
    let open_while_held = async {
        // Until all nine deletes hold a connection and wait on the row. Read
        // from a session outside any transaction: pg_stat_activity is
        // snapshotted once per transaction.
        let mut watcher = PgConnection::connect(&format!("{}/postgres", s.base))
            .await
            .expect("watcher");
        let mut waiting = 0;
        for _ in 0..100 {
            waiting = sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = $1 \
                 AND wait_event_type = 'Lock'",
            )
            .bind(&s.names[0])
            .fetch_one(&mut watcher)
            .await
            .expect("waiters");
            if waiting == 9 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        watcher.close().await.expect("close watcher");
        assert_eq!(
            waiting, 9,
            "precondition: nine DeleteTables hold a connection each"
        );
        let opened = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            s.engine.open_table_snapshot(ACCOUNT, "opened"),
        )
        .await;
        let opened = opened
            .expect("the open gets its connections with nine of ten held")
            .expect("open");
        let items: Vec<_> = opened.items.collect().await;
        sqlx::query("ROLLBACK")
            .execute(&mut holder)
            .await
            .expect("release the row");
        items
    };
    let (results, items) = tokio::join!(deletes, open_while_held);
    eprintln!("open and nine waiting deletes took {:?}", started.elapsed());
    assert_eq!(items.len(), 20);
    assert!(items.iter().all(Result::is_ok));
    for result in results {
        match result {
            Ok(_) | Err(extenddb_storage::error::StorageError::TableNotFound(_)) => {}
            Err(e) => panic!("a queued DeleteTable failed: {e}"),
        }
    }
}
on_both_layouts!(an_open_needs_one_connection_per_pool);

/// On one shared database the open holds no row lock: a `DeleteTable` that
/// runs after the open's snapshot and before its `LOCK TABLE` takes the
/// synchronous path and drops the data table, and the open then reports
/// `TableNotFound` (the `LOCK` fails with `undefined_table`), not an internal
/// error. On the separate layout the barrier makes that delete wait, so the
/// case does not arise there.
#[tokio::test]
async fn a_table_dropped_before_the_lock_is_not_found_on_one_shared_database() {
    if base_conn().is_none() {
        eprintln!("SKIP a_table_dropped_before_the_lock_is_not_found_on_one_shared_database");
        return;
    }
    let s = scratch_with(true).await;
    create_active(&s, snapshot_conformance::table_input("gone")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "gone", 5).await;
    let mut pause = extenddb_storage::snapshot::paging::arm_open_pause(
        extenddb_storage::snapshot::paging::OpenStage::AfterDefinition,
    );
    let reached = pause.take_reached();
    let mut open = s.engine.open_table_snapshot(ACCOUNT, "gone");
    tokio::select! {
        r = &mut open => panic!("the open finished while it should be paused: {r:?}"),
        r = reached => r.expect("the open reached AfterDefinition"),
    }
    delete_within_5s(&s, "gone").await.expect("delete");
    assert_eq!(
        catalog_status(&s, "gone").await,
        None,
        "with no lock on the data table yet, DeleteTable took the synchronous path"
    );
    pause.resume();
    let err = open
        .await
        .expect_err("the data table is gone before the open could lock it");
    assert!(
        matches!(err, extenddb_storage::error::StorageError::TableNotFound(_)),
        "{err}"
    );
}

/// The base and GSI data tables of `table` (a `table_input` table).
async fn base_and_gsi_relations(s: &Scratch, table: &str) -> Vec<String> {
    let table_id = key_info(s, table).await.table_id;
    let index_ids: Vec<String> =
        sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1")
            .bind(&table_id)
            .fetch_all(&s.catalog)
            .await
            .expect("index ids");
    assert_eq!(index_ids.len(), 1, "the check table has one GSI");
    std::iter::once(data_table_name(&table_id))
        .chain(index_ids.iter().map(|id| index_table_name(id)))
        .collect()
}

/// Which of `relations` exist on the data database.
async fn present_relations(s: &Scratch, relations: &[String]) -> Vec<String> {
    let data = s.data_pool().await;
    let mut present = Vec::new();
    for relation in relations {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(relation)
            .fetch_one(&data)
            .await
            .expect("probe");
        if exists {
            present.push(relation.clone());
        }
    }
    data.close().await;
    present
}

/// A reader that takes its lock after `DeleteTable`'s in-use probe.
#[derive(Debug, Clone, Copy)]
enum LateReader {
    /// A table snapshot opened in the window (it locks the base data table).
    Open,
    /// A plain session locking only the GSI data table.
    GsiSession,
}

/// On one shared database `DeleteTable` runs its drops in its own catalog
/// transaction, under a 1 s lock timeout, before the catalog removal
/// commits. A reader that takes its lock after the in-use probe (held there
/// by the `DeleteTable` pause) makes a drop time out: the savepoint is
/// rolled back and `DeleteTable` returns `DELETING` with the row and every
/// data table kept, and the control plane drops them once the reader ends.
/// No data table is left without its row at any point. With the drops after
/// the commit, the delete would wait behind the reader with the row already
/// gone.
async fn a_reader_after_the_probe_sends_delete_table_down_the_async_path(how: LateReader) {
    use futures::StreamExt as _;
    let s = scratch_with(true).await;
    create_active(&s, snapshot_conformance::table_input("late")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "late", 5).await;
    let relations = base_and_gsi_relations(&s, "late").await;

    let mut pause = extenddb_storage_postgres::arm_delete_table_pause();
    let reached = pause.take_reached();
    let mut delete = Box::pin(tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.engine.delete_table(ACCOUNT, delete_input("late")),
    ));
    tokio::select! {
        r = &mut delete => panic!("DeleteTable finished while it should be paused: {r:?}"),
        r = reached => r.expect("DeleteTable reached the pause"),
    }
    let (mut snapshot, mut session) = (None, None);
    match how {
        LateReader::Open => {
            snapshot = Some(
                s.engine
                    .open_table_snapshot(ACCOUNT, "late")
                    .await
                    .expect("the open does not wait on DeleteTable's row lock"),
            );
        }
        LateReader::GsiSession => {
            let mut conn = PgConnection::connect(&s.data_url()).await.expect("reader");
            sqlx::query("BEGIN")
                .execute(&mut conn)
                .await
                .expect("begin");
            sqlx::query(&format!("LOCK TABLE {} IN ACCESS SHARE MODE", relations[1]))
                .execute(&mut conn)
                .await
                .expect("lock the GSI data table");
            session = Some(conn);
        }
    }
    pause.resume();
    let deleted = delete
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{how:?}: DeleteTable did not return within 5 s; its drop waited behind the \
                 reader instead of giving up after its lock timeout"
            )
        })
        .expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert_eq!(
        catalog_status(&s, "late").await.as_deref(),
        Some("DELETING"),
        "{how:?}: the row stays while the reader holds the table"
    );
    assert_eq!(
        present_relations(&s, &relations).await,
        relations,
        "{how:?}: the timed-out drop was rolled back with the catalog removal"
    );

    if let Some(snapshot) = snapshot.take() {
        let items: Vec<_> = snapshot.items.collect().await;
        assert_eq!(items.len(), 5, "the snapshot streams every item");
        assert!(items.iter().all(Result::is_ok));
    }
    if let Some(mut conn) = session.take() {
        sqlx::query("ROLLBACK")
            .execute(&mut conn)
            .await
            .expect("release");
        conn.close().await.expect("close reader");
    }
    for _ in 0..100 {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
        let present = present_relations(&s, &relations).await;
        let row = catalog_status(&s, "late").await;
        assert!(
            row.is_some() || present.is_empty(),
            "{how:?}: data tables {present:?} without a catalog row"
        );
        if row.is_none() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{how:?}: the control plane did not drop the table after the reader ended");
}

#[tokio::test]
async fn an_open_after_the_probe_sends_delete_table_down_the_async_path_on_one_shared_database() {
    if base_conn().is_none() {
        eprintln!("SKIP an_open_after_the_probe_...: no PostgreSQL");
        return;
    }
    a_reader_after_the_probe_sends_delete_table_down_the_async_path(LateReader::Open).await;
}

#[tokio::test]
async fn a_gsi_reader_after_the_probe_sends_delete_table_down_the_async_path_on_one_shared_database()
 {
    if base_conn().is_none() {
        eprintln!("SKIP a_gsi_reader_after_the_probe_...: no PostgreSQL");
        return;
    }
    a_reader_after_the_probe_sends_delete_table_down_the_async_path(LateReader::GsiSession).await;
}

/// Hold `ACCESS SHARE` on `relation` from a new session and return it.
async fn hold_relation(s: &Scratch, relation: &str) -> PgConnection {
    let mut holder = PgConnection::connect(&s.data_url()).await.expect("holder");
    sqlx::query("BEGIN")
        .execute(&mut holder)
        .await
        .expect("begin");
    sqlx::query(&format!("LOCK TABLE {relation} IN ACCESS SHARE MODE"))
        .execute(&mut holder)
        .await
        .expect("hold the relation");
    holder
}

async fn release_relation(mut holder: PgConnection) {
    sqlx::query("ROLLBACK")
        .execute(&mut holder)
        .await
        .expect("release");
    holder.close().await.expect("close holder");
}

/// A reader that spans a restart: `DeleteTable` left the row `DELETING`
/// while a session held the data table, and the server restarted (a new
/// engine on the same databases, running the startup recovery `serve` runs)
/// before the reader ended. The startup pass defers the drop and wakes the
/// poller, which starts in its active loop and stays there past its window
/// for as long as the drop is deferred, so the table is gone within 3 s of
/// the release. A poller that started idle would drop it at its first idle
/// pass, 60 s after startup, and one that went idle at the end of its window
/// would drop it at the idle pass after that.
///
/// The window is `control_plane_delay_seconds` (0) + 5 s from the wake, with
/// a pass at once and one every second (each pass probes the held table and
/// skips it, so it takes milliseconds); the pass that finds the window over,
/// at about 5 s, is the sixth. The reader is released only after the poller
/// has made more attempts than the startup pass and those six, so a poller
/// that went idle at the end of its window never gets there and fails the
/// wait instead.
async fn a_drop_deferred_at_startup_is_retried_while_the_reader_holds(shared: bool) {
    /// The startup pass's attempt plus the most the window can start.
    const ATTEMPTS_THE_WINDOW_CAN_MAKE: u64 = 1 + 6;
    let s = scratch_with(shared).await;
    create_active(&s, plain_input("spans")).await;
    let relation = data_table_name(&key_info(&s, "spans").await.table_id);
    let holder = hold_relation(&s, &relation).await;
    let deleted = delete_within_5s(&s, "spans").await.expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );

    let restarted = PostgresEngine::new(
        &PostgresConfig {
            connection_string: format!("{}/{}", s.base, s.names[0]),
            pool_size: 8,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("the restarted engine");
    restarted.recover_control_plane_at_startup_for_tests().await;
    assert_eq!(
        restarted.control_plane_drop_attempts_for_tests(),
        1,
        "precondition: the startup pass tried the drop"
    );
    let _poller = restarted.spawn_control_plane_poller_for_tests();
    let started = std::time::Instant::now();
    // Past the attempts the window can start, so only the deferred drop can
    // have kept the poller active.
    let attempts = wait_for_drop_attempt(&restarted, ATTEMPTS_THE_WINDOW_CAN_MAKE).await;
    assert_eq!(
        catalog_status(&s, "spans").await.as_deref(),
        Some("DELETING"),
        "the poller cannot drop the table while the reader holds it"
    );
    eprintln!(
        "{attempts} drop attempts by the restarted engine {:?} after its poller started, \
         before the release",
        started.elapsed()
    );
    release_relation(holder).await;
    removed_within_3s(
        &s,
        "spans",
        &[relation],
        std::time::Instant::now(),
        "reader across a restart",
    )
    .await;
}
on_both_layouts!(a_drop_deferred_at_startup_is_retried_while_the_reader_holds);

/// A drop that fails for a reason that persists is not a deferred drop. A
/// view on the data table makes `DROP TABLE` fail with 2BP01 (dependent
/// objects) on every attempt; the poller logs it and follows its normal
/// window, a pass every second for `control_plane_delay_seconds` (0) + 5 s
/// and then idle until its 60 s idle pass, rather than retrying every second
/// for as long as the failure lasts. Counted over the 15 s after the window:
/// at most 3 attempts (a poller kept active would make about 15).
async fn a_drop_that_keeps_failing_does_not_keep_the_poller_active(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, plain_input("viewed")).await;
    let relation = data_table_name(&key_info(&s, "viewed").await.table_id);
    let data = s.data_pool().await;
    sqlx::query(&format!(
        "CREATE VIEW viewed_dependent AS SELECT * FROM {relation}"
    ))
    .execute(&data)
    .await
    .expect("a view on the data table");
    // A reader sends DeleteTable down the asynchronous path, so the row is
    // DELETING and the drop is the poller's.
    let holder = hold_relation(&s, &relation).await;
    let deleted = delete_within_5s(&s, "viewed").await.expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    release_relation(holder).await;

    let _poller = s.engine.spawn_control_plane_poller_for_tests();
    tokio::time::sleep(std::time::Duration::from_secs(7)).await;
    let after_window = s.engine.control_plane_drop_attempts_for_tests();
    assert!(
        after_window >= 1,
        "precondition: the poller attempted the drop during its window"
    );
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    let in_15s = s.engine.control_plane_drop_attempts_for_tests() - after_window;
    eprintln!("{after_window} drop attempts in the window, {in_15s} in the 15 s after it");
    assert!(
        in_15s <= 3,
        "{in_15s} drop attempts in 15 s after the window: a persistent failure kept the \
         poller in its one-second loop"
    );
    assert_eq!(
        catalog_status(&s, "viewed").await.as_deref(),
        Some("DELETING"),
        "the drop still fails, so the row stays"
    );
    sqlx::query("DROP VIEW viewed_dependent")
        .execute(&data)
        .await
        .expect("drop the view");
    data.close().await;
}
on_both_layouts!(a_drop_that_keeps_failing_does_not_keep_the_poller_active);

/// What one open-and-delete pair of [`open_and_delete_race`] left.
#[derive(Debug, Default)]
struct RaceTally {
    /// `DeleteTable` removed the row in the request.
    synchronous: usize,
    /// `DeleteTable` returned with the row `DELETING`.
    asynchronous: usize,
    /// The caller's 2 s timeout cancelled `DeleteTable`.
    delete_timed_out: usize,
    /// The open found the table gone.
    open_not_found: usize,
    /// No catalog row, with a data table still present after the stream
    /// ended and a control-plane pass ran.
    orphaned: Vec<String>,
}

/// An open and a `DeleteTable` of the same table started together, with no
/// test hook: in even pairs the delete 0 to 5 ms after the open, in odd ones
/// the open 0 to 5 ms after the delete, so both can win; the stream held 3 s,
/// the delete under a 2 s caller timeout, then a control-plane pass. Counts
/// the pairs that left a data table with no catalog row. Timing-dependent,
/// so not part of the suite: run with `--ignored open_and_delete_race`.
async fn open_and_delete_race(shared: bool) -> RaceTally {
    let s = scratch_with(shared).await;
    let mut tally = RaceTally::default();
    for i in 0..80_u64 {
        let table = format!("race{i:02}");
        create_active(&s, snapshot_conformance::table_input(&table)).await;
        snapshot_conformance::populate(&s.engine, ACCOUNT, &table, 5).await;
        let relations = base_and_gsi_relations(&s, &table).await;
        let stagger = std::time::Duration::from_micros((i * 997) % 5000);
        let open = async {
            if i % 2 == 1 {
                tokio::time::sleep(stagger).await;
            }
            match s.engine.open_table_snapshot(ACCOUNT, &table).await {
                Ok(snapshot) => {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    drop(snapshot);
                    false
                }
                Err(extenddb_storage::error::StorageError::TableNotFound(_)) => true,
                Err(e) => panic!("open {table}: {e}"),
            }
        };
        let delete = async {
            if i % 2 == 0 {
                tokio::time::sleep(stagger).await;
            }
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                s.engine.delete_table(ACCOUNT, delete_input(&table)),
            )
            .await
        };
        let (not_found, deleted) = tokio::join!(open, delete);
        tally.open_not_found += usize::from(not_found);
        let returned = match deleted {
            Err(_) => {
                tally.delete_timed_out += 1;
                false
            }
            Ok(Ok(_)) => true,
            Ok(Err(e)) => panic!("DeleteTable {table}: {e}"),
        };
        let status_after_delete = catalog_status(&s, &table).await;
        if returned {
            if status_after_delete.is_some() {
                tally.asynchronous += 1;
            } else {
                tally.synchronous += 1;
            }
        }
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
        if catalog_status(&s, &table).await.is_none() {
            for relation in present_relations(&s, &relations).await {
                tally.orphaned.push(format!("{table}: {relation}"));
            }
        }
    }
    eprintln!("open-and-delete race, shared = {shared}: {tally:?}");
    tally
}

#[tokio::test]
#[ignore = "timing-dependent race measurement; run by hand"]
async fn open_and_delete_race_on_one_shared_database() {
    if base_conn().is_none() {
        eprintln!("SKIP open_and_delete_race_on_one_shared_database: no PostgreSQL");
        return;
    }
    let tally = open_and_delete_race(true).await;
    assert!(tally.orphaned.is_empty(), "{tally:?}");
    assert!(
        tally.synchronous > 0 && tally.asynchronous > 0,
        "the race never had both outcomes, so it measured nothing: {tally:?}"
    );
}

#[tokio::test]
#[ignore = "timing-dependent race measurement; run by hand"]
async fn open_and_delete_race_on_a_separate_data_database() {
    if base_conn().is_none() {
        eprintln!("SKIP open_and_delete_race_on_a_separate_data_database: no PostgreSQL");
        return;
    }
    let tally = open_and_delete_race(false).await;
    assert!(tally.orphaned.is_empty(), "{tally:?}");
    assert!(
        tally.synchronous > 0 && tally.asynchronous > 0,
        "the race never had both outcomes, so it measured nothing: {tally:?}"
    );
}

/// Hold `relation` in lock `mode` (`ROW EXCLUSIVE` for an open write
/// transaction, `SHARE` for `LOCK ... IN SHARE MODE`, ...) from a new session
/// on the data database and return it.
async fn hold_relation_in(s: &Scratch, relation: &str, mode: &str) -> PgConnection {
    let mut holder = PgConnection::connect(&s.data_url()).await.expect("holder");
    sqlx::query("BEGIN")
        .execute(&mut holder)
        .await
        .expect("begin");
    sqlx::query(&format!("LOCK TABLE {relation} IN {mode} MODE"))
        .execute(&mut holder)
        .await
        .unwrap_or_else(|e| panic!("hold {relation} in {mode} mode: {e}"));
    holder
}

/// Panics when `table` has no catalog row while one of `relations` is still
/// present on the data database: data tables no request can reach or remove.
async fn assert_no_orphan(s: &Scratch, table: &str, relations: &[String], when: &str) {
    let row = catalog_status(s, table).await;
    let present = present_relations(s, relations).await;
    assert!(
        row.is_some() || present.is_empty(),
        "{when}: data tables {present:?} left without a catalog row"
    );
}

/// Pump the control plane until `table` has no catalog row, asserting at
/// every step that no relation of it is left without one.
async fn pump_until_removed(s: &Scratch, table: &str, relations: &[String]) {
    for _ in 0..100 {
        s.engine
            .process_control_plane_transitions()
            .await
            .expect("pump");
        assert_no_orphan(s, table, relations, "while the control plane drops it").await;
        if catalog_status(s, table).await.is_none() {
            assert!(
                present_relations(s, relations).await.is_empty(),
                "the row and every relation go together"
            );
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the control plane did not remove {table} after its holder ended");
}

/// A session that holds the base data table in a mode other than `ACCESS
/// SHARE` (an open write transaction, `LOCK ... IN SHARE MODE`) is not one
/// the in-use probe matches, so `DeleteTable` tries its synchronous drop and
/// waits on it. That wait is bounded by `SYNC_DROP_LOCK_TIMEOUT` (1 s) on
/// both layouts: `DeleteTable` returns `DELETING` with the row and every data
/// table kept, and the control plane drops them once the holder ends. No
/// data table is left without its row at any point, including while the
/// `DeleteTable` waits.
async fn a_non_reader_holder_sends_delete_table_down_the_async_path(shared: bool, mode: &str) {
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("written")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "written", 5).await;
    let relations = base_and_gsi_relations(&s, "written").await;
    let holder = hold_relation_in(&s, &relations[0], mode).await;

    let started = std::time::Instant::now();
    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        s.engine.delete_table(ACCOUNT, delete_input("written")),
    )
    .await;
    let elapsed = started.elapsed();
    assert_no_orphan(&s, "written", &relations, &format!("{mode} holder")).await;
    let deleted = deleted
        .unwrap_or_else(|_| panic!("{mode} holder: DeleteTable did not return within 5 s"))
        .unwrap_or_else(|e| panic!("{mode} holder: DeleteTable failed: {e}"));
    eprintln!("{mode} holder: DeleteTable returned after {elapsed:?}");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "{mode} holder: DeleteTable took {elapsed:?}, more than about its 1 s lock timeout"
    );
    assert_eq!(
        catalog_status(&s, "written").await.as_deref(),
        Some("DELETING"),
        "{mode} holder: the row stays while the holder has the table"
    );
    assert_eq!(
        present_relations(&s, &relations).await,
        relations,
        "{mode} holder: the drop that timed out was rolled back"
    );
    release_relation(holder).await;
    pump_until_removed(&s, "written", &relations).await;
}

async fn a_write_holder_sends_delete_table_down_the_async_path(shared: bool) {
    a_non_reader_holder_sends_delete_table_down_the_async_path(shared, "ROW EXCLUSIVE").await;
}
on_both_layouts!(a_write_holder_sends_delete_table_down_the_async_path);

async fn a_share_holder_sends_delete_table_down_the_async_path(shared: bool) {
    a_non_reader_holder_sends_delete_table_down_the_async_path(shared, "SHARE").await;
}
on_both_layouts!(a_share_holder_sends_delete_table_down_the_async_path);

/// A `DeleteTable` whose caller gives up while it waits on a write holder (a
/// client timeout shorter than the lock timeout, a dropped connection, a
/// server shutting down) leaves the table either `ACTIVE` with its data or
/// `DELETING` for the control plane, never with its row gone and its data
/// tables left: nothing commits before the drops have their locks.
async fn a_cancelled_delete_table_leaves_no_orphan(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("dropped")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "dropped", 5).await;
    let relations = base_and_gsi_relations(&s, "dropped").await;
    let holder = hold_relation_in(&s, &relations[0], "ROW EXCLUSIVE").await;

    let cancelled = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        s.engine.delete_table(ACCOUNT, delete_input("dropped")),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "precondition: DeleteTable was still waiting on the holder at 300 ms: {cancelled:?}"
    );
    // Long enough for the abandoned transactions to end on the server: the
    // drop waits out what is left of its 1 s lock timeout, then rolls back.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert_no_orphan(&s, "dropped", &relations, "after the cancelled DeleteTable").await;
    let status = catalog_status(&s, "dropped").await;
    eprintln!("cancelled DeleteTable left the row {status:?}");
    assert!(
        matches!(status.as_deref(), Some("ACTIVE" | "DELETING")),
        "the row is kept: {status:?}"
    );
    assert_eq!(
        present_relations(&s, &relations).await,
        relations,
        "and so is every data table"
    );
    release_relation(holder).await;
    if status.as_deref() == Some("ACTIVE") {
        let deleted = delete_within_5s(&s, "dropped").await.expect("delete again");
        assert_eq!(
            deleted.table_status,
            extenddb_core::types::TableStatus::Deleting
        );
    }
    pump_until_removed(&s, "dropped", &relations).await;
}
on_both_layouts!(a_cancelled_delete_table_leaves_no_orphan);

/// Connect to the server's `postgres` database as an admin, for commands
/// against the scratch data database itself (its connection limit).
async fn data_admin_pool(s: &Scratch) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{}/postgres", s.base))
        .await
        .expect("admin pool on postgres")
}

/// Terminate the data-database backend that holds a `DeleteTable`'s
/// uncommitted drops, so its data commit fails. At the `BetweenCommits`
/// pause that backend sits idle in its transaction after the drops; it is
/// the only one on the data database in that state.
async fn terminate_held_data_backend(s: &Scratch) {
    let data = s.data_pool().await;
    let mut pid = None;
    for _ in 0..400 {
        pid = sqlx::query_scalar::<_, i32>(
            "SELECT pid FROM pg_stat_activity \
             WHERE datname = $1 AND state = 'idle in transaction' \
             AND pid <> pg_backend_pid() \
             ORDER BY state_change DESC LIMIT 1",
        )
        .bind(s.data_name())
        .fetch_optional(&data)
        .await
        .expect("find the held data backend");
        if pid.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let pid = pid.expect("a data backend idle in its transaction after the drops");
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&data)
        .await
        .expect("terminate");
    assert!(terminated, "pg_terminate_backend({pid})");
    data.close().await;
}

/// Set whether the scratch data database accepts connections. `false` blocks
/// even the superuser the engine connects as (a connection limit does not).
async fn set_data_allow_connections(s: &Scratch, allow: bool) {
    let admin = data_admin_pool(s).await;
    sqlx::query(&format!(
        "ALTER DATABASE \"{}\" WITH ALLOW_CONNECTIONS {allow}",
        s.data_name()
    ))
    .execute(&admin)
    .await
    .expect("set the data database's allow_connections");
    admin.close().await;
}

/// Make the scratch data database refuse every new connection and end the
/// ones it has, so a `DeleteTable`'s data commit and every retry of its drop
/// fail.
async fn make_data_unreachable(s: &Scratch) {
    set_data_allow_connections(s, false).await;
    let admin = data_admin_pool(s).await;
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(s.data_name())
    .execute(&admin)
    .await
    .expect("terminate every data backend");
    admin.close().await;
}

/// Separate data database: when the data commit fails after the catalog
/// commit, the spawned task retries the drop on a fresh connection, so the
/// delete still succeeds and no data table is left without its row. This is
/// the window the PR used to describe as "a process that dies between the
/// two commits"; any failure of the data commit reaches it, with the process
/// up, and the retry closes it. Mutant: return the commit error instead of
/// retrying, and the relations stay.
#[tokio::test]
async fn a_failed_data_commit_after_the_catalog_commit_is_retried() {
    if base_conn().is_none() {
        eprintln!("SKIP a_failed_data_commit_after_the_catalog_commit_is_retried: no PostgreSQL");
        return;
    }
    let s = scratch_with(false).await;
    create_active(&s, snapshot_conformance::table_input("recommit")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "recommit", 5).await;
    let relations = base_and_gsi_relations(&s, "recommit").await;

    let mut pause = extenddb_storage_postgres::arm_delete_table_pause_at(
        extenddb_storage_postgres::DeleteTablePoint::BetweenCommits,
    );
    let reached = pause.take_reached();
    let mut delete = Box::pin(tokio::time::timeout(
        std::time::Duration::from_secs(30),
        s.engine.delete_table(ACCOUNT, delete_input("recommit")),
    ));
    tokio::select! {
        r = &mut delete => panic!("DeleteTable finished before the between-commits pause: {r:?}"),
        r = reached => r.expect("DeleteTable reached the between-commits pause"),
    }
    assert_eq!(
        catalog_status(&s, "recommit").await.as_deref(),
        None,
        "the catalog commit is in by the pause"
    );
    terminate_held_data_backend(&s).await;
    pause.resume();

    let deleted = delete
        .await
        .expect("the retry completes well within 30 s")
        .expect("DeleteTable returns Ok once the drop is redone");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    for _ in 0..50 {
        if present_relations(&s, &relations).await.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        present_relations(&s, &relations).await.is_empty(),
        "the retry dropped every data relation"
    );
    assert_no_orphan(&s, "recommit", &relations, "after the retried drop").await;
}

/// Separate data database: when the data database stays unreachable past the
/// retry bound, the delete gives up, names the leaked relations in its error
/// (and logs them with the `DROP TABLE` to run), and the catalog row is gone
/// regardless. A test-shortened bound keeps it quick.
#[tokio::test]
async fn an_unreachable_data_database_past_the_bound_reports_the_leaked_relations() {
    if base_conn().is_none() {
        eprintln!(
            "SKIP an_unreachable_data_database_past_the_bound_reports_the_leaked_relations: no \
             PostgreSQL"
        );
        return;
    }
    let s = scratch_with(false).await;
    let _bound = extenddb_storage_postgres::shorten_data_drop_retry_bound_for_tests(
        std::time::Duration::from_millis(500),
    );
    create_active(&s, snapshot_conformance::table_input("stranded")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "stranded", 5).await;
    let relations = base_and_gsi_relations(&s, "stranded").await;

    let mut pause = extenddb_storage_postgres::arm_delete_table_pause_at(
        extenddb_storage_postgres::DeleteTablePoint::BetweenCommits,
    );
    let reached = pause.take_reached();
    let mut delete = Box::pin(tokio::time::timeout(
        std::time::Duration::from_secs(45),
        s.engine.delete_table(ACCOUNT, delete_input("stranded")),
    ));
    tokio::select! {
        r = &mut delete => panic!("DeleteTable finished before the pause: {r:?}"),
        r = reached => r.expect("DeleteTable reached the between-commits pause"),
    }
    make_data_unreachable(&s).await;
    pause.resume();

    let err = delete
        .await
        .expect("the delete gives up within the shortened bound")
        .expect_err("an unreachable data database past the bound is an error");
    let msg = err.to_string();
    for relation in &relations {
        let core = relation.trim_matches('"');
        assert!(msg.contains(core), "the error names {core}: {msg}");
    }
    assert_eq!(
        catalog_status(&s, "stranded").await.as_deref(),
        None,
        "the catalog row is removed even though the data drop could not be redone"
    );
    // Let the engine reach the data database again so the Drop cleanup can
    // force-drop it.
    set_data_allow_connections(&s, true).await;
}

/// Separate data database: a caller that gives up after the catalog commit
/// does not stop the data drop. The commits and retries run in a task the
/// request spawns, so dropping the request only detaches it. Mutant: run the
/// body inline instead of spawning, and a dropped request strands the data
/// tables.
#[tokio::test]
async fn a_caller_cancelled_between_the_commits_does_not_strand_the_data() {
    if base_conn().is_none() {
        eprintln!(
            "SKIP a_caller_cancelled_between_the_commits_does_not_strand_the_data: no PostgreSQL"
        );
        return;
    }
    let s = scratch_with(false).await;
    create_active(&s, snapshot_conformance::table_input("detached")).await;
    snapshot_conformance::populate(&s.engine, ACCOUNT, "detached", 5).await;
    let relations = base_and_gsi_relations(&s, "detached").await;

    let mut pause = extenddb_storage_postgres::arm_delete_table_pause_at(
        extenddb_storage_postgres::DeleteTablePoint::BetweenCommits,
    );
    let reached = pause.take_reached();
    let mut delete = Box::pin(s.engine.delete_table(ACCOUNT, delete_input("detached")));
    tokio::select! {
        r = &mut delete => panic!("DeleteTable finished before the pause: {r:?}"),
        r = reached => r.expect("DeleteTable reached the between-commits pause"),
    }
    // The caller gives up now, after the catalog commit.
    drop(delete);
    // Let the detached task continue past the pause and commit the data drop.
    pause.resume();

    for _ in 0..50 {
        if catalog_status(&s, "detached").await.is_none()
            && present_relations(&s, &relations).await.is_empty()
        {
            assert_no_orphan(&s, "detached", &relations, "after the detached drop").await;
            return;
        }
        assert_no_orphan(&s, "detached", &relations, "while the detached task runs").await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the detached task did not finish the drop after the caller was cancelled");
}

/// A hash-only table with three GSIs, for the drop-deadline test.
fn three_gsi_input(name: &str) -> extenddb_core::types::CreateTableInput {
    let gsi = |i: usize| {
        serde_json::json!({
            "IndexName": format!("g{i}"),
            "KeySchema": [{"AttributeName": format!("a{i}"), "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "KEYS_ONLY"}
        })
    };
    serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "a0", "AttributeType": "S"},
            {"AttributeName": "a1", "AttributeType": "S"},
            {"AttributeName": "a2", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [gsi(0), gsi(1), gsi(2)]
    }))
    .expect("create input")
}

/// The drops of one `DeleteTable` share one deadline. A write holder on the
/// base data table and on each of three GSI data tables, released 0.8 s
/// apart in drop order, keeps every single wait under the 1 s lock timeout;
/// a timeout per `DROP` would have the request wait for all four, about
/// 3.2 s, while it holds the base table's `ACCESS EXCLUSIVE` and stalls every
/// reader of the table. With one deadline the drops give up when the 1 s
/// budget is spent, and `DeleteTable` returns `DELETING` at about 1 s.
async fn the_drops_of_one_delete_table_share_one_deadline(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, three_gsi_input("many_gsis")).await;
    let table_id = key_info(&s, "many_gsis").await.table_id;
    // The drop order: the base table, then the GSIs in the order the
    // engine reads their ids.
    let index_ids: Vec<String> =
        sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1")
            .bind(&table_id)
            .fetch_all(&s.catalog)
            .await
            .expect("index ids");
    assert_eq!(index_ids.len(), 3, "precondition: three GSIs");
    let relations: Vec<String> = std::iter::once(data_table_name(&table_id))
        .chain(index_ids.iter().map(|id| index_table_name(id)))
        .collect();
    let mut holders = Vec::new();
    for relation in &relations {
        holders.push(hold_relation_in(&s, relation, "ROW EXCLUSIVE").await);
    }

    let started = std::time::Instant::now();
    let release = async {
        for holder in holders {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            release_relation(holder).await;
        }
    };
    let delete = async {
        let deleted = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            s.engine.delete_table(ACCOUNT, delete_input("many_gsis")),
        )
        .await;
        (deleted, started.elapsed())
    };
    let ((deleted, elapsed), ()) = tokio::join!(delete, release);
    let deleted = deleted
        .expect("DeleteTable returned within 10 s")
        .expect("DeleteTable");
    eprintln!("staggered holders: DeleteTable returned after {elapsed:?}");
    assert!(
        elapsed < std::time::Duration::from_millis(1600),
        "DeleteTable took {elapsed:?}: its drops waited once per relation instead of sharing \
         one 1 s budget"
    );
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert_eq!(
        catalog_status(&s, "many_gsis").await.as_deref(),
        Some("DELETING"),
        "the budget ran out with holders left, so the drop was deferred"
    );
    pump_until_removed(&s, "many_gsis", &relations).await;
}
on_both_layouts!(the_drops_of_one_delete_table_share_one_deadline);

/// The pids of sessions waiting (not granted) for a lock on one of
/// `relations` on the data database.
async fn lock_waiters(s: &Scratch, relations: &[String]) -> Vec<i32> {
    let data = s.data_pool().await;
    let pids = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'relation' AND NOT granted \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         AND relation = ANY (SELECT to_regclass(n)::oid FROM unnest($1::text[]) AS n)",
    )
    .bind(relations)
    .fetch_all(&data)
    .await
    .expect("lock waiters");
    data.close().await;
    pids
}

/// Four tables left `DELETING` by readers, the production poller running.
struct HeldDeletes {
    tables: Vec<String>,
    relations: Vec<String>,
    holders: Vec<PgConnection>,
}

async fn four_held_deletes(s: &Scratch) -> HeldDeletes {
    let tables: Vec<String> = (0..4).map(|i| format!("held{i}")).collect();
    let mut relations = Vec::new();
    let mut holders = Vec::new();
    for table in &tables {
        create_active(s, plain_input(table)).await;
        let relation = data_table_name(&key_info(s, table).await.table_id);
        holders.push(hold_relation(s, &relation).await);
        let deleted = delete_within_5s(s, table).await.expect("delete");
        assert_eq!(
            deleted.table_status,
            extenddb_core::types::TableStatus::Deleting
        );
        relations.push(relation);
    }
    HeldDeletes {
        tables,
        relations,
        holders,
    }
}

/// With four tables `DELETING` behind readers, the poller probes each one
/// for holders and skips it without waiting, so a pass costs a few queries
/// however many tables are held, rather than a lock timeout per table: no
/// drop ever queues for a held table's lock, and a table whose reader ends
/// is gone within about `active_poll` (1 s) of the release.
async fn held_deleting_tables_are_probed_not_waited_on(shared: bool) {
    let s = scratch_with(shared).await;
    let HeldDeletes {
        tables,
        relations,
        mut holders,
    } = four_held_deletes(&s).await;
    let _poller = s.engine.spawn_control_plane_poller_for_tests();
    // Two passes over the four tables: the poller is retrying them.
    wait_for_drop_attempt(&s.engine, 3).await;
    let watch_until = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < watch_until {
        let waiters = lock_waiters(&s, &relations).await;
        assert!(
            waiters.is_empty(),
            "the control plane queued for a held table's lock (pids {waiters:?}) instead of \
             skipping it"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let attempts = s.engine.control_plane_drop_attempts_for_tests();
    release_relation(holders.remove(0)).await;
    let released_at = std::time::Instant::now();
    let data = s.data_pool().await;
    loop {
        let gone: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NULL")
            .bind(&relations[0])
            .fetch_one(&data)
            .await
            .expect("probe");
        if gone && catalog_status(&s, &tables[0]).await.is_none() {
            break;
        }
        assert!(
            released_at.elapsed() < std::time::Duration::from_secs(2),
            "{} not removed within 2 s of its reader's release with three other tables held",
            tables[0]
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    data.close().await;
    eprintln!(
        "four held tables: {attempts} attempts before the release; the released table was \
         removed {:?} after it",
        released_at.elapsed()
    );
    for table in &tables[1..] {
        assert_eq!(
            catalog_status(&s, table).await.as_deref(),
            Some("DELETING"),
            "{table} is still held"
        );
    }
    for holder in holders {
        release_relation(holder).await;
    }
}
on_both_layouts!(held_deleting_tables_are_probed_not_waited_on);

/// A `CreateTable` reaches `ACTIVE` at its usual pace while other tables'
/// drops are deferred: the passes that retry them do not wait on their
/// holders, so the next pass after the table is due comes about
/// `active_poll` (1 s) later.
async fn create_table_is_not_held_up_by_deferred_drops(shared: bool) {
    let s = scratch_with(shared).await;
    let held = four_held_deletes(&s).await;
    let _poller = s.engine.spawn_control_plane_poller_for_tests();
    wait_for_drop_attempt(&s.engine, 3).await;
    sqlx::query("UPDATE settings SET value = '0.25' WHERE key = 'control_plane_delay_seconds'")
        .execute(&s.catalog)
        .await
        .expect("a delay, so CreateTable goes through the control plane");
    let started = std::time::Instant::now();
    let created = s
        .engine
        .create_table(ACCOUNT, plain_input("fresh"))
        .await
        .expect("create");
    assert_eq!(
        created.table_status,
        extenddb_core::types::TableStatus::Creating,
        "precondition: the table waits for the control plane"
    );
    while catalog_status(&s, "fresh").await.as_deref() != Some("ACTIVE") {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "CreateTable was not ACTIVE within 2 s with four drops deferred"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    eprintln!(
        "CreateTable ACTIVE after {:?} with four drops deferred",
        started.elapsed()
    );
    for table in &held.tables {
        assert_eq!(
            catalog_status(&s, table).await.as_deref(),
            Some("DELETING"),
            "precondition: {table} is still held"
        );
    }
    for holder in held.holders {
        release_relation(holder).await;
    }
}
on_both_layouts!(create_table_is_not_held_up_by_deferred_drops);

/// `DeleteTable` of a table that is already `DELETING` (a retried or
/// duplicated request while a reader holds it) returns the table's
/// description with `TableStatus: DELETING`, as the service does, and
/// changes nothing: the row and its scheduled removal stay as they were.
async fn delete_table_of_a_deleting_table_returns_its_description(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("twice")).await;
    let relations = base_and_gsi_relations(&s, "twice").await;
    let holder = hold_relation(&s, &relations[0]).await;
    let first = delete_within_5s(&s, "twice").await.expect("first delete");
    assert_eq!(
        first.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    let scheduled = || async {
        sqlx::query_scalar::<_, String>(
            "SELECT table_status || ' ' || status_transition_at::text FROM tables \
             WHERE table_name = 'twice'",
        )
        .fetch_one(&s.catalog)
        .await
        .expect("row")
    };
    let before = scheduled().await;
    assert!(before.starts_with("DELETING "), "precondition: {before}");

    let second = delete_within_5s(&s, "twice")
        .await
        .expect("DeleteTable of a DELETING table returns its description");
    assert_eq!(
        second.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert_eq!(second.table_arn, first.table_arn);
    assert_eq!(second.table_id, first.table_id);
    assert_eq!(second.key_schema, first.key_schema);
    assert_eq!(
        second
            .global_secondary_indexes
            .iter()
            .flatten()
            .map(|g| g.index_name.clone())
            .collect::<Vec<_>>(),
        ["gi"],
        "the description lists the indexes"
    );
    assert_eq!(
        scheduled().await,
        before,
        "the second DeleteTable changed nothing"
    );
    release_relation(holder).await;
    pump_until_removed(&s, "twice", &relations).await;
}
on_both_layouts!(delete_table_of_a_deleting_table_returns_its_description);

/// A drop deferred by a reader is due at once, whatever
/// `control_plane_delay_seconds` is (below 1, so `DeleteTable` tried to drop
/// in the request): the table is already being removed, and a reader that
/// ends right after the `DeleteTable` has the next pass drop it rather than a
/// pass after the delay. Here the reader ends before the poller starts, and
/// the poller's first pass, a few milliseconds later and well inside the
/// 0.9 s delay, removes the table; with the drop scheduled after the delay
/// that pass finds nothing due and the next comes 10 s later.
async fn a_deferred_drop_is_due_at_once(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, plain_input("due")).await;
    let relation = data_table_name(&key_info(&s, "due").await.table_id);
    sqlx::query("UPDATE settings SET value = '0.9' WHERE key = 'control_plane_delay_seconds'")
        .execute(&s.catalog)
        .await
        .expect("setting");
    let holder = hold_relation(&s, &relation).await;
    let deleted = delete_within_5s(&s, "due").await.expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    let due_now: bool = sqlx::query_scalar(
        "SELECT status_transition_at <= NOW() FROM tables WHERE table_name = 'due'",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("row");
    assert!(
        due_now,
        "the deferred drop is scheduled after control_plane_delay_seconds"
    );
    release_relation(holder).await;
    let _poller = s
        .engine
        .spawn_control_plane_poller_with_active_poll_for_tests(WAKE_TEST_ACTIVE_POLL);
    removed_within_3s(
        &s,
        "due",
        &[relation],
        std::time::Instant::now(),
        "deferred drop",
    )
    .await;
}
on_both_layouts!(a_deferred_drop_is_due_at_once);

/// An engine on `s`'s databases whose connections take the settings
/// `ALTER DATABASE ... SET` gave the data database after `s` was created.
async fn engine_after_alter(s: &Scratch) -> PostgresEngine {
    PostgresEngine::new(
        &PostgresConfig {
            connection_string: format!("{}/{}", s.base, s.names[0]),
            pool_size: 8,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("engine")
}

/// `ALTER DATABASE <data database> SET <setting> = <value>`, for the
/// connections opened afterwards.
async fn alter_data_database(s: &Scratch, setting: &str, value: &str) {
    let mut admin = PgConnection::connect(&format!("{}/postgres", s.base))
        .await
        .expect("admin");
    sqlx::query(&format!(
        "ALTER DATABASE \"{}\" SET {setting} = '{value}'",
        s.data_name()
    ))
    .execute(&mut admin)
    .await
    .expect("alter database");
    admin.close().await.expect("close admin");
}

/// A `statement_timeout` shorter than the drop's lock timeout (set here on
/// the data database, as an administrator might set it for the role or the
/// database) ends the drop's wait with 57014 rather than 55P03. That is a
/// lock wait like the other, and `DeleteTable` takes its asynchronous path
/// rather than failing.
async fn a_statement_timeout_sends_delete_table_down_the_async_path(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, plain_input("timed")).await;
    let relation = data_table_name(&key_info(&s, "timed").await.table_id);
    alter_data_database(&s, "statement_timeout", "300ms").await;
    let engine = engine_after_alter(&s).await;
    let holder = hold_relation_in(&s, &relation, "ROW EXCLUSIVE").await;
    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        engine.delete_table(ACCOUNT, delete_input("timed")),
    )
    .await
    .expect("DeleteTable returned within 5 s")
    .unwrap_or_else(|e| panic!("a statement timeout failed DeleteTable: {e}"));
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert_eq!(
        catalog_status(&s, "timed").await.as_deref(),
        Some("DELETING")
    );
    release_relation(holder).await;
    pump_until_removed(&s, "timed", std::slice::from_ref(&relation)).await;
}
on_both_layouts!(a_statement_timeout_sends_delete_table_down_the_async_path);

/// A `DeleteTable` chosen as a deadlock victim (40P01) takes its
/// asynchronous path rather than failing. The cycle: a session holds the
/// base data table `ROW EXCLUSIVE`; the drop deletes the table's pending
/// queue rows (`ROW EXCLUSIVE` on `gsi_pending`) and waits on the base
/// table; the session then asks for `gsi_pending` in `SHARE` mode, which
/// waits on the drop. The drop's connections have a 400 ms
/// `deadlock_timeout` and the session's is 10 s, so the drop runs the
/// deadlock check, well before its 1 s lock timeout, and is the victim.
async fn a_deadlock_victim_delete_table_takes_the_async_path(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, plain_input("victim")).await;
    let relation = data_table_name(&key_info(&s, "victim").await.table_id);
    alter_data_database(&s, "deadlock_timeout", "400ms").await;
    let engine = engine_after_alter(&s).await;
    let mut holder = PgConnection::connect(&s.data_url()).await.expect("holder");
    for statement in [
        "SET deadlock_timeout = '10s'".to_owned(),
        "BEGIN".to_owned(),
        format!("LOCK TABLE {relation} IN ROW EXCLUSIVE MODE"),
    ] {
        sqlx::query(&statement)
            .execute(&mut holder)
            .await
            .expect("holder");
    }
    let delete = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        engine.delete_table(ACCOUNT, delete_input("victim")),
    );
    let close_the_cycle = async {
        let mut watcher = PgConnection::connect(&format!("{}/postgres", s.base))
            .await
            .expect("watcher");
        let mut waiting = 0;
        for _ in 0..200 {
            waiting = sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = $1 \
                 AND wait_event_type = 'Lock' AND query LIKE 'DROP TABLE%'",
            )
            .bind(s.data_name())
            .fetch_one(&mut watcher)
            .await
            .expect("waiters");
            if waiting == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        watcher.close().await.expect("close watcher");
        assert_eq!(waiting, 1, "precondition: the drop waits on the holder");
        let started = std::time::Instant::now();
        sqlx::query("LOCK TABLE gsi_pending IN SHARE MODE")
            .execute(&mut holder)
            .await
            .expect("the holder's lock is granted once the drop is gone");
        started.elapsed()
    };
    let (deleted, waited) = tokio::join!(delete, close_the_cycle);
    let deleted = deleted
        .expect("DeleteTable returned within 5 s")
        .unwrap_or_else(|e| panic!("a deadlock failed DeleteTable: {e}"));
    eprintln!("the holder waited {waited:?} for the drop to give way");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert!(
        waited < std::time::Duration::from_millis(900),
        "the drop gave way after {waited:?}: it was not the deadlock victim but ran out its \
         lock timeout"
    );
    assert_eq!(
        catalog_status(&s, "victim").await.as_deref(),
        Some("DELETING")
    );
    sqlx::query("ROLLBACK")
        .execute(&mut holder)
        .await
        .expect("release");
    holder.close().await.expect("close holder");
    pump_until_removed(&s, "victim", std::slice::from_ref(&relation)).await;
}
on_both_layouts!(a_deadlock_victim_delete_table_takes_the_async_path);

/// A reader found by the in-use probe sends `DeleteTable` down the
/// asynchronous path without trying the drop, on both layouts: it returns in
/// well under the 1 s lock timeout a drop would wait out, and queues no
/// `ACCESS EXCLUSIVE` request that would stall the table's new readers.
async fn a_probed_reader_sends_delete_table_down_the_async_path_at_once(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, plain_input("probed")).await;
    let relation = data_table_name(&key_info(&s, "probed").await.table_id);
    let holder = hold_relation(&s, &relation).await;
    let started = std::time::Instant::now();
    let deleted = delete_within_5s(&s, "probed").await.expect("delete");
    let elapsed = started.elapsed();
    eprintln!("probed reader: DeleteTable returned after {elapsed:?}");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "DeleteTable took {elapsed:?}: it tried the drop instead of seeing the reader"
    );
    release_relation(holder).await;
    pump_until_removed(&s, "probed", std::slice::from_ref(&relation)).await;
}
on_both_layouts!(a_probed_reader_sends_delete_table_down_the_async_path_at_once);

/// A queue worker's claim on one of the table's pending GSI rows holds
/// `DeleteTable` for as long as the claim lasts, with no timeout, on both
/// layouts: the pending-row delete waits for it before any lock timeout is
/// set, and nothing commits meanwhile, so the row and every data table stay
/// until the claim ends. Then the drops run and the table is removed in the
/// request. The claim is held by hand (the pending row `FOR UPDATE`, the base
/// row `FOR SHARE`, as a worker mid-apply holds them), so the wait is what is
/// tested.
async fn a_claimed_queue_row_holds_delete_table_until_it_ends(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("claimed")).await;
    let relations = base_and_gsi_relations(&s, "claimed").await;
    let table_id = key_info(&s, "claimed").await.table_id;
    sqlx::query("UPDATE settings SET value = '600000' WHERE key = 'index_propagation_delay_ms'")
        .execute(&s.catalog)
        .await
        .expect("hold queue rows back so one can be claimed by hand");
    snapshot_conformance::populate(&s.engine, ACCOUNT, "claimed", 1).await;
    let mut claim = PgConnection::connect(&s.data_url()).await.expect("claim");
    sqlx::query("BEGIN")
        .execute(&mut claim)
        .await
        .expect("begin");
    sqlx::query("SELECT id FROM gsi_pending WHERE table_id = $1 ORDER BY id LIMIT 1 FOR UPDATE")
        .bind(&table_id)
        .fetch_one(&mut claim)
        .await
        .expect("a pending row to claim");
    sqlx::query(&format!(
        "SELECT 1 FROM {} WHERE pk = 'p000' FOR SHARE",
        relations[0]
    ))
    .fetch_one(&mut claim)
    .await
    .expect("share-lock the base row");

    let mut delete = Box::pin(s.engine.delete_table(ACCOUNT, delete_input("claimed")));
    let early = tokio::time::timeout(std::time::Duration::from_millis(2500), &mut delete).await;
    assert!(
        early.is_err(),
        "DeleteTable returned while a claim for the table was held: {early:?}"
    );
    assert_eq!(
        catalog_status(&s, "claimed").await.as_deref(),
        Some("ACTIVE"),
        "nothing commits while the claim is in flight"
    );
    assert_eq!(present_relations(&s, &relations).await, relations);

    sqlx::query("COMMIT")
        .execute(&mut claim)
        .await
        .expect("end the claim");
    let deleted = tokio::time::timeout(std::time::Duration::from_secs(10), &mut delete)
        .await
        .expect("DeleteTable finished once the claim ended")
        .expect("delete");
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert_eq!(
        catalog_status(&s, "claimed").await,
        None,
        "removed in the request"
    );
    assert!(present_relations(&s, &relations).await.is_empty());
    claim.close().await.expect("close claim");
}
on_both_layouts!(a_claimed_queue_row_holds_delete_table_until_it_ends);

/// `DeleteTable` drops the base data table before the index tables, the
/// order a write takes its locks in (the base row, then an LSI or a
/// synchronous GSI in the same transaction). A write that holds the base
/// when the drops start, and then writes the GSI table, is therefore not
/// held up: the drop waits for it on the base, holding nothing the write
/// needs, and once it commits the table is removed in the request. With the
/// index tables dropped first the drop would hold the GSI table while it
/// waits on the base, the write would wait on the drop, and the two would
/// wait for each other until the drops' 1 s budget ran out.
async fn a_write_in_progress_finishes_before_delete_table_drops(shared: bool) {
    let s = scratch_with(shared).await;
    create_active(&s, snapshot_conformance::table_input("written2")).await;
    let relations = base_and_gsi_relations(&s, "written2").await;
    let mut writer = hold_relation_in(&s, &relations[0], "ROW EXCLUSIVE").await;
    let delete = async {
        let started = std::time::Instant::now();
        let deleted = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            s.engine.delete_table(ACCOUNT, delete_input("written2")),
        )
        .await;
        (deleted, started.elapsed())
    };
    let write_the_index = async {
        let mut watcher = PgConnection::connect(&format!("{}/postgres", s.base))
            .await
            .expect("watcher");
        let mut waiting = 0;
        for _ in 0..400 {
            waiting = sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = $1 \
                 AND wait_event_type = 'Lock' AND query LIKE 'DROP TABLE%'",
            )
            .bind(s.data_name())
            .fetch_one(&mut watcher)
            .await
            .expect("waiters");
            if waiting == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        watcher.close().await.expect("close watcher");
        assert_eq!(waiting, 1, "precondition: a drop waits on the writer");
        let started = std::time::Instant::now();
        sqlx::query(&format!(
            "LOCK TABLE {} IN ROW EXCLUSIVE MODE",
            relations[1]
        ))
        .execute(&mut writer)
        .await
        .expect("the writer's index write");
        let waited = started.elapsed();
        sqlx::query("COMMIT")
            .execute(&mut writer)
            .await
            .expect("commit");
        waited
    };
    let ((deleted, took), waited) = tokio::join!(delete, write_the_index);
    let deleted = deleted
        .expect("DeleteTable returned within 5 s")
        .expect("delete");
    eprintln!("the writer's index write waited {waited:?}; DeleteTable took {took:?}");
    assert!(
        waited < std::time::Duration::from_millis(500),
        "the writer's index write waited {waited:?} on DeleteTable's drops"
    );
    assert_eq!(
        deleted.table_status,
        extenddb_core::types::TableStatus::Deleting
    );
    assert_eq!(
        catalog_status(&s, "written2").await,
        None,
        "the drops waited for the write and removed the table in the request"
    );
    assert!(present_relations(&s, &relations).await.is_empty());
    writer.close().await.expect("close writer");
}
on_both_layouts!(a_write_in_progress_finishes_before_delete_table_drops);
