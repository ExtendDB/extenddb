// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The scratch PostgreSQL fixture of the item-history tests: one database,
//! or a catalog and a data database, built from the shipped migrations and
//! dropped afterwards, on a panic too.

use std::time::SystemTime;

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, Item, KeySchemaElement, KeyType,
    ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::pitr::PitrCaptureStore;
use extenddb_storage::pitr::conformance::HistoryFixture;
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use futures::future::BoxFuture;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

pub struct Scratch {
    pub engine: PostgresEngine,
    /// The data database, which holds `pitr_state` and `item_history`.
    pub db: PgPool,
    /// The catalog database, the same as `db` unless the scratch has two.
    pub catalog: PgPool,
    admin: PgPool,
    guard: DropDatabases,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

/// Drops the scratch databases when dropped while armed: on a panic, when
/// [`close`] never runs. A thread of its own runs the drop, since a panic
/// unwinds inside the test's runtime.
struct DropDatabases {
    base: String,
    names: Vec<String>,
    armed: bool,
}

impl Drop for DropDatabases {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (base, names) = (self.base.clone(), std::mem::take(&mut self.names));
        let _ = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async {
                let Ok(admin) = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&format!("{base}/postgres"))
                    .await
                else {
                    return;
                };
                for name in names {
                    let _ =
                        sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
                            .execute(&admin)
                            .await;
                }
                admin.close().await;
            });
        })
        .join();
    }
}

/// Scratch databases open at once. Each holds about fifteen connections, and
/// the server's default limit is 100.
const SCRATCH_SLOTS: usize = 4;

async fn slot() -> tokio::sync::OwnedSemaphorePermit {
    static SLOTS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    SLOTS
        .get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(SCRATCH_SLOTS)))
        .clone()
        .acquire_owned()
        .await
        .expect("slot")
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

fn shipped_migrations(dirs: &[&str]) -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for dir in dirs {
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

pub async fn open() -> Option<Scratch> {
    open_scratch(false).await
}

/// A scratch whose catalog and data are two databases, as a deployment
/// runs them.
pub async fn open_two_databases() -> Option<Scratch> {
    open_scratch(true).await
}

async fn create_database(admin: &PgPool, base: &str, guard: &mut DropDatabases) -> PgPool {
    let name = format!("eddb_pitr_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    sqlx::query(&format!("CREATE DATABASE \"{name}\""))
        .execute(admin)
        .await
        .expect("create a scratch database");
    guard.names.push(name.clone());
    PgPoolOptions::new()
        .max_connections(3)
        .connect(&format!("{base}/{name}"))
        .await
        .expect("connect to the scratch database")
}

async fn apply(pool: &PgPool, dirs: &[&str]) {
    for sql in shipped_migrations(dirs) {
        sqlx::raw_sql(&sql)
            .execute(pool)
            .await
            .expect("apply a shipped migration");
    }
}

async fn open_scratch(two_databases: bool) -> Option<Scratch> {
    let Some(base) = base_conn() else {
        eprintln!(
            "SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
             to build a scratch catalog in."
        );
        return None;
    };
    let slot = slot().await;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    let mut guard = DropDatabases {
        base: base.clone(),
        names: Vec::new(),
        armed: true,
    };
    let catalog = create_database(&admin, &base, &mut guard).await;
    let catalog_name = guard.names[0].clone();
    let db = if two_databases {
        apply(&catalog, &["migrations"]).await;
        let data = create_database(&admin, &base, &mut guard).await;
        apply(&data, &["data_migrations"]).await;
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(format!("{base}/{}", guard.names[1]))
        .execute(&catalog)
        .await
        .expect("name the data database");
        data
    } else {
        apply(&catalog, &["migrations", "data_migrations"]).await;
        sqlx::query(
            "ALTER TABLE stream_shards DROP CONSTRAINT IF EXISTS stream_shards_table_id_fkey",
        )
        .execute(&catalog)
        .await
        .expect("match the two-database layout");
        catalog.clone()
    };
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&catalog)
    .await
    .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{catalog_name}"))
        .execute(&catalog)
        .await
        .expect("seed the account row");
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: format!("{base}/{catalog_name}"),
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database");
    Some(Scratch {
        engine,
        db,
        catalog,
        admin,
        guard,
        _slot: slot,
    })
}

pub async fn close(s: Scratch) {
    let Scratch {
        engine,
        db,
        catalog,
        admin,
        mut guard,
        _slot,
    } = s;
    drop(engine);
    db.close().await;
    catalog.close().await;
    for name in &guard.names {
        sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(&admin)
            .await
            .expect("drop the scratch database");
    }
    guard.armed = false;
    admin.close().await;
}

impl HistoryFixture for Scratch {
    fn store(&self) -> &dyn PitrCaptureStore {
        &self.engine
    }

    fn data(&self) -> &dyn DataEngine {
        &self.engine
    }

    fn table<'a>(
        &'a self,
        name: &'a str,
        sk: Option<ScalarAttributeType>,
        enabled: bool,
    ) -> BoxFuture<'a, TableKeyInfo> {
        Box::pin(async move {
            let mut key_schema = vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }];
            let mut attribute_definitions = vec![AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            }];
            if let Some(t) = sk {
                key_schema.push(KeySchemaElement {
                    attribute_name: "sk".to_owned(),
                    key_type: KeyType::Range,
                });
                attribute_definitions.push(AttributeDefinition {
                    attribute_name: "sk".to_owned(),
                    attribute_type: t,
                });
            }
            self.engine
                .create_table(
                    ACCOUNT,
                    CreateTableInput {
                        table_name: name.to_owned(),
                        key_schema,
                        attribute_definitions,
                        billing_mode: Some(BillingMode::PayPerRequest),
                        ..Default::default()
                    },
                )
                .await
                .expect("create the table");
            let info = self
                .engine
                .table_key_info(ACCOUNT, name)
                .await
                .expect("read the key info");
            if enabled {
                sqlx::query(
                    "INSERT INTO pitr_state (table_id, recovery_period_days, enabled_at_us) \
                     VALUES ($1, 35, 0)",
                )
                .bind(&info.table_id)
                .execute(&self.db)
                .await
                .expect("pitr_state");
            }
            info
        })
    }

    fn record<'a>(
        &'a self,
        info: &'a TableKeyInfo,
        key: &'a Item,
        image: Option<&'a Item>,
        at: SystemTime,
        commit: bool,
    ) -> BoxFuture<'a, bool> {
        Box::pin(async move {
            self.engine
                .record_item_history_for_tests(info, key, image, at, commit)
                .await
                .expect("record")
        })
    }

    fn commit_times<'a>(&'a self, table_id: &'a str) -> BoxFuture<'a, Vec<i64>> {
        Box::pin(async move {
            sqlx::query_scalar(
                "SELECT commit_time_us FROM item_history WHERE table_id = $1 ORDER BY seq",
            )
            .bind(table_id)
            .fetch_all(&self.db)
            .await
            .expect("commit times")
        })
    }

    fn live_items<'a>(&'a self, info: &'a TableKeyInfo) -> BoxFuture<'a, Vec<Item>> {
        Box::pin(async move {
            let sql = format!("SELECT item_data FROM \"_ddb_{}\"", info.table_id);
            let rows: Vec<serde_json::Value> = sqlx::query_scalar(&sql)
                .fetch_all(&self.db)
                .await
                .expect("live items");
            rows.into_iter()
                .map(|v| serde_json::from_value(v).expect("item"))
                .collect()
        })
    }

    fn history_rows<'a>(&'a self, table_id: &'a str) -> BoxFuture<'a, i64> {
        Box::pin(async move {
            sqlx::query_scalar("SELECT COUNT(*) FROM item_history WHERE table_id = $1")
                .bind(table_id)
                .fetch_one(&self.db)
                .await
                .expect("count")
        })
    }

    fn execute<'a>(&'a self, sql: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            sqlx::query(sql).execute(&self.db).await.expect("execute");
        })
    }
}
