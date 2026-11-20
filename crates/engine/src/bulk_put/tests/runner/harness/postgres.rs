// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The harness on a throwaway PostgreSQL database, for the runner cases
//! that must hold on both backends. Each harness builds its own database,
//! named `eddb_run_*`, from the shipped catalog and data migrations, and
//! drops it in [`Harness::cleanup`], which a case calls once it has passed.
//! A case that fails drops the harness without `cleanup`, and the database
//! is dropped then, so a shared server keeps no database of a failed case.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no
//! database component, pointing at a server whose role may create and drop
//! databases. Without it the PostgreSQL cases report a skip and pass,
//! unless `EXTENDDB_TEST_PG_REQUIRED` is set.

use super::*;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgPool};

/// The database a PostgreSQL harness runs on.
pub(crate) struct PgScratch {
    admin: PgPool,
    catalog: PgPool,
    db_name: ScratchName,
}

/// The name of a scratch database. Dropped while it still holds the name,
/// as when a case panics before `cleanup`, it drops the database through a
/// connection of its own, on a thread with its own runtime, because `Drop`
/// cannot wait on the case's runtime.
struct ScratchName(Option<String>);

impl Drop for ScratchName {
    fn drop(&mut self) {
        let (Some(db_name), Some(base)) = (self.0.take(), base_conn()) else {
            return;
        };
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            runtime.block_on(async {
                let remove = async {
                    let mut conn = sqlx::PgConnection::connect(&format!("{base}/postgres")).await?;
                    sqlx::query(&drop_statement(&db_name))
                        .execute(&mut conn)
                        .await?;
                    conn.close().await
                };
                match tokio::time::timeout(Duration::from_secs(30), remove).await {
                    Ok(result) => result.map_err(|e| format!("{db_name}: {e}")),
                    Err(_) => Err(format!("{db_name}: no answer within 30 s")),
                }
            })
        })
        .join();
        match dropped {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("the scratch database was not dropped: {error}"),
            Err(_) => eprintln!("the thread that drops the scratch database panicked"),
        }
    }
}

/// The statement that drops scratch database `db_name` and ends the
/// connections that still hold it.
fn drop_statement(db_name: &str) -> String {
    format!("DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)")
}

/// Which backend a case that runs on both builds its harnesses on.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Backend {
    Sqlite,
    Postgres,
}

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Whether the PostgreSQL leg of `test` can run; reports a skip otherwise.
pub(crate) fn postgres_configured(test: &str) -> bool {
    if base_conn().is_some() {
        return true;
    }
    assert!(
        std::env::var("EXTENDDB_TEST_PG_REQUIRED").map_or(true, |v| v.is_empty()),
        "{test}: EXTENDDB_TEST_PG_REQUIRED is set but EXTENDDB_TEST_PG_CONNECTION_STRING is not"
    );
    eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
    false
}

/// One PostgreSQL case at a time across the test binary: each harness holds
/// an engine pool of ten connections, and the server is shared with every
/// other suite that runs against it.
pub(crate) async fn postgres_permit() -> tokio::sync::OwnedSemaphorePermit {
    static PERMITS: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();
    Arc::clone(PERMITS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1))))
        .acquire_owned()
        .await
        .expect("the permit semaphore is never closed")
}

/// The shipped migration files of one directory of the PostgreSQL crate,
/// in name order.
fn migration_files(subdir: &str) -> Vec<PathBuf> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../storage-postgres")
        .join(subdir);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("list {}: {e}", dir.display()))
        .map(|entry| entry.expect("read a directory entry").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sql"))
        .collect();
    files.sort();
    files
}

impl Harness {
    /// A harness on `backend` with the plain table.
    pub(crate) async fn on(backend: Backend) -> Self {
        match backend {
            Backend::Sqlite => Self::new().await,
            Backend::Postgres => Self::new_postgres().await,
        }
    }

    /// A harness on a fresh PostgreSQL database with the plain table.
    pub(crate) async fn new_postgres() -> Self {
        let base = base_conn().expect("postgres_configured was checked");
        let db_name = format!("eddb_run_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
        for file in migration_files("migrations")
            .into_iter()
            .chain(migration_files("data_migrations"))
        {
            let sql = std::fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
            sqlx::raw_sql(&sql)
                .execute(&catalog)
                .await
                .unwrap_or_else(|e| panic!("apply {}: {e}", file.display()));
        }
        // The shipped migrations create neither row, so each is inserted.
        for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
            sqlx::query(
                "INSERT INTO settings (key, value) VALUES ($1, '0') \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            )
            .bind(key)
            .execute(&catalog)
            .await
            .expect("pin a delay setting to zero");
        }
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
            .bind(ACCOUNT)
            .bind(format!("runner-tests-{db_name}"))
            .execute(&catalog)
            .await
            .expect("seed the account");
        let engine = extenddb_storage_postgres::PostgresEngine::new(
            &extenddb_storage_postgres::PostgresConfig {
                connection_string: url,
                pool_size: 10,
                max_item_size_bytes: 400_000,
            },
            REGION,
        )
        .await
        .expect("open a PostgresEngine on the scratch database");
        let storage: Arc<dyn StorageEngine> = Arc::new(engine);
        let table_id = Self::create_table(&storage, TABLE).await;
        let catalog_store: Arc<dyn extenddb_storage::CatalogStore> = Arc::new(
            extenddb_storage_postgres::PostgresCatalogStore::new(catalog.clone()),
        );
        Self {
            storage,
            store: store_with_buckets(),
            table_id,
            table_arn: format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{TABLE}"),
            catalog: catalog_store,
            pg: Some(PgScratch {
                admin,
                catalog,
                db_name: ScratchName(Some(db_name)),
            }),
            _db_dir: None,
        }
    }

    /// Drop a PostgreSQL harness's database; nothing for `SQLite`.
    pub(crate) async fn cleanup(self) {
        let Some(PgScratch {
            admin,
            catalog,
            mut db_name,
        }) = self.pg
        else {
            return;
        };
        let db_name = db_name.0.take().expect("only cleanup takes the name");
        drop(self.storage);
        catalog.close().await;
        sqlx::query(&drop_statement(&db_name))
            .execute(&admin)
            .await
            .expect("drop the scratch database");
        admin.close().await;
    }
}

/// Run one case on `SQLite` and, when a server is configured, on
/// PostgreSQL, as two tests.
macro_rules! on_both_backends {
    ($case:ident, $sqlite:ident, $postgres:ident) => {
        #[tokio::test]
        async fn $sqlite() {
            $case(Backend::Sqlite).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $postgres() {
            if !postgres_configured(stringify!($postgres)) {
                return;
            }
            let _permit = postgres_permit().await;
            $case(Backend::Postgres).await;
        }
    };
}
pub(crate) use on_both_backends;
