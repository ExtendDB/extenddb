// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SnapshotEngine` on PostgreSQL: the shared consistency check from
//! `extenddb_storage::snapshot_conformance` against real scratch databases,
//! plus the PostgreSQL-specific marker and key-order checks.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL with no database
//! name); skips otherwise. Each test creates its own catalog and data
//! databases and drops them when its `Scratch` is dropped, on success or on
//! a failed assertion.

use extenddb_storage::snapshot_conformance;
use extenddb_storage::{SnapshotEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Scratch {
    engine: PostgresEngine,
    catalog: PgPool,
    base: String,
    names: [String; 2],
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

impl Drop for Scratch {
    /// Drops the two scratch databases whether the test passed or panicked.
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

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

async fn apply(pool: &PgPool, migrations: &[&str]) {
    for sql in migrations {
        sqlx::raw_sql(sql).execute(pool).await.expect("migration");
    }
}

async fn scratch() -> Scratch {
    let serial = SERIAL.lock().await;
    let base = base_conn().expect("caller checks base_conn() first");
    let stem = format!("eddb_snap_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let names = [format!("{stem}_c"), format!("{stem}_d")];
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to postgres");
    for name in &names {
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&admin)
            .await
            .expect("create scratch database");
    }
    let catalog_url = format!("{base}/{}", names[0]);
    let data_url = format!("{base}/{}", names[1]);
    let catalog = PgPoolOptions::new()
        .max_connections(2)
        .connect(&catalog_url)
        .await
        .expect("catalog pool");
    let data = PgPoolOptions::new()
        .max_connections(2)
        .connect(&data_url)
        .await
        .expect("data pool");
    apply(
        &catalog,
        &[
            include_str!("../migrations/001_schema.sql"),
            include_str!("../migrations/002_vector_indexes.sql"),
            include_str!("../migrations/003_backup_definitions.sql"),
        ],
    )
    .await;
    apply(
        &data,
        &[
            include_str!("../data_migrations/001_data_schema.sql"),
            include_str!("../data_migrations/002_gsi_pending.sql"),
            include_str!("../data_migrations/003_idempotency_account_scope.sql"),
            include_str!("../data_migrations/004_vector_index_state.sql"),
        ],
    )
    .await;
    for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
        sqlx::query("UPDATE settings SET value = '0' WHERE key = $1")
            .bind(key)
            .execute(&catalog)
            .await
            .expect("setting");
    }
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(&data_url)
    .execute(&catalog)
    .await
    .expect("data connection setting");
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
    data.close().await;
    admin.close().await;
    Scratch {
        engine,
        catalog,
        base,
        names,
        _serial: serial,
    }
}

#[tokio::test]
async fn snapshot_is_consistent_under_concurrent_writes() {
    if base_conn().is_none() {
        eprintln!("SKIP snapshot_is_consistent_under_concurrent_writes: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    snapshot_conformance::run(
        &s.engine,
        ACCOUNT,
        "snap_src",
        50,
        extenddb_storage_postgres::set_snapshot_fetch_batch_for_tests,
        || async {
            s.engine
                .process_control_plane_transitions()
                .await
                .expect("pump");
        },
    )
    .await;
    snapshot_conformance::run_refusals(&s.engine, ACCOUNT).await;
}

#[tokio::test]
async fn snapshot_marker_and_key_order() {
    if base_conn().is_none() {
        eprintln!("SKIP snapshot_marker_and_key_order: no PostgreSQL");
        return;
    }
    let s = scratch().await;
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
    assert_eq!(snapshot.info.kind, "postgres_repeatable_read");
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
