// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The runs a fresh install gets after `init`: `--reconverge` on a catalog
//! that a later migration changed, `--forget`, a ledger that cannot be read,
//! the data database the server serves, and a migrator that finds an
//! earlier migrator's statement still running. See `migrations_support` for
//! the database these tests need.

mod migrations_support;

use std::time::{Duration, Instant};

use extenddb_storage::migrations::{CatalogStatus, MigrationError, MigrationId, Runner, StoreKind};
use extenddb_storage_postgres::migrations::{
    MigrationLock, PostgresMigrations, PostgresTarget, catalog_status, migrate,
};
use migrations_support::{LATEST, Scratch, base_conn, ids_in, ledger, pool, skip};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

const RELEASE: &str = "1.0.0-test";

/// A scratch pair with a fresh install, as `init` leaves it.
async fn installed(base: &str, prefix: &str) -> Scratch {
    let s = Scratch::new(base, prefix).await;
    s.migrate_through(MigrationId::LATEST).await;
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    s
}

/// A migration after migration 1 may change a table migration 1 created; a
/// `--reconverge` runs migration 1 again over it, and its check must not
/// refuse what the later migration added.
#[tokio::test]
async fn a_reconverge_keeps_what_a_later_migration_added() {
    let Some(base) = base_conn() else {
        return skip("reconverge");
    };
    let s = installed(&base, "pgm_reconv").await;
    let catalog = s.catalog().await;
    sqlx::query("ALTER TABLE settings ADD COLUMN note TEXT")
        .execute(&catalog)
        .await
        .unwrap();
    sqlx::query("UPDATE settings SET note = 'kept' WHERE key = 'catalog_version'")
        .execute(&catalog)
        .await
        .unwrap();
    let rows_before = ledger(&catalog).await;
    let m = PostgresMigrations::open_unlocked(&s.target())
        .await
        .unwrap();
    let report = Runner::new(&m, RELEASE).reconverge().await;
    m.close().await;
    let report = report.expect("reconverge on a catalog a later migration changed");
    assert_eq!(report.to, Some(LATEST));
    let note: Option<String> =
        sqlx::query_scalar("SELECT note FROM settings WHERE key = 'catalog_version'")
            .fetch_one(&catalog)
            .await
            .unwrap();
    assert_eq!(note.as_deref(), Some("kept"));
    assert_eq!(ledger(&catalog).await, rows_before);
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    catalog.close().await;
}

/// `forget` deletes the rows of the id it is given in every store that
/// records it, and the next run applies that id again.
#[tokio::test]
async fn forget_removes_one_migration_and_the_next_run_applies_it() {
    let Some(base) = base_conn() else {
        return skip("forget");
    };
    let s = installed(&base, "pgm_forget").await;
    let lock = MigrationLock::acquire(&s.options(), Duration::from_secs(5))
        .await
        .unwrap();
    let m = PostgresMigrations::open(&s.target(), &lock).await.unwrap();
    let forgot = Runner::new(&m, RELEASE).forget(LATEST).await;
    m.close().await;
    lock.release().await;
    forgot.expect("forget the latest migration");
    let latest = i32::try_from(LATEST).unwrap();
    let (catalog, data) = (s.catalog().await, s.data().await);
    for (pool, kind) in [(&catalog, StoreKind::Catalog), (&data, StoreKind::Data)] {
        let expected: Vec<i32> = ids_in(kind)
            .into_iter()
            .filter(|id| *id != latest)
            .collect();
        let ids: Vec<i32> = ledger(pool).await.iter().map(|r| r.0).collect();
        assert_eq!(ids, expected, "{kind:?}");
    }
    let CatalogStatus::Behind { version, pending } = s.status().await else {
        panic!("a forgotten migration is pending");
    };
    assert_eq!(version, Some(LATEST - 1));
    let mut pending: Vec<(u32, StoreKind)> = pending.iter().map(|p| (p.id, p.store)).collect();
    pending.sort();
    let expected: Vec<(u32, StoreKind)> = [StoreKind::Catalog, StoreKind::Data]
        .into_iter()
        .filter(|kind| ids_in(*kind).contains(&latest))
        .map(|kind| (LATEST, kind))
        .collect();
    assert_eq!(pending, expected);
    let report = s.migrate_through(MigrationId::LATEST).await;
    assert_eq!(report.applied.len(), expected.len());
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    for p in [catalog, data] {
        p.close().await;
    }
}

/// A ledger the status cannot read is an error naming it, never a catalog
/// read as behind.
#[tokio::test]
async fn a_ledger_that_cannot_be_read_is_an_error() {
    let Some(base) = base_conn() else {
        return skip("unreadable ledger");
    };
    let s = installed(&base, "pgm_ledger").await;
    let data = s.data().await;
    sqlx::query("ALTER TABLE extenddb_migrations RENAME COLUMN checksum TO checksum_before")
        .execute(&data)
        .await
        .unwrap();
    let err = catalog_status(&s.target())
        .await
        .expect_err("an unreadable ledger is an error");
    assert!(err.to_string().contains("extenddb_migrations"), "{err}");
    data.close().await;
}

/// The data database the runner and the status use is the one the
/// connection string row names, which the server serves; a target naming
/// another one is refused unless the command line gave it.
#[tokio::test]
async fn the_status_refuses_when_the_two_data_rows_name_different_databases() {
    let Some(base) = base_conn() else {
        return skip("data rows");
    };
    let s = installed(&base, "pgm_rows").await;
    let other = PostgresTarget::new(s.options(), format!("{}_other", s.data_name));
    let err = catalog_status(&other)
        .await
        .expect_err("the rows disagree with the target");
    let message = err.to_string();
    assert!(
        message.contains(&s.data_name) && message.contains("data_database_connection_string"),
        "{message}"
    );
    assert_eq!(err.exit_code(), 1, "{message}");
    // The row's database is the one the status reads, whatever credentials
    // the catalog was reached with.
    assert_eq!(
        catalog_status(&PostgresTarget::new(s.options(), &s.data_name))
            .await
            .unwrap(),
        CatalogStatus::Current { version: LATEST }
    );
}

/// A session named as another runner's, running a statement in the
/// database at `url`: what a migrator that died while a statement ran
/// leaves behind.
async fn earlier_runner_statement(s: &Scratch, url: &str, seconds: u32) -> (PgPool, i32) {
    let options: PgConnectOptions = url.parse().unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.application_name("extenddb migrate run 0123456789ab"))
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let running = pool.clone();
    tokio::spawn(async move {
        let _ = sqlx::query(&format!("SELECT pg_sleep({seconds})"))
            .execute(&running)
            .await;
    });
    let admin = pool_for_admin(s).await;
    for _ in 0..200 {
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active')",
        )
        .bind(pid)
        .fetch_one(&admin)
        .await
        .unwrap();
        if active {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    admin.close().await;
    (pool, pid)
}

async fn pool_for_admin(s: &Scratch) -> PgPool {
    pool(&format!("{}/postgres", s.base), 1).await
}

#[tokio::test]
async fn a_migrator_waits_for_an_earlier_migrators_statement_to_end() {
    let Some(base) = base_conn() else {
        return skip("earlier migrator");
    };
    let s = Scratch::new(&base, "pgm_dead").await;
    s.load("0.1.14").await;
    let (sleeper, _) = earlier_runner_statement(&s, &s.data_url(), 3).await;
    let started = Instant::now();
    let report = migrate(&s.target(), RELEASE, Duration::from_secs(30))
        .await
        .expect("migrate once the earlier statement ended");
    assert!(
        started.elapsed() >= Duration::from_millis(2500),
        "migrate ran beside the earlier statement ({:?})",
        started.elapsed()
    );
    assert_eq!(report.to, Some(LATEST));
    sleeper.close().await;

    // The same for a statement in the catalog database, which a migrator
    // that died in a catalog body leaves behind.
    let (sleeper, _) = earlier_runner_statement(&s, &s.catalog_url(), 3).await;
    let started = Instant::now();
    migrate(&s.target(), RELEASE, Duration::from_secs(30))
        .await
        .expect("migrate once the earlier statement ended");
    assert!(
        started.elapsed() >= Duration::from_millis(2500),
        "migrate ran beside the earlier statement in the catalog database ({:?})",
        started.elapsed()
    );
    sleeper.close().await;

    // The wait is bounded by the lock timeout, and names the session and
    // the statement that ends it, with no advice to wait for it.
    let (sleeper, pid) = earlier_runner_statement(&s, &s.data_url(), 60).await;
    let err = migrate(&s.target(), RELEASE, Duration::from_secs(1))
        .await
        .expect_err("the earlier statement outlasts the lock timeout");
    assert!(matches!(err, MigrationError::Blocked(_)), "{err}");
    assert!(err.to_string().contains("pg_cancel_backend"), "{err}");
    assert_eq!(err.exit_code(), 6);
    assert!(err.to_string().contains(&format!("pid {pid}")), "{err}");
    let admin = pool_for_admin(&s).await;
    sqlx::query("SELECT pg_cancel_backend($1)")
        .bind(pid)
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    sleeper.close().await;
}
