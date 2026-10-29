// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! A migrator that dies inside each kind of body, a concurrent index build
//! cancelled part way, two migrators, the migration lock's keepalive, and the
//! serving registration against `migrate`. A migrator is killed by ending
//! its sessions with `pg_terminate_backend`, which is what the database sees
//! when the process dies. See `migrations_support` for the database these
//! tests need.

mod migrations_support;

use std::time::Duration;

use extenddb_storage::migrations::{CatalogStatus, MigrationError, MigrationId, RunReport, Runner};
use extenddb_storage_postgres::migrations::{
    MigrationLock, PostgresMigrations, PostgresTarget, ServingRegistration, catalog_status, migrate,
};
use migrations_support::{LATEST, Scratch, base_conn, count, ledger, pool, skip, step_count};
use sqlx::PgPool;

const RELEASE: &str = "1.0.0-test";

/// Run every pending migration in a task, with no lock.
fn spawn_run(target: PostgresTarget) -> tokio::task::JoinHandle<Result<RunReport, MigrationError>> {
    tokio::spawn(async move {
        let m = PostgresMigrations::open_unlocked(&target).await?;
        let result = Runner::new(&m, RELEASE).migrate().await;
        m.close().await;
        result
    })
}

/// The pid of the runner's session in `database` once it waits on a lock
/// while running a statement that contains `statement`.
async fn waiting_runner(admin: &PgPool, database: &str, statement: &str) -> i32 {
    for _ in 0..200 {
        let pid: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE datname = $1 \
             AND starts_with(application_name, 'extenddb migrate run ') AND wait_event_type = 'Lock' \
             AND query LIKE '%' || $2 || '%'",
        )
        .bind(database)
        .bind(statement)
        .fetch_optional(admin)
        .await
        .unwrap();
        if let Some(pid) = pid {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the runner never waited on {statement:?} in {database}");
}

async fn index_valid(data: &PgPool, index: &str) -> Option<bool> {
    sqlx::query_scalar("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)")
        .bind(format!("public.\"{index}\""))
        .fetch_optional(data)
        .await
        .unwrap()
}

/// The GSI index table of seed_tbl and its base key index.
async fn gsi_index(catalog: &PgPool) -> (String, String) {
    let id: String = sqlx::query_scalar("SELECT index_id FROM indexes WHERE index_name = 'gsi1'")
        .fetch_one(catalog)
        .await
        .unwrap();
    (format!("_ddb_{id}"), format!("_ddb_{id}_base_key_idx"))
}

#[tokio::test]
async fn a_cancelled_concurrent_build_is_not_recorded_and_the_rerun_rebuilds_it() {
    let Some(base) = base_conn() else {
        return skip("cancelled build");
    };
    let s = Scratch::new(&base, "pgm_cic").await;
    // Index tables created by 0.1.5 have no base key index.
    s.load("0.1.5").await;
    s.migrate_through(MigrationId::SortKeyDefinitions).await;
    let (catalog, data, admin) = (
        s.catalog().await,
        s.data().await,
        pool(&format!("{base}/postgres"), 1).await,
    );
    let (table, index) = gsi_index(&catalog).await;
    assert_eq!(index_valid(&data, &index).await, None);
    // An open transaction that wrote the index table: the concurrent build
    // waits for it to end.
    let mut blocker = data.begin().await.unwrap();
    sqlx::query(&format!("LOCK TABLE \"{table}\" IN ROW EXCLUSIVE MODE"))
        .execute(&mut *blocker)
        .await
        .unwrap();
    let run = spawn_run(s.target());
    let pid = waiting_runner(&admin, &s.data_name, "CREATE INDEX CONCURRENTLY").await;
    sqlx::query("SELECT pg_cancel_backend($1)")
        .bind(pid)
        .execute(&admin)
        .await
        .unwrap();
    let err = run
        .await
        .unwrap()
        .expect_err("the cancelled build fails the run");
    assert!(err.to_string().contains("canceling statement"), "{err}");
    blocker.rollback().await.unwrap();
    // The build left the index invalid, and nothing recorded migration 4 in
    // the data database, so the catalog is behind.
    assert_eq!(index_valid(&data, &index).await, Some(false));
    assert_eq!(
        ledger(&data).await.iter().map(|r| r.0).collect::<Vec<_>>(),
        [1]
    );
    assert!(matches!(
        s.status().await,
        CatalogStatus::Behind {
            version: Some(3),
            ..
        }
    ));
    let report = s.migrate_through(MigrationId::LATEST).await;
    assert_eq!(report.to, Some(LATEST));
    assert_eq!(index_valid(&data, &index).await, Some(true));
    let notes: String = sqlx::query_scalar("SELECT notes FROM extenddb_migrations WHERE id = 4")
        .fetch_one(&data)
        .await
        .unwrap();
    assert!(
        notes.contains("gsi1: rebuilt") && notes.contains("lsi1: built"),
        "{notes}"
    );
    for p in [catalog, data, admin] {
        p.close().await;
    }
}

/// The migrator's session ends while it waits inside a body; the database
/// rolls the body's transaction back, and a rerun completes.
async fn killed_inside(
    state: &str,
    through: Option<MigrationId>,
    database_is_data: bool,
    lock_sql: &str,
    waits_on: &str,
) {
    let Some(base) = base_conn() else {
        return skip("killed migrator");
    };
    let s = Scratch::new(&base, "pgm_kill").await;
    s.load(state).await;
    if let Some(id) = through {
        s.migrate_through(id).await;
    }
    let (catalog, data, admin) = (
        s.catalog().await,
        s.data().await,
        pool(&format!("{base}/postgres"), 1).await,
    );
    let (table, _) = gsi_index(&catalog).await;
    let target_pool = if database_is_data { &data } else { &catalog };
    let database = if database_is_data {
        s.data_name.clone()
    } else {
        s.catalog_name.clone()
    };
    let rows_before = ledger(target_pool).await;
    let mut blocker = target_pool.begin().await.unwrap();
    sqlx::query(&lock_sql.replace("{table}", &table))
        .execute(&mut *blocker)
        .await
        .unwrap();
    let run = spawn_run(s.target());
    let pid = waiting_runner(&admin, &database, waits_on).await;
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .execute(&admin)
        .await
        .unwrap();
    run.await.unwrap().expect_err("the killed migrator fails");
    blocker.rollback().await.unwrap();
    // The killed body's store has no new row. (Migration 4's catalog row is
    // written first, by design, before its data body runs.)
    assert_eq!(
        ledger(target_pool).await,
        rows_before,
        "the killed body committed rows"
    );
    let report = s.migrate_through(MigrationId::LATEST).await;
    assert_eq!(report.to, Some(LATEST));
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
    for p in [catalog, data, admin] {
        p.close().await;
    }
}

#[tokio::test]
async fn a_migrator_killed_inside_a_file_body_leaves_nothing_and_the_rerun_completes() {
    // pg_stat_activity shows the first 1024 bytes of the file's text.
    killed_inside(
        "0.1.14",
        None,
        false,
        "LOCK TABLE tables IN ACCESS EXCLUSIVE MODE",
        "Migration 1, baseline",
    )
    .await;
}

#[tokio::test]
async fn a_migrator_killed_inside_a_transactional_code_body_leaves_nothing() {
    killed_inside(
        "0.1.14",
        Some(MigrationId::ResourceTagConditionKeys),
        false,
        "LOCK TABLE tables IN ACCESS EXCLUSIVE MODE",
        "FROM tables",
    )
    .await;
}

#[tokio::test]
async fn a_migrator_killed_inside_an_index_build_is_rebuilt_by_the_rerun() {
    killed_inside(
        "0.1.5",
        Some(MigrationId::SortKeyDefinitions),
        true,
        "LOCK TABLE \"{table}\" IN ROW EXCLUSIVE MODE",
        "CREATE INDEX CONCURRENTLY",
    )
    .await;
}

#[tokio::test]
async fn two_migrators_apply_once() {
    let Some(base) = base_conn() else {
        return skip("two migrators");
    };
    let s = Scratch::new(&base, "pgm_two").await;
    s.load("0.1.14").await;
    let target = s.target();
    let (a, b) = tokio::join!(
        migrate(&target, RELEASE, Duration::from_secs(60)),
        migrate(&target, RELEASE, Duration::from_secs(60))
    );
    let (a, b) = (a.expect("first"), b.expect("second"));
    // One applies every step; the other finds them recorded.
    let applied = [a.applied.len(), b.applied.len()];
    assert!(
        applied.contains(&0) && applied.contains(&step_count()),
        "{applied:?}"
    );
    let catalog = s.catalog().await;
    assert_eq!(ledger(&catalog).await.len(), MigrationId::ALL.len());
    catalog.close().await;
}

#[tokio::test]
async fn the_lock_keepalive_outlasts_the_idle_in_transaction_timeout() {
    let Some(base) = base_conn() else {
        return skip("keepalive");
    };
    let s = Scratch::new(&base, "pgm_keep").await;
    let admin = pool(&format!("{base}/postgres"), 1).await;
    sqlx::query(&format!(
        "ALTER DATABASE \"{}\" SET idle_in_transaction_session_timeout = '1s'",
        s.catalog_name
    ))
    .execute(&admin)
    .await
    .unwrap();
    let kept = MigrationLock::acquire_with(
        &s.options(),
        Duration::from_secs(5),
        Duration::from_millis(300),
    )
    .await
    .unwrap();
    let registered =
        ServingRegistration::register_with(&s.options(), Duration::from_millis(300)).await;
    // The registration waits for the lock's holder: it refuses while the
    // migration lock is held.
    assert!(matches!(registered, Err(MigrationError::Locked(_))));
    tokio::time::sleep(Duration::from_millis(2500)).await;
    kept.check_alive()
        .await
        .expect("the keepalive kept the lock's transaction open");
    kept.release().await;
    // Without a renewal inside the timeout the database ends the session.
    let idle = MigrationLock::acquire_with(
        &s.options(),
        Duration::from_secs(5),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(idle.check_alive().await.is_err());
    idle.release().await;
    let serving = ServingRegistration::register_with(&s.options(), Duration::from_millis(300))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    serving
        .check_alive()
        .await
        .expect("the registration's keepalive");
    assert!(serving.lost().borrow().is_none());
    serving.release().await;
    admin.close().await;
}

#[tokio::test]
async fn a_server_refuses_while_a_migrator_holds_only_the_migration_key() {
    let Some(base) = base_conn() else {
        return skip("legacy migrator");
    };
    let s = Scratch::new(&base, "pgm_legacy").await;
    // A migrator of a release before 1.0, or one between taking its own key
    // and the servers' key, holds the migration key alone.
    let catalog = s.catalog().await;
    let mut legacy = catalog.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(4539458, 1)")
        .execute(&mut *legacy)
        .await
        .unwrap();
    let refused = ServingRegistration::register(&s.options()).await;
    assert!(
        matches!(refused, Err(MigrationError::Locked(_))),
        "registered beside a migrator"
    );
    legacy.rollback().await.unwrap();
    ServingRegistration::register(&s.options())
        .await
        .expect("registers once it is gone")
        .release()
        .await;
    catalog.close().await;
}

/// A table lock held a little longer than one lock timeout is waited out by
/// running the body again.
#[tokio::test]
async fn a_lock_held_past_one_timeout_is_waited_out() {
    let Some(base) = base_conn() else {
        return skip("lock retries");
    };
    let s = Scratch::new(&base, "pgm_retry").await;
    s.load("0.1.14").await;
    let catalog = s.catalog().await;
    let mut blocker = catalog.begin().await.unwrap();
    sqlx::query("LOCK TABLE tables IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let run = spawn_run(s.target());
    tokio::time::sleep(Duration::from_secs(7)).await;
    blocker.rollback().await.unwrap();
    let report = run
        .await
        .unwrap()
        .expect("the retry ran after the lock was released");
    assert_eq!(report.to, Some(LATEST));
    catalog.close().await;
}

#[tokio::test]
async fn migrate_waits_for_running_servers_and_names_them() {
    let Some(base) = base_conn() else {
        return skip("serve and migrate");
    };
    let s = Scratch::new(&base, "pgm_serve").await;
    s.load("0.1.14").await;
    let serving = ServingRegistration::register(&s.options()).await.unwrap();
    let admin = pool(&format!("{base}/postgres"), 1).await;
    let app: String = sqlx::query_scalar(
        "SELECT a.application_name FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND l.objid = 2 AND l.mode = 'ShareLock' AND l.granted",
    )
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(app, "extenddb serve");
    let err = migrate(&s.target(), RELEASE, Duration::from_secs(1))
        .await
        .expect_err("refused");
    assert_eq!(err.exit_code(), 6, "{err}");
    assert!(err.to_string().contains("\"extenddb serve\""), "{err}");
    // The status and the preview read beside a running server.
    assert!(matches!(
        catalog_status(&s.target()).await.unwrap(),
        CatalogStatus::Behind { .. }
    ));
    // A migrator waiting while the server stops goes on.
    let waiting = {
        let target = s.target();
        tokio::spawn(async move { migrate(&target, RELEASE, Duration::from_secs(30)).await })
    };
    // Once the waiting migrator holds the migration key, a second server
    // cannot register.
    let mut held = false;
    for _ in 0..400 {
        held = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' \
             AND classid = 4539458 AND objid = 1 AND objsubid = 2 AND mode = 'ExclusiveLock' \
             AND granted AND database = (SELECT oid FROM pg_database WHERE datname = $1))",
        )
        .bind(&s.catalog_name)
        .fetch_one(&admin)
        .await
        .unwrap();
        if held {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(held, "the waiting migrator never took the migration key");
    assert!(ServingRegistration::register(&s.options()).await.is_err());
    serving.release().await;
    let report = waiting
        .await
        .unwrap()
        .expect("migrate after the server stopped");
    assert_eq!(report.to, Some(LATEST));
    let catalog = s.catalog().await;
    assert_eq!(
        count(&catalog, "SELECT count(*) FROM extenddb_migrations").await,
        i64::try_from(MigrationId::ALL.len()).unwrap()
    );
    catalog.close().await;
    admin.close().await;
}
