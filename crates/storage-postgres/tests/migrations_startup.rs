// SPDX-License-Identifier: Apache-2.0
//! The startup policy `serve` runs before it opens the catalog
//! (`[server] catalog_upgrade`, or `serve --migrate`), on released
//! catalogs: under `apply` a catalog behind the binary is migrated through
//! the lock `migrate --yes` takes, a current one is served with nothing
//! written, one that needs a person exits 5 with nothing written, a held
//! migration lock makes the server wait and then exit 6, a zero lock
//! timeout tries each key once, the waits of a run share one timeout, and
//! two `prepare_to_serve` runs that wait for the lock together migrate
//! once. See `migrations_support` for the database these tests need.

mod migrations_support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use extenddb_storage::migrations::{
    CatalogStatus, MigrationError, Prepared, RunKind, Startup, Target, WhenCurrent, exit,
    prepare_to_serve, run_with_lock,
};
use extenddb_storage_postgres::migrations::{Definitions, PostgresCatalogMigrations};
use migrations_support::{
    LATEST, Scratch, base_conn, describe_pool, ledger, settings, skip, step_count,
};

/// The advisory keys of the migration lock and the serving registration,
/// `NAMESPACE`, `MIGRATION_KEY` and `SERVING_KEY` in
/// `src/migrations/locks.rs`.
const NAMESPACE: i32 = 0x0045_4442;
const MIGRATION_KEY: i32 = 1;
const SERVING_KEY: i32 = 2;

fn startup(apply: bool, lock_timeout: Duration) -> Startup<'static> {
    Startup {
        apply,
        creates_uninitialized: false,
        release: "1.0.0-test",
        lock_timeout,
        heartbeat: Startup::HEARTBEAT,
    }
}

/// The policy as `serve` runs it, on the catalog database's URL, which is
/// all the config file gives.
async fn prepare(s: &Scratch, startup: &Startup<'_>) -> Result<Prepared, MigrationError> {
    let target = Target::at(s.catalog_url());
    prepare_to_serve(&PostgresCatalogMigrations, &target, startup, &|_| {}).await
}

/// What a run could change: both databases' definitions, the catalog's
/// settings, and each ledger row with the time it was applied.
async fn state(s: &Scratch) -> (Definitions, Definitions, Vec<(String, String)>, Vec<String>) {
    let (catalog, data) = (s.catalog().await, s.data().await);
    let mut rows = Vec::new();
    for p in [&catalog, &data] {
        let applied: Vec<(i32, String)> =
            sqlx::query_as("SELECT id, applied_at::text FROM extenddb_migrations ORDER BY id")
                .fetch_all(p)
                .await
                .unwrap_or_default();
        rows.extend(applied.into_iter().map(|(id, at)| format!("{id} {at}")));
    }
    let found = (
        describe_pool(&catalog).await,
        describe_pool(&data).await,
        settings(&catalog).await.into_iter().collect(),
        rows,
    );
    catalog.close().await;
    data.close().await;
    found
}

#[tokio::test]
async fn apply_migrates_a_released_catalog_and_refuse_writes_nothing() {
    let Some(base) = base_conn() else {
        return skip("startup apply");
    };
    let s = Scratch::new(&base, "pgs_apply").await;
    s.load("0.1.14").await;
    let before = state(&s).await;
    let err = prepare(&s, &startup(false, Duration::from_secs(10)))
        .await
        .expect_err("refused");
    assert_eq!(err.exit_code(), exit::PENDING, "{err:?}");
    assert!(state(&s).await == before, "refuse wrote to the catalog");

    let prepared = prepare(&s, &startup(true, Duration::from_secs(10)))
        .await
        .expect("applied");
    let report = prepared.run.expect("a run");
    assert_eq!(report.applied.len(), step_count());
    assert_eq!(report.to, Some(LATEST));
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
}

#[tokio::test]
async fn apply_on_a_current_catalog_writes_nothing() {
    let Some(base) = base_conn() else {
        return skip("startup current");
    };
    let s = Scratch::new(&base, "pgs_current").await;
    s.load("0.1.14").await;
    s.migrate_through(extenddb_storage::migrations::MigrationId::LATEST)
        .await;
    let before = state(&s).await;
    let prepared = prepare(&s, &startup(true, Duration::from_secs(10)))
        .await
        .expect("served");
    assert_eq!(prepared, Prepared::default());
    assert!(
        state(&s).await == before,
        "a current catalog was written to"
    );
}

#[tokio::test]
async fn apply_on_a_catalog_that_needs_attention_exits_5_and_writes_nothing() {
    let Some(base) = base_conn() else {
        return skip("startup attention");
    };
    let s = Scratch::new(&base, "pgs_attention").await;
    s.load("0.1.14").await;
    let catalog = s.catalog().await;
    let both = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod","aws:ResourceTag/env":"dev"}}}]}"#;
    sqlx::query(
        "UPDATE iam_policies SET policy_document = $1::jsonb WHERE principal_name = 'tester'",
    )
    .bind(both)
    .execute(&catalog)
    .await
    .unwrap();
    catalog.close().await;
    let before = state(&s).await;
    let err = prepare(&s, &startup(true, Duration::from_secs(10)))
        .await
        .expect_err("refused");
    assert!(
        matches!(
            err,
            MigrationError::Status(CatalogStatus::NeedsAttention { .. })
        ),
        "{err:?}"
    );
    assert_eq!(err.exit_code(), exit::ATTENTION);
    assert!(
        state(&s).await == before,
        "a refused run wrote to the catalog"
    );
}

/// Another session holds the migration lock's key, as a running migration
/// does: the server waits for it up to the timeout, then exits 6 with the
/// catalog unchanged.
#[tokio::test]
async fn apply_waits_for_a_held_migration_lock_and_exits_6() {
    let Some(base) = base_conn() else {
        return skip("startup locked");
    };
    let s = Scratch::new(&base, "pgs_locked").await;
    s.load("0.1.14").await;
    let before = state(&s).await;
    let holder = s.catalog().await;
    let mut tx = holder.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(NAMESPACE)
        .bind(MIGRATION_KEY)
        .execute(&mut *tx)
        .await
        .unwrap();
    let started = Instant::now();
    let err = prepare(&s, &startup(true, Duration::from_secs(2)))
        .await
        .expect_err("locked");
    assert!(
        started.elapsed() >= Duration::from_millis(1800),
        "it waited {:?}",
        started.elapsed()
    );
    assert!(matches!(err, MigrationError::Locked(_)), "{err:?}");
    assert_eq!(err.exit_code(), exit::LOCKED);
    tx.rollback().await.unwrap();
    holder.close().await;
    assert!(
        state(&s).await == before,
        "a server that waited wrote to the catalog"
    );
}

/// Two `prepare_to_serve` runs under `apply` on a released catalog, both
/// waiting for the migration lock another session holds, as the checks of
/// two servers started together do after their first status read: once it
/// is free, one migrates the catalog, and the other takes the lock after it
/// and applies nothing. The servers are not started here; one of two
/// servers started together can exit 6 when it registers while the other
/// holds the lock.
#[tokio::test]
async fn two_prepare_to_serve_runs_under_apply_migrate_once() {
    let Some(base) = base_conn() else {
        return skip("startup two");
    };
    let s = Scratch::new(&base, "pgs_two").await;
    s.load("0.1.14").await;
    let holder = s.catalog().await;
    let mut tx = holder.begin().await.unwrap();
    hold(&mut tx, "pg_advisory_xact_lock", MIGRATION_KEY).await;
    let waiting = AtomicUsize::new(0);
    let progress = |line: &str| {
        if line.starts_with("taking the migration lock") {
            waiting.fetch_add(1, Ordering::SeqCst);
        }
    };
    let target = Target::at(s.catalog_url());
    let apply = startup(true, Duration::from_secs(60));
    let free = async {
        while waiting.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tx.rollback().await.unwrap();
    };
    let (a, b, ()) = tokio::join!(
        prepare_to_serve(&PostgresCatalogMigrations, &target, &apply, &progress),
        prepare_to_serve(&PostgresCatalogMigrations, &target, &apply, &progress),
        free,
    );
    holder.close().await;
    let mut applied: Vec<usize> = [a.expect("first"), b.expect("second")]
        .iter()
        .map(|p| p.run.as_ref().expect("each took the lock").applied.len())
        .collect();
    applied.sort_unstable();
    assert_eq!(
        applied,
        [0, step_count()],
        "one migrates, and the other takes the lock after it and applies nothing"
    );
    let catalog = s.catalog().await;
    assert_eq!(
        ledger(&catalog).await.len(),
        extenddb_storage::migrations::MigrationId::ALL.len()
    );
    catalog.close().await;
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
}

/// Hold an advisory key of the catalog in `tx`, as another migrator
/// (`pg_advisory_xact_lock` on the migration key) or a running server
/// (`pg_advisory_xact_lock_shared` on the serving key) does.
async fn hold(tx: &mut sqlx::Transaction<'static, sqlx::Postgres>, function: &str, key: i32) {
    sqlx::query(&format!("SELECT {function}($1, $2)"))
        .bind(NAMESPACE)
        .bind(key)
        .execute(&mut **tx)
        .await
        .unwrap();
}

/// A zero lock timeout tries each key once. While another migrator holds
/// the migration key, or a server the serving key, the server's run and the
/// run `migrate --yes` makes exit 6 at once, with the catalog unchanged.
/// PostgreSQL reads a `lock_timeout` of 0 as no limit, so a wait would last
/// as long as the holder keeps its key.
#[tokio::test]
async fn a_zero_lock_timeout_tries_each_key_once() {
    let Some(base) = base_conn() else {
        return skip("startup zero timeout");
    };
    let s = Scratch::new(&base, "pgs_zero").await;
    s.load("0.1.14").await;
    let before = state(&s).await;
    let target = Target::at(s.catalog_url());
    for (function, key) in [
        ("pg_advisory_xact_lock", MIGRATION_KEY),
        ("pg_advisory_xact_lock_shared", SERVING_KEY),
    ] {
        let holder = s.catalog().await;
        let mut tx = holder.begin().await.unwrap();
        hold(&mut tx, function, key).await;
        let started = Instant::now();
        let served = tokio::time::timeout(
            Duration::from_secs(20),
            prepare(&s, &startup(true, Duration::ZERO)),
        )
        .await
        .expect("the server's run waited for the key");
        let err = served.expect_err("locked");
        assert_eq!(err.exit_code(), exit::LOCKED, "{function}: {err:?}");
        let migrated = tokio::time::timeout(
            Duration::from_secs(20),
            run_with_lock(
                &PostgresCatalogMigrations,
                &target,
                "1.0.0-test",
                RunKind::Migrate,
                Duration::ZERO,
                WhenCurrent::Run,
            ),
        )
        .await
        .expect("migrate --yes waited for the key");
        let err = migrated.expect_err("locked");
        assert_eq!(err.exit_code(), exit::LOCKED, "{function}: {err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{function}: {:?}",
            started.elapsed()
        );
        tx.rollback().await.unwrap();
        holder.close().await;
    }
    assert!(state(&s).await == before, "a run at a zero timeout wrote");
}

/// The waits of one run share its lock timeout: a server's run that waits
/// for another migrator's key, and then for a running server's key, exits 6
/// once the timeout has passed in all, not after a timeout for each wait.
#[tokio::test]
async fn the_waits_of_a_run_share_one_lock_timeout() {
    let Some(base) = base_conn() else {
        return skip("startup one bound");
    };
    let s = Scratch::new(&base, "pgs_bound").await;
    s.load("0.1.14").await;
    // A server serves the catalog for the whole test, and another migrator
    // holds the migration key for 6 s of the run's 8 s.
    let server = s.catalog().await;
    let mut serving = server.begin().await.unwrap();
    hold(&mut serving, "pg_advisory_xact_lock_shared", SERVING_KEY).await;
    let migrator = s.catalog().await;
    let mut migrating = migrator.begin().await.unwrap();
    hold(&mut migrating, "pg_advisory_xact_lock", MIGRATION_KEY).await;
    let apply = startup(true, Duration::from_secs(8));
    let started = Instant::now();
    let other_finishes = async {
        tokio::time::sleep(Duration::from_secs(6)).await;
        migrating.rollback().await.unwrap();
    };
    let (locked, ()) = tokio::join!(prepare(&s, &apply), other_finishes);
    let waited = started.elapsed();
    let err = locked.expect_err("locked");
    assert_eq!(err.exit_code(), exit::LOCKED, "{err:?}");
    assert!(
        err.to_string()
            .starts_with("servers are serving this catalog"),
        "{err}"
    );
    // 8 s in all, plus the status reads; 8 s for each key would end after
    // 14 s.
    assert!(
        waited >= Duration::from_millis(7500) && waited < Duration::from_secs(13),
        "it waited {waited:?}"
    );
    serving.rollback().await.unwrap();
    server.close().await;
    migrator.close().await;
}
