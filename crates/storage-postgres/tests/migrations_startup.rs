// SPDX-License-Identifier: Apache-2.0
//! The startup policy `serve` runs before it opens the catalog
//! (`[server] catalog_upgrade`, or `serve --migrate`), on released
//! catalogs: under `apply` a catalog behind the binary is migrated through
//! the lock `migrate --yes` takes, a current one is served with nothing
//! written, one that needs a person exits 5 with nothing written, a held
//! migration lock makes the server wait and then exit 6, and two servers
//! started together migrate once. See `migrations_support` for the
//! database these tests need.

mod migrations_support;

use std::time::{Duration, Instant};

use extenddb_storage::migrations::{
    CatalogStatus, MigrationError, Prepared, Startup, Target, exit, prepare_to_serve,
};
use extenddb_storage_postgres::migrations::{Definitions, PostgresCatalogMigrations};
use migrations_support::{
    LATEST, Scratch, base_conn, describe_pool, ledger, settings, skip, step_count,
};

/// The advisory key of the migration lock, `NAMESPACE` and `MIGRATION_KEY`
/// in `src/migrations/locks.rs`.
const NAMESPACE: i32 = 0x0045_4442;
const MIGRATION_KEY: i32 = 1;

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

/// Two servers started together under `apply` on a released catalog: one
/// migrates it, and the other waits for the lock and finds nothing to do.
#[tokio::test]
async fn two_servers_started_together_with_apply_migrate_once() {
    let Some(base) = base_conn() else {
        return skip("startup two");
    };
    let s = Scratch::new(&base, "pgs_two").await;
    s.load("0.1.14").await;
    let apply = startup(true, Duration::from_secs(60));
    let (a, b) = tokio::join!(prepare(&s, &apply), prepare(&s, &apply));
    let applied: Vec<usize> = [a.expect("first"), b.expect("second")]
        .iter()
        .map(|p| p.run.as_ref().map_or(0, |r| r.applied.len()))
        .collect();
    assert!(
        applied.contains(&0) && applied.contains(&step_count()),
        "one migrates and the other finds nothing to do: {applied:?}"
    );
    let catalog = s.catalog().await;
    assert_eq!(
        ledger(&catalog).await.len(),
        extenddb_storage::migrations::MigrationId::ALL.len()
    );
    catalog.close().await;
    assert_eq!(s.status().await, CatalogStatus::Current { version: LATEST });
}
