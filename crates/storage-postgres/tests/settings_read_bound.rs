// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Settings reads on a catalog whose `settings` table another session holds
//! locked. A bounded read fails within its server-side bound and leaves no
//! statement waiting on the lock, including when the caller stops waiting
//! for it first. A plain read waits on the lock as before and answers once
//! it is released. See `migrations_support` for the database these tests
//! need.

mod migrations_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::migrations::MigrationId;
use extenddb_storage_postgres::PostgresCatalogStore;
use migrations_support::{Scratch, base_conn, pool, skip};
use sqlx::PgPool;

/// The test's own deadline for one read, so an unbounded read fails here.
const DEADLINE: Duration = Duration::from_secs(10);

/// Statements of the catalog database waiting on a lock.
async fn waiting(admin: &PgPool, catalog: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = $1 AND wait_event_type = 'Lock'",
    )
    .bind(catalog)
    .fetch_one(admin)
    .await
    .expect("read pg_stat_activity")
}

#[tokio::test]
async fn a_bounded_read_behind_a_held_lock_fails_within_its_bound_and_leaves_nothing_waiting() {
    let Some(base) = base_conn() else {
        return skip("settings read bound");
    };
    let s = Scratch::new(&base, "pgm_settings_bound").await;
    s.migrate_through(MigrationId::LATEST).await;
    let catalog = pool(&s.catalog_url(), 4).await;
    let admin = pool(&format!("{base}/postgres"), 1).await;
    let store = Arc::new(PostgresCatalogStore::new(catalog.clone()));
    store
        .set_setting("search_inequality_filters", "all")
        .await
        .expect("write the setting");

    let locker = pool(&s.catalog_url(), 1).await;
    let mut lock = locker.begin().await.expect("begin the locking transaction");
    sqlx::query("LOCK TABLE settings IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .expect("lock the settings table");

    // A bounded read the caller waits for ends with an error within the
    // bound.
    let started = Instant::now();
    let read = tokio::time::timeout(
        DEADLINE,
        store.get_setting_bounded("search_inequality_filters"),
    )
    .await
    .expect("a read behind the lock ends before the test's deadline");
    assert!(read.is_err(), "a read behind the lock fails: {read:?}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the read failed after {:?}",
        started.elapsed()
    );

    // Bounded reads the caller stops waiting for still end in the server.
    for _ in 0..4 {
        let _ = tokio::time::timeout(
            Duration::from_millis(100),
            store.get_setting_bounded("search_inequality_filters"),
        )
        .await;
    }
    assert!(waiting(&admin, &s.catalog_name).await > 0);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        waiting(&admin, &s.catalog_name).await,
        0,
        "a statement still waits on the lock 3 s after its read was dropped"
    );

    // A plain read is not bounded: it still waits on the lock after the
    // bounded read's bound, and answers once the lock is released.
    let plain = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { store.get_setting("search_inequality_filters").await })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !plain.is_finished(),
        "a plain read behind the lock ended within 3 s"
    );
    assert!(waiting(&admin, &s.catalog_name).await > 0);

    lock.rollback().await.expect("release the lock");
    let read = tokio::time::timeout(DEADLINE, plain)
        .await
        .expect("the plain read ends once the lock is released")
        .expect("the read task runs");
    assert_eq!(read.expect("the read succeeds"), Some("all".to_owned()));
    let read = tokio::time::timeout(
        DEADLINE,
        store.get_setting_bounded("search_inequality_filters"),
    )
    .await
    .expect("a bounded read after the lock ends");
    assert_eq!(read.expect("the read succeeds"), Some("all".to_owned()));

    locker.close().await;
    catalog.close().await;
    admin.close().await;
}
