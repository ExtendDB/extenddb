// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Settings reads on a catalog store whose one connection is inside a
//! `READ ONLY` transaction its owner opened, as the session that `serve`
//! loads its keys on is. Neither read ends that transaction, makes it
//! writable or leaves its settings changed, and a bounded read that fails
//! behind a held lock leaves it usable. See `migrations_support` for the
//! database these tests need.

mod migrations_support;

use std::time::Duration;

use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::migrations::MigrationId;
use extenddb_storage_postgres::PostgresCatalogStore;
use migrations_support::{Scratch, base_conn, pool, skip};
use sqlx::PgPool;

/// The test's own deadline for one read.
const DEADLINE: Duration = Duration::from_secs(10);

/// What the owner's transaction looks like: whether the session is inside a
/// transaction block, its start time, whether it is read-only, and the two
/// settings a bounded read sets.
async fn state(conn: &PgPool) -> (bool, String, String, String, String) {
    sqlx::query_as(
        "SELECT statement_timestamp() <> transaction_timestamp(), \
         transaction_timestamp()::text, current_setting('transaction_read_only'), \
         current_setting('statement_timeout'), current_setting('lock_timeout')",
    )
    .fetch_one(conn)
    .await
    .expect("read the session state")
}

/// A one-connection pool on the catalog whose connection is in a
/// `READ ONLY` transaction, and a store on it.
async fn read_only_store(url: &str) -> (PgPool, PostgresCatalogStore) {
    let owner = pool(url, 1).await;
    sqlx::raw_sql("BEGIN READ ONLY")
        .execute(&owner)
        .await
        .expect("open the owner's transaction");
    let store = PostgresCatalogStore::new(owner.clone());
    (owner, store)
}

#[tokio::test]
async fn reads_inside_a_read_only_transaction_leave_it_open_and_read_only() {
    let Some(base) = base_conn() else {
        return skip("settings read in a transaction");
    };
    let s = Scratch::new(&base, "pgm_settings_in_tx").await;
    s.migrate_through(MigrationId::LATEST).await;
    let writer = pool(&s.catalog_url(), 1).await;
    PostgresCatalogStore::new(writer.clone())
        .set_setting("search_inequality_filters", "all")
        .await
        .expect("write the setting");

    let (owner, store) = read_only_store(&s.catalog_url()).await;
    let before = state(&owner).await;
    assert!(before.0, "the owner's transaction is open: {before:?}");
    assert_eq!(before.2, "on");

    let plain = tokio::time::timeout(DEADLINE, store.get_setting("search_inequality_filters"))
        .await
        .expect("the plain read ends");
    assert_eq!(
        plain.expect("the plain read succeeds"),
        Some("all".to_owned())
    );
    assert_eq!(state(&owner).await, before, "after the plain read");

    let bounded = tokio::time::timeout(
        DEADLINE,
        store.get_setting_bounded("search_inequality_filters"),
    )
    .await
    .expect("the bounded read ends");
    assert_eq!(
        bounded.expect("the bounded read succeeds"),
        Some("all".to_owned())
    );
    assert_eq!(state(&owner).await, before, "after the bounded read");

    let write =
        sqlx::query("UPDATE settings SET value = 'off' WHERE key = 'search_inequality_filters'")
            .execute(&owner)
            .await;
    assert!(write.is_err(), "the owner's transaction is still read-only");

    owner.close().await;
    writer.close().await;
}

#[tokio::test]
async fn a_bounded_read_that_fails_inside_a_transaction_leaves_it_usable() {
    let Some(base) = base_conn() else {
        return skip("settings read in a transaction behind a lock");
    };
    let s = Scratch::new(&base, "pgm_settings_in_tx_lock").await;
    s.migrate_through(MigrationId::LATEST).await;
    let (owner, store) = read_only_store(&s.catalog_url()).await;
    let before = state(&owner).await;

    let locker = pool(&s.catalog_url(), 1).await;
    let mut lock = locker.begin().await.expect("begin the locking transaction");
    sqlx::query("LOCK TABLE settings IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .expect("lock the settings table");

    let read = tokio::time::timeout(
        DEADLINE,
        store.get_setting_bounded("search_inequality_filters"),
    )
    .await
    .expect("a bounded read behind the lock ends before the test's deadline");
    assert!(read.is_err(), "a read behind the lock fails: {read:?}");
    assert_eq!(
        state(&owner).await,
        before,
        "the owner's transaction is open, unaborted and unchanged"
    );

    lock.rollback().await.expect("release the lock");
    owner.close().await;
    locker.close().await;
}
