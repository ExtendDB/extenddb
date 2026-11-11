// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The metrics store on a catalog that has run every migration (#728):
//! `extenddb_storage::metrics_conformance`, and migration 10's replacement of
//! a released catalog's `metrics` table with an empty `account_metrics`. See
//! `migrations_support` for the database these tests need.

mod migrations_support;

use extenddb_storage::migrations::MigrationId;
use extenddb_storage_postgres::PostgresCatalogStore;
use migrations_support::{Scratch, base_conn, count, skip};

/// Two accounts' tables of the same name keep their own rows, and the
/// account filter returns only that account's.
#[tokio::test]
async fn the_metrics_store_keeps_two_accounts_apart() {
    let Some(base) = base_conn() else {
        return skip("metrics store");
    };
    let s = Scratch::new(&base, "pgm_metrics").await;
    s.migrate_through(MigrationId::LATEST).await;
    let catalog = s.catalog().await;
    let store = PostgresCatalogStore::new(catalog.clone());
    extenddb_storage::metrics_conformance::run(&store).await;
    catalog.close().await;
}

/// Migration 10 drops a released catalog's `metrics` table, rows and all,
/// and leaves an empty `account_metrics`; a second run of its body keeps
/// the rows written since.
#[tokio::test]
async fn migration_10_replaces_metrics_with_an_empty_account_metrics() {
    let Some(base) = base_conn() else {
        return skip("migration 10");
    };
    let s = Scratch::new(&base, "pgm_m10").await;
    s.migrate_through(MigrationId::WarmThroughput).await;
    let catalog = s.catalog().await;
    sqlx::query(
        "INSERT INTO metrics (bucket, metric, table_name, operation, sum, count, min, max) \
         VALUES (now(), 'UserErrors', 'Orders', 'PutItem', 1, 1, 1, 1)",
    )
    .execute(&catalog)
    .await
    .expect("insert a metrics row");
    s.migrate_through(MigrationId::AccountMetrics).await;
    assert_eq!(
        count(
            &catalog,
            "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'metrics'"
        )
        .await,
        0,
        "metrics is dropped"
    );
    assert_eq!(
        count(&catalog, "SELECT COUNT(*) FROM account_metrics").await,
        0
    );
    for account in ["111111111111", "222222222222"] {
        sqlx::query(
            "INSERT INTO account_metrics \
             (bucket, account_id, metric, table_name, operation, sum, count, min, max) \
             VALUES (now(), $1, 'UserErrors', 'Orders', 'PutItem', 1, 1, 1, 1)",
        )
        .bind(account)
        .execute(&catalog)
        .await
        .expect("insert an account_metrics row");
    }
    sqlx::raw_sql(include_str!("../migrations/0010_account_metrics.sql"))
        .execute(&catalog)
        .await
        .expect("the body again");
    assert_eq!(
        count(&catalog, "SELECT COUNT(*) FROM account_metrics").await,
        2
    );
    catalog.close().await;
}
