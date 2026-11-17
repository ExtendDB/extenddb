// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A deleted principal or account leaves nothing for one created later with
//! its name or id (`extenddb_storage::iam_leftovers_conformance`). The test
//! works in a scratch database on the server that
//! `EXTENDDB_TEST_PG_CONNECTION_STRING` names, and drops it at the end.

use extenddb_storage_postgres::{PostgresCatalogStore, apply_all_migrations};
use sqlx::postgres::PgPoolOptions;

/// The server's base URL, or `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED`
/// set to any value but `0`, a job that must reach a server fails here
/// instead.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.is_empty() {
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

#[tokio::test]
async fn deleted_principals_and_accounts_leave_nothing() {
    let Some(base) = base_conn() else {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
    let db_name = format!(
        "eddb_leftovers_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");
    let db = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!("{base}/{db_name}"))
        .await
        .expect("connect to the scratch database");
    apply_all_migrations(&db, &db)
        .await
        .expect("apply the crate's migrations");

    let store = PostgresCatalogStore::new(db.clone());
    extenddb_storage::iam_leftovers_conformance::run(&store, "123456789012").await;

    drop(store);
    db.close().await;
    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
    ))
    .execute(&admin)
    .await
    .expect("drop the scratch database");
    admin.close().await;
}
