// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live checks of migration 6's sessions check (`other_sessions`, which both
//! bodies' `detect` call), with the scratch database and the skip convention
//! of `m0006_tests`. That the runner stops on a `detect` item before its first
//! write is checked over two databases in `tests/migrations_golden.rs`.

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::m0006_escape_control_chars::{
    IGNORE_CONNECTIONS_ENV, catalog_has_rows, data_has_rows, other_sessions,
};
use super::m0006_tests::{base_conn, scratch, seed, skip, test_application};

/// `PgPool::close` returns before PostgreSQL removes the closed session from
/// `pg_stat_activity`, so a check right after a close can still count it.
/// Read the items again until there are none, for at most five seconds, and
/// return the last answer.
async fn after_close(pool: &PgPool, which: &str) -> Vec<String> {
    let mut items = other_sessions(pool, which)
        .await
        .expect("look for sessions");
    for _ in 0..50 {
        if items.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        items = other_sessions(pool, which)
            .await
            .expect("look for sessions");
    }
    items
}

/// A pool of one unnamed connection to `database`, as a server or an
/// operator's `psql` would hold.
async fn unnamed(base: &str, database: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/{database}"))
        .await
        .expect("connect as another client would");
    sqlx::query("SELECT 1").execute(&pool).await.unwrap();
    pool
}

/// A server (or any other session) still connected to the data database is
/// one item; the runner's own sessions are not.
#[tokio::test]
async fn another_session_on_the_data_database_is_reported() {
    if base_conn().is_none() {
        skip("another_session_on_the_data_database_is_reported");
        return;
    }
    let s = scratch().await;
    seed(&s.db).await;
    let base = base_conn().unwrap();
    assert_eq!(after_close(&s.db, "data").await, Vec::<String>::new());

    let server = unnamed(&base, &s.db_name).await;
    let items = other_sessions(&s.db, "data").await.unwrap();
    assert_eq!(items.len(), 1, "{items:?}");
    assert!(
        items[0].contains(&format!(
            "1 other session(s) hold the data database {}",
            s.db_name
        )),
        "{items:?}"
    );
    assert!(items[0].contains("no application name"), "{items:?}");
    assert!(items[0].contains(IGNORE_CONNECTIONS_ENV), "{items:?}");

    server.close().await;
    assert_eq!(after_close(&s.db, "data").await, Vec::<String>::new());
    s.cleanup().await;
}

/// With the catalog in its own database, a session holding only that database
/// is reported by the catalog body's check, which names the catalog database,
/// and not by the data body's.
#[tokio::test]
async fn a_session_on_a_separate_catalog_is_reported_for_the_catalog() {
    if base_conn().is_none() {
        skip("a_session_on_a_separate_catalog_is_reported_for_the_catalog");
        return;
    }
    let data = scratch().await;
    let catalog = scratch().await;
    let base = base_conn().unwrap();

    let client = unnamed(&base, &catalog.db_name).await;
    assert_eq!(after_close(&data.db, "data").await, Vec::<String>::new());
    let items = other_sessions(&catalog.db, "catalog").await.unwrap();
    assert_eq!(items.len(), 1, "{items:?}");
    assert!(
        items[0].contains(&format!(
            "other session(s) hold the catalog database {}",
            catalog.db_name
        )),
        "{items:?}"
    );

    client.close().await;
    assert_eq!(
        after_close(&catalog.db, "catalog").await,
        Vec::<String>::new()
    );
    data.cleanup().await;
    catalog.cleanup().await;
}

/// The migrator connects as the application role, which is not a superuser,
/// and `pg_stat_activity` hides `backend_type` of other roles' sessions from
/// it. A session of another role (an operator's `psql`, a server under a
/// different role) still counts.
#[tokio::test]
async fn another_roles_session_counts_from_a_non_superuser_pool() {
    if base_conn().is_none() {
        skip("another_roles_session_counts_from_a_non_superuser_pool");
        return;
    }
    let s = scratch().await;
    let base = base_conn().unwrap();
    let role = format!(
        "eddb_guard_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    sqlx::query(&format!(
        "CREATE ROLE \"{role}\" LOGIN PASSWORD 'guard-test' NOSUPERUSER"
    ))
    .execute(&s.admin)
    .await
    .expect("create a plain login role");

    // The migrator's view: the plain role, named as a runner.
    let options = format!("{base}/{}", s.db_name)
        .parse::<PgConnectOptions>()
        .expect("scratch connection string")
        .username(&role)
        .password("guard-test")
        .application_name(&test_application());
    let migrator = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("connect as the plain role");
    // Only the scratch pool is connected, and it is named as a runner.
    assert_eq!(after_close(&migrator, "data").await, Vec::<String>::new());

    // A superuser session with no application name. The plain role sees its
    // datname and usename but not its backend_type.
    let holder = unnamed(&base, &s.db_name).await;
    let hidden: Option<String> = sqlx::query_scalar(
        "SELECT backend_type FROM pg_stat_activity \
         WHERE datname = current_database() AND usename <> current_user LIMIT 1",
    )
    .fetch_one(&migrator)
    .await
    .expect("read another role's row");
    assert!(
        hidden.is_none(),
        "this test needs a role that cannot see other sessions' backend_type"
    );
    let items = other_sessions(&migrator, "data").await.unwrap();
    assert_eq!(items.len(), 1, "{items:?}");
    assert!(items[0].contains("no client address shown"), "{items:?}");

    holder.close().await;
    assert_eq!(after_close(&migrator, "data").await, Vec::<String>::new());
    migrator.close().await;
    sqlx::query(&format!("DROP ROLE \"{role}\""))
        .execute(&s.admin)
        .await
        .expect("remove the test role");
    s.cleanup().await;
}

/// A store with nothing to rewrite needs no check: a fresh store, such as the
/// one `init` creates, has no table, no backup and no queued or stream row.
#[tokio::test]
async fn only_a_store_with_rows_is_checked() {
    if base_conn().is_none() {
        skip("only_a_store_with_rows_is_checked");
        return;
    }
    let s = scratch().await;
    assert!(!catalog_has_rows(&s.db).await.unwrap());
    assert!(!data_has_rows(&s.db, &s.db).await.unwrap());
    seed(&s.db).await;
    assert!(catalog_has_rows(&s.db).await.unwrap());
    assert!(data_has_rows(&s.db, &s.db).await.unwrap());
    s.cleanup().await;
}
