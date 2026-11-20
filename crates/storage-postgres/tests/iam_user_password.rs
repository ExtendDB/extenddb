// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `verify_iam_user_password` runs one bcrypt check for every name: against
//! the user's hash when the user has a console password, and against the
//! fixed hash at the default cost for a user without one, an unknown user and
//! an unknown account. The test works in a scratch database on the server
//! that `EXTENDDB_TEST_PG_CONNECTION_STRING` names, and drops it at the end.

use extenddb_storage::management_store::ManagementStore;
use extenddb_storage::password::count_verifications;
use extenddb_storage_postgres::{PostgresCatalogStore, apply_all_migrations};
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";

/// bcrypt's lowest cost, for the test user's hash. It keeps the test fast and
/// tells a check against a stored hash apart from one against the fixed hash,
/// which is at the default cost.
const TEST_COST: u32 = 4;

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
async fn an_unknown_user_or_one_without_a_password_runs_one_bcrypt_check() {
    let Some(base) = base_conn() else {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
    let db_name = format!(
        "eddb_iampw_{}",
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
    store.create_account(ACCOUNT, "acct").await.unwrap();
    let hash = bcrypt::hash("correct horse", TEST_COST).unwrap();
    store
        .create_user(ACCOUNT, "alice", Some(&hash))
        .await
        .unwrap();
    store.create_user(ACCOUNT, "bob", None).await.unwrap();

    let (right, right_costs) =
        count_verifications(store.verify_iam_user_password(ACCOUNT, "alice", "correct horse"))
            .await;
    assert!(right.unwrap());
    assert_eq!(right_costs, [TEST_COST]);

    // The cost of the one check each refused login runs: alice's hash for her
    // wrong password, the fixed hash for the others.
    let cases = [
        (ACCOUNT, "alice", TEST_COST, "wrong password"),
        (
            ACCOUNT,
            "bob",
            bcrypt::DEFAULT_COST,
            "user without a console password",
        ),
        (ACCOUNT, "nobody", bcrypt::DEFAULT_COST, "unknown user"),
        (
            "111122223333",
            "alice",
            bcrypt::DEFAULT_COST,
            "unknown account",
        ),
    ];
    for (account_id, user_name, cost, case) in cases {
        let (verified, costs) = count_verifications(store.verify_iam_user_password(
            account_id,
            user_name,
            "battery staple",
        ))
        .await;
        assert!(!verified.unwrap(), "{case}: verified");
        assert_eq!(costs, [cost], "{case}: bcrypt checks and their cost");
    }

    drop(store);
    db.close().await;
    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
    ))
    .execute(&admin)
    .await
    .expect("remove the scratch database");
    admin.close().await;
}
