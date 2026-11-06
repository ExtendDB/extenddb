// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests that seed one SQLite catalog file across several starts, as a dev
//! container does when it restarts on a reused volume.
//!
//! Each start opens the file again, so a test sees what the catalog kept from
//! the previous start, not what one connection still holds.

use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use extenddb_auth::CredentialStore;
use extenddb_storage::management_store::ManagementStore;
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage_sqlite::migrations::SqliteMigrations;
use extenddb_storage_sqlite::{SqliteCatalogStore, SqliteCredentialStore};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::{EXAMPLE_ACCESS_KEY_ID, EXAMPLE_SECRET_ACCESS_KEY, resolve_from, seed};

const ACCOUNT: &str = "123456789012";
const TEAM_KEY: &str = "AKIATEAMEXAMPLE00001";
const TEAM_SECRET: &str = "team-secret";

/// The catalog and credential stores of one start, over `path`.
struct Start {
    catalog: SqliteCatalogStore,
    credentials: SqliteCredentialStore,
}

/// Open the catalog file at `path`, creating and migrating it on the first
/// start, with `ACCOUNT` as the default account.
async fn start(path: &Path) -> Start {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true);
    // The migration runner takes a one-connection pool.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("open the catalog file");
    let migrations =
        SqliteMigrations::on_pool(pool.clone(), &path.display().to_string()).expect("one pool");
    extenddb_storage::migrations::Runner::new(&migrations, "test")
        .migrate()
        .await
        .expect("migrate the catalog");

    // Any fixed 32-byte key: the two stores only have to agree on it.
    let key = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
    let catalog = SqliteCatalogStore::with_encryption_key(
        pool.clone(),
        key.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    match catalog.create_account(ACCOUNT, "dev").await {
        Ok(()) | Err(extenddb_storage::management_store::OpError::AlreadyExists(_)) => {}
        Err(e) => panic!("create the default account: {e:?}"),
    }
    catalog
        .set_setting("default_account_id", ACCOUNT)
        .await
        .expect("record the default account");
    Start {
        catalog,
        credentials: SqliteCredentialStore::new(pool, key),
    }
}

/// Seed one start with the credentials the given environment resolves to.
async fn seed_start(s: &Start, team: bool, disable_example: bool) {
    let (key, secret) = if team {
        (Some(TEAM_KEY.to_owned()), Some(TEAM_SECRET.to_owned()))
    } else {
        (None, None)
    };
    let credentials = resolve_from(key, secret, disable_example).expect("resolve");
    seed(&s.catalog, &s.credentials, &credentials)
        .await
        .expect("seed");
}

/// The account and user that hold `key_id`, or `None` when no user does.
async fn owner(s: &Start, key_id: &str) -> Option<(String, String)> {
    s.credentials
        .lookup_credential(key_id)
        .await
        .expect("look the key up")
        .map(|c| (c.account_id.clone(), c.principal_name.clone()))
}

#[tokio::test]
async fn the_switch_removes_the_example_key_an_earlier_start_seeded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.sqlite");

    // First start: the default, which seeds the example pair on `dev`.
    let first = start(&path).await;
    seed_start(&first, false, false).await;
    let stored = first
        .credentials
        .lookup_credential(EXAMPLE_ACCESS_KEY_ID)
        .await
        .unwrap()
        .expect("the example key is seeded by default");
    assert_eq!(stored.secret_key, EXAMPLE_SECRET_ACCESS_KEY);
    assert_eq!(stored.principal_name, "dev");
    drop(first);

    // Second start on the same file: the switch and a team pair.
    let second = start(&path).await;
    seed_start(&second, true, true).await;
    assert_eq!(owner(&second, EXAMPLE_ACCESS_KEY_ID).await, None);
    assert_eq!(
        owner(&second, TEAM_KEY).await,
        Some((ACCOUNT.to_owned(), "dev".to_owned()))
    );
    drop(second);

    // Third start with the switch: nothing left to remove, and no error.
    let third = start(&path).await;
    seed_start(&third, true, true).await;
    assert_eq!(owner(&third, EXAMPLE_ACCESS_KEY_ID).await, None);
    assert!(owner(&third, TEAM_KEY).await.is_some());
}

#[tokio::test]
async fn the_switch_removes_the_example_key_from_whichever_user_holds_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.sqlite");

    // A start with the switch, then an operator imports the public pair for
    // another user, as `extenddb manage import-access-key` does.
    let first = start(&path).await;
    seed_start(&first, true, true).await;
    first
        .catalog
        .create_user(ACCOUNT, "alice", None)
        .await
        .unwrap();
    first
        .catalog
        .import_access_key(
            ACCOUNT,
            "alice",
            EXAMPLE_ACCESS_KEY_ID,
            EXAMPLE_SECRET_ACCESS_KEY,
        )
        .await
        .unwrap();
    assert_eq!(
        owner(&first, EXAMPLE_ACCESS_KEY_ID).await,
        Some((ACCOUNT.to_owned(), "alice".to_owned()))
    );
    drop(first);

    // The next start with the switch removes it from `alice`, and leaves
    // `alice` and the team key in place.
    let second = start(&path).await;
    seed_start(&second, true, true).await;
    assert_eq!(owner(&second, EXAMPLE_ACCESS_KEY_ID).await, None);
    assert!(
        second
            .catalog
            .get_user_detail(ACCOUNT, "alice")
            .await
            .unwrap()
            .is_some(),
        "the user that held the key is kept"
    );
    assert!(owner(&second, TEAM_KEY).await.is_some());
}

#[tokio::test]
async fn without_the_switch_the_example_key_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.sqlite");

    let first = start(&path).await;
    seed_start(&first, true, false).await;
    drop(first);

    let second = start(&path).await;
    seed_start(&second, true, false).await;
    assert_eq!(
        owner(&second, EXAMPLE_ACCESS_KEY_ID).await,
        Some((ACCOUNT.to_owned(), "dev".to_owned()))
    );
    assert!(owner(&second, TEAM_KEY).await.is_some());
}
