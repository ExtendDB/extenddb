// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog migrations against a live node: migration 1's definitions,
//! the status of keyspaces that are not catalogs, the ledger, the migration
//! lock's lease, the serving registration and CreateAccount's account
//! migrations. Each test uses keyspaces of its own prefix and drops them.
//! Skipped without a node unless `EXTENDDB_TEST_CASSANDRA=required`.
//!
//! With `EXTENDDB_BLESS_EXPECTED=1`,
//! `migration_1_creates_the_expected_definitions` writes
//! `migrations/{catalog,account}/0001_baseline.expected.json` instead of
//! comparing.

#[path = "common/mod.rs"]
mod helpers;
#[path = "migrations/lock.rs"]
mod lock;
mod migrations_support;

use std::sync::Arc;

use extenddb_storage::management_store::{ManagementStore, SettingsStore};
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, MigrationError, MigrationId, RunKind, StoreKind, Target,
    run_locked,
};
use extenddb_storage_cassandra::CassandraCatalogStore;
use extenddb_storage_cassandra::cassandra_util::CassandraSession;
use extenddb_storage_cassandra::migrations::{
    CassandraCatalogMigrations, catalog_status, migrate_account, migrate_catalog_keyspace,
    postcheck, run_baseline_unrecorded,
};
use migrations_support::{TIMEOUT, cql, keyspace, new_prefix, pinned_session, session};

/// A catalog keyspace at migration 4 and one account keyspace.
async fn deployment(session: &Arc<CassandraSession>, prefix: &str) -> String {
    keyspace(session, &format!("{prefix}_catalog")).await;
    migrate_catalog_keyspace(session.clone(), prefix, "test", TIMEOUT)
        .await
        .expect("migrate the catalog");
    let account = format!("{prefix}_account_111111111111");
    keyspace(session, &account).await;
    cql(
        session,
        &format!(
            "INSERT INTO {prefix}_catalog.accounts (account_id, account_name, created_at) VALUES \
             ('111111111111', 'one', toTimestamp(now()))"
        ),
    )
    .await;
    migrate_account(session.clone(), prefix, &account, "test")
        .await
        .expect("migrate the account");
    account
}

fn target_of(prefix: &str) -> Target {
    let config = helpers::test_config();
    Target::at(format!(
        "{}:{}@{}/{prefix}",
        config.username.unwrap_or_default(),
        config.password.unwrap_or_default(),
        config.contact_points.join(",")
    ))
}

/// Two sessions for two migrators, each sending all its requests to one
/// node: the test node, and the node `EXTENDDB_TEST_CASSANDRA_SECOND_CONTACT_POINT`
/// names, the second node of a two-node cluster, so the two migrators reach
/// the catalog through different coordinators. Both go to the test node
/// when it is not set.
async fn migrator_sessions() -> (Arc<CassandraSession>, Arc<CassandraSession>) {
    let first = helpers::test_contact_point();
    let second = std::env::var("EXTENDDB_TEST_CASSANDRA_SECOND_CONTACT_POINT")
        .unwrap_or_else(|_| first.clone());
    (pinned_session(&first).await, pinned_session(&second).await)
}

#[tokio::test]
async fn two_migrators_started_together_apply_a_legacy_catalog_once() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (first_node, second_node) = migrator_sessions().await;
    let (prefix, _drops) = new_prefix("race");
    let catalog = format!("{prefix}_catalog");
    keyspace(&session, &catalog).await;
    run_baseline_unrecorded(&session, &catalog, StoreKind::Catalog)
        .await
        .expect("the legacy tables");
    cql(
        &session,
        &format!("INSERT INTO {catalog}.settings (key, value) VALUES ('catalog_version', '0.0.4')"),
    )
    .await;
    let (first, other) = tokio::join!(
        migrate_catalog_keyspace(first_node.clone(), &prefix, "test", TIMEOUT),
        migrate_catalog_keyspace(second_node.clone(), &prefix, "test", TIMEOUT),
    );
    let applied = [
        first.expect("the first migrator").applied.len(),
        other.expect("the second migrator").applied.len(),
    ];
    assert!(
        applied.iter().filter(|n| **n == 0).count() == 1 && applied.iter().any(|n| *n > 0),
        "one migrator applies and the other finds nothing to do: {applied:?}"
    );
    assert_eq!(
        catalog_status(second_node.clone(), &prefix)
            .await
            .expect("status"),
        CatalogStatus::Current { version: 4 }
    );
}

#[tokio::test]
async fn migration_1_creates_the_expected_definitions() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("expected");
    let bless = std::env::var("EXTENDDB_BLESS_EXPECTED").as_deref() == Ok("1");
    let account = format!("{prefix}_account_111111111111");
    if bless {
        // Migration 1's bodies on empty keyspaces, without the runner, whose
        // post-check reads the file being written.
        for (keyspace_name, kind) in [
            (format!("{prefix}_catalog"), StoreKind::Catalog),
            (account.clone(), StoreKind::Account),
        ] {
            keyspace(&session, &keyspace_name).await;
            run_baseline_unrecorded(&session, &keyspace_name, kind)
                .await
                .expect("migration 1's body");
        }
    } else {
        deployment(&session, &prefix).await;
        assert_eq!(
            catalog_status(session.clone(), &prefix)
                .await
                .expect("status"),
            CatalogStatus::Current {
                version: MigrationId::LATEST.number()
            }
        );
    }
    for (dir, keyspace) in [
        ("catalog", format!("{prefix}_catalog")),
        ("account", account.clone()),
    ] {
        let actual = postcheck::describe(&session, &keyspace)
            .await
            .expect("describe");
        let path = format!(
            "{}/migrations/{dir}/0001_baseline.expected.json",
            env!("CARGO_MANIFEST_DIR")
        );
        if bless {
            let text = serde_json::to_string_pretty(&actual).expect("json") + "\n";
            std::fs::write(&path, text).expect("write the expected file");
            continue;
        }
        let expected: postcheck::Definitions =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("json");
        assert_eq!(
            postcheck::compare(&expected, &actual),
            Vec::<String>::new(),
            "{dir}"
        );
        assert_eq!(
            expected, actual,
            "{dir}: a fresh install names other objects"
        );
    }
}

#[tokio::test]
async fn a_keyspace_that_is_not_a_catalog_is_uninitialized_and_migrate_refuses_it() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("uninit");
    let catalog = format!("{prefix}_catalog");
    let status = || catalog_status(session.clone(), &prefix);
    // No keyspace at all.
    assert_eq!(
        status().await.expect("status"),
        CatalogStatus::Uninitialized
    );
    // An empty keyspace.
    keyspace(&session, &catalog).await;
    assert_eq!(
        status().await.expect("status"),
        CatalogStatus::Uninitialized
    );
    // A keyspace whose settings table holds only the lock row.
    cql(
        &session,
        &format!("CREATE TABLE {catalog}.settings (key text PRIMARY KEY, value text)"),
    )
    .await;
    cql(
        &session,
        &format!("INSERT INTO {catalog}.settings (key, value) VALUES ('migration_lock', 'x')"),
    )
    .await;
    assert_eq!(
        status().await.expect("status"),
        CatalogStatus::Uninitialized
    );
    // migrate --yes refuses it under the lock, naming init (exit 1).
    cql(
        &session,
        &format!("DELETE FROM {catalog}.settings WHERE key = 'migration_lock'"),
    )
    .await;
    let migrations = CassandraCatalogMigrations;
    let target = target_of(&prefix);
    let lock = migrations.lock(&target, TIMEOUT).await.expect("lock");
    let refused = run_locked(&migrations, &target, &lock, "test", RunKind::Migrate).await;
    migrations.release(lock).await;
    assert_eq!(
        refused,
        Err(MigrationError::Status(CatalogStatus::Uninitialized))
    );
    assert_eq!(
        MigrationError::Status(CatalogStatus::Uninitialized).exit_code(),
        1
    );
    // Another program's keyspace of the same name.
    let (other, _other_drops) = new_prefix("foreign");
    let other_catalog = format!("{other}_catalog");
    keyspace(&session, &other_catalog).await;
    cql(
        &session,
        &format!("CREATE TABLE {other_catalog}.settings (name text PRIMARY KEY, val int)"),
    )
    .await;
    cql(
        &session,
        &format!("CREATE TABLE {other_catalog}.accounts (id int PRIMARY KEY)"),
    )
    .await;
    assert_eq!(
        catalog_status(session.clone(), &other)
            .await
            .expect("status"),
        CatalogStatus::Uninitialized
    );
    // migrate --yes, the preview and verify refuse it and name what it
    // holds, as init does; nothing is written to it. The advice for an
    // uninitialized catalog, running init, would fail on this keyspace.
    for refused in [
        migrations.lock(&target_of(&other), TIMEOUT).await.map(drop),
        migrations.status(&target_of(&other)).await.map(drop),
    ] {
        match refused {
            Err(MigrationError::Refused(message)) => assert!(
                message.contains("is not an ExtendDB catalog")
                    && message
                        .contains("the table settings without the catalog's key and value columns")
                    && !message.contains("the table accounts"),
                "{message}"
            ),
            other => panic!("{:?}", other.err()),
        }
    }
    assert_eq!(
        MigrationError::Refused(String::new()).exit_code(),
        1,
        "the script asserts exit 1"
    );
    // Another program's keyspace whose settings table has the catalog's two
    // columns and rows of its own: refused before the lock row is written.
    let (shaped, _shaped_drops) = new_prefix("shaped");
    let shaped_catalog = format!("{shaped}_catalog");
    keyspace(&session, &shaped_catalog).await;
    cql(
        &session,
        &format!("CREATE TABLE {shaped_catalog}.settings (key text PRIMARY KEY, value text)"),
    )
    .await;
    cql(
        &session,
        &format!("INSERT INTO {shaped_catalog}.settings (key, value) VALUES ('theme', 'dark')"),
    )
    .await;
    assert_eq!(
        catalog_status(session.clone(), &shaped)
            .await
            .expect("status"),
        CatalogStatus::Uninitialized
    );
    match migrations.lock(&target_of(&shaped), TIMEOUT).await {
        Err(MigrationError::Refused(message)) => assert!(
            message.contains("is not an ExtendDB catalog")
                && message.contains("the settings row 'theme'"),
            "{message}"
        ),
        other => panic!("{:?}", other.err()),
    }
    let keys: Vec<String> = session
        .query(format!("SELECT key FROM {shaped_catalog}.settings"))
        .await
        .expect("read")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default()
        .iter()
        .map(|row| {
            use cdrs_tokio::types::IntoRustByName;
            row.get_r_by_name("key").expect("key")
        })
        .collect();
    assert_eq!(keys, ["theme"], "no lock row was written");
}

#[tokio::test]
async fn a_ledger_that_cannot_be_read_stops_the_status() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("ledger");
    let catalog = format!("{prefix}_catalog");
    keyspace(&session, &catalog).await;
    // A ledger whose id this binary cannot read is an error, never "not
    // applied" (Cassandra item 2).
    cql(
        &session,
        &format!(
            "CREATE TABLE {catalog}.extenddb_migrations (id text PRIMARY KEY, name text, kind \
             text, checksum text, outcome text, notes text, applied_at timestamp, applied_by text)"
        ),
    )
    .await;
    cql(
        &session,
        &format!(
            "INSERT INTO {catalog}.extenddb_migrations (id, name, kind, checksum, outcome, \
             notes, applied_by) VALUES ('1', 'baseline', 'breaking', 'c', 'applied', '', 'x')"
        ),
    )
    .await;
    let error = catalog_status(session.clone(), &prefix)
        .await
        .expect_err("an unreadable ledger");
    assert!(error.to_string().contains(&catalog), "{error}");
}

#[tokio::test]
async fn migration_1_converges_a_legacy_catalog_and_stops_on_a_table_of_another_shape() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("legacy");
    let catalog = format!("{prefix}_catalog");
    keyspace(&session, &catalog).await;
    // A catalog with the legacy files' tables and no ledger, as a release
    // before 1.0 left it, with the traces in its settings: the version row,
    // the marker, and a setting an operator changed.
    run_baseline_unrecorded(&session, &catalog, StoreKind::Catalog)
        .await
        .expect("the legacy tables");
    for (key, value) in [
        ("catalog_version", "0.0.4"),
        ("resource_tag_migration", "applied"),
        ("control_plane_delay_seconds", "1.5"),
    ] {
        cql(
            &session,
            &format!("INSERT INTO {catalog}.settings (key, value) VALUES ('{key}', '{value}')"),
        )
        .await;
    }
    let status = catalog_status(session.clone(), &prefix)
        .await
        .expect("status");
    assert!(
        matches!(status, CatalogStatus::Behind { version: None, .. }),
        "{status:?}"
    );
    migrate_catalog_keyspace(session.clone(), &prefix, "test", TIMEOUT)
        .await
        .expect("migrate");
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.clone());
    assert_eq!(
        store.get_setting("catalog_version").await.expect("read"),
        Some("4".to_owned())
    );
    assert_eq!(
        store
            .get_setting("resource_tag_migration")
            .await
            .expect("read"),
        None,
        "migration 1 deletes the marker"
    );
    assert_eq!(
        store
            .get_setting("control_plane_delay_seconds")
            .await
            .expect("read"),
        Some("1.5".to_owned()),
        "an operator's setting is kept"
    );

    // A table of another shape: migration 1 stops on it and records nothing.
    let (other, _other_drops) = new_prefix("shape");
    let other_catalog = format!("{other}_catalog");
    keyspace(&session, &other_catalog).await;
    cql(
        &session,
        &format!("CREATE TABLE {other_catalog}.settings (key text PRIMARY KEY, value text)"),
    )
    .await;
    cql(
        &session,
        &format!(
            "CREATE TABLE {other_catalog}.accounts (account_id text PRIMARY KEY, account_name int)"
        ),
    )
    .await;
    let error = migrate_catalog_keyspace(session.clone(), &other, "test", TIMEOUT)
        .await
        .expect_err("a column of another type");
    assert!(
        error.to_string().contains("accounts.account_name"),
        "{error}"
    );
    let status = catalog_status(session.clone(), &other)
        .await
        .expect("status");
    assert_eq!(
        status,
        CatalogStatus::Uninitialized,
        "no ledger row was written"
    );
}

#[tokio::test]
async fn create_account_runs_the_account_migrations_through_the_runner() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("account");
    keyspace(&session, &format!("{prefix}_catalog")).await;
    migrate_catalog_keyspace(session.clone(), &prefix, "test", TIMEOUT)
        .await
        .expect("migrate the catalog");
    let store =
        CassandraCatalogStore::new(session.clone(), prefix.clone(), "datacenter1".to_owned(), 1)
            .recording_release("9.9.9");
    // CreateAccount runs the account migrations over one keyspace and leaves
    // the deployment's version row as it was: a sentinel shows a write.
    cql(
        &session,
        &format!("UPDATE {prefix}_catalog.settings SET value = '7' WHERE key = 'catalog_version'"),
    )
    .await;
    store
        .create_account("111111111111", "one")
        .await
        .expect("create the account");
    assert_eq!(
        store
            .get_setting("catalog_version")
            .await
            .expect("read the version row"),
        Some("7".to_owned()),
        "CreateAccount wrote the deployment's catalog_version"
    );
    cql(
        &session,
        &format!("UPDATE {prefix}_catalog.settings SET value = '4' WHERE key = 'catalog_version'"),
    )
    .await;
    let account = format!("{prefix}_account_111111111111");
    let rows = session
        .query(format!(
            "SELECT id, outcome, applied_by FROM {account}.extenddb_migrations"
        ))
        .await
        .expect("read")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default();
    assert_eq!(rows.len(), 1, "the account keyspace records migration 1");
    use cdrs_tokio::types::IntoRustByName;
    let id: i32 = rows[0].get_r_by_name("id").expect("id");
    let by: String = rows[0].get_r_by_name("applied_by").expect("applied_by");
    assert_eq!((id, by.as_str()), (1, "9.9.9"));
    assert_eq!(
        catalog_status(session.clone(), &prefix)
            .await
            .expect("status"),
        CatalogStatus::Current { version: 4 }
    );
    // A catalog-only store refuses to create a keyspace with a guessed
    // replication.
    let catalog_only = CassandraCatalogStore::catalog_only(session.clone(), prefix.clone());
    assert!(
        catalog_only
            .create_account("222222222222", "two")
            .await
            .is_err()
    );
}
