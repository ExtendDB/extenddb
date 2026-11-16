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
    CassandraCatalogMigrations, catalog_status, foreign_contents, migrate_account,
    migrate_catalog_keyspace, postcheck, run_baseline_unrecorded,
};
use migrations_support::{TIMEOUT, cql, keyspace, new_prefix, pinned_session, session};

/// Taken for reading by each test that runs an account migration, and for
/// writing by the test that runs `init`'s default account under the
/// migration lock. A process runs its account migrations one at a time
/// (#453) and the tests of this binary run at once, so a run under the
/// lock's lease would otherwise wait for other tests' account runs, and the
/// wait records no progress on the lease: on two nodes it outlasted the
/// lease's 30 s stall. `init`'s own process runs no other account migration.
static ACCOUNT_RUNS: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

/// A catalog keyspace at the latest migration and one account keyspace.
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
    let runs = ACCOUNT_RUNS.read().await;
    migrate_account(session.clone(), prefix, &account, "test")
        .await
        .expect("migrate the account");
    drop(runs);
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

/// Two sessions for two migrators, each with query plans that hold one
/// node: the test node, and the node `EXTENDDB_TEST_CASSANDRA_SECOND_CONTACT_POINT`
/// names, the second node of a two-node cluster, so the two migrators reach
/// the catalog through different coordinators. Both hold the test node when
/// it is not set.
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
    // The second migrator waits for the first's whole run, every schema
    // statement of migrations 1 to 6, 9 and 10 with an agreement wait after
    // each: about 16 s on an idle node, and past 60 s beside the rest of this
    // binary's tests on one node.
    let wait = 4 * TIMEOUT;
    let (first, other) = tokio::join!(
        migrate_catalog_keyspace(first_node.clone(), &prefix, "test", wait),
        migrate_catalog_keyspace(second_node.clone(), &prefix, "test", wait),
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
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
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
        let expected = migrations_support::expected_definitions(dir);
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
                    && message.contains(
                        "the table accounts, whose column(s) id migration 1 does not create"
                    ),
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

/// A table is not a catalog's for its name alone: another program's
/// `metrics` and `tags`, with columns migration 1 does not create and a
/// row each, are refused, and so is a `schema_history` with the catalog's
/// columns and a row migration 1 does not insert. A keyspace an `init`
/// left before migration 1 recorded anything, with migration 1's own
/// `schema_history` rows, is still a catalog to finish.
#[tokio::test]
async fn tables_with_catalog_names_and_other_columns_or_rows_are_refused() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let migrations = CassandraCatalogMigrations;
    let (named, _named_drops) = new_prefix("named");
    let catalog = format!("{named}_catalog");
    keyspace(&session, &catalog).await;
    for statement in [
        format!("CREATE TABLE {catalog}.metrics (id int PRIMARY KEY, reading double)"),
        format!("CREATE TABLE {catalog}.tags (name text PRIMARY KEY, color text)"),
        format!("INSERT INTO {catalog}.metrics (id, reading) VALUES (1, 2.5)"),
        format!("INSERT INTO {catalog}.tags (name, color) VALUES ('urgent', 'red')"),
    ] {
        cql(&session, &statement).await;
    }
    match migrations.lock(&target_of(&named), TIMEOUT).await {
        Err(MigrationError::Refused(message)) => assert!(
            message.contains("is not an ExtendDB catalog")
                && message.contains("the table metrics, whose column(s) id, reading")
                && message.contains("the table tags, whose column(s) color, name"),
            "{message}"
        ),
        other => panic!("{:?}", other.err()),
    }
    assert_eq!(
        helpers_tables(&session, &catalog).await,
        ["metrics", "tags"],
        "nothing was created"
    );

    // Empty tables whose columns are a part of a catalog table's, with the
    // same names and types: migration 1 creates each of these tables with
    // the columns they lack, so another program made them.
    let (subset, _subset_drops) = new_prefix("subset");
    let catalog = format!("{subset}_catalog");
    keyspace(&session, &catalog).await;
    for statement in [
        format!("CREATE TABLE {catalog}.accounts (account_id text PRIMARY KEY)"),
        format!(
            "CREATE TABLE {catalog}.tables (account_id text, table_name text, PRIMARY KEY \
             ((account_id), table_name))"
        ),
    ] {
        cql(&session, &statement).await;
    }
    match migrations.lock(&target_of(&subset), TIMEOUT).await {
        Err(MigrationError::Refused(message)) => assert!(
            message.contains("is not an ExtendDB catalog")
                && message.contains("the table accounts, which lacks the column(s) account_name")
                && message.contains("the table tables, which lacks the column(s)")
                && !message.contains("ttl_backfill_cursor"),
            "{message}"
        ),
        other => panic!("{:?}", other.err()),
    }
    assert_eq!(
        helpers_tables(&session, &catalog).await,
        ["accounts", "tables"],
        "nothing was created"
    );

    let (history, _history_drops) = new_prefix("history");
    let catalog = format!("{history}_catalog");
    keyspace(&session, &catalog).await;
    cql(
        &session,
        &format!(
            "CREATE TABLE {catalog}.schema_history (version int PRIMARY KEY, description text, \
             applied_at timestamp)"
        ),
    )
    .await;
    cql(
        &session,
        &format!(
            "INSERT INTO {catalog}.schema_history (version, description) VALUES (1, \
             'initial_schema')"
        ),
    )
    .await;
    assert_eq!(
        foreign_contents(&session, &catalog).await.expect("read"),
        Vec::<String>::new(),
        "migration 1's own row"
    );
    cql(
        &session,
        &format!(
            "INSERT INTO {catalog}.schema_history (version, description) VALUES (9, 'theirs')"
        ),
    )
    .await;
    match migrations.lock(&target_of(&history), TIMEOUT).await {
        Err(MigrationError::Refused(message)) => assert!(
            message.contains("the schema_history row of version 9")
                && !message.contains("version 1"),
            "{message}"
        ),
        other => panic!("{:?}", other.err()),
    }
}

/// `init` refuses another program's keyspace in its first step, before it
/// creates the application role, so a refused `init` leaves no role behind:
/// the keyspace of `metrics` and `tags` with a row each, as in
/// `tables_with_catalog_names_and_other_columns_or_rows_are_refused`.
#[tokio::test]
async fn init_refuses_a_foreign_keyspace_before_it_creates_its_role() {
    use extenddb_storage::bootstrapper::Bootstrapper;
    use extenddb_storage::management_store::OpError;

    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("norole");
    let catalog = format!("{prefix}_catalog");
    keyspace(&session, &catalog).await;
    for statement in [
        format!("CREATE TABLE {catalog}.metrics (id int PRIMARY KEY, reading double)"),
        format!("CREATE TABLE {catalog}.tags (name text PRIMARY KEY, color text)"),
        format!("INSERT INTO {catalog}.metrics (id, reading) VALUES (1, 2.5)"),
        format!("INSERT INTO {catalog}.tags (name, color) VALUES ('urgent', 'red')"),
    ] {
        cql(&session, &statement).await;
    }
    let role = format!("{prefix}_app");
    let bootstrapper = bootstrapper_as(&prefix, &role, "a-new-password").await;
    let refused = bootstrapper.ensure_app_user().await;
    let roles = role_rows(&session, &role).await;
    if roles > 0 {
        cql(&session, &format!("DROP ROLE IF EXISTS {role}")).await;
    }
    match refused {
        Err(OpError::AlreadyExists(message)) => assert!(
            message.contains("is not an ExtendDB catalog") && message.contains("the table metrics"),
            "{message}"
        ),
        other => panic!("{other:?}"),
    }
    assert_eq!(roles, 0, "the refused init created the role {role}");
    assert_eq!(
        helpers_tables(&session, &catalog).await,
        ["metrics", "tags"],
        "nothing was created"
    );
}

/// `migrate --yes` tells whether the keyspace is a catalog before it takes
/// the lock from the ledgers and settings alone, so the status's reads for
/// the pending migrations, which for migration 2 read every principal's
/// policies, run under the lock and inside `--lock-timeout`: with migration
/// 2 pending and the policy table gone, the lock is taken, and the status
/// under the lock is the read that fails.
#[tokio::test]
async fn the_check_before_the_lock_reads_no_policy() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("prelock");
    deployment(&session, &prefix).await;
    let migrations = CassandraCatalogMigrations;
    let target = target_of(&prefix);
    let lock = migrations.lock(&target, TIMEOUT).await.expect("lock");
    migrations
        .forget(&lock, 2)
        .await
        .expect("forget migration 2");
    migrations.release(lock).await;
    // A user, whose policies migration 2's read looks up.
    cql(
        &session,
        &format!(
            "INSERT INTO {prefix}_catalog.iam_users (account_id, user_name, user_arn, created_at) \
             VALUES ('111111111111', 'reader', 'arn:aws:iam::111111111111:user/reader', \
             toTimestamp(now()))"
        ),
    )
    .await;
    cql(
        &session,
        &format!("DROP TABLE {prefix}_catalog.iam_policies"),
    )
    .await;
    let lock = migrations
        .lock(&target, TIMEOUT)
        .await
        .expect("the lock, taken before any policy is read");
    let status = migrations.status_under_lock(&lock, &target).await;
    migrations.release(lock).await;
    match status {
        Err(e) => assert!(e.to_string().contains("iam_policies"), "{e}"),
        Ok(status) => panic!("the status under the lock read no policy: {status:?}"),
    }
}

/// The rows `system_auth.roles` holds for `role`.
async fn role_rows(session: &CassandraSession, role: &str) -> usize {
    session
        .query_with_values(
            "SELECT role FROM system_auth.roles WHERE role = ?",
            cdrs_tokio::query_values!(role),
        )
        .await
        .expect("read system_auth.roles")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default()
        .len()
}

/// The tables of `keyspace`, in name order.
async fn helpers_tables(session: &CassandraSession, keyspace: &str) -> Vec<String> {
    use cdrs_tokio::types::IntoRustByName;
    let mut names: Vec<String> = session
        .query_with_values(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
            cdrs_tokio::query_values!(keyspace),
        )
        .await
        .expect("read")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default()
        .iter()
        .map(|row| row.get_r_by_name("table_name").expect("table_name"))
        .collect();
    names.sort();
    names
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
        Some(MigrationId::LATEST.number().to_string())
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
    let _runs = ACCOUNT_RUNS.read().await;
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
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
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

/// The ids recorded in a keyspace's ledger, sorted.
async fn ledger_ids(session: &CassandraSession, keyspace: &str) -> Vec<i32> {
    use cdrs_tokio::types::IntoRustByName;
    let rows = session
        .query(format!("SELECT id FROM {keyspace}.extenddb_migrations"))
        .await
        .expect("read the ledger")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default();
    let mut ids: Vec<i32> = rows
        .iter()
        .map(|row| row.get_r_by_name("id").expect("id"))
        .collect();
    ids.sort_unstable();
    ids
}

/// CreateAccount creates and migrates the account's keyspace before it
/// writes the `accounts` row, from which the catalog's status reads the
/// account keyspaces. A call stopped before the row, as a server killed
/// there stops, leaves a complete keyspace no status counts, so the catalog
/// is still `Current`; the retried call writes the row. An account keyspace
/// whose ledger lacks migration 1, beside a catalog keyspace at migration 4,
/// is named in the status's message.
#[tokio::test]
async fn a_create_account_stopped_before_its_row_leaves_a_current_catalog_a_retry_completes() {
    use extenddb_storage::management_store::OpError;
    use extenddb_storage::migrations::advice::{Advice, status_message};

    if helpers::skip_without_cassandra() {
        return;
    }
    let _runs = ACCOUNT_RUNS.read().await;
    let session = session().await;
    let (prefix, _drops) = new_prefix("stopacct");
    keyspace(&session, &format!("{prefix}_catalog")).await;
    migrate_catalog_keyspace(session.clone(), &prefix, "test", TIMEOUT)
        .await
        .expect("migrate the catalog");
    let account = format!("{prefix}_account_111111111111");
    let store =
        || CassandraCatalogStore::new(session.clone(), prefix.clone(), "datacenter1".to_owned(), 1);

    let error = store()
        .stopping_after(0)
        .create_account("111111111111", "one")
        .await
        .expect_err("the call stops before the row");
    assert!(
        format!("{error:?}").contains("stopped by the test hook before a write"),
        "{error:?}"
    );
    assert_eq!(
        ledger_ids(&session, &account).await,
        vec![1],
        "the keyspace is created and migrated before the row"
    );
    assert_eq!(
        store().list_all_accounts().await.expect("list"),
        Vec::<(String, String)>::new(),
        "no accounts row was written"
    );
    assert_eq!(
        catalog_status(session.clone(), &prefix)
            .await
            .expect("status"),
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        },
        "the keyspace without a row is not counted"
    );

    store()
        .create_account("111111111111", "one")
        .await
        .expect("the retried call");
    assert_eq!(
        store().list_all_accounts().await.expect("list"),
        vec![("111111111111".to_owned(), "one".to_owned())]
    );
    assert_eq!(ledger_ids(&session, &account).await, vec![1]);
    assert_eq!(
        catalog_status(session.clone(), &prefix)
            .await
            .expect("status"),
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    assert!(
        matches!(
            store().create_account("111111111111", "one").await,
            Err(OpError::AlreadyExists(_))
        ),
        "a third call finds the account"
    );

    // A keyspace a run stopped in before migration 1's row.
    cql(&session, &format!("TRUNCATE {account}.extenddb_migrations")).await;
    let status = catalog_status(session.clone(), &prefix)
        .await
        .expect("status");
    assert!(
        matches!(
            status,
            CatalogStatus::Behind {
                version: Some(0),
                ..
            }
        ),
        "{status:?}"
    );
    let advice = Advice {
        release: "1.0.0-test",
        location: "test",
        backup: "back up the keyspaces",
        config: None,
        assignment: None,
    };
    let message = status_message(&status, &advice);
    assert!(
        message.starts_with(&format!(
            "account store {account} is at migration 0, behind the catalog store at migration \
             {latest}, and ExtendDB 1.0.0-test needs migration {latest}.",
            latest = MigrationId::LATEST.number()
        )),
        "{message}"
    );
    assert!(
        !message.contains("the catalog is at migration 0"),
        "{message}"
    );
}

/// `init`'s bootstrapper for the deployment of `prefix`, connected as the
/// test node's superuser, which is also the application role here, so the
/// grants `init` sends name a role that exists.
async fn bootstrapper(prefix: &str) -> extenddb_storage_cassandra::CassandraBootstrapper {
    let config = helpers::test_config();
    let user = config.username.clone().unwrap_or_default();
    let password = config.password.clone().unwrap_or_default();
    bootstrapper_as(prefix, &user, &password).await
}

/// `init`'s bootstrapper for the deployment of `prefix`, connected as the
/// test node's superuser, with `app_user` as the application role.
async fn bootstrapper_as(
    prefix: &str,
    app_user: &str,
    app_password: &str,
) -> extenddb_storage_cassandra::CassandraBootstrapper {
    let mut config = helpers::test_config();
    config.keyspace_prefix = prefix.to_owned();
    config.rebuild_cached_connection_string();
    let (host, port) = helpers::test_contact_point()
        .split_once(':')
        .map(|(h, p)| (h.to_owned(), p.parse().expect("a port")))
        .expect("host:port");
    let user = config.username.clone().unwrap_or_default();
    let password = config.password.clone().unwrap_or_default();
    let bootstrap = extenddb_storage::bootstrapper::BootstrapConfig {
        host,
        port,
        admin_user: user,
        admin_password: Some(password),
        app_user: app_user.to_owned(),
        app_password: app_password.to_owned(),
        catalog_db: format!("{prefix}_catalog"),
        data_db: String::new(),
    };
    extenddb_storage_cassandra::CassandraBootstrapper::new(&config, bootstrap)
        .await
        .unwrap_or_else(|e| panic!("bootstrapper: {e:?}"))
}

/// `init`'s default account step creates and migrates the account's
/// keyspace before it writes the `accounts` row, and a step run again on a
/// catalog that has an account creates no second account and completes that
/// account's keyspace: an `init` resumed after the step leaves one account
/// whose keyspace holds every account migration.
#[tokio::test]
async fn the_default_account_step_run_again_keeps_one_account_and_completes_its_keyspace() {
    use extenddb_storage::bootstrapper::Bootstrapper;

    if helpers::skip_without_cassandra() {
        return;
    }
    let _runs = ACCOUNT_RUNS.read().await;
    let session = session().await;
    let (prefix, mut drops) = new_prefix("defacct");
    keyspace(&session, &format!("{prefix}_catalog")).await;
    migrate_catalog_keyspace(session.clone(), &prefix, "test", TIMEOUT)
        .await
        .expect("migrate the catalog");
    let bootstrapper = bootstrapper(&prefix).await;
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.clone());

    bootstrapper
        .bootstrap_default_account()
        .await
        .expect("the default account");
    let accounts = store.list_all_accounts().await.expect("list");
    assert_eq!(accounts.len(), 1, "{accounts:?}");
    let (id, name) = accounts[0].clone();
    assert_eq!(name, "default");
    let account = format!("{prefix}_account_{id}");
    drops.0.push(account.clone());
    assert_eq!(ledger_ids(&session, &account).await, vec![1]);

    // The step again, as a resumed `init` runs it.
    bootstrapper
        .bootstrap_default_account()
        .await
        .expect("the step again");
    assert_eq!(
        store.list_all_accounts().await.expect("list"),
        vec![(id.clone(), "default".to_owned())],
        "no second account"
    );

    // A keyspace that lost its migrations is completed by the step.
    cql(&session, &format!("DROP KEYSPACE {account}")).await;
    bootstrapper
        .bootstrap_default_account()
        .await
        .expect("the step on a missing keyspace");
    assert_eq!(ledger_ids(&session, &account).await, vec![1]);
    assert_eq!(
        store.list_all_accounts().await.expect("list"),
        vec![(id, "default".to_owned())]
    );
    assert_eq!(
        catalog_status(session.clone(), &prefix)
            .await
            .expect("status"),
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
}

/// An `init` killed after its default account, before its admin user,
/// leaves a catalog whose status is `Current` and that has no admin user,
/// since the account's keyspace is migrated before its row. The next
/// `init`, a new process, takes the keyspace as `init` does, and its steps
/// under the lock run no migration, keep the key, create no second account
/// and write the admin user; the `init` after that is refused as
/// initialized.
#[tokio::test]
async fn init_finishes_a_catalog_stopped_before_its_admin_user() {
    use extenddb_storage::bootstrapper::{
        Bootstrapper, finishes_a_stopped_init, write_bootstrap_rows,
    };
    use extenddb_storage::migrations::run_with_status;

    if helpers::skip_without_cassandra() {
        return;
    }
    // Both inits run the default account's migrations under the lock's
    // lease, so no other test's account run may be in flight.
    let _runs = ACCOUNT_RUNS.write().await;
    let session = session().await;
    let (prefix, mut drops) = new_prefix("finish");
    let migrations = CassandraCatalogMigrations;
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.clone());

    // The stopped init.
    let first = bootstrapper(&prefix).await;
    let target = first.init_target();
    first
        .create_catalog_db()
        .await
        .expect("the catalog keyspace");
    let lock = migrations.lock(&target, TIMEOUT).await.expect("lock");
    let status = migrations
        .status_under_lock(&lock, &target)
        .await
        .expect("status");
    run_with_status(&migrations, &status, &lock, "test", RunKind::Create)
        .await
        .expect("create");
    first.hold(&lock);
    first.bootstrap_encryption_key().await.expect("key");
    first.bootstrap_default_account().await.expect("account");
    migrations.release(lock).await;
    let key = store.get_setting("encryption_key").await.expect("read");
    let accounts = store.list_all_accounts().await.expect("list");
    assert_eq!(accounts.len(), 1, "{accounts:?}");
    drops.0.push(format!("{prefix}_account_{}", accounts[0].0));
    // `first` holds the lease of the lock it released, so its requests are
    // refused; a bootstrapper of its own reads the admin users.
    assert!(
        !bootstrapper(&prefix)
            .await
            .has_admin_user()
            .await
            .expect("read")
    );

    // The next init finishes it.
    let next = bootstrapper(&prefix).await;
    next.create_catalog_db()
        .await
        .expect("init takes the existing catalog keyspace");
    let lock = migrations.lock(&target, TIMEOUT).await.expect("lock");
    let status = migrations
        .status_under_lock(&lock, &target)
        .await
        .expect("status");
    assert_eq!(
        status,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    assert!(
        finishes_a_stopped_init(&next, &status)
            .await
            .expect("decided")
    );
    next.hold(&lock);
    let admin = write_bootstrap_rows(&next, None, None).await.expect("rows");
    next.record_data_connection()
        .await
        .expect("data connection");
    migrations.release(lock).await;
    assert_eq!(admin.username, "admin");
    assert!(!admin.already_existed && admin.generated_password.is_some());
    assert_eq!(
        store.get_setting("encryption_key").await.expect("read"),
        key
    );
    assert_eq!(store.list_all_accounts().await.expect("list"), accounts);

    // The init after that is refused.
    let last = bootstrapper(&prefix).await;
    let lock = migrations.lock(&target, TIMEOUT).await.expect("lock");
    let status = migrations
        .status_under_lock(&lock, &target)
        .await
        .expect("status");
    assert!(
        !finishes_a_stopped_init(&last, &status)
            .await
            .expect("decided")
    );
    let refused = run_with_status(&migrations, &status, &lock, "test", RunKind::Create)
        .await
        .expect_err("refused");
    migrations.release(lock).await;
    assert!(
        refused
            .to_string()
            .starts_with("the catalog is already initialized"),
        "{refused}"
    );
    drop(drops);
    helpers::flush_pending_drops(&session).await;
}
