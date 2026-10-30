// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The server factory reads the catalog status before it opens the
//! database: it serves only `Current` and `AheadCompatible` and writes
//! nothing when it refuses; a dev build creates a missing or empty
//! database first; an in-memory database is created by the runner every
//! time. The startup policy `serve` applies before the factory, which
//! migrates a catalog behind the binary under `catalog_upgrade = "apply"`,
//! and the lock that keeps a migrator, a second migrator and a server
//! apart are tested here too.

use std::path::Path;
use std::time::Duration;

use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, MigrationError, MigrationId, Prepared, Startup, Target, exit,
    prepare_to_serve,
};
use extenddb_storage::server_components::{
    BackendError, ServerComponents, ServerComponentsOptions,
};

use super::tests::{
    DEV_0_1_14, LATEST, RELEASE, copies, exec, load_dump, plain_pool, run, status, temp_db,
};
use super::{SqliteCatalogMigrations, migrate};

const COLLIDING: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":"prod","dynamodb:ResourceTag/env":"dev"}}}]}"#;

fn options(dev: bool) -> ServerComponentsOptions {
    let mut options = ServerComponentsOptions::default();
    options.bootstrap_if_uninitialized = dev;
    options.release = "1.0.0-test";
    options
}

async fn start(path: &str, dev: bool) -> Result<ServerComponents, BackendError> {
    let config = crate::config::SqliteConfig {
        path: path.to_owned(),
        pool_size: 2,
    };
    crate::sqlite_server_components_factory(&config, "us-east-1", options(dev)).await
}

/// The refusal a factory returned, with its exit code.
fn refusal(result: Result<ServerComponents, BackendError>) -> MigrationError {
    match result {
        Err(BackendError::Catalog(e)) => e,
        Err(other) => panic!("a refusal with an exit code, not {other}"),
        Ok(_) => panic!("the server started"),
    }
}

/// A current catalog as `init` leaves it: every migration, and the
/// encryption key a server reads.
async fn current_catalog(db: &Path) -> String {
    let location = db.to_string_lossy().into_owned();
    super::migrate(&location, "1.0.0-test", Duration::from_secs(5))
        .await
        .expect("a current catalog");
    exec(
        db,
        "INSERT INTO settings (key, value) VALUES \
         ('encryption_key', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=')",
    )
    .await;
    location
}

fn bytes(db: &Path) -> Vec<u8> {
    std::fs::read(db).expect("read the database")
}

#[tokio::test]
async fn a_production_server_refuses_a_missing_catalog_and_creates_no_database() {
    let db = temp_db("serve-missing");
    let err = refusal(start(&db.to_string_lossy(), false).await);
    assert_eq!(err, MigrationError::Status(CatalogStatus::Uninitialized));
    assert_eq!(err.exit_code(), exit::ERROR);
    assert!(!db.exists(), "a refusing server created the database");
}

/// A dev server creates a catalog in a missing or empty file, and refuses a
/// file that holds another program's tables as `migrate --yes` does,
/// naming `extenddb init`, with the file left as it was.
#[tokio::test]
async fn a_dev_server_refuses_a_file_that_is_not_a_catalog() {
    let db = temp_db("serve-dev-foreign");
    exec(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    let before = bytes(&db);
    let err = refusal(start(&db.to_string_lossy(), true).await);
    assert_eq!(err, MigrationError::Status(CatalogStatus::Uninitialized));
    assert_eq!(bytes(&db), before, "the dev server wrote to the file");
    assert!(copies(&db).is_empty());

    let empty = temp_db("serve-dev-empty");
    std::fs::write(&empty, b"").expect("an empty file");
    let components = start(&empty.to_string_lossy(), true)
        .await
        .expect("a dev server creates the catalog in an empty file");
    drop(components);
    assert_eq!(
        status(&empty).await,
        CatalogStatus::Current { version: LATEST }
    );
}

#[tokio::test]
async fn a_production_server_refuses_a_released_catalog_and_writes_nothing() {
    let db = temp_db("serve-released");
    load_dump(&db, DEV_0_1_14).await;
    let before = bytes(&db);
    let err = refusal(start(&db.to_string_lossy(), false).await);
    let MigrationError::Status(CatalogStatus::Behind { version, pending }) = &err else {
        panic!("{err:?}");
    };
    assert_eq!(*version, None);
    assert_eq!(pending.len(), MigrationId::ALL.len());
    assert_eq!(err.exit_code(), exit::PENDING);
    assert_eq!(
        bytes(&db),
        before,
        "a refusing server wrote to the database"
    );
    assert!(copies(&db).is_empty());
}

/// A dev build defaults to `catalog_upgrade = "apply"`: `serve` migrates a
/// released catalog through the startup policy, copy included, and the
/// factory then serves it. The factory alone refuses it, as in a
/// production build, so the policy is the one place that migrates.
#[tokio::test]
async fn a_dev_server_migrates_a_released_catalog_with_its_copy_and_serves() {
    let db = temp_db("serve-dev-released");
    load_dump(&db, DEV_0_1_14).await;
    let before = bytes(&db);
    let err = refusal(start(&db.to_string_lossy(), true).await);
    assert_eq!(err.exit_code(), exit::PENDING, "{err:?}");
    assert_eq!(bytes(&db), before, "the factory wrote to the database");
    let dev = Startup {
        creates_uninitialized: true,
        ..startup(true, Duration::from_secs(5))
    };
    let prepared = prepare(&db, &dev).await.expect("the policy migrates");
    assert!(!prepared.run.expect("a run").applied.is_empty());
    let components = start(&db.to_string_lossy(), true)
        .await
        .expect("a dev server starts");
    drop(components);
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
    assert_eq!(
        copies(&db).len(),
        1,
        "the dev build copies before migrating"
    );
}

fn startup(apply: bool, lock_timeout: Duration) -> Startup<'static> {
    Startup {
        apply,
        creates_uninitialized: false,
        release: RELEASE,
        lock_timeout,
        heartbeat: Startup::HEARTBEAT,
    }
}

/// The startup policy `serve` runs before the factory, on the file at `db`.
async fn prepare(db: &Path, startup: &Startup<'_>) -> Result<Prepared, MigrationError> {
    let target = Target::at(db.to_string_lossy());
    prepare_to_serve(&SqliteCatalogMigrations, &target, startup, &|_| {}).await
}

/// `apply` migrates a released catalog before the server opens it, taking
/// the copy first, and a production server then serves it; `refuse` exits
/// 3 and writes nothing.
#[tokio::test]
async fn apply_migrates_a_released_catalog_before_the_server_opens_it() {
    let db = temp_db("startup-apply");
    load_dump(&db, DEV_0_1_14).await;
    let before = bytes(&db);
    let err = prepare(&db, &startup(false, Duration::from_secs(5)))
        .await
        .expect_err("refused");
    assert_eq!(err.exit_code(), exit::PENDING, "{err:?}");
    assert_eq!(bytes(&db), before, "refuse wrote to the database");
    assert!(copies(&db).is_empty());

    let prepared = prepare(&db, &startup(true, Duration::from_secs(5)))
        .await
        .expect("applied");
    let report = prepared.run.expect("a run");
    assert_eq!(report.from, None);
    assert_eq!(report.to, Some(LATEST));
    assert_eq!(report.applied.len(), MigrationId::ALL.len());
    assert_eq!(copies(&db).len(), 1, "the copy is taken before the run");
    drop(
        start(&db.to_string_lossy(), false)
            .await
            .expect("a production server serves the migrated catalog"),
    );
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
}

#[tokio::test]
async fn apply_on_a_current_catalog_writes_nothing() {
    let db = temp_db("startup-current");
    current_catalog(&db).await;
    let before = bytes(&db);
    let prepared = prepare(&db, &startup(true, Duration::from_secs(5)))
        .await
        .expect("served");
    assert_eq!(prepared, Prepared::default());
    assert_eq!(bytes(&db), before, "a current catalog was written to");
    assert!(copies(&db).is_empty());
}

#[tokio::test]
async fn apply_on_a_catalog_that_needs_attention_exits_5_and_writes_nothing() {
    let db = temp_db("startup-attention");
    load_dump(&db, DEV_0_1_14).await;
    collide(&db).await;
    let before = bytes(&db);
    let err = prepare(&db, &startup(true, Duration::from_secs(5)))
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
    assert_eq!(bytes(&db), before);
    assert!(copies(&db).is_empty());
}

/// Another process holds the database's lock: `apply` waits for it up to
/// the timeout, then exits 6 with the catalog still behind and unchanged.
#[cfg(unix)]
#[tokio::test]
async fn apply_waits_for_a_held_lock_and_exits_6() {
    let db = temp_db("startup-locked");
    load_dump(&db, DEV_0_1_14).await;
    let before = bytes(&db);
    let holder = crate::serve_lock::ServeLock::acquire(&db).expect("another process's lock");
    let started = std::time::Instant::now();
    let err = prepare(&db, &startup(true, Duration::from_millis(800)))
        .await
        .expect_err("locked");
    assert!(
        started.elapsed() >= Duration::from_millis(700),
        "it waited {:?}",
        started.elapsed()
    );
    assert!(matches!(err, MigrationError::Locked(_)), "{err:?}");
    assert_eq!(err.exit_code(), exit::LOCKED);
    assert_eq!(bytes(&db), before);
    assert!(copies(&db).is_empty());
    drop(holder);
}

/// Two servers started together under `apply` on a released catalog: one
/// migrates it, and the other waits for the lock and finds nothing to do.
#[tokio::test]
async fn two_servers_started_together_with_apply_migrate_once() {
    let db = temp_db("startup-two");
    load_dump(&db, DEV_0_1_14).await;
    let apply = startup(true, Duration::from_secs(30));
    let (a, b) = tokio::join!(prepare(&db, &apply), prepare(&db, &apply));
    let applied: Vec<usize> = [a.expect("first"), b.expect("second")]
        .iter()
        .map(|p| p.run.as_ref().map_or(0, |r| r.applied.len()))
        .collect();
    assert!(
        applied.contains(&0) && applied.contains(&MigrationId::ALL.len()),
        "one migrates and the other finds nothing to do: {applied:?}"
    );
    assert_eq!(copies(&db).len(), 1);
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
}

/// An in-memory database belongs to the server that opens it: the policy
/// reads nothing, and the server creates the catalog.
#[tokio::test]
async fn an_in_memory_database_is_left_to_its_server() {
    let target = Target::at(":memory:");
    assert!(SqliteCatalogMigrations.in_process(&target));
    assert!(!SqliteCatalogMigrations.in_process(&Target::at("/srv/x.sqlite")));
    let prepared = prepare_to_serve(
        &SqliteCatalogMigrations,
        &target,
        &startup(true, Duration::from_secs(5)),
        &|_| {},
    )
    .await
    .expect("left to the server");
    assert_eq!(prepared, Prepared::default());
}

#[tokio::test]
async fn a_dev_server_creates_a_missing_catalog_and_its_rows() {
    let db = temp_db("serve-dev-missing");
    let components = start(&db.to_string_lossy(), true)
        .await
        .expect("a dev server creates the catalog");
    let key = components
        .catalog_store
        .get_setting("encryption_key")
        .await
        .expect("read");
    assert!(key.is_some(), "the dev rows are written");
    drop(components);
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
    assert!(copies(&db).is_empty(), "a new database has nothing to copy");
}

/// Give a released catalog's first IAM user a policy migration 2 cannot
/// rewrite, so its status is `NeedsAttention`.
async fn collide(db: &Path) {
    let pool = plain_pool(db).await;
    let (account, user): (String, String) =
        sqlx::query_as("SELECT account_id, user_name FROM iam_users LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("an IAM user");
    sqlx::query(
        "INSERT INTO iam_policies (account_id, principal_type, principal_name, policy_name, \
         policy_document) VALUES (?, 'user', ?, 'colliding', ?)",
    )
    .bind(&account)
    .bind(&user)
    .bind(COLLIDING)
    .execute(&pool)
    .await
    .expect("policy");
    pool.close().await;
}

#[tokio::test]
async fn a_dev_server_refuses_a_catalog_that_needs_attention() {
    let db = temp_db("serve-dev-attention");
    load_dump(&db, DEV_0_1_14).await;
    collide(&db).await;
    let before = bytes(&db);
    let err = refusal(start(&db.to_string_lossy(), true).await);
    assert!(
        matches!(
            err,
            MigrationError::Status(CatalogStatus::NeedsAttention { .. })
        ),
        "{err:?}"
    );
    assert_eq!(err.exit_code(), exit::ATTENTION);
    assert_eq!(bytes(&db), before);
    assert!(copies(&db).is_empty());
}

#[tokio::test]
async fn a_server_refuses_while_a_migration_holds_the_database() {
    let db = temp_db("serve-locked");
    let location = current_catalog(&db).await;
    let lock = super::lock_for_migration(&location, Duration::ZERO)
        .await
        .expect("the migration lock");
    let err = refusal(start(&location, false).await);
    assert!(matches!(err, MigrationError::Locked(_)), "{err:?}");
    assert_eq!(err.exit_code(), exit::LOCKED);
    drop(lock);
    drop(
        start(&location, false)
            .await
            .expect("starts once the lock is free"),
    );
}

#[tokio::test]
async fn a_server_serves_a_catalog_ahead_by_additive_migrations_only() {
    let db = temp_db("serve-ahead");
    let location = current_catalog(&db).await;
    exec(
        &db,
        &format!(
            "INSERT INTO extenddb_migrations (id, name, kind, checksum, outcome, notes, \
             applied_by) VALUES ({}, 'later', 'additive', 'x', 'applied', '', 'later')",
            LATEST + 1
        ),
    )
    .await;
    drop(
        start(&location, false)
            .await
            .expect("an additive catalog serves"),
    );
    exec(
        &db,
        &format!(
            "UPDATE extenddb_migrations SET kind = 'breaking' WHERE id = {}",
            LATEST + 1
        ),
    )
    .await;
    let err = refusal(start(&location, false).await);
    assert_eq!(err.exit_code(), exit::AHEAD_INCOMPATIBLE, "{err:?}");
}

#[tokio::test]
async fn an_in_memory_server_creates_its_catalog_through_the_runner() {
    let components = start(":memory:", false)
        .await
        .expect("an in-memory server starts");
    let version = components
        .catalog_store
        .get_setting("catalog_version")
        .await
        .expect("read");
    assert_eq!(version, Some(LATEST.to_string()));
}

#[tokio::test]
async fn two_migrators_apply_once() {
    let db = temp_db("two");
    load_dump(&db, DEV_0_1_14).await;
    let (a, b) = tokio::join!(run(&db), run(&db));
    let (a, b) = (a.expect("first"), b.expect("second"));
    assert_eq!(
        [a.applied.is_empty(), b.applied.is_empty()]
            .iter()
            .filter(|e| **e)
            .count(),
        1,
        "one applies and the other finds nothing to do: {a:?} {b:?}"
    );
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );
    assert_eq!(copies(&db).len(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn the_lock_is_retried_and_times_out_with_exit_6() {
    let db = temp_db("lock");
    run(&db).await.expect("fresh");
    let server = crate::serve_lock::ServeLock::acquire(&db).expect("server lock");
    let started = std::time::Instant::now();
    let err = super::lock_for_migration(&db.to_string_lossy(), Duration::from_millis(600))
        .await
        .expect_err("held");
    assert!(started.elapsed() >= Duration::from_millis(500), "it waited");
    assert_eq!(err.exit_code(), exit::LOCKED);
    // Released while a migrator waits: the migrator proceeds.
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        drop(server);
    });
    super::lock_for_migration(&db.to_string_lossy(), Duration::from_secs(5))
        .await
        .expect("taken after the server stops")
        .expect("a file database");
    release.await.expect("release");
}

/// The server's factory holds the serve lock before it opens the engine; a
/// second `flock` from the same process is refused, so it migrates through
/// the entry point that takes the held lock.
#[cfg(unix)]
#[tokio::test]
async fn a_caller_holding_the_serve_lock_migrates_through_migrate_holding() {
    let db = temp_db("holding");
    load_dump(&db, DEV_0_1_14).await;
    let location = db.to_string_lossy().into_owned();
    let held = crate::serve_lock::ServeLock::acquire(&db).expect("serve lock");
    let err = migrate(&location, RELEASE, Duration::from_millis(300))
        .await
        .expect_err("the lock is this process's own");
    assert_eq!(err.exit_code(), exit::LOCKED);
    let report = super::migrate_holding(&location, RELEASE, &held)
        .await
        .expect("migrate holding the lock");
    assert_eq!(report.to, Some(LATEST));
    assert_eq!(
        status(&db).await,
        CatalogStatus::Current { version: LATEST }
    );

    // Another database's lock is not this one's.
    let other = temp_db("holding-other");
    let other_lock = crate::serve_lock::ServeLock::acquire(&other).expect("other lock");
    let refused = super::migrate_holding(&location, RELEASE, &other_lock).await;
    assert!(
        matches!(refused, Err(MigrationError::Refused(_))),
        "{refused:?}"
    );
    drop(held);
}
