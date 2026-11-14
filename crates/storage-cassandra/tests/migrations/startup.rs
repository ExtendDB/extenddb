// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The startup policy `serve` runs before it opens the catalog
//! (`[server] catalog_upgrade`, or `serve --migrate`): under `apply` a
//! legacy catalog is migrated through the lock `migrate --yes` takes, a
//! current one is served with nothing written, one that needs a person
//! exits 5 with nothing written, a held lock makes the server wait and then
//! exit 6, and two `prepare_to_serve` runs that wait for the lock together,
//! whose connection strings name the two nodes of a two-node cluster when
//! the job has one, migrate once.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use extenddb_storage::management_store::{ManagementStore, SettingsStore};
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, LedgerRow, MigrationError, MigrationId, Prepared, Startup,
    StoreKind, Target, exit, prepare_to_serve,
};
use extenddb_storage_cassandra::CassandraCatalogStore;
use extenddb_storage_cassandra::cassandra_util::CassandraSession;
use extenddb_storage_cassandra::migrations::{
    CassandraCatalogMigrations, LeaseTimes, MigrationLock, catalog_status, run_baseline_unrecorded,
};

use super::{deployment, target_of};
use crate::helpers;
use crate::migrations_support::{TIMEOUT, cql, keyspace, new_prefix, pinned_session, session};

const ACCOUNT: &str = "111111111111";

fn startup(apply: bool, lock_timeout: Duration) -> Startup<'static> {
    Startup {
        apply,
        creates_uninitialized: false,
        release: "1.0.0-test",
        lock_timeout,
        heartbeat: Startup::HEARTBEAT,
    }
}

/// The policy as `serve` runs it, on the connection string the config
/// file gives.
async fn prepare(target: &Target, startup: &Startup<'_>) -> Result<Prepared, MigrationError> {
    prepare_to_serve(&CassandraCatalogMigrations, target, startup, &|_| {}).await
}

/// The node `EXTENDDB_TEST_CASSANDRA_SECOND_CONTACT_POINT` names, the second
/// node of a two-node cluster; the test node when it is not set.
fn second_contact_point() -> String {
    std::env::var("EXTENDDB_TEST_CASSANDRA_SECOND_CONTACT_POINT")
        .unwrap_or_else(|_| helpers::test_contact_point())
}

/// [`target_of`] with [`second_contact_point`] as its contact point.
fn second_target_of(prefix: &str) -> Target {
    let config = helpers::test_config();
    Target::at(format!(
        "{}:{}@{}/{prefix}",
        config.username.unwrap_or_default(),
        config.password.unwrap_or_default(),
        second_contact_point(),
    ))
}

/// A catalog as a release before 1.0 left it: migration 1's tables with no
/// ledger rows, and `catalog_version` 0.0.4.
async fn legacy_catalog(session: &Arc<CassandraSession>, prefix: &str) {
    let catalog = format!("{prefix}_catalog");
    keyspace(session, &catalog).await;
    run_baseline_unrecorded(session, &catalog, StoreKind::Catalog)
        .await
        .expect("the legacy tables");
    cql(
        session,
        &format!("INSERT INTO {catalog}.settings (key, value) VALUES ('catalog_version', '0.0.4')"),
    )
    .await;
}

/// What a run could change in the catalog: its ledger rows with the time
/// each was applied, its `catalog_version` row, and the lock row.
async fn state(
    session: &Arc<CassandraSession>,
    prefix: &str,
) -> (Vec<LedgerRow>, Option<String>, Option<String>) {
    let rows = CassandraCatalogMigrations
        .catalog_ledger(&target_of(prefix))
        .await
        .expect("the ledger");
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.to_owned());
    let version = store.get_setting("catalog_version").await.expect("read");
    let lock = store.get_setting("migration_lock").await.expect("read");
    (rows, version, lock)
}

#[tokio::test]
async fn apply_migrates_a_legacy_catalog_and_refuse_writes_nothing() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("start_apply");
    legacy_catalog(&session, &prefix).await;
    let target = target_of(&prefix);
    let before = state(&session, &prefix).await;
    let err = prepare(&target, &startup(false, TIMEOUT))
        .await
        .expect_err("refused");
    assert_eq!(err.exit_code(), exit::PENDING, "{err:?}");
    assert_eq!(state(&session, &prefix).await, before, "refuse wrote");

    let prepared = prepare(&target, &startup(true, TIMEOUT))
        .await
        .expect("applied");
    let report = prepared.run.expect("a run");
    assert_eq!(report.applied.len(), MigrationId::ALL.len(), "{report:?}");
    assert_eq!(
        catalog_status(session.clone(), &prefix)
            .await
            .expect("status"),
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
}

#[tokio::test]
async fn apply_on_a_current_catalog_writes_nothing() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("start_current");
    deployment(&session, &prefix).await;
    let before = state(&session, &prefix).await;
    let prepared = prepare(&target_of(&prefix), &startup(true, TIMEOUT))
        .await
        .expect("served");
    assert_eq!(prepared, Prepared::default());
    assert_eq!(
        state(&session, &prefix).await,
        before,
        "a current catalog was written to"
    );
}

#[tokio::test]
async fn apply_on_a_catalog_that_needs_attention_exits_5_and_writes_nothing() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("start_attention");
    legacy_catalog(&session, &prefix).await;
    cql(
        &session,
        &format!(
            "INSERT INTO {prefix}_catalog.accounts (account_id, account_name, created_at) VALUES \
             ('{ACCOUNT}', 'one', toTimestamp(now()))"
        ),
    )
    .await;
    // A policy migration 2 cannot rewrite: both forms of one tag key, with
    // different values.
    let colliding: serde_json::Value = serde_json::from_str(
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":"prod","dynamodb:ResourceTag/env":"dev"}}}]}"#,
    )
    .expect("json");
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.clone());
    store.create_user(ACCOUNT, "dev", None).await.expect("user");
    store
        .put_policy(ACCOUNT, "user", "dev", "colliding", &colliding)
        .await
        .expect("policy");
    let target = target_of(&prefix);
    let before = state(&session, &prefix).await;
    let err = prepare(&target, &startup(true, TIMEOUT))
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
    assert_eq!(
        state(&session, &prefix).await,
        before,
        "a refused run wrote"
    );
}

/// Another process holds the migration lock: the server waits for it up
/// to the timeout, then exits 6 with the catalog unchanged.
#[tokio::test]
async fn apply_waits_for_a_held_migration_lock_and_exits_6() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("start_locked");
    legacy_catalog(&session, &prefix).await;
    let holder = MigrationLock::acquire(
        session.clone(),
        &format!("{prefix}_catalog"),
        LeaseTimes::DEFAULT,
        TIMEOUT,
    )
    .await
    .expect("another process's lock");
    let before = state(&session, &prefix).await;
    let started = Instant::now();
    let err = prepare(&target_of(&prefix), &startup(true, Duration::from_secs(3)))
        .await
        .expect_err("locked");
    assert!(
        started.elapsed() >= Duration::from_millis(2800),
        "it waited {:?}",
        started.elapsed()
    );
    assert!(matches!(err, MigrationError::Locked(_)), "{err:?}");
    assert_eq!(err.exit_code(), exit::LOCKED);
    assert_eq!(
        state(&session, &prefix).await,
        before,
        "a server that waited wrote"
    );
    holder.release().await;
}

/// Two `prepare_to_serve` runs under `apply` on a legacy catalog, their
/// connection strings naming different nodes when the job has two, both
/// waiting for the lock another process holds, as the checks of two servers
/// started together do after their first status read: once it is free, one
/// migrates the catalog, and the other takes the lock after it and applies
/// nothing. Each run's session reaches the cluster at the node its
/// connection string names and then sends requests to both nodes in turn,
/// as a server's session does. The servers are not started here; one of two
/// servers started together can exit 6 when it registers while the other
/// holds the lock.
#[tokio::test]
async fn two_prepare_to_serve_runs_under_apply_migrate_a_legacy_catalog_once() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("start_two");
    legacy_catalog(&session, &prefix).await;
    let holder = MigrationLock::acquire(
        session.clone(),
        &format!("{prefix}_catalog"),
        LeaseTimes::DEFAULT,
        TIMEOUT,
    )
    .await
    .expect("another process's lock");
    let waiting = AtomicUsize::new(0);
    let progress = |line: &str| {
        if line.starts_with("taking the migration lock") {
            waiting.fetch_add(1, Ordering::SeqCst);
        }
    };
    // Each run waits for the holder, and the second also for the first
    // one's migration.
    let apply = startup(true, TIMEOUT * 2);
    let (first, second) = (target_of(&prefix), second_target_of(&prefix));
    let free = async {
        while waiting.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        holder.release().await;
    };
    let (a, b, ()) = tokio::join!(
        prepare_to_serve(&CassandraCatalogMigrations, &first, &apply, &progress),
        prepare_to_serve(&CassandraCatalogMigrations, &second, &apply, &progress),
        free,
    );
    let mut applied: Vec<usize> = [a.expect("first"), b.expect("second")]
        .iter()
        .map(|p| p.run.as_ref().expect("each took the lock").applied.len())
        .collect();
    applied.sort_unstable();
    assert!(
        applied[0] == 0 && applied[1] > 0,
        "one migrates, and the other takes the lock after it and applies nothing: {applied:?}"
    );
    assert_eq!(
        catalog_status(pinned_session(&second_contact_point()).await, &prefix)
            .await
            .expect("status"),
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
}
