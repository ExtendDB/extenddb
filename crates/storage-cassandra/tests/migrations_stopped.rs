// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Runs stopped part way, as a migrator killed at that point stops, and the
//! run after each, which finishes with the definitions of a fresh install;
//! and migration 2's rewrite of a released catalog's policies, compared with
//! a committed dump. Each test uses keyspaces of its own prefix and drops
//! them. Skipped without a node unless `EXTENDDB_TEST_CASSANDRA=required`.
//!
//! With `EXTENDDB_BLESS_EXPECTED=1`, the migration 2 test writes
//! `tests/fixtures/golden/0002_resource_tag_condition_keys.txt` instead of
//! comparing.

#[path = "common/mod.rs"]
mod helpers;
mod migrations_support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::management_store::{ManagementStore, SettingsStore};
use extenddb_storage::migrations::{CatalogStatus, MigrationId, Runner, StoreKind};
use extenddb_storage_cassandra::CassandraCatalogStore;
use extenddb_storage_cassandra::cassandra_util::CassandraSession;
use extenddb_storage_cassandra::migrations::{
    CassandraMigrations, LeaseTimes, MigrationLock, catalog_status, create_lock_table, postcheck,
    run_baseline_unrecorded, status_holding,
};
use migrations_support::{TIMEOUT, cql, keyspace, new_prefix, session};

const ACCOUNT: &str = "111111111111";

const TAGGED: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"dynamodb:*","Resource":"*","Condition":{"StringEquals":{"dynamodb:ResourceTag/env":"prod"}}}]}"#;

/// The catalog keyspace as `init` leaves it before it takes the lock: the
/// keyspace and its `settings` table.
async fn fresh_catalog(session: &Arc<CassandraSession>, prefix: &str) {
    let catalog = format!("{prefix}_catalog");
    keyspace(session, &catalog).await;
    create_lock_table(session, &catalog)
        .await
        .expect("the settings table");
}

/// A catalog as a release before 1.0 left it: the tables migration 1
/// converges and no ledger, `catalog_version` 0.0.4 and the resource-tag
/// marker, one account with its keyspace, and a policy holding a
/// `dynamodb:ResourceTag` key in each of the six places migration 2
/// rewrites: user, group and role inline policies, user and role
/// permissions boundaries, and a session policy.
async fn released_catalog(session: &Arc<CassandraSession>, prefix: &str) {
    let catalog = format!("{prefix}_catalog");
    let account = format!("{prefix}_account_{ACCOUNT}");
    for (name, kind) in [
        (&catalog, StoreKind::Catalog),
        (&account, StoreKind::Account),
    ] {
        keyspace(session, name).await;
        run_baseline_unrecorded(session, name, kind)
            .await
            .expect("the legacy tables");
    }
    for (key, value) in [
        ("catalog_version", "0.0.4"),
        ("resource_tag_migration", "applied"),
    ] {
        cql(
            session,
            &format!("INSERT INTO {catalog}.settings (key, value) VALUES ('{key}', '{value}')"),
        )
        .await;
    }
    cql(
        session,
        &format!(
            "INSERT INTO {catalog}.accounts (account_id, account_name, created_at) VALUES \
             ('{ACCOUNT}', 'one', toTimestamp(now()))"
        ),
    )
    .await;
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.to_owned());
    let tagged: serde_json::Value = serde_json::from_str(TAGGED).expect("json");
    store.create_user(ACCOUNT, "dev", None).await.expect("user");
    store.create_group(ACCOUNT, "devs").await.expect("group");
    store
        .create_role(ACCOUNT, "app", &serde_json::json!({}))
        .await
        .expect("role");
    for (principal_type, name, policy) in [
        ("user", "dev", "tagged-user"),
        ("group", "devs", "tagged-group"),
        ("role", "app", "tagged-role"),
    ] {
        store
            .put_policy(ACCOUNT, principal_type, name, policy, &tagged)
            .await
            .expect("policy");
    }
    store
        .set_user_boundary(ACCOUNT, "dev", &tagged)
        .await
        .expect("user boundary");
    store
        .set_role_boundary(ACCOUNT, "app", &tagged)
        .await
        .expect("role boundary");
    store
        .store_session(
            "token-1",
            "ASIAFIXTURE0001",
            &[0],
            ACCOUNT,
            "app",
            "session",
            &None,
            &Some(tagged),
            time::OffsetDateTime::now_utc() + time::Duration::days(1),
        )
        .await
        .expect("session");
}

/// Add `count` IAM users to [`ACCOUNT`], each with one inline policy holding
/// a `dynamodb:ResourceTag` key, 32 users at a time.
async fn add_tagged_users(store: &CassandraCatalogStore, count: usize) {
    use futures::StreamExt;
    let tagged: serde_json::Value = serde_json::from_str(TAGGED).expect("json");
    futures::stream::iter(0..count)
        .map(|i| {
            let tagged = &tagged;
            async move {
                let name = format!("u{i:05}");
                store.create_user(ACCOUNT, &name, None).await.expect("user");
                store
                    .put_policy(ACCOUNT, "user", &name, "tagged", tagged)
                    .await
                    .expect("policy");
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<()>>()
        .await;
}

/// Take the migration lock and run the pending migrations, as `init` and
/// `migrate --yes` do, with the runner stopped before write `requests`
/// (`usize::MAX` for a run that is not stopped), then release the lock.
/// `catalog_only` leaves out the account keyspaces, and `through` stops
/// after that migration. Returns the run's error, if any, and how many
/// writes its budget had left. A killed holder's lock row stays until it
/// expires, which the lease test in tests/migrations.rs measures; here the
/// lock is released so the next run starts at once.
async fn run_stopped_after(
    session: &Arc<CassandraSession>,
    prefix: &str,
    requests: usize,
    catalog_only: bool,
    through: MigrationId,
) -> (Result<(), String>, usize) {
    let catalog = format!("{prefix}_catalog");
    let lock = MigrationLock::acquire(session.clone(), &catalog, LeaseTimes::DEFAULT, TIMEOUT)
        .await
        .expect("take the lock");
    let (result, left) = {
        let migrations = if catalog_only {
            CassandraMigrations::locked_catalog_only(&lock, prefix)
        } else {
            CassandraMigrations::locked(&lock, prefix)
        }
        .stop_after(requests);
        let result = Runner::new(&migrations, "test")
            .migrate_through(through)
            .await
            .map(drop)
            .map_err(|e| e.to_string());
        (result, migrations.requests_left().unwrap_or_default())
    };
    lock.release().await;
    (result, left)
}

/// The deployment is `Current` at the latest migration, its keyspaces hold
/// the definitions of a fresh install, the version row reads the latest
/// migration and the marker is gone; on a released catalog every policy
/// holds the `aws:ResourceTag` form and none the `dynamodb:` form.
async fn assert_finished(session: &Arc<CassandraSession>, prefix: &str, released: bool, at: &str) {
    assert_eq!(
        catalog_status(session.clone(), prefix)
            .await
            .expect("status"),
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        },
        "{at}"
    );
    let mut keyspaces = vec![("catalog", format!("{prefix}_catalog"))];
    if released {
        keyspaces.push(("account", format!("{prefix}_account_{ACCOUNT}")));
    }
    for (dir, keyspace) in keyspaces {
        let actual = postcheck::describe(session, &keyspace)
            .await
            .expect("describe");
        let path = format!(
            "{}/migrations/{dir}/0001_baseline.expected.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let expected: postcheck::Definitions =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("json");
        assert_eq!(expected, actual, "{at}: {dir}");
    }
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.to_owned());
    assert_eq!(
        store.get_setting("catalog_version").await.expect("read"),
        Some(MigrationId::LATEST.number().to_string()),
        "{at}"
    );
    assert_eq!(
        store
            .get_setting("resource_tag_migration")
            .await
            .expect("read"),
        None,
        "{at}"
    );
    if released {
        let text = policies(&store).await;
        assert!(!text.contains("dynamodb:ResourceTag"), "{at}: {text}");
        assert_eq!(
            text.matches("aws:ResourceTag/env").count(),
            6,
            "{at}: {text}"
        );
    }
}

/// Every policy of the released catalog's account, one line each, in a
/// fixed order: inline policies by principal, the two boundaries, then the
/// session policies.
async fn policies(store: &CassandraCatalogStore) -> String {
    let mut lines = Vec::new();
    for (principal_type, name) in [("group", "devs"), ("role", "app"), ("user", "dev")] {
        let mut found = store
            .list_policies(ACCOUNT, principal_type, name)
            .await
            .expect("policies");
        found.sort_by(|a, b| a.0.cmp(&b.0));
        for (policy, document, _) in found {
            lines.push(format!("{principal_type}\t{name}\t{policy}\t{document}"));
        }
    }
    for (principal_type, boundary) in [
        (
            "role",
            store.get_role_boundary(ACCOUNT, "app").await.expect("role"),
        ),
        (
            "user",
            store.get_user_boundary(ACCOUNT, "dev").await.expect("user"),
        ),
    ] {
        let boundary = boundary.map(|b| b.to_string()).unwrap_or_default();
        lines.push(format!("{principal_type}\tboundary\t{boundary}"));
    }
    let mut sessions = store.list_session_policies().await.expect("sessions");
    sessions.sort_by(|a, b| a.session_token.cmp(&b.session_token));
    for s in sessions {
        lines.push(format!(
            "session\t{}\t{}\t{}",
            s.session_token, s.role_name, s.policy
        ));
    }
    lines.join("\n") + "\n"
}

/// Stop a run before each write in `budgets`, run again with no stop, and
/// check that the second run finished. Returns how many runs stopped.
async fn stop_and_rerun(
    session: &Arc<CassandraSession>,
    released: bool,
    budgets: &[usize],
) -> usize {
    let mut stopped = 0;
    for &requests in budgets {
        let (prefix, drops) = new_prefix("stopped");
        if released {
            released_catalog(session, &prefix).await;
        } else {
            fresh_catalog(session, &prefix).await;
        }
        let at = format!("stopped before write {}", requests + 1);
        let (first, _) =
            run_stopped_after(session, &prefix, requests, false, MigrationId::LATEST).await;
        let error = first.expect_err(&format!("{at}: the run is stopped"));
        stopped += 1;
        let (rerun, _) =
            run_stopped_after(session, &prefix, usize::MAX, false, MigrationId::LATEST).await;
        if let Err(e) = rerun {
            panic!("{at} ({error}): the next run: {e}");
        }
        assert_finished(session, &prefix, released, &at).await;
        drop(drops);
        helpers::flush_pending_drops(session).await;
    }
    stopped
}

#[tokio::test]
async fn a_fresh_install_stopped_between_two_statements_of_migration_1_finishes_on_the_next_run() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    assert_eq!(stop_and_rerun(&session, false, &[2]).await, 1);
}

#[tokio::test]
async fn an_upgrade_stopped_inside_migrations_1_and_2_finishes_on_the_next_run() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    // Count the writes of a run that is not stopped: through migration 1 in
    // the catalog keyspace alone, then in the account keyspace, then the
    // rest.
    let (prefix, drops) = new_prefix("counted");
    released_catalog(&session, &prefix).await;
    let mut sent = Vec::new();
    for (catalog_only, through) in [
        (true, MigrationId::Baseline),
        (false, MigrationId::Baseline),
        (false, MigrationId::LATEST),
    ] {
        let (done, left) =
            run_stopped_after(&session, &prefix, usize::MAX, catalog_only, through).await;
        done.expect("a run that is not stopped");
        sent.push(usize::MAX - left);
    }
    assert_finished(&session, &prefix, true, "not stopped").await;
    drop(drops);
    helpers::flush_pending_drops(&session).await;
    let catalog_1 = sent[0];
    let migration_1 = catalog_1 + sent[1];
    let all = migration_1 + sent[2];
    // Migration 2's body starts with one write and rewrites six policies.
    assert!(all >= migration_1 + 7, "{sent:?}");
    let budgets = [
        // Between two statements of migration 1's catalog file.
        2,
        // Before migration 1's ledger row in the catalog keyspace, which is
        // written in one batch with the version row: as two writes, a stop
        // between them left the row beside the 0.1.x version row, and the
        // next run refused the catalog as one an older release wrote to.
        catalog_1 - 1,
        // Between two statements of migration 1's account file.
        catalog_1 + 2,
        // Inside migration 2, after its first policy rewrite.
        migration_1 + 2,
        // Before the last write of the run.
        all - 1,
    ];
    assert_eq!(
        stop_and_rerun(&session, true, &budgets).await,
        budgets.len()
    );
}

#[tokio::test]
async fn migration_2_rewrites_a_released_catalogs_policies_in_every_place() {
    if helpers::skip_without_cassandra() {
        return;
    }
    let session = session().await;
    let (prefix, _drops) = new_prefix("m2");
    released_catalog(&session, &prefix).await;
    let (done, _) =
        run_stopped_after(&session, &prefix, usize::MAX, false, MigrationId::LATEST).await;
    done.expect("migrate");
    assert_finished(&session, &prefix, true, "migrated").await;
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.clone());
    let mut text = policies(&store).await;
    let rows = session
        .query(format!(
            "SELECT notes FROM {prefix}_catalog.extenddb_migrations WHERE id = 2"
        ))
        .await
        .expect("read")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default();
    let notes: String = rows
        .first()
        .expect("migration 2's row")
        .get_r_by_name("notes")
        .expect("notes");
    assert_eq!(notes.lines().count(), 6, "{notes}");
    text.push_str("-- notes\n");
    text.push_str(&notes);
    text.push('\n');
    let path = format!(
        "{}/tests/fixtures/golden/0002_resource_tag_condition_keys.txt",
        env!("CARGO_MANIFEST_DIR")
    );
    if std::env::var("EXTENDDB_BLESS_EXPECTED").as_deref() == Ok("1") {
        std::fs::create_dir_all(std::path::Path::new(&path).parent().expect("dir"))
            .expect("golden dir");
        std::fs::write(&path, &text).expect("write golden");
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        expected, text,
        "differs from {path}; regenerate with EXTENDDB_BLESS_EXPECTED=1"
    );
}

/// Migration 2's ledger row names every policy it rewrote, so on a catalog
/// with many tagged policies the row's notes pass the node's
/// `batch_size_fail_threshold` (50 KB by default), and the row has to be
/// written on its own, not in a batch with the version row. 800 users with
/// one tagged policy each give notes of about 60 KB.
#[tokio::test]
async fn migration_2_records_its_row_when_it_rewrote_many_policies() {
    if helpers::skip_without_cassandra() {
        return;
    }
    const USERS: usize = 800;
    let session = session().await;
    let (prefix, _drops) = new_prefix("m2many");
    released_catalog(&session, &prefix).await;
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.clone());
    add_tagged_users(&store, USERS).await;
    let (done, _) =
        run_stopped_after(&session, &prefix, usize::MAX, false, MigrationId::LATEST).await;
    done.expect("migrate records migration 2 over many policies");
    assert_eq!(
        catalog_status(session.clone(), &prefix)
            .await
            .expect("status"),
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    let rows = session
        .query(format!(
            "SELECT notes FROM {prefix}_catalog.extenddb_migrations WHERE id = 2"
        ))
        .await
        .expect("read")
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default();
    let notes: String = rows
        .first()
        .expect("migration 2's row")
        .get_r_by_name("notes")
        .expect("notes");
    assert_eq!(notes.lines().count(), 6 + USERS, "{}", notes.len());
    assert!(notes.len() > 50 * 1024, "{} bytes", notes.len());
    let text = policies(&store).await;
    assert!(!text.contains("dynamodb:ResourceTag"));
}

/// `migrate --yes` and `--forget` read the status under the lock before they
/// act, and on a catalog behind migration 2 that status runs migration 2's
/// `detect` over every principal. Read as the holder, each of its requests
/// is progress, so a read longer than the stall leaves the renewal going and
/// the lock is still held one deadline window later. A read that records
/// nothing stops the renewal at the stall, and the deadline after the last
/// renewal passes inside that window.
#[tokio::test]
async fn the_status_read_under_the_lock_keeps_it_through_a_long_scan() {
    if helpers::skip_without_cassandra() {
        return;
    }
    const USERS: usize = 4000;
    let session = session().await;
    let (prefix, _drops) = new_prefix("m2status");
    released_catalog(&session, &prefix).await;
    let store = CassandraCatalogStore::catalog_only(session.clone(), prefix.clone());
    add_tagged_users(&store, USERS).await;
    let times = LeaseTimes {
        ttl: Duration::from_secs(6),
        renew_every: Duration::from_millis(300),
        margin: Duration::from_millis(2500),
        stall: Duration::from_millis(1500),
    };
    let catalog = format!("{prefix}_catalog");
    let lock = MigrationLock::acquire(session.clone(), &catalog, times, TIMEOUT)
        .await
        .expect("take the lock");
    let started = Instant::now();
    let status = status_holding(&lock, &prefix).await;
    let took = started.elapsed();
    let window = times.ttl - times.margin + times.renew_every;
    let held = Instant::now();
    while held.elapsed() < window {
        lock.progress();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let kept = lock.check();
    lock.release().await;
    assert!(
        matches!(status, Ok(CatalogStatus::Behind { .. })),
        "{status:?}"
    );
    // A read shorter than this keeps the lock without the progress it
    // records: the renewal only stops at a tick after the stall.
    assert!(
        took > times.stall + times.renew_every,
        "the status read took {took:?}"
    );
    kept.expect("the status read kept the lock");
}
