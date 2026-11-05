// SPDX-License-Identifier: Apache-2.0

//! The startup policy `serve` runs before it opens the catalog
//! (`[server] catalog_upgrade`, or `serve --migrate`), on released
//! catalogs: under `apply` a catalog behind the binary is migrated through
//! the lock `migrate --yes` takes, a current one is served with nothing
//! written, one that needs a person exits 5 with nothing written, a held
//! migration lock makes the server wait and then exit 6, and two
//! `prepare_to_serve` runs that wait for the lock together migrate once.

mod migrations_support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bson::{Document, doc};
use extenddb_storage::migrations::{
    CatalogStatus, MigrationError, MigrationId, Prepared, Startup, Target, exit, prepare_to_serve,
};
use extenddb_storage_mongodb::migrations::MongoCatalogMigrations;
use migrations_support::{Deployment, fixture, server};

fn startup(apply: bool, lock_timeout: Duration) -> Startup<'static> {
    Startup {
        apply,
        creates_uninitialized: false,
        release: "1.0.0-test",
        lock_timeout,
        heartbeat: Startup::HEARTBEAT,
    }
}

/// The policy as `serve` runs it, through the backend's catalog migrations
/// on the deployment's databases.
async fn prepare(d: &Deployment, startup: &Startup<'_>) -> Result<Prepared, MigrationError> {
    let migrations = MongoCatalogMigrations::with_databases(&d.target.catalog, &d.target.data);
    let target = Target::at(&d.target.connection_string);
    prepare_to_serve(&migrations, &target, startup, &|_| {}).await
}

/// What a run could change: every document of every collection of both
/// databases.
async fn state(d: &Deployment) -> Vec<(String, Vec<Document>)> {
    let mut found = Vec::new();
    for db in [d.catalog(), d.data()] {
        for name in Deployment::collections(&db).await {
            let documents = Deployment::documents(&db, &name).await;
            found.push((format!("{}.{name}", db.name()), documents));
        }
    }
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_migrates_a_released_catalog_and_refuse_writes_nothing() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "start_apply").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let before = state(&d).await;
    let err = prepare(&d, &startup(false, Duration::from_secs(10)))
        .await
        .expect_err("refused");
    assert_eq!(err.exit_code(), exit::PENDING, "{err:?}");
    assert!(state(&d).await == before, "refuse wrote to the catalog");

    let prepared = prepare(&d, &startup(true, Duration::from_secs(10)))
        .await
        .expect("applied");
    let report = prepared.run.expect("a run");
    assert_eq!(report.applied.len(), 7, "{report:?}");
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_on_a_current_catalog_writes_nothing() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "start_current").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    d.migrate().await.expect("migrate");
    let before = state(&d).await;
    let prepared = prepare(&d, &startup(true, Duration::from_secs(10)))
        .await
        .expect("served");
    assert_eq!(prepared, Prepared::default());
    assert!(
        state(&d).await == before,
        "a current catalog was written to"
    );
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_on_a_catalog_that_needs_attention_exits_5_and_writes_nothing() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "start_attention").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    // An IAM user whose policy migration 2 cannot rewrite: both forms of
    // one tag key, with different values.
    let catalog = d.catalog();
    let account = catalog
        .collection::<Document>("accounts")
        .find_one(doc! {})
        .await
        .expect("read accounts")
        .expect("an account");
    let account = account
        .get_str("account_id")
        .or_else(|_| account.get_str("_id"))
        .expect("an account id")
        .to_owned();
    let now = bson::DateTime::now();
    catalog
        .collection::<Document>("iam_users")
        .insert_one(doc! {
            "account_id": &account,
            "user_name": "collider",
            "user_arn": format!("arn:aws:iam::{account}:user/collider"),
            "created_at": now,
        })
        .await
        .expect("a user");
    catalog
        .collection::<Document>("iam_policies")
        .insert_one(doc! {
            "account_id": &account,
            "principal_type": "user",
            "principal_name": "collider",
            "policy_name": "p",
            "policy_document": {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Deny",
                    "Action": "dynamodb:*",
                    "Resource": "*",
                    "Condition": { "StringEquals": {
                        "aws:ResourceTag/env": "prod",
                        "dynamodb:ResourceTag/env": "dev",
                    } },
                }],
            },
            "created_at": now,
        })
        .await
        .expect("a colliding policy");
    assert!(
        matches!(d.status().await, CatalogStatus::NeedsAttention { .. }),
        "the policy does not collide"
    );
    let before = state(&d).await;
    let err = prepare(&d, &startup(true, Duration::from_secs(10)))
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
    assert!(
        state(&d).await == before,
        "a refused run wrote to the catalog"
    );
    d.drop_databases().await;
}

/// Another migration's lock, as its claim writes it: the server waits for
/// it up to the timeout, then exits 6 with the catalog unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn apply_waits_for_a_held_migration_lock_and_exits_6() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "start_locked").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    another_migrations_lock(&d).await;
    let before = state(&d).await;
    let started = Instant::now();
    let err = prepare(&d, &startup(true, Duration::from_secs(2)))
        .await
        .expect_err("locked");
    assert!(
        started.elapsed() >= Duration::from_millis(1800),
        "it waited {:?}",
        started.elapsed()
    );
    assert!(matches!(err, MigrationError::Locked(_)), "{err:?}");
    assert_eq!(err.exit_code(), exit::LOCKED);
    assert!(
        state(&d).await == before,
        "a server that waited wrote to the catalog"
    );
    d.drop_databases().await;
}

/// Another migration's lock document, as its claim writes it, for two
/// minutes.
async fn another_migrations_lock(d: &Deployment) {
    d.catalog()
        .collection::<Document>("settings")
        .insert_one(doc! {
            "_id": "migration_lock",
            "owner": "another",
            "expires_at": bson::DateTime::from_millis(
                bson::DateTime::now().timestamp_millis() + 120_000
            ),
        })
        .await
        .expect("another migration's lock");
}

/// Two `prepare_to_serve` runs under `apply` on a released catalog, both
/// waiting for the lock another migration holds, as the checks of two
/// servers started together do after their first status read: once it is
/// free, one migrates the catalog, and the other takes the lock after it
/// and applies nothing. The servers are not started here; one of two
/// servers started together can exit 6 when it registers while the other
/// holds the lock.
#[tokio::test(flavor = "multi_thread")]
async fn two_prepare_to_serve_runs_under_apply_migrate_once() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "start_two").await;
    fixture::load(&d, &fixture::read("0.1.12")).await;
    another_migrations_lock(&d).await;
    let migrations = MongoCatalogMigrations::with_databases(&d.target.catalog, &d.target.data);
    let target = Target::at(&d.target.connection_string);
    let waiting = AtomicUsize::new(0);
    let progress = |line: &str| {
        if line.starts_with("taking the migration lock") {
            waiting.fetch_add(1, Ordering::SeqCst);
        }
    };
    let apply = startup(true, Duration::from_secs(60));
    let free = async {
        while waiting.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        d.catalog()
            .collection::<Document>("settings")
            .delete_one(doc! { "_id": "migration_lock", "owner": "another" })
            .await
            .expect("the other migration releases its lock");
    };
    let (a, b, ()) = tokio::join!(
        prepare_to_serve(&migrations, &target, &apply, &progress),
        prepare_to_serve(&migrations, &target, &apply, &progress),
        free,
    );
    let mut applied: Vec<usize> = [a.expect("first"), b.expect("second")]
        .iter()
        .map(|p| p.run.as_ref().expect("each took the lock").applied.len())
        .collect();
    applied.sort_unstable();
    assert_eq!(
        applied,
        [0, 7],
        "one migrates, and the other takes the lock after it and applies nothing"
    );
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    d.drop_databases().await;
}
