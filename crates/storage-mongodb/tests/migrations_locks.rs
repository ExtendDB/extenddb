// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The MongoDB migration lock and serving registration: two migrators, the
//! lease and its deadline, a hung holder, and servers beside migrations.

mod migrations_support;

use std::time::{Duration, Instant};

use bson::{Document, doc};
use extenddb_storage::migrations::{CatalogStatus, MigrationError, Runner};
use extenddb_storage_mongodb::migrations::{
    MigrationLock, MongoMigrations, ServingRegistration, Timing, lock, migrate_holding,
    register_and_check,
};
use migrations_support::{Deployment, fixture, server};

/// A lease of 4 s renewed every 1 s, acted on for 2.4 s after a renewal, and
/// registrations of 6 s that a server gives up on 4 s after its last renewal.
/// A renewal has 1.4 s to complete before the holder stops, enough on a
/// loaded CI runner; at 2 s and 0.5 s it had 0.7 s and failed under load.
const SHORT: Timing = Timing {
    lease: Duration::from_secs(4),
    renew_every: Duration::from_secs(1),
    margin: Duration::from_millis(1600),
    idle_limit: Duration::from_secs(30),
    registration: Duration::from_secs(6),
    registration_deadline: Duration::from_secs(4),
    retry_every: Duration::from_millis(100),
};

#[tokio::test(flavor = "multi_thread")]
async fn two_migrators_started_together_apply_once_and_the_other_finds_nothing() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "two").await;
    fixture::load(&d, &fixture::read("0.1.12")).await;
    let run = |target| async move {
        let (held, target) = lock(&target, false, Duration::from_secs(60), Timing::PRODUCTION)
            .await
            .expect("lock");
        let report = migrate_holding(&target, "test", &held)
            .await
            .expect("migrate");
        held.release().await;
        report
    };
    let (a, b) = tokio::join!(run(d.target.clone()), run(d.target.clone()));
    let mut applied = [a.applied.len(), b.applied.len()];
    applied.sort_unstable();
    assert_eq!(
        applied,
        [0, 5],
        "one applies every step and the other finds nothing"
    );
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_holder_keeps_the_lock_for_three_leases_and_a_killed_one_is_replaced_within_a_lease() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "lease").await;
    let holder = MigrationLock::acquire(&d.target, Duration::from_secs(5), SHORT)
        .await
        .expect("lock");
    let started = Instant::now();
    let refused = MigrationLock::acquire(&d.target, SHORT.lease * 3, SHORT)
        .await
        .err()
        .expect("refused for three leases");
    assert!(matches!(refused, MigrationError::Locked(_)), "{refused}");
    assert!(started.elapsed() >= SHORT.lease * 3);
    holder.check().expect("the holder still holds it");
    let killed = Instant::now();
    holder.abandon();
    let next = MigrationLock::acquire(&d.target, Duration::from_secs(10), SHORT)
        .await
        .expect("acquired");
    let waited = killed.elapsed();
    assert!(
        waited <= SHORT.lease + SHORT.renew_every + Duration::from_millis(500),
        "acquired {waited:?} after the holder was killed"
    );
    next.release().await;
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_holder_past_its_deadline_sends_nothing_and_records_nothing() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "deadline").await;
    // No renewal before the deadline: 2.4 s after the claim.
    let timing = Timing {
        renew_every: Duration::from_secs(60),
        ..SHORT
    };
    let held = MigrationLock::acquire(&d.target, Duration::from_secs(5), timing)
        .await
        .expect("lock");
    tokio::time::sleep(Duration::from_millis(2600)).await;
    let migrations = MongoMigrations::open(&d.target, Some(&held))
        .await
        .expect("open");
    let error = Runner::new(&migrations, "test")
        .migrate()
        .await
        .expect_err("stopped");
    migrations.close().await;
    held.release().await;
    assert!(error.to_string().contains("last renewed"), "{error}");
    assert_eq!(Deployment::collections(&d.catalog()).await, ["settings"]);
    assert!(Deployment::collections(&d.data()).await.is_empty());
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_holder_whose_lock_another_process_took_stops_at_its_next_renewal() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "taken").await;
    let holder = MigrationLock::acquire(&d.target, Duration::from_secs(5), SHORT)
        .await
        .expect("lock");
    // Another process's claim, as it writes the document.
    let settings = d.catalog().collection::<Document>("settings");
    settings
        .update_one(
            doc! { "_id": "migration_lock" },
            doc! { "$set": { "owner": "another" } },
        )
        .await
        .expect("take the lock");
    tokio::time::sleep(SHORT.renew_every * 2).await;
    let error = holder.check().expect_err("lost at the next renewal");
    assert!(
        error.to_string().contains("no longer names this process"),
        "{error}"
    );
    // The release deletes this process's document only.
    holder.release().await;
    assert!(
        settings
            .find_one(doc! { "_id": "migration_lock", "owner": "another" })
            .await
            .expect("read")
            .is_some()
    );
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_holder_that_makes_no_progress_stops_renewing_and_loses_the_lock() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "hung").await;
    let timing = Timing {
        idle_limit: Duration::from_secs(1),
        ..SHORT
    };
    let hung = MigrationLock::acquire(&d.target, Duration::from_secs(5), timing)
        .await
        .expect("lock");
    // The renewal task finds the holder idle at its first renewal, 1 s after
    // the claim, and stops renewing; the lease then ends 4 s after the claim.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let error = hung.check().expect_err("lost");
    assert!(error.to_string().contains("no progress"), "{error}");
    let next = MigrationLock::acquire(&d.target, Duration::from_secs(10), SHORT)
        .await
        .expect("taken once the hung holder's lease ends");
    next.release().await;
    hung.release().await;
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn migrate_waits_for_a_running_server_and_a_server_refuses_while_a_migration_runs() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "serving").await;
    d.create().await;

    let serving = register_and_check(&d.target, "test", Timing::PRODUCTION)
        .await
        .expect("serve");
    let error = lock(&d.target, false, Duration::from_secs(2), Timing::PRODUCTION)
        .await
        .err()
        .expect("refused");
    assert_eq!(error.exit_code(), 6);
    let message = error.to_string();
    assert!(
        message.contains(&format!("pid {}", std::process::id())),
        "{message}"
    );
    // The refusal released the lock it took.
    let settings = d.catalog().collection::<Document>("settings");
    assert!(
        settings
            .find_one(doc! { "_id": "migration_lock" })
            .await
            .unwrap()
            .is_none()
    );
    // The preview and verify read the status beside the server.
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    serving.release().await;
    let (held, _) = lock(&d.target, false, Duration::from_secs(2), Timing::PRODUCTION)
        .await
        .expect("no server runs");

    let refused = register_and_check(&d.target, "test", Timing::PRODUCTION)
        .await
        .err()
        .expect("a migration holds the lock");
    assert_eq!(refused.exit_code(), 6);
    let servers = Deployment::documents(&d.catalog(), "extenddb_servers").await;
    assert!(
        servers.is_empty(),
        "a refused server deletes its registration: {servers:?}"
    );
    held.release().await;
    register_and_check(&d.target, "test", Timing::PRODUCTION)
        .await
        .expect("serve")
        .release()
        .await;
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_refuses_a_catalog_it_cannot_serve_and_deletes_its_registration() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "refuse").await;
    let error = register_and_check(&d.target, "test", Timing::PRODUCTION)
        .await
        .err()
        .expect("uninitialized");
    assert_eq!(error, MigrationError::Status(CatalogStatus::Uninitialized));
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let error = register_and_check(&d.target, "test", Timing::PRODUCTION)
        .await
        .err()
        .expect("behind");
    assert_eq!(error.exit_code(), 3);
    assert!(
        Deployment::documents(&d.catalog(), "extenddb_servers")
            .await
            .is_empty()
    );
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_registration_that_cannot_be_renewed_is_reported_lost() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "lost").await;
    d.create().await;
    let registration = ServingRegistration::register(&d.target, "test", SHORT)
        .await
        .expect("register");
    let mut lost = registration.lost();
    d.catalog()
        .collection::<Document>("extenddb_servers")
        .delete_many(doc! {})
        .await
        .expect("delete");
    tokio::time::timeout(Duration::from_secs(5), lost.changed())
        .await
        .expect("reported within a renewal")
        .expect("changed");
    let reason = lost.borrow().clone().expect("a reason");
    assert!(reason.contains("deleted"), "{reason}");
    registration.release().await;
    d.drop_databases().await;
}
