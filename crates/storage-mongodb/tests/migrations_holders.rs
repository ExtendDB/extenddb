// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What a migrator and a server do while they hold the migration lock or a
//! serving registration: migration 2's store calls keep the lock while they
//! complete and stop at its deadline, a server that renews its registration
//! stays registered, a server starts beside a lock whose lease ended, and a
//! dropped lock is released. Also the collision `migrate --yes` reads before
//! its first write. See `migrations_support` for the server these tests need.

mod migrations_support;

use std::time::{Duration, Instant};

use bson::{Document, doc};
use extenddb_storage::management_store::OpError;
use extenddb_storage::migrations::{
    CatalogStatus, HasManagementStore, HeldLock, MigrationBackend, MigrationError,
};
use extenddb_storage_mongodb::migrations::{
    MigrationLock, MongoMigrations, ServingRegistration, Timing, lock, migrate_holding,
    register_and_check, status_holding,
};
use migrations_support::{Deployment, fixture, server};

/// The id of the deployment's first account: `account_id`, or `_id` for the
/// default account a released `init` wrote without one.
async fn first_account(d: &Deployment) -> String {
    let account = d
        .catalog()
        .collection::<Document>("accounts")
        .find_one(doc! {})
        .await
        .expect("read accounts")
        .expect("an account");
    account
        .get_str("account_id")
        .or_else(|_| account.get_str("_id"))
        .expect("an account id")
        .to_owned()
}

/// Add IAM users `<prefix>0` to `<prefix><count - 1>` to `account`, each
/// with one inline policy holding `document`.
async fn add_users(d: &Deployment, account: &str, prefix: &str, count: usize, document: &Document) {
    let now = bson::DateTime::now();
    let mut users = Vec::new();
    let mut policies = Vec::new();
    for i in 0..count {
        let name = format!("{prefix}{i}");
        users.push(doc! {
            "account_id": account,
            "user_name": &name,
            "user_arn": format!("arn:aws:iam::{account}:user/{name}"),
            "created_at": now,
        });
        policies.push(doc! {
            "account_id": account,
            "principal_type": "user",
            "principal_name": &name,
            "policy_name": "p",
            "policy_document": document.clone(),
            "created_at": now,
        });
    }
    let catalog = d.catalog();
    catalog
        .collection::<Document>("iam_users")
        .insert_many(users)
        .await
        .expect("insert users");
    catalog
        .collection::<Document>("iam_policies")
        .insert_many(policies)
        .await
        .expect("insert policies");
}

fn allow_get_item() -> Document {
    doc! {
        "Version": "2012-10-17",
        "Statement": [{ "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*" }],
    }
}

/// Migration 2 sends two requests per user through the leased management
/// store, for its `detect` and again for its run. Each one that completes is
/// progress, so a scan that lasts longer than the idle limit keeps the lock
/// and the migration is applied.
#[tokio::test(flavor = "multi_thread")]
async fn migration_2_keeps_the_lock_through_a_scan_longer_than_the_idle_limit() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "scan").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let account = first_account(&d).await;
    add_users(&d, &account, "scan", 1000, &allow_get_item()).await;
    let timing = Timing {
        lease: Duration::from_secs(20),
        renew_every: Duration::from_millis(100),
        margin: Duration::from_secs(5),
        idle_limit: Duration::from_millis(300),
        ..Timing::PRODUCTION
    };
    let (held, target) = lock(&d.target, false, Duration::from_secs(10), timing)
        .await
        .expect("lock");
    let started = Instant::now();
    let applied = migrate_holding(&target, "test", &held).await;
    let took = started.elapsed();
    held.release().await;
    applied.expect("every migration applies");
    // A shorter run would apply without the progress the scan records.
    assert!(took > timing.idle_limit * 4, "the run took {took:?}");
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    assert!(d.catalog_ledger().await.iter().any(|row| row.id == 2));
    d.drop_databases().await;
}

/// `migrate --yes` reads the status under the lock before it runs, and on a
/// catalog behind migration 2 the status runs migration 2's `detect`. Read
/// as the holder, its calls keep the lock through a scan longer than the
/// idle limit, and the run that follows applies.
#[tokio::test(flavor = "multi_thread")]
async fn the_status_read_under_the_lock_keeps_it_through_a_long_scan() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "lockedstatus").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let account = first_account(&d).await;
    add_users(&d, &account, "status", 1000, &allow_get_item()).await;
    let timing = Timing {
        lease: Duration::from_secs(20),
        renew_every: Duration::from_millis(100),
        margin: Duration::from_secs(5),
        idle_limit: Duration::from_millis(300),
        ..Timing::PRODUCTION
    };
    let (held, target) = lock(&d.target, false, Duration::from_secs(10), timing)
        .await
        .expect("lock");
    let started = Instant::now();
    let status = status_holding(&target, &held).await;
    let took = started.elapsed();
    let applied = migrate_holding(&target, "test", &held).await;
    held.release().await;
    status.expect("the status");
    // A shorter read would keep the lock without the progress it records.
    assert!(
        took > timing.idle_limit * 2,
        "the status read took {took:?}"
    );
    applied.expect("every migration applies");
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    d.drop_databases().await;
}

/// Past the lock's deadline the leased store sends nothing: a policy write
/// migration 2 would send is refused before it reaches the database.
#[tokio::test(flavor = "multi_thread")]
async fn migration_2_writes_nothing_after_the_lock_deadline() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "storedeadline").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let account = first_account(&d).await;
    add_users(&d, &account, "late", 1, &allow_get_item()).await;
    // No renewal before the deadline, 1.2 s after the claim.
    let timing = Timing {
        lease: Duration::from_secs(2),
        renew_every: Duration::from_secs(60),
        margin: Duration::from_millis(800),
        ..Timing::PRODUCTION
    };
    let held = MigrationLock::acquire(&d.target, Duration::from_secs(5), timing)
        .await
        .expect("lock");
    let migrations = MongoMigrations::open(&d.target, Some(&held))
        .await
        .expect("open");
    let store = migrations.stores().management_store();
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{ "Effect": "Allow", "Action": "dynamodb:Query", "Resource": "*" }],
    });
    store
        .put_policy(&account, "user", "late0", "before", &policy)
        .await
        .expect("before the deadline");
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let error = store
        .put_policy(&account, "user", "late0", "after", &policy)
        .await
        .expect_err("past the deadline");
    let OpError::Internal(message) = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("last renewed"), "{message}");
    migrations.close().await;
    held.release().await;
    let mut names: Vec<String> = Deployment::documents(&d.catalog(), "iam_policies")
        .await
        .iter()
        .filter(|p| p.get_str("principal_name") == Ok("late0"))
        .map(|p| p.get_str("policy_name").unwrap_or_default().to_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["before", "p"]);
    d.drop_databases().await;
}

/// A server that renews its registration stays registered: after three of
/// its registration lifetimes `migrate --yes` still finds it and waits.
#[tokio::test(flavor = "multi_thread")]
async fn a_renewed_registration_outlives_three_registration_lifetimes() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "renewed").await;
    d.create().await;
    let timing = Timing {
        registration: Duration::from_secs(2),
        registration_deadline: Duration::from_millis(1500),
        renew_every: Duration::from_millis(200),
        retry_every: Duration::from_millis(100),
        ..Timing::PRODUCTION
    };
    let serving = ServingRegistration::register(&d.target, "test", timing)
        .await
        .expect("register");
    tokio::time::sleep(timing.registration * 3 + Duration::from_millis(500)).await;
    assert_eq!(*serving.lost().borrow(), None, "the registration was lost");
    let error = lock(
        &d.target,
        false,
        Duration::from_millis(500),
        Timing::PRODUCTION,
    )
    .await
    .err()
    .expect("a server is registered");
    assert_eq!(error.exit_code(), 6, "{error}");
    assert!(
        error
            .to_string()
            .contains(&format!("pid {}", std::process::id())),
        "{error}"
    );
    serving.release().await;
    d.drop_databases().await;
}

/// A lock whose lease ended, as a killed migrator leaves it, does not stop a
/// server from starting; a live one does.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_starts_beside_a_lock_whose_lease_ended() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "expiredlock").await;
    d.create().await;
    let timing = Timing {
        lease: Duration::from_secs(2),
        renew_every: Duration::from_millis(500),
        margin: Duration::from_millis(800),
        retry_every: Duration::from_millis(100),
        ..Timing::PRODUCTION
    };
    let claimed = Instant::now();
    MigrationLock::acquire(&d.target, Duration::from_secs(5), timing)
        .await
        .expect("lock")
        .abandon();
    let refused = register_and_check(&d.target, "test", Timing::PRODUCTION)
        .await
        .err()
        .expect("the lease has not ended");
    assert_eq!(refused.exit_code(), 6, "{refused}");
    tokio::time::sleep(
        (timing.lease + Duration::from_millis(500)).saturating_sub(claimed.elapsed()),
    )
    .await;
    register_and_check(&d.target, "test", Timing::PRODUCTION)
        .await
        .expect("serves once the lease ended")
        .release()
        .await;
    d.drop_databases().await;
}

/// `migrate --yes`, `--forget` and `init` release the lock by dropping the
/// shared [`HeldLock`], which has no release call, so a claim right after
/// the drop takes the lock at once. A single-threaded runtime, as the
/// commands use.
#[tokio::test]
async fn a_dropped_lock_is_released_at_once() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "dropped").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let (held, target) = lock(
        &d.target,
        false,
        Duration::from_secs(10),
        Timing::PRODUCTION,
    )
    .await
    .expect("lock");
    migrate_holding(&target, "test", &held)
        .await
        .expect("migrate");
    drop(HeldLock::new(target.connection_string.clone(), held));
    MigrationLock::acquire(&d.target, Duration::from_secs(1), Timing::PRODUCTION)
        .await
        .expect("released by the drop")
        .release()
        .await;
    d.drop_databases().await;
}

/// A policy migration 2 cannot rewrite is found before the first write:
/// `migrate --yes` stops with `NeedsAttention` and records nothing,
/// migration 1 included.
#[tokio::test(flavor = "multi_thread")]
async fn a_collision_is_found_before_the_first_write_and_nothing_is_recorded() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "collision").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    let account = first_account(&d).await;
    let both_forms = doc! {
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
    };
    add_users(&d, &account, "collider", 1, &both_forms).await;
    let error = d.migrate().await.expect_err("needs attention");
    let MigrationError::Status(CatalogStatus::NeedsAttention { items }) = &error else {
        panic!("{error}");
    };
    assert!(
        items.iter().any(|item| item.contains("collider0")),
        "{items:?}"
    );
    assert!(
        Deployment::documents(&d.catalog(), "extenddb_migrations")
            .await
            .is_empty()
    );
    assert!(
        Deployment::documents(&d.data(), "extenddb_migrations")
            .await
            .is_empty()
    );
    d.drop_databases().await;
}
