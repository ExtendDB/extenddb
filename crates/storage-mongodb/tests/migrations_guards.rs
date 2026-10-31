// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What stops a MongoDB run and what a rerun finishes: the post-check, a
//! failed write, indexes a deployment lacks, and a run stopped at every
//! request it sends.

mod migrations_support;

use std::time::Duration;

use bson::{Document, doc};
use extenddb_storage::migrations::{CatalogStatus, MigrationBackend, Runner};
use extenddb_storage_mongodb::migrations::{
    Definitions, MigrationLock, MongoMigrations, Timing, describe_store, lock,
};
use migrations_support::{Deployment, fixture, server};

const CATALOG_EXPECTED: &str = include_str!("../migrations/catalog/0001_baseline.expected.json");
const DATA_EXPECTED: &str = include_str!("../migrations/data/0001_baseline.expected.json");

async fn index_names(db: &mongodb::Database, collection: &str) -> Vec<String> {
    let mut names = db
        .collection::<Document>(collection)
        .list_index_names()
        .await
        .expect("indexes");
    names.sort();
    names
}

/// Run every migration with the runner stopped before request `requests`.
/// Returns whether the run finished, or the error.
async fn run_stopped_after(d: &Deployment, requests: usize) -> Result<(), String> {
    let creating = d.status().await == CatalogStatus::Uninitialized;
    let (held, target) = lock(
        &d.target,
        creating,
        Duration::from_secs(10),
        Timing::PRODUCTION,
    )
    .await
    .expect("lock");
    let migrations = MongoMigrations::open(&target, Some(&held))
        .await
        .expect("open")
        .stop_after(requests);
    let finished = Runner::new(&migrations, "test")
        .migrate()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    migrations.close().await;
    held.release().await;
    finished
}

/// Both databases match a fresh install's definitions.
async fn assert_fresh_definitions(d: &Deployment) {
    let migrations = MongoMigrations::open(&d.target, None).await.expect("open");
    let catalog = describe_store(migrations.stores(), &d.catalog())
        .await
        .expect("catalog");
    let data = describe_store(migrations.stores(), &d.data())
        .await
        .expect("data");
    migrations.close().await;
    let expected_catalog: Definitions = serde_json::from_str(CATALOG_EXPECTED).expect("json");
    let expected_data: Definitions = serde_json::from_str(DATA_EXPECTED).expect("json");
    for (name, want) in &expected_catalog {
        assert_eq!(catalog.get(name), Some(want), "{name}");
    }
    for (name, want) in &expected_data {
        assert_eq!(data.get(name), Some(want), "{name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_whose_objects_differ_records_nothing_and_names_the_object() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "differ").await;
    d.catalog()
        .run_command(doc! { "create": "tags", "capped": true, "size": 4096 })
        .await
        .expect("capped");
    let (held, target) = lock(&d.target, true, Duration::from_secs(5), Timing::PRODUCTION)
        .await
        .expect("lock");
    let migrations = MongoMigrations::open(&target, Some(&held))
        .await
        .expect("open");
    let error = Runner::new(&migrations, "test")
        .migrate()
        .await
        .expect_err("refused");
    migrations.close().await;
    held.release().await;
    assert!(error.to_string().contains("the collection tags"), "{error}");
    assert!(
        Deployment::documents(&d.catalog(), "extenddb_migrations")
            .await
            .is_empty()
    );
    assert_eq!(d.status().await, CatalogStatus::Uninitialized);
    d.drop_databases().await;

    // An index of the same key with other options fails to create, by the
    // server's own conflict code, and nothing is recorded.
    let d = Deployment::new(&uri, "conflict").await;
    d.catalog()
        .collection::<Document>("accounts")
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "account_name": 1 })
                .build(),
        )
        .await
        .expect("index");
    let (held, target) = lock(&d.target, true, Duration::from_secs(5), Timing::PRODUCTION)
        .await
        .expect("lock");
    let migrations = MongoMigrations::open(&target, Some(&held))
        .await
        .expect("open");
    let error = Runner::new(&migrations, "test")
        .migrate()
        .await
        .expect_err("refused");
    migrations.close().await;
    held.release().await;
    assert!(error.to_string().contains("code 86"), "{error}");
    assert!(error.to_string().contains("account_name"), "{error}");
    assert_eq!(d.status().await, CatalogStatus::Uninitialized);
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_write_stops_the_run_instead_of_being_discarded() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "refusedwrite").await;
    // Released bootstraps discarded this error and recorded 001_initial
    // anyway (MongoDB item 13).
    d.catalog()
        .run_command(doc! {
            "create": "schema_history",
            "validator": { "_id": { "$ne": "001_initial" } },
        })
        .await
        .expect("validator");
    let (held, target) = lock(&d.target, true, Duration::from_secs(5), Timing::PRODUCTION)
        .await
        .expect("lock");
    let migrations = MongoMigrations::open(&target, Some(&held))
        .await
        .expect("open");
    let error = Runner::new(&migrations, "test")
        .migrate()
        .await
        .expect_err("refused");
    migrations.close().await;
    held.release().await;
    assert!(error.to_string().contains("code 121"), "{error}");
    assert!(
        Deployment::documents(&d.catalog(), "extenddb_migrations")
            .await
            .is_empty()
    );
    assert_eq!(d.setting("catalog_version").await, None);
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn migrate_creates_the_indexes_a_deployment_lacks() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "missingidx").await;
    fixture::load(&d, &fixture::read("0.1.14")).await;
    for (db, collection, index) in [
        (d.data(), "idempotency_tokens", "account_id_1_token_1"),
        (d.data(), "stream_shards", "shard_id_1"),
        (d.catalog(), "tables", "table_id_1"),
    ] {
        db.collection::<Document>(collection)
            .drop_index(index)
            .await
            .expect("drop index");
    }
    d.migrate().await.expect("migrate");
    assert!(
        index_names(&d.data(), "idempotency_tokens")
            .await
            .contains(&"account_id_1_token_1".to_owned())
    );
    assert!(
        index_names(&d.data(), "stream_shards")
            .await
            .contains(&"shard_id_1".to_owned())
    );
    assert!(
        index_names(&d.catalog(), "tables")
            .await
            .contains(&"table_id_1".to_owned())
    );
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    assert_fresh_definitions(&d).await;
    d.drop_databases().await;
}

/// The runner stopped before each request in turn, as a migrator killed at
/// that point stops, then run again: every rerun finishes with the
/// definitions of a fresh install and no marker, and on a released catalog
/// with the policy rewritten. Returns how many runs stopped.
async fn stop_at_every_request(uri: &str, state: Option<&str>) -> usize {
    let mut stopped_runs = 0;
    for requests in 0.. {
        let d = Deployment::new(uri, "stopped").await;
        if let Some(state) = state {
            fixture::load(&d, &fixture::read(state)).await;
        }
        let finished = run_stopped_after(&d, requests).await.is_ok();
        if !finished {
            stopped_runs += 1;
            if let Err(e) = run_stopped_after(&d, usize::MAX).await {
                panic!("{state:?}: the rerun after {requests}: {e}");
            }
        }
        assert_eq!(
            d.status().await,
            CatalogStatus::Current { version: 4 },
            "{requests}"
        );
        assert_fresh_definitions(&d).await;
        assert_eq!(d.setting("resource_tag_migration").await, None);
        if state.is_some() {
            let deny = d
                .catalog()
                .collection::<Document>("iam_policies")
                .find_one(doc! { "policy_name": "deny-prod" })
                .await
                .expect("policy")
                .expect("deny-prod");
            let text = deny
                .get_document("policy_document")
                .expect("document")
                .to_string();
            assert!(text.contains("aws:ResourceTag/env"), "{requests}: {text}");
            assert!(!text.contains("dynamodb:ResourceTag"), "{requests}: {text}");
        }
        d.drop_databases().await;
        if finished {
            break;
        }
    }
    stopped_runs
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_run_stopped_at_any_request_finishes_on_the_next_run() {
    let Some(uri) = server() else { return };
    assert!(stop_at_every_request(&uri, None).await > 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_stopped_at_any_request_finishes_on_the_next_run() {
    let Some(uri) = server() else { return };
    assert!(stop_at_every_request(&uri, Some("0.1.12")).await > 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lock_left_by_a_killed_holder_is_taken_once_it_expires() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "expired").await;
    let timing = Timing {
        lease: Duration::from_secs(2),
        renew_every: Duration::from_millis(500),
        margin: Duration::from_millis(800),
        idle_limit: Duration::from_secs(30),
        retry_every: Duration::from_millis(100),
        ..Timing::PRODUCTION
    };
    MigrationLock::acquire(&d.target, Duration::from_secs(5), timing)
        .await
        .expect("lock")
        .abandon();
    let refused = MigrationLock::acquire(&d.target, Duration::from_millis(500), timing)
        .await
        .err()
        .expect("held");
    assert_eq!(refused.exit_code(), 6);
    let again = MigrationLock::acquire(&d.target, Duration::from_secs(5), timing)
        .await
        .expect("taken after the lease");
    again.release().await;
    d.drop_databases().await;
}
