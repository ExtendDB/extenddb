// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Fresh installs on MongoDB: what is a catalog, what migration 1 creates,
//! reruns, `--reconverge`, `--forget`, and the statuses the ledger and the
//! settings decide.

mod migrations_support;

use std::time::Duration;

use bson::{Document, doc};
use extenddb_storage::migrations::{
    CatalogStatus, MigrationBackend, MigrationError, MigrationId, Outcome, Runner, exit,
};
use extenddb_storage_mongodb::migrations::{
    Definitions, MigrationLock, MongoMigrations, Timing, describe_store, lock,
};
use migrations_support::{Deployment, server};

const CATALOG_EXPECTED: &str = include_str!("../migrations/catalog/0001_baseline.expected.json");
const DATA_EXPECTED: &str = include_str!("../migrations/data/0001_baseline.expected.json");

async fn definitions(d: &Deployment) -> (Definitions, Definitions) {
    let migrations = MongoMigrations::open(&d.target, None).await.expect("open");
    let stores = migrations.stores();
    let catalog = describe_store(stores, &d.catalog()).await.expect("catalog");
    let data = describe_store(stores, &d.data()).await.expect("data");
    migrations.close().await;
    (catalog, data)
}

#[tokio::test(flavor = "multi_thread")]
async fn stores_that_are_not_catalogs_are_uninitialized_and_migrate_writes_nothing_to_them() {
    let Some(uri) = server() else { return };
    // An empty store.
    let empty = Deployment::new(&uri, "empty").await;
    assert_eq!(empty.status().await, CatalogStatus::Uninitialized);
    let refused = lock(
        &empty.target,
        false,
        Duration::from_secs(2),
        Timing::PRODUCTION,
    )
    .await
    .err()
    .expect("refused");
    assert_eq!(
        refused,
        MigrationError::Status(CatalogStatus::Uninitialized)
    );
    assert_eq!(refused.exit_code(), exit::ERROR);
    assert!(Deployment::collections(&empty.catalog()).await.is_empty());

    // A store holding only the migration lock, left by a holder that died.
    let locked = Deployment::new(&uri, "lockonly").await;
    MigrationLock::acquire(&locked.target, Duration::from_secs(5), Timing::PRODUCTION)
        .await
        .expect("lock")
        .abandon();
    assert_eq!(
        Deployment::collections(&locked.catalog()).await,
        ["settings"]
    );
    assert_eq!(locked.status().await, CatalogStatus::Uninitialized);

    // Another program's database.
    let other = Deployment::new(&uri, "other").await;
    other
        .catalog()
        .collection::<Document>("orders")
        .insert_one(doc! { "order": 1 })
        .await
        .expect("insert");
    assert_eq!(other.status().await, CatalogStatus::Uninitialized);
    let refused = lock(
        &other.target,
        false,
        Duration::from_secs(2),
        Timing::PRODUCTION,
    )
    .await
    .err()
    .expect("refused");
    assert_eq!(refused.exit_code(), exit::ERROR);
    assert_eq!(Deployment::collections(&other.catalog()).await, ["orders"]);
    for d in [empty, locked, other] {
        d.drop_databases().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_store_gets_every_migration_and_the_expected_definitions() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "fresh").await;
    let report = d.create().await;
    assert_eq!((report.from, report.to), (None, Some(4)));
    let applied: Vec<(u32, String)> = report
        .applied
        .iter()
        .map(|s| (s.id, s.store_name.clone()))
        .collect();
    assert_eq!(
        applied,
        [
            (1, d.target.catalog.clone()),
            (1, d.target.data.clone()),
            (2, d.target.catalog.clone()),
            (3, d.target.catalog.clone()),
            (4, d.target.catalog.clone()),
        ]
    );
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    let rows = d.catalog_ledger().await;
    let outcomes: Vec<(u32, Outcome)> = rows.iter().map(|r| (r.id, r.outcome)).collect();
    assert_eq!(
        outcomes,
        [
            (1, Outcome::Applied),
            (2, Outcome::Applied),
            (3, Outcome::NotApplicable),
            (4, Outcome::NotApplicable),
        ]
    );
    assert!(
        rows.iter()
            .all(|r| r.applied_by == "test" && !r.applied_at.is_empty())
    );
    let data_rows = Deployment::documents(&d.data(), "extenddb_migrations").await;
    assert_eq!(data_rows.len(), 1);
    assert_eq!(data_rows[0].get_i64("_id"), Ok(1));
    assert_eq!(d.setting("catalog_version").await.as_deref(), Some("4"));
    assert_eq!(d.setting("resource_tag_migration").await, None);
    let history = Deployment::documents(&d.catalog(), "schema_history").await;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].get_str("_id"), Ok("001_initial"));

    let (catalog, data) = definitions(&d).await;
    if std::env::var("EXTENDDB_BLESS_EXPECTED").as_deref() == Ok("1") {
        let dir = env!("CARGO_MANIFEST_DIR");
        for (path, defs) in [("catalog", &catalog), ("data", &data)] {
            let text = serde_json::to_string_pretty(defs).expect("json") + "\n";
            std::fs::write(
                format!("{dir}/migrations/{path}/0001_baseline.expected.json"),
                text,
            )
            .expect("write");
        }
    }
    let expected_catalog: Definitions = serde_json::from_str(CATALOG_EXPECTED).expect("catalog");
    let expected_data: Definitions = serde_json::from_str(DATA_EXPECTED).expect("data");
    assert_eq!(catalog, expected_catalog);
    assert_eq!(data, expected_data);
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_run_applies_nothing_and_reconverge_keeps_the_rows() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "rerun").await;
    d.create().await;
    let before = d.catalog_ledger().await;
    let again = d.migrate().await.expect("migrate");
    assert!(again.applied.is_empty());
    assert_eq!((again.from, again.to), (Some(4), Some(4)));

    let reconverged = d.reconverge().await.expect("reconverge");
    assert_eq!(reconverged.applied.len(), 5);
    assert_eq!(
        d.catalog_ledger().await,
        before,
        "reconverge leaves the rows as they are"
    );
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forget_clears_an_unreleased_id_and_migrate_applies_it_again() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "forget").await;
    d.create().await;
    let held = MigrationLock::acquire(&d.target, Duration::from_secs(5), Timing::PRODUCTION)
        .await
        .expect("lock");
    let migrations = MongoMigrations::open(&d.target, Some(&held))
        .await
        .expect("open");
    Runner::new(&migrations, "test")
        .forget(1)
        .await
        .expect("forget 1");
    migrations.close().await;
    held.release().await;
    // Migration 1 is gone from both ledgers, so the version is 0.
    assert!(
        Deployment::documents(&d.data(), "extenddb_migrations")
            .await
            .is_empty()
    );
    assert_eq!(d.setting("catalog_version").await.as_deref(), Some("0"));
    let CatalogStatus::Behind { version, pending } = d.status().await else {
        panic!("not behind");
    };
    assert_eq!(version, Some(0));
    assert_eq!(pending.iter().map(|p| p.id).collect::<Vec<_>>(), [1, 1]);
    let report = d.migrate().await.expect("migrate");
    assert_eq!(report.applied.len(), 2);
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_checksum_a_newer_catalog_and_an_older_release_are_reported() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "statuses").await;
    d.create().await;
    let ledger = d.catalog().collection::<Document>("extenddb_migrations");
    let settings = d.catalog().collection::<Document>("settings");

    // A later binary's breaking migration, then an additive one.
    let row = |id: i64, kind: &str| {
        doc! { "_id": id, "name": "later", "kind": kind, "checksum": "c", "outcome": "applied",
        "notes": "", "applied_at": bson::DateTime::now(), "applied_by": "9.9.9" }
    };
    ledger.insert_one(row(5, "breaking")).await.expect("insert");
    let status = d.status().await;
    assert_eq!(status.label(), "AheadIncompatible");
    assert_eq!(status.exit_code(), exit::AHEAD_INCOMPATIBLE);
    ledger
        .delete_one(doc! { "_id": 5_i64 })
        .await
        .expect("delete");
    ledger.insert_one(row(5, "additive")).await.expect("insert");
    assert_eq!(d.status().await.label(), "AheadCompatible");
    assert!(d.migrate().await.expect("migrate").applied.is_empty());
    ledger
        .delete_one(doc! { "_id": 5_i64 })
        .await
        .expect("delete");

    // A released migrate --yes writes the marker again, and a released
    // binary's dotted version.
    settings
        .insert_one(doc! { "_id": "resource_tag_migration", "value": "applied" })
        .await
        .expect("marker");
    assert_eq!(d.status().await.label(), "WrittenByOlderRelease");
    let refused = d.migrate().await.expect_err("refused");
    assert_eq!(refused.exit_code(), exit::ATTENTION);
    d.reconverge().await.expect("reconverge");
    assert_eq!(d.setting("resource_tag_migration").await, None);
    assert_eq!(d.status().await, CatalogStatus::Current { version: 4 });
    settings
        .update_one(
            doc! { "_id": "catalog_version" },
            doc! { "$set": { "value": "0.0.2" } },
        )
        .await
        .expect("version");
    assert_eq!(d.status().await.label(), "WrittenByOlderRelease");
    d.reconverge().await.expect("reconverge");
    assert_eq!(d.setting("catalog_version").await.as_deref(), Some("4"));

    // A recorded checksum this binary's body does not have.
    ledger
        .update_one(
            doc! { "_id": 1_i64 },
            doc! { "$set": { "checksum": "edited" } },
        )
        .await
        .expect("checksum");
    let status = d.status().await;
    assert_eq!(status.label(), "Changed");
    assert_eq!(status.exit_code(), exit::ATTENTION);
    assert!(status.to_string().contains("edited"), "{status}");
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ledger_document_this_binary_cannot_read_stops_the_status() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "badrow").await;
    d.create().await;
    let ledger = d.data().collection::<Document>("extenddb_migrations");
    ledger
        .update_one(doc! { "_id": 1_i64 }, doc! { "$unset": { "kind": "" } })
        .await
        .expect("edit");
    let migrations = MongoMigrations::open(&d.target, None).await.expect("open");
    let error = Runner::new(&migrations, "")
        .status()
        .await
        .expect_err("an error");
    migrations.close().await;
    assert!(error.to_string().contains("has no kind"), "{error}");
    ledger
        .update_one(
            doc! { "_id": 1_i64 },
            doc! { "$set": { "kind": "sideways" } },
        )
        .await
        .expect("edit");
    let migrations = MongoMigrations::open(&d.target, None).await.expect("open");
    let error = Runner::new(&migrations, "")
        .status()
        .await
        .expect_err("an error");
    migrations.close().await;
    assert!(error.to_string().contains("\"sideways\""), "{error}");
    assert_eq!(MigrationId::LATEST.number(), 4);
    d.drop_databases().await;
}
