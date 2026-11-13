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
use migrations_support::{ACCESS_KEY_ID_INDEX, Deployment, server};

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

/// `init` takes the migration lock on a store that is not a catalog, and
/// taking it writes the lock document into the catalog database's
/// `settings`. A database that holds another program's collections or
/// `settings` documents is refused before that write, naming them, and
/// nothing is written; so is one with a catalog collection of another shape
/// or holding documents. A database an `init` stopped in before migration 1
/// recorded anything (the lock document, migration 1's collections and its
/// `schema_history` document) is taken.
#[tokio::test(flavor = "multi_thread")]
async fn init_refuses_another_programs_database_before_it_writes_anything() {
    let Some(uri) = server() else { return };
    let other = Deployment::new(&uri, "foreign").await;
    other
        .catalog()
        .collection::<Document>("orders")
        .insert_one(doc! { "order": 1 })
        .await
        .expect("insert");
    other
        .catalog()
        .collection::<Document>("settings")
        .insert_one(doc! { "_id": "theme", "value": "dark" })
        .await
        .expect("insert");
    let before = Deployment::documents(&other.catalog(), "settings").await;
    let refused = lock(
        &other.target,
        true,
        Duration::from_secs(2),
        Timing::PRODUCTION,
    )
    .await
    .err()
    .expect("refused");
    let message = refused.to_string();
    assert!(
        message.contains(&format!(
            "the database {} is not an ExtendDB catalog: it holds the collection orders, the \
             settings document 'theme'. Nothing was written to it.",
            other.target.catalog
        )),
        "{message}"
    );
    assert_eq!(refused.exit_code(), exit::ERROR);
    assert_eq!(
        Deployment::collections(&other.catalog()).await,
        ["orders", "settings"]
    );
    assert_eq!(
        Deployment::documents(&other.catalog(), "settings").await,
        before,
        "no lock document was written"
    );
    assert!(Deployment::collections(&other.data()).await.is_empty());

    // Catalog names alone: a capped `tags` and a `metrics` with a document.
    let named = Deployment::new(&uri, "named").await;
    named
        .catalog()
        .create_collection("tags")
        .capped(true)
        .size(4096)
        .await
        .expect("create");
    named
        .catalog()
        .collection::<Document>("metrics")
        .insert_one(doc! { "reading": 1.5 })
        .await
        .expect("insert");
    let message = lock(
        &named.target,
        true,
        Duration::from_secs(2),
        Timing::PRODUCTION,
    )
    .await
    .err()
    .expect("refused")
    .to_string();
    assert!(
        message.contains("documents in the collection metrics")
            && message.contains("the collection tags, a collection with options"),
        "{message}"
    );
    assert!(
        Deployment::documents(&named.catalog(), "settings")
            .await
            .is_empty()
    );

    // What an `init` killed before migration 1's ledger row leaves, once its
    // lock's lease has passed.
    let stopped = Deployment::new(&uri, "stopped").await;
    stopped
        .catalog()
        .collection::<Document>("settings")
        .insert_one(doc! {
            "_id": "migration_lock",
            "owner": "a killed init",
            "expires_at": bson::DateTime::from_millis(0),
        })
        .await
        .expect("insert");
    let expected: Definitions = serde_json::from_str(CATALOG_EXPECTED).expect("expected");
    for name in expected.keys() {
        if name != "settings" {
            stopped
                .catalog()
                .create_collection(name)
                .await
                .expect("create");
        }
    }
    stopped
        .catalog()
        .collection::<Document>("schema_history")
        .insert_one(doc! { "_id": "001_initial" })
        .await
        .expect("insert");
    assert_eq!(stopped.status().await, CatalogStatus::Uninitialized);
    let (held, _) = lock(
        &stopped.target,
        true,
        Duration::from_secs(60),
        Timing::PRODUCTION,
    )
    .await
    .expect("init takes the database");
    held.release().await;
    for d in [other, named, stopped] {
        d.drop_databases().await;
    }
}

/// An `init` stopped after its migrations and its encryption key, before
/// its default account and its admin user, leaves a catalog whose status is
/// `Current` and that has no admin user. The next `init`'s steps under the
/// lock run no migration, keep the key and write the account and the admin
/// user; after that a catalog with an admin user is not finished, and
/// `init` refuses it as initialized.
#[tokio::test(flavor = "multi_thread")]
async fn init_finishes_a_catalog_stopped_before_its_admin_user() {
    use extenddb_storage::bootstrapper::{
        Bootstrapper, finishes_a_stopped_init, write_bootstrap_rows,
    };
    use extenddb_storage_mongodb::MongoBootstrapper;
    use extenddb_storage_mongodb::migrations::status_holding;

    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "finish").await;
    let bootstrapper = MongoBootstrapper::for_target(&d.target)
        .await
        .expect("bootstrapper");
    d.create().await;
    bootstrapper.bootstrap_encryption_key().await.expect("key");
    let key = d.setting("encryption_key").await.expect("a key");
    assert!(!bootstrapper.has_admin_user().await.expect("read"));

    let (held, target) = lock(&d.target, true, Duration::from_secs(10), Timing::PRODUCTION)
        .await
        .expect("lock");
    let status = status_holding(&target, &held).await.expect("status");
    assert_eq!(
        status,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    assert!(
        finishes_a_stopped_init(&bootstrapper, &status)
            .await
            .expect("decided")
    );
    let admin = write_bootstrap_rows(&bootstrapper, None, None)
        .await
        .expect("rows");
    bootstrapper
        .record_data_connection()
        .await
        .expect("data connection");
    held.release().await;
    assert_eq!(admin.username, "admin");
    assert!(!admin.already_existed && admin.generated_password.is_some());
    assert_eq!(
        d.setting("encryption_key").await,
        Some(key),
        "the key is kept"
    );
    assert_eq!(
        Deployment::documents(&d.catalog(), "accounts").await.len(),
        1
    );
    assert_eq!(
        Deployment::documents(&d.catalog(), "admin_users")
            .await
            .len(),
        1
    );
    assert!(bootstrapper.has_admin_user().await.expect("read"));
    assert!(
        !finishes_a_stopped_init(&bootstrapper, &d.status().await)
            .await
            .expect("decided"),
        "a catalog with an admin user is refused"
    );
    d.drop_databases().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_store_gets_every_migration_and_the_expected_definitions() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "fresh").await;
    let report = d.create().await;
    assert_eq!(
        (report.from, report.to),
        (None, Some(MigrationId::LATEST.number()))
    );
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
            (5, d.target.catalog.clone()),
            (6, d.target.catalog.clone()),
            (7, d.target.catalog.clone()),
            (8, d.target.catalog.clone()),
            (9, d.target.catalog.clone()),
            (10, d.target.catalog.clone()),
            (11, d.target.catalog.clone()),
            (12, d.target.catalog.clone()),
        ]
    );
    let not_applicable: Vec<u32> = report
        .applied
        .iter()
        .filter(|s| s.outcome == Outcome::NotApplicable)
        .map(|s| s.id)
        .collect();
    assert_eq!(not_applicable, [3, 4, 7, 8, 9, 11], "{report:?}");
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    let rows = d.catalog_ledger().await;
    let outcomes: Vec<(u32, Outcome)> = rows.iter().map(|r| (r.id, r.outcome)).collect();
    assert_eq!(
        outcomes,
        [
            (1, Outcome::Applied),
            (2, Outcome::Applied),
            (3, Outcome::NotApplicable),
            (4, Outcome::NotApplicable),
            (5, Outcome::Applied),
            (6, Outcome::Applied),
            (7, Outcome::NotApplicable),
            (8, Outcome::NotApplicable),
            (9, Outcome::NotApplicable),
            (10, Outcome::Applied),
            (11, Outcome::NotApplicable),
            (12, Outcome::Applied),
        ]
    );
    assert!(
        rows.iter()
            .all(|r| r.applied_by == "test" && !r.applied_at.is_empty())
    );
    let data_rows = Deployment::documents(&d.data(), "extenddb_migrations").await;
    assert_eq!(data_rows.len(), 1);
    assert_eq!(data_rows[0].get_i64("_id"), Ok(1));
    assert_eq!(
        d.setting("catalog_version").await,
        Some(MigrationId::LATEST.number().to_string())
    );
    assert_eq!(d.setting("resource_tag_migration").await, None);
    let history = Deployment::documents(&d.catalog(), "schema_history").await;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].get_str("_id"), Ok("001_initial"));

    let (catalog, data) = definitions(&d).await;
    if std::env::var("EXTENDDB_BLESS_EXPECTED").as_deref() == Ok("1") {
        let dir = env!("CARGO_MANIFEST_DIR");
        // Migration 1's file keeps the collections migrations 5 and 10 drop
        // and leaves out what migrations 6 and 10 add, which their own files
        // hold, and the index migration 12 creates.
        let committed: Definitions = serde_json::from_str(CATALOG_EXPECTED).expect("catalog");
        let added: Definitions =
            serde_json::from_str(migrations_support::CATALOG_MIGRATION_6).expect("migration 6");
        let mut catalog_at_1 = catalog.clone();
        for (name, collection) in &added {
            match committed.get(name) {
                Some(before) => {
                    let mut kept = catalog_at_1[name].clone();
                    kept.indexes
                        .retain(|index, _| !collection.indexes.contains_key(index));
                    assert_eq!(&kept, before, "{name}: migration 1's definition");
                    catalog_at_1.insert(name.clone(), kept);
                }
                None => {
                    catalog_at_1.remove(name);
                }
            }
        }
        for name in ["continuous_backups", "metrics"] {
            if let Some(dropped) = committed.get(name) {
                catalog_at_1.insert(name.to_owned(), dropped.clone());
            }
        }
        if let Some(access_keys) = catalog_at_1.get_mut("access_keys") {
            access_keys.indexes.remove(ACCESS_KEY_ID_INDEX);
        }
        let at_10: Definitions =
            serde_json::from_str::<Definitions>(migrations_support::CATALOG_MIGRATION_10)
                .expect("migration 10")
                .keys()
                .map(|name| {
                    catalog_at_1.remove(name);
                    (name.clone(), catalog[name].clone())
                })
                .collect();
        let mut at_6 = Definitions::new();
        for (name, collection) in &added {
            let mut found = catalog[name].clone();
            if committed.contains_key(name) {
                found
                    .indexes
                    .retain(|index, _| collection.indexes.contains_key(index));
            }
            at_6.insert(name.clone(), found);
        }
        for (path, defs) in [
            (
                "migrations/catalog/0001_baseline.expected.json",
                &catalog_at_1,
            ),
            ("migrations/data/0001_baseline.expected.json", &data),
            (
                "tests/fixtures/0006_backup_catalog_index.expected.json",
                &at_6,
            ),
            ("tests/fixtures/0010_account_metrics.expected.json", &at_10),
        ] {
            let text = serde_json::to_string_pretty(defs).expect("json") + "\n";
            std::fs::write(format!("{dir}/{path}"), text).expect("write");
        }
    }
    let expected_catalog = migrations_support::expected_catalog();
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
    assert_eq!(
        (again.from, again.to),
        (
            Some(MigrationId::LATEST.number()),
            Some(MigrationId::LATEST.number())
        )
    );

    let reconverged = d.reconverge().await.expect("reconverge");
    // Migration 1's steps in the catalog and the data database, and
    // migrations 5's, 6's, 10's and 12's in the catalog: 2 is not
    // convergent, and 3, 4, 7, 8, 9 and 11 do not apply on MongoDB.
    let ran: Vec<(u32, String)> = reconverged
        .applied
        .iter()
        .map(|step| (step.id, step.store_name.clone()))
        .collect();
    assert_eq!(
        ran,
        [
            (1, d.target.catalog.clone()),
            (1, d.target.data.clone()),
            (5, d.target.catalog.clone()),
            (6, d.target.catalog.clone()),
            (10, d.target.catalog.clone()),
            (12, d.target.catalog.clone()),
        ],
        "{reconverged:?}"
    );
    assert!(
        reconverged
            .notes
            .iter()
            .any(|n| n.starts_with("Skipped 2 resource_tag_condition_keys:")),
        "{reconverged:?}"
    );
    assert_eq!(
        d.catalog_ledger().await,
        before,
        "reconverge leaves the rows as they are"
    );
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    d.drop_databases().await;
}

/// A data database migrated by a build whose migration 1 did not create the
/// `{table_id, shard_id}` index on `stream_shards`: its ledgers record every
/// migration, so `migrate --yes` applies nothing, and `migrate --yes
/// --reconverge` runs migration 1's data step again, which creates the index
/// and leaves the ledger rows as they are.
#[tokio::test(flavor = "multi_thread")]
async fn reconverge_adds_the_stream_shards_table_index_to_a_migrated_data_database() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "shardrerun").await;
    d.create().await;
    let shards = d.data().collection::<Document>("stream_shards");
    let names = || async {
        shards
            .list_index_names()
            .await
            .expect("stream_shards index names")
    };
    assert!(names().await.iter().any(|n| n == "table_id_1_shard_id_1"));
    shards
        .drop_index("table_id_1_shard_id_1")
        .await
        .expect("drop the index, as a build without it left the store");
    let before = d.catalog_ledger().await;
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    let again = d.migrate().await.expect("migrate");
    assert!(again.applied.is_empty(), "{again:?}");
    assert!(!names().await.iter().any(|n| n == "table_id_1_shard_id_1"));

    let reconverged = d.reconverge().await.expect("reconverge");
    assert!(
        reconverged
            .applied
            .iter()
            .any(|step| step.id == 1 && step.store_name == d.target.data),
        "{reconverged:?}"
    );
    let indexes: Vec<Document> = {
        use futures::TryStreamExt;
        shards
            .list_indexes()
            .await
            .expect("list the stream_shards indexes")
            .map_ok(|m| m.keys)
            .try_collect()
            .await
            .expect("read the stream_shards indexes")
    };
    assert!(
        indexes.contains(&doc! { "table_id": 1, "shard_id": 1 }),
        "{indexes:?}"
    );
    assert_eq!(d.catalog_ledger().await, before);
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
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
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
    ledger
        .insert_one(row(13, "breaking"))
        .await
        .expect("insert");
    let status = d.status().await;
    assert_eq!(status.label(), "AheadIncompatible");
    assert_eq!(status.exit_code(), exit::AHEAD_INCOMPATIBLE);
    ledger
        .delete_one(doc! { "_id": 13_i64 })
        .await
        .expect("delete");
    ledger
        .insert_one(row(13, "additive"))
        .await
        .expect("insert");
    assert_eq!(d.status().await.label(), "AheadCompatible");
    assert!(d.migrate().await.expect("migrate").applied.is_empty());
    ledger
        .delete_one(doc! { "_id": 13_i64 })
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
    assert_eq!(
        d.status().await,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );
    settings
        .update_one(
            doc! { "_id": "catalog_version" },
            doc! { "$set": { "value": "0.0.2" } },
        )
        .await
        .expect("version");
    assert_eq!(d.status().await.label(), "WrittenByOlderRelease");
    d.reconverge().await.expect("reconverge");
    assert_eq!(
        d.setting("catalog_version").await,
        Some(MigrationId::LATEST.number().to_string())
    );

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
    assert_eq!(MigrationId::LATEST.number(), 12);
    d.drop_databases().await;
}
