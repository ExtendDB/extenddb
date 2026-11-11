// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The metrics store on a catalog that has run every migration (#728):
//! `extenddb_storage::metrics_conformance`, and migration 10's replacement of
//! the catalog's `metrics` collection with an empty `account_metrics`. They
//! need a replica set named by `EXTENDDB_TEST_MONGODB_URI`, and skip without
//! one.

mod migrations_support;

use bson::doc;
use extenddb_storage::migrations::MigrationId;
use extenddb_storage_mongodb::MongoCatalogStore;
use migrations_support::{Deployment, server};

/// Two accounts' tables of the same name keep their own rows, and the
/// account filter returns only that account's.
#[tokio::test(flavor = "multi_thread")]
async fn the_metrics_store_keeps_two_accounts_apart() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "metrics").await;
    d.create().await;
    let store = MongoCatalogStore::on_database_for_tests(d.client.clone(), d.catalog());
    extenddb_storage::metrics_conformance::run(&store).await;
    d.drop_databases().await;
}

/// Migration 10 drops the `metrics` collection, documents and all, and
/// creates `account_metrics`; running it again keeps what was written since.
#[tokio::test(flavor = "multi_thread")]
async fn migration_10_replaces_metrics_with_an_empty_account_metrics() {
    let Some(uri) = server() else { return };
    let d = Deployment::new(&uri, "m10").await;
    d.migrate_through(MigrationId::WarmThroughput)
        .await
        .expect("migrate through 9");
    let catalog = d.catalog();
    catalog
        .collection::<bson::Document>("metrics")
        .insert_one(doc! { "bucket": bson::DateTime::now(), "metric": "UserErrors" })
        .await
        .expect("a metrics document");
    d.migrate().await.expect("migrate");
    let collections = Deployment::collections(&catalog).await;
    assert!(
        !collections.contains(&"metrics".to_owned()),
        "{collections:?}"
    );
    assert!(
        Deployment::documents(&catalog, "account_metrics")
            .await
            .is_empty()
    );

    catalog
        .collection::<bson::Document>("account_metrics")
        .insert_one(doc! { "bucket": bson::DateTime::now(), "account_id": "111111111111" })
        .await
        .expect("an account_metrics document");
    d.reconverge().await.expect("reconverge");
    let collections = Deployment::collections(&catalog).await;
    assert!(
        !collections.contains(&"metrics".to_owned()),
        "{collections:?}"
    );
    assert_eq!(
        Deployment::documents(&catalog, "account_metrics")
            .await
            .len(),
        1
    );
    d.drop_databases().await;
}
