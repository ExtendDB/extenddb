// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB test of the catalog step `extenddb migrate --yes` runs in
//! `run_data_migrations`: dropping the `continuous_backups` collection that a
//! catalog created by an earlier build holds.
//!
//! It reads the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and skips
//! without it; with `EXTENDDB_TEST_MONGODB_REQUIRED` set a missing URI is a
//! failure. It works in a catalog database of its own (a random name, opened
//! with [`crate::catalog_database_named`] and dropped at the end), never in
//! `extenddb_catalog`.

use extenddb_storage::bootstrapper::Bootstrapper;

use super::{MongoBootstrapper, STALE_CONTINUOUS_BACKUPS};

/// The replica set URI, or `None` to skip.
fn uri() -> Option<String> {
    let uri = std::env::var("EXTENDDB_TEST_MONGODB_URI")
        .ok()
        .filter(|u| !u.trim().is_empty());
    assert!(
        uri.is_some() || std::env::var_os("EXTENDDB_TEST_MONGODB_REQUIRED").is_none(),
        "EXTENDDB_TEST_MONGODB_REQUIRED is set but EXTENDDB_TEST_MONGODB_URI is not; the \
         MongoDB bootstrapper test must run, not skip"
    );
    uri
}

/// A catalog that holds the collection is reported as pending, `migrate
/// --yes` drops it, and then nothing is pending and a second run drops
/// nothing.
#[tokio::test]
async fn live_migrate_drops_the_continuous_backups_collection() {
    let Some(uri) = uri() else {
        return;
    };
    let client = mongodb::Client::with_uri_str(&uri).await.expect("client");
    let name = format!("t{}_cat", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let catalog = crate::catalog_database_named(&client, &name);
    let bootstrapper = MongoBootstrapper {
        client: client.clone(),
        connection_string: uri,
        catalog: catalog.clone(),
    };
    catalog
        .create_collection(STALE_CONTINUOUS_BACKUPS)
        .await
        .expect("create the collection an earlier build created");

    let pending_before = bootstrapper.pending_data_migrations().await;
    let first_run = bootstrapper.run_data_migrations().await;
    let left = catalog.list_collection_names().await;
    let pending_after = bootstrapper.pending_data_migrations().await;
    let second_run = bootstrapper.run_data_migrations().await;
    catalog.drop().await.expect("drop the test catalog");

    assert_eq!(
        pending_before.expect("pending_data_migrations before the run"),
        ["catalog: drop the continuous_backups collection"]
    );
    first_run.expect("run_data_migrations");
    let left = left.expect("list_collection_names");
    assert!(
        !left.iter().any(|c| c == STALE_CONTINUOUS_BACKUPS),
        "the run left {STALE_CONTINUOUS_BACKUPS}: {left:?}"
    );
    assert_eq!(
        pending_after.expect("pending_data_migrations after the run"),
        Vec::<String>::new()
    );
    second_run.expect("a second run with nothing to drop");
}
