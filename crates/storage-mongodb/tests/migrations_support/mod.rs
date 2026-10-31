// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What the MongoDB migration tests share: the server, a deployment in
//! databases of its own, and the runner's entry points.
//!
//! The tests run against the replica set `EXTENDDB_TEST_MONGODB_URI` names
//! and skip without it, unless `EXTENDDB_TEST_MONGODB_REQUIRED` is `1`, which
//! CI sets so a missing server fails the job. Each deployment uses a catalog
//! and a data database named after the test, and drops them at the end.

#![allow(dead_code)]

pub mod fixture;

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bson::{Document, doc};
use extenddb_storage::migrations::{
    CatalogStatus, LedgerRow, MigrationError, MigrationId, RunReport, Runner,
};
use extenddb_storage_mongodb::migrations::{
    MigrationLock, MongoMigrations, MongoTarget, Timing, catalog_status, lock, migrate_holding,
};
use futures::TryStreamExt;

/// The server the tests run against, or `None` to skip.
///
/// # Panics
///
/// When `EXTENDDB_TEST_MONGODB_REQUIRED` is `1` and no server is named.
pub fn server() -> Option<String> {
    match std::env::var("EXTENDDB_TEST_MONGODB_URI") {
        Ok(uri) if !uri.is_empty() => Some(uri),
        _ => {
            assert!(
                std::env::var("EXTENDDB_TEST_MONGODB_REQUIRED").as_deref() != Ok("1"),
                "EXTENDDB_TEST_MONGODB_REQUIRED is 1 and EXTENDDB_TEST_MONGODB_URI names no server"
            );
            eprintln!("skipped: set EXTENDDB_TEST_MONGODB_URI to a MongoDB replica set");
            None
        }
    }
}

static NEXT: AtomicU32 = AtomicU32::new(0);

/// One deployment in databases of its own.
pub struct Deployment {
    pub target: MongoTarget,
    pub client: mongodb::Client,
}

impl Deployment {
    /// A deployment named after `tag`, whose databases do not exist yet.
    pub async fn new(uri: &str, tag: &str) -> Self {
        let n = NEXT.fetch_add(1, Ordering::SeqCst);
        let base = format!("t{}_{n}_{tag}", std::process::id());
        let target =
            MongoTarget::with_databases(uri, format!("{base}_catalog"), format!("{base}_data"));
        let client = mongodb::Client::with_uri_str(uri).await.expect("client");
        let deployment = Self { target, client };
        deployment.drop_databases().await;
        deployment
    }

    pub fn catalog(&self) -> mongodb::Database {
        self.client.database(&self.target.catalog)
    }

    pub fn data(&self) -> mongodb::Database {
        self.client.database(&self.target.data)
    }

    pub async fn drop_databases(&self) {
        self.catalog().drop().await.expect("drop catalog");
        self.data().drop().await.expect("drop data");
    }

    /// `init`'s run: take the lock on a store that is not a catalog and
    /// apply every migration.
    pub async fn create(&self) -> RunReport {
        let (held, target) = lock(
            &self.target,
            true,
            Duration::from_secs(10),
            Timing::PRODUCTION,
        )
        .await
        .expect("lock");
        let report = migrate_holding(&target, "test", &held)
            .await
            .expect("create");
        held.release().await;
        report
    }

    /// `migrate --yes`.
    pub async fn migrate(&self) -> Result<RunReport, MigrationError> {
        let (held, target) = lock(
            &self.target,
            false,
            Duration::from_secs(10),
            Timing::PRODUCTION,
        )
        .await?;
        let result = migrate_holding(&target, "test", &held).await;
        held.release().await;
        result
    }

    /// `migrate --yes --reconverge`.
    pub async fn reconverge(&self) -> Result<RunReport, MigrationError> {
        let (held, target) = lock(
            &self.target,
            false,
            Duration::from_secs(10),
            Timing::PRODUCTION,
        )
        .await?;
        let migrations = MongoMigrations::open(&target, Some(&held)).await?;
        let result = Runner::new(&migrations, "test").reconverge().await;
        migrations.close().await;
        held.release().await;
        result
    }

    /// `migrate --yes` through migration `id` only.
    pub async fn migrate_through(&self, id: MigrationId) -> Result<RunReport, MigrationError> {
        let held =
            MigrationLock::acquire(&self.target, Duration::from_secs(10), Timing::PRODUCTION)
                .await?;
        let migrations = MongoMigrations::open(&self.target, Some(&held)).await?;
        let result = Runner::new(&migrations, "test").migrate_through(id).await;
        migrations.close().await;
        held.release().await;
        result
    }

    pub async fn status(&self) -> CatalogStatus {
        catalog_status(&self.target).await.expect("status")
    }

    /// The catalog ledger's rows, in id order.
    pub async fn catalog_ledger(&self) -> Vec<LedgerRow> {
        let migrations = MongoMigrations::open(&self.target, None)
            .await
            .expect("open");
        let rows = Runner::new(&migrations, "")
            .catalog_ledger()
            .await
            .expect("ledger");
        migrations.close().await;
        rows
    }

    /// Every document of `collection` in `db`.
    pub async fn documents(db: &mongodb::Database, collection: &str) -> Vec<Document> {
        db.collection::<Document>(collection)
            .find(doc! {})
            .sort(doc! { "_id": 1 })
            .await
            .expect("find")
            .try_collect()
            .await
            .expect("collect")
    }

    /// The `value` of a catalog settings document, if it exists.
    pub async fn setting(&self, key: &str) -> Option<String> {
        self.catalog()
            .collection::<Document>("settings")
            .find_one(doc! { "_id": key })
            .await
            .expect("setting")
            .map(|d| d.get_str("value").unwrap_or_default().to_owned())
    }

    /// The collection names of `db`, sorted.
    pub async fn collections(db: &mongodb::Database) -> Vec<String> {
        let mut names = db.list_collection_names().await.expect("names");
        names.sort();
        names
    }
}
