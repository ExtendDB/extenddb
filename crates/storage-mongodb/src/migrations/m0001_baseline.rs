// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 1, `baseline`, MongoDB: create the v1.0 collections and indexes
//! in an empty catalog and data database, and bring every released
//! deployment to them.
//!
//! The catalog body creates the catalog collections and their indexes, the
//! `extenddb_servers` collection the serving registration lives in, and the
//! legacy ledger document `001_initial` that every released `init` wrote,
//! and deletes the `resource_tag_migration` settings row every released
//! `init` seeded. The data body creates the data database's collections and
//! indexes, which before 1.0 only `init` created. Every request is safe to
//! send again: creating a collection that exists with the same options
//! succeeds, one that exists with other options fails with NamespaceExists
//! (code 48), which is accepted by its code and left to the post-check, and
//! creating an index that exists with the same key and options succeeds.
//! Every other error stops the body, so the runner records nothing.
//!
//! The checksum test pins this file once the migration is released. The
//! module is excluded from rustfmt and clippy so neither can require an
//! edit, and keeps its index definitions here instead of calling the
//! crate's, which may change.

use std::time::Duration;

use async_trait::async_trait;
use bson::{Document, doc};
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use mongodb::IndexModel;
use mongodb::options::IndexOptions;

use super::MongoStores;

/// This module, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0001_baseline.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0001_baseline/mongodb/1";

/// NamespaceExists: `create` on a collection that exists with other options.
const NAMESPACE_EXISTS: i32 = 48;

/// The catalog collections every released `init` created, and the
/// collection the serving registration lives in.
const CATALOG_COLLECTIONS: [&str; 19] = [
    "schema_history",
    "accounts",
    "tables",
    "indexes",
    "tags",
    "settings",
    "admin_users",
    "iam_users",
    "access_keys",
    "iam_groups",
    "iam_roles",
    "iam_sessions",
    "iam_policies",
    "iam_permissions_boundaries",
    "metrics",
    "login_attempts",
    "backups",
    "continuous_backups",
    "extenddb_servers",
];

/// The data collections every released `init` created.
const DATA_COLLECTIONS: [&str; 3] = ["idempotency_tokens", "stream_shards", "stream_records"];

/// One index: its collection, key and options.
struct Index {
    collection: &'static str,
    key: Document,
    options: IndexOptions,
}

fn plain(collection: &'static str, key: Document) -> Index {
    Index { collection, key, options: IndexOptions::default() }
}

fn unique(collection: &'static str, key: Document) -> Index {
    Index { collection, key, options: IndexOptions::builder().unique(true).build() }
}

fn expiring(collection: &'static str, key: Document, after: Duration) -> Index {
    Index { collection, key, options: IndexOptions::builder().expire_after(after).build() }
}

/// The catalog indexes, as every released `init` created them, and the
/// expiry of the serving registrations.
fn catalog_indexes() -> Vec<Index> {
    vec![
        unique("accounts", doc! { "account_name": 1 }),
        unique("tables", doc! { "table_id": 1 }),
        unique("iam_users", doc! { "user_arn": 1 }),
        plain("access_keys", doc! { "account_id": 1, "user_name": 1 }),
        unique("iam_groups", doc! { "group_arn": 1 }),
        unique("iam_roles", doc! { "role_arn": 1 }),
        unique("iam_sessions", doc! { "access_key_id": 1 }),
        expiring("iam_sessions", doc! { "expires_at": 1 }, Duration::from_secs(0)),
        plain("metrics", doc! { "_id.bucket": 1 }),
        plain("login_attempts", doc! { "principal": 1, "attempted_at": 1 }),
        plain("backups", doc! { "account_id": 1, "table_name": 1 }),
        // A server that stopped without deleting its registration leaves a
        // document whose `expires_at` has passed; the server's TTL monitor
        // removes it. Nothing reads an expired registration as live.
        expiring("extenddb_servers", doc! { "expires_at": 1 }, Duration::from_secs(0)),
    ]
}

/// The data indexes, as every released `init` created them.
fn data_indexes() -> Vec<Index> {
    vec![
        // 540 s, so a token is kept for at most 10 minutes with the TTL
        // monitor's 60 s cadence.
        expiring("idempotency_tokens", doc! { "created_at": 1 }, Duration::from_secs(540)),
        // One TransactWriteItems token runs once per account.
        unique("idempotency_tokens", doc! { "account_id": 1, "token": 1 }),
        // Two stream enables cannot insert the same shard.
        unique("stream_shards", doc! { "shard_id": 1 }),
        // DynamoDB's 24-hour stream retention.
        expiring("stream_records", doc! { "created_at": 1 }, Duration::from_secs(24 * 3600)),
        plain("stream_records", doc! { "shard_id": 1, "sequence_number": 1 }),
    ]
}

/// Create `names` in `db`, accepting a collection that exists.
async fn create_collections(
    stores: &MongoStores,
    db: &mongodb::Database,
    names: &[&str],
) -> Result<(), MigrationError> {
    for name in names {
        let created = stores
            .request(format!("create the collection {}.{name}", db.name()), async {
                match db.create_collection(*name).await {
                    Err(e) if command_code(&e) == Some(NAMESPACE_EXISTS) => Ok(()),
                    other => other,
                }
            })
            .await;
        created?;
    }
    Ok(())
}

/// Create every index in `db`.
async fn create_indexes(
    stores: &MongoStores,
    db: &mongodb::Database,
    indexes: Vec<Index>,
) -> Result<(), MigrationError> {
    for index in indexes {
        let collection = db.collection::<Document>(index.collection);
        let what = format!("create the index {:?} on {}.{}", index.key, db.name(), index.collection);
        let model = IndexModel::builder().keys(index.key).options(index.options).build();
        stores.request(what, async { collection.create_index(model).await.map(|_| ()) }).await?;
    }
    Ok(())
}

/// The server's error code of a failed command, if it reported one.
fn command_code(error: &mongodb::error::Error) -> Option<i32> {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Command(command) => Some(command.code),
        _ => None,
    }
}

/// Migration 1's catalog body.
pub(crate) struct CatalogBaseline;

#[async_trait]
impl CodeBody<MongoStores> for CatalogBaseline {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    async fn detect(&self, _stores: &MongoStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &MongoStores) -> Result<Done, MigrationError> {
        let db = &stores.catalog;
        create_collections(stores, db, &CATALOG_COLLECTIONS).await?;
        create_indexes(stores, db, catalog_indexes()).await?;
        // The legacy ledger: a released `migrate --yes` that finds it runs
        // no legacy step. `$setOnInsert` keeps the date a released `init`
        // wrote.
        let history = db.collection::<Document>("schema_history");
        stores
            .request("record 001_initial in schema_history", async {
                history
                    .update_one(
                        doc! { "_id": "001_initial" },
                        doc! { "$setOnInsert": { "applied_at": bson::DateTime::now() } },
                    )
                    .upsert(true)
                    .await
                    .map(|_| ())
            })
            .await?;
        // Every released `serve` from v0.1.14 requires this row, so deleting
        // it makes them refuse this catalog.
        let settings = db.collection::<Document>("settings");
        stores
            .request("delete the resource_tag_migration settings row", async {
                settings.delete_one(doc! { "_id": "resource_tag_migration" }).await.map(|_| ())
            })
            .await?;
        Ok(Done::default())
    }
}

/// Migration 1's data body.
pub(crate) struct DataBaseline;

#[async_trait]
impl CodeBody<MongoStores> for DataBaseline {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    async fn detect(&self, _stores: &MongoStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &MongoStores) -> Result<Done, MigrationError> {
        let db = &stores.data;
        create_collections(stores, db, &DATA_COLLECTIONS).await?;
        create_indexes(stores, db, data_indexes()).await?;
        Ok(Done::default())
    }
}
