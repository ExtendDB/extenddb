// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 6, `backup_catalog_index`, MongoDB: the catalog index of
//! backups kept in a backup store (the `backup_index` collection and its
//! indexes), the restore job record on the restore registrations (the
//! `restore_job` field of a restore target's `tables` document, and the
//! index the reconciler reads it by), the maintenance claim record (the
//! `backup_maintenance` collection) and the catalog id (the `catalog_id`
//! settings document).
//!
//! Every request is safe to send again: creating a collection that exists
//! fails with NamespaceExists (code 48), which is accepted by its code;
//! creating an index that exists with the same key and options succeeds; the
//! backfill touches only registrations without a job record; and the
//! catalog id is written with `$setOnInsert`. A second run changes nothing,
//! and a migrator stopped part way runs the body again.
//!
//! The backfill: every registration that exists was written by a release
//! without the job record, so each is a legacy restore. One whose table is
//! not CREATING, or whose copy had finished and was handed to the index
//! backfill worker (`restore_backfill_pending`), is DONE, completed when the
//! table was created. One whose table is CREATING with no hand-over is a
//! restore the upgrade's stop interrupted: RUNNING, with a heartbeat at the
//! epoch (not absent, so the staleness test selects it) and an owner
//! generation no job holds (0), so the first reconciler pass fails it and
//! removes its target. The owner and heartbeat fields of the earlier
//! registration shape are removed; the job record replaces them.
//!
//! The checksum test pins this file once the migration is released. The
//! module is excluded from rustfmt and clippy so neither can require an
//! edit, and keeps its collection, field and index names here instead of
//! reading the crate's, which may change.

use async_trait::async_trait;
use bson::{Document, doc};
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use mongodb::IndexModel;
use mongodb::options::IndexOptions;

use super::MongoStores;

/// This module, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0006_backup_catalog_index.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0006_backup_catalog_index/mongodb/1";

/// NamespaceExists: `create` on a collection that exists.
const NAMESPACE_EXISTS: i32 = 48;

/// The collections this migration creates in the catalog.
const COLLECTIONS: [&str; 2] = ["backup_index", "backup_maintenance"];

/// The indexes this migration creates in the catalog: the collection, the
/// key, and whether it is unique.
fn indexes() -> Vec<(&'static str, Document, bool)> {
    vec![
        ("backup_index", doc! { "account_id": 1, "backup_id": 1 }, true),
        ("backup_index", doc! { "backup_id": 1 }, true),
        ("backup_index", doc! { "account_id": 1, "table_name": 1, "created_at": 1 }, false),
        (
            "backup_index",
            doc! { "store_kind": 1, "store_root": 1, "store_id": 1, "status": 1 },
            false,
        ),
        ("tables", doc! { "restore_job.status": 1, "restore_job.heartbeat_at": 1 }, false),
    ]
}

/// Migration 6's catalog body.
pub(crate) struct BackupCatalogIndex;

#[async_trait]
impl CodeBody<MongoStores> for BackupCatalogIndex {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &MongoStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &MongoStores) -> Result<Done, MigrationError> {
        let db = &stores.catalog;
        for name in COLLECTIONS {
            stores
                .request(format!("create the collection {}.{name}", db.name()), async {
                    match db.create_collection(name).await {
                        Err(e) if command_code(&e) == Some(NAMESPACE_EXISTS) => Ok(()),
                        other => other,
                    }
                })
                .await?;
        }
        for (collection, key, unique) in indexes() {
            let what = format!("create the index {key:?} on {}.{collection}", db.name());
            let options = IndexOptions::builder().unique(unique.then_some(true)).build();
            let model = IndexModel::builder().keys(key).options(options).build();
            let collection = db.collection::<Document>(collection);
            stores.request(what, async { collection.create_index(model).await.map(|_| ()) }).await?;
        }

        let mut notes = Vec::new();
        let tables = db.collection::<Document>("tables");
        let interrupted = doc! { "$and": [
            { "$eq": ["$table_status", "CREATING"] },
            { "$ne": ["$restore_backfill_pending", true] },
        ] };
        let filter = doc! {
            "restore_source_backup_arn": { "$exists": true },
            "restore_job": { "$exists": false },
        };
        let running = stores
            .request(format!("count the interrupted restores in {}.tables", db.name()), async {
                let mut running = filter.clone();
                running.insert("$expr", interrupted.clone());
                tables.count_documents(running).await
            })
            .await?;
        let backfilled = stores
            .request(format!("backfill the restore job records in {}.tables", db.name()), async {
                tables
                    .update_many(
                        filter.clone(),
                        vec![
                            doc! { "$set": { "restore_job": {
                                "status": { "$cond": [interrupted.clone(), "RUNNING", "DONE"] },
                                "kind": "legacy",
                                "owner_generation": 0_i64,
                                "heartbeat_at": bson::DateTime::from_millis(0),
                                "completed_at": {
                                    "$cond": [interrupted.clone(), "$$REMOVE", "$creation_date_time"]
                                },
                            } } },
                            doc! { "$unset": ["restore_operation_id", "restore_heartbeat_at"] },
                        ],
                    )
                    .await
            })
            .await?
            .modified_count;
        if backfilled > 0 {
            notes.push(format!(
                "backfilled {backfilled} restore registration(s) as legacy restores, {running} of \
                 them RUNNING with a dead owner for the reconciler to remove"
            ));
        }

        let settings = db.collection::<Document>("settings");
        let written = stores
            .request(format!("write the catalog id in {}.settings", db.name()), async {
                settings
                    .update_one(
                        doc! { "_id": "catalog_id" },
                        doc! { "$setOnInsert": { "value": uuid::Uuid::new_v4().to_string() } },
                    )
                    .upsert(true)
                    .await
            })
            .await?
            .upserted_id
            .is_some();
        if written {
            notes.push("wrote settings.catalog_id".to_owned());
        }
        Ok(Done { notes })
    }
}

/// The server's error code of a failed command, if it reported one.
fn command_code(error: &mongodb::error::Error) -> Option<i32> {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Command(command) => Some(command.code),
        _ => None,
    }
}
