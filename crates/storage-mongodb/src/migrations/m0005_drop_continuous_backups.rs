// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 5, `drop_continuous_backups`, MongoDB: drop the
//! `continuous_backups` collection from the catalog database. The engine
//! answers the point-in-time recovery operations without storage (#335), so
//! no request reads or writes it. The body drops the collection only when
//! the catalog lists it, so a second run changes nothing.
//!
//! The checksum test pins this file once the migration is released. The
//! module is excluded from rustfmt and clippy so neither can require an
//! edit.

use async_trait::async_trait;
use bson::{Document, doc};
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};

use super::MongoStores;

/// This module, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0005_drop_continuous_backups.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0005_drop_continuous_backups/mongodb/1";

/// The catalog collection this migration drops.
const COLLECTION: &str = "continuous_backups";

/// Migration 5's catalog body.
pub(crate) struct DropContinuousBackups;

#[async_trait]
impl CodeBody<MongoStores> for DropContinuousBackups {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    async fn detect(&self, _stores: &MongoStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &MongoStores) -> Result<Done, MigrationError> {
        let db = &stores.catalog;
        let names = stores
            .request(format!("list the collection {}.{COLLECTION}", db.name()), async {
                db.list_collection_names().filter(doc! { "name": COLLECTION }).await
            })
            .await?;
        if !names.is_empty() {
            let collection = db.collection::<Document>(COLLECTION);
            stores
                .request(format!("drop the collection {}.{COLLECTION}", db.name()), async {
                    collection.drop().await
                })
                .await?;
        }
        Ok(Done::default())
    }
}
