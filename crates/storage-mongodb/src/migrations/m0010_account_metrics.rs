// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 10, `account_metrics`, MongoDB: replace the catalog's `metrics`
//! collection with `account_metrics`, whose documents carry the account a
//! row was recorded for, so two accounts' tables of the same name keep their
//! own rows (#728). The body creates `account_metrics` with an index on
//! `bucket`, which the queries and the prune filter on, and drops `metrics`
//! when the catalog lists it. Its documents are dropped, not copied: they
//! carry no account, and the server keeps metrics for a day. A second run
//! changes nothing.
//!
//! The checksum test pins this file once the migration is released. The
//! module is excluded from rustfmt and clippy so neither can require an
//! edit.

use async_trait::async_trait;
use bson::{Document, doc};
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use mongodb::IndexModel;

use super::MongoStores;

/// This module, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0010_account_metrics.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0010_account_metrics/mongodb/1";

/// The server's answer to creating a collection that exists.
const NAMESPACE_EXISTS: i32 = 48;

/// The collection this migration creates.
const CREATED: &str = "account_metrics";

/// The collection this migration drops.
const DROPPED: &str = "metrics";

/// Migration 10's catalog body.
pub(crate) struct AccountMetrics;

#[async_trait]
impl CodeBody<MongoStores> for AccountMetrics {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    async fn detect(&self, _stores: &MongoStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &MongoStores) -> Result<Done, MigrationError> {
        let db = &stores.catalog;
        stores
            .request(format!("create the collection {}.{CREATED}", db.name()), async {
                match db.create_collection(CREATED).await {
                    Err(e) if command_code(&e) == Some(NAMESPACE_EXISTS) => Ok(()),
                    other => other,
                }
            })
            .await?;
        let model = IndexModel::builder().keys(doc! { "bucket": 1 }).build();
        let created = db.collection::<Document>(CREATED);
        stores
            .request(format!("create the index {{ bucket: 1 }} on {}.{CREATED}", db.name()), async {
                created.create_index(model).await.map(|_| ())
            })
            .await?;
        let names = stores
            .request(format!("list the collection {}.{DROPPED}", db.name()), async {
                db.list_collection_names().filter(doc! { "name": DROPPED }).await
            })
            .await?;
        if !names.is_empty() {
            let dropped = db.collection::<Document>(DROPPED);
            stores
                .request(format!("drop the collection {}.{DROPPED}", db.name()), async {
                    dropped.drop().await
                })
                .await?;
        }
        Ok(Done::default())
    }
}

fn command_code(error: &mongodb::error::Error) -> Option<i32> {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Command(command) => Some(command.code),
        _ => None,
    }
}
