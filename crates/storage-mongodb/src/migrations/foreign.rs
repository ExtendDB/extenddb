// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What a catalog database that is not a catalog holds that makes it another
//! program's, which `init` refuses before it writes anything.

use bson::{Bson, Document, doc};
use extenddb_storage::migrations::{MigrationBackend, MigrationError, StoreKind, StoreRef};
use futures::TryStreamExt;

use super::postcheck::{self, Definitions};
use super::{CATALOG_EXPECTED, LEDGER, LOCK_ID, MongoMigrations, MongoStores, MongoTarget};

/// The `schema_history` document migration 1's catalog body writes.
const BASELINE_HISTORY: &str = "001_initial";

/// What the catalog database of `target` holds that makes it another
/// program's: the collections migration 1 does not create; a collection of
/// a catalog collection's name that is of another type, has options
/// migration 1 does not give it, or has an index migration 1 does not create
/// on it; `settings` documents other than the migration lock's;
/// `schema_history` documents other than the one migration 1 writes; and a
/// document in any other catalog collection. Empty for a database that does
/// not exist, and for one an `init` created and stopped in before migration
/// 1 recorded anything: that holds only collections migration 1 creates,
/// with their options and some of their indexes, at most the lock document
/// in `settings` and migration 1's `schema_history` document. A collection
/// name alone shows nothing: the catalog's names (`metrics`, `tags`,
/// `tables`, `accounts`) are common ones. Call it only for a database whose
/// status is `Uninitialized`, since a catalog's collections hold documents.
///
/// # Errors
///
/// A connection or a read failed.
pub async fn foreign_contents(target: &MongoTarget) -> Result<Vec<String>, MigrationError> {
    let migrations = MongoMigrations::open(target, None).await?;
    let found = contents(migrations.stores()).await;
    migrations.close().await;
    found
}

/// [`foreign_contents`] when the catalog database of `target` is not a
/// catalog, and nothing when it is one. It is not a catalog when its ledger
/// holds no row and its `settings` no `catalog_version` document, which is
/// how the status decides `Uninitialized`, read here without the status's
/// other reads: `init` calls this before it takes the lock.
///
/// # Errors
///
/// A connection or a read failed.
pub(crate) async fn foreign_unless_a_catalog(
    target: &MongoTarget,
) -> Result<Vec<String>, MigrationError> {
    let migrations = MongoMigrations::open(target, None).await?;
    let found = async {
        let catalog = StoreRef {
            kind: StoreKind::Catalog,
            name: target.catalog.clone(),
        };
        if !migrations.read_ledger(&catalog).await?.is_empty()
            || migrations.read_traces().await?.catalog_version.is_some()
        {
            return Ok(Vec::new());
        }
        contents(migrations.stores()).await
    }
    .await;
    migrations.close().await;
    found
}

async fn contents(stores: &MongoStores) -> Result<Vec<String>, MigrationError> {
    let db = &stores.catalog;
    let expected: Definitions = serde_json::from_str(CATALOG_EXPECTED)
        .map_err(|e| MigrationError::Store(format!("read the expected definitions: {e}")))?;
    let mut names = stores
        .request(format!("list the collections of {}", db.name()), async {
            db.list_collection_names().await
        })
        .await?;
    names.sort();
    // `system.views` exists when the database has a view; the view itself is
    // named below when it has a catalog collection's name, and here when not.
    let mut foreign: Vec<String> = names
        .iter()
        .filter(|n| *n != LEDGER && !n.starts_with("system.") && !expected.contains_key(*n))
        .map(|n| format!("the collection {n}"))
        .collect();
    let ours: Vec<String> = names
        .into_iter()
        .filter(|n| expected.contains_key(n))
        .collect();
    let actual = postcheck::describe(stores, db, &ours).await?;
    for (name, have) in &actual {
        let Some(want) = expected.get(name) else {
            continue;
        };
        if have.kind != want.kind || have.options != want.options {
            foreign.push(format!(
                "the collection {name}, a {} with options {} that migration 1 does not create",
                have.kind, have.options
            ));
            continue;
        }
        if let Some(index) = have
            .indexes
            .iter()
            .find(|(index, definition)| want.indexes.get(*index) != Some(*definition))
            .map(|(index, _)| index)
        {
            foreign.push(format!(
                "the collection {name}, with the index {index} migration 1 does not create"
            ));
            continue;
        }
        let ours_alone = match name.as_str() {
            "settings" => Some(LOCK_ID),
            "schema_history" => Some(BASELINE_HISTORY),
            _ => None,
        };
        let filter = ours_alone.map_or_else(Document::new, |id| doc! { "_id": { "$ne": id } });
        let collection = db.collection::<Document>(name);
        let found = stores
            .request(
                format!("read the documents of {}.{name}", db.name()),
                async {
                    collection
                        .find(filter)
                        .projection(doc! { "_id": 1 })
                        .limit(3)
                        .await?
                        .try_collect::<Vec<Document>>()
                        .await
                },
            )
            .await?;
        if found.is_empty() {
            continue;
        }
        if ours_alone.is_none() {
            foreign.push(format!("documents in the collection {name}"));
            continue;
        }
        for document in &found {
            let id = match document.get("_id") {
                Some(Bson::String(id)) => id.clone(),
                Some(other) => other.to_string(),
                None => String::new(),
            };
            foreign.push(format!("the {name} document '{id}'"));
        }
    }
    Ok(foreign)
}
