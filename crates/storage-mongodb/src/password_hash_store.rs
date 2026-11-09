// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PasswordHashStore` implementation for `MongoCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult, PasswordHashStore};
use futures::future::BoxFuture;
use mongodb::bson::{Document, doc};

use crate::catalog_store::MongoCatalogStore;

impl PasswordHashStore for MongoCatalogStore {
    fn get_admin_password_hash(&self, admin_name: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let filter = doc! { "_id": admin_name };
        Box::pin(async move {
            find_password_hash(self, "admin_users", filter, "get_admin_password_hash").await
        })
    }

    fn get_user_password_hash(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let filter = doc! { "account_id": account_id, "user_name": user_name };
        Box::pin(async move {
            find_password_hash(self, "iam_users", filter, "get_user_password_hash").await
        })
    }
}

/// The string `password_hash` field of the document matching `filter`:
/// `None` when no document matches or the field is absent or null.
async fn find_password_hash(
    store: &MongoCatalogStore,
    collection: &str,
    filter: Document,
    context: &str,
) -> OpResult<Option<String>> {
    let doc = store
        .catalog_db()
        .collection::<Document>(collection)
        .find_one(filter)
        .await
        .map_err(|e| {
            tracing::error!("{context}: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
    Ok(doc.and_then(|d| d.get_str("password_hash").ok().map(str::to_owned)))
}
