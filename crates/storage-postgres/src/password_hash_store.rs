// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PasswordHashStore` implementation for `PostgresCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult, PasswordHashStore};
use futures::future::BoxFuture;

use crate::catalog_store::PostgresCatalogStore;

impl PasswordHashStore for PostgresCatalogStore {
    fn get_admin_password_hash(&self, admin_name: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let admin_name = admin_name.to_owned();
        Box::pin(async move {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT password_hash FROM admin_users WHERE admin_name = $1")
                    .bind(&admin_name)
                    .fetch_optional(self.pool())
                    .await
                    .map_err(|e| {
                        tracing::error!("get_admin_password_hash: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            Ok(row.map(|(hash,)| hash))
        })
    }

    fn get_user_password_hash(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let row: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT password_hash FROM iam_users WHERE account_id = $1 AND user_name = $2",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("get_user_password_hash: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.and_then(|(hash,)| hash))
        })
    }
}
