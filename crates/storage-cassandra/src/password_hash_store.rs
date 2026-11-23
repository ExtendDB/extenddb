// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PasswordHashStore` implementation for `CassandraCatalogStore`.
//!
//! Both reads run at `LOCAL_QUORUM`. The writes that change or remove these
//! hashes (`delete_admin`, `change_admin_password`, `delete_user`,
//! `change_user_password`) also commit at `LOCAL_QUORUM`, so a read issued
//! after one of them returns sees its result in the same datacenter.

use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::rows::Row;
use extenddb_storage::management_store::{OpError, OpResult, PasswordHashStore};
use futures::future::BoxFuture;

use crate::catalog_store::CassandraCatalogStore;

impl PasswordHashStore for CassandraCatalogStore {
    fn get_admin_password_hash(&self, admin_name: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let admin_name = admin_name.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "SELECT password_hash FROM {catalog_keyspace}.admin_users WHERE admin_name = ?"
            );
            let rows = crate::cassandra_util::query_rows(
                &session,
                &query,
                cdrs_tokio::query_values!(admin_name.as_str()),
                "get_admin_password_hash",
            )
            .await?;
            password_hash_of(rows, "get_admin_password_hash")
        })
    }

    fn get_user_password_hash(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "SELECT password_hash FROM {catalog_keyspace}.iam_users \
                 WHERE account_id = ? AND user_name = ?"
            );
            let rows = crate::cassandra_util::query_rows(
                &session,
                &query,
                cdrs_tokio::query_values!(account_id.as_str(), user_name.as_str()),
                "get_user_password_hash",
            )
            .await?;
            let hash = password_hash_of(rows, "get_user_password_hash")?;
            // A user of a deleted account has no password: its console
            // sessions end and it cannot log in.
            if hash.is_some() && !self.account_exists_quorum(&account_id).await? {
                return Ok(None);
            }
            Ok(hash)
        })
    }
}

/// The `password_hash` column of the first row: `None` when there is no row
/// or the column is null.
fn password_hash_of(rows: Vec<Row>, context: &str) -> OpResult<Option<String>> {
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    row.get_by_name("password_hash").map_err(|e| {
        tracing::error!("{context} parse password_hash: {e}");
        OpError::Internal("Database error".to_owned())
    })
}
