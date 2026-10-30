// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PasswordHashStore` implementation for `SqliteCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult, PasswordHashStore};
use futures::future::BoxFuture;

use crate::catalog_store::SqliteCatalogStore;

impl PasswordHashStore for SqliteCatalogStore {
    fn get_admin_password_hash(&self, admin_name: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let admin_name = admin_name.to_owned();
        Box::pin(async move {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT password_hash FROM admin_users WHERE admin_name = ?")
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
                "SELECT password_hash FROM iam_users WHERE account_id = ? AND user_name = ?",
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

#[cfg(test)]
mod tests {
    use extenddb_storage::management_store::{AdminStore, ManagementStore, PasswordHashStore};
    use sqlx::sqlite::SqlitePoolOptions;

    use crate::catalog_store::SqliteCatalogStore;

    const ACCOUNT: &str = "111111111111";

    async fn store() -> SqliteCatalogStore {
        // One connection keeps the in-memory database alive for the test.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await
            .unwrap();
        crate::schema::apply(&pool).await.expect("apply schema");
        SqliteCatalogStore::new(pool)
    }

    #[tokio::test]
    async fn admin_hash_follows_password_change_and_delete() {
        let s = store().await;
        assert_eq!(s.get_admin_password_hash("ops").await.unwrap(), None);
        s.create_admin("ops", "hash-1").await.unwrap();
        let hash = s.get_admin_password_hash("ops").await.unwrap();
        assert_eq!(hash.as_deref(), Some("hash-1"));
        s.change_admin_password("ops", "hash-2").await.unwrap();
        let hash = s.get_admin_password_hash("ops").await.unwrap();
        assert_eq!(hash.as_deref(), Some("hash-2"));
        s.delete_admin("ops").await.unwrap();
        assert_eq!(s.get_admin_password_hash("ops").await.unwrap(), None);
    }

    #[tokio::test]
    async fn user_hash_follows_password_change_and_delete() {
        let s = store().await;
        s.create_account(ACCOUNT, "acct").await.unwrap();
        s.create_user(ACCOUNT, "bob", None).await.unwrap();
        // A user without a console password has no hash.
        assert_eq!(
            s.get_user_password_hash(ACCOUNT, "bob").await.unwrap(),
            None
        );
        s.change_user_password(ACCOUNT, "bob", "hash-1")
            .await
            .unwrap();
        let hash = s.get_user_password_hash(ACCOUNT, "bob").await.unwrap();
        assert_eq!(hash.as_deref(), Some("hash-1"));
        s.delete_user(ACCOUNT, "bob").await.unwrap();
        assert_eq!(
            s.get_user_password_hash(ACCOUNT, "bob").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn user_hash_is_gone_after_account_delete() {
        let s = store().await;
        s.create_account(ACCOUNT, "acct").await.unwrap();
        s.create_user(ACCOUNT, "bob", Some("hash-1")).await.unwrap();
        s.delete_account(ACCOUNT).await.unwrap();
        assert_eq!(
            s.get_user_password_hash(ACCOUNT, "bob").await.unwrap(),
            None
        );
    }
}
