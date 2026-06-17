// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AdminStore` implementation for `PostgresCatalogStore`.

use extenddb_storage::management_store::{AdminEntry, OpError, OpResult};
use futures::future::BoxFuture;

use super::catalog_store::PostgresCatalogStore;
use super::pg_util::is_unique_violation;

impl extenddb_storage::management_store::AdminStore for PostgresCatalogStore {
    fn create_admin(&self, admin_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            let result =
                sqlx::query("INSERT INTO admin_users (admin_name, password_hash) VALUES ($1, $2)")
                    .bind(&admin_name)
                    .bind(&password_hash)
                    .execute(self.pool())
                    .await;
            match result {
                Ok(_) => Ok(()),
                Err(e) if is_unique_violation(&e) => Err(OpError::AlreadyExists(
                    "Admin user already exists".to_owned(),
                )),
                Err(e) => {
                    tracing::error!("create_admin failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
        })
    }

    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
        Box::pin(async move {
            let rows: Vec<(String, time::OffsetDateTime)> = sqlx::query_as(
                "SELECT admin_name, created_at FROM admin_users ORDER BY admin_name",
            )
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("list_admins: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows
                .into_iter()
                .map(|(admin_name, created_at)| AdminEntry {
                    admin_name,
                    created_at,
                })
                .collect())
        })
    }

    fn delete_admin(&self, admin_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        Box::pin(async move {
            let result = sqlx::query("DELETE FROM admin_users WHERE admin_name = $1")
                .bind(&admin_name)
                .execute(self.pool())
                .await;
            match result {
                Ok(r) if r.rows_affected() == 0 => {
                    Err(OpError::NotFound("Admin user not found".to_owned()))
                }
                Ok(_) => Ok(()),
                Err(e) => {
                    tracing::error!("delete_admin failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
        })
    }

    fn change_admin_password(
        &self,
        admin_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            let result =
                sqlx::query("UPDATE admin_users SET password_hash = $1 WHERE admin_name = $2")
                    .bind(&password_hash)
                    .bind(&admin_name)
                    .execute(self.pool())
                    .await;
            match result {
                Ok(r) if r.rows_affected() == 0 => {
                    Err(OpError::NotFound("Admin user not found".to_owned()))
                }
                Ok(_) => Ok(()),
                Err(e) => {
                    tracing::error!("change_admin_password failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
        })
    }

    fn verify_admin_password(
        &self,
        admin_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<Option<bool>>> {
        let admin_name = admin_name.to_owned();
        let password = password.to_owned();
        Box::pin(async move {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT password_hash FROM admin_users WHERE admin_name = $1")
                    .bind(&admin_name)
                    .fetch_optional(self.pool())
                    .await
                    .map_err(|e| {
                        tracing::error!("verify_admin_password: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            let Some((hash,)) = row else {
                return Ok(None);
            };
            Ok(Some(verify_bcrypt(password, hash).await))
        })
    }
}

/// Verify a bcrypt password on a blocking thread (same logic as `server::password`).
async fn verify_bcrypt(password: String, hash: String) -> bool {
    tokio::task::spawn_blocking(move || bcrypt::verify(password, &hash).unwrap_or(false))
        .await
        .unwrap_or(false)
}
