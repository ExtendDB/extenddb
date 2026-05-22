// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Admin user management for `SqliteCatalogStore`.

use extenddb_storage::management_store::{AdminEntry, OpError, OpResult};
use futures::future::BoxFuture;

use crate::catalog_store::SqliteCatalogStore;

impl extenddb_storage::management_store::AdminStore for SqliteCatalogStore {
    fn create_admin(&self, admin_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            let result = sqlx::query(
                "INSERT INTO admin_users (admin_name, password_hash) VALUES (?, ?)",
            )
            .bind(&admin_name)
            .bind(&password_hash)
            .execute(&pool)
            .await;
            match result {
                Ok(_) => Ok(()),
                Err(e) if crate::sqlite_util::is_unique_violation(&e) => {
                    Err(OpError::AlreadyExists("Admin user already exists".to_owned()))
                }
                Err(e) => {
                    tracing::error!("create_admin failed: {e}");
                    Err(OpError::Internal("Database error".to_owned()))
                }
            }
        })
    }

    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
        let pool = self.pool().clone();
        Box::pin(async move {
            let rows: Vec<(String, String)> =
                sqlx::query_as("SELECT admin_name, created_at FROM admin_users ORDER BY admin_name")
                    .fetch_all(&pool)
                    .await
                    .map_err(|e| {
                        tracing::error!("list_admins: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            Ok(rows
                .into_iter()
                .filter_map(|(name, created_at_str)| {
                    let created_at = crate::sqlite_util::parse_timestamp(&created_at_str).ok()?;
                    Some(AdminEntry { admin_name: name, created_at })
                })
                .collect())
        })
    }

    fn delete_admin(&self, admin_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            let result = sqlx::query("DELETE FROM admin_users WHERE admin_name = ?")
                .bind(&admin_name)
                .execute(&pool)
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
        let pool = self.pool().clone();
        Box::pin(async move {
            let result =
                sqlx::query("UPDATE admin_users SET password_hash = ? WHERE admin_name = ?")
                    .bind(&password_hash)
                    .bind(&admin_name)
                    .execute(&pool)
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
        let pool = self.pool().clone();
        Box::pin(async move {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT password_hash FROM admin_users WHERE admin_name = ?")
                    .bind(&admin_name)
                    .fetch_optional(&pool)
                    .await
                    .map_err(|e| {
                        tracing::error!("verify_admin_password: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;

            let Some((hash,)) = row else {
                return Ok(None);
            };

            let verified = tokio::task::spawn_blocking(move || bcrypt::verify(&password, &hash))
                .await
                .map_err(|e| OpError::Internal(format!("bcrypt verify task failed: {e}")))?
                .map_err(|e| OpError::Internal(format!("bcrypt verify failed: {e}")))?;

            Ok(Some(verified))
        })
    }
}
