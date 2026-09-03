// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AdminStore` implementation: admin-user management, separate from IAM users.

use crate::db;
use extenddb_storage::management_store::{AdminEntry, AdminStore, OpError, OpResult};
use futures::future::BoxFuture;

use crate::catalog_store::DuckDbCatalogStore;
use crate::duckdb_util::{is_unique_violation, parse_timestamp};

impl AdminStore for DuckDbCatalogStore {
    fn create_admin(&self, admin_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            db::query("INSERT INTO admin_users (admin_name, password_hash) VALUES (?, ?)")
                .bind(&admin_name)
                .bind(&password_hash)
                .execute(self.pool())
                .await
                .map_err(|e| {
                    if is_unique_violation(&e) {
                        OpError::AlreadyExists("Admin user already exists".to_owned())
                    } else {
                        tracing::error!("create_admin: {e}");
                        OpError::Internal("Database error".to_owned())
                    }
                })?;
            Ok(())
        })
    }

    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
        Box::pin(async move {
            let rows: Vec<(String, String)> =
                db::query_as("SELECT admin_name, created_at FROM admin_users ORDER BY admin_name")
                    .fetch_all(self.pool())
                    .await
                    .map_err(|e| {
                        tracing::error!("list_admins: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            rows.into_iter()
                .map(|(admin_name, created)| {
                    Ok(AdminEntry {
                        admin_name,
                        created_at: parse_timestamp(&created)
                            .map_err(|e| OpError::Internal(format!("parse created_at: {e}")))?,
                    })
                })
                .collect()
        })
    }

    fn delete_admin(&self, admin_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        Box::pin(async move {
            let result = db::query("DELETE FROM admin_users WHERE admin_name = ?")
                .bind(&admin_name)
                .execute(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("delete_admin: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.rows_affected() == 0 {
                return Err(OpError::NotFound("Admin user not found".to_owned()));
            }
            Ok(())
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
            let result = db::query("UPDATE admin_users SET password_hash = ? WHERE admin_name = ?")
                .bind(&password_hash)
                .bind(&admin_name)
                .execute(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("change_admin_password: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            if result.rows_affected() == 0 {
                return Err(OpError::NotFound("Admin user not found".to_owned()));
            }
            Ok(())
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
                db::query_as("SELECT password_hash FROM admin_users WHERE admin_name = ?")
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
            let verified = tokio::task::spawn_blocking(move || bcrypt::verify(&password, &hash))
                .await
                .map_err(|e| OpError::Internal(format!("bcrypt task: {e}")))?
                .unwrap_or(false);
            Ok(Some(verified))
        })
    }
}
