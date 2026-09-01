// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AdminStore` implementation for `CassandraCatalogStore`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::management_store::{AdminEntry, OpError, OpResult};
use futures::future::BoxFuture;

use super::catalog_store::CassandraCatalogStore;

async fn admin_exists(
    session: &std::sync::Arc<crate::engine::CassandraSession>,
    catalog_keyspace: &str,
    admin_name: &str,
) -> OpResult<bool> {
    let query =
        format!("SELECT admin_name FROM {catalog_keyspace}.admin_users WHERE admin_name = ?");
    let row = crate::cassandra_util::query_optional(
        session,
        &query,
        cdrs_tokio::query_values!(admin_name),
        "admin_exists",
    )
    .await?;
    Ok(row.is_some())
}

impl extenddb_storage::management_store::AdminStore for CassandraCatalogStore {
    fn create_admin(&self, admin_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "INSERT INTO {catalog_keyspace}.admin_users (admin_name, password_hash, created_at) \
                 VALUES (?, ?, toTimestamp(now())) IF NOT EXISTS"
            );

            let result = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(admin_name.as_str(), password_hash.as_str()),
                )
                .await
                .map_err(|e| {
                    tracing::error!("create_admin: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let body = result.response_body().map_err(|e| {
                tracing::error!("create_admin response_body: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let rows = body.into_rows().unwrap_or_default();
            if let Some(row) = rows.first() {
                let applied: bool = row.get_r_by_name("[applied]").map_err(|e| {
                    tracing::error!("create_admin parse [applied]: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                if !applied {
                    return Err(OpError::AlreadyExists(
                        "Admin user already exists".to_owned(),
                    ));
                }
            }

            Ok(())
        })
    }

    fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query =
                format!("SELECT admin_name, created_at FROM {catalog_keyspace}.admin_users");
            let rows = crate::cassandra_util::query_rows(
                &session,
                &query,
                cdrs_tokio::query_values!(),
                "list_admins",
            )
            .await?;

            let mut admins = crate::cassandra_util::map_rows(
                rows,
                |row| {
                    Ok(AdminEntry {
                        admin_name: crate::cassandra_util::get_column(
                            row,
                            "admin_name",
                            "list_admins",
                        )?,
                        created_at: crate::cassandra_util::get_timestamp(
                            row,
                            "created_at",
                            "list_admins",
                        )?,
                    })
                },
                "list_admins",
            )?;
            admins.sort_by(|a, b| a.admin_name.cmp(&b.admin_name));
            Ok(admins)
        })
    }

    fn delete_admin(&self, admin_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            if !admin_exists(&session, &catalog_keyspace, &admin_name).await? {
                return Err(OpError::NotFound("Admin user not found".to_owned()));
            }

            let query = format!("DELETE FROM {catalog_keyspace}.admin_users WHERE admin_name = ?");
            crate::cassandra_util::execute(
                &session,
                &query,
                cdrs_tokio::query_values!(admin_name.as_str()),
                "delete_admin",
            )
            .await
        })
    }

    fn change_admin_password(
        &self,
        admin_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let admin_name = admin_name.to_owned();
        let password_hash = password_hash.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            if !admin_exists(&session, &catalog_keyspace, &admin_name).await? {
                return Err(OpError::NotFound("Admin user not found".to_owned()));
            }

            let query = format!(
                "UPDATE {catalog_keyspace}.admin_users SET password_hash = ? WHERE admin_name = ?"
            );
            crate::cassandra_util::execute(
                &session,
                &query,
                cdrs_tokio::query_values!(password_hash.as_str(), admin_name.as_str()),
                "change_admin_password",
            )
            .await
        })
    }

    fn verify_admin_password(
        &self,
        admin_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<Option<bool>>> {
        let admin_name = admin_name.to_owned();
        let password = password.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "SELECT password_hash FROM {catalog_keyspace}.admin_users WHERE admin_name = ?"
            );

            let row = crate::cassandra_util::query_optional(
                &session,
                &query,
                cdrs_tokio::query_values!(admin_name.as_str()),
                "verify_admin_password",
            )
            .await?;

            let Some(row) = row else {
                return Ok(None);
            };

            let hash: String =
                crate::cassandra_util::get_column(&row, "password_hash", "verify_admin_password")?;

            Ok(Some(verify_bcrypt(password, hash).await))
        })
    }
}

/// Verify a bcrypt password on a blocking thread (same logic as server::password).
async fn verify_bcrypt(password: String, hash: String) -> bool {
    tokio::task::spawn_blocking(move || bcrypt::verify(password, &hash).unwrap_or(false))
        .await
        .unwrap_or(false)
}
