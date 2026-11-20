// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AdminStore` implementation for `CassandraCatalogStore`.

use crate::cassandra_util::PolicySession as _;
use crate::cassandra_util::Stmt;
use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::management_store::{AdminEntry, OpError, OpResult};
use futures::future::BoxFuture;

use super::catalog_store::CassandraCatalogStore;

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
                .run(
                    Stmt::Lwt,
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
            // An LWT, because `create_admin` inserts the row with an LWT.
            // ADR-0021: a plain DELETE on such a row corrupts `system.paxos`.
            // IF EXISTS is also the existence check: not applied means no row.
            let query = format!(
                "DELETE FROM {catalog_keyspace}.admin_users WHERE admin_name = ? IF EXISTS"
            );
            let applied = crate::cassandra_util::apply_lwt::<OpError>(
                &session,
                &query,
                cdrs_tokio::query_values!(admin_name.as_str()),
                "delete_admin",
            )
            .await?;
            if !applied {
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
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            // An LWT, so that no plain write mixes with the LWT insert in
            // `create_admin`. ADR-0021 names the DELETE case. IF EXISTS also
            // stops the UPDATE from creating a row for a missing admin.
            // This holds inside one datacenter: the Paxos round runs at
            // LOCAL_SERIAL, and ExtendDB supports one datacenter.
            let query = format!(
                "UPDATE {catalog_keyspace}.admin_users SET password_hash = ? \
                 WHERE admin_name = ? IF EXISTS"
            );
            let applied = crate::cassandra_util::apply_lwt::<OpError>(
                &session,
                &query,
                cdrs_tokio::query_values!(password_hash.as_str(), admin_name.as_str()),
                "change_admin_password",
            )
            .await?;
            if !applied {
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

            let verified = extenddb_storage::password::verify_password(password, hash).await?;
            Ok(Some(verified))
        })
    }
}
