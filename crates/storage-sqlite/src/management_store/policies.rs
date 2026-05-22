// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy and permissions-boundary operations for `SqliteCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult};

use crate::catalog_store::SqliteCatalogStore;

impl SqliteCatalogStore {
    // ── Policies ───────────────────────────────────────────────────

    pub(crate) async fn put_policy_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        let doc_str = serde_json::to_string(document).unwrap_or_else(|_| "{}".to_owned());
        let result = sqlx::query(
            "INSERT INTO iam_policies \
             (account_id, principal_type, principal_name, policy_name, policy_document) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (account_id, principal_type, principal_name, policy_name) \
             DO UPDATE SET policy_document = excluded.policy_document",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .bind(policy_name)
        .bind(&doc_str)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("put_policy failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn delete_policy_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
    ) -> OpResult<()> {
        let result = sqlx::query(
            "DELETE FROM iam_policies \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ? \
             AND policy_name = ?",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .bind(policy_name)
        .execute(self.pool())
        .await;
        match result {
            Ok(r) if r.rows_affected() == 0 => {
                Err(OpError::NotFound("Policy not found".to_owned()))
            }
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("delete_policy failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn list_policies_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<Vec<(String, serde_json::Value, time::OffsetDateTime)>> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT policy_name, policy_document, created_at FROM iam_policies \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_policies: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        rows.into_iter()
            .map(|(name, doc_str, ts)| {
                let document = serde_json::from_str::<serde_json::Value>(&doc_str)
                    .unwrap_or(serde_json::Value::Object(Default::default()));
                let created_at =
                    crate::sqlite_util::parse_timestamp(&ts).map_err(|e| {
                        tracing::error!("list_policies parse_timestamp: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
                Ok((name, document, created_at))
            })
            .collect()
    }

    // ── Permissions boundaries ─────────────────────────────────────

    pub(crate) async fn set_user_boundary_impl(
        &self,
        account_id: &str,
        user_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        self.set_boundary_impl(account_id, "user", user_name, document)
            .await
    }

    pub(crate) async fn get_user_boundary_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        self.get_boundary_impl(account_id, "user", user_name).await
    }

    pub(crate) async fn delete_user_boundary_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<()> {
        self.delete_boundary_impl(account_id, "user", user_name)
            .await
    }

    pub(crate) async fn set_role_boundary_impl(
        &self,
        account_id: &str,
        role_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        self.set_boundary_impl(account_id, "role", role_name, document)
            .await
    }

    pub(crate) async fn get_role_boundary_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        self.get_boundary_impl(account_id, "role", role_name).await
    }

    pub(crate) async fn delete_role_boundary_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<()> {
        self.delete_boundary_impl(account_id, "role", role_name)
            .await
    }

    async fn set_boundary_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        let doc_str = serde_json::to_string(document).unwrap_or_else(|_| "{}".to_owned());
        let result = sqlx::query(
            "INSERT INTO iam_permissions_boundaries \
             (account_id, principal_type, principal_name, policy_document) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT (account_id, principal_type, principal_name) \
             DO UPDATE SET policy_document = excluded.policy_document",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .bind(&doc_str)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("set_boundary failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    async fn get_boundary_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT policy_document FROM iam_permissions_boundaries \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ?",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_boundary: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        Ok(row.map(|(doc_str,)| {
            serde_json::from_str::<serde_json::Value>(&doc_str)
                .unwrap_or(serde_json::Value::Object(Default::default()))
        }))
    }

    async fn delete_boundary_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<()> {
        let result = sqlx::query(
            "DELETE FROM iam_permissions_boundaries \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ?",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .execute(self.pool())
        .await;
        match result {
            Ok(r) if r.rows_affected() == 0 => {
                Err(OpError::NotFound("Permissions boundary not set".to_owned()))
            }
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("delete_boundary failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }
}
