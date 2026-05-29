// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy and permissions-boundary operations for `TidbCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult};

use crate::catalog_store::TidbCatalogStore;
use crate::tidb_util::is_fk_violation;

impl TidbCatalogStore {
    // ── Policies ───────────────────────────────────────────────────

    pub(crate) async fn put_policy_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        let result = sqlx::query(
            "INSERT INTO iam_policies (account_id, principal_type, principal_name, policy_name, policy_document) \
             VALUES (?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE policy_document = VALUES(policy_document)",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .bind(policy_name)
        .bind(document)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_fk_violation(&e) => Err(OpError::NotFound("Account not found".to_owned())),
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
             WHERE account_id = ? AND principal_type = ? AND principal_name = ? AND policy_name = ?",
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
        sqlx::query_as(
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
        })
    }

    // ── Permissions boundaries ─────────────────────────────────────

    pub(crate) async fn set_boundary_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        let result = sqlx::query(
            "INSERT INTO iam_permissions_boundaries (account_id, principal_type, principal_name, policy_document) \
             VALUES (?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE policy_document = VALUES(policy_document)",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .bind(document)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_fk_violation(&e) => Err(OpError::NotFound("Account not found".to_owned())),
            Err(e) => {
                tracing::error!("set_boundary failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn get_boundary_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
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
        Ok(row.map(|(doc,)| doc))
    }

    pub(crate) async fn delete_boundary_impl(
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
