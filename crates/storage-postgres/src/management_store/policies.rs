// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy and permissions-boundary operations for `PostgresCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult};

use super::database_error;
use crate::catalog_store::PostgresCatalogStore;
use crate::pg_util::is_fk_violation;

/// Delete the inline policies and the permissions boundary stored under a
/// principal's name, inside the caller's transaction.
///
/// `iam_policies` and `iam_permissions_boundaries` name their principal by
/// `(principal_type, principal_name)` and have no foreign key to it, so the
/// cascades that remove a principal's tags, access keys and memberships do not
/// reach them. A principal's delete calls this so that a later principal with
/// the same name does not get them, and a principal's create calls it so that
/// rows a delete left behind before this existed are removed too.
pub(super) async fn delete_principal_attachments(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) -> OpResult<()> {
    for table in ["iam_policies", "iam_permissions_boundaries"] {
        sqlx::query(&format!(
            "DELETE FROM {table} \
             WHERE account_id = $1 AND principal_type = $2 AND principal_name = $3"
        ))
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            database_error(
                &format!("delete {principal_type} {principal_name}'s rows in {table}"),
                &e,
            )
        })?;
    }
    Ok(())
}

impl PostgresCatalogStore {
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
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (account_id, principal_type, principal_name, policy_name) \
             DO UPDATE SET policy_document = EXCLUDED.policy_document",
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
            Err(e) => Err(database_error("put_policy", &e)),
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
             WHERE account_id = $1 AND principal_type = $2 AND principal_name = $3 AND policy_name = $4",
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
             WHERE account_id = $1 AND principal_type = $2 AND principal_name = $3 \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| database_error("list_policies", &e))
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
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (account_id, principal_type, principal_name) \
             DO UPDATE SET policy_document = EXCLUDED.policy_document",
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
            Err(e) => Err(database_error("set_boundary", &e)),
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
             WHERE account_id = $1 AND principal_type = $2 AND principal_name = $3",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| database_error("get_boundary", &e))?;
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
             WHERE account_id = $1 AND principal_type = $2 AND principal_name = $3",
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
