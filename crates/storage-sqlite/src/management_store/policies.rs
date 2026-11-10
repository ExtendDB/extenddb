// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM policy CRUD and permissions boundaries for `SqliteCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult, StoredDocument};
use time::OffsetDateTime;

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::parse_timestamp;

/// Begin the `BEGIN IMMEDIATE` transaction a principal's create or delete
/// runs in. The caller holds the write lock (D1).
pub(super) async fn begin_write<'a>(
    pool: &'a sqlx::SqlitePool,
    what: &str,
) -> OpResult<sqlx::Transaction<'a, sqlx::Sqlite>> {
    pool.begin_with("BEGIN IMMEDIATE").await.map_err(|e| {
        tracing::error!("{what} begin: {e}");
        OpError::Internal("Database error".to_owned())
    })
}

/// Commit `tx` when `result` is `Ok`, and roll it back otherwise, so the
/// connection goes back to the pool with no transaction open.
///
/// Dropping a SQLite transaction only queues its rollback, and a
/// `BEGIN IMMEDIATE` on that connection before the rollback ran fails with
/// "attempted to call begin_with at non-zero transaction depth".
pub(super) async fn finish<T>(
    tx: sqlx::Transaction<'_, sqlx::Sqlite>,
    result: OpResult<T>,
    what: &str,
) -> OpResult<T> {
    match result {
        Ok(value) => {
            tx.commit().await.map_err(|e| {
                tracing::error!("{what} commit: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(value)
        }
        Err(e) => {
            if let Err(rollback) = tx.rollback().await {
                tracing::error!("{what} rollback: {rollback}");
            }
            Err(e)
        }
    }
}

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
    conn: &mut sqlx::SqliteConnection,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) -> OpResult<()> {
    for table in ["iam_policies", "iam_permissions_boundaries"] {
        sqlx::query(&format!(
            "DELETE FROM {table} \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ?"
        ))
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            tracing::error!("delete {principal_type} {principal_name}'s rows in {table}: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
    }
    Ok(())
}

impl SqliteCatalogStore {
    pub(crate) async fn put_policy_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        let doc = serde_json::to_string(document)
            .map_err(|e| OpError::Internal(format!("serialize policy: {e}")))?;
        // D1: every writer holds the write lock. This is the writer observed
        // losing its file lock in the run-integration-sqlite flake
        // (`put_policy: ... database is locked`).
        let _writer = self.lock_writes().await;
        sqlx::query(
            "INSERT INTO iam_policies \
             (account_id, principal_type, principal_name, policy_name, policy_document) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT(account_id, principal_type, principal_name, policy_name) \
             DO UPDATE SET policy_document = excluded.policy_document",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .bind(policy_name)
        .bind(&doc)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| {
            tracing::error!("put_policy: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }

    pub(crate) async fn delete_policy_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
    ) -> OpResult<()> {
        // D1: every writer holds the write lock.
        let _writer = self.lock_writes().await;
        let result = sqlx::query(
            "DELETE FROM iam_policies \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ? AND policy_name = ?",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .bind(policy_name)
        .execute(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("delete_policy: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        if result.rows_affected() == 0 {
            return Err(OpError::NotFound("Policy not found".to_owned()));
        }
        Ok(())
    }

    pub(crate) async fn list_policies_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<Vec<(String, serde_json::Value, OffsetDateTime)>> {
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
            .map(|(name, doc, ts)| {
                Ok((
                    name,
                    serde_json::from_str(&doc)
                        .map_err(|e| OpError::Internal(format!("parse policy doc: {e}")))?,
                    parse_timestamp(&ts)
                        .map_err(|e| OpError::Internal(format!("parse created_at: {e}")))?,
                ))
            })
            .collect()
    }

    /// A principal's policies for the resource-tag migration, each document
    /// decoded on its own: one that does not decode comes back as its decode
    /// error, and the others are still read.
    pub(crate) async fn list_policy_documents_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<Vec<(String, StoredDocument)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT policy_name, policy_document FROM iam_policies \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_policy_documents: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        Ok(rows
            .into_iter()
            .map(|(name, doc)| (name, serde_json::from_str(&doc).map_err(|e| e.to_string())))
            .collect())
    }

    /// A principal's permissions boundary for the resource-tag migration:
    /// the decoded document, or its decode error.
    pub(crate) async fn boundary_document_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<Option<StoredDocument>> {
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
            tracing::error!("boundary_document: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        Ok(row.map(|(doc,)| serde_json::from_str(&doc).map_err(|e| e.to_string())))
    }

    // ── Permissions boundaries ─────────────────────────────────────

    async fn set_boundary(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        let doc = serde_json::to_string(document)
            .map_err(|e| OpError::Internal(format!("serialize boundary: {e}")))?;
        // D1: every writer holds the write lock.
        let _writer = self.lock_writes().await;
        sqlx::query(
            "INSERT INTO iam_permissions_boundaries \
             (account_id, principal_type, principal_name, policy_document) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT(account_id, principal_type, principal_name) \
             DO UPDATE SET policy_document = excluded.policy_document",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .bind(&doc)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| {
            tracing::error!("set_boundary: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }

    async fn get_boundary(
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
        row.map(|(d,)| {
            serde_json::from_str(&d).map_err(|e| OpError::Internal(format!("parse boundary: {e}")))
        })
        .transpose()
    }

    async fn delete_boundary(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<()> {
        // D1: every writer holds the write lock.
        let _writer = self.lock_writes().await;
        sqlx::query(
            "DELETE FROM iam_permissions_boundaries \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ?",
        )
        .bind(account_id)
        .bind(principal_type)
        .bind(principal_name)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| {
            tracing::error!("delete_boundary: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }

    pub(crate) async fn set_user_boundary_impl(
        &self,
        account_id: &str,
        user_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        self.set_boundary(account_id, "user", user_name, document)
            .await
    }

    pub(crate) async fn get_user_boundary_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        self.get_boundary(account_id, "user", user_name).await
    }

    pub(crate) async fn delete_user_boundary_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<()> {
        self.delete_boundary(account_id, "user", user_name).await
    }

    pub(crate) async fn set_role_boundary_impl(
        &self,
        account_id: &str,
        role_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        self.set_boundary(account_id, "role", role_name, document)
            .await
    }

    pub(crate) async fn get_role_boundary_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        self.get_boundary(account_id, "role", role_name).await
    }

    pub(crate) async fn delete_role_boundary_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<()> {
        self.delete_boundary(account_id, "role", role_name).await
    }
}
