// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy and permissions-boundary operations for `CassandraCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult};

use crate::catalog_store::CassandraCatalogStore;

impl CassandraCatalogStore {
    // ── Policies ───────────────────────────────────────────────────

    pub(crate) async fn put_policy_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        // Check if account exists (emulate FK constraint)
        if !self.account_exists(account_id).await? {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();

        // Using natural UPSERT semantics (ADR-0004)
        let insert_query = format!(
            "INSERT INTO {catalog_keyspace}.iam_policies (account_id, principal_type, principal_name, policy_name, policy_document, created_at) \
             VALUES (?, ?, ?, ?, ?, toTimestamp(now()))"
        );

        let doc_str = document.to_string();

        crate::cassandra_util::execute(
            self.session(),
            &insert_query,
            cdrs_tokio::query_values!(
                account_id,
                principal_type,
                principal_name,
                policy_name,
                doc_str.as_str()
            ),
            "put_policy",
        )
        .await
    }

    pub(crate) async fn delete_policy_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
    ) -> OpResult<()> {
        let catalog_keyspace = self.catalog_keyspace();

        // Check if policy exists
        let check_query = format!(
            "SELECT policy_name FROM {catalog_keyspace}.iam_policies \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ? AND policy_name = ?"
        );

        if crate::cassandra_util::query_optional(
            self.session(),
            &check_query,
            cdrs_tokio::query_values!(account_id, principal_type, principal_name, policy_name),
            "delete_policy",
        )
        .await?
        .is_none()
        {
            return Err(OpError::NotFound("Policy not found".to_owned()));
        }

        let delete_query = format!(
            "DELETE FROM {catalog_keyspace}.iam_policies \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ? AND policy_name = ?"
        );

        crate::cassandra_util::execute(
            self.session(),
            &delete_query,
            cdrs_tokio::query_values!(account_id, principal_type, principal_name, policy_name),
            "delete_policy",
        )
        .await
    }

    pub(crate) async fn list_policies_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<Vec<(String, serde_json::Value, time::OffsetDateTime)>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT policy_name, policy_document, created_at FROM {catalog_keyspace}.iam_policies \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ?"
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id, principal_type, principal_name),
            "list_policies",
        )
        .await?;

        let mut policies = crate::cassandra_util::map_rows(
            rows,
            |row| {
                use crate::cassandra_util::{get_column, get_timestamp};
                let doc_str: String = get_column(row, "policy_document", "list_policies")?;
                let doc = serde_json::from_str(&doc_str).map_err(|e| {
                    tracing::error!("list_policies parse policy_document: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "policy_name", "list_policies")?,
                    doc,
                    get_timestamp(row, "created_at", "list_policies")?,
                ))
            },
            "list_policies",
        )?;

        policies.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(policies)
    }

    // ── Permissions boundaries ─────────────────────────────────────

    pub(crate) async fn set_boundary_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        document: &serde_json::Value,
    ) -> OpResult<()> {
        // Check if account exists (emulate FK constraint)
        if !self.account_exists(account_id).await? {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();

        // Using natural UPSERT semantics
        let insert_query = format!(
            "INSERT INTO {catalog_keyspace}.iam_permissions_boundaries (account_id, principal_type, principal_name, policy_document) \
             VALUES (?, ?, ?, ?)"
        );

        let doc_str = document.to_string();

        crate::cassandra_util::execute(
            self.session(),
            &insert_query,
            cdrs_tokio::query_values!(account_id, principal_type, principal_name, doc_str.as_str()),
            "set_boundary",
        )
        .await
    }

    pub(crate) async fn get_boundary_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT policy_document FROM {catalog_keyspace}.iam_permissions_boundaries \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ?"
        );

        let row = crate::cassandra_util::query_optional(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id, principal_type, principal_name),
            "get_boundary",
        )
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let doc_str: String =
            crate::cassandra_util::get_column(&row, "policy_document", "get_boundary")?;
        let doc = serde_json::from_str(&doc_str).map_err(|e| {
            tracing::error!("get_boundary parse policy_document: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        Ok(Some(doc))
    }

    pub(crate) async fn delete_boundary_impl(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<()> {
        let catalog_keyspace = self.catalog_keyspace();

        // Check if boundary exists
        let check_query = format!(
            "SELECT principal_name FROM {catalog_keyspace}.iam_permissions_boundaries \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ?"
        );

        if crate::cassandra_util::query_optional(
            self.session(),
            &check_query,
            cdrs_tokio::query_values!(account_id, principal_type, principal_name),
            "delete_boundary",
        )
        .await?
        .is_none()
        {
            return Err(OpError::NotFound("Permissions boundary not set".to_owned()));
        }

        let delete_query = format!(
            "DELETE FROM {catalog_keyspace}.iam_permissions_boundaries \
             WHERE account_id = ? AND principal_type = ? AND principal_name = ?"
        );

        crate::cassandra_util::execute(
            self.session(),
            &delete_query,
            cdrs_tokio::query_values!(account_id, principal_type, principal_name),
            "delete_boundary",
        )
        .await
    }
}
