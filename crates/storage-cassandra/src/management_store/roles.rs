// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Role management operations for `CassandraCatalogStore`.

use cdrs_tokio::query_values;
use extenddb_storage::management_store::{OpError, OpResult, RoleDetail};
use time::OffsetDateTime;

use crate::cassandra_util::{
    execute, get_column, get_timestamp, map_rows, query_optional, query_rows,
};
use crate::catalog_store::CassandraCatalogStore;

impl CassandraCatalogStore {
    async fn role_exists(&self, account_id: &str, role_name: &str) -> OpResult<bool> {
        let query = format!(
            "SELECT role_name FROM {}.iam_roles WHERE account_id = ? AND role_name = ?",
            self.catalog_keyspace()
        );
        let row = query_optional(
            self.session(),
            &query,
            query_values!(account_id, role_name),
            "role_exists",
        )
        .await?;
        Ok(row.is_some())
    }

    pub(crate) async fn create_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        trust_policy: &serde_json::Value,
    ) -> OpResult<()> {
        if !self.account_exists(account_id).await? {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        let role_arn = format!("arn:aws:iam::{account_id}:role/{role_name}");
        let trust_policy_json = serde_json::to_string(trust_policy).map_err(|e| {
            tracing::error!("create_role serialize trust_policy: {e}");
            OpError::Internal("JSON serialization failed".to_owned())
        })?;
        let now = OffsetDateTime::now_utc();
        let now_ms = now.unix_timestamp() * 1000 + i64::from(now.millisecond());

        let query = format!(
            "INSERT INTO {}.iam_roles \
             (account_id, role_name, role_arn, trust_policy, created_at) \
             VALUES (?, ?, ?, ?, ?) IF NOT EXISTS",
            self.catalog_keyspace()
        );

        let applied = crate::cassandra_util::apply_lwt(
            self.session(),
            &query,
            query_values!(account_id, role_name, role_arn, trust_policy_json, now_ms),
            "create_role",
        )
        .await?;

        if !applied {
            return Err(OpError::AlreadyExists("IAM role already exists".to_owned()));
        }

        Ok(())
    }

    pub(crate) async fn delete_role_impl(&self, account_id: &str, role_name: &str) -> OpResult<()> {
        if !self.role_exists(account_id, role_name).await? {
            return Err(OpError::NotFound("IAM role not found".to_owned()));
        }

        let query = format!(
            "DELETE FROM {}.iam_roles WHERE account_id = ? AND role_name = ?",
            self.catalog_keyspace()
        );
        execute(
            self.session(),
            &query,
            query_values!(account_id, role_name),
            "delete_role",
        )
        .await
    }

    pub(crate) async fn list_roles_impl(
        &self,
        account_id: &str,
    ) -> OpResult<
        Vec<(
            String,
            String,
            String,
            serde_json::Value,
            time::OffsetDateTime,
        )>,
    > {
        let query = format!(
            "SELECT account_id, role_name, role_arn, trust_policy, created_at \
             FROM {}.iam_roles WHERE account_id = ?",
            self.catalog_keyspace()
        );
        let rows = query_rows(
            self.session(),
            &query,
            query_values!(account_id),
            "list_roles",
        )
        .await?;

        let mut roles = map_rows(
            rows,
            |row| {
                let account: String = get_column(row, "account_id", "list_roles")?;
                let role_name: String = get_column(row, "role_name", "list_roles")?;
                let role_arn: String = get_column(row, "role_arn", "list_roles")?;
                let trust_policy_str: String = get_column(row, "trust_policy", "list_roles")?;
                let trust_policy: serde_json::Value = serde_json::from_str(&trust_policy_str)
                    .map_err(|e| {
                        tracing::error!("list_roles deserialize trust_policy: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
                let created_at = get_timestamp(row, "created_at", "list_roles")?;
                Ok((account, role_name, role_arn, trust_policy, created_at))
            },
            "list_roles",
        )?;
        roles.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(roles)
    }

    pub(crate) async fn get_role_detail_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<RoleDetail>> {
        let query = format!(
            "SELECT trust_policy FROM {}.iam_roles WHERE account_id = ? AND role_name = ?",
            self.catalog_keyspace()
        );
        let row = query_optional(
            self.session(),
            &query,
            query_values!(account_id, role_name),
            "get_role_detail",
        )
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let trust_policy_str: String = get_column(&row, "trust_policy", "get_role_detail")?;
        let trust_policy: serde_json::Value =
            serde_json::from_str(&trust_policy_str).map_err(|e| {
                tracing::error!("get_role_detail deserialize trust_policy: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

        let policies_query = format!(
            "SELECT policy_name FROM {}.iam_policies \
             WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?",
            self.catalog_keyspace()
        );
        let policies_rows = query_rows(
            self.session(),
            &policies_query,
            query_values!(account_id, role_name),
            "get_role_detail policies",
        )
        .await?;

        let mut policies = map_rows(
            policies_rows,
            |row| get_column(row, "policy_name", "get_role_detail"),
            "get_role_detail",
        )?;
        policies.sort();

        let tags_query = format!(
            "SELECT tag_key, tag_value FROM {}.iam_role_tags WHERE account_id = ? AND role_name = ?",
            self.catalog_keyspace()
        );
        let tags_rows = query_rows(
            self.session(),
            &tags_query,
            query_values!(account_id, role_name),
            "get_role_detail tags",
        )
        .await?;

        let mut tags = map_rows(
            tags_rows,
            |row| {
                Ok((
                    get_column(row, "tag_key", "get_role_detail")?,
                    get_column(row, "tag_value", "get_role_detail")?,
                ))
            },
            "get_role_detail",
        )?;
        tags.sort();

        Ok(Some(RoleDetail {
            trust_policy,
            policies,
            tags,
        }))
    }

    pub(crate) async fn get_role_trust_policy_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        let query = format!(
            "SELECT trust_policy FROM {}.iam_roles WHERE account_id = ? AND role_name = ?",
            self.catalog_keyspace()
        );
        let row = query_optional(
            self.session(),
            &query,
            query_values!(account_id, role_name),
            "get_role_trust_policy",
        )
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let trust_policy_str: String = get_column(&row, "trust_policy", "get_role_trust_policy")?;
        let trust_policy: serde_json::Value =
            serde_json::from_str(&trust_policy_str).map_err(|e| {
                tracing::error!("get_role_trust_policy deserialize trust_policy: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
        Ok(Some(trust_policy))
    }

    // ── Role tags ──────────────────────────────────────────────────

    pub(crate) async fn tag_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> OpResult<()> {
        if !self.role_exists(account_id, role_name).await? {
            return Err(OpError::NotFound("IAM role not found".to_owned()));
        }

        for (key, value) in tags {
            let query = format!(
                "INSERT INTO {}.iam_role_tags (account_id, role_name, tag_key, tag_value) \
                 VALUES (?, ?, ?, ?)",
                self.catalog_keyspace()
            );
            execute(
                self.session(),
                &query,
                query_values!(account_id, role_name, key.as_str(), value.as_str()),
                "tag_role",
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn untag_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> OpResult<()> {
        for key in tag_keys {
            let query = format!(
                "DELETE FROM {}.iam_role_tags WHERE account_id = ? AND role_name = ? AND tag_key = ?",
                self.catalog_keyspace()
            );
            execute(
                self.session(),
                &query,
                query_values!(account_id, role_name, key.as_str()),
                "untag_role",
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn list_role_tags_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Vec<(String, String)>> {
        let query = format!(
            "SELECT tag_key, tag_value FROM {}.iam_role_tags WHERE account_id = ? AND role_name = ?",
            self.catalog_keyspace()
        );
        let rows = query_rows(
            self.session(),
            &query,
            query_values!(account_id, role_name),
            "list_role_tags",
        )
        .await?;

        let mut tags = map_rows(
            rows,
            |row| {
                Ok((
                    get_column(row, "tag_key", "list_role_tags")?,
                    get_column(row, "tag_value", "list_role_tags")?,
                ))
            },
            "list_role_tags",
        )?;
        tags.sort();
        Ok(tags)
    }
}
