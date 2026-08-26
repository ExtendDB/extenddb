// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Role management, trust-policy access, and role tags for `DuckDbCatalogStore`.
//!
//! Trust policies and policy documents are stored as JSON text; this module
//! serializes `serde_json::Value` on write and parses it back on read.

use crate::db;
use extenddb_storage::management_store::{OpError, OpResult, RoleDetail, RoleListEntry};

use crate::catalog_store::DuckDbCatalogStore;
use crate::duckdb_util::{is_fk_violation, is_unique_violation, parse_timestamp};

/// Parse a stored JSON-text column into a `serde_json::Value`.
fn parse_json(s: &str, ctx: &str) -> OpResult<serde_json::Value> {
    serde_json::from_str(s).map_err(|e| OpError::Internal(format!("{ctx} parse json: {e}")))
}

impl DuckDbCatalogStore {
    pub(crate) async fn create_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        trust_policy: &serde_json::Value,
    ) -> OpResult<()> {
        let role_arn = format!("arn:aws:iam::{account_id}:role/{role_name}");
        let trust = serde_json::to_string(trust_policy)
            .map_err(|e| OpError::Internal(format!("serialize trust policy: {e}")))?;
        crate::referential::ensure_account_exists(self.pool(), account_id).await?;
        db::query(
            "INSERT INTO iam_roles (account_id, role_name, role_arn, trust_policy) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(role_name)
        .bind(&role_arn)
        .bind(&trust)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| {
            if is_unique_violation(&e) {
                OpError::AlreadyExists("IAM role already exists".to_owned())
            } else if is_fk_violation(&e) {
                OpError::NotFound("Account not found".to_owned())
            } else {
                tracing::error!("create_role: {e}");
                OpError::Internal("Database error".to_owned())
            }
        })
    }

    pub(crate) async fn delete_role_impl(&self, account_id: &str, role_name: &str) -> OpResult<()> {
        crate::referential::delete_with_children(
            self.pool(),
            "delete_role",
            async |tx| crate::referential::delete_role_children(tx, account_id, role_name).await,
            "DELETE FROM iam_roles WHERE account_id = ? AND role_name = ?",
            &[account_id, role_name],
            "IAM role not found",
        )
        .await
    }

    pub(crate) async fn list_roles_impl(&self, account_id: &str) -> OpResult<Vec<RoleListEntry>> {
        let rows: Vec<(String, String, String, String, String)> = db::query_as(
            "SELECT account_id, role_name, role_arn, trust_policy, created_at \
             FROM iam_roles WHERE account_id = ? ORDER BY role_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_roles: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        rows.into_iter()
            .map(|(aid, rn, arn, trust, ts)| {
                Ok((
                    aid,
                    rn,
                    arn,
                    parse_json(&trust, "list_roles trust_policy")?,
                    parse_timestamp(&ts)
                        .map_err(|e| OpError::Internal(format!("parse created_at: {e}")))?,
                ))
            })
            .collect()
    }

    pub(crate) async fn get_role_detail_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<RoleDetail>> {
        let row: Option<(String,)> = db::query_as(
            "SELECT trust_policy FROM iam_roles WHERE account_id = ? AND role_name = ?",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_role_detail: {e}")))?;
        let Some((trust,)) = row else {
            return Ok(None);
        };
        let trust_policy = parse_json(&trust, "get_role_detail trust_policy")?;

        let policies: Vec<(String,)> = db::query_as(
            "SELECT policy_name FROM iam_policies \
             WHERE account_id = ? AND principal_type = 'role' AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_role_detail policies: {e}")))?;

        let tags: Vec<(String, String)> = db::query_as(
            "SELECT tag_key, tag_value FROM iam_role_tags \
             WHERE account_id = ? AND role_name = ? ORDER BY tag_key",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_role_detail tags: {e}")))?;

        Ok(Some(RoleDetail {
            trust_policy,
            policies: policies.into_iter().map(|(n,)| n).collect(),
            tags,
        }))
    }

    pub(crate) async fn get_role_trust_policy_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        let row: Option<(String,)> = db::query_as(
            "SELECT trust_policy FROM iam_roles WHERE account_id = ? AND role_name = ?",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_role_trust_policy: {e}")))?;
        row.map(|(t,)| parse_json(&t, "get_role_trust_policy"))
            .transpose()
    }

    pub(crate) async fn tag_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> OpResult<()> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| OpError::Internal(format!("tag_role begin: {e}")))?;
        crate::referential::ensure_role_exists(&mut *tx, account_id, role_name).await?;
        for (k, v) in tags {
            db::query(
                "INSERT INTO iam_role_tags (account_id, role_name, tag_key, tag_value) \
                 VALUES (?, ?, ?, ?) \
                 ON CONFLICT(account_id, role_name, tag_key) DO UPDATE SET tag_value = excluded.tag_value",
            )
            .bind(account_id)
            .bind(role_name)
            .bind(k)
            .bind(v)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                if is_fk_violation(&e) {
                    OpError::NotFound("IAM role not found".to_owned())
                } else {
                    tracing::error!("tag_role: {e}");
                    OpError::Internal("Database error".to_owned())
                }
            })?;
        }
        tx.commit()
            .await
            .map_err(|e| OpError::Internal(format!("tag_role commit: {e}")))?;
        Ok(())
    }

    pub(crate) async fn untag_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> OpResult<()> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| OpError::Internal(format!("untag_role begin: {e}")))?;
        for k in tag_keys {
            db::query(
                "DELETE FROM iam_role_tags WHERE account_id = ? AND role_name = ? AND tag_key = ?",
            )
            .bind(account_id)
            .bind(role_name)
            .bind(k)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::error!("untag_role: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
        }
        tx.commit()
            .await
            .map_err(|e| OpError::Internal(format!("untag_role commit: {e}")))?;
        Ok(())
    }

    pub(crate) async fn list_role_tags_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Vec<(String, String)>> {
        db::query_as(
            "SELECT tag_key, tag_value FROM iam_role_tags \
             WHERE account_id = ? AND role_name = ? ORDER BY tag_key",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_role_tags: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }
}
