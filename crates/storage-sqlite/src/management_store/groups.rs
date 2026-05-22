// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Group management operations for `SqliteCatalogStore`.

use extenddb_storage::management_store::{GroupDetail, GroupListEntry, OpError, OpResult};

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::{is_fk_violation, is_unique_violation};

impl SqliteCatalogStore {
    pub(crate) async fn create_group_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<()> {
        let group_arn = format!("arn:aws:iam::{account_id}:group/{group_name}");
        let result = sqlx::query(
            "INSERT INTO iam_groups (account_id, group_name, group_arn) VALUES (?, ?, ?)",
        )
        .bind(account_id)
        .bind(group_name)
        .bind(&group_arn)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_unique_violation(&e) => Err(OpError::AlreadyExists(
                "IAM group already exists".to_owned(),
            )),
            Err(e) if is_fk_violation(&e) => {
                Err(OpError::NotFound("Account not found".to_owned()))
            }
            Err(e) => {
                tracing::error!("create_group failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn delete_group_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<()> {
        let result =
            sqlx::query("DELETE FROM iam_groups WHERE account_id = ? AND group_name = ?")
                .bind(account_id)
                .bind(group_name)
                .execute(self.pool())
                .await;
        match result {
            Ok(r) if r.rows_affected() == 0 => {
                Err(OpError::NotFound("IAM group not found".to_owned()))
            }
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("delete_group failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn list_groups_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Vec<GroupListEntry>> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT account_id, group_name, group_arn, created_at \
             FROM iam_groups WHERE account_id = ? ORDER BY group_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_groups: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        rows.into_iter()
            .map(|(aid, gn, arn, ts)| {
                let created_at =
                    crate::sqlite_util::parse_timestamp(&ts).map_err(|e| {
                        tracing::error!("list_groups parse_timestamp: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
                Ok((aid, gn, arn, created_at))
            })
            .collect()
    }

    pub(crate) async fn get_group_detail_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<Option<GroupDetail>> {
        let exists: Option<(String,)> = sqlx::query_as(
            "SELECT group_name FROM iam_groups WHERE account_id = ? AND group_name = ?",
        )
        .bind(account_id)
        .bind(group_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_group_detail exists: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        if exists.is_none() {
            return Ok(None);
        }

        let members: Vec<(String,)> = sqlx::query_as(
            "SELECT user_name FROM iam_group_members \
             WHERE account_id = ? AND group_name = ? ORDER BY user_name",
        )
        .bind(account_id)
        .bind(group_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_group_detail members: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let policies: Vec<(String,)> = sqlx::query_as(
            "SELECT policy_name FROM iam_policies \
             WHERE account_id = ? AND principal_type = 'group' AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(group_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_group_detail policies: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let all_users: Vec<(String,)> =
            sqlx::query_as("SELECT user_name FROM iam_users WHERE account_id = ? ORDER BY user_name")
                .bind(account_id)
                .fetch_all(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("get_group_detail all_users: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

        Ok(Some(GroupDetail {
            members: members.into_iter().map(|(n,)| n).collect(),
            policies: policies.into_iter().map(|(n,)| n).collect(),
            all_users: all_users.into_iter().map(|(n,)| n).collect(),
        }))
    }

    pub(crate) async fn add_group_member_impl(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> OpResult<()> {
        let result = sqlx::query(
            "INSERT INTO iam_group_members (account_id, group_name, user_name) VALUES (?, ?, ?)",
        )
        .bind(account_id)
        .bind(group_name)
        .bind(user_name)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_unique_violation(&e) => Err(OpError::AlreadyExists(
                "User is already a member of this group".to_owned(),
            )),
            Err(e) if is_fk_violation(&e) => {
                Err(OpError::NotFound("Group or user not found".to_owned()))
            }
            Err(e) => {
                tracing::error!("add_group_member failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn remove_group_member_impl(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> OpResult<()> {
        let result = sqlx::query(
            "DELETE FROM iam_group_members \
             WHERE account_id = ? AND group_name = ? AND user_name = ?",
        )
        .bind(account_id)
        .bind(group_name)
        .bind(user_name)
        .execute(self.pool())
        .await;
        match result {
            Ok(r) if r.rows_affected() == 0 => {
                Err(OpError::NotFound("Membership not found".to_owned()))
            }
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("remove_group_member failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }
}
