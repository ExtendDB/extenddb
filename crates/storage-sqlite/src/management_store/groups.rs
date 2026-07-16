// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Group management operations for `SqliteCatalogStore`.

use extenddb_storage::management_store::{GroupDetail, GroupListEntry, OpError, OpResult};

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::{is_fk_violation, is_unique_violation, parse_timestamp};

impl SqliteCatalogStore {
    pub(crate) async fn create_group_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<()> {
        let group_arn = format!("arn:aws:iam::{account_id}:group/{group_name}");
        sqlx::query("INSERT INTO iam_groups (account_id, group_name, group_arn) VALUES (?, ?, ?)")
            .bind(account_id)
            .bind(group_name)
            .bind(&group_arn)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(|e| {
                if is_unique_violation(&e) {
                    OpError::AlreadyExists("IAM group already exists".to_owned())
                } else if is_fk_violation(&e) {
                    OpError::NotFound("Account not found".to_owned())
                } else {
                    tracing::error!("create_group: {e}");
                    OpError::Internal("Database error".to_owned())
                }
            })
    }

    pub(crate) async fn delete_group_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<()> {
        let result = sqlx::query("DELETE FROM iam_groups WHERE account_id = ? AND group_name = ?")
            .bind(account_id)
            .bind(group_name)
            .execute(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("delete_group: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
        if result.rows_affected() == 0 {
            return Err(OpError::NotFound("IAM group not found".to_owned()));
        }
        Ok(())
    }

    pub(crate) async fn list_groups_impl(&self, account_id: &str) -> OpResult<Vec<GroupListEntry>> {
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
                Ok((
                    aid,
                    gn,
                    arn,
                    parse_timestamp(&ts)
                        .map_err(|e| OpError::Internal(format!("parse created_at: {e}")))?,
                ))
            })
            .collect()
    }

    pub(crate) async fn get_group_detail_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<Option<GroupDetail>> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM iam_groups WHERE account_id = ? AND group_name = ?)",
        )
        .bind(account_id)
        .bind(group_name)
        .fetch_one(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_group_detail exists: {e}")))?;
        if !exists {
            return Ok(None);
        }

        let names = |rows: Vec<(String,)>| rows.into_iter().map(|(n,)| n).collect::<Vec<_>>();

        let members: Vec<(String,)> = sqlx::query_as(
            "SELECT user_name FROM iam_group_members \
             WHERE account_id = ? AND group_name = ? ORDER BY user_name",
        )
        .bind(account_id)
        .bind(group_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_group_detail members: {e}")))?;

        let policies: Vec<(String,)> = sqlx::query_as(
            "SELECT policy_name FROM iam_policies \
             WHERE account_id = ? AND principal_type = 'group' AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(group_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_group_detail policies: {e}")))?;

        let all_users: Vec<(String,)> = sqlx::query_as(
            "SELECT user_name FROM iam_users WHERE account_id = ? ORDER BY user_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_group_detail all_users: {e}")))?;

        Ok(Some(GroupDetail {
            members: names(members),
            policies: names(policies),
            all_users: names(all_users),
        }))
    }

    pub(crate) async fn add_group_member_impl(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> OpResult<()> {
        sqlx::query(
            "INSERT INTO iam_group_members (account_id, group_name, user_name) VALUES (?, ?, ?)",
        )
        .bind(account_id)
        .bind(group_name)
        .bind(user_name)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| {
            if is_unique_violation(&e) {
                OpError::AlreadyExists("User is already a member of this group".to_owned())
            } else if is_fk_violation(&e) {
                OpError::NotFound("Group or user not found".to_owned())
            } else {
                tracing::error!("add_group_member: {e}");
                OpError::Internal("Database error".to_owned())
            }
        })
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
        .await
        .map_err(|e| {
            tracing::error!("remove_group_member: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        if result.rows_affected() == 0 {
            return Err(OpError::NotFound("Membership not found".to_owned()));
        }
        Ok(())
    }
}
