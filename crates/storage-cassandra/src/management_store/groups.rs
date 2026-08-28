// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Group management operations for `CassandraCatalogStore`.

use extenddb_storage::management_store::{GroupDetail, OpError, OpResult};

use crate::catalog_store::CassandraCatalogStore;

impl CassandraCatalogStore {
    pub(crate) async fn create_group_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<()> {
        // Check if account exists (emulate FK constraint)
        if !self.account_exists(account_id).await? {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();
        let group_arn = format!("arn:aws:iam::{account_id}:group/{group_name}");
        let insert_query = format!(
            "INSERT INTO {}.iam_groups (account_id, group_name, group_arn, created_at) \
             VALUES (?, ?, ?, toTimestamp(now())) IF NOT EXISTS",
            catalog_keyspace
        );

        let applied = crate::cassandra_util::apply_lwt(
            self.session(),
            &insert_query,
            cdrs_tokio::query_values!(account_id, group_name, group_arn.as_str()),
            "create_group",
        )
        .await?;

        if !applied {
            return Err(OpError::AlreadyExists(
                "IAM group already exists".to_owned(),
            ));
        }

        Ok(())
    }

    pub(crate) async fn delete_group_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<()> {
        if !self.group_exists(account_id, group_name).await? {
            return Err(OpError::NotFound("IAM group not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();
        let delete_query = format!(
            "DELETE FROM {}.iam_groups WHERE account_id = ? AND group_name = ?",
            catalog_keyspace
        );

        crate::cassandra_util::execute(
            self.session(),
            &delete_query,
            cdrs_tokio::query_values!(account_id, group_name),
            "delete_group",
        )
        .await
    }

    pub(crate) async fn list_groups_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Vec<(String, String, String, time::OffsetDateTime)>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT account_id, group_name, group_arn, created_at \
             FROM {}.iam_groups WHERE account_id = ?",
            catalog_keyspace
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id),
            "list_groups",
        )
        .await?;

        let mut groups = crate::cassandra_util::map_rows(
            rows,
            |row| {
                use crate::cassandra_util::{get_column, get_timestamp};
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "account_id", "list_groups")?,
                    get_column::<String, _>(row, "group_name", "list_groups")?,
                    get_column::<String, _>(row, "group_arn", "list_groups")?,
                    get_timestamp(row, "created_at", "list_groups")?,
                ))
            },
            "list_groups",
        )?;

        groups.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(groups)
    }

    pub(crate) async fn get_group_detail_impl(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> OpResult<Option<GroupDetail>> {
        if !self.group_exists(account_id, group_name).await? {
            return Ok(None);
        }

        let catalog_keyspace = self.catalog_keyspace();

        // Get members
        let members_query = format!(
            "SELECT user_name FROM {}.iam_group_members WHERE account_id = ? AND group_name = ?",
            catalog_keyspace
        );

        let members_rows = crate::cassandra_util::query_rows(
            self.session(),
            &members_query,
            cdrs_tokio::query_values!(account_id, group_name),
            "get_group_detail",
        )
        .await?;

        let mut members = crate::cassandra_util::map_rows(
            members_rows,
            |row| {
                use crate::cassandra_util::get_column;
                get_column::<String, _>(row, "user_name", "get_group_detail")
            },
            "get_group_detail",
        )?;
        members.sort();

        // Get policies
        let policies_query = format!(
            "SELECT policy_name FROM {}.iam_policies \
             WHERE account_id = ? AND principal_type = 'group' AND principal_name = ?",
            catalog_keyspace
        );

        let policies_rows = crate::cassandra_util::query_rows(
            self.session(),
            &policies_query,
            cdrs_tokio::query_values!(account_id, group_name),
            "get_group_detail",
        )
        .await?;

        let mut policies = crate::cassandra_util::map_rows(
            policies_rows,
            |row| {
                use crate::cassandra_util::get_column;
                get_column::<String, _>(row, "policy_name", "get_group_detail")
            },
            "get_group_detail",
        )?;
        policies.sort();

        // Get all users in account
        let all_users_query = format!(
            "SELECT user_name FROM {}.iam_users WHERE account_id = ?",
            catalog_keyspace
        );

        let all_users_rows = crate::cassandra_util::query_rows(
            self.session(),
            &all_users_query,
            cdrs_tokio::query_values!(account_id),
            "get_group_detail",
        )
        .await?;

        let mut all_users = crate::cassandra_util::map_rows(
            all_users_rows,
            |row| {
                use crate::cassandra_util::get_column;
                get_column::<String, _>(row, "user_name", "get_group_detail")
            },
            "get_group_detail",
        )?;
        all_users.sort();

        Ok(Some(GroupDetail {
            members,
            policies,
            all_users,
        }))
    }

    pub(crate) async fn add_group_member_impl(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> OpResult<()> {
        // Check if group and user exist (emulate FK constraint)
        if !self.group_exists(account_id, group_name).await? {
            return Err(OpError::NotFound("Group or user not found".to_owned()));
        }

        if !self.user_exists(account_id, user_name).await? {
            return Err(OpError::NotFound("Group or user not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();
        let insert_query = format!(
            "INSERT INTO {}.iam_group_members (account_id, group_name, user_name) VALUES (?, ?, ?) IF NOT EXISTS",
            catalog_keyspace
        );

        let applied = crate::cassandra_util::apply_lwt(
            self.session(),
            &insert_query,
            cdrs_tokio::query_values!(account_id, group_name, user_name),
            "add_group_member",
        )
        .await?;

        if !applied {
            return Err(OpError::AlreadyExists(
                "User is already a member of this group".to_owned(),
            ));
        }

        Ok(())
    }

    pub(crate) async fn remove_group_member_impl(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> OpResult<()> {
        let catalog_keyspace = self.catalog_keyspace();

        // Check if membership exists
        let check_query = format!(
            "SELECT user_name FROM {}.iam_group_members \
             WHERE account_id = ? AND group_name = ? AND user_name = ?",
            catalog_keyspace
        );

        if crate::cassandra_util::query_optional(
            self.session(),
            &check_query,
            cdrs_tokio::query_values!(account_id, group_name, user_name),
            "remove_group_member",
        )
        .await?
        .is_none()
        {
            return Err(OpError::NotFound("Membership not found".to_owned()));
        }

        let delete_query = format!(
            "DELETE FROM {}.iam_group_members WHERE account_id = ? AND group_name = ? AND user_name = ?",
            catalog_keyspace
        );

        crate::cassandra_util::execute(
            self.session(),
            &delete_query,
            cdrs_tokio::query_values!(account_id, group_name, user_name),
            "remove_group_member",
        )
        .await
    }

    // Helper to check if group exists
    async fn group_exists(&self, account_id: &str, group_name: &str) -> Result<bool, OpError> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT group_name FROM {}.iam_groups WHERE account_id = ? AND group_name = ?",
            catalog_keyspace
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id, group_name),
            "group_exists",
        )
        .await?;

        Ok(!rows.is_empty())
    }
}
