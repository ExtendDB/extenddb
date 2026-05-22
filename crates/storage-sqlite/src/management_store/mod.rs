// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM management store trait implementation for `SqliteCatalogStore`.

mod access_keys;
mod accounts;
mod groups;
mod policies;
mod roles;
mod users;

use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, GroupDetail, ManagementStore, OpError, OpResult, RoleDetail,
    UserDetail, GroupListEntry, RoleListEntry, UserListEntry,
};
use futures::future::BoxFuture;

use crate::catalog_store::SqliteCatalogStore;

impl ManagementStore for SqliteCatalogStore {
    // ── Accounts ───────────────────────────────────────────────────

    fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let account_name = account_name.to_owned();
        Box::pin(async move { self.create_account_impl(&account_id, &account_name).await })
    }

    fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.delete_account_impl(&account_id).await })
    }

    fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async move { self.list_all_accounts_impl().await })
    }

    fn list_all_accounts_full(
        &self,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, time::OffsetDateTime)>>> {
        Box::pin(async move { self.list_all_accounts_full_impl().await })
    }

    fn list_accounts_for(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.list_accounts_for_impl(&account_id).await })
    }

    fn get_account_detail(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<AccountDetail>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.get_account_detail_impl(&account_id).await })
    }

    fn dashboard_counts(&self) -> BoxFuture<'_, OpResult<(i64, i64)>> {
        Box::pin(async move { self.dashboard_counts_impl().await })
    }

    // ── Users ──────────────────────────────────────────────────────

    fn create_user(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password_hash = password_hash.map(|s| s.to_owned());
        Box::pin(
            async move { self.create_user_impl(&account_id, &user_name, password_hash.as_deref()).await },
        )
    }

    fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move { self.delete_user_impl(&account_id, &user_name).await })
    }

    fn list_users(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<UserListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.list_users_impl(&account_id).await })
    }

    fn get_user_detail(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<UserDetail>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move { self.get_user_detail_impl(&account_id, &user_name).await })
    }

    fn verify_iam_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<bool>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password = password.to_owned();
        Box::pin(
            async move { self.verify_iam_user_password_impl(&account_id, &user_name, &password).await },
        )
    }

    fn change_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password_hash = password_hash.to_owned();
        Box::pin(async move {
            self.change_user_password_impl(&account_id, &user_name, &password_hash)
                .await
        })
    }

    // ── User tags ──────────────────────────────────────────────────

    fn tag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let tags = tags.to_vec();
        Box::pin(async move { self.tag_user_impl(&account_id, &user_name, &tags).await })
    }

    fn untag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move { self.untag_user_impl(&account_id, &user_name, &tag_keys).await })
    }

    fn list_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move { self.list_user_tags_impl(&account_id, &user_name).await })
    }

    // ── Groups ─────────────────────────────────────────────────────

    fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move { self.create_group_impl(&account_id, &group_name).await })
    }

    fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move { self.delete_group_impl(&account_id, &group_name).await })
    }

    fn list_groups(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<GroupListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.list_groups_impl(&account_id).await })
    }

    fn get_group_detail(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<GroupDetail>>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        Box::pin(async move { self.get_group_detail_impl(&account_id, &group_name).await })
    }

    fn add_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(
            async move { self.add_group_member_impl(&account_id, &group_name, &user_name).await },
        )
    }

    fn remove_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let group_name = group_name.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            self.remove_group_member_impl(&account_id, &group_name, &user_name)
                .await
        })
    }

    // ── Roles ──────────────────────────────────────────────────────

    fn create_role(
        &self,
        account_id: &str,
        role_name: &str,
        trust_policy: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let trust_policy = trust_policy.clone();
        Box::pin(
            async move { self.create_role_impl(&account_id, &role_name, &trust_policy).await },
        )
    }

    fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move { self.delete_role_impl(&account_id, &role_name).await })
    }

    fn list_roles(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<RoleListEntry>>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.list_roles_impl(&account_id).await })
    }

    fn get_role_detail(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<RoleDetail>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move { self.get_role_detail_impl(&account_id, &role_name).await })
    }

    fn get_role_trust_policy(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move { self.get_role_trust_policy_impl(&account_id, &role_name).await })
    }

    // ── Role tags ──────────────────────────────────────────────────

    fn tag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let tags = tags.to_vec();
        Box::pin(async move { self.tag_role_impl(&account_id, &role_name, &tags).await })
    }

    fn untag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move { self.untag_role_impl(&account_id, &role_name, &tag_keys).await })
    }

    fn list_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move { self.list_role_tags_impl(&account_id, &role_name).await })
    }

    // ── Policies ───────────────────────────────────────────────────

    fn put_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        let policy_name = policy_name.to_owned();
        let document = document.clone();
        Box::pin(async move {
            self.put_policy_impl(&account_id, &principal_type, &principal_name, &policy_name, &document)
                .await
        })
    }

    fn delete_policy(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
        policy_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        let policy_name = policy_name.to_owned();
        Box::pin(async move {
            self.delete_policy_impl(&account_id, &principal_type, &principal_name, &policy_name)
                .await
        })
    }

    fn list_policies(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, serde_json::Value, time::OffsetDateTime)>>> {
        let account_id = account_id.to_owned();
        let principal_type = principal_type.to_owned();
        let principal_name = principal_name.to_owned();
        Box::pin(async move {
            self.list_policies_impl(&account_id, &principal_type, &principal_name)
                .await
        })
    }

    // ── Permissions boundaries ─────────────────────────────────────

    fn set_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let document = document.clone();
        Box::pin(
            async move { self.set_user_boundary_impl(&account_id, &user_name, &document).await },
        )
    }

    fn get_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move { self.get_user_boundary_impl(&account_id, &user_name).await })
    }

    fn delete_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move { self.delete_user_boundary_impl(&account_id, &user_name).await })
    }

    fn set_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let document = document.clone();
        Box::pin(
            async move { self.set_role_boundary_impl(&account_id, &role_name, &document).await },
        )
    }

    fn get_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move { self.get_role_boundary_impl(&account_id, &role_name).await })
    }

    fn delete_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move { self.delete_role_boundary_impl(&account_id, &role_name).await })
    }

    // ── Access keys ────────────────────────────────────────────────

    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move { self.create_access_key_impl(&account_id, &user_name).await })
    }

    fn delete_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        key_id: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let key_id = key_id.to_owned();
        Box::pin(
            async move { self.delete_access_key_impl(&account_id, &user_name, &key_id).await },
        )
    }

    fn list_access_keys(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, bool, time::OffsetDateTime)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move { self.list_access_keys_impl(&account_id, &user_name).await })
    }

    fn import_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let access_key_id = access_key_id.to_owned();
        let secret_access_key = secret_access_key.to_owned();
        Box::pin(async move {
            self.import_access_key_impl(&account_id, &user_name, &access_key_id, &secret_access_key)
                .await
        })
    }

    // ── Sessions ───────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn store_session(
        &self,
        session_token: &str,
        access_key_id: &str,
        secret_key_encrypted: &[u8],
        account_id: &str,
        role_name: &str,
        session_name: &str,
        session_tags: &Option<serde_json::Value>,
        session_policy: &Option<serde_json::Value>,
        expires_at: time::OffsetDateTime,
    ) -> BoxFuture<'_, OpResult<()>> {
        let session_token = session_token.to_owned();
        let access_key_id = access_key_id.to_owned();
        let secret_key_encrypted = secret_key_encrypted.to_vec();
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        let session_tags = session_tags.clone();
        let session_policy = session_policy.clone();
        let pool = self.pool().clone();
        Box::pin(async move {
            let expires_str = crate::sqlite_util::format_timestamp(expires_at);
            let tags_json = session_tags
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default());
            let policy_json = session_policy
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default());

            sqlx::query(
                "INSERT INTO iam_sessions \
                 (session_token, access_key_id, secret_key_encrypted, account_id, role_name, \
                  session_name, session_tags, session_policy, expires_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&session_token)
            .bind(&access_key_id)
            .bind(&secret_key_encrypted)
            .bind(&account_id)
            .bind(&role_name)
            .bind(&session_name)
            .bind(tags_json.as_deref())
            .bind(policy_json.as_deref())
            .bind(&expires_str)
            .execute(&pool)
            .await
            .map_err(|e| {
                tracing::error!("store_session: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(())
        })
    }

    // ── Caller tags ────────────────────────────────────────────────

    fn fetch_caller_tags(
        &self,
        account_id: &str,
        resource: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let resource = resource.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            // resource can be a user ARN or role ARN
            let user_tags: Vec<(String, String)> = sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_user_tags \
                 WHERE account_id = ? AND user_name = ? ORDER BY tag_key",
            )
            .bind(&account_id)
            .bind(&resource)
            .fetch_all(&pool)
            .await
            .unwrap_or_default();

            if !user_tags.is_empty() {
                return Ok(user_tags);
            }

            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_role_tags \
                 WHERE account_id = ? AND role_name = ? ORDER BY tag_key",
            )
            .bind(&account_id)
            .bind(&resource)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_caller_tags: {e}");
                OpError::Internal("Database error".to_owned())
            })
        })
    }
}
