// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ManagementStore` implementation for `TidbCatalogStore`.
//!
//! The trait impl delegates to `_impl` methods in submodules, keeping each
//! file under the 500-line limit.

use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, GroupDetail, OpResult, RoleDetail, UserDetail,
};
use futures::future::BoxFuture;

use super::catalog_store::TidbCatalogStore;

mod access_keys;
mod accounts;
mod groups;
mod policies;
mod roles;
mod users;

impl extenddb_storage::management_store::ManagementStore for TidbCatalogStore {
    // ── Accounts ───────────────────────────────────────────────────

    fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let account_name = account_name.to_string();
        Box::pin(async move { self.create_account_impl(&account_id, &account_name).await })
    }

    fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
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
        let account_id = account_id.to_string();
        Box::pin(async move { self.list_accounts_for_impl(&account_id).await })
    }

    fn get_account_detail(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<AccountDetail>>> {
        let account_id = account_id.to_string();
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
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        let password_hash = password_hash.map(|s| s.to_string());
        Box::pin(async move {
            self.create_user_impl(&account_id, &user_name, password_hash.as_deref())
                .await
        })
    }

    fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        Box::pin(async move { self.delete_user_impl(&account_id, &user_name).await })
    }

    fn list_users(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, String, bool, time::OffsetDateTime)>>> {
        let account_id = account_id.to_string();
        Box::pin(async move { self.list_users_impl(&account_id).await })
    }

    fn get_user_detail(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<UserDetail>>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        Box::pin(async move { self.get_user_detail_impl(&account_id, &user_name).await })
    }

    fn verify_iam_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<bool>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        let password = password.to_string();
        Box::pin(async move {
            self.verify_iam_user_password_impl(&account_id, &user_name, &password)
                .await
        })
    }

    fn change_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        let password_hash = password_hash.to_string();
        Box::pin(async move {
            self.change_user_password_impl(&account_id, &user_name, &password_hash)
                .await
        })
    }

    fn tag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        let tags = tags.to_vec();
        Box::pin(async move { self.tag_user_impl(&account_id, &user_name, &tags).await })
    }

    fn untag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move {
            self.untag_user_impl(&account_id, &user_name, &tag_keys)
                .await
        })
    }

    fn list_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        Box::pin(async move { self.list_user_tags_impl(&account_id, &user_name).await })
    }

    // ── Groups ─────────────────────────────────────────────────────

    fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let group_name = group_name.to_string();
        Box::pin(async move { self.create_group_impl(&account_id, &group_name).await })
    }

    fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let group_name = group_name.to_string();
        Box::pin(async move { self.delete_group_impl(&account_id, &group_name).await })
    }

    fn list_groups(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, String, time::OffsetDateTime)>>> {
        let account_id = account_id.to_string();
        Box::pin(async move { self.list_groups_impl(&account_id).await })
    }

    fn get_group_detail(
        &self,
        account_id: &str,
        group_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<GroupDetail>>> {
        let account_id = account_id.to_string();
        let group_name = group_name.to_string();
        Box::pin(async move { self.get_group_detail_impl(&account_id, &group_name).await })
    }

    fn add_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let group_name = group_name.to_string();
        let user_name = user_name.to_string();
        Box::pin(async move {
            self.add_group_member_impl(&account_id, &group_name, &user_name)
                .await
        })
    }

    fn remove_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let group_name = group_name.to_string();
        let user_name = user_name.to_string();
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
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        let trust_policy = trust_policy.clone();
        Box::pin(async move {
            self.create_role_impl(&account_id, &role_name, &trust_policy)
                .await
        })
    }

    fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        Box::pin(async move { self.delete_role_impl(&account_id, &role_name).await })
    }

    fn list_roles(
        &self,
        account_id: &str,
    ) -> BoxFuture<
        '_,
        OpResult<
            Vec<(
                String,
                String,
                String,
                serde_json::Value,
                time::OffsetDateTime,
            )>,
        >,
    > {
        let account_id = account_id.to_string();
        Box::pin(async move { self.list_roles_impl(&account_id).await })
    }

    fn get_role_detail(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<RoleDetail>>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        Box::pin(async move { self.get_role_detail_impl(&account_id, &role_name).await })
    }

    fn get_role_trust_policy(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        Box::pin(async move {
            self.get_role_trust_policy_impl(&account_id, &role_name)
                .await
        })
    }

    fn tag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        let tags = tags.to_vec();
        Box::pin(async move { self.tag_role_impl(&account_id, &role_name, &tags).await })
    }

    fn untag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move {
            self.untag_role_impl(&account_id, &role_name, &tag_keys)
                .await
        })
    }

    fn list_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
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
        let account_id = account_id.to_string();
        let principal_type = principal_type.to_string();
        let principal_name = principal_name.to_string();
        let policy_name = policy_name.to_string();
        let document = document.clone();
        Box::pin(async move {
            self.put_policy_impl(
                &account_id,
                &principal_type,
                &principal_name,
                &policy_name,
                &document,
            )
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
        let account_id = account_id.to_string();
        let principal_type = principal_type.to_string();
        let principal_name = principal_name.to_string();
        let policy_name = policy_name.to_string();
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
        let account_id = account_id.to_string();
        let principal_type = principal_type.to_string();
        let principal_name = principal_name.to_string();
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
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        let document = document.clone();
        Box::pin(async move {
            self.set_boundary_impl(&account_id, "user", &user_name, &document)
                .await
        })
    }

    fn get_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        Box::pin(async move {
            self.get_boundary_impl(&account_id, "user", &user_name)
                .await
        })
    }

    fn delete_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        Box::pin(async move {
            self.delete_boundary_impl(&account_id, "user", &user_name)
                .await
        })
    }

    fn set_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        let document = document.clone();
        Box::pin(async move {
            self.set_boundary_impl(&account_id, "role", &role_name, &document)
                .await
        })
    }

    fn get_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        Box::pin(async move {
            self.get_boundary_impl(&account_id, "role", &role_name)
                .await
        })
    }

    fn delete_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        Box::pin(async move {
            self.delete_boundary_impl(&account_id, "role", &role_name)
                .await
        })
    }

    // ── Access keys ────────────────────────────────────────────────

    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        Box::pin(async move { self.create_access_key_impl(&account_id, &user_name).await })
    }

    fn delete_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        key_id: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        let key_id = key_id.to_string();
        Box::pin(async move {
            self.delete_access_key_impl(&account_id, &user_name, &key_id)
                .await
        })
    }

    fn list_access_keys(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, bool, time::OffsetDateTime)>>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        Box::pin(async move { self.list_access_keys_impl(&account_id, &user_name).await })
    }

    fn import_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_string();
        let user_name = user_name.to_string();
        let access_key_id = access_key_id.to_string();
        let secret_access_key = secret_access_key.to_string();
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
        let session_token = session_token.to_string();
        let access_key_id = access_key_id.to_string();
        let secret_key_encrypted = secret_key_encrypted.to_vec();
        let account_id = account_id.to_string();
        let role_name = role_name.to_string();
        let session_name = session_name.to_string();
        let session_tags = session_tags.clone();
        let session_policy = session_policy.clone();
        Box::pin(async move {
            self.store_session_impl(
                &session_token,
                &access_key_id,
                &secret_key_encrypted,
                &account_id,
                &role_name,
                &session_name,
                &session_tags,
                &session_policy,
                expires_at,
            )
            .await
        })
    }

    // ── Caller tags ────────────────────────────────────────────────

    fn fetch_caller_tags(
        &self,
        account_id: &str,
        resource: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_string();
        let resource = resource.to_string();
        Box::pin(async move { self.fetch_caller_tags_impl(&account_id, &resource).await })
    }
}
