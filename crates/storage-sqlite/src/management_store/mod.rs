// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ManagementStore` implementation for `SqliteCatalogStore`.
//!
//! The IAM surface is split across submodules by entity (`accounts`, `users`,
//! `groups`, `roles`, `policies`, `access_keys`), each providing inherent
//! `*_impl` methods. This module declares those submodules and implements the
//! `ManagementStore` trait by delegating to them, plus the session-storage and
//! caller-tag lookups that don't belong to a single entity.

mod access_keys;
mod accounts;
mod groups;
mod policies;
mod roles;
mod users;

use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, GroupDetail, GroupListEntry, ManagementStore, OpError,
    OpResult, RoleDetail, RoleListEntry, SessionPolicyEntry, UserDetail, UserListEntry,
};
use futures::future::BoxFuture;
use time::OffsetDateTime;

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::format_timestamp;

impl ManagementStore for SqliteCatalogStore {
    // ── Accounts ───────────────────────────────────────────────────

    fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, account_name) = (account_id.to_owned(), account_name.to_owned());
        Box::pin(async move { self.create_account_impl(&account_id, &account_name).await })
    }

    fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.delete_account_impl(&account_id).await })
    }

    fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async move { self.list_all_accounts_impl().await })
    }

    fn default_account_id(&self) -> BoxFuture<'_, OpResult<Option<String>>> {
        Box::pin(async move { self.default_account_id_impl().await })
    }

    fn list_all_accounts_full(
        &self,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String, OffsetDateTime)>>> {
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
        let (account_id, user_name) = (account_id.to_owned(), user_name.to_owned());
        let password_hash = password_hash.map(str::to_owned);
        Box::pin(async move {
            self.create_user_impl(&account_id, &user_name, password_hash.as_deref())
                .await
        })
    }

    fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, user_name) = (account_id.to_owned(), user_name.to_owned());
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
        let (account_id, user_name) = (account_id.to_owned(), user_name.to_owned());
        Box::pin(async move { self.get_user_detail_impl(&account_id, &user_name).await })
    }

    fn verify_iam_user_password(
        &self,
        account_id: &str,
        user_name: &str,
        password: &str,
    ) -> BoxFuture<'_, OpResult<bool>> {
        let (account_id, user_name, password) = (
            account_id.to_owned(),
            user_name.to_owned(),
            password.to_owned(),
        );
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
        let (account_id, user_name, password_hash) = (
            account_id.to_owned(),
            user_name.to_owned(),
            password_hash.to_owned(),
        );
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
        let (account_id, user_name, tags) =
            (account_id.to_owned(), user_name.to_owned(), tags.to_vec());
        Box::pin(async move { self.tag_user_impl(&account_id, &user_name, &tags).await })
    }

    fn untag_user(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, user_name, tag_keys) = (
            account_id.to_owned(),
            user_name.to_owned(),
            tag_keys.to_vec(),
        );
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
        let (account_id, user_name) = (account_id.to_owned(), user_name.to_owned());
        Box::pin(async move { self.list_user_tags_impl(&account_id, &user_name).await })
    }

    // ── Groups ─────────────────────────────────────────────────────

    fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, group_name) = (account_id.to_owned(), group_name.to_owned());
        Box::pin(async move { self.create_group_impl(&account_id, &group_name).await })
    }

    fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, group_name) = (account_id.to_owned(), group_name.to_owned());
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
        let (account_id, group_name) = (account_id.to_owned(), group_name.to_owned());
        Box::pin(async move { self.get_group_detail_impl(&account_id, &group_name).await })
    }

    fn add_group_member(
        &self,
        account_id: &str,
        group_name: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, group_name, user_name) = (
            account_id.to_owned(),
            group_name.to_owned(),
            user_name.to_owned(),
        );
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
        let (account_id, group_name, user_name) = (
            account_id.to_owned(),
            group_name.to_owned(),
            user_name.to_owned(),
        );
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
        let (account_id, role_name, trust_policy) = (
            account_id.to_owned(),
            role_name.to_owned(),
            trust_policy.clone(),
        );
        Box::pin(async move {
            self.create_role_impl(&account_id, &role_name, &trust_policy)
                .await
        })
    }

    fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, role_name) = (account_id.to_owned(), role_name.to_owned());
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
        let (account_id, role_name) = (account_id.to_owned(), role_name.to_owned());
        Box::pin(async move { self.get_role_detail_impl(&account_id, &role_name).await })
    }

    fn get_role_trust_policy(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let (account_id, role_name) = (account_id.to_owned(), role_name.to_owned());
        Box::pin(async move {
            self.get_role_trust_policy_impl(&account_id, &role_name)
                .await
        })
    }

    // ── Role tags ──────────────────────────────────────────────────

    fn tag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, role_name, tags) =
            (account_id.to_owned(), role_name.to_owned(), tags.to_vec());
        Box::pin(async move { self.tag_role_impl(&account_id, &role_name, &tags).await })
    }

    fn untag_role(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, role_name, tag_keys) = (
            account_id.to_owned(),
            role_name.to_owned(),
            tag_keys.to_vec(),
        );
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
        let (account_id, role_name) = (account_id.to_owned(), role_name.to_owned());
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
        let (account_id, principal_type, principal_name, policy_name, document) = (
            account_id.to_owned(),
            principal_type.to_owned(),
            principal_name.to_owned(),
            policy_name.to_owned(),
            document.clone(),
        );
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
        let (account_id, principal_type, principal_name, policy_name) = (
            account_id.to_owned(),
            principal_type.to_owned(),
            principal_name.to_owned(),
            policy_name.to_owned(),
        );
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
    ) -> BoxFuture<'_, OpResult<Vec<(String, serde_json::Value, OffsetDateTime)>>> {
        let (account_id, principal_type, principal_name) = (
            account_id.to_owned(),
            principal_type.to_owned(),
            principal_name.to_owned(),
        );
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
        let (account_id, user_name, document) = (
            account_id.to_owned(),
            user_name.to_owned(),
            document.clone(),
        );
        Box::pin(async move {
            self.set_user_boundary_impl(&account_id, &user_name, &document)
                .await
        })
    }

    fn get_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let (account_id, user_name) = (account_id.to_owned(), user_name.to_owned());
        Box::pin(async move { self.get_user_boundary_impl(&account_id, &user_name).await })
    }

    fn delete_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, user_name) = (account_id.to_owned(), user_name.to_owned());
        Box::pin(async move {
            self.delete_user_boundary_impl(&account_id, &user_name)
                .await
        })
    }

    fn set_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, role_name, document) = (
            account_id.to_owned(),
            role_name.to_owned(),
            document.clone(),
        );
        Box::pin(async move {
            self.set_role_boundary_impl(&account_id, &role_name, &document)
                .await
        })
    }

    fn get_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>> {
        let (account_id, role_name) = (account_id.to_owned(), role_name.to_owned());
        Box::pin(async move { self.get_role_boundary_impl(&account_id, &role_name).await })
    }

    fn delete_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, role_name) = (account_id.to_owned(), role_name.to_owned());
        Box::pin(async move {
            self.delete_role_boundary_impl(&account_id, &role_name)
                .await
        })
    }

    // ── Access keys ────────────────────────────────────────────────

    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>> {
        let (account_id, user_name) = (account_id.to_owned(), user_name.to_owned());
        Box::pin(async move { self.create_access_key_impl(&account_id, &user_name).await })
    }

    fn delete_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        key_id: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, user_name, key_id) = (
            account_id.to_owned(),
            user_name.to_owned(),
            key_id.to_owned(),
        );
        Box::pin(async move {
            self.delete_access_key_impl(&account_id, &user_name, &key_id)
                .await
        })
    }

    fn list_access_keys(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, bool, OffsetDateTime)>>> {
        let (account_id, user_name) = (account_id.to_owned(), user_name.to_owned());
        Box::pin(async move { self.list_access_keys_impl(&account_id, &user_name).await })
    }

    fn import_access_key(
        &self,
        account_id: &str,
        user_name: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> BoxFuture<'_, OpResult<()>> {
        let (account_id, user_name, access_key_id, secret_access_key) = (
            account_id.to_owned(),
            user_name.to_owned(),
            access_key_id.to_owned(),
            secret_access_key.to_owned(),
        );
        Box::pin(async move {
            self.import_access_key_impl(&account_id, &user_name, &access_key_id, &secret_access_key)
                .await
        })
    }

    // ── Sessions (AssumeRole) ──────────────────────────────────────

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
        expires_at: OffsetDateTime,
    ) -> BoxFuture<'_, OpResult<()>> {
        let session_token = session_token.to_owned();
        let access_key_id = access_key_id.to_owned();
        let secret_key_encrypted = secret_key_encrypted.to_vec();
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        let session_tags = session_tags.as_ref().map(ToString::to_string);
        let session_policy = session_policy.as_ref().map(ToString::to_string);
        let expires = format_timestamp(expires_at);
        Box::pin(async move {
            // D1: every writer holds the write lock.
            let _writer = self.lock_writes().await;
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
            .bind(&session_tags)
            .bind(&session_policy)
            .bind(&expires)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(|e| {
                tracing::error!("store_session: {e}");
                OpError::Internal("Database error".to_owned())
            })
        })
    }

    // ── Session policies (resource-tag migration) ─────────────────

    fn list_session_policies(&self) -> BoxFuture<'_, OpResult<Vec<SessionPolicyEntry>>> {
        Box::pin(async move {
            let rows: Vec<(String, String, String, String, Option<String>)> = sqlx::query_as(
                "SELECT session_token, account_id, role_name, session_name, session_policy \
                 FROM iam_sessions WHERE session_policy IS NOT NULL",
            )
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("list_session_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let mut out = Vec::new();
            for (session_token, account_id, role_name, session_name, policy) in rows {
                let Some(text) = policy else { continue };
                // A session policy that does not parse is a corrupt row; skip it
                // rather than failing the whole migration.
                if let Ok(policy) = serde_json::from_str::<serde_json::Value>(&text) {
                    out.push(SessionPolicyEntry {
                        session_token,
                        account_id,
                        role_name,
                        session_name,
                        policy,
                    });
                }
            }
            Ok(out)
        })
    }

    fn update_session_policy(
        &self,
        session_token: &str,
        document: &serde_json::Value,
    ) -> BoxFuture<'_, OpResult<()>> {
        let session_token = session_token.to_owned();
        let document = document.to_string();
        Box::pin(async move {
            let _writer = self.lock_writes().await;
            sqlx::query("UPDATE iam_sessions SET session_policy = ? WHERE session_token = ?")
                .bind(&document)
                .bind(&session_token)
                .execute(self.pool())
                .await
                .map(|_| ())
                .map_err(|e| {
                    tracing::error!("update_session_policy: {e}");
                    OpError::Internal("Database error".to_owned())
                })
        })
    }

    // ── Caller tags (trust-policy evaluation) ──────────────────────

    fn fetch_caller_tags(
        &self,
        account_id: &str,
        resource: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let resource = resource.to_owned();
        Box::pin(async move {
            // The server passes the ARN resource part, e.g. `user/<name>`,
            // `role/<name>`, or `assumed-role/<role>/<session>`. Match the same
            // prefixes the other backends use so caller tags resolve on SQLite.
            if let Some(user_name) = resource.strip_prefix("user/") {
                return sqlx::query_as(
                    "SELECT tag_key, tag_value FROM iam_user_tags \
                     WHERE account_id = ? AND user_name = ? ORDER BY tag_key",
                )
                .bind(&account_id)
                .bind(user_name)
                .fetch_all(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("fetch_caller_tags (user): {e}");
                    OpError::Internal("Database error".to_owned())
                });
            }

            // assumed-role/RoleName/SessionName or role/RoleName → role tags.
            let role_name = resource
                .strip_prefix("assumed-role/")
                .or_else(|| resource.strip_prefix("role/"))
                .map(|rest| rest.split('/').next().unwrap_or(rest));

            if let Some(role_name) = role_name {
                if role_name.is_empty() {
                    return Ok(Vec::new());
                }
                return sqlx::query_as(
                    "SELECT tag_key, tag_value FROM iam_role_tags \
                     WHERE account_id = ? AND role_name = ? ORDER BY tag_key",
                )
                .bind(&account_id)
                .bind(role_name)
                .fetch_all(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("fetch_caller_tags (role): {e}");
                    OpError::Internal("Database error".to_owned())
                });
            }

            Ok(Vec::new())
        })
    }
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    use extenddb_storage::management_store::ManagementStore;
    use extenddb_storage::policy_migration::migrate_resource_tag_condition_keys;
    use serde_json::json;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn store() -> SqliteCatalogStore {
        // One connection keeps the in-memory database alive for the test.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await
            .unwrap();
        crate::schema::apply(&pool).await.expect("apply schema");
        SqliteCatalogStore::new(pool)
    }

    fn policy(resource_tag_key: &str) -> serde_json::Value {
        json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": "*",
                "Condition": {"StringEquals": {resource_tag_key: "prod"}}
            }]
        })
    }

    #[tokio::test]
    async fn rewrites_inline_boundary_and_session_policies() {
        let s = store().await;
        s.create_account("111111111111", "acct").await.unwrap();
        s.create_user("111111111111", "alice", None).await.unwrap();
        s.put_policy(
            "111111111111",
            "user",
            "alice",
            "p1",
            &policy("dynamodb:ResourceTag/env"),
        )
        .await
        .unwrap();
        s.set_user_boundary("111111111111", "alice", &policy("dynamodb:ResourceTag/env"))
            .await
            .unwrap();
        s.create_role(
            "111111111111",
            "r1",
            &json!({"Version": "2012-10-17", "Statement": []}),
        )
        .await
        .unwrap();
        s.store_session(
            "tok-1",
            "AKIAEXAMPLE000000001",
            b"enc",
            "111111111111",
            "r1",
            "sess",
            &None,
            &Some(policy("dynamodb:ResourceTag/env")),
            time::OffsetDateTime::now_utc() + time::Duration::hours(1),
        )
        .await
        .unwrap();

        // Detect mode writes nothing.
        let detect = migrate_resource_tag_condition_keys(&s, false).await;
        assert_eq!(detect.errors, 0);
        assert_eq!(detect.rewritten.len(), 3, "inline + boundary + session");
        assert!(detect.needs_attention.is_empty());
        // Still on the old key.
        let before = s
            .list_policies("111111111111", "user", "alice")
            .await
            .unwrap();
        let p1_before = before.iter().find(|(n, ..)| n == "p1").unwrap();
        assert!(
            p1_before
                .1
                .pointer("/Statement/0/Condition/StringEquals")
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("dynamodb:ResourceTag/env")
        );

        // Apply mode writes.
        let applied = migrate_resource_tag_condition_keys(&s, true).await;
        assert_eq!(applied.errors, 0);
        assert_eq!(applied.rewritten.len(), 3);

        let after = s
            .list_policies("111111111111", "user", "alice")
            .await
            .unwrap();
        let p1_after = after.iter().find(|(n, ..)| n == "p1").unwrap();
        assert!(
            p1_after
                .1
                .pointer("/Statement/0/Condition/StringEquals")
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("aws:ResourceTag/env")
        );
        let boundary = s
            .get_user_boundary("111111111111", "alice")
            .await
            .unwrap()
            .unwrap();
        assert!(
            boundary
                .pointer("/Statement/0/Condition/StringEquals")
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("aws:ResourceTag/env")
        );
        let sessions = s.list_session_policies().await.unwrap();
        assert_eq!(sessions.len(), 1);
        assert!(
            sessions[0]
                .policy
                .pointer("/Statement/0/Condition/StringEquals")
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("aws:ResourceTag/env")
        );

        // Idempotent: a second pass changes nothing.
        let again = migrate_resource_tag_condition_keys(&s, true).await;
        assert_eq!(again.errors, 0);
        assert!(again.rewritten.is_empty());
    }

    #[tokio::test]
    async fn collision_is_reported_and_left_unchanged() {
        let s = store().await;
        s.create_account("222222222222", "acct").await.unwrap();
        s.create_user("222222222222", "bob", None).await.unwrap();
        let collide = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": "*",
                "Condition": {"StringEquals": {
                    "aws:ResourceTag/env": "prod",
                    "dynamodb:ResourceTag/env": "prod"
                }}
            }]
        });
        s.put_policy("222222222222", "user", "bob", "p", &collide)
            .await
            .unwrap();

        let report = migrate_resource_tag_condition_keys(&s, true).await;
        assert_eq!(report.errors, 0);
        assert!(report.rewritten.is_empty());
        assert_eq!(report.needs_attention.len(), 1);
        assert_eq!(report.needs_attention[0].principal, "user/bob");
        // Unchanged on disk.
        let stored = s
            .list_policies("222222222222", "user", "bob")
            .await
            .unwrap();
        let p = stored.iter().find(|(n, ..)| n == "p").unwrap();
        assert_eq!(p.1, collide);
    }
}
