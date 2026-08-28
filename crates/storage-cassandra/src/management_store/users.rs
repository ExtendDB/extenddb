// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! User management operations for `CassandraCatalogStore`.

use crate::catalog_store::CassandraCatalogStore;
use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::management_store::{OpError, OpResult, UserDetail};

impl CassandraCatalogStore {
    pub(crate) async fn create_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> OpResult<()> {
        // Check if account exists (emulate FK constraint)
        if !self.account_exists(account_id).await? {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        let user_arn = format!("arn:aws:iam::{account_id}:user/{user_name}");
        let catalog_keyspace = self.catalog_keyspace();
        let session = self.session().clone();

        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let password_hash = password_hash.map(|s| s.to_owned());

        // Seed self-service policy document
        let self_service_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "iam:CreateAccessKey",
                    "iam:DeleteAccessKey",
                    "iam:ListAccessKeys",
                    "iam:ChangePassword"
                ],
                "Resource": format!("arn:aws:iam::{account_id}:user/{user_name}")
            }]
        });

        // Use LWT to atomically create the user, preventing duplicate names.
        let user_query = format!(
            "INSERT INTO {}.iam_users (account_id, user_name, user_arn, password_hash, created_at) \
             VALUES (?, ?, ?, ?, toTimestamp(now())) IF NOT EXISTS",
            catalog_keyspace
        );

        let applied = crate::cassandra_util::apply_lwt(
            &session,
            &user_query,
            cdrs_tokio::query_values!(
                account_id.as_str(),
                user_name.as_str(),
                user_arn.as_str(),
                password_hash.as_deref()
            ),
            "create_user",
        )
        .await?;

        if !applied {
            return Err(OpError::AlreadyExists("IAM user already exists".to_owned()));
        }

        // User was created; now insert the self-service policy.
        let policy_query = format!(
            "INSERT INTO {}.iam_policies (account_id, principal_type, principal_name, policy_name, policy_document, created_at) \
             VALUES (?, 'user', ?, 'SelfServicePolicy', ?, toTimestamp(now()))",
            catalog_keyspace
        );

        session
            .query_with_values(
                &policy_query,
                cdrs_tokio::query_values!(
                    account_id.as_str(),
                    user_name.as_str(),
                    self_service_policy.to_string().as_str()
                ),
            )
            .await
            .map_err(|e| {
                tracing::error!("create_user policy insert failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

        Ok(())
    }

    pub(crate) async fn delete_user_impl(&self, account_id: &str, user_name: &str) -> OpResult<()> {
        // Check if user exists
        if !self.user_exists(account_id, user_name).await? {
            return Err(OpError::NotFound("IAM user not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();
        let delete_query = format!(
            "DELETE FROM {}.iam_users WHERE account_id = ? AND user_name = ?",
            catalog_keyspace
        );

        crate::cassandra_util::execute(
            self.session(),
            &delete_query,
            cdrs_tokio::query_values!(account_id, user_name),
            "delete_user",
        )
        .await
    }

    pub(crate) async fn list_users_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Vec<(String, String, String, bool, time::OffsetDateTime)>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT account_id, user_name, user_arn, password_hash, created_at \
             FROM {}.iam_users WHERE account_id = ?",
            catalog_keyspace
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id),
            "list_users",
        )
        .await?;

        let mut users = crate::cassandra_util::map_rows(
            rows,
            |row| {
                use crate::cassandra_util::{get_column, get_timestamp};
                let password_hash_opt: Option<String> =
                    row.get_by_name("password_hash").ok().flatten();
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "account_id", "list_users")?,
                    get_column::<String, _>(row, "user_name", "list_users")?,
                    get_column::<String, _>(row, "user_arn", "list_users")?,
                    password_hash_opt.is_some(),
                    get_timestamp(row, "created_at", "list_users")?,
                ))
            },
            "list_users",
        )?;

        // Sort by user_name to match PostgreSQL ORDER BY
        users.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(users)
    }

    pub(crate) async fn get_user_detail_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Option<UserDetail>> {
        let catalog_keyspace = self.catalog_keyspace();

        // Check if user exists
        if !self.user_exists(account_id, user_name).await? {
            return Ok(None);
        }

        // Get access keys
        let keys_query = format!(
            "SELECT access_key_id, is_active FROM {}.access_keys \
             WHERE account_id = ? AND user_name = ? ALLOW FILTERING",
            catalog_keyspace
        );

        let keys_rows = crate::cassandra_util::query_rows(
            self.session(),
            &keys_query,
            cdrs_tokio::query_values!(account_id, user_name),
            "get_user_detail",
        )
        .await?;

        let mut keys = crate::cassandra_util::map_rows(
            keys_rows,
            |row| {
                use crate::cassandra_util::get_column;
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "access_key_id", "get_user_detail")?,
                    get_column::<bool, _>(row, "is_active", "get_user_detail")?,
                ))
            },
            "get_user_detail",
        )?;
        keys.sort_by(|a, b| a.0.cmp(&b.0));

        // Get policies
        let policies_query = format!(
            "SELECT policy_name FROM {}.iam_policies \
             WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?",
            catalog_keyspace
        );

        let policies_rows = crate::cassandra_util::query_rows(
            self.session(),
            &policies_query,
            cdrs_tokio::query_values!(account_id, user_name),
            "get_user_detail",
        )
        .await?;

        let mut policies = crate::cassandra_util::map_rows(
            policies_rows,
            |row| {
                use crate::cassandra_util::get_column;
                get_column::<String, _>(row, "policy_name", "get_user_detail")
            },
            "get_user_detail",
        )?;
        policies.sort();

        // Get tags
        let tags_query = format!(
            "SELECT tag_key, tag_value FROM {}.iam_user_tags \
             WHERE account_id = ? AND user_name = ?",
            catalog_keyspace
        );

        let tags_rows = crate::cassandra_util::query_rows(
            self.session(),
            &tags_query,
            cdrs_tokio::query_values!(account_id, user_name),
            "get_user_detail",
        )
        .await?;

        let mut tags = crate::cassandra_util::map_rows(
            tags_rows,
            |row| {
                use crate::cassandra_util::get_column;
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "tag_key", "get_user_detail")?,
                    get_column::<String, _>(row, "tag_value", "get_user_detail")?,
                ))
            },
            "get_user_detail",
        )?;
        tags.sort_by(|a, b| a.0.cmp(&b.0));

        // Get groups
        let groups_query = format!(
            "SELECT group_name FROM {}.iam_group_members \
             WHERE account_id = ? AND user_name = ? ALLOW FILTERING",
            catalog_keyspace
        );

        let groups_rows = crate::cassandra_util::query_rows(
            self.session(),
            &groups_query,
            cdrs_tokio::query_values!(account_id, user_name),
            "get_user_detail",
        )
        .await?;

        let mut groups = crate::cassandra_util::map_rows(
            groups_rows,
            |row| {
                use crate::cassandra_util::get_column;
                get_column::<String, _>(row, "group_name", "get_user_detail")
            },
            "get_user_detail",
        )?;
        groups.sort();

        Ok(Some(UserDetail {
            keys,
            policies,
            tags,
            groups,
        }))
    }

    pub(crate) async fn verify_iam_user_password_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password: &str,
    ) -> OpResult<bool> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT password_hash FROM {}.iam_users \
             WHERE account_id = ? AND user_name = ?",
            catalog_keyspace
        );

        let row = crate::cassandra_util::query_optional(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id, user_name),
            "verify_iam_user_password",
        )
        .await?;

        let Some(row) = row else {
            return Ok(false);
        };

        let hash_opt: Option<String> = row.get_by_name("password_hash").ok().flatten();

        let Some(hash) = hash_opt else {
            return Ok(false);
        };

        let pw = password.to_owned();
        Ok(
            tokio::task::spawn_blocking(move || bcrypt::verify(pw, &hash).unwrap_or(false))
                .await
                .unwrap_or(false),
        )
    }

    pub(crate) async fn change_user_password_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> OpResult<()> {
        // Check if user exists
        if !self.user_exists(account_id, user_name).await? {
            return Err(OpError::NotFound("IAM user not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();
        let update_query = format!(
            "UPDATE {}.iam_users SET password_hash = ? WHERE account_id = ? AND user_name = ?",
            catalog_keyspace
        );

        crate::cassandra_util::execute(
            self.session(),
            &update_query,
            cdrs_tokio::query_values!(password_hash, account_id, user_name),
            "change_user_password",
        )
        .await
    }

    // ── User tags ──────────────────────────────────────────────────

    pub(crate) async fn tag_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        tags: &[(String, String)],
    ) -> OpResult<()> {
        // Check if user exists (emulate FK constraint)
        if !self.user_exists(account_id, user_name).await? {
            return Err(OpError::NotFound("IAM user not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();

        // Insert tags (using natural UPSERT semantics - ADR-0004)
        for (key, value) in tags {
            let insert_query = format!(
                "INSERT INTO {}.iam_user_tags (account_id, user_name, tag_key, tag_value) VALUES (?, ?, ?, ?)",
                catalog_keyspace
            );

            crate::cassandra_util::execute(
                self.session(),
                &insert_query,
                cdrs_tokio::query_values!(account_id, user_name, key.as_str(), value.as_str()),
                "tag_user",
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn untag_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> OpResult<()> {
        let catalog_keyspace = self.catalog_keyspace();

        for key in tag_keys {
            let delete_query = format!(
                "DELETE FROM {}.iam_user_tags WHERE account_id = ? AND user_name = ? AND tag_key = ?",
                catalog_keyspace
            );

            crate::cassandra_util::execute(
                self.session(),
                &delete_query,
                cdrs_tokio::query_values!(account_id, user_name, key.as_str()),
                "untag_user",
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn list_user_tags_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Vec<(String, String)>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT tag_key, tag_value FROM {}.iam_user_tags \
             WHERE account_id = ? AND user_name = ?",
            catalog_keyspace
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id, user_name),
            "list_user_tags",
        )
        .await?;

        let mut tags = crate::cassandra_util::map_rows(
            rows,
            |row| {
                use crate::cassandra_util::get_column;
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "tag_key", "list_user_tags")?,
                    get_column::<String, _>(row, "tag_value", "list_user_tags")?,
                ))
            },
            "list_user_tags",
        )?;

        tags.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(tags)
    }
}
