// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Account management operations for `CassandraCatalogStore`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::management_store::{AccountDetail, OpError, OpResult};

use crate::catalog_store::CassandraCatalogStore;

impl CassandraCatalogStore {
    pub(crate) async fn create_account_impl(
        &self,
        account_id: &str,
        account_name: &str,
    ) -> OpResult<()> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "INSERT INTO {}.accounts (account_id, account_name, created_at) VALUES (?, ?, toTimestamp(now())) IF NOT EXISTS",
            catalog_keyspace
        );

        let result = self
            .session()
            .query_with_values(&query, cdrs_tokio::query_values!(account_id, account_name))
            .await
            .map_err(|e| {
                tracing::error!("create_account query failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

        // Check LWT result
        let body = result.response_body().map_err(|e| {
            tracing::error!("create_account response_body failed: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let applied = if let Some(rows) = body.into_rows() {
            if let Some(row) = rows.first() {
                row.get_r_by_name("[applied]").unwrap_or(false)
            } else {
                false
            }
        } else {
            false
        };

        // Ensure account keyspace exists (whether account was just created OR already existed)
        // This makes retries safe: if account creation succeeded but keyspace creation failed,
        // the retry will create the keyspace before returning AlreadyExists error.
        self.ensure_account_keyspace(account_id).await?;

        // Return AlreadyExists only AFTER keyspace is guaranteed to exist
        if !applied {
            return Err(OpError::AlreadyExists("Account already exists".to_owned()));
        }

        Ok(())
    }

    pub(crate) async fn delete_account_impl(&self, account_id: &str) -> OpResult<()> {
        if !self.account_exists(account_id).await? {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        let catalog_keyspace = self.catalog_keyspace();

        // Check if account has tables
        let tables_query = format!(
            "SELECT table_name FROM {}.tables WHERE account_id = ? LIMIT 1",
            catalog_keyspace
        );

        let has_tables = crate::cassandra_util::query_optional(
            self.session(),
            &tables_query,
            cdrs_tokio::query_values!(account_id),
            "delete_account",
        )
        .await?
        .is_some();

        if has_tables {
            return Err(OpError::HasDependents(
                "Cannot delete account with existing tables. Delete all tables first.".to_owned(),
            ));
        }

        // Backups outlive their source table, but cannot outlive the account
        // keyspace that stores their payload. Remove every denormalized and
        // authoritative catalog row before dropping that keyspace.
        let backup_query = format!(
            "SELECT backup_arn, table_name, created_at FROM {}.backups_by_account WHERE account_id = ?",
            catalog_keyspace
        );
        let backup_rows = crate::cassandra_util::query_rows::<OpError>(
            self.session(),
            &backup_query,
            cdrs_tokio::query_values!(account_id),
            "delete_account_backups",
        )
        .await?;
        for row in backup_rows {
            let backup_arn: String = row
                .get_r_by_name("backup_arn")
                .map_err(|e| OpError::Internal(format!("Parse backup ARN: {e}")))?;
            let table_name: String = row
                .get_r_by_name("table_name")
                .map_err(|e| OpError::Internal(format!("Parse backup table: {e}")))?;
            let created_at: i64 = row
                .get_r_by_name("created_at")
                .map_err(|e| OpError::Internal(format!("Parse backup timestamp: {e}")))?;
            crate::cassandra_util::execute::<OpError>(
                self.session(),
                &format!(
                    "DELETE FROM {}.backups_by_table WHERE account_id = ? AND table_name = ? AND created_at = ? AND backup_arn = ?",
                    catalog_keyspace
                ),
                cdrs_tokio::query_values!(account_id, table_name, created_at, backup_arn.as_str()),
                "delete_account_table_backup",
            )
            .await?;
            crate::cassandra_util::execute::<OpError>(
                self.session(),
                &format!(
                    "DELETE FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ?",
                    catalog_keyspace
                ),
                cdrs_tokio::query_values!(account_id, backup_arn),
                "delete_account_backup",
            )
            .await?;
        }
        crate::cassandra_util::execute::<OpError>(
            self.session(),
            &format!(
                "DELETE FROM {}.backups_by_account WHERE account_id = ?",
                catalog_keyspace
            ),
            cdrs_tokio::query_values!(account_id),
            "delete_account_backup_index",
        )
        .await?;
        crate::cassandra_util::execute::<OpError>(
            self.session(),
            &format!(
                "DELETE FROM {}.continuous_backups WHERE account_id = ?",
                catalog_keyspace
            ),
            cdrs_tokio::query_values!(account_id),
            "delete_account_continuous_backups",
        )
        .await?;

        // Delete account from catalog
        let delete_query = format!(
            "DELETE FROM {}.accounts WHERE account_id = ?",
            catalog_keyspace
        );

        crate::cassandra_util::execute(
            self.session(),
            &delete_query,
            cdrs_tokio::query_values!(account_id),
            "delete_account",
        )
        .await?;

        // Drop account keyspace
        self.drop_account_keyspace(account_id).await?;

        Ok(())
    }

    pub(crate) async fn list_all_accounts_impl(&self) -> OpResult<Vec<(String, String)>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT account_id, account_name FROM {}.accounts",
            catalog_keyspace
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(),
            "list_all_accounts",
        )
        .await?;

        let mut accounts = crate::cassandra_util::map_rows(
            rows,
            |row| {
                use crate::cassandra_util::get_column;
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "account_id", "list_all_accounts")?,
                    get_column::<String, _>(row, "account_name", "list_all_accounts")?,
                ))
            },
            "list_all_accounts",
        )?;

        accounts.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(accounts)
    }

    pub(crate) async fn list_all_accounts_full_impl(
        &self,
    ) -> OpResult<Vec<(String, String, time::OffsetDateTime)>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT account_id, account_name, created_at FROM {}.accounts",
            catalog_keyspace
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(),
            "list_all_accounts_full",
        )
        .await?;

        let mut accounts = crate::cassandra_util::map_rows(
            rows,
            |row| {
                use crate::cassandra_util::{get_column, get_timestamp};
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "account_id", "list_all_accounts_full")?,
                    get_column::<String, _>(row, "account_name", "list_all_accounts_full")?,
                    get_timestamp(row, "created_at", "list_all_accounts_full")?,
                ))
            },
            "list_all_accounts_full",
        )?;

        accounts.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(accounts)
    }

    pub(crate) async fn list_accounts_for_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Vec<(String, String)>> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT account_id, account_name FROM {}.accounts WHERE account_id = ?",
            catalog_keyspace
        );

        let rows = crate::cassandra_util::query_rows(
            self.session(),
            &query,
            cdrs_tokio::query_values!(account_id),
            "list_accounts_for",
        )
        .await?;

        crate::cassandra_util::map_rows(
            rows,
            |row| {
                use crate::cassandra_util::get_column;
                Ok::<_, OpError>((
                    get_column::<String, _>(row, "account_id", "list_accounts_for")?,
                    get_column::<String, _>(row, "account_name", "list_accounts_for")?,
                ))
            },
            "list_accounts_for",
        )
    }

    pub(crate) async fn get_account_detail_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Option<AccountDetail>> {
        let catalog_keyspace = self.catalog_keyspace();

        // Get account name
        let account_query = format!(
            "SELECT account_name FROM {}.accounts WHERE account_id = ?",
            catalog_keyspace
        );

        let account_row = crate::cassandra_util::query_optional(
            self.session(),
            &account_query,
            cdrs_tokio::query_values!(account_id),
            "get_account_detail",
        )
        .await?;

        let Some(row) = account_row else {
            return Ok(None);
        };

        let account_name: String =
            crate::cassandra_util::get_column(&row, "account_name", "get_account_detail")?;

        // Get users
        let users_query = format!(
            "SELECT user_name FROM {}.iam_users WHERE account_id = ?",
            catalog_keyspace
        );

        let users_rows = crate::cassandra_util::query_rows(
            self.session(),
            &users_query,
            cdrs_tokio::query_values!(account_id),
            "get_account_detail",
        )
        .await?;

        let mut users = crate::cassandra_util::map_rows(
            users_rows,
            |row| {
                use crate::cassandra_util::get_column;
                get_column::<String, _>(row, "user_name", "get_account_detail")
            },
            "get_account_detail",
        )?;
        users.sort();

        // Get groups
        let groups_query = format!(
            "SELECT group_name FROM {}.iam_groups WHERE account_id = ?",
            catalog_keyspace
        );

        let groups_rows = crate::cassandra_util::query_rows(
            self.session(),
            &groups_query,
            cdrs_tokio::query_values!(account_id),
            "get_account_detail",
        )
        .await?;

        let mut groups = crate::cassandra_util::map_rows(
            groups_rows,
            |row| {
                use crate::cassandra_util::get_column;
                get_column::<String, _>(row, "group_name", "get_account_detail")
            },
            "get_account_detail",
        )?;
        groups.sort();

        // Get roles
        let roles_query = format!(
            "SELECT role_name FROM {}.iam_roles WHERE account_id = ?",
            catalog_keyspace
        );

        let roles_rows = crate::cassandra_util::query_rows(
            self.session(),
            &roles_query,
            cdrs_tokio::query_values!(account_id),
            "get_account_detail",
        )
        .await?;

        let mut roles = crate::cassandra_util::map_rows(
            roles_rows,
            |row| {
                use crate::cassandra_util::get_column;
                get_column::<String, _>(row, "role_name", "get_account_detail")
            },
            "get_account_detail",
        )?;
        roles.sort();

        Ok(Some(AccountDetail {
            account_name,
            users,
            groups,
            roles,
        }))
    }

    pub(crate) async fn dashboard_counts_impl(&self) -> OpResult<(i64, i64)> {
        let catalog_keyspace = self.catalog_keyspace();

        // Count accounts
        let accounts_query = format!("SELECT account_id FROM {}.accounts", catalog_keyspace);
        let accounts_rows = crate::cassandra_util::query_rows(
            self.session(),
            &accounts_query,
            cdrs_tokio::query_values!(),
            "dashboard_counts",
        )
        .await?;
        let account_count = accounts_rows.len() as i64;

        // Count admins
        let admins_query = format!("SELECT admin_name FROM {}.admin_users", catalog_keyspace);
        let admins_rows = crate::cassandra_util::query_rows(
            self.session(),
            &admins_query,
            cdrs_tokio::query_values!(),
            "dashboard_counts",
        )
        .await?;
        let admin_count = admins_rows.len() as i64;

        Ok((account_count, admin_count))
    }

    pub(crate) async fn get_default_account_id_impl(&self) -> OpResult<Option<String>> {
        let keyspace = self.catalog_keyspace();
        let query =
            format!("SELECT value FROM {keyspace}.settings WHERE key = 'default_account_id'");
        let rows = crate::cassandra_util::query_rows::<extenddb_storage::error::StorageError>(
            &self.session(),
            &query,
            cdrs_tokio::query_values!(),
            "default_account_id",
        )
        .await
        .map_err(|e| {
            tracing::error!("default_account_id: {e}");
            extenddb_storage::management_store::OpError::Internal("Database error".to_owned())
        })?;
        Ok(rows.into_iter().next().and_then(|row| {
            crate::cassandra_util::get_column::<String, extenddb_storage::error::StorageError>(
                &row,
                "value",
                "default_account_id",
            )
            .ok()
        }))
    }
}
