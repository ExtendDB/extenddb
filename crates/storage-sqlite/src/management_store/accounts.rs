// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Account management operations for `SqliteCatalogStore`.

use extenddb_storage::management_store::{AccountDetail, OpError, OpResult};

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::is_unique_violation;

impl SqliteCatalogStore {
    pub(crate) async fn create_account_impl(
        &self,
        account_id: &str,
        account_name: &str,
    ) -> OpResult<()> {
        let result =
            sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, ?)")
                .bind(account_id)
                .bind(account_name)
                .execute(self.pool())
                .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_unique_violation(&e) => {
                Err(OpError::AlreadyExists("Account already exists".to_owned()))
            }
            Err(e) => {
                tracing::error!("create_account failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn delete_account_impl(&self, account_id: &str) -> OpResult<()> {
        let mut tx = self.pool().begin().await.map_err(|e| {
            tracing::error!("delete_account begin transaction: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let exists: Option<(String,)> =
            sqlx::query_as("SELECT account_id FROM accounts WHERE account_id = ?")
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| {
                    tracing::error!("delete_account check account: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

        if exists.is_none() {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        let (has_tables,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM tables WHERE account_id = ?")
                .bind(account_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| {
                    tracing::error!("delete_account check tables: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

        if has_tables > 0 {
            return Err(OpError::HasDependents(
                "Cannot delete account with existing tables. Delete all tables first.".to_owned(),
            ));
        }

        let r = sqlx::query("DELETE FROM accounts WHERE account_id = ?")
            .bind(account_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::error!("delete_account delete: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

        if r.rows_affected() == 0 {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        tx.commit().await.map_err(|e| {
            tracing::error!("delete_account commit: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        Ok(())
    }

    pub(crate) async fn list_all_accounts_impl(&self) -> OpResult<Vec<(String, String)>> {
        sqlx::query_as("SELECT account_id, account_name FROM accounts ORDER BY account_id")
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("list_all_accounts: {e}");
                OpError::Internal("Database error".to_owned())
            })
    }

    pub(crate) async fn list_all_accounts_full_impl(
        &self,
    ) -> OpResult<Vec<(String, String, time::OffsetDateTime)>> {
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT account_id, account_name, created_at FROM accounts ORDER BY account_id",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_all_accounts_full: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        rows.into_iter()
            .map(|(id, name, ts)| {
                let created_at =
                    crate::sqlite_util::parse_timestamp(&ts).map_err(|e| {
                        tracing::error!("list_all_accounts_full parse_timestamp: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
                Ok((id, name, created_at))
            })
            .collect()
    }

    pub(crate) async fn list_accounts_for_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Vec<(String, String)>> {
        sqlx::query_as("SELECT account_id, account_name FROM accounts WHERE account_id = ?")
            .bind(account_id)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("list_accounts_for: {e}");
                OpError::Internal("Database error".to_owned())
            })
    }

    pub(crate) async fn get_account_detail_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Option<AccountDetail>> {
        let acct: Option<(String,)> =
            sqlx::query_as("SELECT account_name FROM accounts WHERE account_id = ?")
                .bind(account_id)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail name: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

        let Some((account_name,)) = acct else {
            return Ok(None);
        };

        let users: Vec<(String,)> = sqlx::query_as(
            "SELECT user_name FROM iam_users WHERE account_id = ? ORDER BY user_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_account_detail users: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let groups: Vec<(String,)> = sqlx::query_as(
            "SELECT group_name FROM iam_groups WHERE account_id = ? ORDER BY group_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_account_detail groups: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let roles: Vec<(String,)> = sqlx::query_as(
            "SELECT role_name FROM iam_roles WHERE account_id = ? ORDER BY role_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_account_detail roles: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        Ok(Some(AccountDetail {
            account_name,
            users: users.into_iter().map(|(n,)| n).collect(),
            groups: groups.into_iter().map(|(n,)| n).collect(),
            roles: roles.into_iter().map(|(n,)| n).collect(),
        }))
    }

    pub(crate) async fn dashboard_counts_impl(&self) -> OpResult<(i64, i64)> {
        let (account_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM accounts")
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("dashboard_counts accounts: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

        let (admin_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM admin_users")
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("dashboard_counts admins: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

        Ok((account_count, admin_count))
    }
}
