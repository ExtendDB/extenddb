// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Account management operations for `DuckDbCatalogStore`.

use crate::db;
use extenddb_storage::management_store::{AccountDetail, OpError, OpResult};
use time::OffsetDateTime;

use crate::catalog_store::DuckDbCatalogStore;
use crate::duckdb_util::{is_unique_violation, parse_timestamp};

impl DuckDbCatalogStore {
    pub(crate) async fn create_account_impl(
        &self,
        account_id: &str,
        account_name: &str,
    ) -> OpResult<()> {
        db::query("INSERT INTO accounts (account_id, account_name) VALUES (?, ?)")
            .bind(account_id)
            .bind(account_name)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(|e| {
                if is_unique_violation(&e) {
                    OpError::AlreadyExists("Account already exists".to_owned())
                } else {
                    tracing::error!("create_account: {e}");
                    OpError::Internal("Database error".to_owned())
                }
            })
    }

    pub(crate) async fn delete_account_impl(&self, account_id: &str) -> OpResult<()> {
        // Refuse to delete an account that still owns tables; all other IAM
        // children are removed explicitly (DuckDB has no cascading deletes).
        let exists: bool =
            db::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts WHERE account_id = ?)")
                .bind(account_id)
                .fetch_one(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("delete_account check: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
        if !exists {
            return Err(OpError::NotFound("Account not found".to_owned()));
        }

        let (table_count,): (i64,) =
            db::query_as("SELECT COUNT(*) FROM tables WHERE account_id = ?")
                .bind(account_id)
                .fetch_one(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("delete_account table count: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
        if table_count > 0 {
            return Err(OpError::HasDependents(
                "Cannot delete account with existing tables. Delete all tables first.".to_owned(),
            ));
        }

        crate::referential::delete_with_children(
            self.pool(),
            "delete_account",
            async |tx| crate::referential::delete_account_children(tx, account_id).await,
            "DELETE FROM accounts WHERE account_id = ?",
            &[account_id],
            "Account not found",
        )
        .await
    }

    pub(crate) async fn list_all_accounts_impl(&self) -> OpResult<Vec<(String, String)>> {
        db::query_as("SELECT account_id, account_name FROM accounts ORDER BY account_id")
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("list_all_accounts: {e}");
                OpError::Internal("Database error".to_owned())
            })
    }

    pub(crate) async fn default_account_id_impl(&self) -> OpResult<Option<String>> {
        db::query_scalar("SELECT value FROM settings WHERE key = 'default_account_id'")
            .fetch_optional(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("default_account_id: {e}");
                OpError::Internal("Database error".to_owned())
            })
    }

    pub(crate) async fn list_all_accounts_full_impl(
        &self,
    ) -> OpResult<Vec<(String, String, OffsetDateTime)>> {
        let rows: Vec<(String, String, String)> = db::query_as(
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
                Ok((
                    id,
                    name,
                    parse_timestamp(&ts)
                        .map_err(|e| OpError::Internal(format!("parse created_at: {e}")))?,
                ))
            })
            .collect()
    }

    pub(crate) async fn list_accounts_for_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Vec<(String, String)>> {
        db::query_as("SELECT account_id, account_name FROM accounts WHERE account_id = ?")
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
            db::query_as("SELECT account_name FROM accounts WHERE account_id = ?")
                .bind(account_id)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("get_account_detail: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
        let Some((account_name,)) = acct else {
            return Ok(None);
        };

        let names = |rows: Vec<(String,)>| rows.into_iter().map(|(n,)| n).collect::<Vec<_>>();

        let users: Vec<(String,)> =
            db::query_as("SELECT user_name FROM iam_users WHERE account_id = ? ORDER BY user_name")
                .bind(account_id)
                .fetch_all(self.pool())
                .await
                .map_err(|e| OpError::Internal(format!("get_account_detail users: {e}")))?;

        let groups: Vec<(String,)> = db::query_as(
            "SELECT group_name FROM iam_groups WHERE account_id = ? ORDER BY group_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_account_detail groups: {e}")))?;

        let roles: Vec<(String,)> =
            db::query_as("SELECT role_name FROM iam_roles WHERE account_id = ? ORDER BY role_name")
                .bind(account_id)
                .fetch_all(self.pool())
                .await
                .map_err(|e| OpError::Internal(format!("get_account_detail roles: {e}")))?;

        Ok(Some(AccountDetail {
            account_name,
            users: names(users),
            groups: names(groups),
            roles: names(roles),
        }))
    }

    pub(crate) async fn dashboard_counts_impl(&self) -> OpResult<(i64, i64)> {
        let (accounts,): (i64,) = db::query_as("SELECT COUNT(*) FROM accounts")
            .fetch_one(self.pool())
            .await
            .map_err(|e| OpError::Internal(format!("dashboard_counts accounts: {e}")))?;
        let (admins,): (i64,) = db::query_as("SELECT COUNT(*) FROM admin_users")
            .fetch_one(self.pool())
            .await
            .map_err(|e| OpError::Internal(format!("dashboard_counts admins: {e}")))?;
        Ok((accounts, admins))
    }
}
