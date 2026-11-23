// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The rows a principal or an account owns in the catalog, and their delete.
//!
//! The catalog has no references between tables, so nothing else removes
//! them. A row left behind belongs to the next principal created with the
//! same name: an inline policy or boundary grants it permissions, an access
//! key or a role session signs in as it, and a membership puts it in a group.
//! A deleted account's rows stay as well, and its users could still sign in.
//!
//! Cassandra has no transaction across tables, so each caller deletes what a
//! principal owns before its own row, credentials first: a failure then
//! leaves a principal with less than it had, never a credential or a
//! permission without it, and a retry finds the principal and finishes.
//!
//! Tables written with `IF NOT EXISTS` are deleted with `IF EXISTS`, and the
//! others with a plain `DELETE` (LWT/non-LWT mixing rule, ADR-0021).

use std::collections::BTreeSet;

use cdrs_tokio::query::QueryValues;
use cdrs_tokio::query_values;
use extenddb_storage::management_store::{OpError, OpResult};

use crate::cassandra_util::{apply_lwt, execute, get_column, query_rows};
use crate::catalog_store::CassandraCatalogStore;

impl CassandraCatalogStore {
    /// The text values of `column` in the rows `query` selects.
    async fn select_texts(
        &self,
        query: &str,
        values: QueryValues,
        column: &str,
        what: &str,
    ) -> OpResult<Vec<String>> {
        query_rows::<OpError>(self.session(), query, values, what)
            .await?
            .iter()
            .map(|row| get_column(row, column, what))
            .collect()
    }

    /// Delete every row a principal owns besides its own row in `iam_users`,
    /// `iam_groups` or `iam_roles`.
    pub(crate) async fn delete_principal_dependents(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<()> {
        let ks = self.catalog_keyspace();
        let session = self.session();
        let what = format!("delete {principal_type} {principal_name}'s rows");
        let what = what.as_str();

        match principal_type {
            "user" => {
                // access_keys has an IF NOT EXISTS insert path (import).
                let key_ids = self
                    .select_texts(
                        &format!(
                            "SELECT access_key_id FROM {ks}.access_keys \
                             WHERE account_id = ? AND user_name = ? ALLOW FILTERING"
                        ),
                        query_values!(account_id, principal_name),
                        "access_key_id",
                        what,
                    )
                    .await?;
                for key_id in &key_ids {
                    apply_lwt::<OpError>(
                        session,
                        &format!("DELETE FROM {ks}.access_keys WHERE access_key_id = ? IF EXISTS"),
                        query_values!(key_id.as_str()),
                        what,
                    )
                    .await?;
                }
                // iam_group_members is written with IF NOT EXISTS.
                let groups = self
                    .select_texts(
                        &format!(
                            "SELECT group_name FROM {ks}.iam_group_members \
                             WHERE account_id = ? AND user_name = ? ALLOW FILTERING"
                        ),
                        query_values!(account_id, principal_name),
                        "group_name",
                        what,
                    )
                    .await?;
                for group in &groups {
                    apply_lwt::<OpError>(
                        session,
                        &format!(
                            "DELETE FROM {ks}.iam_group_members \
                             WHERE account_id = ? AND group_name = ? AND user_name = ? IF EXISTS"
                        ),
                        query_values!(account_id, group.as_str(), principal_name),
                        what,
                    )
                    .await?;
                }
                execute::<OpError>(
                    session,
                    &format!(
                        "DELETE FROM {ks}.iam_user_tags WHERE account_id = ? AND user_name = ?"
                    ),
                    query_values!(account_id, principal_name),
                    what,
                )
                .await?;
                // Failed logins are keyed by sign-in name, the partition key,
                // and would count against the next user of this name.
                let sign_in = format!("{account_id}/{principal_name}");
                execute::<OpError>(
                    session,
                    &format!("DELETE FROM {ks}.login_attempts WHERE principal = ?"),
                    query_values!(sign_in.as_str()),
                    what,
                )
                .await?;
            }
            "group" => {
                // iam_group_members is written with IF NOT EXISTS.
                let members = self
                    .select_texts(
                        &format!(
                            "SELECT user_name FROM {ks}.iam_group_members \
                             WHERE account_id = ? AND group_name = ?"
                        ),
                        query_values!(account_id, principal_name),
                        "user_name",
                        what,
                    )
                    .await?;
                for member in &members {
                    apply_lwt::<OpError>(
                        session,
                        &format!(
                            "DELETE FROM {ks}.iam_group_members \
                             WHERE account_id = ? AND group_name = ? AND user_name = ? IF EXISTS"
                        ),
                        query_values!(account_id, principal_name, member.as_str()),
                        what,
                    )
                    .await?;
                }
            }
            "role" => {
                let tokens = self
                    .select_texts(
                        &format!(
                            "SELECT session_token FROM {ks}.iam_sessions \
                             WHERE account_id = ? AND role_name = ? ALLOW FILTERING"
                        ),
                        query_values!(account_id, principal_name),
                        "session_token",
                        what,
                    )
                    .await?;
                for token in &tokens {
                    execute::<OpError>(
                        session,
                        &format!("DELETE FROM {ks}.iam_sessions WHERE session_token = ?"),
                        query_values!(token.as_str()),
                        what,
                    )
                    .await?;
                }
                execute::<OpError>(
                    session,
                    &format!(
                        "DELETE FROM {ks}.iam_role_tags WHERE account_id = ? AND role_name = ?"
                    ),
                    query_values!(account_id, principal_name),
                    what,
                )
                .await?;
            }
            other => unreachable!("principal type {other}"),
        }

        let policies = self
            .select_texts(
                &format!(
                    "SELECT policy_name FROM {ks}.iam_policies \
                     WHERE account_id = ? AND principal_type = ? AND principal_name = ?"
                ),
                query_values!(account_id, principal_type, principal_name),
                "policy_name",
                what,
            )
            .await?;
        for policy in &policies {
            execute::<OpError>(
                session,
                &format!(
                    "DELETE FROM {ks}.iam_policies WHERE account_id = ? AND principal_type = ? \
                     AND principal_name = ? AND policy_name = ?"
                ),
                query_values!(account_id, principal_type, principal_name, policy.as_str()),
                what,
            )
            .await?;
        }
        if principal_type != "group" {
            execute::<OpError>(
                session,
                &format!(
                    "DELETE FROM {ks}.iam_permissions_boundaries \
                     WHERE account_id = ? AND principal_type = ? AND principal_name = ?"
                ),
                query_values!(account_id, principal_type, principal_name),
                what,
            )
            .await?;
        }
        Ok(())
    }

    /// Delete a principal's own row in `iam_users`, `iam_groups` or
    /// `iam_roles`, all written with `IF NOT EXISTS`. A user's delete commits
    /// at quorum, so the console's quorum session check sees it.
    pub(crate) async fn delete_principal_row(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<()> {
        let ks = self.catalog_keyspace();
        let what = format!("delete {principal_type} {principal_name}");
        match principal_type {
            "user" => apply_lwt::<OpError>(
                self.session(),
                &format!(
                    "DELETE FROM {ks}.iam_users WHERE account_id = ? AND user_name = ? IF EXISTS"
                ),
                query_values!(account_id, principal_name),
                &what,
            )
            .await?,
            "group" => apply_lwt::<OpError>(
                self.session(),
                &format!(
                    "DELETE FROM {ks}.iam_groups WHERE account_id = ? AND group_name = ? IF EXISTS"
                ),
                query_values!(account_id, principal_name),
                &what,
            )
            .await?,
            "role" => apply_lwt::<OpError>(
                self.session(),
                &format!(
                    "DELETE FROM {ks}.iam_roles WHERE account_id = ? AND role_name = ? IF EXISTS"
                ),
                query_values!(account_id, principal_name),
                &what,
            )
            .await?,
            other => unreachable!("principal type {other}"),
        };
        Ok(())
    }

    /// After a principal's row was inserted, delete the rows a delete left
    /// behind under its name before deletes removed them. If that fails,
    /// delete the new row again, so that a retry of the create is not refused
    /// as a duplicate while the rows stay.
    pub(crate) async fn clear_new_principal(
        &self,
        account_id: &str,
        principal_type: &str,
        principal_name: &str,
    ) -> OpResult<()> {
        let cleared = self
            .delete_principal_dependents(account_id, principal_type, principal_name)
            .await;
        if cleared.is_err()
            && let Err(e) = self
                .delete_principal_row(account_id, principal_type, principal_name)
                .await
        {
            tracing::error!(
                "create_{principal_type}: remove the new {principal_type} after a failed clear: {e:?}"
            );
        }
        cleared
    }

    /// Delete every IAM row of an account: each user, group and role that has
    /// a row in any IAM table, with what it owns, and then its own row.
    ///
    /// Names come from every table, not only `iam_users`, `iam_groups` and
    /// `iam_roles`, so that rows a principal's delete left behind before
    /// deletes removed them go too.
    pub(crate) async fn delete_account_iam(&self, account_id: &str) -> OpResult<()> {
        let ks = self.catalog_keyspace();
        let what = "delete the account's IAM rows";
        let mut principals: BTreeSet<(&'static str, String)> = BTreeSet::new();
        let sources: [(&'static str, String, &str); 13] = [
            (
                "user",
                format!("SELECT user_name FROM {ks}.iam_users WHERE account_id = ?"),
                "user_name",
            ),
            (
                "user",
                format!(
                    "SELECT user_name FROM {ks}.access_keys WHERE account_id = ? ALLOW FILTERING"
                ),
                "user_name",
            ),
            (
                "user",
                format!(
                    "SELECT user_name FROM {ks}.iam_group_members \
                     WHERE account_id = ? ALLOW FILTERING"
                ),
                "user_name",
            ),
            (
                "user",
                format!(
                    "SELECT user_name FROM {ks}.iam_user_tags WHERE account_id = ? ALLOW FILTERING"
                ),
                "user_name",
            ),
            (
                "user",
                format!(
                    "SELECT principal_name FROM {ks}.iam_policies \
                     WHERE account_id = ? AND principal_type = 'user'"
                ),
                "principal_name",
            ),
            (
                "user",
                format!(
                    "SELECT principal_name FROM {ks}.iam_permissions_boundaries \
                     WHERE account_id = ? AND principal_type = 'user'"
                ),
                "principal_name",
            ),
            (
                "group",
                format!("SELECT group_name FROM {ks}.iam_groups WHERE account_id = ?"),
                "group_name",
            ),
            (
                "group",
                format!(
                    "SELECT group_name FROM {ks}.iam_group_members \
                     WHERE account_id = ? ALLOW FILTERING"
                ),
                "group_name",
            ),
            (
                "group",
                format!(
                    "SELECT principal_name FROM {ks}.iam_policies \
                     WHERE account_id = ? AND principal_type = 'group'"
                ),
                "principal_name",
            ),
            (
                "role",
                format!("SELECT role_name FROM {ks}.iam_roles WHERE account_id = ?"),
                "role_name",
            ),
            (
                "role",
                format!(
                    "SELECT role_name FROM {ks}.iam_sessions WHERE account_id = ? ALLOW FILTERING"
                ),
                "role_name",
            ),
            (
                "role",
                format!(
                    "SELECT role_name FROM {ks}.iam_role_tags WHERE account_id = ? ALLOW FILTERING"
                ),
                "role_name",
            ),
            (
                "role",
                format!(
                    "SELECT principal_name FROM {ks}.iam_policies \
                     WHERE account_id = ? AND principal_type = 'role'"
                ),
                "principal_name",
            ),
        ];
        for (principal_type, query, column) in &sources {
            for name in self
                .select_texts(query, query_values!(account_id), column, what)
                .await?
            {
                principals.insert((*principal_type, name));
            }
        }
        // Role boundaries: the role's own name is enough, and roles with only a
        // boundary left are found here.
        for name in self
            .select_texts(
                &format!(
                    "SELECT principal_name FROM {ks}.iam_permissions_boundaries \
                     WHERE account_id = ? AND principal_type = 'role'"
                ),
                query_values!(account_id),
                "principal_name",
                what,
            )
            .await?
        {
            principals.insert(("role", name));
        }

        for (principal_type, name) in &principals {
            self.delete_principal_dependents(account_id, principal_type, name)
                .await?;
            self.delete_principal_row(account_id, principal_type, name)
                .await?;
        }

        // Failed logins under any sign-in name of the account, including names
        // no user had. The partition key cannot be matched by prefix, so the
        // distinct names are read and filtered here, as the attempt cleanup
        // reads them; the table holds only recent attempts.
        let prefix = format!("{account_id}/");
        for principal in self
            .select_texts(
                &format!("SELECT DISTINCT principal FROM {ks}.login_attempts"),
                query_values!(),
                "principal",
                what,
            )
            .await?
            .into_iter()
            .filter(|p| p.starts_with(&prefix))
        {
            execute::<OpError>(
                self.session(),
                &format!("DELETE FROM {ks}.login_attempts WHERE principal = ?"),
                query_values!(principal.as_str()),
                what,
            )
            .await?;
        }
        Ok(())
    }
}
