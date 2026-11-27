// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored secrets as the encryption-key commands read and rewrite them,
//! and the write of the data database connection string under the key ring,
//! for `PostgresCatalogStore`.

use extenddb_storage::key_commands::data_connection_key;
use extenddb_storage::key_ring::KEY_RING_SETTING;
use extenddb_storage::management_store::{
    OpError, OpResult, SecretPage, SecretWrite, StoredSecretRow,
};
use extenddb_storage::secret_keys::{CATALOG_ID_SETTING, DATA_CONNECTION_SETTING, SecretTable};
use sqlx::postgres::PgConnectOptions;
use time::OffsetDateTime;

use super::database_error;
use crate::catalog_store::PostgresCatalogStore;

/// An `access_keys` row: key, secret, account, user, created.
type AccessKeyRow = (String, Vec<u8>, String, String, OffsetDateTime);

/// An `iam_sessions` row: token, access key ID, secret, account, role,
/// session name, expiry, created.
type SessionRow = (
    String,
    String,
    Vec<u8>,
    String,
    String,
    String,
    OffsetDateTime,
    OffsetDateTime,
);

impl PostgresCatalogStore {
    pub(crate) async fn read_secret_page_impl(
        &self,
        table: SecretTable,
        page: Option<&str>,
        page_size: usize,
    ) -> OpResult<SecretPage> {
        let limit = i64::try_from(page_size).unwrap_or(i64::MAX);
        let rows: Vec<StoredSecretRow> = match table {
            SecretTable::AccessKeys => {
                let rows: Vec<AccessKeyRow> = sqlx::query_as(
                    "SELECT access_key_id, secret_key_encrypted, account_id, user_name, \
                     created_at FROM access_keys \
                     WHERE $1::text IS NULL OR access_key_id > $1 \
                     ORDER BY access_key_id LIMIT $2",
                )
                .bind(page)
                .bind(limit)
                .fetch_all(self.pool())
                .await
                .map_err(|e| database_error("read_secret_page", &e))?;
                rows.into_iter()
                    .map(
                        |(id, secret, account_id, user_name, created)| StoredSecretRow {
                            table,
                            key: id.clone(),
                            access_key_id: id,
                            account_id,
                            principal_name: user_name,
                            session_name: None,
                            expires_at: None,
                            created_at: Some(created),
                            secret_key_encrypted: secret,
                            write_timestamp: None,
                        },
                    )
                    .collect()
            }
            SecretTable::IamSessions => {
                let rows: Vec<SessionRow> = sqlx::query_as(
                    "SELECT session_token, access_key_id, secret_key_encrypted, account_id, \
                     role_name, session_name, expires_at, created_at FROM iam_sessions \
                     WHERE $1::text IS NULL OR session_token > $1 \
                     ORDER BY session_token LIMIT $2",
                )
                .bind(page)
                .bind(limit)
                .fetch_all(self.pool())
                .await
                .map_err(|e| database_error("read_secret_page", &e))?;
                rows.into_iter()
                    .map(
                        |(token, id, secret, account_id, role, session, expires, created)| {
                            StoredSecretRow {
                                table,
                                key: token,
                                access_key_id: id,
                                account_id,
                                principal_name: role,
                                session_name: Some(session),
                                expires_at: Some(expires.unix_timestamp()),
                                created_at: Some(created),
                                secret_key_encrypted: secret,
                                write_timestamp: None,
                            }
                        },
                    )
                    .collect()
            }
        };
        let next = (rows.len() == page_size)
            .then(|| rows.last().map(|row| row.key.clone()))
            .flatten();
        Ok(SecretPage { rows, next })
    }

    /// One `UPDATE ... FROM (VALUES ...)` by primary key. A key that matches
    /// no row changes nothing.
    pub(crate) async fn write_secrets_impl(
        &self,
        table: SecretTable,
        rows: &[SecretWrite],
    ) -> OpResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut update = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
            "UPDATE {} AS t SET secret_key_encrypted = v.secret FROM (",
            table.name()
        ));
        update.push_values(rows, |mut values, row| {
            values
                .push_bind(row.key.as_str())
                .push_unseparated("::text")
                .push_bind(row.secret_key_encrypted.as_slice())
                .push_unseparated("::bytea");
        });
        update.push(format!(
            ") AS v(key, secret) WHERE t.{} = v.key",
            table.key_column()
        ));
        update
            .build()
            .execute(self.pool())
            .await
            .map_err(|e| database_error("write_secrets", &e))?;
        Ok(())
    }

    pub(crate) async fn delete_secrets_impl(
        &self,
        table: SecretTable,
        keys: &[String],
    ) -> OpResult<()> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {} WHERE {} = ANY($1)",
            table.name(),
            table.key_column()
        )))
        .bind(keys)
        .execute(self.pool())
        .await
        .map_err(|e| database_error("delete_secrets", &e))?;
        Ok(())
    }

    pub(crate) async fn delete_setting_impl(&self, key: &str) -> OpResult<()> {
        sqlx::query("DELETE FROM settings WHERE key = $1")
            .bind(key)
            .execute(self.pool())
            .await
            .map_err(|e| database_error("delete_setting", &e))?;
        Ok(())
    }

    /// `encryption-key set-data-connection`: in one transaction, read the
    /// key ring `FOR SHARE`, open its DEK with `kek`, and write
    /// `connection_string` into the `data_database_connection_string` row,
    /// encrypted under that DEK. A `rotate` that writes the ring waits for
    /// the commit, or this sees its two DEKs and refuses.
    ///
    /// # Errors
    ///
    /// [`OpError::Validation`] for a string that is not a PostgreSQL
    /// connection string, a catalog without a ring, a ring with an
    /// unfinished switch or two DEKs, or a ring the KEK does not open; a
    /// database error otherwise.
    pub(crate) async fn set_data_connection_impl(
        &self,
        kek: &[u8; 32],
        connection_string: &str,
    ) -> OpResult<()> {
        // `serve` opens this row with sqlx's parser, and `migrate` and
        // `verify` with the parser that the command runs first. The two
        // grammars differ, so a string must pass both.
        connection_string.parse::<PgConnectOptions>().map_err(|e| {
            OpError::Validation(format!(
                "the data database connection string is not a PostgreSQL connection \
                     string: {e}"
            ))
        })?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| database_error("set_data_connection", &e))?;
        let ring: Option<String> =
            sqlx::query_scalar("SELECT value FROM settings WHERE key = $1 FOR SHARE")
                .bind(KEY_RING_SETTING)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| database_error("set_data_connection", &e))?;
        let catalog_id: Option<String> =
            sqlx::query_scalar("SELECT value FROM settings WHERE key = $1")
                .bind(CATALOG_ID_SETTING)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| database_error("set_data_connection", &e))?;
        let catalog_id = catalog_id.ok_or_else(|| {
            OpError::Validation(
                "the catalog has no catalog_id row; run extenddb migrate --yes first".to_owned(),
            )
        })?;
        let dek =
            data_connection_key(ring.as_deref(), kek, &catalog_id).map_err(OpError::Validation)?;
        let sealed = dek
            .encrypt_setting(connection_string)
            .map_err(|e| OpError::Internal(format!("set_data_connection: {e}")))?;
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(DATA_CONNECTION_SETTING)
        .bind(&sealed)
        .execute(&mut *tx)
        .await
        .map_err(|e| database_error("set_data_connection", &e))?;
        tx.commit()
            .await
            .map_err(|e| database_error("set_data_connection", &e))
    }
}
