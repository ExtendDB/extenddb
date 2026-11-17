// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored-secret rows as the encryption-key commands read and write them
//! (RFC-0000 section 8.4), for `CassandraCatalogStore`.
//!
//! Every statement of the pass runs at `ALL`, and so does every write of the
//! key ring row and every read of the key rows from the commands' stores
//! ([`key_row_at_all`]), so every replica holds the new ciphertexts and the
//! new ring before the old key goes. Each page selects
//! `WRITETIME(secret_key_encrypted)`, and each write sets `USING TIMESTAMP`
//! above that value: CreateAccessKey and ImportAccessKey write with
//! `INSERT ... IF NOT EXISTS`, whose Paxos timestamp can be ahead of a later
//! plain update's clock, and Cassandra keeps the older value of an update
//! whose timestamp is not above the row's. Each delete reads the write time
//! of every non-key column of the row and stamps its tombstone at the
//! largest, or at this host's clock when that is later, because a tombstone
//! below a cell's timestamp leaves the cell in place. The writes
//! carry no condition, because a conditional update takes no timestamp of its
//! own. An update of a key that no row holds therefore writes a row with the
//! ciphertext alone; the pass writes only keys it read under the migration
//! lock, which keeps every server out, and session rows have no TTL here.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::types::IntoRustByIndex;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::rows::Row;
use extenddb_storage::key_ring::KEY_RING_SETTING;
use extenddb_storage::management_store::{
    OpError, OpResult, SecretPage, SecretWrite, StoredSecretRow,
};
use extenddb_storage::secret_keys::{CATALOG_ID_SETTING, CATALOG_KEY_SETTING, SecretTable};

use crate::cassandra_util::{
    PolicySession as _, Stmt, execute_at, get_column, timestamp_to_datetime,
};
use crate::catalog_store::CassandraCatalogStore;

/// The settings rows that the encryption-key commands read before they
/// write: the key ring, the catalog key and `catalog_id`.
const KEY_ROWS: [&str; 3] = [KEY_RING_SETTING, CATALOG_KEY_SETTING, CATALOG_ID_SETTING];

/// Whether a statement on the settings row `key` runs at `ALL`. Every write
/// of the key ring does, from every store: the ring row is the one stored
/// copy of the wrapped DEK. A read of a key row does from a store opened for
/// the commands (`commands`), so a command run again after a ring write that
/// reached only some replicas reads the newest ring. Every other settings
/// statement, and the server's read of the key rows at startup, runs at
/// `LOCAL_QUORUM` ([`Stmt::Read`], [`Stmt::Write`]).
pub(crate) fn key_row_at_all(key: &str, write: bool, commands: bool) -> bool {
    if write {
        key == KEY_RING_SETTING
    } else {
        commands && KEY_ROWS.contains(&key)
    }
}

/// The error of a statement on the key row `key` at `ALL`, which fails
/// while any replica of the catalog keyspace does not answer. `cause` is the
/// driver's error, or where to find it.
pub(crate) fn key_row_at_all_failed(what: &str, key: &str, cause: &str) -> OpError {
    OpError::Internal(format!(
        "{what} of the {key} row at consistency ALL failed: {cause}. The encryption-key \
         commands read and write the key rows at ALL, so every replica of the catalog \
         keyspace must answer; check the nodes with `nodetool status`"
    ))
}

impl CassandraCatalogStore {
    pub(crate) async fn read_secret_page_impl(
        &self,
        table: SecretTable,
        page: Option<&str>,
        page_size: usize,
    ) -> OpResult<SecretPage> {
        use cdrs_tokio::frame::message_result::RowsMetadataFlags;
        const WHAT: &str = "read_secret_page";
        let columns = match table {
            SecretTable::AccessKeys => "access_key_id, account_id, user_name",
            SecretTable::IamSessions => {
                "session_token, access_key_id, account_id, role_name, session_name, expires_at"
            }
        };
        let query = format!(
            "SELECT {columns}, created_at, secret_key_encrypted, \
             WRITETIME(secret_key_encrypted) AS write_time FROM {}.{}",
            self.catalog_keyspace(),
            table.name()
        );
        let paging_state = page
            .map(|page| {
                BASE64
                    .decode(page)
                    .map(cdrs_tokio::types::CBytes::new)
                    .map_err(|e| {
                        OpError::Internal(format!("{WHAT}: the page token does not decode: {e}"))
                    })
            })
            .transpose()?;
        let failed = |e: &dyn std::fmt::Display| {
            tracing::error!("{WHAT} query at ALL failed: {e}");
            OpError::Internal(format!("{WHAT}: {e}"))
        };
        let body = self
            .session()
            .run_paged(
                Stmt::ReadAll,
                &query,
                QueryValues::SimpleValues(vec![]),
                i32::try_from(page_size).unwrap_or(i32::MAX),
                paging_state,
            )
            .await
            .map_err(|e| failed(&e))?
            .response_body()
            .map_err(|e| failed(&e))?;
        let next = body
            .as_rows_metadata()
            .filter(|metadata| metadata.flags.contains(RowsMetadataFlags::HAS_MORE_PAGES))
            .and_then(|metadata| metadata.paging_state.clone())
            .and_then(cdrs_tokio::types::CBytes::into_bytes)
            .map(|state| BASE64.encode(state));
        let rows = body
            .into_rows()
            .unwrap_or_default()
            .iter()
            .map(|row| Self::stored_secret_row(table, row))
            .collect::<OpResult<Vec<_>>>()?;
        Ok(SecretPage { rows, next })
    }

    /// One row of a page: the row's primary key, its AAD fields, its
    /// ciphertext and the write time of the ciphertext.
    fn stored_secret_row(table: SecretTable, row: &Row) -> OpResult<StoredSecretRow> {
        const WHAT: &str = "read_secret_page";
        let optional = |column: &str| -> OpResult<Option<i64>> {
            row.get_by_name(column).map_err(|e| {
                tracing::error!("{WHAT} parse column '{column}': {e}");
                OpError::Internal("Database error".to_owned())
            })
        };
        let access_key_id: String = get_column(row, "access_key_id", WHAT)?;
        let account_id = get_column(row, "account_id", WHAT)?;
        let (key, principal_name, session_name, expires_at) = match table {
            SecretTable::AccessKeys => (
                access_key_id.clone(),
                get_column(row, "user_name", WHAT)?,
                None,
                None,
            ),
            SecretTable::IamSessions => (
                get_column(row, "session_token", WHAT)?,
                get_column(row, "role_name", WHAT)?,
                Some(get_column(row, "session_name", WHAT)?),
                optional("expires_at")?
                    .map(|millis| timestamp_to_datetime(millis, WHAT))
                    .transpose()?
                    .map(|expires| expires.unix_timestamp()),
            ),
        };
        let created_at = optional("created_at")?
            .map(|millis| timestamp_to_datetime(millis, WHAT))
            .transpose()?;
        let secret_key_encrypted: Blob = get_column(row, "secret_key_encrypted", WHAT)?;
        Ok(StoredSecretRow {
            table,
            key,
            access_key_id,
            account_id,
            principal_name,
            session_name,
            expires_at,
            created_at,
            secret_key_encrypted: secret_key_encrypted.into_vec(),
            write_timestamp: optional("write_time")?,
        })
    }

    pub(crate) async fn write_secrets_impl(
        &self,
        table: SecretTable,
        rows: &[SecretWrite],
    ) -> OpResult<()> {
        let query = format!(
            "UPDATE {}.{} USING TIMESTAMP ? SET secret_key_encrypted = ? WHERE {} = ?",
            self.catalog_keyspace(),
            table.name(),
            table.key_column()
        );
        // Above the row's write time, and never behind the clock: a later
        // plain write of the row keeps winning by time.
        let now = crate::data::version::now_micros();
        let values = rows
            .iter()
            .map(|row| {
                let stamp = row
                    .write_timestamp
                    .map_or(now, |written| now.max(written.saturating_add(1)));
                cdrs_tokio::query_values!(
                    stamp,
                    Blob::new(row.secret_key_encrypted.clone()),
                    row.key.as_str()
                )
            })
            .collect();
        self.execute_each_at_all(&query, values, "write_secrets")
            .await
    }

    pub(crate) async fn delete_secrets_impl(
        &self,
        table: SecretTable,
        keys: &[String],
    ) -> OpResult<()> {
        // Each delete stamps itself at the row's own write time when that is
        // ahead of this host's clock, and never further ahead than it must:
        // a tombstone far in the future would also suppress a later import
        // of the same access key ID, which is how section 11 recovers one.
        // The read runs in the same stream as the delete it stamps, so a
        // page of a thousand keys takes the rounds the deletes alone took
        // and stays inside the store's request bound.
        let columns = Self::write_time_columns(table);
        let read = format!(
            "SELECT {} FROM {}.{} WHERE {} = ?",
            Self::write_time_selection(columns),
            self.catalog_keyspace(),
            table.name(),
            table.key_column()
        );
        let query = format!(
            "DELETE FROM {}.{} USING TIMESTAMP ? WHERE {} = ?",
            self.catalog_keyspace(),
            table.name(),
            table.key_column()
        );
        self.stamp_and_delete_each(&read, &query, columns.len(), keys, "delete_secrets")
            .await
    }

    /// The non-key columns whose write time a row tombstone must cover: a
    /// primary key column has no write time of its own in Cassandra, and a
    /// tombstone below any remaining cell's timestamp leaves the row with
    /// that cell and null key columns, which `stored_secret_row` then fails
    /// to parse. Every non-key column of the table is listed, because a
    /// column that one statement writes alone carries that statement's
    /// timestamp: `update_session_policy_impl` writes `session_policy` with
    /// a plain `UPDATE`, which the coordinator stamps with its own clock.
    fn write_time_columns(table: SecretTable) -> &'static [&'static str] {
        match table {
            SecretTable::AccessKeys => &[
                "account_id",
                "user_name",
                "secret_key_encrypted",
                "is_active",
                "created_at",
            ],
            SecretTable::IamSessions => &[
                "account_id",
                "role_name",
                "session_name",
                "access_key_id",
                "secret_key_encrypted",
                "session_tags",
                "session_policy",
                "expires_at",
                "created_at",
            ],
        }
    }

    /// `columns` as the select list of a write-time read. The reader takes
    /// the values by position ([`Self::max_write_time`]), so none is named.
    fn write_time_selection(columns: &[&str]) -> String {
        columns
            .iter()
            .map(|column| format!("WRITETIME({column})"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Read the write time of each key in `keys` through `read` and delete
    /// its row through `query` stamped at [`Self::tombstone_at`], a few keys
    /// at a time. `count` is the number of write times `read` selects.
    async fn stamp_and_delete_each(
        &self,
        read: &str,
        query: &str,
        count: usize,
        keys: &[String],
        context: &str,
    ) -> OpResult<()> {
        Self::each_at_all(keys.iter().cloned(), |key: String| async move {
            let written = self.max_write_time(read, &key, count, context).await?;
            execute_at(
                self.session(),
                query,
                cdrs_tokio::query_values!(Self::tombstone_at(written), key.as_str()),
                Stmt::WriteAll,
                context,
            )
            .await
        })
        .await
    }

    /// The largest of the `count` write times that `read` selects for the
    /// row `key` names, read at `ALL`, or `None` when the row is gone or
    /// holds no written cell.
    async fn max_write_time(
        &self,
        read: &str,
        key: &str,
        count: usize,
        context: &str,
    ) -> OpResult<Option<i64>> {
        let rows: Vec<Row> = crate::cassandra_util::rows_at(
            self.session(),
            read,
            Stmt::ReadAll,
            cdrs_tokio::query_values!(key),
            context,
        )
        .await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let mut largest = None;
        for index in 0..count {
            let written: Option<i64> = row.get_by_index(index).map_err(|e| {
                tracing::error!("{context} parse write time {index}: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            largest = largest.max(written);
        }
        Ok(largest)
    }

    /// The timestamp of a tombstone over a cell written at `written`: this
    /// host's clock, or the cell's own write time when that is ahead of it.
    /// A tombstone wins a tie, so the cell's own time is enough.
    fn tombstone_at(written: Option<i64>) -> i64 {
        let now = crate::data::version::now_micros();
        written.map_or(now, |written| now.max(written))
    }

    /// Run `query` once per element of `values` at `ALL`, a few at a time,
    /// as a restore's writes run, so a page of a thousand rows completes
    /// inside the request bound of the migration backend's store, which the
    /// offline commands go through.
    async fn execute_each_at_all(
        &self,
        query: &str,
        values: Vec<QueryValues>,
        context: &str,
    ) -> OpResult<()> {
        Self::each_at_all(values, |values| {
            execute_at(self.session(), query, values, Stmt::WriteAll, context)
        })
        .await
    }

    /// Await `run` over each element of `items`, a few at a time, and return
    /// the first error. The concurrency is a restore's write concurrency, so
    /// a page of a thousand items completes inside the request bound of the
    /// migration backend's store, which the offline commands go through. A
    /// per-item read belongs inside `run`, so that it takes the same rounds
    /// as the write it belongs to.
    async fn each_at_all<T, F, Fut>(items: impl IntoIterator<Item = T>, run: F) -> OpResult<()>
    where
        F: Fn(T) -> Fut,
        Fut: Future<Output = OpResult<()>>,
    {
        use futures::StreamExt as _;
        let mut statements = futures::stream::iter(items)
            .map(run)
            .buffer_unordered(crate::backup_engine::RESTORE_WRITE_CONCURRENCY);
        while let Some(result) = statements.next().await {
            result?;
        }
        Ok(())
    }

    pub(crate) async fn delete_setting_impl(&self, key: &str) -> OpResult<()> {
        let read = format!(
            "SELECT WRITETIME(value) FROM {}.settings WHERE key = ?",
            self.catalog_keyspace()
        );
        let query = format!(
            "DELETE FROM {}.settings USING TIMESTAMP ? WHERE key = ?",
            self.catalog_keyspace()
        );
        self.stamp_and_delete_each(&read, &query, 1, &[key.to_owned()], "delete_setting")
            .await
    }

    /// `UPDATE ... IF value = ?` with its commit at `ALL` and its serial
    /// phase at `LOCAL_SERIAL` ([`Stmt::LwtAll`]), then the row read back at
    /// `ALL`, as every write of
    /// the key ring runs ([`key_row_at_all`]). A conditional update takes no
    /// client timestamp, and a cell stamped above the update's timestamp
    /// keeps its value, so an applied update whose row does not hold `value`
    /// is an error, as a delete that leaves its row is for the
    /// encryption-key commands.
    pub(crate) async fn replace_setting_impl(
        &self,
        key: &str,
        expected: &str,
        value: &str,
    ) -> OpResult<bool> {
        const WHAT: &str = "replace_setting";
        let keyspace = self.catalog_keyspace();
        let update = format!("UPDATE {keyspace}.settings SET value = ? WHERE key = ? IF value = ?");
        let values = cdrs_tokio::query_values!(value, key, expected);
        let session = self.session();
        let applied = crate::cassandra_util::apply_lwt_at::<OpError>(
            session,
            &update,
            values,
            Stmt::LwtAll,
            WHAT,
        )
        .await
        .map_err(|_| key_row_at_all_failed(WHAT, key, "the driver's error is in the log"))?;
        if !applied {
            return Ok(false);
        }
        let read = format!("SELECT value FROM {keyspace}.settings WHERE key = ?");
        let rows: Vec<Row> = crate::cassandra_util::rows_at::<OpError>(
            session,
            &read,
            Stmt::ReadAll,
            cdrs_tokio::query_values!(key),
            WHAT,
        )
        .await
        .map_err(|e| key_row_at_all_failed(WHAT, key, e.message()))?;
        let held: Option<String> = rows
            .first()
            .map(|row| get_column(row, "value", WHAT))
            .transpose()?;
        if held.as_deref() != Some(value) {
            return Err(OpError::Internal(format!(
                "{WHAT}: the {key} row does not hold the new value after the applied update"
            )));
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::{key_row_at_all, key_row_at_all_failed};

    /// The settings statements that run at `ALL`: each write of the key
    /// ring, and each read of a key row from the commands' stores.
    #[test]
    fn the_ring_writes_and_the_commands_key_row_reads_run_at_all() {
        // (row, a write, from the commands' stores, at ALL)
        for (key, write, commands, all) in [
            ("encryption_key_ring", true, false, true),
            ("encryption_key_ring", true, true, true),
            ("encryption_key_ring", false, true, true),
            ("encryption_key", false, true, true),
            ("catalog_id", false, true, true),
            ("encryption_key_ring", false, false, false),
            ("catalog_id", false, false, false),
            ("encryption_key", true, true, false),
            ("log_level", false, true, false),
            ("log_level", true, true, false),
        ] {
            assert_eq!(
                key_row_at_all(key, write, commands),
                all,
                "{key}: write {write}, commands {commands}"
            );
        }
    }

    /// A key row statement at `ALL` that fails names the consistency and
    /// the check an operator runs, not only "Database error".
    #[test]
    fn a_failed_key_row_statement_names_all_and_the_node_check() {
        let error = key_row_at_all_failed("get_setting", "encryption_key_ring", "unavailable");
        let message = error.message();
        for part in [
            "get_setting of the encryption_key_ring row at consistency ALL failed: unavailable",
            "every replica of the catalog keyspace must answer",
            "`nodetool status`",
        ] {
            assert!(message.contains(part), "{message}");
        }
    }
}
