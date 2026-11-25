// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored secrets as the encryption-key commands read and rewrite them,
//! for `SqliteCatalogStore`.
//!
//! Each write runs in one `BEGIN IMMEDIATE` transaction with
//! `PRAGMA secure_delete = ON`, so SQLite overwrites a replaced ciphertext
//! or a deleted row with zeros, and ends with `PRAGMA
//! wal_checkpoint(TRUNCATE)`, which copies the new pages into the database
//! file and empties the WAL. After the last write of `switch`, `rotate` and
//! `reset`, `VACUUM` rebuilds every page, which removes the old copies of
//! rows that SQLite moved between pages before the command. A copy of the
//! files taken after a command holds no ciphertext it replaced and no row it
//! deleted.

use extenddb_storage::management_store::{
    OpError, OpResult, SecretPage, SecretWrite, StoredSecretRow,
};
use extenddb_storage::secret_keys::SecretTable;

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::parse_timestamp;

/// An `access_keys` row: key, secret, account, user, created.
type AccessKeyRow = (String, Vec<u8>, String, String, String);

/// An `iam_sessions` row: token, access key ID, secret, account, role,
/// session name, expiry, created.
type SessionRow = (
    String,
    String,
    Vec<u8>,
    String,
    String,
    String,
    String,
    String,
);

impl SqliteCatalogStore {
    pub(crate) async fn read_secret_page_impl(
        &self,
        table: SecretTable,
        page: Option<&str>,
        page_size: usize,
    ) -> OpResult<SecretPage> {
        let what = "read_secret_page";
        let limit = i64::try_from(page_size).unwrap_or(i64::MAX);
        let rows = match table {
            SecretTable::AccessKeys => {
                let rows: Vec<AccessKeyRow> = sqlx::query_as(
                    "SELECT access_key_id, secret_key_encrypted, account_id, user_name, \
                     created_at FROM access_keys \
                     WHERE ? IS NULL OR access_key_id > ? ORDER BY access_key_id LIMIT ?",
                )
                .bind(page)
                .bind(page)
                .bind(limit)
                .fetch_all(self.pool())
                .await
                .map_err(|e| OpError::Internal(format!("{what}: {e}")))?;
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
                            created_at: parse_timestamp(&created).ok(),
                            secret_key_encrypted: secret,
                            write_timestamp: None,
                        },
                    )
                    .collect::<Vec<_>>()
            }
            SecretTable::IamSessions => {
                let rows: Vec<SessionRow> = sqlx::query_as(
                    "SELECT session_token, access_key_id, secret_key_encrypted, account_id, \
                     role_name, session_name, expires_at, created_at FROM iam_sessions \
                     WHERE ? IS NULL OR session_token > ? ORDER BY session_token LIMIT ?",
                )
                .bind(page)
                .bind(page)
                .bind(limit)
                .fetch_all(self.pool())
                .await
                .map_err(|e| OpError::Internal(format!("{what}: {e}")))?;
                rows.into_iter()
                    .map(
                        |(token, id, secret, account_id, role, session, expires, created)| {
                            let expires_at = parse_timestamp(&expires).map_err(|e| {
                                OpError::Internal(format!(
                                    "{what}: the expires_at of session {id} does not parse: {e}"
                                ))
                            })?;
                            Ok(StoredSecretRow {
                                table,
                                key: token,
                                access_key_id: id,
                                account_id,
                                principal_name: role,
                                session_name: Some(session),
                                expires_at: Some(expires_at.unix_timestamp()),
                                created_at: parse_timestamp(&created).ok(),
                                secret_key_encrypted: secret,
                                write_timestamp: None,
                            })
                        },
                    )
                    .collect::<OpResult<Vec<_>>>()?
            }
        };
        let next = (rows.len() == page_size)
            .then(|| rows.last().map(|row| row.key.clone()))
            .flatten();
        Ok(SecretPage { rows, next })
    }

    pub(crate) async fn write_secrets_impl(
        &self,
        table: SecretTable,
        rows: &[SecretWrite],
    ) -> OpResult<()> {
        let sql = format!(
            "UPDATE {} SET secret_key_encrypted = ? WHERE {} = ?",
            table.name(),
            table.key_column()
        );
        let rows: Vec<_> = rows
            .iter()
            .map(|row| (Some(row.secret_key_encrypted.as_slice()), row.key.as_str()))
            .collect();
        self.overwrite("write_secrets", &sql, &rows).await
    }

    pub(crate) async fn delete_secrets_impl(
        &self,
        table: SecretTable,
        keys: &[String],
    ) -> OpResult<()> {
        let sql = format!(
            "DELETE FROM {} WHERE {} = ?",
            table.name(),
            table.key_column()
        );
        let rows: Vec<_> = keys.iter().map(|k| (None, k.as_str())).collect();
        self.overwrite("delete_secrets", &sql, &rows).await
    }

    pub(crate) async fn delete_setting_impl(&self, key: &str) -> OpResult<()> {
        self.overwrite(
            "delete_setting",
            "DELETE FROM settings WHERE key = ?",
            &[(None, key)],
        )
        .await
    }

    /// Run `sql` once per row, binding the row's bytes (when it has them)
    /// and then its key, in one transaction under `secure_delete`, then
    /// checkpoint and truncate the WAL. A key that matches no row changes
    /// nothing and is not an error.
    async fn overwrite(
        &self,
        what: &str,
        sql: &str,
        rows: &[(Option<&[u8]>, &str)],
    ) -> OpResult<()> {
        let failed = |e: sqlx::Error| OpError::Internal(format!("{what}: {e}"));
        // D1: every writer holds the write lock, here through the
        // checkpoint, so no write of this process lands between the two.
        let _writer = self.lock_writes().await;
        let mut tx = self
            .pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(failed)?;
        sqlx::query("PRAGMA secure_delete = ON")
            .execute(&mut *tx)
            .await
            .map_err(failed)?;
        for &(bytes, key) in rows {
            let statement = sqlx::query(sqlx::AssertSqlSafe(sql));
            let statement = match bytes {
                Some(bytes) => statement.bind(bytes),
                None => statement,
            };
            statement
                .bind(key)
                .execute(&mut *tx)
                .await
                .map_err(failed)?;
        }
        tx.commit().await.map_err(failed)?;
        self.checkpoint(what).await
    }

    /// `VACUUM` under `PRAGMA secure_delete = ON` and `PRAGMA temp_store =
    /// MEMORY`, then the checkpoint. A row that SQLite moved to another page
    /// as its table grew, before the command, left its old bytes in the page
    /// it moved from, where `secure_delete` does not reach; `VACUUM`
    /// rebuilds every page, and `temp_store = MEMORY` keeps the rebuild out
    /// of a temporary file.
    pub(crate) async fn erase_free_space_impl(&self) -> OpResult<()> {
        const WHAT: &str = "erase_free_space";
        let failed = |e: sqlx::Error| OpError::Internal(format!("{WHAT}: {e}"));
        let _writer = self.lock_writes().await;
        let mut connection = self.pool().acquire().await.map_err(failed)?;
        for statement in [
            "PRAGMA secure_delete = ON",
            "PRAGMA temp_store = MEMORY",
            "VACUUM",
        ] {
            sqlx::query(statement)
                .execute(&mut *connection)
                .await
                .map_err(failed)?;
        }
        // The migration lock's pool holds one connection, which the
        // checkpoint takes next.
        drop(connection);
        self.checkpoint(WHAT).await
    }

    /// `PRAGMA wal_checkpoint(TRUNCATE)`, which copies the new pages into the
    /// database file and empties the WAL, under the write lock the caller
    /// holds.
    async fn checkpoint(&self, what: &str) -> OpResult<()> {
        let (busy, _, _): (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetch_one(self.pool())
            .await
            .map_err(|e| OpError::Internal(format!("{what}: {e}")))?;
        if busy != 0 {
            return Err(OpError::Internal(format!(
                "{what}: another connection to the database blocked the WAL checkpoint, so the \
                 WAL file can still hold the replaced bytes; stop every process that has the \
                 database open and run the command again"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use extenddb_storage::key_commands::{KekFile, KeyStores, Outcome, RunFlags, switch};
    use extenddb_storage::key_ring::{KEY_RING_SETTING, open_ring};
    use extenddb_storage::management_store::{ManagementStore, SettingsStore};
    use extenddb_storage::secret_keys::{SecretKeys, SecretRow};
    use serde_json::json;
    use zeroize::Zeroizing;

    use super::*;

    const ACCOUNT: &str = "123456789012";

    /// Every row of `table`, read one row per page so the page token is used.
    async fn all_rows(store: &SqliteCatalogStore, table: SecretTable) -> Vec<StoredSecretRow> {
        let mut rows = Vec::new();
        let mut page = None;
        loop {
            let read = store
                .read_secret_page(table, page.as_deref(), 1)
                .await
                .expect("read a page");
            rows.extend(read.rows);
            match read.next {
                Some(next) => page = Some(next),
                None => return rows,
            }
        }
    }

    /// The bytes of every file in `dir`: the database, its WAL and its
    /// shared-memory index. It skips a file that goes between the listing
    /// and the read: SQLite deletes the WAL and its index when the last
    /// connection closes.
    fn files(dir: &Path) -> Vec<(String, Vec<u8>)> {
        std::fs::read_dir(dir)
            .expect("list the directory")
            .filter_map(|entry| {
                let path = entry.expect("an entry").path();
                match std::fs::read(&path) {
                    Ok(bytes) => Some((path.display().to_string(), bytes)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => panic!("read {}: {e}", path.display()),
                }
            })
            .collect()
    }

    /// The files that hold `needle`.
    fn holding(files: &[(String, Vec<u8>)], needle: &[u8]) -> Vec<String> {
        files
            .iter()
            .filter(|(_, bytes)| bytes.windows(needle.len()).any(|w| w == needle))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// A KEK that the test holds in memory.
    struct TestKek([u8; 32]);

    impl KekFile for TestKek {
        fn path(&self) -> &str {
            "/run/credentials/extenddb.service/kek"
        }

        fn read(&self) -> Result<Zeroizing<[u8; 32]>, String> {
            Ok(Zeroizing::new(self.0))
        }
    }

    /// After `switch --yes` (every secret re-encrypted under the new DEK and
    /// the catalog key deleted) and the delete of a session, the files of
    /// the database hold neither the catalog key nor any ciphertext that was
    /// replaced or deleted, though they held the catalog key before. The 80
    /// access keys span several pages, so SQLite moved some rows between
    /// pages as the table grew.
    #[tokio::test]
    async fn a_pass_leaves_no_catalog_key_and_no_old_ciphertext_in_the_files() {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-secure-delete-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("dir");
        let location = dir.join("catalog.db").to_string_lossy().into_owned();
        crate::migrations::migrate(&location, "test", Duration::from_secs(5))
            .await
            .expect("a current catalog");
        let pool = crate::server_backend::catalog_pool(&location)
            .await
            .expect("pool");
        let catalog_key = extenddb_storage::bootstrapper::helpers::generate_encryption_key()
            .expect("a catalog key");
        let catalog = Arc::new(SecretKeys::catalog_key(&catalog_key).expect("key holder"));
        let store = SqliteCatalogStore::with_secret_keys(
            pool.clone(),
            Some(Arc::clone(&catalog)),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        store
            .set_setting("encryption_key", &catalog_key)
            .await
            .expect("the catalog key row");
        store
            .create_account(ACCOUNT, "acct")
            .await
            .expect("account");
        store
            .create_user(ACCOUNT, "alice", None)
            .await
            .expect("user");
        for _ in 0..80 {
            store
                .create_access_key(ACCOUNT, "alice")
                .await
                .expect("access key");
        }
        store
            .create_role(
                ACCOUNT,
                "r1",
                &json!({"Version": "2012-10-17", "Statement": []}),
            )
            .await
            .expect("role");
        let expires = time::OffsetDateTime::now_utc()
            .replace_nanosecond(0)
            .expect("whole")
            + time::Duration::hours(1);
        for (token, id) in [
            ("tok-1", "ASIAEXTENDDBTEST0001"),
            ("tok-2", "ASIAEXTENDDBTEST0002"),
        ] {
            let row = SecretRow::session(id, ACCOUNT, "r1", "s", expires.unix_timestamp());
            let sealed = catalog.encrypt(&row, "session-secret").expect("seal");
            store
                .store_session(
                    token, id, &sealed, ACCOUNT, "r1", "s", &None, &None, expires,
                )
                .await
                .expect("session");
        }
        let old: Vec<Vec<u8>> = {
            let mut old = Vec::new();
            for table in SecretTable::ALL {
                old.extend(
                    all_rows(&store, table)
                        .await
                        .into_iter()
                        .map(|row| row.secret_key_encrypted),
                );
            }
            old
        };
        assert_eq!(
            old.len(),
            82,
            "eighty access keys, which span several pages, and two sessions"
        );
        assert!(
            !holding(&files(&dir), catalog_key.as_bytes()).is_empty(),
            "the files hold the catalog key before the switch"
        );

        // `switch --yes` over a store with no key, as the command opens it.
        let offline = SqliteCatalogStore::new(pool.clone());
        let stores = KeyStores {
            management: &offline,
            settings: &offline,
        };
        let kek = TestKek(rand::random());
        let flags = RunFlags {
            yes: true,
            ..RunFlags::default()
        };
        let outcome = switch(stores, &kek, flags).await.expect("switch");
        assert!(matches!(outcome, Outcome::Done(_)), "{outcome:?}");
        assert_eq!(
            offline.get_setting("encryption_key").await.expect("read"),
            None
        );
        let open = open_ring(
            &offline
                .get_setting(KEY_RING_SETTING)
                .await
                .expect("read")
                .expect("the ring"),
            offline
                .get_setting("catalog_id")
                .await
                .expect("read")
                .as_deref(),
            &kek.0,
        )
        .expect("the ring opens");
        let dek = open.active();
        for table in SecretTable::ALL {
            for row in all_rows(&offline, table).await {
                dek.decrypt(&row.aad_row(), &row.secret_key_encrypted)
                    .expect("every secret opens under the DEK");
            }
        }

        // A deleted session's ciphertext goes too.
        let mut old = old;
        let sessions = all_rows(&offline, SecretTable::IamSessions).await;
        let deleted = sessions
            .iter()
            .find(|row| row.key == "tok-2")
            .expect("tok-2");
        old.push(deleted.secret_key_encrypted.clone());
        offline
            .delete_secrets(SecretTable::IamSessions, &["tok-2".to_owned()])
            .await
            .expect("delete a session");
        assert_eq!(all_rows(&store, SecretTable::IamSessions).await.len(), 1);
        pool.close().await;

        let after = files(&dir);
        assert!(
            holding(&after, catalog_key.as_bytes()).is_empty(),
            "a file still holds the catalog key"
        );
        for (i, ciphertext) in old.iter().enumerate() {
            assert!(
                holding(&after, ciphertext).is_empty(),
                "a file still holds old ciphertext {i}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Writing or deleting a row that is gone changes nothing and succeeds.
    #[tokio::test]
    async fn a_write_or_delete_of_a_missing_row_succeeds() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        crate::migrations::apply_for_tests(&pool).await;
        let store = SqliteCatalogStore::new(pool);
        for table in SecretTable::ALL {
            let write = SecretWrite {
                key: "gone".to_owned(),
                secret_key_encrypted: vec![1; 28],
                write_timestamp: None,
            };
            store.write_secrets(table, &[write]).await.expect("write");
            store
                .delete_secrets(table, &["gone".to_owned()])
                .await
                .expect("delete");
            let page = store.read_secret_page(table, None, 10).await.expect("read");
            assert_eq!(page, SecretPage::default());
        }
        store
            .delete_setting("absent")
            .await
            .expect("delete a setting");
    }
}

/// The encryption-key check every backend runs, on a file catalog as `init`
/// leaves it, through the catalog store with the catalog key.
#[cfg(test)]
mod conformance {
    use std::sync::Arc;
    use std::time::Duration;

    use extenddb_storage::key_conformance::{Expectations, run};
    use extenddb_storage::management_store::SettingsStore;
    use extenddb_storage::secret_keys::SecretKeys;

    use crate::catalog_store::SqliteCatalogStore;

    #[tokio::test]
    async fn the_sqlite_store_passes_the_encryption_key_check() {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-key-conformance-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("dir");
        let location = dir.join("catalog.db").to_string_lossy().into_owned();
        crate::migrations::migrate(&location, "test", Duration::from_secs(5))
            .await
            .expect("a current catalog");
        let pool = crate::server_backend::catalog_pool(&location)
            .await
            .expect("pool");
        let catalog_key = extenddb_storage::bootstrapper::helpers::generate_encryption_key()
            .expect("a catalog key");
        SqliteCatalogStore::new(pool.clone())
            .set_setting("encryption_key", &catalog_key)
            .await
            .expect("the catalog key row");
        let keys = SecretKeys::catalog_key(&catalog_key).expect("key holder");
        let store = SqliteCatalogStore::with_secret_keys(
            pool.clone(),
            Some(Arc::new(keys)),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        run(
            &store,
            &store,
            Expectations {
                write_timestamps: false,
                data_connection: false,
            },
        )
        .await;
        pool.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
