// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored-secret rows as the encryption-key commands read and write them
//! (RFC-0000 section 8.4), and the credential lookups through the key holder.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use extenddb_auth::CredentialStore;
    use extenddb_storage::management_store::{
        ManagementStore, SecretWrite, SettingsStore, StoredSecretRow,
    };
    use extenddb_storage::secret_keys::{SecretKeys, SecretRow, SecretTable};
    use extenddb_storage_cassandra::{CassandraCatalogStore, CassandraCredentialStore};

    use crate::helpers::{isolated_engine, unique_test_id};

    const ACCOUNT: &str = "123456789012";
    const PAGE: usize = 1000;

    /// A catalog key holder and a DEK holder: the two keys a switch holds.
    fn two_keys() -> (Arc<SecretKeys>, Arc<SecretKeys>) {
        let key = extenddb_storage::bootstrapper::helpers::generate_encryption_key().unwrap();
        let catalog_key = Arc::new(SecretKeys::catalog_key(&key).unwrap());
        let dek = Arc::new(SecretKeys::dek_for_tests(
            [7; 32],
            1,
            "catalog-id-for-tests",
        ));
        (catalog_key, dek)
    }

    /// Every row of `table`, read page by page.
    async fn all_rows(
        store: &dyn ManagementStore,
        table: SecretTable,
        page_size: usize,
    ) -> Vec<StoredSecretRow> {
        let mut rows = Vec::new();
        let mut page = None;
        loop {
            let read = store
                .read_secret_page(table, page.as_deref(), page_size)
                .await
                .unwrap();
            rows.extend(read.rows);
            page = read.next;
            if page.is_none() {
                return rows;
            }
        }
    }

    /// The row of the access key `access_key_id`, read alone.
    async fn row_of(
        store: &dyn ManagementStore,
        table: SecretTable,
        access_key_id: &str,
    ) -> StoredSecretRow {
        all_rows(store, table, PAGE)
            .await
            .into_iter()
            .find(|row| row.access_key_id == access_key_id)
            .expect("the row is read")
    }

    /// The re-encryption of `row` under `target`, as the pass writes it.
    fn rewrite(row: &StoredSecretRow, from: &SecretKeys, target: &SecretKeys) -> SecretWrite {
        let secret = from
            .decrypt(&row.aad_row(), &row.secret_key_encrypted)
            .unwrap();
        SecretWrite {
            key: row.key.clone(),
            secret_key_encrypted: target.encrypt(&row.aad_row(), &secret).unwrap(),
            write_timestamp: row.write_timestamp,
        }
    }

    /// The encryption-key command and startup cases of RFC-0000 section 15,
    /// on a catalog as `init` leaves it: a catalog key, a catalog id and no
    /// ring, behind a store that holds the catalog key as the server does.
    #[tokio::test]
    async fn the_encryption_key_commands_run_on_a_cassandra_catalog() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let isolated = isolated_engine("keycmds").await;
        let settings = isolated.settings();
        let key = extenddb_storage::bootstrapper::helpers::generate_encryption_key().unwrap();
        settings
            .set_setting(extenddb_storage::secret_keys::CATALOG_KEY_SETTING, &key)
            .await
            .unwrap();
        let keys = extenddb_storage::server_components::load_secret_keys(
            &settings,
            extenddb_storage::server_components::KeySource::CatalogKey,
        )
        .await
        .unwrap();
        let store = settings.with_secret_keys(keys);
        extenddb_storage::key_conformance::run(
            &store,
            &store,
            extenddb_storage::key_conformance::Expectations {
                write_timestamps: true,
                data_connection: false,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn the_pass_reads_rewrites_and_deletes_stored_secrets() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let isolated = isolated_engine("secrets").await;
        let (catalog_key, dek) = two_keys();
        let store: CassandraCatalogStore = isolated
            .settings()
            .with_secret_keys(Some(Arc::clone(&catalog_key)));
        store.create_account(ACCOUNT, "acct").await.unwrap();
        store.create_user(ACCOUNT, "alice", None).await.unwrap();
        let trust = serde_json::json!({"Version": "2012-10-17", "Statement": []});
        store.create_role(ACCOUNT, "app", &trust).await.unwrap();

        // Three access keys and one session, written as the server writes
        // them: the store encrypts the access keys, AssumeRole the session,
        // with its expiry in whole seconds.
        let mut created = Vec::new();
        for _ in 0..3 {
            created.push(store.create_access_key(ACCOUNT, "alice").await.unwrap());
        }
        let session_key_id = format!("ASIA{}", unique_test_id()[..16].to_uppercase());
        let expires_at = (time::OffsetDateTime::now_utc() + time::Duration::hours(1))
            .replace_nanosecond(0)
            .unwrap();
        let session_secret = "session-secret-for-the-test";
        let session_row = SecretRow::session(
            &session_key_id,
            ACCOUNT,
            "app",
            "run",
            expires_at.unix_timestamp(),
        );
        store
            .store_session(
                "token-for-the-test",
                &session_key_id,
                &catalog_key.encrypt(&session_row, session_secret).unwrap(),
                ACCOUNT,
                "app",
                "run",
                &None,
                &None,
                expires_at,
            )
            .await
            .unwrap();

        // Read: every row with its AAD fields, its write time, and a
        // ciphertext the catalog key opens, in pages of one.
        let keys = all_rows(&store, SecretTable::AccessKeys, 1).await;
        assert_eq!(keys.len(), 3, "{keys:?}");
        for row in &keys {
            assert_eq!(row.key, row.access_key_id);
            assert_eq!(
                (row.account_id.as_str(), row.principal_name.as_str()),
                (ACCOUNT, "alice")
            );
            assert_eq!((row.session_name.as_deref(), row.expires_at), (None, None));
            assert!(
                row.created_at.is_some() && row.write_timestamp.is_some(),
                "{row:?}"
            );
            let plaintext = catalog_key
                .decrypt(&row.aad_row(), &row.secret_key_encrypted)
                .unwrap();
            let made = created
                .iter()
                .find(|k| k.access_key_id == row.access_key_id)
                .unwrap();
            assert_eq!(*plaintext, made.secret_access_key);
        }
        let sessions = all_rows(&store, SecretTable::IamSessions, PAGE).await;
        assert_eq!(sessions.len(), 1, "{sessions:?}");
        let session = &sessions[0];
        assert_eq!(session.key, "token-for-the-test");
        assert_eq!(session.aad_row(), session_row);
        assert_eq!(
            *catalog_key
                .decrypt(&session.aad_row(), &session.secret_key_encrypted)
                .unwrap(),
            session_secret
        );

        // Write: each secret under the DEK, by primary key. The DEK opens
        // the rows read again, and the catalog key no longer does.
        let writes: Vec<SecretWrite> = keys
            .iter()
            .map(|row| rewrite(row, &catalog_key, &dek))
            .collect();
        store
            .write_secrets(SecretTable::AccessKeys, &writes)
            .await
            .unwrap();
        store
            .write_secrets(
                SecretTable::IamSessions,
                &[rewrite(session, &catalog_key, &dek)],
            )
            .await
            .unwrap();
        for table in SecretTable::ALL {
            for row in all_rows(&store, table, PAGE).await {
                assert!(
                    dek.decrypt(&row.aad_row(), &row.secret_key_encrypted)
                        .is_ok(),
                    "{row:?}"
                );
                assert!(
                    catalog_key
                        .decrypt(&row.aad_row(), &row.secret_key_encrypted)
                        .is_err()
                );
            }
        }
        // The credential store shares the DEK and opens both rows; one built
        // with no key opens neither.
        let credentials = CassandraCredentialStore::new(
            isolated.engine.session_arc(),
            isolated.config.keyspace_prefix.clone(),
            Some(Arc::clone(&dek)),
        );
        let found = credentials
            .lookup_credential(&created[0].access_key_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.secret_key, created[0].secret_access_key);
        let found = credentials
            .lookup_credential(&session_key_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (found.secret_key.as_str(), found.expires_at),
            (session_secret, Some(expires_at))
        );
        let keyless = CassandraCredentialStore::new(
            isolated.engine.session_arc(),
            isolated.config.keyspace_prefix.clone(),
            None,
        );
        assert!(
            keyless
                .lookup_credential(&created[0].access_key_id)
                .await
                .is_err()
        );
        let keyless_store = isolated.settings();
        assert!(
            keyless_store
                .create_access_key(ACCOUNT, "alice")
                .await
                .is_err()
        );

        // Delete: the rows reset removes, and a settings row.
        store
            .delete_secrets(
                SecretTable::AccessKeys,
                &[keys[0].key.clone(), keys[1].key.clone()],
            )
            .await
            .unwrap();
        assert_eq!(
            all_rows(&store, SecretTable::AccessKeys, PAGE).await.len(),
            1
        );
        store.set_setting("row_to_delete", "value").await.unwrap();
        store.delete_setting("row_to_delete").await.unwrap();
        store.delete_setting("row_to_delete").await.unwrap();
        assert_eq!(store.get_setting("row_to_delete").await.unwrap(), None);
    }

    /// CreateAccessKey writes with `INSERT ... IF NOT EXISTS`, whose Paxos
    /// timestamp can be ahead of a later plain update's clock. The pass's
    /// update sets `USING TIMESTAMP` above the row's write time, so it lands
    /// on a row written an hour ahead of the clock.
    #[tokio::test]
    async fn a_rewrite_lands_on_a_row_whose_write_time_is_ahead_of_the_clock() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let isolated = isolated_engine("secretsahead").await;
        let (catalog_key, dek) = two_keys();
        let store = isolated
            .settings()
            .with_secret_keys(Some(Arc::clone(&catalog_key)));
        let access_key_id = "AKIAEXTENDDBAHEAD001";
        let row = SecretRow::access_key(access_key_id, ACCOUNT, "alice");
        let ahead = chrono::Utc::now().timestamp_micros() + 3_600_000_000;
        isolated
            .engine
            .session_arc()
            .query_with_values(
                format!(
                    "INSERT INTO {}_catalog.access_keys (access_key_id, account_id, user_name, \
                     secret_key_encrypted, is_active, created_at) VALUES (?, ?, ?, ?, true, \
                     toTimestamp(now())) USING TIMESTAMP {ahead}",
                    isolated.config.keyspace_prefix
                ),
                cdrs_tokio::query_values!(
                    access_key_id,
                    ACCOUNT,
                    "alice",
                    cdrs_tokio::types::blob::Blob::new(
                        catalog_key.encrypt(&row, "secret").unwrap()
                    )
                ),
            )
            .await
            .unwrap();

        let read = row_of(&store, SecretTable::AccessKeys, access_key_id).await;
        assert_eq!(read.write_timestamp, Some(ahead));
        store
            .write_secrets(
                SecretTable::AccessKeys,
                &[rewrite(&read, &catalog_key, &dek)],
            )
            .await
            .unwrap();
        let again = row_of(&store, SecretTable::AccessKeys, access_key_id).await;
        assert_eq!(again.write_timestamp, Some(ahead + 1));
        assert_eq!(
            *dek.decrypt(&again.aad_row(), &again.secret_key_encrypted)
                .unwrap(),
            "secret"
        );
    }

    /// A row tombstone below a cell's timestamp leaves the cell in place, so
    /// `delete_secrets` and `delete_setting` stamp themselves at the largest
    /// write time the row holds when that is ahead of the clock. `reset`
    /// reports the rows it deleted, and a switch reports the catalog key row
    /// gone, so a lost delete would be a wrong report.
    ///
    /// The session row's `session_policy` carries a later timestamp than its
    /// other columns, as the policy migration's plain `UPDATE` leaves it. A
    /// tombstone that covered only the column the page read selects would
    /// leave that cell, and with it a row whose key columns are null, which
    /// no later command can parse.
    #[tokio::test]
    async fn a_delete_lands_on_a_row_whose_write_time_is_ahead_of_the_clock() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }
        let isolated = isolated_engine("secretsdelahead").await;
        let (catalog_key, _) = two_keys();
        let store = isolated
            .settings()
            .with_secret_keys(Some(Arc::clone(&catalog_key)));
        let access_key_id = "AKIAEXTENDDBAHEAD002";
        let row = SecretRow::access_key(access_key_id, ACCOUNT, "alice");
        let ahead = chrono::Utc::now().timestamp_micros() + 3_600_000_000;
        let prefix = &isolated.config.keyspace_prefix;
        let session = isolated.engine.session_arc();
        session
            .query_with_values(
                format!(
                    "INSERT INTO {prefix}_catalog.access_keys (access_key_id, account_id, \
                     user_name, secret_key_encrypted, is_active, created_at) VALUES (?, ?, ?, \
                     ?, true, toTimestamp(now())) USING TIMESTAMP {ahead}"
                ),
                cdrs_tokio::query_values!(
                    access_key_id,
                    ACCOUNT,
                    "alice",
                    cdrs_tokio::types::blob::Blob::new(
                        catalog_key.encrypt(&row, "secret").unwrap()
                    )
                ),
            )
            .await
            .unwrap();
        session
            .query_with_values(
                format!(
                    "INSERT INTO {prefix}_catalog.settings (key, value) VALUES (?, ?) \
                     USING TIMESTAMP {ahead}"
                ),
                cdrs_tokio::query_values!("encryption_key", "a-value"),
            )
            .await
            .unwrap();

        // The session row: every column at `ahead`, then its policy an hour
        // later again, which only that column carries.
        let session_token = "token-written-ahead-of-the-clock";
        let session_key_id = "ASIAEXTENDDBAHEAD002";
        let expires_at = (time::OffsetDateTime::now_utc() + time::Duration::hours(1))
            .replace_nanosecond(0)
            .unwrap();
        let session_row = SecretRow::session(
            session_key_id,
            ACCOUNT,
            "app",
            "run",
            expires_at.unix_timestamp(),
        );
        session
            .query_with_values(
                format!(
                    "INSERT INTO {prefix}_catalog.iam_sessions (session_token, access_key_id, \
                     account_id, role_name, session_name, secret_key_encrypted, expires_at, \
                     created_at) VALUES (?, ?, ?, 'app', 'run', ?, ?, toTimestamp(now())) \
                     USING TIMESTAMP {ahead}"
                ),
                cdrs_tokio::query_values!(
                    session_token,
                    session_key_id,
                    ACCOUNT,
                    cdrs_tokio::types::blob::Blob::new(
                        catalog_key.encrypt(&session_row, "session-secret").unwrap()
                    ),
                    expires_at.unix_timestamp() * 1000
                ),
            )
            .await
            .unwrap();
        let policy_ahead = ahead + 3_600_000_000;
        session
            .query_with_values(
                format!(
                    "UPDATE {prefix}_catalog.iam_sessions USING TIMESTAMP {policy_ahead} \
                     SET session_policy = ? WHERE session_token = ?"
                ),
                cdrs_tokio::query_values!("{\"Version\":\"2012-10-17\"}", session_token),
            )
            .await
            .unwrap();

        store
            .delete_secrets(SecretTable::AccessKeys, &[access_key_id.to_owned()])
            .await
            .unwrap();
        store
            .delete_secrets(SecretTable::IamSessions, &[session_token.to_owned()])
            .await
            .unwrap();
        store.delete_setting("encryption_key").await.unwrap();

        let keys = store
            .read_secret_page(SecretTable::AccessKeys, None, 10)
            .await
            .unwrap();
        assert!(
            !keys.rows.iter().any(|row| row.key == access_key_id),
            "the access key row written an hour ahead of the clock is gone"
        );
        // A page read that parses at all proves no cell of the session row
        // survived: `stored_secret_row` fails on a row with null key columns.
        let sessions = store
            .read_secret_page(SecretTable::IamSessions, None, 10)
            .await
            .expect("the sessions page parses, so no cell of the row is left");
        assert!(
            sessions.rows.is_empty(),
            "the session row whose policy was written two hours ahead is gone"
        );
        assert_eq!(store.get_setting("encryption_key").await.unwrap(), None);
    }
}
