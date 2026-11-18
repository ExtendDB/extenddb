// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Every row of `table`, read one row per page so the page token is used.
async fn all_rows(store: &PostgresCatalogStore, table: SecretTable) -> Vec<StoredSecretRow> {
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

/// The pass methods read every row with its AAD fields, page by page, write
/// a batch of ciphertexts by primary key, delete rows and a settings row,
/// and treat a row that is gone as no error.
#[tokio::test]
async fn the_pass_methods_read_write_and_delete_by_primary_key() {
    let Some(base) = base_conn() else {
        return skip("pass methods");
    };
    let s = installed(&base, "pgk_pass").await;
    let catalog = s.catalog().await;
    let old = Arc::new(SecretKeys::catalog_key(CATALOG_KEY).expect("a catalog key"));
    let store = PostgresCatalogStore::with_secret_keys(catalog.clone(), Some(Arc::clone(&old)));
    store
        .create_account(ACCOUNT, "acct")
        .await
        .expect("account");
    store
        .create_user(ACCOUNT, "alice", None)
        .await
        .expect("user");
    for _ in 0..3 {
        store
            .create_access_key(ACCOUNT, "alice")
            .await
            .expect("access key");
    }
    store
        .create_role(
            ACCOUNT,
            "r1",
            &serde_json::json!({"Version": "2012-10-17", "Statement": []}),
        )
        .await
        .expect("role");
    // An expired session is read and rewritten like any other.
    let expired = time::OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .expect("whole seconds")
        - time::Duration::hours(1);
    for (token, id) in [
        ("tok-a", "ASIAEXTENDDBPGTEST01"),
        ("tok-b", "ASIAEXTENDDBPGTEST02"),
    ] {
        let row = SecretRow::session(id, ACCOUNT, "r1", "s", expired.unix_timestamp());
        let sealed = old.encrypt(&row, "session-secret").expect("seal");
        store
            .store_session(
                token, id, &sealed, ACCOUNT, "r1", "s", &None, &None, expired,
            )
            .await
            .expect("session");
    }

    let new = SecretKeys::dek_for_tests([9; 32], 1, "pg-test");
    for table in SecretTable::ALL {
        let rows = all_rows(&store, table).await;
        assert_eq!(
            rows.len(),
            if table == SecretTable::AccessKeys {
                3
            } else {
                2
            }
        );
        let mut writes: Vec<SecretWrite> = rows
            .iter()
            .map(|row| {
                let secret = old
                    .decrypt(&row.aad_row(), &row.secret_key_encrypted)
                    .expect("the row's fields are its AAD");
                SecretWrite {
                    key: row.key.clone(),
                    secret_key_encrypted: new.encrypt(&row.aad_row(), &secret).expect("seal"),
                    write_timestamp: None,
                }
            })
            .collect();
        writes.push(SecretWrite {
            key: "gone".to_owned(),
            secret_key_encrypted: vec![1; 28],
            write_timestamp: None,
        });
        store
            .write_secrets(table, &writes)
            .await
            .expect("write a batch");
        for row in all_rows(&store, table).await {
            new.decrypt(&row.aad_row(), &row.secret_key_encrypted)
                .expect("each row holds its new ciphertext");
        }
    }
    store
        .delete_secrets(
            SecretTable::IamSessions,
            &["tok-b".to_owned(), "gone".to_owned()],
        )
        .await
        .expect("delete sessions");
    assert_eq!(all_rows(&store, SecretTable::IamSessions).await.len(), 1);
    store
        .set_setting("encryption_key", CATALOG_KEY)
        .await
        .expect("a catalog key row");
    store
        .delete_setting("encryption_key")
        .await
        .expect("delete the catalog key row");
    store
        .delete_setting("encryption_key")
        .await
        .expect("delete it again");
    assert_eq!(
        store.get_setting("encryption_key").await.expect("read"),
        None
    );
    catalog.close().await;
    drop(s);
}

/// The encryption-key check every backend runs, on a catalog as `init`
/// leaves it (the catalog key row and the plaintext connection string row),
/// through the catalog store with the catalog key.
#[tokio::test]
async fn the_postgres_store_passes_the_encryption_key_check() {
    let Some(base) = base_conn() else {
        return skip("encryption-key check");
    };
    let s = installed(&base, "pgk_check").await;
    let catalog = s.catalog().await;
    set_row(&catalog, "encryption_key", Some(CATALOG_KEY)).await;
    let keys = SecretKeys::catalog_key(CATALOG_KEY).expect("a catalog key");
    let store = PostgresCatalogStore::with_secret_keys(catalog.clone(), Some(Arc::new(keys)));
    extenddb_storage::key_conformance::run(
        &store,
        &store,
        extenddb_storage::key_conformance::Expectations {
            write_timestamps: false,
            data_connection: true,
        },
    )
    .await;
    catalog.close().await;
    drop(s);
}
