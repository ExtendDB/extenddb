// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stored-secret documents as the encryption-key commands read and write
//! them (RFC-0000 section 8.4), and the credential lookups through the key
//! holder, against a live replica set named by `EXTENDDB_TEST_MONGODB_URI`.

mod migrations_support;

use std::sync::Arc;

use bson::Document;
use extenddb_auth::CredentialStore;
use extenddb_storage::management_store::{
    ManagementStore, SecretWrite, SettingsStore, StoredSecretRow,
};
use extenddb_storage::migrations::CatalogMigrations;
use extenddb_storage::secret_keys::{SecretKeys, SecretRow, SecretTable};
use extenddb_storage_mongodb::migrations::{
    MongoCatalogMigrations, Timing, held_lock_for_tests, lock,
};
use extenddb_storage_mongodb::{MongoCatalogStore, MongoCredentialStore};
use migrations_support::{Deployment, server};

const ACCOUNT: &str = "123456789012";
const PAGE: usize = 1000;

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

/// The encryption-key command and startup cases of RFC-0000 section 15, on
/// a catalog as `init` leaves it: a catalog key, a catalog id and no ring,
/// behind a store that holds the catalog key as the server does.
#[tokio::test]
async fn the_encryption_key_commands_run_on_a_mongodb_catalog() {
    let Some(uri) = server() else {
        return;
    };
    let deployment = Deployment::new(&uri, "keycmds").await;
    deployment.create().await;
    let settings =
        MongoCatalogStore::on_database_for_tests(deployment.client.clone(), deployment.catalog());
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
        extenddb_storage::key_conformance::Expectations::default(),
    )
    .await;
    deployment.drop_databases().await;
}

#[tokio::test]
async fn the_pass_reads_rewrites_and_deletes_stored_secrets() {
    let Some(uri) = server() else {
        return;
    };
    let deployment = Deployment::new(&uri, "secrets").await;
    deployment.create().await;
    let key = extenddb_storage::bootstrapper::helpers::generate_encryption_key().unwrap();
    let catalog_key = Arc::new(SecretKeys::catalog_key(&key).unwrap());
    let dek = Arc::new(SecretKeys::dek_for_tests(
        [7; 32],
        1,
        "catalog-id-for-tests",
    ));
    let store =
        MongoCatalogStore::on_database_for_tests(deployment.client.clone(), deployment.catalog())
            .with_secret_keys(Some(Arc::clone(&catalog_key)));
    store.create_account(ACCOUNT, "acct").await.unwrap();
    store.create_user(ACCOUNT, "alice", None).await.unwrap();
    let trust = serde_json::json!({"Version": "2012-10-17", "Statement": []});
    store.create_role(ACCOUNT, "app", &trust).await.unwrap();

    // Three access keys and one session, written as the server writes them:
    // the store encrypts the access keys, AssumeRole the session, with its
    // expiry in whole seconds.
    let mut created = Vec::new();
    for _ in 0..3 {
        created.push(store.create_access_key(ACCOUNT, "alice").await.unwrap());
    }
    let session_key_id = "ASIAEXTENDDBSESSION1";
    let expires_at = (time::OffsetDateTime::now_utc() + time::Duration::hours(1))
        .replace_nanosecond(0)
        .unwrap();
    let session_secret = "session-secret-for-the-test";
    let session_row = SecretRow::session(
        session_key_id,
        ACCOUNT,
        "app",
        "run",
        expires_at.unix_timestamp(),
    );
    store
        .store_session(
            "token-for-the-test",
            session_key_id,
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

    // Read: every document with its AAD fields and a ciphertext the catalog
    // key opens, in pages of one; the key is the document's _id.
    let keys = all_rows(&store, SecretTable::AccessKeys, 1).await;
    assert_eq!(keys.len(), 3, "{keys:?}");
    for row in &keys {
        assert_eq!(row.key.len(), 24, "{row:?}");
        assert_eq!(
            (row.account_id.as_str(), row.principal_name.as_str()),
            (ACCOUNT, "alice")
        );
        assert_eq!((row.session_name.as_deref(), row.expires_at), (None, None));
        assert!(row.created_at.is_some() && row.write_timestamp.is_none());
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
    assert_eq!(session.aad_row(), session_row);
    assert_eq!(
        *catalog_key
            .decrypt(&session.aad_row(), &session.secret_key_encrypted)
            .unwrap(),
        session_secret
    );

    // Write: each secret under the DEK, by _id. The DEK opens the documents
    // read again, and the catalog key no longer does. A document that is
    // gone is not written back.
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
    let gone = SecretWrite {
        key: bson::oid::ObjectId::new().to_hex(),
        ..writes[0].clone()
    };
    store
        .write_secrets(SecretTable::AccessKeys, &[gone])
        .await
        .unwrap();
    assert_eq!(
        all_rows(&store, SecretTable::AccessKeys, PAGE).await.len(),
        3
    );

    // The credential store shares the DEK and opens both documents; one
    // built with no key opens neither, and a store with no key writes none.
    let credentials =
        MongoCredentialStore::on_database_for_tests(deployment.catalog(), Some(Arc::clone(&dek)));
    let found = credentials
        .lookup_credential(&created[0].access_key_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.secret_key, created[0].secret_access_key);
    let found = credentials
        .lookup_credential(session_key_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (found.secret_key.as_str(), found.expires_at),
        (session_secret, Some(expires_at))
    );
    let keyless = MongoCredentialStore::on_database_for_tests(deployment.catalog(), None);
    assert!(
        keyless
            .lookup_credential(&created[0].access_key_id)
            .await
            .is_err()
    );
    let keyless_store =
        MongoCatalogStore::on_database_for_tests(deployment.client.clone(), deployment.catalog());
    assert!(
        keyless_store
            .create_access_key(ACCOUNT, "alice")
            .await
            .is_err()
    );

    // Delete: the documents reset removes, and a settings document.
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
    let settings: Vec<Document> = Deployment::documents(&deployment.catalog(), "settings").await;
    assert!(
        settings
            .iter()
            .all(|d| d.get_str("_id") != Ok("row_to_delete")),
        "{settings:?}"
    );
    deployment.drop_databases().await;
}

/// The stores that `CatalogMigrations::locked_stores` opens for the
/// encryption-key commands work while the lock is held, and refuse once the
/// lock is released and its lease has run out.
#[tokio::test]
async fn the_stores_under_the_migration_lock_check_its_lease() {
    let Some(uri) = server() else {
        return;
    };
    let deployment = Deployment::new(&uri, "locked").await;
    deployment.create().await;
    let timing = Timing {
        lease: std::time::Duration::from_secs(3),
        renew_every: std::time::Duration::from_secs(1),
        margin: std::time::Duration::from_secs(1),
        idle_limit: std::time::Duration::from_secs(2),
        ..Timing::PRODUCTION
    };
    let (held, target) = lock(
        &deployment.target,
        false,
        std::time::Duration::from_secs(10),
        timing,
    )
    .await
    .expect("lock");
    let held = held_lock_for_tests(held, target);
    let stores = MongoCatalogMigrations
        .locked_stores(&held, None)
        .await
        .expect("the stores under the lock");
    stores
        .settings()
        .set_setting("row_under_lock", "value")
        .await
        .unwrap();
    assert_eq!(
        stores
            .settings()
            .get_setting("row_under_lock")
            .await
            .unwrap()
            .as_deref(),
        Some("value")
    );
    assert!(
        stores
            .settings()
            .replace_setting("row_under_lock", "value", "replaced")
            .await
            .unwrap()
    );
    let page = stores
        .management()
        .read_secret_page(SecretTable::AccessKeys, None, PAGE)
        .await
        .unwrap();
    assert!(page.rows.is_empty() && page.next.is_none());
    stores
        .management()
        .delete_setting("row_under_lock")
        .await
        .unwrap();

    MongoCatalogMigrations.release(held).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(stores.settings().get_setting("catalog_id").await.is_err());
    drop(stores);
    deployment.drop_databases().await;
}
