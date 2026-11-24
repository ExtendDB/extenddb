// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the check of an S3 store's key against the keys the server
//! accepts from its clients.

use async_trait::async_trait;
use extenddb_auth::StoredCredential;
use extenddb_core::error::DynamoDbError;
use extenddb_storage::server_components::ServerComponentsOptions;

use super::*;
use crate::test_logs::logged;

/// How the store of these tests names itself.
const LABEL: &str = "S3 test store";

/// What the check warns when the store's key is one the server accepts
/// from its clients.
const CLIENT_KEY: &str = "The S3 test store credentials are an access key this server also \
                          accepts from its clients. S3 refuses a key ExtendDB created, and \
                          accepts an imported AWS key as that key's identity. Start the server \
                          without the clients' AWS variables or profile";

const ACCOUNT: &str = "111111111111";

/// An imported key, as an operator imports a client's AWS key.
const IMPORTED: &str = "AKIAIMPORTEDTEST0001";

/// Which key the S3 store's credentials are in one case.
#[derive(Clone, Copy, Debug)]
enum StoreKey {
    /// A key the server created for one of its users.
    Created,
    /// A key imported into the server for one of its users.
    Imported,
    /// The dev-mode example pair, which this credential store does not hold.
    DevPair,
    /// A key the server does not know.
    Unknown,
}

/// The check's log for the S3 store signing with `key`, over a credential
/// store that holds a created key and an imported one. The log never
/// carries the key.
fn client_key_log(key: StoreKey, dev_mode: bool) -> String {
    let (signing, log) = logged(async move {
        let config = extenddb_storage_sqlite::SqliteConfig {
            path: ":memory:".to_owned(),
            pool_size: 2,
        };
        let components = (extenddb_storage_sqlite::backend().server_components)(
            &config,
            "eu-west-3",
            ServerComponentsOptions::default(),
        )
        .await
        .expect("server components");
        let catalog = components.catalog_store;
        catalog.create_account(ACCOUNT, ACCOUNT).await.unwrap();
        catalog.create_user(ACCOUNT, "loader", None).await.unwrap();
        let created = catalog
            .create_access_key(ACCOUNT, "loader")
            .await
            .unwrap()
            .access_key_id;
        catalog
            .import_access_key(ACCOUNT, "loader", IMPORTED, "imported-test-secret")
            .await
            .unwrap();
        let signing = match key {
            StoreKey::Created => created,
            StoreKey::Imported => IMPORTED.to_owned(),
            StoreKey::DevPair => crate::dev_credentials::EXAMPLE_ACCESS_KEY_ID.to_owned(),
            StoreKey::Unknown => "AKIAUNKNOWNTEST00001".to_owned(),
        };
        warn_of_a_client_key(
            LABEL,
            &signing,
            components.credential_store.as_ref(),
            dev_mode,
        )
        .await;
        signing
    });
    assert!(!log.contains(&signing), "{key:?}: {log}");
    log
}

#[test]
fn an_s3_store_key_the_server_accepts_from_its_clients_is_a_warning() {
    for (key, dev_mode) in [
        (StoreKey::Created, false),
        (StoreKey::Imported, false),
        (StoreKey::Created, true),
        (StoreKey::DevPair, true),
    ] {
        let log = client_key_log(key, dev_mode);
        assert_eq!(
            log.matches(CLIENT_KEY).count(),
            1,
            "{key:?} dev mode {dev_mode}: {log}"
        );
    }
}

#[test]
fn an_s3_store_key_the_server_does_not_accept_is_no_warning() {
    for (key, dev_mode) in [
        (StoreKey::Unknown, false),
        (StoreKey::Unknown, true),
        (StoreKey::DevPair, false),
    ] {
        let log = client_key_log(key, dev_mode);
        assert!(!log.contains(LABEL), "{key:?} dev mode {dev_mode}: {log}");
    }
}

/// A credential store whose lookup fails and logs the key it was asked
/// for, as every backend does on a database or decryption error.
struct FailingStore;

#[async_trait]
impl CredentialStore for FailingStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        tracing::error!("Failed to look up credential {access_key_id}: database is locked");
        Err(DynamoDbError::InternalServerError(
            "database is locked".to_owned(),
        ))
    }
}

#[test]
fn a_lookup_that_fails_logs_nothing_and_is_no_warning() {
    let ((), log) = logged(warn_of_a_client_key(LABEL, IMPORTED, &FailingStore, false));
    assert!(!log.contains(IMPORTED), "{log}");
    assert!(!log.contains(LABEL), "{log}");
}
