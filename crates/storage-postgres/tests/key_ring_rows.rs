// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The catalog rows of a deployment that uses a KEK file: the
//! `data_database_connection_string` row under the key ring (RFC-0000
//! section 12), and the store methods the encryption-key commands rewrite
//! the stored secrets with (section 8.4). See `migrations_support` for the
//! database these tests need.

mod migrations_support;
#[path = "key_ring_rows/pass_methods.rs"]
mod pass_methods;

use std::sync::Arc;

use extenddb_storage::diagnostics::{DiagError, DiagnosticsStore};
use extenddb_storage::key_ring::{KEY_RING_SETTING, KeyRing, catalog_key_id};
use extenddb_storage::management_store::{
    ManagementStore, SecretWrite, SettingsStore, StoredSecretRow,
};
use extenddb_storage::migrations::{CatalogStatus, MigrationError, MigrationId};
use extenddb_storage::secret_keys::{SecretKeys, SecretRow, SecretTable};
use extenddb_storage::server_components::KeySource;
use extenddb_storage_postgres::PostgresCatalogStore;
use extenddb_storage_postgres::migrations::{catalog_status, data_connection_string};
use migrations_support::{ACCOUNT, LATEST, Scratch, base_conn, skip};
use sqlx::PgPool;
use zeroize::Zeroizing;

const KEK: [u8; 32] = [3; 32];
const OTHER_KEK: [u8; 32] = [4; 32];
const CATALOG_KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

async fn installed(base: &str, prefix: &str) -> Scratch {
    let s = Scratch::new(base, prefix).await;
    s.migrate_through(MigrationId::LATEST).await;
    s
}

async fn set_row(catalog: &PgPool, key: &str, value: Option<&str>) {
    match value {
        Some(value) => sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .bind(value)
        .execute(catalog)
        .await
        .expect("write a settings row"),
        None => sqlx::query("DELETE FROM settings WHERE key = $1")
            .bind(key)
            .execute(catalog)
            .await
            .expect("delete a settings row"),
    };
}

/// The key a case's target carries.
#[derive(Clone, Copy, Debug)]
enum Key {
    None,
    Kek,
    KekDuringSwitch,
    OtherKek,
    Loaded,
}

/// The ring a case's catalog holds.
#[derive(Clone, Copy, Debug)]
enum Ring {
    Absent,
    Switching,
    Finished,
}

/// The `data_database_connection_string` row of a case.
#[derive(Clone, Copy, Debug)]
enum Row {
    Plaintext,
    Encrypted,
    NotAUrl,
    Absent,
}

/// Every reader of the row (the status read, the registration, `migrate`)
/// opens it the same way: plaintext without a ring; with a ring, only under
/// a DEK of the ring, except the plaintext row of an unfinished switch,
/// which only the switch's own backend accepts.
#[tokio::test]
async fn the_data_connection_row_opens_by_the_ring_and_the_target_key() {
    let Some(base) = base_conn() else {
        return skip("data connection row under the key ring");
    };
    let s = installed(&base, "pgk_dataconn").await;
    let catalog = s.catalog().await;
    let catalog_id: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_id'")
            .fetch_one(&catalog)
            .await
            .expect("migration 6 writes the catalog id");
    let dek = Arc::new(SecretKeys::dek_for_tests([5; 32], 1, &catalog_id));
    let catalog_key = SecretKeys::catalog_key(CATALOG_KEY).expect("a catalog key");
    let ring = |switching: bool| {
        let retiring = switching.then(|| catalog_key_id(&KEK, &catalog_key));
        KeyRing::seal(&KEK, &catalog_id, &[&dek], switching, retiring)
            .expect("seal the ring")
            .to_json()
    };
    let url = s.data_url();
    let encrypted = dek.encrypt_setting(&url).expect("encrypt the row");

    let (opens, refused) = (true, false);
    let cases: &[(&str, Ring, Row, Key, bool)] = &[
        (
            "no ring, plaintext",
            Ring::Absent,
            Row::Plaintext,
            Key::None,
            opens,
        ),
        (
            "switching, plaintext, the switch",
            Ring::Switching,
            Row::Plaintext,
            Key::KekDuringSwitch,
            opens,
        ),
        (
            "switching, plaintext, another command",
            Ring::Switching,
            Row::Plaintext,
            Key::Kek,
            refused,
        ),
        (
            "switching, plaintext, no key",
            Ring::Switching,
            Row::Plaintext,
            Key::None,
            refused,
        ),
        (
            "switching, encrypted, the switch",
            Ring::Switching,
            Row::Encrypted,
            Key::KekDuringSwitch,
            opens,
        ),
        (
            "finished, encrypted, a command",
            Ring::Finished,
            Row::Encrypted,
            Key::Kek,
            opens,
        ),
        (
            "finished, encrypted, the server",
            Ring::Finished,
            Row::Encrypted,
            Key::Loaded,
            opens,
        ),
        (
            "finished, encrypted, no key",
            Ring::Finished,
            Row::Encrypted,
            Key::None,
            refused,
        ),
        (
            "finished, encrypted, another KEK",
            Ring::Finished,
            Row::Encrypted,
            Key::OtherKek,
            refused,
        ),
        (
            "finished, plaintext, the switch",
            Ring::Finished,
            Row::Plaintext,
            Key::KekDuringSwitch,
            refused,
        ),
        (
            "finished, plaintext, the server",
            Ring::Finished,
            Row::Plaintext,
            Key::Loaded,
            refused,
        ),
        (
            "finished, not a URL",
            Ring::Finished,
            Row::NotAUrl,
            Key::Kek,
            refused,
        ),
        (
            "finished, no row",
            Ring::Finished,
            Row::Absent,
            Key::Kek,
            refused,
        ),
    ];
    for (name, ring_state, row, key, expected) in cases {
        let ring_text = match ring_state {
            Ring::Absent => None,
            Ring::Switching => Some(ring(true)),
            Ring::Finished => Some(ring(false)),
        };
        set_row(&catalog, KEY_RING_SETTING, ring_text.as_deref()).await;
        let row_text = match row {
            Row::Plaintext => Some(url.as_str()),
            Row::Encrypted => Some(encrypted.as_str()),
            Row::NotAUrl => Some("not a connection string"),
            Row::Absent => None,
        };
        set_row(&catalog, "data_database_connection_string", row_text).await;
        let target = s.target();
        let target = match key {
            Key::None => target,
            Key::Kek => target.with_key_source(KeySource::Kek(Zeroizing::new(KEK).into()), false),
            Key::KekDuringSwitch => {
                target.with_key_source(KeySource::Kek(Zeroizing::new(KEK).into()), true)
            }
            Key::OtherKek => {
                target.with_key_source(KeySource::Kek(Zeroizing::new(OTHER_KEK).into()), false)
            }
            Key::Loaded => target.with_secret_keys(Arc::clone(&dek)),
        };
        let status = catalog_status(&target).await;
        if *expected {
            assert_eq!(
                status.as_ref().ok(),
                Some(&CatalogStatus::Current { version: LATEST }),
                "{name}: {:?}",
                status.as_ref().err()
            );
            let opened = data_connection_string(&target)
                .await
                .expect("the row opens")
                .expect("the row exists");
            assert!(
                *opened == url,
                "{name}: the opened row is not the stored string"
            );
        } else {
            let Err(MigrationError::Refused(message)) = &status else {
                panic!("{name}: not refused: {:?}", status.as_ref().ok());
            };
            assert!(
                !message.contains(&url),
                "{name}: the refusal prints the row"
            );
            if matches!(key, Key::OtherKek) {
                assert!(message.contains("KEK"), "{name}: {message}");
            }
        }
    }
    catalog.close().await;
    drop(s);
}

/// `set-data-connection` writes the row under the ring's DEK, where the
/// readers open it, and refuses a string that does not parse, a catalog
/// without a ring, an unfinished switch and an unfinished rotation.
#[tokio::test]
async fn set_data_connection_writes_the_row_under_the_ring() {
    let Some(base) = base_conn() else {
        return skip("set-data-connection");
    };
    let s = installed(&base, "pgk_setconn").await;
    let catalog = s.catalog().await;
    let catalog_id: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_id'")
            .fetch_one(&catalog)
            .await
            .expect("migration 6 writes the catalog id");
    let first = SecretKeys::dek_for_tests([5; 32], 1, &catalog_id);
    let second = SecretKeys::dek_for_tests([6; 32], 2, &catalog_id);
    let catalog_key = SecretKeys::catalog_key(CATALOG_KEY).expect("a catalog key");
    let seal = |deks: &[&SecretKeys], switching: bool| {
        let retiring = switching.then(|| catalog_key_id(&KEK, &catalog_key));
        KeyRing::seal(&KEK, &catalog_id, deks, switching, retiring)
            .expect("seal the ring")
            .to_json()
    };
    let store = PostgresCatalogStore::new(catalog.clone());
    let url = s.data_url();
    let cases = [
        ("no ring", None, url.as_str(), false),
        (
            "an unfinished switch",
            Some(seal(&[&first], true)),
            url.as_str(),
            false,
        ),
        (
            "two DEKs",
            Some(seal(&[&first, &second], false)),
            url.as_str(),
            false,
        ),
        (
            "not a URL",
            Some(seal(&[&first], false)),
            "postgres://[bad",
            false,
        ),
        ("one DEK", Some(seal(&[&first], false)), url.as_str(), true),
    ];
    for (name, ring, value, writes) in &cases {
        set_row(&catalog, KEY_RING_SETTING, ring.as_deref()).await;
        set_row(
            &catalog,
            "data_database_connection_string",
            Some("unchanged"),
        )
        .await;
        let result = store.set_data_connection(&KEK, value).await;
        let row: String = sqlx::query_scalar(
            "SELECT value FROM settings WHERE key = 'data_database_connection_string'",
        )
        .fetch_one(&catalog)
        .await
        .expect("read the row");
        if *writes {
            result.unwrap_or_else(|e| panic!("{name}: {e:?}"));
            assert!(row != url, "{name}: the row is in plaintext");
            let target = s
                .target()
                .with_key_source(KeySource::Kek(Zeroizing::new(KEK).into()), false);
            let opened = data_connection_string(&target)
                .await
                .expect("the row opens")
                .expect("the row exists");
            assert!(
                *opened == url,
                "{name}: the opened row is not the written string"
            );
            // `verify`'s data database check opens the row as the status
            // read does: with the KEK of the ring, and not without it.
            let data_name = store
                .test_data_database_connection(&KeySource::Kek(Zeroizing::new(KEK).into()))
                .await
                .unwrap_or_else(|e| panic!("{name}: verify's check with the KEK: {e}"));
            assert!(
                url.ends_with(&format!("/{data_name}")),
                "{name}: {data_name}"
            );
            assert!(
                store
                    .test_data_database_connection(&KeySource::None)
                    .await
                    .is_err(),
                "{name}: verify's check opened the row without a key"
            );
            // `catalog-check` opens the row as `verify` does. Without the KEK
            // it gives the refusal alone, as for a catalog without the row.
            store
                .check_storage_relations(&KeySource::Kek(Zeroizing::new(KEK).into()))
                .await
                .unwrap_or_else(|e| panic!("{name}: catalog-check with the KEK: {e}"));
            match store.check_storage_relations(&KeySource::CatalogKey).await {
                Err(DiagError::NotConfigured(msg)) => assert_eq!(
                    msg,
                    "the catalog has a key ring, so its data_database_connection_string row is \
                     encrypted; run the command with the configuration whose [auth.encryption] \
                     key_file names the KEK of the ring",
                    "{name}"
                ),
                other => panic!("{name}: catalog-check without the KEK gave {other:?}"),
            }
        } else {
            assert!(result.is_err(), "{name}: not refused");
            assert_eq!(row, "unchanged", "{name}: the row changed");
        }
    }
    catalog.close().await;
    drop(s);
}

/// The server factory loads the key inside its registration: a switched
/// catalog (a key ring, the connection string row encrypted under its DEK,
/// no catalog key) starts with the KEK of the ring, with the DEK in the
/// stores; without key_file the start is refused.
#[tokio::test]
async fn a_server_starts_on_a_switched_catalog_with_its_kek() {
    let Some(base) = base_conn() else {
        return skip("server start on a switched catalog");
    };
    let s = installed(&base, "pgk_serve").await;
    let catalog = s.catalog().await;
    let catalog_id: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_id'")
            .fetch_one(&catalog)
            .await
            .expect("migration 6 writes the catalog id");
    let dek = SecretKeys::dek_for_tests([5; 32], 1, &catalog_id);
    let ring = KeyRing::seal(&KEK, &catalog_id, &[&dek], false, None).expect("seal the ring");
    set_row(&catalog, KEY_RING_SETTING, Some(&ring.to_json())).await;
    let encrypted = dek.encrypt_setting(&s.data_url()).expect("encrypt the row");
    set_row(
        &catalog,
        "data_database_connection_string",
        Some(&encrypted),
    )
    .await;
    catalog.close().await;

    let config = extenddb_storage_postgres::PostgresStorageConfig {
        connection_string: s.catalog_url(),
        pool_size: 10,
        catalog_pool_size: None,
        ..Default::default()
    };
    let start = |key_source: KeySource| {
        let mut options = extenddb_storage::server_components::ServerComponentsOptions::default();
        options.startup_recovery = false;
        options.key_source = key_source;
        (extenddb_storage_postgres::backend().server_components)(&config, "us-east-1", options)
    };
    let started = start(KeySource::Kek(Zeroizing::new(KEK).into()))
        .await
        .unwrap_or_else(|e| panic!("starts with the KEK: {e}"));
    let keys = started.catalog_store.secret_keys().expect("a key holder");
    assert_eq!(keys.dek_id(), Some(1));
    drop(started);
    match start(KeySource::CatalogKey).await {
        Err(extenddb_storage::server_components::BackendError::InitializationFailed(message)) => {
            assert!(
                message.contains("Set [auth.encryption] key_file"),
                "{message}"
            );
        }
        Err(other) => panic!("refused for another reason: {other}"),
        Ok(_) => panic!("started without key_file"),
    }
    drop(s);
}

/// The commands reach the data database through `Target`: its key opens
/// the connection string row (the plaintext row of an unfinished switch for
/// `switch` alone), and after a lost KEK (`reset`) the status read under the
/// lock and the stores of the lock use the connection string the command
/// read, which no key of the run opens.
#[tokio::test]
async fn the_commands_reach_the_data_database_through_the_target() {
    use std::time::Duration;

    use extenddb_storage::migrations::{CatalogMigrations, Target};
    use extenddb_storage_postgres::migrations::PostgresCatalogMigrations;

    let Some(base) = base_conn() else {
        return skip("the target's key and connection string");
    };
    let s = installed(&base, "pgk_target").await;
    let catalog = s.catalog().await;
    let catalog_id: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_id'")
            .fetch_one(&catalog)
            .await
            .expect("migration 6 writes the catalog id");
    let dek = SecretKeys::dek_for_tests([5; 32], 1, &catalog_id);
    let catalog_key = SecretKeys::catalog_key(CATALOG_KEY).expect("a catalog key");
    let migrations = PostgresCatalogMigrations;
    let at = |kek: [u8; 32], plaintext: bool, data_connection: Option<String>| Target {
        location: s.catalog_url(),
        key_source: KeySource::Kek(Zeroizing::new(kek).into()),
        plaintext_data_connection: plaintext,
        data_connection,
        ..Target::default()
    };
    let current = CatalogStatus::Current { version: LATEST };

    // An unfinished switch with the plaintext row: only the switch's target
    // reads it.
    let switching = KeyRing::seal(
        &KEK,
        &catalog_id,
        &[&dek],
        true,
        Some(catalog_key_id(&KEK, &catalog_key)),
    )
    .expect("seal the ring");
    set_row(&catalog, KEY_RING_SETTING, Some(&switching.to_json())).await;
    assert_eq!(
        migrations.status(&at(KEK, true, None)).await.ok(),
        Some(current.clone())
    );
    assert!(matches!(
        migrations.status(&at(KEK, false, None)).await,
        Err(MigrationError::Refused(_))
    ));

    // A finished ring with the row under its DEK, and a lost KEK.
    let finished = KeyRing::seal(&KEK, &catalog_id, &[&dek], false, None).expect("seal the ring");
    set_row(&catalog, KEY_RING_SETTING, Some(&finished.to_json())).await;
    let encrypted = dek.encrypt_setting(&s.data_url()).expect("encrypt the row");
    set_row(
        &catalog,
        "data_database_connection_string",
        Some(&encrypted),
    )
    .await;
    catalog.close().await;
    assert!(matches!(
        migrations.status(&at(OTHER_KEK, false, None)).await,
        Err(MigrationError::Refused(_))
    ));
    let given = at(OTHER_KEK, false, Some(s.data_url()));
    let lock = migrations
        .lock(&given, Duration::from_secs(5))
        .await
        .expect("the migration lock");
    assert_eq!(
        migrations.status_under_lock(&lock, &given).await.ok(),
        Some(current.clone())
    );
    let stores = migrations
        .locked_stores(&lock, Some(&s.data_url()))
        .await
        .expect("the stores of the lock");
    assert_eq!(
        stores
            .settings()
            .get_setting("catalog_id")
            .await
            .expect("read"),
        Some(catalog_id.clone())
    );
    drop(stores);
    migrations.release(lock).await;

    // With the KEK of the ring, the row opens and no string is needed.
    let keyed = at(KEK, false, None);
    let lock = migrations
        .lock(&keyed, Duration::from_secs(5))
        .await
        .expect("the migration lock");
    assert_eq!(
        migrations.status_under_lock(&lock, &keyed).await.ok(),
        Some(current)
    );
    let stores = migrations
        .locked_stores(&lock, None)
        .await
        .expect("the stores of the lock");
    assert!(
        stores
            .management()
            .read_secret_page(SecretTable::AccessKeys, None, 10)
            .await
            .expect("read the access keys")
            .rows
            .is_empty()
    );
    drop(stores);
    migrations.release(lock).await;
    drop(s);
}
