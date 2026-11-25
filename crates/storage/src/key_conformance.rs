// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The encryption-key command and startup cases of RFC-0000 section 15 that a
//! store can show, as one check that each backend's tests run with their own
//! store, as [`crate::metrics_conformance`] does.
//!
//! The check needs a fresh catalog that holds its catalog key (after `init`)
//! and no key ring, behind a store that holds that key. It first checks the
//! three answers of [`SettingsStore::replace_setting`] on a row of its own.
//! It then seeds an account, a user with two access keys and a role with two
//! sessions, one of them expired, then puts one access key's ciphertext under
//! no key with
//! [`ManagementStore::write_secrets`]. It then runs, in order: the preview and
//! the refused and the kept run of `switch`, a second `switch`, the loader
//! with and without the KEK, a catalog key written beside the finished ring
//! with and without `--accept-catalog-key`, a hand-edited ring, `rotate`,
//! `rewrap`, and `reset` for the KEK that `rewrap` replaced. After each step
//! it reads the rows back through [`ManagementStore::read_secret_page`].

use time::OffsetDateTime;
use zeroize::Zeroizing;

use crate::key_commands::{
    KekFile, KeyCommandError, KeyStores, Outcome, RunFlags, reset, rewrap, rotate, switch,
};
use crate::key_ring::{KEY_RING_SETTING, KeyRefusal, kek_id, open_ring};
use crate::management_store::{ManagementStore, SecretWrite, SettingsStore, StoredSecretRow};
use crate::secret_keys::{
    CATALOG_ID_SETTING, CATALOG_KEY_SETTING, DATA_CONNECTION_SETTING, SecretKeys, SecretRow,
    SecretTable,
};
use crate::server_components::{KeySource, load_secret_keys};

/// What the backend under test does beyond the common contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Expectations {
    /// The store reports a write timestamp
    /// ([`crate::management_store::StoredSecretRow::write_timestamp`]), which
    /// Cassandra does.
    pub write_timestamps: bool,
    /// The catalog holds a `data_database_connection_string` row in
    /// plaintext, which PostgreSQL does.
    pub data_connection: bool,
}

const ROLE: &str = "ConformanceRole";
const USER: &str = "conformance-user";
const SESSION: &str = "conformance-session";

/// A KEK file in memory.
struct MemoryKek(&'static str, [u8; 32]);

impl KekFile for MemoryKek {
    fn path(&self) -> &str {
        self.0
    }

    fn read(&self) -> Result<Zeroizing<[u8; 32]>, String> {
        Ok(Zeroizing::new(self.1))
    }
}

/// Every row of `table`, through the paged read.
async fn all_rows(store: &dyn ManagementStore, table: SecretTable) -> Vec<StoredSecretRow> {
    let mut rows = Vec::new();
    crate::key_pass::for_each_page(store, table, |page| {
        rows.extend(page);
        Ok(Vec::new())
    })
    .await
    .unwrap_or_else(|e| panic!("read_secret_page({table:?}): {e}"));
    rows
}

/// The one row of `table` with `access_key_id`.
async fn row_of(
    store: &dyn ManagementStore,
    table: SecretTable,
    access_key_id: &str,
) -> Option<StoredSecretRow> {
    all_rows(store, table)
        .await
        .into_iter()
        .find(|row| row.access_key_id == access_key_id)
}

/// The key of each of `ids` among `keys`, by index, or `None`.
async fn opener(
    store: &dyn ManagementStore,
    table: SecretTable,
    access_key_id: &str,
    keys: &[&SecretKeys],
) -> Option<usize> {
    let row = row_of(store, table, access_key_id).await.expect("the row");
    keys.iter().position(|key| {
        key.decrypt(&row.aad_row(), &row.secret_key_encrypted)
            .is_ok()
    })
}

/// The active DEK of the catalog's ring under `kek`.
async fn ring_dek(settings: &dyn SettingsStore, kek: &[u8; 32]) -> SecretKeys {
    let text = settings
        .get_setting(KEY_RING_SETTING)
        .await
        .expect("read the ring")
        .expect("a ring");
    let catalog_id = settings
        .get_setting(CATALOG_ID_SETTING)
        .await
        .expect("read the catalog id");
    let mut open = open_ring(&text, catalog_id.as_deref(), kek).expect("the ring opens");
    open.deks.pop().expect("a DEK")
}

fn done(step: &str, outcome: Result<Outcome, KeyCommandError>) -> crate::key_commands::Summary {
    match outcome {
        Ok(Outcome::Done(summary)) => summary,
        other => panic!("{step}: expected the command to finish, got {other:?}"),
    }
}

fn refused(step: &str, outcome: Result<Outcome, KeyCommandError>, part: &str) {
    match outcome {
        Err(e) if e.to_string().contains(part) => {}
        other => panic!("{step}: expected a refusal naming {part:?}, got {other:?}"),
    }
}

/// Runs the check against a fresh catalog behind `management` and `settings`,
/// which hold no `encryption_key_ring` row. `management` holds the catalog
/// key, so that its `create_access_key` writes.
///
/// # Panics
///
/// When a case's result differs from the expected one, or a store call fails.
pub async fn run(
    management: &dyn ManagementStore,
    settings: &dyn SettingsStore,
    expect: Expectations,
) {
    let stores = KeyStores {
        management,
        settings,
    };
    let kek = MemoryKek("/etc/extenddb/kek", [0x4b; 32]);
    let new_kek = MemoryKek("/etc/extenddb/kek.new", [0x6f; 32]);
    let yes = RunFlags {
        yes: true,
        ..RunFlags::default()
    };

    // ── replace_setting, on a row that no command reads ───────────────
    const ROW: &str = "conformance_replace_setting";
    for (held, expected, replaced, after) in [
        (None, "a", false, None),
        (Some("a"), "b", false, Some("a")),
        (Some("a"), "a", true, Some("c")),
    ] {
        if let Some(held) = held {
            settings.set_setting(ROW, held).await.expect("set_setting");
        }
        let answer = settings
            .replace_setting(ROW, expected, "c")
            .await
            .expect("replace_setting");
        let read = settings.get_setting(ROW).await.expect("get_setting");
        assert_eq!(
            (answer, read.as_deref()),
            (replaced, after),
            "replace_setting of {held:?}, expecting {expected:?}"
        );
    }
    management
        .delete_setting(ROW)
        .await
        .expect("delete_setting");

    // ── Seed ───────────────────────────────────────────────────────
    let catalog_key = SecretKeys::catalog_key(
        &settings
            .get_setting(CATALOG_KEY_SETTING)
            .await
            .expect("read the catalog key")
            .expect("a fresh catalog holds its catalog key"),
    )
    .expect("a valid catalog key");
    let account = format!(
        "{:012}",
        OffsetDateTime::now_utc().unix_timestamp_nanos() % 1_000_000_000_000
    );
    management
        .create_account(&account, "conformance")
        .await
        .expect("create_account");
    management
        .create_user(&account, USER, None)
        .await
        .expect("create_user");
    management
        .create_role(
            &account,
            ROLE,
            &serde_json::json!({"Version": "2012-10-17", "Statement": []}),
        )
        .await
        .expect("create_role");
    let kept = management
        .create_access_key(&account, USER)
        .await
        .expect("create_access_key")
        .access_key_id;
    let unopenable = management
        .create_access_key(&account, USER)
        .await
        .expect("create_access_key")
        .access_key_id;
    let expires = |seconds: i64| OffsetDateTime::from_unix_timestamp(seconds).expect("a time");
    let live_until = OffsetDateTime::now_utc().unix_timestamp() + 3600;
    let sessions = [
        ("ASIACONFORMANCE0001", live_until),
        ("ASIACONFORMANCE0002", 1_000_000_000),
    ];
    for (n, (access_key_id, expires_at)) in sessions.iter().enumerate() {
        let row = SecretRow::session(access_key_id, &account, ROLE, SESSION, *expires_at);
        management
            .store_session(
                &format!("conformance-token-{account}-{n}"),
                access_key_id,
                &catalog_key
                    .encrypt(&row, "conformance-secret")
                    .expect("encrypt"),
                &account,
                ROLE,
                SESSION,
                &None,
                &None,
                expires(*expires_at),
            )
            .await
            .expect("store_session");
    }

    // The paged read gives back each AAD field as written.
    let session = row_of(management, SecretTable::IamSessions, sessions[0].0)
        .await
        .expect("the session row");
    assert_eq!(
        (
            session.account_id.as_str(),
            session.principal_name.as_str(),
            session.session_name.as_deref(),
            session.expires_at,
        ),
        (account.as_str(), ROLE, Some(SESSION), Some(live_until)),
        "the session row's AAD fields"
    );
    assert_eq!(session.write_timestamp.is_some(), expect.write_timestamps);
    let key_row = row_of(management, SecretTable::AccessKeys, &kept)
        .await
        .expect("the access key row");
    assert_eq!(
        (
            key_row.account_id.as_str(),
            key_row.principal_name.as_str(),
            key_row.session_name.as_deref(),
            key_row.expires_at,
        ),
        (account.as_str(), USER, None, None),
        "the access key row's AAD fields"
    );

    // One access key's secret under no key: a key the run does not hold.
    let lost = row_of(management, SecretTable::AccessKeys, &unopenable)
        .await
        .expect("the row");
    let nobody = SecretKeys::dek_for_tests([0xee; 32], 9, "nobody");
    management
        .write_secrets(
            SecretTable::AccessKeys,
            &[SecretWrite {
                key: lost.key.clone(),
                secret_key_encrypted: nobody.encrypt(&lost.aad_row(), "lost").expect("encrypt"),
                write_timestamp: lost.write_timestamp,
            }],
        )
        .await
        .expect("write_secrets");
    assert_eq!(
        opener(management, SecretTable::AccessKeys, &unopenable, &[&nobody]).await,
        Some(0),
        "write_secrets wrote the new ciphertext"
    );

    // ── switch ─────────────────────────────────────────────────────
    match switch(stores, &kek, RunFlags::default()).await {
        Ok(Outcome::Preview(_)) => {}
        other => panic!("switch preview: {other:?}"),
    }
    refused(
        "switch with a secret under no key",
        switch(stores, &kek, yes).await,
        &unopenable,
    );
    assert_eq!(
        settings.get_setting(KEY_RING_SETTING).await.expect("read"),
        None,
        "a refused switch writes nothing"
    );
    let summary = done(
        "switch --keep-unopenable",
        switch(
            stores,
            &kek,
            RunFlags {
                keep_unopenable: true,
                ..yes
            },
        )
        .await,
    );
    assert_eq!(
        summary
            .unopenable
            .iter()
            .map(|s| s.access_key_id.as_str())
            .collect::<Vec<_>>(),
        [unopenable.as_str()]
    );
    assert_eq!(
        settings
            .get_setting(CATALOG_KEY_SETTING)
            .await
            .expect("read"),
        None,
        "delete_setting deleted the catalog key"
    );
    let dek = ring_dek(settings, &kek.1).await;
    for (table, id) in [
        (SecretTable::AccessKeys, kept.as_str()),
        (SecretTable::IamSessions, sessions[0].0),
        (SecretTable::IamSessions, sessions[1].0),
    ] {
        assert_eq!(
            opener(management, table, id, &[&dek]).await,
            Some(0),
            "{id} is under the DEK after the switch"
        );
    }
    if expect.data_connection {
        let row = settings
            .get_setting(DATA_CONNECTION_SETTING)
            .await
            .expect("read")
            .expect("the connection string row");
        assert!(
            dek.decrypt_setting(&row).is_ok(),
            "the connection string row is under the DEK"
        );
        assert_eq!(summary.data_connection_rewritten, Some(true));
    }
    match switch(stores, &kek, yes).await {
        Ok(Outcome::NothingToDo { kek_id: id, .. }) => assert_eq!(id, kek_id(&kek.1)),
        other => panic!("a second switch: {other:?}"),
    }

    // ── The loader ─────────────────────────────────────────────────
    let loaded = load_secret_keys(settings, KeySource::Kek(Zeroizing::new(kek.1).into()))
        .await
        .expect("the server starts with the KEK")
        .expect("a key");
    assert_eq!(loaded.dek_id(), dek.dek_id());
    let without_kek = load_secret_keys(settings, KeySource::CatalogKey)
        .await
        .expect_err("refused without key_file");
    assert!(
        without_kek.to_string().contains(
            &KeyRefusal::RingNeedsKeyFile {
                kek_id: kek_id(&kek.1)
            }
            .to_string()
        )
    );

    // ── A catalog key beside the finished ring (8.5) ───────────────
    let forged_key = "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUE=";
    settings
        .set_setting(CATALOG_KEY_SETTING, forged_key)
        .await
        .expect("set_setting");
    let forged = SecretKeys::catalog_key(forged_key).expect("a key");
    let row = row_of(management, SecretTable::AccessKeys, &kept)
        .await
        .expect("the row");
    management
        .write_secrets(
            SecretTable::AccessKeys,
            &[SecretWrite {
                key: row.key.clone(),
                secret_key_encrypted: forged.encrypt(&row.aad_row(), "forged").expect("encrypt"),
                write_timestamp: row.write_timestamp,
            }],
        )
        .await
        .expect("write_secrets");
    let start = load_secret_keys(settings, KeySource::Kek(Zeroizing::new(kek.1).into()))
        .await
        .expect_err("refused beside a catalog key");
    assert!(
        start
            .to_string()
            .contains(&KeyRefusal::CatalogKeyAfterSwitch.to_string())
    );
    refused(
        "switch beside a catalog key",
        switch(stores, &kek, yes).await,
        "--accept-catalog-key",
    );
    done(
        "switch --accept-catalog-key",
        switch(
            stores,
            &kek,
            RunFlags {
                accept_catalog_key: true,
                keep_unopenable: true,
                ..yes
            },
        )
        .await,
    );
    assert_eq!(
        opener(management, SecretTable::AccessKeys, &kept, &[&dek]).await,
        Some(0)
    );
    assert_eq!(
        settings
            .get_setting(CATALOG_KEY_SETTING)
            .await
            .expect("read"),
        None
    );

    // ── A hand-edited ring ─────────────────────────────────────────
    let ring = settings
        .get_setting(KEY_RING_SETTING)
        .await
        .expect("read")
        .expect("a ring");
    let flipped = ring.replace(
        "\"switch_from_catalog_key\":false",
        "\"switch_from_catalog_key\":true",
    );
    assert_ne!(flipped, ring);
    settings
        .set_setting(KEY_RING_SETTING, &flipped)
        .await
        .expect("set_setting");
    let start = load_secret_keys(settings, KeySource::Kek(Zeroizing::new(kek.1).into()))
        .await
        .expect_err("refused on a changed ring");
    assert!(start.to_string().contains("has damage or a changed field"));
    settings
        .set_setting(KEY_RING_SETTING, &ring)
        .await
        .expect("set_setting");

    // ── rotate, rewrap, reset ──────────────────────────────────────
    let summary = done(
        "rotate",
        rotate(
            stores,
            &kek,
            RunFlags {
                keep_unopenable: true,
                ..yes
            },
        )
        .await,
    );
    assert_eq!(summary.dek_id, 2);
    let rotated = ring_dek(settings, &kek.1).await;
    for (table, id) in [
        (SecretTable::AccessKeys, kept.as_str()),
        (SecretTable::IamSessions, sessions[1].0),
    ] {
        assert_eq!(
            opener(management, table, id, &[&rotated]).await,
            Some(0),
            "{id} is under the new DEK"
        );
    }
    done("rewrap", rewrap(stores, &kek, &new_kek, true).await);
    assert_eq!(ring_dek(settings, &new_kek.1).await.dek_id(), Some(2));
    // The KEK that rewrap replaced is a lost KEK for this ring.
    let summary = done(
        "reset",
        reset(
            stores,
            &kek,
            yes,
            true,
            expect
                .data_connection
                .then_some("postgres://extenddb:pw@localhost:5432/extenddb_data"),
        )
        .await,
    );
    assert_eq!(summary.dek_id, 3);
    for table in SecretTable::ALL {
        let left: Vec<String> = all_rows(management, table)
            .await
            .into_iter()
            .map(|row| row.access_key_id)
            .filter(|id| *id == kept || *id == unopenable || id.starts_with("ASIACONFORMANCE"))
            .collect();
        assert!(left.is_empty(), "reset left {left:?} in {table:?}");
    }
    assert_eq!(ring_dek(settings, &kek.1).await.dek_id(), Some(3));
}
