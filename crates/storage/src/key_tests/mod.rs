// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the AAD encoding, the key ring, the startup loader, the pass and
//! the encryption-key commands (RFC-0000 sections 4, 5, 7 and 8), over an
//! in-memory catalog.

mod commands;
mod loader;
mod pass;
mod ring;
mod status;
mod stops;

use std::collections::BTreeMap;
use std::sync::Mutex;

use futures::future::BoxFuture;
use serde_json::{Value, json};
use time::OffsetDateTime;
use zeroize::Zeroizing;

use crate::key_ring::{
    KEY_RING_SETTING, KeyId, KeyRefusal, KeyRing, LOST_KEK_STEPS, RESTORE_STEPS, catalog_key_id,
    kek_id, new_dek, open_ring,
};
use crate::management_store::{
    AccessKeyCreated, AccountDetail, GroupDetail, GroupListEntry, ManagementStore, OpError,
    OpResult, RoleDetail, RoleListEntry, SecretPage, SecretWrite, SettingsStore, StoredSecretRow,
    UserDetail, UserListEntry,
};
use crate::secret_keys::{
    AadKind, CATALOG_ID_SETTING, CATALOG_KEY_SETTING, SecretKeys, SecretRow, SecretTable,
    encode_aad,
};
use crate::server_components::{BackendError, KeySource, load_secret_keys};

pub(crate) const CATALOG_ID: &str = "0f3c2a1e-8b7d-4c6e-9f10-112233445566";
pub(crate) const CATALOG_KEY_B64: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
pub(crate) const KEK: [u8; 32] = [0x4b; 32];
pub(crate) const OTHER_KEK: [u8; 32] = [0x6f; 32];

// ── The in-memory catalog ──────────────────────────────────────────────

/// A catalog in memory: the settings rows and the rows of the two secret
/// tables, keyed by primary key.
#[derive(Default)]
pub(crate) struct FakeCatalog {
    pub(crate) settings: Mutex<BTreeMap<String, String>>,
    pub(crate) rows: Mutex<BTreeMap<(SecretTable, String), StoredSecretRow>>,
    /// Writes that report success and change nothing, as a store method that
    /// matches no row does.
    pub(crate) lossy_writes: Mutex<bool>,
    /// Deletes that report success and leave the row, as a Cassandra
    /// `DELETE` whose timestamp is below the row's own write time does.
    pub(crate) lossy_deletes: Mutex<bool>,
    /// For each `replace_setting` in order, the text that another writer
    /// puts in its row first, between a command's read and its write.
    pub(crate) written_before_replace: Mutex<std::collections::VecDeque<Option<String>>>,
    /// The `erase_free_space` calls, and whether they fail.
    pub(crate) erased: Mutex<usize>,
    pub(crate) erase_fails: Mutex<bool>,
}

impl FakeCatalog {
    pub(crate) fn with_settings(settings: &[(&str, &str)]) -> Self {
        let catalog = Self::default();
        for (key, value) in settings {
            catalog.set(key, value);
        }
        catalog
    }

    pub(crate) fn set(&self, key: &str, value: &str) {
        self.settings
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_owned());
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        self.settings.lock().unwrap().get(key).cloned()
    }

    /// Add a row whose stored secret is `stored`.
    pub(crate) fn add_stored(&self, row: &SecretRow<'_>, stored: Vec<u8>) {
        let key = format!("{}#{}", row.table.name(), row.access_key_id);
        self.rows.lock().unwrap().insert(
            (row.table, key.clone()),
            StoredSecretRow {
                table: row.table,
                key,
                access_key_id: row.access_key_id.to_owned(),
                account_id: row.account_id.to_owned(),
                principal_name: row.principal_name.to_owned(),
                session_name: row.session_name.map(str::to_owned),
                expires_at: row.expires_at,
                created_at: Some(OffsetDateTime::UNIX_EPOCH),
                secret_key_encrypted: stored,
                write_timestamp: None,
            },
        );
    }

    pub(crate) fn stored(&self, table: SecretTable, access_key_id: &str) -> Option<Vec<u8>> {
        let key = format!("{}#{access_key_id}", table.name());
        self.rows
            .lock()
            .unwrap()
            .get(&(table, key))
            .map(|r| r.secret_key_encrypted.clone())
    }
}

fn no_data<T: Send + 'static>() -> BoxFuture<'static, OpResult<T>> {
    Box::pin(async {
        Err(OpError::Internal(
            "the fake catalog holds no IAM data".to_owned(),
        ))
    })
}

macro_rules! no_data_calls {
    ($($name:ident($($arg:ty),*) -> $out:ty;)*) => {
        $(
            fn $name(&self, $(_: $arg),*) -> BoxFuture<'_, OpResult<$out>> {
                no_data()
            }
        )*
    };
}

impl ManagementStore for FakeCatalog {
    no_data_calls! {
        delete_account(&str) -> ();
        list_all_accounts() -> Vec<(String, String)>;
        default_account_id() -> Option<String>;
        list_all_accounts_full() -> Vec<(String, String, OffsetDateTime)>;
        list_accounts_for(&str) -> Vec<(String, String)>;
        get_account_detail(&str) -> Option<AccountDetail>;
        dashboard_counts() -> (i64, i64);
        delete_user(&str, &str) -> ();
        list_users(&str) -> Vec<UserListEntry>;
        get_user_detail(&str, &str) -> Option<UserDetail>;
        verify_iam_user_password(&str, &str, &str) -> bool;
        change_user_password(&str, &str, &str) -> ();
        tag_user(&str, &str, &[(String, String)]) -> ();
        untag_user(&str, &str, &[String]) -> ();
        list_user_tags(&str, &str) -> Vec<(String, String)>;
        create_group(&str, &str) -> ();
        delete_group(&str, &str) -> ();
        list_groups(&str) -> Vec<GroupListEntry>;
        get_group_detail(&str, &str) -> Option<GroupDetail>;
        add_group_member(&str, &str, &str) -> ();
        remove_group_member(&str, &str, &str) -> ();
        delete_role(&str, &str) -> ();
        list_roles(&str) -> Vec<RoleListEntry>;
        get_role_detail(&str, &str) -> Option<RoleDetail>;
        get_role_trust_policy(&str, &str) -> Option<Value>;
        tag_role(&str, &str, &[(String, String)]) -> ();
        untag_role(&str, &str, &[String]) -> ();
        list_role_tags(&str, &str) -> Vec<(String, String)>;
        put_policy(&str, &str, &str, &str, &Value) -> ();
        delete_policy(&str, &str, &str, &str) -> ();
        list_policies(&str, &str, &str) -> Vec<(String, Value, OffsetDateTime)>;
        set_user_boundary(&str, &str, &Value) -> ();
        get_user_boundary(&str, &str) -> Option<Value>;
        delete_user_boundary(&str, &str) -> ();
        set_role_boundary(&str, &str, &Value) -> ();
        get_role_boundary(&str, &str) -> Option<Value>;
        delete_role_boundary(&str, &str) -> ();
        delete_access_key(&str, &str, &str) -> ();
        list_access_keys(&str, &str) -> Vec<(String, bool, OffsetDateTime)>;
        import_access_key(&str, &str, &str, &str) -> ();
        fetch_caller_tags(&str, &str) -> Vec<(String, String)>;
    }

    // The calls the conformance check seeds its rows with.

    fn create_account(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn create_user(&self, _: &str, _: &str, _: Option<&str>) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn create_role(&self, _: &str, _: &str, _: &Value) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn create_access_key(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<AccessKeyCreated>> {
        let created = AccessKeyCreated {
            access_key_id: format!("AKIAFAKE{:012}", self.rows.lock().unwrap().len()),
            secret_access_key: "fake-secret".to_owned(),
        };
        let keys = SecretKeys::catalog_key(&self.get(CATALOG_KEY_SETTING).expect("a catalog key"))
            .expect("catalog key");
        let row = SecretRow::access_key(&created.access_key_id, account_id, user_name);
        self.add_stored(
            &row,
            keys.encrypt(&row, &created.secret_access_key).unwrap(),
        );
        Box::pin(async move { Ok(created) })
    }

    #[allow(clippy::too_many_arguments)]
    fn store_session(
        &self,
        _session_token: &str,
        access_key_id: &str,
        secret_key_encrypted: &[u8],
        account_id: &str,
        role_name: &str,
        session_name: &str,
        _: &Option<Value>,
        _: &Option<Value>,
        expires_at: OffsetDateTime,
    ) -> BoxFuture<'_, OpResult<()>> {
        let row = SecretRow::session(
            access_key_id,
            account_id,
            role_name,
            session_name,
            expires_at.unix_timestamp(),
        );
        self.add_stored(&row, secret_key_encrypted.to_vec());
        Box::pin(async { Ok(()) })
    }

    fn read_secret_page(
        &self,
        table: SecretTable,
        page: Option<&str>,
        page_size: usize,
    ) -> BoxFuture<'_, OpResult<SecretPage>> {
        let rows = self.rows.lock().unwrap();
        let mut page_rows: Vec<StoredSecretRow> = rows
            .iter()
            .filter(|((t, key), _)| *t == table && page.is_none_or(|after| key.as_str() > after))
            .map(|(_, row)| row.clone())
            .take(page_size + 1)
            .collect();
        let next = (page_rows.len() > page_size).then(|| {
            page_rows.truncate(page_size);
            page_rows.last().map(|r| r.key.clone()).unwrap_or_default()
        });
        Box::pin(async move {
            Ok(SecretPage {
                rows: page_rows,
                next,
            })
        })
    }

    fn write_secrets(
        &self,
        table: SecretTable,
        writes: &[SecretWrite],
    ) -> BoxFuture<'_, OpResult<()>> {
        if !*self.lossy_writes.lock().unwrap() {
            let mut rows = self.rows.lock().unwrap();
            for write in writes {
                if let Some(row) = rows.get_mut(&(table, write.key.clone())) {
                    row.secret_key_encrypted = write.secret_key_encrypted.clone();
                }
            }
        }
        Box::pin(async { Ok(()) })
    }

    fn delete_secrets(&self, table: SecretTable, keys: &[String]) -> BoxFuture<'_, OpResult<()>> {
        if !*self.lossy_deletes.lock().unwrap() {
            let mut rows = self.rows.lock().unwrap();
            for key in keys {
                rows.remove(&(table, key.clone()));
            }
        }
        Box::pin(async { Ok(()) })
    }

    fn delete_setting(&self, key: &str) -> BoxFuture<'_, OpResult<()>> {
        if !*self.lossy_deletes.lock().unwrap() {
            self.settings.lock().unwrap().remove(key);
        }
        Box::pin(async { Ok(()) })
    }

    fn erase_free_space(&self) -> BoxFuture<'_, OpResult<()>> {
        *self.erased.lock().unwrap() += 1;
        let fails = *self.erase_fails.lock().unwrap();
        Box::pin(async move {
            if fails {
                Err(OpError::Internal("database or disk is full".to_owned()))
            } else {
                Ok(())
            }
        })
    }
}

impl SettingsStore for FakeCatalog {
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let value = self.get(key);
        Box::pin(async move { Ok(value) })
    }

    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>> {
        self.set(key, value);
        Box::pin(async { Ok(()) })
    }

    fn replace_setting(
        &self,
        key: &str,
        expected: &str,
        value: &str,
    ) -> BoxFuture<'_, OpResult<bool>> {
        if let Some(Some(other)) = self.written_before_replace.lock().unwrap().pop_front() {
            self.set(key, &other);
        }
        let replaced = self.get(key).as_deref() == Some(expected);
        if replaced {
            self.set(key, value);
        }
        Box::pin(async move { Ok(replaced) })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let all = self.settings.lock().unwrap().clone().into_iter().collect();
        Box::pin(async move { Ok(all) })
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

pub(crate) fn dek(id: u64) -> SecretKeys {
    SecretKeys::dek_for_tests([id as u8; 32], id, CATALOG_ID)
}

pub(crate) fn catalog_key() -> SecretKeys {
    SecretKeys::catalog_key(CATALOG_KEY_B64).expect("catalog key")
}

/// The text of a ring of `ids` under `kek`.
pub(crate) fn ring_text(
    kek: &[u8; 32],
    ids: &[u64],
    switch: bool,
    retiring: Option<KeyId>,
) -> String {
    let deks: Vec<SecretKeys> = ids.iter().map(|id| dek(*id)).collect();
    let refs: Vec<&SecretKeys> = deks.iter().collect();
    KeyRing::seal(kek, CATALOG_ID, &refs, switch, retiring)
        .expect("seal")
        .to_json()
}

fn edit_ring(text: &str, edit: impl FnOnce(&mut Value)) -> String {
    let mut value: Value = serde_json::from_str(text).expect("ring json");
    edit(&mut value);
    value.to_string()
}
