// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The key holder of the stored secrets, and the AAD encoding (RFC-0000
//! sections 4, 5 and 6.2).
//!
//! A server holds one key: the catalog key, or the DEK of the key ring. Each
//! backend builds one [`SecretKeys`] at startup through
//! [`crate::server_components::load_secret_keys`], and its catalog store and
//! credential store share it. Each encrypt and decrypt site describes its row
//! with a [`SecretRow`], and the key holder builds the AAD from it:
//!
//! - under the catalog key, the access key id, as before the key ring;
//! - under a DEK, `extenddb-secret-v1` followed by the catalog id and the row
//!   fields of section 5.
//!
//! The key bytes live in [`KeyBytes`], the one type of this crate that holds
//! them.

use std::fmt::{self, Write as _};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use zeroize::Zeroizing;

use crate::management_store::OpError;
use crate::secret_cipher::{self, KEY_LEN};

/// The settings row that holds the catalog key.
pub const CATALOG_KEY_SETTING: &str = "encryption_key";

/// The settings row that holds the catalog id, which migration 6 writes.
pub const CATALOG_ID_SETTING: &str = "catalog_id";

/// The PostgreSQL settings row that holds the data database connection
/// string. With a key ring its value is encrypted under the DEK (section 12).
pub const DATA_CONNECTION_SETTING: &str = "data_database_connection_string";

/// The kind of an AAD, which fixes its prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AadKind {
    /// A DEK wrapped under the KEK in the key ring.
    Dek,
    /// A stored secret under a DEK.
    Secret,
    /// The data database connection string row under a DEK.
    Setting,
}

impl AadKind {
    /// The fixed text that starts every AAD of this kind.
    #[must_use]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Dek => "extenddb-dek-v1",
            Self::Secret => "extenddb-secret-v1",
            Self::Setting => "extenddb-setting-v1",
        }
    }
}

/// Encode an AAD: the prefix of `kind`, then for each field a `|`, the
/// field's length in bytes in decimal, a `:` and the field's bytes. No field
/// value can change where the next field starts, and an empty field (a null
/// value) is `0:`.
#[must_use]
pub fn encode_aad(kind: AadKind, fields: &[&str]) -> String {
    let mut aad = String::from(kind.prefix());
    for field in fields {
        // Writing to a String cannot fail.
        let _ = write!(aad, "|{}:{field}", field.len());
    }
    aad
}

/// A catalog table that holds stored secrets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SecretTable {
    /// `access_keys`: the secret of each access key of an IAM user.
    AccessKeys,
    /// `iam_sessions`: the secret of each AssumeRole session.
    IamSessions,
}

impl SecretTable {
    /// Both tables, in the order the commands read them.
    pub const ALL: [Self; 2] = [Self::AccessKeys, Self::IamSessions];

    /// The table's name, which is also its field in the AAD.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AccessKeys => "access_keys",
            Self::IamSessions => "iam_sessions",
        }
    }

    /// The table's primary key column.
    #[must_use]
    pub const fn key_column(self) -> &'static str {
        match self {
            Self::AccessKeys => "access_key_id",
            Self::IamSessions => "session_token",
        }
    }
}

/// The fields of a stored secret's row that its AAD binds (section 5).
///
/// Every encrypt and decrypt site has these fields after its row read. The
/// AAD carries `expires_at` in whole Unix seconds, because the backends store
/// it at different precisions: a writer truncates the value before the
/// encrypt and before the write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretRow<'a> {
    /// The table that holds the row.
    pub table: SecretTable,
    /// The access key ID (`AKIA...` or `ASIA...`).
    pub access_key_id: &'a str,
    /// The account of the user or role.
    pub account_id: &'a str,
    /// The user name for an access key, the role name for a session.
    pub principal_name: &'a str,
    /// The session name, for a session only.
    pub session_name: Option<&'a str>,
    /// The session's `expires_at` in whole Unix seconds, for a session only.
    pub expires_at: Option<i64>,
}

impl<'a> SecretRow<'a> {
    /// The row of an IAM user's access key.
    #[must_use]
    pub const fn access_key(
        access_key_id: &'a str,
        account_id: &'a str,
        user_name: &'a str,
    ) -> Self {
        Self {
            table: SecretTable::AccessKeys,
            access_key_id,
            account_id,
            principal_name: user_name,
            session_name: None,
            expires_at: None,
        }
    }

    /// The row of an AssumeRole session. `expires_at` is in whole Unix seconds.
    #[must_use]
    pub const fn session(
        access_key_id: &'a str,
        account_id: &'a str,
        role_name: &'a str,
        session_name: &'a str,
        expires_at: i64,
    ) -> Self {
        Self {
            table: SecretTable::IamSessions,
            access_key_id,
            account_id,
            principal_name: role_name,
            session_name: Some(session_name),
            expires_at: Some(expires_at),
        }
    }
}

/// The 32 bytes of one key: the catalog key or a DEK. The one type of this
/// crate that holds key bytes. They sit in a slot of the locked key store
/// (`extenddb_auth::secmem`, RFC-0000 section 6.2), which wipes the slot when
/// it drops.
pub(crate) struct KeyBytes(extenddb_auth::secmem::KeySlot);

impl KeyBytes {
    /// Copy `bytes` into a slot of the locked key store. `bytes` is wiped
    /// when it drops.
    ///
    /// # Errors
    ///
    /// When the store has no free slot.
    pub(crate) fn new(bytes: Zeroizing<[u8; KEY_LEN]>) -> Result<Self, String> {
        extenddb_auth::secmem::KeySlot::from_bytes(&bytes)
            .map(Self)
            .map_err(|e| e.to_string())
    }

    /// A new random key, from the generator that `init` uses for the catalog
    /// key ([`crate::bootstrapper::helpers::generate_encryption_key`]).
    pub(crate) fn generate() -> Result<Self, String> {
        let text = Zeroizing::new(crate::bootstrapper::helpers::generate_encryption_key()?);
        Self::new(secret_cipher::decode_key(&text)?)
    }

    /// The key bytes.
    pub(crate) fn expose(&self) -> &[u8; KEY_LEN] {
        self.0.expose()
    }
}

/// The key a [`SecretKeys`] holds.
enum Mode {
    /// The catalog key: the AAD is the access key ID.
    CatalogKey,
    /// A DEK of the key ring: the AAD is section 5's.
    Dek { id: u64, catalog_id: String },
}

/// The key holder of a store: the one key that encrypts and decrypts the
/// stored secrets, the catalog key or a DEK.
///
/// A backend builds one per process and shares it between its catalog store
/// and its credential store. A store built with no key holder refuses to
/// create, import or store a credential.
pub struct SecretKeys {
    key: KeyBytes,
    mode: Mode,
}

impl SecretKeys {
    /// A key holder for the catalog key, from the base64 text of the
    /// `encryption_key` row.
    ///
    /// # Errors
    ///
    /// The secret cipher's reason when the text is not base64 or does not
    /// decode to 32 bytes.
    pub fn catalog_key(key_b64: &str) -> Result<Self, String> {
        Ok(Self {
            key: KeyBytes::new(secret_cipher::decode_key(key_b64)?)?,
            mode: Mode::CatalogKey,
        })
    }

    /// A key holder for the DEK `id` of the key ring of catalog `catalog_id`.
    pub(crate) fn dek(key: KeyBytes, id: u64, catalog_id: &str) -> Self {
        Self {
            key,
            mode: Mode::Dek {
                id,
                catalog_id: catalog_id.to_owned(),
            },
        }
    }

    /// A DEK key holder over `bytes`, for the tests of a backend's encrypt
    /// and decrypt sites.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn dek_for_tests(bytes: [u8; KEY_LEN], id: u64, catalog_id: &str) -> Self {
        Self::dek(
            KeyBytes::new(Zeroizing::new(bytes)).expect("a free slot of the locked key store"),
            id,
            catalog_id,
        )
    }

    /// The key bytes, for the key ring's wrap and key ids.
    pub(crate) fn key_bytes(&self) -> &KeyBytes {
        &self.key
    }

    /// The DEK id, or `None` for the catalog key.
    #[must_use]
    pub fn dek_id(&self) -> Option<u64> {
        match &self.mode {
            Mode::CatalogKey => None,
            Mode::Dek { id, .. } => Some(*id),
        }
    }

    /// The catalog id in the AAD of a DEK, or `None` for the catalog key.
    #[must_use]
    pub fn catalog_id(&self) -> Option<&str> {
        match &self.mode {
            Mode::CatalogKey => None,
            Mode::Dek { catalog_id, .. } => Some(catalog_id),
        }
    }

    /// The AAD of a stored secret in `row` under this key.
    #[must_use]
    pub fn aad(&self, row: &SecretRow<'_>) -> String {
        match &self.mode {
            Mode::CatalogKey => row.access_key_id.to_owned(),
            Mode::Dek { catalog_id, .. } => {
                let expires_at = row.expires_at.map(|t| t.to_string());
                encode_aad(
                    AadKind::Secret,
                    &[
                        catalog_id,
                        row.table.name(),
                        row.access_key_id,
                        row.account_id,
                        row.principal_name,
                        row.session_name.unwrap_or(""),
                        expires_at.as_deref().unwrap_or(""),
                    ],
                )
            }
        }
    }

    /// Encrypt the secret of `row`. Returns `nonce(12) || ciphertext || tag(16)`.
    ///
    /// # Errors
    ///
    /// The secret cipher's reason, for example an empty access key ID under
    /// the catalog key.
    pub fn encrypt(&self, row: &SecretRow<'_>, secret: &str) -> Result<Vec<u8>, String> {
        secret_cipher::seal(self.key.expose(), secret.as_bytes(), &self.aad(row))
    }

    /// Decrypt the stored secret of `row`. The returned string is wiped when
    /// it drops.
    ///
    /// # Errors
    ///
    /// `decrypt: authentication failed` when this key does not open the
    /// secret: another key, another row, or altered bytes.
    pub fn decrypt(&self, row: &SecretRow<'_>, stored: &[u8]) -> Result<Zeroizing<String>, String> {
        secret_cipher::open_text(self.key.expose(), stored, &self.aad(row))
    }

    /// Encrypt the data database connection string under a DEK, with the AAD
    /// of section 12. Returns the base64 text the settings row stores.
    ///
    /// # Errors
    ///
    /// Under the catalog key, which encrypts no setting, and on a cipher error.
    pub fn encrypt_setting(&self, value: &str) -> Result<String, String> {
        let sealed =
            secret_cipher::seal(self.key.expose(), value.as_bytes(), &self.setting_aad()?)?;
        Ok(BASE64.encode(sealed))
    }

    /// Decrypt the base64 text of the data database connection string row.
    ///
    /// # Errors
    ///
    /// Under the catalog key, and when the text is not base64 or this DEK
    /// does not open it (a plaintext row included).
    pub fn decrypt_setting(&self, stored: &str) -> Result<Zeroizing<String>, String> {
        let aad = self.setting_aad()?;
        let sealed = BASE64
            .decode(stored)
            .map_err(|_| "decrypt: the stored value is not base64".to_owned())?;
        secret_cipher::open_text(self.key.expose(), &sealed, &aad)
    }

    /// `extenddb-setting-v1|<catalog_id>|data_database_connection_string`.
    fn setting_aad(&self) -> Result<String, String> {
        match &self.mode {
            Mode::CatalogKey => Err("the catalog key encrypts no setting".to_owned()),
            Mode::Dek { catalog_id, .. } => Ok(encode_aad(
                AadKind::Setting,
                &[catalog_id, DATA_CONNECTION_SETTING],
            )),
        }
    }
}

impl fmt::Debug for SecretKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretKeys")
            .field("dek_id", &self.dek_id())
            .finish_non_exhaustive()
    }
}

/// The key holder of a store that creates, imports or stores a credential,
/// or the refusal of a store built with no key (RFC-0000 section 7).
///
/// # Errors
///
/// [`OpError::Internal`] when `keys` is `None`.
pub fn require_keys(keys: Option<&SecretKeys>) -> Result<&SecretKeys, OpError> {
    keys.ok_or_else(|| {
        OpError::Internal(
            "this store was opened without an encryption key, so it writes no credential"
                .to_owned(),
        )
    })
}
