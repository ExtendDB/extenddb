// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The key ring (RFC-0000 section 4): the `encryption_key_ring` settings row
//! of a deployment with a KEK file.
//!
//! ```json
//! {"version":1,"kek":{"source":"file","id":"3f9a0c51e2d47b86"},"keys":[{"id":2,"wrapped":"<base64>"}],"switch_from_catalog_key":false,"retiring_catalog_key_id":null}
//! ```
//!
//! Each `wrapped` value is a DEK sealed under the KEK by the secret cipher. Its
//! AAD covers every other field of the ring and the sorted list of DEK ids,
//! so a changed field, an added or deleted entry, or a wrapped DEK copied from
//! another ring makes every DEK fail to unwrap. Every write of the ring
//! re-seals each DEK ([`KeyRing::seal`]). The DEK with the highest id is the
//! active one, and the order of `keys` carries no meaning.

use std::fmt;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::management_store::{OpError, SettingsStore};
use crate::secret_cipher::{self, KEY_LEN};
use crate::secret_keys::{
    AadKind, CATALOG_ID_SETTING, CATALOG_KEY_SETTING, KeyBytes, SecretKeys, encode_aad,
};

/// The settings row that holds the key ring.
pub const KEY_RING_SETTING: &str = "encryption_key_ring";

/// The ring format this release writes and reads.
pub const RING_VERSION: u64 = 1;

/// The message of a KEK id, under HMAC-SHA256 keyed with the KEK.
const KEK_ID_MESSAGE: &[u8] = b"extenddb key-encryption-key id";

/// Where the KEK comes from. This release knows one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KekSource {
    /// The file that `[auth.encryption] key_file` names.
    File,
}

impl KekSource {
    /// The source's value in the ring and in the AAD.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::File => "file",
        }
    }
}

/// A key id: the first 8 bytes, in lowercase hex, of HMAC-SHA256 keyed with
/// a KEK. It identifies a key and does not reveal it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct KeyId(String);

impl KeyId {
    /// Parse 16 lowercase hex characters.
    ///
    /// # Errors
    ///
    /// For any other text, the empty string included.
    pub fn parse(text: &str) -> Result<Self, String> {
        if text.len() == 16
            && text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            Ok(Self(text.to_owned()))
        } else {
            Err(format!(
                "a key id is 16 lowercase hex characters, not {text:?}"
            ))
        }
    }

    /// The 16 hex characters.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for KeyId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// The id of a KEK: HMAC-SHA256 keyed with the KEK over
/// `extenddb key-encryption-key id`.
#[must_use]
pub fn kek_id(kek: &[u8; KEY_LEN]) -> KeyId {
    key_id(kek, KEK_ID_MESSAGE)
}

/// The id of a catalog key under a KEK: HMAC-SHA256 keyed with the KEK over
/// the 32 decoded bytes of the catalog key. `retiring_catalog_key_id` holds it
/// during a switch.
#[must_use]
pub fn catalog_key_id(kek: &[u8; KEY_LEN], catalog_key: &SecretKeys) -> KeyId {
    key_id(kek, catalog_key.key_bytes().expose())
}

fn key_id(kek: &[u8; KEY_LEN], message: &[u8]) -> KeyId {
    let tag = aws_lc_rs::hmac::sign(
        &aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, kek),
        message,
    );
    let tag = &tag.as_ref()[..8];
    KeyId(tag.iter().map(|b| format!("{b:02x}")).collect())
}

/// Parse the contents of a KEK file (section 2): 32 bytes in standard base64
/// with padding, which is 44 characters, and at most one trailing newline.
///
/// The key is decoded into the one allocation that holds it until the last
/// [`Kek`] drops.
///
/// # Errors
///
/// For anything else. The reason of a file that does not decode is the one
/// the secret cipher gives for a damaged catalog key, which names the kind of
/// error and its offset and never a character of the file.
pub fn parse_kek_file(contents: &[u8]) -> Result<Kek, String> {
    let text = contents.strip_suffix(b"\n").unwrap_or(contents);
    if text.len() != 44 {
        return Err(format!(
            "a KEK file holds 44 characters of base64 and at most one trailing newline; \
             this one holds {} bytes",
            contents.len()
        ));
    }
    let text = std::str::from_utf8(text).map_err(|e| {
        format!(
            "decode encryption key: not text at offset {}",
            e.valid_up_to()
        )
    })?;
    let mut kek = Arc::new(Zeroizing::new([0; KEY_LEN]));
    secret_cipher::decode_key_into(text, Arc::get_mut(&mut kek).expect("a new Arc"))?;
    Ok(kek)
}

/// A KEK, in one heap allocation that is wiped when the last reference
/// drops. A move or a clone copies a pointer and leaves no copy of the key.
pub type Kek = Arc<Zeroizing<[u8; KEY_LEN]>>;

/// The KEK entry of a ring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KekRef {
    /// Where the KEK comes from.
    pub source: KekSource,
    /// The KEK's id ([`kek_id`]).
    pub id: KeyId,
}

/// One wrapped DEK of a ring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WrappedDek {
    /// The DEK id. Ids start at 1, and each DEK replacement takes the next.
    pub id: u64,
    /// The DEK sealed under the KEK, in base64.
    pub wrapped: String,
}

/// The `encryption_key_ring` row.
///
/// The parser refuses a field that the format does not have and a value of
/// another type. Every field is required; `retiring_catalog_key_id` is null
/// or a key id, never an empty string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyRing {
    /// The ring format, [`RING_VERSION`].
    pub version: u64,
    /// The KEK that wraps every DEK.
    pub kek: KekRef,
    /// One DEK, or two during an unfinished DEK replacement.
    pub keys: Vec<WrappedDek>,
    /// True from the start of a switch until the switch finishes.
    pub switch_from_catalog_key: bool,
    /// The id of the catalog key that an unfinished switch retires.
    #[serde(deserialize_with = "Option::deserialize")]
    pub retiring_catalog_key_id: Option<KeyId>,
}

impl KeyRing {
    /// Parse the text of the `encryption_key_ring` row.
    ///
    /// # Errors
    ///
    /// For text that is not a ring of this release: an unknown or missing
    /// field, a value of another type, another version, no DEK or more than
    /// two, a DEK id of 0, or two entries with one id.
    pub fn parse(text: &str) -> Result<Self, String> {
        let ring: Self = serde_json::from_str(text).map_err(|e| format!("{e}"))?;
        ring.check_shape()?;
        Ok(ring)
    }

    /// The version, and one or two DEKs with distinct ids from 1.
    fn check_shape(&self) -> Result<(), String> {
        if self.version != RING_VERSION {
            return Err(format!(
                "version {} is not the ring version {RING_VERSION} of this release",
                self.version
            ));
        }
        let ids = self.dek_ids();
        if ids.is_empty() || ids.len() > 2 {
            return Err(format!("a ring holds one or two DEKs, not {}", ids.len()));
        }
        if ids[0] == 0 || ids.windows(2).any(|w| w[0] == w[1]) {
            return Err("DEK ids start at 1 and each appears once".to_owned());
        }
        Ok(())
    }

    /// The row text.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("a key ring serializes")
    }

    /// The DEK ids, in increasing order.
    #[must_use]
    pub fn dek_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.keys.iter().map(|k| k.id).collect();
        ids.sort_unstable();
        ids
    }

    /// The id of the active DEK, the highest.
    #[must_use]
    pub fn active_dek_id(&self) -> u64 {
        self.keys.iter().map(|k| k.id).max().unwrap_or(0)
    }

    /// The id of the DEK that a rotation or a reset writes next, one above
    /// the active DEK's. The ring row carries the active id, so a row that
    /// was written by hand can hold `u64::MAX`, which has no successor.
    ///
    /// # Errors
    ///
    /// [`KeyRefusal::DamagedRing`] when the active id is `u64::MAX`.
    pub fn next_dek_id(&self) -> Result<u64, KeyRefusal> {
        let active = self.active_dek_id();
        active
            .checked_add(1)
            .ok_or_else(|| KeyRefusal::DamagedRing {
                reason: format!(
                    "the active DEK's id is {active}, the largest id a ring holds, so this run \
                     has no id for a new DEK"
                ),
            })
    }

    /// The AAD of DEK `id` in a ring of this state.
    fn dek_aad(&self, catalog_id: &str, id: u64) -> String {
        let ids = self
            .dek_ids()
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        encode_aad(
            AadKind::Dek,
            &[
                &self.version.to_string(),
                catalog_id,
                self.kek.source.name(),
                self.kek.id.as_str(),
                &ids,
                &id.to_string(),
                if self.switch_from_catalog_key {
                    "true"
                } else {
                    "false"
                },
                self.retiring_catalog_key_id
                    .as_ref()
                    .map_or("", KeyId::as_str),
            ],
        )
    }

    /// Unwrap every DEK under `kek`, in increasing id order.
    ///
    /// The caller compares [`kek_id`] with `kek.id` first. No other field of
    /// the ring is authentic until this returns `Ok`.
    ///
    /// # Errors
    ///
    /// The ids of the DEKs that fail to unwrap, when any does.
    pub fn unwrap(
        &self,
        kek: &[u8; KEY_LEN],
        catalog_id: &str,
    ) -> Result<Vec<SecretKeys>, Vec<u64>> {
        let mut deks = Vec::with_capacity(self.keys.len());
        let mut failed = Vec::new();
        let mut entries: Vec<&WrappedDek> = self.keys.iter().collect();
        entries.sort_unstable_by_key(|k| k.id);
        for entry in entries {
            let opened = BASE64
                .decode(&entry.wrapped)
                .map_err(|_| ())
                .and_then(|sealed| {
                    secret_cipher::open(kek, &sealed, &self.dek_aad(catalog_id, entry.id))
                        .map_err(|_| ())
                })
                .and_then(|plain| {
                    let mut key = Zeroizing::new([0; KEY_LEN]);
                    if plain.len() != KEY_LEN {
                        return Err(());
                    }
                    key.copy_from_slice(&plain);
                    Ok(key)
                });
            match opened {
                // The store is sized for every key a run holds at once
                // (`serve_slots` and `COMMAND_SLOTS` in the server crate),
                // so a full store here is a sizing defect, not a ring fault.
                Ok(key) => deks.push(SecretKeys::dek(
                    KeyBytes::new(key).expect("a free slot of the locked key store for a DEK"),
                    entry.id,
                    catalog_id,
                )),
                Err(()) => failed.push(entry.id),
            }
        }
        if failed.is_empty() {
            Ok(deks)
        } else {
            Err(failed)
        }
    }

    /// Build a ring that holds `deks`, each wrapped under `kek` with the AAD
    /// of the new state.
    ///
    /// # Errors
    ///
    /// When `deks` is empty or holds more than two keys, when a key is not a
    /// DEK of `catalog_id`, or on a cipher error.
    pub fn seal(
        kek: &[u8; KEY_LEN],
        catalog_id: &str,
        deks: &[&SecretKeys],
        switch_from_catalog_key: bool,
        retiring_catalog_key_id: Option<KeyId>,
    ) -> Result<Self, String> {
        let mut ring = Self {
            version: RING_VERSION,
            kek: KekRef {
                source: KekSource::File,
                id: kek_id(kek),
            },
            keys: Vec::with_capacity(deks.len()),
            switch_from_catalog_key,
            retiring_catalog_key_id,
        };
        for dek in deks {
            if dek.catalog_id() != Some(catalog_id) {
                return Err("a ring holds only DEKs of its own catalog".to_owned());
            }
            let id = dek.dek_id().ok_or("the catalog key is not a DEK")?;
            ring.keys.push(WrappedDek {
                id,
                wrapped: String::new(),
            });
        }
        ring.check_shape()?;
        // Every id is in `ring.keys` now, so each AAD holds the whole list.
        let wrapped = deks
            .iter()
            .zip(&ring.keys)
            .map(|(dek, entry)| {
                secret_cipher::seal(
                    kek,
                    dek.key_bytes().expose(),
                    &ring.dek_aad(catalog_id, entry.id),
                )
                .map(|sealed| BASE64.encode(sealed))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (entry, wrapped) in ring.keys.iter_mut().zip(wrapped) {
            entry.wrapped = wrapped;
        }
        Ok(ring)
    }
}

/// A new DEK with id `id` for catalog `catalog_id`, from the generator that
/// `init` uses for the catalog key.
///
/// # Errors
///
/// When the random number generator fails.
pub fn new_dek(id: u64, catalog_id: &str) -> Result<SecretKeys, String> {
    Ok(SecretKeys::dek(KeyBytes::generate()?, id, catalog_id))
}

/// The key rows of a catalog, as the loader and the commands read them.
pub struct KeyRows {
    /// The `encryption_key` row: the catalog key in base64. Wiped on drop.
    pub catalog_key: Option<Zeroizing<String>>,
    /// The `encryption_key_ring` row.
    pub ring: Option<String>,
    /// The `catalog_id` row.
    pub catalog_id: Option<String>,
}

impl KeyRows {
    /// Read the three rows through [`SettingsStore::get_setting`].
    ///
    /// # Errors
    ///
    /// A store error.
    pub async fn read(settings: &dyn SettingsStore) -> Result<Self, OpError> {
        Ok(Self {
            catalog_key: settings
                .get_setting(CATALOG_KEY_SETTING)
                .await?
                .map(Zeroizing::new),
            ring: settings.get_setting(KEY_RING_SETTING).await?,
            catalog_id: settings.get_setting(CATALOG_ID_SETTING).await?,
        })
    }
}

/// A ring whose every DEK unwrapped under the KEK: all its fields are
/// authentic.
pub struct OpenRing {
    /// The ring as stored.
    pub ring: KeyRing,
    /// Its DEKs, in increasing id order. The last is the active one.
    pub deks: Vec<SecretKeys>,
}

impl OpenRing {
    /// The active DEK, the one with the highest id.
    #[must_use]
    pub fn active(&self) -> &SecretKeys {
        self.deks.last().expect("an open ring holds a DEK")
    }

    /// The older DEK of a ring that holds two.
    #[must_use]
    pub fn older(&self) -> Option<&SecretKeys> {
        (self.deks.len() == 2).then(|| &self.deks[0])
    }
}

/// Open the ring row with `kek` in the order of section 7: parse it, compare
/// the KEK ids, then unwrap every DEK. No other field is read before all of
/// them unwrap.
///
/// # Errors
///
/// [`KeyRefusal::KekMismatch`] or [`KeyRefusal::DamagedRing`].
pub fn open_ring(
    ring_text: &str,
    catalog_id: Option<&str>,
    kek: &[u8; KEY_LEN],
) -> Result<OpenRing, KeyRefusal> {
    let ring = KeyRing::parse(ring_text).map_err(|reason| KeyRefusal::DamagedRing {
        reason: format!("the ring does not parse: {reason}"),
    })?;
    let configured = kek_id(kek);
    if ring.kek.id != configured {
        return Err(KeyRefusal::KekMismatch {
            ring: ring.kek.id.clone(),
            configured,
        });
    }
    let catalog_id = catalog_id.ok_or_else(|| KeyRefusal::DamagedRing {
        reason: "the catalog has no catalog_id row, which the ring's AAD holds".to_owned(),
    })?;
    let deks = ring
        .unwrap(kek, catalog_id)
        .map_err(|failed| KeyRefusal::DamagedRing {
            reason: match failed.as_slice() {
                [id] => format!("DEK {id} fails to unwrap"),
                ids => format!("DEKs {} fail to unwrap", join_ids(ids)),
            },
        })?;
    Ok(OpenRing { ring, deks })
}

/// Ids as `1, 2`.
pub(crate) fn join_ids(ids: &[u64]) -> String {
    ids.iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Where the refusals send an operator for a restore (RFC-0000 section 11).
/// The text names the heading of that section, so an operator can search
/// the manual for it.
pub(crate) const RESTORE_STEPS: &str =
    "see \"Restore a damaged or missing key ring\" in docs/manuals/14-backup-and-restore.md";

/// Where the refusal of a ring under another KEK sends an operator whose
/// KEK is gone: the steps that run `reset` (RFC-0000 section 11).
pub(crate) const LOST_KEK_STEPS: &str =
    "see \"The KEK of a catalog copy is lost\" in docs/manuals/14-backup-and-restore.md";

/// A key state that the server and the commands refuse (RFC-0000 section 7).
/// Its `Display` is the message, which names the next step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyRefusal {
    /// No `key_file`, and a ring whose switch did not finish.
    UnfinishedSwitchWithoutKeyFile,
    /// No `key_file`, and a finished ring.
    RingNeedsKeyFile {
        /// The ring's KEK id, not authenticated.
        kek_id: KeyId,
    },
    /// `key_file` set, a catalog key row and no ring.
    CatalogKeyWithoutRing,
    /// `key_file` set, and neither key row.
    NoRing,
    /// The ring names another KEK.
    KekMismatch {
        /// The ring's KEK id.
        ring: KeyId,
        /// The id of the KEK the run was given.
        configured: KeyId,
    },
    /// The ring does not parse, or a DEK fails to unwrap.
    DamagedRing {
        /// What failed.
        reason: String,
    },
    /// `key_file` set, and a ring whose switch did not finish.
    UnfinishedSwitchWithKeyFile,
    /// A catalog key row beside a finished ring.
    CatalogKeyAfterSwitch,
    /// A ring with two DEKs.
    UnfinishedRotation,
}

impl fmt::Display for KeyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnfinishedSwitchWithoutKeyFile => write!(
                f,
                "the catalog's encryption_key_ring row says that a switch to a KEK file did not \
                 finish. Run `extenddb encryption-key switch --key-file <path> --yes` with the KEK \
                 file of that switch: it checks the ring and resumes the switch. If no switch of \
                 yours stopped, {RESTORE_STEPS} first"
            ),
            Self::RingNeedsKeyFile { kek_id } => write!(
                f,
                "the catalog's stored secrets are under a key ring wrapped by the KEK with id \
                 {kek_id}. Set [auth.encryption] key_file to the file that holds that KEK"
            ),
            Self::CatalogKeyWithoutRing => write!(
                f,
                "[auth.encryption] key_file is set, but the catalog has no encryption_key_ring \
                 row and holds an encryption_key row. key_file is set only after a switch \
                 finishes, so the ring was deleted or key_file was set too early. Run \
                 `extenddb encryption-key status`, and {RESTORE_STEPS}"
            ),
            Self::NoRing => write!(
                f,
                "[auth.encryption] key_file is set, but the catalog has no encryption_key_ring \
                 row. Restore a catalog copy that holds it: {RESTORE_STEPS}"
            ),
            Self::KekMismatch { ring, configured } => write!(
                f,
                "the catalog's key ring is wrapped by the KEK with id {ring}, and the key file \
                 holds the KEK with id {configured}. After an `extenddb encryption-key rewrap`, \
                 set key_file to the new file. If key_file names the KEK that your last \
                 `encryption-key` run wrote the ring under and you restored no catalog copy \
                 since that run, a writer to the catalog database put this ring row back: \
                 {RESTORE_STEPS}. Otherwise find the KEK with id {ring} and set key_file to it; \
                 if that KEK is lost, {LOST_KEK_STEPS}"
            ),
            Self::DamagedRing { reason } => write!(
                f,
                "the catalog's key ring has damage or a changed field: {reason}. Run \
                 `extenddb encryption-key status`, and {RESTORE_STEPS}"
            ),
            Self::UnfinishedSwitchWithKeyFile => write!(
                f,
                "the catalog's key ring says that a switch to a KEK file did not finish, and \
                 [auth.encryption] key_file is already set, which is done only after a switch \
                 finishes. key_file was set while a stopped switch was unfinished, or the ring \
                 row was written back from a catalog copy taken during a switch. Run \
                 `extenddb encryption-key status`, and {RESTORE_STEPS}"
            ),
            Self::CatalogKeyAfterSwitch => write!(
                f,
                "the catalog holds an encryption_key row beside a finished switch to a KEK \
                 file: a catalog key appeared after the switch. Run \
                 `extenddb encryption-key status`, which lists the stored secrets under that key"
            ),
            Self::UnfinishedRotation => write!(
                f,
                "the catalog's key ring holds two DEKs: a DEK replacement did not finish. \
                 `extenddb encryption-key rotate` lists the stored secrets under the older DEK, \
                 and `extenddb encryption-key rotate --yes` resumes it"
            ),
        }
    }
}
