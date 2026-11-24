// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `extenddb encryption-key` commands as library functions (RFC-0000
//! sections 8, 8.1 to 8.3, 8.5 and 8.6), over the store methods.
//!
//! The command line around them is the app's: it parses the arguments, reads
//! the KEK file through the file rule ([`KekFile`]), opens the stores through
//! the migration backend, takes and frees the migration lock for `switch`,
//! `rotate` and `reset`, prints the [`Outcome`] and sets the exit code. These
//! functions make the checks and the writes in the order the RFC gives:
//!
//! - the nothing-to-do check comes first, right after the KEK file is read
//!   and the ring unwrapped;
//! - every command except `status` and `reset` checks the ring before it
//!   writes, in the order of section 7;
//! - `switch --yes` and `rotate --yes` open every stored secret with the keys
//!   the run holds before their first write, and stop when one opens under
//!   none, unless `--keep-unopenable`;
//! - without `--yes` a command previews and writes nothing.

mod reset;
mod rewrap;
mod switch;

pub use reset::{data_connection_key, reset};
pub use rewrap::{rewrap, rotate};
pub use switch::switch;

use zeroize::Zeroizing;

use crate::key_pass::{
    DataConnection, PAGE_SIZE, PassError, PassKeys, SecretListing, Survey, TableCounts, survey,
};
use crate::key_ring::{
    KEY_RING_SETTING, KeyId, KeyRefusal, KeyRing, KeyRows, LOST_KEK_STEPS, catalog_key_id,
    join_ids, kek_id,
};
use crate::management_store::{ManagementStore, OpError, SettingsStore};
use crate::secret_cipher::KEY_LEN;
use crate::secret_keys::{CATALOG_KEY_SETTING, SecretKeys, SecretTable};

/// The notice at the end of every command that writes, except `reset`.
pub const NO_REVOCATION_NOTICE: &str = "This does not revoke any credential. Each access key \
     keeps its secret. A secret that leaked before this run works until its access key is \
     re-created.";

/// The notice at the end of `reset`, which deletes every stored secret.
#[must_use]
pub fn reset_notice() -> String {
    format!(
        "Every access key and every session was deleted. For the steps after a reset, \
         {LOST_KEK_STEPS}."
    )
}

/// A KEK file, read through the file rule (section 2). The app implements it;
/// this crate reads no file.
pub trait KekFile: Send + Sync {
    /// The path as given. The `key_file` line of the summary prints it made
    /// absolute ([`Summary::key_file_line`]).
    fn path(&self) -> &str;

    /// Read the file through the file rule and parse it
    /// ([`crate::key_ring::parse_kek_file`]). The commands call it at the
    /// start, and the pass calls it again before an old key goes.
    ///
    /// # Errors
    ///
    /// The path and the check that failed.
    fn read(&self) -> Result<Zeroizing<[u8; KEY_LEN]>, String>;
}

/// The stores a command runs over, opened through the migration backend for
/// `switch`, `rotate` and `reset`.
#[derive(Clone, Copy)]
pub struct KeyStores<'a> {
    /// The rows of `access_keys` and `iam_sessions`.
    pub management: &'a dyn ManagementStore,
    /// The `encryption_key`, `encryption_key_ring`, `catalog_id` and, on
    /// PostgreSQL, `data_database_connection_string` rows.
    pub settings: &'a dyn SettingsStore,
}

/// The options of `switch` and `rotate`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunFlags {
    /// `--yes`: write. Without it the command previews.
    pub yes: bool,
    /// `--keep-unopenable`.
    pub keep_unopenable: bool,
    /// `--accept-catalog-key` (section 8.5).
    pub accept_catalog_key: bool,
    /// The configuration sets `key_file`. `switch` and `reset` read it.
    pub key_file_configured: bool,
}

/// What a command found or did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The ring is already in the target state. Exit 0.
    NothingToDo {
        /// The ring's KEK id.
        kek_id: KeyId,
        /// The ring's DEK id.
        dek_id: u64,
    },
    /// No `--yes`: what `--yes` would do. Nothing was written.
    Preview(Preview),
    /// The command wrote and finished.
    Done(Summary),
}

/// What `--yes` would do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preview {
    /// One line per write, in order.
    pub steps: Vec<String>,
    /// The stored secrets and the key each opens under. `reset` tries no key
    /// and has none.
    pub survey: Option<Survey>,
}

/// The summary that every command that writes prints at the end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    /// The KEK id of the ring the command wrote.
    pub kek_id: KeyId,
    /// The DEK id of that ring.
    pub dek_id: u64,
    /// The name of each key of the run, indexed as in
    /// [`crate::key_pass::PassKeys::names`].
    pub key_names: Vec<String>,
    /// The counts of `access_keys` and `iam_sessions`.
    pub tables: [(SecretTable, TableCounts); 2],
    /// `None` without a connection string row (every backend but
    /// PostgreSQL), else whether the command rewrote it.
    pub data_connection_rewritten: Option<bool>,
    /// The stored secrets that no key opens, by access key ID.
    pub unopenable: Vec<SecretListing>,
    /// `reset`: the access key IDs it deleted.
    pub deleted_access_key_ids: Vec<String>,
    /// Whether the command deleted an `encryption_key` row.
    pub deleted_catalog_key: bool,
    /// The path for the `key_file` line of the configuration.
    pub key_file: String,
}

impl Summary {
    /// The `[auth.encryption]` line for the configuration. A relative path
    /// is made absolute against this process's working directory, as the
    /// migration advice prints a config path, because a daemon `serve` reads
    /// the file from `/`. Symlinks stay as given: the file rule resolves them
    /// at each read, and a Kubernetes secret volume replaces its target.
    #[must_use]
    pub fn key_file_line(&self) -> String {
        let path = std::path::absolute(&self.key_file)
            .map_or_else(|_| self.key_file.clone(), |p| p.display().to_string());
        format!("key_file = {path:?}")
    }
}

/// What `status` prints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusReport {
    /// The catalog holds an `encryption_key` row.
    pub catalog_key_present: bool,
    /// The ring, when the catalog has one.
    pub ring: Option<RingStatus>,
    /// The stored secrets per key, when a key opens them: the catalog key,
    /// or the ring's DEKs with the KEK given.
    pub survey: Option<Survey>,
}

/// The ring as `status` prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingStatus {
    /// The ring's fields. Not authenticated unless `failed_dek_ids` is empty
    /// and the KEK id matches.
    pub ring: KeyRing,
    /// The id of the KEK given to `status`, if one was.
    pub given_kek_id: Option<KeyId>,
    /// The DEK ids that fail to unwrap under the given KEK.
    pub failed_dek_ids: Vec<u64>,
}

/// Why a command stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyCommandError {
    /// The key state refuses the command; the message names the next step.
    Refused(String),
    /// The KEK file failed the file rule or did not parse.
    Kek(String),
    /// A store call failed.
    Store(OpError),
    /// The pass stopped.
    Pass(PassError),
}

impl std::fmt::Display for KeyCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(message) | Self::Kek(message) => f.write_str(message),
            Self::Store(e) => write!(f, "storage error: {}", e.message()),
            Self::Pass(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for KeyCommandError {}

impl From<OpError> for KeyCommandError {
    fn from(e: OpError) -> Self {
        Self::Store(e)
    }
}

impl From<PassError> for KeyCommandError {
    fn from(e: PassError) -> Self {
        Self::Pass(e)
    }
}

/// Section 8.5, rule 1: a catalog key row is an old key only while the ring's
/// `switch_from_catalog_key` is true and the row's id under the KEK equals
/// `retiring_catalog_key_id`. `ring` is authentic: every DEK unwrapped.
#[must_use]
pub fn catalog_key_is_retiring(
    ring: &KeyRing,
    kek: &[u8; KEY_LEN],
    catalog_key: &SecretKeys,
) -> bool {
    ring.switch_from_catalog_key
        && ring.retiring_catalog_key_id.as_ref() == Some(&catalog_key_id(kek, catalog_key))
}

fn refused(refusal: KeyRefusal) -> KeyCommandError {
    KeyCommandError::Refused(refusal.to_string())
}

fn refuse(message: impl Into<String>) -> KeyCommandError {
    KeyCommandError::Refused(message.into())
}

fn read_kek(file: &dyn KekFile) -> Result<Zeroizing<[u8; KEY_LEN]>, KeyCommandError> {
    file.read().map_err(KeyCommandError::Kek)
}

/// The ring row that a command needs, or the section 7 refusal for its
/// absence.
fn ring_row(rows: &KeyRows) -> Result<&str, KeyCommandError> {
    rows.ring.as_deref().ok_or_else(|| {
        refused(if rows.catalog_key.is_some() {
            KeyRefusal::CatalogKeyWithoutRing
        } else {
            KeyRefusal::NoRing
        })
    })
}

fn catalog_id(rows: &KeyRows) -> Result<&str, KeyCommandError> {
    rows.catalog_id.as_deref().ok_or_else(|| {
        refuse(
            "the catalog has no catalog_id row, which migration 6 writes. Run \
             `extenddb migrate --yes` first",
        )
    })
}

fn catalog_key(rows: &KeyRows) -> Result<Option<SecretKeys>, KeyCommandError> {
    rows.catalog_key
        .as_deref()
        .map(|key| {
            SecretKeys::catalog_key(key)
                .map_err(|reason| refuse(format!("the encryption_key row is not a key: {reason}")))
        })
        .transpose()
}

/// The refusal of a catalog key beside a finished ring, without
/// `--accept-catalog-key` (section 8.5).
fn catalog_key_not_accepted() -> KeyCommandError {
    refuse(format!(
        "{}. After reading that list, `--accept-catalog-key` re-encrypts those secrets under \
         the ring's DEK and deletes the row",
        KeyRefusal::CatalogKeyAfterSwitch
    ))
}

fn seal(
    kek: &[u8; KEY_LEN],
    catalog_id: &str,
    deks: &[&SecretKeys],
    switch_from_catalog_key: bool,
    retiring: Option<KeyId>,
) -> Result<KeyRing, KeyCommandError> {
    KeyRing::seal(kek, catalog_id, deks, switch_from_catalog_key, retiring)
        .map_err(|reason| refuse(format!("seal the key ring: {reason}")))
}

/// Write the ring row, with one audit line, as `extenddb settings set` does.
/// With `read`, the ring text the command read, the write lands only while
/// the row still holds that text: `rewrap` runs outside the migration lock,
/// so a `rotate` can write the ring between another command's read and its
/// write.
async fn write_ring(
    settings: &dyn SettingsStore,
    ring: &KeyRing,
    command: &str,
    read: Option<&str>,
) -> Result<(), KeyCommandError> {
    let text = ring.to_json();
    match read {
        None => settings.set_setting(KEY_RING_SETTING, &text).await?,
        Some(read)
            if settings
                .replace_setting(KEY_RING_SETTING, read, &text)
                .await? => {}
        Some(_) => {
            return Err(refuse(format!(
                "the key ring changed while `extenddb encryption-key {command}` ran, so nothing \
                 was written. Run the command again"
            )));
        }
    }
    tracing::warn!(
        target: "extenddb::audit::settings",
        "encryption-key {command}: wrote {KEY_RING_SETTING}, kek_id={}, dek_ids={}, \
         switch_from_catalog_key={}",
        ring.kek.id,
        join_ids(&ring.dek_ids()),
        ring.switch_from_catalog_key,
    );
    Ok(())
}

/// Step 3 of the pass: read the KEK file again before an old key goes, and
/// stop when it is gone or holds another KEK.
fn reread_kek(file: &dyn KekFile, ring: &KeyRing) -> Result<(), KeyCommandError> {
    let kek = file.read().map_err(|reason| {
        refuse(format!(
            "the KEK file cannot be read again before the old key is deleted: {reason}. Every \
             key is kept; run the command again"
        ))
    })?;
    let id = kek_id(&kek);
    if id != ring.kek.id {
        return Err(refuse(format!(
            "the KEK file now holds the KEK with id {id}, not {}, the KEK of the key ring. \
             Every key is kept; put the file back and run the command again",
            ring.kek.id
        )));
    }
    Ok(())
}

/// The check before a first write (section 8, common rules, and section 12).
fn check_before_write(
    survey: &Survey,
    flags: RunFlags,
    plaintext_data_connection: bool,
) -> Result<(), KeyCommandError> {
    let unopenable: Vec<SecretListing> = survey.unopenable().into_iter().cloned().collect();
    if !unopenable.is_empty() && !flags.keep_unopenable {
        return Err(PassError::Unopenable(unopenable).into());
    }
    match survey.data_connection {
        DataConnection::Absent | DataConnection::Opens(_) => Ok(()),
        DataConnection::Plaintext if plaintext_data_connection => Ok(()),
        DataConnection::Plaintext | DataConnection::Unopenable => Err(refuse(
            "the data_database_connection_string row opens under no key of this run. Run \
             `extenddb encryption-key status`",
        )),
    }
}

/// `None` without a connection string row, else `Some(false)`: for the
/// summary of a run that did not rewrite it.
fn untouched(data_connection: DataConnection) -> Option<bool> {
    (data_connection != DataConnection::Absent).then_some(false)
}

fn dek_id(keys: &SecretKeys) -> u64 {
    keys.dek_id().unwrap_or(0)
}

/// The last step of `switch`, `rotate` and `reset`: rebuild the catalog
/// file so that no free page keeps a ciphertext the run replaced (SQLite
/// only). Every row is written by then, and a run of the same command again
/// rebuilds the file, so a failure says both.
async fn erase_free_space(stores: KeyStores<'_>, command: &str) -> Result<(), KeyCommandError> {
    stores.management.erase_free_space().await.map_err(|e| {
        refuse(format!(
            "`extenddb encryption-key {command}` wrote every row, and then the rebuild of the \
             database file that erases the ciphertexts it replaced failed: {}. Run the same \
             command again",
            e.message()
        ))
    })
}

/// Delete the `encryption_key` row, then read it back and stop when it is
/// still there.
///
/// A store's `Ok` from a delete does not prove the row is gone. A run that
/// reported the catalog key deleted while the plaintext row stayed readable
/// would leave the operator with a key they believe is gone, so the run
/// stops instead, with every key kept, and runs again from there.
///
/// # Errors
///
/// [`KeyCommandError::Store`] from either call, or
/// [`KeyCommandError::Refused`] when the row survived the delete.
async fn delete_catalog_key(stores: KeyStores<'_>, command: &str) -> Result<(), KeyCommandError> {
    stores
        .management
        .delete_setting(CATALOG_KEY_SETTING)
        .await?;
    let read_back = stores.settings.get_setting(CATALOG_KEY_SETTING).await;
    let failed = match &read_back {
        Ok(None) => String::new(),
        Ok(Some(_)) => {
            return Err(refuse(
                "the catalog's encryption_key row is still there after the delete, so the \
                 catalog key was not removed. Every key is kept; run the command again",
            ));
        }
        // The delete ran and may have landed. A re-run that finds no row
        // deletes nothing, so this line is the only record of it.
        Err(e) => format!("; the read-back failed: {}", e.message()),
    };
    tracing::warn!(
        target: "extenddb::audit::settings",
        "encryption-key {command}: deleted {CATALOG_KEY_SETTING}{failed}"
    );
    read_back.map(drop).map_err(Into::into)
}

/// Delete the rows of `table` that `keys` names, a page at a time, then read
/// one row back and stop when the table still holds one. A row that remains
/// is a delete that did not land, or a row a server wrote after the deletes.
/// Either stops the run, as [`delete_catalog_key`] describes.
///
/// # Errors
///
/// [`KeyCommandError::Store`] from either call, or
/// [`KeyCommandError::Refused`] when a row survived the deletes.
async fn delete_secret_rows(
    stores: KeyStores<'_>,
    table: SecretTable,
    keys: &[String],
) -> Result<(), KeyCommandError> {
    for batch in keys.chunks(PAGE_SIZE) {
        stores.management.delete_secrets(table, batch).await?;
    }
    let page = stores.management.read_secret_page(table, None, 1).await?;
    if !page.rows.is_empty() {
        return Err(refuse(format!(
            "the catalog's {} table still holds a row after the delete. Every key is kept; run \
             the command again",
            table.name()
        )));
    }
    Ok(())
}

/// `extenddb encryption-key status`: the key source, the KEK id, the DEK ids
/// and the stored secrets per key. Writes nothing. A ring whose DEKs fail to
/// unwrap is reported with its fields marked as not authenticated.
///
/// # Errors
///
/// A store error, or a KEK file that fails.
pub async fn status(
    stores: KeyStores<'_>,
    kek: Option<&dyn KekFile>,
) -> Result<StatusReport, KeyCommandError> {
    let kek = kek.map(read_kek).transpose()?;
    let rows = KeyRows::read(stores.settings).await?;
    let catalog_key = catalog_key(&rows)?;
    let mut opened: Option<Vec<SecretKeys>> = None;
    let ring = match rows.ring.as_deref() {
        None => None,
        Some(text) => {
            let ring = KeyRing::parse(text).map_err(|reason| {
                refused(KeyRefusal::DamagedRing {
                    reason: format!("the ring does not parse: {reason}"),
                })
            })?;
            let mut failed_dek_ids = Vec::new();
            if let (Some(kek), Some(catalog_id)) = (&kek, rows.catalog_id.as_deref()) {
                match ring.unwrap(kek, catalog_id) {
                    Ok(deks) => opened = Some(deks),
                    Err(failed) => failed_dek_ids = failed,
                }
            }
            Some(RingStatus {
                given_kek_id: kek.as_ref().map(|kek| kek_id(kek)),
                ring,
                failed_dek_ids,
            })
        }
    };
    // The target is the ring's active DEK when the ring opens, else the
    // catalog key; every other key the run holds is an old key.
    let keys = match (&opened, &catalog_key) {
        (Some(deks), catalog_key) => {
            let (active, older) = deks.split_last().expect("an open ring holds a DEK");
            let old = older.iter().chain(catalog_key.as_ref()).collect();
            Some(PassKeys {
                target: active,
                old,
            })
        }
        (None, Some(catalog_key)) => Some(PassKeys {
            target: catalog_key,
            old: Vec::new(),
        }),
        (None, None) => None,
    };
    let survey = match keys {
        Some(keys) => Some(survey(stores.management, stores.settings, &keys).await?),
        None => None,
    };
    Ok(StatusReport {
        catalog_key_present: rows.catalog_key.is_some(),
        ring,
        survey,
    })
}
