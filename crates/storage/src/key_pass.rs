// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The pass over the stored secrets (RFC-0000 section 8.4), and the survey
//! that `status`, the previews and the check before a first write read.
//!
//! No stored byte records which key encrypted a secret, so the pass finds
//! each secret's key by trial: the target first, then each old key in turn.
//! A secret that opens under the target is done. A secret that opens under an
//! old key is re-encrypted under the target, with the AAD of section 5. Step 3
//! reads every row again and stops when a secret still opens under an old
//! key: a store write that matches no row reports success, and so does a
//! Cassandra update whose write timestamp is not above the row's. A row that
//! is gone passes.
//!
//! The driver runs over [`ManagementStore`] and [`SettingsStore`] only, as
//! the pass of migration 2 does ([`crate::policy_migration`]).

use std::collections::HashSet;

use zeroize::Zeroizing;

use crate::management_store::{
    ManagementStore, OpError, SecretPage, SecretWrite, SettingsStore, StoredSecretRow,
};
use crate::secret_keys::{DATA_CONNECTION_SETTING, SecretKeys, SecretTable};

/// The most rows of one page read and one batch write.
pub const PAGE_SIZE: usize = 1000;

/// The keys of one run: the target, then the old keys in the order the
/// trial tries them.
pub struct PassKeys<'a> {
    /// The key every stored secret is under when the pass finishes.
    pub target: &'a SecretKeys,
    /// The keys the pass re-encrypts from: the catalog key in a switch, the
    /// older DEK in a rotate, and both in a rotate with `--accept-catalog-key`.
    pub old: Vec<&'a SecretKeys>,
}

impl PassKeys<'_> {
    /// The name of each key of the run: `[0]` for [`Opener::Target`], then
    /// one per [`Opener::Old`] at `[i + 1]`: "the catalog key" or "DEK 2".
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        std::iter::once(self.target)
            .chain(self.old.iter().copied())
            .map(key_name)
            .collect()
    }
}

/// "the catalog key", or "DEK <id>".
#[must_use]
pub fn key_name(keys: &SecretKeys) -> String {
    match keys.dek_id() {
        None => "the catalog key".to_owned(),
        Some(id) => format!("DEK {id}"),
    }
}

/// Which key of a [`PassKeys`] opens a stored secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Opener {
    /// [`PassKeys::target`].
    Target,
    /// [`PassKeys::old`] at this index.
    Old(usize),
    /// No key of the run.
    None,
}

/// Find the key that opens `row`'s secret, target first, and wipe the
/// plaintext at once.
#[must_use]
pub fn open_by_trial(keys: &PassKeys<'_>, row: &StoredSecretRow) -> Opener {
    trial(keys, row).0
}

/// [`open_by_trial`], keeping the plaintext of a secret under an old key,
/// which the pass re-encrypts.
fn trial(keys: &PassKeys<'_>, row: &StoredSecretRow) -> (Opener, Option<Zeroizing<String>>) {
    let aad_row = row.aad_row();
    if keys
        .target
        .decrypt(&aad_row, &row.secret_key_encrypted)
        .is_ok()
    {
        return (Opener::Target, None);
    }
    for (index, key) in keys.old.iter().enumerate() {
        if let Ok(plaintext) = key.decrypt(&aad_row, &row.secret_key_encrypted) {
            return (Opener::Old(index), Some(plaintext));
        }
    }
    (Opener::None, None)
}

/// Each row of `table`, page by page; `each` returns the writes for its page,
/// which go out as one batch.
pub(crate) async fn for_each_page(
    store: &dyn ManagementStore,
    table: SecretTable,
    mut each: impl FnMut(Vec<StoredSecretRow>) -> Result<Vec<SecretWrite>, PassError>,
) -> Result<(), PassError> {
    let mut page: Option<String> = None;
    loop {
        let SecretPage { rows, next } = store
            .read_secret_page(table, page.as_deref(), PAGE_SIZE)
            .await
            .map_err(PassError::Store)?;
        let writes = each(rows)?;
        if !writes.is_empty() {
            store
                .write_secrets(table, &writes)
                .await
                .map_err(PassError::Store)?;
        }
        match next {
            Some(next) => page = Some(next),
            None => return Ok(()),
        }
    }
}

fn listing(row: &StoredSecretRow, opener: Opener) -> SecretListing {
    SecretListing {
        table: row.table,
        access_key_id: row.access_key_id.clone(),
        account_id: row.account_id.clone(),
        principal_name: row.principal_name.clone(),
        created_at: row.created_at,
        opener,
    }
}

/// `n` access key IDs as one line.
pub(crate) fn access_key_ids(listings: &[SecretListing]) -> String {
    listings
        .iter()
        .map(|s| s.access_key_id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// One stored secret in a listing: never the secret, and never the row's
/// primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretListing {
    /// The table that holds the row.
    pub table: SecretTable,
    /// The access key ID.
    pub access_key_id: String,
    /// The account of the owner.
    pub account_id: String,
    /// The user or role that owns the secret.
    pub principal_name: String,
    /// When the row was created, when the backend stores it.
    pub created_at: Option<time::OffsetDateTime>,
    /// The key that opens the secret.
    pub opener: Opener,
}

/// The counts of one table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableCounts {
    /// Rows under the target.
    pub target: u64,
    /// Rows under each old key, by the index of [`PassKeys::old`].
    pub old: Vec<u64>,
    /// Rows that no key of the run opens.
    pub unopenable: u64,
    /// Rows the pass re-encrypted under the target.
    pub rewritten: u64,
}

impl TableCounts {
    fn new(keys: &PassKeys<'_>) -> Self {
        Self {
            old: vec![0; keys.old.len()],
            ..Self::default()
        }
    }

    fn count(&mut self, opener: Opener) {
        match opener {
            Opener::Target => self.target += 1,
            Opener::Old(index) => self.old[index] += 1,
            Opener::None => self.unopenable += 1,
        }
    }
}

fn table_counts(keys: &PassKeys<'_>) -> [(SecretTable, TableCounts); 2] {
    SecretTable::ALL.map(|table| (table, TableCounts::new(keys)))
}

/// The state of the PostgreSQL `data_database_connection_string` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataConnection {
    /// The catalog has no such row: every backend but PostgreSQL.
    Absent,
    /// The row opens under this key of the run.
    Opens(Opener),
    /// The row opens under no key and is a connection string in plaintext:
    /// the form before a switch (section 12).
    Plaintext,
    /// The row opens under no key and is not a connection string.
    Unopenable,
}

/// What a read-only survey of the stored secrets found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Survey {
    /// The name of each key of the run: `[0]` for [`Opener::Target`], then
    /// one per [`Opener::Old`] at `[i + 1]` ([`PassKeys::names`]).
    pub key_names: Vec<String>,
    /// The counts of `access_keys` and `iam_sessions`, in [`SecretTable::ALL`] order.
    pub tables: [(SecretTable, TableCounts); 2],
    /// Each stored secret under a key other than the target, unopenable
    /// ones included.
    pub not_target: Vec<SecretListing>,
    /// The connection string row.
    pub data_connection: DataConnection,
}

impl Survey {
    /// The secrets that no key of the run opens.
    #[must_use]
    pub fn unopenable(&self) -> Vec<&SecretListing> {
        self.not_target
            .iter()
            .filter(|s| s.opener == Opener::None)
            .collect()
    }
}

/// Open every `access_keys` and `iam_sessions` secret, and the connection
/// string row, with the keys of the run, and wipe each plaintext at once.
/// Writes nothing. `status`, the previews, and the check before the first
/// write of `switch --yes` and `rotate --yes` read this.
///
/// # Errors
///
/// A store error.
pub async fn survey(
    store: &dyn ManagementStore,
    settings: &dyn SettingsStore,
    keys: &PassKeys<'_>,
) -> Result<Survey, OpError> {
    let mut tables = table_counts(keys);
    let mut not_target = Vec::new();
    for (table, counts) in &mut tables {
        let read = for_each_page(store, *table, |rows| {
            for row in &rows {
                let opener = open_by_trial(keys, row);
                counts.count(opener);
                if opener != Opener::Target {
                    not_target.push(listing(row, opener));
                }
            }
            Ok(Vec::new())
        })
        .await;
        match read {
            Ok(()) => {}
            Err(PassError::Store(e)) => return Err(e),
            Err(other) => return Err(OpError::Internal(other.to_string())),
        }
    }
    let (data_connection, _) = data_connection(settings, keys).await?;
    Ok(Survey {
        key_names: keys.names(),
        tables,
        not_target,
        data_connection,
    })
}

/// The state of the connection string row, and its plaintext when it opens
/// under an old key or is in the plaintext form.
async fn data_connection(
    settings: &dyn SettingsStore,
    keys: &PassKeys<'_>,
) -> Result<(DataConnection, Option<Zeroizing<String>>), OpError> {
    let Some(value) = settings
        .get_setting(DATA_CONNECTION_SETTING)
        .await?
        .map(Zeroizing::new)
    else {
        return Ok((DataConnection::Absent, None));
    };
    if keys.target.decrypt_setting(&value).is_ok() {
        return Ok((DataConnection::Opens(Opener::Target), None));
    }
    for (index, key) in keys.old.iter().enumerate() {
        if let Ok(plaintext) = key.decrypt_setting(&value) {
            return Ok((DataConnection::Opens(Opener::Old(index)), Some(plaintext)));
        }
    }
    if is_connection_string(&value) {
        Ok((DataConnection::Plaintext, Some(value)))
    } else {
        Ok((DataConnection::Unopenable, None))
    }
}

/// Whether `value` is a PostgreSQL connection string in the URL form that
/// `init` and migration 1 write.
fn is_connection_string(value: &str) -> bool {
    ["postgres://", "postgresql://"]
        .iter()
        .any(|scheme| value.len() > scheme.len() && value.starts_with(scheme))
}

/// The options of a pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PassOptions {
    /// `--keep-unopenable`: leave the secrets that no key opens as they are
    /// and list them, instead of stopping.
    pub keep_unopenable: bool,
    /// A switch: a connection string row that does not open under the
    /// target is the plaintext form, which the pass encrypts (section 12).
    /// Every other run refuses that row.
    pub plaintext_data_connection: bool,
}

/// What a pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassSummary {
    /// The name of each key of the run ([`PassKeys::names`]).
    pub key_names: Vec<String>,
    /// The counts of each table after the pass, in [`SecretTable::ALL`]
    /// order: each secret the pass re-encrypted counts under the target.
    pub tables: [(SecretTable, TableCounts); 2],
    /// The secrets that no key opens, kept as they are.
    pub unopenable: Vec<SecretListing>,
    /// `None` without a connection string row, else whether the pass
    /// rewrote it.
    pub data_connection_rewritten: Option<bool>,
}

/// Why a pass stopped. Every key is kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassError {
    /// A store call failed.
    Store(OpError),
    /// The cipher failed on an encrypt.
    Cipher(String),
    /// Secrets that no key opens, without `--keep-unopenable`. Found before
    /// the first write.
    Unopenable(Vec<SecretListing>),
    /// Step 3: secrets that still open under an old key after their write,
    /// or that no key opens any more.
    NotRewritten(Vec<SecretListing>),
    /// The connection string row does not open, or its plaintext form does
    /// not parse.
    DataConnection(String),
}

impl std::fmt::Display for PassError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(e) => write!(f, "storage error: {}", e.message()),
            Self::Cipher(reason) => write!(f, "encrypt a stored secret: {reason}"),
            Self::Unopenable(listings) => write!(
                f,
                "{} stored secrets open under no key of this run: {}. Run \
                 `extenddb encryption-key status`, or pass --keep-unopenable to leave them as \
                 they are",
                listings.len(),
                access_key_ids(listings)
            ),
            Self::NotRewritten(listings) => write!(
                f,
                "{} stored secrets do not open under the target key after their write: {}. \
                 A store write reported success and changed nothing. Every key is kept; run \
                 the command again",
                listings.len(),
                access_key_ids(listings)
            ),
            Self::DataConnection(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for PassError {}

/// Run the pass of section 8.4: open each row by trial, re-encrypt under the
/// target each secret that opens under an old key, in batches of
/// [`PAGE_SIZE`] by primary key, then read every row again (step 3). On
/// PostgreSQL it also handles the connection string row by the rules of
/// section 12.
///
/// The caller holds the migration lock, ran the check before its first
/// write ([`survey`]), and after this returns re-reads the KEK file before
/// any old key goes.
///
/// # Errors
///
/// [`PassError`]; a run that stops runs again with the same arguments.
pub async fn run_pass(
    store: &dyn ManagementStore,
    settings: &dyn SettingsStore,
    keys: &PassKeys<'_>,
    options: PassOptions,
) -> Result<PassSummary, PassError> {
    // Steps 1 and 2: trial, then one batch write per page.
    let mut tables = table_counts(keys);
    let mut unopenable = Vec::new();
    let mut kept: HashSet<(SecretTable, String)> = HashSet::new();
    for (table, counts) in &mut tables {
        for_each_page(store, *table, |rows| {
            let mut writes = Vec::new();
            for row in &rows {
                let (opener, plaintext) = trial(keys, row);
                counts.count(opener);
                match (opener, plaintext) {
                    (Opener::Old(_), Some(plaintext)) => {
                        let sealed = keys
                            .target
                            .encrypt(&row.aad_row(), &plaintext)
                            .map_err(PassError::Cipher)?;
                        counts.rewritten += 1;
                        writes.push(SecretWrite {
                            key: row.key.clone(),
                            secret_key_encrypted: sealed,
                            write_timestamp: row.write_timestamp,
                        });
                    }
                    (Opener::None, _) => {
                        unopenable.push(listing(row, opener));
                        kept.insert((row.table, row.key.clone()));
                    }
                    _ => {}
                }
            }
            if !options.keep_unopenable && !unopenable.is_empty() {
                return Err(PassError::Unopenable(std::mem::take(&mut unopenable)));
            }
            Ok(writes)
        })
        .await?;
    }
    let data_connection_rewritten = rewrite_data_connection(settings, keys, options).await?;

    // Step 3: every row again. A row that is gone passes.
    let mut not_rewritten = Vec::new();
    for table in SecretTable::ALL {
        for_each_page(store, table, |rows| {
            for row in &rows {
                match open_by_trial(keys, row) {
                    Opener::Target => {}
                    Opener::None if kept.contains(&(row.table, row.key.clone())) => {}
                    opener => not_rewritten.push(listing(row, opener)),
                }
            }
            Ok(Vec::new())
        })
        .await?;
    }
    if !not_rewritten.is_empty() {
        return Err(PassError::NotRewritten(not_rewritten));
    }
    if data_connection_rewritten.is_some()
        && data_connection(settings, keys)
            .await
            .map_err(PassError::Store)?
            .0
            != DataConnection::Opens(Opener::Target)
    {
        return Err(PassError::DataConnection(
            "the data_database_connection_string row does not open under the target key after \
             its write. Every key is kept; run the command again"
                .to_owned(),
        ));
    }
    // The counts after the pass. Step 3 read every row again and found each
    // one under the target or kept as unopenable, so each secret that steps
    // 1 and 2 re-encrypted is under the target now, and none is under an old
    // key.
    for (_, counts) in &mut tables {
        counts.target += counts.rewritten;
        counts.old.fill(0);
    }
    Ok(PassSummary {
        key_names: keys.names(),
        tables,
        unopenable,
        data_connection_rewritten,
    })
}

/// Steps 1 and 2 for the connection string row: `None` without the row,
/// else whether the pass rewrote it.
async fn rewrite_data_connection(
    settings: &dyn SettingsStore,
    keys: &PassKeys<'_>,
    options: PassOptions,
) -> Result<Option<bool>, PassError> {
    let (state, plaintext) = data_connection(settings, keys)
        .await
        .map_err(PassError::Store)?;
    let plaintext = match (state, plaintext) {
        (DataConnection::Absent, _) => return Ok(None),
        (DataConnection::Opens(Opener::Target), _) => return Ok(Some(false)),
        (DataConnection::Plaintext, _) if !options.plaintext_data_connection => {
            return Err(PassError::DataConnection(
                "the data_database_connection_string row is not encrypted under a DEK of the \
                 key ring. Run `extenddb encryption-key set-data-connection` to write it"
                    .to_owned(),
            ));
        }
        (_, Some(plaintext)) => plaintext,
        (_, None) => {
            return Err(PassError::DataConnection(
                "the data_database_connection_string row opens under no key of this run and is \
                 not a connection string. Run `extenddb encryption-key status`"
                    .to_owned(),
            ));
        }
    };
    let sealed = keys
        .target
        .encrypt_setting(&plaintext)
        .map_err(PassError::Cipher)?;
    settings
        .set_setting(DATA_CONNECTION_SETTING, &sealed)
        .await
        .map_err(PassError::Store)?;
    Ok(Some(true))
}
