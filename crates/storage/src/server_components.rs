// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend factory infrastructure for creating server components.
//!
//! This module provides the factory pattern for creating storage backends.
//! A binary serves exactly one backend, installed from its thin `main` via
//! [`crate::set_backend`]; this factory is reached through the installed
//! [`crate::Backend`] value, which is what keeps `cmd_serve` backend-agnostic
//! without a registry to look anything up in.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use extenddb_auth::CredentialStore;

use crate::config::StorageConfig;
use crate::hooks::ServerRuntimeHooks;
use crate::key_ring::{Kek, KeyRefusal, KeyRing, KeyRows, open_ring};
use crate::management_store::SettingsStore;
use crate::secret_keys::SecretKeys;
use crate::{CatalogStore, StorageEngine};

/// Components needed to run the extenddb server.
///
/// Returned by backend factories. Contains all the trait objects needed
/// by `cmd_serve` to start the HTTP server and spawn workers.
pub struct ServerComponents {
    /// Storage engine implementing all data/metadata operations
    pub engine: Arc<dyn StorageEngine>,

    /// Catalog store for management API operations
    pub catalog_store: Arc<dyn CatalogStore>,

    /// Raw (uncached) credential store. The bin layer wraps this in
    /// `CachedCredentialStore` using the operator-configured TTL before
    /// constructing the auth provider.
    pub credential_store: Arc<dyn CredentialStore>,

    /// Optional backend-specific runtime hooks for worker spawning
    pub runtime_hooks: Option<Box<dyn ServerRuntimeHooks>>,
}

/// Errors that can occur during backend initialization.
#[derive(Debug)]
pub enum BackendError {
    /// No storage backend has been installed (set_backend was not called).
    BackendNotInstalled,

    /// Failed to connect to backend database
    ConnectionFailed { backend: String, details: String },

    /// Encryption key not found in settings table
    MissingEncryptionKey,

    /// The encryption key in the settings table is one the secret cipher
    /// refuses: it is not base64, or it does not decode to 32 bytes. Carries
    /// the cipher's reason.
    InvalidEncryptionKey(String),

    /// Generic initialization failure
    InitializationFailed(String),

    /// The catalog's status refuses this server, or a migration the factory
    /// ran failed; carries the exit code `serve` ends with.
    Catalog(crate::migrations::MigrationError),
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendNotInstalled => {
                write!(
                    f,
                    "no storage backend installed (set_backend was not called)"
                )
            }
            Self::ConnectionFailed { backend, details } => {
                write!(f, "Failed to connect to {backend}: {details}")
            }
            Self::MissingEncryptionKey => write!(
                f,
                "Encryption key not found in settings table. Run 'extenddb init'"
            ),
            Self::InvalidEncryptionKey(reason) => write!(
                f,
                "Encryption key in settings table is not valid: {}. Restore the \
                 original key: every stored access key and session secret is encrypted \
                 under it",
                reason.trim_end_matches('.')
            ),
            Self::InitializationFailed(msg) => write!(f, "Backend initialization failed: {msg}"),
            Self::Catalog(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BackendError {}

/// The key a factory loads (RFC-0000 section 7). `serve` and each command
/// pass it, so no code in this crate reads a file.
#[derive(Clone, Default)]
pub enum KeySource {
    /// `key_file` is not set: the catalog key of the `encryption_key` row.
    #[default]
    CatalogKey,
    /// `key_file` is set: the KEK that the caller read from it through the
    /// file rule. The loader unwraps the ring's DEK with it and drops it,
    /// which wipes it when it holds the last reference. A move or a
    /// `mem::take` of the source copies a pointer and leaves the key where
    /// it is.
    Kek(Kek),
    /// No key: the stores refuse to create, import or store a credential.
    /// For callers that write no credential and read no stored secret.
    None,
}

impl std::fmt::Debug for KeySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::CatalogKey => "CatalogKey",
            Self::Kek(_) => "Kek(..)",
            Self::None => "None",
        })
    }
}

/// Two KEK sources are equal only when their bytes match, compared in
/// constant time (`CRYPTO_memcmp` through `aws-lc-rs`).
impl PartialEq for KeySource {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::CatalogKey, Self::CatalogKey) | (Self::None, Self::None) => true,
            (Self::Kek(a), Self::Kek(b)) => {
                aws_lc_rs::constant_time::verify_slices_are_equal(&a[..], &b[..]).is_ok()
            }
            _ => false,
        }
    }
}

impl Eq for KeySource {}

/// Load the key of a store from the catalog's key rows (RFC-0000 section 7).
///
/// Each backend factory calls this once, in place of its own key query, with
/// a settings store over the catalog and the [`ServerComponentsOptions::key_source`]
/// it was given (take it with `std::mem::take`, so the KEK is wiped here). It
/// reads the `encryption_key`, `encryption_key_ring` and `catalog_id` rows
/// through [`SettingsStore::get_setting`], then applies the rows of the
/// section 7 table in order, and the first row that matches decides. With a
/// KEK it compares the KEK id first and unwraps every DEK second, and reads
/// no other ring field until all of them unwrap.
///
/// Returns the one key holder that the backend's stores share, or `None` for
/// [`KeySource::None`], which reads nothing.
///
/// # Errors
///
/// [`BackendError::MissingEncryptionKey`] for a catalog with neither key row
/// and no `key_file`, [`BackendError::InvalidEncryptionKey`] for a catalog key
/// that the cipher refuses, and [`BackendError::InitializationFailed`] with the
/// table row's message for every other refusal and for a failed read.
pub async fn load_secret_keys(
    settings: &dyn SettingsStore,
    source: KeySource,
) -> Result<Option<Arc<SecretKeys>>, BackendError> {
    let kek = match source {
        KeySource::None => return Ok(None),
        KeySource::CatalogKey => None,
        KeySource::Kek(kek) => Some(kek),
    };
    let rows = KeyRows::read(settings).await.map_err(|e| {
        BackendError::InitializationFailed(format!("read the encryption key rows: {}", e.message()))
    })?;
    let refused = |refusal: KeyRefusal| BackendError::InitializationFailed(refusal.to_string());
    let keys = match (kek, rows.ring.as_deref()) {
        (None, Some(text)) => {
            // No KEK to check the ring with: name only the next step, whose
            // command checks the ring before it writes.
            return Err(refused(match KeyRing::parse(text) {
                Err(reason) => KeyRefusal::DamagedRing {
                    reason: format!("the ring does not parse: {reason}"),
                },
                Ok(ring) if ring.switch_from_catalog_key => {
                    KeyRefusal::UnfinishedSwitchWithoutKeyFile
                }
                Ok(ring) => KeyRefusal::RingNeedsKeyFile {
                    kek_id: ring.kek.id,
                },
            }));
        }
        (None, None) => {
            let key = rows
                .catalog_key
                .as_deref()
                .ok_or(BackendError::MissingEncryptionKey)?;
            SecretKeys::catalog_key(key).map_err(BackendError::InvalidEncryptionKey)?
        }
        (Some(_), None) if rows.catalog_key.is_some() => {
            return Err(refused(KeyRefusal::CatalogKeyWithoutRing));
        }
        (Some(_), None) => return Err(refused(KeyRefusal::NoRing)),
        (Some(kek), Some(text)) => {
            let mut open = open_ring(text, rows.catalog_id.as_deref(), &kek).map_err(refused)?;
            if open.ring.switch_from_catalog_key {
                return Err(refused(KeyRefusal::UnfinishedSwitchWithKeyFile));
            }
            if rows.catalog_key.is_some() {
                return Err(refused(KeyRefusal::CatalogKeyAfterSwitch));
            }
            if open.deks.len() > 1 {
                return Err(refused(KeyRefusal::UnfinishedRotation));
            }
            open.deks.pop().expect("an open ring holds a DEK")
        }
    };
    Ok(Some(Arc::new(keys)))
}

/// Options for server-component creation, beyond the storage config itself.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ServerComponentsOptions {
    /// Bootstrap the catalog at serve time when it is not initialized,
    /// instead of failing with `InitializationFailed`. Set by dev-mode builds
    /// so `serve` works with no prior `init` (zero-config local/CI use).
    /// Backends whose bootstrap requires operator input ignore this and fail
    /// as usual; backends with a self-contained bootstrap (SQLite) honor it.
    pub bootstrap_if_uninitialized: bool,
    /// The item size limit the storage layer applies to the finished item of
    /// an update, standalone or transacted. Comes from `limits.max_item_size_bytes`
    /// so the storage layer and the engine's put path enforce one figure.
    pub max_item_size_bytes: usize,
    /// The ExtendDB release that is starting, recorded in the ledger rows of
    /// migrations the factory runs (a dev build, an in-memory database).
    pub release: &'static str,
    /// `backup.max_concurrent_jobs`: the backup and restore jobs the
    /// instance runs at once. A backend whose pool has to admit every job
    /// sizes it from this ([`crate::backup_catalog::pool_size_for_jobs`]);
    /// `0` (the default outside a server) asks for no job capacity.
    pub max_concurrent_jobs: usize,
    /// The caller is a command that reads the catalog beside a running
    /// server (`extenddb backup list`), not a server: no serve lock is taken
    /// (on SQLite the file is opened as another reader of a served
    /// database) and none of the startup recovery a server runs (see
    /// `startup_recovery`, which this implies off), since the server that
    /// owns the catalog does that. A command that writes (`backup purge`)
    /// leaves this `false`: on SQLite it takes the serve lock, as a server
    /// does.
    pub beside_server: bool,
    /// Whether the factory runs the startup recovery a server runs
    /// (interrupted control-plane transitions, incomplete indexes,
    /// PostgreSQL's GSI queue workers, MongoDB's repair of documents stored
    /// without their key, SQLite's expiry of interrupted restores and of a
    /// left-behind maintenance claim). `true` for a server; a command that
    /// writes (`backup purge`) sets it `false` beside `beside_server =
    /// false`, so on PostgreSQL, MongoDB and Cassandra it runs none of that
    /// work beside the live servers and exits with none of it half done,
    /// and on SQLite it holds the serve lock and resolves what it needs
    /// itself. Read only when `beside_server` is `false`.
    pub startup_recovery: bool,
    /// The command that opens the catalog, when it is not `serve`
    /// (`extenddb backup list`, `extenddb backup purge`). A backend whose
    /// factory takes a serving registration names it after the command
    /// (PostgreSQL's session `application_name`, MongoDB's and Cassandra's
    /// `extenddb_servers` entry), which a migration waiting for the
    /// registrations prints; `None` is `extenddb serve`.
    pub command: Option<&'static str>,
    /// The key the factory loads with [`load_secret_keys`]. The default is
    /// the catalog key, as before the key ring.
    pub key_source: KeySource,
}

impl Default for ServerComponentsOptions {
    fn default() -> Self {
        Self {
            bootstrap_if_uninitialized: false,
            max_item_size_bytes: extenddb_core::limits::LimitsConfig::default().max_item_size_bytes,
            release: "",
            max_concurrent_jobs: 0,
            beside_server: false,
            startup_recovery: true,
            command: None,
            key_source: KeySource::CatalogKey,
        }
    }
}

/// Factory function type for creating server components.
///
/// Takes a `StorageConfig` trait object, region string, and options; returns
/// a Future that resolves to `ServerComponents` or `BackendError`.
///
/// The trait object is bound `+ 'static` (rather than the reference's own
/// elided lifetime) so factories can use [`StorageConfig::as_any`] to
/// downcast to a concrete backend config and read backend-specific settings
/// (e.g. the mongo backend's `transaction_read_concern`) — `Any::downcast_ref`
/// requires the pointee to be provably `'static`, which an unannotated `&dyn
/// StorageConfig` does not guarantee even though every real implementor owns
/// its data.
pub type ServerComponentsFactory =
    fn(
        &(dyn StorageConfig + 'static),
        &str,
        ServerComponentsOptions,
    ) -> Pin<Box<dyn Future<Output = Result<ServerComponents, BackendError>> + Send>>;

/// Create server components using the installed backend.
///
/// Calls the factory of the [`Backend`](crate::Backend) installed via
/// [`set_backend`](crate::set_backend). Returns `BackendNotInstalled` if no
/// backend has been installed.
///
/// A `region` the `BackupArn` parser would refuse is logged at warn first
/// ([`crate::config::server_region_warning`]): the server still starts, but
/// the operator is told `CreateBackup` is refused under it.
pub async fn create_server_components(
    config: &(dyn StorageConfig + 'static),
    region: &str,
    options: ServerComponentsOptions,
) -> Result<ServerComponents, BackendError> {
    if let Some(warning) = crate::config::server_region_warning(region) {
        tracing::warn!("{}", warning.log_message());
    }
    let backend = crate::backend::try_backend().ok_or(BackendError::BackendNotInstalled)?;
    (backend.server_components)(config, region, options).await
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    use super::*;

    /// Collects the message of every warn-level event.
    #[derive(Default)]
    struct Warnings(Arc<Mutex<Vec<String>>>);

    struct Message(String);

    impl Visit for Message {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                let _ = write!(self.0, "{value:?}");
            }
        }
    }

    impl<S: Subscriber> Layer<S> for Warnings {
        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            if *event.metadata().level() == Level::WARN {
                let mut message = Message(String::new());
                event.record(&mut message);
                self.0.lock().unwrap().push(message.0);
            }
        }
    }

    #[derive(Debug)]
    struct NoConfig;

    impl StorageConfig for NoConfig {
        fn connection_config(&self) -> &str {
            ""
        }
        fn max_connections(&self) -> u32 {
            1
        }
        fn max_catalog_connections(&self) -> u32 {
            1
        }
        fn clone_box(&self) -> Box<dyn StorageConfig> {
            Box::new(NoConfig)
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// No backend is installed in this crate's tests, so the call fails
    /// after the region check; the warning is what is under test.
    async fn warnings_for(region: &str) -> Vec<String> {
        let warnings = Warnings::default();
        let collected = Arc::clone(&warnings.0);
        let subscriber = tracing_subscriber::registry().with(warnings);
        let _guard = tracing::subscriber::set_default(subscriber);
        let result =
            create_server_components(&NoConfig, region, ServerComponentsOptions::default()).await;
        assert!(matches!(result, Err(BackendError::BackendNotInstalled)));
        collected.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn a_region_outside_the_backup_arn_class_is_warned_about_at_startup() {
        let warnings = warnings_for("LOCAL").await;
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("server.region"), "{warnings:?}");
        assert!(warnings[0].contains("LOCAL"), "{warnings:?}");
        assert!(
            warnings[0].contains("BackupArn is not valid"),
            "{warnings:?}"
        );
        assert!(warnings_for("us-east-1").await.is_empty());
    }

    /// A key source is smaller than a key, so it cannot hold the KEK's bytes:
    /// a move of it, or the `mem::take` of a factory, copies a pointer to the
    /// one allocation of the KEK, and a clone shares that allocation.
    #[test]
    fn a_key_source_holds_a_pointer_to_the_kek() {
        assert!(std::mem::size_of::<KeySource>() < crate::secret_cipher::KEY_LEN);
        let kek =
            crate::key_ring::parse_kek_file(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n")
                .expect("a KEK file");
        let source = KeySource::Kek(Arc::clone(&kek));
        let KeySource::Kek(held) = source.clone() else {
            panic!("a KEK source");
        };
        assert!(Arc::ptr_eq(&held, &kek));
    }

    #[test]
    fn a_key_the_cipher_refuses_is_an_invalid_encryption_key() {
        // The error that `load_secret_keys` gives a catalog key that does
        // not load.
        let invalid = |key: &str| {
            SecretKeys::catalog_key(key)
                .map(drop)
                .map_err(BackendError::InvalidEncryptionKey)
                .unwrap_err()
                .to_string()
        };
        // 16 zero bytes.
        assert_eq!(
            invalid("AAAAAAAAAAAAAAAAAAAAAA=="),
            "Encryption key in settings table is not valid: encryption key must be 32 \
             bytes. Restore the original key: every stored access key and session \
             secret is encrypted under it"
        );
        // The base64 decoder's reason ends with a period of its own.
        let err = invalid("AAAA-AAA");
        assert!(err.contains("offset 4. Restore"), "{err}");
    }
}
