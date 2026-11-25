// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb encryption-key`: the key commands of RFC-0000 section 8.
//!
//! `status`, `rewrap` and `set-data-connection` may run beside the server.
//! `switch`, `rotate` and `reset` run while every server is stopped: they take
//! the migration lock as `migrate --yes` does, wait up to `--lock-timeout`
//! for it, and exit 6 when it stays held. Without `--yes` a command prints
//! what it would do and writes nothing.
//!
//! The checks and the writes are the storage crate's
//! ([`extenddb_storage::key_commands`]). This module reads the configuration
//! and the KEK file (through the file rule), sets up the locked key store,
//! opens the stores, and prints what a command found or did. Nothing it
//! prints holds a secret, a key or a connection string.
//!
//! Exit status: 0 done, nothing to do, or a preview; 1 refused or failed; 6
//! the lock was not free within `--lock-timeout`; 3, 4 or 5 a catalog whose
//! migrations are not current, with the code `migrate` gives it. On
//! PostgreSQL `status` does not check the migrations, and `rewrap` and
//! `set-data-connection` check those of the catalog database.

use std::time::Duration;

use clap::{Args, Subcommand};
use extenddb_server::key_file::{COMMAND_SLOTS, KeyFile, init_key_store};
use extenddb_storage::key_commands::{self, KekFile, KeyCommandError, KeyStores, RunFlags};
use extenddb_storage::management_store::{ManagementStore, SettingsStore};
use extenddb_storage::migrations::{CatalogMigrations, CatalogStatus, MigrationError, Target};
use extenddb_storage::server_components::KeySource;
use zeroize::Zeroizing;

use text::{outcome_text, status_text};

#[derive(Args)]
pub struct EncryptionKeyArgs {
    #[command(subcommand)]
    command: Command,
}

/// The option every command takes.
#[derive(Args)]
struct Common {
    /// Path to configuration file (default: extenddb.toml)
    #[arg(short, long)]
    config: Option<String>,
}

/// The options of a command that writes.
#[derive(Args)]
struct Write {
    #[command(flatten)]
    common: Common,
    /// Write. Without it the command prints what it would do and writes
    /// nothing
    #[arg(long)]
    yes: bool,
}

/// The options of a command that runs while every server is stopped.
#[derive(Args)]
struct Offline {
    #[command(flatten)]
    write: Write,
    /// Seconds to wait for a server or a migration to release the catalog
    /// before exiting 6 (default 60, at most 86400)
    #[arg(long, value_name = "SECONDS", value_parser = clap::value_parser!(u64).range(0..=86_400))]
    lock_timeout: Option<u64>,
}

/// The options of `switch` and `rotate` that section 8.5 and the common
/// rules give.
#[derive(Args)]
struct PassArgs {
    /// Treat a catalog key that appeared after the switch finished as an old
    /// key, and re-encrypt the stored secrets under it. Read the list that
    /// `encryption-key status` prints first
    #[arg(long)]
    accept_catalog_key: bool,
    /// Leave the stored secrets that no key opens as they are, and list them
    #[arg(long)]
    keep_unopenable: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Print the key source, the KEK id, the DEK ids and the stored secrets
    /// per key. Writes nothing
    Status(Common),
    /// Switch from the catalog key to a KEK file. Every server must be stopped
    Switch {
        #[command(flatten)]
        offline: Offline,
        /// The KEK file to switch to
        #[arg(long, value_name = "PATH")]
        key_file: String,
        #[command(flatten)]
        pass: PassArgs,
    },
    /// Wrap the DEK under a new KEK. Rewrites the key ring only, beside a
    /// running server
    Rewrap {
        #[command(flatten)]
        write: Write,
        /// The new KEK file
        #[arg(long, value_name = "PATH")]
        key_file: String,
    },
    /// Replace the DEK and re-encrypt every stored secret under the new one.
    /// Every server must be stopped
    Rotate {
        #[command(flatten)]
        offline: Offline,
        #[command(flatten)]
        pass: PassArgs,
    },
    /// Delete every access key and session and write a new key ring, for a
    /// restored catalog whose KEK is lost. Every server must be stopped
    Reset {
        #[command(flatten)]
        offline: Offline,
        /// The new KEK file
        #[arg(long, value_name = "PATH")]
        key_file: String,
        /// Required: every access key and every session is deleted
        #[arg(long)]
        discard_secrets: bool,
    },
    /// PostgreSQL only: read a connection string from standard input and
    /// write it as the data database connection string, encrypted under the
    /// DEK
    SetDataConnection(Write),
}

/// Why a command stopped, with its exit status.
#[derive(Debug)]
struct Failure {
    code: u8,
    message: String,
}

impl Failure {
    fn new(message: impl std::fmt::Display) -> Self {
        Self {
            code: extenddb_storage::migrations::exit::ERROR,
            message: message.to_string(),
        }
    }
}

impl From<MigrationError> for Failure {
    fn from(e: MigrationError) -> Self {
        Self {
            code: e.exit_code(),
            message: e.to_string(),
        }
    }
}

impl From<KeyCommandError> for Failure {
    fn from(e: KeyCommandError) -> Self {
        Self::new(e)
    }
}

impl From<anyhow::Error> for Failure {
    fn from(e: anyhow::Error) -> Self {
        Self::new(format!("{e:#}"))
    }
}

/// Run `extenddb encryption-key`; returns the exit status.
pub async fn run(args: EncryptionKeyArgs) -> anyhow::Result<u8> {
    match command(args.command).await {
        Ok(()) => Ok(extenddb_storage::migrations::exit::OK),
        Err(failure) => {
            eprintln!("Error: {}", failure.message);
            Ok(failure.code)
        }
    }
}

/// The configuration a command reads, and the deployment it names.
struct Context {
    config: extenddb_config::AppConfig,
    target: Target,
}

impl Context {
    fn load(common: &Common) -> Result<Self, Failure> {
        let (config, _) = crate::util::command_config(common.config.as_deref())?;
        let target = Target::at(config.storage.connection_config());
        Ok(Self { config, target })
    }

    /// The KEK file the configuration names, for `rewrap`, `rotate` and
    /// `set-data-connection`, which open the ring with it.
    fn configured_kek(&self, command: &str) -> Result<KeyFile, Failure> {
        self.config
            .auth
            .encryption
            .key_file_path()
            .map(KeyFile)
            .ok_or_else(|| {
                Failure::new(format!(
                    "`encryption-key {command}` opens the key ring with the KEK in \
                     [auth.encryption] key_file, which this configuration does not set"
                ))
            })
    }

    fn key_file_configured(&self) -> bool {
        self.config.auth.encryption.key_file.is_some()
    }

    /// Put the KEK of `kek`, read through the file rule, on the target that
    /// the migration backend opens: on PostgreSQL it opens the data
    /// connection string row of a catalog with a key ring (RFC-0000 section
    /// 12).
    fn use_kek(&mut self, kek: &KeyFile) -> Result<(), Failure> {
        let kek = extenddb_server::key_file::read_key_file(&kek.0).map_err(Failure::new)?;
        self.target.key_source = KeySource::Kek(kek);
        Ok(())
    }

    /// Set up the locked key store before any key is read (RFC-0000 section
    /// 7, the last rows). A command that reads a KEK file holds a KEK.
    fn init_key_store(&self, kek_in_use: bool) -> Result<(), Failure> {
        let protection = init_key_store(
            kek_in_use,
            self.config.auth.encryption.require_locked_memory,
            COMMAND_SLOTS,
        )
        .map_err(Failure::new)?;
        tracing::info!("Locked key store: {protection}");
        Ok(())
    }
}

async fn command(command: Command) -> Result<(), Failure> {
    match command {
        Command::Status(common) => {
            let cx = Context::load(&common)?;
            let kek = cx.config.auth.encryption.key_file_path().map(KeyFile);
            cx.init_key_store(kek.is_some())?;
            let stores = Stores::open(&cx.target, false).await?;
            let report =
                key_commands::status(stores.view(), kek.as_ref().map(|k| k as &dyn KekFile))
                    .await?;
            print!("{}", status_text(&report));
            Ok(())
        }
        Command::Switch {
            offline,
            key_file,
            pass,
        } => {
            let mut cx = Context::load(&offline.write.common)?;
            cx.init_key_store(true)?;
            let kek = KeyFile(key_file);
            cx.use_kek(&kek)?;
            // The one caller whose backend accepts the plaintext connection
            // string row of an unfinished switch (section 12).
            cx.target.plaintext_data_connection = true;
            let flags = RunFlags {
                yes: offline.write.yes,
                keep_unopenable: pass.keep_unopenable,
                accept_catalog_key: pass.accept_catalog_key,
                key_file_configured: cx.key_file_configured(),
            };
            let outcome = locked(
                migrations()?,
                &cx.target,
                offline.lock_timeout,
                None,
                async |stores| key_commands::switch(stores, &kek, flags).await,
            )
            .await?;
            print!("{}", outcome_text("switch", &outcome));
            Ok(())
        }
        Command::Rewrap { write, key_file } => {
            let cx = Context::load(&write.common)?;
            let current = cx.configured_kek("rewrap")?;
            cx.init_key_store(true)?;
            let stores = Stores::open(&cx.target, true).await?;
            let new = KeyFile(key_file);
            let outcome = key_commands::rewrap(stores.view(), &current, &new, write.yes).await?;
            print!("{}", outcome_text("rewrap", &outcome));
            Ok(())
        }
        Command::Rotate { offline, pass } => {
            let mut cx = Context::load(&offline.write.common)?;
            let kek = cx.configured_kek("rotate")?;
            cx.init_key_store(true)?;
            cx.use_kek(&kek)?;
            let flags = RunFlags {
                yes: offline.write.yes,
                keep_unopenable: pass.keep_unopenable,
                accept_catalog_key: pass.accept_catalog_key,
                key_file_configured: true,
            };
            let outcome = locked(
                migrations()?,
                &cx.target,
                offline.lock_timeout,
                None,
                async |stores| key_commands::rotate(stores, &kek, flags).await,
            )
            .await?;
            print!("{}", outcome_text("rotate", &outcome));
            Ok(())
        }
        Command::Reset {
            offline,
            key_file,
            discard_secrets,
        } => {
            let mut cx = Context::load(&offline.write.common)?;
            cx.init_key_store(true)?;
            let kek = KeyFile(key_file);
            cx.use_kek(&kek)?;
            // On PostgreSQL the stores open with the connection string read
            // here in place of the row, preview included (section 8.6).
            let data_connection = if crate::util::is_postgres() {
                Some(read_secret_input(
                    std::io::stdin().lock(),
                    std::io::IsTerminal::is_terminal(&std::io::stdin()),
                )?)
            } else {
                None
            };
            let flags = RunFlags {
                yes: offline.write.yes,
                key_file_configured: cx.key_file_configured(),
                ..RunFlags::default()
            };
            let connection = data_connection.as_deref().map(String::as_str);
            // The status read and the stores reach the data database with
            // this string in place of the row (RFC-0000 section 8.6, step 1).
            cx.target.data_connection = connection.map(str::to_owned);
            let outcome = locked(
                migrations()?,
                &cx.target,
                offline.lock_timeout,
                connection,
                async |stores| {
                    key_commands::reset(stores, &kek, flags, discard_secrets, connection).await
                },
            )
            .await?;
            print!("{}", outcome_text("reset", &outcome));
            Ok(())
        }
        Command::SetDataConnection(write) => set_data_connection(&write).await,
    }
}

/// `set-data-connection` (section 8.7). The connection string is read only
/// with `--yes`, and parsed before anything is written.
async fn set_data_connection(write: &Write) -> Result<(), Failure> {
    if !crate::util::is_postgres() {
        return Err(Failure::new(format!(
            "`encryption-key set-data-connection` writes the PostgreSQL data database \
             connection string; this binary serves {}, whose catalog has no such row",
            extenddb_storage::backend_name().unwrap_or("another backend")
        )));
    }
    let cx = Context::load(&write.common)?;
    let kek = cx.configured_kek("set-data-connection")?;
    cx.init_key_store(true)?;
    if !write.yes {
        println!(
            "Nothing was written. With --yes this command reads a connection string from \
             standard input and writes the data_database_connection_string row, encrypted \
             under the DEK of the key ring."
        );
        return Ok(());
    }
    let connection = read_secret_input(
        std::io::stdin().lock(),
        std::io::IsTerminal::is_terminal(&std::io::stdin()),
    )?;
    extenddb_storage::operations::parse_connection_string(&connection).map_err(|_| {
        Failure::new("standard input does not parse as a PostgreSQL connection string")
    })?;
    // The store reads the ring with `SELECT ... FOR SHARE` and writes the
    // row in one transaction, so a ring write of `rotate` waits for it.
    let stores = Stores::open(&cx.target, true).await?;
    let key = kek.read().map_err(Failure::new)?;
    stores
        .settings
        .set_data_connection(&key, &connection)
        .await
        .map_err(|e| Failure::new(e.message()))?;
    println!(
        "The data_database_connection_string row was written, encrypted under the DEK of the \
         key ring. A running server reads it at its next start."
    );
    Ok(())
}

/// Run `f` over the catalog's stores, which the migration backend opens
/// under the migration lock that this process holds, as `migrate --yes`
/// does, and free the lock after. The catalog's migrations must be current.
async fn locked<T>(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    lock_timeout: Option<u64>,
    data_connection: Option<&str>,
    f: impl AsyncFnOnce(KeyStores<'_>) -> Result<T, KeyCommandError>,
) -> Result<T, Failure> {
    let timeout =
        Duration::from_secs(lock_timeout.unwrap_or(crate::cmd_migrate::DEFAULT_LOCK_TIMEOUT_SECS));
    println!(
        "--- Taking the migration lock (waiting up to {} s for a server or another migration)...",
        timeout.as_secs()
    );
    let lock = migrations.lock(target, timeout).await?;
    let result = async {
        current(migrations.status_under_lock(&lock, target).await?)?;
        let stores = migrations.locked_stores(&lock, data_connection).await?;
        Ok::<T, Failure>(
            f(KeyStores {
                management: stores.management(),
                settings: stores.settings(),
            })
            .await?,
        )
    }
    .await;
    migrations.release(lock).await;
    result
}

/// The installed backend's catalog migrations.
fn migrations() -> Result<&'static dyn CatalogMigrations, Failure> {
    extenddb_storage::migrations::installed()
        .ok_or_else(|| Failure::new("no storage backend is installed"))
}

/// Refuse a catalog whose migrations are not current: the commands read and
/// write the rows of this binary's catalog.
fn current(status: CatalogStatus) -> Result<(), Failure> {
    match status {
        CatalogStatus::Current { .. } | CatalogStatus::AheadCompatible { .. } => Ok(()),
        status => Err(Failure {
            code: status.exit_code(),
            message: format!(
                "{status}. The encryption-key commands run on a catalog whose migrations are \
                 current: run `extenddb migrate` first"
            ),
        }),
    }
}

/// The catalog's management and settings stores, holding no key.
struct Stores {
    management: Box<dyn ManagementStore>,
    settings: Box<dyn SettingsStore>,
}

impl Stores {
    /// The stores of `status`, `rewrap` and `set-data-connection`, which run
    /// beside a server. `writes` is false for `status`.
    ///
    /// On every backend but PostgreSQL the catalog status is read first,
    /// without the lock, and a catalog whose migrations are not current is
    /// refused: opening the stores of a path that holds no catalog would
    /// create an empty database there. On PostgreSQL an open creates nothing,
    /// and the full status read opens the `data_database_connection_string`
    /// row, which a catalog with a key ring keeps encrypted (RFC-0000 section
    /// 12). There `rewrap` and `set-data-connection`, which write rows of the
    /// catalog database, read the status of the catalog database alone,
    /// which opens no such row. `status` writes nothing and reads no status,
    /// so it can report a ring or a row that does not open.
    async fn open(target: &Target, writes: bool) -> Result<Self, Failure> {
        if writes {
            current(migrations()?.catalog_store_status(target).await?)?;
        } else if !crate::util::is_postgres() {
            current(migrations()?.status(target).await?)?;
        }
        let location = target.location.as_str();
        let management =
            extenddb_storage::management_store_factory::create_management_store(location)
                .await
                .map_err(|e| Failure::new(format!("cannot open the catalog: {e}")))?;
        let settings = extenddb_storage::settings_store::create_settings_store(location)
            .await
            .map_err(|e| Failure::new(format!("cannot open the catalog: {e}")))?;
        Ok(Self {
            management,
            settings,
        })
    }

    fn view(&self) -> KeyStores<'_> {
        KeyStores {
            management: &*self.management,
            settings: &*self.settings,
        }
    }
}

/// Standard input as section 8.7 reads it for `reset` and
/// `set-data-connection`: to the end, with one trailing newline stripped.
/// Refused when it is a terminal, so the command never waits with no
/// prompt, when it is empty, and past 8 KiB. The text is wiped when it drops.
fn read_secret_input(
    mut input: impl std::io::Read,
    is_terminal: bool,
) -> Result<Zeroizing<String>, Failure> {
    const LIMIT: usize = 8192;
    if is_terminal {
        return Err(Failure::new(
            "standard input is a terminal; give the connection string on standard input, for \
             example `< conn.txt` from a file written under `umask 077`",
        ));
    }
    let mut buffer = Zeroizing::new([0u8; LIMIT + 1]);
    let filled = extenddb_server::key_file::read_bounded(&mut input, &mut buffer[..])
        .map_err(|e| Failure::new(format!("cannot read standard input: {e}")))?;
    if filled > LIMIT {
        return Err(Failure::new(format!(
            "standard input holds more than {LIMIT} bytes; give one connection string"
        )));
    }
    let bytes = &buffer[..filled];
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    if bytes.is_empty() {
        return Err(Failure::new(
            "standard input is empty; give the connection string on standard input",
        ));
    }
    let text =
        std::str::from_utf8(bytes).map_err(|_| Failure::new("standard input is not UTF-8 text"))?;
    Ok(Zeroizing::new(text.to_owned()))
}

#[path = "cmd_encryption_key_text.rs"]
mod text;

#[cfg(test)]
#[path = "cmd_encryption_key_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "cmd_encryption_key_sqlite_tests.rs"]
mod sqlite_tests;
