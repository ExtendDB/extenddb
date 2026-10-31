// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Startup of the backup store: the `[backup.store]` resolution, `open`,
//! the spool and its lock, and, for a filesystem store, `validate` and the
//! identity check; the split between a configuration error, which stops
//! startup, and a store condition or identity failure, which starts the
//! server with the store unhealthy; and the rule that nothing here waits on
//! the network, so a store endpoint that stalls never holds the data plane.
//!
//! Configuration errors, each named with the path or bucket in the store's
//! own message, every one decided without a request to the store: `open`
//! refusing the configuration (a missing section, an empty or S3 Express
//! bucket name, a part size, prefix or key-length problem, an insecure
//! endpoint, a filesystem root that does not exist or fails the owner-only
//! rule), a filesystem store's `validate` refusing the root for a reason of
//! the same kind (a root another user can reach, a root replaced by a
//! symlink), a spool that breaks its placement rule, that another live
//! instance holds or that exists with a mode other than owner-only, and an
//! S3 store without a spool on a backend with no default. A filesystem
//! root's `validate` also writes a probe file, which is a request to the
//! store: an I/O failure there or in resolving the root (a full or read-only
//! disk, a stale network handle, a root that vanished after `open`) is a
//! store condition and degrades the store, as the same failure one cycle
//! later does, rather than stopping startup.
//!
//! The one exception to the configuration errors is the defaulted SQLite
//! root (`backups/` beside the database file when `[backup.store]` is
//! absent) while the catalog has no backup rows for it: a failure of its
//! creation, `open`, `validate` or identity check is a logged warning that
//! leaves the store unconfigured, so an upgrade, a fresh deployment or a
//! catalog re-created by `destroy` and `init` under the old root never stops
//! a server that never configured backups. Once the catalog has rows for
//! that root ([`extenddb_storage::BackupCatalog::read_store_ids`]), the root
//! has been used and its backups must not vanish behind a warning: the
//! defaulted root is then handled as an explicitly configured one, a
//! refusal of `open` or `validate` a startup error, and an identity failure
//! an unhealthy store (reported on `/health`, its rows' restores and deletes
//! refused naming the kind and the category, rechecked each cycle). A
//! command that found no store learns why from [`NotConfigured`].
//!
//! A filesystem store's `validate` and identity check run here, before the
//! listener, bounded by [`LOCAL_CHECK_TIMEOUT`]: a root on a local disk
//! answers at once, and a mount that stops answering after the root is
//! opened leaves the server starting with the store `checking`; a root that
//! does not answer at startup holds the server before the listener, as the
//! database directory would (the banner's resolution of the root, the
//! default root's creation and `open`'s own resolution run unbounded, before
//! the checks). The socket is bound before the banner, so a startup held
//! that way leaves the port accepting connections it never answers until
//! the mount answers or the process is killed. A check abandoned at the
//! bound leaves its filesystem call parked on a blocking thread until the
//! mount answers (the engine's runtime module says how the later checks
//! avoid parking another); the thread costs nothing at exit, where the
//! binary ends its runtime with a bounded shutdown. An S3 store's
//! `validate` and identity check are requests to the endpoint, and run after
//! the listener is up, in the reconciler's first cycle
//! ([`extenddb_engine::check_configured_store`]); the store is `checking`
//! until that cycle records its result, and the operations that touch the
//! store refuse meanwhile as they do while it is unhealthy. A `validate`
//! refusal that is not a transport failure (a missing or denied bucket) then
//! makes the store unhealthy rather than stopping startup.
//!
//! Transport failures (the store unreachable: no region, no credentials, a
//! refused connection) and failures of the identity check or the manifest
//! probe (a denial the store reports while a check runs, say a directory
//! under the root that another user can reach, included) leave the server
//! running with the store unhealthy: logged once, reported on `/health`,
//! the operations that touch the store refused, and the check run again
//! each reconciler cycle until it passes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use extenddb_backup_store::{BackupStore, BackupStoreKind, StoreError};
use extenddb_config::backup::{BackupConfig, EffectiveStore, Unconfigured};
pub use extenddb_engine::LOCAL_CHECK_TIMEOUT;
use extenddb_engine::{BackupRuntime, ConfiguredStore, Spool, StoreProblem, store_identity};
use extenddb_storage::backup_catalog::{StoreIdentity, StoreKind};
use extenddb_storage::{BackupCatalog, StorageEngine};

/// The lock on the spool directory, held for the process's life so a second
/// instance on this host refuses the same `spool_path`.
pub struct SpoolLock {
    _file: std::fs::File,
    path: PathBuf,
}

impl std::fmt::Debug for SpoolLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpoolLock")
            .field("path", &self.path)
            .finish()
    }
}

/// The name of the lock file inside the spool.
pub const SPOOL_LOCK_FILE: &str = ".lock";

/// Who is configuring the store, which decides the spool and the checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caller {
    /// A server: create the spool and hold its lock for the process's life;
    /// a filesystem store's checks run here (module doc).
    Server,
    /// A command that writes (`backup purge`): the spool rule is checked,
    /// the directory neither created nor locked; the checks run as a
    /// server's do.
    Command,
    /// A command that reads beside a server (`backup list`, the purge's dry
    /// run): as `Command`, and no check of the store runs, so a root is left
    /// as it is found (neither the default directory nor an identity file is
    /// created; a default root that does not exist yet leaves the store
    /// unconfigured, logged); the store is returned `checking` and the
    /// command reads what it needs itself.
    ReadOnlyCommand,
}

/// Why [`configure_described`] left the store unconfigured; `Display` is the
/// sentence a command prints for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotConfigured {
    /// `[backup.store]` is absent and this backend has no default root.
    Absent,
    /// `kind = "disabled"`.
    Disabled,
    /// A read-only command found that the default root does not exist yet:
    /// no server has run with this database file.
    DefaultRootMissing(PathBuf),
    /// The defaulted root could not be created, opened, validated or checked
    /// while the catalog has no rows for it (module doc): the warning path.
    DefaultRootUnusable {
        /// The root, as the configuration or `open` spelled it.
        root: String,
        /// The failure, as the warning logged it.
        problem: String,
    },
}

impl std::fmt::Display for NotConfigured {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => f.write_str(
                "no backup store is configured: [backup.store] is absent and this backend has \
                 no default root, so there are no store-backed backups to list or purge",
            ),
            Self::Disabled => f.write_str(
                "no backup store is configured: [backup.store] is disabled, so there are no \
                 store-backed backups to list or purge",
            ),
            Self::DefaultRootMissing(root) => write!(
                f,
                "the default root {} does not exist yet: no server has run with this database \
                 file, so there are no store-backed backups to list or purge",
                root.display()
            ),
            Self::DefaultRootUnusable { root, problem } => write!(
                f,
                "the default root {root} failed its check ({problem}) and the catalog has no \
                 rows for it, so there are no store-backed backups to list or purge; the \
                 warning above says how to fix the root"
            ),
        }
    }
}

/// What [`configure_described`] built.
pub struct StoreSetup {
    /// The job slots and, when one is configured, the store with its health.
    pub runtime: BackupRuntime,
    /// The spool's lock, held by a server for the process's life.
    pub spool_lock: Option<SpoolLock>,
    /// Why the runtime has no store, when it has none.
    pub not_configured: Option<NotConfigured>,
}

impl StoreSetup {
    fn without_store(runtime: BackupRuntime, why: NotConfigured) -> Self {
        Self {
            runtime,
            spool_lock: None,
            not_configured: Some(why),
        }
    }

    fn with_store(
        runtime: BackupRuntime,
        store: ConfiguredStore,
        spool_lock: Option<SpoolLock>,
    ) -> Self {
        Self {
            runtime: runtime.with_store(store),
            spool_lock,
            not_configured: None,
        }
    }
}

/// Build the backup runtime for this process: the job slots and, when the
/// configuration and the backend select one, the store with its health
/// ([`configure_described`] without the reason for an absent store).
///
/// # Errors
///
/// A configuration error (module doc), with the key, path or bucket named.
pub async fn configure(
    backup: &BackupConfig,
    database_file: Option<&Path>,
    backend: &str,
    storage: &Arc<dyn StorageEngine>,
    caller: Caller,
) -> anyhow::Result<(BackupRuntime, Option<SpoolLock>)> {
    let setup = configure_described(backup, database_file, backend, &**storage, caller).await?;
    Ok((setup.runtime, setup.spool_lock))
}

/// Build the backup runtime for this process: the job slots and, when the
/// configuration and the backend select one, the store with its health;
/// otherwise why there is none. `catalog` is the catalog the identity check
/// reads and the rows rule of the module doc consults.
///
/// # Errors
///
/// A configuration error (module doc), with the key, path or bucket named.
pub async fn configure_described<C: BackupCatalog + ?Sized>(
    backup: &BackupConfig,
    database_file: Option<&Path>,
    backend: &str,
    catalog: &C,
    caller: Caller,
) -> anyhow::Result<StoreSetup> {
    let runtime = BackupRuntime::new(backup.max_concurrent_jobs)
        .with_legacy_create_only(backend == "cassandra")
        .with_stale_job_after(std::time::Duration::from_secs(backup.stale_job_after_secs));
    let resolved = backup
        .resolve_store(database_file)
        .map_err(|e| anyhow::anyhow!("[backup] configuration: {e}"))?;
    let (config, defaulted) = match resolved.store {
        EffectiveStore::Unconfigured(Unconfigured::Absent) => {
            tracing::info!(
                "backup store: none configured ([backup.store] is absent and this backend has \
                 no default root); CreateBackup writes in-catalog backups, with a deprecation \
                 warning on each call"
            );
            return Ok(StoreSetup::without_store(runtime, NotConfigured::Absent));
        }
        EffectiveStore::Unconfigured(Unconfigured::Disabled) => {
            tracing::info!(
                "backup store: disabled ([backup.store] kind = \"disabled\"); CreateBackup \
                 writes in-catalog backups, with a deprecation warning on each call"
            );
            return Ok(StoreSetup::without_store(runtime, NotConfigured::Disabled));
        }
        EffectiveStore::Configured { config, defaulted } => (config, defaulted),
    };

    // The defaulted root beside the database file is created if missing,
    // private to this user; an explicitly configured root must exist. A
    // read-only command creates nothing: a defaulted root no server has
    // created yet is reported as such, and the command has nothing to read.
    if defaulted
        && caller == Caller::ReadOnlyCommand
        && let Some(fs) = &config.filesystem
        && !fs.path.exists()
    {
        tracing::info!(
            "backup store: the default root {} does not exist yet (no server has run with this \
             database file); there are no store-backed backups to list",
            fs.path.display()
        );
        return Ok(StoreSetup::without_store(
            runtime,
            NotConfigured::DefaultRootMissing(fs.path.clone()),
        ));
    }
    if defaulted
        && let Some(fs) = &config.filesystem
        && let Err(e) = create_private_dir_if_missing(&fs.path)
    {
        // A root that could not be created does not exist, so no row can
        // name it: the warning path, whatever the catalog holds.
        let root = fs.path.display().to_string();
        let problem = format!("it could not be created: {e}");
        warn_default_root_unusable(&root, &problem);
        return Ok(StoreSetup::without_store(
            runtime,
            NotConfigured::DefaultRootUnusable { root, problem },
        ));
    }

    let opened = match extenddb_backup_store::open_described(&config).await {
        Ok(opened) => opened,
        Err(e) => {
            // The label rows record is the root's canonical path, which the
            // refused root may still have (a parent others can write to).
            let label = config
                .filesystem
                .as_ref()
                .and_then(|fs| std::fs::canonicalize(&fs.path).ok())
                .map(|p| p.display().to_string());
            if defaulted
                && let Some(root) = label.as_deref()
                && !default_root_has_rows(catalog, root).await
            {
                let problem = format!("it is not usable: {e}");
                warn_default_root_unusable(root, &problem);
                return Ok(StoreSetup::without_store(
                    runtime,
                    NotConfigured::DefaultRootUnusable {
                        root: root.to_owned(),
                        problem,
                    },
                ));
            }
            anyhow::bail!(
                "[backup.store] ({}) cannot be opened: {e}{}",
                describe_target(&config),
                rows_note(defaulted)
            );
        }
    };
    let kind = match opened.kind {
        BackupStoreKind::Filesystem => StoreKind::Filesystem,
        BackupStoreKind::S3 => StoreKind::S3,
        BackupStoreKind::Disabled => unreachable!("open_described never returns Disabled"),
    };

    // The spool and its lock: configuration errors on every backend.
    let (spool, spool_lock) = match (resolved.spool_path, caller) {
        (Some(path), Caller::Server) => {
            let (spool, lock) = take_spool(&path)?;
            (Some(spool), Some(lock))
        }
        (Some(_), Caller::Command | Caller::ReadOnlyCommand) | (None, _) => (None, None),
    };
    let spool_line = spool
        .as_ref()
        .map(|s| format!(", spool {}", s.path.display()))
        .unwrap_or_default();

    // An S3 store's checks are requests to the endpoint: they run once the
    // listener is up, in the reconciler's first cycle. A read-only command
    // runs no check at all.
    if caller == Caller::ReadOnlyCommand {
        let configured = ConfiguredStore::checking(kind, opened.root, opened.store, spool);
        return Ok(StoreSetup::with_store(runtime, configured, spool_lock));
    }
    if kind == StoreKind::S3 {
        if caller == Caller::Server {
            tracing::info!(
                "backup store: {kind} {}{spool_line}; its validation and identity check run \
                 after the listener is up, and CreateBackup writes in-catalog backups in this \
                 release",
                opened.root
            );
        }
        let configured = ConfiguredStore::checking(kind, opened.root, opened.store, spool);
        return Ok(StoreSetup::with_store(runtime, configured, spool_lock));
    }

    // A filesystem store: `validate` and the identity check read the root
    // here (`run_local_checks`). A `validate` refusal is a configuration
    // error; a store condition or the identity check's failure degrades the
    // store; on the defaulted root without rows either is the warning.
    let outcome = match run_local_checks(&*opened.store, kind, &opened.root, catalog, backend).await
    {
        LocalChecks::Done(outcome) => outcome,
        LocalChecks::Refused(e) => {
            if defaulted && !default_root_has_rows(catalog, &opened.root).await {
                let problem = format!("it failed its validation: {e}");
                warn_default_root_unusable(&opened.root, &problem);
                return Ok(StoreSetup::without_store(
                    runtime,
                    NotConfigured::DefaultRootUnusable {
                        root: opened.root,
                        problem,
                    },
                ));
            }
            anyhow::bail!(
                "[backup.store] ({}) failed its startup validation: {e}{}",
                describe_target(&config),
                rows_note(defaulted)
            );
        }
        LocalChecks::TimedOut => {
            tracing::warn!(
                "backup store: {kind} {} did not answer its startup checks within {} s (a mount \
                 that has stopped answering?); the server starts with the store checking, and \
                 the reconciler's first cycle runs the checks again",
                opened.root,
                LOCAL_CHECK_TIMEOUT.as_secs()
            );
            let configured = ConfiguredStore::checking(kind, opened.root, opened.store, spool);
            return Ok(StoreSetup::with_store(runtime, configured, spool_lock));
        }
    };
    match &outcome {
        Ok(identity) => tracing::info!(
            "backup store: {kind} {} (store id {}){spool_line}; CreateBackup writes in-catalog \
             backups in this release, and the store serves restores and deletes of the backups \
             it holds",
            opened.root,
            identity.store_id
        ),
        Err(problem) if defaulted && !default_root_has_rows(catalog, &opened.root).await => {
            let problem = problem.to_string();
            warn_default_root_unusable(&opened.root, &problem);
            return Ok(StoreSetup::without_store(
                runtime,
                NotConfigured::DefaultRootUnusable {
                    root: opened.root,
                    problem,
                },
            ));
        }
        Err(problem) => tracing::error!(
            "backup store: {kind} {} is unhealthy: {problem}; the server starts, restore and \
             delete of a backup in the store are refused, /health reports the backup_store \
             component unhealthy, and the check runs again each reconciler cycle{}",
            opened.root,
            rows_note(defaulted)
        ),
    }
    let configured = ConfiguredStore::new(kind, opened.root, opened.store, spool, outcome);
    Ok(StoreSetup::with_store(runtime, configured, spool_lock))
}

/// What a filesystem store's startup checks decided.
enum LocalChecks {
    /// `validate` passed and the identity check ran, or `validate` failed on
    /// a store condition (module doc): the identity, or the problem that
    /// makes the store unhealthy.
    Done(Result<StoreIdentity, StoreProblem>),
    /// `validate` refused the root for a configuration reason.
    Refused(StoreError),
    /// Nothing answered within [`LOCAL_CHECK_TIMEOUT`].
    TimedOut,
}

/// A filesystem store's startup checks: `validate`, then
/// [`store_identity::establish`], bounded by [`LOCAL_CHECK_TIMEOUT`] so a
/// root on a mount that stops answering after `open` does not hold the
/// listener. The store's synchronous filesystem calls run on the blocking
/// pool, so the bound fires whichever call stalls; a timed-out check leaves
/// that thread parked until the mount answers (module doc).
async fn run_local_checks<C: BackupCatalog + ?Sized>(
    store: &dyn BackupStore,
    kind: StoreKind,
    root: &str,
    catalog: &C,
    backend: &str,
) -> LocalChecks {
    let checks = async {
        match store.validate().await {
            Ok(()) => LocalChecks::Done(
                store_identity::establish(store, kind, root, catalog, backend).await,
            ),
            // A store condition, not a configuration error: the probe write
            // or the root's resolution failed (module doc).
            Err(e @ (StoreError::Transport { .. } | StoreError::Io { .. })) => {
                LocalChecks::Done(Err(StoreProblem::from(e)))
            }
            Err(e) => LocalChecks::Refused(e),
        }
    };
    tokio::time::timeout(LOCAL_CHECK_TIMEOUT, checks)
        .await
        .unwrap_or(LocalChecks::TimedOut)
}

/// Whether the catalog has backup rows for the defaulted root at `root`
/// (the label rows record), which ends the root's leniency (module doc). A
/// catalog that cannot be read is answered as having rows, so a read
/// failure never makes a used root's backups vanish behind a warning.
async fn default_root_has_rows<C: BackupCatalog + ?Sized>(catalog: &C, root: &str) -> bool {
    match catalog.read_store_ids(StoreKind::Filesystem, root).await {
        Ok(ids) => !ids.is_empty(),
        Err(e) => {
            tracing::warn!(
                "backup store: could not read the catalog's rows for the default root {root} \
                 ({e}); the root is handled as a configured one"
            );
            true
        }
    }
}

/// The clause a defaulted root's startup error or unhealthy line carries,
/// saying why it was not the warning.
fn rows_note(defaulted: bool) -> &'static str {
    if defaulted {
        " (the default root beside the database file; the catalog has backup rows for it, so \
         it is handled as a configured root: fix it, or configure [backup.store])"
    } else {
        ""
    }
}

/// The warning for a defaulted root whose creation, `open`, `validate` or
/// identity check failed while the catalog has no rows for it: the store is
/// left unconfigured, as the module doc says.
fn warn_default_root_unusable(root: &str, problem: &str) {
    tracing::warn!(
        "backup store: the default root {root} failed its check ({problem}); the catalog has no \
         backup rows for it, so the store is unconfigured and CreateBackup writes in-catalog \
         backups. Fix the root (it must be owned by this user with mode 0700, under directories \
         owned by root or this user that others cannot write to, and its extenddb-store.json, \
         if any, must name this catalog: a catalog re-created by `destroy` and `init` needs the \
         old file removed or a new root), configure [backup.store] to choose another, or set \
         kind = \"disabled\" to silence this"
    );
}

/// The store line of the console banner: what `[backup.store]` and the
/// backend select, before anything is opened. The configuration's own
/// refusals are not applied here; `configure` reports them.
#[must_use]
pub fn banner_line(backup: &BackupConfig, database_file: Option<&Path>) -> String {
    let resolved = match backup.resolve_store(database_file) {
        Ok(resolved) => resolved,
        Err(e) => return format!("backup store: configuration error ({e})"),
    };
    let spool = resolved
        .spool_path
        .as_ref()
        .map(|p| format!(", spool {}", p.display()))
        .unwrap_or_default();
    match resolved.store {
        EffectiveStore::Unconfigured(Unconfigured::Absent) => {
            "backup store: none ([backup.store] is absent); CreateBackup writes in-catalog \
             backups"
                .to_owned()
        }
        EffectiveStore::Unconfigured(Unconfigured::Disabled) => {
            "backup store: disabled; CreateBackup writes in-catalog backups".to_owned()
        }
        EffectiveStore::Configured { config, defaulted } => format!(
            "backup store: {}{}{spool}",
            describe_target(&config),
            if defaulted {
                " (the default beside the database file)"
            } else {
                ""
            }
        ),
    }
}

/// The root a configuration names, for a startup error: the bucket of an S3
/// store, the path of a filesystem one. Neither is a secret, and an operator
/// reading the error wants to know which store it is about.
fn describe_target(config: &extenddb_backup_store::BackupStoreConfig) -> String {
    match config.kind {
        BackupStoreKind::S3 => config.s3.as_ref().map_or_else(
            || "s3".to_owned(),
            |s3| format!("s3 bucket {:?}", s3.bucket),
        ),
        BackupStoreKind::Filesystem => config.filesystem.as_ref().map_or_else(
            || "filesystem".to_owned(),
            |fs| {
                // A path that is empty or begins or ends with whitespace is
                // quoted, so the message shows where it ends.
                let path = fs.path.to_string_lossy();
                if path.is_empty() {
                    "filesystem path \"\" (empty)".to_owned()
                } else if path.trim() != path {
                    format!("filesystem path {path:?}")
                } else {
                    format!("filesystem path {path}")
                }
            },
        ),
        BackupStoreKind::Disabled => "disabled".to_owned(),
    }
}

/// Create `path` with mode `0700` unless it exists.
fn create_private_dir_if_missing(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        return Ok(());
    }
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.recursive(true).create(path)
}

/// Create the spool directory (`0700`) if missing and take its `.lock`
/// (`0600`, `flock`), refusing a spool another live instance holds. An
/// existing spool must meet the root's owner-only rule (owned by this user,
/// no group or other permission bits): it will hold downloaded backup
/// files, and a readable lock file would let any local user hold the spool.
fn take_spool(path: &Path) -> anyhow::Result<(Spool, SpoolLock)> {
    create_private_dir_if_missing(path).map_err(|e| {
        anyhow::anyhow!(
            "backup.spool_path {} cannot be created: {e}",
            path.display()
        )
    })?;
    let canonical = path
        .canonicalize()
        .map_err(|e| anyhow::anyhow!("backup.spool_path {} is not usable: {e}", path.display()))?;
    check_spool_mode(&canonical)?;
    let lock_path = canonical.join(SPOOL_LOCK_FILE);
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&lock_path)
        .map_err(|e| anyhow::anyhow!("cannot open the spool lock {}: {e}", lock_path.display()))?;
    // `create` honours the umask and the file may predate this rule: flock
    // needs only a read handle, so a readable lock file would let any local
    // user hold the spool. Set the mode on every open.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|e| anyhow::anyhow!("cannot set the mode of {}: {e}", lock_path.display()))?;
    }
    try_lock_exclusive(&file).map_err(|e| {
        if e.kind() == std::io::ErrorKind::WouldBlock {
            anyhow::anyhow!(
                "backup.spool_path {} is held by another running extenddb instance (lock {}); \
                 every instance on a host needs a spool of its own",
                canonical.display(),
                lock_path.display()
            )
        } else {
            anyhow::anyhow!("cannot lock {}: {e}", lock_path.display())
        }
    })?;
    let spool = Spool {
        spool_host: format!("{}:{}", hostname(), canonical.display()),
        path: canonical,
    };
    Ok((
        spool,
        SpoolLock {
            _file: file,
            path: lock_path,
        },
    ))
}

/// The owner-only rule on the spool directory, the one the filesystem store
/// applies to its root (`extenddb_backup_store::check_private_directory`):
/// owned by this user with mode `0700`, under directories owned by root or
/// this user that others cannot write to.
fn check_spool_mode(spool: &Path) -> anyhow::Result<()> {
    if !spool.is_dir() {
        anyhow::bail!("backup.spool_path {} is not a directory", spool.display());
    }
    extenddb_backup_store::check_private_directory(spool, "spool").map_err(|e| {
        anyhow::anyhow!(
            "backup.spool_path {} is not private: {e}; the spool must have mode 0700 under \
             directories others cannot write to",
            spool.display()
        )
    })
}

/// This host's name, for `spool_host` and a maintenance claim's holder;
/// `unknown-host` when the system does not say.
#[must_use]
pub fn hostname() -> String {
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        // SAFETY: `gethostname` writes at most `len` bytes into the buffer it
        // is given, which is valid for its whole length.
        let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
        if rc == 0 {
            let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
            if let Ok(name) = std::str::from_utf8(&buf[..end])
                && !name.is_empty()
            {
                return name.to_owned();
            }
        }
    }
    "unknown-host".to_owned()
}

#[cfg(unix)]
fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `flock` takes a file descriptor and flags and touches no memory
    // the caller owns; the descriptor belongs to `file`, borrowed for the
    // call.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &std::fs::File) -> std::io::Result<()> {
    tracing::warn!(
        "no file lock is taken on the spool on this platform: nothing prevents a second \
         extenddb instance from using the same spool_path"
    );
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    /// The spool is created private with a private lock file, a second take
    /// of the same spool is refused while the first lock lives, and a spool
    /// elsewhere is not.
    #[test]
    fn the_spool_lock_refuses_a_second_instance() {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("spool");
        let (first, lock) = take_spool(&spool).unwrap();
        assert_eq!(mode_of(&spool), 0o700);
        assert_eq!(mode_of(&spool.join(SPOOL_LOCK_FILE)), 0o600);
        assert!(
            first
                .spool_host
                .ends_with(&format!(":{}", spool.canonicalize().unwrap().display()))
        );
        assert!(!first.spool_host.starts_with(':'), "the host name is set");
        let err = take_spool(&spool).unwrap_err().to_string();
        assert!(
            err.contains("held by another running extenddb instance"),
            "{err}"
        );
        assert!(err.contains(&spool.display().to_string()), "{err}");
        take_spool(&dir.path().join("other")).unwrap();
        drop(lock);
        take_spool(&spool).expect("free once the holder is gone");
    }

    /// An existing spool that group or others can reach, or that another
    /// user owns, is refused naming the path and the mode; one with mode
    /// 0700 is taken.
    #[test]
    fn an_existing_spool_must_be_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("spool");
        std::fs::create_dir(&spool).unwrap();
        for mode in [0o755u32, 0o750, 0o707, 0o770] {
            std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(mode)).unwrap();
            let err = take_spool(&spool).unwrap_err().to_string();
            assert!(
                err.contains(&format!("{mode:04o}")) && err.contains("mode 0700"),
                "{mode:04o}: {err}"
            );
            assert!(err.contains("not private"), "{err}");
            assert!(err.contains(&spool.display().to_string()), "{err}");
            assert!(
                !spool.join(SPOOL_LOCK_FILE).exists(),
                "no lock file is created"
            );
        }
        std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
        take_spool(&spool).expect("an owner-only spool is taken");
        // A spool under a parent others can write to is refused as the root
        // would be: the parent could be swapped under it.
        let parent = dir.path().join("open");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o777)).unwrap();
        let under = parent.join("spool");
        std::fs::create_dir(&under).unwrap();
        std::fs::set_permissions(&under, std::fs::Permissions::from_mode(0o700)).unwrap();
        let err = take_spool(&under).unwrap_err().to_string();
        assert!(
            err.contains("an ancestor of the spool") && err.contains("0777"),
            "{err}"
        );
    }

    /// A lock file that predates the mode rule is narrowed to 0600 on open.
    #[test]
    fn a_permissive_lock_file_is_narrowed() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("spool");
        std::fs::create_dir(&spool).unwrap();
        std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
        let lock = spool.join(SPOOL_LOCK_FILE);
        std::fs::write(&lock, b"").unwrap();
        std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o644)).unwrap();
        let _held = take_spool(&spool).unwrap();
        assert_eq!(mode_of(&lock), 0o600);
    }

    // --- the startup checks against doubles of the catalog and the store ---

    use extenddb_backup_store::{ByteStream, ObjectMeta};
    use extenddb_core::backup_format::BackupArn;
    use extenddb_storage::BoxedFuture;
    use extenddb_storage::backup_catalog::{
        BackupKey, BackupRow, BackupStatus, DeleteStart, JobClaim, MaintenanceClaim, RestoreJobRow,
        RestoreTargetKey, TimeBounds,
    };
    use extenddb_storage::error::StorageError;
    use futures::stream::BoxStream;

    /// A catalog whose rows for every root carry `ids`, or whose rows cannot
    /// be read (`rows_unreadable`), with no backup row to list; every other
    /// method is an error, since the startup checks never reach it.
    #[derive(Default)]
    struct StubCatalog {
        ids: Vec<String>,
        rows_unreadable: bool,
    }

    fn unused<T: Send + 'static>() -> BoxedFuture<'static, Result<T, StorageError>> {
        Box::pin(async {
            Err(StorageError::Internal(
                "not reached by the startup checks".to_owned(),
            ))
        })
    }

    impl BackupCatalog for StubCatalog {
        fn catalog_id(&self) -> BoxedFuture<'_, Result<String, StorageError>> {
            Box::pin(async { Ok("catalog-1".to_owned()) })
        }
        fn claim_backup<'a>(
            &'a self,
            _row: &'a BackupRow,
        ) -> BoxedFuture<'a, Result<Box<dyn JobClaim>, StorageError>> {
            unused()
        }
        fn list_backup_rows<'a>(
            &'a self,
            _status: Option<BackupStatus>,
            _store: &'a StoreIdentity,
        ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
            Box::pin(futures::stream::empty())
        }
        fn list_account_backup_rows<'a>(
            &'a self,
            _account_id: &'a str,
            _table_name: Option<&'a str>,
            _bounds: &'a TimeBounds,
        ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
            Box::pin(futures::stream::empty())
        }
        fn read_backup_row<'a>(
            &'a self,
            _arn: &'a BackupArn,
        ) -> BoxedFuture<'a, Result<Option<BackupRow>, StorageError>> {
            unused()
        }
        fn start_backup_delete<'a>(
            &'a self,
            _key: &'a BackupKey,
        ) -> BoxedFuture<'a, Result<DeleteStart, StorageError>> {
            unused()
        }
        fn set_backup_deleted<'a>(
            &'a self,
            _key: &'a BackupKey,
        ) -> BoxedFuture<'a, Result<bool, StorageError>> {
            unused()
        }
        fn remove_backup_row<'a>(
            &'a self,
            _key: &'a BackupKey,
        ) -> BoxedFuture<'a, Result<bool, StorageError>> {
            unused()
        }
        fn remove_dead_creating_backup_row<'a>(
            &'a self,
            _key: &'a BackupKey,
            _stale_after: std::time::Duration,
        ) -> BoxedFuture<'a, Result<bool, StorageError>> {
            unused()
        }
        fn backup_in_use<'a>(
            &'a self,
            _key: &'a BackupKey,
        ) -> BoxedFuture<'a, Result<Option<String>, StorageError>> {
            unused()
        }
        fn remove_restore_target<'a>(
            &'a self,
            _key: &'a RestoreTargetKey,
        ) -> BoxedFuture<'a, Result<bool, StorageError>> {
            unused()
        }
        fn list_dead_restore_jobs(
            &self,
            _stale_after: std::time::Duration,
        ) -> BoxedFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
            unused()
        }
        fn fail_restore_job<'a>(
            &'a self,
            _key: &'a RestoreTargetKey,
            _reason: &'a str,
        ) -> BoxedFuture<'a, Result<bool, StorageError>> {
            unused()
        }
        fn read_store_ids<'a>(
            &'a self,
            _store_kind: StoreKind,
            _store_root: &'a str,
        ) -> BoxedFuture<'a, Result<Vec<String>, StorageError>> {
            if self.rows_unreadable {
                return Box::pin(async {
                    Err(StorageError::Internal(
                        "the catalog is not answering".to_owned(),
                    ))
                });
            }
            let ids = self.ids.clone();
            Box::pin(async move { Ok(ids) })
        }
        fn maintenance_claim<'a>(
            &'a self,
            _holder: &'a str,
            _stale_after: std::time::Duration,
        ) -> BoxedFuture<'a, Result<Box<dyn MaintenanceClaim>, StorageError>> {
            unused()
        }
        fn check_maintenance_claim(
            &self,
            _stale_after: std::time::Duration,
        ) -> BoxedFuture<'_, Result<bool, StorageError>> {
            unused()
        }
    }

    /// A store whose `validate` answers as told: never (a mount that has
    /// stopped answering), or with a given error; nothing else is reached.
    enum Validate {
        Never,
        Fails(fn() -> StoreError),
    }

    struct FakeStore(Validate);

    fn not_reached<T: Send + 'static>() -> futures::future::BoxFuture<'static, Result<T, StoreError>>
    {
        Box::pin(async { Err(StoreError::Other("not reached".to_owned())) })
    }

    impl BackupStore for FakeStore {
        fn put<'a>(
            &'a self,
            _key: &'a str,
            _body: ByteStream,
        ) -> futures::future::BoxFuture<'a, Result<ObjectMeta, StoreError>> {
            not_reached()
        }
        fn put_if_absent<'a>(
            &'a self,
            _key: &'a str,
            _body: ByteStream,
        ) -> futures::future::BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
            not_reached()
        }
        fn get<'a>(
            &'a self,
            _key: &'a str,
        ) -> futures::future::BoxFuture<'a, Result<ByteStream, StoreError>> {
            not_reached()
        }
        fn head<'a>(
            &'a self,
            _key: &'a str,
        ) -> futures::future::BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
            not_reached()
        }
        fn list<'a>(&'a self, _prefix: &'a str) -> BoxStream<'a, Result<ObjectMeta, StoreError>> {
            Box::pin(futures::stream::empty())
        }
        fn delete_prefix<'a>(
            &'a self,
            _prefix: &'a str,
        ) -> futures::future::BoxFuture<'a, Result<u64, StoreError>> {
            not_reached()
        }
        fn validate(&self) -> futures::future::BoxFuture<'_, Result<(), StoreError>> {
            match &self.0 {
                Validate::Never => Box::pin(std::future::pending()),
                Validate::Fails(make) => Box::pin(std::future::ready(Err(make()))),
            }
        }
    }

    /// A SQLite deployment's `[backup]` without a store, and the database
    /// file whose directory the default root is resolved against.
    fn defaulted(dir: &Path) -> (BackupConfig, PathBuf) {
        (BackupConfig::default(), dir.join("extenddb.sqlite"))
    }

    /// The defaulted root's leniency ends with the catalog's first row for
    /// it. The root is created empty (no identity file) each time: with no
    /// rows for it, a fresh root gets its file and the store is healthy;
    /// with rows for it and no file, an explicit root would be unhealthy
    /// (`identity file missing`), and so is the defaulted one, configured
    /// and reported rather than left unconfigured behind a warning.
    #[tokio::test]
    async fn a_defaulted_root_with_rows_is_handled_as_a_configured_one() {
        let dir = tempfile::tempdir().unwrap();
        let (backup, database) = defaulted(dir.path());
        let root = dir.path().join("backups");

        // Rows for the root (seeded, as a server that used the root leaves
        // them), no identity file: an explicit root's `identity file
        // missing`, on `/health` and in the refusals.
        let with_rows = StubCatalog {
            ids: vec!["store-1".to_owned()],
            ..StubCatalog::default()
        };
        let setup = configure_described(
            &backup,
            Some(&database),
            "sqlite",
            &with_rows,
            Caller::Server,
        )
        .await
        .unwrap();
        assert_eq!(setup.not_configured, None, "the store is configured");
        let store = setup
            .runtime
            .configured_store()
            .expect("a defaulted root with rows is a configured store");
        assert_eq!(store.kind, StoreKind::Filesystem);
        assert_eq!(
            store.root,
            root.canonicalize().unwrap().display().to_string()
        );
        assert_eq!(
            store.health().category(),
            Some("identity file missing"),
            "{:?}",
            store.health()
        );
        let report = setup.runtime.store_health().unwrap();
        assert_eq!(
            (report.status, report.reason),
            ("unhealthy", Some("identity file missing"))
        );
        assert!(
            !root.join(store_identity::STORE_IDENTITY_KEY).exists(),
            "no file is created over rows that name another id"
        );

        // No rows: the fresh-root case, the file created, the store healthy.
        let without_rows = StubCatalog::default();
        let setup = configure_described(
            &backup,
            Some(&database),
            "sqlite",
            &without_rows,
            Caller::Server,
        )
        .await
        .unwrap();
        assert_eq!(setup.not_configured, None);
        assert!(setup.runtime.configured_store().unwrap().is_healthy());
        assert!(root.join(store_identity::STORE_IDENTITY_KEY).exists());

        // No rows and a failed check (the file names another catalog, as a
        // catalog re-created by `destroy` and `init` leaves it): the
        // warning path, with the reason carried out for the commands.
        let foreign = store_identity::StoreIdentityFile::parse(
            br#"{"format_version":1,"store_id":"s-9","catalog_id":"other","backend":"sqlite","created_at_ms":0}"#,
        )
        .unwrap();
        std::fs::write(
            root.join(store_identity::STORE_IDENTITY_KEY),
            foreign.to_bytes(),
        )
        .unwrap();
        let setup = configure_described(
            &backup,
            Some(&database),
            "sqlite",
            &without_rows,
            Caller::Server,
        )
        .await
        .unwrap();
        assert!(setup.runtime.configured_store().is_none());
        let why = setup.not_configured.expect("the reason is carried out");
        assert!(
            matches!(&why, NotConfigured::DefaultRootUnusable { root, problem }
                if root.contains("backups") && problem.contains("names catalog other")),
            "{why:?}"
        );
        assert!(why.to_string().contains("no rows for it"), "{why}");
        // The same file over rows for the root: a configured, unhealthy
        // store (`owned by another catalog`), not the warning.
        let setup = configure_described(
            &backup,
            Some(&database),
            "sqlite",
            &with_rows,
            Caller::Server,
        )
        .await
        .unwrap();
        assert_eq!(
            setup
                .runtime
                .configured_store()
                .unwrap()
                .health()
                .category(),
            Some("owned by another catalog")
        );
    }

    /// A defaulted root `open` refuses (a parent others can write to) is the
    /// warning while the catalog has no rows for it, and a startup error
    /// naming the root once it has; an explicitly configured root is the
    /// startup error either way.
    #[tokio::test]
    async fn a_defaulted_root_open_refuses_is_a_warning_only_without_rows() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let (backup, database) = defaulted(dir.path());
        let setup = configure_described(
            &backup,
            Some(&database),
            "sqlite",
            &StubCatalog::default(),
            Caller::Server,
        )
        .await
        .unwrap();
        let why = setup.not_configured.expect("the warning path");
        assert!(
            matches!(&why, NotConfigured::DefaultRootUnusable { problem, .. }
                if problem.contains("writable by group or others")),
            "{why:?}"
        );
        let err = configure_described(
            &backup,
            Some(&database),
            "sqlite",
            &StubCatalog {
                ids: vec!["store-1".to_owned()],
                ..StubCatalog::default()
            },
            Caller::Server,
        )
        .await
        .err()
        .expect("with rows, the refusal stops startup")
        .to_string();
        assert!(err.contains("cannot be opened"), "{err}");
        assert!(err.contains("backup rows for it"), "{err}");
        assert!(err.contains(&dir.path().display().to_string()), "{err}");
    }

    /// A catalog whose rows cannot be read is answered as having rows, so a
    /// defaulted root whose check then fails is a configured, unhealthy
    /// store and not the warning that leaves the store unconfigured: the
    /// root may hold backups the read would have shown. Here the root
    /// carries a foreign identity file; the read failure reaches `establish`
    /// first, so the problem is the catalog's (`catalog unreadable`), and
    /// the next cycle's check reads again.
    #[tokio::test]
    async fn an_unreadable_catalog_keeps_a_failing_default_root_configured() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let (backup, database) = defaulted(dir.path());
        let root = dir.path().join("backups");
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let foreign = store_identity::StoreIdentityFile::parse(
            br#"{"format_version":1,"store_id":"s-9","catalog_id":"other","backend":"sqlite","created_at_ms":0}"#,
        )
        .unwrap();
        std::fs::write(
            root.join(store_identity::STORE_IDENTITY_KEY),
            foreign.to_bytes(),
        )
        .unwrap();
        let unreadable = StubCatalog {
            rows_unreadable: true,
            ..StubCatalog::default()
        };
        let setup = configure_described(
            &backup,
            Some(&database),
            "sqlite",
            &unreadable,
            Caller::Server,
        )
        .await
        .unwrap();
        assert_eq!(setup.not_configured, None, "the store is configured");
        let store = setup
            .runtime
            .configured_store()
            .expect("a root whose rows cannot be read is a configured store");
        assert_eq!(
            store.health().category(),
            Some("catalog unreadable"),
            "{:?}",
            store.health()
        );
    }

    /// The timed startup checks: a store that never answers leaves the
    /// server starting with the store `checking` after `LOCAL_CHECK_TIMEOUT`
    /// (not sooner, since a store that answers must get its outcome; not
    /// later, since the listener waits on this); `validate`'s I/O failure
    /// is a store condition that degrades (`failed validation`), its denial
    /// a configuration refusal.
    #[tokio::test(start_paused = true)]
    async fn the_startup_checks_are_bounded_and_split_store_conditions_from_refusals() {
        let catalog = StubCatalog::default();
        // The bound's value is part of the rule (a local disk answers in
        // milliseconds), so it is pinned here rather than read back.
        assert_eq!(LOCAL_CHECK_TIMEOUT, std::time::Duration::from_secs(5));
        let started = tokio::time::Instant::now();
        let stalled = FakeStore(Validate::Never);
        // The test's own guard, far past the bound: a check with no bound
        // fails here instead of hanging.
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            run_local_checks(&stalled, StoreKind::Filesystem, "/r", &catalog, "test"),
        )
        .await
        .expect("the checks return at their bound");
        assert!(matches!(outcome, LocalChecks::TimedOut));
        assert_eq!(
            started.elapsed(),
            LOCAL_CHECK_TIMEOUT,
            "the bound is the whole wait, no more and no less"
        );

        let io = FakeStore(Validate::Fails(|| StoreError::Io {
            context: "write probe /r/.put-validate.tmp".to_owned(),
            source: std::io::Error::from_raw_os_error(28),
        }));
        let outcome = run_local_checks(&io, StoreKind::Filesystem, "/r", &catalog, "test").await;
        let LocalChecks::Done(Err(problem)) = outcome else {
            panic!("an I/O failure of the probe degrades the store");
        };
        assert_eq!(problem.category(), "failed validation");
        assert!(problem.to_string().contains("write probe"), "{problem}");

        let denied = FakeStore(Validate::Fails(|| {
            StoreError::PermissionDenied("/r has mode 0755".to_owned())
        }));
        let outcome =
            run_local_checks(&denied, StoreKind::Filesystem, "/r", &catalog, "test").await;
        assert!(matches!(
            outcome,
            LocalChecks::Refused(StoreError::PermissionDenied(_))
        ));
    }

    /// `NotConfigured`'s sentences: each says why there are no store-backed
    /// backups, and the unusable root's names the root and the failure.
    #[test]
    fn not_configured_reads_as_the_commands_print_it() {
        assert!(
            NotConfigured::Absent
                .to_string()
                .contains("[backup.store] is absent")
        );
        assert!(NotConfigured::Disabled.to_string().contains("is disabled"));
        let missing = NotConfigured::DefaultRootMissing(PathBuf::from("/srv/db/backups"));
        assert!(
            missing
                .to_string()
                .contains("/srv/db/backups does not exist yet")
        );
        let unusable = NotConfigured::DefaultRootUnusable {
            root: "/srv/db/backups".to_owned(),
            problem: "extenddb-store.json names catalog other".to_owned(),
        }
        .to_string();
        assert!(
            unusable.contains("/srv/db/backups failed its check"),
            "{unusable}"
        );
        assert!(unusable.contains("names catalog other"), "{unusable}");
        assert!(unusable.contains("no rows for it"), "{unusable}");
    }

    /// The banner and the startup errors spell a root as written, quoting
    /// one that is empty or begins or ends with whitespace so the message
    /// shows where the path ends.
    #[test]
    fn a_root_with_edge_whitespace_is_quoted_in_the_banner() {
        let line = |path: &str| {
            let backup = BackupConfig {
                store: Some(extenddb_backup_store::BackupStoreConfig {
                    kind: BackupStoreKind::Filesystem,
                    filesystem: Some(extenddb_backup_store::FilesystemStoreConfig {
                        path: PathBuf::from(path),
                    }),
                    s3: None,
                }),
                ..BackupConfig::default()
            };
            banner_line(&backup, None)
        };
        assert!(
            line("/srv/backups ").contains("filesystem path \"/srv/backups \""),
            "{}",
            line("/srv/backups ")
        );
        assert!(
            line(" /srv/backups").contains("filesystem path \" /srv/backups\""),
            "{}",
            line(" /srv/backups")
        );
        assert!(
            line("/srv/backups").contains("filesystem path /srv/backups"),
            "{}",
            line("/srv/backups")
        );
        assert!(
            line("").contains("filesystem path \"\" (empty)"),
            "{}",
            line("")
        );
    }
}
