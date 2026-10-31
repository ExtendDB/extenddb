// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `[backup]` section: the knobs backup and restore jobs read, and the
//! store they write to.
//!
//! ```toml
//! [backup]
//! data_file_target_bytes = 67108864  # uncompressed bytes per data file; minimum 32 MiB
//! heartbeat_interval_secs = 15       # job heartbeat timer
//! stale_job_after_secs = 300         # heartbeat age after which a job's owner is dead
//! failed_row_retention_secs = 604800 # FAILED tombstones kept for the operator, 7 days
//! reconcile_interval_secs = 60       # reconciler cycle
//! max_concurrent_jobs = 4            # backup and restore jobs running at once on this instance
//! # spool_path = "/var/lib/extenddb/spool"  # restore download area for an S3 store
//!
//! [backup.store]
//! kind = "filesystem"                # "filesystem" | "s3" | "disabled"
//! [backup.store.filesystem]
//! path = "/var/lib/extenddb/backups"
//! ```
//!
//! `stale_job_after_secs` must be at least three heartbeat periods: a job
//! checks before each store write that its latest heartbeat confirmed its
//! claim within one period, and the margin is what makes that detection
//! precede a reconciler's deletion of the job's prefix. The period the floor
//! is computed from is the one the jobs run at: the larger of
//! `heartbeat_interval_secs` and [`JOB_HEARTBEAT_INTERVAL`] (15 s), since
//! until a later release wires the knob to the jobs every backend heartbeats
//! at that constant, and a floor on a shorter configured interval would let
//! the reconciler declare a live job dead between two of its heartbeats. The
//! floor is a floor: on SQLite a heartbeat waits for the engine write lock
//! like any writer, so a long write transaction delays it, and a
//! `stale_job_after_secs` near three periods on a busy file can declare a
//! live restore dead.
//!
//! `max_concurrent_jobs` is at most [`MAX_CONCURRENT_JOBS`]: a value over
//! the runtime's permit ceiling would stop the server at startup rather than
//! at load, and the PostgreSQL pool rule makes anything over a few hundred
//! meaningless anyway.
//!
//! The store. `[backup.store]` is the backup store crate's
//! [`BackupStoreConfig`], nested under its own key so the one definition of
//! the shape stays in that crate and `deny_unknown_fields` holds on both
//! tables (`serde(flatten)` would drop it). Its keys, defaults and `open`'s
//! refusals are that crate's `config` module doc. What this module adds is
//! the per-backend rule for an absent section and the spool
//! ([`BackupConfig::resolve_store`]):
//!
//! - On a backend whose database is one file on this host (file-backed
//!   SQLite), a missing `[backup.store]` is `kind = "filesystem"` with
//!   `path = "backups/"` beside the database file. That defaulted root is
//!   created `0700` if missing, and a failure of its validation is a logged
//!   warning that leaves the store unconfigured, not a startup error, so an
//!   upgrade never stops a server that never configured backups; an
//!   explicitly configured root that fails validation is a startup error.
//! - On PostgreSQL, MongoDB, Cassandra and an in-memory SQLite database
//!   (no file, so no default root), a missing section leaves the store
//!   unconfigured and startup logs that.
//! - `kind = "disabled"` leaves the store unconfigured on purpose, on any
//!   backend.
//! - Without a configured store, CreateBackup writes the in-catalog format
//!   and logs a deprecation warning naming `[backup.store]` on each call;
//!   backups of either format stay readable, listable, restorable and
//!   deletable.
//!
//! The spool. Restores from an S3 store download to `spool_path`, created
//! `0700` with files `0600`. On file-backed SQLite it defaults to
//! `backup-spool/` beside the database file; on every other backend an S3
//! store needs `spool_path` and startup refuses without it (a filesystem
//! store needs no spool: files are verified in place). A `spool_path` that
//! contains or is contained in a filesystem store root is refused (the store
//! would list and delete it, and a sync would read its first component as an
//! account id), and so is one that is or contains the database file's
//! directory. A filesystem store root that is or contains the database
//! file's directory is refused too: the database and its sidecars would sit
//! among the backups. An empty root is compared with nothing (it names no
//! directory) and left for `open` to refuse by name. The spool's `.lock`
//! another live instance holds is refused at startup by the server, which
//! owns the lock file.
//!
//! A relative `[backup.store.filesystem] path` or `spool_path` is made
//! absolute against the working directory at config load
//! ([`BackupConfig::make_paths_absolute`]), where `storage.path` is: `serve`
//! daemonizes after loading, and the child's working directory is `/`.

use std::path::{Path, PathBuf};

pub use extenddb_backup_store::{
    BackupStoreConfig, BackupStoreKind, FilesystemStoreConfig, S3StoreConfig,
};
use extenddb_storage::backup_catalog::JOB_HEARTBEAT_INTERVAL;
use serde::Deserialize;

/// The smallest `data_file_target_bytes`: the cap on one line of a data
/// file, so a file closes at most one line past the knob.
pub const MIN_DATA_FILE_TARGET_BYTES: u64 = 32 * 1024 * 1024;

/// The smallest ratio of `stale_job_after_secs` to the heartbeat period
/// ([`BackupConfig::effective_heartbeat_interval_secs`]).
pub const MIN_STALE_HEARTBEATS: u64 = 3;

/// The largest `max_concurrent_jobs`.
pub const MAX_CONCURRENT_JOBS: usize = 1024;

/// The directory name of the defaulted filesystem store root beside a
/// database file.
pub const DEFAULT_ROOT_DIR_NAME: &str = "backups";

/// The directory name of the defaulted restore spool beside a database file.
pub const DEFAULT_SPOOL_DIR_NAME: &str = "backup-spool";

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackupConfig {
    /// Uncompressed bytes per data file; a file closes once it reaches this,
    /// so it exceeds it by less than one line.
    #[serde(default = "default_data_file_target_bytes")]
    pub data_file_target_bytes: u64,
    /// The job heartbeat timer.
    #[serde(default = "default_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
    /// Heartbeat age after which a job's owner is dead, and the delay before
    /// the reconciler deletes a dead job's prefix.
    #[serde(default = "default_stale_job_after_secs")]
    pub stale_job_after_secs: u64,
    /// How long FAILED tombstones are kept for the operator.
    #[serde(default = "default_failed_row_retention_secs")]
    pub failed_row_retention_secs: u64,
    /// The reconciler's cycle.
    #[serde(default = "default_reconcile_interval_secs")]
    pub reconcile_interval_secs: u64,
    /// Backup and restore jobs running at once on this instance, the
    /// in-catalog creates and restores included; a further CreateBackup or
    /// RestoreTableFromBackup is `LimitExceededException`.
    #[serde(default = "default_max_concurrent_jobs")]
    pub max_concurrent_jobs: usize,
    /// Restore download area for an S3 store. Defaults beside the database
    /// file on SQLite; required on the other backends with an S3 store.
    #[serde(default)]
    pub spool_path: Option<String>,
    /// `[backup.store]`: the store this instance writes backups to. `None`
    /// when the section is absent, which [`BackupConfig::resolve_store`]
    /// turns into the per-backend default.
    #[serde(default)]
    pub store: Option<BackupStoreConfig>,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            data_file_target_bytes: default_data_file_target_bytes(),
            heartbeat_interval_secs: default_heartbeat_interval_secs(),
            stale_job_after_secs: default_stale_job_after_secs(),
            failed_row_retention_secs: default_failed_row_retention_secs(),
            reconcile_interval_secs: default_reconcile_interval_secs(),
            max_concurrent_jobs: default_max_concurrent_jobs(),
            spool_path: None,
            store: None,
        }
    }
}

/// Why no store is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unconfigured {
    /// `[backup.store]` is absent and this backend has no default root (a
    /// server database, or an in-memory SQLite database).
    Absent,
    /// `kind = "disabled"`.
    Disabled,
}

/// The store `[backup.store]` and the backend together select.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectiveStore {
    /// No store: CreateBackup writes the in-catalog format.
    Unconfigured(Unconfigured),
    /// A store to open and validate at startup.
    Configured {
        config: BackupStoreConfig,
        /// The section was absent and the root is the default beside the
        /// database file: the server creates it if missing and treats a
        /// validation failure as a warning that leaves the store
        /// unconfigured.
        defaulted: bool,
    },
}

/// What [`BackupConfig::resolve_store`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedStore {
    pub store: EffectiveStore,
    /// The restore spool, when the configured store is S3.
    pub spool_path: Option<PathBuf>,
}

impl BackupConfig {
    /// The period the jobs heartbeat at: the larger of
    /// `heartbeat_interval_secs` and [`JOB_HEARTBEAT_INTERVAL`], which is
    /// what every backend runs at until the knob reaches the jobs.
    #[must_use]
    pub fn effective_heartbeat_interval_secs(&self) -> u64 {
        self.heartbeat_interval_secs
            .max(JOB_HEARTBEAT_INTERVAL.as_secs())
    }

    /// Make `[backup.store.filesystem] path` and `spool_path` absolute
    /// against `cwd` (`~` expanded first), so a relative value written in
    /// the config file names the directory it does from the shell that
    /// started the server, in the daemonized child too. An absolute value
    /// is left as it is, and so is an empty one, which `open` refuses as it
    /// did (it would otherwise name the working directory as the root).
    pub fn make_paths_absolute(&mut self, cwd: &Path) {
        if let Some(fs) = self.store.as_mut().and_then(|s| s.filesystem.as_mut())
            && !fs.path.is_absolute()
            && !fs.path.as_os_str().is_empty()
        {
            let expanded = PathBuf::from(crate::expand_tilde(&fs.path.to_string_lossy()));
            fs.path = if expanded.is_absolute() {
                expanded
            } else {
                cwd.join(expanded)
            };
        }
        if let Some(spool) = self.spool_path.as_mut() {
            let expanded = PathBuf::from(crate::expand_tilde(spool));
            *spool = if expanded.is_absolute() {
                expanded
            } else {
                cwd.join(expanded)
            }
            .to_string_lossy()
            .into_owned();
        }
    }

    /// The cross-field rules, checked at load.
    ///
    /// # Errors
    ///
    /// A message naming the key and the rule it breaks.
    pub fn validate(&self) -> Result<(), String> {
        if self.heartbeat_interval_secs == 0 {
            return Err("backup.heartbeat_interval_secs must be at least 1".to_owned());
        }
        let period = self.effective_heartbeat_interval_secs();
        let floor = period.saturating_mul(MIN_STALE_HEARTBEATS);
        if self.stale_job_after_secs < floor {
            return Err(format!(
                "backup.stale_job_after_secs = {} is under {MIN_STALE_HEARTBEATS} * the job \
                 heartbeat period of {period} s = {floor} (the larger of \
                 backup.heartbeat_interval_secs = {} and the {} s the jobs heartbeat at in \
                 this release); a reconciler could declare a running job dead between two \
                 of its heartbeats",
                self.stale_job_after_secs,
                self.heartbeat_interval_secs,
                JOB_HEARTBEAT_INTERVAL.as_secs()
            ));
        }
        if self.data_file_target_bytes < MIN_DATA_FILE_TARGET_BYTES {
            return Err(format!(
                "backup.data_file_target_bytes = {} is under the minimum of \
                 {MIN_DATA_FILE_TARGET_BYTES} (32 MiB, the cap on one line of a data file)",
                self.data_file_target_bytes
            ));
        }
        if self.reconcile_interval_secs == 0 {
            return Err("backup.reconcile_interval_secs must be at least 1".to_owned());
        }
        if self.max_concurrent_jobs == 0 {
            return Err("backup.max_concurrent_jobs must be at least 1".to_owned());
        }
        if self.max_concurrent_jobs > MAX_CONCURRENT_JOBS {
            return Err(format!(
                "backup.max_concurrent_jobs = {} is over the maximum of {MAX_CONCURRENT_JOBS}",
                self.max_concurrent_jobs
            ));
        }
        if let Some(spool) = &self.spool_path
            && spool.trim().is_empty()
        {
            return Err("backup.spool_path must not be empty".to_owned());
        }
        Ok(())
    }

    /// The store and spool this configuration selects on a backend whose
    /// database is `database_file` (`Some` for file-backed SQLite, `None`
    /// for a server database or an in-memory one), by the rules in the
    /// module doc.
    ///
    /// # Errors
    ///
    /// A message naming the key: an S3 store without `spool_path` on a
    /// backend with no default spool; a `spool_path` inside or containing a
    /// filesystem store root, or inside or containing the database file's
    /// directory; a filesystem root that is or contains the database file's
    /// directory.
    pub fn resolve_store(&self, database_file: Option<&Path>) -> Result<ResolvedStore, String> {
        let database_dir = database_file.and_then(Path::parent).map(Path::to_path_buf);
        let store = match &self.store {
            None => match &database_dir {
                Some(dir) => EffectiveStore::Configured {
                    config: BackupStoreConfig {
                        kind: BackupStoreKind::Filesystem,
                        filesystem: Some(FilesystemStoreConfig {
                            path: dir.join(DEFAULT_ROOT_DIR_NAME),
                        }),
                        s3: None,
                    },
                    defaulted: true,
                },
                None => EffectiveStore::Unconfigured(Unconfigured::Absent),
            },
            Some(config) if config.kind == BackupStoreKind::Disabled => {
                EffectiveStore::Unconfigured(Unconfigured::Disabled)
            }
            Some(config) => EffectiveStore::Configured {
                config: config.clone(),
                defaulted: false,
            },
        };

        let explicit_spool = self
            .spool_path
            .as_deref()
            .map(|p| PathBuf::from(crate::expand_tilde(p)));
        let filesystem_root = match &store {
            EffectiveStore::Configured { config, .. }
                if config.kind == BackupStoreKind::Filesystem =>
            {
                // An empty `path` names no directory: it is compared with
                // nothing here (compared as written it would be the working
                // directory, and refused by name as the database directory
                // when the server starts there), and `open` refuses it.
                config
                    .filesystem
                    .as_ref()
                    .map(|f| f.path.clone())
                    .filter(|path| !path.as_os_str().is_empty())
            }
            _ => None,
        };
        let is_s3 = matches!(
            &store,
            EffectiveStore::Configured { config, .. } if config.kind == BackupStoreKind::S3
        );

        // The spool rules apply to every explicit `spool_path`, so a
        // configuration that breaks them is refused before the store it
        // will one day be used with is selected. Each message spells both
        // paths as they were compared (resolved through existing symlinks).
        if let Some(spool) = &explicit_spool {
            if let Some(root) = &filesystem_root
                && paths_overlap(spool, root)
            {
                return Err(format!(
                    "backup.spool_path {} is inside or contains the filesystem backup store \
                     root {}; the store would list and delete the spool. Put the spool \
                     outside the root",
                    comparable_path(spool).display(),
                    comparable_path(root).display()
                ));
            }
            if let Some(dir) = &database_dir
                && path_is_or_contains(spool, dir)
            {
                return Err(format!(
                    "backup.spool_path {} is or contains the database directory {}; put the \
                     spool in a directory of its own",
                    comparable_path(spool).display(),
                    comparable_path(dir).display()
                ));
            }
        }
        // A filesystem root that is the database directory or an ancestor of
        // it would hold the database file and its sidecars among the
        // backups: a listing would read the directory's entries as account
        // ids and a deletion of the root's whole prefix would walk them. The
        // default root is a child of the directory and passes.
        if let (Some(root), Some(dir)) = (&filesystem_root, &database_dir)
            && path_is_or_contains(root, dir)
        {
            return Err(format!(
                "backup.store.filesystem.path {} is or contains the database directory {}; \
                 put the backup root in a directory of its own (the default is \
                 {DEFAULT_ROOT_DIR_NAME}/ beside the database file)",
                comparable_path(root).display(),
                comparable_path(dir).display()
            ));
        }

        let spool_path = if is_s3 {
            match (explicit_spool, &database_dir) {
                (Some(spool), _) => Some(spool),
                (None, Some(dir)) => Some(dir.join(DEFAULT_SPOOL_DIR_NAME)),
                (None, None) => {
                    return Err(
                        "backup.spool_path is required with an S3 backup store on this \
                         backend: a restore downloads its files there before loading them, \
                         and only a file-backed SQLite database has a default beside its file"
                            .to_owned(),
                    );
                }
            }
        } else {
            None
        };
        Ok(ResolvedStore { store, spool_path })
    }
}

/// Whether one path is the other or an ancestor of it, after each is made
/// absolute and lexically normalized (`.` and `..` resolved without
/// consulting the filesystem, since neither directory need exist yet) and
/// canonicalized when it exists, so a symlinked spelling of the same
/// directory compares equal.
#[must_use]
pub fn paths_overlap(a: &Path, b: &Path) -> bool {
    let a = comparable_path(a);
    let b = comparable_path(b);
    a.starts_with(&b) || b.starts_with(&a)
}

/// Whether `a` is `b` or an ancestor of it, compared as [`paths_overlap`]
/// compares.
#[must_use]
pub fn path_is_or_contains(a: &Path, b: &Path) -> bool {
    comparable_path(b).starts_with(comparable_path(a))
}

fn comparable_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    // The deepest existing ancestor decides the canonical spelling; the
    // rest is appended as written.
    let mut existing = normalized.clone();
    let mut tail = Vec::new();
    loop {
        if let Ok(canonical) = std::fs::canonicalize(&existing) {
            let mut out = canonical;
            for part in tail.iter().rev() {
                out.push(part);
            }
            return out;
        }
        match (existing.file_name(), existing.parent()) {
            (Some(name), Some(parent)) => {
                tail.push(name.to_os_string());
                existing = parent.to_path_buf();
            }
            _ => return normalized,
        }
    }
}

fn default_data_file_target_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_heartbeat_interval_secs() -> u64 {
    15
}
fn default_stale_job_after_secs() -> u64 {
    300
}
fn default_failed_row_retention_secs() -> u64 {
    7 * 24 * 3600
}
fn default_reconcile_interval_secs() -> u64 {
    60
}
fn default_max_concurrent_jobs() -> usize {
    4
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_the_documented_ones() {
        let c = BackupConfig::default();
        assert_eq!(c.data_file_target_bytes, 67_108_864);
        assert_eq!(c.heartbeat_interval_secs, 15);
        assert_eq!(c.stale_job_after_secs, 300);
        assert_eq!(c.failed_row_retention_secs, 604_800);
        assert_eq!(c.reconcile_interval_secs, 60);
        assert_eq!(c.max_concurrent_jobs, 4);
        assert_eq!(c.spool_path, None);
        assert_eq!(c.store, None);
        c.validate().unwrap();
        let parsed: BackupConfig = toml::from_str("").unwrap();
        assert_eq!(parsed, c);
    }

    /// A relative root or spool is made absolute against the directory
    /// given (the shell's at load); an absolute one, and `~`, resolve as
    /// written.
    #[test]
    fn relative_store_and_spool_paths_are_made_absolute_at_load() {
        let mut c: BackupConfig = toml::from_str(
            "spool_path = \"rel-spool\"\n[store]\nkind = \"filesystem\"\n\
             [store.filesystem]\npath = \"rel-backups\"\n",
        )
        .unwrap();
        c.make_paths_absolute(Path::new("/srv/extenddb"));
        assert_eq!(
            c.store.as_ref().unwrap().filesystem.as_ref().unwrap().path,
            PathBuf::from("/srv/extenddb/rel-backups")
        );
        assert_eq!(c.spool_path.as_deref(), Some("/srv/extenddb/rel-spool"));

        let mut absolute: BackupConfig = toml::from_str(
            "spool_path = \"~/spool\"\n[store]\nkind = \"filesystem\"\n\
             [store.filesystem]\npath = \"/var/lib/extenddb/backups\"\n",
        )
        .unwrap();
        absolute.make_paths_absolute(Path::new("/srv/extenddb"));
        assert_eq!(
            absolute
                .store
                .as_ref()
                .unwrap()
                .filesystem
                .as_ref()
                .unwrap()
                .path,
            PathBuf::from("/var/lib/extenddb/backups")
        );
        let home = std::env::var("HOME").unwrap();
        assert_eq!(
            absolute.spool_path.as_deref(),
            Some(format!("{home}/spool").as_str())
        );
        // An S3 store has no path to resolve; `None` stays `None`.
        let mut none = BackupConfig::default();
        none.make_paths_absolute(Path::new("/srv"));
        assert_eq!(none, BackupConfig::default());
        // An empty root stays empty, for `open` to refuse; it must not
        // become the working directory.
        let mut empty: BackupConfig =
            toml::from_str("[store]\nkind = \"filesystem\"\n[store.filesystem]\npath = \"\"\n")
                .unwrap();
        empty.make_paths_absolute(Path::new("/srv/extenddb"));
        assert_eq!(
            empty
                .store
                .as_ref()
                .unwrap()
                .filesystem
                .as_ref()
                .unwrap()
                .path,
            PathBuf::from("")
        );
    }

    #[test]
    fn an_unknown_key_is_refused_at_every_level() {
        let e = toml::from_str::<BackupConfig>("store_kind = \"s3\"\n").unwrap_err();
        assert!(e.to_string().contains("store_kind"), "{e}");
        let e = toml::from_str::<BackupConfig>("[store]\nkid = \"s3\"\n").unwrap_err();
        assert!(e.to_string().contains("kid"), "{e}");
        let e = toml::from_str::<BackupConfig>(
            "[store]\nkind = \"s3\"\n[store.s3]\nbucket = \"b\"\nbukcet = \"b\"\n",
        )
        .unwrap_err();
        assert!(e.to_string().contains("bukcet"), "{e}");
    }

    #[test]
    fn the_store_section_is_the_crates_shape() {
        let c: BackupConfig =
            toml::from_str("[store]\nkind = \"s3\"\n[store.s3]\nbucket = \"b\"\nprefix = \"p/\"\n")
                .unwrap();
        let store = c.store.unwrap();
        assert_eq!(store.kind, BackupStoreKind::S3);
        assert_eq!(store.s3.unwrap().bucket, "b");
        let c: BackupConfig = toml::from_str("[store]\nkind = \"disabled\"\n").unwrap();
        assert_eq!(c.store.unwrap().kind, BackupStoreKind::Disabled);
    }

    #[test]
    fn a_stale_window_under_three_heartbeats_is_refused() {
        let c = BackupConfig {
            heartbeat_interval_secs: 20,
            stale_job_after_secs: 59,
            ..BackupConfig::default()
        };
        let e = c.validate().unwrap_err();
        assert!(e.contains("stale_job_after_secs = 59"), "{e}");
        assert!(e.contains("= 60"), "{e}");
        let ok = BackupConfig {
            stale_job_after_secs: 60,
            ..c
        };
        ok.validate().unwrap();
    }

    /// The floor is computed from the period the jobs run at, not from a
    /// configured interval shorter than it: `heartbeat_interval_secs = 1`
    /// with `stale_job_after_secs = 3` would let the reconciler remove a live
    /// restore between two 15 s heartbeats, and is refused naming the 15 s
    /// period and the 45 s floor.
    #[test]
    fn the_stale_floor_is_three_times_the_period_the_jobs_heartbeat_at() {
        let c = BackupConfig {
            heartbeat_interval_secs: 1,
            stale_job_after_secs: 3,
            ..BackupConfig::default()
        };
        assert_eq!(c.effective_heartbeat_interval_secs(), 15);
        let e = c.validate().unwrap_err();
        assert!(e.contains("stale_job_after_secs = 3 "), "{e}");
        assert!(e.contains("period of 15 s = 45"), "{e}");
        assert!(e.contains("heartbeat_interval_secs = 1 "), "{e}");
        let under = BackupConfig {
            stale_job_after_secs: 44,
            ..c.clone()
        };
        assert!(under.validate().is_err());
        let ok = BackupConfig {
            stale_job_after_secs: 45,
            ..c
        };
        ok.validate().unwrap();
        // A configured interval over the constant is the period.
        let long = BackupConfig {
            heartbeat_interval_secs: 40,
            stale_job_after_secs: 120,
            ..BackupConfig::default()
        };
        assert_eq!(long.effective_heartbeat_interval_secs(), 40);
        long.validate().unwrap();
    }

    #[test]
    fn max_concurrent_jobs_has_an_upper_bound() {
        let at_most = BackupConfig {
            max_concurrent_jobs: MAX_CONCURRENT_JOBS,
            ..BackupConfig::default()
        };
        at_most.validate().unwrap();
        let over = BackupConfig {
            max_concurrent_jobs: MAX_CONCURRENT_JOBS + 1,
            ..BackupConfig::default()
        };
        let e = over.validate().unwrap_err();
        assert!(e.contains("max_concurrent_jobs = 1025"), "{e}");
        assert!(e.contains("1024"), "{e}");
        let huge = BackupConfig {
            max_concurrent_jobs: usize::MAX,
            ..BackupConfig::default()
        };
        assert!(huge.validate().is_err());
    }

    #[test]
    fn the_other_floors_are_checked() {
        let small_file = BackupConfig {
            data_file_target_bytes: MIN_DATA_FILE_TARGET_BYTES - 1,
            ..BackupConfig::default()
        };
        assert!(
            small_file
                .validate()
                .unwrap_err()
                .contains("data_file_target_bytes")
        );
        let no_jobs = BackupConfig {
            max_concurrent_jobs: 0,
            ..BackupConfig::default()
        };
        assert!(
            no_jobs
                .validate()
                .unwrap_err()
                .contains("max_concurrent_jobs")
        );
        let no_heartbeat = BackupConfig {
            heartbeat_interval_secs: 0,
            ..BackupConfig::default()
        };
        assert!(
            no_heartbeat
                .validate()
                .unwrap_err()
                .contains("heartbeat_interval_secs")
        );
        let no_cycle = BackupConfig {
            reconcile_interval_secs: 0,
            ..BackupConfig::default()
        };
        assert!(
            no_cycle
                .validate()
                .unwrap_err()
                .contains("reconcile_interval_secs")
        );
        let empty_spool = BackupConfig {
            spool_path: Some("  ".to_owned()),
            ..BackupConfig::default()
        };
        assert!(empty_spool.validate().unwrap_err().contains("spool_path"));
    }

    fn s3_store() -> BackupStoreConfig {
        toml::from_str("kind = \"s3\"\n[s3]\nbucket = \"b\"\n").unwrap()
    }

    fn fs_store(path: &str) -> BackupStoreConfig {
        toml::from_str(&format!(
            "kind = \"filesystem\"\n[filesystem]\npath = {path:?}\n"
        ))
        .unwrap()
    }

    /// An absent section on a file-backed database is the filesystem store
    /// beside the file, marked defaulted; on a server or in-memory database
    /// it is unconfigured; `disabled` is unconfigured on both.
    #[test]
    fn an_absent_section_defaults_per_backend() {
        let db = Path::new("/var/lib/extenddb/data.sqlite");
        let resolved = BackupConfig::default().resolve_store(Some(db)).unwrap();
        assert_eq!(
            resolved.store,
            EffectiveStore::Configured {
                config: fs_store("/var/lib/extenddb/backups"),
                defaulted: true,
            }
        );
        assert_eq!(
            resolved.spool_path, None,
            "a filesystem store needs no spool"
        );

        let resolved = BackupConfig::default().resolve_store(None).unwrap();
        assert_eq!(
            resolved.store,
            EffectiveStore::Unconfigured(Unconfigured::Absent)
        );
        assert_eq!(resolved.spool_path, None);

        let disabled = BackupConfig {
            store: Some(toml::from_str("kind = \"disabled\"").unwrap()),
            ..BackupConfig::default()
        };
        for database in [Some(db), None] {
            assert_eq!(
                disabled.resolve_store(database).unwrap().store,
                EffectiveStore::Unconfigured(Unconfigured::Disabled)
            );
        }

        let explicit = BackupConfig {
            store: Some(fs_store("/srv/backups")),
            ..BackupConfig::default()
        };
        assert_eq!(
            explicit.resolve_store(Some(db)).unwrap().store,
            EffectiveStore::Configured {
                config: fs_store("/srv/backups"),
                defaulted: false,
            }
        );
    }

    /// An S3 store's spool: explicit on any backend, defaulted beside a
    /// database file, required otherwise with the key named.
    #[test]
    fn the_spool_is_required_with_s3_except_beside_a_database_file() {
        let db = Path::new("/var/lib/extenddb/data.sqlite");
        let s3 = BackupConfig {
            store: Some(s3_store()),
            ..BackupConfig::default()
        };
        assert_eq!(
            s3.resolve_store(Some(db)).unwrap().spool_path,
            Some(PathBuf::from("/var/lib/extenddb/backup-spool"))
        );
        let e = s3.resolve_store(None).unwrap_err();
        assert!(e.contains("backup.spool_path is required"), "{e}");

        let with_spool = BackupConfig {
            spool_path: Some("/srv/spool".to_owned()),
            ..s3.clone()
        };
        for database in [Some(db), None] {
            assert_eq!(
                with_spool.resolve_store(database).unwrap().spool_path,
                Some(PathBuf::from("/srv/spool"))
            );
        }
    }

    /// Every cell of the spool placement rule: inside the root, containing
    /// the root, equal to the root; the database directory itself, an
    /// ancestor of it; and the accepted shapes (a sibling of the root, a
    /// directory under the database directory, a path elsewhere), each
    /// against a filesystem store and against an S3 store.
    #[test]
    fn a_spool_inside_or_containing_the_root_or_the_database_directory_is_refused() {
        let db = Path::new("/var/lib/extenddb/data.sqlite");
        let resolve = |store: BackupStoreConfig, spool: &str| {
            BackupConfig {
                store: Some(store),
                spool_path: Some(spool.to_owned()),
                ..BackupConfig::default()
            }
            .resolve_store(Some(db))
        };
        for spool in [
            "/srv/backups/spool",
            "/srv",
            "/srv/backups",
            "/srv/backups/../backups",
        ] {
            let e = resolve(fs_store("/srv/backups"), spool).unwrap_err();
            assert!(e.contains("filesystem backup store root"), "{spool}: {e}");
        }
        for spool in ["/var/lib/extenddb", "/var/lib", "/var/lib/extenddb/./"] {
            let e = resolve(fs_store("/srv/backups"), spool).unwrap_err();
            assert!(e.contains("database directory"), "{spool}: {e}");
            let e = resolve(s3_store(), spool).unwrap_err();
            assert!(e.contains("database directory"), "{spool}: {e}");
        }
        for spool in [
            "/srv/backup-spool",
            "/var/lib/extenddb/backup-spool",
            "/tmp/extenddb-spool",
        ] {
            resolve(fs_store("/srv/backups"), spool).unwrap_or_else(|e| panic!("{spool}: {e}"));
            let resolved = resolve(s3_store(), spool).unwrap_or_else(|e| panic!("{spool}: {e}"));
            assert_eq!(resolved.spool_path, Some(PathBuf::from(spool)));
        }
        // The defaulted root beside the database file is a root too.
        let e = BackupConfig {
            spool_path: Some("/var/lib/extenddb/backups/spool".to_owned()),
            ..BackupConfig::default()
        }
        .resolve_store(Some(db))
        .unwrap_err();
        assert!(e.contains("filesystem backup store root"), "{e}");
    }

    #[test]
    fn path_overlap_follows_an_existing_symlink_and_normalizes_dots() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(paths_overlap(&link.join("spool"), &real));
        assert!(paths_overlap(&real, &link));
        assert!(paths_overlap(
            &dir.path().join("real/x/../y"),
            &dir.path().join("real/y/z")
        ));
        assert!(!paths_overlap(
            &dir.path().join("real-2"),
            &dir.path().join("real")
        ));
    }

    /// A filesystem root that is the database directory (`"."` with the
    /// database file in the working directory, or the directory spelled
    /// out) or an ancestor of it is refused naming the key and both
    /// directories; a child of the directory (the default root among them),
    /// a sibling and a root elsewhere are accepted, and the rule does not
    /// apply to an S3 store or to a server database (no directory).
    #[test]
    fn a_root_that_is_or_contains_the_database_directory_is_refused() {
        let db = Path::new("/var/lib/extenddb/data.sqlite");
        let resolve = |path: &str| {
            BackupConfig {
                store: Some(fs_store(path)),
                ..BackupConfig::default()
            }
            .resolve_store(Some(db))
        };
        for root in ["/var/lib/extenddb", "/var/lib/extenddb/./", "/var/lib", "/"] {
            let e = resolve(root).unwrap_err();
            assert!(e.contains("backup.store.filesystem.path"), "{root}: {e}");
            assert!(
                e.contains("database directory /var/lib/extenddb"),
                "{root}: {e}"
            );
        }
        for root in [
            "/var/lib/extenddb/backups",
            "/var/lib/extenddb-backups",
            "/srv/backups",
        ] {
            resolve(root).unwrap_or_else(|e| panic!("{root}: {e}"));
        }
        BackupConfig::default()
            .resolve_store(Some(db))
            .expect("the default root is a child of the directory");
        BackupConfig {
            store: Some(fs_store("/var/lib/extenddb")),
            ..BackupConfig::default()
        }
        .resolve_store(None)
        .expect("a server database has no directory to protect");
    }

    /// An empty root is compared with nothing: a server started from the
    /// database's directory (`cd /var/lib/extenddb && extenddb serve`, the
    /// layout `init` leaves) with `path = ""` is not refused as a root that
    /// is the database directory, a path the operator never wrote; the
    /// configuration resolves with the empty path, which `open` refuses by
    /// name. The spool rule likewise compares nothing with it.
    #[test]
    fn an_empty_root_is_left_for_open_to_refuse() {
        let cwd = std::env::current_dir().unwrap();
        let db = cwd.join("data.sqlite");
        let resolved = BackupConfig {
            store: Some(fs_store("")),
            ..BackupConfig::default()
        }
        .resolve_store(Some(&db))
        .expect("an empty root is not the working directory");
        assert!(matches!(
            resolved.store,
            EffectiveStore::Configured { config, defaulted: false }
                if config.filesystem.as_ref().unwrap().path.as_os_str().is_empty()
        ));
        BackupConfig {
            spool_path: Some("/srv/spool".to_owned()),
            store: Some(fs_store("")),
            ..BackupConfig::default()
        }
        .resolve_store(Some(&db))
        .expect("a spool is not inside an empty root");
    }

    /// A refusal spells both paths as they were compared, so a spool given
    /// through a symlink and a database directory given by its real path do
    /// not read as two different places.
    #[test]
    fn a_placement_refusal_spells_both_paths_the_same_way() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let e = BackupConfig {
            spool_path: Some(link.display().to_string()),
            store: Some(toml::from_str("kind = \"disabled\"").unwrap()),
            ..BackupConfig::default()
        }
        .resolve_store(Some(&real.join("data.sqlite")))
        .unwrap_err();
        let real = real.canonicalize().unwrap().display().to_string();
        assert!(
            e.contains(&format!(
                "backup.spool_path {real} is or contains the database directory {real}"
            )),
            "{e}"
        );
        assert!(!e.contains("link"), "{e}");
    }
}
