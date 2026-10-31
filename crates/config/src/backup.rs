// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `[backup]` section: the knobs backup and restore jobs read.
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
//! The store itself (`[backup.store]`) is configured separately and is not
//! part of this struct.

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
    /// file on SQLite; required on the other backends once a store is
    /// configured.
    #[serde(default)]
    pub spool_path: Option<String>,
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
        }
    }
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
        Ok(())
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
        c.validate().unwrap();
        let parsed: BackupConfig = toml::from_str("").unwrap();
        assert_eq!(parsed, c);
    }

    #[test]
    fn an_unknown_key_is_refused() {
        let e = toml::from_str::<BackupConfig>("store_kind = \"s3\"\n").unwrap_err();
        assert!(e.to_string().contains("store_kind"), "{e}");
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
    }
}
