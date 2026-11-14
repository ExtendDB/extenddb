// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Step 3 of a restore: every data file read from the store and compared
//! with the manifest's `sha256`, `md5` and `bytes` before any file is
//! loaded.
//!
//! On an S3 store each file is written to the job's spool entry as it
//! arrives ([`Dest::Spool`]), and committed with its `.verified` marker once
//! the digests match; a resume skips the files an earlier run marked (the
//! load hashes the spooled bytes again as it decodes them, so a marker is a
//! reason not to download, not a reason not to check). On a
//! filesystem store ([`Dest::InPlace`]) the file is hashed as the store
//! streams it and nothing is written: the load reads the same file through
//! a second `get`, hashing it again, so a file replaced between the two
//! passes fails there.
//!
//! A `get` that fails as a transport error, at the call or while the body
//! arrives, is retried with a backoff of `min(2^n s, 60 s)` plus jitter for
//! as long as the job has been downloading for less than
//! [`DOWNLOAD_RETRY_BOUND`], counted from the first download attempt of the
//! job (one clock for every file, so a store that flaps through a backup of
//! many files costs the restore the bound once, not once per file), the
//! record RUNNING and heartbeated meanwhile; the shutdown token and the
//! heartbeat's loss end the wait. Every other failure fails the restore at
//! once: a digest or size mismatch (the file is not the backup's), an absent
//! file (a prefix changed under an AVAILABLE row), a permission or key
//! refusal, a local I/O error (the filesystem store's `Io`, or the spool's
//! disk), or any other store error. The filesystem store reports a mount
//! that stopped answering as `Io`, so the retry covers S3 stores only.
//! Every `get` the store fails, retried or not, is one `BackupStoreErrors{get}`;
//! a mismatch and the spool's own errors are not the store's.

use std::time::Duration;

use extenddb_backup_store::{BackupStore, StoreError};
use extenddb_core::backup_format::{CompressedHasher, DataFileEntry, ExtenddbManifest};
use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::CancellationToken;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;

use super::super::job::heartbeat::HeartbeatHandle;
use super::spool::Entry;

/// How long the job's downloads are retried over transport errors, from
/// the job's first download attempt, before the restore fails.
pub(crate) const DOWNLOAD_RETRY_BOUND: Duration = Duration::from_secs(15 * 60);

/// The longest wait between two attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Where a verified file goes.
pub(crate) enum Dest<'a> {
    /// Hashed as it streams, written nowhere: the load reads the store
    /// again (a filesystem store).
    InPlace,
    /// Written to the job's spool entry and marked `.verified`.
    Spool(&'a Entry),
}

/// Why the download stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Stop {
    /// The restore fails with this reason.
    Fail(String),
    /// The shutdown token fired.
    Cancelled,
    /// The heartbeat no longer confirms the claim.
    ClaimLost(String),
    /// The job stops with the record RUNNING and no FAILED written (the
    /// final count kept failing after every item was loaded and recorded);
    /// the reconciler resumes it once the record is stale.
    LeftRunning(String),
}

/// What one download runs with.
pub(crate) struct Downloader<'a> {
    pub store: &'a dyn BackupStore,
    /// The backup's store prefix, which every key is under.
    pub prefix: &'a str,
    pub heartbeat: &'a HeartbeatHandle,
    pub shutdown: &'a CancellationToken,
    pub metrics: Option<&'a MetricsCollector>,
}

/// Download and verify every data file of `manifest` into `dest` (module
/// doc), except the files `complete` marks (a resume's progress records
/// them applied whole, so they are neither downloaded nor loaded) and the
/// files the spool holds verified with the manifest's digest. Returns the
/// bytes read from the store over every attempt.
pub(crate) async fn download_all(
    d: &Downloader<'_>,
    manifest: &ExtenddbManifest,
    dest: &Dest<'_>,
    complete: &[bool],
) -> Result<u64, Stop> {
    // The retry bound's clock: one for the job, started here.
    let started = Instant::now();
    let mut bytes = 0;
    for (index, entry) in manifest.data_files.iter().enumerate() {
        if complete.get(index).copied().unwrap_or(false) {
            continue;
        }
        let index =
            u32::try_from(index).map_err(|_| Stop::Fail("too many data files".to_owned()))?;
        if let Dest::Spool(spool) = dest
            && spool.is_verified(index, &entry.sha256)
        {
            continue;
        }
        bytes += fetch_verified(d, entry, index, dest, started).await?;
    }
    Ok(bytes)
}

/// One file: attempts until the file verifies, a non-transport failure, the
/// retry bound (measured from `started`, the job's first attempt), the
/// shutdown or a lost claim.
async fn fetch_verified(
    d: &Downloader<'_>,
    entry: &DataFileEntry,
    index: u32,
    dest: &Dest<'_>,
    started: Instant,
) -> Result<u64, Stop> {
    let key = format!("{}{}", d.prefix, entry.key);
    let mut bytes = 0;
    for attempt in 0u32.. {
        if d.shutdown.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        if !d.heartbeat.confirms_the_claim() {
            return Err(Stop::ClaimLost(
                d.heartbeat
                    .why_not_confirmed(&format!("to download {}", entry.key)),
            ));
        }
        let outcome = tokio::select! {
            biased;
            () = d.shutdown.cancelled() => return Err(Stop::Cancelled),
            () = d.heartbeat.lost().cancelled() => return Err(Stop::ClaimLost(d.heartbeat.reason())),
            outcome = attempt_once(d, entry, index, &key, dest) => outcome,
        };
        bytes += outcome.bytes;
        if let Some(metrics) = d.metrics
            && outcome.bytes > 0
        {
            metrics.record_restore_bytes_downloaded(outcome.bytes);
        }
        let transport = match outcome.result {
            Ok(()) => return Ok(bytes),
            Err(Attempt::Fail(reason)) => return Err(Stop::Fail(reason)),
            Err(Attempt::StoreFailed(reason)) => {
                // The store's refusal counts as its error, as a transport
                // failure does; the restore then fails at once.
                if let Some(metrics) = d.metrics {
                    metrics.record_backup_store_error("get");
                }
                return Err(Stop::Fail(reason));
            }
            Err(Attempt::Transport(e)) => e,
        };
        if let Some(metrics) = d.metrics {
            metrics.record_backup_store_error("get");
        }
        let elapsed = started.elapsed();
        if elapsed >= DOWNLOAD_RETRY_BOUND {
            return Err(Stop::Fail(format!(
                "downloading {}: {transport} (the job has retried its downloads for {} s; the \
                 bound is {} s)",
                entry.key,
                elapsed.as_secs(),
                DOWNLOAD_RETRY_BOUND.as_secs()
            )));
        }
        let wait = backoff(attempt);
        tracing::warn!(
            "downloading {key}: {transport}; attempt {} retried in {:.1} s",
            attempt + 2,
            wait.as_secs_f64()
        );
        tokio::select! {
            biased;
            () = d.shutdown.cancelled() => return Err(Stop::Cancelled),
            () = d.heartbeat.lost().cancelled() => return Err(Stop::ClaimLost(d.heartbeat.reason())),
            () = tokio::time::sleep(wait) => {}
        }
    }
    unreachable!("the attempt loop returns")
}

/// `min(2^attempt s, 60 s)` plus a jitter of up to one second.
fn backoff(attempt: u32) -> Duration {
    let base = Duration::from_secs(1u64 << attempt.min(6));
    let jitter_ms = u64::from(uuid::Uuid::new_v4().as_u128() as u32) % 1000;
    base.min(MAX_BACKOFF) + Duration::from_millis(jitter_ms)
}

/// How one attempt ended.
enum Attempt {
    /// The restore fails with this reason; no retry, and not the store's
    /// error (a digest or size mismatch, the spool's disk).
    Fail(String),
    /// The store failed the `get` with an error that is not retried (an
    /// absent file, a refusal, an I/O error, any other); the restore fails
    /// with this reason and the error is counted as the store's.
    StoreFailed(String),
    /// Retried within the bound.
    Transport(StoreError),
}

struct AttemptOutcome {
    result: Result<(), Attempt>,
    /// Bytes this attempt read from the store.
    bytes: u64,
}

/// One `get` of the file, hashed and compared, written to the spool when
/// `dest` is one.
async fn attempt_once(
    d: &Downloader<'_>,
    entry: &DataFileEntry,
    index: u32,
    key: &str,
    dest: &Dest<'_>,
) -> AttemptOutcome {
    let mut bytes = 0;
    let result = attempt_inner(d, entry, index, key, dest, &mut bytes).await;
    AttemptOutcome { result, bytes }
}

async fn attempt_inner(
    d: &Downloader<'_>,
    entry: &DataFileEntry,
    index: u32,
    key: &str,
    dest: &Dest<'_>,
    bytes: &mut u64,
) -> Result<(), Attempt> {
    let mut body = match d.store.get(key).await {
        Ok(body) => body,
        Err(e @ StoreError::Transport { .. }) => return Err(Attempt::Transport(e)),
        Err(StoreError::NotFound) => {
            return Err(Attempt::StoreFailed(format!(
                "data file {} is not in the store",
                entry.key
            )));
        }
        Err(e) => {
            return Err(Attempt::StoreFailed(format!(
                "reading data file {} from the store: {e}",
                entry.key
            )));
        }
    };
    let mut file = match dest {
        Dest::InPlace => None,
        Dest::Spool(spool) => Some(spool.begin_file(index).await.map_err(|e| {
            Attempt::Fail(format!("writing data file {} to the spool: {e}", entry.key))
        })?),
    };
    let mut hasher = CompressedHasher::new();
    loop {
        let chunk = match body.try_next().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(e @ StoreError::Transport { .. }) => return Err(Attempt::Transport(e)),
            Err(e) => {
                return Err(Attempt::StoreFailed(format!(
                    "reading data file {} from the store: {e}",
                    entry.key
                )));
            }
        };
        *bytes += chunk.len() as u64;
        hasher.update(&chunk);
        if let Some(file) = &mut file {
            file.write_all(&chunk).await.map_err(|e| {
                Attempt::Fail(format!("writing data file {} to the spool: {e}", entry.key))
            })?;
        }
    }
    entry
        .expectations()
        .verify_digest(&hasher.finish())
        .map_err(|e| {
            Attempt::Fail(format!(
                "data file {} does not match the manifest: {e}",
                entry.key
            ))
        })?;
    if let (Some(file), Dest::Spool(spool)) = (file, dest) {
        spool
            .commit_file(index, file, &entry.sha256)
            .await
            .map_err(|e| {
                Attempt::Fail(format!("writing data file {} to the spool: {e}", entry.key))
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_backoff_doubles_to_a_minute_with_jitter() {
        for (attempt, base) in [(0, 1), (1, 2), (3, 8), (5, 32), (6, 64), (20, 64)] {
            let wait = backoff(attempt);
            let base = Duration::from_secs(base).min(MAX_BACKOFF);
            assert!(
                wait >= base && wait < base + Duration::from_secs(1),
                "{attempt}: {wait:?}"
            );
        }
    }
}
