// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The MongoDB backend's [`CatalogMigrations`], which `init`, `migrate` and
//! `verify` call through the installed backend.
//!
//! A command's [`Target`] gives the connection string the config file
//! names, which the server uses too; the databases are always
//! `extenddb_catalog` and `extenddb_data`. `migrate --yes` and `--forget`
//! read the status before they take the lock, and a store that is not a
//! catalog is refused before anything is written to it: taking the lock
//! writes the lock document into the catalog's `settings` collection. Only
//! `init`, whose target names the data connection it records, takes the
//! lock on a store that is not a catalog yet.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, HeldLock, LedgerRow, Listed, MigrationError, MigrationId,
    RunKind, RunReport, Runner, Target,
};

use super::{
    CATALOG_DATABASE, DATA_DATABASE, Lease, MigrationLock, MongoMigrations, MongoTarget, Timing,
    catalog_status, status_holding,
};

/// The MongoDB backend's catalog migrations for the commands.
pub struct MongoCatalogMigrations;

/// What the migration lock carries for [`CatalogMigrations::run`] and
/// [`CatalogMigrations::forget`].
struct Held {
    lock: MigrationLock,
    target: MongoTarget,
}

fn held(lock: &HeldLock) -> Result<&Held, MigrationError> {
    lock.get::<Held>().ok_or_else(|| {
        MigrationError::Refused("the migration lock was not taken by the MongoDB backend".into())
    })
}

/// The lease of a lock this backend took, which `init`'s bootstrapper checks
/// before each row it writes; `None` for another backend's lock.
pub(crate) fn lease_of(lock: &HeldLock) -> Option<Arc<Lease>> {
    held(lock).ok().map(|held| held.lock.lease())
}

#[async_trait]
impl CatalogMigrations for MongoCatalogMigrations {
    fn listed(&self, id: MigrationId) -> Listed {
        Listed::of(&super::migration(id))
    }

    fn backup_advice(&self, _target: &Target) -> String {
        format!(
            "back up both MongoDB databases, the catalog database {CATALOG_DATABASE} and the data \
             database {DATA_DATABASE}"
        )
    }

    async fn status(&self, target: &Target) -> Result<CatalogStatus, MigrationError> {
        catalog_status(&MongoTarget::new(&target.location)).await
    }

    /// Read as the holder, so the requests of a long status read are
    /// progress that keeps the lock.
    async fn status_under_lock(
        &self,
        lock: &HeldLock,
        _target: &Target,
    ) -> Result<CatalogStatus, MigrationError> {
        let held = held(lock)?;
        status_holding(&held.target, &held.lock).await
    }

    async fn catalog_ledger(&self, target: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        let migrations = MongoMigrations::open(&MongoTarget::new(&target.location), None).await?;
        let rows = Runner::new(&migrations, "").catalog_ledger().await;
        migrations.close().await;
        rows
    }

    async fn lock(&self, target: &Target, timeout: Duration) -> Result<HeldLock, MigrationError> {
        lock(
            &MongoTarget::new(&target.location),
            target.data_connection.is_some(),
            timeout,
            Timing::PRODUCTION,
        )
        .await
        .map(|(lock, target)| {
            HeldLock::new(target.connection_string.clone(), Held { lock, target })
        })
    }

    async fn run(
        &self,
        lock: &HeldLock,
        release: &str,
        kind: RunKind,
    ) -> Result<RunReport, MigrationError> {
        let held = held(lock)?;
        let migrations = MongoMigrations::open(&held.target, Some(&held.lock)).await?;
        let runner = Runner::new(&migrations, release);
        let result = match kind {
            RunKind::Create | RunKind::Migrate => runner.migrate().await,
            RunKind::Reconverge => runner.reconverge().await,
        };
        migrations.close().await;
        result
    }

    async fn forget(&self, lock: &HeldLock, id: u32) -> Result<(), MigrationError> {
        let held = held(lock)?;
        let migrations = MongoMigrations::open(&held.target, Some(&held.lock)).await?;
        let result = Runner::new(&migrations, "").forget(id).await;
        migrations.close().await;
        result
    }
}

/// Take the migration lock on `target`, waiting up to `timeout` for another
/// migrator and then, within the same timeout, for running servers to stop.
/// Unless `creating` (`init`), a store that is not a catalog is refused
/// first, with nothing written.
///
/// # Errors
///
/// The store is not a catalog (`Uninitialized`, exit 1), the lock or a
/// server outlasted `timeout` ([`MigrationError::Locked`], exit 6), or a
/// request failed.
pub async fn lock(
    target: &MongoTarget,
    creating: bool,
    timeout: Duration,
    timing: Timing,
) -> Result<(MigrationLock, MongoTarget), MigrationError> {
    if !creating {
        let status = catalog_status(target).await?;
        if status == CatalogStatus::Uninitialized {
            return Err(MigrationError::Status(status));
        }
    }
    let started = Instant::now();
    let lock = MigrationLock::acquire(target, timeout, timing).await?;
    if let Err(e) = lock
        .wait_for_servers(timeout.saturating_sub(started.elapsed()))
        .await
    {
        lock.release().await;
        return Err(e);
    }
    Ok((lock, target.clone()))
}

#[cfg(test)]
mod tests {
    use extenddb_storage::migrations::{CatalogMigrations, Target};

    use super::MongoCatalogMigrations;

    #[test]
    fn the_backup_advice_names_both_databases_and_no_password() {
        let target = Target::at("mongodb://app:s3cret@localhost:27017/?replicaSet=rs0");
        let advice = MongoCatalogMigrations.backup_advice(&target);
        assert_eq!(
            advice,
            "back up both MongoDB databases, the catalog database extenddb_catalog and the data \
             database extenddb_data"
        );
    }
}
