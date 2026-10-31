// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The Cassandra backend's [`CatalogMigrations`], which `init`, `migrate`
//! and `verify` call through the installed backend.
//!
//! A command's [`Target`] gives the config file's connection string, which
//! carries the role the config names, and the admin role the command was
//! given (`--cassandra-user`), which replaces it. Every connect is bounded
//! by 10 s.

use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, HeldLock, LedgerRow, Listed, MigrationError, MigrationId,
    RunKind, RunReport, Runner, Target,
};

use super::backend::CassandraMigrations;
use super::connect::{Deployment, has_columns};
use super::locks::{LeaseHandle, LeaseTimes, MigrationLock};
use super::registrations::wait_for_no_servers;
use super::{catalog_status, foreign_contents, migration, status_holding};
use crate::cassandra_util::CassandraSession;

/// The Cassandra backend's catalog migrations for the commands.
pub struct CassandraCatalogMigrations;

/// What the migration lock carries for [`CatalogMigrations::run`] and
/// [`CatalogMigrations::forget`].
struct Held {
    lock: MigrationLock,
    keyspace_prefix: String,
    /// The application role, when the command connects as another role.
    app_role: Option<String>,
}

fn held(lock: &HeldLock) -> Result<&Held, MigrationError> {
    lock.get::<Held>().ok_or_else(|| {
        MigrationError::Refused("the migration lock was not taken by the Cassandra backend".into())
    })
}

/// The lease of a lock this backend took, which `init`'s bootstrapper checks
/// before each row it writes; `None` for another backend's lock.
pub(crate) fn lease_of(lock: &HeldLock) -> Option<LeaseHandle> {
    held(lock).ok().map(|held| held.lock.handle())
}

/// Refuse an uninitialized catalog keyspace that holds another program's
/// tables or `settings` rows, naming them: the status's advice for
/// `Uninitialized` is to run `extenddb init`, which refuses such a keyspace,
/// so the preview, `verify` and `migrate --yes` say what `init` would say.
///
/// # Errors
///
/// [`MigrationError::Refused`] naming the contents; a read failed.
async fn refuse_foreign(
    session: &CassandraSession,
    deployment: &Deployment,
    status: &CatalogStatus,
) -> Result<(), MigrationError> {
    if *status != CatalogStatus::Uninitialized {
        return Ok(());
    }
    let catalog = deployment.catalog_keyspace();
    let foreign = foreign_contents(session, &catalog).await?;
    if foreign.is_empty() {
        return Ok(());
    }
    Err(MigrationError::Refused(format!(
        "the keyspace {catalog} is not an ExtendDB catalog: it holds {}. Nothing was written to \
         it; check keyspace_prefix in the config file",
        foreign.join(", ")
    )))
}

#[async_trait]
impl CatalogMigrations for CassandraCatalogMigrations {
    fn listed(&self, id: MigrationId) -> Listed {
        Listed::of(&migration(id))
    }

    fn backup_advice(&self, target: &Target) -> String {
        match Deployment::from_target(target) {
            Ok(d) => format!(
                "back up the Cassandra catalog keyspace {} and every account keyspace {}, with \
                 a snapshot on every node taken while every server is stopped",
                d.catalog_keyspace(),
                d.account_keyspace("<account id>")
            ),
            Err(_) => "back up the Cassandra catalog keyspace and every account keyspace, with \
                       a snapshot on every node taken while every server is stopped"
                .to_owned(),
        }
    }

    async fn status(&self, target: &Target) -> Result<CatalogStatus, MigrationError> {
        let deployment = Deployment::from_target(target)?;
        let session = deployment.connect().await?;
        let status = catalog_status(session.clone(), &deployment.keyspace_prefix).await?;
        refuse_foreign(&session, &deployment, &status).await?;
        Ok(status)
    }

    /// Read as the holder, on the lock's session, so the requests of a long
    /// status read are progress that keeps the lock ([`status_holding`]).
    /// Another program's keyspace is refused as [`Self::status`] refuses it.
    async fn status_under_lock(
        &self,
        lock: &HeldLock,
        target: &Target,
    ) -> Result<CatalogStatus, MigrationError> {
        let held = held(lock)?;
        let deployment = Deployment::from_target(target)?;
        let status = status_holding(&held.lock, &held.keyspace_prefix).await?;
        refuse_foreign(held.lock.session(), &deployment, &status).await?;
        Ok(status)
    }

    async fn catalog_ledger(&self, target: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        let deployment = Deployment::from_target(target)?;
        let session = deployment.connect().await?;
        let migrations = CassandraMigrations::for_status(session, &deployment.keyspace_prefix);
        Runner::new(&migrations, "").catalog_ledger().await
    }

    /// Take the lock, then wait for every registered server to go away,
    /// both within `timeout`: a migration needs every replica of the catalog
    /// keyspace, and no server may run while the catalog changes. An
    /// uninitialized keyspace that holds another program's tables or
    /// `settings` rows is refused before anything is written to it, and
    /// named, since `init` refuses it too. A keyspace without the catalog's
    /// `settings` table is not a catalog; `init` creates it before it takes
    /// the lock.
    async fn lock(&self, target: &Target, timeout: Duration) -> Result<HeldLock, MigrationError> {
        let deployment = Deployment::from_target(target)?;
        let session = deployment.connect().await?;
        let catalog = deployment.catalog_keyspace();
        let status = catalog_status(session.clone(), &deployment.keyspace_prefix).await?;
        refuse_foreign(&session, &deployment, &status).await?;
        if !has_columns(&session, &catalog, "settings", &["key", "value"]).await? {
            return Err(MigrationError::Status(CatalogStatus::Uninitialized));
        }
        let lock = MigrationLock::acquire(session, &catalog, LeaseTimes::DEFAULT, timeout).await?;
        if let Err(e) = wait_for_no_servers(&lock, timeout).await {
            lock.release().await;
            return Err(e);
        }
        Ok(HeldLock::new(
            target.location.as_str(),
            Held {
                lock,
                app_role: deployment.app_role_if_other(),
                keyspace_prefix: deployment.keyspace_prefix,
            },
        ))
    }

    async fn run(
        &self,
        lock: &HeldLock,
        release: &str,
        kind: RunKind,
    ) -> Result<RunReport, MigrationError> {
        let held = held(lock)?;
        let migrations = CassandraMigrations::locked(&held.lock, &held.keyspace_prefix)
            .granting(held.app_role.clone());
        let runner = Runner::new(&migrations, release);
        match kind {
            RunKind::Create | RunKind::Migrate => runner.migrate().await,
            RunKind::Reconverge => runner.reconverge().await,
        }
    }

    async fn forget(&self, lock: &HeldLock, id: u32) -> Result<(), MigrationError> {
        let held = held(lock)?;
        let migrations = CassandraMigrations::locked(&held.lock, &held.keyspace_prefix);
        Runner::new(&migrations, "").forget(id).await
    }

    async fn release(&self, lock: HeldLock) {
        if let Ok(held) = held(&lock) {
            held.lock.release().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use extenddb_storage::migrations::{CatalogMigrations, Target};

    use super::CassandraCatalogMigrations;

    #[test]
    fn the_backup_advice_names_the_keyspaces_and_no_password() {
        let advice =
            CassandraCatalogMigrations.backup_advice(&Target::at("app:pw@127.0.0.1:9042/acme"));
        assert_eq!(
            advice,
            "back up the Cassandra catalog keyspace acme_catalog and every account keyspace \
             acme_account_<account id>, with a snapshot on every node taken while every server \
             is stopped"
        );
    }
}
