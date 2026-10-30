// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The installed backend's catalog migrations, as `init`, `migrate`,
//! `verify` and `serve` use them.
//!
//! The app crate does not name a backend crate, so every backend installs
//! its [`CatalogMigrations`] in its [`Backend`](crate::Backend).

use std::any::Any;
use std::time::Duration;

use async_trait::async_trait;

use super::ledger::LedgerRow;
use super::registry::MigrationId;
use super::status::{CatalogStatus, MigrationError, RunReport};
use super::table::{Kind, Migration, StoreKind};

/// One migration of a backend's table, as `migrate --list` prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Listed {
    /// The migration has bodies on this backend.
    Applies {
        kind: Kind,
        convergent: bool,
        stores: Vec<StoreKind>,
    },
    /// The migration changes nothing on this backend, for this reason.
    NotApplicable(&'static str),
}

impl Listed {
    /// The listing of one entry of a backend's table.
    #[must_use]
    pub fn of<H>(migration: &Migration<H>) -> Self {
        match migration {
            Migration::NotApplicable(reason) => Self::NotApplicable(reason),
            Migration::Applies { .. } => Self::Applies {
                kind: migration.kind(),
                convergent: migration.convergent(),
                stores: migration.store_kinds(),
            },
        }
    }
}

/// What a locked run does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunKind {
    /// Create the catalog (`init`): apply every migration to a store whose
    /// status is `Uninitialized`.
    Create,
    /// Apply every pending migration ([`Runner::migrate`](super::Runner::migrate)).
    Migrate,
    /// Run every migration again ([`Runner::reconverge`](super::Runner::reconverge)).
    Reconverge,
}

/// The migration lock of one deployment, held until it is dropped. What it
/// holds is the backend's own: the backend that took it reads it back with
/// [`HeldLock::get`].
pub struct HeldLock {
    location: String,
    held: Box<dyn Any + Send + Sync>,
}

impl HeldLock {
    /// A lock on the deployment at `location`.
    pub fn new(location: impl Into<String>, held: impl Any + Send + Sync) -> Self {
        Self {
            location: location.into(),
            held: Box::new(held),
        }
    }

    /// The location the lock was taken for.
    #[must_use]
    pub fn location(&self) -> &str {
        &self.location
    }

    /// The backend's lock value, if it has type `T`.
    #[must_use]
    pub fn get<T: Any>(&self) -> Option<&T> {
        self.held.downcast_ref()
    }
}

impl std::fmt::Debug for HeldLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldLock")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

/// The deployment a command works on, with the values the command's flags
/// gave (parsed by the command, or resolved by `init`'s bootstrapper). A
/// backend reads these fields and never the command line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Target {
    /// The deployment's storage location as the config file gives it
    /// (`[storage.<backend>]`): on SQLite the database file, on PostgreSQL
    /// the catalog database's connection string.
    pub location: String,
    /// The data store the command names (`--data-db` on PostgreSQL); `None`
    /// lets the backend read it from the catalog.
    pub data_store: Option<String>,
    /// The admin role for the backend's admin connection: on PostgreSQL the
    /// role of the pgvector attempt (`--pg-user` and `--pg-pass`, or the
    /// role `init` created the databases with).
    pub admin: Option<AdminRole>,
    /// The data store's connection string, which `init` records with
    /// migration 1. Only `init` sets it.
    pub data_connection: Option<String>,
}

impl Target {
    /// A target at `location` with no other values.
    #[must_use]
    pub fn at(location: impl Into<String>) -> Self {
        Self {
            location: location.into(),
            ..Self::default()
        }
    }
}

/// An admin role and its password. `Debug` prints no password.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct AdminRole {
    pub user: String,
    pub password: Option<String>,
}

impl std::fmt::Debug for AdminRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminRole")
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// A backend's catalog migrations, for the deployment a [`Target`] names.
#[async_trait]
pub trait CatalogMigrations: Send + Sync {
    /// This backend's entry for `id`. Opens nothing.
    fn listed(&self, id: MigrationId) -> Listed;

    /// What an operator backs up before migrating the deployment, as one
    /// clause: "back up ...". Opens nothing.
    fn backup_advice(&self, target: &Target) -> String;

    /// The environment assignment that names this deployment's store to a
    /// command run without a config file, such as
    /// `EXTENDDB__STORAGE__SQLITE__PATH=<absolute path>`, for the command a
    /// server started without one advises. `None` (the default) when the
    /// backend's location cannot be given that way without a secret in it.
    fn location_assignment(&self, target: &Target) -> Option<String> {
        let _ = target;
        None
    }

    /// Whether this process's environment names the store, in the variable
    /// [`location_assignment`](Self::location_assignment) sets, which
    /// overrides a config file's location. The advised command then carries
    /// that assignment beside the config file, so it acts on the store the
    /// command or server used. `false` (the default) when the backend has no
    /// such variable.
    fn location_from_environment(&self) -> bool {
        false
    }

    /// The catalog status. Takes no lock and writes nothing, so it runs
    /// beside a server.
    async fn status(&self, target: &Target) -> Result<CatalogStatus, MigrationError>;

    /// The catalog status, read under a lock this backend took, which
    /// [`run_locked`] and [`forget_locked`] read before they act. The
    /// status as [`CatalogMigrations::status`] reads it by default; a
    /// backend whose lock ends when its holder stops making progress reads
    /// it as the holder, so a long status read keeps the lock.
    async fn status_under_lock(
        &self,
        _lock: &HeldLock,
        target: &Target,
    ) -> Result<CatalogStatus, MigrationError> {
        self.status(target).await
    }

    /// The catalog store's ledger rows, in id order, for `verify` to print
    /// their notes. Read-only; empty for a store with no ledger.
    async fn catalog_ledger(&self, target: &Target) -> Result<Vec<LedgerRow>, MigrationError>;

    /// Take the migration lock, retrying for up to `timeout` while another
    /// process holds it ([`MigrationError::Locked`] after, exit 6). The lock
    /// carries what [`CatalogMigrations::run`] needs of `target`.
    async fn lock(&self, target: &Target, timeout: Duration) -> Result<HeldLock, MigrationError>;

    /// Run the migrations under a lock this backend took.
    async fn run(
        &self,
        lock: &HeldLock,
        release: &str,
        kind: RunKind,
    ) -> Result<RunReport, MigrationError>;

    /// Delete the ledger rows of an unreleased id under a lock this backend
    /// took ([`Runner::forget`](super::Runner::forget)).
    async fn forget(&self, lock: &HeldLock, id: u32) -> Result<(), MigrationError>;

    /// Release a lock this backend took. The default drops it, which frees
    /// a lock its connection or file holds; a backend whose lock is a row
    /// (Cassandra) deletes the row here, so the next migration or server
    /// need not wait for its TTL.
    async fn release(&self, lock: HeldLock) {
        drop(lock);
    }

    /// Notes about the deployment that its status does not show, for the
    /// preview and `verify` to print and `migrate --yes` to check before it
    /// takes the lock ([`before_lock`]). On PostgreSQL: tables and sequences
    /// the application role does not own. Read-only, like the status. None
    /// by default.
    async fn status_notes(&self, target: &Target) -> Result<Vec<StatusNote>, MigrationError> {
        let _ = target;
        Ok(Vec::new())
    }
}

/// A note from [`CatalogMigrations::status_notes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusNote {
    /// The line the preview and `verify` print.
    pub text: String,
    /// Whether `migrate --yes` refuses before it takes the lock while the
    /// note holds and the run would apply something.
    pub refuses_runs: bool,
}

/// What `migrate --yes` checks before it takes the lock: the backend's
/// status notes, returned for the operator to read, or a refusal (exit 1)
/// when a note refuses runs and the run would apply something, which is
/// every `--reconverge` and a `migrate` whose status is not `Current` or
/// `AheadCompatible`. `init` creates its objects and checks nothing.
///
/// # Errors
///
/// A note refuses the run, or as [`CatalogMigrations::status_notes`] and
/// [`CatalogMigrations::status`].
pub async fn before_lock(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    kind: RunKind,
) -> Result<Vec<StatusNote>, MigrationError> {
    if kind == RunKind::Create {
        return Ok(Vec::new());
    }
    let notes = migrations.status_notes(target).await?;
    let refusing: Vec<&str> = notes
        .iter()
        .filter(|n| n.refuses_runs)
        .map(|n| n.text.as_str())
        .collect();
    if refusing.is_empty() {
        return Ok(notes);
    }
    if kind == RunKind::Migrate {
        let status = migrations.status(target).await?;
        if matches!(
            status,
            CatalogStatus::Current { .. } | CatalogStatus::AheadCompatible { .. }
        ) {
            return Ok(notes);
        }
    }
    // One note per line, as the preview prints them, so the statements
    // each note ends with can be copied on their own.
    Err(MigrationError::Refused(format!(
        "migrate --yes refuses to apply migrations while these notes hold:\n{}\nNothing was \
         changed and no lock was taken.",
        refusing
            .iter()
            .map(|n| format!("Note: {n}"))
            .collect::<Vec<_>>()
            .join("\n")
    )))
}

/// Run `kind` under `lock`, which `migrations` took for `target`. The
/// status is read under the lock first, and a status the run would refuse
/// stops it before the backend opens anything to write: `migrate --yes`
/// refuses a store that is not a catalog (`Uninitialized`, exit 1, naming
/// `extenddb init`), and `init` refuses a store that is one.
///
/// # Errors
///
/// The status refuses the run, or as [`CatalogMigrations::run`].
pub async fn run_locked(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    lock: &HeldLock,
    release: &str,
    kind: RunKind,
) -> Result<RunReport, MigrationError> {
    let status = migrations.status_under_lock(lock, target).await?;
    if let Some(refusal) = refusal(&status, kind) {
        return Err(refusal);
    }
    migrations.run(lock, release, kind).await
}

/// Delete the ledger rows of `id` under `lock`, which `migrations` took for
/// `target`, after reading the status under the lock as [`run_locked`]
/// does: a store that is not a catalog (`Uninitialized`) is refused, naming
/// `extenddb init`, before the backend opens anything to write.
///
/// # Errors
///
/// The store is not a catalog, or as [`CatalogMigrations::forget`].
pub async fn forget_locked(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    lock: &HeldLock,
    id: u32,
) -> Result<(), MigrationError> {
    let status = migrations.status_under_lock(lock, target).await?;
    if status == CatalogStatus::Uninitialized {
        return Err(MigrationError::Status(status));
    }
    migrations.forget(lock, id).await
}

/// The error a run of `kind` stops with on `status`, if any. The runner
/// checks the same statuses again under its own reads.
fn refusal(status: &CatalogStatus, kind: RunKind) -> Option<MigrationError> {
    match (kind, status) {
        (RunKind::Create, CatalogStatus::Uninitialized) => None,
        (RunKind::Create, status) => Some(MigrationError::Refused(format!(
            "the catalog is already initialized ({status}); use 'extenddb migrate' for pending \
             migrations"
        ))),
        (
            _,
            CatalogStatus::Uninitialized
            | CatalogStatus::Changed { .. }
            | CatalogStatus::AheadIncompatible { .. }
            | CatalogStatus::NeedsAttention { .. },
        )
        | (RunKind::Migrate, CatalogStatus::WrittenByOlderRelease { .. }) => {
            Some(MigrationError::Status(status.clone()))
        }
        _ => None,
    }
}

/// The installed backend's catalog migrations, or `None` when no backend is
/// installed.
#[must_use]
pub fn installed() -> Option<&'static dyn CatalogMigrations> {
    crate::backend::try_backend().map(|backend| backend.catalog_migrations)
}

/// What `extenddb version`, `verify` and the `serve` banner call the catalog
/// this binary needs: `migration <n>`, the highest id it carries.
#[must_use]
pub fn binary_catalog_label() -> String {
    format!("migration {}", MigrationId::LATEST.number())
}

#[cfg(test)]
mod tests {
    use super::{AdminRole, RunKind, Target, refusal};
    use crate::migrations::{CatalogStatus, MigrationError, exit};

    #[test]
    fn an_admin_password_is_not_printed() {
        let target = Target {
            admin: Some(AdminRole {
                user: "admin".to_owned(),
                password: Some("s3cret".to_owned()),
            }),
            ..Target::at("db")
        };
        let shown = format!("{target:?}");
        assert!(shown.contains("admin"), "{shown}");
        assert!(!shown.contains("s3cret"), "{shown}");
    }

    #[test]
    fn a_run_refuses_what_its_kind_cannot_apply() {
        let current = CatalogStatus::Current { version: 4 };
        let older = CatalogStatus::WrittenByOlderRelease {
            trace: "catalog_version is 0.0.4".to_owned(),
        };
        let attention = CatalogStatus::NeedsAttention {
            items: vec!["collision".to_owned()],
        };
        let code = |status: &CatalogStatus, kind| refusal(status, kind).map(|e| e.exit_code());
        // migrate --yes and --reconverge refuse a store that is not a catalog
        // with the status, so the message names `extenddb init` (exit 1).
        for kind in [RunKind::Migrate, RunKind::Reconverge] {
            assert_eq!(
                refusal(&CatalogStatus::Uninitialized, kind),
                Some(MigrationError::Status(CatalogStatus::Uninitialized))
            );
            assert_eq!(code(&attention, kind), Some(exit::ATTENTION));
            assert_eq!(refusal(&current, kind), None);
        }
        assert_eq!(code(&older, RunKind::Migrate), Some(exit::ATTENTION));
        assert_eq!(refusal(&older, RunKind::Reconverge), None);
        // init creates only what is not a catalog yet.
        assert_eq!(
            refusal(&CatalogStatus::Uninitialized, RunKind::Create),
            None
        );
        let again = refusal(&current, RunKind::Create).expect("refused");
        assert_eq!(again.exit_code(), exit::ERROR);
        assert!(again.to_string().contains("already initialized"), "{again}");
    }
}
