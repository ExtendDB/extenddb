// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The PostgreSQL backend's [`CatalogMigrations`], which `init`, `migrate`
//! and `verify` call through the installed backend, and the target the
//! server factory reads before it serves.
//!
//! A command's [`Target`] gives the catalog database's connection string
//! and the values of the command's flags. The data database is the target's
//! data store (`--data-db`, or the name `init` resolved) when given, else
//! the catalog's `data_database_name` row, else the catalog database's name
//! without its `_catalog` suffix. `migrate --yes` connects as an admin role
//! only for the pgvector attempt and only when `--pg-user` is given; `init`
//! always has one, the role it created the databases with.

use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::migrations::{
    AdminRole, CatalogMigrations, CatalogStatus, HeldLock, LedgerRow, Listed, MigrationError,
    MigrationId, RunKind, RunReport, Runner, StatusNote, StoreKind, Target,
};
use sqlx::postgres::PgConnectOptions;

use super::backend::{OpenError, PostgresMigrations};
use super::connect::{missing_database, one_connection, relation_exists, sql_error};
use super::sessions::denied_with_notes;
use super::{MigrationLock, PostgresTarget, catalog_status, migrate_holding};

/// SQLSTATE `insufficient_privilege`.
const INSUFFICIENT_PRIVILEGE: &str = "42501";

/// The PostgreSQL backend's catalog migrations for the commands.
pub struct PostgresCatalogMigrations;

/// What the migration lock carries for [`CatalogMigrations::run`] and
/// [`CatalogMigrations::forget`]: the lock, the resolved databases, and the
/// admin role for the pgvector attempt.
struct Held {
    lock: MigrationLock,
    target: PostgresTarget,
    admin: Option<AdminRole>,
}

#[async_trait]
impl CatalogMigrations for PostgresCatalogMigrations {
    fn listed(&self, id: MigrationId) -> Listed {
        Listed::of(&super::migration(id))
    }

    fn backup_advice(&self, target: &Target) -> String {
        let catalog = catalog_options(target).ok();
        let name = catalog
            .as_ref()
            .and_then(PgConnectOptions::get_database)
            .unwrap_or("postgres");
        match target.data_store.as_deref() {
            Some(data) => format!(
                "back up both PostgreSQL databases, the catalog database {name} and the data \
                 database {data}"
            ),
            None => format!(
                "back up both PostgreSQL databases, the catalog database {name} and its data \
                 database"
            ),
        }
    }

    async fn status(&self, target: &Target) -> Result<CatalogStatus, MigrationError> {
        let status = async {
            match resolve(target).await? {
                Some(target) => catalog_status(&target).await,
                None => Ok(CatalogStatus::Uninitialized),
            }
        };
        explain_denied(target, status.await).await
    }

    async fn catalog_ledger(&self, target: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        let rows = async {
            let Some(target) = resolve(target).await? else {
                return Ok(Vec::new());
            };
            let migrations = match PostgresMigrations::open_for_status(&target).await {
                Ok(m) => m,
                Err(OpenError::NoCatalogDatabase) => return Ok(Vec::new()),
                Err(OpenError::Failed(e)) => return Err(e),
            };
            let rows = Runner::new(&migrations, "").catalog_ledger().await;
            migrations.close().await;
            rows
        };
        explain_denied(target, rows.await).await
    }

    async fn lock(&self, target: &Target, timeout: Duration) -> Result<HeldLock, MigrationError> {
        let catalog = catalog_options(target)?;
        let lock = MigrationLock::acquire(&catalog, timeout).await?;
        let resolved = match resolve(target).await {
            Ok(Some(resolved)) => Ok(resolved),
            Ok(None) => Err(no_catalog_database(&catalog)),
            Err(e) => Err(e),
        };
        let mut resolved = match resolved {
            Ok(resolved) => resolved,
            Err(e) => {
                lock.release().await;
                return Err(e);
            }
        };
        // `init` hands over the data database's connection string its
        // bootstrapper built; migration 1 records it with the catalog rows.
        if let Some(row) = &target.data_connection {
            resolved = resolved.with_data_connection_row(row.clone());
        }
        Ok(HeldLock::new(
            target.location.as_str(),
            Held {
                lock,
                target: resolved,
                admin: target.admin.clone(),
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
        // Beside the ledger, on every run: an operator who installs pgvector
        // after `init` gets it through the next `migrate --yes`. It reaches
        // the data database the server serves, as the runner does; when that
        // cannot be read, the catalog's server with the data database's
        // name, and the runner then reports why.
        let data = super::served_data_options(&held.target)
            .await
            .unwrap_or_else(|_| held.target.data());
        // The admin role connects to the data database's server, which the
        // row may name apart from the catalog's.
        let admin = held.admin.as_ref().map(|admin| admin_options(admin, &data));
        let vector = super::try_create_vector_extension(&data, admin.as_ref()).await;
        let mut report = match kind {
            RunKind::Create | RunKind::Migrate => {
                migrate_holding(&held.target, release, &held.lock).await?
            }
            RunKind::Reconverge => {
                let migrations = PostgresMigrations::open(&held.target, &held.lock).await?;
                let result = Runner::new(&migrations, release).reconverge().await;
                migrations.close().await;
                result?
            }
        };
        report.notes.insert(0, vector);
        Ok(report)
    }

    async fn forget(&self, lock: &HeldLock, id: u32) -> Result<(), MigrationError> {
        let held = held(lock)?;
        let migrations = PostgresMigrations::open(&held.target, &held.lock).await?;
        let result = Runner::new(&migrations, "").forget(id).await;
        migrations.close().await;
        result
    }

    async fn status_notes(&self, target: &Target) -> Result<Vec<StatusNote>, MigrationError> {
        let notes = async {
            match resolve(target).await? {
                Some(target) => super::status_notes(&target).await,
                None => Ok(Vec::new()),
            }
        };
        explain_denied(target, notes.await).await
    }
}

/// A read the application role may not make (SQLSTATE 42501) answered with
/// the ownership note of each database it reaches: the catalog database,
/// and the data database the target resolves to. When the data database's
/// name cannot be read, it is not guessed from the catalog's, since a
/// database of the guessed name may be another application's: the notes
/// say how to see the data database's note instead.
async fn explain_denied<T>(
    target: &Target,
    read: Result<T, MigrationError>,
) -> Result<T, MigrationError> {
    let error = match read {
        Err(e) if e.sqlstate() == Some(INSUFFICIENT_PRIVILEGE) => e,
        other => return other,
    };
    let Ok(catalog) = catalog_options(target) else {
        return Err(error);
    };
    let mut databases = vec![(StoreKind::Catalog, catalog)];
    let resolved = resolve(target).await;
    if let Ok(Some(resolved)) = &resolved {
        let data = super::served_data_options(resolved)
            .await
            .unwrap_or_else(|_| resolved.data());
        databases.push((StoreKind::Data, data));
    }
    let data_unnamed = resolved.is_err();
    Err(denied_with_notes(error, &databases, data_unnamed, Duration::from_secs(10)).await)
}

fn held(lock: &HeldLock) -> Result<&Held, MigrationError> {
    lock.get::<Held>().ok_or_else(|| {
        MigrationError::Refused("the migration lock was not taken by the PostgreSQL backend".into())
    })
}

fn catalog_options(target: &Target) -> Result<PgConnectOptions, MigrationError> {
    target.location.parse::<PgConnectOptions>().map_err(|e| {
        MigrationError::Refused(format!(
            "the catalog connection string is not a PostgreSQL URL: {e}"
        ))
    })
}

fn no_catalog_database(catalog: &PgConnectOptions) -> MigrationError {
    MigrationError::Store(format!(
        "the catalog database {} does not exist; create the deployment with 'extenddb init'",
        catalog.get_database().unwrap_or("postgres")
    ))
}

/// The databases `target` names, or `None` when its catalog database does
/// not exist. Reads the `data_database_name` row on a read-only connection.
///
/// # Errors
///
/// The connection string is not a PostgreSQL URL, or the catalog cannot be
/// reached or read.
pub async fn resolve(target: &Target) -> Result<Option<PostgresTarget>, MigrationError> {
    let catalog = catalog_options(target)?;
    let catalog_db = catalog.get_database().unwrap_or("postgres").to_owned();
    if let Some(data) = &target.data_store {
        // Given on the command line (`--data-db`, or the name `init`
        // resolved), so it wins over the database the catalog's connection
        // string row names, on that row's server.
        return Ok(Some(
            PostgresTarget::new(catalog, data.clone()).with_given_data_database(),
        ));
    }
    let fallback = crate::bootstrapper::derive_data_db_name(&catalog_db);
    let pool = match one_connection(catalog.clone(), true, Duration::from_secs(10)).await {
        Ok(pool) => pool,
        Err(e) if missing_database(&e) => return Ok(None),
        Err(e) => {
            return Err(MigrationError::store(
                format!("connect to the catalog database {catalog_db}"),
                e,
            ));
        }
    };
    let recorded = async {
        if !relation_exists(&pool, "settings").await? {
            return Ok(None);
        }
        sqlx::query_scalar::<_, String>(
            "SELECT value FROM settings WHERE key = 'data_database_name'",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|e| sql_error("read data_database_name", &e))
    }
    .await;
    pool.close().await;
    let data = recorded?.unwrap_or(fallback);
    Ok(Some(PostgresTarget::new(catalog, data)))
}

/// The admin role's connection on the server of `data`, the data database
/// the server serves.
fn admin_options(admin: &AdminRole, data: &PgConnectOptions) -> PgConnectOptions {
    let options = PgConnectOptions::new()
        .host(data.get_host())
        .port(data.get_port())
        .username(&admin.user);
    match &admin.password {
        Some(password) => options.password(password),
        None => options,
    }
}

#[cfg(test)]
mod tests {
    use extenddb_storage::migrations::{CatalogMigrations, Target};

    use super::PostgresCatalogMigrations;

    #[test]
    fn the_backup_advice_names_both_databases() {
        let mut target = Target::at("postgresql://u:p@localhost:5432/acme_catalog");
        let advice = PostgresCatalogMigrations.backup_advice(&target);
        assert_eq!(
            advice,
            "back up both PostgreSQL databases, the catalog database acme_catalog and its data \
             database"
        );
        target.data_store = Some("acme_items".into());
        let advice = PostgresCatalogMigrations.backup_advice(&target);
        assert!(
            advice.ends_with("and the data database acme_items"),
            "{advice}"
        );
        assert!(!advice.contains(":p@"), "{advice}");
    }
}
