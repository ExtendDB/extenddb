// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The SQLite backend's [`CatalogMigrations`], which `init`, `migrate` and
//! `verify` call through the installed backend.

use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, HeldLock, LedgerRow, Listed, MigrationError, MigrationId,
    RunKind, RunReport, Runner, Target,
};

use super::{
    MigrationLock, SqliteMigrations, catalog_status, in_memory_refusal, lock_for_migration,
};

/// The SQLite backend's catalog migrations for the commands.
pub struct SqliteCatalogMigrations;

#[async_trait]
impl CatalogMigrations for SqliteCatalogMigrations {
    fn listed(&self, id: MigrationId) -> Listed {
        Listed::of(&super::migration(id))
    }

    fn backup_advice(&self, target: &Target) -> String {
        let location = &target.location;
        format!(
            "back up the SQLite database file {location} with its -wal and -shm files \
             (migrate --yes also copies the database beside it before its first change)"
        )
    }

    /// `EXTENDDB__STORAGE__SQLITE__PATH=<the database's absolute path>`,
    /// quoted for a POSIX shell when it needs it; none in memory.
    fn location_assignment(&self, target: &Target) -> Option<String> {
        let file = crate::serve_lock::database_file(&target.location).ok()??;
        let path = std::path::absolute(&file).unwrap_or(file);
        Some(format!(
            "EXTENDDB__STORAGE__SQLITE__PATH={}",
            shell_word(&path.to_string_lossy())
        ))
    }

    /// `EXTENDDB__STORAGE__SQLITE__PATH` is set, which overrides the config
    /// file's `path`.
    fn location_from_environment(&self) -> bool {
        std::env::var_os("EXTENDDB__STORAGE__SQLITE__PATH").is_some_and(|v| !v.is_empty())
    }

    async fn status(&self, target: &Target) -> Result<CatalogStatus, MigrationError> {
        catalog_status(&target.location).await
    }

    async fn catalog_ledger(&self, target: &Target) -> Result<Vec<LedgerRow>, MigrationError> {
        let location = target.location.as_str();
        let file = crate::serve_lock::database_file(location)
            .map_err(MigrationError::Store)?
            .ok_or_else(in_memory_refusal)?;
        if !file.exists() {
            return Ok(Vec::new());
        }
        let migrations = SqliteMigrations::open_for_status(location).await?;
        let rows = Runner::new(&migrations, "").catalog_ledger().await;
        migrations.close().await;
        rows
    }

    /// The lock on an existing database file. A file that does not exist is
    /// not a catalog: `migrate` refuses it as `Uninitialized` and creates
    /// nothing, not even the lock file. `init` creates the file before it
    /// takes the lock.
    async fn lock(&self, target: &Target, timeout: Duration) -> Result<HeldLock, MigrationError> {
        let location = target.location.as_str();
        let file = crate::serve_lock::database_file(location)
            .map_err(MigrationError::Store)?
            .ok_or_else(in_memory_refusal)?;
        if !file.exists() {
            return Err(MigrationError::Status(CatalogStatus::Uninitialized));
        }
        let lock = lock_for_migration(location, timeout)
            .await?
            .ok_or_else(in_memory_refusal)?;
        Ok(HeldLock::new(location, lock))
    }

    async fn run(
        &self,
        lock: &HeldLock,
        release: &str,
        kind: RunKind,
    ) -> Result<RunReport, MigrationError> {
        let location = locked_location(lock)?;
        let migrations = SqliteMigrations::open(location).await?;
        let runner = Runner::new(&migrations, release);
        let result = match kind {
            RunKind::Create | RunKind::Migrate => runner.migrate().await,
            RunKind::Reconverge => runner.reconverge().await,
        };
        let result = migrations.discard_unused_copy(result);
        migrations.close().await;
        result
    }

    async fn forget(&self, lock: &HeldLock, id: u32) -> Result<(), MigrationError> {
        let location = locked_location(lock)?;
        let migrations = SqliteMigrations::open(location).await?;
        let result = Runner::new(&migrations, "").forget(id).await;
        migrations.close().await;
        result
    }
}

/// `word` as one word of a POSIX shell command: as it is when every
/// character is one a shell leaves alone, else in single quotes.
fn shell_word(word: &str) -> String {
    let plain = !word.is_empty()
        && word
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._-+:@%".contains(&b));
    if plain {
        word.to_owned()
    } else {
        format!("'{}'", word.replace('\'', "'\\''"))
    }
}

/// The location of a lock this backend took.
fn locked_location(lock: &HeldLock) -> Result<&str, MigrationError> {
    lock.get::<MigrationLock>()
        .map(|_| lock.location())
        .ok_or_else(|| {
            MigrationError::Refused(format!(
                "the lock on {} was not taken by the SQLite backend",
                lock.location()
            ))
        })
}

#[cfg(test)]
mod tests {
    use extenddb_storage::migrations::{CatalogMigrations, Target};

    use super::{SqliteCatalogMigrations, shell_word};

    #[test]
    fn the_assignment_names_the_absolute_path_as_one_shell_word() {
        assert_eq!(shell_word("/srv/x.sqlite"), "/srv/x.sqlite");
        assert_eq!(shell_word("/a b/it's"), "'/a b/it'\\''s'");
        let shown = SqliteCatalogMigrations
            .location_assignment(&Target::at("/srv/db/x.sqlite"))
            .expect("a file");
        assert_eq!(shown, "EXTENDDB__STORAGE__SQLITE__PATH=/srv/db/x.sqlite");
        let relative = SqliteCatalogMigrations
            .location_assignment(&Target::at("x.sqlite"))
            .expect("a file");
        let cwd = std::env::current_dir().expect("cwd");
        assert_eq!(
            relative,
            format!(
                "EXTENDDB__STORAGE__SQLITE__PATH={}",
                shell_word(&cwd.join("x.sqlite").to_string_lossy())
            )
        );
        assert_eq!(
            SqliteCatalogMigrations.location_assignment(&Target::at(":memory:")),
            None
        );
    }
}
