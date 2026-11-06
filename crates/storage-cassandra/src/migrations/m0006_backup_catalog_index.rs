// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 6, `backup_catalog_index`, Cassandra: the catalog index of
//! backups kept in a backup store (columns of `backups_by_arn` and the
//! `backup_index_by_status` table), the restore job record on the restore
//! registrations (`tables.restore_*`), the maintenance claim record
//! (`backup_maintenance`) and the catalog id (`settings.catalog_id`).
//!
//! The body executes `0006_backup_catalog_index.cql` as the runner executes
//! a file body, then backfills the registrations that exist and writes the
//! catalog id: CQL has no `UPDATE ... WHERE` on a non-key predicate, and the
//! id is a random value. Every statement of the file is `IF NOT EXISTS`, each
//! backfill write is a lightweight transaction conditioned on the job status
//! being unset, and the id is inserted `IF NOT EXISTS`, so a second run
//! changes nothing; a run stopped part way runs again from the start.
//!
//! The backfill: every registration that exists was written by a release
//! without the job record, so each is a legacy restore. One whose table is
//! not CREATING finished (DONE, completed when the table was created). One
//! whose table is CREATING is a restore the upgrade's stop interrupted:
//! RUNNING, with a heartbeat at the epoch (not null, so the staleness test
//! selects it) and an owner generation no job holds (0), so the first
//! reconciler pass fails it and removes its target.
//!
//! The checksum test pins this file together with the CQL file (`SOURCE`)
//! once the migration is released. The module is excluded from rustfmt and
//! clippy so neither can require an edit.

use async_trait::async_trait;
use cdrs_tokio::consistency::Consistency;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};

use super::CassandraStores;
use super::connect::{column, nullable, rows, send};

/// The file this body executes first.
pub(crate) const FILE_NAME: &str = "0006_backup_catalog_index.cql";

/// The text of [`FILE_NAME`].
pub(crate) const FILE: &str =
    extenddb_storage::file_body!("migrations/catalog", "0006_backup_catalog_index.cql");

/// This module and the file it executes, whose SHA-256 the checksum test
/// pins.
pub(crate) const SOURCE: &str = concat!(
    include_str!("m0006_backup_catalog_index.rs"),
    extenddb_storage::file_body!("migrations/catalog", "0006_backup_catalog_index.cql")
);

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0006_backup_catalog_index/cassandra/1";

/// Migration 6's catalog body.
pub(crate) struct BackupCatalogIndex;

#[async_trait]
impl CodeBody<CassandraStores> for BackupCatalogIndex {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &CassandraStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &CassandraStores) -> Result<Done, MigrationError> {
        let catalog = stores.catalog_keyspace();
        stores.run_file(&catalog, FILE_NAME, FILE).await?;
        let mut notes = Vec::new();
        let (done, running) = backfill(stores, &catalog).await?;
        if done + running > 0 {
            notes.push(format!(
                "backfilled {} restore registration(s) as legacy restores, {running} of them \
                 RUNNING with a dead owner for the reconciler to remove",
                done + running
            ));
        }
        if write_catalog_id(stores, &catalog).await? {
            notes.push("wrote settings.catalog_id".to_owned());
        }
        Ok(Done { notes })
    }
}

/// Give every `tables` row that carries a restore registration without a job
/// status its job record; returns how many became DONE and how many RUNNING.
/// A row whose write does not apply (its status was set, or its table was
/// replaced, since the read) is left as it is.
async fn backfill(stores: &CassandraStores, catalog: &str) -> Result<(u32, u32), MigrationError> {
    const CONTEXT: &str = "backfill the restore registrations";
    let found = rows(
        &stores.session,
        &format!(
            "SELECT account_id, table_name, table_id, table_status, created_at, \
             restore_source_backup_arn, restore_status FROM {catalog}.tables"
        ),
        cdrs_tokio::query_values!(),
        Consistency::Quorum,
        CONTEXT,
    )
    .await?;
    stores.progress();
    let (mut done, mut running) = (0_u32, 0_u32);
    for row in found {
        let source: Option<String> = nullable(&row, "restore_source_backup_arn", CONTEXT)?;
        let status: Option<String> = nullable(&row, "restore_status", CONTEXT)?;
        if source.is_none() || status.is_some() {
            continue;
        }
        let account_id: String = column(&row, "account_id", CONTEXT)?;
        let table_name: String = column(&row, "table_name", CONTEXT)?;
        let table_id: String = column(&row, "table_id", CONTEXT)?;
        let table_status: Option<String> = nullable(&row, "table_status", CONTEXT)?;
        let created_at: i64 = nullable(&row, "created_at", CONTEXT)?.unwrap_or(0);
        let interrupted = table_status.as_deref() == Some("CREATING");
        let (update, values) = if interrupted {
            (
                format!(
                    "UPDATE {catalog}.tables SET restore_status = 'RUNNING', \
                     restore_kind = 'legacy', restore_owner_generation = 0, \
                     restore_heartbeat_at = 0 WHERE account_id = ? AND table_name = ? \
                     IF table_id = ? AND restore_status = null"
                ),
                cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str(), table_id.as_str()),
            )
        } else {
            (
                format!(
                    "UPDATE {catalog}.tables SET restore_status = 'DONE', \
                     restore_kind = 'legacy', restore_owner_generation = 0, \
                     restore_heartbeat_at = 0, restore_completed_at = ? \
                     WHERE account_id = ? AND table_name = ? \
                     IF table_id = ? AND restore_status = null"
                ),
                cdrs_tokio::query_values!(
                    created_at,
                    account_id.as_str(),
                    table_name.as_str(),
                    table_id.as_str()
                ),
            )
        };
        stores.check()?;
        if applied(stores, &update, values, CONTEXT).await? {
            if interrupted {
                running += 1;
            } else {
                done += 1;
            }
        }
    }
    Ok((done, running))
}

/// Insert `settings.catalog_id`, a random id, unless the row exists; returns
/// whether this run wrote it.
async fn write_catalog_id(stores: &CassandraStores, catalog: &str) -> Result<bool, MigrationError> {
    let id = uuid::Uuid::new_v4().to_string();
    stores.check()?;
    applied(
        stores,
        &format!(
            "INSERT INTO {catalog}.settings (key, value) VALUES ('catalog_id', ?) IF NOT EXISTS"
        ),
        cdrs_tokio::query_values!(id.as_str()),
        "write settings.catalog_id",
    )
    .await
}

/// Send a lightweight transaction at `QUORUM` and `SERIAL` and return its
/// `[applied]` column.
async fn applied(
    stores: &CassandraStores,
    cql: &str,
    values: cdrs_tokio::query::QueryValues,
    context: &str,
) -> Result<bool, MigrationError> {
    let envelope = send(
        &stores.session,
        cql,
        values,
        Consistency::Quorum,
        Some(Consistency::Serial),
        context,
    )
    .await?;
    stores.progress();
    let found = envelope
        .response_body()
        .map_err(|e| MigrationError::store(context, e))?
        .into_rows()
        .ok_or_else(|| MigrationError::Store(format!("{context}: the answer holds no rows")))?;
    let row = found
        .first()
        .ok_or_else(|| MigrationError::Store(format!("{context}: no [applied] row")))?;
    column(row, "[applied]", context)
}
