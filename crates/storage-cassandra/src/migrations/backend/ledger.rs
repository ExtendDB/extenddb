// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The writes that record a body in one keyspace's ledger.

use async_trait::async_trait;
use cdrs_tokio::consistency::Consistency;
use extenddb_storage::migrations::{
    LedgerRow, LedgerWrites, MigrationBackend, MigrationError, MigrationId,
};

use super::KeyspaceLedger;
use crate::migrations::connect::{send, send_within};

#[async_trait]
impl LedgerWrites for KeyspaceLedger<'_> {
    type Error = MigrationError;

    async fn create_ledger(&self) -> Result<(), MigrationError> {
        self.backend.check()?;
        send_within(
            self.backend.session(),
            &format!(
                "CREATE TABLE IF NOT EXISTS {}.extenddb_migrations (id int PRIMARY KEY, name \
                 text, kind text, checksum text, outcome text, notes text, applied_at timestamp, \
                 applied_by text)",
                self.keyspace
            ),
            cdrs_tokio::query_values!(),
            Consistency::Quorum,
            None,
            &format!("create the ledger of {}", self.keyspace),
            self.backend.ddl_timeout(),
        )
        .await?;
        self.backend.agree().await
    }

    async fn insert_row(&self, row: &LedgerRow) -> Result<(), MigrationError> {
        let id = i32::try_from(row.id)
            .map_err(|_| MigrationError::Store(format!("migration id {} is too large", row.id)))?;
        let insert = format!(
            "INSERT INTO {}.extenddb_migrations (id, name, kind, checksum, outcome, notes, \
             applied_at, applied_by) VALUES (?, ?, ?, ?, ?, ?, toTimestamp(now()), ?)",
            self.keyspace
        );
        // A later id's version goes first, on its own (see
        // `KeyspaceLedger::version`); `write_version` then skips it.
        if let Some(version) = self
            .version
            .filter(|_| row.id != MigrationId::Baseline.number())
        {
            self.backend.write_catalog_version(version).await?;
            self.wrote_version
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        self.backend.check()?;
        let version = self
            .version
            .filter(|_| row.id == MigrationId::Baseline.number());
        let Some(version) = version else {
            return send(
                self.backend.session(),
                &insert,
                cdrs_tokio::query_values!(
                    id,
                    row.name.as_str(),
                    row.kind.as_str(),
                    row.checksum.as_str(),
                    row.outcome.as_str(),
                    row.notes.as_str(),
                    row.applied_by.as_str()
                ),
                Consistency::Quorum,
                None,
                &format!("record migration {} in {}", row.id, self.keyspace),
            )
            .await
            .map(drop);
        };
        send(
            self.backend.session(),
            &format!(
                "BEGIN BATCH {insert}; INSERT INTO {}.settings (key, value) VALUES \
                 ('catalog_version', ?); APPLY BATCH",
                self.backend.stores.catalog_keyspace()
            ),
            cdrs_tokio::query_values!(
                id,
                row.name.as_str(),
                row.kind.as_str(),
                row.checksum.as_str(),
                row.outcome.as_str(),
                row.notes.as_str(),
                row.applied_by.as_str(),
                version.to_string()
            ),
            Consistency::Quorum,
            None,
            &format!(
                "record migration {} in {} with the catalog version",
                row.id, self.keyspace
            ),
        )
        .await?;
        self.wrote_version
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn write_version(&self, version: u32) -> Result<(), MigrationError> {
        if self.wrote_version.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        self.backend.write_catalog_version(version).await
    }
}
