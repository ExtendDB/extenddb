// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The rows the runner writes with a body, in PostgreSQL's statements: the
//! ledger row and the version row.

use async_trait::async_trait;
use extenddb_storage::migrations::{LedgerRow, LedgerWrites, sql_ledger_ddl};
use sqlx::PgPool;

use super::failure::{Failure, exec};

/// The ledger's `applied_at` column in PostgreSQL.
const APPLIED_AT: &str = "TIMESTAMPTZ NOT NULL DEFAULT now()";

/// The ledger rows of one of the runner's connections, the same in both
/// databases.
pub(super) struct Rows<'a>(pub(super) &'a PgPool);

#[async_trait]
impl LedgerWrites for Rows<'_> {
    type Error = Failure;

    async fn create_ledger(&self) -> Result<(), Failure> {
        exec(self.0, &sql_ledger_ddl(APPLIED_AT)).await
    }

    async fn insert_row(&self, row: &LedgerRow) -> Result<(), Failure> {
        sqlx::query(
            "INSERT INTO extenddb_migrations (id, name, kind, checksum, outcome, notes, applied_by) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(i32::try_from(row.id).unwrap_or(i32::MAX))
        .bind(&row.name)
        .bind(row.kind.as_str())
        .bind(&row.checksum)
        .bind(row.outcome.as_str())
        .bind(&row.notes)
        .bind(&row.applied_by)
        .execute(self.0)
        .await
        .map(|_| ())
        .map_err(|e| Failure::sqlx(&format!("record migration {}", row.id), &e))
    }

    async fn write_version(&self, version: u32) -> Result<(), Failure> {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('catalog_version', $1) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(version.to_string())
        .execute(self.0)
        .await
        .map(|_| ())
        .map_err(|e| Failure::sqlx("write catalog_version", &e))
    }
}
