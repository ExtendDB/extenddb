// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 9, `warm_throughput`, SQLite: a nullable `warm_throughput`
//! column on `tables` and on `indexes`.
//!
//! The column holds the `WarmThroughput` a client set on a table or a global
//! secondary index, one JSON record (see
//! `extenddb_core::types::WarmThroughputRecord`). It is stored and reported
//! only: nothing pre-warms or scales on it. A row without a value reports the
//! defaults for its billing mode.
//!
//! SQLite has no `ADD COLUMN IF NOT EXISTS`, so the body reads each table's
//! columns first and adds the column only when it is missing. A second run
//! changes nothing, which makes the migration convergent.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};

use super::SqliteStores;

/// This file, whose SHA-256 the checksum test pins.
pub(crate) const SOURCE: &str = include_str!("m0009_warm_throughput.rs");

/// Recorded as this migration's checksum. Changes only with a new migration.
const FINGERPRINT: &str = "0009_warm_throughput/sqlite/1";

/// The catalog tables that get the column.
const TABLES: [&str; 2] = ["tables", "indexes"];

pub(crate) struct WarmThroughput;

#[async_trait]
impl CodeBody<SqliteStores> for WarmThroughput {
    fn fingerprint(&self) -> &'static str {
        FINGERPRINT
    }

    /// Nothing this migration finds stops it.
    async fn detect(&self, _stores: &SqliteStores) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &SqliteStores) -> Result<Done, MigrationError> {
        for table in TABLES {
            let has_column: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?) WHERE name = 'warm_throughput')",
            )
            .bind(table)
            .fetch_one(&stores.pool)
            .await
            .map_err(|e| MigrationError::store(format!("read the columns of {table}"), e))?;
            if !has_column {
                sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN warm_throughput TEXT"))
                    .execute(&stores.pool)
                    .await
                    .map_err(|e| MigrationError::store(format!("add {table}.warm_throughput"), e))?;
            }
        }
        Ok(Done::default())
    }
}
