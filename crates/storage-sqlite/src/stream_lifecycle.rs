// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stream lifecycle on SQLite: creating a table's stream, closing it on
//! `UpdateTable StreamEnabled=false`, and reading the table and shard rows the
//! shared rules in `extenddb_storage::stream_lineage` group into streams.
//!
//! Shards and records live in the catalog file, so every step runs in the
//! caller's transaction under the engine write lock, and a data write cannot
//! interleave with a close.

use extenddb_core::types::{StreamSpecification, StreamViewType};
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage::{
    SHARDS_PER_STREAM, ShardRow, StreamChange, TableStreamRow, next_stream_label, shard_id,
    stored_specification, stream_change,
};

use crate::store::SqliteEngine;

type ShardTuple = (String, String, Option<String>, String, Option<String>);
/// `(table_id, table_status, stream_label, stream_specification, key_schema)`.
type TableTuple = (String, String, Option<String>, Option<String>, String);
/// `(table_name, table_id, table_status, stream_label, stream_specification)`.
type TableListTuple = (String, String, String, Option<String>, Option<String>);

fn shard_row((table_id, shard_id, parent, start, end): ShardTuple) -> ShardRow {
    ShardRow {
        table_id,
        shard_id,
        parent_shard_id: parent,
        starting_sequence_number: start,
        ending_sequence_number: end,
    }
}

fn internal(e: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(e.to_string())
}

impl SqliteEngine {
    /// Create a new stream on the table within the caller's transaction: four
    /// open shards named for a new label, and `tables.stream_label` set to that
    /// label. Returns the label.
    pub(crate) async fn init_stream_shards(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        view_type: StreamViewType,
    ) -> Result<String, StorageError> {
        let previous: Option<String> = sqlx::query_scalar(
            "SELECT stream_label FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut **tx)
        .await
        .map_err(internal)?
        .flatten();
        let label = next_stream_label(previous.as_deref());
        sqlx::query("UPDATE tables SET stream_label = ? WHERE account_id = ? AND table_name = ?")
            .bind(&label)
            .bind(account_id)
            .bind(table_name)
            .execute(&mut **tx)
            .await
            .map_err(internal)?;
        for i in 0..SHARDS_PER_STREAM {
            sqlx::query(
                "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
                 VALUES (?, ?, ?)",
            )
            .bind(shard_id(table_id, &label, view_type, i)?)
            .bind(table_id)
            .bind(format!("{:021}", 0))
            .execute(&mut **tx)
            .await
            .map_err(internal)?;
        }
        Ok(label)
    }

    /// Close every open shard of the table: its `EndingSequenceNumber` becomes
    /// the sequence number of its last record, or its starting number when it
    /// holds none (both measured 2026-10-24).
    async fn close_stream_shards(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE stream_shards SET ending_sequence_number = COALESCE(\
               (SELECT MAX(r.sequence_number) FROM stream_records r \
                WHERE r.shard_id = stream_shards.shard_id), \
               starting_sequence_number) \
             WHERE table_id = ? AND ending_sequence_number IS NULL",
        )
        .bind(table_id)
        .execute(&mut **tx)
        .await
        .map_err(internal)?;
        Ok(())
    }

    /// Apply an `UpdateTable` `StreamSpecification` within the caller's
    /// transaction.
    ///
    /// Disabling clears the stored specification, so DescribeTable omits
    /// `StreamSpecification`, keeps `stream_label` (`LatestStreamArn` keeps
    /// naming the disabled stream) and closes the stream's shards. Enabling
    /// closes any shard still open (a stream disabled before shards were
    /// closed on disable) and creates a new stream.
    pub(crate) async fn apply_stream_specification(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        requested: &StreamSpecification,
    ) -> Result<(), StorageError> {
        let current: Option<String> = sqlx::query_scalar(
            "SELECT stream_specification FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut **tx)
        .await
        .map_err(internal)?
        .flatten();
        let current: Option<StreamSpecification> = current
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(internal)?;
        let change = stream_change(table_name, current.as_ref(), requested)?;
        let stored =
            serde_json::to_string(&stored_specification(change, current.as_ref(), requested))
                .map_err(internal)?;
        sqlx::query(
            "UPDATE tables SET stream_specification = ? WHERE account_id = ? AND table_name = ?",
        )
        .bind(stored)
        .bind(account_id)
        .bind(table_name)
        .execute(&mut **tx)
        .await
        .map_err(internal)?;
        Self::close_stream_shards(tx, table_id).await?;
        if let StreamChange::Enable(view_type) = change {
            Self::init_stream_shards(tx, account_id, table_name, table_id, view_type).await?;
        }
        Ok(())
    }

    /// The table's stream columns and its key schema JSON, by name.
    pub(crate) async fn stream_table_row(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<(TableStreamRow, String)>, StorageError> {
        let row: Option<TableTuple> = sqlx::query_as(
            "SELECT table_id, table_status, stream_label, stream_specification, key_schema \
                 FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)?;
        row.map(|(table_id, table_status, stream_label, spec, key_schema)| {
            Ok((
                TableStreamRow {
                    table_name: table_name.to_owned(),
                    table_id,
                    table_status,
                    stream_label,
                    stream_specification: spec
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(internal)?,
                },
                key_schema,
            ))
        })
        .transpose()
    }

    /// Every table of the account that has had a stream.
    pub(crate) async fn stream_table_rows(
        &self,
        account_id: &str,
    ) -> Result<Vec<TableStreamRow>, StorageError> {
        let rows: Vec<TableListTuple> = sqlx::query_as(
            "SELECT table_name, table_id, table_status, stream_label, stream_specification \
             FROM tables WHERE account_id = ? AND stream_label IS NOT NULL",
        )
        .bind(account_id)
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;
        rows.into_iter()
            .map(|(table_name, table_id, table_status, stream_label, spec)| {
                Ok(TableStreamRow {
                    table_name,
                    table_id,
                    table_status,
                    stream_label,
                    stream_specification: spec
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(internal)?,
                })
            })
            .collect()
    }

    /// The shard rows of one table, or of every table of the account when
    /// `table_id` is `None`.
    pub(crate) async fn stream_shard_rows(
        &self,
        account_id: &str,
        table_id: Option<&str>,
    ) -> Result<Vec<ShardRow>, StorageError> {
        let rows: Vec<ShardTuple> = sqlx::query_as(
            "SELECT s.table_id, s.shard_id, s.parent_shard_id, s.starting_sequence_number, \
                    s.ending_sequence_number \
             FROM stream_shards s JOIN tables t ON t.table_id = s.table_id \
             WHERE t.account_id = ? AND (? IS NULL OR s.table_id = ?)",
        )
        .bind(account_id)
        .bind(table_id)
        .bind(table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;
        Ok(rows.into_iter().map(shard_row).collect())
    }
}

#[cfg(test)]
#[path = "stream_lifecycle_tests.rs"]
mod tests;
