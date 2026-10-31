// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stream lifecycle on PostgreSQL: creating a table's stream, closing it on
//! `UpdateTable StreamEnabled=false`, and reading the table and shard rows the
//! shared rules in `extenddb_storage::stream_lineage` group into streams.
//!
//! The table row is in the catalog database and the shards and records in the
//! data database, so a stream change is two commits. `UpdateTable` changes the
//! catalog row (specification and label) in its transaction and changes the
//! shards after that transaction commits ([`PendingStream`]). A failure between
//! the two leaves the catalog's answer in force: a disabled stream whose shards
//! are still open (closed by the next change of that stream), or an enabled stream with no
//! shards, which records nothing and is replaced by a disable and an enable.

use extenddb_core::types::{StreamSpecification, StreamViewType};
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage::{
    SHARDS_PER_STREAM, ShardRow, StreamChange, TableStreamRow, enable_still_current,
    next_stream_label, open_shard_ids_of, shard_id, stored_specification, stream_change,
};
use sqlx::PgPool;

use crate::PostgresEngine;

type ShardTuple = (String, String, Option<String>, String, Option<String>);

fn internal(e: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(e.to_string())
}

fn spec_of(value: Option<serde_json::Value>) -> Result<Option<StreamSpecification>, StorageError> {
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(internal)
}

/// The shard changes of a committed `UpdateTable` stream change: close the
/// open shards of the stream the change read (`close`, the table's label at
/// that time), then, on an enable, create the new stream's shards.
#[must_use]
pub(crate) struct PendingStream {
    table_id: String,
    close: Option<String>,
    open: Option<(String, StreamViewType)>,
}

impl PendingStream {
    /// Run the shard changes once the catalog transaction has committed.
    ///
    /// The enable's shards are inserted after its catalog row is visible, so a
    /// disable that reads the row in between passes its check, commits the
    /// disabled specification and closes the shards it finds, which may be
    /// none or some of the four. The enable re-reads the table's stream fields
    /// after its inserts and, when the row no longer says enabled with this
    /// label, closes the shards it inserted. A disable committed before the
    /// re-read is seen by it; one committed after finds all four rows open
    /// and closes them itself; both closing is idempotent through the
    /// `ending_sequence_number IS NULL` guard.
    pub(crate) async fn apply(
        self,
        engine: &PostgresEngine,
        account_id: &str,
        table_name: &str,
    ) -> Result<(), StorageError> {
        let data_pool = &engine.data_pool;
        if let Some(label) = &self.close {
            PostgresEngine::close_stream_shards(data_pool, &self.table_id, label).await?;
        }
        if let Some((label, view_type)) = self.open {
            PostgresEngine::insert_stream_shards(data_pool, &self.table_id, &label, view_type)
                .await?;
            let row = engine.stream_table_row(account_id, table_name).await?;
            if !enable_still_current(row.as_ref().map(|(t, _)| t), &label) {
                PostgresEngine::close_stream_shards(data_pool, &self.table_id, &label).await?;
            }
        }
        Ok(())
    }
}

impl PostgresEngine {
    /// Create a new stream on the table: `tables.stream_label` set to a new
    /// label in the caller's catalog transaction, and four open shards named
    /// for it committed in the data database. Returns the label.
    pub(crate) async fn init_stream_shards(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        data_pool: &PgPool,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        view_type: StreamViewType,
    ) -> Result<String, StorageError> {
        let label = Self::set_new_stream_label(tx, account_id, table_name).await?;
        Self::insert_stream_shards(data_pool, table_id, &label, view_type).await?;
        Ok(label)
    }

    /// Set `tables.stream_label` to a label later than the table's previous
    /// one, in the caller's catalog transaction.
    async fn set_new_stream_label(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        account_id: &str,
        table_name: &str,
    ) -> Result<String, StorageError> {
        let previous: Option<String> = sqlx::query_scalar(
            "SELECT stream_label FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut **tx)
        .await
        .map_err(internal)?
        .flatten();
        let label = next_stream_label(previous.as_deref());
        sqlx::query(
            "UPDATE tables SET stream_label = $3 WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .bind(&label)
        .execute(&mut **tx)
        .await
        .map_err(internal)?;
        Ok(label)
    }

    /// Insert the four open shards of stream `label`. Stream shards live in
    /// the data database for atomic writes with stream records and item data;
    /// one transaction creates all four.
    async fn insert_stream_shards(
        data_pool: &PgPool,
        table_id: &str,
        label: &str,
        view_type: StreamViewType,
    ) -> Result<(), StorageError> {
        let mut data_tx = data_pool.begin().await.map_err(internal)?;
        for i in 0..SHARDS_PER_STREAM {
            sqlx::query(
                "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
                 VALUES ($1, $2, $3)",
            )
            .bind(shard_id(table_id, label, view_type, i)?)
            .bind(table_id)
            .bind(format!("{:021}", 0))
            .execute(&mut *data_tx)
            .await
            .map_err(internal)?;
        }
        data_tx.commit().await.map_err(internal)?;
        Ok(())
    }

    /// Close every open shard of the table: its `EndingSequenceNumber` becomes
    /// the sequence number of its last record, or its starting number when it
    /// holds none (both measured 2026-10-24).
    ///
    /// The service's rule, measured 2026-10-24 with 569 writes racing a
    /// disable: every write accepted before the stream reports `DISABLED`
    /// lands in the closed stream before its shard's `EndingSequenceNumber`,
    /// and a write after that records nothing. Held here by a lock in the data
    /// database: the write path reads the open shards `FOR SHARE` in its data
    /// transaction, and this close locks them `FOR UPDATE` first and takes the
    /// ending numbers in the same transaction, so a write that read the open
    /// set commits before the ending number is taken, and a write that arrives
    /// after the lock re-evaluates the closed rows and records nothing. What
    /// remains is the gap every backend has: a write acknowledged after the
    /// disable response that recorded into no stream, because its key info
    /// pre-dated the disable and it found no open shard. A `TransactWriteItems`
    /// across two tables whose disables run at the same instant can form a
    /// lock cycle through the shard locks; PostgreSQL's deadlock detector
    /// cancels one side, which the transaction path reports as a cancellation.
    pub(crate) async fn close_stream_shards(
        data_pool: &PgPool,
        table_id: &str,
        label: &str,
    ) -> Result<(), StorageError> {
        let rows: Vec<ShardTuple> = sqlx::query_as(
            "SELECT table_id, shard_id, parent_shard_id, starting_sequence_number, \
                    ending_sequence_number \
             FROM stream_shards WHERE table_id = $1",
        )
        .bind(table_id)
        .fetch_all(data_pool)
        .await
        .map_err(internal)?;
        let ids = open_shard_ids_of(table_id, label, &shard_rows(rows));
        if ids.is_empty() {
            return Ok(());
        }
        let mut data_tx = data_pool.begin().await.map_err(internal)?;
        sqlx::query(
            "SELECT shard_id FROM stream_shards \
             WHERE shard_id = ANY($1) AND ending_sequence_number IS NULL FOR UPDATE",
        )
        .bind(&ids)
        .fetch_all(&mut *data_tx)
        .await
        .map_err(internal)?;
        sqlx::query(
            "UPDATE stream_shards s SET ending_sequence_number = COALESCE(\
               (SELECT MAX(r.sequence_number) FROM stream_records r \
                WHERE r.shard_id = s.shard_id), \
               s.starting_sequence_number) \
             WHERE s.shard_id = ANY($1) AND s.ending_sequence_number IS NULL",
        )
        .bind(&ids)
        .execute(&mut *data_tx)
        .await
        .map_err(internal)?;
        data_tx.commit().await.map_err(internal)?;
        Ok(())
    }

    /// Apply an `UpdateTable` `StreamSpecification` in the caller's catalog
    /// transaction, which holds the table row `FOR UPDATE`.
    ///
    /// Disabling clears the stored specification, so DescribeTable omits
    /// `StreamSpecification`, and keeps `stream_label`, so `LatestStreamArn`
    /// keeps naming the disabled stream. Enabling stores the specification and
    /// a new label. The shard changes are returned, for the caller to apply
    /// with [`PendingStream::apply`] once the catalog has committed: a request
    /// refused after this point (an invalid index update in the same call)
    /// then leaves no shards behind.
    pub(crate) async fn apply_stream_specification(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        requested: &StreamSpecification,
    ) -> Result<PendingStream, StorageError> {
        let current: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT stream_specification FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut **tx)
        .await
        .map_err(internal)?
        .flatten();
        let current = spec_of(current)?;
        let current_label: Option<String> = sqlx::query_scalar(
            "SELECT stream_label FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut **tx)
        .await
        .map_err(internal)?
        .flatten();
        let change = stream_change(table_name, current.as_ref(), requested)?;
        let stored =
            serde_json::to_value(stored_specification(change, current.as_ref(), requested))
                .map_err(internal)?;
        sqlx::query(
            "UPDATE tables SET stream_specification = $1 \
             WHERE account_id = $2 AND table_name = $3",
        )
        .bind(stored)
        .bind(account_id)
        .bind(table_name)
        .execute(&mut **tx)
        .await
        .map_err(internal)?;
        let open = match change {
            StreamChange::Disable => None,
            StreamChange::Enable(view_type) => Some((
                Self::set_new_stream_label(tx, account_id, table_name).await?,
                view_type,
            )),
        };
        Ok(PendingStream {
            table_id: table_id.to_owned(),
            close: current_label,
            open,
        })
    }

    /// The table's stream columns and its key schema, by name.
    pub(crate) async fn stream_table_row(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<(TableStreamRow, serde_json::Value)>, StorageError> {
        type Row = (
            String,
            String,
            Option<String>,
            Option<serde_json::Value>,
            serde_json::Value,
        );
        let row: Option<Row> = sqlx::query_as(
            "SELECT table_id, table_status, stream_label, stream_specification, key_schema \
             FROM tables WHERE account_id = $1 AND table_name = $2",
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
                    stream_specification: spec_of(spec)?,
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
        type Row = (
            String,
            String,
            String,
            Option<String>,
            Option<serde_json::Value>,
        );
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT table_name, table_id, table_status, stream_label, stream_specification \
             FROM tables WHERE account_id = $1 AND stream_label IS NOT NULL",
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
                    stream_specification: spec_of(spec)?,
                })
            })
            .collect()
    }

    /// The shard rows of the given tables.
    pub(crate) async fn stream_shard_rows(
        &self,
        table_ids: &[String],
    ) -> Result<Vec<ShardRow>, StorageError> {
        if table_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<ShardTuple> = sqlx::query_as(
            "SELECT table_id, shard_id, parent_shard_id, starting_sequence_number, \
                    ending_sequence_number \
             FROM stream_shards WHERE table_id = ANY($1)",
        )
        .bind(table_ids)
        .fetch_all(&self.data_pool)
        .await
        .map_err(internal)?;
        Ok(shard_rows(rows))
    }
}

fn shard_rows(rows: Vec<ShardTuple>) -> Vec<ShardRow> {
    rows.into_iter()
        .map(|(table_id, shard_id, parent, start, end)| ShardRow {
            table_id,
            shard_id,
            parent_shard_id: parent,
            starting_sequence_number: start,
            ending_sequence_number: end,
        })
        .collect()
}
