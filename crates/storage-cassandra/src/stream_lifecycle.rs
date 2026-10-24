// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stream lifecycle on Cassandra: closing a table's stream on
//! `UpdateTable StreamEnabled=false`, creating the next one, and reading the
//! table and shard rows the shared rules in `extenddb_storage::stream_lineage`
//! group into streams.
//!
//! The write paths compute a record's shard from the table id and the
//! partition key alone (`stream_util::assign_shard_id`), inside the LOGGED
//! BATCH of the data write, with no read of the shard rows. So records are
//! stored in one partition per shard index, `shardId-{table_id}-{index:012}`,
//! whatever stream is current, and a stream's shard is a range of that
//! partition: the records after its `StartingSequenceNumber`, up to its
//! `EndingSequenceNumber` once closed. A new stream's shard starts at a clock
//! value drawn when the stream is enabled, above every sequence issued before,
//! so a record belongs to at most one stream. The first stream of a table
//! starts at zero.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{StreamSpecification, StreamViewType};
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage::{
    SHARDS_PER_STREAM, ShardRow, StreamChange, TableStreamRow, enable_still_current,
    next_stream_label, open_shard_ids_of, parse_shard_id, shard_id, stored_specification,
    stream_change,
};

use crate::CassandraEngine;
use crate::cassandra_util::query_rows;
use crate::stream_util::ZERO_SEQUENCE;

fn internal(e: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(e.to_string())
}

/// The partition holding the records of `shard_id`: the table's partition for
/// the shard's index. A shard id this table did not issue maps to itself.
#[must_use]
pub(crate) fn physical_shard_id(table_id: &str, shard_id: &str) -> String {
    parse_shard_id(table_id, shard_id).map_or_else(
        || shard_id.to_owned(),
        |(_, index)| format!("shardId-{table_id}-{index:012}"),
    )
}

/// The table id a shard id names (`shardId-{table_id}-{digits}`).
pub(crate) fn shard_table_id(shard_id: &str) -> Option<&str> {
    shard_id
        .strip_prefix("shardId-")?
        .rsplit_once('-')
        .map(|(table_id, _)| table_id)
}

/// The record range of one shard, for reading its partition.
pub(crate) struct ShardRange {
    /// The partition its records are in.
    pub physical: String,
    /// Records after this sequence number belong to the shard.
    pub after: String,
    /// Records up to this one, once the shard is closed.
    pub until: Option<String>,
}

/// The `tables` columns an `UpdateTable` stream change sets, as text.
pub(crate) type StreamColumns = Vec<(&'static str, String)>;

/// The shard changes of an applied `UpdateTable` stream change: close the
/// open shards of the stream the change read (`close`, the table's label at
/// that time), then, on an enable, create the new stream's shards.
#[must_use]
pub(crate) struct PendingStream {
    /// The specification JSON and label the change was decided on; the
    /// catalog write is conditioned on both still being these values.
    pub read_specification: Option<String>,
    pub read_label: Option<String>,
    pub close: Option<String>,
    pub open: Option<(String, StreamViewType)>,
}

impl PendingStream {
    /// Run the shard changes once the catalog row is written.
    ///
    /// The enable's shards are inserted after its catalog row is visible, so a
    /// disable that reads the row in between passes its condition, writes the
    /// disabled specification and closes the shards it finds, which may be
    /// none or some of the four. The enable re-reads the table's stream fields
    /// after its inserts and, when the row no longer says enabled with this
    /// label, closes the shards it inserted. A disable written before the
    /// re-read is seen by it; one written after finds all four rows open and
    /// closes them itself; the two closes carry the same ending number.
    pub(crate) async fn apply(
        self,
        engine: &CassandraEngine,
        account_id: &str,
        account_ks: &str,
        table_name: &str,
        table_id: &str,
    ) -> Result<(), StorageError> {
        if let Some(label) = &self.close {
            engine
                .close_stream_shards(account_ks, table_id, label)
                .await?;
        }
        let Some((label, view_type)) = self.open else {
            return Ok(());
        };
        // Each new shard starts at a sequence number drawn now from the same
        // clock the writers draw from, so it is above every sequence issued so
        // far. A write of the closed stream that commits after the close (its
        // LOGGED BATCH drew its sequence before the ending number was taken)
        // then falls between the old shard's ending number and the new
        // shard's starting number: in neither stream, rather than in the new
        // one under the old view type.
        let start = engine
            .hlc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generate();
        let starts: Vec<String> = vec![start; SHARDS_PER_STREAM as usize];
        engine
            .insert_stream_shards(account_ks, table_id, &label, view_type, &starts)
            .await?;
        let row = engine.stream_table_row(account_id, table_name).await?;
        if !enable_still_current(row.as_ref().map(|(t, _)| t), &label) {
            engine
                .close_stream_shards(account_ks, table_id, &label)
                .await?;
        }
        Ok(())
    }
}

impl PendingStream {
    /// Decide an `UpdateTable` `StreamSpecification` against the table row
    /// fetched by `update_table_impl`: the catalog columns to set (as text)
    /// and the shard changes to run once the row is written.
    pub(crate) fn decide(
        row: &cdrs_tokio::types::rows::Row,
        table_name: &str,
        requested: &StreamSpecification,
    ) -> Result<(StreamColumns, Self), StorageError> {
        let read_specification: Option<String> =
            row.get_by_name("stream_specification").ok().flatten();
        let current: Option<StreamSpecification> = read_specification
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(internal)?;
        let read_label: Option<String> = row.get_by_name("stream_label").ok().flatten();
        let change = stream_change(table_name, current.as_ref(), requested)?;
        let mut cols = vec![(
            "stream_specification",
            serde_json::to_string(&stored_specification(change, current.as_ref(), requested))
                .map_err(internal)?,
        )];
        let open = match change {
            StreamChange::Disable => None,
            StreamChange::Enable(view_type) => {
                let label = next_stream_label(read_label.as_deref());
                cols.push(("stream_label", label.clone()));
                Some((label, view_type))
            }
        };
        Ok((
            cols,
            Self {
                read_specification,
                close: read_label.clone(),
                read_label,
                open,
            },
        ))
    }
}

impl CassandraEngine {
    /// The refusal for a stream change whose conditional catalog write found
    /// the stream fields changed: re-read the row and decide the request
    /// against it, which refuses the loser of two overlapping changes with the
    /// ordinary sentence (`Table already has an enabled stream` or `Table has
    /// no stream to disable`). When the re-read state would accept the request
    /// (a disable overlapped an enable), the table is reported as not ACTIVE,
    /// the nearest of ExtendDB's answers to the service's in-progress refusal.
    pub(crate) async fn refuse_overlapping_stream_change(
        &self,
        account_id: &str,
        table_name: &str,
        input: &extenddb_core::types::UpdateTableInput,
    ) -> Result<(), StorageError> {
        let Some(spec) = &input.stream_specification else {
            return Ok(());
        };
        let catalog_ks = self.catalog_keyspace();
        let row = query_rows::<StorageError>(
            &self.session,
            &format!(
                "SELECT stream_specification FROM {catalog_ks}.tables \
                 WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id, table_name),
            "update_table stream re-read",
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        let current: Option<String> = row.get_by_name("stream_specification").ok().flatten();
        let current: Option<StreamSpecification> = current
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(internal)?;
        stream_change(table_name, current.as_ref(), spec)?;
        Err(StorageError::TableNotActive(table_name.to_owned()))
    }

    /// Insert the four open shards of stream `label`, shard `i` starting at
    /// `starts[i]`. The caller stores the label on the table row.
    pub(crate) async fn insert_stream_shards(
        &self,
        account_ks: &str,
        table_id: &str,
        label: &str,
        view_type: StreamViewType,
        starts: &[String],
    ) -> Result<(), StorageError> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut statements = Vec::new();
        for (i, start) in (0..SHARDS_PER_STREAM).zip(starts) {
            // Interpolated, not bound: a LOGGED BATCH here carries plain
            // strings. Every value is server-generated (UUID table id, label
            // digits, sequence digits).
            statements.push(format!(
                "INSERT INTO {account_ks}.stream_shards \
                 (shard_id, table_id, starting_sequence_number, created_at) \
                 VALUES ('{}', '{table_id}', '{start}', {now_ms})",
                shard_id(table_id, label, view_type, i)?
            ));
        }
        let batch = format!("BEGIN BATCH\n{}\nAPPLY BATCH", statements.join(";\n"));
        self.session.query(&batch).await.map_err(|e| {
            tracing::error!("insert_stream_shards batch: {e}");
            StorageError::Internal(format!("Failed to initialize stream shards: {e}"))
        })?;
        Ok(())
    }

    /// Close every open shard of the table: its `EndingSequenceNumber` becomes
    /// the sequence number of its last record, or its starting number when it
    /// holds none (both measured 2026-10-24).
    pub(crate) async fn close_stream_shards(
        &self,
        account_ks: &str,
        table_id: &str,
        label: &str,
    ) -> Result<(), StorageError> {
        let rows = self.stream_shard_rows(account_ks, Some(table_id)).await?;
        let ids = open_shard_ids_of(table_id, label, &rows);
        for row in rows {
            if !ids.contains(&row.shard_id) {
                continue;
            }
            let range = ShardRange {
                physical: physical_shard_id(table_id, &row.shard_id),
                after: row.starting_sequence_number.clone(),
                until: None,
            };
            let ending = self
                .last_sequence_in(account_ks, &range)
                .await?
                .unwrap_or(row.starting_sequence_number);
            crate::cassandra_util::execute::<StorageError>(
                &self.session,
                &format!(
                    "UPDATE {account_ks}.stream_shards SET ending_sequence_number = ? \
                     WHERE shard_id = ?"
                ),
                cdrs_tokio::query_values!(ending.as_str(), row.shard_id.as_str()),
                "close_stream_shards",
            )
            .await?;
        }
        Ok(())
    }

    /// The sequence number of the last record in a shard's range.
    pub(crate) async fn last_sequence_in(
        &self,
        account_ks: &str,
        range: &ShardRange,
    ) -> Result<Option<String>, StorageError> {
        let rows = match &range.until {
            Some(until) => {
                query_rows::<StorageError>(
                    &self.session,
                    &format!(
                        "SELECT sequence_number FROM {account_ks}.stream_records \
                         WHERE shard_id = ? AND sequence_number > ? AND sequence_number <= ? \
                         ORDER BY sequence_number DESC LIMIT 1"
                    ),
                    cdrs_tokio::query_values!(
                        range.physical.as_str(),
                        range.after.as_str(),
                        until.as_str()
                    ),
                    "last_sequence_in",
                )
                .await?
            }
            None => {
                query_rows::<StorageError>(
                    &self.session,
                    &format!(
                        "SELECT sequence_number FROM {account_ks}.stream_records \
                         WHERE shard_id = ? AND sequence_number > ? \
                         ORDER BY sequence_number DESC LIMIT 1"
                    ),
                    cdrs_tokio::query_values!(range.physical.as_str(), range.after.as_str()),
                    "last_sequence_in",
                )
                .await?
            }
        };
        rows.into_iter()
            .next()
            .map(|r| r.get_r_by_name("sequence_number").map_err(internal))
            .transpose()
    }

    /// The record range of a shard, from its row.
    pub(crate) async fn shard_range(
        &self,
        account_ks: &str,
        shard_id: &str,
    ) -> Result<ShardRange, StorageError> {
        let table_id = shard_table_id(shard_id).unwrap_or_default();
        let row = query_rows::<StorageError>(
            &self.session,
            &format!(
                "SELECT starting_sequence_number, ending_sequence_number \
                 FROM {account_ks}.stream_shards WHERE shard_id = ?"
            ),
            cdrs_tokio::query_values!(shard_id),
            "shard_range",
        )
        .await?
        .into_iter()
        .next();
        let (after, until) = match row {
            Some(r) => (
                {
                    let start: Option<String> =
                        r.get_by_name("starting_sequence_number").ok().flatten();
                    start.unwrap_or_else(|| ZERO_SEQUENCE.to_owned())
                },
                r.get_by_name("ending_sequence_number").ok().flatten(),
            ),
            None => (ZERO_SEQUENCE.to_owned(), None),
        };
        Ok(ShardRange {
            physical: physical_shard_id(table_id, shard_id),
            after,
            until,
        })
    }

    fn table_stream_row(
        table_name: String,
        row: &cdrs_tokio::types::rows::Row,
    ) -> Result<TableStreamRow, StorageError> {
        let spec: Option<String> = row.get_by_name("stream_specification").ok().flatten();
        Ok(TableStreamRow {
            table_name,
            table_id: row.get_r_by_name("table_id").map_err(internal)?,
            table_status: row.get_r_by_name("table_status").map_err(internal)?,
            stream_label: row.get_by_name("stream_label").ok().flatten(),
            stream_specification: spec
                .as_deref()
                .map(serde_json::from_str::<StreamSpecification>)
                .transpose()
                .map_err(internal)?,
        })
    }

    /// The table's stream columns and its key schema JSON, by name.
    pub(crate) async fn stream_table_row(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<(TableStreamRow, String)>, StorageError> {
        let catalog_ks = self.catalog_keyspace();
        let row = query_rows::<StorageError>(
            &self.session,
            &format!(
                "SELECT table_id, table_status, stream_label, stream_specification, key_schema \
                 FROM {catalog_ks}.tables WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id, table_name),
            "stream_table_row",
        )
        .await?
        .into_iter()
        .next();
        row.map(|r| {
            Ok((
                Self::table_stream_row(table_name.to_owned(), &r)?,
                r.get_r_by_name("key_schema").map_err(internal)?,
            ))
        })
        .transpose()
    }

    /// Every table of the account that has had a stream.
    pub(crate) async fn stream_table_rows(
        &self,
        account_id: &str,
    ) -> Result<Vec<TableStreamRow>, StorageError> {
        let catalog_ks = self.catalog_keyspace();
        let rows = query_rows::<StorageError>(
            &self.session,
            &format!(
                "SELECT table_name, table_id, table_status, stream_label, stream_specification \
                 FROM {catalog_ks}.tables WHERE account_id = ? ALLOW FILTERING"
            ),
            cdrs_tokio::query_values!(account_id),
            "stream_table_rows",
        )
        .await?;
        let mut tables = Vec::new();
        for r in rows {
            let label: Option<String> = r.get_by_name("stream_label").ok().flatten();
            if label.is_none() {
                continue;
            }
            let name: String = r.get_r_by_name("table_name").map_err(internal)?;
            tables.push(Self::table_stream_row(name, &r)?);
        }
        Ok(tables)
    }

    /// The shard rows of one table, or of every table in the account's
    /// keyspace when `table_id` is `None`.
    pub(crate) async fn stream_shard_rows(
        &self,
        account_ks: &str,
        table_id: Option<&str>,
    ) -> Result<Vec<ShardRow>, StorageError> {
        let columns = "table_id, shard_id, parent_shard_id, starting_sequence_number, \
                       ending_sequence_number";
        let rows = match table_id {
            Some(id) => {
                query_rows::<StorageError>(
                    &self.session,
                    &format!("SELECT {columns} FROM {account_ks}.stream_shards WHERE table_id = ?"),
                    cdrs_tokio::query_values!(id),
                    "stream_shard_rows",
                )
                .await?
            }
            None => {
                query_rows::<StorageError>(
                    &self.session,
                    &format!("SELECT {columns} FROM {account_ks}.stream_shards"),
                    cdrs_tokio::query::QueryValues::SimpleValues(Vec::new()),
                    "stream_shard_rows",
                )
                .await?
            }
        };
        rows.into_iter()
            .map(|r| {
                Ok(ShardRow {
                    table_id: r.get_r_by_name("table_id").map_err(internal)?,
                    shard_id: r.get_r_by_name("shard_id").map_err(internal)?,
                    parent_shard_id: r.get_by_name("parent_shard_id").ok().flatten(),
                    starting_sequence_number: r
                        .get_by_name("starting_sequence_number")
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| ZERO_SEQUENCE.to_owned()),
                    ending_sequence_number: r.get_by_name("ending_sequence_number").ok().flatten(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn every_stream_of_a_table_reads_the_partition_of_its_shard_index() {
        let label = "2026-10-24T08:54:32.805";
        for i in 0..SHARDS_PER_STREAM {
            let labelled = shard_id(TID, label, StreamViewType::NewImage, i).unwrap();
            let legacy = format!("shardId-{TID}-{i:012}");
            assert_eq!(physical_shard_id(TID, &labelled), legacy);
            assert_eq!(physical_shard_id(TID, &legacy), legacy);
            assert_eq!(shard_table_id(&labelled), Some(TID));
        }
        assert_eq!(physical_shard_id(TID, "shardId-other-1"), "shardId-other-1");
    }
}
