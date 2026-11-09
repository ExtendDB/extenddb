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
//! `EndingSequenceNumber` once closed. A new stream's shards start one above
//! the larger of this server's next clock value and the last ending number
//! among the table's shards, drawn when the stream is enabled, so a record
//! belongs to at most one stream. The first stream of a table starts at zero.
//!
//! With several servers, the sequence a write draws is its own server's clock
//! (`stream_util::HybridClock`), while the start is taken on the server that
//! runs the enable. Taking the closed shards' ending numbers into the start
//! keeps a record of the old stream, written by a server whose clock is ahead,
//! from reading in both streams. A server whose clock is behind the start can
//! still write a new-stream record below it, which no stream reads; that case
//! stays with the deferred cross-instance consistency item in the design
//! document.
//!
//! `UpdateTable` writes the catalog row first and changes the shards after it
//! ([`PendingStream`]), as on PostgreSQL and MongoDB. A disable whose close
//! failed leaves a disabled stream whose shards are still open: a retried
//! disable is refused and closes them, and an enable closes them too, since an
//! enable closes the open shards of every earlier stream of the table. An
//! enable whose shard insert failed leaves an enabled stream with no shards,
//! which no record belongs to, replaced by a disable and an enable.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{StreamSpecification, StreamViewType};
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage::{
    SHARDS_PER_STREAM, ShardRow, StreamChange, disabled_stream_to_close, enable_still_current,
    next_stream_label, open_shard_ids_before, open_shard_ids_of, parse_shard_id, shard_id,
    stored_specification, stream_change,
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

/// The sequence number one above `sequence`, in its 23-digit shape.
fn next_sequence(sequence: &str) -> Result<String, StorageError> {
    let n: u128 = sequence
        .parse()
        .map_err(|_| StorageError::Internal(format!("malformed sequence number {sequence}")))?;
    Ok(format!("{:0width$}", n + 1, width = sequence.len()))
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

/// The record range of the shard whose `stream_shards` row is `row` (with its
/// `starting_sequence_number` and `ending_sequence_number` columns), in the
/// partition `physical`.
pub(crate) fn shard_range_of(physical: String, row: &cdrs_tokio::types::rows::Row) -> ShardRange {
    let start: Option<String> = row.get_by_name("starting_sequence_number").ok().flatten();
    ShardRange {
        physical,
        after: start.unwrap_or_else(|| ZERO_SEQUENCE.to_owned()),
        until: row.get_by_name("ending_sequence_number").ok().flatten(),
    }
}

/// The `tables` columns an `UpdateTable` stream change sets, as text.
pub(crate) type StreamColumns = Vec<(&'static str, String)>;

/// The shard changes of an applied `UpdateTable` stream change: on a disable,
/// close the open shards of the stream the change read (`close`, the table's
/// label at that time); on an enable, close the open shards of every earlier
/// stream, then create the new stream's shards.
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
    /// closes them itself. A shard keeps the ending number of the first close
    /// that writes one (`close_stream_shards`), so a disable whose close runs
    /// after an overlapping enable started the next stream cannot move the
    /// closed shard's ending number up into the new stream's records.
    pub(crate) async fn apply(
        self,
        engine: &CassandraEngine,
        account_id: &str,
        account_ks: &str,
        table_name: &str,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let Some((label, view_type)) = self.open else {
            if let Some(label) = &self.close {
                engine
                    .close_stream_shards(account_ks, table_id, label)
                    .await?;
            }
            return Ok(());
        };
        engine
            .close_streams_before(account_ks, table_id, &label)
            .await?;
        // Each new shard starts one above the larger of a sequence number
        // drawn now from the clock this server's writers draw from and the
        // last ending number among the table's shards (the ones just closed
        // included), so it is above every sequence issued so far on this
        // server and every record the closed streams hold, whichever server
        // wrote it. A write of the closed stream that commits after the close
        // (its LOGGED BATCH drew its sequence before the ending number was
        // taken) then falls between the old shard's ending number and the new
        // shard's starting number: in neither stream, rather than in the new
        // one under the old view type.
        let local = engine
            .hlc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generate();
        let last_end = engine
            .stream_shard_rows(account_ks, Some(table_id))
            .await?
            .into_iter()
            .filter_map(|r| r.ending_sequence_number)
            .max();
        let start = next_sequence(&last_end.map_or(local.clone(), |end| end.max(local)))?;
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
    /// [`PendingStream::decide`], and for a disable refused because the stored
    /// specification is already disabled, the close of the current label's
    /// open shards first (left by a release that did not close them, or by a
    /// close that failed after the catalog write); the refusal is still the
    /// answer.
    pub(crate) async fn decide_stream_change_or_close(
        &self,
        row: &cdrs_tokio::types::rows::Row,
        account_ks: &str,
        table_id: &str,
        table_name: &str,
        requested: &StreamSpecification,
    ) -> Result<(StreamColumns, PendingStream), StorageError> {
        match PendingStream::decide(row, table_name, requested) {
            Ok(decided) => Ok(decided),
            Err(refusal) => {
                let stored: Option<String> = row.get_by_name("stream_specification").ok().flatten();
                let current: Option<StreamSpecification> = stored
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(internal)?;
                let label: Option<String> = row.get_by_name("stream_label").ok().flatten();
                if let Some(label) =
                    disabled_stream_to_close(current.as_ref(), label.as_deref(), requested)
                {
                    self.close_stream_shards(account_ks, table_id, label)
                        .await?;
                }
                Err(refusal)
            }
        }
    }

    /// The table id, stream specification JSON and stream label of the
    /// table's row, as the conditional catalog write of a stream change
    /// compares them, read at `LOCAL_SERIAL` so that a write another
    /// lightweight transaction committed is seen; `None` when the row is gone.
    pub(crate) async fn stream_condition_fields(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<(Option<String>, Option<String>, Option<String>)>, StorageError> {
        let catalog_ks = self.catalog_keyspace();
        let row = crate::cassandra_util::query_rows_serial::<StorageError>(
            &self.session_arc(),
            &format!(
                "SELECT table_id, stream_specification, stream_label FROM {catalog_ks}.tables \
                 WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id, table_name),
            "update_table stream condition re-read",
        )
        .await?
        .into_iter()
        .next();
        Ok(row.map(|r| {
            (
                r.get_by_name("table_id").ok().flatten(),
                r.get_by_name("stream_specification").ok().flatten(),
                r.get_by_name("stream_label").ok().flatten(),
            )
        }))
    }

    /// The refusal for a stream change whose conditional catalog write found
    /// the stream fields changed: re-read the row and decide the request
    /// against it, which refuses the loser of two overlapping changes with the
    /// ordinary sentence (`Table already has an enabled stream` or `Table has
    /// no stream to disable`). When the re-read state would accept the request
    /// (a disable overlapped an enable), the table is reported as not ACTIVE,
    /// the nearest of ExtendDB's answers to the service's in-progress refusal.
    /// A row that is gone, or that holds a table created again under the same
    /// name (another `table_id` than `table_id`, the one the request read), is
    /// the table the request read no longer existing: `TableNotFound`.
    pub(crate) async fn refuse_overlapping_stream_change(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        input: &extenddb_core::types::UpdateTableInput,
    ) -> Result<(), StorageError> {
        let Some(spec) = &input.stream_specification else {
            return Ok(());
        };
        let (_, current, _) = self
            .stream_condition_fields(account_id, table_name)
            .await?
            .filter(|(id, _, _)| id.as_deref() == Some(table_id))
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
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
    ///
    /// A shard's range has no upper bound while it is open, so the ending
    /// number is written only if the shard has none yet: a close that read the
    /// shard as open and takes its ending after another close has written one
    /// (a disable applying after an overlapping enable closed the stream and
    /// started the next) would otherwise take a record of the next stream from
    /// the shared partition.
    pub(crate) async fn close_stream_shards(
        &self,
        account_ks: &str,
        table_id: &str,
        label: &str,
    ) -> Result<(), StorageError> {
        self.close_selected_shards(account_ks, table_id, label, open_shard_ids_of)
            .await
    }

    /// Close the open shards of every stream of the table older than `label`
    /// (`stream_lineage::open_shard_ids_before`), as [`Self::close_stream_shards`]
    /// closes one stream's.
    async fn close_streams_before(
        &self,
        account_ks: &str,
        table_id: &str,
        label: &str,
    ) -> Result<(), StorageError> {
        self.close_selected_shards(account_ks, table_id, label, open_shard_ids_before)
            .await
    }

    async fn close_selected_shards(
        &self,
        account_ks: &str,
        table_id: &str,
        label: &str,
        select: fn(&str, &str, &[ShardRow]) -> Vec<String>,
    ) -> Result<(), StorageError> {
        let rows = self.stream_shard_rows(account_ks, Some(table_id)).await?;
        let ids = select(table_id, label, &rows);
        let open = rows
            .into_iter()
            .filter(|r| ids.contains(&r.shard_id))
            .collect();
        self.close_shards_as_read(account_ks, table_id, open).await
    }

    /// Write the ending numbers of `rows`, shard rows of table `table_id` this
    /// close read as open: each one's last record after its start, or its
    /// start when it holds none, unless the shard already has an ending
    /// number. Public for the direct tests, which run a close that read its
    /// rows before another close wrote them.
    #[doc(hidden)]
    pub async fn close_shards_as_read(
        &self,
        account_ks: &str,
        table_id: &str,
        rows: Vec<ShardRow>,
    ) -> Result<(), StorageError> {
        for row in rows {
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
                     WHERE shard_id = ? IF ending_sequence_number = null"
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
        let physical = physical_shard_id(table_id, shard_id);
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
        Ok(match row {
            Some(r) => shard_range_of(physical, &r),
            None => ShardRange {
                physical,
                after: ZERO_SEQUENCE.to_owned(),
                until: None,
            },
        })
    }
}

#[path = "stream_lifecycle_rows.rs"]
mod rows;

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
