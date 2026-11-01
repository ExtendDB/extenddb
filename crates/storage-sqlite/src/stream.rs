// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `SqliteEngine`.
//!
//! Shards, records, and the sequence counter all live in the one SQLite file,
//! so `init_stream_shards` runs in the caller's catalog transaction.
//! Monotonic sequence numbers come from the `seq_counters` table (there is no
//! PostgreSQL sequence). Retention cleanup compares an RFC 3339 cutoff computed
//! in Rust.

use extenddb_core::types::{
    DescribeStreamInput, SequenceNumberRange, Shard, StreamDescription, StreamRecord, StreamStatus,
    StreamSummary, StreamViewType,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_stream_arn, shard_in_stream, stream_arn};
use extenddb_storage::{StreamEngine, StreamListResult, StreamRecordsResult};
use futures::future::BoxFuture;

use crate::sqlite_util::format_timestamp;
use crate::store::SqliteEngine;

/// Fixed shards per stream (hash-based partition-key assignment).
const SHARDS_PER_STREAM: u32 = 4;

/// Most stream records one retention cleanup statement deletes. Each statement
/// holds the engine write lock for its own duration only, so this bounds how
/// long a write waits behind the cleanup.
///
/// Measured with `stream_cleanup_bench`: release build, file database,
/// 100,000 expired records with a 500-byte payload each, three rounds per
/// size, on 16 cores with a one-minute load average of 31.1 at the start of
/// the run and 27.4 at the end. A 250-record statement held the lock for a
/// median of 7 to 8 ms and at most 234 to 309 ms, and a pass over the
/// 100,000 records took 10.5 to 14.8 s. A 500-record statement held it for a
/// median of 8 to 17 ms and at most 236 to 362 ms; a pass took 7.6 to 8.1 s.
/// A 1,000-record statement held it for a median of 20 to 22 ms and at most
/// 265 to 849 ms; a pass took 5.1 to 7.7 s. In the two runs of this shape
/// that included 250, the holds at 250 and 500 in the same round differed by
/// at most 11 ms at the median and 119 ms at the longest, less than the spread
/// between rounds at 500, and 250 made the pass 1.4 to 1.8 times longer. The
/// longest 500-record hold was 236 to 512 ms across three runs of this shape
/// and 931 ms in a run of 280,000 records during which the load fell from
/// 75.4 to 18.2. The single statement this replaces deleted 843 to 9,761
/// records per second in a soak run, which puts 500 records at 51 to 593 ms
/// and 1,000 at up to 1.19 s.
pub(crate) const STREAM_CLEANUP_BATCH_ROWS: u32 = 500;

/// One cleanup batch: the oldest `?2` records created before the cutoff `?1`.
/// The inner select reads `idx_stream_records_created` in index order, and the
/// outer delete finds each row by rowid.
pub(crate) const DELETE_EXPIRED_STREAM_RECORDS_BATCH: &str = "DELETE FROM stream_records \
     WHERE rowid IN (SELECT rowid FROM stream_records WHERE created_at < ?1 \
     ORDER BY created_at LIMIT ?2)";

impl SqliteEngine {
    /// Delete the stream records created before `cutoff`, at most `batch_rows`
    /// per statement, until a statement deletes fewer than `batch_rows`.
    /// Returns the records deleted and the statements run.
    ///
    /// Each statement takes the engine write lock (D1) for itself and releases
    /// it before the next one. The lock is a FIFO tokio mutex, so a write that
    /// queued during a statement runs before the next statement, and waits for
    /// one batch instead of the whole cleanup.
    pub(crate) async fn delete_stream_records_before(
        &self,
        cutoff: &str,
        batch_rows: u32,
    ) -> Result<(u64, u64), StorageError> {
        let batch_rows = batch_rows.max(1);
        let mut deleted = 0;
        let mut statements = 0;
        loop {
            let batch = self.delete_stream_record_batch(cutoff, batch_rows).await?;
            deleted += batch;
            statements += 1;
            if batch < u64::from(batch_rows) {
                return Ok((deleted, statements));
            }
        }
    }

    /// Run one cleanup batch under the engine write lock and return the
    /// records it deleted.
    pub(crate) async fn delete_stream_record_batch(
        &self,
        cutoff: &str,
        batch_rows: u32,
    ) -> Result<u64, StorageError> {
        // D1: every writer holds the engine write lock, here for one batch.
        let _writer = self.write_lock.lock().await;
        let result = sqlx::query(DELETE_EXPIRED_STREAM_RECORDS_BATCH)
            .bind(cutoff)
            .bind(i64::from(batch_rows))
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(result.rows_affected())
    }

    /// Initialize stream shards and set `stream_label`, within the caller's
    /// transaction. Returns the assigned stream label.
    pub(crate) async fn init_stream_shards(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        account_id: &str,
        table_name: &str,
        table_id: &str,
    ) -> Result<String, StorageError> {
        // Formatted here, not in SQL, so every backend issues the same label
        // shape from one function (`extenddb_storage::util::format_stream_label`).
        let label = extenddb_storage::util::new_stream_label();
        sqlx::query("UPDATE tables SET stream_label = ? WHERE account_id = ? AND table_name = ?")
            .bind(&label)
            .bind(account_id)
            .bind(table_name)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for i in 0..SHARDS_PER_STREAM {
            // Derived from the table id, not its name. A name is shared by
            // every account that picks it, while `stream_shards.shard_id` is
            // unique across the database. A name of up to 255 bytes also
            // overflowed the 65-character ShardId limit
            // the AWS SDKs enforce. The table id is a UUID, giving a
            // 61-character id.
            let shard_id = format!("shardId-{table_id}-{i:016}");
            let start_seq = format!("{:021}", 0);
            sqlx::query(
                "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
                 VALUES (?, ?, ?)",
            )
            .bind(&shard_id)
            .bind(table_id)
            .bind(&start_seq)
            .execute(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        Ok(label)
    }
}

impl StreamEngine for SqliteEngine {
    fn write_stream_record(
        &self,
        account_id: &str,
        record: &StreamRecord,
        shard_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let record = record.clone();
        let shard_id = shard_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let record_json = serde_json::to_string(&record)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let table_id: String = sqlx::query_scalar(
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let _writer = self.write_lock.lock().await;
            sqlx::query(
                "INSERT INTO stream_records (sequence_number, shard_id, table_id, event_name, record_data) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&record.dynamodb.sequence_number)
            .bind(&shard_id)
            .bind(&table_id)
            .bind(format!("{:?}", record.event_name))
            .bind(&record_json)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn get_stream_records(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        let account_id = account_id.to_owned();
        let stream_arn = stream_arn.to_owned();
        let shard_id = shard_id.to_owned();
        let after = after_sequence.map(str::to_owned);
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn)?;
            // One query resolves the shard against the table the iterator's
            // ARN names (scoped to the caller's account) and reads its
            // records. The shard row is the driving row and the records are
            // LEFT JOINed, so a shard of the named table with no records
            // returns one row with a NULL record, and a shard that is not that
            // table's (another table, or another account) returns no rows at
            // all. The sequence bound sits in the join condition so the LEFT
            // JOIN keeps its one row for an idle shard. Whether the shard
            // belongs to the stream the ARN's label names is decided in Rust
            // from the table's label and the shard id (`shard_in_stream`).
            let rows: Vec<(String, Option<String>, String, Option<String>)> =
                if let Some(after) = after {
                    sqlx::query_as(
                        "SELECT t.table_id, t.stream_label, s.shard_id, r.record_data \
                         FROM stream_shards s \
                         JOIN tables t ON t.table_id = s.table_id \
                         LEFT JOIN stream_records r \
                           ON r.shard_id = s.shard_id AND r.sequence_number > ? \
                         WHERE s.shard_id = ? AND t.account_id = ? AND t.table_name = ? \
                         ORDER BY r.sequence_number LIMIT ?",
                    )
                    .bind(&after)
                    .bind(&shard_id)
                    .bind(&account_id)
                    .bind(&table_name)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await
                } else {
                    sqlx::query_as(
                        "SELECT t.table_id, t.stream_label, s.shard_id, r.record_data \
                         FROM stream_shards s \
                         JOIN tables t ON t.table_id = s.table_id \
                         LEFT JOIN stream_records r ON r.shard_id = s.shard_id \
                         WHERE s.shard_id = ? AND t.account_id = ? AND t.table_name = ? \
                         ORDER BY r.sequence_number LIMIT ?",
                    )
                    .bind(&shard_id)
                    .bind(&account_id)
                    .bind(&table_name)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let Some((table_id, table_label, row_shard_id, _)) = rows.first() else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            if !shard_in_stream(
                table_id,
                row_shard_id,
                table_label.as_deref(),
                &stream_label,
            ) {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }

            let records: Vec<StreamRecord> = rows
                .into_iter()
                .filter_map(|(_, _, _, d)| d)
                .map(|d| {
                    serde_json::from_str(&d).map_err(|e| StorageError::Internal(e.to_string()))
                })
                .collect::<Result<_, _>>()?;
            let last = records.last().map(|r| r.dynamodb.sequence_number.clone());
            Ok((records, last))
        })
    }

    fn describe_stream(
        &self,
        account_id: &str,
        input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let stream_arn = input.stream_arn.clone();
        let limit = input.limit.unwrap_or(100);
        let start = input.exclusive_start_shard_id.clone();
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn)?;
            let row: Option<(String, Option<String>, String, String)> = sqlx::query_as(
                "SELECT key_schema, stream_specification, table_status, table_id \
                 FROM tables WHERE account_id = ? AND table_name = ? AND stream_label = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .bind(&stream_label)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (ks_json, stream_spec_json, table_status, table_id) =
                row.ok_or_else(|| extenddb_storage::util::stream_not_found(&stream_arn))?;

            let key_schema = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let stream_view_type = stream_spec_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                .and_then(|v| {
                    v.get("StreamViewType")
                        .and_then(|sv| serde_json::from_value::<StreamViewType>(sv.clone()).ok())
                })
                .unwrap_or(StreamViewType::KeysOnly);

            let shard_rows: Vec<(String, Option<String>, String, Option<String>)> =
                if let Some(ref s) = start {
                    sqlx::query_as(
                        "SELECT shard_id, parent_shard_id, starting_sequence_number, ending_sequence_number \
                         FROM stream_shards WHERE table_id = ? AND shard_id > ? \
                         ORDER BY shard_id LIMIT ?",
                    )
                    .bind(&table_id)
                    .bind(s)
                    .bind(limit + 1)
                    .fetch_all(&self.pool)
                    .await
                } else {
                    sqlx::query_as(
                        "SELECT shard_id, parent_shard_id, starting_sequence_number, ending_sequence_number \
                         FROM stream_shards WHERE table_id = ? ORDER BY shard_id LIMIT ?",
                    )
                    .bind(&table_id)
                    .bind(limit + 1)
                    .fetch_all(&self.pool)
                    .await
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit.max(0) as usize;
            let last_shard =
                (shard_rows.len() > limit_usize).then(|| shard_rows[limit_usize - 1].0.clone());
            let shards: Vec<Shard> = shard_rows
                .into_iter()
                .take(limit_usize)
                .map(|(id, parent, start, end)| Shard {
                    shard_id: id,
                    parent_shard_id: parent,
                    sequence_number_range: SequenceNumberRange {
                        starting_sequence_number: start,
                        ending_sequence_number: end,
                    },
                })
                .collect();

            let stream_status = if table_status == "DELETING" {
                StreamStatus::Disabling
            } else {
                StreamStatus::Enabled
            };

            Ok(StreamDescription {
                stream_arn,
                stream_label,
                stream_status,
                stream_view_type,
                table_name,
                key_schema,
                shards,
                last_evaluated_shard_id: last_shard,
            })
        })
    }

    fn list_streams(
        &self,
        account_id: &str,
        table_name: Option<&str>,
        limit: i64,
        exclusive_start_stream_arn: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(str::to_owned);
        let start_arn = exclusive_start_stream_arn.map(str::to_owned);
        Box::pin(async move {
            let rows: Vec<(String, String)> = match (table_name.as_deref(), start_arn.as_deref()) {
                (Some(tn), Some(arn)) => {
                    let (_, start_label) = parse_stream_arn(arn)?;
                    sqlx::query_as(
                        "SELECT table_name, stream_label FROM tables \
                         WHERE account_id = ? AND stream_label IS NOT NULL AND table_name = ? \
                           AND stream_label > ? ORDER BY stream_label LIMIT ?",
                    )
                    .bind(&account_id)
                    .bind(tn)
                    .bind(&start_label)
                    .bind(limit + 1)
                    .fetch_all(&self.pool)
                    .await
                }
                (Some(tn), None) => {
                    sqlx::query_as(
                        "SELECT table_name, stream_label FROM tables \
                     WHERE account_id = ? AND stream_label IS NOT NULL AND table_name = ? \
                     ORDER BY stream_label LIMIT ?",
                    )
                    .bind(&account_id)
                    .bind(tn)
                    .bind(limit + 1)
                    .fetch_all(&self.pool)
                    .await
                }
                (None, Some(arn)) => {
                    let (start_table, start_label) = parse_stream_arn(arn)?;
                    sqlx::query_as(
                        "SELECT table_name, stream_label FROM tables \
                         WHERE account_id = ? AND stream_label IS NOT NULL \
                           AND (table_name, stream_label) > (?, ?) \
                         ORDER BY table_name, stream_label LIMIT ?",
                    )
                    .bind(&account_id)
                    .bind(&start_table)
                    .bind(&start_label)
                    .bind(limit + 1)
                    .fetch_all(&self.pool)
                    .await
                }
                (None, None) => {
                    sqlx::query_as(
                        "SELECT table_name, stream_label FROM tables \
                     WHERE account_id = ? AND stream_label IS NOT NULL \
                     ORDER BY table_name, stream_label LIMIT ?",
                    )
                    .bind(&account_id)
                    .bind(limit + 1)
                    .fetch_all(&self.pool)
                    .await
                }
            }
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit.max(0) as usize;
            let summaries: Vec<StreamSummary> = rows
                .iter()
                .take(limit_usize)
                .map(|(tn, label)| StreamSummary {
                    stream_arn: stream_arn(&self.region, &account_id, tn, label),
                    stream_label: label.clone(),
                    table_name: tn.clone(),
                })
                .collect();
            let last = (rows.len() > limit_usize)
                .then(|| summaries.last().map(|s| s.stream_arn.clone()))
                .flatten();
            Ok((summaries, last))
        })
    }

    fn cleanup_expired_stream_records(
        &self,
        retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            // One cutoff for the whole run, so the batches delete exactly the
            // records that had expired when the run started.
            let cutoff = format_timestamp(
                time::OffsetDateTime::now_utc() - time::Duration::hours(retention_hours),
            );
            let (deleted, _) = self
                .delete_stream_records_before(&cutoff, STREAM_CLEANUP_BATCH_ROWS)
                .await?;
            Ok(deleted)
        })
    }

    fn assign_shard(
        &self,
        account_id: &str,
        table_name: &str,
        partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let partition_key = partition_key.to_owned();
        Box::pin(async move {
            let table_id: String = sqlx::query_scalar(
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let shards: Vec<(String,)> = sqlx::query_as(
                "SELECT shard_id FROM stream_shards WHERE table_id = ? ORDER BY shard_id",
            )
            .bind(&table_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if shards.is_empty() {
                return Err(StorageError::Internal(format!(
                    "No stream shards for table {table_name}"
                )));
            }
            let idx = (crc32fast::hash(partition_key.as_bytes()) as usize) % shards.len();
            Ok(shards[idx].0.clone())
        })
    }

    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move {
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query("UPDATE seq_counters SET value = value + 1 WHERE name = 'stream'")
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let value: i64 =
                sqlx::query_scalar("SELECT value FROM seq_counters WHERE name = 'stream'")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(format!("{value:021}"))
        })
    }

    fn validate_shard(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let stream_arn = stream_arn.to_owned();
        let shard_id = shard_id.to_owned();
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn)?;
            let table_id: Option<String> = sqlx::query_scalar(
                "SELECT table_id FROM tables \
                 WHERE account_id = ? AND table_name = ? AND stream_label = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .bind(&stream_label)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let Some(table_id) = table_id else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            let exists: Option<(i64,)> =
                sqlx::query_as("SELECT 1 FROM stream_shards WHERE shard_id = ? AND table_id = ?")
                    .bind(&shard_id)
                    .bind(&table_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            if exists.is_none() {
                return Err(extenddb_storage::util::shard_not_in_stream(&stream_arn));
            }
            Ok(())
        })
    }

    fn latest_sequence_number(
        &self,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        let shard_id = shard_id.to_owned();
        Box::pin(async move {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT sequence_number FROM stream_records \
                 WHERE shard_id = ? ORDER BY sequence_number DESC LIMIT 1",
            )
            .bind(&shard_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(row.map(|(s,)| s))
        })
    }
}
