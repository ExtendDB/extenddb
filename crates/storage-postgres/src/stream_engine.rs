// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `PostgresEngine`.

use extenddb_core::types::{
    SequenceNumberRange, Shard, StreamDescription, StreamRecord, StreamStatus, StreamSummary,
    StreamViewType,
};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{new_stream_label, parse_stream_arn, shard_in_stream, stream_arn};
use futures::future::BoxFuture;
use sqlx::PgPool;
use std::time::Duration;

use crate::PostgresEngine;
use crate::stream_records_sql::{STREAM_RECORDS_AFTER, STREAM_RECORDS_FROM_START};

/// Number of fixed shards per stream (hash-based assignment).
const SHARDS_PER_STREAM: u32 = 4;

/// The most pending records one numbering transaction moves.
const NUMBER_BATCH: i64 = 1_000;

/// The record bytes after which a numbering transaction adds no more records
/// to its batch. A batch always takes its first record, so it ends at most one
/// record past this. The shard's row stays locked until the batch commits, and
/// the time a batch takes grows with its bytes, because each record is copied
/// to `stream_records`: on PostgreSQL 16, a batch of 1,000 records took 26 ms
/// at about 1 KB a record and 2.4 s at 100 KB.
const NUMBER_BATCH_BYTES: i64 = 1 << 20;

/// How many pending records a read numbers, in batches, before it reads the
/// shard, and how many the numbering worker numbers on one shard in one pass.
/// A read right after a write returns the write's record unless the shard
/// holds more pending records than this. The rest are numbered later, by the
/// next read or the worker, after every number already visible.
const NUMBER_LIMIT: u64 = 10_000;

/// How long a read waits in all for other transactions that number its shard.
/// The read then reads the shard as it is: the records those transactions
/// number get higher numbers than any the read returns, so the next read
/// returns them.
const NUMBER_WAIT: Duration = Duration::from_secs(1);

/// The longest pause between two looks at a shard that another transaction
/// numbers. The pauses start at 1 ms and double.
const NUMBER_PAUSE: Duration = Duration::from_millis(16);

/// Lock a shard's row for a numbering transaction, or no row when another
/// transaction numbers the shard. `$2` is the table the shard must belong to,
/// or NULL for any.
const LOCK_SHARD: &str = "SELECT 1 FROM stream_shards \
     WHERE shard_id = $1 AND ($2::text IS NULL OR table_id = $2) \
     FOR NO KEY UPDATE SKIP LOCKED";

/// Whether the shard [`LOCK_SHARD`] names exists, without a lock.
const SHARD_EXISTS: &str = "SELECT EXISTS (SELECT 1 FROM stream_shards \
     WHERE shard_id = $1 AND ($2::text IS NULL OR table_id = $2))";

/// The shards that have pending records. Each step finds the next shard id
/// through the primary key of `stream_pending`, so the cost grows with the
/// shards that have pending records, not with the rows of the table.
const PENDING_SHARDS: &str = "\
    WITH RECURSIVE shards AS ( \
        (SELECT shard_id FROM stream_pending ORDER BY shard_id LIMIT 1) \
        UNION ALL \
        SELECT (SELECT p.shard_id FROM stream_pending p WHERE p.shard_id > shards.shard_id \
                ORDER BY p.shard_id LIMIT 1) \
        FROM shards WHERE shards.shard_id IS NOT NULL \
    ) \
    SELECT shard_id FROM shards WHERE shard_id IS NOT NULL";

/// Move a batch of shard `$1`'s committed records from `stream_pending` to
/// `stream_records`: the records with the lowest `id`, at most `$2` of them,
/// and no more once the records before one hold `$3` bytes or more. It draws
/// one `stream_seq` value for each record, sorts the values, and gives them
/// out in `id` order, so a record stored later gets a higher number. The
/// number goes in the `sequence_number` column and in the record's
/// `SequenceNumber`. Returns how many records moved and how many of the
/// first `$2` it looked at.
const NUMBER_PENDING: &str = "\
    WITH first AS ( \
        SELECT id, pg_column_size(record_data) AS size FROM stream_pending \
        WHERE shard_id = $1 ORDER BY id LIMIT $2 \
    ), sized AS ( \
        SELECT id, sum(size) OVER (ORDER BY id) - size AS before FROM first \
    ), batch AS MATERIALIZED ( \
        SELECT p.id, p.table_id, p.event_name, p.record_data, p.created_at \
        FROM stream_pending p JOIN sized ON p.shard_id = $1 AND p.id = sized.id \
        WHERE sized.before < $3 \
    ), ranked AS ( \
        SELECT batch.*, row_number() OVER (ORDER BY id) AS rn FROM batch \
    ), numbers AS MATERIALIZED ( \
        SELECT lpad(n::text, 21, '0') AS seq, row_number() OVER (ORDER BY n) AS rn \
        FROM (SELECT nextval('stream_seq') AS n FROM batch) drawn \
    ), moved AS ( \
        DELETE FROM stream_pending p USING batch WHERE p.shard_id = $1 AND p.id = batch.id \
    ), inserted AS ( \
        INSERT INTO stream_records \
            (sequence_number, shard_id, table_id, event_name, record_data, created_at) \
        SELECT numbers.seq, $1, ranked.table_id, ranked.event_name, \
            jsonb_set(ranked.record_data, '{dynamodb,SequenceNumber}', to_jsonb(numbers.seq)), \
            ranked.created_at \
        FROM ranked JOIN numbers USING (rn) \
        RETURNING 1 \
    ) \
    SELECT (SELECT count(*) FROM inserted), (SELECT count(*) FROM first)";

/// What one numbering transaction did.
enum Numbered {
    /// It moved `moved` records, and `more` is set when the shard had more
    /// pending records than the batch took.
    Moved { moved: u64, more: bool },
    /// Another transaction holds the shard's row: it numbers the shard.
    Busy,
    /// The shard is gone, or it belongs to another table than the caller
    /// named.
    Missing,
}

impl PostgresEngine {
    /// Give a batch of a shard's committed records in `stream_pending` their
    /// sequence numbers and move them to `stream_records`, in one
    /// transaction ([`NUMBER_PENDING`]).
    ///
    /// A write stores its record in `stream_pending` with no number
    /// (`write_stream_record_in_tx`), and the record is numbered here once the
    /// write has committed. That is what keeps a reader from passing a record
    /// (#808). The transaction holds the shard's row `FOR NO KEY UPDATE` until
    /// it commits, so the numberings of one shard run one after another, each
    /// after the previous one committed. `stream_seq` keeps no values cached
    /// per session (CACHE 1, which migration 6 sets), so it hands out values
    /// in the order of the calls across all sessions, and every number drawn
    /// here is higher than every number an earlier numbering of the shard
    /// committed. The batch holds the records committed when the statement
    /// starts, after the lock is granted, and a write that commits later is
    /// numbered by a later call, with a higher number still. Two writes to one
    /// item take the item's row lock in turn, so the second stores its record
    /// after the first commits, with a higher `id`, and it is numbered after
    /// the first.
    ///
    /// The lock does not wait: when another transaction holds the shard's row,
    /// this returns [`Numbered::Busy`] and gives its connection back, so a
    /// caller that waits for that numbering holds no pool connection while it
    /// waits. A writer locks the shard's row only `FOR KEY SHARE`, through the
    /// foreign key of its insert, and that mode does not conflict with
    /// `FOR NO KEY UPDATE`: writers never wait for a numbering, and a
    /// numbering does not wait for writers.
    ///
    /// The transaction is READ COMMITTED whatever the database's default, so
    /// the batch is read with a snapshot taken after the lock is granted.
    /// Under REPEATABLE READ the snapshot would date from the start of the
    /// lock statement, and a numbering that committed between that start and
    /// the lock would leave records in the snapshot that it had already
    /// moved. The delete would then fail with a serialization error.
    async fn number_shard_batch(
        &self,
        shard_id: &str,
        table_id: Option<&str>,
    ) -> Result<Numbered, StorageError> {
        let internal = |e: sqlx::Error| StorageError::Internal(e.to_string());
        let mut tx = self
            .data_pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await
            .map_err(internal)?;
        let locked: Option<i32> = sqlx::query_scalar(LOCK_SHARD)
            .bind(shard_id)
            .bind(table_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(internal)?;
        if locked.is_none() {
            let exists: bool = sqlx::query_scalar(SHARD_EXISTS)
                .bind(shard_id)
                .bind(table_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(internal)?;
            tx.rollback().await.map_err(internal)?;
            return Ok(if exists {
                Numbered::Busy
            } else {
                Numbered::Missing
            });
        }
        let (moved, looked): (i64, i64) = sqlx::query_as(NUMBER_PENDING)
            .bind(shard_id)
            .bind(NUMBER_BATCH)
            .bind(NUMBER_BATCH_BYTES)
            .fetch_one(&mut *tx)
            .await
            .map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        Ok(Numbered::Moved {
            moved: moved.unsigned_abs(),
            more: moved < looked || looked == NUMBER_BATCH,
        })
    }

    /// Number the shard's committed pending records before a read of it, up
    /// to [`NUMBER_LIMIT`]. A read then returns every record whose write
    /// committed before it started. When another transaction numbers the
    /// shard, the read waits for it with no connection held, looking again
    /// after a pause, for [`NUMBER_WAIT`] at most. With nothing pending this
    /// costs one indexed `EXISTS`.
    async fn number_before_read(
        &self,
        shard_id: &str,
        table_id: Option<&str>,
    ) -> Result<(), StorageError> {
        let deadline = tokio::time::Instant::now() + NUMBER_WAIT;
        let mut pause = Duration::from_millis(1);
        let mut numbered = 0;
        while numbered < NUMBER_LIMIT {
            // A record that a running numbering moves still counts as
            // pending here until that numbering commits.
            let pending: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM stream_pending WHERE shard_id = $1)",
            )
            .bind(shard_id)
            .fetch_one(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if !pending {
                break;
            }
            match self.number_shard_batch(shard_id, table_id).await? {
                Numbered::Moved { moved, more } => {
                    numbered += moved;
                    if !more {
                        break;
                    }
                }
                Numbered::Busy => {
                    if tokio::time::Instant::now() + pause > deadline {
                        break;
                    }
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(NUMBER_PAUSE);
                }
                Numbered::Missing => break,
            }
        }
        Ok(())
    }

    /// Number up to [`NUMBER_LIMIT`] of one shard's committed pending
    /// records, in batches, and stop at a shard that another transaction
    /// numbers. Returns how many records moved.
    async fn number_shard(&self, shard_id: &str) -> Result<u64, StorageError> {
        let mut numbered = 0;
        while numbered < NUMBER_LIMIT {
            match self.number_shard_batch(shard_id, None).await? {
                Numbered::Moved { moved, more } => {
                    numbered += moved;
                    if !more {
                        break;
                    }
                }
                Numbered::Busy | Numbered::Missing => break,
            }
        }
        Ok(numbered)
    }

    /// Number the committed pending records of every shard that has some, up
    /// to [`NUMBER_LIMIT`] per shard, skipping a shard that another
    /// transaction numbers. The numbering worker runs this, so a shard that
    /// nobody reads still has its records numbered, and the retention sweep
    /// trims them as usual. A shard that fails does not stop the others: the
    /// pass numbers every shard it can and then returns the first error.
    /// Returns how many records moved.
    pub(crate) async fn number_pending_stream_records(&self) -> Result<u64, StorageError> {
        let shards: Vec<String> = sqlx::query_scalar(PENDING_SHARDS)
            .fetch_all(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let mut total = 0;
        let mut first_error = None;
        for shard_id in &shards {
            match self.number_shard(shard_id).await {
                Ok(n) => total += n,
                Err(e) => {
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(total),
        }
    }

    /// [`Self::number_pending_stream_records`], for tests that stand in for
    /// the numbering worker.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn number_pending_stream_records_for_tests(&self) -> Result<u64, StorageError> {
        self.number_pending_stream_records().await
    }

    /// Initialize stream shards for a table and set the `stream_label`.
    ///
    /// Updates the stream label in the catalog (via the provided catalog
    /// transaction) and creates shard rows in the data database within a
    /// data transaction for atomicity.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Internal`] if any query fails.
    /// Remove the shards of deleted tables once their stream has aged out.
    ///
    /// DeleteTable leaves a table's shards and records in place, as the service
    /// keeps a deleted table's stream readable for 24 hours, and the records
    /// are trimmed by the retention sweep above. The shards were never
    /// trimmed, so every deleted stream-enabled table left four rows behind
    /// for good. `stream_shards` is in the data database and `tables` in the
    /// catalog, so this is two steps: candidates are shards older than the
    /// retention window belonging to a table with no records left, and only those whose table id
    /// is absent from the catalog are removed. A table created moments ago is
    /// never a candidate, because its shards are younger than the window, so
    /// the gap between inserting shards and committing the catalog row cannot
    /// be mistaken for a deletion. A live table's unused shards (after a
    /// stream was disabled and re-enabled) are left alone.
    async fn cleanup_orphaned_stream_shards(
        &self,
        retention_hours: i64,
    ) -> Result<u64, StorageError> {
        // A record still in stream_pending counts as one of the table's
        // records: deleting the shard would take it with it, through the
        // foreign key's cascade, before the numbering worker numbers it.
        let candidates: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT s.table_id FROM stream_shards s \
             WHERE s.created_at < NOW() - make_interval(hours => $1::integer) \
               AND NOT EXISTS (SELECT 1 FROM stream_records r WHERE r.table_id = s.table_id) \
               AND NOT EXISTS (SELECT 1 FROM stream_pending p WHERE p.table_id = s.table_id)",
        )
        .bind(retention_hours)
        .fetch_all(&self.data_pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        if candidates.is_empty() {
            return Ok(0);
        }
        let live: Vec<String> =
            sqlx::query_scalar("SELECT table_id FROM tables WHERE table_id = ANY($1)")
                .bind(&candidates)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        let gone: Vec<String> = candidates
            .into_iter()
            .filter(|id| !live.contains(id))
            .collect();
        if gone.is_empty() {
            return Ok(0);
        }
        // Re-check for records: a table deleted since the first query may
        // still have records inside the window. A table's shards go together,
        // once none of its records remain.
        let result = sqlx::query(
            "DELETE FROM stream_shards s WHERE s.table_id = ANY($1) \
               AND NOT EXISTS (SELECT 1 FROM stream_records r WHERE r.table_id = s.table_id) \
               AND NOT EXISTS (SELECT 1 FROM stream_pending p WHERE p.table_id = s.table_id)",
        )
        .bind(&gone)
        .execute(&self.data_pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        if result.rows_affected() > 0 {
            tracing::debug!(
                shards = result.rows_affected(),
                tables = gone.len(),
                "removed stream shards of deleted tables past the retention window"
            );
        }
        Ok(result.rows_affected())
    }

    pub(crate) async fn init_stream_shards(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        data_pool: &PgPool,
        account_id: &str,
        table_name: &str,
        table_id: &str,
    ) -> Result<String, StorageError> {
        // Formatted here, not in SQL, so every backend issues the same label
        // shape from one function (`extenddb_storage::util::format_stream_label`).
        let label = new_stream_label();
        sqlx::query(
            "UPDATE tables SET stream_label = $3 WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(table_name)
        .bind(&label)
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // P54 Bug 1: Stream shards live in the data database for atomic
        // writes with stream records and item data. Use a transaction so
        // all shards are created atomically.
        let mut data_tx = data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for i in 0..SHARDS_PER_STREAM {
            // Derived from the table id, not its name. A name is reused by
            // delete-and-recreate and by every account that picks it, while
            // `stream_shards.shard_id` is unique across the data database
            // and PostgreSQL keeps a deleted table's shards. A name of
            // up to 255 bytes also overflowed the 65-character ShardId limit
            // the AWS SDKs enforce. The table id is a UUID, giving a
            // 61-character id.
            let shard_id = format!("shardId-{table_id}-{i:016}");
            let start_seq = format!("{:021}", 0);
            sqlx::query(
                "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
                 VALUES ($1, $2, $3)",
            )
            .bind(&shard_id)
            .bind(table_id)
            .bind(&start_seq)
            .execute(&mut *data_tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        data_tx
            .commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(label)
    }
}

impl StreamEngine for PostgresEngine {
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // A record stored apart from its write would not wait for the write's
        // item lock, and its number would not follow the write's commit
        // (#808). Every write path stores its record in its own transaction
        // instead (`write_stream_record_in_tx`).
        Box::pin(async move {
            Err(StorageError::Internal(
                "write_stream_record is not used in the PostgreSQL backend; each write stores \
                 its stream record in its own transaction, and the record is numbered after \
                 the write commits"
                    .to_owned(),
            ))
        })
    }

    fn get_stream_records(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, Result<(Vec<StreamRecord>, Option<String>), StorageError>> {
        let account_id = account_id.to_string();
        let stream_arn = stream_arn.to_string();
        let shard_id = shard_id.to_string();
        let after_sequence = after_sequence.map(std::string::ToString::to_string);
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn)?;
            // The `tables` catalog lives in the catalog database and
            // `stream_shards`/`stream_records` in the data database, which may
            // be a different server, so the stream resolves in two round
            // trips: the table the ARN names (catalog pool: its id and current
            // stream label), then the shard and its records in one query
            // (data pool). The shard row drives the data query and the page of
            // records is a LEFT JOIN LATERAL subquery that carries its own
            // ORDER BY and LIMIT (`stream_records_sql`): a shard of the named
            // table with no records returns one row with a NULL record, and a
            // shard that is not that table's (another table, or another
            // account) returns no rows. Whether the shard belongs to the stream
            // the ARN's label names is decided in Rust from the table's label
            // and the shard id (`shard_in_stream`).
            let table: Option<(String, Option<String>)> = sqlx::query_as(
                "SELECT table_id, stream_label FROM tables \
                 WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let Some((table_id, table_label)) = table else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };

            // Number the shard's committed records first, so the page holds
            // every write that committed before this read. Only the named
            // table's shard is numbered.
            self.number_before_read(&shard_id, Some(&table_id)).await?;

            let rows: Vec<(String, Option<serde_json::Value>)> = if let Some(after) = after_sequence
            {
                sqlx::query_as(STREAM_RECORDS_AFTER)
                    .bind(&shard_id)
                    .bind(&table_id)
                    .bind(&after)
                    .bind(limit)
                    .fetch_all(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
            } else {
                sqlx::query_as(STREAM_RECORDS_FROM_START)
                    .bind(&shard_id)
                    .bind(&table_id)
                    .bind(limit)
                    .fetch_all(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
            };
            let Some((row_shard_id, _)) = rows.first() else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            if !shard_in_stream(
                &table_id,
                row_shard_id,
                table_label.as_deref(),
                &stream_label,
            ) {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }

            let records: Vec<StreamRecord> = rows
                .into_iter()
                .filter_map(|(_, data)| data)
                .map(|data| {
                    serde_json::from_value(data).map_err(|e| StorageError::Internal(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let last_seq = records.last().map(|r| r.dynamodb.sequence_number.clone());
            Ok((records, last_seq))
        })
    }

    fn describe_stream(
        &self,
        account_id: &str,
        input: &extenddb_core::types::DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        let account_id = account_id.to_string();
        let stream_arn = input.stream_arn.clone();
        let limit = input.limit;
        let exclusive_start_shard_id = input.exclusive_start_shard_id.clone();
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn)?;

            let row: Option<(serde_json::Value, serde_json::Value, Option<serde_json::Value>, String, String)> =
                sqlx::query_as(
                    "SELECT key_schema, attribute_definitions, stream_specification, table_status, table_id \
                     FROM tables WHERE account_id = $1 AND table_name = $2 AND stream_label = $3",
                )
                .bind(&account_id)
                .bind(&table_name)
                .bind(&stream_label)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (ks_json, _ad_json, stream_spec_json, table_status, table_id) =
                row.ok_or_else(|| extenddb_storage::util::stream_not_found(&stream_arn))?;

            let key_schema = serde_json::from_value(ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let stream_view_type = stream_spec_json
                .and_then(|v| {
                    v.get("StreamViewType")
                        .and_then(|sv| serde_json::from_value::<StreamViewType>(sv.clone()).ok())
                })
                .unwrap_or(StreamViewType::KeysOnly);

            let limit = limit.unwrap_or(100);
            let shard_rows: Vec<(String, Option<String>, String, Option<String>)> = if let Some(
                ref start,
            ) =
                exclusive_start_shard_id
            {
                sqlx::query_as(
                        "SELECT shard_id, parent_shard_id, starting_sequence_number, ending_sequence_number \
                         FROM stream_shards WHERE table_id = $1 AND shard_id > $2 \
                         ORDER BY shard_id LIMIT $3",
                    )
                    .bind(&table_id)
                    .bind(start)
                    .bind(limit + 1)
                    .fetch_all(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
            } else {
                sqlx::query_as(
                        "SELECT shard_id, parent_shard_id, starting_sequence_number, ending_sequence_number \
                         FROM stream_shards WHERE table_id = $1 \
                         ORDER BY shard_id LIMIT $2",
                    )
                    .bind(&table_id)
                    .bind(limit + 1)
                    .fetch_all(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
            };

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit as usize;
            let last_shard = if shard_rows.len() > limit_usize {
                Some(shard_rows[limit_usize - 1].0.clone())
            } else {
                None
            };

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
    ) -> BoxFuture<'_, Result<(Vec<StreamSummary>, Option<String>), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.map(std::string::ToString::to_string);
        let exclusive_start_stream_arn =
            exclusive_start_stream_arn.map(std::string::ToString::to_string);
        Box::pin(async move {
            let rows: Vec<(String, String, String)> = match (
                table_name.as_deref(),
                exclusive_start_stream_arn.as_deref(),
            ) {
                (Some(tn), Some(start_arn)) => {
                    let (_, start_label) = parse_stream_arn(start_arn)?;
                    sqlx::query_as(
                        "SELECT table_name, table_arn, stream_label FROM tables \
                         WHERE account_id = $1 AND stream_label IS NOT NULL AND table_name = $2 AND stream_label > $3 \
                         ORDER BY stream_label LIMIT $4",
                    )
                    .bind(&account_id)
                    .bind(tn)
                    .bind(&start_label)
                    .bind(limit + 1)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                }
                (Some(tn), None) => sqlx::query_as(
                    "SELECT table_name, table_arn, stream_label FROM tables \
                         WHERE account_id = $1 AND stream_label IS NOT NULL AND table_name = $2 \
                         ORDER BY stream_label LIMIT $3",
                )
                .bind(&account_id)
                .bind(tn)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?,
                (None, Some(start_arn)) => {
                    let (start_table, start_label) = parse_stream_arn(start_arn)?;
                    sqlx::query_as(
                        "SELECT table_name, table_arn, stream_label FROM tables \
                         WHERE account_id = $1 AND stream_label IS NOT NULL \
                           AND (table_name, stream_label) > ($2, $3) \
                         ORDER BY table_name, stream_label LIMIT $4",
                    )
                    .bind(&account_id)
                    .bind(&start_table)
                    .bind(&start_label)
                    .bind(limit + 1)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                }
                (None, None) => sqlx::query_as(
                    "SELECT table_name, table_arn, stream_label FROM tables \
                         WHERE account_id = $1 AND stream_label IS NOT NULL \
                         ORDER BY table_name, stream_label LIMIT $2",
                )
                .bind(&account_id)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?,
            };

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit as usize;

            let summaries: Vec<StreamSummary> = rows
                .iter()
                .take(limit_usize)
                .map(|(tn, _table_arn, label)| StreamSummary {
                    stream_arn: stream_arn(&self.region, &account_id, tn, label),
                    stream_label: label.clone(),
                    table_name: tn.clone(),
                })
                .collect();

            let last_arn = if rows.len() > limit_usize {
                summaries.last().map(|s| s.stream_arn.clone())
            } else {
                None
            };

            Ok((summaries, last_arn))
        })
    }

    fn cleanup_expired_stream_records(
        &self,
        retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move {
            let result = sqlx::query(
                "DELETE FROM stream_records \
                 WHERE created_at < NOW() - make_interval(hours => $1::integer)",
            )
            .bind(retention_hours)
            .execute(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            self.cleanup_orphaned_stream_shards(retention_hours).await?;
            Ok(result.rows_affected())
        })
    }

    fn assign_shard(
        &self,
        account_id: &str,
        table_name: &str,
        partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let partition_key = partition_key.to_string();
        Box::pin(async move {
            let table_id: String = sqlx::query_scalar(
                "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let shards: Vec<(String,)> = sqlx::query_as(
                "SELECT shard_id FROM stream_shards \
                 WHERE table_id = $1 \
                 ORDER BY shard_id",
            )
            .bind(&table_id)
            .fetch_all(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if shards.is_empty() {
                return Err(StorageError::Internal(format!(
                    "No stream shards for table {table_name}"
                )));
            }

            let hash = crc32fast::hash(partition_key.as_bytes());
            #[allow(clippy::cast_possible_truncation)]
            let idx = (hash as usize) % shards.len();
            Ok(shards[idx].0.clone())
        })
    }

    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        // A number drawn before a record is stored could commit after a higher
        // one that a reader has already passed (#808). Records get their
        // numbers after their writes commit (`number_shard_batch`).
        Box::pin(async move {
            Err(StorageError::Internal(
                "next_sequence_number is not used in the PostgreSQL backend; stream records get \
                 their sequence numbers after their writes commit"
                    .to_owned(),
            ))
        })
    }

    fn validate_shard(
        &self,
        account_id: &str,
        stream_arn: &str,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let stream_arn = stream_arn.to_string();
        let shard_id = shard_id.to_string();
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn)?;

            let table_id: Option<String> = sqlx::query_scalar(
                "SELECT table_id FROM tables \
                 WHERE account_id = $1 AND table_name = $2 AND stream_label = $3",
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

            let exists: Option<(i32,)> =
                sqlx::query_as("SELECT 1 FROM stream_shards WHERE shard_id = $1 AND table_id = $2")
                    .bind(&shard_id)
                    .bind(&table_id)
                    .fetch_optional(&self.data_pool)
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
        let shard_id = shard_id.to_string();
        Box::pin(async move {
            // A LATEST iterator starts after every record whose write has
            // committed, so those are numbered first, up to NUMBER_LIMIT of
            // them. A write still open is numbered later with a higher
            // number, and the iterator returns it. When more than
            // NUMBER_LIMIT records of the shard wait for their numbers, the
            // rest are numbered later too, above the number returned here,
            // and the iterator returns them although their writes committed
            // before it was taken.
            self.number_before_read(&shard_id, None).await?;
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT sequence_number FROM stream_records \
                 WHERE shard_id = $1 ORDER BY sequence_number DESC LIMIT 1",
            )
            .bind(&shard_id)
            .fetch_optional(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(row.map(|(s,)| s))
        })
    }
}
