// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `PostgresEngine`.

use extenddb_core::types::{StreamDescription, StreamRecord, StreamSummary};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage;
use extenddb_storage::util::{parse_stream_arn, shard_in_stream, stream_arn};
use futures::future::BoxFuture;
use std::time::Duration;

use crate::PostgresEngine;
use crate::stream_records_sql::{STREAM_RECORDS_AFTER, STREAM_RECORDS_FROM_START};

/// The most pending records one numbering transaction moves.
const NUMBER_BATCH: i64 = 1_000;

/// The record bytes after which a numbering transaction adds no more records
/// to its batch. A batch always takes its first record, so it ends at most one
/// record past this. The shard's row stays locked until the batch commits, and
/// the time a batch takes grows with its bytes, because each record is copied
/// to `stream_records`: on PostgreSQL 16, a batch of 1,000 records took 26 ms
/// at about 1 KB a record and 2.4 s at 100 KB. The bytes are the
/// `record_bytes` column, the length of each record's JSON text as the write
/// sent it. The copy decompresses and rewrites each record, so its time
/// follows that length, which TOAST compression does not shrink.
const NUMBER_BATCH_BYTES: i64 = 1 << 20;

/// How many pending records the numbering worker numbers on one shard in one
/// pass, in batches. A read numbers one batch at most before it reads the
/// shard (`number_before_read`).
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
        SELECT id, record_bytes AS size FROM stream_pending \
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
    /// per session (CACHE 1, which migration 7 sets), so it hands out values
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

    /// Number one batch of the shard's committed pending records before a
    /// read of it: [`NUMBER_BATCH`] records, or fewer once they hold
    /// [`NUMBER_BATCH_BYTES`]. One batch bounds the time a read spends here
    /// to about one batch's copy (26 ms for 1,000 records of about 1 KB on
    /// PostgreSQL 16).
    ///
    /// A read then returns the records whose writes committed before it
    /// started, except in three cases: the shard holds more pending records
    /// than one batch takes, another transaction numbers the shard for longer
    /// than [`NUMBER_WAIT`], or this numbering fails. The records left
    /// pending get higher numbers than any the read returns, from the next
    /// read or the numbering worker, and a later read returns them.
    ///
    /// When another transaction numbers the shard, the read waits for it with
    /// no connection held, looking again after a pause, for [`NUMBER_WAIT`]
    /// at most. With nothing pending this costs one indexed `EXISTS`.
    async fn number_before_read(
        &self,
        shard_id: &str,
        table_id: Option<&str>,
    ) -> Result<(), StorageError> {
        let deadline = tokio::time::Instant::now() + NUMBER_WAIT;
        let mut pause = Duration::from_millis(1);
        loop {
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
                return Ok(());
            }
            match self.number_shard_batch(shard_id, table_id).await? {
                Numbered::Moved { .. } | Numbered::Missing => return Ok(()),
                Numbered::Busy => {
                    if tokio::time::Instant::now() + pause > deadline {
                        return Ok(());
                    }
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(NUMBER_PAUSE);
                }
            }
        }
    }

    /// [`Self::number_before_read`], with a failure logged instead of
    /// returned: the read goes on and returns the records numbered so far.
    /// The records left pending are numbered by the next read or the
    /// numbering worker, with higher numbers than any this read returns.
    async fn number_before_read_or_log(&self, shard_id: &str, table_id: Option<&str>) {
        if let Err(e) = self.number_before_read(shard_id, table_id).await {
            tracing::warn!("Stream record numbering before a read of shard {shard_id} failed: {e}");
        }
    }

    /// Number up to [`NUMBER_LIMIT`] of one shard's committed pending
    /// records, in batches, and stop at a shard that another transaction
    /// numbers. Returns how many records moved.
    pub(crate) async fn number_shard(&self, shard_id: &str) -> Result<u64, StorageError> {
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

    /// Number every committed pending record of shard `shard_id` in `tx`, a
    /// READ COMMITTED transaction that holds the shard's row `FOR UPDATE`:
    /// the close of a shard (`close_stream_shards` in `stream_lifecycle.rs`),
    /// so the ending number it takes in `tx` covers every record whose write
    /// committed before it. The lock conflicts with the `FOR KEY SHARE` lock
    /// a write takes on its shard's row, so no record of the shard is stored
    /// while `tx` runs, and with [`LOCK_SHARD`], so no other transaction
    /// numbers the shard. The close numbers the shard's backlog with
    /// [`Self::number_shard`] before it takes the lock, so this moves the
    /// records committed after that. Returns how many records moved.
    pub(crate) async fn number_locked_shard(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        shard_id: &str,
    ) -> Result<u64, StorageError> {
        let mut numbered = 0;
        loop {
            let (moved, looked): (i64, i64) = sqlx::query_as(NUMBER_PENDING)
                .bind(shard_id)
                .bind(NUMBER_BATCH)
                .bind(NUMBER_BATCH_BYTES)
                .fetch_one(&mut **tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            numbered += moved.unsigned_abs();
            // A batch takes at least one record when one is pending, so the
            // loop ends once a batch takes every record it looked at and
            // looked at fewer than a full batch.
            if moved == looked && looked < NUMBER_BATCH {
                return Ok(numbered);
            }
        }
    }

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
            // and the shard id (`shard_in_stream`), with a third round trip for
            // a legacy id only: whether the table has a labelled shard.
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

            // Number a batch of the shard's committed records first, so the
            // page holds the writes that committed before this read, within
            // the bounds `number_before_read` names. Only the named table's
            // shard is numbered. A numbering failure is logged, and the page
            // holds the records numbered so far.
            self.number_before_read_or_log(&shard_id, Some(&table_id))
                .await;

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
                || self.table_has_labelled_shard(&table_id),
            )
            .await?
            {
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
            let (table, ks_json) = self
                .stream_table_row(&account_id, &table_name)
                .await?
                .ok_or_else(|| extenddb_storage::util::stream_not_found(&stream_arn))?;
            let key_schema = serde_json::from_value(ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let shards = self
                .stream_shard_rows(std::slice::from_ref(&table.table_id))
                .await?;
            stream_lineage::describe_stream(
                stream_arn,
                &table,
                key_schema,
                stream_label,
                &shards,
                limit,
                exclusive_start_shard_id.as_deref(),
            )
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
            let start = exclusive_start_stream_arn
                .as_deref()
                .map(parse_stream_arn)
                .transpose()?;
            let tables = self.stream_table_rows(&account_id).await?;
            let ids: Vec<String> = tables.iter().map(|t| t.table_id.clone()).collect();
            let shards = self.stream_shard_rows(&ids).await?;
            stream_lineage::list_streams(
                &tables,
                &shards,
                table_name.as_deref(),
                limit,
                start,
                |t, l| stream_arn(&self.region, &account_id, t, l),
            )
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

            // Only the open shards: they belong to the table's enabled stream.
            // A disabled stream's closed shards take no new records.
            let shards: Vec<(String,)> = sqlx::query_as(
                "SELECT shard_id FROM stream_shards \
                 WHERE table_id = $1 AND ending_sequence_number IS NULL \
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
            let table = self.stream_table_row(&account_id, &table_name).await?;
            let shards = match &table {
                Some((t, _)) => {
                    self.stream_shard_rows(std::slice::from_ref(&t.table_id))
                        .await?
                }
                None => Vec::new(),
            };
            stream_lineage::validate_shard(
                table.as_ref().map(|(t, _)| t),
                &stream_label,
                &shards,
                &shard_id,
            )
        })
    }

    fn latest_sequence_number(
        &self,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        let shard_id = shard_id.to_string();
        Box::pin(async move {
            // A LATEST iterator starts after the records whose writes have
            // committed, so a batch of them is numbered first. A write still
            // open is numbered later with a higher number, and the iterator
            // returns it. When more records of the shard wait for their
            // numbers than one batch takes, or the numbering fails (it is
            // logged), the rest are numbered later too, above the number
            // returned here, and the iterator returns them although their
            // writes committed before it was taken.
            self.number_before_read_or_log(&shard_id, None).await;
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

impl PostgresEngine {
    /// Whether table `table_id` has a labelled shard row
    /// (`stream_lineage::labelled_shard_id_range`, compared in byte order
    /// whatever the database's collation), which makes its legacy rows an
    /// earlier, closed stream (`util::shard_in_stream`).
    async fn table_has_labelled_shard(&self, table_id: &str) -> Result<bool, StorageError> {
        let (from, to) = extenddb_storage::stream_lineage::labelled_shard_id_range(table_id);
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM stream_shards WHERE table_id = $1 \
             AND shard_id COLLATE \"C\" >= $2 AND shard_id COLLATE \"C\" < $3)",
        )
        .bind(table_id)
        .bind(&from)
        .bind(&to)
        .fetch_one(&self.data_pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))
    }
}
