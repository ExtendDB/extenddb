// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `SqliteEngine`.
//!
//! Shards, records, and the sequence counter all live in the one SQLite file,
//! so `init_stream_shards` (`stream_lifecycle.rs`) runs in the caller's
//! catalog transaction. Monotonic sequence numbers come from the
//! `seq_counters` table (there is no PostgreSQL sequence). Retention cleanup
//! compares an RFC 3339 cutoff computed in Rust.

use extenddb_core::types::{DescribeStreamInput, StreamDescription, StreamRecord};
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage;
use extenddb_storage::util::{parse_stream_arn, shard_in_stream, stream_arn};
use extenddb_storage::{StreamEngine, StreamListResult, StreamRecordsResult};
use futures::future::BoxFuture;

use crate::sqlite_util::format_timestamp;
use crate::store::SqliteEngine;

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
            // created_at follows the record's own timestamp (whole seconds here,
            // since that is all the record carries), never a second clock read.
            let created_at = crate::sqlite_util::format_epoch_millis(
                record
                    .dynamodb
                    .approximate_creation_date_time
                    .saturating_mul(1_000),
            );
            sqlx::query(
                "INSERT INTO stream_records (sequence_number, shard_id, table_id, event_name, record_data, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&record.dynamodb.sequence_number)
            .bind(&shard_id)
            .bind(&table_id)
            .bind(format!("{:?}", record.event_name))
            .bind(&record_json)
            .bind(created_at)
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
            // from the table's label and the shard id (`shard_in_stream`),
            // with a second query for a legacy id only: whether the table has
            // a labelled shard.
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
                || self.table_has_labelled_shard(table_id),
            )
            .await?
            {
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
        let limit = input.limit;
        let start = input.exclusive_start_shard_id.clone();
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn)?;
            let (table, ks_json) = self
                .stream_table_row(&account_id, &table_name)
                .await?
                .ok_or_else(|| extenddb_storage::util::stream_not_found(&stream_arn))?;
            let key_schema = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let shards = self
                .stream_shard_rows(&account_id, Some(&table.table_id))
                .await?;
            stream_lineage::describe_stream(
                stream_arn,
                &table,
                key_schema,
                stream_label,
                &shards,
                limit,
                start.as_deref(),
            )
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
            let start = start_arn.as_deref().map(parse_stream_arn).transpose()?;
            let tables = self.stream_table_rows(&account_id).await?;
            let shards = self.stream_shard_rows(&account_id, None).await?;
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
            // D1: every writer holds the engine write lock.
            let _writer = self.write_lock.lock().await;
            let cutoff = format_timestamp(
                time::OffsetDateTime::now_utc() - time::Duration::hours(retention_hours),
            );
            let result = sqlx::query("DELETE FROM stream_records WHERE created_at < ?")
                .bind(&cutoff)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(result.rows_affected())
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

            // Only the open shards: they belong to the table's enabled stream.
            // A disabled stream's closed shards take no new records.
            let shards: Vec<(String,)> = sqlx::query_as(
                "SELECT shard_id FROM stream_shards \
                 WHERE table_id = ? AND ending_sequence_number IS NULL ORDER BY shard_id",
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
            let table = self.stream_table_row(&account_id, &table_name).await?;
            let shards = match &table {
                Some((t, _)) => {
                    self.stream_shard_rows(&account_id, Some(&t.table_id))
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

    fn first_sequence_number_at_or_after(
        &self,
        shard_id: &str,
        at_ms: i64,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        let shard_id = shard_id.to_owned();
        Box::pin(async move {
            self.first_sequence_number_at_or_after_impl(&shard_id, at_ms)
                .await
        })
    }
}

impl SqliteEngine {
    /// Whether table `table_id` has a labelled shard row
    /// (`stream_lineage::labelled_shard_id_range`), which makes its legacy
    /// rows an earlier, closed stream (`util::shard_in_stream`).
    async fn table_has_labelled_shard(&self, table_id: &str) -> Result<bool, StorageError> {
        let (from, to) = stream_lineage::labelled_shard_id_range(table_id);
        let found: i64 = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM stream_shards \
             WHERE table_id = ? AND shard_id >= ? AND shard_id < ?)",
        )
        .bind(table_id)
        .bind(&from)
        .bind(&to)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(found != 0)
    }
}
