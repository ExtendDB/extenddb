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
use extenddb_storage::util::{parse_stream_arn, stream_arn};
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
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        let account_id = account_id.to_owned();
        let shard_id = shard_id.to_owned();
        let after = after_sequence.map(str::to_owned);
        Box::pin(async move {
            // Ownership guard: only return records if the shard's backing table
            // belongs to the calling account. SQLite keeps shards and the table
            // catalog in the same database, so ownership resolves in one join.
            // A shard iterator resolves to a shard the caller does not own or
            // one that does not exist. Real DynamoDB returns
            // `ValidationException: Invalid ShardIterator` for a GetRecords
            // iterator it did not issue, and does NOT distinguish "exists but
            // not yours" from "does not exist" — so neither do we (both
            // collapse here). Verified against DynamoDB Streams (us-east-1).
            let owned: Option<(i32,)> = sqlx::query_as(
                "SELECT 1 FROM stream_shards s \
                 JOIN tables t ON t.table_id = s.table_id \
                 WHERE s.shard_id = ? AND t.account_id = ?",
            )
            .bind(&shard_id)
            .bind(&account_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if owned.is_none() {
                return Err(StorageError::Validation("Invalid ShardIterator".to_owned()));
            }

            let rows: Vec<(String,)> = if let Some(after) = after {
                sqlx::query_as(
                    "SELECT record_data FROM stream_records \
                     WHERE shard_id = ? AND sequence_number > ? ORDER BY sequence_number LIMIT ?",
                )
                .bind(&shard_id)
                .bind(&after)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            } else {
                sqlx::query_as(
                    "SELECT record_data FROM stream_records \
                     WHERE shard_id = ? ORDER BY sequence_number LIMIT ?",
                )
                .bind(&shard_id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let records: Vec<StreamRecord> = rows
                .into_iter()
                .map(|(d,)| {
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
                .ok_or_else(|| stream_lineage::stream_not_found(&stream_arn))?;
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
}
