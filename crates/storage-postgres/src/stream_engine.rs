// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `PostgresEngine`.

use extenddb_core::types::{StreamDescription, StreamRecord, StreamSummary};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage;
use extenddb_storage::util::{parse_stream_arn, shard_in_stream, stream_arn};
use futures::future::BoxFuture;

use crate::PostgresEngine;
use crate::stream_records_sql::{STREAM_RECORDS_AFTER, STREAM_RECORDS_FROM_START};

impl PostgresEngine {
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
        let candidates: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT s.table_id FROM stream_shards s \
             WHERE s.created_at < NOW() - make_interval(hours => $1::integer) \
               AND NOT EXISTS (SELECT 1 FROM stream_records r WHERE r.table_id = s.table_id)",
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
               AND NOT EXISTS (SELECT 1 FROM stream_records r WHERE r.table_id = s.table_id)",
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
        account_id: &str,
        record: &StreamRecord,
        shard_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let record = record.clone();
        let shard_id = shard_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let record_json =
                serde_json::to_value(&record).map_err(|e| StorageError::Internal(e.to_string()))?;

            let table_id: String = sqlx::query_scalar(
                "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query(
                "INSERT INTO stream_records (sequence_number, shard_id, table_id, event_name, record_data) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(&record.dynamodb.sequence_number)
            .bind(&shard_id)
            .bind(&table_id)
            .bind(format!("{:?}", record.event_name))
            .bind(&record_json)
            .execute(&self.data_pool)
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
        Box::pin(async move {
            let (seq_val,): (i64,) = sqlx::query_as("SELECT nextval('stream_seq')")
                .fetch_one(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(format!("{seq_val:021}"))
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
