// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `TidbEngine`.

use extenddb_core::types::{
    SequenceNumberRange, Shard, StreamDescription, StreamRecord, StreamStatus, StreamSummary,
    StreamViewType,
};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_stream_arn, stream_arn};
use futures::future::BoxFuture;
use sqlx::MySqlPool;

use crate::TidbEngine;

/// Number of fixed shards per stream (hash-based assignment).
const SHARDS_PER_STREAM: u32 = 4;

impl TidbEngine {
    pub(crate) fn new_stream_label() -> String {
        time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| time::OffsetDateTime::now_utc().unix_timestamp().to_string())
    }

    /// Ensure the catalog stream label exists while the caller holds the table row.
    pub(crate) async fn ensure_stream_label(
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        account_id: &str,
        table_name: &str,
        stream_label: Option<String>,
    ) -> Result<String, StorageError> {
        let label = stream_label.unwrap_or_else(Self::new_stream_label);
        sqlx::query(
            "UPDATE tables SET stream_label = COALESCE(stream_label, ?) \
             WHERE account_id = ? AND table_name = ?",
        )
        .bind(&label)
        .bind(account_id)
        .bind(table_name)
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(label)
    }

    /// Idempotently create stream shards for a stream-enabled table.
    pub(crate) async fn ensure_stream_shard_rows(
        data_pool: &MySqlPool,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let mut data_tx = data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for i in 0..SHARDS_PER_STREAM {
            let shard_id = format!("shardId-{table_id}-{i:012}");
            let start_seq = format!("{:021}", 0);
            sqlx::query(
                "INSERT IGNORE INTO stream_shards \
                 (shard_id, table_id, starting_sequence_number) \
                 VALUES (?, ?, ?)",
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

        Ok(())
    }
}

impl StreamEngine for TidbEngine {
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
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query(
                "INSERT INTO stream_records (sequence_number, shard_id, table_id, event_name, record_data) \
                 VALUES (?, ?, ?, ?, ?)",
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
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, Result<(Vec<StreamRecord>, Option<String>), StorageError>> {
        let shard_id = shard_id.to_string();
        let after_sequence = after_sequence.map(|s| s.to_string());
        Box::pin(async move {
            let rows: Vec<(serde_json::Value,)> = if let Some(after) = after_sequence {
                sqlx::query_as(
                    "SELECT record_data FROM stream_records \
                     WHERE shard_id = ? AND sequence_number > ? \
                     ORDER BY sequence_number LIMIT ?",
                )
                .bind(&shard_id)
                .bind(&after)
                .bind(limit)
                .fetch_all(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            } else {
                sqlx::query_as(
                    "SELECT record_data FROM stream_records \
                     WHERE shard_id = ? \
                     ORDER BY sequence_number LIMIT ?",
                )
                .bind(&shard_id)
                .bind(limit)
                .fetch_all(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            };

            let records: Vec<StreamRecord> = rows
                .into_iter()
                .map(|(data,)| {
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
                     FROM tables WHERE account_id = ? AND table_name = ? AND stream_label = ?",
                )
                .bind(&account_id)
                .bind(&table_name)
                .bind(&stream_label)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (ks_json, _ad_json, stream_spec_json, table_status, table_id) =
                row.ok_or_else(|| {
                    StorageError::TableNotFound(format!(
                        "Requested resource not found: Stream: {arn} not found.",
                        arn = stream_arn
                    ))
                })?;

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
                         FROM stream_shards WHERE table_id = ? AND shard_id > ? \
                         ORDER BY shard_id LIMIT ?",
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
                         FROM stream_shards WHERE table_id = ? \
                         ORDER BY shard_id LIMIT ?",
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
        let table_name = table_name.map(|s| s.to_string());
        let exclusive_start_stream_arn = exclusive_start_stream_arn.map(|s| s.to_string());
        Box::pin(async move {
            let rows: Vec<(String, String, String)> = match (
                table_name.as_deref(),
                exclusive_start_stream_arn.as_deref(),
            ) {
                (Some(tn), Some(start_arn)) => {
                    let (_, start_label) = parse_stream_arn(start_arn)?;
                    sqlx::query_as(
                        "SELECT table_name, table_arn, stream_label FROM tables \
                         WHERE account_id = ? AND stream_label IS NOT NULL AND table_name = ? AND stream_label > ? \
                         ORDER BY stream_label LIMIT ?",
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
                         WHERE account_id = ? AND stream_label IS NOT NULL AND table_name = ? \
                         ORDER BY stream_label LIMIT ?",
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
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                }
                (None, None) => sqlx::query_as(
                    "SELECT table_name, table_arn, stream_label FROM tables \
                         WHERE account_id = ? AND stream_label IS NOT NULL \
                         ORDER BY table_name, stream_label LIMIT ?",
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
        _retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        // TiDB native TTL owns stream retention; no duplicate manual delete path.
        Box::pin(async move { Ok(0) })
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
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let shards: Vec<(String,)> = sqlx::query_as(
                "SELECT shard_id FROM stream_shards \
                 WHERE table_id = ? \
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

    fn next_sequence_number(&self, shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        let shard_id = shard_id.to_string();
        Box::pin(async move {
            let mut tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let sequence_number =
                crate::data::next_shard_sequence_in_tx(&mut tx, &shard_id).await?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(sequence_number)
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
                 WHERE account_id = ? AND table_name = ? AND stream_label = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .bind(&stream_label)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let Some(table_id) = table_id else {
                return Err(StorageError::TableNotFound(format!(
                    "Requested resource not found: Stream: {stream_arn} not found."
                )));
            };

            let exists: Option<(i32,)> =
                sqlx::query_as("SELECT 1 FROM stream_shards WHERE shard_id = ? AND table_id = ?")
                    .bind(&shard_id)
                    .bind(&table_id)
                    .fetch_optional(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

            if exists.is_none() {
                return Err(StorageError::TableNotFound(format!(
                    "Requested resource not found: Stream: {stream_arn} not found."
                )));
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
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT sequence_number FROM stream_records \
                 WHERE shard_id = ? ORDER BY sequence_number DESC LIMIT 1",
            )
            .bind(&shard_id)
            .fetch_optional(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(row.map(|(s,)| s))
        })
    }
}
