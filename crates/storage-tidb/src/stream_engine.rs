// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `TidbEngine`.

use extenddb_core::types::{
    SequenceNumberRange, Shard, StreamDescription, StreamRecord, StreamSpecification, StreamStatus,
    StreamSummary, StreamViewType,
};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_stream_arn, stream_arn};
use futures::future::BoxFuture;

use crate::TidbEngine;
use crate::data::{finalize_pending_stream_record_batch_for_shard, next_stream_sequence};

/// Number of fixed shards per stream (hash-based assignment).
///
/// Keep this aligned with the TiDB stream_records Region split points. The
/// bucket is the first varying component in `shard_id` so one hot table can
/// spread stream writes across TiDB Regions instead of concentrating under the
/// table_id prefix.
pub(crate) const SHARDS_PER_STREAM: u32 = 16;
const LEGACY_TABLE_PREFIX_SHARDS_PER_STREAM: u32 = 4;

pub(crate) struct StreamGenerationCatalog<'a> {
    pub account_id: &'a str,
    pub table_name: &'a str,
    pub table_id: &'a str,
    pub stream_label: &'a str,
    pub key_schema: &'a serde_json::Value,
    pub stream_specification: &'a serde_json::Value,
}

pub(crate) fn stream_shard_id(table_id: &str, stream_label: &str, shard_index: u32) -> String {
    format!("shardId-{shard_index:012}-{stream_label}-{table_id}")
}

fn legacy_bucket_prefix_stream_shard_id(table_id: &str, shard_index: u32) -> String {
    format!("shardId-{shard_index:012}-{table_id}")
}

fn legacy_table_prefix_stream_shard_id(table_id: &str, shard_index: u32) -> String {
    format!("shardId-{table_id}-{shard_index:012}")
}

pub(crate) fn stream_shard_index(partition_key: &[u8]) -> u32 {
    crc32fast::hash(partition_key) % SHARDS_PER_STREAM
}

pub(crate) fn stream_shard_id_for_partition_key(
    table_id: &str,
    stream_label: &str,
    partition_key: &[u8],
) -> String {
    stream_shard_id(table_id, stream_label, stream_shard_index(partition_key))
}

const STREAM_FINALIZE_READ_REPAIR_BATCH_SIZE: i64 = 256;

impl TidbEngine {
    pub(crate) fn new_stream_label() -> String {
        time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| time::OffsetDateTime::now_utc().unix_timestamp().to_string())
    }

    pub(crate) async fn upsert_enabled_stream_generation_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        generation: StreamGenerationCatalog<'_>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO stream_generations \
             (account_id, table_name, table_id, stream_label, key_schema, \
              stream_specification, stream_status, disabled_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?, 'ENABLED', NULL, NULL) \
             ON DUPLICATE KEY UPDATE \
              table_id = VALUES(table_id), \
              key_schema = VALUES(key_schema), \
              stream_specification = VALUES(stream_specification), \
              stream_status = 'ENABLED', \
              disabled_at = NULL, \
              expires_at = NULL",
        )
        .bind(generation.account_id)
        .bind(generation.table_name)
        .bind(generation.table_id)
        .bind(generation.stream_label)
        .bind(generation.key_schema)
        .bind(generation.stream_specification)
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    pub(crate) async fn disable_stream_generation_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        generation: StreamGenerationCatalog<'_>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO stream_generations \
             (account_id, table_name, table_id, stream_label, key_schema, \
              stream_specification, stream_status, disabled_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?, 'DISABLED', CURRENT_TIMESTAMP(6), \
                     CURRENT_TIMESTAMP(6) + INTERVAL 24 HOUR) \
             ON DUPLICATE KEY UPDATE \
              table_id = VALUES(table_id), \
              key_schema = VALUES(key_schema), \
              stream_specification = VALUES(stream_specification), \
              stream_status = 'DISABLED', \
              disabled_at = COALESCE(disabled_at, CURRENT_TIMESTAMP(6)), \
              expires_at = COALESCE(expires_at, CURRENT_TIMESTAMP(6) + INTERVAL 24 HOUR)",
        )
        .bind(generation.account_id)
        .bind(generation.table_name)
        .bind(generation.table_id)
        .bind(generation.stream_label)
        .bind(generation.key_schema)
        .bind(generation.stream_specification)
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    pub(crate) fn fixed_stream_shards(table_id: &str, stream_label: &str) -> Vec<Shard> {
        (0..SHARDS_PER_STREAM)
            .map(|index| Shard {
                shard_id: stream_shard_id(table_id, stream_label, index),
                parent_shard_id: None,
                sequence_number_range: SequenceNumberRange {
                    starting_sequence_number: format!("{:027}", 0),
                    ending_sequence_number: None,
                },
            })
            .collect()
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
                "INSERT INTO stream_records \
                 (sequence_number, commit_sequence_number, shard_id, table_id, event_name, record_data) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&record.dynamodb.sequence_number)
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
            finalize_pending_stream_record_batch_for_shard(
                &self.data_pool,
                &shard_id,
                STREAM_FINALIZE_READ_REPAIR_BATCH_SIZE,
            )
            .await?;
            let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new(
                "SELECT record_data FROM stream_records \
                 WHERE shard_id = ",
            );
            query.push_bind(&shard_id);
            query.push(" AND commit_sequence_number IS NOT NULL");
            if let Some(after) = &after_sequence {
                query.push(" AND commit_sequence_number > ");
                query.push_bind(after);
            }
            query.push(" ORDER BY commit_sequence_number LIMIT ");
            query.push_bind(limit);

            let rows: Vec<(serde_json::Value,)> = query
                .build_query_as()
                .fetch_all(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

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

            let row: Option<(serde_json::Value, serde_json::Value, String, String)> =
                sqlx::query_as(
                    "SELECT key_schema, stream_specification, stream_status, table_id \
                     FROM stream_generations \
                     WHERE account_id = ? AND table_name = ? AND stream_label = ? \
                       AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP(6))",
                )
                .bind(&account_id)
                .bind(&table_name)
                .bind(&stream_label)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (ks_json, stream_spec_json, generation_status, table_id) =
                row.ok_or_else(|| {
                    StorageError::TableNotFound(format!(
                        "Requested resource not found: Stream: {arn} not found.",
                        arn = stream_arn
                    ))
                })?;

            let key_schema = serde_json::from_value(ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let stream_specification: StreamSpecification =
                serde_json::from_value(stream_spec_json)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            let stream_view_type = stream_specification
                .stream_view_type
                .unwrap_or(StreamViewType::KeysOnly);

            let limit = limit.unwrap_or(100);
            let all_shards = Self::fixed_stream_shards(&table_id, &stream_label);
            let shard_rows = all_shards
                .into_iter()
                .filter(|shard| {
                    exclusive_start_shard_id
                        .as_ref()
                        .is_none_or(|start| shard.shard_id.as_str() > start.as_str())
                })
                .take(usize::try_from(limit + 1).unwrap_or(usize::MAX))
                .collect::<Vec<_>>();

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit as usize;
            let last_shard = if shard_rows.len() > limit_usize {
                Some(shard_rows[limit_usize - 1].shard_id.clone())
            } else {
                None
            };

            let shards: Vec<Shard> = shard_rows.into_iter().take(limit_usize).collect();

            let stream_status = match generation_status.as_str() {
                "ENABLED" => StreamStatus::Enabled,
                "DISABLED" => StreamStatus::Disabled,
                "DISABLING" => StreamStatus::Disabling,
                other => {
                    return Err(StorageError::Internal(format!(
                        "unknown TiDB stream generation status: {other}"
                    )));
                }
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
            let rows: Vec<(String, String)> =
                match (table_name.as_deref(), exclusive_start_stream_arn.as_deref()) {
                    (Some(tn), Some(start_arn)) => {
                        let (_, start_label) = parse_stream_arn(start_arn)?;
                        sqlx::query_as(
                            "SELECT table_name, stream_label FROM stream_generations \
                         WHERE account_id = ? AND table_name = ? AND stream_label > ? \
                           AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP(6)) \
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
                        "SELECT table_name, stream_label FROM stream_generations \
                         WHERE account_id = ? AND table_name = ? \
                           AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP(6)) \
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
                            "SELECT table_name, stream_label FROM stream_generations \
                         WHERE account_id = ? \
                           AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP(6)) \
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
                        "SELECT table_name, stream_label FROM stream_generations \
                         WHERE account_id = ? \
                           AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP(6)) \
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
                .map(|(tn, label)| StreamSummary {
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
            let row: Option<(String, Option<String>)> = sqlx::query_as(
                "SELECT table_id, stream_label FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let (table_id, stream_label) =
                row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            let stream_label = stream_label.ok_or_else(|| {
                StorageError::Internal(format!(
                    "stream label missing for stream-enabled table {table_name}"
                ))
            })?;

            Ok(stream_shard_id_for_partition_key(
                &table_id,
                &stream_label,
                partition_key.as_bytes(),
            ))
        })
    }

    fn next_sequence_number(&self, shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        let shard_id = shard_id.to_string();
        Box::pin(async move { crate::data::next_stream_sequence(&self.data_pool, &shard_id).await })
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
                "SELECT table_id FROM stream_generations \
                 WHERE account_id = ? AND table_name = ? AND stream_label = ? \
                   AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP(6))",
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

            let shard_belongs_to_stream = (0..SHARDS_PER_STREAM)
                .any(|index| stream_shard_id(&table_id, &stream_label, index) == shard_id)
                || (0..SHARDS_PER_STREAM).any(|index| {
                    legacy_bucket_prefix_stream_shard_id(&table_id, index) == shard_id
                })
                || (0..LEGACY_TABLE_PREFIX_SHARDS_PER_STREAM)
                    .any(|index| legacy_table_prefix_stream_shard_id(&table_id, index) == shard_id);

            if !shard_belongs_to_stream {
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
            // LATEST needs a high-water mark, not stream-row maintenance. TiDB's
            // current TSO is a native cluster-wide marker; future stream commit
            // TSOs will sort after it, while already-committed rows sort before it.
            next_stream_sequence(&self.data_pool, &shard_id)
                .await
                .map(Some)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SHARDS_PER_STREAM, legacy_bucket_prefix_stream_shard_id,
        legacy_table_prefix_stream_shard_id, stream_shard_id, stream_shard_id_for_partition_key,
        stream_shard_index,
    };

    #[test]
    fn stream_shard_ids_put_bucket_before_table_id_for_tidb_region_splits() {
        assert_eq!(
            stream_shard_id("table-1", "stream-a", 3),
            "shardId-000000000003-stream-a-table-1"
        );
        assert_eq!(SHARDS_PER_STREAM, 16);
    }

    #[test]
    fn stream_shard_ids_include_generation_label() {
        let first_generation = stream_shard_id("table-1", "stream-a", 3);
        let second_generation = stream_shard_id("table-1", "stream-b", 3);

        assert_ne!(first_generation, second_generation);
        assert_eq!(
            legacy_bucket_prefix_stream_shard_id("table-1", 3),
            "shardId-000000000003-table-1"
        );
    }

    #[test]
    fn legacy_table_prefix_stream_shard_ids_remain_validatable() {
        assert_eq!(
            legacy_table_prefix_stream_shard_id("table-1", 3),
            "shardId-table-1-000000000003"
        );
    }

    #[test]
    fn stream_shard_assignment_is_deterministic_and_in_range() {
        let first = stream_shard_id_for_partition_key("table-1", "stream-a", b"customer-a");
        let second = stream_shard_id_for_partition_key("table-1", "stream-a", b"customer-a");

        assert_eq!(first, second);
        assert!(stream_shard_index(b"customer-a") < SHARDS_PER_STREAM);
    }
}
