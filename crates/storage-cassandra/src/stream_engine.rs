// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `CassandraEngine`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{StreamDescription, StreamRecord, StreamSummary};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage;
use extenddb_storage::util::{parse_stream_arn, stream_arn};
use futures::future::BoxFuture;

use crate::CassandraEngine;

impl CassandraEngine {
    fn account_keyspace_for(&self, account_id: &str) -> String {
        self.account_keyspace(account_id)
    }

    /// Resolve the account keyspace for a shard by looking up the `table_id`
    /// (embedded in the shard ID) via the secondary index on `tables.table_id`.
    ///
    /// Shard ID format: `shardId-{table_id}-{index:012}`
    async fn account_keyspace_for_shard(&self, shard_id: &str) -> Result<String, StorageError> {
        // Strip "shardId-" prefix and trailing "-{12 digits}" to get table_id.
        let without_prefix = shard_id.strip_prefix("shardId-").ok_or_else(|| {
            StorageError::Internal(format!("Invalid shard_id format: {shard_id}"))
        })?;
        let table_id = without_prefix
            .rsplit_once('-')
            .map(|x| x.0)
            .ok_or_else(|| {
                StorageError::Internal(format!("Invalid shard_id format: {shard_id}"))
            })?;

        let catalog_keyspace = self.catalog_keyspace();
        let query = format!("SELECT account_id FROM {catalog_keyspace}.tables WHERE table_id = ?");
        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(table_id))
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let account_id: String = result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .into_rows()
            .unwrap_or_default()
            .into_iter()
            .next()
            .ok_or_else(|| {
                StorageError::TableNotFound(format!("No table found for shard {shard_id}"))
            })?
            .get_r_by_name("account_id")
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(self.account_keyspace(&account_id))
    }

    /// Like `account_keyspace_for_shard`, but also enforces that the shard's
    /// table belongs to `caller_account_id`. Returns `Validation("Invalid
    /// ShardIterator")` for both "shard not found" and "shard belongs to a
    /// different account" — no information leakage about other accounts.
    async fn account_keyspace_for_shard_owned_by(
        &self,
        shard_id: &str,
        caller_account_id: &str,
    ) -> Result<String, StorageError> {
        let without_prefix = shard_id
            .strip_prefix("shardId-")
            .ok_or_else(|| StorageError::Validation("Invalid ShardIterator".to_owned()))?;
        let table_id = without_prefix
            .rsplit_once('-')
            .map(|x| x.0)
            .ok_or_else(|| StorageError::Validation("Invalid ShardIterator".to_owned()))?;

        let catalog_keyspace = self.catalog_keyspace();
        let query = format!("SELECT account_id FROM {catalog_keyspace}.tables WHERE table_id = ?");
        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(table_id))
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let table_account_id: Option<String> = result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .into_rows()
            .unwrap_or_default()
            .into_iter()
            .next()
            .and_then(|row| row.get_r_by_name("account_id").ok());

        // Collapse "not found" and "wrong account" into the same error.
        match table_account_id {
            Some(ref owner) if owner == caller_account_id => Ok(self.account_keyspace(owner)),
            _ => Err(StorageError::Validation("Invalid ShardIterator".to_owned())),
        }
    }
}

impl StreamEngine for CassandraEngine {
    /// Not used for atomic writes — see ADR-0008. Stream records are injected
    /// directly into LOGGED `BATCHes` via `stream_record_statement` in each write path.
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            Err(StorageError::Internal(
                "write_stream_record is not used in the Cassandra backend; \
                 stream records are written atomically via LOGGED BATCH in each write path"
                    .to_owned(),
            ))
        })
    }

    #[allow(unused_variables)] // `after` and `start` are used in query_with_values below
    fn get_stream_records(
        &self,
        account_id: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, Result<(Vec<StreamRecord>, Option<String>), StorageError>> {
        let account_id = account_id.to_string();
        let shard_id = shard_id.to_string();
        let after_sequence = after_sequence.map(str::to_string);
        Box::pin(async move {
            // Ownership guard: resolve the shard's owning account from the
            // catalog and reject the request if it doesn't match the caller.
            // Mirrors the PostgreSQL two-step check. Both "shard not found" and
            // "shard belongs to a different account" collapse to the same
            // `Invalid ShardIterator` error — consistent with real DynamoDB
            // Streams behaviour (no information leakage about other accounts).
            let keyspace = self
                .account_keyspace_for_shard_owned_by(&shard_id, &account_id)
                .await?;
            // A shard's records are the range of its index's partition after
            // its starting number and, once closed, up to its ending number
            // (`stream_lifecycle`).
            let range = self.shard_range(&keyspace, &shard_id).await?;
            let after = match after_sequence {
                Some(a) if a > range.after => a,
                _ => range.after.clone(),
            };
            let bound = if range.until.is_some() {
                " AND sequence_number <= ?"
            } else {
                ""
            };
            let query = format!(
                "SELECT record_data FROM {keyspace}.stream_records \
                 WHERE shard_id = ? AND sequence_number > ?{bound} \
                 LIMIT {limit}"
            );
            let result = match &range.until {
                Some(until) => {
                    self.session
                        .query_with_values(
                            &query,
                            cdrs_tokio::query_values!(
                                range.physical.as_str(),
                                after.as_str(),
                                until.as_str()
                            ),
                        )
                        .await
                }
                None => {
                    self.session
                        .query_with_values(
                            &query,
                            cdrs_tokio::query_values!(range.physical.as_str(), after.as_str()),
                        )
                        .await
                }
            }
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let rows = result
                .response_body()
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_rows()
                .unwrap_or_default();

            let records: Vec<StreamRecord> = rows
                .into_iter()
                .map(|row| {
                    let data: String = row
                        .get_r_by_name("record_data")
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    serde_json::from_str(&data).map_err(|e| StorageError::Internal(e.to_string()))
                })
                .collect::<Result<_, _>>()?;

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
        let stream_arn_str = input.stream_arn.clone();
        let limit = input.limit;
        let exclusive_start_shard_id = input.exclusive_start_shard_id.clone();
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn_str)?;
            let (table, ks_json) = self
                .stream_table_row(&account_id, &table_name)
                .await?
                .ok_or_else(|| stream_lineage::stream_not_found(&stream_arn_str))?;
            let key_schema = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let account_keyspace = self.account_keyspace_for(&account_id);
            let shards = self
                .stream_shard_rows(&account_keyspace, Some(&table.table_id))
                .await?;
            stream_lineage::describe_stream(
                stream_arn_str,
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
        let table_name = table_name.map(str::to_string);
        let exclusive_start_stream_arn = exclusive_start_stream_arn.map(str::to_string);
        Box::pin(async move {
            let start = exclusive_start_stream_arn
                .as_deref()
                .map(parse_stream_arn)
                .transpose()?;
            let tables = self.stream_table_rows(&account_id).await?;
            let account_keyspace = self.account_keyspace_for(&account_id);
            let shards = self.stream_shard_rows(&account_keyspace, None).await?;
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

    /// No-op: Cassandra native TTL handles stream record expiry automatically.
    fn cleanup_expired_stream_records(
        &self,
        _retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        Box::pin(async move { Ok(0) })
    }

    fn assign_shard(
        &self,
        _account_id: &str,
        _table_name: &str,
        partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        // Pure computation — table_id is not available here, but the trait is used
        // by the engine layer which resolves table_id before calling write paths.
        // The write paths use stream_record_statement directly with table_id.
        // This method is provided for completeness; callers should prefer the write-path injection.
        let partition_key = partition_key.to_string();
        Box::pin(async move {
            // Without table_id we cannot produce a fully-qualified shard_id.
            // Return an error directing callers to use the write-path injection instead.
            Err(StorageError::Internal(format!(
                "assign_shard requires table_id; use stream_record_statement directly. pk={partition_key}"
            )))
        })
    }

    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        let seq = self
            .hlc
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generate();
        Box::pin(async move { Ok(seq) })
    }

    /// Override the stream sequence number fixed width to 23 for the HLC format:
    ///   13-digit ms timestamp + 6-digit counter + 4-digit node ID.
    fn sequence_number_width(&self) -> usize {
        23
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
                    let account_keyspace = self.account_keyspace_for(&account_id);
                    self.stream_shard_rows(&account_keyspace, Some(&t.table_id))
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
            let keyspace = self.account_keyspace_for_shard(&shard_id).await?;
            let range = self.shard_range(&keyspace, &shard_id).await?;
            self.last_sequence_in(&keyspace, &range).await
        })
    }
}
