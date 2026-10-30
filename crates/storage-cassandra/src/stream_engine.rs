// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `CassandraEngine`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{
    SequenceNumberRange, Shard, StreamDescription, StreamRecord, StreamStatus, StreamSummary,
    StreamViewType,
};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_stream_arn, shard_in_stream, stream_arn};
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
    pub(crate) async fn account_keyspace_for_shard(
        &self,
        shard_id: &str,
    ) -> Result<String, StorageError> {
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

    /// The table id and current stream label of table `table_name` within
    /// `account_id` (a primary-key read of the catalog), or `None` when the
    /// account has no table of that name.
    async fn stream_table_row(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<(String, Option<String>)>, StorageError> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT table_id, stream_label FROM {catalog_keyspace}.tables \
             WHERE account_id = ? AND table_name = ?"
        );
        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(account_id, table_name))
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let rows = result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .into_rows()
            .unwrap_or_default();
        rows.into_iter()
            .next()
            .map(|row| {
                let table_id: String = row
                    .get_r_by_name("table_id")
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let label: Option<String> = row.get_by_name("stream_label").ok().flatten();
                Ok((table_id, label))
            })
            .transpose()
    }

    /// The table id of the stream `stream_arn` names within `account_id`, or
    /// the "stream does not exist" error when no table of that account carries
    /// that name and stream label.
    async fn stream_table_id(
        &self,
        account_id: &str,
        stream_arn: &str,
    ) -> Result<String, StorageError> {
        let (table_name, stream_label) = parse_stream_arn(stream_arn)?;
        match self.stream_table_row(account_id, &table_name).await? {
            Some((table_id, label)) if label.as_deref() == Some(stream_label.as_str()) => {
                Ok(table_id)
            }
            _ => Err(extenddb_storage::util::stream_does_not_exist()),
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
        stream_arn: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, Result<(Vec<StreamRecord>, Option<String>), StorageError>> {
        let account_id = account_id.to_string();
        let stream_arn = stream_arn.to_string();
        let shard_id = shard_id.to_string();
        let after_sequence = after_sequence.map(str::to_string);
        Box::pin(async move {
            // Cassandra has no joins, so the stream resolves in two sequential
            // round trips: the table the ARN names (catalog keyspace, by
            // primary key: its id and current stream label), then the shard
            // row and the records, issued together and awaited together on the
            // account keyspace. The shard is the table's when its row carries
            // that table id, and the stream's when `shard_in_stream` says so
            // from the table's label and the shard id; otherwise (another
            // table, another account, or an earlier stream label on the same
            // table) the answer is the "stream does not exist" error and the
            // records read are discarded. The keyspace follows from the caller's
            // account, so no table id is parsed out of the shard id.
            let (table_name, stream_label) = parse_stream_arn(&stream_arn)?;
            let Some((table_id, table_label)) =
                self.stream_table_row(&account_id, &table_name).await?
            else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            let keyspace = self.account_keyspace_for(&account_id);

            let shard_query =
                format!("SELECT table_id FROM {keyspace}.stream_shards WHERE shard_id = ?");
            let shard_read = self
                .session
                .query_with_values(&shard_query, cdrs_tokio::query_values!(shard_id.as_str()));

            let query = if let Some(ref after) = after_sequence {
                format!(
                    "SELECT record_data FROM {keyspace}.stream_records \
                     WHERE shard_id = ? AND sequence_number > ? \
                     LIMIT {limit}"
                )
            } else {
                format!(
                    "SELECT record_data FROM {keyspace}.stream_records \
                     WHERE shard_id = ? \
                     LIMIT {limit}"
                )
            };
            let records_read = async {
                if let Some(ref after) = after_sequence {
                    self.session
                        .query_with_values(
                            &query,
                            cdrs_tokio::query_values!(shard_id.as_str(), after.as_str()),
                        )
                        .await
                } else {
                    self.session
                        .query_with_values(&query, cdrs_tokio::query_values!(shard_id.as_str()))
                        .await
                }
            };

            let (shard_result, result) = futures::join!(shard_read, records_read);
            let shard_table_id: Option<String> = shard_result
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .response_body()
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_rows()
                .unwrap_or_default()
                .into_iter()
                .next()
                .and_then(|row| row.get_r_by_name("table_id").ok());
            if shard_table_id.as_deref() != Some(table_id.as_str())
                || !shard_in_stream(&table_id, &shard_id, table_label.as_deref(), &stream_label)
            {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }

            let rows = result
                .map_err(|e| StorageError::Internal(e.to_string()))?
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

    #[allow(unused_variables)] // `start` is used in query_with_values below
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
            let catalog_keyspace = self.catalog_keyspace();
            let account_keyspace = self.account_keyspace_for(&account_id);

            // Fetch table metadata from catalog
            let query = format!(
                "SELECT key_schema, stream_specification, table_status, table_id \
                 FROM {catalog_keyspace}.tables \
                 WHERE account_id = ? AND table_name = ? AND stream_label = ? \
                 ALLOW FILTERING"
            );
            let result = self
                .session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(
                        account_id.as_str(),
                        table_name.as_str(),
                        stream_label.as_str()
                    ),
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let rows = result
                .response_body()
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_rows()
                .unwrap_or_default();

            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| extenddb_storage::util::stream_not_found(&stream_arn_str))?;

            let ks_json: String = row
                .get_r_by_name("key_schema")
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let stream_spec_json: Option<String> =
                row.get_by_name("stream_specification").ok().flatten();
            let table_status: String = row
                .get_r_by_name("table_status")
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let table_id: String = row
                .get_r_by_name("table_id")
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let key_schema = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let stream_view_type = stream_spec_json
                .and_then(|s| {
                    let v: serde_json::Value = serde_json::from_str(&s).ok()?;
                    serde_json::from_value(v["StreamViewType"].clone()).ok()
                })
                .unwrap_or(StreamViewType::KeysOnly);

            // Fetch shards from account keyspace via secondary index on table_id
            let limit_val = limit.unwrap_or(100);
            let shard_query = if let Some(ref start) = exclusive_start_shard_id {
                format!(
                    "SELECT shard_id, parent_shard_id, starting_sequence_number, ending_sequence_number \
                     FROM {account_keyspace}.stream_shards \
                     WHERE table_id = ? AND shard_id > ? \
                     LIMIT {} ALLOW FILTERING",
                    limit_val + 1
                )
            } else {
                format!(
                    "SELECT shard_id, parent_shard_id, starting_sequence_number, ending_sequence_number \
                     FROM {account_keyspace}.stream_shards \
                     WHERE table_id = ? \
                     LIMIT {} ALLOW FILTERING",
                    limit_val + 1
                )
            };

            let shard_result = if let Some(ref start) = exclusive_start_shard_id {
                self.session
                    .query_with_values(
                        &shard_query,
                        cdrs_tokio::query_values!(table_id.as_str(), start.as_str()),
                    )
                    .await
            } else {
                self.session
                    .query_with_values(&shard_query, cdrs_tokio::query_values!(table_id.as_str()))
                    .await
            }
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let shard_rows = shard_result
                .response_body()
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_rows()
                .unwrap_or_default();

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit_val as usize;
            let last_shard = if shard_rows.len() > limit_usize {
                let row = &shard_rows[limit_usize - 1];
                let id: String = row
                    .get_r_by_name("shard_id")
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Some(id)
            } else {
                None
            };

            let shards: Vec<Shard> = shard_rows
                .into_iter()
                .take(limit_usize)
                .map(|row| {
                    let shard_id: String = row
                        .get_r_by_name("shard_id")
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let parent_shard_id: Option<String> =
                        row.get_by_name("parent_shard_id").ok().flatten();
                    let starting: String = row
                        .get_r_by_name("starting_sequence_number")
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let ending: Option<String> =
                        row.get_by_name("ending_sequence_number").ok().flatten();
                    Ok(Shard {
                        shard_id,
                        parent_shard_id,
                        sequence_number_range: SequenceNumberRange {
                            starting_sequence_number: starting,
                            ending_sequence_number: ending,
                        },
                    })
                })
                .collect::<Result<_, StorageError>>()?;

            let stream_status = if table_status == "DELETING" {
                StreamStatus::Disabling
            } else {
                StreamStatus::Enabled
            };

            Ok(StreamDescription {
                stream_arn: stream_arn_str,
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
        let table_name = table_name.map(str::to_string);
        let exclusive_start_stream_arn = exclusive_start_stream_arn.map(str::to_string);
        Box::pin(async move {
            let catalog_keyspace = self.catalog_keyspace();

            // Cassandra doesn't support IS NOT NULL filtering efficiently; fetch all and filter.
            // For list_streams the result set is bounded by the number of tables per account.
            let query = format!(
                "SELECT table_name, stream_label FROM {catalog_keyspace}.tables \
                 WHERE account_id = ? ALLOW FILTERING"
            );
            let result = self
                .session
                .query_with_values(&query, cdrs_tokio::query_values!(account_id.as_str()))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            use cdrs_tokio::types::IntoRustByName;
            let rows = result
                .response_body()
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_rows()
                .unwrap_or_default();

            // Parse start cursor
            let start_cursor = exclusive_start_stream_arn
                .as_deref()
                .map(parse_stream_arn)
                .transpose()?;

            let mut summaries: Vec<StreamSummary> = rows
                .into_iter()
                .filter_map(|row| {
                    let tn: String = row.get_r_by_name("table_name").ok()?;
                    let label: Option<String> = row.get_by_name("stream_label").ok().flatten();
                    let label = label?; // skip tables without streams
                    if let Some(ref filter_tn) = table_name
                        && &tn != filter_tn
                    {
                        return None;
                    }
                    Some(StreamSummary {
                        stream_arn: stream_arn(&self.region, &account_id, &tn, &label),
                        stream_label: label,
                        table_name: tn,
                    })
                })
                .collect();

            // Sort for stable pagination (table_name, stream_label)
            summaries.sort_by(|a, b| {
                a.table_name
                    .cmp(&b.table_name)
                    .then(a.stream_label.cmp(&b.stream_label))
            });

            // Apply cursor
            if let Some((start_table, start_label)) = start_cursor {
                summaries
                    .retain(|s| (&s.table_name, &s.stream_label) > (&start_table, &start_label));
            }

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit as usize;
            let last_arn = if summaries.len() > limit_usize {
                summaries.get(limit_usize - 1).map(|s| s.stream_arn.clone())
            } else {
                None
            };
            summaries.truncate(limit_usize);

            Ok((summaries, last_arn))
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
            let table_id = self.stream_table_id(&account_id, &stream_arn).await?;
            let account_keyspace = self.account_keyspace_for(&account_id);

            let shard_query = format!(
                "SELECT shard_id FROM {account_keyspace}.stream_shards \
                 WHERE shard_id = ? AND table_id = ? ALLOW FILTERING"
            );
            let shard_result = self
                .session
                .query_with_values(
                    &shard_query,
                    cdrs_tokio::query_values!(shard_id.as_str(), table_id.as_str()),
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let found = shard_result
                .response_body()
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_rows()
                .is_some_and(|r| !r.is_empty());

            if !found {
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
            let keyspace = self.account_keyspace_for_shard(&shard_id).await?;
            let query = format!(
                "SELECT sequence_number FROM {keyspace}.stream_records \
                 WHERE shard_id = ? ORDER BY sequence_number DESC LIMIT 1"
            );
            let result = self
                .session
                .query_with_values(&query, cdrs_tokio::query_values!(shard_id.as_str()))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let rows = result
                .response_body()
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_rows()
                .unwrap_or_default();

            Ok(rows
                .into_iter()
                .next()
                .map(|row| row.get_r_by_name("sequence_number").unwrap_or_default()))
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
