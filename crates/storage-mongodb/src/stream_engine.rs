// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` trait implementation for `MongoEngine`.
//!
//! `DynamoDB` Streams are implemented using `MongoDB`'s own stream record storage.
//! Stream records are written to a `stream_records` collection in the data database,
//! grouped by shard. Shards are stored in `stream_shards` in the data database.
//! This approach uses the same storage model as the `PostgreSQL` backend rather than
//! `MongoDB` Change Streams, to maintain behavioral parity (explicit sequence numbers,
//! shard assignment, retention cleanup).

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::DateTime as BsonDateTime;
use mongodb::bson::{self, Document, doc};
use mongodb::options::FindOptions;

use extenddb_core::types::{
    DescribeStreamInput, SequenceNumberRange, Shard, StreamDescription, StreamEventName,
    StreamRecord, StreamStatus, StreamSummary, StreamViewType,
};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_stream_arn, stream_arn};
use extenddb_storage::{StreamListResult, StreamRecordsResult};

use crate::MongoEngine;

const SHARDS_PER_STREAM: u32 = 4;

/// Map a `StreamEventName` to its DynamoDB wire-format string.
///
/// DynamoDB Streams records use uppercase event names (`INSERT`, `MODIFY`, `REMOVE`).
/// The enum's `Debug` output is Rust-cased (`Insert`, ...) so we must not use that.
pub(crate) fn event_name_ddb_str(name: StreamEventName) -> &'static str {
    match name {
        StreamEventName::Insert => "INSERT",
        StreamEventName::Modify => "MODIFY",
        StreamEventName::Remove => "REMOVE",
    }
}

/// Build the mongo `stream_shards.shard_id` for a given table_id + shard index.
///
/// **Security invariant (RFC-0003 §5.3, §8.2):** shard_id must incorporate
/// the table's globally-unique `table_id` (a UUID), not the caller-visible
/// `table_name`. Table names are only unique per-account, so two accounts
/// creating "orders" tables would generate colliding shard_ids under a
/// name-derived scheme, letting one account's `GetRecords(shard_id)` read
/// the other's stream records. `table_id` is a per-table-instance UUID that
/// resets on `DeleteTable + CreateTable` — the recreated table gets fresh
/// shard_ids, so leftover stream records from the deleted table never
/// resurface either.
///
/// UUIDs are not guessable, so an attacker cannot synthesize a shard_id
/// belonging to another tenant without first observing it (which itself
/// requires an authenticated path scoped to that tenant's account).
pub(crate) fn build_shard_id(table_id: &str, shard_index: u32) -> String {
    format!("shardId-{table_id}-{shard_index:012}")
}

impl MongoEngine {
    /// Initialize stream shards for a table. Only creates shard documents;
    /// the caller is responsible for setting `stream_label` on the table doc.
    ///
    /// Uses the table's UUID (`table_id`), not `table_name`, in the shard_id
    /// — see `build_shard_id` for the security rationale.
    pub(crate) async fn init_stream_shards(&self, table_id: &str) -> Result<(), StorageError> {
        let shards_coll = self.data_db.collection::<Document>("stream_shards");
        for i in 0..SHARDS_PER_STREAM {
            let shard_id = build_shard_id(table_id, i);
            let start_seq = format!("{:021}", 0);
            shards_coll
                .insert_one(doc! {
                    "shard_id": &shard_id,
                    "table_id": table_id,
                    "starting_sequence_number": &start_seq,
                    "created_at": BsonDateTime::now(),
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    /// Draw the next sequence number for a shard *inside* the given
    /// transaction session.
    ///
    /// Sequence assignment must participate in the same transaction as the
    /// stream-record insert, otherwise a fast writer B can obtain seq=6
    /// and commit before a slow writer A (which obtained seq=5) commits.
    /// A consumer polling at `after_sequence_number=cursor` between B's
    /// commit and A's commit sees seq=6 and advances past it; when A
    /// finally commits, seq=5 lands behind the cursor and is never
    /// returned. RFC-0003 §5.1 (atomicity with data writes) and §5.2
    /// (per-shard ordering).
    ///
    /// Placing the counter increment inside the session also serializes
    /// concurrent writers on the same shard: two writes racing to
    /// $inc the same counter under snapshot isolation will conflict at
    /// commit time, so the loser retries — the transaction retry loop
    /// upstream in the caller (see D-C3 followup) handles this.
    pub(crate) async fn next_sequence_number_in_session(
        &self,
        shard_id: &str,
        session: &mut mongodb::ClientSession,
    ) -> Result<String, StorageError> {
        let counters_coll = self.data_db.collection::<Document>("counters");
        let opts = mongodb::options::FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(mongodb::options::ReturnDocument::After)
            .build();
        let counter_id = format!("stream_seq:{shard_id}");
        let doc = counters_coll
            .find_one_and_update(
                doc! { "_id": counter_id },
                doc! { "$inc": { "value": 1_i64 } },
            )
            .with_options(opts)
            .session(&mut *session)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| {
                StorageError::Internal("Failed to generate sequence number".to_owned())
            })?;

        let seq_val = doc.get_i64("value").unwrap_or(1);
        Ok(format!("{seq_val:021}"))
    }

    /// Resolve the shard_id for a given (account, table, partition-key)
    /// *inside* the given transaction session. Pairs with
    /// `next_sequence_number_in_session` so the shard set the write is
    /// routed to is read at the same snapshot as the sequence draw.
    pub(crate) async fn assign_shard_in_session(
        &self,
        account_id: &str,
        table_name: &str,
        partition_key: &str,
        session: &mut mongodb::ClientSession,
    ) -> Result<String, StorageError> {
        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let table_doc = tables_coll
            .find_one(doc! { "_id": { "account_id": account_id, "table_name": table_name } })
            .session(&mut *session)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::Internal(format!("Table {table_name} not found")))?;
        let table_id = table_doc.get_str("table_id").unwrap_or_default();

        let shards_coll = self.data_db.collection::<Document>("stream_shards");
        let opts = FindOptions::builder().sort(doc! { "shard_id": 1 }).build();
        let mut cursor = shards_coll
            .find(doc! { "table_id": table_id })
            .with_options(opts)
            .session(&mut *session)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let shard_docs: Vec<Document> = cursor
            .stream(&mut *session)
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if shard_docs.is_empty() {
            return Err(StorageError::Internal(format!(
                "No stream shards for table {table_name}"
            )));
        }

        let shard_ids: Vec<&str> = shard_docs
            .iter()
            .filter_map(|d| d.get_str("shard_id").ok())
            .collect();

        let hash = crc32fast::hash(partition_key.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let idx = (hash as usize) % shard_ids.len();
        Ok(shard_ids[idx].to_owned())
    }

    /// Delete every stream_shards document for a given table_id, and every
    /// stream_records document written to any of its shards. Invoked from
    /// `delete_table_impl` so that a table recreated with the same name
    /// (which will get a fresh table_id) cannot inherit the deleted table's
    /// stream history.
    pub(crate) async fn cleanup_stream_state_for_table(
        &self,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let shards_coll = self.data_db.collection::<Document>("stream_shards");
        let records_coll = self.data_db.collection::<Document>("stream_records");

        // Collect shard_ids for this table so we can delete their records.
        // Records don't carry table_id directly — they're addressed by shard_id.
        let cursor = shards_coll
            .find(doc! { "table_id": table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let shard_docs: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let shard_ids: Vec<String> = shard_docs
            .iter()
            .filter_map(|d| d.get_str("shard_id").ok().map(str::to_owned))
            .collect();

        if !shard_ids.is_empty() {
            records_coll
                .delete_many(doc! { "shard_id": { "$in": &shard_ids } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        shards_coll
            .delete_many(doc! { "table_id": table_id })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Sequence-number counters are keyed as "stream_seq:<shard_id>".
        let counters_coll = self.data_db.collection::<Document>("counters");
        let counter_ids: Vec<String> = shard_ids
            .iter()
            .map(|sid| format!("stream_seq:{sid}"))
            .collect();
        if !counter_ids.is_empty() {
            counters_coll
                .delete_many(doc! { "_id": { "$in": &counter_ids } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(())
    }
}

impl StreamEngine for MongoEngine {
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
            let record_json =
                serde_json::to_value(&record).map_err(|e| StorageError::Internal(e.to_string()))?;
            let record_bson =
                bson::to_bson(&record_json).map_err(|e| StorageError::Internal(e.to_string()))?;

            // Look up table_id
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let table_doc = tables_coll
                .find_one(doc! { "_id": { "account_id": &account_id, "table_name": &table_name } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::Internal(format!("Table {table_name} not found in catalog"))
                })?;
            let table_id = table_doc.get_str("table_id").unwrap_or_default();

            let records_coll = self.data_db.collection::<Document>("stream_records");
            records_coll
                .insert_one(doc! {
                    "sequence_number": &record.dynamodb.sequence_number,
                    "shard_id": &shard_id,
                    "table_id": table_id,
                    "event_name": event_name_ddb_str(record.event_name),
                    "record_data": record_bson,
                    "created_at": BsonDateTime::now(),
                })
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
    ) -> BoxFuture<'_, StreamRecordsResult> {
        let shard_id = shard_id.to_owned();
        let after_sequence = after_sequence.map(std::borrow::ToOwned::to_owned);
        Box::pin(async move {
            let records_coll = self.data_db.collection::<Document>("stream_records");

            let filter = if let Some(ref after) = after_sequence {
                doc! {
                    "shard_id": &shard_id,
                    "sequence_number": { "$gt": after },
                }
            } else {
                doc! { "shard_id": &shard_id }
            };

            let opts = FindOptions::builder()
                .sort(doc! { "sequence_number": 1 })
                .limit(limit)
                .build();

            let cursor = records_coll
                .find(filter)
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let docs: Vec<Document> = cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let records: Vec<StreamRecord> = docs
                .into_iter()
                .map(|d| {
                    let record_bson = d
                        .get("record_data")
                        .ok_or_else(|| StorageError::Internal("Missing record_data".to_owned()))?;
                    let json_val: serde_json::Value = bson::from_bson(record_bson.clone())
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    serde_json::from_value(json_val)
                        .map_err(|e| StorageError::Internal(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let last_seq = records.last().map(|r| r.dynamodb.sequence_number.clone());
            Ok((records, last_seq))
        })
    }

    fn describe_stream(
        &self,
        account_id: &str,
        input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let stream_arn_val = input.stream_arn.clone();
        let limit = input.limit;
        let exclusive_start_shard_id = input.exclusive_start_shard_id.clone();
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn_val)?;

            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let table_doc = tables_coll
                .find_one(doc! {
                    "_id": { "account_id": &account_id, "table_name": &table_name },
                    "stream_label": &stream_label,
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::TableNotFound(format!(
                        "Requested resource not found: Stream: {stream_arn_val} not found."
                    ))
                })?;

            let key_schema = table_doc
                .get("key_schema")
                .and_then(|b| bson::from_bson(b.clone()).ok())
                .ok_or_else(|| StorageError::Internal("Missing key_schema".to_owned()))?;

            let stream_view_type = table_doc
                .get("stream_specification")
                .and_then(|b| {
                    let json: serde_json::Value = bson::from_bson(b.clone()).ok()?;
                    json.get("StreamViewType")
                        .and_then(|sv| serde_json::from_value::<StreamViewType>(sv.clone()).ok())
                })
                .unwrap_or(StreamViewType::KeysOnly);

            let table_status = table_doc.get_str("table_status").unwrap_or("ACTIVE");
            let table_id = table_doc.get_str("table_id").unwrap_or_default();

            let limit = limit.unwrap_or(100);
            let shards_coll = self.data_db.collection::<Document>("stream_shards");

            let filter = if let Some(ref start) = exclusive_start_shard_id {
                doc! {
                    "table_id": table_id,
                    "shard_id": { "$gt": start },
                }
            } else {
                doc! { "table_id": table_id }
            };

            let opts = FindOptions::builder()
                .sort(doc! { "shard_id": 1 })
                .limit(limit + 1)
                .build();

            let cursor = shards_coll
                .find(filter)
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let shard_docs: Vec<Document> = cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit as usize;
            let last_shard = if shard_docs.len() > limit_usize {
                shard_docs.get(limit_usize - 1).and_then(|d| {
                    d.get_str("shard_id")
                        .ok()
                        .map(std::borrow::ToOwned::to_owned)
                })
            } else {
                None
            };

            let shards: Vec<Shard> = shard_docs
                .into_iter()
                .take(limit_usize)
                .filter_map(|d| {
                    Some(Shard {
                        shard_id: d.get_str("shard_id").ok()?.to_owned(),
                        parent_shard_id: d
                            .get_str("parent_shard_id")
                            .ok()
                            .map(std::borrow::ToOwned::to_owned),
                        sequence_number_range: SequenceNumberRange {
                            starting_sequence_number: d
                                .get_str("starting_sequence_number")
                                .ok()?
                                .to_owned(),
                            ending_sequence_number: d
                                .get_str("ending_sequence_number")
                                .ok()
                                .map(std::borrow::ToOwned::to_owned),
                        },
                    })
                })
                .collect();

            let stream_status = if table_status == "DELETING" {
                StreamStatus::Disabling
            } else {
                StreamStatus::Enabled
            };

            Ok(StreamDescription {
                stream_arn: stream_arn_val,
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
        let table_name = table_name.map(std::borrow::ToOwned::to_owned);
        let exclusive_start_stream_arn =
            exclusive_start_stream_arn.map(std::borrow::ToOwned::to_owned);
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");

            let mut filter = doc! {
                "_id.account_id": &account_id,
                "stream_label": { "$ne": null },
            };

            if let Some(ref tn) = table_name {
                filter.insert("_id.table_name", tn.as_str());
            }

            if let Some(ref start_arn) = exclusive_start_stream_arn {
                let (start_table, start_label) = parse_stream_arn(start_arn)?;
                if table_name.is_some() {
                    filter.insert("stream_label", doc! { "$gt": &start_label });
                } else {
                    filter.insert(
                        "$or",
                        bson::bson!([
                            { "_id.table_name": { "$gt": &start_table } },
                            { "_id.table_name": &start_table, "stream_label": { "$gt": &start_label } }
                        ]),
                    );
                }
            }

            let opts = FindOptions::builder()
                .sort(doc! { "_id.table_name": 1, "stream_label": 1 })
                .limit(limit + 1)
                .build();

            let cursor = tables_coll
                .find(filter)
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let docs: Vec<Document> = cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            #[allow(clippy::cast_sign_loss)]
            let limit_usize = limit as usize;

            let summaries: Vec<StreamSummary> = docs
                .iter()
                .take(limit_usize)
                .filter_map(|d| {
                    let id = d.get_document("_id").ok()?;
                    let tn = id.get_str("table_name").ok()?;
                    let label = d.get_str("stream_label").ok()?;
                    Some(StreamSummary {
                        stream_arn: stream_arn(&self.region, &account_id, tn, label),
                        stream_label: label.to_owned(),
                        table_name: tn.to_owned(),
                    })
                })
                .collect();

            let last_arn = if docs.len() > limit_usize {
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
            let records_coll = self.data_db.collection::<Document>("stream_records");
            let cutoff = time::OffsetDateTime::now_utc()
                - std::time::Duration::from_secs(retention_hours as u64 * 3600);
            let cutoff_bson = BsonDateTime::from_millis(cutoff.unix_timestamp() * 1000);
            let result = records_coll
                .delete_many(doc! { "created_at": { "$lt": cutoff_bson } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(result.deleted_count)
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
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let table_doc = tables_coll
                .find_one(doc! { "_id": { "account_id": &account_id, "table_name": &table_name } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| StorageError::Internal(format!("Table {table_name} not found")))?;
            let table_id = table_doc.get_str("table_id").unwrap_or_default();

            let shards_coll = self.data_db.collection::<Document>("stream_shards");
            let opts = FindOptions::builder().sort(doc! { "shard_id": 1 }).build();
            let cursor = shards_coll
                .find(doc! { "table_id": table_id })
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let shard_docs: Vec<Document> = cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if shard_docs.is_empty() {
                return Err(StorageError::Internal(format!(
                    "No stream shards for table {table_name}"
                )));
            }

            let shard_ids: Vec<&str> = shard_docs
                .iter()
                .filter_map(|d| d.get_str("shard_id").ok())
                .collect();

            let hash = crc32fast::hash(partition_key.as_bytes());
            #[allow(clippy::cast_possible_truncation)]
            let idx = (hash as usize) % shard_ids.len();
            Ok(shard_ids[idx].to_owned())
        })
    }

    fn next_sequence_number(&self, shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        let shard_id = shard_id.to_owned();
        Box::pin(async move {
            // Per-shard atomic counter. DynamoDB Streams' contract is that
            // sequence numbers are strictly monotonic *within a shard* and
            // independent *across shards*. A single global counter would
            // couple the sequence spaces of unrelated shards — a writer
            // pushing records into shard B would advance the counter shard A
            // reads back, producing non-contiguous sequence numbers on
            // shard A's GetRecords pages. Keying the counter document by
            // shard_id preserves the per-shard monotonicity guarantee.
            let counters_coll = self.data_db.collection::<Document>("counters");
            let opts = mongodb::options::FindOneAndUpdateOptions::builder()
                .upsert(true)
                .return_document(mongodb::options::ReturnDocument::After)
                .build();
            let counter_id = format!("stream_seq:{shard_id}");
            let doc = counters_coll
                .find_one_and_update(
                    doc! { "_id": counter_id },
                    doc! { "$inc": { "value": 1_i64 } },
                )
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::Internal("Failed to generate sequence number".to_owned())
                })?;

            let seq_val = doc.get_i64("value").unwrap_or(1);
            Ok(format!("{seq_val:021}"))
        })
    }

    fn validate_shard(
        &self,
        account_id: &str,
        stream_arn_val: &str,
        shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let stream_arn_val = stream_arn_val.to_owned();
        let shard_id = shard_id.to_owned();
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn_val)?;

            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let table_doc = tables_coll
                .find_one(doc! {
                    "_id": { "account_id": &account_id, "table_name": &table_name },
                    "stream_label": &stream_label,
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let Some(table_doc) = table_doc else {
                return Err(StorageError::TableNotFound(format!(
                    "Requested resource not found: Stream: {stream_arn_val} not found."
                )));
            };

            let table_id = table_doc.get_str("table_id").unwrap_or_default();

            let shards_coll = self.data_db.collection::<Document>("stream_shards");
            let exists = shards_coll
                .find_one(doc! { "shard_id": &shard_id, "table_id": table_id })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if exists.is_none() {
                return Err(StorageError::TableNotFound(format!(
                    "Requested resource not found: Stream: {stream_arn_val} not found."
                )));
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
            let records_coll = self.data_db.collection::<Document>("stream_records");
            let opts = FindOptions::builder()
                .sort(doc! { "sequence_number": -1 })
                .limit(1)
                .build();
            let cursor = records_coll
                .find(doc! { "shard_id": &shard_id })
                .with_options(opts)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let docs: Vec<Document> = cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(docs.first().and_then(|d| {
                d.get_str("sequence_number")
                    .ok()
                    .map(std::borrow::ToOwned::to_owned)
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_name_maps_to_ddb_wire_format() {
        assert_eq!(event_name_ddb_str(StreamEventName::Insert), "INSERT");
        assert_eq!(event_name_ddb_str(StreamEventName::Modify), "MODIFY");
        assert_eq!(event_name_ddb_str(StreamEventName::Remove), "REMOVE");
    }

    #[test]
    fn shard_id_embeds_table_id_not_table_name() {
        let table_id = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            build_shard_id(table_id, 0),
            "shardId-550e8400-e29b-41d4-a716-446655440000-000000000000"
        );
        assert_eq!(
            build_shard_id(table_id, 3),
            "shardId-550e8400-e29b-41d4-a716-446655440000-000000000003"
        );
    }

    #[test]
    fn shard_ids_for_different_table_ids_do_not_collide() {
        // Regression test for RFC-0003 §5.3 (account and table isolation).
        // Two tables with the same shard index (0) must have different
        // shard_ids so a caller in one tenant cannot address the other's
        // shard.
        let table_a = "550e8400-e29b-41d4-a716-446655440000";
        let table_b = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
        assert_ne!(build_shard_id(table_a, 0), build_shard_id(table_b, 0));
    }

    #[test]
    fn shard_id_format_is_stable_across_shards_of_same_table() {
        // Same table_id, different shard index → deterministic ordering.
        let table_id = "550e8400-e29b-41d4-a716-446655440000";
        let s0 = build_shard_id(table_id, 0);
        let s1 = build_shard_id(table_id, 1);
        let s2 = build_shard_id(table_id, 2);
        let s3 = build_shard_id(table_id, 3);
        assert!(s0 < s1);
        assert!(s1 < s2);
        assert!(s2 < s3);
    }
}
