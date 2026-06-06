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
    DescribeStreamInput, SequenceNumberRange, Shard, StreamDescription, StreamRecord, StreamStatus,
    StreamSummary, StreamViewType,
};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{parse_stream_arn, stream_arn};
use extenddb_storage::{StreamListResult, StreamRecordsResult};

use crate::MongoEngine;

const SHARDS_PER_STREAM: u32 = 4;

impl MongoEngine {
    /// Initialize stream shards for a table. Only creates shard documents;
    /// the caller is responsible for setting `stream_label` on the table doc.
    pub(crate) async fn init_stream_shards(
        &self,
        table_name: &str,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let shards_coll = self.data_db.collection::<Document>("stream_shards");
        for i in 0..SHARDS_PER_STREAM {
            let shard_id = format!("shardId-{table_name}-{i:012}");
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
                    "event_name": format!("{:?}", record.event_name),
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

    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move {
            // Use atomic findAndModify on a sequence counter document
            let counters_coll = self.data_db.collection::<Document>("counters");
            let opts = mongodb::options::FindOneAndUpdateOptions::builder()
                .upsert(true)
                .return_document(mongodb::options::ReturnDocument::After)
                .build();
            let doc = counters_coll
                .find_one_and_update(
                    doc! { "_id": "stream_seq" },
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
