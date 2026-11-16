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
use mongodb::bson::{Document, doc};
use mongodb::options::FindOptions;

use extenddb_core::types::{DescribeStreamInput, StreamDescription, StreamEventName, StreamRecord};
use extenddb_storage::StreamEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage;
use extenddb_storage::util::{parse_stream_arn, shard_in_stream, stream_arn};
use extenddb_storage::{StreamListResult, StreamRecordsResult};

use crate::MongoEngine;

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

impl MongoEngine {
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
    /// $inc the same counter under snapshot isolation conflict, MongoDB
    /// aborts the loser with `WriteConflict`, and this function reports
    /// that as `StorageError::Transient` so that the caller's transaction
    /// retry loop re-runs the write with backoff. Any other driver error is
    /// `StorageError::Internal`.
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
            .map_err(crate::write_conflict::transaction_error)?
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
    ///
    /// Only the open shards count: they belong to the table's enabled
    /// stream. `None` when there are none, because the stream was disabled
    /// after the caller read the table's key info; the write then records
    /// nothing.
    pub(crate) async fn assign_shard_in_session(
        &self,
        account_id: &str,
        table_name: &str,
        partition_key: &str,
        session: &mut mongodb::ClientSession,
    ) -> Result<Option<String>, StorageError> {
        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let table_doc = tables_coll
            .find_one(doc! { "_id": { "account_id": account_id, "table_name": table_name } })
            .session(&mut *session)
            .await
            .map_err(crate::write_conflict::transaction_error)?
            .ok_or_else(|| StorageError::Internal(format!("Table {table_name} not found")))?;
        let table_id = table_doc.get_str("table_id").unwrap_or_default();

        let shards_coll = self.data_db.collection::<Document>("stream_shards");
        let opts = FindOptions::builder().sort(doc! { "shard_id": 1 }).build();
        let mut cursor = shards_coll
            .find(
                doc! { "table_id": table_id, "ending_sequence_number": mongodb::bson::Bson::Null },
            )
            .with_options(opts)
            .session(&mut *session)
            .await
            .map_err(crate::write_conflict::transaction_error)?;
        let shard_docs: Vec<Document> = cursor
            .stream(&mut *session)
            .try_collect()
            .await
            .map_err(crate::write_conflict::transaction_error)?;

        let shard_ids: Vec<&str> = shard_docs
            .iter()
            .filter_map(|d| d.get_str("shard_id").ok())
            .collect();
        if shard_ids.is_empty() {
            return Ok(None);
        }

        let hash = crc32fast::hash(partition_key.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let idx = (hash as usize) % shard_ids.len();
        Ok(Some(shard_ids[idx].to_owned()))
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
    /// Superseded by `MongoEngine::write_stream_inline_in_session` which
    /// writes the stream record inside the same session as the data
    /// write (D-C2 / RFC-0003 §3.2). The trait method has no callers
    /// in the mongo backend after that change — it lives on only
    /// because the `StreamEngine` trait still declares it. If invoked
    /// externally, it would race against concurrent data writes: this
    /// path does not enroll in any transaction and can commit a stream
    /// record whose base-table write later rolls back. Return an
    /// explicit error rather than performing a subtly-wrong write.
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            Err(StorageError::Internal(
                "MongoDB backend: non-transactional write_stream_record is unused; \
                 the data-plane path always routes through the in-session variant"
                    .to_owned(),
            ))
        })
    }

    fn get_stream_records(
        &self,
        account_id: &str,
        stream_arn_val: &str,
        shard_id: &str,
        after_sequence: Option<&str>,
        limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        let account_id = account_id.to_owned();
        let stream_arn_val = stream_arn_val.to_owned();
        let shard_id = shard_id.to_owned();
        let after_sequence = after_sequence.map(std::borrow::ToOwned::to_owned);
        Box::pin(async move {
            let (table_name, stream_label) = parse_stream_arn(&stream_arn_val)?;
            // The `tables` catalog lives in the catalog database and
            // `stream_shards`/`stream_records` in the data database, so the
            // stream resolves in two round trips: the table the ARN names
            // (catalog: its id and current stream label), then the shard and
            // its records in one aggregation (data). The shard document drives
            // the aggregation with the records joined by `$lookup` and
            // unwound, so each record is its own output document and the page
            // is bounded by `limit` rather than by the 16 MB document cap (the
            // server coalesces the `$lookup` with the `$unwind` and never
            // materializes the array). A shard of the named table with no
            // records returns one document with `has_record` false, and a
            // shard that is not that table's (another table, or another
            // account) returns no document. Whether the shard belongs to the
            // stream the ARN's label names is decided in Rust from the table's
            // label and the shard id (`shard_in_stream`), with a third round
            // trip for a legacy id only: whether the table has a labelled
            // shard.
            let table_doc = self
                .catalog_db
                .collection::<Document>("tables")
                .find_one(doc! {
                    "_id": { "account_id": &account_id, "table_name": &table_name },
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let Some(table_doc) = table_doc else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            let table_id = table_doc
                .get_str("table_id")
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .to_owned();
            let table_label = table_doc.get_str("stream_label").ok();

            let mut record_match = doc! { "$eq": ["$shard_id", "$$shard"] };
            if let Some(ref after) = after_sequence {
                record_match = doc! { "$and": [
                    record_match,
                    { "$gt": ["$sequence_number", after] },
                ] };
            }
            let pipeline = vec![
                doc! { "$match": { "shard_id": &shard_id, "table_id": &table_id } },
                doc! { "$limit": 1 },
                doc! { "$lookup": {
                    "from": "stream_records",
                    "let": { "shard": "$shard_id" },
                    "pipeline": [
                        { "$match": { "$expr": record_match } },
                        { "$sort": { "sequence_number": 1 } },
                        { "$limit": limit },
                        { "$project": { "_id": 0, "record_data": 1 } },
                    ],
                    "as": "records",
                } },
                doc! { "$unwind": { "path": "$records", "preserveNullAndEmptyArrays": true } },
                doc! { "$project": {
                    "_id": 0,
                    "shard_id": 1,
                    "has_record": { "$ne": [{ "$type": "$records" }, "missing"] },
                    "record_data": "$records.record_data",
                } },
            ];
            let cursor = self
                .data_db
                .collection::<Document>("stream_shards")
                .aggregate(pipeline)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let docs: Vec<Document> = cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let Some(row_shard_id) = docs.first().and_then(|d| d.get_str("shard_id").ok()) else {
                return Err(extenddb_storage::util::stream_does_not_exist());
            };
            if !shard_in_stream(&table_id, row_shard_id, table_label, &stream_label, || {
                self.table_has_labelled_shard(&table_id)
            })
            .await?
            {
                return Err(extenddb_storage::util::stream_does_not_exist());
            }

            let records = record_docs::records_from_docs(docs)?;

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
            let (table, table_doc) = self
                .stream_table_row(&account_id, &table_name)
                .await?
                .ok_or_else(|| extenddb_storage::util::stream_not_found(&stream_arn_val))?;
            let key_schema = table_doc
                .get("key_schema")
                .and_then(|b| mongodb::bson::from_bson(b.clone()).ok())
                .ok_or_else(|| StorageError::Internal("Missing key_schema".to_owned()))?;
            let shards = self
                .stream_shard_rows(std::slice::from_ref(&table.table_id))
                .await?;
            stream_lineage::describe_stream(
                stream_arn_val,
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
    ) -> BoxFuture<'_, StreamListResult> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(std::borrow::ToOwned::to_owned);
        let exclusive_start_stream_arn =
            exclusive_start_stream_arn.map(std::borrow::ToOwned::to_owned);
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
            // Only the open shards: they belong to the table's enabled stream.
            let cursor = shards_coll
                .find(doc! { "table_id": table_id, "ending_sequence_number": mongodb::bson::Bson::Null })
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

    const LABEL: &str = "2026-10-24T08:54:14.037";

    fn shard(table_id: &str, index: u32) -> String {
        stream_lineage::shard_id(
            table_id,
            LABEL,
            extenddb_core::types::StreamViewType::KeysOnly,
            index,
        )
        .unwrap()
    }

    #[test]
    fn shard_id_embeds_table_id_not_table_name() {
        let table_id = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            shard(table_id, 0),
            "shardId-550e8400-e29b-41d4-a716-446655440000-1792832054037100"
        );
        assert_eq!(
            shard(table_id, 3),
            "shardId-550e8400-e29b-41d4-a716-446655440000-1792832054037103"
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
        assert_ne!(shard(table_a, 0), shard(table_b, 0));
    }

    #[test]
    fn shard_id_format_is_stable_across_shards_of_same_table() {
        // Same table_id, different shard index → deterministic ordering.
        let table_id = "550e8400-e29b-41d4-a716-446655440000";
        let ids: Vec<String> = (0..4).map(|i| shard(table_id, i)).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
    }
}

/// The records in the result of the `get_stream_records` aggregation; see the
/// module documentation.
impl MongoEngine {
    /// Whether table `table_id` has a labelled shard document
    /// (`stream_lineage::labelled_shard_id_pattern`, on the
    /// `{table_id, shard_id}` index), which makes its legacy shards an
    /// earlier, closed stream (`util::shard_in_stream`).
    async fn table_has_labelled_shard(&self, table_id: &str) -> Result<bool, StorageError> {
        let found = self
            .data_db
            .collection::<Document>("stream_shards")
            .find_one(doc! {
                "table_id": table_id,
                "shard_id": { "$regex": stream_lineage::labelled_shard_id_pattern(table_id) },
            })
            .projection(doc! { "_id": 0, "shard_id": 1 })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(found.is_some())
    }
}

#[path = "stream_record_docs.rs"]
mod record_docs;

/// Live `get_stream_records` tests against a MongoDB server; see the module
/// documentation.
#[cfg(test)]
#[path = "stream_records_live_tests.rs"]
mod records_live_tests;
