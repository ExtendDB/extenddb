// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The stream records in the result of the `get_stream_records` aggregation.
//!
//! The aggregation starts from the shard document and joins the shard's
//! records with `$lookup` and `$unwind`, keeping the shard document when no
//! record follows the bound. Each output document carries `has_record`, which
//! is false for that shard-only document and true when a record was joined. A
//! joined record document that lacks `record_data` would otherwise look like
//! the shard-only document and be dropped, where the read answers `Internal`.

use extenddb_core::types::StreamRecord;
use extenddb_storage::error::StorageError;
use mongodb::bson::{self, Document};

/// The records in `docs`, in order. A document whose `has_record` is false
/// carries none. One whose `has_record` is true must carry `record_data`.
pub(crate) fn records_from_docs(docs: Vec<Document>) -> Result<Vec<StreamRecord>, StorageError> {
    docs.into_iter()
        .filter(|d| d.get_bool("has_record").unwrap_or(false))
        .map(|mut d| {
            let record_bson = d
                .remove("record_data")
                .ok_or_else(|| StorageError::Internal("Missing record_data".to_owned()))?;
            let json_val: serde_json::Value =
                bson::from_bson(record_bson).map_err(|e| StorageError::Internal(e.to_string()))?;
            serde_json::from_value(json_val).map_err(|e| StorageError::Internal(e.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{self, Document, doc};

    use super::*;

    fn record(seq: u64) -> Document {
        let json = serde_json::json!({
            "eventID": format!("e{seq}"),
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-1",
            "dynamodb": {
                "ApproximateCreationDateTime": 1_700_000_000,
                "Keys": { "pk": { "S": format!("k{seq}") } },
                "SequenceNumber": format!("{seq:021}"),
                "SizeBytes": 10,
                "StreamViewType": "KEYS_ONLY"
            }
        });
        doc! {
            "shard_id": "shardId-t-0",
            "has_record": true,
            "record_data": bson::to_bson(&json).expect("record json to bson"),
        }
    }

    #[test]
    fn the_shard_only_document_carries_no_record() {
        let idle = doc! { "shard_id": "shardId-t-0", "has_record": false };
        assert!(
            records_from_docs(vec![idle])
                .expect("idle shard")
                .is_empty()
        );
    }

    #[test]
    fn joined_records_come_back_in_order() {
        let docs = vec![record(1), record(2), record(3)];
        let seqs: Vec<String> = records_from_docs(docs)
            .expect("records")
            .into_iter()
            .map(|r| r.dynamodb.sequence_number)
            .collect();
        assert_eq!(
            seqs,
            (1..=3).map(|n| format!("{n:021}")).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_joined_record_without_record_data_is_an_internal_error() {
        let mut broken = record(2);
        broken.remove("record_data");
        match records_from_docs(vec![record(1), broken]) {
            Err(StorageError::Internal(msg)) => assert_eq!(msg, "Missing record_data"),
            other => panic!("expected Internal(Missing record_data), got {other:?}"),
        }
    }
}
