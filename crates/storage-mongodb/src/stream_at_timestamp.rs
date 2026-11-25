// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AT_TIMESTAMP` seek: the first stream record in a shard created at or
//! after a point in time.

use extenddb_storage::error::StorageError;
use futures::TryStreamExt;
use mongodb::bson::{DateTime as BsonDateTime, Document, doc};
use mongodb::options::FindOptions;

use crate::MongoEngine;

impl MongoEngine {
    pub(crate) async fn first_sequence_number_at_or_after_impl(
        &self,
        shard_id: &str,
        at_ms: i64,
    ) -> Result<Option<String>, StorageError> {
        let records_coll = self.data_db.collection::<Document>("stream_records");
        let opts = FindOptions::builder()
            .sort(doc! { "sequence_number": 1 })
            .limit(1)
            .projection(doc! { "sequence_number": 1 })
            .build();
        let cursor = records_coll
            .find(doc! {
                "shard_id": shard_id,
                "created_at": { "$gte": BsonDateTime::from_millis(at_ms) },
            })
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
    }
}
