// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stored `WarmThroughput` for `MongoEngine`: the BSON shape of the record on
//! table and index documents, and the `UpdateTable` merges.
//!
//! The record is written as sent and read back by DescribeTable; nothing
//! pre-warms or scales on it, and it has no effect on throughput, throttling
//! or storage. See `extenddb_core::types::WarmThroughputRecord`.

use bson::{Bson, Document, doc};
use extenddb_core::types::{
    UpdateTableInput, WarmThroughput, WarmThroughputRecord, epoch_seconds_now,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::warm_throughput_record_for_create;
use mongodb::Collection;

use crate::MongoEngine;

/// The `warm_throughput` field value for a record: the record as a document,
/// or `Null` when the client never set one.
pub(crate) fn warm_record_bson(
    record: Option<&WarmThroughputRecord>,
) -> Result<Bson, StorageError> {
    match record {
        Some(r) => bson::to_bson(r).map_err(|e| StorageError::Internal(e.to_string())),
        None => Ok(Bson::Null),
    }
}

/// The record on a table or index document, if any. A field that is not a
/// record is logged and read as never set, so the defaults are reported (see
/// `extenddb_storage::util::parse_warm_throughput_record`).
pub(crate) fn warm_record_from_doc(doc: &Document) -> Option<WarmThroughputRecord> {
    warm_record_from_bson(doc.get("warm_throughput"))
}

fn warm_record_from_bson(field: Option<&Bson>) -> Option<WarmThroughputRecord> {
    match field {
        None | Some(Bson::Null) => None,
        Some(b) => match bson::from_bson(b.clone()) {
            Ok(record) => Some(record),
            Err(e) => {
                tracing::warn!(
                    "stored warm_throughput is not a record, reporting the defaults: {e}"
                );
                None
            }
        },
    }
}

/// The field a create path writes for a request's `WarmThroughput`.
pub(crate) fn warm_record_bson_for_create(
    spec: Option<&WarmThroughput>,
    delay_secs: f64,
) -> Result<(Option<WarmThroughputRecord>, Bson), StorageError> {
    let record = warm_throughput_record_for_create(spec, delay_secs);
    let bson = warm_record_bson(record.as_ref())?;
    Ok((record, bson))
}

/// How many times a merge reads the record again after another write changed
/// it between the read and the conditional write.
const MERGE_ATTEMPTS: usize = 32;

/// Merge `spec` into the `warm_throughput` field of the document `filter`
/// selects, starting from `current`, the field as read. The write is
/// conditional on the field still holding what was read; when another
/// `UpdateTable` wrote it first, the document is read again and the merge
/// retried, so two requests that set different sides both keep their side.
async fn merge_warm_throughput(
    coll: &Collection<Document>,
    filter: &Document,
    mut current: Bson,
    spec: &WarmThroughput,
    (updating_until, now): (f64, f64),
    not_found: impl Fn() -> StorageError,
) -> Result<(), StorageError> {
    for _ in 0..MERGE_ATTEMPTS {
        let record = WarmThroughputRecord::apply_update(
            warm_record_from_bson(Some(&current)),
            spec,
            updating_until,
            now,
        );
        let mut guarded = filter.clone();
        guarded.insert("warm_throughput", current.clone());
        let result = coll
            .update_one(
                guarded,
                doc! { "$set": { "warm_throughput": warm_record_bson(Some(&record))? } },
            )
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        if result.matched_count > 0 {
            return Ok(());
        }
        let doc = coll
            .find_one(filter.clone())
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(&not_found)?;
        current = doc.get("warm_throughput").cloned().unwrap_or(Bson::Null);
    }
    Err(StorageError::Internal(
        "warm throughput: the record changed on every attempt to merge the request".to_owned(),
    ))
}

impl MongoEngine {
    /// Store the request's table-level `WarmThroughput` and each
    /// `GlobalSecondaryIndexUpdates.Update.WarmThroughput`, merged over what
    /// is stored. Each merge is a conditional write on the value it read,
    /// retried when another request wrote first (see
    /// `merge_warm_throughput`). The engine has already validated the values
    /// against the current ones.
    pub(crate) async fn store_warm_throughput_updates(
        &self,
        account_id: &str,
        input: &UpdateTableInput,
    ) -> Result<(), StorageError> {
        let gsi_updates: Vec<_> = input
            .global_secondary_index_updates
            .iter()
            .flatten()
            .filter_map(|u| u.update.as_ref())
            .filter_map(|u| u.warm_throughput.as_ref().map(|wt| (&u.index_name, wt)))
            .collect();
        if input.warm_throughput.is_none() && gsi_updates.is_empty() {
            return Ok(());
        }
        let now = epoch_seconds_now();
        let updating_until = now + self.control_plane_delay_seconds().await.max(0.0);
        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let table_filter =
            doc! { "_id": { "account_id": account_id, "table_name": &input.table_name } };
        let table_doc = tables_coll
            .find_one(table_filter.clone())
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        if let Some(spec) = &input.warm_throughput {
            merge_warm_throughput(
                &tables_coll,
                &table_filter,
                table_doc
                    .get("warm_throughput")
                    .cloned()
                    .unwrap_or(Bson::Null),
                spec,
                (updating_until, now),
                || StorageError::TableNotFound(input.table_name.clone()),
            )
            .await?;
        }

        if gsi_updates.is_empty() {
            return Ok(());
        }
        let table_id = table_doc
            .get_str("table_id")
            .map_err(|_| StorageError::Internal("missing table_id".to_owned()))?;
        let indexes_coll = self.catalog_db.collection::<Document>("indexes");
        for (index_name, spec) in gsi_updates {
            let filter = doc! {
                "_id": { "table_id": table_id, "index_name": index_name },
                "index_type": "GSI",
            };
            let index_doc = indexes_coll
                .find_one(filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| StorageError::IndexNotFound(index_name.clone()))?;
            merge_warm_throughput(
                &indexes_coll,
                &filter,
                index_doc
                    .get("warm_throughput")
                    .cloned()
                    .unwrap_or(Bson::Null),
                spec,
                (updating_until, now),
                || StorageError::IndexNotFound(index_name.clone()),
            )
            .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_round_trips_through_bson_and_null_reads_as_none() {
        let record = WarmThroughputRecord {
            read_units_per_second: Some(20_000),
            write_units_per_second: None,
            previous_read_units_per_second: Some(12_000),
            previous_write_units_per_second: None,
            updating_until: 1.5,
        };
        let doc = doc! { "warm_throughput": warm_record_bson(Some(&record)).expect("bson") };
        assert_eq!(warm_record_from_doc(&doc), Some(record));
        let null = doc! { "warm_throughput": Bson::Null };
        assert_eq!(warm_record_from_doc(&null), None);
        assert_eq!(warm_record_from_doc(&doc! {}), None);
        assert_eq!(warm_record_bson(None).expect("bson"), Bson::Null);
    }

    #[test]
    fn garbage_in_the_field_reads_as_never_set() {
        for bad in [
            doc! { "warm_throughput": "not a record" },
            doc! { "warm_throughput": { "ReadUnitsPerSecond": "lots" } },
        ] {
            assert_eq!(warm_record_from_doc(&bad), None, "{bad}");
        }
    }
}
