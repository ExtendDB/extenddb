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

/// The record on a table or index document, if any.
///
/// # Errors
///
/// `Internal` when the stored field is present but not a record.
pub(crate) fn warm_record_from_doc(
    doc: &Document,
) -> Result<Option<WarmThroughputRecord>, StorageError> {
    match doc.get("warm_throughput") {
        None | Some(Bson::Null) => Ok(None),
        Some(b) => bson::from_bson(b.clone())
            .map(Some)
            .map_err(|e| StorageError::Internal(format!("warm throughput record: {e}"))),
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

impl MongoEngine {
    /// Store the request's table-level `WarmThroughput` and each
    /// `GlobalSecondaryIndexUpdates.Update.WarmThroughput`, merged over what
    /// is stored. Read-modify-write without a transaction, like the rest of
    /// this backend's UpdateTable. The engine has already validated the
    /// values against the current ones.
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
            let record = WarmThroughputRecord::apply_update(
                warm_record_from_doc(&table_doc)?,
                spec,
                updating_until,
                now,
            );
            tables_coll
                .update_one(
                    table_filter,
                    doc! { "$set": { "warm_throughput": warm_record_bson(Some(&record))? } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
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
            let record = WarmThroughputRecord::apply_update(
                warm_record_from_doc(&index_doc)?,
                spec,
                updating_until,
                now,
            );
            indexes_coll
                .update_one(
                    filter,
                    doc! { "$set": { "warm_throughput": warm_record_bson(Some(&record))? } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
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
        assert_eq!(warm_record_from_doc(&doc).expect("parses"), Some(record));
        let null = doc! { "warm_throughput": Bson::Null };
        assert_eq!(warm_record_from_doc(&null).expect("parses"), None);
        assert_eq!(warm_record_from_doc(&doc! {}).expect("parses"), None);
        assert_eq!(warm_record_bson(None).expect("bson"), Bson::Null);
    }

    #[test]
    fn garbage_in_the_field_is_an_internal_error() {
        let bad = doc! { "warm_throughput": "not a record" };
        assert!(matches!(
            warm_record_from_doc(&bad),
            Err(StorageError::Internal(m)) if m.starts_with("warm throughput record:")
        ));
    }
}
