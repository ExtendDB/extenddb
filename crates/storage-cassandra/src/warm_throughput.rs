// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stored `WarmThroughput` for the Cassandra backend: the control-plane delay
//! the `UPDATING` window takes, and the `UpdateTable` merges.
//!
//! The record is written as sent (one JSON text per table and per index, see
//! catalog migration 8) and read back by DescribeTable; nothing pre-warms
//! or scales on it, and it has no effect on throughput, throttling or
//! storage. See `extenddb_core::types::WarmThroughputRecord`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::WarmThroughput;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{warm_throughput_record_for_update, warm_throughput_record_json};

use cdrs_tokio::query::QueryValues;
use cdrs_tokio::types::value::Value;

use crate::CassandraEngine;
use crate::cassandra_util::{lwt_applied, query_lwt, query_optional};

/// How many times a conditional warm throughput write reads the record again
/// after another `UpdateTable` changed it between the read and the write.
pub(crate) const MERGE_ATTEMPTS: usize = 32;

/// A stored record as read (`None` when the column is null) and the JSON to
/// write over it. The write is conditional on `read`, so a merge that another
/// request overtook is retried instead of overwriting that request's side.
pub(crate) struct WarmMerge {
    pub(crate) read: Option<String>,
    pub(crate) merged: String,
}

/// The bind value of a lightweight transaction condition on a text column
/// that may be null.
pub(crate) fn condition_value(read: Option<&str>) -> Value {
    read.map_or(Value::Null, Value::from)
}

impl CassandraEngine {
    /// `control_plane_delay_seconds` from the settings table (0.25 when
    /// unset): the window a fresh record reports `UPDATING` for, the same
    /// one the CREATING to ACTIVE flip uses.
    pub(crate) async fn control_plane_delay_secs(&self) -> f64 {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!("SELECT value FROM {catalog_keyspace}.settings WHERE key = ?");
        query_optional::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!("control_plane_delay_seconds"),
            "control_plane_delay_seconds",
        )
        .await
        .ok()
        .flatten()
        .and_then(|row| {
            let value: String = row.get_r_by_name("value").ok()?;
            value.parse::<f64>().ok()
        })
        .unwrap_or(0.25)
    }

    /// The JSON an `UpdateTable` stores in `tables.warm_throughput`: the
    /// request merged over the current record, with the record as read.
    pub(crate) async fn merged_table_warm_throughput(
        &self,
        account_id: &str,
        table_name: &str,
        spec: &WarmThroughput,
    ) -> Result<WarmMerge, StorageError> {
        let catalog_ks = self.catalog_keyspace();
        let row = query_optional::<StorageError>(
            &self.session,
            &format!(
                "SELECT warm_throughput FROM {catalog_ks}.tables \
                 WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id, table_name),
            "update_table warm throughput",
        )
        .await?
        .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        let existing: Option<String> = row.get_by_name("warm_throughput").ok().flatten();
        let record = warm_throughput_record_for_update(
            existing.as_deref(),
            spec,
            self.control_plane_delay_secs().await,
        );
        Ok(WarmMerge {
            merged: warm_throughput_record_json(&record)?,
            read: existing,
        })
    }

    /// The JSON an `UpdateTable` stores in `indexes.warm_throughput` for a
    /// `GlobalSecondaryIndexUpdates.Update`: the request merged over the
    /// index's current record, with the record as read. `IndexNotFound` when
    /// the table has no such GSI.
    pub(crate) async fn merged_index_warm_throughput(
        &self,
        table_id: &str,
        index_name: &str,
        spec: &WarmThroughput,
    ) -> Result<WarmMerge, StorageError> {
        let catalog_ks = self.catalog_keyspace();
        let row = query_optional::<StorageError>(
            &self.session,
            &format!(
                "SELECT index_type, warm_throughput FROM {catalog_ks}.indexes \
                 WHERE table_id = ? AND index_name = ?"
            ),
            cdrs_tokio::query_values!(table_id, index_name),
            "update_table gsi warm throughput",
        )
        .await?
        .ok_or_else(|| StorageError::IndexNotFound(index_name.to_owned()))?;
        let index_type: Option<String> = row.get_by_name("index_type").ok().flatten();
        if index_type.as_deref() != Some("GSI") {
            return Err(StorageError::IndexNotFound(index_name.to_owned()));
        }
        let existing: Option<String> = row.get_by_name("warm_throughput").ok().flatten();
        let record = warm_throughput_record_for_update(
            existing.as_deref(),
            spec,
            self.control_plane_delay_secs().await,
        );
        Ok(WarmMerge {
            merged: warm_throughput_record_json(&record)?,
            read: existing,
        })
    }

    /// Store a `GlobalSecondaryIndexUpdates.Update.WarmThroughput` merged over
    /// the index's record, as a lightweight transaction conditional on the
    /// record read; when another `UpdateTable` wrote the record first it is
    /// read again and merged again, so two requests that set different sides
    /// both keep their side.
    pub(crate) async fn store_index_warm_throughput(
        &self,
        table_id: &str,
        index_name: &str,
        spec: &WarmThroughput,
    ) -> Result<(), StorageError> {
        let catalog_ks = self.catalog_keyspace();
        let cql = format!(
            "UPDATE {catalog_ks}.indexes SET warm_throughput = ? \
             WHERE table_id = ? AND index_name = ? \
             IF index_type = 'GSI' AND warm_throughput = ?"
        );
        for _ in 0..MERGE_ATTEMPTS {
            let merge = self
                .merged_index_warm_throughput(table_id, index_name, spec)
                .await?;
            let result = query_lwt(
                &self.session,
                &cql,
                QueryValues::SimpleValues(vec![
                    Value::from(merge.merged.as_str()),
                    Value::from(table_id),
                    Value::from(index_name),
                    condition_value(merge.read.as_deref()),
                ]),
            )
            .await?;
            if lwt_applied(&result)? {
                return Ok(());
            }
        }
        Err(StorageError::Internal(format!(
            "warm throughput of index {index_name}: the record changed on every attempt to \
             merge the request"
        )))
    }
}
