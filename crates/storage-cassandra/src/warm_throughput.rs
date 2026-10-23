// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stored `WarmThroughput` for the Cassandra backend: the control-plane delay
//! the `UPDATING` window takes, and the `UpdateTable` merges.
//!
//! The record is written as sent (one JSON text per table and per index, see
//! catalog migration V006) and read back by DescribeTable; nothing pre-warms
//! or scales on it, and it has no effect on throughput, throttling or
//! storage. See `extenddb_core::types::WarmThroughputRecord`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::WarmThroughput;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{warm_throughput_record_for_update, warm_throughput_record_json};

use crate::CassandraEngine;
use crate::cassandra_util::query_optional;

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
    /// request merged over the current record. Read as its own statement so
    /// a catalog that has not run V006 only fails a request that names the
    /// member.
    pub(crate) async fn merged_table_warm_throughput(
        &self,
        account_id: &str,
        table_name: &str,
        spec: &WarmThroughput,
    ) -> Result<String, StorageError> {
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
        )?;
        warm_throughput_record_json(&record)
    }

    /// The JSON an `UpdateTable` stores in `indexes.warm_throughput` for a
    /// `GlobalSecondaryIndexUpdates.Update`: the request merged over the
    /// index's current record. `IndexNotFound` when the table has no such GSI.
    pub(crate) async fn merged_index_warm_throughput(
        &self,
        table_id: &str,
        index_name: &str,
        spec: &WarmThroughput,
    ) -> Result<String, StorageError> {
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
        )?;
        warm_throughput_record_json(&record)
    }
}
