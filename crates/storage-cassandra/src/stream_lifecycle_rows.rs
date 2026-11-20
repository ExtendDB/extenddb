// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The table rows and shard rows of the Cassandra stream lifecycle, read for
//! the shared rules in `extenddb_storage::stream_lineage` to group into
//! streams.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::StreamSpecification;
use extenddb_storage::error::StorageError;
use extenddb_storage::stream_lineage::{ShardRow, TableStreamRow};

use super::internal;
use crate::CassandraEngine;
use crate::cassandra_util::query_rows;
use crate::stream_util::ZERO_SEQUENCE;

impl CassandraEngine {
    fn table_stream_row(
        table_name: String,
        row: &cdrs_tokio::types::rows::Row,
    ) -> Result<TableStreamRow, StorageError> {
        let spec: Option<String> = row.get_by_name("stream_specification").ok().flatten();
        Ok(TableStreamRow {
            table_name,
            table_id: row.get_r_by_name("table_id").map_err(internal)?,
            table_status: row.get_r_by_name("table_status").map_err(internal)?,
            stream_label: row.get_by_name("stream_label").ok().flatten(),
            stream_specification: spec
                .as_deref()
                .map(serde_json::from_str::<StreamSpecification>)
                .transpose()
                .map_err(internal)?,
        })
    }

    /// The table's stream columns and its key schema JSON, by name.
    pub(crate) async fn stream_table_row(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<(TableStreamRow, String)>, StorageError> {
        let catalog_ks = self.catalog_keyspace();
        let row = query_rows::<StorageError>(
            &self.session,
            &format!(
                "SELECT table_id, table_status, stream_label, stream_specification, key_schema \
                 FROM {catalog_ks}.tables WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id, table_name),
            "stream_table_row",
        )
        .await?
        .into_iter()
        .next();
        row.map(|r| {
            Ok((
                Self::table_stream_row(table_name.to_owned(), &r)?,
                r.get_r_by_name("key_schema").map_err(internal)?,
            ))
        })
        .transpose()
    }

    /// Every table of the account that has had a stream.
    pub(crate) async fn stream_table_rows(
        &self,
        account_id: &str,
    ) -> Result<Vec<TableStreamRow>, StorageError> {
        let catalog_ks = self.catalog_keyspace();
        let rows = query_rows::<StorageError>(
            &self.session,
            &format!(
                "SELECT table_name, table_id, table_status, stream_label, stream_specification \
                 FROM {catalog_ks}.tables WHERE account_id = ? ALLOW FILTERING"
            ),
            cdrs_tokio::query_values!(account_id),
            "stream_table_rows",
        )
        .await?;
        let mut tables = Vec::new();
        for r in rows {
            let label: Option<String> = r.get_by_name("stream_label").ok().flatten();
            if label.is_none() {
                continue;
            }
            let name: String = r.get_r_by_name("table_name").map_err(internal)?;
            tables.push(Self::table_stream_row(name, &r)?);
        }
        Ok(tables)
    }

    /// The shard rows of one table, or of every table in the account's
    /// keyspace when `table_id` is `None`.
    pub(crate) async fn stream_shard_rows(
        &self,
        account_ks: &str,
        table_id: Option<&str>,
    ) -> Result<Vec<ShardRow>, StorageError> {
        let columns = "table_id, shard_id, parent_shard_id, starting_sequence_number, \
                       ending_sequence_number";
        let rows = match table_id {
            Some(id) => {
                query_rows::<StorageError>(
                    &self.session,
                    &format!("SELECT {columns} FROM {account_ks}.stream_shards WHERE table_id = ?"),
                    cdrs_tokio::query_values!(id),
                    "stream_shard_rows",
                )
                .await?
            }
            None => {
                query_rows::<StorageError>(
                    &self.session,
                    &format!("SELECT {columns} FROM {account_ks}.stream_shards"),
                    cdrs_tokio::query::QueryValues::SimpleValues(Vec::new()),
                    "stream_shard_rows",
                )
                .await?
            }
        };
        rows.into_iter()
            .map(|r| {
                Ok(ShardRow {
                    table_id: r.get_r_by_name("table_id").map_err(internal)?,
                    shard_id: r.get_r_by_name("shard_id").map_err(internal)?,
                    parent_shard_id: r.get_by_name("parent_shard_id").ok().flatten(),
                    starting_sequence_number: r
                        .get_by_name("starting_sequence_number")
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| ZERO_SEQUENCE.to_owned()),
                    ending_sequence_number: r.get_by_name("ending_sequence_number").ok().flatten(),
                })
            })
            .collect()
    }
}
