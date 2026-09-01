// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `CassandraEngine`.

use extenddb_core::types::{Item, Tag, TimeToLiveDescription};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::CassandraEngine;

impl MetadataEngine for CassandraEngine {
    fn describe_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        Box::pin(async move { todo!("describe_ttl not implemented") })
    }

    fn update_ttl(
        &self,
        _account_id: &str,
        _table_name: &str,
        _attribute_name: &str,
        _enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move { todo!("update_ttl not implemented") })
    }

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_string();
        let tags = tags.to_vec();
        let catalog = self.catalog_keyspace();
        Box::pin(async move {
            for tag in &tags {
                let query = format!(
                    "INSERT INTO {catalog}.tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?)"
                );
                self.session_arc()
                    .query_with_values(
                        &query,
                        cdrs_tokio::query_values!(
                            arn.as_str(),
                            tag.key.as_str(),
                            tag.value.as_str()
                        ),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("tag_resource: {e}");
                        StorageError::Internal("Database error".to_owned())
                    })?;
            }
            Ok(())
        })
    }

    fn untag_resource(
        &self,
        arn: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_string();
        let tag_keys = tag_keys.to_vec();
        let catalog = self.catalog_keyspace();
        Box::pin(async move {
            for key in &tag_keys {
                let query =
                    format!("DELETE FROM {catalog}.tags WHERE resource_arn = ? AND tag_key = ?");
                self.session_arc()
                    .query_with_values(
                        &query,
                        cdrs_tokio::query_values!(arn.as_str(), key.as_str()),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("untag_resource: {e}");
                        StorageError::Internal("Database error".to_owned())
                    })?;
            }
            Ok(())
        })
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let arn = arn.to_string();
        let catalog = self.catalog_keyspace();
        Box::pin(async move {
            let query =
                format!("SELECT tag_key, tag_value FROM {catalog}.tags WHERE resource_arn = ?");
            let result = self
                .session_arc()
                .query_with_values(&query, cdrs_tokio::query_values!(arn.as_str()))
                .await
                .map_err(|e| {
                    tracing::error!("list_tags: {e}");
                    StorageError::Internal("Database error".to_owned())
                })?;

            let rows = result
                .response_body()
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .into_rows()
                .unwrap_or_default();

            let mut tags = Vec::with_capacity(rows.len());
            for row in rows {
                let key: String = crate::cassandra_util::get_column(&row, "tag_key", "list_tags")?;
                let value: String =
                    crate::cassandra_util::get_column(&row, "tag_value", "list_tags")?;
                tags.push(Tag { key, value });
            }
            Ok(tags)
        })
    }

    fn tables_with_ttl(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move { todo!("tables_with_ttl not implemented") })
    }

    fn all_tables_with_ttl(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move { todo!("all_tables_with_ttl not implemented") })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move { todo!("all_tables_with_ttl_index_ready not implemented") })
    }

    fn create_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move { todo!("create_ttl_index not implemented") })
    }

    fn drop_ttl_index(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move { todo!("drop_ttl_index not implemented") })
    }

    fn find_expired_items_indexed(
        &self,
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        Box::pin(async move { todo!("find_expired_items_indexed not implemented") })
    }

    fn refresh_table_size(
        &self,
        _account_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move { todo!("refresh_table_size not implemented") })
    }

    fn list_active_table_names(
        &self,
        _account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        Box::pin(async move { todo!("list_active_table_names not implemented") })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move { todo!("all_active_tables not implemented") })
    }
}
