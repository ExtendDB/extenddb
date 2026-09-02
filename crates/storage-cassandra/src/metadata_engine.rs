// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `CassandraEngine`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::CassandraEngine;

impl CassandraEngine {
    pub(crate) async fn ttl_config_for_table(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<crate::data::ttl::TtlConfig>, StorageError> {
        let query = format!(
            "SELECT ttl_attribute, ttl_generation FROM {}.tables \
             WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let row = crate::cassandra_util::query_optional(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, table_name),
            "ttl_config_for_table",
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let attribute: Option<String> = row.get_by_name("ttl_attribute").ok().flatten();
        let generation: Option<uuid::Uuid> = row.get_by_name("ttl_generation").ok().flatten();
        let Some(attribute) = attribute else {
            return Ok(None);
        };
        if let Some(generation) = generation {
            return Ok(Some(crate::data::ttl::TtlConfig {
                attribute,
                generation,
            }));
        }

        let generation = uuid::Uuid::new_v4();
        let adopt = format!(
            "UPDATE {}.tables SET ttl_generation = ?, ttl_index_ready = false \
             WHERE account_id = ? AND table_name = ? \
             IF ttl_attribute = ? AND ttl_generation = null",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &adopt,
            cdrs_tokio::query_values!(
                cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
                account_id,
                table_name,
                attribute.as_str()
            ),
        )
        .await?;
        if metadata_lwt_applied(&result)? {
            return Ok(Some(crate::data::ttl::TtlConfig {
                attribute,
                generation,
            }));
        }

        let row = crate::cassandra_util::query_optional(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, table_name),
            "ttl_config_for_table_recheck",
        )
        .await?;
        Ok(row.and_then(|row| {
            let attribute: Option<String> = row.get_by_name("ttl_attribute").ok().flatten();
            let generation: Option<uuid::Uuid> = row.get_by_name("ttl_generation").ok().flatten();
            attribute
                .zip(generation)
                .map(|(attribute, generation)| crate::data::ttl::TtlConfig {
                    attribute,
                    generation,
                })
        }))
    }

    pub(crate) async fn clear_ttl_entries_for_table_id(
        &self,
        account_id: &str,
        table_id: &str,
    ) -> Result<(), StorageError> {
        crate::data::ttl::clear_ttl_entries(self, &self.account_keyspace(account_id), table_id)
            .await
    }

    #[doc(hidden)]
    pub async fn acquire_current_ttl_sweep_lease(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<uuid::Uuid>, StorageError> {
        let Some(config) = self.ttl_config_for_table(account_id, table_name).await? else {
            return Ok(None);
        };
        self.acquire_ttl_sweep_lease(account_id, table_name, &config)
            .await
    }

    pub(crate) async fn acquire_ttl_control_lease(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<uuid::Uuid>, StorageError> {
        let owner = uuid::Uuid::new_v4();
        let query = format!(
            "UPDATE {}.tables USING TTL 900 SET ttl_sweep_owner = ? \
             WHERE account_id = ? AND table_name = ? IF ttl_sweep_owner = null \
             AND table_status = 'ACTIVE'",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(
                cdrs_tokio::types::value::Bytes::new(owner.as_bytes().to_vec()),
                account_id,
                table_name
            ),
        )
        .await?;
        Ok(metadata_lwt_applied(&result)?.then_some(owner))
    }
    pub(crate) async fn acquire_ttl_sweep_lease(
        &self,
        account_id: &str,
        table_name: &str,
        config: &crate::data::ttl::TtlConfig,
    ) -> Result<Option<uuid::Uuid>, StorageError> {
        let owner = uuid::Uuid::new_v4();
        let query = format!(
            "UPDATE {}.tables USING TTL 900 SET ttl_sweep_owner = ? \
             WHERE account_id = ? AND table_name = ? IF ttl_sweep_owner = null \
             AND ttl_attribute = ? AND ttl_generation = ? AND ttl_index_ready = true",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(
                cdrs_tokio::types::value::Bytes::new(owner.as_bytes().to_vec()),
                account_id,
                table_name,
                config.attribute.as_str(),
                cdrs_tokio::types::value::Bytes::new(config.generation.as_bytes().to_vec())
            ),
        )
        .await?;
        Ok(metadata_lwt_applied(&result)?.then_some(owner))
    }

    pub(crate) async fn renew_ttl_sweep_lease(
        &self,
        account_id: &str,
        table_name: &str,
        config: &crate::data::ttl::TtlConfig,
        owner: uuid::Uuid,
    ) -> Result<bool, StorageError> {
        let query = format!(
            "UPDATE {}.tables USING TTL 900 SET ttl_sweep_owner = ? \
             WHERE account_id = ? AND table_name = ? IF ttl_sweep_owner = ? \
             AND ttl_attribute = ? AND ttl_generation = ? AND ttl_index_ready = true",
            self.catalog_keyspace()
        );
        let owner_bytes = cdrs_tokio::types::value::Bytes::new(owner.as_bytes().to_vec());
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(
                owner_bytes.clone(),
                account_id,
                table_name,
                owner_bytes,
                config.attribute.as_str(),
                cdrs_tokio::types::value::Bytes::new(config.generation.as_bytes().to_vec())
            ),
        )
        .await?;
        metadata_lwt_applied(&result)
    }

    #[doc(hidden)]
    pub async fn release_ttl_sweep_lease(
        &self,
        account_id: &str,
        table_name: &str,
        owner: uuid::Uuid,
    ) -> Result<(), StorageError> {
        let query = format!(
            "UPDATE {}.tables SET ttl_sweep_owner = null \
             WHERE account_id = ? AND table_name = ? IF ttl_sweep_owner = ?",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(
                account_id,
                table_name,
                cdrs_tokio::types::value::Bytes::new(owner.as_bytes().to_vec())
            ),
        )
        .await?;
        let _ = metadata_lwt_applied(&result)?;
        Ok(())
    }

    pub(crate) async fn pending_ttl_cleanups(
        &self,
    ) -> Result<Vec<(String, String, String, uuid::Uuid)>, StorageError> {
        let mut pending = Vec::new();
        for account_id in self.account_ids().await? {
            let query = format!(
                "SELECT table_name, table_id, ttl_cleanup_generation FROM {}.tables \
                 WHERE account_id = ?",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows(
                &self.session,
                &query,
                cdrs_tokio::query_values!(account_id.as_str()),
                "pending_ttl_cleanups",
            )
            .await?;
            for row in rows {
                let generation: Option<uuid::Uuid> =
                    row.get_by_name("ttl_cleanup_generation").ok().flatten();
                if let Some(generation) = generation {
                    pending.push((
                        account_id.clone(),
                        crate::cassandra_util::get_column(
                            &row,
                            "table_name",
                            "pending_ttl_cleanups",
                        )?,
                        crate::cassandra_util::get_column(
                            &row,
                            "table_id",
                            "pending_ttl_cleanups",
                        )?,
                        generation,
                    ));
                }
            }
        }
        Ok(pending)
    }

    pub(crate) async fn complete_ttl_cleanup(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        generation: uuid::Uuid,
    ) -> Result<(), StorageError> {
        crate::data::ttl::clear_ttl_generation(
            self,
            &self.account_keyspace(account_id),
            table_id,
            generation,
        )
        .await?;
        let query = format!(
            "UPDATE {}.tables SET ttl_cleanup_generation = null \
             WHERE account_id = ? AND table_name = ? IF ttl_cleanup_generation = ?",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(
                account_id,
                table_name,
                cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec())
            ),
        )
        .await?;
        let _ = metadata_lwt_applied(&result)?;
        Ok(())
    }

    pub(crate) async fn reconcile_ttl_item(
        &self,
        key_info: &extenddb_core::types::TableKeyInfo,
        item: &Item,
    ) -> Result<(), StorageError> {
        let Some(config) = self
            .ttl_config_for_table(&key_info.account_id, &key_info.table_name)
            .await?
        else {
            return Ok(());
        };
        let Some(entry) = crate::data::ttl::entry_for_item(key_info, item, &config.attribute)?
        else {
            return Ok(());
        };
        crate::data::ttl::insert_ttl_entry(
            self,
            &self.account_keyspace(&key_info.account_id),
            &key_info.table_id,
            config.generation,
            &entry,
        )
        .await
    }

    pub(crate) async fn reconcile_ttl_item_by_table_id(
        &self,
        table_id: &str,
        item_data: &str,
    ) -> Result<(), StorageError> {
        let query = format!(
            "SELECT account_id, table_name FROM {}.tables WHERE table_id = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows(
            &self.session,
            &query,
            cdrs_tokio::query_values!(table_id),
            "reconcile_ttl_item_by_table_id",
        )
        .await?;
        let Some(row) = rows.first() else {
            return Ok(());
        };
        let account_id: String =
            crate::cassandra_util::get_column(row, "account_id", "reconcile_ttl_item_by_table_id")?;
        let table_name: String =
            crate::cassandra_util::get_column(row, "table_name", "reconcile_ttl_item_by_table_id")?;
        let key_info = self.fetch_table_key_info(&account_id, &table_name).await?;
        let item: Item = serde_json::from_str(item_data).map_err(|error| {
            StorageError::Internal(format!("Parse recovered TTL item: {error}"))
        })?;
        self.reconcile_ttl_item(&key_info, &item).await
    }

    async fn account_ids(&self) -> Result<Vec<String>, StorageError> {
        let query = format!(
            "SELECT account_id FROM {}.accounts",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows(
            &self.session,
            &query,
            cdrs_tokio::query_values!(),
            "ttl_account_ids",
        )
        .await?;
        rows.iter()
            .map(|row| crate::cassandra_util::get_column(row, "account_id", "ttl_account_ids"))
            .collect()
    }

    async fn ttl_tables_for_account(
        &self,
        account_id: &str,
        require_ready: bool,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let query = format!(
            "SELECT table_name, table_status, ttl_attribute, ttl_index_ready \
             FROM {}.tables WHERE account_id = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id),
            "ttl_tables_for_account",
        )
        .await?;
        let mut tables = Vec::new();
        for row in rows {
            let status: String =
                crate::cassandra_util::get_column(&row, "table_status", "ttl_tables_for_account")?;
            let attribute: Option<String> = row.get_by_name("ttl_attribute").ok().flatten();
            let ready: bool = row
                .get_by_name("ttl_index_ready")
                .ok()
                .flatten()
                .unwrap_or(false);
            if status == "ACTIVE" && (!require_ready || ready) {
                let Some(attribute) = attribute else {
                    continue;
                };
                let table_name: String = crate::cassandra_util::get_column(
                    &row,
                    "table_name",
                    "ttl_tables_for_account",
                )?;
                tables.push((table_name, attribute));
            }
        }
        Ok(tables)
    }
}

impl MetadataEngine for CassandraEngine {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let query = format!(
                "SELECT ttl_attribute FROM {}.tables WHERE account_id = ? AND table_name = ?",
                self.catalog_keyspace()
            );
            let row = crate::cassandra_util::query_optional(
                &self.session,
                &query,
                cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
                "describe_ttl",
            )
            .await?
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            let attribute: Option<String> = row.get_by_name("ttl_attribute").ok().flatten();
            Ok(TimeToLiveDescription {
                time_to_live_status: if attribute.is_some() {
                    TimeToLiveStatus::Enabled
                } else {
                    TimeToLiveStatus::Disabled
                },
                attribute_name: attribute,
            })
        })
    }

    fn update_ttl(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let attribute_name = attribute_name.to_owned();
        Box::pin(async move {
            let status_query = format!(
                "SELECT table_status, table_id, ttl_generation FROM {}.tables \
                 WHERE account_id = ? AND table_name = ?",
                self.catalog_keyspace()
            );
            let row = crate::cassandra_util::query_optional(
                &self.session,
                &status_query,
                cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
                "update_ttl_status",
            )
            .await?
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            let status: String =
                crate::cassandra_util::get_column(&row, "table_status", "update_ttl_status")?;
            let table_id: String =
                crate::cassandra_util::get_column(&row, "table_id", "update_ttl_status")?;
            let previous_generation: Option<uuid::Uuid> =
                row.get_by_name("ttl_generation").ok().flatten();
            if status != "ACTIVE" {
                return Err(StorageError::TableNotActive(table_name));
            }
            if enabled {
                let indexes = crate::data::index::fetch_indexes_for_table(
                    &table_id,
                    &self.session,
                    &self.catalog_keyspace(),
                )
                .await?;
                let default_delay = self
                    .gsi_default_delay_ms
                    .load(std::sync::atomic::Ordering::Relaxed);
                if indexes.iter().any(|index| {
                    index.index_type == "GSI"
                        && crate::data::index::effective_delay(index, default_delay) != 0
                }) {
                    return Err(StorageError::Validation(
                        "TTL is not supported while a table has asynchronously propagated GSIs"
                            .to_owned(),
                    ));
                }
            }

            let generation = uuid::Uuid::new_v4();
            let cleanup_generation = previous_generation.unwrap_or(generation);
            if !enabled
                && previous_generation.is_some()
                && crate::data::ttl::has_inflight_ttl_work(
                    self,
                    &self.account_keyspace(&account_id),
                    &table_id,
                    cleanup_generation,
                )
                .await?
            {
                return Err(StorageError::TableNotActive(table_name));
            }
            let query = if enabled {
                format!(
                    "UPDATE {}.tables SET ttl_attribute = ?, ttl_generation = ?, \
                     ttl_index_ready = false WHERE account_id = ? AND table_name = ? \
                     IF table_status = 'ACTIVE' AND ttl_sweep_owner = null \
                     AND ttl_attribute = null AND ttl_generation = null",
                    self.catalog_keyspace()
                )
            } else {
                format!(
                    "UPDATE {}.tables SET ttl_attribute = null, ttl_generation = null, \
                     ttl_cleanup_generation = ?, ttl_index_ready = false \
                     WHERE account_id = ? AND table_name = ? \
                     IF table_status = 'ACTIVE' AND ttl_sweep_owner = null \
                     AND ttl_attribute = ? AND ttl_generation = ?",
                    self.catalog_keyspace()
                )
            };
            let result = if enabled {
                crate::cassandra_util::query_lwt(
                    &self.session,
                    &query,
                    cdrs_tokio::query_values!(
                        attribute_name.as_str(),
                        cdrs_tokio::types::value::Bytes::new(generation.as_bytes().to_vec()),
                        account_id.as_str(),
                        table_name.as_str()
                    ),
                )
                .await
            } else {
                crate::cassandra_util::query_lwt(
                    &self.session,
                    &query,
                    cdrs_tokio::query_values!(
                        cdrs_tokio::types::value::Bytes::new(
                            cleanup_generation.as_bytes().to_vec()
                        ),
                        account_id.as_str(),
                        table_name.as_str(),
                        attribute_name.as_str(),
                        cdrs_tokio::types::value::Bytes::new(
                            cleanup_generation.as_bytes().to_vec()
                        )
                    ),
                )
                .await
            };
            let result = result?;
            let applied = result
                .response_body()
                .ok()
                .and_then(|body| body.into_rows())
                .and_then(|rows| rows.into_iter().next())
                .and_then(|row| {
                    let applied: Result<bool, _> = row.get_r_by_name("[applied]");
                    applied.ok()
                })
                .unwrap_or(false);
            if !applied {
                let row = crate::cassandra_util::query_optional(
                    &self.session,
                    &status_query,
                    cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
                    "update_ttl_recheck",
                )
                .await?;
                return if row.is_some() {
                    Err(StorageError::TableNotActive(table_name))
                } else {
                    Err(StorageError::TableNotFound(table_name))
                };
            }
            if !enabled {
                self.complete_ttl_cleanup(&account_id, &table_name, &table_id, cleanup_generation)
                    .await?;
            }
            Ok(())
        })
    }

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_owned();
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
                    .map_err(|error| {
                        tracing::error!("tag_resource: {error}");
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
        let arn = arn.to_owned();
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
                    .map_err(|error| {
                        tracing::error!("untag_resource: {error}");
                        StorageError::Internal("Database error".to_owned())
                    })?;
            }
            Ok(())
        })
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let arn = arn.to_owned();
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
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move { self.ttl_tables_for_account(&account_id, false).await })
    }

    fn all_tables_with_ttl(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move {
            let mut result = Vec::new();
            for account_id in self.account_ids().await? {
                for (table_name, attribute) in
                    self.ttl_tables_for_account(&account_id, false).await?
                {
                    result.push((account_id.clone(), table_name, attribute));
                }
            }
            Ok(result)
        })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move {
            let mut result = Vec::new();
            for account_id in self.account_ids().await? {
                for (table_name, attribute) in
                    self.ttl_tables_for_account(&account_id, true).await?
                {
                    result.push((account_id.clone(), table_name, attribute));
                }
            }
            Ok(result)
        })
    }

    fn create_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let ttl_attribute = ttl_attribute.to_owned();
        Box::pin(async move {
            let Some(config) = self.ttl_config_for_table(&account_id, &table_name).await? else {
                return Ok(());
            };
            if config.attribute != ttl_attribute {
                return Ok(());
            }
            let key_info = self.fetch_table_key_info(&account_id, &table_name).await?;
            let account_keyspace = self.account_keyspace(&account_id);

            let mut start_key = None;
            loop {
                if self.ttl_config_for_table(&account_id, &table_name).await?
                    != Some(config.clone())
                {
                    return Ok(());
                }
                let (items, next_key) = self
                    .scan_impl(&key_info, Some(1_000), start_key.as_ref(), None, None, None)
                    .await?;
                for item in items {
                    if let Some(entry) =
                        crate::data::ttl::entry_for_item(&key_info, &item, &ttl_attribute)?
                    {
                        crate::data::ttl::insert_ttl_entry(
                            self,
                            &account_keyspace,
                            &key_info.table_id,
                            config.generation,
                            &entry,
                        )
                        .await?;
                    }
                }
                match next_key {
                    Some(key) => start_key = Some(key),
                    None => break,
                }
            }

            let query = format!(
                "UPDATE {}.tables SET ttl_index_ready = true \
                 WHERE account_id = ? AND table_name = ? \
                 IF ttl_attribute = ? AND ttl_generation = ? AND table_status = 'ACTIVE'",
                self.catalog_keyspace()
            );
            let ready_result = crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(
                    account_id.as_str(),
                    table_name.as_str(),
                    ttl_attribute.as_str(),
                    cdrs_tokio::types::value::Bytes::new(config.generation.as_bytes().to_vec())
                ),
            )
            .await
            .map_err(|error| StorageError::Internal(format!("Mark TTL queue ready: {error}")))?;
            let _ = metadata_lwt_applied(&ready_result)?;
            Ok(())
        })
    }

    fn drop_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let ready_query = format!(
                "UPDATE {}.tables SET ttl_index_ready = false \
                 WHERE account_id = ? AND table_name = ? IF ttl_attribute = null",
                self.catalog_keyspace()
            );
            let ready_result = crate::cassandra_util::query_lwt(
                &self.session,
                &ready_query,
                cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
            )
            .await
            .map_err(|error| StorageError::Internal(format!("Disable TTL queue: {error}")))?;
            let _ = metadata_lwt_applied(&ready_result)?;
            Ok(())
        })
    }

    fn find_expired_items_indexed(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let ttl_attribute = ttl_attribute.to_owned();
        Box::pin(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let Some(config) = self.ttl_config_for_table(&account_id, &table_name).await? else {
                return Ok(Vec::new());
            };
            if config.attribute != ttl_attribute {
                return Ok(Vec::new());
            }
            let key_info = self.fetch_table_key_info(&account_id, &table_name).await?;
            let account_keyspace = self.account_keyspace(&account_id);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let current_bucket = now / crate::data::ttl::TTL_BUCKET_SECONDS;
            let bucket_query = format!(
                "SELECT bucket, shard FROM {account_keyspace}.ttl_expiration_buckets \
                 WHERE table_id = ? AND generation = ? AND bucket <= ?"
            );
            let bucket_rows = crate::cassandra_util::query_rows(
                &self.session,
                &bucket_query,
                cdrs_tokio::query_values!(
                    key_info.table_id.as_str(),
                    cdrs_tokio::types::value::Bytes::new(config.generation.as_bytes().to_vec()),
                    current_bucket
                ),
                "find_expired_buckets",
            )
            .await?;
            let mut partitions: Vec<(i64, i32)> = Vec::with_capacity(bucket_rows.len());
            for row in bucket_rows {
                partitions.push((
                    crate::cassandra_util::get_column(&row, "bucket", "find_expired_buckets")?,
                    crate::cassandra_util::get_column(&row, "shard", "find_expired_buckets")?,
                ));
            }
            if !partitions.is_empty() {
                let rotation = ((now / 60) as usize) % partitions.len();
                partitions.rotate_left(rotation);
            }

            let mut expired = Vec::with_capacity(limit);
            for (index, (bucket, shard)) in partitions.iter().enumerate() {
                let remaining = limit - expired.len();
                if remaining == 0 {
                    break;
                }
                let partitions_left = partitions.len() - index;
                let partition_limit = remaining.div_ceil(partitions_left).max(1);
                let query = format!(
                    "SELECT expires_at, key_hash, key_data \
                     FROM {account_keyspace}.ttl_expirations \
                     WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ? \
                     AND expires_at <= ? LIMIT {partition_limit}"
                );
                let rows = crate::cassandra_util::query_rows(
                    &self.session,
                    &query,
                    cdrs_tokio::query_values!(
                        key_info.table_id.as_str(),
                        cdrs_tokio::types::value::Bytes::new(config.generation.as_bytes().to_vec()),
                        *bucket,
                        *shard,
                        now
                    ),
                    "find_expired_items",
                )
                .await?;
                for row in rows {
                    let entry = crate::data::ttl::TtlEntry {
                        bucket: *bucket,
                        expires_at: crate::cassandra_util::get_column(
                            &row,
                            "expires_at",
                            "find_expired_items",
                        )?,
                        shard: *shard,
                        key_hash: crate::cassandra_util::get_column(
                            &row,
                            "key_hash",
                            "find_expired_items",
                        )?,
                        key_data: crate::cassandra_util::get_column(
                            &row,
                            "key_data",
                            "find_expired_items",
                        )?,
                    };
                    let key: Item = serde_json::from_str(&entry.key_data).map_err(|error| {
                        StorageError::Internal(format!("Parse TTL key: {error}"))
                    })?;
                    let current = self.get_item_impl(&key_info, &key).await?;
                    if let Some(item) = current.filter(|item| {
                        crate::data::ttl::ttl_epoch_seconds(item, &ttl_attribute)
                            == Some(entry.expires_at)
                    }) {
                        expired.push(item);
                    } else {
                        crate::data::ttl::delete_ttl_entry(
                            self,
                            &account_keyspace,
                            &key_info.table_id,
                            config.generation,
                            &entry,
                        )
                        .await?;
                    }
                }
            }
            Ok(expired)
        })
    }

    fn refresh_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let key_info = self.fetch_table_key_info(&account_id, &table_name).await?;
            let mut count = 0_i64;
            let mut size = 0_i64;
            let mut start_key = None;
            loop {
                let (items, next_key) = self
                    .scan_impl(&key_info, Some(1_000), start_key.as_ref(), None, None, None)
                    .await?;
                for item in items {
                    count += 1;
                    size = size.saturating_add(
                        serde_json::to_vec(&item)
                            .map_err(|error| StorageError::Internal(error.to_string()))?
                            .len() as i64,
                    );
                }
                match next_key {
                    Some(key) => start_key = Some(key),
                    None => break,
                }
            }
            let query = format!(
                "UPDATE {}.tables SET item_count = ?, table_size_bytes = ? \
                 WHERE account_id = ? AND table_name = ?",
                self.catalog_keyspace()
            );
            self.session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(
                        count,
                        size,
                        account_id.as_str(),
                        table_name.as_str()
                    ),
                )
                .await
                .map_err(|error| StorageError::Internal(format!("Refresh table size: {error}")))?;
            Ok(())
        })
    }

    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let query = format!(
                "SELECT table_name, table_status FROM {}.tables WHERE account_id = ?",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows(
                &self.session,
                &query,
                cdrs_tokio::query_values!(account_id.as_str()),
                "list_active_table_names",
            )
            .await?;
            let mut tables = Vec::new();
            for row in rows {
                let status: String = crate::cassandra_util::get_column(
                    &row,
                    "table_status",
                    "list_active_table_names",
                )?;
                if status == "ACTIVE" {
                    tables.push(crate::cassandra_util::get_column(
                        &row,
                        "table_name",
                        "list_active_table_names",
                    )?);
                }
            }
            Ok(tables)
        })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move {
            let mut result = Vec::new();
            for account_id in self.account_ids().await? {
                for table_name in self.list_active_table_names(&account_id).await? {
                    result.push((account_id.clone(), table_name));
                }
            }
            Ok(result)
        })
    }
}

fn metadata_lwt_applied(result: &cdrs_tokio::frame::Envelope) -> Result<bool, StorageError> {
    let rows = result
        .response_body()
        .map_err(|error| StorageError::Internal(format!("Parse TTL metadata LWT: {error}")))?
        .into_rows()
        .unwrap_or_default();
    let Some(row) = rows.first() else {
        return Err(StorageError::Internal(
            "TTL metadata LWT returned no result".to_owned(),
        ));
    };
    row.get_r_by_name("[applied]")
        .map_err(|error| StorageError::Internal(format!("Parse TTL metadata result: {error}")))
}
