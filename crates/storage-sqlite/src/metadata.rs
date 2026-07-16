// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `SqliteEngine`: TTL configuration,
//! resource tags, table statistics, and TTL-expiry scanning.
//!
//! Expired-item lookup uses `json_extract(item_data, ?)` with the JSON path
//! bound as a parameter (`$."attr".N`), so the TTL attribute name is never
//! interpolated into SQL. Table size uses `SUM(length(item_data))` (SQLite has
//! no per-table physical size function).

use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::data;
use crate::store::SqliteEngine;

/// JSON path selecting a DynamoDB number attribute's value: `$."attr".N`.
fn ttl_json_path(attr: &str) -> String {
    // Escape embedded double quotes for the JSON path string. The value is
    // bound as a parameter, so this is defense-in-depth, not SQL safety.
    format!("$.\"{}\".N", attr.replace('"', "\"\""))
}

/// True if a TTL attribute name is safe to embed in a `CREATE INDEX` expression.
fn safe_index_attr(attr: &str) -> bool {
    !attr.is_empty()
        && attr
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

impl SqliteEngine {
    async fn table_id_for(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<String, StorageError> {
        sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?")
            .bind(account_id)
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))
    }
}

impl MetadataEngine for SqliteEngine {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let row: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT ttl_attribute FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let (ttl_attr,) = row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            Ok(match ttl_attr {
                Some(attr) => TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Enabled,
                    attribute_name: Some(attr),
                },
                None => TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Disabled,
                    attribute_name: None,
                },
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
            let ttl_val: Option<&str> = if enabled { Some(&attribute_name) } else { None };
            let result = sqlx::query(
                "UPDATE tables SET ttl_attribute = ?, ttl_index_ready = 0 \
                 WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
            )
            .bind(ttl_val)
            .bind(&account_id)
            .bind(&table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if result.rows_affected() == 0 {
                let exists: Option<(String,)> = sqlx::query_as(
                    "SELECT table_status FROM tables WHERE account_id = ? AND table_name = ?",
                )
                .bind(&account_id)
                .bind(&table_name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                return match exists {
                    None => Err(StorageError::TableNotFound(table_name)),
                    Some(_) => Err(StorageError::TableNotActive(table_name)),
                };
            }
            Ok(())
        })
    }

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_owned();
        let tags = tags.to_vec();
        Box::pin(async move {
            for tag in &tags {
                sqlx::query(
                    "INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?) \
                     ON CONFLICT (resource_arn, tag_key) DO UPDATE SET tag_value = excluded.tag_value",
                )
                .bind(&arn)
                .bind(&tag.key)
                .bind(&tag.value)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
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
        Box::pin(async move {
            for key in &tag_keys {
                sqlx::query("DELETE FROM tags WHERE resource_arn = ? AND tag_key = ?")
                    .bind(&arn)
                    .bind(key)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT tag_key, tag_value FROM tags WHERE resource_arn = ? ORDER BY tag_key",
            )
            .bind(&arn)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(key, value)| Tag { key, value })
                .collect())
        })
    }

    fn tables_with_ttl(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            sqlx::query_as(
                "SELECT table_name, ttl_attribute FROM tables \
                 WHERE account_id = ? AND ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
            )
            .bind(&account_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))
        })
    }

    fn all_tables_with_ttl(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move {
            sqlx::query_as(
                "SELECT account_id, table_name, ttl_attribute FROM tables \
                 WHERE ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))
        })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move {
            sqlx::query_as(
                "SELECT account_id, table_name, ttl_attribute FROM tables \
                 WHERE ttl_attribute IS NOT NULL AND ttl_index_ready = 1 AND table_status = 'ACTIVE'",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))
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
            Self::validate_account_id(&account_id)?;
            let table_id = self.table_id_for(&account_id, &table_name).await?;
            let data_table = data::data_table_name(&table_id);

            // Best-effort partial expression index to accelerate the sweep.
            if safe_index_attr(&ttl_attribute) {
                let path = ttl_json_path(&ttl_attribute);
                let index_name = format!("idx_ttl_{table_id}");
                let sql = format!(
                    "CREATE INDEX IF NOT EXISTS \"{index_name}\" ON {data_table} \
                     (CAST(json_extract(item_data, '{path}') AS INTEGER)) \
                     WHERE json_extract(item_data, '{path}') IS NOT NULL"
                );
                sqlx::query(&sql).execute(&self.pool).await.map_err(|e| {
                    StorageError::Internal(format!("TTL index creation failed: {e}"))
                })?;
            }

            sqlx::query(
                "UPDATE tables SET ttl_index_ready = 1 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
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
            Self::validate_account_id(&account_id)?;
            let table_id = self.table_id_for(&account_id, &table_name).await?;
            sqlx::query(
                "UPDATE tables SET ttl_index_ready = 0 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let index_name = format!("idx_ttl_{table_id}");
            sqlx::query(&format!("DROP INDEX IF EXISTS \"{index_name}\""))
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("TTL index drop failed: {e}")))?;
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
            Self::validate_account_id(&account_id)?;
            let table_id = self.table_id_for(&account_id, &table_name).await?;
            let data_table = data::data_table_name(&table_id);
            let path = ttl_json_path(&ttl_attribute);

            let now = i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            )
            .unwrap_or(i64::MAX);
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);

            // Path bound as a parameter — TTL attribute never interpolated.
            let sql = format!(
                "SELECT item_data FROM {data_table} \
                 WHERE CAST(json_extract(item_data, ?) AS INTEGER) BETWEEN 1 AND ? \
                 ORDER BY CAST(json_extract(item_data, ?) AS INTEGER) LIMIT ?"
            );
            let rows: Vec<(String,)> = sqlx::query_as(&sql)
                .bind(&path)
                .bind(now)
                .bind(&path)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            rows.into_iter()
                .map(|(s,)| {
                    serde_json::from_str(&s).map_err(|e| StorageError::Internal(e.to_string()))
                })
                .collect()
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
            Self::validate_account_id(&account_id)?;
            let table_id = self.table_id_for(&account_id, &table_name).await?;
            let data_table = data::data_table_name(&table_id);
            let (item_count, table_size): (i64, i64) = sqlx::query_as(&format!(
                "SELECT COUNT(*), COALESCE(SUM(length(item_data)), 0) FROM {data_table}"
            ))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query(
                "UPDATE tables SET item_count = ?, table_size_bytes = ? \
                 WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
            )
            .bind(item_count)
            .bind(table_size)
            .bind(&account_id)
            .bind(&table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT table_name FROM tables \
                 WHERE account_id = ? AND table_status = 'ACTIVE' ORDER BY table_name",
            )
            .bind(&account_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(rows.into_iter().map(|(n,)| n).collect())
        })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move {
            sqlx::query_as(
                "SELECT account_id, table_name FROM tables \
                 WHERE table_status = 'ACTIVE' ORDER BY account_id, table_name",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))
        })
    }
}
