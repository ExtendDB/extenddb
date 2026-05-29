// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `TidbEngine`.

use extenddb_core::types::{
    Item, StreamSpecification, Tag, TimeToLiveDescription, TimeToLiveStatus,
};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::TidbEngine;
use crate::data;

const TTL_EXPIRES_AT_COLUMN: &str = "_edb_ttl_expires_at";
const TTL_EXPIRES_AT_INDEX: &str = "_edb_ttl_expires_at_idx";
const LEGACY_TTL_EPOCH_COLUMN: &str = "_edb_ttl_epoch";
const LEGACY_TTL_EPOCH_INDEX: &str = "_edb_ttl_epoch_idx";

type TtlArtifactRow = (String, Option<String>, Option<serde_json::Value>, bool);

fn ttl_json_path(ttl_attribute: &str) -> String {
    format!(
        "$.\"{}\".N",
        ttl_attribute.replace('\\', "\\\\").replace('"', "\\\"")
    )
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn ttl_json_value_expr(ttl_attribute: &str) -> String {
    let ttl_path = sql_string_literal(&ttl_json_path(ttl_attribute));
    format!("JSON_UNQUOTE(JSON_EXTRACT(item_data, {ttl_path}))")
}

fn ttl_expires_at_expr(ttl_attribute: &str) -> String {
    let ttl_value = ttl_json_value_expr(ttl_attribute);
    format!(
        "CASE \
             WHEN {ttl_value} REGEXP '^[0-9]+$' \
                  AND CAST({ttl_value} AS UNSIGNED) > 0 \
             THEN FROM_UNIXTIME(CAST({ttl_value} AS UNSIGNED)) \
             ELSE NULL \
         END"
    )
}

fn stream_enabled(stream_spec_json: Option<serde_json::Value>) -> Result<bool, StorageError> {
    stream_spec_json
        .map(serde_json::from_value::<StreamSpecification>)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))
        .map(|spec| spec.is_some_and(|s| s.stream_enabled))
}

async fn data_table_has_native_ttl(
    pool: &sqlx::MySqlPool,
    data_table: &str,
) -> Result<bool, StorageError> {
    let (_table_name, create_table): (String, String) =
        sqlx::query_as(&format!("SHOW CREATE TABLE {data_table}"))
            .fetch_one(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    let create_table = create_table.to_ascii_uppercase();
    Ok(create_table.contains(" TTL =") || create_table.contains("/*T![TTL] TTL ="))
}

pub(crate) async fn drop_ttl_artifacts(
    pool: &sqlx::MySqlPool,
    table_id: &str,
) -> Result<(), StorageError> {
    let data_table = data::data_table_name(table_id);

    if data_table_has_native_ttl(pool, &data_table).await? {
        let sql = format!("ALTER TABLE {data_table} REMOVE TTL");
        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }

    for index_name in [TTL_EXPIRES_AT_INDEX, LEGACY_TTL_EPOCH_INDEX] {
        let sql = format!("DROP INDEX IF EXISTS `{index_name}` ON {data_table}");
        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }

    for column_name in [TTL_EXPIRES_AT_COLUMN, LEGACY_TTL_EPOCH_COLUMN] {
        let sql = format!("ALTER TABLE {data_table} DROP COLUMN IF EXISTS `{column_name}`");
        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    }

    Ok(())
}

async fn add_ttl_generated_column(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    ttl_attribute: &str,
) -> Result<(), StorageError> {
    let data_table = data::data_table_name(table_id);
    let ttl_expr = ttl_expires_at_expr(ttl_attribute);
    let add_column = format!(
        "ALTER TABLE {data_table} ADD COLUMN `{TTL_EXPIRES_AT_COLUMN}` DATETIME \
         AS ({ttl_expr}) VIRTUAL"
    );
    sqlx::query(&add_column)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

async fn configure_native_ttl(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    ttl_attribute: &str,
) -> Result<(), StorageError> {
    drop_ttl_artifacts(pool, table_id).await?;
    add_ttl_generated_column(pool, table_id, ttl_attribute).await?;

    let data_table = data::data_table_name(table_id);
    let enable_ttl = format!(
        "ALTER TABLE {data_table} \
         TTL = `{TTL_EXPIRES_AT_COLUMN}` + INTERVAL 0 SECOND \
         TTL_JOB_INTERVAL = '1h'"
    );
    sqlx::query(&enable_ttl)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

async fn configure_stream_ttl_index(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    ttl_attribute: &str,
) -> Result<(), StorageError> {
    drop_ttl_artifacts(pool, table_id).await?;
    add_ttl_generated_column(pool, table_id, ttl_attribute).await?;

    let data_table = data::data_table_name(table_id);
    let add_index = format!(
        "CREATE INDEX IF NOT EXISTS `{TTL_EXPIRES_AT_INDEX}` ON {data_table} (`{TTL_EXPIRES_AT_COLUMN}`)"
    );
    sqlx::query(&add_index)
        .execute(pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

async fn configure_ttl_artifacts(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    ttl_attribute: &str,
    stream_spec_json: Option<serde_json::Value>,
) -> Result<bool, StorageError> {
    let use_native_ttl = !stream_enabled(stream_spec_json)?;

    if use_native_ttl {
        configure_native_ttl(pool, table_id, ttl_attribute).await?;
    } else {
        configure_stream_ttl_index(pool, table_id, ttl_attribute).await?;
    }

    Ok(use_native_ttl)
}

impl TidbEngine {
    pub(crate) async fn disable_native_ttl_for_table_id(
        &self,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let data_table = data::data_table_name(table_id);
        if data_table_has_native_ttl(&self.data_pool, &data_table).await? {
            let sql = format!("ALTER TABLE {data_table} REMOVE TTL");
            sqlx::query(&sql)
                .execute(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    pub(crate) async fn streaming_ttl_tables_ready(
        &self,
    ) -> Result<Vec<(String, String, String)>, StorageError> {
        let rows: Vec<(String, String, String, Option<serde_json::Value>)> = sqlx::query_as(
            "SELECT account_id, table_name, ttl_attribute, stream_specification FROM tables \
             WHERE ttl_attribute IS NOT NULL AND ttl_index_ready = TRUE AND table_status = 'ACTIVE'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        rows.into_iter()
            .filter_map(
                |(account_id, table_name, ttl_attribute, stream_spec_json)| match stream_enabled(
                    stream_spec_json,
                ) {
                    Ok(true) => Some(Ok((account_id, table_name, ttl_attribute))),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
            )
            .collect()
    }
}

impl MetadataEngine for TidbEngine {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
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
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let attribute_name = attribute_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let row: Option<(String, Option<serde_json::Value>, String)> = sqlx::query_as(
                "SELECT table_id, stream_specification, table_status \
                 FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let Some((table_id, stream_spec_json, status)) = row else {
                return Err(StorageError::TableNotFound(table_name));
            };
            if status != "ACTIVE" {
                return Err(StorageError::TableNotActive(table_name));
            }

            if enabled {
                let use_native_ttl = configure_ttl_artifacts(
                    &self.data_pool,
                    &table_id,
                    &attribute_name,
                    stream_spec_json,
                )
                .await?;

                sqlx::query(
                    "UPDATE tables SET ttl_attribute = ?, ttl_index_ready = TRUE, \
                         ttl_native_enabled = ? \
                     WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
                )
                .bind(&attribute_name)
                .bind(use_native_ttl)
                .bind(&account_id)
                .bind(&table_name)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            } else {
                drop_ttl_artifacts(&self.data_pool, &table_id).await?;

                sqlx::query(
                    "UPDATE tables SET ttl_attribute = NULL, ttl_index_ready = FALSE, \
                         ttl_native_enabled = FALSE \
                     WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
                )
                .bind(&account_id)
                .bind(&table_name)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }

            Ok(())
        })
    }

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_string();
        let tags = tags.to_vec();
        Box::pin(async move {
            for tag in &tags {
                sqlx::query(
                    "INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?) \
                     ON DUPLICATE KEY UPDATE tag_value = VALUES(tag_value)",
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
        let arn = arn.to_string();
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
        let arn = arn.to_string();
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
        let account_id = account_id.to_string();
        Box::pin(async move {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT table_name, ttl_attribute FROM tables \
                 WHERE account_id = ? AND ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
            )
            .bind(&account_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows)
        })
    }

    fn refresh_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let data_table = data::data_table_name(&table_id);
            let raw_table = data_table.trim_matches('`');
            let (item_count, table_size): (i64, i64) = sqlx::query_as(
                "SELECT COALESCE(TABLE_ROWS, 0), COALESCE(DATA_LENGTH, 0) \
                 FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_name = ?",
            )
            .bind(raw_table)
            .fetch_optional(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .unwrap_or((0, 0));

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
        let account_id = account_id.to_string();
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT table_name FROM tables WHERE account_id = ? AND table_status = 'ACTIVE' ORDER BY table_name",
            )
            .bind(&account_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows.into_iter().map(|(n,)| n).collect())
        })
    }

    fn all_tables_with_ttl(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT account_id, table_name, ttl_attribute FROM tables \
                 WHERE ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows)
        })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String, String)>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT account_id, table_name, ttl_attribute FROM tables \
                 WHERE ttl_attribute IS NOT NULL AND ttl_index_ready = TRUE AND table_status = 'ACTIVE'",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows)
        })
    }

    fn create_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let ttl_attribute = ttl_attribute.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let row: Option<TtlArtifactRow> = sqlx::query_as(
                "SELECT table_id, ttl_attribute, stream_specification, ttl_index_ready \
                 FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (table_id, catalog_ttl_attribute, stream_spec_json, index_ready) =
                row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            if index_ready && catalog_ttl_attribute.as_deref() == Some(ttl_attribute.as_str()) {
                return Ok(());
            }

            let use_native_ttl = configure_ttl_artifacts(
                &self.data_pool,
                &table_id,
                &ttl_attribute,
                stream_spec_json,
            )
            .await?;

            sqlx::query(
                "UPDATE tables SET ttl_index_ready = TRUE, ttl_native_enabled = ? \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(use_native_ttl)
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
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            drop_ttl_artifacts(&self.data_pool, &table_id).await?;

            sqlx::query(
                "UPDATE tables SET ttl_index_ready = FALSE, ttl_native_enabled = FALSE \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(())
        })
    }

    fn find_expired_items_indexed(
        &self,
        account_id: &str,
        table_name: &str,
        _ttl_attribute: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(account_id)
            .bind(table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let data_table = data::data_table_name(&table_id);

            let now_epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
            let now_i64 = i64::try_from(now_epoch).unwrap_or(i64::MAX);
            let sql = format!(
                "SELECT item_data FROM {data_table} \
                 WHERE `{TTL_EXPIRES_AT_COLUMN}` IS NOT NULL \
                   AND `{TTL_EXPIRES_AT_COLUMN}` <= FROM_UNIXTIME(?) \
                 ORDER BY `{TTL_EXPIRES_AT_COLUMN}` \
                 LIMIT ?"
            );
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(now_i64)
                .bind(limit_i64)
                .fetch_all(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            rows.into_iter().map(|(v,)| data::json_to_item(v)).collect()
        })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT account_id, table_name FROM tables \
                 WHERE table_status = 'ACTIVE' ORDER BY account_id, table_name",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows)
        })
    }
}
