// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `PostgresEngine`.

use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::PostgresEngine;
use crate::data;

type TtlTableInfo = (String, String, String);

fn sql_string_literal(value: &str) -> Result<String, StorageError> {
    if value.contains('\0') {
        return Err(StorageError::Validation(
            "TimeToLiveSpecification.AttributeName contains an unsupported null character"
                .to_owned(),
        ));
    }

    for suffix in 0..1024 {
        let delimiter = format!("$edb_ttl_{suffix}$");
        if !value.contains(&delimiter) {
            return Ok(format!("{delimiter}{value}{delimiter}"));
        }
    }

    Err(StorageError::Validation(
        "TimeToLiveSpecification.AttributeName contains unsupported SQL delimiter sequences"
            .to_owned(),
    ))
}

impl MetadataEngine for PostgresEngine {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let row: Option<(Option<String>,)> = sqlx::query_as(
                "SELECT ttl_attribute FROM tables WHERE account_id = $1 AND table_name = $2",
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
            let ttl_val: Option<&str> = if enabled { Some(&attribute_name) } else { None };
            let index_ready = false;

            let result = sqlx::query(
                "UPDATE tables SET ttl_attribute = $1, ttl_index_ready = $4 \
                 WHERE account_id = $2 AND table_name = $3 AND table_status = 'ACTIVE'",
            )
            .bind(ttl_val)
            .bind(&account_id)
            .bind(&table_name)
            .bind(index_ready)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if result.rows_affected() == 0 {
                let exists: Option<(String,)> = sqlx::query_as(
                    "SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2",
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

    fn apply_ttl_update(
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
            if !enabled {
                self.drop_ttl_expiration_artifacts(&account_id, &table_name)
                    .await?;
            }

            MetadataEngine::update_ttl(self, &account_id, &table_name, &attribute_name, enabled)
                .await?;

            if enabled
                && let Err(err) = self
                    .ensure_ttl_expiration_artifacts(&account_id, &table_name, &attribute_name)
                    .await
            {
                tracing::warn!("TTL expiration artifact creation deferred for {table_name}: {err}");
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
                    "INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES ($1, $2, $3) \
                     ON CONFLICT (resource_arn, tag_key) DO UPDATE SET tag_value = EXCLUDED.tag_value",
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
                sqlx::query("DELETE FROM tags WHERE resource_arn = $1 AND tag_key = $2")
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
                "SELECT tag_key, tag_value FROM tags WHERE resource_arn = $1 ORDER BY tag_key",
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
}

impl PostgresEngine {
    pub(crate) fn refresh_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let data_table = data::data_table_name(&table_id);

            let count_sql = format!("SELECT COUNT(*) FROM {data_table}");
            let (item_count,): (i64,) = sqlx::query_as(&count_sql)
                .fetch_one(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let size_sql = format!("SELECT COALESCE(pg_total_relation_size('{data_table}'), 0)");
            let (table_size,): (i64,) = sqlx::query_as(&size_sql)
                .fetch_one(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query(
                "UPDATE tables SET item_count = $1, table_size_bytes = $2 \
                 WHERE account_id = $3 AND table_name = $4 AND table_status = 'ACTIVE'",
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

    pub(crate) fn all_tables_with_ttl(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
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

    pub(crate) fn ttl_sweeper_tables(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
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

    pub(crate) fn ensure_ttl_expiration_artifacts(
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
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            // Strip quotes for index name (data_table_name returns quoted identifier).
            let data_table = data::data_table_name(&table_id);
            let bare_table = data_table.trim_matches('"');
            let index_name = format!("idx_ttl_{bare_table}");
            let ttl_attribute = sql_string_literal(&ttl_attribute)?;

            let sql = format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{index_name}\" \
                 ON {data_table} (((item_data->{ttl_attribute}->>'N')::BIGINT)) \
                 WHERE (item_data->{ttl_attribute}->>'N') IS NOT NULL"
            );
            sqlx::query(&sql)
                .execute(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(format!("TTL index creation failed: {e}")))?;

            sqlx::query(
                "UPDATE tables SET ttl_index_ready = TRUE \
                 WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(())
        })
    }

    pub(crate) fn drop_ttl_expiration_artifacts(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let data_table = data::data_table_name(&table_id);
            let bare_table = data_table.trim_matches('"');
            let index_name = format!("idx_ttl_{bare_table}");

            sqlx::query(
                "UPDATE tables SET ttl_index_ready = FALSE \
                 WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let sql = format!("DROP INDEX IF EXISTS \"{index_name}\"");
            sqlx::query(&sql)
                .execute(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(format!("TTL index drop failed: {e}")))?;

            Ok(())
        })
    }

    pub(crate) fn find_expired_items_for_sweeper(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let ttl_attribute = ttl_attribute.to_string();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let (table_id,): (String,) = sqlx::query_as(
                "SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2",
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
                 WHERE (item_data->$1->>'N')::BIGINT BETWEEN 1 AND $2 \
                 ORDER BY (item_data->$1->>'N')::BIGINT \
                 LIMIT $3"
            );
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(&ttl_attribute)
                .bind(now_i64)
                .bind(limit_i64)
                .fetch_all(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            rows.into_iter().map(|(v,)| data::json_to_item(v)).collect()
        })
    }

    pub(crate) fn all_active_tables(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
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

#[cfg(test)]
mod tests {
    use super::sql_string_literal;

    #[test]
    fn ttl_attribute_literal_escapes_postgres_quotes() {
        assert_eq!(
            sql_string_literal("it's ttl").expect("literal"),
            "$edb_ttl_0$it's ttl$edb_ttl_0$"
        );
        assert_eq!(
            sql_string_literal("过期\"ttl").expect("literal"),
            "$edb_ttl_0$过期\"ttl$edb_ttl_0$"
        );
        assert_eq!(
            sql_string_literal("$edb_ttl_0$").expect("literal"),
            "$edb_ttl_1$$edb_ttl_0$$edb_ttl_1$"
        );
    }
}
