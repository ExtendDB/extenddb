// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `PostgresEngine`.

use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::PostgresEngine;
use crate::data;

/// A table's TTL description from its catalog columns, as DescribeTimeToLive
/// reports it. Shared with CreateBackup, which records it for DescribeBackup.
pub(crate) fn ttl_description(
    ttl_attr: Option<String>,
    index_ready: Option<bool>,
) -> TimeToLiveDescription {
    match ttl_attr {
        Some(attr) => TimeToLiveDescription {
            // Enabled only once the queue backfill has published
            // readiness; until then pre-existing items are still
            // being registered. Postgres disable tears the queue
            // down synchronously, so it has no Disabling state.
            time_to_live_status: if index_ready == Some(true) {
                TimeToLiveStatus::Enabled
            } else {
                TimeToLiveStatus::Enabling
            },
            attribute_name: Some(attr),
        },
        None => TimeToLiveDescription {
            time_to_live_status: TimeToLiveStatus::Disabled,
            attribute_name: None,
        },
    }
}

/// How long one attempt to drop a table's TTL index waits for its lock.
const TTL_DROP_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(200);
/// The pause between attempts, in which requests queued behind a timed-out
/// attempt get their locks.
const TTL_DROP_RETRY_PAUSE: std::time::Duration = std::time::Duration::from_millis(250);
/// How long `drop_ttl_index` keeps retrying a lock wait before it gives up.
/// The index then stays, which only costs space: readiness is already off.
const TTL_DROP_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

impl MetadataEngine for PostgresEngine {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let row: Option<(Option<String>, Option<bool>)> = sqlx::query_as(
                "SELECT ttl_attribute, ttl_index_ready FROM tables \
                 WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (ttl_attr, index_ready) =
                row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            Ok(ttl_description(ttl_attr, index_ready))
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

    fn tables_with_ttl(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT table_name, ttl_attribute FROM tables \
                 WHERE account_id = $1 AND ttl_attribute IS NOT NULL AND table_status = 'ACTIVE'",
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

    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT table_name FROM tables WHERE account_id = $1 AND table_status = 'ACTIVE' ORDER BY table_name",
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

            // A CONCURRENTLY build that fails partway leaves an INVALID
            // index behind, and IF NOT EXISTS would silently keep it on the
            // retry — publishing readiness over an index that indexes
            // nothing. Drop any invalid leftover before building.
            let invalid: Option<(bool,)> = sqlx::query_as(
                "SELECT i.indisvalid FROM pg_index i \
                 JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = $1",
            )
            .bind(&index_name)
            .fetch_optional(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if invalid == Some((false,)) {
                sqlx::query(&format!("DROP INDEX IF EXISTS \"{index_name}\""))
                    .execute(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(format!("Drop invalid TTL index: {e}")))?;
            }

            let sql = format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{index_name}\" \
                 ON {data_table} (((item_data->'{ttl_attribute}'->>'N')::BIGINT)) \
                 WHERE (item_data->'{ttl_attribute}'->>'N') IS NOT NULL"
            );
            sqlx::query(&sql)
                .execute(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(format!("TTL index creation failed: {e}")))?;

            // Fenced on the attribute this build actually indexed: a stale
            // task surviving a disable / re-enable-with-different-attribute
            // must not certify the new lifecycle's readiness.
            sqlx::query(
                "UPDATE tables SET ttl_index_ready = TRUE \
                 WHERE account_id = $1 AND table_name = $2 AND ttl_attribute = $3",
            )
            .bind(&account_id)
            .bind(&table_name)
            .bind(&ttl_attribute)
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
        _ttl_attribute: &str,
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

            // The drop needs ACCESS EXCLUSIVE, and while it waits, every later
            // lock request on the table queues behind it. So it waits only
            // `TTL_DROP_LOCK_TIMEOUT` per attempt, which bounds the stall a
            // long reader can cause, and retries until `TTL_DROP_BUDGET`.
            let sql = format!("DROP INDEX IF EXISTS \"{index_name}\"");
            let deadline = std::time::Instant::now() + TTL_DROP_BUDGET;
            loop {
                let mut tx =
                    self.data_pool.begin().await.map_err(|e| {
                        StorageError::Internal(format!("TTL index drop failed: {e}"))
                    })?;
                sqlx::query("SELECT set_config('lock_timeout', $1, true)")
                    .bind(format!("{}ms", TTL_DROP_LOCK_TIMEOUT.as_millis()))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(format!("TTL index drop failed: {e}")))?;
                let dropped = sqlx::query(&sql).execute(&mut *tx).await;
                match dropped {
                    Ok(_) => {
                        return tx.commit().await.map_err(|e| {
                            StorageError::Internal(format!("TTL index drop failed: {e}"))
                        });
                    }
                    Err(e)
                        if data::lock_wait_failed(&e) && std::time::Instant::now() < deadline =>
                    {
                        // Roll back and pause, so the requests queued behind
                        // the timed-out wait run before the next attempt.
                        drop(tx);
                        tokio::time::sleep(TTL_DROP_RETRY_PAUSE).await;
                    }
                    Err(e) => {
                        return Err(StorageError::Internal(format!(
                            "TTL index drop failed: {e}"
                        )));
                    }
                }
            }
        })
    }

    fn find_expired_items_indexed(
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
