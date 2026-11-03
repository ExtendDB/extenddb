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
    /// The id, the item count and the summed item size of the table named
    /// `table_name`, read without the engine write lock in one read
    /// transaction, so all three come from the same snapshot. `None` when the
    /// table is not `ACTIVE`, for example a restore target that is still
    /// being filled.
    async fn scan_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<(String, i64, i64)>, StorageError> {
        let internal = |e: sqlx::Error| StorageError::Internal(e.to_string());
        let mut tx = self.pool.begin().await.map_err(internal)?;
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT table_id, table_status FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        let Some((table_id, status)) = row else {
            return Err(StorageError::TableNotFound(table_name.to_owned()));
        };
        if status != "ACTIVE" {
            return Ok(None);
        }
        let data_table = data::data_table_name(&table_id);
        let (item_count, table_size): (i64, i64) = sqlx::query_as(&format!(
            "SELECT COUNT(*), COALESCE(SUM(length(item_data)), 0) FROM {data_table}"
        ))
        .fetch_one(&mut *tx)
        .await
        .map_err(internal)?;
        tx.rollback().await.map_err(internal)?;
        Ok(Some((table_id, item_count, table_size)))
    }

    /// Record the counts of the scan of `table_id`. The caller holds the
    /// engine write lock. A table deleted, or deleted and created again under
    /// the same name, since the scan has another `table_id` or is no longer
    /// `ACTIVE`, and keeps its counts.
    async fn store_table_size(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        item_count: i64,
        table_size: i64,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE tables SET item_count = ?, table_size_bytes = ? \
             WHERE account_id = ? AND table_name = ? AND table_id = ? \
             AND table_status = 'ACTIVE'",
        )
        .bind(item_count)
        .bind(table_size)
        .bind(account_id)
        .bind(table_name)
        .bind(table_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

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
            // D1: every writer holds the engine write lock.
            let _writer = self.write_lock.lock().await;
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
            // D1: every writer holds the engine write lock.
            let _writer = self.write_lock.lock().await;
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
            // D1: every writer holds the engine write lock.
            let _writer = self.write_lock.lock().await;
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
            // D1: every writer holds the engine write lock. CREATE INDEX over a
            // populated data table is the slowest single statement this backend
            // issues; unserialized it can hold the SQLite write lock past a
            // concurrent writer's busy_timeout.
            let _writer = self.write_lock.lock().await;
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
        _ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            // D1: every writer holds the engine write lock.
            let _writer = self.write_lock.lock().await;
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
            // The scan only reads, so it takes no lock (D1). Holding the write
            // lock across it made writes wait for a full scan of each table in
            // turn.
            let Some((table_id, item_count, table_size)) =
                self.scan_table_size(&account_id, &table_name).await?
            else {
                return Ok(());
            };
            // D1: every writer holds the engine write lock.
            let _writer = self.write_lock.lock().await;
            self.store_table_size(&account_id, &table_name, &table_id, item_count, table_size)
                .await?;
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

#[cfg(test)]
mod size_refresh_tests {
    use std::time::Duration;

    use extenddb_core::types::{Item, TableKeyInfo};
    use extenddb_storage::{DataEngine, MetadataEngine};
    use serde_json::json;

    use crate::SqliteEngine;

    const ACCOUNT: &str = "000000000000";

    async fn engine() -> SqliteEngine {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::migrations::apply_for_tests(&engine.pool).await;
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(ACCOUNT)
            .execute(&engine.pool)
            .await
            .expect("account");
        // Zero control-plane delay: tables are ACTIVE at create time.
        sqlx::query(
            "INSERT OR REPLACE INTO settings (key, value) \
             VALUES ('control_plane_delay_seconds', '0')",
        )
        .execute(&engine.pool)
        .await
        .expect("settings");
        engine
    }

    /// A table with a string hash key holding `items` items; returns its id.
    async fn table_with_items(engine: &SqliteEngine, name: &str, items: usize) -> String {
        let input = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        let desc = engine
            .create_table_impl(ACCOUNT, input, false)
            .await
            .expect("create");
        let key_info = TableKeyInfo {
            table_name: name.to_owned(),
            account_id: ACCOUNT.to_owned(),
            table_id: desc.table_id.clone(),
            key_schema: desc.key_schema.clone(),
            base_key_schema: desc.key_schema.clone(),
            attribute_definitions: desc.attribute_definitions.clone(),
            ..Default::default()
        };
        for i in 0..items {
            let item: Item = serde_json::from_value(json!({
                "pk": {"S": format!("p{i}")},
                "v": {"S": "x"}
            }))
            .expect("item");
            engine
                .put_item(
                    &key_info,
                    item,
                    false,
                    None,
                    &extenddb_core::expression::ExpressionMaps::default(),
                    None,
                )
                .await
                .expect("put");
        }
        desc.table_id
    }

    async fn stored_counts(engine: &SqliteEngine, name: &str) -> (i64, i64) {
        sqlx::query_as(
            "SELECT item_count, table_size_bytes FROM tables \
             WHERE account_id = ? AND table_name = ?",
        )
        .bind(ACCOUNT)
        .bind(name)
        .fetch_one(&engine.pool)
        .await
        .expect("counts")
    }

    #[tokio::test]
    async fn refresh_stores_the_item_count_and_size() {
        let engine = engine().await;
        let table_id = table_with_items(&engine, "t", 3).await;
        MetadataEngine::refresh_table_size(&engine, ACCOUNT, "t")
            .await
            .expect("refresh");
        let (scanned_id, count, size) = engine
            .scan_table_size(ACCOUNT, "t")
            .await
            .expect("scan")
            .expect("active");
        assert_eq!(scanned_id, table_id);
        assert_eq!(count, 3);
        assert!(size > 0, "{size}");
        assert_eq!(stored_counts(&engine, "t").await, (count, size));
    }

    /// The scan takes no lock, so it runs while a writer holds the engine
    /// write lock (#719).
    #[tokio::test]
    async fn the_scan_runs_while_a_writer_holds_the_lock() {
        let engine = engine().await;
        table_with_items(&engine, "t", 3).await;
        let _writer = engine.write_lock.lock().await;
        let scanned = tokio::time::timeout(
            Duration::from_secs(10),
            engine.scan_table_size(ACCOUNT, "t"),
        )
        .await
        .expect("the scan waited for the write lock")
        .expect("scan")
        .expect("active");
        assert_eq!(scanned.1, 3);
    }

    /// A table that is not ACTIVE, such as a restore target still being
    /// filled, is neither scanned nor given counts.
    #[tokio::test]
    async fn a_table_that_is_not_active_is_not_counted() {
        let engine = engine().await;
        table_with_items(&engine, "t", 3).await;
        sqlx::query("UPDATE tables SET table_status = 'CREATING' WHERE table_name = 't'")
            .execute(&engine.pool)
            .await
            .expect("status");
        assert_eq!(
            engine.scan_table_size(ACCOUNT, "t").await.expect("scan"),
            None
        );
        MetadataEngine::refresh_table_size(&engine, ACCOUNT, "t")
            .await
            .expect("refresh");
        assert_eq!(stored_counts(&engine, "t").await, (0, 0));
    }

    /// Storing the counts is a write, so the refresh waits for the lock before
    /// it stores them (D1).
    #[tokio::test]
    async fn refresh_takes_the_write_lock_to_store_the_counts() {
        let engine = engine().await;
        table_with_items(&engine, "t", 3).await;
        let guard = engine.write_lock.lock().await;
        let e = engine.clone();
        let task =
            tokio::spawn(async move { MetadataEngine::refresh_table_size(&e, ACCOUNT, "t").await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !task.is_finished(),
            "the refresh stored its counts while the write lock was held"
        );
        assert_eq!(stored_counts(&engine, "t").await, (0, 0));
        drop(guard);
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the refresh did not finish after the lock was released")
            .expect("task")
            .expect("refresh");
        assert_eq!(stored_counts(&engine, "t").await.0, 3);
    }

    /// Counts scanned from a table that was then deleted, or deleted and
    /// created again under the same name, are not stored on the name's row.
    #[tokio::test]
    async fn counts_for_another_table_id_are_not_stored() {
        let engine = engine().await;
        let table_id = table_with_items(&engine, "t", 3).await;
        let _writer = engine.write_lock.lock().await;
        engine
            .store_table_size(ACCOUNT, "t", "another-table-id", 7, 70)
            .await
            .expect("store");
        assert_eq!(stored_counts(&engine, "t").await, (0, 0));
        engine
            .store_table_size(ACCOUNT, "t", &table_id, 7, 70)
            .await
            .expect("store");
        assert_eq!(stored_counts(&engine, "t").await, (7, 70));
    }
}
