// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` trait implementation for `TidbEngine`.

use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::MetadataEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::TidbEngine;
use crate::data;
use crate::tidb_util::{execute_tidb_idempotent_ddl, is_table_not_found_tidb_storage_error};

const TTL_EXPIRES_AT_COLUMN: &str = "_edb_ttl_expires_at";
const TTL_EXPIRES_AT_INDEX: &str = "_edb_ttl_expires_at_idx";
const LEGACY_TTL_EPOCH_COLUMN: &str = "_edb_ttl_epoch";
const LEGACY_TTL_EPOCH_INDEX: &str = "_edb_ttl_epoch_idx";
const TTL_STATUS_DISABLED: &str = "DISABLED";
const TTL_STATUS_ENABLING: &str = "ENABLING";
const TTL_STATUS_ENABLED: &str = "ENABLED";
const TTL_STATUS_DISABLING: &str = "DISABLING";

type TtlArtifactRow = (String, Option<String>, String);

struct FixedNativeTtl<'a> {
    pool: &'a sqlx::MySqlPool,
    table: &'static str,
    ttl_expr: &'static str,
    job_interval: &'static str,
}

fn ttl_json_path(ttl_attribute: &str) -> String {
    let quoted_attr = serde_json::to_string(ttl_attribute)
        .expect("serializing a Rust string into a JSON string cannot fail");
    format!("$.{quoted_attr}.N")
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

async fn data_table_has_native_ttl(
    pool: &sqlx::MySqlPool,
    data_table: &str,
) -> Result<bool, StorageError> {
    let create_table = show_create_table(pool, data_table).await?;
    Ok(create_table_has_native_ttl(&create_table))
}

async fn native_ttl_needs_repair(
    pool: &sqlx::MySqlPool,
    data_table: &str,
) -> Result<bool, StorageError> {
    let create_table = show_create_table(pool, data_table).await?;
    Ok(!create_table_has_native_ttl(&create_table) || create_table_has_disabled_ttl(&create_table))
}

async fn show_create_table(
    pool: &sqlx::MySqlPool,
    data_table: &str,
) -> Result<String, StorageError> {
    let (_table_name, create_table): (String, String) =
        sqlx::query_as(&format!("SHOW CREATE TABLE {data_table}"))
            .fetch_one(pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
    let create_table = create_table.to_ascii_uppercase();
    Ok(create_table)
}

pub(crate) fn create_table_has_native_ttl(create_table: &str) -> bool {
    create_table.contains(" TTL =")
        || create_table.contains(" TTL=")
        || create_table.contains("/*T![TTL] TTL =")
        || create_table.contains("/*T![TTL] TTL=")
}

pub(crate) fn create_table_has_disabled_ttl(create_table: &str) -> bool {
    create_table.contains("TTL_ENABLE = 'OFF'") || create_table.contains("TTL_ENABLE='OFF'")
}

fn table_accepts_native_schema_change(status: &str) -> bool {
    matches!(status, "ACTIVE" | "UPDATING")
}

fn ttl_status_from_catalog(status: &str) -> Result<TimeToLiveStatus, StorageError> {
    match status {
        TTL_STATUS_ENABLING => Ok(TimeToLiveStatus::Enabling),
        TTL_STATUS_ENABLED => Ok(TimeToLiveStatus::Enabled),
        TTL_STATUS_DISABLING => Ok(TimeToLiveStatus::Disabling),
        TTL_STATUS_DISABLED => Ok(TimeToLiveStatus::Disabled),
        other => Err(StorageError::Internal(format!(
            "unknown TiDB TTL catalog status: {other}"
        ))),
    }
}

pub(crate) async fn drop_ttl_artifacts(
    pool: &sqlx::MySqlPool,
    table_id: &str,
) -> Result<(), StorageError> {
    let data_table = data::data_table_name(table_id);

    if data_table_has_native_ttl(pool, &data_table).await? {
        let sql = format!("ALTER TABLE {data_table} REMOVE TTL");
        execute_tidb_idempotent_ddl(pool, "drop_ttl_artifacts_remove_ttl", &sql).await?;
    }

    let sql = drop_indexes_sql(&data_table, &[TTL_EXPIRES_AT_INDEX, LEGACY_TTL_EPOCH_INDEX]);
    execute_tidb_idempotent_ddl(pool, "drop_ttl_artifacts_drop_indexes", &sql).await?;

    let sql = drop_columns_sql(
        &data_table,
        &[TTL_EXPIRES_AT_COLUMN, LEGACY_TTL_EPOCH_COLUMN],
    );
    execute_tidb_idempotent_ddl(pool, "drop_ttl_artifacts_drop_columns", &sql).await?;

    Ok(())
}

async fn drop_legacy_ttl_lookup_artifacts(
    pool: &sqlx::MySqlPool,
    table_id: &str,
) -> Result<(), StorageError> {
    let data_table = data::data_table_name(table_id);

    let sql = drop_indexes_sql(&data_table, &[TTL_EXPIRES_AT_INDEX, LEGACY_TTL_EPOCH_INDEX]);
    execute_tidb_idempotent_ddl(pool, "drop_legacy_ttl_lookup_artifacts_drop_indexes", &sql)
        .await?;

    let sql = drop_columns_sql(&data_table, &[LEGACY_TTL_EPOCH_COLUMN]);
    execute_tidb_idempotent_ddl(
        pool,
        "drop_legacy_ttl_lookup_artifacts_drop_epoch_column",
        &sql,
    )
    .await?;

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
        "ALTER TABLE {data_table} ADD COLUMN IF NOT EXISTS `{TTL_EXPIRES_AT_COLUMN}` DATETIME \
         AS ({ttl_expr}) VIRTUAL"
    );
    execute_tidb_idempotent_ddl(pool, "add_ttl_generated_column", &add_column).await?;

    Ok(())
}

async fn configure_native_ttl(
    pool: &sqlx::MySqlPool,
    table_id: &str,
    ttl_attribute: &str,
) -> Result<(), StorageError> {
    add_ttl_generated_column(pool, table_id, ttl_attribute).await?;

    let data_table = data::data_table_name(table_id);
    let enable_ttl = native_ttl_attribute_sql(&data_table);
    execute_tidb_idempotent_ddl(pool, "configure_native_ttl", &enable_ttl).await?;
    let enable_ttl_jobs = native_ttl_enable_sql(&data_table);
    execute_tidb_idempotent_ddl(pool, "configure_native_ttl_enable_jobs", &enable_ttl_jobs).await?;
    drop_legacy_ttl_lookup_artifacts(pool, table_id).await?;

    Ok(())
}

fn native_ttl_attribute_sql(data_table: &str) -> String {
    format!(
        "ALTER TABLE {data_table} TTL = `{TTL_EXPIRES_AT_COLUMN}` + INTERVAL 0 SECOND TTL_JOB_INTERVAL = '1h'"
    )
}

fn fixed_native_ttl_attribute_sql(data_table: &str, ttl_expr: &str, job_interval: &str) -> String {
    format!("ALTER TABLE {data_table} TTL = {ttl_expr} TTL_JOB_INTERVAL = '{job_interval}'")
}

fn native_ttl_enable_sql(data_table: &str) -> String {
    format!("ALTER TABLE {data_table} TTL_ENABLE = 'ON'")
}

fn drop_indexes_sql(data_table: &str, index_names: &[&str]) -> String {
    let specs = index_names
        .iter()
        .map(|index_name| format!("DROP INDEX IF EXISTS `{index_name}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("ALTER TABLE {data_table} {specs}")
}

fn drop_columns_sql(data_table: &str, column_names: &[&str]) -> String {
    let specs = column_names
        .iter()
        .map(|column_name| format!("DROP COLUMN IF EXISTS `{column_name}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("ALTER TABLE {data_table} {specs}")
}

impl TidbEngine {
    async fn claim_ttl_enable(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let row: Option<(String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT table_id, table_status, ttl_attribute, ttl_status \
             FROM tables WHERE account_id = ? AND table_name = ? FOR UPDATE",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((table_id, status, ttl_attribute, ttl_status)) = row else {
            return Err(StorageError::TableNotFound(table_name.to_owned()));
        };
        if !table_accepts_native_schema_change(&status) {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }
        if ttl_status != TTL_STATUS_DISABLED || ttl_attribute.is_some() {
            let message = if ttl_status == TTL_STATUS_ENABLED {
                "TimeToLive is already enabled"
            } else {
                "TimeToLive is currently being modified"
            };
            return Err(StorageError::Validation(message.to_owned()));
        }

        sqlx::query(
            "UPDATE tables SET ttl_attribute = ?, ttl_status = 'ENABLING', \
             table_status = 'UPDATING', status_transition_at = CURRENT_TIMESTAMP(6) \
             WHERE table_id = ?",
        )
        .bind(attribute_name)
        .bind(&table_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn finalize_ttl_enable(
        &self,
        table_id: &str,
        attribute_name: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE tables SET ttl_status = 'ENABLED' \
             WHERE table_id = ? AND ttl_attribute = ? AND ttl_status = 'ENABLING' \
               AND table_status IN ('ACTIVE', 'UPDATING')",
        )
        .bind(table_id)
        .bind(attribute_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        if result.rows_affected() == 1 {
            return Ok(());
        }

        let row: Option<(Option<String>, String, String)> = sqlx::query_as(
            "SELECT ttl_attribute, ttl_status, table_status FROM tables WHERE table_id = ?",
        )
        .bind(table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        match row {
            Some((Some(attr), status, _))
                if attr == attribute_name && status == TTL_STATUS_ENABLED =>
            {
                Ok(())
            }
            Some((_, _, table_status)) if !table_accepts_native_schema_change(&table_status) => {
                Err(StorageError::TableNotActive(table_id.to_owned()))
            }
            Some((_, ttl_status, _)) => Err(StorageError::Internal(format!(
                "unexpected TiDB TTL status while finalizing enable for {table_id}: {ttl_status}"
            ))),
            None => Err(StorageError::TableNotFound(table_id.to_owned())),
        }
    }

    async fn claim_ttl_disable(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let row: Option<(String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT table_id, table_status, ttl_attribute, ttl_status \
             FROM tables WHERE account_id = ? AND table_name = ? FOR UPDATE",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((table_id, status, ttl_attribute, ttl_status)) = row else {
            return Err(StorageError::TableNotFound(table_name.to_owned()));
        };
        if !table_accepts_native_schema_change(&status) {
            return Err(StorageError::TableNotActive(table_name.to_owned()));
        }
        if ttl_status == TTL_STATUS_ENABLING || ttl_status == TTL_STATUS_DISABLING {
            return Err(StorageError::Validation(
                "TimeToLive is currently being modified".to_owned(),
            ));
        }
        if ttl_status == TTL_STATUS_DISABLED {
            return Err(StorageError::Validation(
                "TimeToLive is already disabled".to_owned(),
            ));
        }
        let Some(_ttl_attribute) = ttl_attribute else {
            return Err(StorageError::Internal(
                "TiDB TTL catalog status is ENABLED without an attribute".to_owned(),
            ));
        };
        if ttl_status != TTL_STATUS_ENABLED {
            return Err(StorageError::Validation(
                "TimeToLive is currently being modified".to_owned(),
            ));
        }

        sqlx::query(
            "UPDATE tables SET ttl_status = 'DISABLING', \
             table_status = 'UPDATING', status_transition_at = CURRENT_TIMESTAMP(6) \
             WHERE table_id = ?",
        )
        .bind(&table_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn finalize_ttl_disable(
        &self,
        table_id: &str,
        attribute_name: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE tables SET ttl_attribute = NULL, ttl_status = 'DISABLED' \
             WHERE table_id = ? AND ttl_attribute = ? AND ttl_status = 'DISABLING'",
        )
        .bind(table_id)
        .bind(attribute_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn ttl_table_is_deleting_or_absent(&self, table_id: &str) -> Result<bool, StorageError> {
        let status: Option<String> =
            sqlx::query_scalar("SELECT table_status FROM tables WHERE table_id = ?")
                .bind(table_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(matches!(status.as_deref(), None | Some("DELETING")))
    }

    async fn ignore_stale_ttl_error_if_table_deleted(
        &self,
        table_id: &str,
        err: StorageError,
    ) -> Result<(), StorageError> {
        let stale_physical_table = is_table_not_found_tidb_storage_error(&err);
        let stale_catalog_state = matches!(
            &err,
            StorageError::TableNotActive(_) | StorageError::TableNotFound(_)
        );
        if (stale_physical_table || stale_catalog_state)
            && self.ttl_table_is_deleting_or_absent(table_id).await?
        {
            Ok(())
        } else {
            Err(err)
        }
    }

    pub(crate) async fn reconcile_native_ttl_transition(
        &self,
        table_id: &str,
        ttl_attribute: Option<&str>,
        ttl_status: &str,
    ) -> Result<(), StorageError> {
        match ttl_status {
            TTL_STATUS_ENABLING => {
                let ttl_attribute = ttl_attribute.ok_or_else(|| {
                    StorageError::Internal(format!(
                        "TiDB TTL catalog status is ENABLING without an attribute for {table_id}"
                    ))
                })?;
                match configure_native_ttl(&self.data_pool, table_id, ttl_attribute).await {
                    Ok(()) => match self.finalize_ttl_enable(table_id, ttl_attribute).await {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            self.ignore_stale_ttl_error_if_table_deleted(table_id, err)
                                .await
                        }
                    },
                    Err(err) => {
                        self.ignore_stale_ttl_error_if_table_deleted(table_id, err)
                            .await
                    }
                }
            }
            TTL_STATUS_DISABLING => {
                let ttl_attribute = ttl_attribute.ok_or_else(|| {
                    StorageError::Internal(format!(
                        "TiDB TTL catalog status is DISABLING without an attribute for {table_id}"
                    ))
                })?;
                match drop_ttl_artifacts(&self.data_pool, table_id).await {
                    Ok(()) => self.finalize_ttl_disable(table_id, ttl_attribute).await,
                    Err(err) => {
                        self.ignore_stale_ttl_error_if_table_deleted(table_id, err)
                            .await
                    }
                }
            }
            TTL_STATUS_ENABLED | TTL_STATUS_DISABLED => Ok(()),
            other => Err(StorageError::Internal(format!(
                "unknown TiDB TTL catalog status: {other}"
            ))),
        }
    }

    pub(crate) async fn repair_native_ttl(&self) -> Result<(), StorageError> {
        self.repair_fixed_native_ttl().await?;
        self.repair_user_table_native_ttl().await?;
        Ok(())
    }

    async fn repair_fixed_native_ttl(&self) -> Result<(), StorageError> {
        for fixed in [
            FixedNativeTtl {
                pool: &self.pool,
                table: "metrics_samples",
                ttl_expr: "`bucket` + INTERVAL 24 HOUR",
                job_interval: "1h",
            },
            FixedNativeTtl {
                pool: &self.pool,
                table: "login_attempts",
                ttl_expr: "`attempted_at` + INTERVAL 24 HOUR",
                job_interval: "1h",
            },
            FixedNativeTtl {
                pool: &self.pool,
                table: "iam_sessions",
                ttl_expr: "`expires_at` + INTERVAL 24 HOUR",
                job_interval: "1h",
            },
            FixedNativeTtl {
                pool: &self.data_pool,
                table: "stream_records",
                ttl_expr: "`created_at` + INTERVAL 24 HOUR",
                job_interval: "1h",
            },
            FixedNativeTtl {
                pool: &self.data_pool,
                table: "idempotency_tokens",
                ttl_expr: "`created_at` + INTERVAL 600 SECOND",
                job_interval: "10m",
            },
        ] {
            if !native_ttl_needs_repair(fixed.pool, fixed.table).await? {
                continue;
            }

            let ttl =
                fixed_native_ttl_attribute_sql(fixed.table, fixed.ttl_expr, fixed.job_interval);
            execute_tidb_idempotent_ddl(fixed.pool, "repair_fixed_native_ttl", &ttl).await?;
            let enable = native_ttl_enable_sql(fixed.table);
            execute_tidb_idempotent_ddl(fixed.pool, "repair_fixed_native_ttl_enable_jobs", &enable)
                .await?;
        }

        Ok(())
    }

    async fn repair_user_table_native_ttl(&self) -> Result<(), StorageError> {
        let rows: Vec<(String, Option<String>, String)> = sqlx::query_as(
            "SELECT table_id, ttl_attribute, ttl_status FROM tables \
             WHERE (ttl_attribute IS NOT NULL OR ttl_status <> 'DISABLED') \
               AND table_status IN ('ACTIVE', 'UPDATING')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        for (table_id, ttl_attribute, ttl_status) in rows {
            let Some(ttl_attribute) = ttl_attribute else {
                sqlx::query("UPDATE tables SET ttl_status = 'DISABLED' WHERE table_id = ?")
                    .bind(&table_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                continue;
            };

            if ttl_status == TTL_STATUS_DISABLING {
                drop_ttl_artifacts(&self.data_pool, &table_id).await?;
                self.finalize_ttl_disable(&table_id, &ttl_attribute).await?;
                continue;
            }

            if ttl_status == TTL_STATUS_DISABLED {
                drop_ttl_artifacts(&self.data_pool, &table_id).await?;
                sqlx::query("UPDATE tables SET ttl_attribute = NULL WHERE table_id = ?")
                    .bind(&table_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                continue;
            }

            if ttl_status != TTL_STATUS_ENABLING && ttl_status != TTL_STATUS_ENABLED {
                return Err(StorageError::Internal(format!(
                    "unknown TiDB TTL catalog status: {ttl_status}"
                )));
            }

            let data_table = data::data_table_name(&table_id);
            if ttl_status == TTL_STATUS_ENABLED
                && !native_ttl_needs_repair(&self.data_pool, &data_table).await?
            {
                continue;
            }

            configure_native_ttl(&self.data_pool, &table_id, &ttl_attribute).await?;
            if ttl_status == TTL_STATUS_ENABLING {
                self.finalize_ttl_enable(&table_id, &ttl_attribute).await?;
            }
        }

        Ok(())
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
            let row: Option<(Option<String>, String)> = sqlx::query_as(
                "SELECT ttl_attribute, ttl_status FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (ttl_attr, ttl_status) =
                row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            let time_to_live_status = ttl_status_from_catalog(&ttl_status)?;

            Ok(TimeToLiveDescription {
                time_to_live_status,
                attribute_name: ttl_attr,
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
            if enabled {
                self.claim_ttl_enable(&account_id, &table_name, &attribute_name)
                    .await?;
            } else {
                self.claim_ttl_disable(&account_id, &table_name).await?;
            }
            self.control_plane_notify.notify_one();

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
        MetadataEngine::update_ttl(self, account_id, table_name, attribute_name, enabled)
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
                 WHERE account_id = ? AND ttl_status = 'ENABLED' AND table_status = 'ACTIVE'",
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
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = ? AND table_name = ?)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if !exists {
                return Err(StorageError::TableNotFound(table_name));
            }

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
                 WHERE ttl_status = 'ENABLED' AND table_status = 'ACTIVE'",
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
            // TiDB native TTL owns item expiration. Do not expose user tables
            // to the generic indexed TTL sweeper.
            Ok(Vec::new())
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
                "SELECT table_id, ttl_attribute, ttl_status \
                 FROM tables WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (table_id, catalog_ttl_attribute, ttl_status) =
                row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
            if ttl_status == TTL_STATUS_ENABLED
                && catalog_ttl_attribute.as_deref() == Some(ttl_attribute.as_str())
            {
                return Ok(());
            }
            if ttl_status != TTL_STATUS_ENABLING {
                return Ok(());
            }
            if catalog_ttl_attribute.as_deref() != Some(ttl_attribute.as_str()) {
                return Err(StorageError::Validation(
                    "TimeToLive is currently being modified".to_owned(),
                ));
            }

            configure_native_ttl(&self.data_pool, &table_id, &ttl_attribute).await?;

            sqlx::query(
                "UPDATE tables SET ttl_status = 'ENABLED' \
                 WHERE account_id = ? AND table_name = ? AND ttl_status = 'ENABLING' \
                   AND ttl_attribute = ?",
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
                "UPDATE tables SET ttl_attribute = NULL, ttl_status = 'DISABLED' \
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
        _account_id: &str,
        _table_name: &str,
        _ttl_attribute: &str,
        _limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        Box::pin(async move {
            // TiDB native TTL deletes expired rows inside TiDB. Returning rows
            // here would reintroduce an application-level TTL deletion path and
            // duplicate native cleanup work.
            Ok(Vec::new())
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

#[cfg(test)]
mod tests {
    use super::{
        create_table_has_disabled_ttl, create_table_has_native_ttl, drop_columns_sql,
        drop_indexes_sql, fixed_native_ttl_attribute_sql, native_ttl_attribute_sql,
        native_ttl_enable_sql, table_accepts_native_schema_change, ttl_json_path,
        ttl_status_from_catalog,
    };
    use extenddb_core::types::TimeToLiveStatus;

    #[test]
    fn native_ttl_configuration_reenables_ttl_jobs() {
        assert_eq!(
            native_ttl_attribute_sql("`_ddb_table`"),
            "ALTER TABLE `_ddb_table` TTL = `_edb_ttl_expires_at` + INTERVAL 0 SECOND TTL_JOB_INTERVAL = '1h'"
        );
        assert_eq!(
            native_ttl_enable_sql("`_ddb_table`"),
            "ALTER TABLE `_ddb_table` TTL_ENABLE = 'ON'"
        );
    }

    #[test]
    fn ttl_json_path_uses_json_quoted_attribute_names() {
        assert_eq!(ttl_json_path("ttl"), "$.\"ttl\".N");
        assert_eq!(ttl_json_path("expires at"), "$.\"expires at\".N");
        assert_eq!(
            ttl_json_path("it's\"ttl\\name"),
            "$.\"it's\\\"ttl\\\\name\".N"
        );
        assert_eq!(ttl_json_path("过期时间"), "$.\"过期时间\".N");
    }

    #[test]
    fn fixed_native_ttl_configuration_reenables_ttl_jobs() {
        assert_eq!(
            fixed_native_ttl_attribute_sql(
                "idempotency_tokens",
                "`created_at` + INTERVAL 600 SECOND",
                "10m",
            ),
            "ALTER TABLE idempotency_tokens TTL = `created_at` + INTERVAL 600 SECOND TTL_JOB_INTERVAL = '10m'"
        );
        assert_eq!(
            native_ttl_enable_sql("idempotency_tokens"),
            "ALTER TABLE idempotency_tokens TTL_ENABLE = 'ON'"
        );
    }

    #[test]
    fn show_create_parser_detects_native_ttl_and_disabled_jobs() {
        let create = "CREATE TABLE `t` (`created_at` timestamp) \
                      /*T![TTL] TTL = `created_at` + INTERVAL 1 HOUR */ \
                      TTL_ENABLE = 'OFF'";

        assert!(create_table_has_native_ttl(create));
        assert!(create_table_has_disabled_ttl(create));

        let real_tidb_comment = "/*T![ttl] TTL=`_edb_ttl_expires_at` + INTERVAL 0 SECOND */ \
             /*T![ttl] TTL_ENABLE='ON' */"
            .to_ascii_uppercase();
        assert!(create_table_has_native_ttl(&real_tidb_comment));
    }

    #[test]
    fn native_schema_changes_can_run_while_online_ddl_is_pending() {
        assert!(table_accepts_native_schema_change("ACTIVE"));
        assert!(table_accepts_native_schema_change("UPDATING"));
        assert!(!table_accepts_native_schema_change("CREATING"));
        assert!(!table_accepts_native_schema_change("DELETING"));
    }

    #[test]
    fn ttl_catalog_status_maps_to_dynamodb_api_states() {
        assert_eq!(
            ttl_status_from_catalog("ENABLING").expect("enabling"),
            TimeToLiveStatus::Enabling
        );
        assert_eq!(
            ttl_status_from_catalog("ENABLED").expect("enabled"),
            TimeToLiveStatus::Enabled
        );
        assert_eq!(
            ttl_status_from_catalog("DISABLING").expect("disabling"),
            TimeToLiveStatus::Disabling
        );
        assert_eq!(
            ttl_status_from_catalog("DISABLED").expect("disabled"),
            TimeToLiveStatus::Disabled
        );
        assert!(ttl_status_from_catalog("READY").is_err());
    }

    #[test]
    fn ttl_artifact_cleanup_uses_multi_schema_drop_ddl() {
        assert_eq!(
            drop_indexes_sql("`_ddb_table`", &["idx_a", "idx_b"]),
            "ALTER TABLE `_ddb_table` DROP INDEX IF EXISTS `idx_a`, DROP INDEX IF EXISTS `idx_b`"
        );
        assert_eq!(
            drop_columns_sql("`_ddb_table`", &["col_a", "col_b"]),
            "ALTER TABLE `_ddb_table` DROP COLUMN IF EXISTS `col_a`, DROP COLUMN IF EXISTS `col_b`"
        );
    }
}
