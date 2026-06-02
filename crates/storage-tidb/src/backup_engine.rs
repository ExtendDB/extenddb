// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup and point-in-time recovery implementation for TiDB storage.
//!
//! TiDB has native physical backup/restore through BR. This module deliberately
//! does not keep a logical `backup_items` copy path: if a requested DynamoDB
//! shape cannot be represented by BR without changing semantics, the TiDB
//! backend returns an explicit validation error.

use std::ffi::OsString;

use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, ContinuousBackupsDescription,
    KeySchemaElement, PointInTimeRecoveryDescription, SourceTableDetails, TableDescription,
};
use extenddb_storage::BackupEngine;
use extenddb_storage::config::NativeBackupConfig;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::table_arn;
use futures::future::BoxFuture;
use tokio::process::Command;

use crate::TidbEngine;
use crate::data::physical_data_table_name;
use crate::metadata_engine::drop_ttl_artifacts;
use crate::table_helpers::TableStats;
use crate::tidb_util::{
    current_tidb_tso, execute_tidb_idempotent_ddl, is_unique_violation, tidb_as_of_tso_clause,
};

const TIDB_BACKUP_BACKEND: &str = "tidb-br";

/// Current epoch milliseconds for unique ARN generation.
fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Convert a TiDB timestamp to epoch seconds as `f64`.
#[allow(clippy::cast_precision_loss)]
fn timestamp_to_epoch(ts: time::OffsetDateTime) -> f64 {
    ts.unix_timestamp() as f64 + f64::from(ts.nanosecond()) / 1_000_000_000.0
}

#[derive(sqlx::FromRow)]
struct BackupSourceRow {
    table_id: String,
    table_arn: String,
    key_schema: serde_json::Value,
    attribute_definitions: serde_json::Value,
    billing_mode: String,
    provisioned_throughput: Option<serde_json::Value>,
    stream_specification: Option<serde_json::Value>,
    deletion_protection_enabled: bool,
}

#[derive(sqlx::FromRow)]
struct BackupRestoreRow {
    key_schema: serde_json::Value,
    attribute_definitions: serde_json::Value,
    billing_mode: String,
    provisioned_throughput: Option<serde_json::Value>,
    backup_backend: String,
    storage_uri: Option<String>,
    physical_table_name: Option<String>,
}

#[derive(sqlx::FromRow)]
struct BackupIndexSnapshotRow {
    index_id: String,
    index_name: String,
    index_type: String,
    key_schema: serde_json::Value,
    projection: serde_json::Value,
    provisioned_throughput: Option<serde_json::Value>,
}

struct BackupMetadataSnapshot {
    source: BackupSourceRow,
    indexes: Vec<BackupIndexSnapshotRow>,
    tags: Vec<(String, String)>,
    stats: TableStats,
    native_snapshot_tso: i64,
}

struct BackupInsert<'a> {
    backup_arn: &'a str,
    backup_name: &'a str,
    table_name: &'a str,
    account_id: &'a str,
    snapshot: &'a BackupMetadataSnapshot,
    storage_uri: &'a str,
    physical_table: &'a str,
}

struct RestoreCatalogInsert<'a> {
    account_id: &'a str,
    table_name: &'a str,
    table_id: &'a str,
    table_arn: &'a str,
    metadata: RestoreCatalogMetadata<'a>,
    indexes: &'a [BackupIndexSnapshotRow],
}

struct RestoreCatalogMetadata<'a> {
    key_schema: &'a serde_json::Value,
    attribute_definitions: &'a serde_json::Value,
    billing_mode: &'a str,
    provisioned_throughput: &'a Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TidbNativeBackupConfig {
    binary: String,
    component: Option<String>,
    pd_endpoint: Option<String>,
    storage_uri: Option<String>,
    send_credentials_to_tikv: Option<bool>,
}

impl TidbNativeBackupConfig {
    pub(crate) fn from_storage_config(config: NativeBackupConfig) -> Self {
        Self {
            binary: non_empty_string(config.binary).unwrap_or_else(|| "tiup".to_owned()),
            component: match config.component {
                Some(component) if component.trim().is_empty() => None,
                Some(component) => Some(component),
                None => Some("br".to_owned()),
            },
            pd_endpoint: non_empty_string(config.coordinator_endpoint),
            storage_uri: non_empty_string(config.storage_uri),
            send_credentials_to_tikv: config.send_credentials_to_storage_nodes,
        }
    }

    fn require_snapshot(&self) -> Result<NativeSnapshotConfig<'_>, StorageError> {
        let pd_endpoint = self.require_pd_endpoint("TiDB native backup")?;
        let storage_uri = self.storage_uri.as_deref().ok_or_else(|| {
            StorageError::Validation(
                "TiDB native backup requires storage.tidb.backup.storage_uri".to_owned(),
            )
        })?;
        Ok(NativeSnapshotConfig {
            pd_endpoint,
            storage_uri,
        })
    }

    fn require_pd_endpoint(&self, operation: &str) -> Result<&str, StorageError> {
        self.pd_endpoint.as_deref().ok_or_else(|| {
            StorageError::Validation(format!(
                "{operation} requires storage.tidb.backup.pd_endpoint"
            ))
        })
    }

    fn command_args(&self, action: BrAction<'_>) -> Result<Vec<OsString>, StorageError> {
        let mut args = Vec::new();
        if let Some(component) = &self.component {
            args.push(OsString::from(component));
        }
        match action {
            BrAction::BackupTable {
                database,
                table,
                storage_uri,
                backup_tso,
            } => {
                let snapshot = self.require_snapshot()?;
                validate_br_name(database, "database")?;
                validate_br_name(table, "table")?;
                args.extend([
                    "backup".into(),
                    "table".into(),
                    "--pd".into(),
                    snapshot.pd_endpoint.into(),
                    "--db".into(),
                    database.into(),
                    "--table".into(),
                    table.into(),
                    "--storage".into(),
                    storage_uri.into(),
                    "--backupts".into(),
                    backup_tso.to_string().into(),
                ]);
                if let Some(send) = self.send_credentials_to_tikv {
                    args.push(format!("--send-credentials-to-tikv={send}").into());
                }
            }
            BrAction::RestoreTable {
                database,
                table,
                storage_uri,
            } => {
                let pd_endpoint = self.require_pd_endpoint("TiDB native restore")?;
                validate_br_name(database, "database")?;
                validate_br_name(table, "table")?;
                args.extend([
                    "restore".into(),
                    "table".into(),
                    "--pd".into(),
                    pd_endpoint.into(),
                    "--db".into(),
                    database.into(),
                    "--table".into(),
                    table.into(),
                    "--storage".into(),
                    storage_uri.into(),
                ]);
                if let Some(send) = self.send_credentials_to_tikv {
                    args.push(format!("--send-credentials-to-tikv={send}").into());
                }
            }
        }
        Ok(args)
    }

    async fn run(&self, action: BrAction<'_>) -> Result<(), StorageError> {
        let args = self.command_args(action)?;
        let mut command = Command::new(&self.binary);
        command.args(&args);
        let output = command
            .output()
            .await
            .map_err(|e| StorageError::Internal(format!("Run TiDB BR: {e}")))?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        Err(StorageError::Internal(format!(
            "TiDB BR exited with {}: {}{}{}",
            output.status,
            truncate_output(stderr.trim()),
            if stderr.trim().is_empty() || stdout.trim().is_empty() {
                ""
            } else {
                " / "
            },
            truncate_output(stdout.trim())
        )))
    }
}

struct NativeSnapshotConfig<'a> {
    pd_endpoint: &'a str,
    storage_uri: &'a str,
}

enum BrAction<'a> {
    BackupTable {
        database: &'a str,
        table: &'a str,
        storage_uri: &'a str,
        backup_tso: i64,
    },
    RestoreTable {
        database: &'a str,
        table: &'a str,
        storage_uri: &'a str,
    },
}

fn non_empty_string(value: Option<String>) -> Option<String> {
    value.and_then(|s| {
        let trimmed = s.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

fn truncate_output(value: &str) -> String {
    const MAX: usize = 1_000;
    if value.chars().count() <= MAX {
        value.to_owned()
    } else {
        format!("{}...", value.chars().take(MAX).collect::<String>())
    }
}

fn backup_storage_uri(base: &str, account_id: &str, table_id: &str, millis: u128) -> String {
    format!(
        "{}/snapshots/{account_id}/{table_id}/{millis}",
        base.trim_end_matches('/')
    )
}

fn validate_br_name(value: &str, label: &str) -> Result<(), StorageError> {
    if value.is_empty() {
        return Err(StorageError::Internal(format!("empty TiDB {label} name")));
    }
    if value
        .chars()
        .any(|c| matches!(c, '*' | '?' | '[' | ']' | '!' | '`' | '\0') || c.is_whitespace())
    {
        return Err(StorageError::Internal(format!(
            "TiDB {label} name is not safe for BR table filters: {value}"
        )));
    }
    Ok(())
}

fn quote_identifier(value: &str, label: &str) -> Result<String, StorageError> {
    if value.contains('`') || value.contains('\0') || !value.is_ascii() || value.is_empty() {
        return Err(StorageError::Internal(format!(
            "TiDB {label} name is not safe for SQL identifiers"
        )));
    }
    Ok(format!("`{value}`"))
}

fn qualified_table(database: &str, table: &str) -> Result<String, StorageError> {
    Ok(format!(
        "{}.{}",
        quote_identifier(database, "database")?,
        quote_identifier(table, "table")?
    ))
}

fn parse_json<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
    label: &str,
) -> Result<T, StorageError> {
    serde_json::from_value(value).map_err(|e| StorageError::Internal(format!("Parse {label}: {e}")))
}

fn backup_arn_account_id(backup_arn: &str) -> Result<String, StorageError> {
    backup_arn
        .split(':')
        .nth(4)
        .map(str::to_owned)
        .ok_or_else(|| StorageError::Validation(format!("Invalid backup ARN: {backup_arn}")))
}

impl TidbEngine {
    async fn current_tso(&self) -> Result<i64, StorageError> {
        current_tidb_tso(&self.pool).await
    }

    async fn data_database_name(&self) -> Result<String, StorageError> {
        let database: Option<String> = sqlx::query_scalar("SELECT DATABASE()")
            .fetch_one(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
        database.ok_or_else(|| StorageError::Internal("TiDB data database not selected".to_owned()))
    }

    async fn physical_table_exists(
        &self,
        database: &str,
        table: &str,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = ? AND table_name = ?)",
        )
        .bind(database)
        .bind(table)
        .fetch_one(&self.data_pool)
        .await
        .map_err(|e| StorageError::Internal(format!("Database error: {e}")))
    }

    async fn drop_physical_table_if_exists(&self, table: &str) {
        let Ok(table) = quote_identifier(table, "table") else {
            return;
        };
        let sql = format!("DROP TABLE IF EXISTS {table}");
        if let Err(err) =
            execute_tidb_idempotent_ddl(&self.data_pool, "drop_failed_br_restore_table", &sql).await
        {
            tracing::error!("failed to drop failed BR restore table '{table}': {err}");
        }
    }

    async fn rename_physical_table(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let database = self.data_database_name().await?;
        let from = qualified_table(&database, from)?;
        let to = qualified_table(&database, to)?;
        let sql = format!("RENAME TABLE {from} TO {to}");
        sqlx::query(&sql)
            .execute(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
        Ok(())
    }

    async fn snapshot_backup_metadata(
        &self,
        account_id: &str,
        table_name: &str,
        native_snapshot_tso: i64,
    ) -> Result<BackupMetadataSnapshot, StorageError> {
        let as_of = tidb_as_of_tso_clause(native_snapshot_tso)?;
        let source_sql = format!(
            "SELECT table_id, table_arn, key_schema, attribute_definitions, billing_mode, \
             provisioned_throughput, stream_specification, deletion_protection_enabled \
             FROM tables {as_of} \
             WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'"
        );
        let source: BackupSourceRow = sqlx::query_as(&source_sql)
            .bind(account_id)
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            .ok_or_else(|| StorageError::TableNotFound(format!("Table not found: {table_name}")))?;

        let stats = self.current_table_stats(&source.table_id).await?;

        let indexes_sql = format!(
            "SELECT index_id, index_name, index_type, key_schema, projection, provisioned_throughput \
             FROM indexes {as_of} \
             WHERE table_id = ? ORDER BY index_type, index_name"
        );
        let indexes: Vec<BackupIndexSnapshotRow> = sqlx::query_as(&indexes_sql)
            .bind(&source.table_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

        let tags_sql = format!(
            "SELECT tag_key, tag_value FROM tags \
             {as_of} \
             WHERE resource_arn = ? ORDER BY tag_key"
        );
        let tags = sqlx::query_as(&tags_sql)
            .bind(&source.table_arn)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

        Ok(BackupMetadataSnapshot {
            source,
            indexes,
            tags,
            stats,
            native_snapshot_tso,
        })
    }

    async fn insert_backup_metadata(
        &self,
        insert: BackupInsert<'_>,
    ) -> Result<time::OffsetDateTime, StorageError> {
        let key_schema_json = insert.snapshot.source.key_schema.clone();
        let attr_defs_json = insert.snapshot.source.attribute_definitions.clone();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

        sqlx::query(
            "INSERT INTO backups \
             (backup_arn, backup_name, table_id, table_name, account_id, backup_status, \
             backup_size_bytes, item_count, key_schema, attribute_definitions, billing_mode, \
             provisioned_throughput, stream_specification, deletion_protection_enabled, \
              backup_backend, storage_uri, physical_table_name, native_snapshot_tso) \
             VALUES (?, ?, ?, ?, ?, 'AVAILABLE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(insert.backup_arn)
        .bind(insert.backup_name)
        .bind(&insert.snapshot.source.table_id)
        .bind(insert.table_name)
        .bind(insert.account_id)
        .bind(insert.snapshot.stats.table_size_bytes)
        .bind(insert.snapshot.stats.item_count)
        .bind(&key_schema_json)
        .bind(&attr_defs_json)
        .bind(&insert.snapshot.source.billing_mode)
        .bind(&insert.snapshot.source.provisioned_throughput)
        .bind(&insert.snapshot.source.stream_specification)
        .bind(insert.snapshot.source.deletion_protection_enabled)
        .bind(TIDB_BACKUP_BACKEND)
        .bind(insert.storage_uri)
        .bind(insert.physical_table)
        .bind(insert.snapshot.native_snapshot_tso.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

        for index in &insert.snapshot.indexes {
            sqlx::query(
                "INSERT INTO backup_indexes \
                 (backup_arn, index_id, index_name, index_type, key_schema, projection, provisioned_throughput) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(insert.backup_arn)
            .bind(&index.index_id)
            .bind(&index.index_name)
            .bind(&index.index_type)
            .bind(&index.key_schema)
            .bind(&index.projection)
            .bind(&index.provisioned_throughput)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
        }

        for (key, value) in &insert.snapshot.tags {
            sqlx::query(
                "INSERT INTO backup_tags (backup_arn, tag_key, tag_value) VALUES (?, ?, ?)",
            )
            .bind(insert.backup_arn)
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
        }

        let created_at: time::OffsetDateTime =
            sqlx::query_scalar("SELECT created_at FROM backups WHERE backup_arn = ?")
                .bind(insert.backup_arn)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

        Ok(created_at)
    }

    async fn publish_restored_table_catalog(
        &self,
        insert: RestoreCatalogInsert<'_>,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

        sqlx::query(
            "INSERT INTO tables \
             (account_id, table_name, key_schema, attribute_definitions, billing_mode, \
              provisioned_throughput, stream_specification, table_status, table_arn, \
              table_id, deletion_protection_enabled, status_transition_at, \
              stream_label, ttl_attribute, ttl_status) \
             VALUES (?, ?, ?, ?, ?, ?, NULL, 'ACTIVE', ?, ?, FALSE, NULL, NULL, NULL, 'DISABLED')",
        )
        .bind(insert.account_id)
        .bind(insert.table_name)
        .bind(insert.metadata.key_schema)
        .bind(insert.metadata.attribute_definitions)
        .bind(insert.metadata.billing_mode)
        .bind(insert.metadata.provisioned_throughput)
        .bind(insert.table_arn)
        .bind(insert.table_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                StorageError::TableAlreadyExists(insert.table_name.to_owned())
            } else {
                StorageError::Internal(format!("Database error: {e}"))
            }
        })?;

        for index in insert.indexes {
            sqlx::query(
                "INSERT INTO indexes \
                 (table_id, index_name, index_id, index_type, key_schema, projection, \
                  index_status, provisioned_throughput) \
                 VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?)",
            )
            .bind(insert.table_id)
            .bind(&index.index_name)
            .bind(&index.index_id)
            .bind(&index.index_type)
            .bind(&index.key_schema)
            .bind(&index.projection)
            .bind(&index.provisioned_throughput)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

        Ok(())
    }
}

impl BackupEngine for TidbEngine {
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let backup_name = backup_name.to_string();
        Box::pin(async move {
            let snapshot_config = self.native_backup.require_snapshot()?;
            let native_snapshot_tso = self.current_tso().await?;
            let metadata = self
                .snapshot_backup_metadata(&account_id, &table_name, native_snapshot_tso)
                .await?;
            let database = self.data_database_name().await?;
            let physical_table = physical_data_table_name(&metadata.source.table_id);
            let ts = epoch_millis();
            let backup_arn = format!(
                "arn:aws:dynamodb:{}:{account_id}:table/{table_name}/backup/{ts}",
                self.region
            );
            let storage_uri = backup_storage_uri(
                snapshot_config.storage_uri,
                &account_id,
                &metadata.source.table_id,
                ts,
            );
            self.native_backup
                .run(BrAction::BackupTable {
                    database: &database,
                    table: &physical_table,
                    storage_uri: &storage_uri,
                    backup_tso: metadata.native_snapshot_tso,
                })
                .await?;

            let created_at = match self
                .insert_backup_metadata(BackupInsert {
                    backup_arn: &backup_arn,
                    backup_name: &backup_name,
                    table_name: &table_name,
                    account_id: &account_id,
                    snapshot: &metadata,
                    storage_uri: &storage_uri,
                    physical_table: &physical_table,
                })
                .await
            {
                Ok(created_at) => created_at,
                Err(err) => {
                    tracing::warn!(
                        "TiDB BR backup completed but catalog publish failed for '{backup_arn}'; \
                         backup data remains at '{storage_uri}' for external lifecycle cleanup"
                    );
                    return Err(err);
                }
            };

            Ok(BackupDetails {
                backup_arn,
                backup_name,
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes: metadata.stats.table_size_bytes,
                backup_creation_date_time: timestamp_to_epoch(created_at),
            })
        })
    }

    fn describe_backup(
        &self,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = backup_arn_account_id(backup_arn);
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let account_id = account_id?;
            #[derive(sqlx::FromRow)]
            struct Row {
                backup_name: String,
                backup_status: String,
                table_id: String,
                table_name: String,
                backup_size_bytes: i64,
                item_count: i64,
                key_schema: serde_json::Value,
                billing_mode: String,
                created_at: time::OffsetDateTime,
                table_arn: Option<String>,
                backup_created_at: time::OffsetDateTime,
            }

            let row: Row = sqlx::query_as(
                "SELECT b.backup_name, b.backup_status, b.table_id, b.table_name, \
                 b.backup_size_bytes, b.item_count, b.key_schema, b.billing_mode, \
                 COALESCE(t.creation_date_time, b.created_at) as created_at, \
                 t.table_arn, b.created_at as backup_created_at \
                 FROM backups b \
                 LEFT JOIN tables t ON t.table_id = b.table_id \
                 WHERE b.backup_arn = ? AND b.account_id = ?",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            .ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))?;

            let key_schema: Vec<KeySchemaElement> =
                parse_json(row.key_schema, "backup key schema")?;
            let table_arn = row.table_arn.unwrap_or_else(|| {
                format!(
                    "arn:aws:dynamodb:{}:{account_id}:table/{}",
                    self.region, row.table_name
                )
            });

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_arn: backup_arn.to_owned(),
                    backup_name: row.backup_name,
                    backup_status: row.backup_status,
                    backup_type: "USER".to_owned(),
                    backup_size_bytes: row.backup_size_bytes,
                    backup_creation_date_time: timestamp_to_epoch(row.backup_created_at),
                },
                source_table_details: SourceTableDetails {
                    table_name: row.table_name,
                    table_id: row.table_id,
                    table_arn,
                    key_schema,
                    item_count: row.item_count,
                    table_size_bytes: row.backup_size_bytes,
                    billing_mode: Some(row.billing_mode),
                    table_creation_date_time: timestamp_to_epoch(row.created_at),
                },
            })
        })
    }

    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.map(str::to_string);
        Box::pin(async move {
            let rows: Vec<(
                String,
                String,
                String,
                String,
                String,
                i64,
                time::OffsetDateTime,
            )> = if let Some(table) = &table_name {
                sqlx::query_as(
                    "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                     COALESCE(t.table_arn, CONCAT('arn:aws:dynamodb:', ?, ':', b.account_id, \
                     ':table/', b.table_name)) as table_arn, b.backup_size_bytes, b.created_at \
                     FROM backups b \
                     LEFT JOIN tables t ON t.table_id = b.table_id \
                     WHERE b.account_id = ? AND b.table_name = ? AND b.backup_status != 'DELETED' \
                     ORDER BY b.created_at DESC",
                )
                .bind(&self.region)
                .bind(&account_id)
                .bind(table)
                .fetch_all(&self.pool)
                .await
            } else {
                sqlx::query_as(
                    "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                     COALESCE(t.table_arn, CONCAT('arn:aws:dynamodb:', ?, ':', b.account_id, \
                     ':table/', b.table_name)) as table_arn, b.backup_size_bytes, b.created_at \
                     FROM backups b \
                     LEFT JOIN tables t ON t.table_id = b.table_id \
                     WHERE b.account_id = ? AND b.backup_status != 'DELETED' \
                     ORDER BY b.created_at DESC",
                )
                .bind(&self.region)
                .bind(&account_id)
                .fetch_all(&self.pool)
                .await
            }
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            Ok(rows
                .into_iter()
                .map(
                    |(arn, name, table_name, status, table_arn, size, created_at)| BackupSummary {
                        backup_arn: arn,
                        backup_name: name,
                        table_name,
                        table_arn,
                        backup_status: status,
                        backup_type: "USER".to_owned(),
                        backup_size_bytes: size,
                        backup_creation_date_time: timestamp_to_epoch(created_at),
                    },
                )
                .collect())
        })
    }

    fn delete_backup(
        &self,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = backup_arn_account_id(backup_arn);
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let account_id = account_id?;
            let desc = self.describe_backup(&backup_arn).await?;

            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            sqlx::query("DELETE FROM backup_indexes WHERE backup_arn = ?")
                .bind(&backup_arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            sqlx::query("DELETE FROM backup_tags WHERE backup_arn = ?")
                .bind(&backup_arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            sqlx::query(
                "UPDATE backups SET backup_status = 'DELETED' \
                 WHERE backup_arn = ? AND account_id = ?",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_status: "DELETED".to_owned(),
                    ..desc.backup_details
                },
                ..desc
            })
        })
    }

    fn restore_table_from_backup(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        let target_table_name = target_table_name.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let backup_row: BackupRestoreRow = sqlx::query_as(
                "SELECT key_schema, attribute_definitions, billing_mode, \
                 provisioned_throughput, backup_backend, storage_uri, physical_table_name \
                 FROM backups \
                 WHERE backup_arn = ? AND account_id = ? AND backup_status = 'AVAILABLE'",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            .ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))?;

            if backup_row.backup_backend != TIDB_BACKUP_BACKEND {
                return Err(StorageError::Validation(
                    "TiDB can restore only native BR backups".to_owned(),
                ));
            }

            let storage_uri = backup_row.storage_uri.as_deref().ok_or_else(|| {
                StorageError::Internal(format!("Backup missing TiDB BR storage URI: {backup_arn}"))
            })?;
            let source_physical_table =
                backup_row.physical_table_name.as_deref().ok_or_else(|| {
                    StorageError::Internal(format!(
                        "Backup missing TiDB physical table name: {backup_arn}"
                    ))
                })?;

            let backup_index_rows: Vec<BackupIndexSnapshotRow> = sqlx::query_as(
                "SELECT index_id, index_name, index_type, key_schema, projection, provisioned_throughput \
                 FROM backup_indexes WHERE backup_arn = ? ORDER BY index_type, index_name",
            )
            .bind(&backup_arn)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            for index in &backup_index_rows {
                match index.index_type.as_str() {
                    "GSI" | "LSI" => {}
                    other => {
                        return Err(StorageError::Internal(format!(
                            "Invalid backup index type: {other}"
                        )));
                    }
                }
            }

            let target_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = ? AND table_name = ?)",
            )
            .bind(&account_id)
            .bind(&target_table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
            if target_exists {
                return Err(StorageError::TableAlreadyExists(target_table_name.clone()));
            }

            let database = self.data_database_name().await?;
            if self
                .physical_table_exists(&database, source_physical_table)
                .await?
            {
                return Err(StorageError::Validation(format!(
                    "TiDB BR restores table backups to their original physical table name \
                     ({source_physical_table}); restore requires an empty or conflict-free \
                     TiDB target"
                )));
            }

            let target_table_id = uuid::Uuid::new_v4().to_string();
            let target_physical_table = physical_data_table_name(&target_table_id);
            let target_table_arn = table_arn(&self.region, &account_id, &target_table_name);

            let restore_result = async {
                self.native_backup
                    .run(BrAction::RestoreTable {
                        database: &database,
                        table: source_physical_table,
                        storage_uri,
                    })
                    .await?;

                self.rename_physical_table(source_physical_table, &target_physical_table)
                    .await?;

                // DynamoDB restores table data, not TTL settings. BR restores the
                // source table's physical shape, so normalize restored TiDB TTL
                // artifacts before the catalog row becomes ACTIVE.
                drop_ttl_artifacts(&self.data_pool, &target_table_id).await?;

                self.publish_restored_table_catalog(RestoreCatalogInsert {
                    account_id: &account_id,
                    table_name: &target_table_name,
                    table_id: &target_table_id,
                    table_arn: &target_table_arn,
                    metadata: RestoreCatalogMetadata {
                        key_schema: &backup_row.key_schema,
                        attribute_definitions: &backup_row.attribute_definitions,
                        billing_mode: &backup_row.billing_mode,
                        provisioned_throughput: &backup_row.provisioned_throughput,
                    },
                    indexes: &backup_index_rows,
                })
                .await?;

                Ok::<(), StorageError>(())
            }
            .await;

            if let Err(err) = restore_result {
                self.drop_physical_table_if_exists(source_physical_table)
                    .await;
                self.drop_physical_table_if_exists(&target_physical_table)
                    .await;
                return Err(err);
            }

            self.build_table_description(&account_id, &target_table_name)
                .await
        })
    }

    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = ? AND table_name = ?)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            if !exists {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }

            let pitr_row: Option<(
                bool,
                Option<time::OffsetDateTime>,
                Option<time::OffsetDateTime>,
            )> = sqlx::query_as(
                "SELECT pitr_enabled, earliest_restorable, latest_restorable \
                     FROM continuous_backups WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            let (pitr_enabled, earliest, latest) = pitr_row
                .map_or((false, None, None), |(enabled, earliest, latest)| {
                    (enabled, earliest, latest)
                });

            Ok(ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                    point_in_time_recovery_status: if pitr_enabled {
                        "ENABLED".to_owned()
                    } else {
                        "DISABLED".to_owned()
                    },
                    earliest_restorable_date_time: earliest.map(timestamp_to_epoch),
                    latest_restorable_date_time: latest.map(timestamp_to_epoch),
                }),
            })
        })
    }

    fn update_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
        pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = ? AND table_name = ?)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            if !exists {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }

            if pitr_enabled {
                return Err(StorageError::Validation(
                    "TiDB table-level point-in-time recovery is not supported; \
                     TiDB BR PITR restores into an empty or conflict-free target cluster, \
                     and TiDB historical reads cannot be copied into a live target table \
                     as one native online DDL/data operation"
                        .to_owned(),
                ));
            }

            sqlx::query(
                "INSERT INTO continuous_backups \
                 (account_id, table_name, pitr_enabled, earliest_restorable, latest_restorable) \
                 VALUES (?, ?, ?, NULL, NULL) \
                 ON DUPLICATE KEY UPDATE pitr_enabled = VALUES(pitr_enabled)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .bind(pitr_enabled)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            self.describe_continuous_backups(&account_id, &table_name)
                .await
        })
    }

    fn restore_table_to_point_in_time(
        &self,
        _account_id: &str,
        _source_table_name: &str,
        _target_table_name: &str,
        _restore_time_epoch: Option<f64>,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        Box::pin(async move {
            Err(StorageError::Validation(
                "TiDB cannot perform DynamoDB table-level point-in-time restore as a native \
                 same-cluster online operation: BR PITR restores into an empty or conflict-free \
                 target cluster, FLASHBACK TABLE restores dropped or truncated tables, and \
                 TiDB historical reads are read-only for this live-target restore shape"
                    .to_owned(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{BrAction, TidbNativeBackupConfig, backup_storage_uri};
    use extenddb_storage::config::NativeBackupConfig;

    #[test]
    fn builds_tiup_br_backup_command() {
        let cfg = TidbNativeBackupConfig::from_storage_config(NativeBackupConfig {
            coordinator_endpoint: Some("127.0.0.1:2379".to_owned()),
            storage_uri: Some("s3://bucket/extenddb".to_owned()),
            send_credentials_to_storage_nodes: Some(false),
            ..NativeBackupConfig::default()
        });

        let args = cfg
            .command_args(BrAction::BackupTable {
                database: "extenddb_data",
                table: "_ddb_123",
                storage_uri: "s3://bucket/extenddb/snapshots/a/t/1",
                backup_tso: 450456244814610433,
            })
            .expect("command should build");
        let rendered: Vec<_> = args.iter().map(|arg| arg.to_string_lossy()).collect();
        assert_eq!(
            rendered,
            vec![
                "br",
                "backup",
                "table",
                "--pd",
                "127.0.0.1:2379",
                "--db",
                "extenddb_data",
                "--table",
                "_ddb_123",
                "--storage",
                "s3://bucket/extenddb/snapshots/a/t/1",
                "--backupts",
                "450456244814610433",
                "--send-credentials-to-tikv=false",
            ]
        );
    }

    #[test]
    fn builds_direct_br_restore_command() {
        let cfg = TidbNativeBackupConfig::from_storage_config(NativeBackupConfig {
            binary: Some("br".to_owned()),
            component: Some(String::new()),
            coordinator_endpoint: Some("pd:2379".to_owned()),
            storage_uri: Some("local:///backup".to_owned()),
            send_credentials_to_storage_nodes: Some(false),
            ..NativeBackupConfig::default()
        });

        let args = cfg
            .command_args(BrAction::RestoreTable {
                database: "extenddb_data",
                table: "_ddb_source",
                storage_uri: "local:///backup/snapshots/a/t/1",
            })
            .expect("command should build");
        let rendered: Vec<_> = args.iter().map(|arg| arg.to_string_lossy()).collect();
        assert_eq!(
            rendered,
            vec![
                "restore",
                "table",
                "--pd",
                "pd:2379",
                "--db",
                "extenddb_data",
                "--table",
                "_ddb_source",
                "--storage",
                "local:///backup/snapshots/a/t/1",
                "--send-credentials-to-tikv=false",
            ]
        );
    }

    #[test]
    fn backup_uri_is_stable_under_base_slashes() {
        assert_eq!(
            backup_storage_uri("s3://bucket/root/", "acct", "table-id", 42),
            "s3://bucket/root/snapshots/acct/table-id/42"
        );
    }
}
