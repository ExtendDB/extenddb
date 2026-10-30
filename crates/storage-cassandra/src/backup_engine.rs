// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup and restore implementation for Cassandra storage.
//!
//! Cassandra's native snapshots are node-local operational artifacts and require
//! filesystem/JMX orchestration that is not available through CQL. This module
//! therefore implements the DynamoDB-facing API as a logical snapshot: immutable
//! item JSON is stored in bounded partitions in the owning account keyspace,
//! while metadata and list access paths are stored in the catalog keyspace.
//!
//! # Restore
//!
//! The engine resolves the backup into the target's `CreateTableInput` and runs
//! every refusal; this module creates exactly that table, registers the
//! restore, and copies the items in a detached task.
//!
//! Registration and DeleteBackup are serialized by lightweight transactions on
//! one partition, the backup's own `backups_by_arn` row:
//!
//! * a restore adds its target to `active_restores` with
//!   `IF backup_status = 'AVAILABLE'`;
//! * DeleteBackup moves the backup to DELETING with
//!   `IF backup_status = 'AVAILABLE' AND active_restores = null`.
//!
//! Paxos orders the two on that partition, so a restore can register only
//! while the backup is AVAILABLE, and the backup cannot leave AVAILABLE while a
//! restore is registered. What Cassandra cannot give is one transaction over
//! the target's `tables` row and the backup row (they are different
//! partitions, in general on different replicas). The target row is therefore
//! created first (its `IF NOT EXISTS` reserves the name), and the registration
//! second; if the registration finds the backup no longer AVAILABLE, the target
//! is removed and the restore fails with "Backup not found". Between the two
//! steps a concurrent DescribeTable can see a CREATING target for a restore
//! that then fails; no restore ever copies from a backup DeleteBackup has
//! claimed.
//!
//! A registration whose target is no longer a CREATING restore target of this
//! backup (the copy activated it and then crashed before unregistering, or a
//! cleanup removed the target and crashed before unregistering) is stale.
//! DeleteBackup removes stale registrations before claiming the backup, so one
//! never blocks it.
//!
//! From the insert of the target row until the copy ends, the restoring
//! process refreshes `tables.restore_heartbeat` every
//! [`RESTORE_HEARTBEAT_INTERVAL`] (first refresh immediately);
//! [`CassandraEngine::sweep_abandoned_restores`] removes a target whose
//! heartbeat is older than [`RESTORE_STALE_AFTER`]. The servers run it at
//! startup and every [`RESTORE_SWEEP_INTERVAL`] ([`poll_abandoned_restores`]).
//!
//! `restore_table_from_backup` spawns one task that registers and then
//! copies; the request future waits only for the registration outcome and
//! returns the CREATING description, so a dropped request does not stop the
//! restore.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::query::BatchQueryBuilder;
use cdrs_tokio::types::map::Map;
use cdrs_tokio::types::{AsRust, IntoRustByName, rows::Row};
use extenddb_core::types::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary,
    ContinuousBackupsDescription, DeleteTableInput, GsiInput, KeySchemaElement, LsiInput,
    PointInTimeRecoveryDescription, ProvisionedThroughput, SourceTableDetails, TableDescription,
    TableKeyInfo,
};
use extenddb_storage::backup_definition::{
    BACKUP_DEFINITION_VERSION, BackupTableDefinition, throughput_from_catalog,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, BackupRestoreSource, MetadataEngine, RestoreRequest};
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};

use crate::CassandraEngine;
use crate::create_table::RestoreTargetColumns;
use crate::delete_table::DeleteTableCaller;

/// Maximum number of item payloads stored in one Cassandra partition.
///
/// DynamoDB items are at most 400 KiB, so 64 items bound a partition to roughly
/// 25 MiB before Cassandra overhead.
const BACKUP_ITEMS_PER_BUCKET: i64 = 64;
const BACKUP_SCAN_PAGE_SIZE: i64 = 1_000;

/// Items a restore copy writes concurrently. Each write is a plain INSERT
/// per row (`data::restore_copy`), so this bounds in-flight statements per
/// index-less item; an item with indexes adds one statement per index row.
const RESTORE_WRITE_CONCURRENCY: usize = 64;

/// Backup payload buckets a restore copy reads ahead of its writes.
const RESTORE_READ_AHEAD_BUCKETS: usize = 2;

/// How often a running restore copy refreshes its target's heartbeat.
pub const RESTORE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// A restore target whose heartbeat is older than this is abandoned: the
/// process copying into it is gone. Four missed heartbeats.
pub const RESTORE_STALE_AFTER: Duration = Duration::from_secs(60);

/// How often the abandoned-restore sweep runs.
pub const RESTORE_SWEEP_INTERVAL: Duration = Duration::from_secs(15);

/// Attempts a restore makes to register on its backup when a stale
/// registration (cleared between attempts) kept the previous one from
/// applying.
const REGISTRATION_ATTEMPTS: usize = 3;

fn bucket_count(item_count: i64) -> i64 {
    if item_count <= 0 {
        0
    } else {
        (item_count + BACKUP_ITEMS_PER_BUCKET - 1) / BACKUP_ITEMS_PER_BUCKET
    }
}

fn backup_not_found(backup_arn: &str) -> StorageError {
    StorageError::Validation(format!("Backup not found: {backup_arn}"))
}

fn backup_in_use(table_name: &str, backup_arn: &str) -> StorageError {
    StorageError::BackupInUse(format!(
        "Backup is being used to restore table {table_name}: {backup_arn}"
    ))
}

/// The error of a restore whose [`REGISTRATION_ATTEMPTS`] all found the
/// backup's registration taken by a restore that was just starting (or just
/// stopping, its stale entry cleared between attempts). The backup is
/// AVAILABLE, so this is `BackupInUse`, not a missing backup; the same text
/// as the MongoDB backend's for the same situation.
fn registration_exhausted(
    registered: Result<bool, StorageError>,
    backup_arn: &str,
) -> StorageError {
    match registered {
        Err(error) => error,
        Ok(_) => StorageError::BackupInUse(format!(
            "Backup is being used by another restore that is starting; retry: {backup_arn}"
        )),
    }
}

/// See [`CassandraEngine::require_migrated_catalog`].
fn unmigrated_catalog_refusal(pending: &[&str]) -> StorageError {
    StorageError::Unsupported(format!(
        "Backups are unavailable until the ExtendDB catalog is migrated: it does not record \
         catalog migration {}. Run `extenddb migrate --yes` with the admin credentials.",
        pending.join(", ")
    ))
}

#[derive(Debug)]
struct StoredBackup {
    backup_arn: String,
    account_id: String,
    backup_name: String,
    table_id: String,
    table_name: String,
    table_arn: String,
    backup_status: String,
    backup_type: String,
    backup_size_bytes: i64,
    item_count: i64,
    key_schema: String,
    attribute_definitions: String,
    billing_mode: String,
    provisioned_throughput: Option<String>,
    table_created_at: i64,
    created_at: i64,
    /// `BackupTableDefinition` JSON; `None` for a row written before V005.
    definition: Option<String>,
    /// Restore target `table_id` -> target table name, while registered.
    active_restores: HashMap<String, String>,
}

impl StoredBackup {
    fn from_row(row: &Row) -> Result<Self, StorageError> {
        let active_restores: Result<Option<Map>, _> = row.get_by_name("active_restores");
        let active_restores = match active_restores {
            Ok(Some(map)) => map
                .as_rust::<HashMap<String, String>>()
                .map_err(|e| StorageError::Internal(format!("Parse active_restores: {e}")))?
                .unwrap_or_default(),
            // Absent before V005 has been applied, and null when empty.
            Ok(None) | Err(_) => HashMap::new(),
        };
        Ok(Self {
            backup_arn: row
                .get_r_by_name("backup_arn")
                .map_err(|e| StorageError::Internal(format!("Parse backup_arn: {e}")))?,
            account_id: row
                .get_r_by_name("account_id")
                .map_err(|e| StorageError::Internal(format!("Parse account_id: {e}")))?,
            backup_name: row
                .get_r_by_name("backup_name")
                .map_err(|e| StorageError::Internal(format!("Parse backup_name: {e}")))?,
            table_id: row
                .get_r_by_name("table_id")
                .map_err(|e| StorageError::Internal(format!("Parse table_id: {e}")))?,
            table_name: row
                .get_r_by_name("table_name")
                .map_err(|e| StorageError::Internal(format!("Parse table_name: {e}")))?,
            table_arn: row
                .get_r_by_name("table_arn")
                .map_err(|e| StorageError::Internal(format!("Parse table_arn: {e}")))?,
            backup_status: row
                .get_r_by_name("backup_status")
                .map_err(|e| StorageError::Internal(format!("Parse backup_status: {e}")))?,
            backup_type: row
                .get_r_by_name("backup_type")
                .map_err(|e| StorageError::Internal(format!("Parse backup_type: {e}")))?,
            backup_size_bytes: row.get_r_by_name("backup_size_bytes").unwrap_or(0),
            item_count: row.get_r_by_name("item_count").unwrap_or(0),
            key_schema: row
                .get_r_by_name("key_schema")
                .map_err(|e| StorageError::Internal(format!("Parse key_schema: {e}")))?,
            attribute_definitions: row
                .get_r_by_name("attribute_definitions")
                .map_err(|e| StorageError::Internal(format!("Parse attribute_definitions: {e}")))?,
            billing_mode: row
                .get_r_by_name("billing_mode")
                .map_err(|e| StorageError::Internal(format!("Parse billing_mode: {e}")))?,
            provisioned_throughput: row.get_by_name("provisioned_throughput").ok().flatten(),
            table_created_at: row
                .get_r_by_name("table_created_at")
                .map_err(|e| StorageError::Internal(format!("Parse table_created_at: {e}")))?,
            created_at: row
                .get_r_by_name("created_at")
                .map_err(|e| StorageError::Internal(format!("Parse created_at: {e}")))?,
            definition: row.get_by_name("definition").ok().flatten(),
            active_restores,
        })
    }

    fn details(&self) -> BackupDetails {
        BackupDetails {
            backup_arn: self.backup_arn.clone(),
            backup_name: self.backup_name.clone(),
            backup_status: self.backup_status.clone(),
            backup_type: self.backup_type.clone(),
            backup_size_bytes: self.backup_size_bytes,
            backup_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(
                self.created_at,
            ),
        }
    }

    fn summary(&self) -> BackupSummary {
        BackupSummary {
            backup_arn: self.backup_arn.clone(),
            backup_name: self.backup_name.clone(),
            table_name: self.table_name.clone(),
            table_id: self.table_id.clone(),
            table_arn: self.table_arn.clone(),
            backup_status: self.backup_status.clone(),
            backup_type: self.backup_type.clone(),
            backup_size_bytes: self.backup_size_bytes,
            backup_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(
                self.created_at,
            ),
        }
    }

    fn parsed_key_schema(&self) -> Result<Vec<KeySchemaElement>, StorageError> {
        serde_json::from_str(&self.key_schema)
            .map_err(|e| StorageError::Internal(format!("Parse key schema: {e}")))
    }

    fn parsed_attribute_definitions(&self) -> Result<Vec<AttributeDefinition>, StorageError> {
        serde_json::from_str(&self.attribute_definitions)
            .map_err(|e| StorageError::Internal(format!("Parse attributes: {e}")))
    }

    fn parsed_definition(&self) -> Result<Option<BackupTableDefinition>, StorageError> {
        self.definition
            .as_deref()
            .map(|text| {
                let value = serde_json::from_str(text).map_err(|e| {
                    StorageError::Internal(format!(
                        "backup {} table definition: {e}",
                        self.backup_arn
                    ))
                })?;
                BackupTableDefinition::from_json(value, &self.backup_arn)
            })
            .transpose()
    }

    fn description(&self) -> Result<BackupDescription, StorageError> {
        Ok(BackupDescription {
            backup_details: self.details(),
            source_table_details: SourceTableDetails {
                table_name: self.table_name.clone(),
                table_id: self.table_id.clone(),
                table_arn: self.table_arn.clone(),
                key_schema: self.parsed_key_schema()?,
                item_count: self.item_count,
                table_size_bytes: self.backup_size_bytes,
                billing_mode: Some(self.billing_mode.clone()),
                table_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(
                    self.table_created_at,
                ),
                provisioned_throughput: None,
                on_demand_throughput: None,
            },
            source_table_feature_details: None,
        })
    }
}

/// Everything the detached restore copy needs, owned.
#[derive(Clone)]
struct RestoreJob {
    account_id: String,
    target_name: String,
    table_id: String,
    backup_arn: String,
    item_count: i64,
    backup_size_bytes: i64,
    key_schema: Vec<KeySchemaElement>,
    attribute_definitions: Vec<AttributeDefinition>,
    owner: uuid::Uuid,
}

enum CopyError {
    /// The target is no longer this copy's (the sweep claimed it). Whoever
    /// claimed it removes it.
    OwnershipLost,
    Failed(StorageError),
}

/// A spawned task that is aborted when this value is dropped. Dropping a bare
/// `JoinHandle` detaches its task; holding the handle in this guard instead
/// stops the task on every exit of its owner: return, `?`, panic unwinding,
/// or the owner's future being dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The heartbeat of one restore target, running from the moment the target
/// row is inserted until this value is dropped.
pub(crate) struct RestoreHeartbeat {
    _task: AbortOnDrop,
    lost: Arc<AtomicBool>,
}

impl RestoreHeartbeat {
    /// Run `beat` now and then every `interval` until this value is dropped.
    /// `beat` returns whether the target is still this process's; the first
    /// `Ok(false)` sets [`Self::is_lost`] and stops the heartbeat. An error
    /// is logged and the next tick tries again (the sweep, not one missed
    /// beat, decides that a target is abandoned).
    fn spawn<F, Fut>(interval: Duration, target_name: String, mut beat: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<bool, StorageError>> + Send,
    {
        let lost = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&lost);
        let task = tokio::spawn(async move {
            // The first tick completes immediately.
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                match beat().await {
                    Ok(true) => {}
                    Ok(false) => {
                        flag.store(true, Ordering::Release);
                        break;
                    }
                    Err(error) => {
                        tracing::warn!("Restore of table {target_name}: heartbeat failed: {error}")
                    }
                }
            }
        });
        Self {
            _task: AbortOnDrop(task),
            lost,
        }
    }

    /// Whether a heartbeat found the target no longer owned by this process.
    fn is_lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }
}

/// What the heartbeat's lightweight transaction is conditioned on.
struct HeartbeatTarget {
    account_id: String,
    target_name: String,
    table_id: String,
    owner: uuid::Uuid,
}

impl CassandraEngine {
    /// Refuse a backup operation on a catalog that has not run every catalog
    /// migration this binary carries. Catalog migration V005 adds the columns
    /// CreateBackup, DeleteBackup and RestoreTableFromBackup write; without
    /// it they failed with an internal error (`Undefined column name ...`),
    /// which SDKs retry. `Unsupported` reaches the client as
    /// `ValidationException` with this message. Table, item, ListBackups and
    /// DescribeBackup operations do not call this: they work on such a
    /// catalog.
    pub(crate) async fn require_migrated_catalog(&self) -> Result<(), StorageError> {
        use std::sync::atomic::Ordering;
        if self.catalog_migrated.load(Ordering::Acquire) {
            return Ok(());
        }
        let pending =
            crate::migrations::pending_catalog_migrations(&self.session, &self.catalog_keyspace())
                .await?;
        if pending.is_empty() {
            self.catalog_migrated.store(true, Ordering::Release);
            return Ok(());
        }
        Err(unmigrated_catalog_refusal(&pending))
    }

    async fn load_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> Result<StoredBackup, StorageError> {
        let query = format!(
            "SELECT * FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ?",
            self.catalog_keyspace()
        );
        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(account_id, backup_arn))
            .await
            .map_err(|e| StorageError::Internal(format!("Query backup: {e}")))?;
        let rows = result
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Parse backup response: {e}")))?
            .into_rows()
            .unwrap_or_default();
        let backup = rows
            .first()
            .map(StoredBackup::from_row)
            .transpose()?
            .filter(|backup| backup.account_id == account_id)
            .ok_or_else(|| backup_not_found(backup_arn))?;
        Ok(backup)
    }

    async fn load_available_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> Result<StoredBackup, StorageError> {
        let backup = self.load_backup(account_id, backup_arn).await?;
        if backup.backup_status != "AVAILABLE" {
            return Err(backup_not_found(backup_arn));
        }
        Ok(backup)
    }

    async fn cleanup_backup_payload(
        &self,
        account_id: &str,
        backup_arn: &str,
        item_count: i64,
    ) -> Result<(), StorageError> {
        let account_keyspace = self.account_keyspace(account_id);
        let query = format!(
            "DELETE FROM {account_keyspace}.backup_items WHERE backup_arn = ? AND bucket = ?"
        );
        for bucket in 0..bucket_count(item_count) {
            #[allow(clippy::cast_possible_truncation)]
            let bucket_i32 = bucket as i32;
            self.session
                .query_with_values(&query, cdrs_tokio::query_values!(backup_arn, bucket_i32))
                .await
                .map_err(|e| StorageError::Internal(format!("Delete backup payload: {e}")))?;
        }
        Ok(())
    }

    async fn remove_backup_index_rows(&self, backup: &StoredBackup) -> Result<(), StorageError> {
        let catalog = self.catalog_keyspace();
        let by_account = format!(
            "DELETE FROM {catalog}.backups_by_account WHERE account_id = ? AND created_at = ? AND backup_arn = ?"
        );
        self.session
            .query_with_values(
                &by_account,
                cdrs_tokio::query_values!(
                    backup.account_id.as_str(),
                    backup.created_at,
                    backup.backup_arn.as_str()
                ),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Delete account backup index: {e}")))?;

        let by_table = format!(
            "DELETE FROM {catalog}.backups_by_table WHERE account_id = ? AND table_name = ? AND created_at = ? AND backup_arn = ?"
        );
        self.session
            .query_with_values(
                &by_table,
                cdrs_tokio::query_values!(
                    backup.account_id.as_str(),
                    backup.table_name.as_str(),
                    backup.created_at,
                    backup.backup_arn.as_str()
                ),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Delete table backup index: {e}")))?;
        Ok(())
    }

    /// The definition a backup of `table_name` records, read from the table's
    /// catalog row (billing, throughput, class, SSE, on-demand throughput),
    /// its description (indexes, stream specification), and DescribeTimeToLive.
    async fn backup_definition_for_table(
        &self,
        account_id: &str,
        table_name: &str,
        table: &TableDescription,
    ) -> Result<BackupTableDefinition, StorageError> {
        let query = format!(
            "SELECT billing_mode, provisioned_throughput, table_class, sse_specification, \
             on_demand_throughput FROM {}.tables WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let row = crate::cassandra_util::query_optional::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, table_name),
            "backup definition",
        )
        .await?
        .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        let json_column = |name: &str| -> Result<Option<serde_json::Value>, StorageError> {
            let text: Option<String> = row.get_by_name(name).ok().flatten();
            text.map(|t| {
                serde_json::from_str(&t)
                    .map_err(|e| StorageError::Internal(format!("Parse {name}: {e}")))
            })
            .transpose()
        };
        let billing_mode: String = row
            .get_r_by_name("billing_mode")
            .map_err(|e| StorageError::Internal(format!("Parse billing_mode: {e}")))?;
        let provisioned_throughput =
            throughput_from_catalog(json_column("provisioned_throughput")?.as_ref());
        let sse_specification = json_column("sse_specification")?;
        let on_demand_throughput = json_column("on_demand_throughput")?
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| StorageError::Internal(format!("Parse on_demand_throughput: {e}")))?;
        let table_class: Option<String> = row.get_by_name("table_class").ok().flatten();

        let global_secondary_indexes = table
            .global_secondary_indexes
            .iter()
            .flatten()
            .map(|gsi| GsiInput {
                index_name: gsi.index_name.clone(),
                key_schema: gsi.key_schema.clone(),
                projection: gsi.projection.clone(),
                provisioned_throughput: gsi.provisioned_throughput.as_ref().map(|pt| {
                    ProvisionedThroughput {
                        read_capacity_units: pt.read_capacity_units,
                        write_capacity_units: pt.write_capacity_units,
                    }
                }),
            })
            .collect();
        let local_secondary_indexes = table
            .local_secondary_indexes
            .iter()
            .flatten()
            .map(|lsi| LsiInput {
                index_name: lsi.index_name.clone(),
                key_schema: lsi.key_schema.clone(),
                projection: lsi.projection.clone(),
            })
            .collect();
        let time_to_live = MetadataEngine::describe_ttl(self, account_id, table_name).await?;

        Ok(BackupTableDefinition {
            version: BACKUP_DEFINITION_VERSION,
            billing_mode,
            provisioned_throughput,
            global_secondary_indexes,
            local_secondary_indexes,
            table_class,
            sse_specification,
            on_demand_throughput,
            // The Cassandra backend has no vector indexes (CreateTable does
            // not create them), so a Cassandra table never has any to record.
            vector_index_names: Vec::new(),
            time_to_live: Some(time_to_live),
            stream_specification: table.stream_specification.clone(),
        })
    }

    async fn table_exists_for_backup(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<bool, StorageError> {
        let query = format!(
            "SELECT table_name FROM {}.tables WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let rows = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(account_id, table_name))
            .await
            .map_err(|e| StorageError::Internal(format!("Query table: {e}")))?
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Parse table response: {e}")))?
            .into_rows()
            .unwrap_or_default();
        Ok(!rows.is_empty())
    }

    // ── Restore registration ────────────────────────────────────────────────

    /// Register a restore target on its source backup, only while the backup
    /// is AVAILABLE and no other restore is registered on it (one restore of
    /// a backup at a time, as the service: a second one while the first is
    /// CREATING is `BackupInUse`). Returns whether it registered; when it did
    /// not, [`Self::registration_refusal`] tells which condition failed.
    async fn register_restore(
        &self,
        account_id: &str,
        backup_arn: &str,
        table_id: &str,
        target_name: &str,
    ) -> Result<bool, StorageError> {
        let query = format!(
            "UPDATE {}.backups_by_arn SET active_restores[?] = ? \
             WHERE account_id = ? AND backup_arn = ? \
             IF backup_status = 'AVAILABLE' AND active_restores = null",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(table_id, target_name, account_id, backup_arn),
        )
        .await?;
        crate::cassandra_util::lwt_applied(&result)
    }

    /// Why [`Self::register_restore`] did not apply, from a re-read of the
    /// backup: `BackupInUse` naming the target when a registered restore is
    /// still copying; after clearing stale registrations (a target that
    /// activated or was removed before unregistering) `Ok(())` so the caller
    /// can register again; `Backup not found` when it is no longer AVAILABLE.
    async fn registration_refusal(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> Result<(), StorageError> {
        let backup = self.load_backup(account_id, backup_arn).await?;
        if backup.backup_status != "AVAILABLE" {
            return Err(backup_not_found(backup_arn));
        }
        self.ensure_backup_not_restoring(&backup).await
    }

    async fn unregister_restore(
        &self,
        account_id: &str,
        backup_arn: &str,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let query = format!(
            "DELETE active_restores[?] FROM {}.backups_by_arn \
             WHERE account_id = ? AND backup_arn = ? IF EXISTS",
            self.catalog_keyspace()
        );
        crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(table_id, account_id, backup_arn),
        )
        .await?;
        Ok(())
    }

    /// True if `target_name` is, right now, a CREATING restore target with
    /// this `table_id` restoring from `backup_arn`.
    async fn restore_target_in_progress(
        &self,
        account_id: &str,
        backup_arn: &str,
        table_id: &str,
        target_name: &str,
    ) -> Result<bool, StorageError> {
        let query = format!(
            "SELECT table_id, table_status, restore_source_backup_arn FROM {}.tables \
             WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let row = crate::cassandra_util::query_rows_quorum::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, target_name),
            "restore target",
        )
        .await?
        .into_iter()
        .next();
        let Some(row) = row else {
            return Ok(false);
        };
        let row_table_id: Option<String> = row.get_by_name("table_id").ok().flatten();
        let status: Option<String> = row.get_by_name("table_status").ok().flatten();
        let source: Option<String> = row.get_by_name("restore_source_backup_arn").ok().flatten();
        Ok(row_table_id.as_deref() == Some(table_id)
            && status.as_deref() == Some("CREATING")
            && source.as_deref() == Some(backup_arn))
    }

    /// The source backup ARN of the CREATING restore target named
    /// `table_name`, if there is one right now; `None` for no table, a table
    /// that is not CREATING, or a CREATING table that is not a restore target.
    async fn creating_restore_target_under_name(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<String>, StorageError> {
        let query = format!(
            "SELECT table_status, restore_source_backup_arn FROM {}.tables \
             WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let row = crate::cassandra_util::query_rows_quorum::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, table_name),
            "restore target",
        )
        .await?
        .into_iter()
        .next();
        let Some(row) = row else {
            return Ok(None);
        };
        let status: Option<String> = row.get_by_name("table_status").ok().flatten();
        let source: Option<String> = row.get_by_name("restore_source_backup_arn").ok().flatten();
        Ok(match (status.as_deref(), source) {
            (Some("CREATING"), Some(source)) => Some(source),
            _ => None,
        })
    }

    /// BackupInUse if a registered restore of `backup` is still copying;
    /// otherwise remove every stale registration so it cannot block the
    /// DELETING claim.
    async fn ensure_backup_not_restoring(&self, backup: &StoredBackup) -> Result<(), StorageError> {
        for (table_id, target_name) in &backup.active_restores {
            if self
                .restore_target_in_progress(
                    &backup.account_id,
                    &backup.backup_arn,
                    table_id,
                    target_name,
                )
                .await?
            {
                return Err(backup_in_use(target_name, &backup.backup_arn));
            }
            tracing::info!(
                "Backup {}: clearing stale restore registration of table {target_name} \
                 ({table_id}); the target is no longer being restored",
                backup.backup_arn
            );
            self.unregister_restore(&backup.account_id, &backup.backup_arn, table_id)
                .await?;
        }
        Ok(())
    }

    // ── Restore copy (detached) ─────────────────────────────────────────────

    /// Start the heartbeat of the restore target `table_id`, whose row has
    /// just been inserted. Called by the restore path of `create_table` right
    /// after the insert, before the target's data and index DDL, so the time
    /// that DDL takes (each statement waits for schema agreement) does not
    /// count against [`RESTORE_STALE_AFTER`].
    pub(crate) fn start_restore_heartbeat(
        &self,
        account_id: &str,
        target_name: &str,
        table_id: &str,
        owner: uuid::Uuid,
    ) -> RestoreHeartbeat {
        let state = Arc::new((
            self.clone(),
            HeartbeatTarget {
                account_id: account_id.to_owned(),
                target_name: target_name.to_owned(),
                table_id: table_id.to_owned(),
                owner,
            },
        ));
        RestoreHeartbeat::spawn(
            RESTORE_HEARTBEAT_INTERVAL,
            target_name.to_owned(),
            move || {
                let state = Arc::clone(&state);
                async move { state.0.heartbeat_restore_target(&state.1).await }
            },
        )
    }

    async fn heartbeat_restore_target(&self, job: &HeartbeatTarget) -> Result<bool, StorageError> {
        let query = format!(
            "UPDATE {}.tables SET restore_heartbeat = ? WHERE account_id = ? AND table_name = ? \
             IF table_id = ? AND table_status = 'CREATING' AND restore_owner = ?",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(
                crate::cassandra_util::now_millis(),
                job.account_id.as_str(),
                job.target_name.as_str(),
                job.table_id.as_str(),
                job.owner
            ),
        )
        .await?;
        crate::cassandra_util::lwt_applied(&result)
    }

    async fn activate_restore_target(
        &self,
        job: &RestoreJob,
        item_count: i64,
        table_size_bytes: i64,
    ) -> Result<bool, StorageError> {
        let query = format!(
            "UPDATE {}.tables SET table_status = 'ACTIVE', status_transition_at = NULL, \
             item_count = ?, table_size_bytes = ? WHERE account_id = ? AND table_name = ? \
             IF table_id = ? AND table_status = 'CREATING' AND restore_owner = ?",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(
                item_count,
                table_size_bytes,
                job.account_id.as_str(),
                job.target_name.as_str(),
                job.table_id.as_str(),
                job.owner
            ),
        )
        .await?;
        crate::cassandra_util::lwt_applied(&result)
    }

    /// Remove a restore target this caller has claimed (or that never had a
    /// copy): unregister it from its backup first, so a crash between the two
    /// steps leaves a target the sweep removes rather than a registration
    /// that names nothing; then drop the incarnation with this `table_id`.
    async fn remove_restore_target(
        &self,
        account_id: &str,
        target_name: &str,
        table_id: &str,
        backup_arn: &str,
    ) -> Result<(), StorageError> {
        self.unregister_restore(account_id, backup_arn, table_id)
            .await?;
        match self
            .delete_table_as(
                account_id,
                DeleteTableInput {
                    table_name: target_name.to_owned(),
                },
                DeleteTableCaller::RestoreCleanup { table_id },
            )
            .await
        {
            Ok(_) | Err(StorageError::TableNotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// The copy failed while this process still owned the target: claim it
    /// (CREATING -> DELETING, owner-fenced) and remove it.
    async fn abandon_restore_target(&self, job: &RestoreJob) {
        let query = format!(
            "UPDATE {}.tables SET table_status = 'DELETING' WHERE account_id = ? AND table_name = ? \
             IF table_id = ? AND table_status = 'CREATING' AND restore_owner = ?",
            self.catalog_keyspace()
        );
        let claimed = crate::cassandra_util::query_lwt(
            &self.session,
            &query,
            cdrs_tokio::query_values!(
                job.account_id.as_str(),
                job.target_name.as_str(),
                job.table_id.as_str(),
                job.owner
            ),
        )
        .await
        .and_then(|result| crate::cassandra_util::lwt_applied(&result));
        match claimed {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    "Restore of table {} from {}: target no longer owned by this process; \
                     leaving it to its new owner",
                    job.target_name,
                    job.backup_arn
                );
                return;
            }
            Err(error) => {
                tracing::error!(
                    "Restore of table {} from {}: could not claim the failed target for \
                     removal ({error}); the abandoned-restore sweep removes it",
                    job.target_name,
                    job.backup_arn
                );
                return;
            }
        }
        match self
            .remove_restore_target(
                &job.account_id,
                &job.target_name,
                &job.table_id,
                &job.backup_arn,
            )
            .await
        {
            Ok(()) => tracing::info!(
                "Restore of table {} from {}: removed the failed target",
                job.target_name,
                job.backup_arn
            ),
            Err(error) => tracing::error!(
                "Restore of table {} from {}: removing the failed target failed ({error}); \
                 the abandoned-restore sweep retries it",
                job.target_name,
                job.backup_arn
            ),
        }
    }

    async fn copy_backup_into_target(
        &self,
        job: &RestoreJob,
        heartbeat: &RestoreHeartbeat,
    ) -> Result<(i64, i64), CopyError> {
        let key_info = TableKeyInfo {
            table_name: job.target_name.clone(),
            account_id: job.account_id.clone(),
            table_id: job.table_id.clone(),
            key_schema: job.key_schema.clone(),
            base_key_schema: job.key_schema.clone(),
            attribute_definitions: job.attribute_definitions.clone(),
            // The restore write path reads the target's indexes from the
            // catalog by table_id; these routing fields are not consulted by it.
            has_lsi: false,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            stream_specification: None,
            vector_indexes: Vec::new(),
        };
        let account_keyspace = self.account_keyspace(&job.account_id);
        let writer = self
            .restore_writer(&key_info)
            .await
            .map_err(CopyError::Failed)?;
        let read_bucket = |bucket: i64| {
            let account_keyspace = &account_keyspace;
            async move {
                if heartbeat.is_lost() {
                    return Err(CopyError::OwnershipLost);
                }
                self.read_backup_bucket(account_keyspace, &job.backup_arn, bucket)
                    .await
                    .map_err(CopyError::Failed)
            }
        };
        // Buckets are read ahead of the writes so the copy is never waiting on
        // a payload read with nothing in flight; the write stage runs
        // RESTORE_WRITE_CONCURRENCY items at a time across bucket boundaries.
        let mut restored_count = 0_i64;
        let mut restored_size = 0_i64;
        let mut writes = futures::stream::iter(0..bucket_count(job.item_count))
            .map(read_bucket)
            .buffered(RESTORE_READ_AHEAD_BUCKETS)
            .map_ok(|items| futures::stream::iter(items.into_iter().map(Ok)))
            .try_flatten()
            .map_ok(|(item, item_len)| {
                let writer = &writer;
                let heartbeat = &heartbeat;
                async move {
                    // The read stage checks ownership per bucket; with two
                    // buckets read ahead and 64 writes in flight, up to three
                    // buckets of items would otherwise still be written into
                    // a target the sweep has claimed.
                    if heartbeat.is_lost() {
                        return Err(CopyError::OwnershipLost);
                    }
                    self.write_restored_item(writer, &item)
                        .await
                        .map_err(CopyError::Failed)?;
                    Ok::<usize, CopyError>(item_len)
                }
            })
            .try_buffer_unordered(RESTORE_WRITE_CONCURRENCY);
        while let Some(result) = writes.next().await {
            let item_len = result?;
            #[allow(clippy::cast_possible_wrap)]
            let item_len = item_len as i64;
            restored_size = restored_size.saturating_add(item_len);
            restored_count += 1;
        }
        if restored_count != job.item_count {
            return Err(CopyError::Failed(StorageError::Internal(format!(
                "Backup item count mismatch: expected {}, restored {restored_count}",
                job.item_count
            ))));
        }
        Ok((restored_count, job.backup_size_bytes.max(restored_size)))
    }

    /// The detached restore task: copy, then activate or remove. `heartbeat`
    /// was started when the target row was inserted; it is dropped (which
    /// aborts it) when the copy ends, and on every other exit from this
    /// function, a panic or cancellation included.
    async fn run_restore_copy(self, job: RestoreJob, heartbeat: RestoreHeartbeat) {
        let outcome = self.copy_backup_into_target(&job, &heartbeat).await;
        drop(heartbeat);
        match outcome {
            Ok((count, size)) => match self.activate_restore_target(&job, count, size).await {
                Ok(true) => {
                    tracing::info!(
                        "Restored table {} from {} ({count} items); ACTIVE",
                        job.target_name,
                        job.backup_arn
                    );
                    if let Err(error) = self
                        .unregister_restore(&job.account_id, &job.backup_arn, &job.table_id)
                        .await
                    {
                        // Stale now; DeleteBackup clears it.
                        tracing::warn!(
                            "Restore of table {}: could not clear its registration on {}: {error}",
                            job.target_name,
                            job.backup_arn
                        );
                    }
                }
                Ok(false) => tracing::warn!(
                    "Restore of table {} from {}: target claimed by another owner before \
                     activation; not activating",
                    job.target_name,
                    job.backup_arn
                ),
                Err(error) => {
                    tracing::error!(
                        "Restore of table {} from {}: activation failed: {error}",
                        job.target_name,
                        job.backup_arn
                    );
                    self.abandon_restore_target(&job).await;
                }
            },
            Err(CopyError::OwnershipLost) => tracing::warn!(
                "Restore of table {} from {}: stopped; the target was claimed by the \
                 abandoned-restore sweep",
                job.target_name,
                job.backup_arn
            ),
            Err(CopyError::Failed(error)) => {
                tracing::error!(
                    "Restore of table {} from {} failed: {error}",
                    job.target_name,
                    job.backup_arn
                );
                self.abandon_restore_target(&job).await;
            }
        }
    }

    // ── Abandoned-restore sweep ─────────────────────────────────────────────

    /// Remove restore targets whose copy is gone: a CREATING or DELETING
    /// target carrying a restore registration whose heartbeat is older than
    /// `stale_after` (or absent). Returns the names removed.
    ///
    /// Each removal first claims the target with a lightweight transaction
    /// conditioned on the heartbeat it observed, setting DELETING, a fresh
    /// heartbeat, and this engine as owner, so a copy that heartbeats in the
    /// meantime wins and keeps its target, a copy that comes back later finds
    /// itself no longer owner and stops, and two sweepers do not both remove
    /// it. The servers' periodic worker calls this with
    /// [`RESTORE_STALE_AFTER`].
    ///
    /// # Errors
    ///
    /// [`StorageError`] if the catalog cannot be read; a failure to remove one
    /// target is logged and left for the next pass.
    pub async fn sweep_abandoned_restores(
        &self,
        stale_after: Duration,
    ) -> Result<Vec<String>, StorageError> {
        let catalog = self.catalog_keyspace();
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let cutoff = crate::cassandra_util::now_millis() - stale_after.as_millis() as i64;
        let mut removed = Vec::new();
        for status in ["CREATING", "DELETING"] {
            // Cluster-wide filtered scan, as the control-plane poller does for
            // CREATING tables; restore targets are few and short-lived.
            let query = format!(
                "SELECT account_id, table_name, table_id, table_status, \
                 restore_source_backup_arn, restore_heartbeat FROM {catalog}.tables \
                 WHERE table_status = ? ALLOW FILTERING"
            );
            let rows = crate::cassandra_util::query_rows::<StorageError>(
                &self.session,
                &query,
                cdrs_tokio::query_values!(status),
                "restore sweep",
            )
            .await?;
            for row in rows {
                let backup_arn: Option<String> =
                    row.get_by_name("restore_source_backup_arn").ok().flatten();
                let Some(backup_arn) = backup_arn else {
                    continue;
                };
                let heartbeat: Option<i64> = row.get_by_name("restore_heartbeat").ok().flatten();
                if heartbeat.is_some_and(|at| at >= cutoff) {
                    continue;
                }
                let account_id: String = crate::cassandra_util::get_column::<_, StorageError>(
                    &row,
                    "account_id",
                    "restore sweep",
                )?;
                let table_name: String = crate::cassandra_util::get_column::<_, StorageError>(
                    &row,
                    "table_name",
                    "restore sweep",
                )?;
                let table_id: String = crate::cassandra_util::get_column::<_, StorageError>(
                    &row,
                    "table_id",
                    "restore sweep",
                )?;
                match self
                    .claim_abandoned_restore(&account_id, &table_name, &table_id, status, heartbeat)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(error) => {
                        tracing::warn!("Restore sweep: claiming table {table_name}: {error}");
                        continue;
                    }
                }
                match self
                    .remove_restore_target(&account_id, &table_name, &table_id, &backup_arn)
                    .await
                {
                    Ok(()) => {
                        tracing::info!(
                            "Restore sweep: removed abandoned restore target {table_name} \
                             (account {account_id}, restoring from {backup_arn}); its copy \
                             stopped heartbeating"
                        );
                        removed.push(table_name);
                    }
                    Err(error) => tracing::warn!(
                        "Restore sweep: removing abandoned restore target {table_name}: {error}; \
                         retrying on a later pass"
                    ),
                }
            }
        }
        Ok(removed)
    }

    async fn claim_abandoned_restore(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        observed_status: &str,
        observed_heartbeat: Option<i64>,
    ) -> Result<bool, StorageError> {
        let catalog = self.catalog_keyspace();
        let now = crate::cassandra_util::now_millis();
        let result = if let Some(heartbeat) = observed_heartbeat {
            let query = format!(
                "UPDATE {catalog}.tables SET table_status = 'DELETING', restore_owner = ?, \
                 restore_heartbeat = ? WHERE account_id = ? AND table_name = ? \
                 IF table_id = ? AND table_status = ? AND restore_heartbeat = ?"
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(
                    self.restore_owner,
                    now,
                    account_id,
                    table_name,
                    table_id,
                    observed_status,
                    heartbeat
                ),
            )
            .await?
        } else {
            let query = format!(
                "UPDATE {catalog}.tables SET table_status = 'DELETING', restore_owner = ?, \
                 restore_heartbeat = ? WHERE account_id = ? AND table_name = ? \
                 IF table_id = ? AND table_status = ? AND restore_heartbeat = null"
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(
                    self.restore_owner,
                    now,
                    account_id,
                    table_name,
                    table_id,
                    observed_status
                ),
            )
            .await?
        };
        crate::cassandra_util::lwt_applied(&result)
    }

    /// Create the CREATING restore target described by `request.input` and
    /// register the restore on its source backup. Returns the target's
    /// description and the job the copy runs.
    ///
    /// Guarantee (Cassandra has no transaction spanning the target's `tables`
    /// row and the backup's `backups_by_arn` row, so this is two
    /// lightweight transactions, not one atomic step): the target row is
    /// inserted first with `IF NOT EXISTS` (reserving the name, already
    /// carrying the restore columns), then the restore is added to the
    /// backup's `active_restores` with `IF backup_status = 'AVAILABLE'`.
    /// DeleteBackup claims the backup with `IF backup_status = 'AVAILABLE'
    /// AND active_restores = null` on the same partition, so Paxos orders the
    /// two: either the registration lands first and DeleteBackup returns
    /// BackupInUse, or DeleteBackup lands first and the registration fails,
    /// the target is removed, and the restore returns "Backup not found". No
    /// copy starts from a backup DeleteBackup has claimed. What is not
    /// atomic: between the two steps a concurrent DescribeTable can see a
    /// CREATING target of a restore that then fails, and a process exit
    /// between them leaves a target that the abandoned-restore sweep removes
    /// once its heartbeat (refreshed from the insert until the process
    /// exited) is older than
    /// [`RESTORE_STALE_AFTER`].
    async fn register_restore_target(
        &self,
        account_id: &str,
        request: RestoreRequest,
    ) -> Result<(TableDescription, RestoreJob, RestoreHeartbeat), StorageError> {
        let RestoreRequest {
            backup_arn,
            source_table_arn,
            restore_date_time,
            input,
        } = request;
        // Early, unfenced checks so a taken name or a missing or in-use
        // backup does not create and then remove a target. The name is
        // checked first, so a restore of a backup into its own CREATING
        // target's name is refused as the name (TableBeingRestored, the
        // service's TableInUseException) and not as the backup, as on the
        // other backends; the fenced re-checks are the IF NOT EXISTS insert
        // and the registration below. The backup check also clears
        // registrations whose target is no longer being restored (a stale
        // entry a crashed restore left), before this restore has created
        // anything.
        if let Some(source_backup_arn) = self
            .creating_restore_target_under_name(account_id, &input.table_name)
            .await?
        {
            return Err(StorageError::TableBeingRestored {
                table_name: input.table_name.clone(),
                backup_arn: source_backup_arn,
            });
        }
        let backup = self.load_available_backup(account_id, &backup_arn).await?;
        self.ensure_backup_not_restoring(&backup).await?;

        let target_name = input.table_name.clone();
        let key_schema = input.key_schema.clone();
        let attribute_definitions = input.attribute_definitions.clone();
        #[allow(clippy::cast_possible_truncation)]
        let restore_date_time_ms = (restore_date_time * 1_000.0).round() as i64;
        let columns = RestoreTargetColumns {
            table_id: uuid::Uuid::new_v4().to_string(),
            source_backup_arn: backup_arn.clone(),
            source_table_arn,
            restore_date_time_ms,
            owner: self.restore_owner,
        };
        let table_id = columns.table_id.clone();

        // Step 1: the target row (IF NOT EXISTS reserves the name), its
        // indexes, and its data tables, exactly as `input` describes. The
        // heartbeat starts at the row insert; on an error it has already
        // been dropped (and so stopped) when this match sees it.
        let (description, heartbeat) = match self
            .create_table_for_restore_impl(account_id, input, &columns)
            .await
        {
            Ok(created) => created,
            Err(StorageError::TableAlreadyExists(name)) => {
                return Err(StorageError::TableAlreadyExists(name));
            }
            Err(error) => {
                // Part of the target may exist under our table_id.
                if let Err(cleanup) = self
                    .remove_restore_target(account_id, &target_name, &table_id, &backup_arn)
                    .await
                {
                    tracing::warn!(
                        "Restore of table {target_name}: removing the partly created \
                         target failed ({cleanup}); the abandoned-restore sweep removes it"
                    );
                }
                return Err(error);
            }
        };

        // Step 2: register on the backup, fenced on AVAILABLE and on no other
        // registration. See the module docs for what this does and does not
        // guarantee. A refusal caused only by stale registrations is retried
        // once they are cleared, a bounded number of times.
        let mut registered = Ok(false);
        for _ in 0..REGISTRATION_ATTEMPTS {
            registered = self
                .register_restore(account_id, &backup_arn, &table_id, &target_name)
                .await;
            match registered {
                Ok(true) => break,
                Ok(false) => {
                    if let Err(refusal) = self.registration_refusal(account_id, &backup_arn).await {
                        registered = Err(refusal);
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        if !matches!(registered, Ok(true)) {
            drop(heartbeat);
            if let Err(cleanup) = self
                .remove_restore_target(account_id, &target_name, &table_id, &backup_arn)
                .await
            {
                tracing::warn!(
                    "Restore of table {target_name}: removing the unregistered target \
                     failed ({cleanup}); the abandoned-restore sweep removes it"
                );
            }
            return Err(registration_exhausted(registered, &backup_arn));
        }

        // The copy the caller's detached task runs next, with owned clones.
        let job = RestoreJob {
            account_id: account_id.to_owned(),
            target_name,
            table_id,
            backup_arn,
            item_count: backup.item_count,
            backup_size_bytes: backup.backup_size_bytes,
            key_schema,
            attribute_definitions,
            owner: self.restore_owner,
        };
        Ok((description, job, heartbeat))
    }
}

/// The abandoned-restore sweep, for the server's worker set: one sweep at
/// startup, then one every [`RESTORE_SWEEP_INTERVAL`], until `shutdown` is
/// cancelled. A sweep already running when `shutdown` fires finishes first.
pub async fn poll_abandoned_restores(
    engine: Arc<CassandraEngine>,
    shutdown: extenddb_storage::CancellationToken,
) {
    loop {
        if let Err(error) = engine.sweep_abandoned_restores(RESTORE_STALE_AFTER).await {
            tracing::warn!("Abandoned-restore sweep failed: {error}");
        }
        if !extenddb_storage::sleep_or_shutdown(&shutdown, RESTORE_SWEEP_INTERVAL).await {
            return;
        }
    }
}

impl BackupEngine for CassandraEngine {
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let backup_name = backup_name.to_owned();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            self.require_migrated_catalog().await?;

            // fetch_table_key_info enforces the DynamoDB requirement that only
            // ACTIVE tables can be backed up.
            let key_info = self.fetch_table_key_info(&account_id, &table_name).await?;
            let table = self
                .build_table_description(&account_id, &table_name)
                .await?;
            let definition = self
                .backup_definition_for_table(&account_id, &table_name, &table)
                .await?;
            let definition_json = definition.to_json()?.to_string();
            // One instant for the ARN, BackupCreationDateTime, and the stored
            // creation time, so the ListBackups cursor can place the ARN.
            let id = extenddb_storage::util::new_backup_id();
            let backup_arn =
                extenddb_storage::util::backup_arn(&self.region, &account_id, &table_name, &id);
            let created_at = extenddb_storage::util::backup_id_epoch_millis(&id)
                .and_then(|ms| i64::try_from(ms).ok())
                .ok_or_else(|| StorageError::Internal(format!("Backup id {id} has no time")))?;
            #[allow(clippy::cast_possible_truncation)]
            let table_created_at = (table.creation_date_time * 1_000.0) as i64;
            let billing_mode = definition.billing_mode.as_str();
            let key_schema = serde_json::to_string(&table.key_schema)
                .map_err(|e| StorageError::Internal(format!("Serialize key schema: {e}")))?;
            let attribute_definitions = serde_json::to_string(&table.attribute_definitions)
                .map_err(|e| StorageError::Internal(format!("Serialize attributes: {e}")))?;
            let provisioned = ProvisionedThroughput {
                read_capacity_units: table.provisioned_throughput.read_capacity_units,
                write_capacity_units: table.provisioned_throughput.write_capacity_units,
            };
            let provisioned_throughput = serde_json::to_string(&provisioned)
                .map_err(|e| StorageError::Internal(format!("Serialize throughput: {e}")))?;
            let stream_specification = table
                .stream_specification
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| StorageError::Internal(format!("Serialize stream: {e}")))?;

            let catalog = self.catalog_keyspace();
            let insert_metadata = format!(
                "INSERT INTO {catalog}.backups_by_arn (backup_arn, account_id, backup_name, table_id, table_name, table_arn, backup_status, backup_type, backup_size_bytes, item_count, key_schema, attribute_definitions, billing_mode, provisioned_throughput, stream_specification, table_created_at, created_at, definition) VALUES (?, ?, ?, ?, ?, ?, 'CREATING', 'USER', ?, 0, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
            );
            self.session
                .query_with_values(
                    &insert_metadata,
                    cdrs_tokio::query_values!(
                        backup_arn.as_str(),
                        account_id.as_str(),
                        backup_name.as_str(),
                        table.table_id.as_str(),
                        table_name.as_str(),
                        table.table_arn.as_str(),
                        table.table_size_bytes,
                        key_schema.as_str(),
                        attribute_definitions.as_str(),
                        billing_mode,
                        provisioned_throughput.as_str(),
                        stream_specification.as_deref(),
                        table_created_at,
                        created_at,
                        definition_json.as_str()
                    ),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Create backup metadata: {e}")))?;

            let account_keyspace = self.account_keyspace(&account_id);
            let insert_item = format!(
                "INSERT INTO {account_keyspace}.backup_items (backup_arn, bucket, item_index, item_data) VALUES (?, ?, ?, ?)"
            );
            let mut item_count = 0_i64;
            let mut payload_size = 0_i64;
            let mut start_key = None;

            let snapshot_result: Result<(), StorageError> = async {
                loop {
                    let (items, next_key) = self
                        .scan_impl(
                            &key_info,
                            Some(BACKUP_SCAN_PAGE_SIZE),
                            start_key.as_ref(),
                            None,
                            None,
                            None,
                        )
                        .await?;
                    for item in items {
                        let item_data = serde_json::to_string(&item).map_err(|e| {
                            StorageError::Internal(format!("Serialize backup item: {e}"))
                        })?;
                        let bucket = item_count / BACKUP_ITEMS_PER_BUCKET;
                        #[allow(clippy::cast_possible_truncation)]
                        let bucket_i32 = bucket as i32;
                        self.session
                            .query_with_values(
                                &insert_item,
                                cdrs_tokio::query_values!(
                                    backup_arn.as_str(),
                                    bucket_i32,
                                    item_count,
                                    item_data.as_str()
                                ),
                            )
                            .await
                            .map_err(|e| {
                                StorageError::Internal(format!("Write backup item: {e}"))
                            })?;
                        item_count += 1;
                        #[allow(clippy::cast_possible_wrap)]
                        let item_len = item_data.len() as i64;
                        payload_size = payload_size.saturating_add(item_len);
                    }
                    match next_key {
                        Some(key) => start_key = Some(key),
                        None => break,
                    }
                }
                Ok(())
            }
            .await;

            if let Err(error) = snapshot_result {
                let _ = self
                    .cleanup_backup_payload(&account_id, &backup_arn, item_count)
                    .await;
                let delete_metadata = format!(
                    "DELETE FROM {catalog}.backups_by_arn \
                     WHERE account_id = ? AND backup_arn = ? IF EXISTS"
                );
                let _ = crate::cassandra_util::query_lwt(
                    &self.session,
                    &delete_metadata,
                    cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
                )
                .await;
                return Err(error);
            }

            let backup_size_bytes = table.table_size_bytes.max(payload_size);
            let insert_by_account = format!(
                "INSERT INTO {catalog}.backups_by_account (account_id, created_at, backup_arn, backup_name, table_id, table_name, table_arn, backup_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            );
            let insert_by_table = format!(
                "INSERT INTO {catalog}.backups_by_table (account_id, table_name, created_at, backup_arn, backup_name, table_id, table_arn, backup_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            );
            let publish = format!(
                "UPDATE {catalog}.backups_by_arn SET backup_status = 'AVAILABLE', backup_size_bytes = ?, item_count = ? WHERE account_id = ? AND backup_arn = ?"
            );
            // Publication is a fixed three-statement operation. A logged batch
            // prevents readers from observing list rows without the matching
            // AVAILABLE authoritative row, or vice versa.
            let publish_result: Result<(), StorageError> = async {
                let batch = BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
                    .add_query(
                        insert_by_account,
                        cdrs_tokio::query_values!(
                            account_id.as_str(),
                            created_at,
                            backup_arn.as_str(),
                            backup_name.as_str(),
                            table.table_id.as_str(),
                            table_name.as_str(),
                            table.table_arn.as_str(),
                            backup_size_bytes
                        ),
                    )
                    .add_query(
                        insert_by_table,
                        cdrs_tokio::query_values!(
                            account_id.as_str(),
                            table_name.as_str(),
                            created_at,
                            backup_arn.as_str(),
                            backup_name.as_str(),
                            table.table_id.as_str(),
                            table.table_arn.as_str(),
                            backup_size_bytes
                        ),
                    )
                    .add_query(
                        publish,
                        cdrs_tokio::query_values!(
                            backup_size_bytes,
                            item_count,
                            account_id.as_str(),
                            backup_arn.as_str()
                        ),
                    );
                self.session
                    .batch(
                        batch
                            .build()
                            .map_err(|e| StorageError::Internal(e.to_string()))?,
                    )
                    .await
                    .map_err(|e| StorageError::Internal(format!("Publish backup batch: {e}")))?;
                Ok(())
            }
            .await;

            if let Err(error) = publish_result {
                if let Ok(backup) = self.load_backup(&account_id, &backup_arn).await {
                    let _ = self.remove_backup_index_rows(&backup).await;
                }
                let _ = self
                    .cleanup_backup_payload(&account_id, &backup_arn, item_count)
                    .await;
                let delete_metadata = format!(
                    "DELETE FROM {catalog}.backups_by_arn \
                     WHERE account_id = ? AND backup_arn = ? IF EXISTS"
                );
                let _ = crate::cassandra_util::query_lwt(
                    &self.session,
                    &delete_metadata,
                    cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
                )
                .await;
                return Err(error);
            }

            Ok(BackupDetails {
                backup_arn,
                backup_name,
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes,
                backup_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(created_at),
            })
        })
    }

    fn describe_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            self.load_available_backup(&account_id, &backup_arn)
                .await?
                .description()
        })
    }

    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(ToOwned::to_owned);
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let catalog = self.catalog_keyspace();
            let result = if let Some(table_name) = table_name.as_deref() {
                let query = format!(
                    "SELECT backup_arn FROM {catalog}.backups_by_table WHERE account_id = ? AND table_name = ?"
                );
                self.session
                    .query_with_values(
                        &query,
                        cdrs_tokio::query_values!(account_id.as_str(), table_name),
                    )
                    .await
            } else {
                let query = format!(
                    "SELECT backup_arn FROM {catalog}.backups_by_account WHERE account_id = ?"
                );
                self.session
                    .query_with_values(&query, cdrs_tokio::query_values!(account_id.as_str()))
                    .await
            };
            let rows = result
                .map_err(|e| StorageError::Internal(format!("List backups: {e}")))?
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse backup list: {e}")))?
                .into_rows()
                .unwrap_or_default();

            let mut summaries = Vec::with_capacity(rows.len());
            for row in rows {
                let backup_arn: String = row
                    .get_r_by_name("backup_arn")
                    .map_err(|e| StorageError::Internal(format!("Parse backup_arn: {e}")))?;
                match self.load_available_backup(&account_id, &backup_arn).await {
                    Ok(backup) => summaries.push(backup.summary()),
                    Err(StorageError::Validation(_)) => {
                        // A stale denormalized row from an interrupted create or
                        // delete must never advertise an unavailable backup.
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(summaries)
        })
    }

    fn delete_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            /// Claim attempts when a restore registers or unregisters between
            /// the registration check and the DELETING claim.
            const CLAIM_ATTEMPTS: usize = 3;

            self.require_migrated_catalog().await?;
            let mut backup = self.load_backup(&account_id, &backup_arn).await?;
            if backup.backup_status != "AVAILABLE" && backup.backup_status != "DELETING" {
                return Err(backup_not_found(&backup_arn));
            }
            let original_description = backup.description()?;
            // The claim is conditioned on the registration map being empty, on
            // the same partition the restore registers on (see the module
            // docs), so a registered restore keeps the backup AVAILABLE.
            let mark_deleting = format!(
                "UPDATE {}.backups_by_arn SET backup_status = 'DELETING' \
                 WHERE account_id = ? AND backup_arn = ? \
                 IF backup_status = 'AVAILABLE' AND active_restores = null",
                self.catalog_keyspace()
            );
            let mut attempt = 0;
            while backup.backup_status == "AVAILABLE" {
                self.ensure_backup_not_restoring(&backup).await?;
                let result = crate::cassandra_util::query_lwt(
                    &self.session,
                    &mark_deleting,
                    cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Mark backup deleting: {e}")))?;
                if crate::cassandra_util::lwt_applied(&result)? {
                    break;
                }
                attempt += 1;
                backup = self.load_backup(&account_id, &backup_arn).await?;
                match backup.backup_status.as_str() {
                    "AVAILABLE" if attempt < CLAIM_ATTEMPTS => {}
                    "AVAILABLE" => {
                        let target = backup
                            .active_restores
                            .values()
                            .next()
                            .cloned()
                            .unwrap_or_default();
                        return Err(backup_in_use(&target, &backup_arn));
                    }
                    // A concurrent DeleteBackup claimed it; finishing the
                    // cleanup is idempotent.
                    "DELETING" => {}
                    _ => return Err(backup_not_found(&backup_arn)),
                }
            }

            self.remove_backup_index_rows(&backup).await?;
            self.cleanup_backup_payload(&account_id, &backup_arn, backup.item_count)
                .await?;

            let mark_deleted = format!(
                "UPDATE {}.backups_by_arn SET backup_status = 'DELETED' \
                 WHERE account_id = ? AND backup_arn = ? \
                 IF backup_status = 'DELETING'",
                self.catalog_keyspace()
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &mark_deleted,
                cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Mark backup deleted: {e}")))?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_status: "DELETED".to_owned(),
                    ..original_description.backup_details
                },
                source_table_details: original_description.source_table_details,
                source_table_feature_details: None,
            })
        })
    }

    fn backup_restore_source(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupRestoreSource, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let backup = self.load_available_backup(&account_id, &backup_arn).await?;
            let definition = backup.parsed_definition()?;
            let provisioned_throughput = match &definition {
                Some(definition) => definition.provisioned_throughput.clone(),
                // A backup row written before V005: the throughput column it
                // has always carried. That column holds {0, 0} for an
                // on-demand source; treat a non-positive value as unrecorded,
                // as the restore of catalog 0.0.3 did (it fell back to 5/5).
                None => {
                    let value = backup
                        .provisioned_throughput
                        .as_deref()
                        .map(serde_json::from_str::<serde_json::Value>)
                        .transpose()
                        .map_err(|e| StorageError::Internal(format!("Parse throughput: {e}")))?;
                    throughput_from_catalog(value.as_ref())
                        .filter(|pt| pt.read_capacity_units > 0 && pt.write_capacity_units > 0)
                }
            };
            Ok(BackupRestoreSource {
                backup_arn: backup.backup_arn.clone(),
                backup_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(
                    backup.created_at,
                ),
                source_table_name: backup.table_name.clone(),
                source_table_arn: backup.table_arn.clone(),
                source_table_id: backup.table_id.clone(),
                key_schema: backup.parsed_key_schema()?,
                attribute_definitions: backup.parsed_attribute_definitions()?,
                billing_mode: backup.billing_mode.clone(),
                provisioned_throughput,
                item_count: backup.item_count,
                table_size_bytes: backup.backup_size_bytes,
                table_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(
                    backup.table_created_at,
                ),
                definition,
                legacy_vector_index_count: 0,
            })
        })
    }

    fn restore_table_from_backup(
        &self,
        account_id: &str,
        request: RestoreRequest,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            // Registration and copy both run in one detached task that owns a
            // clone of the engine and the request. The request future only
            // waits for the registration outcome, so dropping it (a client
            // timeout) at any await point after the first poll does not stop
            // the restore; before the first poll nothing has been done.
            // Contrast: if registration ran here and only the copy were
            // spawned, a drop between creating the target and spawning would
            // leave a CREATING target with no copy until the sweep removed it.
            let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
            let engine = self.clone();
            tokio::spawn(async move {
                // Inside the task, like the registration, so the first poll
                // still spawns before any await.
                if let Err(error) = engine.require_migrated_catalog().await {
                    let _ = registered_tx.send(Err(error));
                    return;
                }
                match engine.register_restore_target(&account_id, request).await {
                    Ok((description, job, heartbeat)) => {
                        // The receiver is gone if the request was dropped;
                        // the copy runs regardless.
                        let _ = registered_tx.send(Ok(description));
                        engine.run_restore_copy(job, heartbeat).await;
                    }
                    Err(error) => {
                        let _ = registered_tx.send(Err(error));
                    }
                }
            });
            registered_rx.await.map_err(|_| {
                StorageError::Internal("restore task ended before registering".to_owned())
            })?
        })
    }

    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            if !self
                .table_exists_for_backup(&account_id, &table_name)
                .await?
            {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }
            let query = format!(
                "SELECT pitr_enabled FROM {}.continuous_backups WHERE account_id = ? AND table_name = ?",
                self.catalog_keyspace()
            );
            let rows = self
                .session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Query continuous backups: {e}")))?
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse continuous backups: {e}")))?
                .into_rows()
                .unwrap_or_default();
            let pitr_enabled = rows
                .first()
                .and_then(|row| {
                    let value: Result<bool, _> = row.get_r_by_name("pitr_enabled");
                    value.ok()
                })
                .unwrap_or(false);
            let now =
                crate::cassandra_util::millis_to_seconds_f64(crate::cassandra_util::now_millis());
            Ok(ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                    point_in_time_recovery_status: if pitr_enabled {
                        "ENABLED".to_owned()
                    } else {
                        "DISABLED".to_owned()
                    },
                    earliest_restorable_date_time: pitr_enabled
                        .then_some(now - 35.0 * 24.0 * 3_600.0),
                    latest_restorable_date_time: pitr_enabled.then_some(now),
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
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            if !self
                .table_exists_for_backup(&account_id, &table_name)
                .await?
            {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }
            let query = format!(
                "INSERT INTO {}.continuous_backups (account_id, table_name, pitr_enabled) VALUES (?, ?, ?)",
                self.catalog_keyspace()
            );
            self.session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(
                        account_id.as_str(),
                        table_name.as_str(),
                        pitr_enabled
                    ),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Update continuous backups: {e}")))?;
            self.describe_continuous_backups(&account_id, &table_name)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_buckets_are_bounded() {
        assert_eq!(bucket_count(0), 0);
        assert_eq!(bucket_count(1), 1);
        assert_eq!(bucket_count(BACKUP_ITEMS_PER_BUCKET), 1);
        assert_eq!(bucket_count(BACKUP_ITEMS_PER_BUCKET + 1), 2);
    }

    #[test]
    fn a_restore_is_declared_abandoned_only_after_missed_heartbeats() {
        assert!(RESTORE_STALE_AFTER >= RESTORE_HEARTBEAT_INTERVAL * 4);
    }

    /// Registration attempts that all lost to a restore just starting end in
    /// `BackupInUse` naming that, not in a missing backup; an error from the
    /// attempts themselves is reported as it is.
    #[test]
    fn exhausted_registration_attempts_are_backup_in_use() {
        const ARN: &str =
            "arn:aws:dynamodb:us-east-1:123456789012:table/t/backup/01700000000000-abc";
        match registration_exhausted(Ok(false), ARN) {
            StorageError::BackupInUse(m) => assert_eq!(
                m,
                format!("Backup is being used by another restore that is starting; retry: {ARN}")
            ),
            other => panic!("exhausted attempts are BackupInUse: {other:?}"),
        }
        assert!(matches!(
            registration_exhausted(Err(StorageError::Internal("db".to_owned())), ARN),
            StorageError::Internal(m) if m == "db"
        ));
    }

    use std::sync::atomic::AtomicUsize;

    /// A heartbeat that counts its beats, every 10 ms, answering `owned`.
    fn counting_heartbeat(beats: &Arc<AtomicUsize>, owned: bool) -> RestoreHeartbeat {
        let beats = Arc::clone(beats);
        RestoreHeartbeat::spawn(Duration::from_millis(10), "T".to_owned(), move || {
            beats.fetch_add(1, Ordering::SeqCst);
            async move { Ok(owned) }
        })
    }

    /// Wait until `beats` reaches `at_least`; panic after 5 s.
    async fn wait_for_beats(beats: &AtomicUsize, at_least: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while beats.load(Ordering::SeqCst) < at_least {
            assert!(std::time::Instant::now() < deadline, "heartbeat never ran");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Assert no beat happens over 20 heartbeat intervals.
    async fn assert_stopped(beats: &AtomicUsize) {
        let stopped_at = beats.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            beats.load(Ordering::SeqCst),
            stopped_at,
            "the heartbeat kept running after its owner ended"
        );
    }

    #[tokio::test]
    async fn the_heartbeat_stops_when_the_task_owning_it_panics() {
        let beats = Arc::new(AtomicUsize::new(0));
        let owner = {
            let beats = Arc::clone(&beats);
            tokio::spawn(async move {
                let _heartbeat = counting_heartbeat(&beats, true);
                wait_for_beats(&beats, 3).await;
                panic!("copy panicked");
            })
        };
        let joined = owner.await;
        assert!(
            joined.is_err_and(|e| e.is_panic()),
            "the owner must have panicked"
        );
        assert!(
            beats.load(Ordering::SeqCst) >= 3,
            "the heartbeat ran before the panic"
        );
        assert_stopped(&beats).await;
    }

    #[tokio::test]
    async fn the_heartbeat_stops_when_the_future_owning_it_is_dropped() {
        let beats = Arc::new(AtomicUsize::new(0));
        let owner = {
            let beats = Arc::clone(&beats);
            async move {
                let _heartbeat = counting_heartbeat(&beats, true);
                futures::future::pending::<()>().await;
            }
        };
        // Cancellation: the owning future is dropped at its await point.
        let timed_out = tokio::time::timeout(Duration::from_millis(100), owner).await;
        assert!(timed_out.is_err());
        assert!(
            beats.load(Ordering::SeqCst) >= 2,
            "the heartbeat ran before the drop"
        );
        assert_stopped(&beats).await;
    }

    #[tokio::test]
    async fn a_lost_target_stops_the_heartbeat_and_is_reported() {
        let beats = Arc::new(AtomicUsize::new(0));
        let heartbeat = counting_heartbeat(&beats, false);
        wait_for_beats(&beats, 1).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !heartbeat.is_lost() {
            assert!(std::time::Instant::now() < deadline, "loss never reported");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_stopped(&beats).await;
        assert_eq!(beats.load(Ordering::SeqCst), 1, "one beat, then stopped");
    }
}
