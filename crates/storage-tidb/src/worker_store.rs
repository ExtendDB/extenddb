// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and control plane transition processing.

use futures::{StreamExt, future::BoxFuture, stream};

use extenddb_core::types::{AttributeDefinition, KeySchemaElement, StreamSpecification};
use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::TidbEngine;
use crate::tidb_util::is_table_not_found_tidb_storage_error;

type CreatingTableRow = (
    String,
    serde_json::Value,
    serde_json::Value,
    Option<serde_json::Value>,
);
type CreateIndexRow = (String, serde_json::Value);
type UpdatingTableRow = (
    String,
    serde_json::Value,
    Option<serde_json::Value>,
    Option<String>,
    String,
);
type PendingIndexRow = (String, String, String, serde_json::Value);

const CONTROL_PLANE_TRANSITION_CONCURRENCY: usize = 16;
const CONTROL_PLANE_TRANSITION_SCAN_LIMIT: i64 = 256;

struct CreateReconcilePlan {
    table_name: String,
    key_schema: Vec<KeySchemaElement>,
    attr_defs: Vec<AttributeDefinition>,
    stream_enabled: bool,
    indexes: Vec<(String, Vec<KeySchemaElement>)>,
}

struct UpdateReconcilePlan {
    table_name: String,
    base_attr_defs: Vec<AttributeDefinition>,
    stream_enabled: bool,
    ttl_attribute: Option<String>,
    ttl_status: String,
    pending_indexes: Vec<PendingIndexPlan>,
}

struct PendingIndexPlan {
    index_id: String,
    index_name: String,
    index_status: String,
    key_schema: Vec<KeySchemaElement>,
}

struct DeleteReconcilePlan {
    table_name: String,
    table_arn: String,
    table_id: String,
}

#[derive(sqlx::FromRow)]
struct ControlPlaneTransitionRow {
    table_status: String,
    table_name: String,
    table_id: String,
    table_arn: String,
}

enum ControlPlaneReconcilePlan {
    Create { table_id: String },
    Update { table_id: String },
    Delete(DeleteReconcilePlan),
}

impl ControlPlaneReconcilePlan {
    fn from_row(row: ControlPlaneTransitionRow) -> Result<Self, StorageError> {
        match row.table_status.as_str() {
            "CREATING" => Ok(Self::Create {
                table_id: row.table_id,
            }),
            "UPDATING" => Ok(Self::Update {
                table_id: row.table_id,
            }),
            "DELETING" => Ok(Self::Delete(DeleteReconcilePlan {
                table_name: row.table_name,
                table_arn: row.table_arn,
                table_id: row.table_id,
            })),
            other => Err(StorageError::Internal(format!(
                "unknown TiDB control-plane table status: {other}"
            ))),
        }
    }
}

fn parse_json<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
    label: &str,
) -> Result<T, StorageError> {
    serde_json::from_value(value)
        .map_err(|e| StorageError::Internal(format!("invalid {label}: {e}")))
}

fn index_id_placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn mark_creating_indexes_active_sql(count: usize) -> String {
    format!(
        "UPDATE indexes SET index_status = 'ACTIVE' \
         WHERE table_id = ? AND index_status = 'CREATING' \
           AND index_id IN ({}) \
           AND EXISTS ( \
               SELECT 1 FROM tables \
               WHERE tables.table_id = indexes.table_id \
                 AND tables.table_status = 'UPDATING' \
           )",
        index_id_placeholders(count)
    )
}

fn delete_pending_indexes_sql(count: usize) -> String {
    format!(
        "DELETE FROM indexes \
         WHERE table_id = ? AND index_status = 'DELETING' \
           AND index_id IN ({}) \
           AND EXISTS ( \
               SELECT 1 FROM tables \
               WHERE tables.table_id = indexes.table_id \
                 AND tables.table_status = 'UPDATING' \
           )",
        index_id_placeholders(count)
    )
}

impl WorkerStore for TidbEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move { Self::process_control_plane_transitions(self).await })
    }
}

impl TidbEngine {
    async fn drop_table_data_artifacts(&self, table_id: &str) -> Result<(), StorageError> {
        // `stream_records` is a shared TiDB TTL table keyed by immutable table
        // ids. Deleting a table must not issue a large foreground delete over
        // stream history; native TTL owns retention and the removed catalog row
        // makes the stream inaccessible to new requests.
        Self::drop_data_table(&self.data_pool, table_id).await?;
        Ok(())
    }

    async fn drop_create_artifacts_if_table_was_deleted(
        &self,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let status: Option<String> =
            sqlx::query_scalar("SELECT table_status FROM tables WHERE table_id = ?")
                .bind(table_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

        if matches!(status.as_deref(), None | Some("DELETING")) {
            self.drop_table_data_artifacts(table_id).await?;
        }

        Ok(())
    }

    async fn ensure_stream_label_for_table_id(&self, table_id: &str) -> Result<(), StorageError> {
        let label = Self::new_stream_label();
        sqlx::query(
            "UPDATE tables SET stream_label = COALESCE(stream_label, ?) \
             WHERE table_id = ? AND table_status IN ('CREATING', 'UPDATING', 'ACTIVE')",
        )
        .bind(&label)
        .bind(table_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Reconcile a CREATING table by replaying the durable catalog row as
    /// idempotent TiDB online DDL, then publishing ACTIVE when the catalog row
    /// still represents the same pending transition.
    pub(crate) async fn reconcile_table_create(
        &self,
        table_id: &str,
    ) -> Result<Option<String>, StorageError> {
        let row: Option<CreatingTableRow> = sqlx::query_as(
            "SELECT table_name, key_schema, attribute_definitions, stream_specification \
             FROM tables \
             WHERE table_id = ? AND table_status = 'CREATING' \
               AND status_transition_at <= CURRENT_TIMESTAMP(6)",
        )
        .bind(table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((table_name, key_schema_json, attr_defs_json, stream_json)) = row else {
            return Ok(None);
        };

        let key_schema: Vec<KeySchemaElement> = parse_json(key_schema_json, "table key schema")?;
        let attr_defs: Vec<AttributeDefinition> =
            parse_json(attr_defs_json, "table attribute definitions")?;
        let stream_spec: Option<StreamSpecification> = stream_json
            .map(|v| parse_json(v, "stream specification"))
            .transpose()?;
        let stream_enabled = stream_spec.as_ref().is_some_and(|spec| spec.stream_enabled);

        let index_rows: Vec<CreateIndexRow> =
            sqlx::query_as("SELECT index_id, key_schema FROM indexes WHERE table_id = ?")
                .bind(table_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        let indexes = index_rows
            .into_iter()
            .map(|(index_id, index_key_schema_json)| {
                parse_json(index_key_schema_json, "index key schema")
                    .map(|index_key_schema| (index_id, index_key_schema))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let plan = CreateReconcilePlan {
            table_name,
            key_schema,
            attr_defs,
            stream_enabled,
            indexes,
        };

        let indexes = plan
            .indexes
            .iter()
            .map(|(index_id, key_schema)| (index_id.as_str(), key_schema.as_slice()))
            .collect::<Vec<_>>();
        Self::create_data_table(
            &self.data_pool,
            table_id,
            &plan.key_schema,
            &plan.attr_defs,
            &indexes,
        )
        .await?;

        if plan.stream_enabled {
            self.ensure_stream_label_for_table_id(table_id).await?;
        }

        let result = sqlx::query(
            "UPDATE tables \
             SET table_status = 'ACTIVE', status_transition_at = NULL \
             WHERE table_id = ? AND table_status = 'CREATING' \
               AND status_transition_at <= CURRENT_TIMESTAMP(6)",
        )
        .bind(table_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() > 0 {
            Ok(Some(plan.table_name))
        } else {
            self.drop_create_artifacts_if_table_was_deleted(table_id)
                .await?;
            Ok(None)
        }
    }

    /// Reconcile an UPDATING table. Pending GSI creates/deletes are retried
    /// from catalog metadata until complete. TiDB stream shards are fixed and
    /// derived from the table id, so stream metadata does not require data-side
    /// shard rows.
    async fn reconcile_table_update(&self, table_id: &str) -> Result<Option<String>, StorageError> {
        let row: Option<UpdatingTableRow> = sqlx::query_as(
            "SELECT table_name, attribute_definitions, \
                    stream_specification, ttl_attribute, ttl_status \
             FROM tables \
             WHERE table_id = ? AND table_status = 'UPDATING'",
        )
        .bind(table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((table_name, attr_defs_json, stream_json, ttl_attribute, ttl_status)) = row else {
            return Ok(None);
        };

        let base_attr_defs: Vec<AttributeDefinition> =
            parse_json(attr_defs_json, "table attribute definitions")?;
        let stream_spec: Option<StreamSpecification> = stream_json
            .map(|v| parse_json(v, "stream specification"))
            .transpose()?;
        let stream_enabled = stream_spec.as_ref().is_some_and(|spec| spec.stream_enabled);

        let pending_indexes: Vec<PendingIndexRow> = sqlx::query_as(
            "SELECT index_id, index_name, index_status, key_schema \
             FROM indexes \
             WHERE table_id = ? AND index_type = 'GSI' \
               AND index_status IN ('CREATING', 'DELETING') \
             ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let pending_indexes = pending_indexes
            .into_iter()
            .map(|(index_id, index_name, index_status, key_schema_json)| {
                let key_schema = parse_json(key_schema_json, "index key schema")?;
                Ok(PendingIndexPlan {
                    index_id,
                    index_name,
                    index_status,
                    key_schema,
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;

        let plan = UpdateReconcilePlan {
            table_name,
            base_attr_defs,
            stream_enabled,
            ttl_attribute,
            ttl_status,
            pending_indexes,
        };

        if plan.stream_enabled {
            self.ensure_stream_label_for_table_id(table_id).await?;
        }

        self.reconcile_native_ttl_transition(
            table_id,
            plan.ttl_attribute.as_deref(),
            &plan.ttl_status,
        )
        .await?;

        self.reconcile_pending_indexes(table_id, plan.pending_indexes, &plan.base_attr_defs)
            .await?;

        let result = sqlx::query(
            "UPDATE tables \
             SET table_status = 'ACTIVE', status_transition_at = NULL \
             WHERE table_id = ? AND table_status = 'UPDATING' \
               AND ttl_status NOT IN ('ENABLING', 'DISABLING') \
               AND NOT EXISTS ( \
                   SELECT 1 FROM indexes \
                   WHERE indexes.table_id = tables.table_id \
                     AND index_status IN ('CREATING', 'DELETING') \
               )",
        )
        .bind(table_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() > 0 {
            Ok(Some(plan.table_name))
        } else {
            Ok(None)
        }
    }

    async fn reconcile_pending_indexes(
        &self,
        table_id: &str,
        pending_indexes: Vec<PendingIndexPlan>,
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let mut creating = Vec::new();
        let mut deleting = Vec::new();
        for pending in pending_indexes {
            match pending.index_status.as_str() {
                "CREATING" => creating.push(pending),
                "DELETING" => deleting.push(pending),
                other => {
                    return Err(StorageError::Internal(format!(
                        "unknown pending GSI status for {}: {other}",
                        pending.index_name
                    )));
                }
            }
        }

        self.reconcile_creating_indexes(table_id, &creating, base_attr_defs)
            .await?;
        self.reconcile_deleting_indexes(table_id, &deleting, base_attr_defs)
            .await?;
        Ok(())
    }

    async fn reconcile_creating_indexes(
        &self,
        table_id: &str,
        pending_indexes: &[PendingIndexPlan],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        if pending_indexes.is_empty() || !self.table_status_matches(table_id, "UPDATING").await? {
            return Ok(());
        }

        let indexes = pending_indexes
            .iter()
            .map(|pending| (pending.index_id.as_str(), pending.key_schema.as_slice()))
            .collect::<Vec<_>>();
        self.create_index_artifacts_batch_for_pending_create(table_id, &indexes, base_attr_defs)
            .await?;

        let sql = mark_creating_indexes_active_sql(pending_indexes.len());
        let mut query = sqlx::query(&sql).bind(table_id);
        for pending in pending_indexes {
            query = query.bind(&pending.index_id);
        }
        query
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn reconcile_deleting_indexes(
        &self,
        table_id: &str,
        pending_indexes: &[PendingIndexPlan],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        if pending_indexes.is_empty() || !self.table_status_matches(table_id, "UPDATING").await? {
            return Ok(());
        }

        let indexes = pending_indexes
            .iter()
            .map(|pending| (pending.index_id.as_str(), pending.key_schema.as_slice()))
            .collect::<Vec<_>>();
        self.drop_index_artifacts_batch_for_pending_removal(table_id, &indexes, base_attr_defs)
            .await?;

        let sql = delete_pending_indexes_sql(pending_indexes.len());
        let mut query = sqlx::query(&sql).bind(table_id);
        for pending in pending_indexes {
            query = query.bind(&pending.index_id);
        }
        query
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn fetch_table_status(&self, table_id: &str) -> Result<Option<String>, StorageError> {
        sqlx::query_scalar("SELECT table_status FROM tables WHERE table_id = ?")
            .bind(table_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))
    }

    async fn table_status_matches(
        &self,
        table_id: &str,
        expected_status: &str,
    ) -> Result<bool, StorageError> {
        let status = self.fetch_table_status(table_id).await?;

        Ok(status.as_deref() == Some(expected_status))
    }

    async fn table_is_deleting_or_absent(&self, table_id: &str) -> Result<bool, StorageError> {
        let status = self.fetch_table_status(table_id).await?;
        Ok(matches!(status.as_deref(), None | Some("DELETING")))
    }

    async fn create_index_artifacts_batch_for_pending_create(
        &self,
        table_id: &str,
        indexes: &[(&str, &[KeySchemaElement])],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        match Self::create_index_artifacts_batch(&self.data_pool, table_id, indexes, attr_defs)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if is_table_not_found_tidb_storage_error(&err) => {
                if self.table_is_deleting_or_absent(table_id).await? {
                    Ok(())
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn drop_index_artifacts_batch_for_pending_removal(
        &self,
        table_id: &str,
        indexes: &[(&str, &[KeySchemaElement])],
        attr_defs: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        match Self::drop_index_artifacts_batch(&self.data_pool, table_id, indexes, attr_defs).await
        {
            Ok(()) => Ok(()),
            Err(err) if is_table_not_found_tidb_storage_error(&err) => {
                if self.table_is_deleting_or_absent(table_id).await? {
                    Ok(())
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn reconcile_control_plane_plan(
        &self,
        plan: ControlPlaneReconcilePlan,
    ) -> Result<Option<(String, &'static str)>, StorageError> {
        match plan {
            ControlPlaneReconcilePlan::Create { table_id } => self
                .reconcile_table_create(&table_id)
                .await
                .map(|table_name| table_name.map(|name| (name, "CREATING → active"))),
            ControlPlaneReconcilePlan::Update { table_id } => self
                .reconcile_table_update(&table_id)
                .await
                .map(|table_name| table_name.map(|name| (name, "UPDATING → active"))),
            ControlPlaneReconcilePlan::Delete(plan) => {
                self.drop_table_data_artifacts(&plan.table_id).await?;

                let mut finalize = self
                    .pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let result = sqlx::query(
                    "DELETE FROM tables \
                     WHERE table_id = ? AND table_status = 'DELETING'",
                )
                .bind(&plan.table_id)
                .execute(&mut *finalize)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                let deleted = result.rows_affected() > 0;
                if deleted {
                    sqlx::query("DELETE FROM tags WHERE resource_arn = ?")
                        .bind(&plan.table_arn)
                        .execute(&mut *finalize)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                }

                finalize
                    .commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                Ok(deleted.then_some((plan.table_name, "DELETING → deleted")))
            }
        }
    }

    /// Process pending control plane transitions.
    ///
    /// TiDB owns distributed online DDL ordering and backfill. ExtendDB keeps
    /// the catalog as durable desired state and lets every frontend replay the
    /// same idempotent transition work. Concurrent workers may race, but they
    /// converge through TiDB `IF [NOT] EXISTS` DDL and conditional catalog
    /// publication instead of an ExtendDB-specific ownership lease.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the database is unreachable or a query fails.
    pub async fn process_control_plane_transitions(
        &self,
    ) -> Result<Vec<(String, &'static str)>, StorageError> {
        let candidates: Vec<ControlPlaneTransitionRow> = sqlx::query_as(
            r"SELECT table_status, table_name, table_id, table_arn
               FROM tables
              WHERE (table_status = 'CREATING'
                     AND status_transition_at <= CURRENT_TIMESTAMP(6))
                 OR (table_status = 'UPDATING'
                     AND (status_transition_at IS NULL
                          OR status_transition_at <= CURRENT_TIMESTAMP(6)))
                 OR (table_status = 'DELETING'
                     AND status_transition_at <= CURRENT_TIMESTAMP(6))
              ORDER BY status_transition_at, table_name
              LIMIT ?",
        )
        .bind(CONTROL_PLANE_TRANSITION_SCAN_LIMIT)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let plans = candidates
            .into_iter()
            .map(ControlPlaneReconcilePlan::from_row)
            .collect::<Result<Vec<_>, _>>()?;

        let results = stream::iter(plans)
            .map(|plan| async move { self.reconcile_control_plane_plan(plan).await })
            .buffer_unordered(CONTROL_PLANE_TRANSITION_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut transitions = Vec::new();
        for result in results {
            if let Some(transition) = result? {
                transitions.push(transition);
            }
        }
        Ok(transitions)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ControlPlaneReconcilePlan, ControlPlaneTransitionRow, delete_pending_indexes_sql,
        mark_creating_indexes_active_sql,
    };

    fn row(status: &str) -> ControlPlaneTransitionRow {
        ControlPlaneTransitionRow {
            table_status: status.to_owned(),
            table_name: "orders".to_owned(),
            table_id: "table-1".to_owned(),
            table_arn: "arn:aws:dynamodb:us-east-1:000000000000:table/orders".to_owned(),
        }
    }

    #[test]
    fn transition_rows_map_to_replayable_reconcile_plans() {
        assert!(matches!(
            ControlPlaneReconcilePlan::from_row(row("CREATING")).expect("create"),
            ControlPlaneReconcilePlan::Create { .. }
        ));
        assert!(matches!(
            ControlPlaneReconcilePlan::from_row(row("UPDATING")).expect("update"),
            ControlPlaneReconcilePlan::Update { .. }
        ));
        assert!(matches!(
            ControlPlaneReconcilePlan::from_row(row("DELETING")).expect("delete"),
            ControlPlaneReconcilePlan::Delete(_)
        ));
    }

    #[test]
    fn transition_rows_reject_unknown_status() {
        let Err(error) = ControlPlaneReconcilePlan::from_row(row("ARCHIVING")) else {
            panic!("unknown status should be rejected");
        };
        assert!(error.to_string().contains("unknown TiDB control-plane"));
    }

    #[test]
    fn pending_index_publication_uses_one_set_based_catalog_statement() {
        let sql = mark_creating_indexes_active_sql(2);

        assert!(sql.contains("SET index_status = 'ACTIVE'"));
        assert!(sql.contains("index_status = 'CREATING'"));
        assert!(sql.contains("index_id IN (?, ?)"));
        assert!(sql.contains("tables.table_status = 'UPDATING'"));
    }

    #[test]
    fn pending_index_delete_uses_one_set_based_catalog_statement() {
        let sql = delete_pending_indexes_sql(3);

        assert!(sql.starts_with("DELETE FROM indexes"));
        assert!(sql.contains("index_status = 'DELETING'"));
        assert!(sql.contains("index_id IN (?, ?, ?)"));
        assert!(sql.contains("tables.table_status = 'UPDATING'"));
    }
}
