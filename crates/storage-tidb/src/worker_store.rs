// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and control plane transition processing.

use futures::future::BoxFuture;

use extenddb_core::types::{AttributeDefinition, KeySchemaElement, StreamSpecification};
use extenddb_storage::error::StorageError;
use extenddb_storage::{MetadataEngine, WorkerStore};

use crate::TidbEngine;

type CreatingTableRow = (
    String,
    String,
    serde_json::Value,
    serde_json::Value,
    Option<serde_json::Value>,
    Option<String>,
);
type CreateIndexRow = (String, serde_json::Value);
type UpdatingTableRow = (
    String,
    String,
    serde_json::Value,
    serde_json::Value,
    Option<serde_json::Value>,
    Option<String>,
    Option<String>,
    bool,
);
type PendingIndexRow = (String, String, String, serde_json::Value);

const CONTROL_PLANE_LEASE_SECONDS: i64 = 60;

struct CreateReconcilePlan {
    table_name: String,
    key_schema: Vec<KeySchemaElement>,
    attr_defs: Vec<AttributeDefinition>,
    stream_enabled: bool,
    indexes: Vec<(String, Vec<KeySchemaElement>)>,
    token: String,
}

struct UpdateReconcilePlan {
    account_id: String,
    table_name: String,
    base_key_schema: Vec<KeySchemaElement>,
    base_attr_defs: Vec<AttributeDefinition>,
    stream_enabled: bool,
    ttl_attribute: Option<String>,
    ttl_index_ready: bool,
    pending_indexes: Vec<PendingIndexPlan>,
    token: String,
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
    token: String,
}

fn parse_json<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
    label: &str,
) -> Result<T, StorageError> {
    serde_json::from_value(value)
        .map_err(|e| StorageError::Internal(format!("invalid {label}: {e}")))
}

impl WorkerStore for TidbEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move {
            // Delegate to the inherent method.
            Self::process_control_plane_transitions(self).await
        })
    }
}

impl TidbEngine {
    async fn refresh_control_plane_lease(
        &self,
        table_id: &str,
        token: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE tables \
             SET control_plane_lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND) \
             WHERE table_id = ? AND control_plane_token = ?",
        )
        .bind(CONTROL_PLANE_LEASE_SECONDS)
        .bind(table_id)
        .bind(token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(StorageError::Internal(
                "lost TiDB control-plane lease".to_owned(),
            ));
        }
        Ok(())
    }

    async fn drop_table_data_artifacts(&self, table_id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM stream_shards WHERE table_id = ?")
            .bind(table_id)
            .execute(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Self::drop_data_table(&self.data_pool, table_id).await?;
        Ok(())
    }

    /// Reconcile a CREATING table by creating all TiDB data artifacts from the
    /// durable catalog row, then activating the table once its transition time
    /// has arrived.
    pub(crate) async fn reconcile_table_create(
        &self,
        table_id: &str,
        include_deferred: bool,
    ) -> Result<Option<String>, StorageError> {
        let token = uuid::Uuid::new_v4().to_string();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let query = if include_deferred {
            "SELECT account_id, table_name, key_schema, attribute_definitions, \
                    stream_specification, stream_label \
             FROM tables \
             WHERE table_id = ? AND table_status = 'CREATING' \
               AND (control_plane_lease_until IS NULL \
                    OR control_plane_lease_until <= CURRENT_TIMESTAMP(6)) \
             FOR UPDATE"
        } else {
            "SELECT account_id, table_name, key_schema, attribute_definitions, \
                    stream_specification, stream_label \
             FROM tables \
             WHERE table_id = ? AND table_status = 'CREATING' \
               AND status_transition_at IS NOT NULL \
               AND (control_plane_lease_until IS NULL \
                    OR control_plane_lease_until <= CURRENT_TIMESTAMP(6)) \
             FOR UPDATE"
        };
        let row: Option<CreatingTableRow> = sqlx::query_as(query)
            .bind(table_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((
            account_id,
            table_name,
            key_schema_json,
            attr_defs_json,
            stream_json,
            stream_label,
        )) = row
        else {
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return Ok(None);
        };

        let key_schema: Vec<KeySchemaElement> = parse_json(key_schema_json, "table key schema")?;
        let attr_defs: Vec<AttributeDefinition> =
            parse_json(attr_defs_json, "table attribute definitions")?;
        let stream_spec: Option<StreamSpecification> = stream_json
            .map(|v| parse_json(v, "stream specification"))
            .transpose()?;
        let stream_enabled = stream_spec.as_ref().is_some_and(|spec| spec.stream_enabled);

        if stream_enabled {
            Self::ensure_stream_label(&mut tx, &account_id, &table_name, stream_label).await?;
        }

        let index_rows: Vec<CreateIndexRow> =
            sqlx::query_as("SELECT index_id, key_schema FROM indexes WHERE table_id = ?")
                .bind(table_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        let indexes = index_rows
            .into_iter()
            .map(|(index_id, index_key_schema_json)| {
                parse_json(index_key_schema_json, "index key schema")
                    .map(|index_key_schema| (index_id, index_key_schema))
            })
            .collect::<Result<Vec<_>, _>>()?;

        sqlx::query(
            "UPDATE tables \
             SET control_plane_token = ?, \
                 control_plane_lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND) \
             WHERE table_id = ? AND table_status = 'CREATING'",
        )
        .bind(&token)
        .bind(CONTROL_PLANE_LEASE_SECONDS)
        .bind(table_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let plan = CreateReconcilePlan {
            table_name,
            key_schema,
            attr_defs,
            stream_enabled,
            indexes,
            token,
        };

        Self::create_data_table(&self.data_pool, table_id, &plan.key_schema, &plan.attr_defs)
            .await?;
        self.refresh_control_plane_lease(table_id, &plan.token)
            .await?;

        for (index_id, index_key_schema) in &plan.indexes {
            Self::create_index_artifacts(
                &self.data_pool,
                table_id,
                index_id,
                index_key_schema,
                &plan.attr_defs,
                &plan.key_schema,
                &plan.attr_defs,
            )
            .await?;
            self.refresh_control_plane_lease(table_id, &plan.token)
                .await?;
        }

        if plan.stream_enabled {
            Self::ensure_stream_shard_rows(&self.data_pool, table_id).await?;
            self.refresh_control_plane_lease(table_id, &plan.token)
                .await?;
        }

        let result = sqlx::query(
            "UPDATE tables \
             SET table_status = 'ACTIVE', status_transition_at = NULL, \
                 control_plane_token = NULL, control_plane_lease_until = NULL \
             WHERE table_id = ? AND table_status = 'CREATING' \
               AND control_plane_token = ? \
               AND status_transition_at <= CURRENT_TIMESTAMP(6)",
        )
        .bind(table_id)
        .bind(&plan.token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() > 0 {
            Ok(Some(plan.table_name))
        } else {
            sqlx::query(
                "UPDATE tables \
                 SET control_plane_token = NULL, control_plane_lease_until = NULL \
                 WHERE table_id = ? AND table_status = 'CREATING' \
                   AND control_plane_token = ?",
            )
            .bind(table_id)
            .bind(&plan.token)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(None)
        }
    }

    /// Reconcile an UPDATING table. Pending GSI creates/deletes and stream
    /// shard initialization are retried from catalog metadata until complete.
    async fn reconcile_table_update(&self, table_id: &str) -> Result<Option<String>, StorageError> {
        let token = uuid::Uuid::new_v4().to_string();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row: Option<UpdatingTableRow> = sqlx::query_as(
            "SELECT account_id, table_name, key_schema, attribute_definitions, \
                    stream_specification, stream_label, ttl_attribute, ttl_index_ready \
             FROM tables \
             WHERE table_id = ? AND table_status = 'UPDATING' \
               AND (control_plane_lease_until IS NULL \
                    OR control_plane_lease_until <= CURRENT_TIMESTAMP(6)) \
             FOR UPDATE",
        )
        .bind(table_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((
            account_id,
            table_name,
            key_schema_json,
            attr_defs_json,
            stream_json,
            stream_label,
            ttl_attribute,
            ttl_index_ready,
        )) = row
        else {
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return Ok(None);
        };

        let base_key_schema: Vec<KeySchemaElement> =
            parse_json(key_schema_json, "table key schema")?;
        let base_attr_defs: Vec<AttributeDefinition> =
            parse_json(attr_defs_json, "table attribute definitions")?;
        let stream_spec: Option<StreamSpecification> = stream_json
            .map(|v| parse_json(v, "stream specification"))
            .transpose()?;
        let stream_enabled = stream_spec.as_ref().is_some_and(|spec| spec.stream_enabled);

        if stream_enabled {
            Self::ensure_stream_label(&mut tx, &account_id, &table_name, stream_label).await?;
        }

        let pending_indexes: Vec<PendingIndexRow> = sqlx::query_as(
            "SELECT index_id, index_name, index_status, key_schema \
             FROM indexes \
             WHERE table_id = ? AND index_type = 'GSI' \
               AND index_status IN ('CREATING', 'DELETING') \
             ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&mut *tx)
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

        sqlx::query(
            "UPDATE tables \
             SET control_plane_token = ?, \
                 control_plane_lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND) \
             WHERE table_id = ? AND table_status = 'UPDATING'",
        )
        .bind(&token)
        .bind(CONTROL_PLANE_LEASE_SECONDS)
        .bind(table_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let plan = UpdateReconcilePlan {
            account_id,
            table_name,
            base_key_schema,
            base_attr_defs,
            stream_enabled,
            ttl_attribute,
            ttl_index_ready,
            pending_indexes,
            token,
        };

        if plan.stream_enabled {
            Self::ensure_stream_shard_rows(&self.data_pool, table_id).await?;
            self.refresh_control_plane_lease(table_id, &plan.token)
                .await?;
        }

        if let Some(ttl_attribute) = &plan.ttl_attribute
            && !plan.ttl_index_ready
        {
            MetadataEngine::create_ttl_index(
                self,
                &plan.account_id,
                &plan.table_name,
                ttl_attribute,
            )
            .await?;
            self.refresh_control_plane_lease(table_id, &plan.token)
                .await?;
        }

        for pending in &plan.pending_indexes {
            match pending.index_status.as_str() {
                "CREATING" => {
                    Self::create_index_artifacts(
                        &self.data_pool,
                        table_id,
                        &pending.index_id,
                        &pending.key_schema,
                        &plan.base_attr_defs,
                        &plan.base_key_schema,
                        &plan.base_attr_defs,
                    )
                    .await?;
                    self.refresh_control_plane_lease(table_id, &plan.token)
                        .await?;

                    sqlx::query(
                        "UPDATE indexes SET index_status = 'ACTIVE' \
                         WHERE table_id = ? AND index_id = ? AND index_status = 'CREATING'",
                    )
                    .bind(table_id)
                    .bind(&pending.index_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
                "DELETING" => {
                    Self::drop_index_artifacts(
                        &self.data_pool,
                        table_id,
                        &pending.index_id,
                        &pending.key_schema,
                        &plan.base_attr_defs,
                    )
                    .await?;
                    self.refresh_control_plane_lease(table_id, &plan.token)
                        .await?;
                    sqlx::query(
                        "DELETE FROM indexes \
                         WHERE table_id = ? AND index_id = ? AND index_status = 'DELETING'",
                    )
                    .bind(table_id)
                    .bind(&pending.index_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
                other => {
                    return Err(StorageError::Internal(format!(
                        "unknown pending GSI status for {}: {other}",
                        pending.index_name
                    )));
                }
            }
        }

        let result = sqlx::query(
            "UPDATE tables \
             SET table_status = 'ACTIVE', status_transition_at = NULL, \
                 control_plane_token = NULL, control_plane_lease_until = NULL \
             WHERE table_id = ? AND table_status = 'UPDATING' \
               AND control_plane_token = ?",
        )
        .bind(table_id)
        .bind(&plan.token)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if result.rows_affected() > 0 {
            Ok(Some(plan.table_name))
        } else {
            Ok(None)
        }
    }

    /// Process pending control plane transitions.
    ///
    /// Tables in CREATING state whose `status_transition_at` has passed have
    /// their TiDB data artifacts created and are moved to ACTIVE. Tables in
    /// UPDATING state reconcile pending GSI/stream work before returning to
    /// ACTIVE. Tables in DELETING state whose transition time has passed are
    /// removed (along with their indexes and tags).
    ///
    /// Called by the background poller in `cmd_serve`. Also called at startup
    /// to recover in-flight operations from a previous server instance.
    ///
    /// Returns a list of `(table_name, transition)` pairs describing what
    /// changed, so the caller can log meaningful state-change messages (D-4).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the database is unreachable or a query fails.
    pub async fn process_control_plane_transitions(
        &self,
    ) -> Result<Vec<(String, &'static str)>, StorageError> {
        let mut transitions = Vec::new();

        // CREATING → ACTIVE, with data artifacts created by the reconciler.
        let pending_creates: Vec<(String, String)> = sqlx::query_as(
            r"SELECT table_name, table_id FROM tables
               WHERE table_status = 'CREATING'
                 AND status_transition_at <= CURRENT_TIMESTAMP(6)
                 AND (control_plane_lease_until IS NULL
                      OR control_plane_lease_until <= CURRENT_TIMESTAMP(6))",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        for (name, table_id) in pending_creates {
            if self
                .reconcile_table_create(&table_id, false)
                .await?
                .is_some()
            {
                transitions.push((name, "CREATING → active"));
            }
        }

        // UPDATING → ACTIVE after pending GSI and stream artifacts are reconciled.
        let pending_updates: Vec<(String, String)> = sqlx::query_as(
            r"SELECT table_name, table_id FROM tables
               WHERE table_status = 'UPDATING'
                 AND (status_transition_at IS NULL OR status_transition_at <= CURRENT_TIMESTAMP(6))
                 AND (control_plane_lease_until IS NULL
                      OR control_plane_lease_until <= CURRENT_TIMESTAMP(6))",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        for (name, table_id) in pending_updates {
            if self.reconcile_table_update(&table_id).await?.is_some() {
                transitions.push((name, "UPDATING → active"));
            }
        }

        // DELETING → remove row (with tags and data table cleanup).
        //
        // Strategy: SELECT ... FOR UPDATE SKIP LOCKED to make short durable
        // claims, commit the catalog transaction, drop TiDB data artifacts, then
        // delete catalog metadata in a second short transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let candidates: Vec<(String, String, String, String)> = sqlx::query_as(
            r"SELECT account_id, table_name, table_arn, table_id FROM tables
               WHERE table_status = 'DELETING' AND status_transition_at <= CURRENT_TIMESTAMP(6)
                 AND (control_plane_lease_until IS NULL
                      OR control_plane_lease_until <= CURRENT_TIMESTAMP(6))
               FOR UPDATE SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut drop_info = Vec::new();

        for (_acct_id, name, arn, table_id) in &candidates {
            let token = uuid::Uuid::new_v4().to_string();
            sqlx::query(
                "UPDATE tables \
                 SET control_plane_token = ?, \
                     control_plane_lease_until = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND) \
                 WHERE table_id = ? AND table_status = 'DELETING'",
            )
            .bind(&token)
            .bind(CONTROL_PLANE_LEASE_SECONDS)
            .bind(table_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            drop_info.push(DeleteReconcilePlan {
                table_name: name.clone(),
                table_arn: arn.clone(),
                table_id: table_id.clone(),
                token,
            });
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for plan in &drop_info {
            self.drop_table_data_artifacts(&plan.table_id).await?;
            self.refresh_control_plane_lease(&plan.table_id, &plan.token)
                .await?;

            let mut finalize = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let result = sqlx::query(
                "DELETE FROM tables \
                 WHERE table_id = ? AND table_status = 'DELETING' \
                   AND control_plane_token = ?",
            )
            .bind(&plan.table_id)
            .bind(&plan.token)
            .execute(&mut *finalize)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if result.rows_affected() > 0 {
                sqlx::query("DELETE FROM tags WHERE resource_arn = ?")
                    .bind(&plan.table_arn)
                    .execute(&mut *finalize)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                transitions.push((plan.table_name.clone(), "DELETING → deleted"));
            }

            finalize
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(transitions)
    }
}
