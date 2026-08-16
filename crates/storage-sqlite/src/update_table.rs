// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_table` implementation for `SqliteEngine` (REQ-CTRL-003).
//!
//! Mirrors the PostgreSQL backend: billing mode, provisioned throughput,
//! deletion protection, stream specification, table class, on-demand
//! throughput, and GSI create/delete. Single-pool; the engine write lock
//! replaces `FOR UPDATE`. GSI data tables are created (and backfilled) or
//! dropped after the catalog transaction commits, with catalog cleanup on
//! a data-DDL failure.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, TableDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::merge_attribute_definitions;

use crate::store::SqliteEngine;

impl SqliteEngine {
    pub(crate) async fn update_table_impl(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row: Option<(String, String, String, String)> = sqlx::query_as(
            "SELECT table_status, table_id, key_schema, attribute_definitions \
             FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (status, table_id, ks_json, ad_json) =
            row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(input.table_name.clone()));
        }

        // Reject ProvisionedThroughput when the effective billing mode is
        // PAY_PER_REQUEST. The effective mode is the requested billing_mode when
        // the request changes it, otherwise the table's current mode. Real
        // DynamoDB returns "Neither ReadCapacityUnits nor WriteCapacityUnits can
        // be specified when BillingMode is PAY_PER_REQUEST". Checked here, under
        // the write transaction, because it depends on the table's current
        // billing mode. Mirrors the PostgreSQL backend.
        if input.provisioned_throughput.is_some() {
            let effective_ppr = match input.billing_mode {
                Some(BillingMode::PayPerRequest) => true,
                Some(BillingMode::Provisioned) => false,
                None => {
                    let current_bm: Option<Option<String>> = sqlx::query_scalar(
                        "SELECT billing_mode FROM tables WHERE account_id = ? AND table_name = ?",
                    )
                    .bind(account_id)
                    .bind(&input.table_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    current_bm.flatten().as_deref() == Some("PAY_PER_REQUEST")
                }
            };
            if effective_ppr {
                return Err(StorageError::Validation(
                    "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST".to_owned(),
                ));
            }
        }

        // No-op rejection: same PROVISIONED throughput.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned))
            && let Some(ref pt) = input.provisioned_throughput
        {
            let current: Option<(Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT billing_mode, provisioned_throughput FROM tables \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(account_id)
            .bind(&input.table_name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if let Some((bm, cur_pt)) = current {
                let is_prov = bm.as_deref() == Some("PROVISIONED") || bm.is_none();
                let cur: serde_json::Value = cur_pt
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                let cur_rcu = cur
                    .get("ReadCapacityUnits")
                    .or_else(|| cur.get("read_capacity_units"))
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                let cur_wcu = cur
                    .get("WriteCapacityUnits")
                    .or_else(|| cur.get("write_capacity_units"))
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                if is_prov
                    && cur_rcu == pt.read_capacity_units
                    && cur_wcu == pt.write_capacity_units
                {
                    return Err(StorageError::NoOpUpdate(format!(
                        "The provisioned throughput for the table will not change. \
                         The requested value equals the current value. \
                         Current ReadCapacityUnits provisioned for the table: {cur_rcu}. \
                         Requested ReadCapacityUnits: {}. \
                         Current WriteCapacityUnits provisioned for the table: {cur_wcu}. \
                         Requested WriteCapacityUnits: {}.",
                        pt.read_capacity_units, pt.write_capacity_units
                    )));
                }
            }
        }

        if let Some(bm) = &input.billing_mode {
            let s = match bm {
                BillingMode::Provisioned => "PROVISIONED",
                BillingMode::PayPerRequest => "PAY_PER_REQUEST",
            };
            update_col(&mut tx, account_id, &input.table_name, "billing_mode", s).await?;
        }
        if let Some(pt) = &input.provisioned_throughput {
            let j = serde_json::to_string(pt).map_err(|e| StorageError::Internal(e.to_string()))?;
            update_col(
                &mut tx,
                account_id,
                &input.table_name,
                "provisioned_throughput",
                &j,
            )
            .await?;
        }
        if let Some(dp) = input.deletion_protection_enabled {
            sqlx::query(
                "UPDATE tables SET deletion_protection_enabled = ? \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(dp)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        if let Some(tc) = &input.table_class {
            update_col(&mut tx, account_id, &input.table_name, "table_class", tc).await?;
        }
        if let Some(odt) = &input.on_demand_throughput {
            let j =
                serde_json::to_string(odt).map_err(|e| StorageError::Internal(e.to_string()))?;
            update_col(
                &mut tx,
                account_id,
                &input.table_name,
                "on_demand_throughput",
                &j,
            )
            .await?;
        }
        if let Some(spec) = &input.stream_specification {
            let j =
                serde_json::to_string(spec).map_err(|e| StorageError::Internal(e.to_string()))?;
            update_col(
                &mut tx,
                account_id,
                &input.table_name,
                "stream_specification",
                &j,
            )
            .await?;
            if spec.stream_enabled {
                let existing: Option<(String,)> =
                    sqlx::query_as("SELECT shard_id FROM stream_shards WHERE table_id = ? LIMIT 1")
                        .bind(&table_id)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                if existing.is_none() {
                    Self::init_stream_shards(&mut tx, account_id, &input.table_name, &table_id)
                        .await?;
                } else {
                    let label: Option<String> = sqlx::query_scalar(
                        "SELECT stream_label FROM tables WHERE account_id = ? AND table_name = ?",
                    )
                    .bind(account_id)
                    .bind(&input.table_name)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    if label.is_none() {
                        sqlx::query(
                            "UPDATE tables SET stream_label = strftime('%Y-%m-%dT%H:%M:%S','now') \
                             WHERE account_id = ? AND table_name = ?",
                        )
                        .bind(account_id)
                        .bind(&input.table_name)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    }
                }
            }
        }

        // GSI create/delete.
        let mut created: Vec<String> = Vec::new();
        let mut deleted: Vec<String> = Vec::new();
        // The merged attribute definitions persisted by this UpdateTable, carried
        // out of the catalog transaction so the post-commit index DDL builds its
        // columns from the same set the catalog now holds.
        let mut merged_attr_defs_for_ddl: Option<Vec<AttributeDefinition>> = None;
        if let Some(updates) = &input.global_secondary_index_updates {
            for update in updates {
                if let Some(create) = &update.create {
                    let dup: Option<(String,)> = sqlx::query_as(
                        "SELECT index_name FROM indexes WHERE table_id = ? AND index_name = ?",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    if dup.is_some() {
                        return Err(StorageError::IndexAlreadyExists(create.index_name.clone()));
                    }
                    let ks = serde_json::to_string(&create.key_schema)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let proj = serde_json::to_string(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let pt = create
                        .provisioned_throughput
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let index_id = uuid::Uuid::new_v4().to_string();
                    sqlx::query(
                        "INSERT INTO indexes \
                         (table_id, index_name, index_id, index_type, key_schema, projection, \
                          index_status, provisioned_throughput) \
                         VALUES (?, ?, ?, 'GSI', ?, ?, 'CREATING', ?)",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .bind(&index_id)
                    .bind(&ks)
                    .bind(&proj)
                    .bind(&pt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    created.push(index_id);
                }
                if let Some(delete) = &update.delete {
                    let existing: Option<(String,)> = sqlx::query_as(
                        "SELECT index_id FROM indexes WHERE table_id = ? AND index_name = ?",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let (del_id,) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;
                    sqlx::query("DELETE FROM indexes WHERE table_id = ? AND index_name = ?")
                        .bind(&table_id)
                        .bind(&delete.index_name)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    deleted.push(del_id);
                }
            }
            // Merge the request's attribute definitions into the stored set.
            //
            // The request carries only the attributes it needs (a created index's
            // key attributes), so replacing the column would drop the base table's
            // own pk/sk definitions and silently degrade keyed reads to a
            // partition-only lookup (issue #259). The existing set was read inside
            // this BEGIN IMMEDIATE transaction, so the read-modify-write is atomic.
            if let Some(new_attr_defs) = &input.attribute_definitions {
                let existing: Vec<AttributeDefinition> = serde_json::from_str(&ad_json)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let merged = merge_attribute_definitions(&existing, new_attr_defs);
                let j = serde_json::to_string(&merged)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                update_col(
                    &mut tx,
                    account_id,
                    &input.table_name,
                    "attribute_definitions",
                    &j,
                )
                .await?;
                merged_attr_defs_for_ddl = Some(merged);
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Data DDL after catalog commit.
        if let Some(updates) = &input.global_secondary_index_updates {
            let base_ks: Vec<KeySchemaElement> = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let base_ad: Vec<AttributeDefinition> = serde_json::from_str(&ad_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            // Build index columns from the merged set, not the request's subset: a
            // new index may key on an attribute the base table already defined, and
            // the request is not required to re-declare it.
            let effective_ad = merged_attr_defs_for_ddl.as_deref().unwrap_or(&base_ad);

            let mut ci = 0usize;
            let mut di = 0usize;
            for update in updates {
                if let Some(create) = &update.create {
                    let idx_id = created[ci].clone();
                    ci += 1;
                    let result = async {
                        let mut data_tx = self
                            .pool
                            .begin_with("BEGIN IMMEDIATE")
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        Self::create_index_data_table(
                            &mut data_tx,
                            &idx_id,
                            &create.key_schema,
                            effective_ad,
                            &base_ks,
                            &base_ad,
                        )
                        .await?;
                        Self::backfill_gsi(
                            &mut data_tx,
                            &table_id,
                            &idx_id,
                            &create.key_schema,
                            effective_ad,
                            &base_ks,
                            &base_ad,
                            &create.projection,
                        )
                        .await?;
                        data_tx
                            .commit()
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        Ok::<(), StorageError>(())
                    }
                    .await;
                    if let Err(e) = result {
                        tracing::error!(
                            "Failed to build GSI '{}' on '{}', cleaning up catalog: {e}",
                            create.index_name,
                            input.table_name
                        );
                        let _ = sqlx::query(
                            "DELETE FROM indexes WHERE table_id = ? AND index_name = ?",
                        )
                        .bind(&table_id)
                        .bind(&create.index_name)
                        .execute(&self.pool)
                        .await;
                        return Err(e);
                    }
                    // Backfill succeeded and the data table is populated, so the
                    // index is now queryable: flip CREATING -> ACTIVE. DescribeTable
                    // reports CREATING until this point (matching DynamoDB), so a
                    // Query never hits a not-yet-populated index, and a crash before
                    // here leaves a CREATING row the startup reconciler rebuilds.
                    sqlx::query(
                        "UPDATE indexes SET index_status = 'ACTIVE' \
                         WHERE table_id = ? AND index_id = ?",
                    )
                    .bind(&table_id)
                    .bind(&idx_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
                if update.delete.is_some() {
                    let idx_id = deleted[di].clone();
                    di += 1;
                    let mut data_tx = self
                        .pool
                        .begin_with("BEGIN IMMEDIATE")
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    Self::drop_index_data_table(&mut data_tx, &idx_id).await?;
                    data_tx
                        .commit()
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
            }
        }

        self.build_table_description(account_id, &input.table_name)
            .await
    }

    /// Rebuild any GSI left in `CREATING` by a crash between the catalog commit
    /// and the completion of its data-table backfill. Runs once at startup: for
    /// each such index it drops any partial data table, recreates and backfills
    /// it, then flips the catalog row to `ACTIVE`. This closes the non-atomic
    /// create+backfill gap (an `ACTIVE` index with no/partial data table can
    /// never be observed, and nothing is left permanently stuck in `CREATING`).
    pub(crate) async fn reconcile_incomplete_gsis(&self) -> Result<usize, StorageError> {
        let rows: Vec<(String, String, String, String, String, String)> = sqlx::query_as(
            "SELECT i.index_id, i.table_id, i.key_schema, i.projection, \
                    t.key_schema, t.attribute_definitions \
             FROM indexes i JOIN tables t ON i.table_id = t.table_id \
             WHERE i.index_status = 'CREATING' AND i.index_type = 'GSI'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut rebuilt = 0usize;
        for (index_id, table_id, idx_ks_json, proj_json, base_ks_json, base_ad_json) in rows {
            let index_key_schema: Vec<KeySchemaElement> = serde_json::from_str(&idx_ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let projection: extenddb_core::types::Projection = serde_json::from_str(&proj_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let base_key_schema: Vec<KeySchemaElement> = serde_json::from_str(&base_ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&base_ad_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let _writer = self.write_lock.lock().await;
            let mut data_tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Self::drop_index_data_table(&mut data_tx, &index_id).await?;
            Self::create_index_data_table(
                &mut data_tx,
                &index_id,
                &index_key_schema,
                &attr_defs,
                &base_key_schema,
                &attr_defs,
            )
            .await?;
            Self::backfill_gsi(
                &mut data_tx,
                &table_id,
                &index_id,
                &index_key_schema,
                &attr_defs,
                &base_key_schema,
                &attr_defs,
                &projection,
            )
            .await?;
            data_tx
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "UPDATE indexes SET index_status = 'ACTIVE' \
                 WHERE table_id = ? AND index_id = ?",
            )
            .bind(&table_id)
            .bind(&index_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            rebuilt += 1;
            tracing::info!("Reconciled incomplete GSI {index_id} on table {table_id}");
        }
        Ok(rebuilt)
    }
}

/// Update a single string column on the `tables` row.
async fn update_col(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    account_id: &str,
    table_name: &str,
    column: &str,
    value: &str,
) -> Result<(), StorageError> {
    // `column` is a compile-time constant from this module, never user input.
    let sql = format!("UPDATE tables SET {column} = ? WHERE account_id = ? AND table_name = ?");
    sqlx::query(&sql)
        .bind(value)
        .bind(account_id)
        .bind(table_name)
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod reconciler_tests {
    use crate::SqliteEngine;
    use serde_json::json;

    /// A GSI left in `CREATING` by a crash (catalog row committed, but its data
    /// table never finished backfilling) must be rebuilt on startup: the
    /// reconciler recreates + backfills the data table and flips the row to
    /// `ACTIVE`. A second run is a no-op (idempotent). The `ACTIVE` flip only
    /// happens after create+backfill both succeed, so asserting `ACTIVE` proves
    /// the full rebuild ran.
    #[tokio::test]
    async fn reconcile_rebuilds_creating_gsi_to_active() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");

        let account = "000000000000";
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(account)
            .execute(&engine.pool)
            .await
            .expect("account");

        // Create the base table via the real path (this also creates its
        // `_ddb_<table_id>` data table that backfill will scan).
        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsipk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl(account, input)
            .await
            .expect("create table");

        let (table_id,): (String,) =
            sqlx::query_as("SELECT table_id FROM tables WHERE account_id = ? AND table_name = 't'")
                .bind(account)
                .fetch_one(&engine.pool)
                .await
                .expect("table_id");

        // Simulate a crash mid-build: a CREATING GSI catalog row whose data table
        // was never created.
        let ks = json!([{"AttributeName": "gsipk", "KeyType": "HASH"}]).to_string();
        let proj = json!({"ProjectionType": "ALL"}).to_string();
        sqlx::query(
            "INSERT INTO indexes \
             (table_id, index_id, index_name, index_type, key_schema, projection, index_status) \
             VALUES (?, 'idx-1', 'gsi1', 'GSI', ?, ?, 'CREATING')",
        )
        .bind(&table_id)
        .bind(&ks)
        .bind(&proj)
        .execute(&engine.pool)
        .await
        .expect("insert CREATING index");

        // Reconcile rebuilds it.
        let rebuilt = engine.reconcile_incomplete_gsis().await.expect("reconcile");
        assert_eq!(rebuilt, 1, "one CREATING GSI should be rebuilt");

        let (status,): (String,) = sqlx::query_as(
            "SELECT index_status FROM indexes WHERE table_id = ? AND index_id = 'idx-1'",
        )
        .bind(&table_id)
        .fetch_one(&engine.pool)
        .await
        .expect("status");
        assert_eq!(status, "ACTIVE", "reconciled GSI must be flipped to ACTIVE");

        // Idempotent: nothing left in CREATING, so a second run rebuilds nothing.
        assert_eq!(
            engine
                .reconcile_incomplete_gsis()
                .await
                .expect("second reconcile"),
            0,
            "reconciler must be idempotent"
        );
    }
}
