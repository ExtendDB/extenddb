// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_table` implementation for the Cassandra backend.

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::query::BatchQueryBuilder;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::value::Value;
use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, TableDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::effective_attribute_definitions;

use crate::CassandraEngine;
use crate::cassandra_util::query_optional;

impl CassandraEngine {
    pub(crate) async fn update_table_impl(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> Result<TableDescription, StorageError> {
        let catalog_ks = self.catalog_keyspace();

        // Fetch table_id, status, key_schema, attribute_definitions.
        let row = query_optional(
            &self.session,
            &format!(
                "SELECT table_id, table_status, key_schema, attribute_definitions, \
                 billing_mode, provisioned_throughput \
                 FROM {catalog_ks}.tables WHERE account_id = ? AND table_name = ?"
            ),
            cdrs_tokio::query_values!(account_id, input.table_name.as_str()),
            "update_table",
        )
        .await?
        .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        let status: String =
            crate::cassandra_util::get_column(&row, "table_status", "update_table")?;
        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(input.table_name.clone()));
        }
        let table_id: String = crate::cassandra_util::get_column(&row, "table_id", "update_table")?;
        let ks_json: String =
            crate::cassandra_util::get_column(&row, "key_schema", "update_table")?;
        let ad_json: String =
            crate::cassandra_util::get_column(&row, "attribute_definitions", "update_table")?;

        // No-op rejection: PROVISIONED billing with identical throughput values.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned))
            && let Some(ref pt) = input.provisioned_throughput
        {
            let current_bm: Option<String> = row.get_by_name("billing_mode").ok().flatten();
            let current_pt_str: Option<String> =
                row.get_by_name("provisioned_throughput").ok().flatten();
            let is_provisioned =
                current_bm.as_deref() == Some("PROVISIONED") || current_bm.is_none();
            if is_provisioned {
                let (cur_rcu, cur_wcu) = current_pt_str
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                    .map_or((0, 0), |v| {
                        let rcu = v
                            .get("ReadCapacityUnits")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0);
                        let wcu = v
                            .get("WriteCapacityUnits")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0);
                        (rcu, wcu)
                    });
                if cur_rcu == pt.read_capacity_units && cur_wcu == pt.write_capacity_units {
                    return Err(StorageError::NoOpUpdate(format!(
                        "The provisioned throughput for the table will not change. \
                         The requested value equals the current value. \
                         Current ReadCapacityUnits provisioned for the table: {}. \
                         Requested ReadCapacityUnits: {}. \
                         Current WriteCapacityUnits provisioned for the table: {}. \
                         Requested WriteCapacityUnits: {}.",
                        cur_rcu, pt.read_capacity_units, cur_wcu, pt.write_capacity_units
                    )));
                }
            }
        }

        // Build a LOGGED BATCH for all catalog column updates on `tables`.
        // All statements touch the same partition (account_id, table_name) so
        // they are atomic. Reads (no-op check, shard existence) happen before
        // this batch; DDL (CREATE/DROP TABLE for GSIs) happens after.
        let mut batch = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
        let mut batch_has_statements = false;

        macro_rules! add_update {
            ($col:expr, $val:expr) => {{
                batch = batch.add_query(
                    format!(
                        "UPDATE {catalog_ks}.tables SET {} = ? \
                         WHERE account_id = ? AND table_name = ?",
                        $col
                    ),
                    QueryValues::SimpleValues(vec![
                        Value::from($val),
                        Value::from(account_id),
                        Value::from(input.table_name.as_str()),
                    ]),
                );
                batch_has_statements = true;
            }};
        }

        if let Some(bm) = &input.billing_mode {
            let bm_str = match bm {
                BillingMode::Provisioned => "PROVISIONED",
                BillingMode::PayPerRequest => "PAY_PER_REQUEST",
            };
            add_update!("billing_mode", bm_str);
        }

        if let Some(pt) = &input.provisioned_throughput {
            let pt_json =
                serde_json::to_string(pt).map_err(|e| StorageError::Internal(e.to_string()))?;
            add_update!("provisioned_throughput", pt_json.as_str());
        }

        if let Some(dp) = input.deletion_protection_enabled {
            add_update!("deletion_protection_enabled", dp);
        }

        if let Some(tc) = &input.table_class {
            add_update!("table_class", tc.as_str());
        }

        if let Some(odt) = &input.on_demand_throughput {
            let odt_json =
                serde_json::to_string(odt).map_err(|e| StorageError::Internal(e.to_string()))?;
            add_update!("on_demand_throughput", odt_json.as_str());
        }

        // Stream specification: add to batch, then handle shard init separately
        // (init_stream_shards writes to a different keyspace and does its own batch).
        let mut needs_shard_init = false;
        let mut needs_label_restore = false;
        if let Some(spec) = &input.stream_specification {
            let spec_json =
                serde_json::to_string(spec).map_err(|e| StorageError::Internal(e.to_string()))?;
            add_update!("stream_specification", spec_json.as_str());

            if spec.stream_enabled {
                let account_ks = self.account_keyspace(account_id);
                let existing = query_optional(
                    &self.session,
                    &format!(
                        "SELECT shard_id FROM {account_ks}.stream_shards \
                         WHERE table_id = ? LIMIT 1 ALLOW FILTERING"
                    ),
                    cdrs_tokio::query_values!(table_id.as_str()),
                    "update_table stream_shards check",
                )
                .await?;

                if existing.is_none() {
                    needs_shard_init = true;
                } else {
                    // Re-enabling: check if stream_label needs restoring.
                    let label_row = query_optional(
                        &self.session,
                        &format!(
                            "SELECT stream_label FROM {catalog_ks}.tables \
                             WHERE account_id = ? AND table_name = ?"
                        ),
                        cdrs_tokio::query_values!(account_id, input.table_name.as_str()),
                        "update_table stream_label check",
                    )
                    .await?;
                    let has_label = label_row.is_some_and(|r| {
                        let v: Option<String> = r.get_by_name("stream_label").ok().flatten();
                        v.is_some()
                    });
                    if !has_label {
                        needs_label_restore = true;
                        let label = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
                        add_update!("stream_label", label.as_str());
                    }
                }
            }
        }

        // GSI updates: pre-validate, then add catalog changes to batch.
        let base_key_schema: Vec<extenddb_core::types::KeySchemaElement>;
        let base_attr_defs: Vec<extenddb_core::types::AttributeDefinition>;
        let mut gsi_creates: Vec<(String, String)> = Vec::new(); // (index_id, index_name)
        let mut gsi_deletes: Vec<String> = Vec::new(); // index_names

        if let Some(updates) = &input.global_secondary_index_updates {
            base_key_schema = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            base_attr_defs = serde_json::from_str(&ad_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            // Collect existing index key schemas so we can compute the merged
            // attribute_definitions after we know all creates/deletes.
            let mut surviving_index_key_schemas: Vec<Vec<extenddb_core::types::KeySchemaElement>> = {
                let rows = crate::cassandra_util::query_rows::<StorageError>(
                    &self.session,
                    &format!("SELECT key_schema FROM {catalog_ks}.indexes WHERE table_id = ?"),
                    cdrs_tokio::query_values!(table_id.as_str()),
                    "update_table fetch index schemas",
                )
                .await?;
                rows.into_iter()
                    .filter_map(|row| {
                        let ks_text: String =
                            crate::cassandra_util::get_column::<String, StorageError>(
                                &row,
                                "key_schema",
                                "update_table fetch index schemas",
                            )
                            .ok()?;
                        serde_json::from_str(&ks_text).ok()
                    })
                    .collect()
            };

            // effective_attr_defs for DDL (create_index_data_table) — uses the
            // merged set computed after the loop.
            let effective_attr_defs = input
                .attribute_definitions
                .as_deref()
                .unwrap_or(&base_attr_defs);

            for update in updates {
                if let Some(create) = &update.create {
                    // Reject duplicate index name.
                    let existing = query_optional(
                        &self.session,
                        &format!(
                            "SELECT index_name FROM {catalog_ks}.indexes \
                             WHERE table_id = ? AND index_name = ? ALLOW FILTERING"
                        ),
                        cdrs_tokio::query_values!(table_id.as_str(), create.index_name.as_str()),
                        "update_table gsi duplicate check",
                    )
                    .await?;
                    if existing.is_some() {
                        return Err(StorageError::IndexAlreadyExists(create.index_name.clone()));
                    }

                    let index_id = uuid::Uuid::new_v4().to_string();
                    let idx_ks_json = serde_json::to_string(&create.key_schema)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let proj_json = serde_json::to_string(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let pt_json = create
                        .provisioned_throughput
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?
                        .unwrap_or_default();

                    batch = batch.add_query(
                        format!(
                            "INSERT INTO {catalog_ks}.indexes \
                             (table_id, index_name, index_id, index_type, key_schema, \
                              projection, index_status, provisioned_throughput) \
                             VALUES (?, ?, ?, 'GSI', ?, ?, 'ACTIVE', ?)"
                        ),
                        QueryValues::SimpleValues(vec![
                            Value::from(table_id.as_str()),
                            Value::from(create.index_name.as_str()),
                            Value::from(index_id.as_str()),
                            Value::from(idx_ks_json.as_str()),
                            Value::from(proj_json.as_str()),
                            Value::from(pt_json.as_str()),
                        ]),
                    );
                    gsi_creates.push((index_id, create.index_name.clone()));
                    surviving_index_key_schemas.push(create.key_schema.clone());

                    // Create the data table after the batch commits (DDL can't be batched).
                    // Store what we need for post-batch DDL.
                    let _ = (effective_attr_defs, &base_key_schema, &base_attr_defs);
                }

                if let Some(delete) = &update.delete {
                    // Verify index exists before adding delete to batch.
                    let existing = query_optional(
                        &self.session,
                        &format!(
                            "SELECT index_id, key_schema FROM {catalog_ks}.indexes \
                             WHERE table_id = ? AND index_name = ? ALLOW FILTERING"
                        ),
                        cdrs_tokio::query_values!(table_id.as_str(), delete.index_name.as_str()),
                        "update_table gsi delete check",
                    )
                    .await?
                    .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;
                    let index_id: String = crate::cassandra_util::get_column(
                        &existing,
                        "index_id",
                        "update_table gsi delete",
                    )?;
                    // Remove this index's key schema from the surviving set.
                    if let Ok(ks_text) = crate::cassandra_util::get_column::<String, StorageError>(
                        &existing,
                        "key_schema",
                        "update_table gsi delete ks",
                    ) {
                        if let Ok(del_ks) = serde_json::from_str::<
                            Vec<extenddb_core::types::KeySchemaElement>,
                        >(&ks_text)
                        {
                            if let Some(pos) = surviving_index_key_schemas
                                .iter()
                                .position(|s| *s == del_ks)
                            {
                                surviving_index_key_schemas.remove(pos);
                            }
                        }
                    }

                    batch = batch.add_query(
                        format!(
                            "DELETE FROM {catalog_ks}.indexes \
                             WHERE table_id = ? AND index_name = ?"
                        ),
                        QueryValues::SimpleValues(vec![
                            Value::from(table_id.as_str()),
                            Value::from(delete.index_name.as_str()),
                        ]),
                    );
                    gsi_deletes.push(index_id);
                }
            }

            // Write the merged attribute_definitions now that we know all surviving
            // index key schemas (existing + created - deleted).
            let merged_attr_defs = effective_attribute_definitions(
                &base_attr_defs,
                input.attribute_definitions.as_deref().unwrap_or(&[]),
                &base_key_schema,
                &surviving_index_key_schemas,
            );
            let merged_ad_json = serde_json::to_string(&merged_attr_defs)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            add_update!("attribute_definitions", merged_ad_json.as_str());
        } else {
            base_key_schema = Vec::new();
            base_attr_defs = Vec::new();
        }

        // The table's TTL control lease fences index creation against the TTL
        // lifecycle. It is needed in two cases:
        //
        // * an asynchronously propagated GSI is being created — holding the
        //   lease stops TTL being enabled underneath it, which the TTL design
        //   does not admit; and
        // * TTL is already enabled — holding the lease keeps the expiration
        //   sweep out of the window between the catalog publishing the new
        //   index as ACTIVE and its data table actually existing. Without it a
        //   sweep can take a base-row claim and then fail applying index
        //   effects against a table that is not there yet.
        let creating_async_gsi = !gsi_creates.is_empty()
            && self
                .gsi_default_delay_ms
                .load(std::sync::atomic::Ordering::Relaxed)
                != 0;
        let ttl_enabled = self
            .ttl_config_for_table(account_id, &input.table_name)
            .await?
            .is_some();
        let ttl_control_owner = if !gsi_creates.is_empty() && (creating_async_gsi || ttl_enabled) {
            let owner = self
                .acquire_ttl_control_lease(account_id, &input.table_name)
                .await?
                .ok_or_else(|| {
                    StorageError::IndexesInUse(format!(
                        "Index changes for table {} cannot be applied while an expiration \
                         sweep is in progress. Retry the request.",
                        input.table_name
                    ))
                })?;
            if creating_async_gsi && ttl_enabled {
                self.release_ttl_sweep_lease(account_id, &input.table_name, owner)
                    .await?;
                return Err(StorageError::Validation(
                    "Cannot create an asynchronously propagated GSI while TTL is enabled"
                        .to_owned(),
                ));
            }
            Some(owner)
        } else {
            None
        };

        // Everything below runs under that lease, so it is released on every
        // path rather than only on success.
        let outcome = self
            .apply_table_update(
                account_id,
                &input,
                batch,
                batch_has_statements,
                needs_shard_init,
                needs_label_restore,
                &table_id,
                &gsi_creates,
                &gsi_deletes,
                &base_key_schema,
                &base_attr_defs,
            )
            .await;
        if let Some(owner) = ttl_control_owner
            && let Err(error) = self
                .release_ttl_sweep_lease(account_id, &input.table_name, owner)
                .await
        {
            tracing::warn!(
                table = %input.table_name,
                "TTL control lease release failed; TTL lifecycle changes are blocked \
                 until it expires: {error}"
            );
        }
        outcome
    }

    /// Apply the catalog batch and the post-batch DDL for `update_table`.
    ///
    /// Split out so the caller can hold the TTL control lease across it and
    /// release it on every path.
    #[allow(clippy::too_many_arguments)]
    async fn apply_table_update(
        &self,
        account_id: &str,
        input: &UpdateTableInput,
        batch: BatchQueryBuilder,
        batch_has_statements: bool,
        needs_shard_init: bool,
        needs_label_restore: bool,
        table_id: &str,
        gsi_creates: &[(String, String)],
        gsi_deletes: &[String],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<TableDescription, StorageError> {
        // Execute the catalog batch atomically.
        if batch_has_statements {
            self.session
                .batch(
                    batch
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))?,
                )
                .await
                .map_err(|e| {
                    tracing::error!("update_table batch: {e}");
                    StorageError::Internal("Database error".to_owned())
                })?;
        }

        // Post-batch: shard init (has its own internal batch across two keyspaces).
        if needs_shard_init {
            let account_ks = self.account_keyspace(account_id);
            self.init_stream_shards(account_id, &input.table_name, &account_ks, table_id)
                .await?;
        }
        let _ = needs_label_restore; // handled inside the batch above

        // Post-batch: GSI data table DDL (CREATE/DROP TABLE cannot be batched).
        if let Some(updates) = &input.global_secondary_index_updates {
            let effective_attr_defs = input
                .attribute_definitions
                .as_deref()
                .unwrap_or(base_attr_defs);
            let account_ks = self.account_keyspace(account_id);

            let mut create_idx = 0usize;
            let mut delete_idx = 0usize;
            for update in updates {
                if let Some(create) = &update.create {
                    let (index_id, _) = &gsi_creates[create_idx];
                    create_idx += 1;
                    // TODO: backfill existing items into the new GSI.
                    self.create_index_data_table(
                        &account_ks,
                        index_id,
                        &create.key_schema,
                        effective_attr_defs,
                        base_key_schema,
                        base_attr_defs,
                    )
                    .await?;
                }
                if update.delete.is_some() {
                    let index_id = &gsi_deletes[delete_idx];
                    delete_idx += 1;
                    self.drop_index_data_table(&account_ks, index_id).await?;
                }
            }
        }

        self.build_table_description(account_id, &input.table_name)
            .await
    }
}
