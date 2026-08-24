// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_table` implementation for `PostgresEngine`.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, TableDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::effective_attribute_definitions;

use crate::PostgresEngine;

impl PostgresEngine {
    /// Core implementation of `update_table` (REQ-CTRL-003).
    pub(crate) async fn update_table_impl(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Lock the row and fetch table_id, key_schema, attribute_definitions.
        let row: Option<(String, String, serde_json::Value, serde_json::Value)> = sqlx::query_as(
            "SELECT table_status, table_id, key_schema, attribute_definitions FROM tables WHERE account_id = $1 AND table_name = $2 FOR UPDATE",
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

        // A table holding vector indexes cannot leave PAY_PER_REQUEST. Same
        // message as CreateTable's rejection, and the same rule seen from the
        // stored state rather than from the request, so it is checked here under
        // the row lock where the current index set is readable.
        //
        // This is one of the two directions the rule has. SQLite also refuses
        // adding a vector index when the request's net billing mode is not
        // PAY_PER_REQUEST; that guard belongs with the create path, which this
        // backend refuses outright for now, so porting it here would be an
        // unreachable second refusal with different wording. It lands with the
        // create path.
        //
        // Both backends count the stored rows before this request's deletes are
        // applied, so switching to PROVISIONED and deleting the last vector index
        // in one call is refused. The service evaluates the net effect of a
        // request rather than its starting state, so it would probably accept
        // that combination. Same answer on both backends, so it is a shared
        // conformance question rather than a difference between them.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned)) {
            let vector_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM vector_indexes WHERE table_id = $1")
                    .bind(&table_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            if vector_count > 0 {
                return Err(StorageError::Validation(
                    extenddb_core::types::VECTOR_INDEX_REQUIRES_PAY_PER_REQUEST.to_owned(),
                ));
            }
        }

        // Reject ProvisionedThroughput when the effective billing mode is
        // PAY_PER_REQUEST. The effective mode is the requested billing_mode when
        // the request changes it, otherwise the table's current mode. Real
        // DynamoDB returns "Neither ReadCapacityUnits nor WriteCapacityUnits can
        // be specified when BillingMode is PAY_PER_REQUEST". This is checked
        // here, under the FOR UPDATE row lock, because it depends on the table's
        // current billing mode (a stateless engine-layer check would race with a
        // concurrent billing-mode change). The engine layer already rejects the
        // explicit `BillingMode=PAY_PER_REQUEST + ProvisionedThroughput` combo;
        // this additionally covers the omitted-BillingMode case on a table that
        // is already PAY_PER_REQUEST.
        if input.provisioned_throughput.is_some() {
            let effective_ppr = match input.billing_mode {
                Some(BillingMode::PayPerRequest) => true,
                Some(BillingMode::Provisioned) => false,
                None => {
                    let current_bm: Option<Option<String>> = sqlx::query_scalar(
                        "SELECT billing_mode FROM tables WHERE account_id = $1 AND table_name = $2",
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

        // No-op rejection: setting same billing mode to PROVISIONED with same
        // throughput values is rejected by DynamoDB. This check runs under the
        // FOR UPDATE lock to eliminate the TOCTOU race that existed when the
        // check was in the engine layer.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned))
            && let Some(ref pt) = input.provisioned_throughput
        {
            let current_row: Option<(Option<String>, Option<serde_json::Value>)> = sqlx::query_as(
                "SELECT billing_mode, provisioned_throughput FROM tables \
                     WHERE account_id = $1 AND table_name = $2",
            )
            .bind(account_id)
            .bind(&input.table_name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if let Some((current_bm, current_pt_opt)) = current_row {
                let current_pt =
                    current_pt_opt.unwrap_or(serde_json::Value::Object(serde_json::Map::default()));
                let is_provisioned =
                    current_bm.as_deref() == Some("PROVISIONED") || current_bm.is_none();
                let current_rcu = current_pt
                    .get("ReadCapacityUnits")
                    .or_else(|| current_pt.get("read_capacity_units"))
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                let current_wcu = current_pt
                    .get("WriteCapacityUnits")
                    .or_else(|| current_pt.get("write_capacity_units"))
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);

                if is_provisioned
                    && current_rcu == pt.read_capacity_units
                    && current_wcu == pt.write_capacity_units
                {
                    return Err(StorageError::NoOpUpdate(format!(
                        "The provisioned throughput for the table will not change. \
                             The requested value equals the current value. \
                             Current ReadCapacityUnits provisioned for the table: {}. \
                             Requested ReadCapacityUnits: {}. \
                             Current WriteCapacityUnits provisioned for the table: {}. \
                             Requested WriteCapacityUnits: {}.",
                        current_rcu, pt.read_capacity_units, current_wcu, pt.write_capacity_units
                    )));
                }
            }
        }

        // Apply billing mode change.
        if let Some(bm) = &input.billing_mode {
            let bm_str = match bm {
                BillingMode::Provisioned => "PROVISIONED",
                BillingMode::PayPerRequest => "PAY_PER_REQUEST",
            };
            sqlx::query(
                "UPDATE tables SET billing_mode = $1 WHERE account_id = $2 AND table_name = $3",
            )
            .bind(bm_str)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply provisioned throughput change.
        if let Some(pt) = &input.provisioned_throughput {
            let pt_json =
                serde_json::to_value(pt).map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query("UPDATE tables SET provisioned_throughput = $1 WHERE account_id = $2 AND table_name = $3")
                .bind(&pt_json)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply deletion protection change.
        if let Some(dp) = input.deletion_protection_enabled {
            sqlx::query("UPDATE tables SET deletion_protection_enabled = $1 WHERE account_id = $2 AND table_name = $3")
                .bind(dp)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply stream specification change (enable/disable streams).
        if let Some(spec) = &input.stream_specification {
            let spec_json =
                serde_json::to_value(spec).map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "UPDATE tables SET stream_specification = $1 \
                 WHERE account_id = $2 AND table_name = $3",
            )
            .bind(&spec_json)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if spec.stream_enabled {
                // Check if shards already exist (re-enabling streams on a table
                // that previously had them). Query the data pool since
                // stream_shards lives in the data database.
                let existing: Option<(String,)> = sqlx::query_as(
                    "SELECT shard_id FROM stream_shards \
                     WHERE table_id = $1 \
                     LIMIT 1",
                )
                .bind(&table_id)
                .fetch_optional(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                if existing.is_none() {
                    Self::init_stream_shards(
                        &mut tx,
                        &self.data_pool,
                        account_id,
                        &input.table_name,
                        &table_id,
                    )
                    .await?;
                } else {
                    // Shards exist but stream_label may be NULL if streams were
                    // previously disabled and the disable path cleared the label.
                    // This is a defensive check — init_stream_shards sets the
                    // label on first enable, but re-enable after disable needs
                    // to restore it.
                    let current_label: Option<String> = sqlx::query_scalar(
                        "SELECT stream_label FROM tables \
                         WHERE account_id = $1 AND table_name = $2",
                    )
                    .bind(account_id)
                    .bind(&input.table_name)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    if current_label.is_none() {
                        sqlx::query(
                            "UPDATE tables SET stream_label = \
                             to_char(NOW(), 'YYYY-MM-DD\"T\"HH24:MI:SS') \
                             WHERE account_id = $1 AND table_name = $2",
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

        // Apply table class change.
        if let Some(tc) = &input.table_class {
            sqlx::query(
                "UPDATE tables SET table_class = $1 WHERE account_id = $2 AND table_name = $3",
            )
            .bind(tc)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply on-demand throughput change.
        if let Some(odt) = &input.on_demand_throughput {
            let odt_json =
                serde_json::to_value(odt).map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "UPDATE tables SET on_demand_throughput = $1 \
                 WHERE account_id = $2 AND table_name = $3",
            )
            .bind(&odt_json)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply GSI updates (create/delete).
        let mut created_index_ids: Vec<String> = Vec::new();
        let mut deleted_index_ids: Vec<String> = Vec::new();
        // The merged and pruned attribute definitions persisted by this
        // UpdateTable, carried out of the catalog transaction so the post-commit
        // index DDL builds its columns from the same set the catalog now holds.
        let mut merged_attr_defs_for_ddl: Option<Vec<AttributeDefinition>> = None;
        if let Some(updates) = &input.global_secondary_index_updates {
            for update in updates {
                if let Some(create) = &update.create {
                    // Check for duplicate index name.
                    let existing: Option<(String,)> = sqlx::query_as(
                        "SELECT index_name FROM indexes WHERE table_id = $1 AND index_name = $2",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    if existing.is_some() {
                        return Err(StorageError::IndexAlreadyExists(create.index_name.clone()));
                    }

                    let gsi_ks = serde_json::to_value(&create.key_schema)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_proj = serde_json::to_value(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_pt = create
                        .provisioned_throughput
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let index_id = uuid::Uuid::new_v4().to_string();
                    sqlx::query(
                        r"INSERT INTO indexes
                           (table_id, index_name, index_id, index_type, key_schema, projection,
                            index_status, provisioned_throughput)
                           VALUES ($1, $2, $3, 'GSI', $4, $5, 'ACTIVE', $6)",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .bind(&index_id)
                    .bind(&gsi_ks)
                    .bind(&gsi_proj)
                    .bind(&gsi_pt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    created_index_ids.push(index_id);

                    // Create the index data table on the data pool (P54 Bug 1).
                    // Catalog metadata is committed first; data DDL follows.
                }

                if let Some(delete) = &update.delete {
                    // Verify the index exists and fetch its index_id.
                    let existing: Option<(String, String)> = sqlx::query_as(
                        "SELECT index_name, index_id FROM indexes WHERE table_id = $1 AND index_name = $2",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let (_, del_index_id) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;

                    // Delete the index metadata.
                    sqlx::query("DELETE FROM indexes WHERE table_id = $1 AND index_name = $2")
                        .bind(&table_id)
                        .bind(&delete.index_name)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    deleted_index_ids.push(del_index_id);

                    // Drop the index data table on the data pool after catalog commit.
                }
            }

            // Recompute attribute_definitions for the post-update table.
            //
            // The effective set is the stored definitions merged with the
            // request's, then pruned to the attributes still referenced by the
            // table key schema or by an index surviving this update. See
            // effective_attribute_definitions for the measured behaviour and the
            // reason merging alone is not enough (issue #259).
            //
            // This runs whether or not the request carried AttributeDefinitions,
            // because a GSI deletion prunes without the request naming anything.
            // The index rows were created and deleted above in this same
            // transaction, so `indexes` already holds exactly the surviving set;
            // the stored definitions were read under the FOR UPDATE lock, so the
            // read-modify-write is atomic against a concurrent UpdateTable.
            let stored_attr_defs: Vec<AttributeDefinition> =
                serde_json::from_value(ad_json.clone())
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            let table_key_schema: Vec<KeySchemaElement> =
                serde_json::from_value(ks_json.clone())
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

            let surviving_rows: Vec<(serde_json::Value,)> =
                sqlx::query_as("SELECT key_schema FROM indexes WHERE table_id = $1")
                    .bind(&table_id)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            let mut surviving_index_key_schemas: Vec<Vec<KeySchemaElement>> =
                Vec::with_capacity(surviving_rows.len());
            for (ks_value,) in surviving_rows {
                surviving_index_key_schemas.push(
                    serde_json::from_value(ks_value)
                        .map_err(|e| StorageError::Internal(e.to_string()))?,
                );
            }

            let effective = effective_attribute_definitions(
                &stored_attr_defs,
                input.attribute_definitions.as_deref().unwrap_or(&[]),
                &table_key_schema,
                &surviving_index_key_schemas,
            );

            if effective != stored_attr_defs {
                let effective_json = serde_json::to_value(&effective)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                sqlx::query(
                    "UPDATE tables SET attribute_definitions = $1 \
                     WHERE account_id = $2 AND table_name = $3",
                )
                .bind(&effective_json)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            merged_attr_defs_for_ddl = Some(effective);
        }

        // Vector index create/delete.
        //
        // Delete is implemented; Create is refused. Creating an index means
        // building and maintaining its data table, which this backend cannot yet
        // do, and a catalog row with no storage behind it would be an index that
        // reports ACTIVE and answers nothing. Refusing is the same fail-closed
        // posture the backend takes for every other vector operation.
        //
        // Neither branch is reachable over the wire yet: the engine refuses
        // vector index updates while this backend declares no vector search
        // capability, so these paths are exercised below the wire. They are
        // implemented now because the catalog state they act on is created here.
        if let Some(updates) = &input.vector_index_updates {
            for update in updates {
                if let Some(create) = &update.create {
                    return Err(StorageError::Unsupported(format!(
                        "vector index '{}' cannot be created: this backend does not yet \
                         build vector index storage",
                        create.index_name
                    )));
                }
                if let Some(delete) = &update.delete {
                    // The index id is deliberately not selected: this backend
                    // builds no per-index storage yet, so there is nothing to drop
                    // by id, and reading a value only to discard it invites the
                    // reader to think otherwise.
                    let existing: Option<(String, Option<bool>)> = sqlx::query_as(
                        "SELECT index_status, backfilling FROM vector_indexes \
                         WHERE table_id = $1 AND index_name = $2",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let (index_status, backfilling) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;

                    // Deleting an index that is still being created is
                    // phase-dependent, and the discriminator is the same
                    // `backfilling` flag the wire reports. While the index is
                    // allocating resources the service refuses the delete and
                    // asks the caller to retry; once the backfill is running it
                    // accepts. Measured against the service on 2026-08-19.
                    if index_status == "CREATING" && backfilling == Some(false) {
                        return Err(StorageError::ResourceInUse(
                            extenddb_core::types::vector_index_delete_in_allocation_phase(
                                &input.table_name,
                                &delete.index_name,
                            ),
                        ));
                    }

                    // Deleted synchronously: this backend has no observable
                    // DELETING window, because the catalog row and the storage go
                    // away together. The divergence from the service, which
                    // leaves the index in DELETING long enough to observe, is a
                    // documented one.
                    sqlx::query(
                        "DELETE FROM vector_indexes WHERE table_id = $1 AND index_name = $2",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // P54 Bug 1: Execute data DDL on the data pool after catalog commit.
        if let Some(updates) = &input.global_secondary_index_updates {
            let base_key_schema: Vec<KeySchemaElement> = serde_json::from_value(ks_json.clone())
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let base_attr_defs: Vec<AttributeDefinition> = serde_json::from_value(ad_json.clone())
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            // Build index columns from the merged and pruned set rather than the
            // request's subset: a new index may key on an attribute the base
            // table already defined and the request need not re-declare it, and
            // on a conflicting redeclaration the stored type is the one the data
            // table must be built from.
            let effective_attr_defs = merged_attr_defs_for_ddl
                .as_deref()
                .unwrap_or(&base_attr_defs);

            let mut create_idx = 0usize;
            let mut delete_idx = 0usize;
            for update in updates {
                if let Some(create) = &update.create {
                    let idx_id = &created_index_ids[create_idx];
                    create_idx += 1;
                    let data_result = async {
                        let mut data_tx = self
                            .data_pool
                            .begin()
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;

                        Self::create_index_data_table(
                            &mut data_tx,
                            idx_id,
                            &create.key_schema,
                            effective_attr_defs,
                            &base_key_schema,
                            &base_attr_defs,
                        )
                        .await?;

                        Self::backfill_gsi(
                            &mut data_tx,
                            &table_id,
                            idx_id,
                            &create.key_schema,
                            effective_attr_defs,
                            &base_key_schema,
                            &base_attr_defs,
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

                    if let Err(e) = data_result {
                        tracing::error!(
                            "Failed to create data table for GSI '{}' on '{}', \
                             cleaning up catalog: {e}",
                            create.index_name,
                            input.table_name,
                        );
                        let _ = sqlx::query(
                            "DELETE FROM indexes WHERE table_id = $1 AND index_name = $2",
                        )
                        .bind(&table_id)
                        .bind(&create.index_name)
                        .execute(&self.pool)
                        .await;
                        return Err(e);
                    }
                }

                if update.delete.is_some() {
                    let idx_id = &deleted_index_ids[delete_idx];
                    delete_idx += 1;
                    let idx_table = Self::index_table_name_static(idx_id);
                    if let Err(e) = sqlx::query(&format!("DROP TABLE IF EXISTS {idx_table}"))
                        .execute(&self.data_pool)
                        .await
                    {
                        tracing::warn!(
                            "Failed to drop data table for deleted GSI on '{}': {e}",
                            input.table_name,
                        );
                    }
                }
            }
        }

        self.build_table_description(account_id, &input.table_name)
            .await
    }
}
