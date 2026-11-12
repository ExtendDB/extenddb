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
use extenddb_storage::util::{
    effective_attribute_definitions, warm_throughput_record_for_create, warm_throughput_record_json,
};

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
                 billing_mode, provisioned_throughput, stream_specification, stream_label \
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

        // Reject ProvisionedThroughput when the effective billing mode is PAY_PER_REQUEST.
        // Covers the case where BillingMode is omitted but the table is already PAY_PER_REQUEST.
        if input.provisioned_throughput.is_some() {
            let stored_bm: Option<String> = row.get_by_name("billing_mode").ok().flatten();
            let effective_ppr = match input.billing_mode {
                Some(BillingMode::PayPerRequest) => true,
                Some(BillingMode::Provisioned) => false,
                None => stored_bm.as_deref() == Some("PAY_PER_REQUEST"),
            };
            if effective_ppr {
                return Err(StorageError::Validation(
                    "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST".to_owned(),
                ));
            }
        }

        // Collect column updates for `tables`. These will be executed as a single
        // LWT UPDATE ... IF EXISTS to avoid mixing plain/LWT writes on a row
        // created with IF NOT EXISTS. See ADR-0021.
        let mut table_cols: Vec<(&'static str, Value)> = Vec::new();
        // Separate plain batch for `indexes` (plain INSERT/DELETE, no LWT mixing issue).
        let mut indexes_batch = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
        let mut indexes_batch_has_statements = false;

        macro_rules! add_update {
            ($col:expr, $val:expr) => {{
                table_cols.push(($col, Value::from($val)));
            }};
        }
        macro_rules! add_index_op {
            ($query:expr, $vals:expr) => {{
                indexes_batch = indexes_batch.add_query($query, $vals);
                indexes_batch_has_statements = true;
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

        // WarmThroughput on the table and on updated indexes: stored as sent,
        // nothing acts on it. Validated by the engine against the current values.
        // The table's record is written by the table LWT below, conditional on
        // the record read here; the indexes' by their own LWTs after the
        // indexes batch (`store_index_warm_throughput`). Both read again and
        // retry when another UpdateTable wrote the record first.
        let mut warm_read: Option<String> = None;
        if let Some(spec) = &input.warm_throughput {
            let merge = self
                .merged_table_warm_throughput(account_id, &input.table_name, spec)
                .await?;
            add_update!("warm_throughput", merge.merged.as_str());
            warm_read = merge.read;
        }
        for update in input.global_secondary_index_updates.iter().flatten() {
            if let Some(action) = &update.update
                && let Some(spec) = &action.warm_throughput
            {
                // Refuse a missing index before anything is written.
                self.merged_index_warm_throughput(&table_id, &action.index_name, spec)
                    .await?;
            }
        }

        // Stream specification: the catalog columns go in the tables LWT; the
        // shard changes run after it (`PendingStream`), since the shards are
        // in a different keyspace.
        let mut pending_stream = None;
        if let Some(spec) = &input.stream_specification {
            let (cols, pending) = self
                .decide_stream_change_or_close(
                    &row,
                    &self.account_keyspace(account_id),
                    &table_id,
                    &input.table_name,
                    spec,
                )
                .await?;
            for (col, val) in cols {
                table_cols.push((col, Value::from(val.as_str())));
            }
            pending_stream = Some(pending);
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

                    let mut index_values = vec![
                        Value::from(table_id.as_str()),
                        Value::from(create.index_name.as_str()),
                        Value::from(index_id.as_str()),
                        Value::from(idx_ks_json.as_str()),
                        Value::from(proj_json.as_str()),
                        Value::from(pt_json.as_str()),
                    ];
                    // WarmThroughput named only when sent, as in create_table.
                    let gsi_warm = warm_throughput_record_for_create(
                        create.warm_throughput.as_ref(),
                        self.control_plane_delay_secs().await,
                    );
                    let (warm_column, warm_placeholder) = match gsi_warm.as_ref() {
                        Some(record) => {
                            index_values
                                .push(Value::from(warm_throughput_record_json(record)?.as_str()));
                            (", warm_throughput", ", ?")
                        }
                        None => ("", ""),
                    };
                    add_index_op!(
                        format!(
                            "INSERT INTO {catalog_ks}.indexes \
                             (table_id, index_name, index_id, index_type, key_schema, \
                              projection, index_status, provisioned_throughput{warm_column}) \
                             VALUES (?, ?, ?, 'GSI', ?, ?, 'CREATING', ?{warm_placeholder})"
                        ),
                        QueryValues::SimpleValues(index_values)
                    );
                    gsi_creates.push((index_id, create.index_name.clone()));
                    surviving_index_key_schemas.push(create.key_schema.clone());

                    // Create the data table after the batch commits (DDL can't be batched).
                    // Store what we need for post-batch DDL.
                    let _ = (effective_attr_defs, &base_key_schema, &base_attr_defs);
                    // Propagation hold is taken before the catalog batch executes (see below).
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
                    ) && let Ok(del_ks) = serde_json::from_str::<
                        Vec<extenddb_core::types::KeySchemaElement>,
                    >(&ks_text)
                        && let Some(pos) = surviving_index_key_schemas
                            .iter()
                            .position(|s| *s == del_ks)
                    {
                        surviving_index_key_schemas.remove(pos);
                    }

                    add_index_op!(
                        format!(
                            "DELETE FROM {catalog_ks}.indexes \
                             WHERE table_id = ? AND index_name = ?"
                        ),
                        QueryValues::SimpleValues(vec![
                            Value::from(table_id.as_str()),
                            Value::from(delete.index_name.as_str()),
                        ])
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
        // A created GSI sets no delay of its own, so it follows the system
        // setting: read it live (only when a GSI is being created), so a
        // `settings set` made just before this request decides the fence.
        let creating_async_gsi =
            !gsi_creates.is_empty() && self.index_propagation_delay().await != 0;
        // Quorum and uncached: the control-lease decision below fences GSI
        // creation against TTL lifecycle changes, and a cached stale None
        // would skip that fence in exactly the window it exists for.
        let ttl_enabled = self
            .ttl_config_for_table_quorum(account_id, &input.table_name)
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
                table_cols,
                warm_read,
                indexes_batch,
                indexes_batch_has_statements,
                pending_stream,
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
        mut table_cols: Vec<(&'static str, Value)>,
        mut warm_read: Option<String>,
        indexes_batch: BatchQueryBuilder,
        indexes_batch_has_statements: bool,
        pending_stream: Option<crate::stream_lifecycle::PendingStream>,
        table_id: &str,
        gsi_creates: &[(String, String)],
        gsi_deletes: &[String],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
    ) -> Result<TableDescription, StorageError> {
        let account_ks = self.account_keyspace(account_id);

        // Take propagation holds for all new GSIs BEFORE the catalog batch
        // commits. This ensures no worker can apply a queued write to the index
        // between the CREATING row becoming visible and the backfill completing.
        // If the batch subsequently fails, we release the holds in the error path.
        let mut taken_holds: Vec<String> = Vec::new();
        if let Some(updates) = &input.global_secondary_index_updates {
            let mut create_idx = 0usize;
            for update in updates {
                if update.create.is_some() {
                    let (index_id, _) = &gsi_creates[create_idx];
                    create_idx += 1;
                    if let Err(e) = crate::propagation_hold::take_propagation_hold(
                        &self.session_arc(),
                        &account_ks,
                        table_id,
                        index_id,
                    )
                    .await
                    {
                        for held_id in &taken_holds {
                            let _ = crate::propagation_hold::release_propagation_hold(
                                &self.session_arc(),
                                &account_ks,
                                table_id,
                                held_id,
                            )
                            .await;
                        }
                        return Err(e);
                    }
                    taken_holds.push(index_id.clone());
                }
            }
        }

        // Execute tables update as a single LWT, conditional on the values the
        // request read (`tables_write_condition`), or IF EXISTS.
        // This avoids mixing plain/LWT writes on the tables row (created with IF NOT EXISTS).
        if !table_cols.is_empty() {
            let warm_col = table_cols
                .iter()
                .position(|(col, _)| *col == "warm_throughput");
            let mut attempts = 0;
            let applied = loop {
                let written = self
                    .write_tables_row(
                        account_id,
                        &input.table_name,
                        &table_cols,
                        table_id,
                        warm_col.map(|_| warm_read.as_deref()),
                        pending_stream
                            .as_ref()
                            .map(|p| (p.read_specification.as_deref(), p.read_label.as_deref())),
                    )
                    .await
                    .inspect_err(|_e| {
                        for held_id in &taken_holds {
                            let session = self.session_arc();
                            let account_ks = account_ks.clone();
                            let held_id = held_id.clone();
                            let table_id = table_id.to_owned();
                            tokio::spawn(async move {
                                let _ = crate::propagation_hold::release_propagation_hold(
                                    &session,
                                    &account_ks,
                                    &table_id,
                                    &held_id,
                                )
                                .await;
                            });
                        }
                    })?;
                if written {
                    break Ok(true);
                }
                // The write did not apply for one of these reasons:
                // - The row is gone, or it holds a table created again under
                //   the same name.
                // - A stream change committed after this request read the row.
                // - Another UpdateTable wrote the warm throughput after this
                //   request read it.
                // The loop retries only the last case, merged again over what
                // that request wrote. With a stream change pending, the other
                // two end the loop, and the code below answers them. Without
                // one, the merge reads the record by name, so the loop retries
                // a table created again until MERGE_ATTEMPTS and then answers
                // Internal. A stream change that finds its table and stream
                // fields as it read them ran into a warm throughput write.
                let (Some(col), Some(spec)) = (warm_col, input.warm_throughput.as_ref()) else {
                    break Ok(false);
                };
                if let Some(pending) = &pending_stream {
                    match self
                        .stream_condition_fields(account_id, &input.table_name)
                        .await
                    {
                        Ok(Some((id, specification, label)))
                            if id.as_deref() == Some(table_id)
                                && specification == pending.read_specification
                                && label == pending.read_label => {}
                        Ok(_) => break Ok(false),
                        Err(e) => break Err(e),
                    }
                }
                attempts += 1;
                if attempts >= crate::warm_throughput::MERGE_ATTEMPTS {
                    break Err(StorageError::Internal(
                        "warm throughput: the record changed on every attempt to merge the \
                         request"
                            .to_owned(),
                    ));
                }
                match self
                    .merged_table_warm_throughput(account_id, &input.table_name, spec)
                    .await
                {
                    Ok(merge) => {
                        table_cols[col].1 = Value::from(merge.merged.as_str());
                        warm_read = merge.read;
                    }
                    Err(StorageError::TableNotFound(_)) => break Ok(false),
                    Err(e) => break Err(e),
                }
            };
            // A condition that stays false means the row was deleted between
            // our earlier SELECT and this UPDATE: treat as TableNotFound. Under
            // a stream change the condition also fails when another stream
            // change committed in between; `refuse_overlapping_stream_change`
            // re-reads the row and answers as that later request would be, or
            // with TableNotFound when the row is gone or holds another table.
            // NOTE: the tables LWT commits before the indexes batch below runs.
            // A failure in the indexes batch leaves the table row updated with
            // no corresponding index rows; the recovery worker does not cover
            // this gap (it only scans existing CREATING index rows). This is a
            // known atomicity gap — conditional batches cannot span tables in
            // Cassandra. Tracked in ADR-0021.
            let applied = applied.inspect_err(|_e| {
                for held_id in &taken_holds {
                    let session = self.session_arc();
                    let account_ks = account_ks.clone();
                    let held_id = held_id.clone();
                    let table_id = table_id.to_owned();
                    tokio::spawn(async move {
                        let _ = crate::propagation_hold::release_propagation_hold(
                            &session,
                            &account_ks,
                            &table_id,
                            &held_id,
                        )
                        .await;
                    });
                }
            })?;
            if !applied {
                for held_id in &taken_holds {
                    let session = self.session_arc();
                    let account_ks = account_ks.clone();
                    let held_id = held_id.clone();
                    let table_id = table_id.to_owned();
                    tokio::spawn(async move {
                        let _ = crate::propagation_hold::release_propagation_hold(
                            &session,
                            &account_ks,
                            &table_id,
                            &held_id,
                        )
                        .await;
                    });
                }
                if pending_stream.is_some() {
                    self.refuse_overlapping_stream_change(
                        account_id,
                        &input.table_name,
                        table_id,
                        input,
                    )
                    .await?;
                }
                return Err(StorageError::TableNotFound(input.table_name.clone()));
            }
        }

        // Execute indexes batch (plain INSERT/DELETE — no LWT mixing issue).
        if indexes_batch_has_statements
            && let Err(e) = self
                .session
                .batch(
                    indexes_batch
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))?,
                )
                .await
                .map_err(|e| {
                    tracing::error!("update_table indexes batch: {e}");
                    StorageError::Internal("Database error".to_owned())
                })
        {
            for held_id in &taken_holds {
                let _ = crate::propagation_hold::release_propagation_hold(
                    &self.session_arc(),
                    &account_ks,
                    table_id,
                    held_id,
                )
                .await;
            }
            return Err(e);
        }

        // Post-batch: each GSI Update's WarmThroughput, as its own conditional
        // write on the index row (see `store_index_warm_throughput`).
        for update in input.global_secondary_index_updates.iter().flatten() {
            if let Some(action) = &update.update
                && let Some(spec) = &action.warm_throughput
            {
                self.store_index_warm_throughput(table_id, &action.index_name, spec)
                    .await?;
            }
        }

        // Post-batch: close the stream's shards and create the new stream's.
        if let Some(pending) = pending_stream {
            pending
                .apply(self, account_id, &account_ks, &input.table_name, table_id)
                .await?;
        }

        // Post-batch: GSI data table DDL and async backfill.
        if let Some(updates) = &input.global_secondary_index_updates {
            let effective_attr_defs = input
                .attribute_definitions
                .as_deref()
                .unwrap_or(base_attr_defs)
                .to_vec();

            let mut create_idx = 0usize;
            let mut delete_idx = 0usize;
            for update in updates {
                if let Some(create) = &update.create {
                    let (index_id, index_name) = &gsi_creates[create_idx];
                    create_idx += 1;

                    // Create the data table synchronously — it must exist before
                    // the spawned backfill task or any worker tries to write to it.
                    if let Err(e) = self
                        .create_index_data_table(
                            &account_ks,
                            index_id,
                            &create.key_schema,
                            &effective_attr_defs,
                            base_key_schema,
                            base_attr_defs,
                        )
                        .await
                    {
                        tracing::error!(
                            "Failed to create data table for GSI '{}' on '{}': {e}",
                            index_name,
                            input.table_name,
                        );
                        let _ = crate::cassandra_util::execute::<StorageError>(
                            &self.session,
                            &format!(
                                "DELETE FROM {}.indexes WHERE table_id = ? AND index_name = ?",
                                self.catalog_keyspace()
                            ),
                            cdrs_tokio::query_values!(table_id, index_name.as_str()),
                            "cleanup_failed_gsi",
                        )
                        .await;
                        let _ = crate::propagation_hold::release_propagation_hold(
                            &self.session_arc(),
                            &account_ks,
                            table_id,
                            index_id,
                        )
                        .await;
                        return Err(e);
                    }

                    // The backfill recovery worker picks up any CREATING index
                    // with a null or stale heartbeat. Wake it immediately so
                    // the new index doesn't wait for the periodic scan interval.
                    self.gsi_backfill_notify.notify_one();
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

    /// One attempt of `update_table`'s write of the `tables` row: `UPDATE ...
    /// SET` the columns of `set`, conditional on [`tables_write_condition`].
    /// `true` when the write applied. Public for the direct tests, which run
    /// it against a row that does not exist.
    #[doc(hidden)]
    pub async fn write_tables_row(
        &self,
        account_id: &str,
        table_name: &str,
        set: &[(&'static str, Value)],
        table_id: &str,
        warm_read: Option<Option<&str>>,
        stream_read: Option<(Option<&str>, Option<&str>)>,
    ) -> Result<bool, StorageError> {
        let set_clause = set
            .iter()
            .map(|(col, _)| format!("{col} = ?"))
            .collect::<Vec<_>>()
            .join(", ");
        let (condition, condition_values) =
            tables_write_condition(table_id, warm_read, stream_read);
        let cql = format!(
            "UPDATE {}.tables SET {set_clause} \
             WHERE account_id = ? AND table_name = ? {condition}",
            self.catalog_keyspace()
        );
        let mut vals: Vec<Value> = set.iter().map(|(_, v)| v.clone()).collect();
        vals.push(Value::from(account_id));
        vals.push(Value::from(table_name));
        vals.extend(condition_values);
        let result =
            crate::cassandra_util::query_lwt(&self.session, &cql, QueryValues::SimpleValues(vals))
                .await?;
        crate::cassandra_util::lwt_applied(&result)
    }
}

/// The condition of `update_table`'s write of the `tables` row and the values
/// it binds, in the order of its parts.
///
/// `warm_read` is the warm throughput record the request read, when the write
/// carries a WarmThroughput. `stream_read` is the stream specification and
/// label the request read, when a stream change is pending: of two overlapping
/// stream changes the one whose write finds them changed re-reads the row and
/// is refused by `stream_change` as a request arriving after the other's commit
/// would be. With either, the write is also conditional on `table_id`, which a
/// missing row does not have. A condition that compares a column with null is
/// met by a missing row, so without it a stream enable on a table created
/// without a stream (both stream columns null) that runs after a DeleteTable
/// would write a row holding only the columns it sets. A table created again
/// under the same name has another `table_id` and fails the condition too.
/// With neither, the write is conditional on the row existing.
fn tables_write_condition(
    table_id: &str,
    warm_read: Option<Option<&str>>,
    stream_read: Option<(Option<&str>, Option<&str>)>,
) -> (String, Vec<Value>) {
    let mut parts = Vec::new();
    let mut values = Vec::new();
    if warm_read.is_some() || stream_read.is_some() {
        parts.push("table_id = ?");
        values.push(Value::from(table_id));
    }
    if let Some(read) = warm_read {
        parts.push("warm_throughput = ?");
        values.push(crate::warm_throughput::condition_value(read));
    }
    if let Some((specification, label)) = stream_read {
        parts.push("stream_specification = ?");
        values.push(Value::from(specification));
        parts.push("stream_label = ?");
        values.push(Value::from(label));
    }
    if parts.is_empty() {
        return ("IF EXISTS".to_owned(), values);
    }
    (format!("IF {}", parts.join(" AND ")), values)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TID: &str = "550e8400-e29b-41d4-a716-446655440000";
    const WARM: &str = r#"{"ReadUnitsPerSecond":12000,"WriteUnitsPerSecond":4000}"#;
    const SPEC: &str = r#"{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}"#;
    const LABEL: &str = "2026-11-09T18:00:00.000";

    fn condition(
        warm_read: Option<Option<&str>>,
        stream_read: Option<(Option<&str>, Option<&str>)>,
    ) -> (String, Vec<Value>) {
        let (text, values) = tables_write_condition(TID, warm_read, stream_read);
        assert_eq!(text.matches('?').count(), values.len(), "{text}");
        (text, values)
    }

    /// The condition of the `tables` write and the values it binds, in order,
    /// with neither a WarmThroughput nor a stream change, with each alone, and
    /// with both. Every case with a part is also conditional on `table_id`,
    /// bound first.
    #[test]
    fn the_tables_write_condition_binds_its_values_in_the_order_of_its_parts() {
        assert_eq!(condition(None, None), ("IF EXISTS".to_owned(), vec![]));

        let warm_only = "IF table_id = ? AND warm_throughput = ?".to_owned();
        assert_eq!(
            condition(Some(Some(WARM)), None),
            (warm_only.clone(), vec![Value::from(TID), Value::from(WARM)])
        );
        assert_eq!(
            condition(Some(None), None),
            (warm_only, vec![Value::from(TID), Value::Null])
        );

        // A table created without a stream has both stream columns null, so
        // its first enable binds two nulls. `table_id` keeps a row that does
        // not exist from meeting the condition.
        let stream_only =
            "IF table_id = ? AND stream_specification = ? AND stream_label = ?".to_owned();
        assert_eq!(
            condition(None, Some((None, None))),
            (
                stream_only.clone(),
                vec![Value::from(TID), Value::Null, Value::Null]
            )
        );
        assert_eq!(
            condition(None, Some((Some(SPEC), Some(LABEL)))),
            (
                stream_only,
                vec![Value::from(TID), Value::from(SPEC), Value::from(LABEL)]
            )
        );

        assert_eq!(
            condition(Some(Some(WARM)), Some((Some(SPEC), None))),
            (
                "IF table_id = ? AND warm_throughput = ? AND stream_specification = ? \
                 AND stream_label = ?"
                    .to_owned(),
                vec![
                    Value::from(TID),
                    Value::from(WARM),
                    Value::from(SPEC),
                    Value::Null
                ]
            )
        );
    }
}
