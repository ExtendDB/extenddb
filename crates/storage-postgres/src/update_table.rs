// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_table` implementation for `PostgresEngine`.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, TableDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::effective_attribute_definitions;

use crate::PostgresEngine;

/// Give back the build holds a failed `UpdateTable` took.
///
/// A hold stops the propagation queue claiming anything for its table, so one left
/// behind pauses that table's index propagation until recovery notices. Failures to
/// release are logged rather than propagated: the request has already failed, and the
/// orphan sweep is the backstop.
async fn release_taken_holds(
    data_pool: &sqlx::PgPool,
    table_id: &str,
    taken_holds: &[String],
    table_name: &str,
) {
    for index_id in taken_holds {
        if let Err(release) =
            crate::data::vector_index::release_hold(data_pool, table_id, index_id).await
        {
            tracing::error!(
                "could not release a build hold after a failed UpdateTable on '{table_name}'; \
                 the table's index propagation stays paused until the orphan sweep clears it: \
                 {release}"
            );
        }
    }
}

/// Refuse a new index whose name is already taken on this table, whatever index
/// family holds it.
///
/// CreateTable enforces uniqueness across secondary and vector index names
/// together, in one place. UpdateTable builds each family's create path
/// separately, and each one used to consult only its own catalog table, so a GSI
/// and a vector index could end up sharing a name on the same table, and with it
/// a single index ARN.
///
/// The error wording is the existing duplicate-index message. The service's own
/// wording for the cross-family case is not measured, so it is not claimed here.
async fn ensure_index_name_free(
    conn: &mut sqlx::PgConnection,
    table_id: &str,
    index_name: &str,
) -> Result<(), StorageError> {
    let taken: Option<(String,)> = sqlx::query_as(
        "SELECT index_name FROM indexes WHERE table_id = $1 AND index_name = $2 \
         UNION ALL \
         SELECT index_name FROM vector_indexes WHERE table_id = $1 AND index_name = $2",
    )
    .bind(table_id)
    .bind(index_name)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    if taken.is_some() {
        return Err(StorageError::IndexAlreadyExists(index_name.to_owned()));
    }
    Ok(())
}

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

        // Lock the row and fetch table_id, key_schema, attribute_definitions and
        // the stored billing mode.
        //
        // `stored_billing_mode` is read once here and threaded through every
        // later check that needs it, matching the SQLite backend. Re-querying it
        // per check cost extra round trips and, because the row is already
        // locked, could never return a different answer.
        let row: Option<(
            String,
            String,
            serde_json::Value,
            serde_json::Value,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT table_status, table_id, key_schema, attribute_definitions, billing_mode FROM tables WHERE account_id = $1 AND table_name = $2 FOR UPDATE",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (status, table_id, ks_json, ad_json, stored_billing_mode) =
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
                // Two measured shapes, two strings. A plain switch reports the
                // create-side rule; a switch that also carries VectorIndexUpdates
                // reports its own message, measured 2026-08-19 on a switch combined
                // with deleting the last vector index, which the service refuses
                // even though the net state would carry none.
                //
                // Only those two shapes are measured. Which of the two fires for a
                // switch combined with a vector index CREATE is unmapped, and that
                // shape is refused earlier here anyway, so it cannot reach this
                // choice. If the trigger turns out to be the delete specifically
                // rather than the presence of updates, this condition is where that
                // changes.
                let message = if input
                    .vector_index_updates
                    .as_ref()
                    .is_some_and(|u| !u.is_empty())
                {
                    extenddb_core::types::VECTOR_TABLE_REQUIRES_PAY_PER_REQUEST_MODE
                } else {
                    extenddb_core::types::VECTOR_INDEX_REQUIRES_PAY_PER_REQUEST
                };
                return Err(StorageError::Validation(message.to_owned()));
            }
        }

        // The other direction of the same rule: a vector index cannot be ADDED to a
        // table that is provisioned. Measured 2026-08-19 against a live PROVISIONED
        // table, which returned the identical string, so both directions share one
        // constant.
        //
        // The check is on the request's NET billing mode rather than the table's
        // stored mode, because an UpdateTable that switches to PAY_PER_REQUEST and
        // creates the index in one call was measured to succeed. That is the same
        // net-effect evaluation the index-count cap uses, and it is why the
        // request's own billing mode wins when it carries one.
        //
        // Deliberately not in stage one: the guard belongs with the create path,
        // because before that path existed this would have been an unreachable
        // second refusal with different wording from the one that did fire.
        let creates_vector_index = input
            .vector_index_updates
            .as_ref()
            .is_some_and(|updates| updates.iter().any(|u| u.create.is_some()));
        if creates_vector_index {
            let net_pay_per_request = match input.billing_mode {
                Some(mode) => mode == BillingMode::PayPerRequest,
                None => stored_billing_mode.as_deref() == Some("PAY_PER_REQUEST"),
            };
            if !net_pay_per_request {
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
                None => stored_billing_mode.as_deref() == Some("PAY_PER_REQUEST"),
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
                    // Across families, not just this one: see
                    // `ensure_index_name_free`.
                    ensure_index_name_free(&mut tx, &table_id, &create.index_name).await?;

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

        // Ids of the vector indexes this request deletes, so their data tables can
        // be dropped after the catalog commit, in the same order as the GSI drops.
        let mut deleted_vector_index_ids: Vec<String> = Vec::new();
        // Indexes this request creates, with their specifications, so the build can
        // start after the catalog commit.
        let mut created_vector_indexes: Vec<(
            String,
            extenddb_core::types::VectorIndexSpecification,
        )> = Vec::new();
        // Vector index create/delete.
        //
        // Both are implemented and both are reachable over the wire: this backend
        // declares vector search capability whenever pgvector is present, so the
        // engine's gate passes and a client can create or delete a vector index
        // here. Create records the catalog row in this transaction and starts the
        // build after the commit, which is why the created ids are collected
        // rather than acted on inline.
        //
        // A catalog row with no storage behind it would be an index that reports
        // ACTIVE and answers nothing, so the create path's failure story is to
        // leave the index CREATING for recovery rather than to publish it.
        // Wrapped so a failure anywhere in this block gives the holds back.
        //
        // `take_hold` writes to the data database, a different database from the
        // catalog, so it commits on its own and the catalog rollback cannot undo it.
        // Any error between the first hold and the commit below would leave a hold
        // with no catalog row to release it, and a hold stops the propagation queue
        // claiming anything for that table: an ordinary 400, such as one create
        // paired with a delete of an index that does not exist, would silently freeze
        // that table's index propagation until a restart.
        let mut taken_holds: Vec<String> = Vec::new();
        let vector_updates: Result<(), StorageError> = async {
            if let Some(updates) = &input.vector_index_updates {
                // Per-table cap on the NET effect of the whole request, not per action,
                // so a delete paired with a create against a full table passes whatever
                // order they are listed in: the request is a set of changes rather than
                // a program. Deletes of absent indexes fail below anyway, so counting
                // every delete here cannot let an over-cap request through.
                //
                // UpdateTable reports this as LimitExceededException with different
                // wording from CreateTable's ValidationException. Counted inside the
                // transaction under the row lock, so a concurrent request cannot change
                // the answer between the count and the insert.
                let existing: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM vector_indexes WHERE table_id = $1")
                        .bind(&table_id)
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                let creates = i64::try_from(updates.iter().filter(|u| u.create.is_some()).count())
                    .unwrap_or(i64::MAX);
                let deletes =
                    i64::try_from(updates.iter().filter(|u| u.delete.is_some()).count()).unwrap_or(0);
                if existing + creates - deletes
                    > i64::try_from(extenddb_core::types::MAX_VECTOR_INDEXES_PER_TABLE)
                        .unwrap_or(i64::MAX)
                {
                    return Err(StorageError::LimitExceeded(
                        extenddb_core::types::VECTOR_INDEX_COUNT_LIMIT_UPDATE.to_owned(),
                    ));
                }

                for update in updates {
                    if let Some(create) = &update.create {
                        // The vector attribute cannot be a key attribute. CreateTable
                        // reports that through the conflicting-definition rule, because
                        // the key must be declared there; on UpdateTable the key is not
                        // re-declared, and the service instead reports a redefinition
                        // naming both shapes, with the vector as type L and its
                        // dimension count.
                        let base_key_schema: Vec<KeySchemaElement> =
                            serde_json::from_value(ks_json.clone())
                                .map_err(|e| StorageError::Internal(e.to_string()))?;
                        let stored_attr_defs: Vec<AttributeDefinition> =
                            serde_json::from_value(ad_json.clone())
                                .map_err(|e| StorageError::Internal(e.to_string()))?;
                        let vec_attr_name = &create.vector_attribute.attribute_name;
                        if let Some(ks) = base_key_schema
                            .iter()
                            .find(|ks| &ks.attribute_name == vec_attr_name)
                        {
                            let existing_type = stored_attr_defs
                                .iter()
                                .find(|ad| &ad.attribute_name == vec_attr_name)
                                .map_or("S", |ad| match ad.attribute_type {
                                    extenddb_core::types::ScalarAttributeType::S => "S",
                                    extenddb_core::types::ScalarAttributeType::N => "N",
                                    extenddb_core::types::ScalarAttributeType::B => "B",
                                });
                            let key_type = match ks.key_type {
                                extenddb_core::types::KeyType::Hash => "HASH",
                                extenddb_core::types::KeyType::Range => "RANGE",
                            };
                            return Err(StorageError::Validation(
                                extenddb_core::types::vector_attribute_redefines_key(
                                    vec_attr_name,
                                    existing_type,
                                    key_type,
                                    create.dimensions,
                                ),
                            ));
                        }

                        ensure_index_name_free(&mut tx, &table_id, &create.index_name).await?;

                        let index_id = uuid::Uuid::new_v4().to_string();
                        let vec_attr = serde_json::to_value(&create.vector_attribute)
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        let search_schema = create
                            .search_schema_for_storage()
                            .map(serde_json::to_value)
                            .transpose()
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        let projection = serde_json::to_value(&create.projection)
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        let distance = extenddb_storage::vector_catalog::distance_function_token(
                            create.distance_function,
                        )?;

                        // The hold goes in BEFORE the CREATING row commits, so there is
                        // no instant where a writer can enqueue against an index the
                        // propagation queue does not yet know to hold back.
                        crate::data::vector_index::take_hold(&self.data_pool, &table_id, &index_id)
                            .await?;
                        taken_holds.push(index_id.clone());

                        // `backfilling` starts false rather than absent or true: the
                        // member appears as false while the index exists and its scan
                        // has not started, flips to true during, and is removed once
                        // ACTIVE.
                        sqlx::query(
                            r"INSERT INTO vector_indexes
                               (table_id, index_name, index_id, dimensions, distance_function,
                                vector_attribute, search_schema, projection, index_status, backfilling)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'CREATING', false)",
                        )
                        .bind(&table_id)
                        .bind(&create.index_name)
                        .bind(&index_id)
                        .bind(i32::try_from(create.dimensions).map_err(|_| {
                            StorageError::Internal(format!(
                                "vector dimensions out of range: {}",
                                create.dimensions
                            ))
                        })?)
                        .bind(&distance)
                        .bind(&vec_attr)
                        .bind(&search_schema)
                        .bind(&projection)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                        created_vector_indexes.push((index_id, create.clone()));
                        continue;
                    }
                    if let Some(delete) = &update.delete {
                        let existing: Option<(String, String, Option<bool>)> = sqlx::query_as(
                            "SELECT index_id, index_status, backfilling FROM vector_indexes \
                             WHERE table_id = $1 AND index_name = $2",
                        )
                        .bind(&table_id)
                        .bind(&delete.index_name)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                        let (del_index_id, index_status, backfilling) = existing
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
                        deleted_vector_index_ids.push(del_index_id);
                    }
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = vector_updates {
            release_taken_holds(&self.data_pool, &table_id, &taken_holds, &input.table_name).await;
            return Err(e);
        }

        // The commit is inside the same recovery, because it is the one remaining path
        // that can fail after a hold has been taken. A failing commit is also the
        // moment the infrastructure is already unhappy, which is the worst time to
        // leave a table's index propagation paused for a request the client already
        // knows failed.
        if let Err(e) = tx
            .commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))
        {
            release_taken_holds(&self.data_pool, &table_id, &taken_holds, &input.table_name).await;
            return Err(e);
        }

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

        // Vector index builds start after the catalog commit, so a crash in between
        // leaves a CREATING row for the reconciler rather than an ACTIVE index over
        // a table that was never populated.
        for (index_id, create) in &created_vector_indexes {
            let base_key_schema: Vec<KeySchemaElement> = serde_json::from_value(ks_json.clone())
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let base_attr_defs: Vec<AttributeDefinition> = serde_json::from_value(ad_json.clone())
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let started = self
                .start_vector_index_build(
                    &table_id,
                    index_id,
                    create,
                    base_key_schema,
                    base_attr_defs,
                )
                .await;
            if let Err(e) = started {
                // Setting up the build failed, so nothing will ever publish this
                // index. Roll the whole thing back rather than leaving a CREATING
                // row that a reconciler would keep retrying: the request failed, and
                // the client will see that.
                tracing::error!(
                    "Failed to start the build for vector index '{}' on '{}', cleaning up: {e}",
                    create.index_name,
                    input.table_name,
                );
                let _ = sqlx::query(
                    "DELETE FROM vector_indexes WHERE table_id = $1 AND index_name = $2",
                )
                .bind(&table_id)
                .bind(&create.index_name)
                .execute(&self.pool)
                .await;
                let _ =
                    crate::data::vector_index::release_hold(&self.data_pool, &table_id, index_id)
                        .await;
                let mut data_tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let _ = Self::drop_vector_data_table(&mut data_tx, index_id).await;
                let _ = data_tx.commit().await;
                return Err(e);
            }
        }

        // Vector data tables are dropped after the catalog commit, like the GSI
        // ones: the catalog is the record of what exists, so it commits first and
        // a crash in between leaves an unreferenced table rather than an index
        // whose rows are gone.
        for index_id in &deleted_vector_index_ids {
            // The hold goes with the index. A build in progress will fail on its
            // next batch because the data table is gone, and a failed build
            // deliberately leaves its index CREATING for recovery, so nothing else
            // would ever release this. Left behind it stops the propagation queue
            // claiming ANY row for this table, permanently: one index deleted
            // mid-build would freeze every later write on the table.
            if let Err(e) =
                crate::data::vector_index::release_hold(&self.data_pool, &table_id, index_id).await
            {
                tracing::warn!(
                    "Failed to release the queue hold for a deleted vector index on '{}': {e}",
                    input.table_name,
                );
            }
            let mut data_tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            if let Err(e) = Self::drop_vector_data_table(&mut data_tx, index_id).await {
                tracing::warn!(
                    "Failed to drop the data table for a deleted vector index on '{}': {e}",
                    input.table_name,
                );
                continue;
            }
            // The queue rows for this index outlive it, deliberately. The worker
            // consumes them when it finds the data table gone, which is the route
            // that keeps a partition moving; deleting them here would need the same
            // transaction as the catalog change to be safe, and it is not.
            if let Err(e) = data_tx
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))
            {
                tracing::warn!(
                    "Failed to commit the vector data table drop on '{}': {e}",
                    input.table_name,
                );
            }
        }

        self.build_table_description(account_id, &input.table_name)
            .await
    }

    /// Create a vector index's data table and start its backfill.
    ///
    /// Ownership is taken here rather than being a shared-lifecycle primitive,
    /// which is what the lifecycle contract asks of a multi-process backend: the
    /// token is acquired before the driver is spawned and released when the task
    /// ends. A session-scoped advisory lock is the right token because it dies
    /// with the connection, so a crashed front-end's claim disappears on its own
    /// rather than needing a timeout to be disbelieved. The heartbeat column is
    /// what a peer reads to tell a slow build from a dead one without taking the
    /// lock.
    async fn start_vector_index_build(
        &self,
        table_id: &str,
        index_id: &str,
        create: &extenddb_core::types::VectorIndexSpecification,
        base_key_schema: Vec<KeySchemaElement>,
        base_attr_defs: Vec<AttributeDefinition>,
    ) -> Result<(), StorageError> {
        let mut data_tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Self::create_vector_data_table(
            &mut data_tx,
            index_id,
            create.dimensions,
            &base_key_schema,
            &base_attr_defs,
        )
        .await?;
        data_tx
            .commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut ops = crate::data::vector_index::PostgresVectorBuild {
            catalog: self.pool.clone(),
            data: self.data_pool.clone(),
            queue_notify: self.gsi_queue.clone(),
            table_id: table_id.to_owned(),
            index_id: index_id.to_owned(),
            base_key_schema,
            attribute_definitions: base_attr_defs,
            dimensions: create.dimensions,
            meta: None,
        };
        ops.load_meta().await?;

        let index_name = create.index_name.clone();
        let batch_delay =
            std::time::Duration::from_millis(self.vector_backfill_batch_delay().await);
        // Zero in production. A test sets it to hold the index in the
        // resource-allocation phase, which is the only way a client can observe that
        // phase: it otherwise exists only between the catalog row's insert and the
        // flip below, the second inside the detached task this call spawns.
        let allocation_delay =
            std::time::Duration::from_millis(self.vector_allocation_phase_delay().await);
        let ownership_pool = self.data_pool.clone();
        let ownership_id = index_id.to_owned();
        let hold_table_id = table_id.to_owned();
        tokio::spawn(async move {
            // Held for the life of the build and dropped with the task, which is
            // what makes a dead builder's claim vanish. A peer that finds the lock
            // free and the heartbeat stale may rebuild.
            let Some(_owner) =
                crate::data::vector_index::build_ownership(&ownership_pool, &ownership_id).await
            else {
                // Someone else owns it, so leave the hold to them: releasing it here
                // would let writes reach an index whose backfill is still running.
                tracing::info!(
                    index_name = %index_name,
                    "another process already owns this vector index build; leaving it to that one"
                );
                return;
            };
            // Nothing waits and no branch is taken when the lever is unset, which is
            // the same shape the shared driver uses for its own inter-batch pause.
            if !allocation_delay.is_zero() {
                tokio::time::sleep(allocation_delay).await;
            }
            // Outside the backfill transaction, deliberately: the flag exists to be
            // readable while the scan runs, so setting it inside would make it
            // invisible to every observer. Inside the task rather than before the
            // spawn, so the caller returns while the index is still allocating,
            // which is the state the service reports first.
            if let Err(e) =
                extenddb_storage::vector_lifecycle::VectorIndexBuild::set_backfilling(&mut ops)
                    .await
            {
                // Give up the hold on the way out, or this index becomes undeletable
                // on a table that has stopped propagating: the delete path refuses a
                // CREATING index reporting `Backfilling: false` and tells the caller
                // to retry during backfilling, which is exactly the phase that just
                // failed to arrive. The only other exit would be a restart.
                //
                // Safe because recovery rebuilds rather than resumes:
                // `reset_data_table` drops the data table first, so any queue rows
                // applied in the meantime are discarded rather than colliding with
                // the backfill's deliberately plain INSERT. This turns a wedge into
                // the ordinary crashed-build state the reconciler already repairs.
                if let Err(release) = crate::data::vector_index::release_hold(
                    &ownership_pool,
                    &hold_table_id,
                    &ownership_id,
                )
                .await
                {
                    tracing::error!(
                        index_name = %index_name,
                        "could not release the build hold for an abandoned build; the table's \
                         index propagation stays paused until startup reconciliation: {release}"
                    );
                }
                tracing::error!(
                    index_name = %index_name,
                    "could not mark the vector index as backfilling, leaving it CREATING \
                     for startup reconciliation: {e}"
                );
                return;
            }
            extenddb_storage::vector_lifecycle::complete_build(
                ops,
                &index_name,
                extenddb_storage::vector_lifecycle::BACKFILL_BATCH,
                batch_delay,
            )
            .await;
        });
        Ok(())
    }
}
