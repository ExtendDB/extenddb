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
//!
//! Vector index create/delete follows the same two-phase shape, with the
//! backfill lifecycle the service was measured to report.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, TableDescription, UpdateTableInput,
    VectorIndexSpecification,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::effective_attribute_definitions;

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

        // A table holding vector indexes cannot leave PAY_PER_REQUEST. Same
        // message as CreateTable's rejection; measured 2026-08-13 by switching
        // a live vector table to PROVISIONED.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned)) {
            let vector_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM vector_indexes WHERE table_id = ?")
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

        // The other direction of the same rule: a vector index cannot be ADDED to
        // a table that is provisioned. Measured 2026-08-19 against a live
        // PROVISIONED table, which returned the identical string, so the two
        // directions share one constant.
        //
        // The check is on the request's NET billing mode, not the table's stored
        // mode: an UpdateTable that switches to PAY_PER_REQUEST and creates the
        // index in the same call was measured to succeed. That is the same
        // net-effect evaluation the index-count limit below uses, and it is why
        // the request's own billing_mode wins when present.
        let creates_vector_index = input
            .vector_index_updates
            .as_ref()
            .is_some_and(|updates| updates.iter().any(|u| u.create.is_some()));
        if creates_vector_index {
            let net_pay_per_request = match input.billing_mode {
                Some(mode) => mode == BillingMode::PayPerRequest,
                None => {
                    // A NULL stored billing_mode means PROVISIONED, matching how
                    // the throughput checks below read it.
                    let stored: Option<String> = sqlx::query_scalar(
                        "SELECT billing_mode FROM tables WHERE account_id = ? AND table_name = ?",
                    )
                    .bind(account_id)
                    .bind(&input.table_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .flatten();
                    stored.as_deref() == Some("PAY_PER_REQUEST")
                }
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
            // The index rows were created and deleted above inside this BEGIN
            // IMMEDIATE transaction, so `indexes` already holds exactly the
            // surviving set and the read-modify-write is atomic.
            let stored_attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&ad_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let table_key_schema: Vec<KeySchemaElement> = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let surviving_rows: Vec<(String,)> =
                sqlx::query_as("SELECT key_schema FROM indexes WHERE table_id = ?")
                    .bind(&table_id)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            let mut surviving_index_key_schemas: Vec<Vec<KeySchemaElement>> =
                Vec::with_capacity(surviving_rows.len());
            for (ks_text,) in &surviving_rows {
                surviving_index_key_schemas.push(
                    serde_json::from_str(ks_text)
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
                let j = serde_json::to_string(&effective)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                update_col(
                    &mut tx,
                    account_id,
                    &input.table_name,
                    "attribute_definitions",
                    &j,
                )
                .await?;
            }
            merged_attr_defs_for_ddl = Some(effective);
        }

        // Vector index create/delete. Structured exactly like the GSI block above
        // and for the same reason: the catalog row is committed first at CREATING,
        // and the data table plus backfill happen after, so a crash in between
        // leaves a CREATING row the startup reconciler rebuilds rather than an
        // ACTIVE index with a partial table.
        let mut vec_created: Vec<(String, VectorIndexSpecification)> = Vec::new();
        let mut vec_deleted: Vec<String> = Vec::new();
        if let Some(updates) = &input.vector_index_updates {
            // Per-table count limit, evaluated on the NET effect of the whole
            // request rather than per-action, so a delete+create against a full
            // table passes regardless of the order the actions are listed in.
            // DynamoDB's model is a set of index changes, not an ordered
            // program, so list order must not decide acceptance. Deletes of
            // indexes that do not exist fail below anyway, so counting every
            // delete here cannot let an over-cap request through.
            //
            // UpdateTable reports the limit as LimitExceededException with
            // different wording from CreateTable's ValidationException;
            // measured 2026-08-13 by adding a sixth index to a five-index
            // table. Counted inside the transaction under the write lock, so
            // no concurrent request can change the answer.
            let existing: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM vector_indexes WHERE table_id = ?")
                    .bind(&table_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            let creates = updates.iter().filter(|u| u.create.is_some()).count() as i64;
            let deletes = updates.iter().filter(|u| u.delete.is_some()).count() as i64;
            if existing + creates - deletes
                > extenddb_core::types::MAX_VECTOR_INDEXES_PER_TABLE as i64
            {
                return Err(StorageError::LimitExceeded(
                    extenddb_core::types::VECTOR_INDEX_COUNT_LIMIT_UPDATE.to_owned(),
                ));
            }

            for update in updates {
                if let Some(create) = &update.create {
                    // The vector attribute cannot collide with a key attribute.
                    // CreateTable reports this via the conflicting-definition
                    // rule (the key must be declared there); on UpdateTable the
                    // key is not re-declared, and the service instead reports a
                    // redefinition message embedding both schemas, with the
                    // vector reported as type L and its dimension count.
                    // Measured 2026-08-13 with the confound removed (the first
                    // probe failed on the SearchSchema rule instead).
                    let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
                        serde_json::from_str(&ks_json)
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let attr_defs: Vec<extenddb_core::types::AttributeDefinition> =
                        serde_json::from_str(&ad_json)
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let vec_attr_name = &create.vector_attribute.attribute_name;
                    if let Some(ks) = key_schema
                        .iter()
                        .find(|ks| &ks.attribute_name == vec_attr_name)
                    {
                        let existing_type = attr_defs
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

                    // The per-table count limit is enforced on the request's
                    // net effect before this loop; see above.

                    // A create whose vector attribute an EXISTING index already
                    // uses must agree with it on Dimensions: the attribute
                    // stores one vector. Measured live 2026-08-24 (us-east-1):
                    // the service answers the attribute-redefinition message
                    // with both sides in the VectorIndexSchema shape, and
                    // accepts the same attribute at the SAME dimensions.
                    // Checked inside the transaction under the write lock, so a
                    // concurrent create cannot slip a disagreeing sibling in.
                    let existing_dims: Option<(i64,)> = sqlx::query_as(
                        "SELECT dimensions FROM vector_indexes \
                         WHERE table_id = ? AND vector_attribute = ? \
                           AND dimensions <> ? LIMIT 1",
                    )
                    .bind(&table_id)
                    .bind(
                        serde_json::to_string(&create.vector_attribute)
                            .map_err(|e| StorageError::Internal(e.to_string()))?,
                    )
                    .bind(i64::from(create.dimensions))
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    if let Some((existing,)) = existing_dims {
                        return Err(StorageError::Validation(
                            extenddb_core::types::vector_attribute_redefines_vector(
                                vec_attr_name,
                                u32::try_from(existing).unwrap_or_default(),
                                create.dimensions,
                            ),
                        ));
                    }

                    let dup: Option<(String,)> = sqlx::query_as(
                        "SELECT index_name FROM vector_indexes \
                         WHERE table_id = ? AND index_name = ?",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    if dup.is_some() {
                        return Err(StorageError::IndexAlreadyExists(create.index_name.clone()));
                    }
                    let index_id = uuid::Uuid::new_v4().to_string();
                    let vec_attr = serde_json::to_string(&create.vector_attribute)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let search_schema = create
                        .search_schema
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let projection = serde_json::to_string(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let dimensions = i64::from(create.dimensions);
                    let distance = serde_json::to_string(&create.distance_function)
                        .map_err(|e| StorageError::Internal(e.to_string()))?
                        .trim_matches('"')
                        .to_owned();
                    // `backfilling` starts at false rather than absent or true.
                    // Measured against the service on 2026-08-06: the member appears
                    // as false while the index exists but its backfill has not
                    // started, flips to true during, and is removed once ACTIVE.
                    sqlx::query(
                        "INSERT INTO vector_indexes \
                         (table_id, index_id, index_name, dimensions, distance_function, \
                          vector_attribute, search_schema, projection, index_status, backfilling) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'CREATING', 0)",
                    )
                    .bind(&table_id)
                    .bind(&index_id)
                    .bind(&create.index_name)
                    .bind(dimensions)
                    .bind(&distance)
                    .bind(&vec_attr)
                    .bind(&search_schema)
                    .bind(&projection)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    vec_created.push((index_id, create.clone()));
                }
                if let Some(delete) = &update.delete {
                    let existing: Option<(String,)> = sqlx::query_as(
                        "SELECT index_id FROM vector_indexes \
                         WHERE table_id = ? AND index_name = ?",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let (del_id,) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;
                    sqlx::query("DELETE FROM vector_indexes WHERE table_id = ? AND index_name = ?")
                        .bind(&table_id)
                        .bind(&delete.index_name)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    vec_deleted.push(del_id);
                }
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

        // Vector index data DDL and backfill, after the catalog commit.
        if !vec_created.is_empty() || !vec_deleted.is_empty() {
            let base_ks: Vec<KeySchemaElement> = serde_json::from_str(&ks_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let base_ad: Vec<AttributeDefinition> = serde_json::from_str(&ad_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let effective_ad = input.attribute_definitions.as_deref().unwrap_or(&base_ad);

            for (index_id, create) in &vec_created {
                let result = self
                    .build_vector_index(&table_id, index_id, create, &base_ks, effective_ad)
                    .await;
                if let Err(e) = result {
                    tracing::error!(
                        "Failed to build vector index '{}' on '{}', cleaning up catalog: {e}",
                        create.index_name,
                        input.table_name
                    );
                    // Same cleanup as the GSI path: leaving the CREATING row behind
                    // would have the reconciler retry a build that just failed
                    // deterministically, on every startup.
                    let _ = sqlx::query(
                        "DELETE FROM vector_indexes WHERE table_id = ? AND index_name = ?",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .execute(&self.pool)
                    .await;
                    let _ =
                        Self::drop_vector_data_table_by_id(&self.pool, &table_id, index_id).await;
                    return Err(e);
                }
            }

            for index_id in &vec_deleted {
                Self::drop_vector_data_table_by_id(&self.pool, &table_id, index_id).await?;
            }
        }

        self.build_table_description(account_id, &input.table_name)
            .await
    }

    /// Create a vector index's data table and populate it, then mark it ready.
    ///
    /// The status sequence is the one measured against the service on 2026-08-06:
    /// `CREATING` with `Backfilling: false`, then `CREATING` with `true` while the
    /// scan runs, then `ACTIVE` with the member absent. Writing `false` first rather
    /// than jumping straight to `true` matters because a client is documented to
    /// read the value rather than test for presence, so an index that exists but
    /// has not started backfilling must say so.
    ///
    /// The backfill is DETACHED and commits per batch, so the base table stays
    /// writable while the index builds, as the service's does. This call returns with
    /// the index still `CREATING`.
    ///
    /// That means a half-populated data table is now reachable in principle, so the
    /// guarantee rests on the engine refusing to route a search to an index that is
    /// not `ACTIVE` rather than, as before, on the backfill being one transaction.
    async fn build_vector_index(
        &self,
        table_id: &str,
        index_id: &str,
        create: &VectorIndexSpecification,
        base_ks: &[KeySchemaElement],
        effective_ad: &[AttributeDefinition],
    ) -> Result<(), StorageError> {
        let mut data_tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Self::create_vector_data_table(&mut data_tx, table_id, index_id, base_ks, effective_ad)
            .await?;
        data_tx
            .commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // The scan is about to start, so the member becomes true. Set outside the
        // backfill transaction, otherwise no observer could see it: the whole point
        // of the flag is to be readable while the scan is in progress.
        sqlx::query(
            "UPDATE vector_indexes SET backfilling = 1 WHERE table_id = ? AND index_id = ?",
        )
        .bind(table_id)
        .bind(index_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut meta_tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let metas =
            crate::data::vector_index::fetch_vector_indexes_for_table(&mut meta_tx, table_id)
                .await?
                .into_iter()
                .find(|m| m.index_id == index_id)
                .ok_or_else(|| {
                    StorageError::Internal(
                        "the vector index catalog row vanished between commit and backfill"
                            .to_owned(),
                    )
                })?;
        meta_tx
            .commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Everything the scan needs, owned, so it can outlive this call.
        let pool = self.pool.clone();
        let write_lock = std::sync::Arc::clone(&self.write_lock);
        let batch_delay =
            std::time::Duration::from_millis(self.vector_backfill_batch_delay().await);
        // The floor on how long the index stays CREATING. Captured with the
        // creation instant here rather than inside the task, so the hold
        // measures from when the caller could first observe the index, and a
        // live settings change mid-build does not move an in-flight deadline.
        let min_creating =
            std::time::Duration::from_millis(self.vector_index_min_creating_ms().await);
        let created_at = tokio::time::Instant::now();
        let owned_table_id = table_id.to_owned();
        let owned_index_id = index_id.to_owned();
        let owned_index_name = create.index_name.clone();
        let owned_base_ks = base_ks.to_vec();
        let owned_ad = effective_ad.to_vec();
        let gsi_notify = self.gsi_notify();

        // Registered BEFORE the spawn, so there is no instant where the catalog
        // says CREATING and the registry disagrees while the task is viable. The
        // guard deregisters on every exit path including a panic, which is the
        // whole point: a CREATING index with no registry entry is provably
        // orphaned, and the worker's recovery sweep may rebuild it.
        self.vector_builds_running
            .lock()
            .expect("registry poisoned")
            .insert(index_id.to_owned());
        let registry = std::sync::Arc::clone(&self.vector_builds_running);

        // Detached, so UpdateTable returns while the index is still CREATING. The
        // service behaves this way, and it is the whole point: a table stays ACTIVE
        // and writable throughout, taking over eight minutes on an empty table when
        // measured, and searches against the index are refused until it is ACTIVE.
        //
        // Not awaited, so failures cannot be returned to the caller. They are logged
        // and the index is deliberately LEFT in CREATING, which is the state the
        // worker's recovery sweep repairs at runtime (and
        // `reconcile_incomplete_vector_indexes` at startup). Flipping it to
        // ACTIVE on error would publish a partially populated index, and there is no
        // failure state on the wire for an index to sit in.
        tokio::spawn(async move {
            struct Deregister(
                std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
                String,
            );
            impl Drop for Deregister {
                fn drop(&mut self) {
                    if let Ok(mut set) = self.0.lock() {
                        set.remove(&self.1);
                    }
                }
            }
            let _deregister = Deregister(registry, owned_index_id.clone());
            let result = crate::data::vector_index::backfill_vector_index_in_batches(
                &pool,
                &write_lock,
                &owned_table_id,
                &metas,
                &owned_base_ks,
                &owned_ad,
                batch_delay,
            )
            .await;
            match result {
                Ok(outcome) => {
                    // The service's online-index machinery never finishes an
                    // added index instantly, so the CREATING walk (`Backfilling`
                    // false, then true, then ACTIVE with the member absent) is
                    // always observable there. A local backfill over a small
                    // table completes in milliseconds, which would collapse that
                    // walk into an instant no DescribeTable poll can catch, so
                    // the flip waits out the remainder of the configured floor.
                    // Searches keep answering the documented not-ready rejection
                    // and writes keep queuing behind the CREATING hold for the
                    // duration, exactly as during a real backfill.
                    tokio::time::sleep_until(created_at + min_creating).await;
                    // Populated, so the index can serve. `backfilling` is cleared to
                    // NULL rather than set to 0, because the service removes the
                    // member once ACTIVE and the catalog CHECK constraint enforces
                    // that pairing. The skipped count is recorded in the same flip,
                    // but only in the catalog (and the log line below): DescribeTable
                    // parity forbids inventing a response field for it, so an
                    // operator diagnosing missing search results finds it by
                    // querying the catalog, not through the API.
                    let flip = sqlx::query(
                        "UPDATE vector_indexes SET index_status = 'ACTIVE', backfilling = NULL, \
                         skipped_item_count = ? \
                         WHERE table_id = ? AND index_id = ?",
                    )
                    .bind(i64::try_from(outcome.skipped).unwrap_or(i64::MAX))
                    .bind(&owned_table_id)
                    .bind(&owned_index_id)
                    .execute(&pool)
                    .await;
                    match flip {
                        Ok(_) => {
                            tracing::info!(
                                index_name = %owned_index_name,
                                vectors_indexed = outcome.written,
                                vectors_skipped = outcome.skipped,
                                "vector index backfill complete"
                            );
                            // Writes that landed during the backfill were held by the
                            // worker because this index was CREATING. It is ACTIVE
                            // now, so wake the worker rather than leaving them to sit
                            // until its next idle timeout.
                            gsi_notify.notify_waiters();
                        }
                        Err(e) => tracing::error!(
                            index_name = %owned_index_name,
                            "vector index backfill finished but the ACTIVE flip failed, \
                             leaving it CREATING for startup reconciliation: {e}"
                        ),
                    }
                }
                Err(e) => tracing::error!(
                    index_name = %owned_index_name,
                    "vector index backfill failed, leaving it CREATING for startup \
                     reconciliation: {e}"
                ),
            }
        });
        Ok(())
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

    /// Rebuild any vector index left in `CREATING` by a crash between the catalog
    /// commit and the end of its backfill.
    ///
    /// Same contract as [`Self::reconcile_incomplete_gsis`]: an `ACTIVE` vector index
    /// with a missing or partial data table can never be observed, and nothing is
    /// left permanently stuck in `CREATING`. Idempotent, because the data table is
    /// dropped and rebuilt rather than appended to; without the drop, a retry would
    /// duplicate every row it had already written before the crash, and a search
    /// would return the same item several times.
    pub(crate) async fn reconcile_incomplete_vector_indexes(&self) -> Result<usize, StorageError> {
        // `backfilling IS NOT NULL` scopes this to UpdateTable-created indexes,
        // the only kind whose CREATING means an interrupted backfill. An index
        // created WITH its table (backfilling NULL) is CREATING only because
        // its table is; the control-plane worker activates both together, and
        // rebuilding it here would publish it ACTIVE ahead of its own table.
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT v.index_id, v.table_id, t.key_schema, t.attribute_definitions \
             FROM vector_indexes v JOIN tables t ON v.table_id = t.table_id \
             WHERE v.index_status = 'CREATING' AND v.backfilling IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut rebuilt = 0usize;
        for (index_id, table_id, base_ks_json, base_ad_json) in rows {
            let written = self
                .rebuild_one_vector_index(&index_id, &table_id, &base_ks_json, &base_ad_json)
                .await?;
            rebuilt += 1;
            tracing::info!(
                vectors_indexed = written,
                "Reconciled incomplete vector index {index_id} on table {table_id}"
            );
        }
        Ok(rebuilt)
    }

    /// Index ids that are `CREATING` with no live backfill task right now.
    ///
    /// The worker's cheap per-pass probe. A single sighting is NOT proof of a
    /// dead build: the catalog row commits before `build_vector_index` registers
    /// the task, so a sweep landing in that window would see a healthy build as
    /// orphaned. The worker therefore requires the same id on two consecutive
    /// passes before invoking [`Self::recover_stuck_vector_builds`], which
    /// re-checks the registry itself at execution time.
    pub(crate) async fn stuck_vector_build_candidates(&self) -> Result<Vec<String>, StorageError> {
        // `backfilling IS NOT NULL` scopes this to UpdateTable-created indexes,
        // which are the only ones with a build task to lose. An index created
        // WITH its table sits in CREATING (backfilling NULL) until the
        // control-plane worker activates it beside the table; it has no task,
        // and sweeping it here would rebuild it early and flip it ACTIVE ahead
        // of its own table.
        let ids: Vec<(String,)> = sqlx::query_as(
            "SELECT index_id FROM vector_indexes \
             WHERE index_status = 'CREATING' AND backfilling IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let registry = self
            .vector_builds_running
            .lock()
            .map_err(|_| StorageError::Internal("vector build registry poisoned".to_owned()))?;
        Ok(ids
            .into_iter()
            .map(|(id,)| id)
            .filter(|id| !registry.contains(id))
            .collect())
    }

    /// Recover `CREATING` vector indexes whose backfill task is dead.
    ///
    /// A build can die without a trace on the wire: the spawned task panics, or
    /// its terminal `ACTIVE` flip fails and is logged. The index then sits in
    /// `CREATING`, which the worker treats as "hold every queued index write for
    /// this table", so one dead build wedges ALL asynchronous index maintenance
    /// for the table until a restart runs the startup reconciler. This is the
    /// runtime half of the same repair: the GSI worker calls it on a sighting of
    /// a `CREATING` index that has no live task in `vector_builds_running`.
    ///
    /// The registry is what makes the sweep safe to run at any time: a healthy
    /// in-flight build is registered before its task is spawned and deregisters
    /// by drop guard, so "CREATING and unregistered" cannot describe a build that
    /// is still making progress in this process. Rebuilding rather than resuming,
    /// for the reconciler's reason: rows already written would collide with the
    /// backfill's deliberately plain `INSERT`.
    ///
    /// `confirmed` is the set of index ids the CALLER has seen stuck on two
    /// consecutive passes, and only those are recovered. This is per index on
    /// purpose: an early version gated only the decision to sweep, and then
    /// recovered every currently-unregistered `CREATING` index, so one genuinely
    /// stuck index could drag a just-created sibling (still inside its
    /// commit-to-register window) into a rebuild while its live task was also
    /// backfilling, double-populating the data table. The registry is re-checked
    /// here per index as well, so an id whose task registered since the caller's
    /// last pass is skipped even when named.
    pub(crate) async fn recover_stuck_vector_builds(
        &self,
        confirmed: &std::collections::HashSet<String>,
    ) -> Result<usize, StorageError> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT v.index_id, v.table_id, t.key_schema, t.attribute_definitions \
             FROM vector_indexes v JOIN tables t ON v.table_id = t.table_id \
             WHERE v.index_status = 'CREATING'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut recovered = 0usize;
        for (index_id, table_id, base_ks_json, base_ad_json) in rows {
            if !confirmed.contains(&index_id) {
                continue;
            }
            let alive = self
                .vector_builds_running
                .lock()
                .map(|set| set.contains(&index_id))
                .unwrap_or(true);
            if alive {
                continue;
            }
            let written = self
                .rebuild_one_vector_index(&index_id, &table_id, &base_ks_json, &base_ad_json)
                .await?;
            recovered += 1;
            tracing::warn!(
                vectors_indexed = written,
                "Recovered vector index {index_id} on table {table_id}: it was CREATING \
                 with no live backfill task"
            );
        }
        if recovered > 0 {
            // Writes held while the index was CREATING are claimable now.
            self.gsi_notify.notify_waiters();
        }
        Ok(recovered)
    }

    /// Drop, recreate, backfill, and flip one vector index to `ACTIVE`.
    ///
    /// The shared body of startup reconciliation and the worker's runtime
    /// recovery, factored so the two repairs cannot drift. The backfill runs on
    /// the batched path, releasing the write lock between batches, so a recovery
    /// on a large table cannot become a write-availability outage for every
    /// other table.
    async fn rebuild_one_vector_index(
        &self,
        index_id: &str,
        table_id: &str,
        base_ks_json: &str,
        base_ad_json: &str,
    ) -> Result<usize, StorageError> {
        let base_key_schema: Vec<KeySchemaElement> = serde_json::from_str(base_ks_json)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs: Vec<AttributeDefinition> = serde_json::from_str(base_ad_json)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Drop and recreate under the lock, then backfill BATCHED, releasing the
        // lock between batches exactly as the normal create path does. An earlier
        // version ran the whole backfill in one lock-held transaction, which on a
        // large table blocked writes to EVERY table for the full rebuild: a
        // write-availability outage as the price of recovering one index. The
        // batched path is safe here for the same reasons it is safe on create:
        // the index is CREATING throughout, so the worker holds this table's
        // queue rows and searches are refused, and the rowid cursor tolerates
        // concurrent base-table writes. Recovery uses no batch delay: the lever
        // exists for tests, and recovery should finish as fast as batching
        // allows.
        let meta;
        {
            let _writer = self.write_lock.lock().await;
            Self::drop_vector_data_table_by_id(&self.pool, table_id, index_id).await?;

            let mut data_tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Self::create_vector_data_table(
                &mut data_tx,
                table_id,
                index_id,
                &base_key_schema,
                &attr_defs,
            )
            .await?;
            // The definition is read from the catalog rather than reconstructed,
            // because the request that created it is long gone.
            meta = crate::data::vector_index::fetch_vector_indexes_for_table(
                &mut data_tx,
                table_id,
            )
            .await?
            .into_iter()
            .find(|m| m.index_id == index_id)
            .ok_or_else(|| {
                StorageError::Internal(format!(
                    "vector index {index_id} was selected as CREATING but has no catalog row"
                ))
            })?;
            data_tx
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        let outcome = crate::data::vector_index::backfill_vector_index_in_batches(
            &self.pool,
            &self.write_lock,
            table_id,
            &meta,
            &base_key_schema,
            &attr_defs,
            std::time::Duration::ZERO,
        )
        .await?;

        sqlx::query(
            "UPDATE vector_indexes SET index_status = 'ACTIVE', backfilling = NULL, \
             skipped_item_count = ? \
             WHERE table_id = ? AND index_id = ?",
        )
        .bind(i64::try_from(outcome.skipped).unwrap_or(i64::MAX))
        .bind(table_id)
        .bind(index_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(outcome.written)
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

    /// A vector index left in `CREATING` by a crash must be rebuilt on startup and
    /// flipped to `ACTIVE`, with `Backfilling` cleared.
    ///
    /// Asserts the rebuilt data table is POPULATED, not merely that the status
    /// changed. Flipping the row to `ACTIVE` over an empty or missing data table
    /// would satisfy a status-only assertion while every search returned nothing,
    /// which is the exact failure the reconciler exists to prevent.
    #[tokio::test]
    async fn reconcile_rebuilds_a_creating_vector_index_and_populates_it() {
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

        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
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

        // Two items already in the base table, so a rebuild has something to find.
        let base_table = crate::data::data_table_name(&table_id);
        for (pk, vec) in [("a", "[1,0]"), ("b", "[0,1]")] {
            let item = format!(
                r#"{{"pk":{{"S":"{pk}"}},"emb":{{"L":[{{"N":"{}"}},{{"N":"{}"}}]}}}}"#,
                if vec == "[1,0]" { 1 } else { 0 },
                if vec == "[1,0]" { 0 } else { 1 }
            );
            sqlx::query(&format!(
                "INSERT INTO {base_table} (pk, item_data) VALUES (?, ?)"
            ))
            .bind(pk)
            .bind(&item)
            .execute(&engine.pool)
            .await
            .expect("seed item");
        }

        // Simulate a crash mid-build: a CREATING row, mid-backfill, whose data table
        // was never created.
        sqlx::query(
            "INSERT INTO vector_indexes \
             (table_id, index_id, index_name, dimensions, distance_function, vector_attribute, \
              projection, index_status, backfilling) \
             VALUES (?, 'vidx-1', 'vidx', 2, 'COSINE', ?, ?, 'CREATING', 1)",
        )
        .bind(&table_id)
        .bind(json!({"AttributeName": "emb"}).to_string())
        .bind(json!({"ProjectionType": "ALL"}).to_string())
        .execute(&engine.pool)
        .await
        .expect("insert CREATING vector index");

        let rebuilt = engine
            .reconcile_incomplete_vector_indexes()
            .await
            .expect("reconcile");
        assert_eq!(rebuilt, 1, "one CREATING vector index should be rebuilt");

        let (status, backfilling): (String, Option<i64>) = sqlx::query_as(
            "SELECT index_status, backfilling FROM vector_indexes \
             WHERE table_id = ? AND index_id = 'vidx-1'",
        )
        .bind(&table_id)
        .fetch_one(&engine.pool)
        .await
        .expect("status");
        assert_eq!(status, "ACTIVE");
        assert_eq!(
            backfilling, None,
            "an ACTIVE index must not carry the Backfilling member"
        );

        // The rebuilt table must actually hold the rows.
        let vec_table = crate::data::vector_table_name(&table_id, "vidx-1");
        let (rows,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {vec_table}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count");
        assert_eq!(rows, 2, "the rebuild must backfill both seeded items");

        // Idempotent, and the second run must not duplicate the rows it already
        // wrote: the reconciler drops and rebuilds rather than appending.
        assert_eq!(
            engine
                .reconcile_incomplete_vector_indexes()
                .await
                .expect("second reconcile"),
            0,
            "reconciler must be idempotent"
        );
        let (rows_after,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {vec_table}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count again");
        assert_eq!(rows_after, 2, "a second pass must not duplicate rows");
    }

    /// The crash that actually happens: the data table exists and holds SOME of the
    /// rows, because the process died partway through the backfill.
    ///
    /// The reconciler must drop and rebuild rather than resume, or the rows already
    /// written are written again and a search returns the same item twice. The
    /// previous test cannot catch this: its simulated crash leaves no data table at
    /// all, so the drop is a no-op there.
    #[tokio::test]
    async fn reconcile_rebuilds_a_partially_backfilled_vector_index_without_duplicating() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind("000000000000")
            .execute(&engine.pool)
            .await
            .expect("account");
        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl("000000000000", input)
            .await
            .expect("create table");
        let (table_id,): (String,) =
            sqlx::query_as("SELECT table_id FROM tables WHERE table_name = 't'")
                .fetch_one(&engine.pool)
                .await
                .expect("table_id");

        let base_table = crate::data::data_table_name(&table_id);
        for pk in ["a", "b"] {
            sqlx::query(&format!(
                "INSERT INTO {base_table} (pk, item_data) VALUES (?, ?)"
            ))
            .bind(pk)
            .bind(format!(
                r#"{{"pk":{{"S":"{pk}"}},"emb":{{"L":[{{"N":"1"}},{{"N":"0"}}]}}}}"#
            ))
            .execute(&engine.pool)
            .await
            .expect("seed");
        }

        sqlx::query(
            "INSERT INTO vector_indexes \
             (table_id, index_id, index_name, dimensions, distance_function, vector_attribute, \
              projection, index_status, backfilling) \
             VALUES (?, 'vidx-2', 'vidx', 2, 'COSINE', ?, ?, 'CREATING', 1)",
        )
        .bind(&table_id)
        .bind(json!({"AttributeName": "emb"}).to_string())
        .bind(json!({"ProjectionType": "ALL"}).to_string())
        .execute(&engine.pool)
        .await
        .expect("insert CREATING");

        // The partial state: the data table exists and already holds one of the two
        // rows, exactly as a crash midway through the scan would leave it.
        let ks = vec![extenddb_core::types::KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: extenddb_core::types::KeyType::Hash,
        }];
        let ad = vec![extenddb_core::types::AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: extenddb_core::types::ScalarAttributeType::S,
        }];
        let mut tx = engine.pool.begin_with("BEGIN IMMEDIATE").await.expect("tx");
        SqliteEngine::create_vector_data_table(&mut tx, &table_id, "vidx-2", &ks, &ad)
            .await
            .expect("create partial data table");
        let meta = crate::data::vector_index::fetch_vector_indexes_for_table(&mut tx, &table_id)
            .await
            .expect("metas")
            .into_iter()
            .find(|m| m.index_id == "vidx-2")
            .expect("meta");
        let item: extenddb_core::types::Item =
            serde_json::from_str(r#"{"pk":{"S":"a"},"emb":{"L":[{"N":"1"},{"N":"0"}]}}"#)
                .expect("item");
        crate::data::vector_index::insert_vector_row(
            &mut tx,
            &table_id,
            &meta,
            &item,
            &ks,
            &ad,
            &["base_pk".to_owned()],
            crate::data::vector_index::VectorRowConflict::Fail,
        )
        .await
        .expect("partial row");
        tx.commit().await.expect("commit");

        let vec_table = crate::data::vector_table_name(&table_id, "vidx-2");
        let (before,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {vec_table}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count before");
        assert_eq!(before, 1, "the setup must leave a genuinely partial table");

        engine
            .reconcile_incomplete_vector_indexes()
            .await
            .expect("reconcile");

        let (after,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {vec_table}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count after");
        assert_eq!(
            after, 2,
            "the rebuild must drop the partial table, not append to it: 3 rows would mean \
             item 'a' was indexed twice and a search would return it twice"
        );
    }

    /// A CREATING vector index with no live backfill task must be recovered at
    /// runtime, not just at startup.
    ///
    /// The failure this guards: the detached backfill task dies (panic, or its
    /// terminal ACTIVE flip fails) and the index sits in CREATING forever. The
    /// worker holds every queued index write for the table while any of its
    /// vector indexes is CREATING, so without runtime recovery one dead build
    /// wedges ALL asynchronous index maintenance for the table until a restart.
    /// The orphan is simulated exactly as the reconciler tests simulate a crash:
    /// a CREATING catalog row with no task, which is indistinguishable from the
    /// real thing because a dead task leaves nothing else behind.
    #[tokio::test]
    async fn a_creating_index_with_no_live_build_task_is_recovered_at_runtime() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind("000000000000")
            .execute(&engine.pool)
            .await
            .expect("account");
        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl("000000000000", input)
            .await
            .expect("create table");
        let (table_id,): (String,) =
            sqlx::query_as("SELECT table_id FROM tables WHERE table_name = 't'")
                .fetch_one(&engine.pool)
                .await
                .expect("table_id");

        let base_table = crate::data::data_table_name(&table_id);
        sqlx::query(&format!(
            "INSERT INTO {base_table} (pk, item_data) VALUES ('a', ?)"
        ))
        .bind(r#"{"pk":{"S":"a"},"emb":{"L":[{"N":"1"},{"N":"0"}]}}"#)
        .execute(&engine.pool)
        .await
        .expect("seed");

        sqlx::query(
            "INSERT INTO vector_indexes \
             (table_id, index_id, index_name, dimensions, distance_function, vector_attribute, \
              projection, index_status, backfilling) \
             VALUES (?, 'vidx-dead', 'vidx', 2, 'COSINE', ?, ?, 'CREATING', 1)",
        )
        .bind(&table_id)
        .bind(json!({"AttributeName": "emb"}).to_string())
        .bind(json!({"ProjectionType": "ALL"}).to_string())
        .execute(&engine.pool)
        .await
        .expect("insert orphaned CREATING index");

        // The probe must name it, and recovery must repair it.
        let candidates = engine
            .stuck_vector_build_candidates()
            .await
            .expect("candidates");
        assert_eq!(candidates, vec!["vidx-dead".to_owned()]);
        let confirmed: std::collections::HashSet<String> = candidates.iter().cloned().collect();
        let recovered = engine
            .recover_stuck_vector_builds(&confirmed)
            .await
            .expect("recover");
        assert_eq!(recovered, 1, "the orphaned build must be recovered");

        let (status, backfilling): (String, Option<i64>) = sqlx::query_as(
            "SELECT index_status, backfilling FROM vector_indexes WHERE index_id = 'vidx-dead'",
        )
        .fetch_one(&engine.pool)
        .await
        .expect("status");
        assert_eq!(status, "ACTIVE");
        assert_eq!(backfilling, None);

        // Recovered means populated, not merely flipped: the seeded row must be
        // in the rebuilt index, or the "recovery" published an empty index.
        let vec_table = crate::data::vector_table_name(&table_id, "vidx-dead");
        let (rows,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {vec_table}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count");
        assert_eq!(rows, 1, "recovery must backfill the seeded row");
    }

    /// The discriminating control for the sweep: a CREATING index whose build IS
    /// registered as alive must be left alone. Without this the previous test
    /// would also pass for a sweep that rebuilds every CREATING index it sees,
    /// which would corrupt a healthy in-flight build by dropping its data table
    /// out from under the running backfill.
    #[tokio::test]
    async fn a_creating_index_with_a_live_build_task_is_left_alone() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind("000000000000")
            .execute(&engine.pool)
            .await
            .expect("account");
        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl("000000000000", input)
            .await
            .expect("create table");
        let (table_id,): (String,) =
            sqlx::query_as("SELECT table_id FROM tables WHERE table_name = 't'")
                .fetch_one(&engine.pool)
                .await
                .expect("table_id");

        sqlx::query(
            "INSERT INTO vector_indexes \
             (table_id, index_id, index_name, dimensions, distance_function, vector_attribute, \
              projection, index_status, backfilling) \
             VALUES (?, 'vidx-live', 'vidx', 2, 'COSINE', ?, ?, 'CREATING', 1)",
        )
        .bind(&table_id)
        .bind(json!({"AttributeName": "emb"}).to_string())
        .bind(json!({"ProjectionType": "ALL"}).to_string())
        .execute(&engine.pool)
        .await
        .expect("insert CREATING index");

        // The build is alive: exactly what build_vector_index records before it
        // spawns the task.
        engine
            .vector_builds_running
            .lock()
            .expect("registry")
            .insert("vidx-live".to_owned());

        assert!(
            engine
                .stuck_vector_build_candidates()
                .await
                .expect("candidates")
                .is_empty(),
            "a registered build must not be a candidate"
        );
        // Named explicitly as confirmed-stuck, so the registry re-check alone
        // must protect it: the strongest form of the control.
        let confirmed: std::collections::HashSet<String> =
            std::iter::once("vidx-live".to_owned()).collect();
        assert_eq!(
            engine
                .recover_stuck_vector_builds(&confirmed)
                .await
                .expect("recover"),
            0,
            "a registered build must not be recovered"
        );
        let (status,): (String,) =
            sqlx::query_as("SELECT index_status FROM vector_indexes WHERE index_id = 'vidx-live'")
                .fetch_one(&engine.pool)
                .await
                .expect("status");
        assert_eq!(status, "CREATING", "the live build must be untouched");
    }
}
