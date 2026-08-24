// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `create_table` implementation for `SqliteEngine`.
//!
//! Catalog metadata (table row, indexes, tags, stream shards) is written in one
//! transaction; the per-table/index data tables are created in a second
//! transaction afterward, with catalog cleanup if the data DDL fails. The
//! control-plane delay (`control_plane_delay_seconds`) decides whether the
//! table starts ACTIVE (delay 0) or CREATING with a scheduled transition.

use extenddb_core::types::{
    BillingMode, BillingModeSummary, CreateTableInput, GsiDescription, LsiDescription,
    ProvisionedThroughputDescription, SseDescription, SseType, TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn, table_arn};

use crate::sqlite_util::{format_timestamp, is_unique_violation};
use crate::store::SqliteEngine;

impl SqliteEngine {
    pub(crate) async fn create_table_impl(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        let table_id = uuid::Uuid::new_v4().to_string();
        let table_arn = table_arn(&self.region, account_id, &input.table_name);
        let billing_mode = input.billing_mode.unwrap_or(BillingMode::Provisioned);
        let billing_str = match billing_mode {
            BillingMode::Provisioned => "PROVISIONED",
            BillingMode::PayPerRequest => "PAY_PER_REQUEST",
        };

        let to_str = |v: &serde_json::Value| -> Result<String, StorageError> {
            serde_json::to_string(v).map_err(|e| StorageError::Internal(e.to_string()))
        };
        let key_schema_json = serde_json::to_string(&input.key_schema)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs_json = serde_json::to_string(&input.attribute_definitions)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let pt_json = input
            .provisioned_throughput
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let stream_json = input
            .stream_specification
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let sse_json = input.sse_specification.as_ref().map(to_str).transpose()?;
        let on_demand_json = input
            .on_demand_throughput
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let deletion_protection = input.deletion_protection_enabled.unwrap_or(false);

        // Control-plane delay decides initial status and scheduled transition.
        let delay_secs: f64 = sqlx::query_scalar::<_, String>(
            "SELECT value FROM settings WHERE key = 'control_plane_delay_seconds'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.25);

        let now = time::OffsetDateTime::now_utc();
        let creation_ts = format_timestamp(now);
        #[allow(clippy::cast_precision_loss)]
        let creation_epoch = now.unix_timestamp() as f64;
        let (initial_status, status_transition_at) = if delay_secs <= 0.0 {
            ("ACTIVE", None)
        } else {
            (
                "CREATING",
                Some(format_timestamp(
                    now + time::Duration::seconds_f64(delay_secs),
                )),
            )
        };

        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        sqlx::query(
            "INSERT INTO tables \
             (account_id, table_name, key_schema, attribute_definitions, billing_mode, \
              provisioned_throughput, stream_specification, table_status, creation_date_time, \
              table_arn, table_id, deletion_protection_enabled, status_transition_at, \
              table_class, sse_specification, on_demand_throughput) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .bind(&key_schema_json)
        .bind(&attr_defs_json)
        .bind(billing_str)
        .bind(&pt_json)
        .bind(&stream_json)
        .bind(initial_status)
        .bind(&creation_ts)
        .bind(&table_arn)
        .bind(&table_id)
        .bind(deletion_protection)
        .bind(&status_transition_at)
        .bind(&input.table_class)
        .bind(&sse_json)
        .bind(&on_demand_json)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                StorageError::TableAlreadyExists(input.table_name.clone())
            } else {
                StorageError::Internal(e.to_string())
            }
        })?;

        // GSI / LSI metadata.
        let mut gsi_ids: Vec<String> = Vec::new();
        if let Some(gsis) = &input.global_secondary_indexes {
            for gsi in gsis {
                let ks = serde_json::to_string(&gsi.key_schema)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let proj = serde_json::to_string(&gsi.projection)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let pt = gsi
                    .provisioned_throughput
                    .as_ref()
                    .map(|pt| {
                        serde_json::to_string(&ProvisionedThroughputDescription {
                            read_capacity_units: pt.read_capacity_units,
                            write_capacity_units: pt.write_capacity_units,
                            number_of_decreases_today: 0,
                            last_increase_date_time: None,
                            last_decrease_date_time: None,
                        })
                    })
                    .transpose()
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let index_id = uuid::Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO indexes \
                     (table_id, index_name, index_id, index_type, key_schema, projection, \
                      index_status, provisioned_throughput) \
                     VALUES (?, ?, ?, 'GSI', ?, ?, 'ACTIVE', ?)",
                )
                .bind(&table_id)
                .bind(&gsi.index_name)
                .bind(&index_id)
                .bind(&ks)
                .bind(&proj)
                .bind(&pt)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                gsi_ids.push(index_id);
            }
        }
        let mut lsi_ids: Vec<String> = Vec::new();
        if let Some(lsis) = &input.local_secondary_indexes {
            for lsi in lsis {
                let ks = serde_json::to_string(&lsi.key_schema)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let proj = serde_json::to_string(&lsi.projection)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let index_id = uuid::Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO indexes \
                     (table_id, index_name, index_id, index_type, key_schema, projection, \
                      index_status, provisioned_throughput) \
                     VALUES (?, ?, ?, 'LSI', ?, ?, 'ACTIVE', NULL)",
                )
                .bind(&table_id)
                .bind(&lsi.index_name)
                .bind(&index_id)
                .bind(&ks)
                .bind(&proj)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                lsi_ids.push(index_id);
            }
        }

        // Vector indexes. A CreateTable's table is empty, so there is nothing to
        // backfill and no `backfilling` member is ever reported on this path.
        // The index's status tracks the TABLE's: measured against the service
        // (2026-08-21, eu-west-2, three runs polling at 250ms), an index created
        // with its table reports CREATING while the table is CREATING and
        // reaches ACTIVE in the same DescribeTable poll as the table, with no
        // observable gap in either direction. The control-plane worker flips
        // both in one pass; see `process_control_plane_transitions`.
        let mut vector_ids: Vec<String> = Vec::new();
        if let Some(vis) = &input.vector_indexes {
            for vi in vis {
                let vec_attr = serde_json::to_string(&vi.vector_attribute)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let search_schema = vi
                    .search_schema
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let proj = vi
                    .projection
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .ok_or_else(|| {
                        // Core validation requires Projection, so reaching here
                        // means the request bypassed validation rather than that
                        // the caller omitted it.
                        StorageError::Internal(
                            "vector index reached storage without a projection".to_owned(),
                        )
                    })?;
                let distance = serde_json::to_string(&vi.distance_function)
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .trim_matches('"')
                    .to_owned();
                let index_id = uuid::Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO vector_indexes \
                     (table_id, index_name, index_id, dimensions, distance_function, \
                      vector_attribute, search_schema, projection, index_status, backfilling) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                )
                .bind(&table_id)
                .bind(&vi.index_name)
                .bind(&index_id)
                .bind(i64::from(vi.dimensions))
                .bind(&distance)
                .bind(&vec_attr)
                .bind(&search_schema)
                .bind(&proj)
                .bind(initial_status)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                vector_ids.push(index_id);
            }
        }

        // Tags.
        if let Some(tags) = &input.tags {
            for tag in tags {
                sqlx::query("INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?)")
                    .bind(&table_arn)
                    .bind(&tag.key)
                    .bind(&tag.value)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        // Stream shards (same transaction — single file).
        let stream_label = if input
            .stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled)
        {
            Some(Self::init_stream_shards(&mut tx, account_id, &input.table_name, &table_id).await?)
        } else {
            None
        };

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Data tables in a second transaction; clean up catalog on failure.
        let data_result = async {
            let mut data_tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Self::create_data_table(
                &mut data_tx,
                &table_id,
                &input.key_schema,
                &input.attribute_definitions,
            )
            .await?;
            if let Some(gsis) = &input.global_secondary_indexes {
                for (i, gsi) in gsis.iter().enumerate() {
                    Self::create_index_data_table(
                        &mut data_tx,
                        &gsi_ids[i],
                        &gsi.key_schema,
                        &input.attribute_definitions,
                        &input.key_schema,
                        &input.attribute_definitions,
                    )
                    .await?;
                }
            }
            if let Some(lsis) = &input.local_secondary_indexes {
                for (i, lsi) in lsis.iter().enumerate() {
                    Self::create_index_data_table(
                        &mut data_tx,
                        &lsi_ids[i],
                        &lsi.key_schema,
                        &input.attribute_definitions,
                        &input.key_schema,
                        &input.attribute_definitions,
                    )
                    .await?;
                }
            }
            if input.vector_indexes.is_some() {
                for index_id in &vector_ids {
                    Self::create_vector_data_table(
                        &mut data_tx,
                        &table_id,
                        index_id,
                        &input.key_schema,
                        &input.attribute_definitions,
                    )
                    .await?;
                }
            }
            data_tx
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok::<(), StorageError>(())
        }
        .await;

        if let Err(e) = data_result {
            tracing::error!(
                "Failed to create data tables for '{}', cleaning up catalog: {e}",
                input.table_name
            );
            let _ = sqlx::query("DELETE FROM tables WHERE account_id = ? AND table_name = ?")
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&self.pool)
                .await;
            return Err(e);
        }

        self.control_plane_notify.notify_one();

        // Build the response from in-scope data (avoids a post-commit read race).
        let (rcu, wcu) = input.provisioned_throughput.as_ref().map_or((0, 0), |pt| {
            (pt.read_capacity_units, pt.write_capacity_units)
        });

        let gsis = input.global_secondary_indexes.as_ref().map(|gs| {
            gs.iter()
                .map(|g| GsiDescription {
                    index_name: g.index_name.clone(),
                    key_schema: g.key_schema.clone(),
                    projection: g.projection.clone(),
                    index_status: "ACTIVE".to_owned(),
                    provisioned_throughput: Some(ProvisionedThroughputDescription {
                        read_capacity_units: g
                            .provisioned_throughput
                            .as_ref()
                            .map_or(0, |pt| pt.read_capacity_units),
                        write_capacity_units: g
                            .provisioned_throughput
                            .as_ref()
                            .map_or(0, |pt| pt.write_capacity_units),
                        number_of_decreases_today: 0,
                        last_increase_date_time: None,
                        last_decrease_date_time: None,
                    }),
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: index_arn(
                        &self.region,
                        account_id,
                        &input.table_name,
                        &g.index_name,
                    ),
                })
                .collect()
        });
        let lsis = input.local_secondary_indexes.as_ref().map(|ls| {
            ls.iter()
                .map(|l| LsiDescription {
                    index_name: l.index_name.clone(),
                    key_schema: l.key_schema.clone(),
                    projection: l.projection.clone(),
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: index_arn(
                        &self.region,
                        account_id,
                        &input.table_name,
                        &l.index_name,
                    ),
                })
                .collect()
        });

        let billing_mode_summary = (billing_mode == BillingMode::PayPerRequest).then_some({
            BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: Some(creation_epoch),
            }
        });
        let latest_stream_arn = stream_label
            .as_ref()
            .map(|label| stream_arn(&self.region, account_id, &input.table_name, label));
        let response_status = if initial_status == "ACTIVE" {
            TableStatus::Active
        } else {
            TableStatus::Creating
        };
        let sse_description = input.sse_specification.as_ref().and_then(|spec| {
            let enabled = spec
                .get("Enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            enabled.then(|| SseDescription {
                status: "ENABLED".to_owned(),
                sse_type: Some(SseType::KMS),
                kms_master_key_arn: Some(format!(
                    "arn:aws:kms:{}:{}:key/default",
                    self.region, account_id
                )),
            })
        });

        // Echo the vector indexes we just created. Built from the request plus the
        // ids assigned above rather than re-read from the catalog, which would add
        // a round trip to say something already known. A CreateTable's table is
        // empty, so each index is ACTIVE with no `backfilling` member.
        let vector_index_descs: Option<Vec<extenddb_core::types::VectorIndexDescription>> = input
            .vector_indexes
            .as_ref()
            .map(|vis| {
                vis.iter()
                    .map(|vi| extenddb_core::types::VectorIndexDescription {
                        index_name: vi.index_name.clone(),
                        vector_attribute: vi.vector_attribute.clone(),
                        dimensions: vi.dimensions,
                        search_schema: vi.search_schema.clone(),
                        distance_function: vi.distance_function,
                        index_status: extenddb_core::types::IndexStatus::Active,
                        backfilling: None,
                        index_size_bytes: 0,
                        item_count: 0,
                        index_arn: extenddb_storage::util::index_arn(
                            &self.region,
                            account_id,
                            &input.table_name,
                            &vi.index_name,
                        ),
                        projection: vi.projection.clone(),
                    })
                    .collect()
            })
            .filter(|v: &Vec<_>| !v.is_empty());

        Ok(TableDescription {
            table_name: input.table_name,
            key_schema: input.key_schema,
            attribute_definitions: input.attribute_definitions,
            table_status: response_status,
            creation_date_time: creation_epoch,
            table_size_bytes: 0,
            item_count: 0,
            table_arn,
            table_id,
            provisioned_throughput: ProvisionedThroughputDescription {
                read_capacity_units: rcu,
                write_capacity_units: wcu,
                number_of_decreases_today: 0,
                last_increase_date_time: None,
                last_decrease_date_time: None,
            },
            billing_mode_summary,
            global_secondary_indexes: gsis,
            local_secondary_indexes: lsis,
            stream_specification: input.stream_specification,
            latest_stream_arn,
            latest_stream_label: stream_label,
            deletion_protection_enabled: deletion_protection,
            sse_description,
            table_class_summary: input
                .table_class
                .as_ref()
                .map(|tc| serde_json::json!({ "TableClass": tc })),
            on_demand_throughput: input.on_demand_throughput,
            // Every field is populated deliberately, with no `..Default::default()`
            // spread. This response is the complete description of what was just
            // created, so a new core field should break this site and force a
            // decision about whether create must report it, rather than silently
            // defaulting. Sites that legitimately opt out still use the spread.
            vector_indexes: vector_index_descs,
        })
    }
}
