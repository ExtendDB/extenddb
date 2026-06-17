// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `create_table` implementation for `PostgresEngine`.

use extenddb_core::types::{
    BillingMode, BillingModeSummary, CreateTableInput, GsiDescription, LsiDescription,
    ProvisionedThroughputDescription, SseDescription, SseType, TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn, table_arn};

use crate::PostgresEngine;

impl PostgresEngine {
    /// Core implementation of `create_table` (Fix #4: wrapped in a transaction).
    pub(crate) async fn create_table_impl(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        let table_id = uuid::Uuid::new_v4().to_string();
        let table_arn = table_arn(&self.region, account_id, &input.table_name);
        let billing_mode = input.billing_mode.unwrap_or(BillingMode::Provisioned);
        let key_schema_json = serde_json::to_value(&input.key_schema)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs_json = serde_json::to_value(&input.attribute_definitions)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let billing_str = match billing_mode {
            BillingMode::Provisioned => "PROVISIONED",
            BillingMode::PayPerRequest => "PAY_PER_REQUEST",
        };
        // Fix #7: Use serde_json::to_value directly instead of redundant closures
        let pt_json = input
            .provisioned_throughput
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let stream_json = input
            .stream_specification
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let deletion_protection = input.deletion_protection_enabled.unwrap_or(false);
        let sse_spec_json = input.sse_specification.clone();
        let on_demand_json = input
            .on_demand_throughput
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Insert table metadata, returning creation timestamp and actual status.
        // Use PG error code 23505 for robust duplicate detection instead of string matching.
        // H-5: Insert as CREATING with a scheduled transition to ACTIVE,
        // or directly as ACTIVE when control_plane_delay_seconds=0 (no async
        // transition needed). This lets external test suites that don't call
        // waitForActive() work correctly.
        let (creation_epoch, actual_status): (f64, String) = sqlx::query_as(
            r"WITH delay AS (
                SELECT COALESCE(
                  (SELECT value::FLOAT8 FROM settings WHERE key = 'control_plane_delay_seconds'), 0.25
                ) AS secs
              )
              INSERT INTO tables
               (account_id, table_name, key_schema, attribute_definitions, billing_mode,
                provisioned_throughput, stream_specification, table_status,
                creation_date_time, table_arn, table_id, deletion_protection_enabled,
                status_transition_at, table_class, sse_specification, on_demand_throughput)
               VALUES ($1, $2, $3, $4, $5, $6, $7,
                CASE WHEN (SELECT secs FROM delay) = 0
                     THEN 'ACTIVE' ELSE 'CREATING' END,
                NOW(), $8, $9, $10,
                CASE WHEN (SELECT secs FROM delay) = 0
                     THEN NULL
                     ELSE NOW() + make_interval(secs => (SELECT secs FROM delay))
                END,
                $11, $12, $13)
               RETURNING EXTRACT(EPOCH FROM creation_date_time)::FLOAT8, table_status",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .bind(&key_schema_json)
        .bind(&attr_defs_json)
        .bind(billing_str)
        .bind(&pt_json)
        .bind(&stream_json)
        .bind(&table_arn)
        .bind(&table_id)
        .bind(deletion_protection)
        .bind(&input.table_class)
        .bind(&sse_spec_json)
        .bind(&on_demand_json)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| match &e {
            sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("23505") => {
                StorageError::TableAlreadyExists(input.table_name.clone())
            }
            _ => StorageError::Internal(e.to_string()),
        })?;

        // Insert GSI metadata
        // F-1: Store full ProvisionedThroughputDescription (not the input
        // ProvisionedThroughput) so DescribeTable can deserialize it without
        // failing on the missing NumberOfDecreasesToday field.
        let mut gsi_index_ids: Vec<String> = Vec::new();
        if let Some(gsis) = &input.global_secondary_indexes {
            for gsi in gsis {
                let gsi_ks = serde_json::to_value(&gsi.key_schema)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let gsi_proj = serde_json::to_value(&gsi.projection)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let gsi_pt = gsi
                    .provisioned_throughput
                    .as_ref()
                    .map(|pt| {
                        serde_json::to_value(ProvisionedThroughputDescription {
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
                    r"INSERT INTO indexes
                       (table_id, index_name, index_id, index_type, key_schema, projection,
                        index_status, provisioned_throughput)
                       VALUES ($1, $2, $3, 'GSI', $4, $5, 'ACTIVE', $6)",
                )
                .bind(&table_id)
                .bind(&gsi.index_name)
                .bind(&index_id)
                .bind(&gsi_ks)
                .bind(&gsi_proj)
                .bind(&gsi_pt)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                gsi_index_ids.push(index_id);
            }
        }

        // Insert LSI metadata
        let mut lsi_index_ids: Vec<String> = Vec::new();
        if let Some(lsis) = &input.local_secondary_indexes {
            for lsi in lsis {
                let lsi_ks = serde_json::to_value(&lsi.key_schema)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let lsi_proj = serde_json::to_value(&lsi.projection)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                let index_id = uuid::Uuid::new_v4().to_string();
                sqlx::query(
                    r"INSERT INTO indexes
                       (table_id, index_name, index_id, index_type, key_schema, projection,
                        index_status, provisioned_throughput)
                       VALUES ($1, $2, $3, 'LSI', $4, $5, 'ACTIVE', NULL)",
                )
                .bind(&table_id)
                .bind(&lsi.index_name)
                .bind(&index_id)
                .bind(&lsi_ks)
                .bind(&lsi_proj)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
                lsi_index_ids.push(index_id);
            }
        }

        // Insert tags
        if let Some(tags) = &input.tags {
            for tag in tags {
                sqlx::query(
                    "INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES ($1, $2, $3)",
                )
                .bind(&table_arn)
                .bind(&tag.key)
                .bind(&tag.value)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        // Create the per-DynamoDB-table data table for item storage.
        // P54 Bug 1: Data tables live in the data database, not the catalog.
        // Commit catalog metadata first, then create data tables on data_pool.
        // If data DDL fails, the catalog entry is cleaned up (see below).

        // Initialize stream shards and label if streams are enabled on this table.
        let stream_label = if input
            .stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled)
        {
            let label = Self::init_stream_shards(
                &mut tx,
                &self.data_pool,
                account_id,
                &input.table_name,
                &table_id,
            )
            .await?;
            Some(label)
        } else {
            None
        };

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // P54 Bug 1: Create data tables on the data pool after catalog commit.
        let data_ddl_result = async {
            let mut data_tx = self
                .data_pool
                .begin()
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
                        &gsi_index_ids[i],
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
                        &lsi_index_ids[i],
                        &lsi.key_schema,
                        &input.attribute_definitions,
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

        if let Err(e) = data_ddl_result {
            // Data table creation failed. Clean up the catalog entry so the
            // table name is not permanently stuck in CREATING state.
            tracing::error!(
                "Failed to create data tables for '{}', cleaning up catalog: {e}",
                input.table_name,
            );
            let _ = sqlx::query("DELETE FROM tables WHERE account_id = $1 AND table_name = $2")
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&self.pool)
                .await;
            return Err(e);
        }

        // F-3: Wake the control plane poller so it processes the CREATING →
        // ACTIVE transition without waiting for the idle timeout.
        // If the server crashes between commit and notify, the 60s defensive
        // sweep recovers the transition.
        self.control_plane_notify.notify_one();

        // Build response from in-scope data — avoids post-commit read race
        // (another request could delete the table between commit and read).
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

        let billing_mode_summary = if billing_mode == BillingMode::PayPerRequest {
            Some(BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: Some(creation_epoch),
            })
        } else {
            None
        };

        let latest_stream_arn = stream_label
            .as_ref()
            .map(|label| stream_arn(&self.region, account_id, &input.table_name, label));

        let response_status = if actual_status == "ACTIVE" {
            TableStatus::Active
        } else {
            TableStatus::Creating
        };

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
            deletion_protection_enabled: input.deletion_protection_enabled.unwrap_or(false),
            sse_description: input.sse_specification.as_ref().and_then(|spec| {
                let enabled = spec
                    .get("Enabled")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                if enabled {
                    Some(SseDescription {
                        status: "ENABLED".to_string(),
                        sse_type: Some(SseType::KMS),
                        kms_master_key_arn: Some(format!(
                            "arn:aws:kms:{}:{}:key/default",
                            self.region, account_id
                        )),
                    })
                } else {
                    None
                }
            }),
            table_class_summary: input
                .table_class
                .as_ref()
                .map(|tc| serde_json::json!({ "TableClass": tc })),
            on_demand_throughput: input.on_demand_throughput,
        })
    }
}
