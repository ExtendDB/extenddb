// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `create_table` implementation for `SqliteEngine`.

use extenddb_core::types::{
    BillingMode, BillingModeSummary, CreateTableInput, GsiDescription, LsiDescription,
    ProvisionedThroughputDescription, TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn, table_arn};

use crate::engine::SqliteEngine;
use crate::sqlite_util::format_timestamp;

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
        let key_schema_json = serde_json::to_string(&input.key_schema)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs_json = serde_json::to_string(&input.attribute_definitions)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let billing_str = match billing_mode {
            BillingMode::Provisioned => "PROVISIONED",
            BillingMode::PayPerRequest => "PAY_PER_REQUEST",
        };
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
        let deletion_protection = input.deletion_protection_enabled.unwrap_or(false);

        // Read control plane delay before starting the transaction.
        let delay_row: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM settings WHERE key = 'control_plane_delay_seconds'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let delay_secs: f64 = delay_row
            .and_then(|(v,)| v.parse::<f64>().ok())
            .unwrap_or(0.25);

        let initial_status = if delay_secs == 0.0 { "ACTIVE" } else { "CREATING" };

        let now = time::OffsetDateTime::now_utc();
        let creation_ts = format_timestamp(now);
        let creation_epoch = now.unix_timestamp() as f64;

        let transition_at = if delay_secs == 0.0 {
            None
        } else {
            let secs = delay_secs as i64;
            Some(format!(
                "datetime('now', '+{secs} seconds')"
            ))
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Insert with or without transition_at using a computed SQL string.
        let insert_sql = if transition_at.is_some() {
            "INSERT INTO tables \
              (account_id, table_name, key_schema, attribute_definitions, billing_mode, \
               provisioned_throughput, stream_specification, table_status, \
               creation_date_time, table_arn, table_id, deletion_protection_enabled, \
               status_transition_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
               datetime('now', '+' || (SELECT value FROM settings WHERE key = 'control_plane_delay_seconds') || ' seconds'))"
        } else {
            "INSERT INTO tables \
              (account_id, table_name, key_schema, attribute_definitions, billing_mode, \
               provisioned_throughput, stream_specification, table_status, \
               creation_date_time, table_arn, table_id, deletion_protection_enabled) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        };

        sqlx::query(insert_sql)
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
            .execute(&mut *tx)
            .await
            .map_err(|e| match &e {
                sqlx::Error::Database(db_err)
                    if db_err.message().contains("UNIQUE constraint failed") =>
                {
                    StorageError::TableAlreadyExists(input.table_name.clone())
                }
                _ => StorageError::Internal(e.to_string()),
            })?;

        // Insert GSI metadata
        let mut gsi_index_ids: Vec<String> = Vec::new();
        if let Some(gsis) = &input.global_secondary_indexes {
            for gsi in gsis {
                let gsi_ks = serde_json::to_string(&gsi.key_schema)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let gsi_proj = serde_json::to_string(&gsi.projection)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let gsi_pt = gsi
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
                let lsi_ks = serde_json::to_string(&lsi.key_schema)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let lsi_proj = serde_json::to_string(&lsi.projection)
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
                    "INSERT INTO tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?)",
                )
                .bind(&table_arn)
                .bind(&tag.key)
                .bind(&tag.value)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        // Initialize stream shards if streams are enabled.
        let stream_label = if input
            .stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled)
        {
            let label = Self::init_stream_shards(
                &mut tx,
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

        // Create data tables in a separate transaction after catalog commit.
        let data_ddl_result = async {
            let mut data_tx = self
                .pool
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
            tracing::error!(
                "Failed to create data tables for '{}', cleaning up catalog: {e}",
                input.table_name,
            );
            let _ = sqlx::query("DELETE FROM tables WHERE account_id = ? AND table_name = ?")
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&self.pool)
                .await;
            return Err(e);
        }

        self.control_plane_notify.notify_one();

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

        let response_status = if initial_status == "ACTIVE" {
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
            sse_description: None,
            table_class_summary: None,
        })
    }
}
