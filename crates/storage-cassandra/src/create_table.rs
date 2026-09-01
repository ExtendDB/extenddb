// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `create_table` implementation for `CassandraEngine`.

use cdrs_tokio::types::value::Value;
use extenddb_core::types::{
    BillingMode, BillingModeSummary, CreateTableInput, GsiDescription, LsiDescription,
    ProvisionedThroughputDescription, TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn, table_arn};

use crate::CassandraEngine;

impl CassandraEngine {
    /// Initialize stream shards for a table atomically.
    ///
    /// Writes 4 shard rows into the account keyspace and updates
    /// `catalog.tables.stream_label` in a single LOGGED BATCH.
    /// Returns the stream label (ISO 8601 timestamp).
    pub(crate) async fn init_stream_shards(
        &self,
        account_id: &str,
        table_name: &str,
        account_keyspace: &str,
        table_id: &str,
    ) -> Result<String, StorageError> {
        let label = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        let catalog_keyspace = self.catalog_keyspace();
        let now_ms = chrono::Utc::now().timestamp_millis();

        let mut statements = Vec::new();

        // Update stream_label in catalog
        statements.push(format!(
            "UPDATE {catalog_keyspace}.tables SET stream_label = '{label}' \
             WHERE account_id = '{account_id}' AND table_name = '{table_name}'"
        ));

        // Insert 4 shard rows into account keyspace
        for i in 0..crate::stream_util::SHARDS_PER_STREAM {
            let shard_id = format!("shardId-{table_id}-{i:012}");
            statements.push(format!(
                "INSERT INTO {account_keyspace}.stream_shards \
                 (shard_id, table_id, starting_sequence_number, created_at) \
                 VALUES ('{shard_id}', '{table_id}', '{}', {now_ms})",
                crate::stream_util::ZERO_SEQUENCE
            ));
        }

        let batch = format!("BEGIN BATCH\n{}\nAPPLY BATCH", statements.join(";\n"));
        // Note: values are interpolated rather than bound because Cassandra LOGGED BATCH
        // does not support parameterized statements spanning multiple tables.
        // All interpolated values are server-generated (UUIDs, timestamps, label from chrono).
        self.session.query(&batch).await.map_err(|e| {
            tracing::error!("init_stream_shards batch: {e}");
            StorageError::Internal(format!("Failed to initialize stream shards: {e}"))
        })?;

        Ok(label)
    }

    /// Core implementation of `create_table` with the normal control-plane transition.
    pub(crate) async fn create_table_impl(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> Result<TableDescription, StorageError> {
        self.create_table_impl_inner(account_id, input, true).await
    }

    /// Create an inaccessible restore target that must be activated explicitly
    /// after all backup items have been copied and verified.
    pub(crate) async fn create_table_for_restore_impl(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> Result<TableDescription, StorageError> {
        self.create_table_impl_inner(account_id, input, false).await
    }

    async fn create_table_impl_inner(
        &self,
        account_id: &str,
        input: CreateTableInput,
        schedule_activation: bool,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        let table_id = uuid::Uuid::new_v4().to_string();
        let table_arn = table_arn(&self.region, account_id, &input.table_name);
        let billing_mode = input.billing_mode.unwrap_or(BillingMode::Provisioned);

        // Serialize key_schema and attribute_definitions to JSON
        let key_schema_json = serde_json::to_value(&input.key_schema)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs_json = serde_json::to_value(&input.attribute_definitions)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let billing_str = match billing_mode {
            BillingMode::Provisioned => "PROVISIONED",
            BillingMode::PayPerRequest => "PAY_PER_REQUEST",
        };

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

        // Read control_plane_delay_seconds from settings
        let catalog_keyspace = self.catalog_keyspace();
        let delay_query = format!("SELECT value FROM {catalog_keyspace}.settings WHERE key = ?");
        let delay_seconds: f64 = self
            .session
            .query_with_values(
                &delay_query,
                cdrs_tokio::query_values!("control_plane_delay_seconds"),
            )
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .and_then(|rows| rows.first().cloned())
            .and_then(|row| {
                use cdrs_tokio::types::IntoRustByName;
                let value: String = row.get_r_by_name("value").ok()?;
                value.parse::<f64>().ok()
            })
            .unwrap_or(0.25);

        // Restore targets have no generic transition deadline; only the restore
        // path may publish them after payload verification.
        let (initial_status, status_transition_at) = if !schedule_activation {
            ("CREATING", None)
        } else if delay_seconds == 0.0 {
            ("ACTIVE", None)
        } else {
            #[allow(clippy::cast_possible_truncation)]
            let delay_ms = (delay_seconds * 1000.0) as i64;
            let transition_at = chrono::Utc::now() + chrono::Duration::milliseconds(delay_ms);
            ("CREATING", Some(transition_at.timestamp_millis()))
        };

        let creation_timestamp = chrono::Utc::now().timestamp_millis();

        // Ensure account keyspace exists
        let account_keyspace = self.account_keyspace(account_id);
        if !self.keyspace_exists(&account_keyspace).await? {
            return Err(StorageError::Internal(format!(
                "Account keyspace '{account_keyspace}' does not exist. Account must be provisioned first."
            )));
        }

        // Insert table metadata with LWT (IF NOT EXISTS)
        let insert_table_cql = format!(
            "INSERT INTO {catalog_keyspace}.tables (account_id, table_name, table_id, table_arn, key_schema, \
             attribute_definitions, billing_mode, provisioned_throughput, stream_specification, \
             table_status, created_at, deletion_protection_enabled, status_transition_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
        );

        let result = self
            .session
            .query_with_values(
                &insert_table_cql,
                cdrs_tokio::query::QueryValues::SimpleValues(vec![
                    Value::from(account_id),
                    Value::from(input.table_name.as_str()),
                    Value::from(table_id.as_str()),
                    Value::from(table_arn.as_str()),
                    Value::from(key_schema_json.to_string().as_str()),
                    Value::from(attr_defs_json.to_string().as_str()),
                    Value::from(billing_str),
                    match pt_json.as_ref() {
                        Some(v) => Value::from(v.to_string().as_str()),
                        None => Value::NotSet,
                    },
                    match stream_json.as_ref() {
                        Some(v) => Value::from(v.to_string().as_str()),
                        None => Value::NotSet,
                    },
                    Value::from(initial_status),
                    Value::from(creation_timestamp),
                    Value::from(deletion_protection),
                    Value::from(status_transition_at),
                ]),
            )
            .await
            .map_err(|e| {
                tracing::error!("create_table insert table: {e}");
                StorageError::Internal(format!("Failed to insert table: {e}"))
            })?;

        // Check if LWT succeeded
        let body = result
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Failed to get response body: {e}")))?;

        if let Some(rows) = body.into_rows()
            && let Some(row) = rows.first()
        {
            use cdrs_tokio::types::IntoRustByName;
            let applied: bool = row
                .get_r_by_name("[applied]")
                .map_err(|e| StorageError::Internal(format!("Failed to parse [applied]: {e}")))?;

            if !applied {
                return Err(StorageError::TableAlreadyExists(input.table_name.clone()));
            }
        }

        // Insert GSI metadata
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
                let insert_index_cql = format!(
                    "INSERT INTO {catalog_keyspace}.indexes (table_id, index_name, index_id, index_type, \
                     key_schema, projection, index_status, provisioned_throughput) \
                     VALUES (?, ?, ?, 'GSI', ?, ?, 'ACTIVE', ?)"
                );

                self.session
                    .query_with_values(
                        &insert_index_cql,
                        cdrs_tokio::query::QueryValues::SimpleValues(vec![
                            Value::from(table_id.as_str()),
                            Value::from(gsi.index_name.as_str()),
                            Value::from(index_id.as_str()),
                            Value::from(gsi_ks.to_string().as_str()),
                            Value::from(gsi_proj.to_string().as_str()),
                            match gsi_pt.as_ref() {
                                Some(v) => Value::from(v.to_string().as_str()),
                                None => Value::NotSet,
                            },
                        ]),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("create_table insert GSI: {e}");
                        StorageError::Internal(format!("Failed to insert GSI: {e}"))
                    })?;

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
                let insert_index_cql = format!(
                    "INSERT INTO {catalog_keyspace}.indexes (table_id, index_name, index_id, index_type, \
                     key_schema, projection, index_status, provisioned_throughput) \
                     VALUES (?, ?, ?, 'LSI', ?, ?, 'ACTIVE', ?)"
                );

                self.session
                    .query_with_values(
                        &insert_index_cql,
                        cdrs_tokio::query::QueryValues::SimpleValues(vec![
                            Value::from(table_id.as_str()),
                            Value::from(lsi.index_name.as_str()),
                            Value::from(index_id.as_str()),
                            Value::from(lsi_ks.to_string().as_str()),
                            Value::from(lsi_proj.to_string().as_str()),
                            Value::NotSet,
                        ]),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("create_table insert LSI: {e}");
                        StorageError::Internal(format!("Failed to insert LSI: {e}"))
                    })?;

                lsi_index_ids.push(index_id);
            }
        }

        // Insert tags
        if let Some(tags) = &input.tags {
            for tag in tags {
                let insert_tag_cql = format!(
                    "INSERT INTO {catalog_keyspace}.tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?)"
                );

                self.session
                    .query_with_values(
                        &insert_tag_cql,
                        cdrs_tokio::query_values!(
                            table_arn.as_str(),
                            tag.key.as_str(),
                            tag.value.as_str()
                        ),
                    )
                    .await
                    .map_err(|e| StorageError::Internal(format!("Failed to insert tag: {e}")))?;
            }
        }

        // Initialize stream shards (if enabled)
        let stream_label: Option<String> = if input
            .stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled)
        {
            Some(
                self.init_stream_shards(
                    account_id,
                    &input.table_name,
                    &account_keyspace,
                    &table_id,
                )
                .await?,
            )
        } else {
            None
        };

        // Create data table in account keyspace
        self.create_data_table(
            &account_keyspace,
            &table_id,
            &input.key_schema,
            &input.attribute_definitions,
        )
        .await?;

        // Create GSI data tables
        if let Some(gsis) = &input.global_secondary_indexes {
            for (i, gsi) in gsis.iter().enumerate() {
                self.create_index_data_table(
                    &account_keyspace,
                    &gsi_index_ids[i],
                    &gsi.key_schema,
                    &input.attribute_definitions,
                    &input.key_schema,
                    &input.attribute_definitions,
                )
                .await?;
            }
        }

        // Create LSI data tables
        if let Some(lsis) = &input.local_secondary_indexes {
            for (i, lsi) in lsis.iter().enumerate() {
                self.create_index_data_table(
                    &account_keyspace,
                    &lsi_index_ids[i],
                    &lsi.key_schema,
                    &input.attribute_definitions,
                    &input.key_schema,
                    &input.attribute_definitions,
                )
                .await?;
            }
        }

        // Build and return TableDescription
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
                last_update_to_pay_per_request_date_time: Some(
                    crate::cassandra_util::millis_to_seconds_f64(creation_timestamp),
                ),
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

        // Wake the control-plane poller only when a transition was scheduled.
        if schedule_activation && status_transition_at.is_some() {
            self.control_plane_notify.notify_one();
        }

        Ok(TableDescription {
            table_name: input.table_name,
            key_schema: input.key_schema,
            attribute_definitions: input.attribute_definitions,
            table_status: response_status,
            creation_date_time: crate::cassandra_util::millis_to_seconds_f64(creation_timestamp),
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
            on_demand_throughput: None,
            restore_summary: None,
            vector_indexes: None,
        })
    }
}
