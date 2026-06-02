// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `create_table` implementation for `TidbEngine`.

use extenddb_core::types::{
    BillingMode, BillingModeSummary, CreateTableInput, GsiDescription, GsiInput, KeySchemaElement,
    LsiDescription, LsiInput, Projection, ProvisionedThroughputDescription, TableDescription,
    TableStatus, Tag,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn, table_arn};
use sqlx::{MySql, QueryBuilder};

use crate::TidbEngine;
use crate::data::validate_native_key_schema_shape;
use crate::stream_engine::StreamGenerationCatalog;
use crate::throughput::provisioned_throughput_description;
use crate::tidb_util::is_unique_violation;

enum SecondaryIndexCreateRef<'a> {
    Global(&'a GsiInput),
    Local(&'a LsiInput),
}

impl SecondaryIndexCreateRef<'_> {
    fn api_type(&self) -> &'static str {
        match self {
            Self::Global(_) => "GSI",
            Self::Local(_) => "LSI",
        }
    }

    fn index_name(&self) -> &str {
        match self {
            Self::Global(index) => &index.index_name,
            Self::Local(index) => &index.index_name,
        }
    }

    fn key_schema(&self) -> &[KeySchemaElement] {
        match self {
            Self::Global(index) => &index.key_schema,
            Self::Local(index) => &index.key_schema,
        }
    }

    fn projection(&self) -> &Projection {
        match self {
            Self::Global(index) => &index.projection,
            Self::Local(index) => &index.projection,
        }
    }

    fn provisioned_throughput_description(&self) -> Option<ProvisionedThroughputDescription> {
        match self {
            Self::Global(index) => index
                .provisioned_throughput
                .as_ref()
                .map(provisioned_throughput_description),
            Self::Local(_) => None,
        }
    }
}

struct SecondaryIndexCatalogRow {
    index_name: String,
    index_id: String,
    index_type: &'static str,
    key_schema: serde_json::Value,
    projection: serde_json::Value,
    provisioned_throughput: Option<serde_json::Value>,
}

fn secondary_index_catalog_rows(
    input: &CreateTableInput,
) -> Result<Vec<SecondaryIndexCatalogRow>, StorageError> {
    let global_indexes = input
        .global_secondary_indexes
        .iter()
        .flatten()
        .map(SecondaryIndexCreateRef::Global);
    let local_indexes = input
        .local_secondary_indexes
        .iter()
        .flatten()
        .map(SecondaryIndexCreateRef::Local);

    global_indexes
        .chain(local_indexes)
        .map(|index| {
            let key_schema = serde_json::to_value(index.key_schema())
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let projection = serde_json::to_value(index.projection())
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let provisioned_throughput_description = index.provisioned_throughput_description();
            let provisioned_throughput = provisioned_throughput_description
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(SecondaryIndexCatalogRow {
                index_name: index.index_name().to_owned(),
                index_id: uuid::Uuid::new_v4().to_string(),
                index_type: index.api_type(),
                key_schema,
                projection,
                provisioned_throughput,
            })
        })
        .collect()
}

async fn insert_secondary_index_catalog_rows(
    tx: &mut sqlx::Transaction<'_, MySql>,
    table_id: &str,
    rows: &[SecondaryIndexCatalogRow],
) -> Result<(), StorageError> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut query = QueryBuilder::<MySql>::new(
        "INSERT INTO indexes \
         (table_id, index_name, index_id, index_type, key_schema, projection, \
          index_status, provisioned_throughput) ",
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(table_id)
            .push_bind(&row.index_name)
            .push_bind(&row.index_id)
            .push_bind(row.index_type)
            .push_bind(&row.key_schema)
            .push_bind(&row.projection)
            .push_bind("ACTIVE")
            .push_bind(&row.provisioned_throughput);
    });

    query
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

async fn insert_table_tags(
    tx: &mut sqlx::Transaction<'_, MySql>,
    table_arn: &str,
    tags: &[Tag],
) -> Result<(), StorageError> {
    if tags.is_empty() {
        return Ok(());
    }

    let mut query =
        QueryBuilder::<MySql>::new("INSERT INTO tags (resource_arn, tag_key, tag_value) ");
    query.push_values(tags, |mut values, tag| {
        values
            .push_bind(table_arn)
            .push_bind(&tag.key)
            .push_bind(&tag.value);
    });

    query
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

impl TidbEngine {
    pub(crate) async fn create_table_impl(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        validate_native_key_schema_shape("table key schema", &input.key_schema)?;
        if let Some(indexes) = &input.global_secondary_indexes {
            for index in indexes {
                validate_native_key_schema_shape(
                    &format!("global secondary index {}", index.index_name),
                    &index.key_schema,
                )?;
            }
        }
        if let Some(indexes) = &input.local_secondary_indexes {
            for index in indexes {
                validate_native_key_schema_shape(
                    &format!("local secondary index {}", index.index_name),
                    &index.key_schema,
                )?;
            }
        }
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

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Insert catalog metadata as CREATING first. TiDB owns distributed
        // online DDL scheduling; the reconciler only replays the desired state
        // immediately and idempotently from the committed catalog row.
        let now = time::OffsetDateTime::now_utc();
        let creation_epoch =
            now.unix_timestamp() as f64 + f64::from(now.nanosecond()) / 1_000_000_000.0;
        let stream_label = input
            .stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled)
            .then(Self::new_stream_label);
        let index_rows = secondary_index_catalog_rows(&input)?;

        sqlx::query(
            r"INSERT INTO tables
               (account_id, table_name, key_schema, attribute_definitions, billing_mode,
                provisioned_throughput, stream_specification, table_status,
                creation_date_time, table_arn, table_id, deletion_protection_enabled,
                status_transition_at, stream_label)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(6), ?, ?, ?,
                       CURRENT_TIMESTAMP(6), ?)",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .bind(&key_schema_json)
        .bind(&attr_defs_json)
        .bind(billing_str)
        .bind(&pt_json)
        .bind(&stream_json)
        .bind("CREATING")
        .bind(&table_arn)
        .bind(&table_id)
        .bind(deletion_protection)
        .bind(&stream_label)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                StorageError::TableAlreadyExists(input.table_name.clone())
            } else {
                StorageError::Internal(e.to_string())
            }
        })?;

        // TiDB has one physical secondary-index mechanism. The GSI/LSI split is
        // DynamoDB API metadata, not a separate storage path.
        insert_secondary_index_catalog_rows(&mut tx, &table_id, &index_rows).await?;

        if let (Some(label), Some(spec_json)) = (&stream_label, &stream_json) {
            Self::upsert_enabled_stream_generation_in_tx(
                &mut tx,
                StreamGenerationCatalog {
                    account_id,
                    table_name: &input.table_name,
                    table_id: &table_id,
                    stream_label: label,
                    key_schema: &key_schema_json,
                    stream_specification: spec_json,
                },
            )
            .await?;
        }

        if let Some(tags) = &input.tags {
            insert_table_tags(&mut tx, &table_arn, tags).await?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let response_status = TableStatus::Creating;

        // Wake the control plane poller so it processes the CREATING → ACTIVE
        // transition without waiting for the idle timeout.
        // If the server crashes between commit and notify, the startup recovery
        // and defensive sweep recover the transition.
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
