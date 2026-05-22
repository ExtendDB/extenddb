// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper types and methods for `TableEngine` operations (SQLite backend).

use extenddb_core::types::{
    AttributeDefinition, BillingMode, BillingModeSummary, GsiDescription, KeySchemaElement,
    LsiDescription, Projection, ProvisionedThroughputDescription, TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn};

use crate::data;
use crate::engine::SqliteEngine;

/// Row type for table metadata queries (SQLite version with TEXT timestamps).
#[derive(sqlx::FromRow)]
pub(crate) struct TableRow {
    pub table_name: String,
    pub key_schema: String,
    pub attribute_definitions: String,
    pub billing_mode: String,
    pub provisioned_throughput: Option<String>,
    pub stream_specification: Option<String>,
    pub table_status: String,
    pub creation_date_time: Option<String>,
    pub table_size_bytes: i64,
    pub item_count: i64,
    pub table_arn: String,
    pub table_id: String,
    pub deletion_protection_enabled: bool,
    pub stream_label: Option<String>,
}

/// Row type for index metadata queries (SQLite version with TEXT JSON).
#[derive(sqlx::FromRow)]
pub(crate) struct IndexRow {
    pub index_name: String,
    pub index_id: String,
    pub index_type: String,
    pub key_schema: String,
    pub projection: String,
    pub index_status: String,
    pub provisioned_throughput: Option<String>,
}

impl SqliteEngine {
    /// SQL table name for a GSI data table (for use outside `data` module).
    pub(crate) fn index_table_name_static(index_id: &str) -> String {
        data::index_table_name(index_id)
    }

    /// Backfill existing items from the base table into a newly created GSI.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn backfill_gsi(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
        base_key_schema: &[KeySchemaElement],
        base_attr_defs: &[AttributeDefinition],
        projection: &Projection,
    ) -> Result<(), StorageError> {
        const BATCH_SIZE: i64 = 500;

        let base_table = data::data_table_name(table_id);
        let idx_table = data::index_table_name(index_id);

        let idx_sks = data::all_sort_key_info(index_key_schema, attr_defs);
        let base_sks = data::all_sort_key_info(base_key_schema, base_attr_defs);

        let sql = format!(
            "SELECT item_data FROM {base_table} ORDER BY pk, sk_s, sk_n, sk_b LIMIT ? OFFSET ?"
        );
        let mut offset: i64 = 0;
        loop {
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(BATCH_SIZE)
                .bind(offset)
                .fetch_all(&mut **tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if rows.is_empty() {
                break;
            }

            let batch_len = i64::from(u16::try_from(rows.len()).unwrap_or(u16::MAX));

            for (item_json,) in rows {
                let item = data::json_to_item(item_json)?;

                let has_all_keys = index_key_schema
                    .iter()
                    .all(|ks| item.contains_key(&ks.attribute_name));
                if !has_all_keys {
                    continue;
                }

                let projected = data::project_item_for_index(
                    &item,
                    index_key_schema,
                    base_key_schema,
                    projection,
                );

                data::insert_index_row_multi(
                    tx,
                    &idx_table,
                    &item,
                    &projected,
                    index_key_schema,
                    base_key_schema,
                    attr_defs,
                    &idx_sks,
                    &base_sks,
                )
                .await?;
            }

            if batch_len < BATCH_SIZE {
                break;
            }
            offset += batch_len;
        }

        Ok(())
    }

    pub(crate) async fn build_table_description(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableDescription, StorageError> {
        let row: Option<TableRow> = sqlx::query_as(
            "SELECT table_name, key_schema, attribute_definitions, billing_mode, \
              provisioned_throughput, stream_specification, table_status, \
              creation_date_time, \
              table_size_bytes, item_count, table_arn, table_id, \
              deletion_protection_enabled, stream_label \
             FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        let index_rows: Vec<IndexRow> = sqlx::query_as(
            "SELECT index_name, index_id, index_type, key_schema, projection, \
              index_status, provisioned_throughput \
             FROM indexes WHERE table_id = ?",
        )
        .bind(&row.table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        self.build_table_description_from_row(account_id, row, index_rows)
    }

    pub(crate) fn build_table_description_from_row(
        &self,
        account_id: &str,
        row: TableRow,
        index_rows: Vec<IndexRow>,
    ) -> Result<TableDescription, StorageError> {
        let mut gsis: Vec<GsiDescription> = Vec::new();
        let mut lsis: Vec<LsiDescription> = Vec::new();

        for idx in index_rows {
            let ks: Vec<KeySchemaElement> = serde_json::from_str(&idx.key_schema)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let proj: Projection = serde_json::from_str(&idx.projection)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if idx.index_type == "GSI" {
                let pt: Option<ProvisionedThroughputDescription> = idx
                    .provisioned_throughput
                    .as_deref()
                    .map(|s| {
                        serde_json::from_str::<ProvisionedThroughputDescription>(s)
                            .or_else(|_| {
                                let old: extenddb_core::types::ProvisionedThroughput =
                                    serde_json::from_str(s)?;
                                Ok(ProvisionedThroughputDescription {
                                    read_capacity_units: old.read_capacity_units,
                                    write_capacity_units: old.write_capacity_units,
                                    number_of_decreases_today: 0,
                                    last_increase_date_time: None,
                                    last_decrease_date_time: None,
                                })
                            })
                    })
                    .transpose()
                    .map_err(|e: serde_json::Error| StorageError::Internal(e.to_string()))?;

                gsis.push(GsiDescription {
                    index_name: idx.index_name.clone(),
                    key_schema: ks,
                    projection: proj,
                    index_status: idx.index_status,
                    provisioned_throughput: pt.or(Some(ProvisionedThroughputDescription {
                        read_capacity_units: 0,
                        write_capacity_units: 0,
                        number_of_decreases_today: 0,
                        last_increase_date_time: None,
                        last_decrease_date_time: None,
                    })),
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: index_arn(
                        &self.region,
                        account_id,
                        &row.table_name,
                        &idx.index_name,
                    ),
                });
            } else {
                lsis.push(LsiDescription {
                    index_name: idx.index_name.clone(),
                    key_schema: ks,
                    projection: proj,
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: index_arn(
                        &self.region,
                        account_id,
                        &row.table_name,
                        &idx.index_name,
                    ),
                });
            }
        }

        let key_schema: Vec<KeySchemaElement> = serde_json::from_str(&row.key_schema)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs: Vec<AttributeDefinition> =
            serde_json::from_str(&row.attribute_definitions)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        let stream_spec = row
            .stream_specification
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (rcu, wcu) = match &row.provisioned_throughput {
            Some(s) => {
                let pt: extenddb_core::types::ProvisionedThroughput = serde_json::from_str(s)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                (pt.read_capacity_units, pt.write_capacity_units)
            }
            None => (0, 0),
        };

        let table_status = match row.table_status.as_str() {
            "ACTIVE" => TableStatus::Active,
            "CREATING" => TableStatus::Creating,
            "DELETING" => TableStatus::Deleting,
            "UPDATING" => TableStatus::Updating,
            other => {
                return Err(StorageError::Internal(format!(
                    "unknown table status in database: {other}"
                )));
            }
        };

        // Compute creation epoch from stored text timestamp
        let creation_epoch = row
            .creation_date_time
            .as_deref()
            .and_then(|s| crate::sqlite_util::parse_timestamp(s).ok())
            .map(|dt| dt.unix_timestamp() as f64)
            .unwrap_or(0.0);

        let billing_mode_summary = if row.billing_mode == "PAY_PER_REQUEST" {
            Some(BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: Some(creation_epoch),
            })
        } else {
            None
        };

        let latest_stream_arn = row
            .stream_label
            .as_ref()
            .map(|label| stream_arn(&self.region, account_id, &row.table_name, label));

        Ok(TableDescription {
            table_name: row.table_name,
            key_schema,
            attribute_definitions: attr_defs,
            table_status,
            creation_date_time: creation_epoch,
            table_size_bytes: row.table_size_bytes,
            item_count: row.item_count,
            table_arn: row.table_arn,
            table_id: row.table_id,
            provisioned_throughput: ProvisionedThroughputDescription {
                read_capacity_units: rcu,
                write_capacity_units: wcu,
                number_of_decreases_today: 0,
                last_increase_date_time: None,
                last_decrease_date_time: None,
            },
            billing_mode_summary,
            global_secondary_indexes: if gsis.is_empty() { None } else { Some(gsis) },
            local_secondary_indexes: if lsis.is_empty() { None } else { Some(lsis) },
            stream_specification: stream_spec,
            latest_stream_arn,
            latest_stream_label: row.stream_label,
            deletion_protection_enabled: row.deletion_protection_enabled,
            sse_description: None,
            table_class_summary: None,
        })
    }
}
