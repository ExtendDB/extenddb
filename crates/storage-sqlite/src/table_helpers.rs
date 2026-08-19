// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row types and helpers for `TableEngine`: catalog row mapping, building a
//! `TableDescription`, and GSI backfill. JSON columns are stored as TEXT, so
//! they are fetched as `String` and parsed here.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, BillingModeSummary, GsiDescription, KeySchemaElement,
    LsiDescription, Projection, ProvisionedThroughput, ProvisionedThroughputDescription,
    SseDescription, SseType, TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn};

use crate::data;
use crate::sqlite_util::parse_timestamp;
use crate::store::SqliteEngine;

/// Catalog `tables` row (JSON columns as TEXT, timestamp as RFC 3339 TEXT).
#[derive(sqlx::FromRow)]
pub(crate) struct TableRow {
    pub table_name: String,
    pub key_schema: String,
    pub attribute_definitions: String,
    pub billing_mode: String,
    pub provisioned_throughput: Option<String>,
    pub stream_specification: Option<String>,
    pub table_status: String,
    pub creation_date_time: String,
    pub table_size_bytes: i64,
    pub item_count: i64,
    pub table_arn: String,
    pub table_id: String,
    pub deletion_protection_enabled: bool,
    pub stream_label: Option<String>,
    pub table_class: Option<String>,
    pub sse_specification: Option<String>,
    pub on_demand_throughput: Option<String>,
}

/// Catalog `indexes` row.
#[derive(sqlx::FromRow)]
pub(crate) struct IndexRow {
    pub index_name: String,
    #[allow(dead_code)]
    pub index_id: String,
    pub index_type: String,
    pub key_schema: String,
    pub projection: String,
    pub index_status: String,
    pub provisioned_throughput: Option<String>,
}

/// Columns selected for a `TableRow`, in `FromRow` field order.
pub(crate) const TABLE_COLUMNS: &str = "table_name, key_schema, attribute_definitions, \
     billing_mode, provisioned_throughput, stream_specification, table_status, \
     creation_date_time, table_size_bytes, item_count, table_arn, table_id, \
     deletion_protection_enabled, stream_label, table_class, sse_specification, \
     on_demand_throughput";

/// Columns selected for an `IndexRow`, in `FromRow` field order.
pub(crate) const INDEX_COLUMNS: &str = "index_name, index_id, index_type, key_schema, \
     projection, index_status, provisioned_throughput";

fn parse_json<T: serde::de::DeserializeOwned>(s: &str, ctx: &str) -> Result<T, StorageError> {
    serde_json::from_str(s).map_err(|e| StorageError::Internal(format!("{ctx}: {e}")))
}

impl SqliteEngine {
    /// Build a `TableDescription` by reading the catalog.
    pub(crate) async fn build_table_description(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableDescription, StorageError> {
        let row: Option<TableRow> = sqlx::query_as(&format!(
            "SELECT {TABLE_COLUMNS} FROM tables WHERE account_id = ? AND table_name = ?"
        ))
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        let index_rows: Vec<IndexRow> = sqlx::query_as(&format!(
            "SELECT {INDEX_COLUMNS} FROM indexes WHERE table_id = ?"
        ))
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
            let ks = parse_json(&idx.key_schema, "index key_schema")?;
            let proj = parse_json(&idx.projection, "index projection")?;
            if idx.index_type == "GSI" {
                let pt = idx
                    .provisioned_throughput
                    .as_deref()
                    .map(|s| parse_json::<ProvisionedThroughputDescription>(s, "index pt"))
                    .transpose()?;
                gsis.push(GsiDescription {
                    index_name: idx.index_name.clone(),
                    key_schema: ks,
                    projection: proj,
                    index_status: idx.index_status,
                    provisioned_throughput: pt.or(Some(zero_throughput())),
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

        let key_schema = parse_json(&row.key_schema, "key_schema")?;
        let attr_defs = parse_json(&row.attribute_definitions, "attribute_definitions")?;
        let stream_spec = row
            .stream_specification
            .as_deref()
            .map(|s| parse_json(s, "stream_specification"))
            .transpose()?;

        let (rcu, wcu) = match &row.provisioned_throughput {
            Some(s) => {
                let pt: ProvisionedThroughput = parse_json(s, "provisioned_throughput")?;
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

        let creation_epoch = parse_timestamp(&row.creation_date_time)
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

        let sse_description = row.sse_specification.as_deref().and_then(|spec| {
            let enabled = serde_json::from_str::<serde_json::Value>(spec)
                .ok()
                .and_then(|v| v.get("Enabled").and_then(serde_json::Value::as_bool))
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
            global_secondary_indexes: (!gsis.is_empty()).then_some(gsis),
            local_secondary_indexes: (!lsis.is_empty()).then_some(lsis),
            stream_specification: stream_spec,
            latest_stream_arn,
            latest_stream_label: row.stream_label,
            deletion_protection_enabled: row.deletion_protection_enabled,
            sse_description,
            table_class_summary: row
                .table_class
                .as_ref()
                .map(|tc| serde_json::json!({ "TableClass": tc })),
            on_demand_throughput: row
                .on_demand_throughput
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok()),
            // Fields for features this backend does not implement, vector
            // indexes today, take their defaults, so adding one to this type
            // does not break this build.
            ..Default::default()
        })
    }

    /// Backfill existing base-table items into a newly created GSI, batched to
    /// bound memory.
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
        const BATCH: i64 = 500;
        let base_table = data::data_table_name(table_id);
        let idx_table = data::index_table_name(index_id);
        let idx_sks = data::all_sort_key_info(index_key_schema, attr_defs);
        let base_sks = data::all_sort_key_info(base_key_schema, base_attr_defs);

        let sql = format!("SELECT item_data FROM {base_table} ORDER BY pk LIMIT ? OFFSET ?");
        let mut offset: i64 = 0;
        loop {
            let rows: Vec<(String,)> = sqlx::query_as(&sql)
                .bind(BATCH)
                .bind(offset)
                .fetch_all(&mut **tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            if rows.is_empty() {
                break;
            }
            let len = rows.len() as i64;
            for (item_json,) in rows {
                let item: extenddb_core::types::Item = serde_json::from_str(&item_json)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                if !index_key_schema
                    .iter()
                    .all(|ks| item.contains_key(&ks.attribute_name))
                {
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
                    &idx_sks,
                    &base_sks,
                )
                .await?;
            }
            if len < BATCH {
                break;
            }
            offset += len;
        }
        Ok(())
    }
}

fn zero_throughput() -> ProvisionedThroughputDescription {
    ProvisionedThroughputDescription {
        read_capacity_units: 0,
        write_capacity_units: 0,
        number_of_decreases_today: 0,
        last_increase_date_time: None,
        last_decrease_date_time: None,
    }
}
