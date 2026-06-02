// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper types and methods for `TableEngine` operations.

use extenddb_core::types::{
    BillingMode, BillingModeSummary, GsiDescription, LsiDescription,
    ProvisionedThroughputDescription, TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn};
use sqlx::Row;

use crate::TidbEngine;
use crate::data::physical_data_table_name;
use crate::throughput::zero_provisioned_throughput_description;

/// Row type for table metadata queries.
#[derive(sqlx::FromRow)]
pub(crate) struct TableRow {
    pub table_name: String,
    pub key_schema: serde_json::Value,
    pub attribute_definitions: serde_json::Value,
    pub billing_mode: String,
    pub provisioned_throughput: Option<serde_json::Value>,
    pub stream_specification: Option<serde_json::Value>,
    pub table_status: String,
    pub creation_epoch: Option<f64>,
    pub table_arn: String,
    pub table_id: String,
    pub deletion_protection_enabled: bool,
    pub stream_label: Option<String>,
}

/// TiDB table statistics used for DynamoDB table descriptions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableStats {
    pub table_size_bytes: i64,
    pub item_count: i64,
}

/// Row type for index metadata queries.
#[derive(sqlx::FromRow)]
pub(crate) struct IndexRow {
    pub index_name: String,
    pub index_type: String,
    pub key_schema: serde_json::Value,
    pub projection: serde_json::Value,
    pub index_status: String,
    pub provisioned_throughput: Option<serde_json::Value>,
}

fn current_table_logical_size_sql() -> &'static str {
    "SELECT CAST(COALESCE(MAX(DATA_LENGTH), 0) AS SIGNED) \
     FROM information_schema.tables \
     WHERE table_schema = DATABASE() AND table_name = ?"
}

fn current_table_item_count_sql() -> &'static str {
    "SHOW STATS_META WHERE db_name = DATABASE() AND table_name = ?"
}

fn item_count_from_stats_meta_rows(rows: &[(String, i64)]) -> i64 {
    rows.iter()
        .find_map(|(partition_name, row_count)| (partition_name == "global").then_some(*row_count))
        .unwrap_or_else(|| {
            rows.iter()
                .map(|(_, row_count)| *row_count)
                .fold(0_i64, i64::saturating_add)
        })
}

impl TidbEngine {
    pub(crate) async fn current_table_stats(
        &self,
        table_id: &str,
    ) -> Result<TableStats, StorageError> {
        let physical_table = physical_data_table_name(table_id);
        let table_size_bytes = sqlx::query_scalar(current_table_logical_size_sql())
            .bind(&physical_table)
            .fetch_one(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let stats_meta_rows = sqlx::query(current_table_item_count_sql())
            .bind(&physical_table)
            .fetch_all(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let item_count_rows = stats_meta_rows
            .iter()
            .map(|row| {
                let partition_name = row
                    .try_get::<String, _>(2)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                let row_count = row
                    .try_get::<i64, _>(5)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok((partition_name, row_count))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        let item_count = item_count_from_stats_meta_rows(&item_count_rows);

        Ok(TableStats {
            table_size_bytes,
            item_count,
        })
    }

    pub(crate) async fn build_table_description(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableDescription, StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row: Option<TableRow> = sqlx::query_as(
            r"SELECT table_name, key_schema, attribute_definitions, billing_mode,
                      provisioned_throughput, stream_specification, table_status,
                      CAST(UNIX_TIMESTAMP(creation_date_time) AS DOUBLE) as creation_epoch,
                      table_arn, table_id, deletion_protection_enabled, stream_label
               FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        let index_rows: Vec<IndexRow> = sqlx::query_as(
            r"SELECT index_name, index_type, key_schema, projection,
                      index_status, provisioned_throughput
               FROM indexes WHERE table_id = ?",
        )
        .bind(&row.table_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let stats = self.current_table_stats(&row.table_id).await?;

        self.build_table_description_from_row(account_id, row, index_rows, stats)
    }

    pub(crate) fn build_table_description_from_row(
        &self,
        account_id: &str,
        row: TableRow,
        index_rows: Vec<IndexRow>,
        stats: TableStats,
    ) -> Result<TableDescription, StorageError> {
        let mut gsis: Vec<GsiDescription> = Vec::new();
        let mut lsis: Vec<LsiDescription> = Vec::new();

        for idx in index_rows {
            let ks = serde_json::from_value(idx.key_schema)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let proj = serde_json::from_value(idx.projection)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            match idx.index_type.as_str() {
                "GSI" => {
                    let provisioned_throughput = idx
                        .provisioned_throughput
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?
                        .unwrap_or_else(zero_provisioned_throughput_description);

                    gsis.push(GsiDescription {
                        index_name: idx.index_name.clone(),
                        key_schema: ks,
                        projection: proj,
                        index_status: idx.index_status,
                        provisioned_throughput: Some(provisioned_throughput),
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
                "LSI" => {
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
                other => {
                    return Err(StorageError::Internal(format!(
                        "unknown index type in database: {other}"
                    )));
                }
            }
        }

        let key_schema = serde_json::from_value(row.key_schema)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let attr_defs = serde_json::from_value(row.attribute_definitions)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let stream_spec = row
            .stream_specification
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (rcu, wcu) = match &row.provisioned_throughput {
            Some(v) => {
                let pt: extenddb_core::types::ProvisionedThroughput =
                    serde_json::from_value(v.clone())
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

        let creation_epoch = row.creation_epoch.unwrap_or(0.0);

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
            table_size_bytes: stats.table_size_bytes,
            item_count: stats.item_count,
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

#[cfg(test)]
mod tests {
    use super::{
        current_table_item_count_sql, current_table_logical_size_sql,
        item_count_from_stats_meta_rows,
    };

    #[test]
    fn table_description_stats_use_dynamodb_logical_size_and_tidb_row_count() {
        let size_sql = current_table_logical_size_sql();

        assert!(size_sql.contains("information_schema.tables"));
        assert!(size_sql.contains("DATA_LENGTH"));
        assert!(size_sql.contains("COALESCE(MAX(DATA_LENGTH), 0)"));
        assert!(!size_sql.contains("information_schema.table_storage_stats"));
        assert!(!size_sql.contains("TABLE_SIZE"));
        assert!(!size_sql.contains("TABLE_ROWS"));

        let count_sql = current_table_item_count_sql();
        assert_eq!(
            count_sql,
            "SHOW STATS_META WHERE db_name = DATABASE() AND table_name = ?"
        );
    }

    #[test]
    fn table_description_item_count_prefers_tidb_global_stats_meta_row() {
        let rows = [
            ("global".to_owned(), 3),
            ("p0".to_owned(), 2),
            ("p1".to_owned(), 1),
        ];

        assert_eq!(item_count_from_stats_meta_rows(&rows), 3);
    }

    #[test]
    fn table_description_item_count_sums_stats_meta_without_global_row() {
        let rows = [("".to_owned(), 2), ("".to_owned(), 3)];

        assert_eq!(item_count_from_stats_meta_rows(&rows), 5);
    }
}
