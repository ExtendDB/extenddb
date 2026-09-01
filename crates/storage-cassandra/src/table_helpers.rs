// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper methods for `TableEngine` operations.

use cdrs_tokio::types::{IntoRustByName, rows::Row};
use extenddb_core::types::{
    BillingMode, BillingModeSummary, GsiDescription, LsiDescription,
    ProvisionedThroughputDescription, TableDescription, TableStatus,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{index_arn, stream_arn};

use crate::CassandraEngine;

impl CassandraEngine {
    // TODO(fidelity): These two queries are not in a transaction. Under concurrent
    // UpdateTable (future phase), the table row and index rows could be read at
    // different points in time, producing an inconsistent snapshot. Cassandra
    // doesn't have SELECT ... FOR SHARE, so we'd need application-level locking
    // or accept eventual consistency.
    pub(crate) async fn build_table_description(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<TableDescription, StorageError> {
        let catalog_keyspace = self.catalog_keyspace();

        // Query table metadata
        let table_query = format!(
            "SELECT * FROM {catalog_keyspace}.tables WHERE account_id = ? AND table_name = ?"
        );

        let table_result = self
            .session
            .query_with_values(
                &table_query,
                cdrs_tokio::query_values!(account_id, table_name),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Query tables failed: {e}")))?;

        let table_body = table_result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let table_rows = table_body
            .into_rows()
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        let table_row = table_rows
            .first()
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;

        // Extract table_id for index query
        let table_id: String = table_row
            .get_r_by_name("table_id")
            .map_err(|e| StorageError::Internal(format!("Parse table_id: {e}")))?;

        // Query indexes
        let index_query = format!("SELECT * FROM {catalog_keyspace}.indexes WHERE table_id = ?");

        let index_result = self
            .session
            .query_with_values(&index_query, cdrs_tokio::query_values!(table_id.as_str()))
            .await
            .map_err(|e| StorageError::Internal(format!("Query indexes failed: {e}")))?;

        let index_body = index_result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let index_rows = index_body.into_rows().unwrap_or_default();

        // Delegate to builder
        self.build_table_description_from_row(account_id, table_row, index_rows)
    }

    pub(crate) fn build_table_description_from_row(
        &self,
        account_id: &str,
        table_row: &Row,
        index_rows: Vec<Row>,
    ) -> Result<TableDescription, StorageError> {
        // Parse table fields
        let table_name: String = table_row
            .get_r_by_name("table_name")
            .map_err(|e| StorageError::Internal(format!("Parse table_name: {e}")))?;

        let key_schema_str: String = table_row
            .get_r_by_name("key_schema")
            .map_err(|e| StorageError::Internal(format!("Parse key_schema: {e}")))?;
        let key_schema = serde_json::from_str(&key_schema_str)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let attr_defs_str: String = table_row
            .get_r_by_name("attribute_definitions")
            .map_err(|e| StorageError::Internal(format!("Parse attribute_definitions: {e}")))?;
        let attr_defs = serde_json::from_str(&attr_defs_str)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let billing_mode_str: String = table_row
            .get_r_by_name("billing_mode")
            .map_err(|e| StorageError::Internal(format!("Parse billing_mode: {e}")))?;

        let pt_str: Option<String> = table_row.get_r_by_name("provisioned_throughput").ok();
        let (rcu, wcu) = if let Some(ref s) = pt_str {
            let pt: extenddb_core::types::ProvisionedThroughput =
                serde_json::from_str(s).map_err(|e| StorageError::Internal(e.to_string()))?;
            (pt.read_capacity_units, pt.write_capacity_units)
        } else {
            (0, 0)
        };

        let stream_spec_str: Option<String> = table_row.get_r_by_name("stream_specification").ok();
        let stream_specification = stream_spec_str
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok());

        let table_status_str: String = table_row
            .get_r_by_name("table_status")
            .map_err(|e| StorageError::Internal(format!("Parse table_status: {e}")))?;
        let table_status = match table_status_str.as_str() {
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

        let creation_timestamp: i64 = table_row
            .get_r_by_name("created_at")
            .map_err(|e| StorageError::Internal(format!("Parse created_at: {e}")))?;
        let creation_epoch = crate::cassandra_util::millis_to_seconds_f64(creation_timestamp);

        let table_size_bytes: i64 = table_row.get_r_by_name("table_size_bytes").unwrap_or(0);
        let item_count: i64 = table_row.get_r_by_name("item_count").unwrap_or(0);

        let table_arn: String = table_row
            .get_r_by_name("table_arn")
            .map_err(|e| StorageError::Internal(format!("Parse table_arn: {e}")))?;
        let table_id: String = table_row
            .get_r_by_name("table_id")
            .map_err(|e| StorageError::Internal(format!("Parse table_id: {e}")))?;

        let deletion_protection_enabled: bool = table_row
            .get_r_by_name("deletion_protection_enabled")
            .unwrap_or(false);

        let stream_label: Option<String> = table_row.get_r_by_name("stream_label").ok();

        let table_class: Option<String> = table_row.get_by_name("table_class").ok().flatten();
        let on_demand_throughput: Option<extenddb_core::types::OnDemandThroughput> = table_row
            .get_by_name("on_demand_throughput")
            .ok()
            .flatten()
            .and_then(|s: String| serde_json::from_str(&s).ok());

        // Build GSI/LSI descriptions
        let mut gsis: Vec<GsiDescription> = Vec::new();
        let mut lsis: Vec<LsiDescription> = Vec::new();

        for row in index_rows {
            let index_name: String = row
                .get_r_by_name("index_name")
                .map_err(|e| StorageError::Internal(format!("Parse index_name: {e}")))?;
            let index_type: String = row
                .get_r_by_name("index_type")
                .map_err(|e| StorageError::Internal(format!("Parse index_type: {e}")))?;

            let ks_str: String = row
                .get_r_by_name("key_schema")
                .map_err(|e| StorageError::Internal(format!("Parse key_schema: {e}")))?;
            let ks =
                serde_json::from_str(&ks_str).map_err(|e| StorageError::Internal(e.to_string()))?;

            let proj_str: String = row
                .get_r_by_name("projection")
                .map_err(|e| StorageError::Internal(format!("Parse projection: {e}")))?;
            let proj = serde_json::from_str(&proj_str)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let index_status: String = row
                .get_r_by_name("index_status")
                .map_err(|e| StorageError::Internal(format!("Parse index_status: {e}")))?;

            if index_type == "GSI" {
                let pt_str: Option<String> = row.get_r_by_name("provisioned_throughput").ok();
                let pt = pt_str.as_ref().and_then(|s| serde_json::from_str(s).ok());

                gsis.push(GsiDescription {
                    index_name: index_name.clone(),
                    key_schema: ks,
                    projection: proj,
                    index_status,
                    provisioned_throughput: pt,
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: index_arn(&self.region, account_id, &table_name, &index_name),
                });
            } else {
                lsis.push(LsiDescription {
                    index_name: index_name.clone(),
                    key_schema: ks,
                    projection: proj,
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: index_arn(&self.region, account_id, &table_name, &index_name),
                });
            }
        }

        let billing_mode_summary = if billing_mode_str == "PAY_PER_REQUEST" {
            Some(BillingModeSummary {
                billing_mode: BillingMode::PayPerRequest,
                last_update_to_pay_per_request_date_time: Some(creation_epoch),
            })
        } else {
            None
        };

        let latest_stream_arn = stream_label
            .as_ref()
            .map(|label| stream_arn(&self.region, account_id, &table_name, label));

        Ok(TableDescription {
            table_name,
            key_schema,
            attribute_definitions: attr_defs,
            table_status,
            creation_date_time: creation_epoch,
            table_size_bytes,
            item_count,
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
            global_secondary_indexes: if gsis.is_empty() { None } else { Some(gsis) },
            local_secondary_indexes: if lsis.is_empty() { None } else { Some(lsis) },
            stream_specification,
            latest_stream_arn,
            latest_stream_label: stream_label,
            deletion_protection_enabled,
            sse_description: None,
            table_class_summary: table_class.map(|tc| serde_json::json!({"TableClass": tc})),
            on_demand_throughput,
            restore_summary: None,
            vector_indexes: None,
        })
    }
}
