// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_table` implementation for `TidbEngine`.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, ProvisionedThroughput, StreamSpecification,
    TableDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;

use crate::TidbEngine;
use crate::data::validate_native_key_schema_shape;
use crate::stream_engine::StreamGenerationCatalog;
use crate::throughput::provisioned_throughput_description;

type UpdateTableCatalogRow = (
    String,
    String,
    serde_json::Value,
    serde_json::Value,
    String,
    Option<serde_json::Value>,
    Option<serde_json::Value>,
    Option<String>,
);

fn table_accepts_update_table(status: &str) -> bool {
    matches!(status, "ACTIVE" | "UPDATING")
}

fn merge_attribute_definitions(
    current: &[AttributeDefinition],
    incoming: Option<&[AttributeDefinition]>,
) -> Result<Vec<AttributeDefinition>, StorageError> {
    let Some(incoming) = incoming else {
        return Ok(current.to_vec());
    };

    let mut merged = current.to_vec();
    for attr in incoming {
        if let Some(existing) = merged
            .iter()
            .find(|existing| existing.attribute_name == attr.attribute_name)
        {
            if existing.attribute_type != attr.attribute_type {
                return Err(StorageError::Validation(format!(
                    "One or more parameter values were invalid: AttributeDefinition type mismatch for {}",
                    attr.attribute_name
                )));
            }
            continue;
        }
        merged.push(attr.clone());
    }

    Ok(merged)
}

fn validate_index_key_definitions(
    index_name: &str,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), StorageError> {
    for key in key_schema {
        if !attr_defs
            .iter()
            .any(|attr| attr.attribute_name == key.attribute_name)
        {
            return Err(StorageError::Validation(format!(
                "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions for index {index_name}: {}",
                key.attribute_name
            )));
        }
    }

    Ok(())
}

fn stream_enabled_from_catalog(
    stream_specification: Option<&serde_json::Value>,
) -> Result<bool, StorageError> {
    stream_specification
        .map(|value| {
            serde_json::from_value::<StreamSpecification>(value.clone())
                .map(|spec| spec.stream_enabled)
        })
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))
        .map(|enabled| enabled.unwrap_or(false))
}

fn next_stream_label_for_update(
    current_stream_specification: Option<&serde_json::Value>,
    current_stream_label: Option<&str>,
    requested_stream_enabled: bool,
    generated_stream_label: String,
) -> Result<Option<String>, StorageError> {
    if !requested_stream_enabled {
        return Ok(None);
    }

    if stream_enabled_from_catalog(current_stream_specification)? {
        return Ok(current_stream_label
            .map(str::to_owned)
            .or(Some(generated_stream_label)));
    }

    Ok(Some(generated_stream_label))
}

impl TidbEngine {
    /// Core implementation of `update_table` (REQ-CTRL-003).
    pub(crate) async fn update_table_impl(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Lock only the short-lived catalog row while appending new intent.
        // TiDB owns the long-running distributed online DDL jobs.
        let row: Option<UpdateTableCatalogRow> = sqlx::query_as(
            "SELECT table_status, table_id, key_schema, attribute_definitions, billing_mode, \
                    provisioned_throughput, stream_specification, stream_label \
             FROM tables \
             WHERE account_id = ? AND table_name = ? FOR UPDATE",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (
            status,
            table_id,
            current_key_schema_json,
            current_attr_defs_json,
            current_billing_mode,
            current_pt_json,
            current_stream_spec_json,
            current_stream_label,
        ) = row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
        if !table_accepts_update_table(&status) {
            return Err(StorageError::TableNotActive(input.table_name.clone()));
        }
        let has_gsi_updates = input
            .global_secondary_index_updates
            .as_ref()
            .is_some_and(|updates| !updates.is_empty());
        let has_gsi_create = input
            .global_secondary_index_updates
            .as_deref()
            .is_some_and(|updates| updates.iter().any(|update| update.create.is_some()));
        let current_attr_defs: Vec<AttributeDefinition> =
            serde_json::from_value(current_attr_defs_json)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        let merged_attr_defs = if has_gsi_create {
            merge_attribute_definitions(&current_attr_defs, input.attribute_definitions.as_deref())?
        } else {
            current_attr_defs
        };

        // No-op rejection: setting same billing mode to PROVISIONED with same
        // throughput values is rejected by DynamoDB. This check runs under the
        // FOR UPDATE lock to eliminate the TOCTOU race that existed when the
        // check was in the engine layer.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned))
            && let Some(ref pt) = input.provisioned_throughput
        {
            let current_pt: Option<ProvisionedThroughput> = current_pt_json
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let (current_rcu, current_wcu) = current_pt.as_ref().map_or((0, 0), |pt| {
                (pt.read_capacity_units, pt.write_capacity_units)
            });

            if current_billing_mode == "PROVISIONED"
                && current_rcu == pt.read_capacity_units
                && current_wcu == pt.write_capacity_units
            {
                return Err(StorageError::NoOpUpdate(format!(
                    "The provisioned throughput for the table will not change. \
                     The requested value equals the current value. \
                     Current ReadCapacityUnits provisioned for the table: {}. \
                     Requested ReadCapacityUnits: {}. \
                     Current WriteCapacityUnits provisioned for the table: {}. \
                     Requested WriteCapacityUnits: {}.",
                    current_rcu, pt.read_capacity_units, current_wcu, pt.write_capacity_units
                )));
            }
        }

        if has_gsi_updates {
            sqlx::query(
                "UPDATE tables SET table_status = 'UPDATING', \
                    status_transition_at = CURRENT_TIMESTAMP(6) \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply billing mode change.
        if let Some(bm) = &input.billing_mode {
            let bm_str = match bm {
                BillingMode::Provisioned => "PROVISIONED",
                BillingMode::PayPerRequest => "PAY_PER_REQUEST",
            };
            sqlx::query(
                "UPDATE tables SET billing_mode = ? WHERE account_id = ? AND table_name = ?",
            )
            .bind(bm_str)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply provisioned throughput change.
        if let Some(pt) = &input.provisioned_throughput {
            let pt_json =
                serde_json::to_value(pt).map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query("UPDATE tables SET provisioned_throughput = ? WHERE account_id = ? AND table_name = ?")
                .bind(&pt_json)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply deletion protection change.
        if let Some(dp) = input.deletion_protection_enabled {
            sqlx::query("UPDATE tables SET deletion_protection_enabled = ? WHERE account_id = ? AND table_name = ?")
                .bind(dp)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply GSI updates (create/delete).
        if let Some(updates) = &input.global_secondary_index_updates {
            for update in updates {
                if let Some(create) = &update.create {
                    validate_index_key_definitions(
                        &create.index_name,
                        &create.key_schema,
                        &merged_attr_defs,
                    )?;
                    validate_native_key_schema_shape(
                        &format!("global secondary index {}", create.index_name),
                        &create.key_schema,
                    )?;

                    // Check for duplicate index name.
                    let existing: Option<(String,)> = sqlx::query_as(
                        "SELECT index_name FROM indexes WHERE table_id = ? AND index_name = ?",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    if existing.is_some() {
                        return Err(StorageError::IndexAlreadyExists(create.index_name.clone()));
                    }

                    let gsi_ks = serde_json::to_value(&create.key_schema)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_proj = serde_json::to_value(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_pt_description = create
                        .provisioned_throughput
                        .as_ref()
                        .map(provisioned_throughput_description);
                    let gsi_pt = gsi_pt_description
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let index_id = uuid::Uuid::new_v4().to_string();
                    sqlx::query(
                        r"INSERT INTO indexes
                           (table_id, index_name, index_id, index_type, key_schema, projection,
                            index_status, provisioned_throughput)
                           VALUES (?, ?, ?, 'GSI', ?, ?, 'CREATING', ?)",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .bind(&index_id)
                    .bind(&gsi_ks)
                    .bind(&gsi_proj)
                    .bind(&gsi_pt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                }

                if let Some(delete) = &update.delete {
                    // Verify the index exists and fetch its index_id.
                    let existing: Option<(String,)> = sqlx::query_as(
                        "SELECT index_name FROM indexes \
                         WHERE table_id = ? AND index_name = ? AND index_type = 'GSI' \
                           AND index_status = 'ACTIVE'",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let (_existing_name,) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;

                    // Hide the index from read/write paths immediately. Metadata
                    // is deleted only after the TiDB data table is dropped.
                    sqlx::query(
                        "UPDATE indexes SET index_status = 'DELETING' \
                         WHERE table_id = ? AND index_name = ? AND index_type = 'GSI'",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
            }

            // AttributeDefinitions are a table-level catalog of key attributes.
            // UpdateTable supplies only the new GSI key definitions, so merge
            // them instead of replacing definitions needed by the base table or
            // other native TiDB indexes.
            if has_gsi_create {
                let ad_json = serde_json::to_value(&merged_attr_defs)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                sqlx::query("UPDATE tables SET attribute_definitions = ? WHERE account_id = ? AND table_name = ?")
                    .bind(&ad_json)
                    .bind(account_id)
                    .bind(&input.table_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        // Apply stream specification after user-visible validation has passed.
        // TiDB stream shards are derived from table_id; there are no data-side
        // shard rows to create before metadata becomes visible.
        if let Some(spec) = &input.stream_specification {
            let spec_json =
                serde_json::to_value(spec).map_err(|e| StorageError::Internal(e.to_string()))?;
            let current_stream_enabled =
                stream_enabled_from_catalog(current_stream_spec_json.as_ref())?;
            let next_stream_label = next_stream_label_for_update(
                current_stream_spec_json.as_ref(),
                current_stream_label.as_deref(),
                spec.stream_enabled,
                Self::new_stream_label(),
            )?;
            match (
                spec.stream_enabled,
                current_stream_enabled,
                &current_stream_label,
            ) {
                (true, true, Some(label)) => {
                    Self::upsert_enabled_stream_generation_in_tx(
                        &mut tx,
                        StreamGenerationCatalog {
                            account_id,
                            table_name: &input.table_name,
                            table_id: &table_id,
                            stream_label: label,
                            key_schema: &current_key_schema_json,
                            stream_specification: &spec_json,
                        },
                    )
                    .await?;
                }
                (true, _, _) => {
                    let label = next_stream_label.as_deref().ok_or_else(|| {
                        StorageError::Internal(
                            "stream enable did not allocate a stream label".to_owned(),
                        )
                    })?;
                    Self::upsert_enabled_stream_generation_in_tx(
                        &mut tx,
                        StreamGenerationCatalog {
                            account_id,
                            table_name: &input.table_name,
                            table_id: &table_id,
                            stream_label: label,
                            key_schema: &current_key_schema_json,
                            stream_specification: &spec_json,
                        },
                    )
                    .await?;
                }
                (false, _, Some(label)) => {
                    let generation_spec_json =
                        current_stream_spec_json.as_ref().unwrap_or(&spec_json);
                    Self::disable_stream_generation_in_tx(
                        &mut tx,
                        StreamGenerationCatalog {
                            account_id,
                            table_name: &input.table_name,
                            table_id: &table_id,
                            stream_label: label,
                            key_schema: &current_key_schema_json,
                            stream_specification: generation_spec_json,
                        },
                    )
                    .await?;
                }
                (false, _, None) => {}
            }
            sqlx::query(
                "UPDATE tables SET stream_specification = ?, stream_label = ? \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&spec_json)
            .bind(&next_stream_label)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let desc = self
            .build_table_description(account_id, &input.table_name)
            .await?;
        if has_gsi_updates {
            self.control_plane_notify.notify_one();
        }

        Ok(desc)
    }
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    use super::{
        merge_attribute_definitions, next_stream_label_for_update, table_accepts_update_table,
        validate_index_key_definitions,
    };

    fn attr(name: &str, attribute_type: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type,
        }
    }

    fn hash_key(name: &str) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Hash,
        }
    }

    #[test]
    fn update_table_accepts_online_ddl_status() {
        assert!(table_accepts_update_table("ACTIVE"));
        assert!(table_accepts_update_table("UPDATING"));
        assert!(!table_accepts_update_table("CREATING"));
        assert!(!table_accepts_update_table("DELETING"));
    }

    #[test]
    fn update_table_gsi_create_merges_attribute_definitions() {
        let merged = merge_attribute_definitions(
            &[
                attr("pk", ScalarAttributeType::S),
                attr("sk", ScalarAttributeType::N),
            ],
            Some(&[attr("gsi_pk", ScalarAttributeType::S)]),
        )
        .expect("merge");

        assert_eq!(
            merged,
            vec![
                attr("pk", ScalarAttributeType::S),
                attr("sk", ScalarAttributeType::N),
                attr("gsi_pk", ScalarAttributeType::S),
            ]
        );
    }

    #[test]
    fn update_table_gsi_create_rejects_attribute_type_drift() {
        let err = merge_attribute_definitions(
            &[attr("gsi_pk", ScalarAttributeType::S)],
            Some(&[attr("gsi_pk", ScalarAttributeType::N)]),
        )
        .expect_err("type conflict");

        assert!(
            err.to_string()
                .contains("AttributeDefinition type mismatch")
        );
    }

    #[test]
    fn update_table_gsi_create_can_reuse_existing_attribute_definition() {
        validate_index_key_definitions(
            "by_customer",
            &[hash_key("customer_id")],
            &[
                attr("pk", ScalarAttributeType::S),
                attr("customer_id", ScalarAttributeType::S),
            ],
        )
        .expect("existing attribute definition is enough");
    }

    #[test]
    fn update_table_stream_reenable_starts_new_generation() {
        let disabled = serde_json::json!({"StreamEnabled": false});

        let label = next_stream_label_for_update(
            Some(&disabled),
            Some("2026-01-01T00:00:00Z"),
            true,
            "2026-01-02T00:00:00Z".to_owned(),
        )
        .expect("stream label");

        assert_eq!(label.as_deref(), Some("2026-01-02T00:00:00Z"));
    }

    #[test]
    fn update_table_stream_update_keeps_active_generation() {
        let enabled = serde_json::json!({
            "StreamEnabled": true,
            "StreamViewType": "NEW_AND_OLD_IMAGES"
        });

        let label = next_stream_label_for_update(
            Some(&enabled),
            Some("2026-01-01T00:00:00Z"),
            true,
            "2026-01-02T00:00:00Z".to_owned(),
        )
        .expect("stream label");

        assert_eq!(label.as_deref(), Some("2026-01-01T00:00:00Z"));
    }

    #[test]
    fn update_table_stream_disable_removes_active_generation() {
        let enabled = serde_json::json!({
            "StreamEnabled": true,
            "StreamViewType": "KEYS_ONLY"
        });

        let label = next_stream_label_for_update(
            Some(&enabled),
            Some("2026-01-01T00:00:00Z"),
            false,
            "2026-01-02T00:00:00Z".to_owned(),
        )
        .expect("stream label");

        assert_eq!(label, None);
    }
}
