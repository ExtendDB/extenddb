// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TableEngine` implementation for the DynamoDB-at-home backend.
//!
//! Each operation:
//!  1. Resolves the physical DynamoDB table name via `self.namer.physical`.
//!  2. Calls the corresponding AWS SDK operation.
//!  3. Maps the SDK response back to core `extenddb_core` types.

use futures::future::BoxFuture;

use extenddb_core::types::{
    AttributeDefinition, BillingMode, BillingModeSummary, CreateTableInput, DeleteTableInput,
    DescribeTableInput, GsiDescription, IndexInfo, KeySchemaElement, KeyType, ListTablesInput,
    ListTablesOutput, LsiDescription, ProjectionType, ProvisionedThroughputDescription, Projection,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableDescription, TableKeyInfo,
    TableStatus, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::TableEngine;

use crate::DynamoEngine;

// ── Trait implementation ─────────────────────────────────────────────────────

impl TableEngine for DynamoEngine {
    fn create_table(
        &self,
        account_id: &str,
        input: CreateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let logical = input.table_name.clone();
            let physical = self.namer.physical(&account_id, &logical);

            // Map core KeySchemaElements → SDK KeySchemaElements
            let sdk_key_schema: Result<Vec<_>, _> = input
                .key_schema
                .iter()
                .map(|ks| {
                    aws_sdk_dynamodb::types::KeySchemaElement::builder()
                        .attribute_name(ks.attribute_name.clone())
                        .key_type(map_key_type_to_sdk(&ks.key_type))
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))
                })
                .collect();
            let sdk_key_schema = sdk_key_schema?;

            // Map core AttributeDefinitions → SDK AttributeDefinitions
            let sdk_attr_defs: Result<Vec<_>, _> = input
                .attribute_definitions
                .iter()
                .map(|ad| {
                    aws_sdk_dynamodb::types::AttributeDefinition::builder()
                        .attribute_name(ad.attribute_name.clone())
                        .attribute_type(map_scalar_type_to_sdk(&ad.attribute_type))
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))
                })
                .collect();
            let sdk_attr_defs = sdk_attr_defs?;

            // Build the create_table request
            let mut req = self
                .client
                .create_table()
                .table_name(physical)
                .set_key_schema(Some(sdk_key_schema))
                .set_attribute_definitions(Some(sdk_attr_defs));

            // Billing mode
            let billing_mode = input.billing_mode.unwrap_or(BillingMode::PayPerRequest);
            req = req.billing_mode(map_billing_mode_to_sdk(&billing_mode));

            // Provisioned throughput (only set when billing mode is Provisioned)
            if matches!(billing_mode, BillingMode::Provisioned) {
                if let Some(pt) = &input.provisioned_throughput {
                    let sdk_pt = aws_sdk_dynamodb::types::ProvisionedThroughput::builder()
                        .read_capacity_units(pt.read_capacity_units)
                        .write_capacity_units(pt.write_capacity_units)
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    req = req.provisioned_throughput(sdk_pt);
                }
            }

            // GSIs
            if let Some(gsis) = &input.global_secondary_indexes {
                for gsi in gsis {
                    let sdk_gsi_ks: Result<Vec<_>, _> = gsi
                        .key_schema
                        .iter()
                        .map(|ks| {
                            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                                .attribute_name(ks.attribute_name.clone())
                                .key_type(map_key_type_to_sdk(&ks.key_type))
                                .build()
                                .map_err(|e| StorageError::Internal(e.to_string()))
                        })
                        .collect();
                    let sdk_gsi_ks = sdk_gsi_ks?;

                    let sdk_proj = build_sdk_projection(&gsi.projection);

                    let mut gsi_builder = aws_sdk_dynamodb::types::GlobalSecondaryIndex::builder()
                        .index_name(gsi.index_name.clone())
                        .set_key_schema(Some(sdk_gsi_ks))
                        .projection(sdk_proj);

                    if let Some(pt) = &gsi.provisioned_throughput {
                        let sdk_pt = aws_sdk_dynamodb::types::ProvisionedThroughput::builder()
                            .read_capacity_units(pt.read_capacity_units)
                            .write_capacity_units(pt.write_capacity_units)
                            .build()
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        gsi_builder = gsi_builder.provisioned_throughput(sdk_pt);
                    }

                    let sdk_gsi = gsi_builder
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    req = req.global_secondary_indexes(sdk_gsi);
                }
            }

            // LSIs
            if let Some(lsis) = &input.local_secondary_indexes {
                for lsi in lsis {
                    let sdk_lsi_ks: Result<Vec<_>, _> = lsi
                        .key_schema
                        .iter()
                        .map(|ks| {
                            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                                .attribute_name(ks.attribute_name.clone())
                                .key_type(map_key_type_to_sdk(&ks.key_type))
                                .build()
                                .map_err(|e| StorageError::Internal(e.to_string()))
                        })
                        .collect();
                    let sdk_lsi_ks = sdk_lsi_ks?;

                    let sdk_proj = build_sdk_projection(&lsi.projection);

                    let sdk_lsi = aws_sdk_dynamodb::types::LocalSecondaryIndex::builder()
                        .index_name(lsi.index_name.clone())
                        .set_key_schema(Some(sdk_lsi_ks))
                        .projection(sdk_proj)
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    req = req.local_secondary_indexes(sdk_lsi);
                }
            }

            // Deletion protection
            if let Some(dp) = input.deletion_protection_enabled {
                req = req.deletion_protection_enabled(dp);
            }

            let out = req
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;

            match out.table_description() {
                Some(t) => to_table_description(t, &account_id, &self.namer),
                None => Err(StorageError::Internal(
                    "create_table: no TableDescription in response".into(),
                )),
            }
        })
    }

    fn delete_table(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let physical = self.namer.physical(&account_id, &input.table_name);
            let out = self
                .client
                .delete_table()
                .table_name(physical)
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;

            match out.table_description() {
                Some(t) => to_table_description(t, &account_id, &self.namer),
                None => Err(StorageError::Internal(
                    "delete_table: no TableDescription in response".into(),
                )),
            }
        })
    }

    fn describe_table(
        &self,
        account_id: &str,
        input: DescribeTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let logical = input.table_name.clone();
            let physical = self.namer.physical(&account_id, &logical);
            let out = self
                .client
                .describe_table()
                .table_name(physical)
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;

            match out.table() {
                Some(t) => to_table_description(t, &account_id, &self.namer),
                None => Err(StorageError::TableNotFound(logical)),
            }
        })
    }

    fn list_tables(
        &self,
        account_id: &str,
        input: ListTablesInput,
    ) -> BoxFuture<'_, Result<ListTablesOutput, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let account_prefix = self.namer.account_prefix(&account_id);

            // Build the SDK request, honoring pagination parameters
            let mut req = self.client.list_tables();

            if let Some(limit) = input.limit {
                // SDK takes i32; core limit is i32 — pass through directly
                req = req.limit(limit);
            }
            if let Some(start) = &input.exclusive_start_table_name {
                // The core exclusive_start_table_name is a *logical* name; we must
                // translate it to the physical name before passing to DynamoDB.
                let physical_start = self.namer.physical(&account_id, start);
                req = req.exclusive_start_table_name(physical_start);
            }

            let out = req.send().await.map_err(crate::errors::from_sdk_error)?;

            // Filter to this account's tables and strip the physical prefix back to logical
            let table_names: Vec<String> = out
                .table_names()
                .iter()
                .filter(|phys| phys.starts_with(&account_prefix))
                .filter_map(|phys| self.namer.logical(&account_id, phys).ok())
                .collect();

            // last_evaluated_table_name from DynamoDB is a physical name; convert to logical
            let last_evaluated_table_name = out
                .last_evaluated_table_name()
                .and_then(|phys| self.namer.logical(&account_id, phys).ok());

            Ok(ListTablesOutput {
                table_names,
                last_evaluated_table_name,
            })
        })
    }

    fn update_table(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        Box::pin(async move {
            let physical = self.namer.physical(&account_id, &input.table_name);

            let mut req = self.client.update_table().table_name(physical);

            // Billing mode change
            if let Some(bm) = &input.billing_mode {
                req = req.billing_mode(map_billing_mode_to_sdk(bm));
            }

            // Provisioned throughput change
            if let Some(pt) = &input.provisioned_throughput {
                let sdk_pt = aws_sdk_dynamodb::types::ProvisionedThroughput::builder()
                    .read_capacity_units(pt.read_capacity_units)
                    .write_capacity_units(pt.write_capacity_units)
                    .build()
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                req = req.provisioned_throughput(sdk_pt);
            }

            // Deletion protection change
            if let Some(dp) = input.deletion_protection_enabled {
                req = req.deletion_protection_enabled(dp);
            }

            // GSI updates
            // TODO: CreateGsiAction and DeleteGsiAction on UpdateTable are not yet forwarded.
            // The SDK's GlobalSecondaryIndexUpdate type supports Create/Update/Delete actions,
            // but the core types (CreateGsiAction, DeleteGsiAction, UpdateGsiAction) need a
            // full mapping to SDK GlobalSecondaryIndex/UpdateGlobalSecondaryIndexAction types.
            // For now we accept the input but only forward billing/throughput/deletion-protection.
            if input.global_secondary_index_updates.is_some() {
                return Err(StorageError::Internal(
                    "update_table: GlobalSecondaryIndexUpdates not yet supported in the DynamoDB backend".into(),
                ));
            }

            let out = req.send().await.map_err(crate::errors::from_sdk_error)?;

            match out.table_description() {
                Some(t) => to_table_description(t, &account_id, &self.namer),
                None => Err(StorageError::Internal(
                    "update_table: no TableDescription in response".into(),
                )),
            }
        })
    }

    fn table_key_info(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TableKeyInfo, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let physical = self.namer.physical(&account_id, &table_name);
            let out = self
                .client
                .describe_table()
                .table_name(physical)
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;

            let t = out
                .table()
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            // Key schema
            let key_schema: Vec<KeySchemaElement> = t
                .key_schema()
                .iter()
                .map(map_key_schema_from_sdk)
                .collect();

            // Attribute definitions
            let attribute_definitions: Vec<AttributeDefinition> = t
                .attribute_definitions()
                .iter()
                .map(map_attr_def_from_sdk)
                .collect();

            // Has LSI?
            let has_lsi = !t.local_secondary_indexes().is_empty();

            // Stream specification
            let stream_specification = t.stream_specification().map(map_stream_spec_from_sdk);

            // table_id: prefer SDK TableId, fall back to ARN, then physical name
            let physical_fallback = self.namer.physical(&account_id, &table_name);
            let table_id = t
                .table_id()
                .or(t.table_arn())
                .unwrap_or(physical_fallback.as_str())
                .to_owned();

            Ok(TableKeyInfo {
                table_name,
                account_id,
                table_id,
                key_schema,
                attribute_definitions,
                has_lsi,
                stream_specification,
            })
        })
    }

    fn index_info(
        &self,
        account_id: &str,
        table_name: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let index_name = index_name.to_owned();
        Box::pin(async move {
            let physical = self.namer.physical(&account_id, &table_name);
            let out = self
                .client
                .describe_table()
                .table_name(physical)
                .send()
                .await
                .map_err(crate::errors::from_sdk_error)?;

            let t = out
                .table()
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            // Search GSIs first
            for gsi in t.global_secondary_indexes() {
                if gsi.index_name() == Some(index_name.as_str()) {
                    let key_schema: Vec<KeySchemaElement> = gsi
                        .key_schema()
                        .iter()
                        .map(map_key_schema_from_sdk)
                        .collect();
                    let projection = gsi
                        .projection()
                        .map(map_projection_from_sdk)
                        .unwrap_or_else(default_projection);
                    let index_id = gsi
                        .index_arn()
                        .unwrap_or(&index_name)
                        .to_owned();
                    return Ok(IndexInfo {
                        index_name,
                        index_id,
                        index_type: extenddb_core::types::IndexType::Gsi,
                        key_schema,
                        projection,
                    });
                }
            }

            // Then LSIs
            for lsi in t.local_secondary_indexes() {
                if lsi.index_name() == Some(index_name.as_str()) {
                    let key_schema: Vec<KeySchemaElement> = lsi
                        .key_schema()
                        .iter()
                        .map(map_key_schema_from_sdk)
                        .collect();
                    let projection = lsi
                        .projection()
                        .map(map_projection_from_sdk)
                        .unwrap_or_else(default_projection);
                    let index_id = lsi
                        .index_arn()
                        .unwrap_or(&index_name)
                        .to_owned();
                    return Ok(IndexInfo {
                        index_name,
                        index_id,
                        index_type: extenddb_core::types::IndexType::Lsi,
                        key_schema,
                        projection,
                    });
                }
            }

            Err(StorageError::IndexNotFound(index_name))
        })
    }

    fn index_info_by_table_id(
        &self,
        table_id: &str,
        index_name: &str,
    ) -> BoxFuture<'_, Result<IndexInfo, StorageError>> {
        // TODO: index_info_by_table_id is not tractable in the DynamoDB backend without a
        // separate mapping from table_id → (account_id, physical_name). DynamoDB does not
        // provide a reverse-lookup API for table IDs. A future implementation could maintain
        // such a mapping in the catalog (Postgres) layer, but that is out of scope for v1.
        let _ = (table_id, index_name);
        Box::pin(async move {
            Err(StorageError::Internal(
                "index_info_by_table_id not supported in the DynamoDB backend".into(),
            ))
        })
    }
}

// ── Helpers: core → SDK conversions ─────────────────────────────────────────

fn map_key_type_to_sdk(kt: &KeyType) -> aws_sdk_dynamodb::types::KeyType {
    match kt {
        KeyType::Hash => aws_sdk_dynamodb::types::KeyType::Hash,
        KeyType::Range => aws_sdk_dynamodb::types::KeyType::Range,
    }
}

fn map_scalar_type_to_sdk(
    sat: &ScalarAttributeType,
) -> aws_sdk_dynamodb::types::ScalarAttributeType {
    match sat {
        ScalarAttributeType::S => aws_sdk_dynamodb::types::ScalarAttributeType::S,
        ScalarAttributeType::N => aws_sdk_dynamodb::types::ScalarAttributeType::N,
        ScalarAttributeType::B => aws_sdk_dynamodb::types::ScalarAttributeType::B,
    }
}

fn map_billing_mode_to_sdk(bm: &BillingMode) -> aws_sdk_dynamodb::types::BillingMode {
    match bm {
        BillingMode::PayPerRequest => aws_sdk_dynamodb::types::BillingMode::PayPerRequest,
        BillingMode::Provisioned => aws_sdk_dynamodb::types::BillingMode::Provisioned,
    }
}

fn build_sdk_projection(proj: &Projection) -> aws_sdk_dynamodb::types::Projection {
    let sdk_pt = match proj.projection_type {
        ProjectionType::All => aws_sdk_dynamodb::types::ProjectionType::All,
        ProjectionType::KeysOnly => aws_sdk_dynamodb::types::ProjectionType::KeysOnly,
        ProjectionType::Include => aws_sdk_dynamodb::types::ProjectionType::Include,
    };
    let mut builder = aws_sdk_dynamodb::types::Projection::builder().projection_type(sdk_pt);
    if let Some(attrs) = &proj.non_key_attributes {
        for attr in attrs {
            builder = builder.non_key_attributes(attr.clone());
        }
    }
    builder.build()
}

// ── Helpers: SDK → core conversions ─────────────────────────────────────────

fn map_key_schema_from_sdk(
    ks: &aws_sdk_dynamodb::types::KeySchemaElement,
) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: ks.attribute_name.clone(),
        key_type: match ks.key_type {
            aws_sdk_dynamodb::types::KeyType::Hash => KeyType::Hash,
            aws_sdk_dynamodb::types::KeyType::Range => KeyType::Range,
            _ => KeyType::Hash, // forward-compat: unknown → Hash
        },
    }
}

fn map_attr_def_from_sdk(
    ad: &aws_sdk_dynamodb::types::AttributeDefinition,
) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: ad.attribute_name.clone(),
        attribute_type: match ad.attribute_type {
            aws_sdk_dynamodb::types::ScalarAttributeType::S => ScalarAttributeType::S,
            aws_sdk_dynamodb::types::ScalarAttributeType::N => ScalarAttributeType::N,
            aws_sdk_dynamodb::types::ScalarAttributeType::B => ScalarAttributeType::B,
            _ => ScalarAttributeType::S, // forward-compat
        },
    }
}

fn map_projection_from_sdk(proj: &aws_sdk_dynamodb::types::Projection) -> Projection {
    let projection_type = match proj.projection_type() {
        Some(aws_sdk_dynamodb::types::ProjectionType::All) => ProjectionType::All,
        Some(aws_sdk_dynamodb::types::ProjectionType::KeysOnly) => ProjectionType::KeysOnly,
        Some(aws_sdk_dynamodb::types::ProjectionType::Include) => ProjectionType::Include,
        _ => ProjectionType::All, // forward-compat default
    };
    let non_key_attributes = {
        let attrs: Vec<String> = proj.non_key_attributes().to_vec();
        if attrs.is_empty() { None } else { Some(attrs) }
    };
    Projection {
        projection_type,
        non_key_attributes,
    }
}

fn default_projection() -> Projection {
    Projection {
        projection_type: ProjectionType::All,
        non_key_attributes: None,
    }
}

fn map_stream_spec_from_sdk(
    ss: &aws_sdk_dynamodb::types::StreamSpecification,
) -> StreamSpecification {
    let stream_view_type = ss.stream_view_type().map(|svt| match svt {
        aws_sdk_dynamodb::types::StreamViewType::KeysOnly => StreamViewType::KeysOnly,
        aws_sdk_dynamodb::types::StreamViewType::NewImage => StreamViewType::NewImage,
        aws_sdk_dynamodb::types::StreamViewType::OldImage => StreamViewType::OldImage,
        aws_sdk_dynamodb::types::StreamViewType::NewAndOldImages => {
            StreamViewType::NewAndOldImages
        }
        _ => StreamViewType::KeysOnly, // forward-compat
    });
    StreamSpecification {
        stream_enabled: ss.stream_enabled(),
        stream_view_type,
    }
}

fn map_table_status_from_sdk(
    ts: &aws_sdk_dynamodb::types::TableStatus,
) -> TableStatus {
    match ts {
        aws_sdk_dynamodb::types::TableStatus::Active => TableStatus::Active,
        aws_sdk_dynamodb::types::TableStatus::Creating => TableStatus::Creating,
        aws_sdk_dynamodb::types::TableStatus::Deleting => TableStatus::Deleting,
        aws_sdk_dynamodb::types::TableStatus::Updating => TableStatus::Updating,
        _ => TableStatus::Active, // forward-compat: Archived/Archiving/etc → Active
    }
}

// ── Main mapping helper ──────────────────────────────────────────────────────

/// Map an SDK [`TableDescription`] to a core [`TableDescription`].
///
/// `account_id` is used to derive the logical table name via the namer.
fn to_table_description(
    t: &aws_sdk_dynamodb::types::TableDescription,
    account_id: &str,
    namer: &crate::naming::Namer,
) -> Result<TableDescription, StorageError> {
    // Logical table name: strip account prefix from physical
    let physical_name = t.table_name().unwrap_or("");
    let table_name = namer
        .logical(account_id, physical_name)
        .unwrap_or_else(|_| physical_name.to_owned());

    let table_status = t
        .table_status()
        .map(map_table_status_from_sdk)
        .unwrap_or(TableStatus::Active);

    // creation_date_time: seconds since Unix epoch as f64
    let creation_date_time = t
        .creation_date_time()
        .map(|dt| dt.secs() as f64 + dt.subsec_nanos() as f64 / 1_000_000_000.0)
        .unwrap_or(0.0);

    let table_size_bytes = t.table_size_bytes().unwrap_or(0);
    let item_count = t.item_count().unwrap_or(0);

    let table_arn = t.table_arn().unwrap_or("").to_owned();
    let table_id = t.table_id().unwrap_or("").to_owned();

    // Key schema and attribute definitions
    let key_schema: Vec<KeySchemaElement> =
        t.key_schema().iter().map(map_key_schema_from_sdk).collect();
    let attribute_definitions: Vec<AttributeDefinition> = t
        .attribute_definitions()
        .iter()
        .map(map_attr_def_from_sdk)
        .collect();

    // ProvisionedThroughputDescription
    let provisioned_throughput = t
        .provisioned_throughput()
        .map(|pt| ProvisionedThroughputDescription {
            read_capacity_units: pt.read_capacity_units().unwrap_or(0),
            write_capacity_units: pt.write_capacity_units().unwrap_or(0),
            number_of_decreases_today: pt.number_of_decreases_today().unwrap_or(0),
            last_increase_date_time: pt
                .last_increase_date_time()
                .map(|dt| dt.secs() as f64 + dt.subsec_nanos() as f64 / 1_000_000_000.0),
            last_decrease_date_time: pt
                .last_decrease_date_time()
                .map(|dt| dt.secs() as f64 + dt.subsec_nanos() as f64 / 1_000_000_000.0),
        })
        .unwrap_or(ProvisionedThroughputDescription {
            read_capacity_units: 0,
            write_capacity_units: 0,
            number_of_decreases_today: 0,
            last_increase_date_time: None,
            last_decrease_date_time: None,
        });

    // BillingModeSummary
    let billing_mode_summary = t.billing_mode_summary().map(|bms| BillingModeSummary {
        billing_mode: bms
            .billing_mode()
            .map(|bm| match bm {
                aws_sdk_dynamodb::types::BillingMode::PayPerRequest => BillingMode::PayPerRequest,
                aws_sdk_dynamodb::types::BillingMode::Provisioned => BillingMode::Provisioned,
                _ => BillingMode::PayPerRequest,
            })
            .unwrap_or(BillingMode::PayPerRequest),
        last_update_to_pay_per_request_date_time: bms
            .last_update_to_pay_per_request_date_time()
            .map(|dt| dt.secs() as f64 + dt.subsec_nanos() as f64 / 1_000_000_000.0),
    });

    // GSIs
    let sdk_gsis = t.global_secondary_indexes();
    let global_secondary_indexes = if sdk_gsis.is_empty() {
        None
    } else {
        Some(
            sdk_gsis
                .iter()
                .map(|gsi| GsiDescription {
                    index_name: gsi.index_name().unwrap_or("").to_owned(),
                    key_schema: gsi.key_schema().iter().map(map_key_schema_from_sdk).collect(),
                    projection: gsi
                        .projection()
                        .map(map_projection_from_sdk)
                        .unwrap_or_else(default_projection),
                    index_status: gsi
                        .index_status()
                        .map(|s| s.as_str().to_owned())
                        .unwrap_or_else(|| "ACTIVE".to_owned()),
                    provisioned_throughput: gsi.provisioned_throughput().map(|pt| {
                        ProvisionedThroughputDescription {
                            read_capacity_units: pt.read_capacity_units().unwrap_or(0),
                            write_capacity_units: pt.write_capacity_units().unwrap_or(0),
                            number_of_decreases_today: pt.number_of_decreases_today().unwrap_or(0),
                            last_increase_date_time: pt.last_increase_date_time().map(|dt| {
                                dt.secs() as f64 + dt.subsec_nanos() as f64 / 1_000_000_000.0
                            }),
                            last_decrease_date_time: pt.last_decrease_date_time().map(|dt| {
                                dt.secs() as f64 + dt.subsec_nanos() as f64 / 1_000_000_000.0
                            }),
                        }
                    }),
                    index_size_bytes: gsi.index_size_bytes().unwrap_or(0),
                    item_count: gsi.item_count().unwrap_or(0),
                    index_arn: gsi.index_arn().unwrap_or("").to_owned(),
                })
                .collect(),
        )
    };

    // LSIs
    let sdk_lsis = t.local_secondary_indexes();
    let local_secondary_indexes = if sdk_lsis.is_empty() {
        None
    } else {
        Some(
            sdk_lsis
                .iter()
                .map(|lsi| LsiDescription {
                    index_name: lsi.index_name().unwrap_or("").to_owned(),
                    key_schema: lsi.key_schema().iter().map(map_key_schema_from_sdk).collect(),
                    projection: lsi
                        .projection()
                        .map(map_projection_from_sdk)
                        .unwrap_or_else(default_projection),
                    index_size_bytes: lsi.index_size_bytes().unwrap_or(0),
                    item_count: lsi.item_count().unwrap_or(0),
                    index_arn: lsi.index_arn().unwrap_or("").to_owned(),
                })
                .collect(),
        )
    };

    // Stream specification
    let stream_specification = t.stream_specification().map(map_stream_spec_from_sdk);
    let latest_stream_arn = t.latest_stream_arn().map(str::to_owned);
    let latest_stream_label = t.latest_stream_label().map(str::to_owned);

    let deletion_protection_enabled = t.deletion_protection_enabled().unwrap_or(false);

    Ok(TableDescription {
        table_name,
        key_schema,
        attribute_definitions,
        table_status,
        creation_date_time,
        table_size_bytes,
        item_count,
        table_arn,
        table_id,
        provisioned_throughput,
        billing_mode_summary,
        global_secondary_indexes,
        local_secondary_indexes,
        stream_specification,
        latest_stream_arn,
        latest_stream_label,
        deletion_protection_enabled,
        sse_description: None, // Not mapped in v1
        table_class_summary: None,
    })
}
