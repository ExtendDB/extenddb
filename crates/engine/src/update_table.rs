// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateTable` operation handler.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{BillingMode, UpdateTableInput};
use serde_json::Value;

use crate::OperationContext;
use crate::serialize_output;

/// Handle `UpdateTable` — modify billing mode, throughput, deletion protection,
/// or GSI configuration.
///
/// REQ-CTRL-003: `UpdateTable` must support changing billing mode, provisioned
/// throughput, and GSI create/delete.
///
/// # Errors
///
/// Returns `ValidationException` if no fields are specified, or if switching to
/// `PROVISIONED` without providing throughput values.
/// Returns `ResourceNotFoundException` if the table does not exist.
/// Returns `ResourceInUseException` if the table is not ACTIVE.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_update_table(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    // Per-member enum validation precedes everything else, including the table
    // lookup, matching the measured service ordering.
    crate::validate_enum_fields(
        &body,
        &[
            crate::EnumField {
                json_name: "BillingMode",
                valid: &["PROVISIONED", "PAY_PER_REQUEST"],
                clause: crate::EnumClause::Named("billingMode"),
            },
            crate::EnumField {
                json_name: "TableThroughputMode",
                valid: &["PROVISIONED", "PAY_PER_REQUEST"],
                clause: crate::EnumClause::Named("tableThroughputMode"),
            },
        ],
    )?;

    let mut input: UpdateTableInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;
    input.resolve_table_throughput_mode();

    if input.table_name.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "TableName must not be empty".to_owned(),
        ));
    }

    crate::vector_gate::ensure_update_table_supported(
        input.vector_index_updates.as_ref(),
        ctx.storage.as_vector_search(),
    )?;
    // Same per-index rules CreateTable applies, so a malformed index is rejected
    // identically whichever path it arrives by.
    extenddb_core::validation::validate_vector_index_updates(
        input.vector_index_updates.as_ref(),
        input.attribute_definitions.as_deref().unwrap_or_default(),
    )?;
    // Same collapse CreateTable applies, for the same reason: an index added by
    // either path must reach every backend in one shape.
    for update in input.vector_index_updates.iter_mut().flatten() {
        if let Some(create) = update.create.as_mut() {
            create.normalize_search_schema();
        }
    }

    let has_gsi_updates = input
        .global_secondary_index_updates
        .as_ref()
        .is_some_and(|u| !u.is_empty());
    // Vector index changes count as a specified field. Measured 2026-08-06: the
    // service accepts an UpdateTable carrying only VectorIndexUpdates, which is
    // how the backfill lifecycle was observed. Omitting it here rejected that
    // request as empty, so a vector-capable backend could never have been reached
    // by the one operation the contract models a lifecycle for. An empty list is
    // not a change, matching how the capability gate treats it.
    let has_vector_updates = input
        .vector_index_updates
        .as_ref()
        .is_some_and(|u| !u.is_empty());

    // Validate: at least one field must be specified.
    if input.billing_mode.is_none()
        && input.provisioned_throughput.is_none()
        && input.deletion_protection_enabled.is_none()
        && input.stream_specification.is_none()
        && input.table_class.is_none()
        && input.on_demand_throughput.is_none()
        && !has_gsi_updates
        && !has_vector_updates
    {
        return Err(DynamoDbError::ValidationException(
            "At least one of BillingMode, ProvisionedThroughput, DeletionProtectionEnabled, StreamSpecification, or GlobalSecondaryIndexUpdates must be specified".to_owned(),
        ));
    }

    // Switching to PROVISIONED requires explicit throughput values.
    if matches!(input.billing_mode, Some(BillingMode::Provisioned))
        && input.provisioned_throughput.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: ProvisionedThroughput must be specified when BillingMode is PROVISIONED".to_owned(),
        ));
    }

    // PAY_PER_REQUEST with ProvisionedThroughput is invalid.
    if matches!(input.billing_mode, Some(BillingMode::PayPerRequest))
        && input.provisioned_throughput.is_some()
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST".to_owned(),
        ));
    }

    // Validate throughput values (must be > 0).
    if let Some(ref tp) = input.provisioned_throughput
        && (tp.read_capacity_units < 1 || tp.write_capacity_units < 1)
    {
        return Err(DynamoDbError::ValidationException(
                "One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must each be at least 1".to_owned(),
            ));
    }

    validate_gsi_updates(&input)?;
    validate_stream_specification(&input)?;

    // A created GSI's projected attributes count toward the table's limit
    // together with the indexes it already has: the total CreateTable checks,
    // and so the total a restore of this table's backups checks.
    if let Some(updates) = &input.global_secondary_index_updates
        && updates.iter().any(|u| {
            u.create.as_ref().is_some_and(|c| {
                c.projection
                    .non_key_attributes
                    .as_ref()
                    .is_some_and(|a| !a.is_empty())
            })
        })
    {
        let existing = ctx
            .storage
            .describe_table(
                &ctx.account_id,
                extenddb_core::types::DescribeTableInput {
                    table_name: input.table_name.clone(),
                },
            )
            .await
            .map_err(|e| update_table_err_to_dynamo(e, &input.table_name))?;
        extenddb_core::validation::validate_projected_attribute_total(
            projected_after_gsi_updates(&existing, updates),
        )?;
    }

    let table_name = input.table_name.clone();
    // Kept for the post-condition check below, since `input` is moved into the
    // backend call.
    let vector_index_updates = input.vector_index_updates.clone();
    let mut desc = ctx
        .storage
        .update_table(&ctx.account_id, input)
        .await
        .map_err(|e| update_table_err_to_dynamo(e, &table_name))?;

    // A declared-capable backend must not silently drop the vector index change.
    // Checked against the description it returned, so this cannot be opted out of.
    crate::vector_gate::ensure_vector_updates_applied(vector_index_updates.as_ref(), &desc)?;

    // Drop the cached TableKeyInfo: index changes, stream-spec changes, and
    // throughput changes all alter what the cached value contains.
    //
    // NOTE: UpdateTable does NOT currently accept Tags. If that ever
    // changes, also invalidate `resource_tags` for the table ARN here —
    // the request itself populates resource_tags during authorize_request,
    // so a stale empty entry would otherwise hide the new tags. See
    // handle_create_table for the same pattern.
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, &table_name)
        .await;

    desc.prepare_for_response();
    let output = extenddb_core::types::UpdateTableOutput {
        table_description: desc,
    };
    serialize_output(&output)
}

/// Enabling a stream requires a view type. The sentence is the service's
/// (measured 2026-10-24 on a never-streamed table and on a table whose stream
/// is enabled). The service reports a defective index Create and a missing
/// ProvisionedThroughput before this (measured 2026-10-25), and this before
/// the table lookup, so the call sits after the billing and index rules and
/// before the backend call, where it also precedes the already-enabled
/// refusal. The `CreateTable` form has its own sentence.
fn validate_stream_specification(input: &UpdateTableInput) -> Result<(), DynamoDbError> {
    if let Some(spec) = &input.stream_specification
        && spec.stream_enabled
        && spec.stream_view_type.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: If stream is being enabled then UpdateViewType is required".to_owned(),
        ));
    }
    Ok(())
}

/// The per-entry rules for `GlobalSecondaryIndexUpdates`, checked before any
/// storage read: each entry has exactly one of Create, Update, or Delete,
/// and a Create has a valid name, a key schema of defined attributes, and
/// (when given) a throughput of at least 1 unit each.
fn validate_gsi_updates(input: &UpdateTableInput) -> Result<(), DynamoDbError> {
    if let Some(updates) = &input.global_secondary_index_updates {
        for update in updates {
            if update.create.is_some() && update.delete.is_some() {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: Only one of Create or Delete can be specified per GlobalSecondaryIndexUpdate".to_owned(),
                ));
            }
            if let Some(ref upd) = update.update {
                // S3: Acknowledge the Update action but reject it as unsupported.
                let _ = upd;
                return Err(DynamoDbError::ValidationException(
                    "UpdateGlobalSecondaryIndex is not yet supported".to_owned(),
                ));
            }
            if update.create.is_none() && update.delete.is_none() {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: GlobalSecondaryIndexUpdate must contain Create, Update, or Delete".to_owned(),
                ));
            }
            // M3: Validate index names the same way CreateTable does.
            if let Some(create) = &update.create {
                extenddb_core::validation::validate_index_name(&create.index_name)?;
                extenddb_core::validation::validate_index_throughput_minimum(
                    &create.index_name,
                    create.provisioned_throughput.as_ref(),
                )?;
                if create.key_schema.is_empty() {
                    return Err(DynamoDbError::ValidationException(
                        "One or more parameter values were invalid: KeySchema must not be empty for GSI creation".to_owned(),
                    ));
                }
                // Every key attribute must be in AttributeDefinitions. The
                // service's sentence depends on the member's shape (measured
                // 2026-10-25 and 2026-10-26): absent names the index; a name
                // defined twice is refused first; then the CreateTable rules
                // for the index's key schema (core's key_definitions.rs).
                let Some(defs) = &input.attribute_definitions else {
                    return Err(DynamoDbError::ValidationException(format!(
                        "One or more parameter values were invalid: AttributeDefinitions is not specified for index: {}",
                        create.index_name
                    )));
                };
                extenddb_core::validation::validate_unique_attribute_definitions(defs)?;
                extenddb_core::validation::validate_key_schema_defined(&create.key_schema, defs)?;
            }
            if let Some(delete) = &update.delete {
                extenddb_core::validation::validate_index_name(&delete.index_name)?;
            }
        }
    }
    Ok(())
}

/// The table's projected `NonKeyAttributes` once `updates` apply: its LSIs,
/// its GSIs that are not deleted, and the GSIs created.
fn projected_after_gsi_updates(
    existing: &extenddb_core::types::TableDescription,
    updates: &[extenddb_core::types::GlobalSecondaryIndexUpdate],
) -> usize {
    let deleted = |name: &str| {
        updates
            .iter()
            .filter_map(|u| u.delete.as_ref())
            .any(|d| d.index_name == name)
    };
    extenddb_core::validation::projected_non_key_attribute_count(
        existing
            .global_secondary_indexes
            .iter()
            .flatten()
            .filter(|g| !deleted(&g.index_name))
            .map(|g| &g.projection)
            .chain(
                existing
                    .local_secondary_indexes
                    .iter()
                    .flatten()
                    .map(|l| &l.projection),
            )
            .chain(
                updates
                    .iter()
                    .filter_map(|u| u.create.as_ref())
                    .map(|c| &c.projection),
            ),
    )
}

/// Map a backend failure from `update_table` onto the wire error.
///
/// A named function rather than an inline closure so every arm is reachable from
/// a unit test. Two of them are not reachable any other way: no in-tree backend
/// yet returns `Unsupported` or `IndexesInUse` from `update_table`, and the
/// vector capability gate refuses vector requests before the backend is called
/// at all, so a wire test cannot provoke either one today.
fn update_table_err_to_dynamo(
    e: extenddb_storage::error::StorageError,
    table_name: &str,
) -> DynamoDbError {
    use extenddb_storage::error::StorageError;
    match e {
        StorageError::TableNotFound(_name) => {
            DynamoDbError::ResourceNotFoundException("Requested resource not found".to_string())
        }
        StorageError::TableNotActive(name) => {
            DynamoDbError::ResourceInUseException(format!("Table {name} is not in ACTIVE state"))
        }
        StorageError::IndexAlreadyExists(name) => DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Index already exists: {name}"
        )),
        StorageError::IndexNotFound(name) => DynamoDbError::ResourceNotFoundException(format!(
            "Requested resource not found: Index {name} for table {table_name}"
        )),
        StorageError::NoOpUpdate(msg) => DynamoDbError::ValidationException(msg),
        StorageError::Validation(msg) => DynamoDbError::ValidationException(msg),
        StorageError::LimitExceeded(msg) => DynamoDbError::LimitExceededException(msg),
        // Not a fault, so deliberately not logged at error level: the backend
        // never claimed the feature. Same mapping CreateTable uses; without this
        // arm a capability refusal fell through to a 500, which tells a caller
        // the server is broken when the request simply cannot be served here.
        StorageError::Unsupported(msg) => DynamoDbError::ValidationException(msg),
        // The change is refused by the resource's current state, not by the
        // request. The backend supplies the whole message because only it knows
        // the state.
        StorageError::IndexesInUse(msg) => DynamoDbError::ResourceInUseException(msg),
        other => {
            tracing::error!(internal_error = %other, "storage internal error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
#[path = "update_table_tests.rs"]
mod tests;
