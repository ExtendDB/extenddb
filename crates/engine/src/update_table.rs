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
    let input: UpdateTableInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

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

    // Validate: enabling streams requires a view type.
    if let Some(spec) = &input.stream_specification
        && spec.stream_enabled
        && spec.stream_view_type.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "StreamViewType must be specified when StreamEnabled is true".to_owned(),
        ));
    }

    // Switching to PROVISIONED requires explicit throughput values.
    if matches!(input.billing_mode, Some(BillingMode::Provisioned))
        && input.provisioned_throughput.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: ProvisionedThroughput must be specified when changing BillingMode to PROVISIONED".to_owned(),
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

    // Validate GSI updates: each entry must have exactly one of Create, Update, or Delete.
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
                if create.key_schema.is_empty() {
                    return Err(DynamoDbError::ValidationException(
                        "One or more parameter values were invalid: KeySchema must not be empty for GSI creation".to_owned(),
                    ));
                }
                // Validate that all key attributes are defined in AttributeDefinitions
                let attr_defs = input.attribute_definitions.as_deref().unwrap_or(&[]);
                for ks in &create.key_schema {
                    if !attr_defs
                        .iter()
                        .any(|ad| ad.attribute_name == ks.attribute_name)
                    {
                        return Err(DynamoDbError::ValidationException(format!(
                            "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. \
                             Keys: [{}], AttributeDefinitions: [{}]",
                            ks.attribute_name,
                            attr_defs
                                .iter()
                                .map(|ad| ad.attribute_name.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )));
                    }
                }
            }
            if let Some(delete) = &update.delete {
                extenddb_core::validation::validate_index_name(&delete.index_name)?;
            }
        }
    }

    let table_name = input.table_name.clone();
    // Kept for the post-condition check below, since `input` is moved into the
    // backend call.
    let vector_index_updates = input.vector_index_updates.clone();
    let desc = ctx
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

    let output = extenddb_core::types::UpdateTableOutput {
        table_description: desc,
    };
    serialize_output(&output)
}

/// Map a backend failure from `update_table` onto the wire error.
///
/// A named function rather than an inline closure so every arm is reachable from
/// a unit test. Two of them are not reachable any other way: no in-tree backend
/// yet returns `Unsupported` or `ResourceInUse` from `update_table`, and the
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
        StorageError::ResourceInUse(msg) => DynamoDbError::ResourceInUseException(msg),
        other => {
            tracing::error!(internal_error = %other, "storage internal error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::error::StorageError;

    /// A backend that cannot serve the request reports a 400, not a 500.
    ///
    /// This arm was missing while CreateTable had it, so the same refusal
    /// answered differently depending on which operation carried it. It matters
    /// for a backend whose vector capability is decided at runtime rather than at
    /// compile time: its refusal arrives through this path.
    #[test]
    fn an_unsupported_feature_is_a_validation_exception() {
        let err = update_table_err_to_dynamo(
            StorageError::Unsupported("Vector indexes are not supported here".to_owned()),
            "t",
        );
        match err {
            DynamoDbError::ValidationException(msg) => {
                assert_eq!(msg, "Vector indexes are not supported here");
            }
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    /// The backend's whole message survives, unwrapped and unprefixed, because
    /// the service's own wording for this case names both the table and the index
    /// and no layer above the backend knows either.
    #[test]
    fn a_resource_in_use_refusal_keeps_the_backend_message() {
        let measured = extenddb_core::types::vector_index_delete_in_allocation_phase("t", "vidx");
        let err = update_table_err_to_dynamo(StorageError::ResourceInUse(measured.clone()), "t");
        match err {
            DynamoDbError::ResourceInUseException(msg) => assert_eq!(msg, measured),
            other => panic!("expected ResourceInUseException, got {other:?}"),
        }
    }

    /// The measured whole string, byte for byte, from probe P2 on 2026-08-19.
    #[test]
    fn the_allocation_phase_refusal_is_the_measured_whole_string() {
        assert_eq!(
            extenddb_core::types::vector_index_delete_in_allocation_phase(
                "eddbprobe-backfill",
                "vidx2"
            ),
            "Attempt to change a resource which is still in use: Index creation is in resource \
             allocation phase. Retry deletion during backfilling phase or when the index is \
             active. Table: eddbprobe-backfill Index: vidx2"
        );
    }

    /// A genuine fault still reports a 500 and is still logged as one.
    #[test]
    fn an_internal_failure_is_still_an_internal_server_error() {
        let err =
            update_table_err_to_dynamo(StorageError::Internal("disk on fire".to_owned()), "t");
        assert!(matches!(err, DynamoDbError::InternalServerError(_)));
    }
}
