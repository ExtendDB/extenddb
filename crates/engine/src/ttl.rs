// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DescribeTimeToLive` and `UpdateTimeToLive` operation handlers.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    DescribeTimeToLiveInput, DescribeTimeToLiveOutput, TimeToLiveSpecificationOutput,
    TimeToLiveStatus, UpdateTimeToLiveInput, UpdateTimeToLiveOutput,
};
use extenddb_core::validation::validate_table_name;
use extenddb_storage::error::StorageError;
use serde_json::Value;

use crate::OperationContext;
use crate::serialize_output;

/// Handle `DescribeTimeToLive` — return TTL configuration for a table.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the table does not exist.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_describe_time_to_live(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: DescribeTimeToLiveInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    validate_table_name(&input.table_name, &ctx.limits)?;

    let desc = ctx
        .storage
        .describe_ttl(&ctx.account_id, &input.table_name)
        .await
        .map_err(storage_to_dynamo)?;

    let output = DescribeTimeToLiveOutput {
        time_to_live_description: desc,
    };
    serialize_output(&output)
}

/// Handle `UpdateTimeToLive` — enable or disable TTL on a table attribute.
///
/// The storage backend owns the full TTL mutation, including any
/// backend-specific lookup index or native TTL DDL.
///
/// # Errors
///
/// Returns `ValidationException` if the attribute name is empty, or if TTL
/// is already in the requested state (idempotency check).
/// Returns `ResourceNotFoundException` if the table does not exist.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_update_time_to_live(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: UpdateTimeToLiveInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    validate_table_name(&input.table_name, &ctx.limits)?;
    validate_ttl_attribute_name(&input.time_to_live_specification.attribute_name)?;

    // S4: Idempotency check — DynamoDB rejects enabling TTL when already
    // enabled, and disabling when already disabled.
    let current = ctx
        .storage
        .describe_ttl(&ctx.account_id, &input.table_name)
        .await
        .map_err(storage_to_dynamo)?;

    match (
        input.time_to_live_specification.enabled,
        current.time_to_live_status,
    ) {
        (true, TimeToLiveStatus::Enabled) => {
            return Err(DynamoDbError::ValidationException(
                "TimeToLive is already enabled".to_owned(),
            ));
        }
        (false, TimeToLiveStatus::Disabled) => {
            return Err(DynamoDbError::ValidationException(
                "TimeToLive is already disabled".to_owned(),
            ));
        }
        (_, TimeToLiveStatus::Enabling | TimeToLiveStatus::Disabling) => {
            return Err(DynamoDbError::ValidationException(
                "TimeToLive is currently being modified".to_owned(),
            ));
        }
        _ => {}
    }

    ctx.storage
        .apply_ttl_update(
            &ctx.account_id,
            &input.table_name,
            &input.time_to_live_specification.attribute_name,
            input.time_to_live_specification.enabled,
        )
        .await
        .map_err(storage_to_dynamo)?;

    let output = UpdateTimeToLiveOutput {
        time_to_live_specification: TimeToLiveSpecificationOutput {
            attribute_name: input.time_to_live_specification.attribute_name,
            enabled: input.time_to_live_specification.enabled,
        },
    };
    serialize_output(&output)
}

/// Validate a TTL attribute name.
///
/// DynamoDB's `TimeToLiveSpecification.AttributeName` has no character-pattern
/// restriction; it is a UTF-8 string with a 1-255 byte bound. Backends that need
/// the name in DDL must escape it there instead of narrowing the API here.
fn validate_ttl_attribute_name(name: &str) -> Result<(), DynamoDbError> {
    if name.is_empty() || name.len() > 255 {
        return Err(DynamoDbError::ValidationException(
            "TimeToLiveSpecification.AttributeName must be between 1 and 255 UTF-8 bytes"
                .to_owned(),
        ));
    }
    if name.contains('\0') {
        return Err(DynamoDbError::ValidationException(
            "TimeToLiveSpecification.AttributeName contains an unsupported null character"
                .to_owned(),
        ));
    }
    Ok(())
}

fn storage_to_dynamo(e: StorageError) -> DynamoDbError {
    match e {
        StorageError::TableNotFound(_name) => {
            DynamoDbError::ResourceNotFoundException("Requested resource not found".to_string())
        }
        StorageError::TableNotActive(name) => {
            DynamoDbError::ResourceInUseException(format!("Table {name} is not in ACTIVE state"))
        }
        StorageError::Validation(message) => DynamoDbError::ValidationException(message),
        other => {
            tracing::error!(internal_error = %other, "storage internal error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ttl_attribute_names() {
        assert!(validate_ttl_attribute_name("ttl").is_ok());
        assert!(validate_ttl_attribute_name("TTL_field").is_ok());
        assert!(validate_ttl_attribute_name("my.ttl-attr").is_ok());
        assert!(validate_ttl_attribute_name("a").is_ok());
        assert!(validate_ttl_attribute_name("A0_.-z9").is_ok());
        assert!(validate_ttl_attribute_name("expires at").is_ok());
        assert!(validate_ttl_attribute_name("it's").is_ok());
        assert!(validate_ttl_attribute_name("a\"b").is_ok());
        assert!(validate_ttl_attribute_name("a\\b").is_ok());
        assert!(validate_ttl_attribute_name("a/b#c:d").is_ok());
        assert!(validate_ttl_attribute_name("过期时间").is_ok());
    }

    #[test]
    fn empty_name_rejected() {
        assert!(validate_ttl_attribute_name("").is_err());
    }

    #[test]
    fn too_long_name_rejected() {
        let long = "a".repeat(256);
        assert!(validate_ttl_attribute_name(&long).is_err());
    }

    #[test]
    fn multibyte_name_length_is_measured_in_utf8_bytes() {
        let max = "界".repeat(85);
        assert_eq!(max.len(), 255);
        assert!(validate_ttl_attribute_name(&max).is_ok());

        let too_long = "界".repeat(86);
        assert_eq!(too_long.len(), 258);
        assert!(validate_ttl_attribute_name(&too_long).is_err());
    }

    #[test]
    fn max_length_accepted() {
        let max = "a".repeat(255);
        assert!(validate_ttl_attribute_name(&max).is_ok());
    }

    #[test]
    fn null_byte_rejected() {
        assert!(validate_ttl_attribute_name("a\0b").is_err());
    }
}
