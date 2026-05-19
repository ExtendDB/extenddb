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
/// When enabling, kicks off creation of a `PostgreSQL` expression index on the
/// TTL attribute. When disabling, marks TTL disabled (sweeper stops) then
/// drops the index.
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

    let already_enabled = current.time_to_live_status == TimeToLiveStatus::Enabled;
    if input.time_to_live_specification.enabled && already_enabled {
        return Err(DynamoDbError::ValidationException(
            "TimeToLive is already enabled".to_owned(),
        ));
    }
    if !input.time_to_live_specification.enabled && !already_enabled {
        return Err(DynamoDbError::ValidationException(
            "TimeToLive is already disabled".to_owned(),
        ));
    }

    ctx.storage
        .update_ttl(
            &ctx.account_id,
            &input.table_name,
            &input.time_to_live_specification.attribute_name,
            input.time_to_live_specification.enabled,
        )
        .await
        .map_err(storage_to_dynamo)?;

    if input.time_to_live_specification.enabled {
        // Kick off index creation (CONCURRENTLY — non-blocking for other database
        // operations on the table, but the handler awaits completion).
        // If it fails, the TTL sweeper will retry on its next cycle.
        let account_id = ctx.account_id.clone();
        let table_name = input.table_name.clone();
        let attr = input.time_to_live_specification.attribute_name.clone();
        if let Err(e) = ctx
            .storage
            .create_ttl_index(&account_id, &table_name, &attr)
            .await
        {
            tracing::warn!("TTL index creation deferred for {table_name}: {e}");
        }
    } else {
        // Disable path: metadata already updated (sweeper won't pick up this table).
        // Drop the index. Safe because sweeper checks ttl_index_ready which is now FALSE.
        if let Err(e) = ctx
            .storage
            .drop_ttl_index(&ctx.account_id, &input.table_name)
            .await
        {
            tracing::warn!("TTL index drop failed for {}: {e}", input.table_name);
        }
    }

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
/// Real `DynamoDB` allows any UTF-8 (1–255 bytes). However, the TTL attribute
/// name is interpolated into `PostgreSQL` DDL (expression index creation) where
/// parameterized queries are not possible. We use a strict allowlist:
/// `^[a-zA-Z0-9._-]+$` (1–255 bytes). This eliminates the entire class of
/// SQL injection risk. See `docs/differences-from-dynamodb.md`.
///
/// Defense-in-depth per `docs/adr/sql-injection-defense.md`.
fn validate_ttl_attribute_name(name: &str) -> Result<(), DynamoDbError> {
    if name.is_empty() || name.len() > 255 {
        return Err(DynamoDbError::ValidationException(
            "TimeToLiveSpecification.AttributeName must be between 1 and 255 characters".to_owned(),
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        return Err(DynamoDbError::ValidationException(
            "TimeToLiveSpecification.AttributeName contains invalid characters".to_owned(),
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
    fn max_length_accepted() {
        let max = "a".repeat(255);
        assert!(validate_ttl_attribute_name(&max).is_ok());
    }

    #[test]
    fn special_chars_rejected() {
        assert!(validate_ttl_attribute_name("it's").is_err());
        assert!(validate_ttl_attribute_name("a\"b").is_err());
        assert!(validate_ttl_attribute_name("a\\b").is_err());
        assert!(validate_ttl_attribute_name("a\0b").is_err());
        assert!(validate_ttl_attribute_name("a b").is_err());
        assert!(validate_ttl_attribute_name("a/b").is_err());
        assert!(validate_ttl_attribute_name("a#b").is_err());
        assert!(validate_ttl_attribute_name("a:b").is_err());
        assert!(validate_ttl_attribute_name("café").is_err());
    }
}
