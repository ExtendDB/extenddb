// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TagResource`, `UntagResource`, and `ListTagsOfResource` operation handlers.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    ListTagsOfResourceInput, ListTagsOfResourceOutput, TagResourceInput, UntagResourceInput,
};
use serde_json::Value;

use crate::OperationContext;
use crate::sanitize_storage_error;
use crate::serialize_output;

/// Extract the table name from a `DynamoDB` table ARN.
///
/// Expected format: `arn:aws:dynamodb:{region}:{account}:table/{name}[/...]`
fn extract_table_name_from_arn(arn: &str) -> Option<&str> {
    let resource = arn.strip_prefix("arn:aws:dynamodb:")?.split(':').nth(2)?;
    let table_name = resource.strip_prefix("table/")?;
    // Strip any sub-resource (e.g. /index/foo, /stream/label)
    Some(table_name.split('/').next().unwrap_or(table_name))
}

/// Extract the account ID from a DynamoDB table ARN.
fn extract_account_from_arn(arn: &str) -> Option<&str> {
    arn.strip_prefix("arn:aws:dynamodb:")?.split(':').nth(1)
}

/// Validate that the ARN refers to an existing table.
///
/// Returns `ResourceNotFoundException` if the table does not exist.
async fn validate_resource_arn(arn: &str, ctx: &OperationContext) -> Result<(), DynamoDbError> {
    let table_name = extract_table_name_from_arn(arn).ok_or_else(|| {
        DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{arn}' at 'resourceArn' failed to satisfy constraint: \
             Member must satisfy regular expression pattern: arn:aws:dynamodb:.+"
        ))
    })?;

    // Check the ARN's account matches the caller's account.
    if let Some(arn_account) = extract_account_from_arn(arn) {
        if arn_account != ctx.account_id.as_ref() {
            return Err(DynamoDbError::AccessDeniedException(
                "Access is denied".to_owned(),
            ));
        }
    }

    // Verify the table exists via table_key_info (lightweight check).
    ctx.storage
        .table_key_info(&ctx.account_id, table_name)
        .await
        .map_err(|e| match e {
            extenddb_storage::error::StorageError::TableNotFound(_) => {
                DynamoDbError::ResourceNotFoundException(format!(
                    "Requested resource not found: {arn}"
                ))
            }
            other => sanitize_storage_error(other),
        })?;

    Ok(())
}

/// Handle `TagResource` — add or overwrite tags on a resource.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the resource does not exist.
/// Returns `ValidationException` if the resource ARN is empty.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_tag_resource(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: TagResourceInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    if input.resource_arn.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "ResourceArn must not be empty".to_owned(),
        ));
    }

    validate_resource_arn(&input.resource_arn, ctx).await?;

    ctx.storage
        .tag_resource(&input.resource_arn, &input.tags)
        .await
        .map_err(sanitize_storage_error)?;

    // Drop any cached resource-tag entry so the new tags are visible to
    // ABAC policy evaluation immediately.
    ctx.auth_cache
        .invalidate_resource_tags(&input.resource_arn)
        .await;

    // TagResource returns an empty body on success.
    Ok(Value::Object(serde_json::Map::new()))
}

/// Handle `UntagResource` — remove tags by key from a resource.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the resource does not exist.
/// Returns `ValidationException` if the resource ARN is empty.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_untag_resource(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: UntagResourceInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    if input.resource_arn.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "ResourceArn must not be empty".to_owned(),
        ));
    }

    validate_resource_arn(&input.resource_arn, ctx).await?;

    ctx.storage
        .untag_resource(&input.resource_arn, &input.tag_keys)
        .await
        .map_err(sanitize_storage_error)?;

    ctx.auth_cache
        .invalidate_resource_tags(&input.resource_arn)
        .await;

    // UntagResource returns an empty body on success.
    Ok(Value::Object(serde_json::Map::new()))
}

/// Handle `ListTagsOfResource` — list all tags for a resource.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the resource does not exist.
/// Returns `ValidationException` if the resource ARN is empty.
/// Returns `InternalServerError` on storage failures.
pub async fn handle_list_tags_of_resource(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: ListTagsOfResourceInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    if input.resource_arn.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "ResourceArn must not be empty".to_owned(),
        ));
    }

    validate_resource_arn(&input.resource_arn, ctx).await?;

    let tags = ctx
        .storage
        .list_tags(&input.resource_arn)
        .await
        .map_err(sanitize_storage_error)?;

    let output = ListTagsOfResourceOutput {
        tags,
        next_token: None, // All tags returned in one page.
    };
    serialize_output(&output)
}
