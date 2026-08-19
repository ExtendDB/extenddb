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

/// Split a `DynamoDB` resource ARN into `(region, account, resource)`.
///
/// Expected format: `arn:aws:dynamodb:{region}:{account}:table/{name}[/...]`
fn split_dynamodb_arn(arn: &str) -> Option<(&str, &str, &str)> {
    let rest = arn.strip_prefix("arn:aws:dynamodb:")?;
    let mut parts = rest.splitn(3, ':');
    let region = parts.next()?;
    let account = parts.next()?;
    let resource = parts.next()?;
    Some((region, account, resource))
}

/// Extract the table name from a `DynamoDB` table ARN.
///
/// Expected format: `arn:aws:dynamodb:{region}:{account}:table/{name}[/...]`
fn extract_table_name_from_arn(arn: &str) -> Option<&str> {
    let (_, _, resource) = split_dynamodb_arn(arn)?;
    let table_name = resource.strip_prefix("table/")?;
    // Strip any sub-resource (e.g. /index/foo, /stream/label)
    Some(table_name.split('/').next().unwrap_or(table_name))
}

/// Validate that the ARN refers to an existing table owned by the caller.
///
/// The error classes and messages below match `DynamoDB`, verified against the
/// service:
///
/// | Input | Result |
/// |---|---|
/// | Does not start with `arn:` | `ValidationException` (ARNs must start with `arn:`) |
/// | Valid ARN for another service | `AccessDeniedException` |
/// | `DynamoDB` ARN naming a non-table resource | `ValidationException` (not a `DynamoDB` resource arn) |
/// | Account differs from the caller's | `AccessDeniedException` |
/// | Region differs from this deployment's | `ValidationException` (invalid `TableArn`) |
/// | Table does not exist | `ResourceNotFoundException` |
///
/// Account is checked before region, matching the service: a cross-account ARN
/// is denied without revealing whether its region is valid.
async fn validate_resource_arn(arn: &str, ctx: &OperationContext) -> Result<(), DynamoDbError> {
    if !arn.starts_with("arn:") {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: ARNs must start with 'arn:': {arn}"
        )));
    }

    // An ARN for another service is denied rather than reported as malformed:
    // authorization is evaluated against the resource before its shape is
    // inspected further.
    let Some((arn_region, arn_account, resource)) = split_dynamodb_arn(arn) else {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    };

    if !resource.starts_with("table/") {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: \
             Provided Arn is not a DynamoDB resource arn: {arn}"
        )));
    }

    if arn_account != ctx.account_id.as_ref() {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }

    if arn_region != ctx.region.as_ref() {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid TableArn: Invalid ResourceArn provided as input {arn}"
        )));
    }

    let table_name = extract_table_name_from_arn(arn).ok_or_else(|| {
        DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: \
             Provided Arn is not a DynamoDB resource arn: {arn}"
        ))
    })?;

    // Verify the table exists via table_key_info (lightweight check).
    ctx.storage
        .table_key_info(&ctx.account_id, table_name)
        .await
        .map_err(|e| match e {
            extenddb_storage::error::StorageError::TableNotFound(_) => {
                DynamoDbError::ResourceNotFoundException(format!(
                    "Requested resource not found: ResourceArn: {arn} not found"
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
