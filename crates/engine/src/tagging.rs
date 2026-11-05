// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TagResource`, `UntagResource`, and `ListTagsOfResource` operation handlers.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    ListTagsOfResourceInput, ListTagsOfResourceOutput, TagResourceInput, UntagResourceInput,
};
use extenddb_core::validation::{validate_tag_keys, validate_tags};
use serde_json::Value;

use crate::OperationContext;
use crate::sanitize_storage_error;
use crate::serialize_output;

/// True for a table ARN naming a backup, export, or import. The service reports
/// these as not a `DynamoDB` resource ARN on a tag operation (measured live).
fn tag_subresource_not_dynamodb_resource(resource: &str) -> bool {
    resource
        .strip_prefix("table/")
        .is_some_and(|tail| matches!(tail.split('/').nth(1), Some("backup" | "export" | "import")))
}

/// True for a `table/...` resource whose sub-resource is anything other than a
/// stream (an index or an unknown kind; a backup, export or import is refused
/// earlier by `tag_subresource_not_dynamodb_resource`). Tags are accepted only
/// on a table or a stream.
fn tag_subresource_not_table_or_stream(resource: &str) -> bool {
    let Some(tail) = resource.strip_prefix("table/") else {
        return false;
    };
    let mut segs = tail.split('/');
    let _table = segs.next();
    !matches!(segs.next(), None | Some("") | Some("stream"))
}

/// Extract the table name from a `DynamoDB` table ARN, using the one shared
/// resource parser ([`extenddb_core::arn::fields`]) so this layer splits an ARN
/// the same way authorization and the engine stream resolver do.
///
/// Expected format: `arn:aws:dynamodb:{region}:{account}:table/{name}[/...]`
fn extract_table_name_from_arn(arn: &str) -> Option<&str> {
    let resource = extenddb_core::arn::fields(arn)?.resource;
    let table_name = resource.strip_prefix("table/")?;
    // Strip any sub-resource (e.g. /index/foo, /stream/label).
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
/// | `DynamoDB` ARN whose resource is not a table | `ValidationException` (not a `DynamoDB` resource arn) |
/// | Non-`DynamoDB`-service ARN whose resource is not a table | `AccessDeniedException` |
/// | Table ARN naming a backup, export, or import | `ValidationException` (not a `DynamoDB` resource arn) |
/// | Table ARN naming an index or other sub-resource | `ValidationException` (only table or stream is accepted) |
/// | Account differs from the caller's | `AccessDeniedException` |
/// | Table ARN with a different partition, service, or region | `ValidationException` (invalid `TableArn`) |
/// | Table does not exist | `ResourceNotFoundException` |
///
/// Account is checked before the partition, service, and region, matching the
/// service: a cross-account ARN is denied without revealing whether its other
/// fields are valid. For a table ARN, a no-permissions principal is denied by
/// authorization before this runs; the partition, service, region, and
/// existence errors therefore surface only for an authorized principal.
///
/// The two halves are called separately by the tag-writing handlers, because
/// tag keys and values are checked between them: a request carrying an invalid
/// tag key is refused for the tag on a cross-region ARN and on an ARN naming no
/// existing table, and denied on a cross-account ARN (measured).
async fn validate_resource_arn(arn: &str, ctx: &OperationContext) -> Result<(), DynamoDbError> {
    validate_arn_shape_and_account(arn, ctx)?;
    validate_arn_region_and_table(arn, ctx).await
}

/// First half of [`validate_resource_arn`]: the ARN prefix, its resource kind,
/// and the account.
fn validate_arn_shape_and_account(arn: &str, ctx: &OperationContext) -> Result<(), DynamoDbError> {
    if !arn.starts_with("arn:") {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: ARNs must start with 'arn:': {arn}"
        )));
    }

    // A string that is not shaped like an ARN (fewer than the five
    // colon-separated fields) is denied rather than reported as malformed.
    let Some(extenddb_core::arn::ArnFields {
        service,
        account,
        resource,
        ..
    }) = extenddb_core::arn::fields(arn)
    else {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    };

    if !resource.starts_with("table/") {
        // A DynamoDB ARN whose resource is not a table (a bare index, an
        // export, and so on) is reported as not a DynamoDB resource. An ARN for
        // another service is denied rather than reported as malformed, as the
        // service does.
        if service == "dynamodb" {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: \
                 Provided Arn is not a DynamoDB resource arn: {arn}"
            )));
        }
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }

    if tag_subresource_not_dynamodb_resource(resource) {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: \
             Provided Arn is not a DynamoDB resource arn: {arn}"
        )));
    }

    // Tags are accepted only on a table or a stream; an index or other
    // sub-resource is rejected.
    if tag_subresource_not_table_or_stream(resource) {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Invalid resource arn \
             provided, only table or stream is accepted. Provided resource arn: {arn}"
        )));
    }

    if account != ctx.account_id.as_ref() {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }

    Ok(())
}

/// Second half of [`validate_resource_arn`]: the partition, service and region,
/// then the table's existence.
async fn validate_arn_region_and_table(
    arn: &str,
    ctx: &OperationContext,
) -> Result<(), DynamoDbError> {
    let Some(extenddb_core::arn::ArnFields {
        partition,
        service,
        region,
        ..
    }) = extenddb_core::arn::fields(arn)
    else {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    };

    if partition != "aws" || service != "dynamodb" || region != ctx.region.as_ref() {
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
/// Returns `ValidationException` if the resource ARN is empty or a tag key or
/// value is outside the grammar the service accepts.
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

    validate_arn_shape_and_account(&input.resource_arn, ctx)?;
    validate_tags(&input.tags)?;
    validate_arn_region_and_table(&input.resource_arn, ctx).await?;

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
/// Returns `ValidationException` if the resource ARN is empty or a tag key is
/// outside the grammar the service accepts.
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

    validate_arn_shape_and_account(&input.resource_arn, ctx)?;
    validate_tag_keys(&input.tag_keys)?;
    validate_arn_region_and_table(&input.resource_arn, ctx).await?;

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

#[cfg(test)]
mod tests {
    use super::{tag_subresource_not_dynamodb_resource, tag_subresource_not_table_or_stream};

    #[test]
    fn tag_subresource_classification() {
        for r in [
            "table/t/backup/01-ab",
            "table/t/export/01-ab",
            "table/t/import/01-ab",
        ] {
            assert!(tag_subresource_not_dynamodb_resource(r), "{r}");
        }
        for r in [
            "table/t",
            "table/t/stream/2026-10-20T16:17:40.188",
            "table/t/index/g",
        ] {
            assert!(!tag_subresource_not_dynamodb_resource(r), "{r}");
        }
        assert!(tag_subresource_not_table_or_stream("table/t/index/g"));
        assert!(!tag_subresource_not_table_or_stream("table/t"));
        assert!(!tag_subresource_not_table_or_stream(
            "table/t/stream/2026-10-20T16:17:40.188"
        ));
    }
}
