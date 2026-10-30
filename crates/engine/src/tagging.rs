// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TagResource`, `UntagResource`, and `ListTagsOfResource` operation handlers.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    ListTagsOfResourceInput, ListTagsOfResourceOutput, TableStatus, TagResourceInput,
    UntagResourceInput,
};
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

/// True for a stream ARN, `table/<name>/stream/<label>`. A stream's tags belong
/// to the stream, which stays describable after its table is deleted, so the
/// table's `DELETING` refusal does not apply to them.
fn arn_names_stream(arn: &str) -> bool {
    extenddb_core::arn::fields(arn)
        .and_then(|f| f.resource.strip_prefix("table/"))
        .is_some_and(|tail| tail.split('/').nth(1) == Some("stream"))
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

/// Validate that the ARN refers to an existing table owned by the caller, and
/// report that table's catalog status so the caller can decide on it.
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
async fn validate_resource_arn<'a>(
    arn: &'a str,
    ctx: &OperationContext,
) -> Result<(TableStatus, &'a str), DynamoDbError> {
    if !arn.starts_with("arn:") {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: ARNs must start with 'arn:': {arn}"
        )));
    }

    // A string that is not shaped like an ARN (fewer than the five
    // colon-separated fields) is denied rather than reported as malformed.
    let Some(extenddb_core::arn::ArnFields {
        partition,
        service,
        region,
        account,
        resource,
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

    // The table's status, which TableKeyInfo does not carry and the key-info
    // read cannot supply for a DELETING row on MongoDB and Cassandra (it
    // reports one as not found). The status read comes first so every backend
    // reaches the same answer for the window, for a table ARN and for a stream
    // ARN alike; every other status is left to the key-info read below, which
    // already classifies it.
    let status = crate::table_lifecycle::read_table_status(table_name, ctx)
        .await
        .map_err(|e| arn_storage_error(e, arn))?;
    if status == TableStatus::Deleting {
        return Ok((status, table_name));
    }

    // Verify the table exists via table_key_info (lightweight check).
    ctx.storage
        .table_key_info(&ctx.account_id, table_name)
        .await
        .map_err(|e| arn_storage_error(e, arn))?;

    Ok((status, table_name))
}

/// Report a storage error from the table lookup behind a resource ARN: a
/// missing table names the ARN, as the service does, and anything else is a
/// fault.
fn arn_storage_error(e: extenddb_storage::error::StorageError, arn: &str) -> DynamoDbError {
    match e {
        extenddb_storage::error::StorageError::TableNotFound(_) => {
            DynamoDbError::ResourceNotFoundException(format!(
                "Requested resource not found: ResourceArn: {arn} not found"
            ))
        }
        other => sanitize_storage_error(other),
    }
}

/// Refuse a tag write on a table ARN while the table is being deleted.
///
/// `TagResource` and `UntagResource` on a table ARN during the table's
/// `DELETING` window answer 400 `ResourceInUseException` with the `Table is
/// being deleted` sentence, and `ListTagsOfResource` is served with the tags
/// (measured 2026-10-25). On the table's stream ARN both writes are served in
/// the same window (measured 2026-10-26), so `arn` decides. A write admitted
/// on the table ARN in the window would land an unconditioned tag row that can
/// outlive the table, since the delete removes the ARN's tag rows before the
/// catalog row. The refusal removes those admissions; a write whose status
/// read came before the delete can still land after the delete removed the
/// tag rows, as it could before.
fn refuse_tag_write_while_deleting(
    status: TableStatus,
    table_name: &str,
    arn: &str,
) -> Result<(), DynamoDbError> {
    if status == TableStatus::Deleting && !arn_names_stream(arn) {
        return Err(crate::table_lifecycle::being_deleted(table_name));
    }
    Ok(())
}

/// Handle `TagResource` — add or overwrite tags on a resource.
///
/// # Errors
///
/// Returns `ResourceNotFoundException` if the resource does not exist.
/// Returns `ResourceInUseException` if the ARN names a table that is being
/// deleted.
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

    let (status, table_name) = validate_resource_arn(&input.resource_arn, ctx).await?;
    refuse_tag_write_while_deleting(status, table_name, &input.resource_arn)?;

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
/// Returns `ResourceInUseException` if the ARN names a table that is being
/// deleted.
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

    let (status, table_name) = validate_resource_arn(&input.resource_arn, ctx).await?;
    refuse_tag_write_while_deleting(status, table_name, &input.resource_arn)?;

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

    // Served while the table is DELETING, with the tags, as the service serves
    // it (measured 2026-10-25): the read is not a change to the resource.
    let (_status, _table_name) = validate_resource_arn(&input.resource_arn, ctx).await?;

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
    use super::{
        arn_storage_error, refuse_tag_write_while_deleting, tag_subresource_not_dynamodb_resource,
        tag_subresource_not_table_or_stream,
    };
    use extenddb_core::error::DynamoDbError;
    use extenddb_core::types::TableStatus;
    use extenddb_storage::error::StorageError;

    const TABLE_ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/t";
    const STREAM_ARN: &str =
        "arn:aws:dynamodb:us-east-1:123456789012:table/t/stream/2026-10-26T00:13:40.188";

    /// A tag write on the table ARN is refused while the table is DELETING,
    /// with the service's sentence, and admitted in every other status.
    #[test]
    fn a_tag_write_is_refused_only_while_deleting() {
        match refuse_tag_write_while_deleting(TableStatus::Deleting, "t", TABLE_ARN).unwrap_err() {
            DynamoDbError::ResourceInUseException(msg) => assert_eq!(
                msg,
                "Attempt to change a resource which is still in use: Table is being deleted: t"
            ),
            other => panic!("expected ResourceInUseException, got {other:?}"),
        }
        for status in [
            TableStatus::Active,
            TableStatus::Creating,
            TableStatus::Updating,
        ] {
            assert!(
                refuse_tag_write_while_deleting(status, "t", TABLE_ARN).is_ok(),
                "{status:?} must keep its existing answer"
            );
        }
    }

    /// A tag write on the table's stream ARN is admitted in every status,
    /// DELETING included: the service serves both writes there during the
    /// window (measured 2026-10-26).
    #[test]
    fn a_stream_arn_tag_write_is_admitted_while_deleting() {
        for status in [
            TableStatus::Deleting,
            TableStatus::Active,
            TableStatus::Creating,
            TableStatus::Updating,
        ] {
            assert!(
                refuse_tag_write_while_deleting(status, "t", STREAM_ARN).is_ok(),
                "{status:?} on the stream ARN must be admitted"
            );
        }
    }

    /// A missing table behind a resource ARN names the ARN; anything else is a
    /// fault, not a client error.
    #[test]
    fn a_missing_table_behind_an_arn_names_the_arn() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/t";
        match arn_storage_error(StorageError::TableNotFound("t".to_owned()), arn) {
            DynamoDbError::ResourceNotFoundException(msg) => assert_eq!(
                msg,
                format!("Requested resource not found: ResourceArn: {arn} not found")
            ),
            other => panic!("expected ResourceNotFoundException, got {other:?}"),
        }
        assert!(matches!(
            arn_storage_error(StorageError::Internal("boom".to_owned()), arn),
            DynamoDbError::InternalServerError(_)
        ));
    }

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
