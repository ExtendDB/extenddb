// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde_json::Value;

use extenddb_core::error::{DynamoDbError, ErrorMessageKey, error_message};
use extenddb_core::types::{CreateTableInput, CreateTableOutput};
use extenddb_core::validation::validate_create_table;

use crate::OperationContext;
use crate::serialize_output;

pub async fn handle_create_table(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    crate::validate_enum_fields(
        &body,
        &[(
            "BillingMode",
            "billingMode",
            &["PROVISIONED", "PAY_PER_REQUEST"],
        )],
    )?;

    let input: CreateTableInput = serde_json::from_value(body).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("validation error detected")
            || msg.contains("parameter values were invalid")
            || msg.contains("must not be empty")
            || msg.contains("Syntax error; key")
            || msg.contains("AttributeValue is empty")
            || msg.contains("AttributeValue has more than one datatypes set")
        {
            DynamoDbError::ValidationException(msg)
        } else if msg.contains("missing field") && msg.contains("TableName") {
            DynamoDbError::ValidationException(
                "The parameter 'TableName' is required but was not present in the request"
                    .to_owned(),
            )
        } else {
            DynamoDbError::SerializationException(format!(
                "Start of structure or map found where not expected: {e}"
            ))
        }
    })?;

    validate_create_table(&input, &ctx.limits)?;

    let table_name = input.table_name.clone();
    let table_desc = ctx
        .storage
        .create_table(&ctx.account_id, input)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Drop any cached TableKeyInfo (typically a negative-cached "not found"
    // from a prior describe attempt) so requests against the new table see
    // it immediately.
    ctx.auth_cache
        .invalidate_table_key_info(&ctx.account_id, &table_name)
        .await;

    // The CreateTable request itself ran through authorize_request, which
    // populates resource_tags for this ARN. At that point the tags row didn't
    // exist yet, so the cache holds an empty TagMap. Drop it so subsequent
    // ABAC evaluations see the tags supplied via CreateTable.Tags.
    let arn = format!(
        "arn:aws:dynamodb:{}:{}:table/{}",
        ctx.region, ctx.account_id, table_name
    );
    ctx.auth_cache.invalidate_resource_tags(&arn).await;

    let output = CreateTableOutput {
        table_description: table_desc,
    };
    serialize_output(&output)
}

pub(crate) fn storage_err_to_dynamo(e: extenddb_storage::error::StorageError) -> DynamoDbError {
    use extenddb_storage::error::StorageError;
    match e {
        StorageError::TableNotFound(name) => DynamoDbError::ResourceNotFoundException(
            error_message(ErrorMessageKey::TableNotFound, &[&name]),
        ),
        StorageError::TableAlreadyExists(name) => DynamoDbError::ResourceInUseException(
            error_message(ErrorMessageKey::TableAlreadyExists, &[&name]),
        ),
        StorageError::TableNotActive(name) => DynamoDbError::ResourceInUseException(error_message(
            ErrorMessageKey::TableInUse,
            &[&name],
        )),
        StorageError::IndexNotFound(name) => DynamoDbError::ValidationException(format!(
            "The table does not have the specified index: {name}"
        )),
        StorageError::IndexAlreadyExists(name) => DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Index already exists: {name}"
        )),
        StorageError::DeletionProtected(arn) => DynamoDbError::ValidationException(format!(
            "Resource '{arn}' cannot be deleted as it is currently protected against deletion. Disable deletion protection first then try again."
        )),
        StorageError::Connection(msg) => {
            tracing::error!(internal_error = %msg, "storage connection error");
            DynamoDbError::ServiceUnavailable("Service is temporarily unavailable".to_owned())
        }
        StorageError::CatalogVersionMismatch { expected, found } => {
            tracing::error!("Catalog version mismatch: expected {expected}, found {found}");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
        StorageError::CatalogNotInitialized => {
            tracing::error!("Catalog not initialized");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
        // Generic path: discard the old item (callers that need it use
        // `storage_err_to_dynamo_with_ccf` instead).
        StorageError::ConditionFailed(_) => DynamoDbError::ConditionalCheckFailedException(
            "The conditional request failed".to_owned(),
            None,
        ),
        StorageError::TransactionCanceled(reasons) => {
            let reason_strs: Vec<String> = reasons.iter().map(|r| r.code.clone()).collect();
            DynamoDbError::TransactionCanceledException {
                message: format!(
                    "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
                    reason_strs.join(", ")
                ),
                cancellation_reasons: reasons,
            }
        }
        StorageError::Validation(msg) => DynamoDbError::ValidationException(msg),
        StorageError::NoOpUpdate(msg) => DynamoDbError::ValidationException(msg),
        StorageError::IdempotentReplay | StorageError::IdempotentMismatch => {
            // These are handled directly by the transact_write_items caller.
            // If they reach here, it's a programming error.
            tracing::error!("Unexpected idempotency error in generic error handler");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
        StorageError::Internal(msg) => {
            // Log the raw message for debugging but do not expose storage
            // backend details (e.g. PostgreSQL error text) to the client.
            // REQ-ERR: tenet 4 — only DynamoDB-shaped errors cross the wire.
            tracing::error!(internal_error = %msg, "storage internal error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

/// Like [`storage_err_to_dynamo`], but includes the old item in
/// `ConditionalCheckFailedException` when `ReturnValuesOnConditionCheckFailure`
/// is `ALL_OLD`.
pub(crate) fn storage_err_to_dynamo_with_ccf(
    e: extenddb_storage::error::StorageError,
    ccf: extenddb_core::types::ReturnValuesOnConditionCheckFailure,
) -> DynamoDbError {
    use extenddb_core::types::ReturnValuesOnConditionCheckFailure;
    use extenddb_storage::error::StorageError;
    match e {
        StorageError::ConditionFailed(item) => {
            let return_item = if ccf == ReturnValuesOnConditionCheckFailure::AllOld {
                item
            } else {
                None
            };
            DynamoDbError::ConditionalCheckFailedException(
                "The conditional request failed".to_owned(),
                return_item,
            )
        }
        other => storage_err_to_dynamo(other),
    }
}
