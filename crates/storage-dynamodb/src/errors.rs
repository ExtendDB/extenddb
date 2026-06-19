// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Maps AWS SDK DynamoDB errors to ExtendDB's `StorageError`.

use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_smithy_runtime_api::client::result::SdkError;
use extenddb_storage::error::StorageError;

/// Map a DynamoDB error code and message string to the appropriate [`StorageError`] variant.
///
/// This is a pure function — no SDK types required — and is the primary unit-tested surface.
pub fn classify(error_code: &str, message: &str) -> StorageError {
    match error_code {
        "ConditionalCheckFailedException" => StorageError::ConditionFailed(None),
        "ResourceNotFoundException" => StorageError::TableNotFound(message.to_string()),
        "ResourceInUseException" => StorageError::TableAlreadyExists(message.to_string()),
        "TransactionCanceledException" | "TransactionConflictException" => {
            StorageError::TransactionCanceled(vec![])
        }
        "ValidationException" => StorageError::Validation(message.to_string()),
        "ProvisionedThroughputExceededException"
        | "RequestLimitExceeded"
        | "ThrottlingException" => StorageError::Internal(format!("throttled: {message}")),
        _ => StorageError::Internal(format!("{error_code}: {message}")),
    }
}

/// Convert a generic AWS SDK [`SdkError`] into a [`StorageError`].
///
/// Service errors are routed through [`classify`] using the error code and message extracted via
/// [`ProvideErrorMetadata`]. All other `SdkError` variants (dispatch failures, timeouts,
/// construction errors, response parse errors) are mapped to [`StorageError::Connection`].
pub fn from_sdk_error<E, R>(err: SdkError<E, R>) -> StorageError
where
    E: ProvideErrorMetadata,
{
    match err {
        SdkError::ServiceError(context) => {
            let source = context.into_err();
            let code = source.code().unwrap_or("Unknown");
            let message = source.message().unwrap_or("");
            classify(code, message)
        }
        other => StorageError::Connection(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditional_check_failed_maps_to_condition_failed() {
        assert!(matches!(
            classify("ConditionalCheckFailedException", ""),
            StorageError::ConditionFailed(None)
        ));
    }

    #[test]
    fn resource_not_found_maps_to_table_not_found() {
        assert!(matches!(
            classify("ResourceNotFoundException", "Requested resource not found"),
            StorageError::TableNotFound(_)
        ));
    }

    #[test]
    fn resource_in_use_maps_to_table_already_exists() {
        assert!(matches!(
            classify("ResourceInUseException", "x"),
            StorageError::TableAlreadyExists(_)
        ));
    }

    #[test]
    fn validation_maps_to_validation() {
        assert!(matches!(
            classify("ValidationException", "bad"),
            StorageError::Validation(_)
        ));
    }

    #[test]
    fn transaction_canceled_maps_to_transaction_canceled() {
        assert!(matches!(
            classify("TransactionCanceledException", ""),
            StorageError::TransactionCanceled(_)
        ));
    }

    #[test]
    fn throttle_maps_to_internal() {
        assert!(matches!(
            classify("ThrottlingException", "slow down"),
            StorageError::Internal(_)
        ));
    }

    #[test]
    fn unknown_maps_to_internal() {
        assert!(matches!(
            classify("SomethingNew", "msg"),
            StorageError::Internal(_)
        ));
    }
}
