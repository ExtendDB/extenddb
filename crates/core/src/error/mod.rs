// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
mod messages;

pub use messages::{ErrorMessageKey, error_message, validation_error, validation_errors};

use crate::types::{CancellationReason, Item};

/// All Virtual `DynamoDB` error types with HTTP status codes.
///
/// REQ-ERR-001 through REQ-ERR-004: error JSON format, status codes, and SDK retry behavior.
/// SP-ERR-002: HTTP status codes match the real DynamoDB error catalog exactly.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum DynamoDbError {
    #[error("{0}")]
    ValidationException(String),
    #[error("{0}")]
    ResourceNotFoundException(String),
    #[error("{0}")]
    ResourceInUseException(String),
    #[error("{0}")]
    ConditionalCheckFailedException(String, Option<Item>),
    #[error("{message}")]
    TransactionCanceledException {
        /// Human-readable message.
        message: String,
        /// Per-item cancellation reasons, ordered by request position.
        cancellation_reasons: Vec<CancellationReason>,
    },
    /// Returned when a `ClientRequestToken` is reused with different parameters.
    #[error("{0}")]
    IdempotentParameterMismatchException(String),
    #[error("{0}")]
    SerializationException(String),
    /// SP-ERR-002: HTTP 404.
    #[error("{0}")]
    UnknownOperationException(String),
    #[error("{0}")]
    InternalServerError(String),
    #[error("{0}")]
    AccessDeniedException(String),
    /// SP-ERR-002: HTTP 403.
    #[error("{0}")]
    MissingAuthenticationToken(String),
    /// SP-ERR-002: HTTP 403.
    #[error("{0}")]
    IncompleteSignature(String),
    /// SP-ERR-002: HTTP 403.
    #[error("{0}")]
    UnrecognizedClientException(String),
    /// Returned when session credentials (ASIA*) have expired.
    #[error("{0}")]
    ExpiredTokenException(String),
    /// Returned when a shard iterator has expired (older than 15 minutes).
    #[error("{0}")]
    ExpiredIteratorException(String),
    #[error("{0}")]
    ServiceUnavailable(String),
    /// SP-ERR-002: Concurrent modification of the same item in a transaction.
    #[error("{0}")]
    TransactionConflictException(String),
    /// SP-ERR-002: Throughput exceeded.
    #[error("{0}")]
    ProvisionedThroughputExceededException(String),
    /// SP-ERR-002: General throttling.
    #[error("{0}")]
    ThrottlingException(String),
    /// SP-ERR-002: Per-account RPS cap.
    #[error("{0}")]
    RequestLimitExceeded(String),
    /// SP-ERR-002 / SP-WIRE-007: Request body exceeds 16 MB. HTTP 413.
    #[error("{0}")]
    RequestEntityTooLargeException(String),
    /// SP-ERR-002: Malformed HTTP request (e.g. bad gzip).
    #[error("{0}")]
    MalformedHttpRequestException(String),
    /// SP-ERR-002: Idempotent transaction still in progress.
    #[error("{0}")]
    TransactionInProgressException(String),
    /// SP-ERR-002: LSI collection size exceeded.
    #[error("{0}")]
    ItemCollectionSizeLimitExceededException(String),
    /// SP-ERR-002: Per-request timeout. HTTP 408.
    #[error("{0}")]
    RequestTimeoutException(String),
    /// SP-ERR-002: SigV4 signature mismatch. HTTP 403.
    #[error("{0}")]
    InvalidSignatureException(String),
}

impl DynamoDbError {
    /// HTTP status code for this error.
    ///
    /// SP-ERR-002: status codes match the real DynamoDB error catalog.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::ValidationException(_)
            | Self::ResourceNotFoundException(_)
            | Self::ResourceInUseException(_)
            | Self::ConditionalCheckFailedException(..)
            | Self::TransactionCanceledException { .. }
            | Self::IdempotentParameterMismatchException(_)
            | Self::SerializationException(_)
            | Self::AccessDeniedException(_)
            | Self::ExpiredTokenException(_)
            | Self::ExpiredIteratorException(_)
            | Self::TransactionConflictException(_)
            | Self::ProvisionedThroughputExceededException(_)
            | Self::ThrottlingException(_)
            | Self::RequestLimitExceeded(_)
            | Self::MalformedHttpRequestException(_)
            | Self::TransactionInProgressException(_)
            | Self::ItemCollectionSizeLimitExceededException(_)
            // SP-ERR-002: real DynamoDB returns 400 for invalid/unknown credentials.
            | Self::UnrecognizedClientException(_)
            | Self::InvalidSignatureException(_)
            // SP-ERR-002: DynamoDB returns 400 for missing/incomplete auth
            // (unlike IAM which uses 403). Verified against real DynamoDB.
            | Self::MissingAuthenticationToken(_)
            | Self::IncompleteSignature(_)
            // SP-ERR-002: unknown operation is 400 in DynamoDB.
            | Self::UnknownOperationException(_) => 400,
            // SP-ERR-002: request timeout is 408
            Self::RequestTimeoutException(_) => 408,
            // SP-ERR-002: body too large is 413
            Self::RequestEntityTooLargeException(_) => 413,
            Self::InternalServerError(_) => 500,
            Self::ServiceUnavailable(_) => 503,
        }
    }

    /// Error type string for the `__type` field (without prefix).
    #[must_use]
    pub fn error_type(&self) -> &str {
        match self {
            Self::ValidationException(_) => "ValidationException",
            Self::ResourceNotFoundException(_) => "ResourceNotFoundException",
            Self::ResourceInUseException(_) => "ResourceInUseException",
            Self::ConditionalCheckFailedException(..) => "ConditionalCheckFailedException",
            Self::TransactionCanceledException { .. } => "TransactionCanceledException",
            Self::IdempotentParameterMismatchException(_) => "IdempotentParameterMismatchException",
            Self::SerializationException(_) => "SerializationException",
            Self::UnknownOperationException(_) => "UnknownOperationException",
            Self::InternalServerError(_) => "InternalServerError",
            Self::AccessDeniedException(_) => "AccessDeniedException",
            Self::MissingAuthenticationToken(_) => "MissingAuthenticationToken",
            Self::IncompleteSignature(_) => "IncompleteSignature",
            Self::UnrecognizedClientException(_) => "UnrecognizedClientException",
            Self::ExpiredTokenException(_) => "ExpiredTokenException",
            Self::ExpiredIteratorException(_) => "ExpiredIteratorException",
            Self::ServiceUnavailable(_) => "ServiceUnavailable",
            Self::TransactionConflictException(_) => "TransactionConflictException",
            Self::ProvisionedThroughputExceededException(_) => {
                "ProvisionedThroughputExceededException"
            }
            Self::ThrottlingException(_) => "ThrottlingException",
            Self::RequestLimitExceeded(_) => "RequestLimitExceeded",
            Self::RequestEntityTooLargeException(_) => "RequestEntityTooLargeException",
            Self::MalformedHttpRequestException(_) => "MalformedHttpRequestException",
            Self::TransactionInProgressException(_) => "TransactionInProgressException",
            Self::ItemCollectionSizeLimitExceededException(_) => {
                "ItemCollectionSizeLimitExceededException"
            }
            Self::RequestTimeoutException(_) => "RequestTimeoutException",
            Self::InvalidSignatureException(_) => "InvalidSignatureException",
        }
    }

    /// Full `__type` value with wire format prefix.
    ///
    /// REQ-ERR-001: Real `DynamoDB` uses different prefixes per error variant.
    /// SDKs strip the prefix at `#`, so only raw HTTP tests observe the difference.
    ///
    /// - `ValidationException` → `com.amazon.coral.validate#`
    /// - Infrastructure/auth errors → `com.amazon.coral.service#`
    ///   (`SerializationException`, `UnknownOperationException`,
    ///   `MissingAuthenticationToken`, `IncompleteSignature`,
    ///   `InvalidSignatureException`)
    /// - All other `DynamoDB` errors → `com.amazonaws.dynamodb.v20120810#`
    ///
    /// Verified against real DynamoDB 2026-05-04.
    #[must_use]
    pub fn full_error_type(&self) -> String {
        let prefix = match self {
            Self::ValidationException(_) => "com.amazon.coral.validate#",
            Self::SerializationException(_)
            | Self::UnknownOperationException(_)
            | Self::MissingAuthenticationToken(_)
            | Self::IncompleteSignature(_)
            | Self::InvalidSignatureException(_) => "com.amazon.coral.service#",
            _ => "com.amazonaws.dynamodb.v20120810#",
        };
        format!("{prefix}{}", self.error_type())
    }

    /// The error message (the inner string).
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::ValidationException(m)
            | Self::ResourceNotFoundException(m)
            | Self::ResourceInUseException(m)
            | Self::ConditionalCheckFailedException(m, _)
            | Self::IdempotentParameterMismatchException(m)
            | Self::SerializationException(m)
            | Self::UnknownOperationException(m)
            | Self::InternalServerError(m)
            | Self::AccessDeniedException(m)
            | Self::MissingAuthenticationToken(m)
            | Self::IncompleteSignature(m)
            | Self::UnrecognizedClientException(m)
            | Self::ExpiredTokenException(m)
            | Self::ExpiredIteratorException(m)
            | Self::ServiceUnavailable(m)
            | Self::TransactionConflictException(m)
            | Self::ProvisionedThroughputExceededException(m)
            | Self::ThrottlingException(m)
            | Self::RequestLimitExceeded(m)
            | Self::RequestEntityTooLargeException(m)
            | Self::MalformedHttpRequestException(m)
            | Self::TransactionInProgressException(m)
            | Self::ItemCollectionSizeLimitExceededException(m)
            | Self::RequestTimeoutException(m)
            | Self::InvalidSignatureException(m) => m,
            Self::TransactionCanceledException { message, .. } => message,
        }
    }

    /// For `TransactionCanceledException`, returns the per-item cancellation reasons.
    #[must_use]
    pub fn cancellation_reasons(&self) -> Option<&[CancellationReason]> {
        match self {
            Self::TransactionCanceledException {
                cancellation_reasons,
                ..
            } => Some(cancellation_reasons),
            _ => None,
        }
    }

    /// For `ConditionalCheckFailedException`, returns the existing item when
    /// `ReturnValuesOnConditionCheckFailure` was `ALL_OLD`.
    #[must_use]
    pub fn condition_check_item(&self) -> Option<&Item> {
        match self {
            Self::ConditionalCheckFailedException(_, item) => item.as_ref(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SP-ERR-002: verify every variant returns the correct HTTP status code.
    #[test]
    fn status_codes_match_sp_err_002() {
        let cases: Vec<(DynamoDbError, u16)> = vec![
            (DynamoDbError::AccessDeniedException(String::new()), 400),
            (
                DynamoDbError::ConditionalCheckFailedException(String::new(), None),
                400,
            ),
            (
                DynamoDbError::IdempotentParameterMismatchException(String::new()),
                400,
            ),
            (DynamoDbError::IncompleteSignature(String::new()), 400),
            (DynamoDbError::InternalServerError(String::new()), 500),
            (
                DynamoDbError::ItemCollectionSizeLimitExceededException(String::new()),
                400,
            ),
            (
                DynamoDbError::MalformedHttpRequestException(String::new()),
                400,
            ),
            (
                DynamoDbError::MissingAuthenticationToken(String::new()),
                400,
            ),
            (
                DynamoDbError::ProvisionedThroughputExceededException(String::new()),
                400,
            ),
            (
                DynamoDbError::RequestEntityTooLargeException(String::new()),
                413,
            ),
            (DynamoDbError::RequestLimitExceeded(String::new()), 400),
            (DynamoDbError::RequestTimeoutException(String::new()), 408),
            (DynamoDbError::ResourceInUseException(String::new()), 400),
            (DynamoDbError::ResourceNotFoundException(String::new()), 400),
            (DynamoDbError::SerializationException(String::new()), 400),
            (DynamoDbError::ServiceUnavailable(String::new()), 503),
            (DynamoDbError::ThrottlingException(String::new()), 400),
            (
                DynamoDbError::TransactionCanceledException {
                    message: String::new(),
                    cancellation_reasons: vec![],
                },
                400,
            ),
            (
                DynamoDbError::TransactionConflictException(String::new()),
                400,
            ),
            (
                DynamoDbError::TransactionInProgressException(String::new()),
                400,
            ),
            (DynamoDbError::UnknownOperationException(String::new()), 400),
            (
                DynamoDbError::UnrecognizedClientException(String::new()),
                400,
            ),
            (DynamoDbError::ValidationException(String::new()), 400),
            (DynamoDbError::ExpiredTokenException(String::new()), 400),
            (DynamoDbError::ExpiredIteratorException(String::new()), 400),
            (DynamoDbError::InvalidSignatureException(String::new()), 400),
        ];
        for (err, expected) in cases {
            assert_eq!(
                err.status_code(),
                expected,
                "{} should return {}",
                err.error_type(),
                expected
            );
        }
    }
}
