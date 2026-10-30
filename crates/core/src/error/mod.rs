// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
mod messages;

pub use messages::{ErrorMessageKey, error_message, validation_error, validation_errors};

use crate::types::{CancellationReason, Item};

/// The `ExpiredIteratorException` message for a shard iterator older than its
/// 15-minute lifetime. The lifetime is documented under `GetShardIterator` in
/// the API Reference ("A shard iterator expires 15 minutes after it is
/// returned to the requester",
/// <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html>).
///
/// The message was measured on 2026-10-22 in us-east-1: two iterators, read
/// with `GetRecords` 15 min 20 s and 16 min 30 s after `GetShardIterator`
/// returned them, both answered HTTP 400 `ExpiredIteratorException` with
/// this text. The first response is quoted in the
/// `stream_errors_use_the_streams_namespace` test.
pub const EXPIRED_ITERATOR_MESSAGE: &str = "Shard iterator has expired";

/// The `TrimmedDataAccessException` message: the exception's description
/// under Errors for `GetRecords` and `GetShardIterator` in the API Reference
/// (<https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html>).
/// It is not a captured response. No read below a shard's trim horizon was
/// provoked on the service, so the text the service sends is unmeasured, and
/// the same Errors entries give a different `message` text, "The data you
/// are trying to access has been trimmed." The one measured Streams error
/// whose Errors entry documents a `message` member,
/// `ExpiredIteratorException`, sent "Shard iterator has expired", where the
/// `GetRecords` entry documents "The provided iterator exceeds the maximum
/// age allowed.", so a documented `message` member is not taken as the wire
/// text.
pub const TRIMMED_DATA_ACCESS_MESSAGE: &str =
    "The operation attempted to read past the oldest stream record in a shard.";

/// All Virtual `DynamoDB` error types with HTTP status codes.
///
/// REQ-ERR-001 through REQ-ERR-004: error JSON format, status codes, and SDK retry behavior.
/// SP-ERR-002: HTTP status codes match the real `DynamoDB` error catalog exactly.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum DynamoDbError {
    #[error("{0}")]
    ValidationException(String),
    #[error("{0}")]
    ResourceNotFoundException(String),
    /// SP-ERR-002: HTTP 400. Backup-specific not-found, distinct from
    /// `ResourceNotFoundException`. Returned by `DescribeBackup`, `DeleteBackup`
    /// and `RestoreTableFromBackup` for a backup ARN in the caller's account
    /// that does not exist (or has been deleted).
    #[error("{0}")]
    BackupNotFoundException(String),
    #[error("{0}")]
    BackupInUseException(String),
    #[error("{0}")]
    ResourceInUseException(String),
    /// HTTP 400. `RestoreTableFromBackup` into a table name that is already
    /// taken. Listed in the operation's errors in the 2012-08-10 service model
    /// (botocore 1.33.13 `service-2.json`, shape `TableAlreadyExistsException`)
    /// and measured on DynamoDB. `CreateTable` keeps
    /// `ResourceInUseException`, which is what the service returns there.
    #[error("{0}")]
    TableAlreadyExistsException(String),
    /// HTTP 400. `RestoreTableFromBackup` into a name that is itself a
    /// `CREATING` restore target. Measured on DynamoDB: `Table: <name> is
    /// already being restored from backup: <arn>`.
    #[error("{0}")]
    TableInUseException(String),
    /// HTTP 400. `CreateBackup` on a table that does not exist or is not yet
    /// `ACTIVE`. Measured on DynamoDB: `Table not found: <name>`. Distinct
    /// from `ResourceNotFoundException`, which the data plane returns.
    #[error("{0}")]
    TableNotFoundException(String),
    /// A per-table or per-account limit was exceeded.
    ///
    /// Distinct from `ValidationException` on purpose: the service uses this
    /// class for the vector-index count limit when the index is added through
    /// `UpdateTable`, while `CreateTable` reports the same limit as a
    /// `ValidationException`. Measured 2026-08-13; see
    /// `VECTOR_INDEX_COUNT_LIMIT_UPDATE`.
    #[error("{0}")]
    LimitExceededException(String),
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
    /// Returned when a shard iterator has expired (older than the iterator
    /// lifetime, 15 minutes on the service). Measured 2026-10-22: HTTP 400,
    /// type `com.amazonaws.dynamodb.streams.v20120810#ExpiredIteratorException`,
    /// message [`EXPIRED_ITERATOR_MESSAGE`]; the raw response is quoted in the
    /// `stream_errors_use_the_streams_namespace` test.
    #[error("{0}")]
    ExpiredIteratorException(String),
    /// Returned when a `GetRecords` iterator points before the shard's trim
    /// horizon (records past the 24-hour retention period). HTTP 400, as the
    /// API Reference gives it. The type is placed in the Streams namespace on
    /// the assumption that it matches the measured `ExpiredIteratorException`;
    /// the API Reference does not give the namespace and a trimmed read was
    /// not provoked on the service. The message is the API Reference's
    /// description ([`TRIMMED_DATA_ACCESS_MESSAGE`]), not a captured response.
    #[error("{0}")]
    TrimmedDataAccessException(String),
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
    /// SP-ERR-002: `SigV4` signature mismatch. HTTP 403.
    #[error("{0}")]
    InvalidSignatureException(String),
}

impl DynamoDbError {
    /// HTTP status code for this error.
    ///
    /// SP-ERR-002: status codes match the real `DynamoDB` error catalog.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::ValidationException(_)
            | Self::ResourceNotFoundException(_)
            | Self::BackupNotFoundException(_)
            | Self::BackupInUseException(_)
            | Self::ResourceInUseException(_)
            | Self::TableAlreadyExistsException(_)
            | Self::TableInUseException(_)
            | Self::TableNotFoundException(_)
            | Self::LimitExceededException(_)
            | Self::ConditionalCheckFailedException(..)
            | Self::TransactionCanceledException { .. }
            | Self::IdempotentParameterMismatchException(_)
            | Self::SerializationException(_)
            | Self::AccessDeniedException(_)
            | Self::ExpiredTokenException(_)
            | Self::ExpiredIteratorException(_)
            | Self::TrimmedDataAccessException(_)
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
            Self::BackupNotFoundException(_) => "BackupNotFoundException",
            Self::BackupInUseException(_) => "BackupInUseException",
            Self::ResourceInUseException(_) => "ResourceInUseException",
            Self::TableAlreadyExistsException(_) => "TableAlreadyExistsException",
            Self::TableInUseException(_) => "TableInUseException",
            Self::TableNotFoundException(_) => "TableNotFoundException",
            Self::LimitExceededException(_) => "LimitExceededException",
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
            Self::TrimmedDataAccessException(_) => "TrimmedDataAccessException",
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
    /// - `ExpiredIteratorException`, `TrimmedDataAccessException` →
    ///   `com.amazonaws.dynamodb.streams.v20120810#` (the expiry measured
    ///   2026-10-22, quoted in `stream_errors_use_the_streams_namespace`; the
    ///   trimmed read assumed to match it, neither measured nor documented)
    /// - All other `DynamoDB` errors → `com.amazonaws.dynamodb.v20120810#`
    ///
    /// The other rules were verified against real `DynamoDB` 2026-05-04.
    #[must_use]
    pub fn full_error_type(&self) -> String {
        let prefix = match self {
            Self::ValidationException(_) => "com.amazon.coral.validate#",
            Self::SerializationException(_)
            | Self::UnknownOperationException(_)
            | Self::MissingAuthenticationToken(_)
            | Self::IncompleteSignature(_)
            | Self::InvalidSignatureException(_) => "com.amazon.coral.service#",
            Self::ExpiredIteratorException(_) | Self::TrimmedDataAccessException(_) => {
                "com.amazonaws.dynamodb.streams.v20120810#"
            }
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
            | Self::BackupNotFoundException(m)
            | Self::BackupInUseException(m)
            | Self::ResourceInUseException(m)
            | Self::TableAlreadyExistsException(m)
            | Self::TableInUseException(m)
            | Self::TableNotFoundException(m)
            | Self::LimitExceededException(m)
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
            | Self::TrimmedDataAccessException(m)
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
            (DynamoDbError::LimitExceededException(String::new()), 400),
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
            (
                DynamoDbError::TableAlreadyExistsException(String::new()),
                400,
            ),
            (DynamoDbError::ResourceNotFoundException(String::new()), 400),
            (DynamoDbError::BackupNotFoundException(String::new()), 400),
            (DynamoDbError::BackupInUseException(String::new()), 400),
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
            (
                DynamoDbError::TrimmedDataAccessException(String::new()),
                400,
            ),
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

    #[test]
    fn table_already_exists_uses_the_dynamodb_namespace() {
        let err = DynamoDbError::TableAlreadyExistsException("Table already exists: t".to_owned());
        assert_eq!(
            err.full_error_type(),
            "com.amazonaws.dynamodb.v20120810#TableAlreadyExistsException"
        );
        assert_eq!(err.message(), "Table already exists: t");
    }

    /// The two Streams errors carry the Streams API namespace, HTTP 400 and
    /// the message the variant holds.
    ///
    /// The expired-iterator assertions pin a measured response. On
    /// 2026-10-22 in us-east-1, `GetRecords` (`X-Amz-Target:
    /// DynamoDBStreams_20120810.GetRecords`) with an iterator read 15 min
    /// 20 s after `GetShardIterator` returned it answered, at 20:13:59 GMT:
    ///
    /// ```text
    /// 400 {"__type":"com.amazonaws.dynamodb.streams.v20120810#ExpiredIteratorException","message":"Shard iterator has expired"}
    /// ```
    ///
    /// The same iterator was served 14 min 30 s after it was returned. The
    /// trimmed-read assertions pin an assumption instead: the namespace
    /// follows the expired iterator's, and the message is the API
    /// Reference's description of the exception.
    #[test]
    fn stream_errors_use_the_streams_namespace() {
        let expired = DynamoDbError::ExpiredIteratorException(EXPIRED_ITERATOR_MESSAGE.to_owned());
        assert_eq!(
            expired.full_error_type(),
            "com.amazonaws.dynamodb.streams.v20120810#ExpiredIteratorException"
        );
        assert_eq!(expired.status_code(), 400);
        assert_eq!(expired.message(), "Shard iterator has expired");

        let trimmed =
            DynamoDbError::TrimmedDataAccessException(TRIMMED_DATA_ACCESS_MESSAGE.to_owned());
        assert_eq!(
            trimmed.full_error_type(),
            "com.amazonaws.dynamodb.streams.v20120810#TrimmedDataAccessException"
        );
        assert_eq!(trimmed.status_code(), 400);
        assert_eq!(trimmed.error_type(), "TrimmedDataAccessException");
        assert_eq!(
            trimmed.message(),
            "The operation attempted to read past the oldest stream record in a shard."
        );
    }
}
