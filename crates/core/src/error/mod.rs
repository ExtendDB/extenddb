// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
mod messages;

pub use messages::{ErrorMessageKey, error_message, validation_error, validation_errors};

use crate::types::{CancellationReason, Item};

/// One entry of the reasons list the service returns on a throttle error.
/// Measured against Amazon DynamoDB on 2026-10-20 (435 raw throttle bodies,
/// one Region) and 2026-10-24 (9,622, two Regions): every reasons list that
/// was present carried exactly one entry of this shape.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThrottlingReason {
    /// `ResourceType + OperationType + LimitType`, for example
    /// `TableWriteProvisionedThroughputExceeded` or
    /// `IndexReadProvisionedThroughputExceeded`.
    pub reason: String,
    /// The table or index ARN, the string `DescribeTable` returns as
    /// `TableArn` or `IndexArn`.
    pub resource: String,
}

/// The `ProvisionedThroughputExceededException` message the service sends
/// when a table's own provisioned capacity is exceeded, on every read and
/// write operation, and on a hot key or an index read of a provisioned
/// table. Measured 2026-10-20 (424 of 424: 423 writes on the table's own
/// capacity and one hot-key write) and 2026-10-24 (1,538 read throttles on
/// the table's own capacity, 648 index reads and 162 writes in a second
/// Region, all carrying this text; the 104 index-write bodies of that run
/// carry a different message naming global secondary indexes,
/// [`INDEX_WRITE_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE`]). Trailing period.
pub const PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE: &str = "The level of configured provisioned throughput for the table was exceeded. Consider increasing your provisioning level with the UpdateTable API.";

/// The `ProvisionedThroughputExceededException` message the service sends
/// when a write to the base table is held back by a provisioned global
/// secondary index's write capacity. Measured 2026-10-24 (104
/// `BatchWriteItem`) and 2026-10-25 (5,876 bodies: 2,376 `PutItem`, 2,000
/// `UpdateItem` and 1,500 single-item `TransactWriteItems`), every body
/// carrying this text with the reason
/// `IndexWriteProvisionedThroughputExceeded` and the index ARN. No trailing
/// period. An index read past its read capacity carries the table message.
pub const INDEX_WRITE_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE: &str = "The level of configured provisioned throughput for one or more global secondary indexes of the table was exceeded. Consider increasing your provisioning level for the under-provisioned global secondary indexes with the UpdateTable API";

/// The `ThrottlingException` message the service sends on an on-demand table
/// whose partition is hot. Measured 2026-10-20 (11 of 11) and 2026-10-24
/// (7,070 of 7,070, two Regions). No trailing period.
pub const THROTTLING_EXCEPTION_MESSAGE: &str = "Throughput exceeds the current capacity of your table or index. DynamoDB is automatically scaling your table or index so please try again shortly. If exceptions persist, check if you have a hot key: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html";

/// The `ThrottlingException` message the service sends when an on-demand
/// table reaches the `OnDemandThroughput` caps set on it. Measured
/// 2026-10-24 (100 of 100: 52 `Query` reads, 48 `BatchWriteItem` writes). No
/// trailing period.
pub const MAX_ON_DEMAND_THROUGHPUT_MESSAGE: &str =
    "Throughput exceeds the maximum OnDemandThroughput configured on table or index";

/// `RequestLimitExceeded` wording. The API Reference (PutItem Errors, read
/// 2026-10-20) describes the error as throughput exceeding the account's
/// quota and points at Support for an increase; the wire `message` field has
/// not been measured, so this text is the documented description.
pub const REQUEST_LIMIT_EXCEEDED_MESSAGE: &str = "Throughput exceeds the current throughput quota for your account. Please contact Amazon Web Services Support to request a quota increase.";

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
    /// Returned when a shard iterator has expired (older than 15 minutes).
    #[error("{0}")]
    ExpiredIteratorException(String),
    #[error("{0}")]
    ServiceUnavailable(String),
    /// SP-ERR-002: Concurrent modification of the same item in a transaction.
    #[error("{0}")]
    TransactionConflictException(String),
    /// SP-ERR-002: Throughput exceeded. HTTP 400. The reasons list is
    /// rendered as `ThrottlingReasons` (capital T, measured 2026-10-20 and
    /// 2026-10-24) and omitted when empty.
    #[error("{message}")]
    ProvisionedThroughputExceededException {
        /// Human-readable message; the service's text for a table's own
        /// capacity is [`PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE`].
        message: String,
        /// One entry per throttled resource.
        reasons: Vec<ThrottlingReason>,
    },
    /// SP-ERR-002: General throttling. HTTP 400. The `__type` namespace is
    /// `com.amazon.coral.availability#` and the reasons list is rendered as
    /// `throttlingReasons` (lower t), both measured 2026-10-20 and
    /// 2026-10-24. The list is omitted when empty, which is what the service
    /// sends for a hot-key read on an on-demand table (5,615 of 5,615).
    #[error("{message}")]
    ThrottlingException {
        /// Human-readable message; [`THROTTLING_EXCEPTION_MESSAGE`] for a hot
        /// key, [`MAX_ON_DEMAND_THROUGHPUT_MESSAGE`] at the table's caps.
        message: String,
        /// One entry per throttled resource.
        reasons: Vec<ThrottlingReason>,
    },
    /// SP-ERR-002: Per-account throughput quota. HTTP 400. The API Reference
    /// documents a `ThrottlingReasons` member on this error (PutItem Errors,
    /// read 2026-10-20); rendered with that casing and omitted when empty.
    /// Not measured: the probes did not reach an account quota.
    #[error("{message}")]
    RequestLimitExceeded {
        /// Human-readable message; [`REQUEST_LIMIT_EXCEEDED_MESSAGE`] is the
        /// documented wording.
        message: String,
        /// One entry per throttled resource.
        reasons: Vec<ThrottlingReason>,
    },
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
            | Self::TransactionConflictException(_)
            | Self::ProvisionedThroughputExceededException { .. }
            | Self::ThrottlingException { .. }
            | Self::RequestLimitExceeded { .. }
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
            Self::ServiceUnavailable(_) => "ServiceUnavailable",
            Self::TransactionConflictException(_) => "TransactionConflictException",
            Self::ProvisionedThroughputExceededException { .. } => {
                "ProvisionedThroughputExceededException"
            }
            Self::ThrottlingException { .. } => "ThrottlingException",
            Self::RequestLimitExceeded { .. } => "RequestLimitExceeded",
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
    /// - `ThrottlingException` → `com.amazon.coral.availability#` (measured
    ///   2026-10-20, 11 of 11, and 2026-10-24, 7,170 of 7,170 on-demand
    ///   throttles)
    /// - All other `DynamoDB` errors → `com.amazonaws.dynamodb.v20120810#`
    ///
    /// Verified against the service 2026-05-04 and 2026-10-24.
    #[must_use]
    pub fn full_error_type(&self) -> String {
        let prefix = match self {
            Self::ValidationException(_) => "com.amazon.coral.validate#",
            Self::SerializationException(_)
            | Self::UnknownOperationException(_)
            | Self::MissingAuthenticationToken(_)
            | Self::IncompleteSignature(_)
            | Self::InvalidSignatureException(_) => "com.amazon.coral.service#",
            Self::ThrottlingException { .. } => "com.amazon.coral.availability#",
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
            | Self::ServiceUnavailable(m)
            | Self::TransactionConflictException(m)
            | Self::ProvisionedThroughputExceededException { message: m, .. }
            | Self::ThrottlingException { message: m, .. }
            | Self::RequestLimitExceeded { message: m, .. }
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

    /// A `ProvisionedThroughputExceededException` carrying `reasons`.
    #[must_use]
    pub fn provisioned_throughput_exceeded(
        message: impl Into<String>,
        reasons: Vec<ThrottlingReason>,
    ) -> Self {
        Self::ProvisionedThroughputExceededException {
            message: message.into(),
            reasons,
        }
    }

    /// A `ThrottlingException` carrying `reasons`.
    #[must_use]
    pub fn throttling(message: impl Into<String>, reasons: Vec<ThrottlingReason>) -> Self {
        Self::ThrottlingException {
            message: message.into(),
            reasons,
        }
    }

    /// A `RequestLimitExceeded` carrying `reasons`.
    #[must_use]
    pub fn request_limit_exceeded(
        message: impl Into<String>,
        reasons: Vec<ThrottlingReason>,
    ) -> Self {
        Self::RequestLimitExceeded {
            message: message.into(),
            reasons,
        }
    }

    /// For the three throttle errors (`ProvisionedThroughputExceededException`,
    /// `ThrottlingException`, `RequestLimitExceeded`), the per-resource
    /// reasons, possibly empty; `None` for every other variant.
    #[must_use]
    pub fn throttling_reasons(&self) -> Option<&[ThrottlingReason]> {
        match self {
            Self::ProvisionedThroughputExceededException { reasons, .. }
            | Self::ThrottlingException { reasons, .. }
            | Self::RequestLimitExceeded { reasons, .. } => Some(reasons),
            _ => None,
        }
    }

    /// Whether this error is one of the three throttle errors.
    #[must_use]
    pub fn is_throttle(&self) -> bool {
        self.throttling_reasons().is_some()
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
                DynamoDbError::provisioned_throughput_exceeded(String::new(), vec![]),
                400,
            ),
            (
                DynamoDbError::RequestEntityTooLargeException(String::new()),
                413,
            ),
            (
                DynamoDbError::request_limit_exceeded(String::new(), vec![]),
                400,
            ),
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
            (DynamoDbError::throttling(String::new(), vec![]), 400),
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

    #[test]
    fn table_already_exists_uses_the_dynamodb_namespace() {
        let err = DynamoDbError::TableAlreadyExistsException("Table already exists: t".to_owned());
        assert_eq!(
            err.full_error_type(),
            "com.amazonaws.dynamodb.v20120810#TableAlreadyExistsException"
        );
        assert_eq!(err.message(), "Table already exists: t");
    }
}

#[cfg(test)]
mod throttle_tests;
