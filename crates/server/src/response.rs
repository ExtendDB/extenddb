// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire-format response construction and error metrics recording.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use extenddb_core::error::DynamoDbError;
use serde_json::Value;

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;

/// Hardcoded fallback body for when response serialization itself fails.
const SERIALIZATION_FAILURE_BODY: &[u8] =
    br#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"Internal error"}"#;

/// REQ-WIRE-010: `x-amzn-RequestId` on every response.
/// REQ-WIRE-011: `x-amz-crc32` checksum.
/// REQ-WIRE-012: `Content-Type`: `application/x-amz-json-1.0`.
pub(crate) fn success_response(body: &Value, request_id: &str) -> Response {
    // Fix #8: Use fallback error body instead of empty bytes on serialization failure
    let body_bytes =
        serde_json::to_vec(body).unwrap_or_else(|_| SERIALIZATION_FAILURE_BODY.to_vec());
    let crc = crc32fast::hash(&body_bytes);

    (
        StatusCode::OK,
        [
            ("content-type", "application/x-amz-json-1.0"),
            ("x-amzn-RequestId", request_id),
            ("x-amz-crc32", &crc.to_string()),
        ],
        body_bytes,
    )
        .into_response()
}

/// REQ-ERR-001: `__type` with prefix. REQ-ERR-002: message field, under the
/// key [`message_key`] picks. REQ-ERR-003: Omit the message field when empty
/// (real `DynamoDB` behavior, verified 2026-05-04).
pub(crate) fn error_response(error: &DynamoDbError, request_id: &str) -> Response {
    // Fix #14: Use full_error_type() which includes the prefix
    let mut body = serde_json::json!({
        "__type": error.full_error_type(),
    });
    // Only include message field when non-empty (real DynamoDB omits it for
    // UnknownOperationException and other errors with no message).
    let msg = error.message();
    if !msg.is_empty() {
        body[message_key(error)] = serde_json::Value::String(msg.to_owned());
    }
    // TransactionCanceledException includes per-item CancellationReasons
    if let Some(reasons) = error.cancellation_reasons() {
        body["CancellationReasons"] = serde_json::to_value(reasons).unwrap_or_default();
    }
    // ConditionalCheckFailedException includes the old item when requested
    if let Some(item) = error.condition_check_item() {
        body["Item"] = serde_json::to_value(item).unwrap_or_default();
    }
    let body_bytes =
        serde_json::to_vec(&body).unwrap_or_else(|_| SERIALIZATION_FAILURE_BODY.to_vec());
    let crc = crc32fast::hash(&body_bytes);
    let status =
        StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    (
        status,
        [
            ("content-type", "application/x-amz-json-1.0"),
            ("x-amzn-RequestId", request_id),
            ("x-amz-crc32", &crc.to_string()),
        ],
        body_bytes,
    )
        .into_response()
}

/// The key that carries the error message. `DynamoDB` sends `Message` on
/// `TransactionCanceledException` and `IdempotentParameterMismatchException`
/// and `message` on every other error measured (us-east-1, raw bodies,
/// 2026-10-29 and 2026-11-03). `TransactionInProgressException` also models
/// its member as `Message`, but it was not measured and ExtendDB does not
/// raise it, so it keeps `message` here.
fn message_key(error: &DynamoDbError) -> &'static str {
    match error {
        DynamoDbError::TransactionCanceledException { .. }
        | DynamoDbError::IdempotentParameterMismatchException(_) => "Message",
        _ => "message",
    }
}

/// Classify and record error metrics for a failed `DynamoDB` request.
///
/// M-2: `ServiceUnavailable` is a system error (503), not a user error.
/// M-3: `TransactionConflict` metric maps to `TransactionConflictException` (OCC
/// conflict on individual items), not `TransactionCanceledException`.
///
/// `UserErrors` carries no table name: CloudWatch reports it per account and
/// Region only, and a 4xx is often a request whose `TableName` broke the
/// rules. A failed condition counts in `ConditionalCheckFailedRequests` and not
/// in `UserErrors`, as CloudWatch's metrics page says. `table_name` is the name
/// `request_helpers::metric_table_name` checked, used by the metrics that carry
/// one. Every metric carries `account_id`, the account the request was
/// authenticated for.
pub(crate) fn record_error_metrics(
    metrics: &extenddb_core::metrics::MetricsCollector,
    error: &DynamoDbError,
    account_id: &str,
    table_name: Option<&str>,
    operation: &str,
) {
    match error {
        DynamoDbError::InternalServerError(_) | DynamoDbError::ServiceUnavailable(_) => {
            metrics.record_system_error(account_id, table_name, operation);
        }
        DynamoDbError::ConditionalCheckFailedException(..) => {
            metrics.record_conditional_check_failure(account_id, table_name, operation);
        }
        DynamoDbError::TransactionConflictException(_) => {
            metrics.record_user_error(account_id, None, operation);
            metrics.record_transaction_conflict(account_id, operation);
        }
        _ => {
            metrics.record_user_error(account_id, None, operation);
        }
    }
}

#[cfg(test)]
mod tests {
    use extenddb_core::metrics::{
        Dimension, MetricName, MetricsCollector, MetricsQuery, TimeWindow,
    };

    use super::*;

    const ACCOUNT: &str = "111111111111";

    /// The dimensions of every snapshot of `metric` in `collector`.
    fn dimensions_of(collector: &MetricsCollector, metric: MetricName) -> Vec<Vec<Dimension>> {
        collector
            .query(&MetricsQuery {
                metric: Some(metric),
                window: Some(TimeWindow::AllTime),
                ..Default::default()
            })
            .into_iter()
            .map(|s| s.dimensions)
            .collect()
    }

    fn has_table(dimensions: &[Dimension]) -> bool {
        dimensions
            .iter()
            .any(|d| matches!(d, Dimension::TableName(_)))
    }

    #[test]
    fn user_errors_carry_no_table_name() {
        let collector = MetricsCollector::new();
        for error in [
            DynamoDbError::ValidationException("bad".to_owned()),
            DynamoDbError::ResourceNotFoundException("missing".to_owned()),
            DynamoDbError::TransactionConflictException("conflict".to_owned()),
        ] {
            record_error_metrics(&collector, &error, ACCOUNT, Some("Orders"), "PutItem");
        }
        let user_errors = dimensions_of(&collector, MetricName::UserErrors);
        assert_eq!(user_errors.len(), 1, "{user_errors:?}");
        assert!(!has_table(&user_errors[0]), "{user_errors:?}");
        // The error still names the account the request was made for, and
        // so does the conflict.
        assert!(
            user_errors[0].contains(&Dimension::AccountId(ACCOUNT.to_owned())),
            "{user_errors:?}"
        );
        let conflicts = dimensions_of(&collector, MetricName::TransactionConflict);
        assert_eq!(conflicts.len(), 1, "{conflicts:?}");
        assert!(
            conflicts[0].contains(&Dimension::AccountId(ACCOUNT.to_owned())),
            "{conflicts:?}"
        );
    }

    /// A failed condition counts in `ConditionalCheckFailedRequests` only.
    #[test]
    fn a_failed_condition_is_not_a_user_error() {
        let collector = MetricsCollector::new();
        record_error_metrics(
            &collector,
            &DynamoDbError::ConditionalCheckFailedException("failed".to_owned(), None),
            ACCOUNT,
            Some("Orders"),
            "PutItem",
        );
        assert!(dimensions_of(&collector, MetricName::UserErrors).is_empty());
        assert_eq!(
            dimensions_of(&collector, MetricName::ConditionalCheckFailedRequests).len(),
            1
        );
    }

    #[test]
    fn table_scoped_error_metrics_keep_the_checked_table_name() {
        let collector = MetricsCollector::new();
        record_error_metrics(
            &collector,
            &DynamoDbError::ConditionalCheckFailedException("failed".to_owned(), None),
            ACCOUNT,
            Some("Orders"),
            "PutItem",
        );
        record_error_metrics(
            &collector,
            &DynamoDbError::InternalServerError("boom".to_owned()),
            ACCOUNT,
            Some("Orders"),
            "PutItem",
        );
        for metric in [
            MetricName::ConditionalCheckFailedRequests,
            MetricName::SystemErrors,
        ] {
            let dims = dimensions_of(&collector, metric);
            assert_eq!(dims.len(), 1, "{metric:?}: {dims:?}");
            assert!(
                dims[0]
                    .iter()
                    .any(|d| matches!(d, Dimension::TableName(t) if t == "Orders")),
                "{metric:?}: {dims:?}"
            );
            assert!(
                dims[0].contains(&Dimension::AccountId(ACCOUNT.to_owned())),
                "{metric:?}: {dims:?}"
            );
        }
    }
}
