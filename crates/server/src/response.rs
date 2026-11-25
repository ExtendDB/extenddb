// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire-format response construction and error metrics recording.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use extenddb_core::error::{DynamoDbError, SERIALIZATION_FAILURE_BODY, error_document};
use serde_json::Value;

#[cfg(test)]
#[path = "response_tests.rs"]
mod message_key_tests;

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
/// key `error_document` picks. REQ-ERR-003: Omit the message field when empty
/// (real `DynamoDB` behavior, verified 2026-05-04).
pub(crate) fn error_response(error: &DynamoDbError, request_id: &str) -> Response {
    // The body shape (`__type`, optional message, `CancellationReasons`, the
    // throttle reasons list, `Item`) is built by the shared `error_document`
    // helper so the wire body and the in-process body come from the same code;
    // this transport adds the HTTP status and headers.
    let body = error_document(error);
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

/// Classify and record error metrics for a failed `DynamoDB` request.
///
/// M-2: `ServiceUnavailable` is a system error (503), not a user error.
/// M-3: `TransactionConflict` metric maps to `TransactionConflictException` (OCC
/// conflict on individual items), not `TransactionCanceledException`.
///
/// `UserErrors` carries no table name: CloudWatch reports it per account and
/// Region only, and a 4xx is often a request whose `TableName` broke the
/// rules. A failed condition counts in `ConditionalCheckFailedRequests` and a
/// throttle error in `ThrottledRequests`, neither in `UserErrors`, as
/// CloudWatch's metrics page says. The server's own bucket records its
/// refusals before dispatch, so a throttle error here is one a backend
/// reported. On `BatchGetItem` and `BatchWriteItem` it answers a batch whose
/// every entry was throttled, the one batch CloudWatch counts in
/// `ThrottledRequests`. `table_name` is the name
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
        e if e.is_throttle() => {
            metrics.record(
                extenddb_core::metrics::MetricName::ThrottledRequests,
                1.0,
                Some(account_id),
                table_name,
                None,
                Some(operation),
            );
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

    /// A throttle error of each class counts in `ThrottledRequests`, under the
    /// table the request named, and not in `UserErrors`.
    #[test]
    fn a_throttle_error_is_a_throttled_request_and_not_a_user_error() {
        for error in [
            DynamoDbError::provisioned_throughput_exceeded("capacity", vec![]),
            DynamoDbError::throttling("throughput", vec![]),
            DynamoDbError::request_limit_exceeded("quota", vec![]),
        ] {
            let collector = MetricsCollector::new();
            record_error_metrics(&collector, &error, ACCOUNT, Some("Orders"), "GetItem");
            record_error_metrics(&collector, &error, ACCOUNT, None, "BatchGetItem");
            assert!(
                dimensions_of(&collector, MetricName::UserErrors).is_empty(),
                "{error:?}"
            );
            let mut throttled = dimensions_of(&collector, MetricName::ThrottledRequests);
            throttled.sort_by_key(|dims| has_table(dims));
            assert_eq!(throttled.len(), 2, "{error:?}: {throttled:?}");
            assert!(!has_table(&throttled[0]), "{error:?}: {throttled:?}");
            assert!(
                throttled[1].contains(&Dimension::TableName("Orders".to_owned())),
                "{error:?}: {throttled:?}"
            );
        }
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
