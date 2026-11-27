// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire-format response construction and error metrics recording.

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use extenddb_core::error::{DynamoDbError, SERIALIZATION_FAILURE_BODY, error_document};
use serde_json::Value;

/// REQ-WIRE-012: the media type of the API's answers.
pub(crate) const AMZ_JSON_1_0: &str = "application/x-amz-json-1.0";

#[cfg(test)]
#[path = "response_tests.rs"]
mod message_key_tests;

/// An answer with the headers every answer of the service carries:
/// REQ-WIRE-010 `x-amzn-RequestId` and REQ-WIRE-011 `x-amz-crc32` of `body`,
/// and `content_type`, or no Content-Type when it is `None`.
pub(crate) fn wire_response(
    status: StatusCode,
    content_type: Option<&'static str>,
    body: Vec<u8>,
    request_id: &str,
) -> Response {
    let crc = crc32fast::hash(&body);
    let mut response = (
        status,
        [
            ("x-amzn-RequestId", request_id),
            ("x-amz-crc32", &crc.to_string()),
        ],
        body,
    )
        .into_response();
    // A byte body comes with `application/octet-stream`; this replaces it.
    let headers = response.headers_mut();
    if let Some(content_type) = content_type {
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    } else {
        headers.remove(header::CONTENT_TYPE);
    }
    response
}

/// REQ-WIRE-010: `x-amzn-RequestId` on every response.
/// REQ-WIRE-011: `x-amz-crc32` checksum.
/// REQ-WIRE-012: `Content-Type`: `application/x-amz-json-1.0`.
pub(crate) fn success_response(body: &Value, request_id: &str) -> Response {
    // Fix #8: Use fallback error body instead of empty bytes on serialization failure
    let body_bytes =
        serde_json::to_vec(body).unwrap_or_else(|_| SERIALIZATION_FAILURE_BODY.to_vec());
    wire_response(StatusCode::OK, Some(AMZ_JSON_1_0), body_bytes, request_id)
}

/// REQ-ERR-001: `__type` with prefix. REQ-ERR-002: message field, under the
/// key `error_document` picks. REQ-ERR-003: Omit the message field when empty
/// (real `DynamoDB` behavior, verified 2026-05-04).
pub(crate) fn error_response(error: &DynamoDbError, request_id: &str) -> Response {
    // The body shape (`__type`, optional message, `CancellationReasons`, `Item`)
    // is built by the shared `error_document` helper so the wire body and the
    // in-process body come from the same code; this transport adds the HTTP status and headers.
    let body = error_document(error);
    let body_bytes =
        serde_json::to_vec(&body).unwrap_or_else(|_| SERIALIZATION_FAILURE_BODY.to_vec());
    let status =
        StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    wire_response(status, Some(AMZ_JSON_1_0), body_bytes, request_id)
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
