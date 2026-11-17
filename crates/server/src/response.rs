// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire-format response construction and error metrics recording.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use extenddb_core::error::DynamoDbError;
use serde_json::Value;

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

/// REQ-ERR-001: `__type` with prefix. REQ-ERR-002: message field.
/// REQ-ERR-003: Omit `message` when empty (real `DynamoDB` behavior, verified 2026-05-04).
pub(crate) fn error_response(error: &DynamoDbError, request_id: &str) -> Response {
    // Fix #14: Use full_error_type() which includes the prefix
    let mut body = serde_json::json!({
        "__type": error.full_error_type(),
    });
    // Only include message field when non-empty (real DynamoDB omits it for
    // UnknownOperationException and other errors with no message).
    let msg = error.message();
    if !msg.is_empty() {
        body["message"] = serde_json::Value::String(msg.to_owned());
    }
    // TransactionCanceledException includes per-item CancellationReasons
    if let Some(reasons) = error.cancellation_reasons() {
        body["CancellationReasons"] = serde_json::to_value(reasons).unwrap_or_default();
    }
    // Throttle errors carry one reason per throttled resource. The field name
    // differs by error class: `ThrottlingReasons` on
    // ProvisionedThroughputExceededException and RequestLimitExceeded,
    // `throttlingReasons` on ThrottlingException (measured against the service
    // 2026-10-20 and 2026-10-24). Omitted when the list is empty, which is what
    // the service sends for a hot-key read on an on-demand table.
    if let Some(reasons) = error.throttling_reasons()
        && !reasons.is_empty()
    {
        let field = match error {
            DynamoDbError::ThrottlingException { .. } => "throttlingReasons",
            _ => "ThrottlingReasons",
        };
        body[field] = serde_json::to_value(reasons).unwrap_or_default();
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

#[cfg(test)]
mod throttle_body_tests {
    use extenddb_core::error::{
        DynamoDbError, MAX_ON_DEMAND_THROUGHPUT_MESSAGE, PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
        THROTTLING_EXCEPTION_MESSAGE, ThrottlingReason,
    };
    use serde_json::Value;

    use super::error_response;

    const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/T";

    fn reason(code: &str) -> ThrottlingReason {
        ThrottlingReason {
            reason: code.to_owned(),
            resource: ARN.to_owned(),
        }
    }

    async fn body_of(error: &DynamoDbError) -> (u16, Value, String) {
        let response = error_response(error, "req-1");
        let status = response.status().as_u16();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .expect("read body");
        let text = String::from_utf8(bytes.to_vec()).expect("utf-8 body");
        let json: Value = serde_json::from_str(&text).expect("body is json");
        (status, json, text)
    }

    /// Provisioned throttle: `ThrottlingReasons` (capital T), dynamodb
    /// namespace, the measured message with its trailing period. Compared as
    /// parsed JSON because the serializer orders object keys alphabetically.
    #[tokio::test]
    async fn provisioned_throughput_exceeded_body_matches_the_service() {
        let error = DynamoDbError::provisioned_throughput_exceeded(
            PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
            vec![reason("TableWriteProvisionedThroughputExceeded")],
        );
        let (status, actual, text) = body_of(&error).await;
        assert_eq!(status, 400);
        assert!(text.contains(r#""ThrottlingReasons":"#), "{text}");
        assert!(!text.contains("throttlingReasons"), "{text}");
        assert_eq!(
            actual,
            serde_json::json!({
                "__type": "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
                "message": PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                "ThrottlingReasons": [
                    {"reason": "TableWriteProvisionedThroughputExceeded", "resource": ARN}
                ],
            })
        );
    }

    /// On-demand hot-key write: `throttlingReasons` (lower t), availability
    /// namespace, the measured message without a trailing period.
    #[tokio::test]
    async fn throttling_exception_body_matches_the_service() {
        let error = DynamoDbError::throttling(
            THROTTLING_EXCEPTION_MESSAGE,
            vec![reason("TableWriteKeyRangeThroughputExceeded")],
        );
        let (status, actual, text) = body_of(&error).await;
        assert_eq!(status, 400);
        assert!(text.contains(r#""throttlingReasons":"#), "{text}");
        assert!(!text.contains("ThrottlingReasons"), "{text}");
        assert_eq!(
            actual,
            serde_json::json!({
                "__type": "com.amazon.coral.availability#ThrottlingException",
                "message": THROTTLING_EXCEPTION_MESSAGE,
                "throttlingReasons": [
                    {"reason": "TableWriteKeyRangeThroughputExceeded", "resource": ARN}
                ],
            })
        );
    }

    /// On-demand hot-key read: the service sends no reasons list under either
    /// spelling; the body is `__type` and `message` only.
    #[tokio::test]
    async fn empty_reasons_list_omits_the_field() {
        for error in [
            DynamoDbError::throttling(THROTTLING_EXCEPTION_MESSAGE, vec![]),
            DynamoDbError::provisioned_throughput_exceeded("m", vec![]),
            DynamoDbError::request_limit_exceeded("m", vec![]),
        ] {
            let (_, actual, text) = body_of(&error).await;
            let keys: Vec<&str> = actual
                .as_object()
                .expect("object")
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(keys, ["__type", "message"], "{text}");
        }
    }

    /// The `OnDemandThroughput` caps body: the on-demand class and field name
    /// with the caps message and the `MaxOnDemandThroughputExceeded` limit.
    #[tokio::test]
    async fn max_on_demand_throughput_body_matches_the_service() {
        let error = DynamoDbError::throttling(
            MAX_ON_DEMAND_THROUGHPUT_MESSAGE,
            vec![reason("TableReadMaxOnDemandThroughputExceeded")],
        );
        let (_, actual, _) = body_of(&error).await;
        assert_eq!(
            actual,
            serde_json::json!({
                "__type": "com.amazon.coral.availability#ThrottlingException",
                "message": MAX_ON_DEMAND_THROUGHPUT_MESSAGE,
                "throttlingReasons": [
                    {"reason": "TableReadMaxOnDemandThroughputExceeded", "resource": ARN}
                ],
            })
        );
    }

    /// `RequestLimitExceeded` renders `ThrottlingReasons` (capital T, the API
    /// Reference member name; not measured) in the dynamodb namespace.
    #[tokio::test]
    async fn request_limit_exceeded_renders_reasons_with_the_documented_casing() {
        let error = DynamoDbError::request_limit_exceeded(
            "Too many requests",
            vec![reason("TableWriteAccountLimitExceeded")],
        );
        let (status, actual, _) = body_of(&error).await;
        assert_eq!(status, 400);
        assert_eq!(
            actual,
            serde_json::json!({
                "__type": "com.amazonaws.dynamodb.v20120810#RequestLimitExceeded",
                "message": "Too many requests",
                "ThrottlingReasons": [
                    {"reason": "TableWriteAccountLimitExceeded", "resource": ARN}
                ],
            })
        );
    }

    /// The throttle block does not disturb the transaction cancel shape.
    #[tokio::test]
    async fn cancellation_reasons_unaffected() {
        let error = DynamoDbError::TransactionCanceledException {
            message: "Transaction cancelled".to_owned(),
            cancellation_reasons: vec![],
        };
        let (_, _, text) = body_of(&error).await;
        assert!(text.contains(r#""CancellationReasons":[]"#), "{text}");
        assert!(!text.contains("hrottlingReasons"), "{text}");
    }
}
