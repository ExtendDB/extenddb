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
pub(crate) fn record_error_metrics(
    metrics: &extenddb_core::metrics::MetricsCollector,
    error: &DynamoDbError,
    table_name: Option<&str>,
    operation: &str,
) {
    match error {
        DynamoDbError::InternalServerError(_) | DynamoDbError::ServiceUnavailable(_) => {
            metrics.record_system_error(table_name, operation);
        }
        DynamoDbError::ConditionalCheckFailedException(..) => {
            metrics.record_user_error(table_name, operation);
            metrics.record_conditional_check_failure(table_name, operation);
        }
        DynamoDbError::TransactionConflictException(_) => {
            metrics.record_user_error(table_name, operation);
            metrics.record_transaction_conflict(operation);
        }
        _ => {
            metrics.record_user_error(table_name, operation);
        }
    }
}
