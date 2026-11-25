// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the error bodies [`super::error_response`] sends.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, CancellationReason, Item};

use super::error_response;

/// The body `error_response` sends for `error`.
async fn body(error: &DynamoDbError) -> String {
    let response = error_response(error, "request-1");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    String::from_utf8(bytes.to_vec()).expect("UTF-8 body")
}

/// Each expected body is the one the service sent for that error (us-east-1,
/// raw bodies, 2026-11-20, each request twice): `__type` first, the other
/// members in the service's order, and the message under the key the service
/// uses for that error. An `Item` here holds one attribute, because the service
/// changed the order of the attributes inside an `Item` between two sends of
/// one request. The `SerializationException` with an empty text has no measured
/// body: ExtendDB omits an empty message (REQ-ERR-003).
#[tokio::test]
async fn the_members_follow_the_service_per_error() {
    let cancelled = DynamoDbError::TransactionCanceledException {
        message: "Transaction cancelled, please refer cancellation reasons for specific \
                  reasons [ConditionalCheckFailed]"
            .to_owned(),
        cancellation_reasons: vec![CancellationReason {
            code: "ConditionalCheckFailed".to_owned(),
            message: Some("The conditional request failed".to_owned()),
            item: None,
        }],
    };
    let old_item = Item::from([("pk".to_owned(), AttributeValue::S("e1222".to_owned()))]);
    let failed = "The conditional request failed";
    let rows: [(DynamoDbError, &str); 7] = [
        (
            cancelled,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#TransactionCanceledException","CancellationReasons":[{"Code":"ConditionalCheckFailed","Message":"The conditional request failed"}],"Message":"Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]"}"#,
        ),
        (
            DynamoDbError::ConditionalCheckFailedException(failed.into(), Some(old_item)),
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","Item":{"pk":{"S":"e1222"}},"message":"The conditional request failed"}"#,
        ),
        (
            DynamoDbError::ConditionalCheckFailedException(failed.into(), None),
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
        ),
        (
            DynamoDbError::IdempotentParameterMismatchException(
                "Specified idempotent token was used with different request parameters \
                 within the idempotency window"
                    .to_owned(),
            ),
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#IdempotentParameterMismatchException","Message":"Specified idempotent token was used with different request parameters within the idempotency window"}"#,
        ),
        (
            DynamoDbError::SerializationException(
                "STRING_VALUE cannot be converted to Integer".to_owned(),
            ),
            r#"{"__type":"com.amazon.coral.service#SerializationException","Message":"STRING_VALUE cannot be converted to Integer"}"#,
        ),
        (
            DynamoDbError::SerializationException(String::new()),
            r#"{"__type":"com.amazon.coral.service#SerializationException"}"#,
        ),
        (
            DynamoDbError::ValidationException(
                "1 validation error detected: Value null at 'key' failed to satisfy \
                 constraint: Member must not be null"
                    .to_owned(),
            ),
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value null at 'key' failed to satisfy constraint: Member must not be null"}"#,
        ),
    ];
    let mut got = Vec::new();
    let mut want = Vec::new();
    for (error, expected) in &rows {
        got.push(body(error).await);
        want.push(*expected);
    }
    assert_eq!(got, want);
}
