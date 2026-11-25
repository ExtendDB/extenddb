// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the error bodies [`super::error_response`] sends.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::CancellationReason;

use super::error_response;

/// The top-level keys of the body `error_response` sends for `error`.
async fn body_keys(error: &DynamoDbError) -> Vec<String> {
    let response = error_response(error, "request-1");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    let mut keys: Vec<String> = body.as_object().expect("object").keys().cloned().collect();
    keys.sort_unstable();
    keys
}

/// `TransactionCanceledException`, `IdempotentParameterMismatchException` and
/// `TransactionInProgressException` carry the message under `Message`, as the
/// service sends them; the errors measured with `message` keep it.
#[tokio::test]
async fn the_message_key_follows_the_service_per_error() {
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
    assert_eq!(
        body_keys(&cancelled).await,
        ["CancellationReasons", "Message", "__type"]
    );
    let mismatch = DynamoDbError::IdempotentParameterMismatchException(
        "Specified idempotent token was used with different request parameters within \
         the idempotency window"
            .to_owned(),
    );
    assert_eq!(body_keys(&mismatch).await, ["Message", "__type"]);
    let in_progress = DynamoDbError::TransactionInProgressException(
        "Transaction from previous request is still in progress".to_owned(),
    );
    assert_eq!(body_keys(&in_progress).await, ["Message", "__type"]);
    for other in [
        DynamoDbError::ValidationException("One or more parameter values were invalid".into()),
        DynamoDbError::ConditionalCheckFailedException(
            "The conditional request failed".into(),
            None,
        ),
        DynamoDbError::TransactionConflictException("Transaction is ongoing".into()),
    ] {
        assert_eq!(body_keys(&other).await, ["__type", "message"], "{other:?}");
    }
}
