// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The three throttle error classes: status, type name, namespace, reasons.

use super::*;

fn reason(code: &str) -> ThrottlingReason {
    ThrottlingReason {
        reason: code.to_owned(),
        resource: "arn:aws:dynamodb:us-east-1:123456789012:table/T".to_owned(),
    }
}

/// The three throttle classes keep their status and type name, carry their
/// reasons through the accessor, and answer `is_throttle`.
#[test]
fn throttle_errors_carry_reasons_and_keep_their_class() {
    let cases = [
        (
            DynamoDbError::provisioned_throughput_exceeded(
                PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                vec![reason("TableWriteProvisionedThroughputExceeded")],
            ),
            "ProvisionedThroughputExceededException",
            PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
        ),
        (
            DynamoDbError::throttling(
                THROTTLING_EXCEPTION_MESSAGE,
                vec![reason("TableWriteKeyRangeThroughputExceeded")],
            ),
            "ThrottlingException",
            THROTTLING_EXCEPTION_MESSAGE,
        ),
        (
            DynamoDbError::request_limit_exceeded(
                REQUEST_LIMIT_EXCEEDED_MESSAGE,
                vec![reason("TableWriteAccountLimitExceeded")],
            ),
            "RequestLimitExceeded",
            REQUEST_LIMIT_EXCEEDED_MESSAGE,
        ),
    ];
    for (err, type_name, message) in cases {
        assert_eq!(err.status_code(), 400, "{type_name}");
        assert_eq!(err.error_type(), type_name);
        assert_eq!(err.message(), message);
        assert_eq!(err.to_string(), message);
        assert!(err.is_throttle(), "{type_name}");
        let reasons = err.throttling_reasons().expect("reasons");
        assert_eq!(reasons.len(), 1, "{type_name}");
        assert_eq!(
            reasons[0].resource,
            "arn:aws:dynamodb:us-east-1:123456789012:table/T"
        );
    }
}

/// Only `ThrottlingException` moves to the availability namespace; the
/// other two throttle classes stay in the dynamodb namespace.
#[test]
fn throttling_exception_uses_the_availability_namespace() {
    assert_eq!(
        DynamoDbError::throttling("m", vec![]).full_error_type(),
        "com.amazon.coral.availability#ThrottlingException"
    );
    assert_eq!(
        DynamoDbError::provisioned_throughput_exceeded("m", vec![]).full_error_type(),
        "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"
    );
    assert_eq!(
        DynamoDbError::request_limit_exceeded("m", vec![]).full_error_type(),
        "com.amazonaws.dynamodb.v20120810#RequestLimitExceeded"
    );
}

/// An empty reasons list is still `Some` (the class is a throttle); every
/// other variant answers `None` and is not a throttle.
#[test]
fn non_throttle_errors_have_no_reasons() {
    assert_eq!(
        DynamoDbError::throttling("m", vec![]).throttling_reasons(),
        Some(&[][..])
    );
    for err in [
        DynamoDbError::ValidationException("v".to_owned()),
        DynamoDbError::InternalServerError("i".to_owned()),
        DynamoDbError::ServiceUnavailable("s".to_owned()),
        DynamoDbError::TransactionConflictException("c".to_owned()),
    ] {
        assert!(err.throttling_reasons().is_none(), "{err:?}");
        assert!(!err.is_throttle(), "{err:?}");
    }
}

/// The reason entry serializes with the two lower-case field names the
/// service uses, in that order.
#[test]
fn throttling_reason_serializes_with_the_measured_field_names() {
    let json = serde_json::to_string(&reason("TableReadProvisionedThroughputExceeded"))
        .expect("serialize");
    assert_eq!(
        json,
        r#"{"reason":"TableReadProvisionedThroughputExceeded","resource":"arn:aws:dynamodb:us-east-1:123456789012:table/T"}"#
    );
}
