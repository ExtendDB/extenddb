// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The three throttle error classes: status, type name, namespace, reasons,
//! and the error document of each.

use super::*;

const ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/T";

fn reason(code: &str) -> ThrottlingReason {
    ThrottlingReason {
        reason: code.to_owned(),
        resource: ARN.to_owned(),
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
        assert_eq!(reasons[0].resource, ARN);
    }
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

/// The error document of each throttle class: the namespace, the reasons
/// field and its casing (`ThrottlingReasons` on the provisioned class and
/// `RequestLimitExceeded`, `throttlingReasons` on `ThrottlingException`), the
/// list omitted when empty, and the message. The three measured messages are
/// written out here, so a change to a message constant fails this test.
#[test]
fn throttle_error_documents_match_the_service() {
    let one = |code: &str| vec![reason(code)];
    let listed = |code: &str| serde_json::json!([{"reason": code, "resource": ARN}]);
    let provisioned = "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException";
    let throttling = "com.amazon.coral.availability#ThrottlingException";
    let request_limit = "com.amazonaws.dynamodb.v20120810#RequestLimitExceeded";
    let measured_provisioned = "The level of configured provisioned throughput for the table was exceeded. Consider increasing your provisioning level with the UpdateTable API.";
    let measured_hot_key = "Throughput exceeds the current capacity of your table or index. DynamoDB is automatically scaling your table or index so please try again shortly. If exceptions persist, check if you have a hot key: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html";
    let measured_caps =
        "Throughput exceeds the maximum OnDemandThroughput configured on table or index";
    let cases = [
        (
            DynamoDbError::provisioned_throughput_exceeded(
                PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                one("TableWriteProvisionedThroughputExceeded"),
            ),
            serde_json::json!({
                "__type": provisioned,
                "message": measured_provisioned,
                "ThrottlingReasons": listed("TableWriteProvisionedThroughputExceeded"),
            }),
        ),
        (
            DynamoDbError::throttling(
                THROTTLING_EXCEPTION_MESSAGE,
                one("TableWriteKeyRangeThroughputExceeded"),
            ),
            serde_json::json!({
                "__type": throttling,
                "message": measured_hot_key,
                "throttlingReasons": listed("TableWriteKeyRangeThroughputExceeded"),
            }),
        ),
        (
            DynamoDbError::throttling(
                MAX_ON_DEMAND_THROUGHPUT_MESSAGE,
                one("TableReadMaxOnDemandThroughputExceeded"),
            ),
            serde_json::json!({
                "__type": throttling,
                "message": measured_caps,
                "throttlingReasons": listed("TableReadMaxOnDemandThroughputExceeded"),
            }),
        ),
        (
            DynamoDbError::request_limit_exceeded(
                "Too many requests",
                one("TableWriteAccountLimitExceeded"),
            ),
            serde_json::json!({
                "__type": request_limit,
                "message": "Too many requests",
                "ThrottlingReasons": listed("TableWriteAccountLimitExceeded"),
            }),
        ),
        // A hot-key read on an on-demand table: no reasons list under either
        // spelling, which is what the service sends (5,615 of 5,615).
        (
            DynamoDbError::throttling(THROTTLING_EXCEPTION_MESSAGE, vec![]),
            serde_json::json!({"__type": throttling, "message": measured_hot_key}),
        ),
        (
            DynamoDbError::provisioned_throughput_exceeded("m", vec![]),
            serde_json::json!({"__type": provisioned, "message": "m"}),
        ),
        (
            DynamoDbError::request_limit_exceeded("m", vec![]),
            serde_json::json!({"__type": request_limit, "message": "m"}),
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(error_document(&error), expected, "{error:?}");
    }
}
