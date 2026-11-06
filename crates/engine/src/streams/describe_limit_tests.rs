// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DescribeStream` refuses a `Limit` below 1 or above 100 with the service's
//! messages before storage is reached, and passes the bounds themselves
//! through.
//!
//! The messages were measured on 2026-10-22 in us-east-1 with
//! `DescribeStream` on an existing stream, the SDK's own parameter
//! validation switched off so the requests reached the service:
//!
//! ```text
//! Limit 0 and Limit -1:
//! 400 {"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}
//! Limit 101 and Limit 1000:
//! 400 {"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Limit cannot be greater than the maximum: 100"}
//! ```
//!
//! Limit 1 returned one shard and `LastEvaluatedShardId`. The below-one text
//! leaves out the value, where this crate's `ListTables` and `Scan` messages
//! for the same constraint include it.
//!
//! The order of the checks against a missing stream is unmeasured: the
//! measurement ran on an existing stream only, so whether the service
//! answers a bad `Limit` on a missing stream with `ValidationException` or
//! `ResourceNotFoundException` is unknown. The handler refuses the `Limit`
//! before it looks the stream up.

use serde_json::json;

use extenddb_core::error::DynamoDbError;

use super::handle_describe_stream;
use super::test_storage::{STREAM_ARN, StreamStorage, context};

const BELOW_ONE: &str = "1 validation error detected: Value at 'limit' failed to satisfy \
                         constraint: Member must have value greater than or equal to 1";
const ABOVE_MAX: &str =
    "One or more parameter values were invalid: Limit cannot be greater than the maximum: 100";

#[tokio::test]
async fn describe_stream_refuses_a_limit_outside_1_to_100_before_storage() {
    let storage = StreamStorage::new();
    let ctx = context(&storage);
    for (limit, expected) in [
        (0, BELOW_ONE),
        (-3, BELOW_ONE),
        (101, ABOVE_MAX),
        (1000, ABOVE_MAX),
    ] {
        let body = json!({"StreamArn": STREAM_ARN, "Limit": limit});
        match handle_describe_stream(body, &ctx).await {
            Err(DynamoDbError::ValidationException(m)) => assert_eq!(m, expected, "Limit {limit}"),
            other => panic!("Limit {limit}: expected ValidationException, got {other:?}"),
        }
    }
    assert_eq!(
        storage.describe_calls(),
        0,
        "no refused Limit reached storage"
    );
}

#[tokio::test]
async fn describe_stream_passes_limits_1_and_100_to_storage() {
    let storage = StreamStorage::new();
    let ctx = context(&storage);
    for limit in [1, 100] {
        let body = json!({"StreamArn": STREAM_ARN, "Limit": limit});
        let described = handle_describe_stream(body, &ctx).await;
        assert!(described.is_ok(), "Limit {limit}: {described:?}");
    }
    assert_eq!(storage.describe_calls(), 2);
}
