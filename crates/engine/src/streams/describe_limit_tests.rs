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
//!
//! The tests run the handler on the engine's in-memory SQLite test context
//! (`partiql::test_context::context`). Its database holds no stream, so a
//! request that reaches storage is answered with an error that is neither
//! `Limit` message. A refused `Limit` carries its exact message, which shows
//! the check ran before storage. For Limit 1 and Limit 100 the tests assert
//! only that neither `Limit` message comes back: the crate has no helper that
//! opens a migrated in-memory engine, so no stream exists to describe.

use serde_json::json;

use extenddb_core::error::DynamoDbError;

use super::handle_describe_stream;
use crate::partiql::test_context::context;

/// A stream ARN in the context's account. Its database holds no table, so a
/// request that reaches storage is not answered with a stream description.
const STREAM_ARN: &str =
    "arn:aws:dynamodb:us-east-1:000000000000:table/Orders/stream/2026-10-22T00:00:00.000";

const BELOW_ONE: &str = "1 validation error detected: Value at 'limit' failed to satisfy \
                         constraint: Member must have value greater than or equal to 1";
const ABOVE_MAX: &str =
    "One or more parameter values were invalid: Limit cannot be greater than the maximum: 100";

#[tokio::test]
async fn describe_stream_refuses_a_limit_outside_1_to_100_before_storage() {
    let ctx = context(None).await;
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
}

#[tokio::test]
async fn describe_stream_passes_limits_1_and_100() {
    let ctx = context(None).await;
    for limit in [1, 100] {
        let body = json!({"StreamArn": STREAM_ARN, "Limit": limit});
        let answer = handle_describe_stream(body, &ctx).await;
        let refused = matches!(
            &answer,
            Err(DynamoDbError::ValidationException(m)) if m == BELOW_ONE || m == ABOVE_MAX
        );
        assert!(!refused, "Limit {limit} was refused: {answer:?}");
    }
}
