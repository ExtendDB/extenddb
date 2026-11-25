// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DescribeStream` refuses a `Limit` below 1 together with the other member
//! constraints before it looks the stream up, and a `Limit` above 100 only
//! after it has found the stream.
//!
//! Measured in us-east-1 with the SDK's own parameter validation switched
//! off, so the requests reached the service. On 2026-10-22, on an existing
//! stream:
//!
//! ```text
//! Limit 0 and Limit -1:
//! 400 {"__type":"com.amazon.coral.validate#ValidationException","message":"1 validation error detected: Value at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"}
//! Limit 101 and Limit 1000:
//! 400 {"__type":"com.amazon.coral.validate#ValidationException","message":"One or more parameter values were invalid: Limit cannot be greater than the maximum: 100"}
//! ```
//!
//! On 2026-11-25, each request twice, on a stream of a table that does not
//! exist: Limit 0 and -1 got the first body above; Limit 1, 101 and 1000, and
//! Limit 101 with a 36-character `ExclusiveStartShardId`, got 400
//! `ResourceNotFoundException` "Requested resource not found: Stream: <arn>
//! not found"; Limit 0 with a 13-character `ExclusiveStartShardId` got the
//! two clauses in one message ([`SHORT_START_AND_BELOW_ONE`]); Limit 101 with
//! that start got the start clause alone ([`SHORT_START`]).
//!
//! The tests run the handler on the engine's test context over a migrated
//! in-memory engine, so a request that reaches storage for a missing stream
//! gets `ResourceNotFoundException`.

use std::sync::Arc;

use serde_json::json;

use extenddb_core::error::DynamoDbError;

use super::handle_describe_stream;
use crate::OperationContext;
use crate::partiql::test_context::context;

const MISSING: &str =
    "arn:aws:dynamodb:us-east-1:000000000000:table/Missing/stream/2026-10-22T00:00:00.000";
const SHORT_ID: &str = "shardId-short";
const VALID_ID: &str = "shardId-00000001700000000000-00000000";

const BELOW_ONE: &str = "1 validation error detected: Value at 'limit' failed to satisfy \
                         constraint: Member must have value greater than or equal to 1";
const ABOVE_MAX: &str =
    "One or more parameter values were invalid: Limit cannot be greater than the maximum: 100";
const SHORT_START: &str = "1 validation error detected: Value at 'exclusiveStartShardId' \
                           failed to satisfy constraint: Member must have length greater \
                           than or equal to 28";
const SHORT_START_AND_BELOW_ONE: &str = "2 validation errors detected: Value at \
     'exclusiveStartShardId' failed to satisfy constraint: Member must have length greater \
     than or equal to 28; Value at 'limit' failed to satisfy constraint: Member must have \
     value greater than or equal to 1";

/// The answer a row expects.
enum Answer {
    Refused(&'static str),
    NotFound,
    Served,
}

/// The test context over a migrated in-memory engine holding the table
/// `Orders` with a stream. Returns the context and the stream's ARN.
async fn context_with_stream() -> (OperationContext, String) {
    let mut ctx = context(None).await;
    let storage =
        extenddb_storage_sqlite::SqliteEngine::new_in_memory("us-east-1", &ctx.account_id, 409_600)
            .await
            .expect("a migrated in-memory engine opens");
    ctx.storage = Arc::new(storage);
    let created = crate::handle_create_table(
        json!({
            "TableName": "Orders",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}
        }),
        &ctx,
    )
    .await
    .expect("the table is created");
    let arn = created["TableDescription"]["LatestStreamArn"]
        .as_str()
        .expect("the table has a stream")
        .to_owned();
    (ctx, arn)
}

#[tokio::test]
async fn describe_stream_checks_limit_where_the_service_does() {
    let (ctx, existing) = context_with_stream().await;
    let not_found = format!("Requested resource not found: Stream: {MISSING} not found");
    let rows: [(&str, i64, Option<&str>, Answer); 14] = [
        (existing.as_str(), 0, None, Answer::Refused(BELOW_ONE)),
        (existing.as_str(), -1, None, Answer::Refused(BELOW_ONE)),
        (existing.as_str(), 101, None, Answer::Refused(ABOVE_MAX)),
        (existing.as_str(), 1000, None, Answer::Refused(ABOVE_MAX)),
        (existing.as_str(), 1, None, Answer::Served),
        (existing.as_str(), 100, None, Answer::Served),
        (MISSING, 0, None, Answer::Refused(BELOW_ONE)),
        (MISSING, -1, None, Answer::Refused(BELOW_ONE)),
        (MISSING, 1, None, Answer::NotFound),
        (MISSING, 101, None, Answer::NotFound),
        (MISSING, 1000, None, Answer::NotFound),
        (
            MISSING,
            0,
            Some(SHORT_ID),
            Answer::Refused(SHORT_START_AND_BELOW_ONE),
        ),
        (MISSING, 101, Some(SHORT_ID), Answer::Refused(SHORT_START)),
        (MISSING, 101, Some(VALID_ID), Answer::NotFound),
    ];
    for (arn, limit, start, expected) in rows {
        let mut body = json!({"StreamArn": arn, "Limit": limit});
        if let Some(start) = start {
            body["ExclusiveStartShardId"] = json!(start);
        }
        let row = format!("{arn} Limit {limit} start {start:?}");
        let answer = handle_describe_stream(body, &ctx).await;
        match (answer, expected) {
            (Err(DynamoDbError::ValidationException(m)), Answer::Refused(e)) => {
                assert_eq!(m, e, "{row}");
            }
            (Err(DynamoDbError::ResourceNotFoundException(m)), Answer::NotFound) => {
                assert_eq!(m, not_found, "{row}");
            }
            (Ok(v), Answer::Served) => {
                assert_eq!(v["StreamDescription"]["StreamArn"], json!(arn), "{row}");
            }
            (other, _) => panic!("{row}: got {other:?}"),
        }
    }
}
