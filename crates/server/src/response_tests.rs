// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the error bodies [`super::error_response`] sends.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, CancellationReason, Item};
use serde::de::{Deserializer as _, IgnoredAny, MapAccess, Visitor};

use super::error_response;

/// The top-level member names of the body `error_response` sends for
/// `error`, in the order of the bytes.
async fn body_members(error: &DynamoDbError) -> Vec<String> {
    struct Names;
    impl<'de> Visitor<'de> for Names {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an object")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut names = Vec::new();
            while let Some((name, IgnoredAny)) = map.next_entry::<String, IgnoredAny>()? {
                names.push(name);
            }
            Ok(names)
        }
    }
    let response = error_response(error, "request-1");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::Deserializer::from_slice(&bytes)
        .deserialize_map(Names)
        .expect("json object")
}

/// Each body writes `__type` first and the other members in the order the
/// service writes them, with the message under the key the service uses for
/// that error (us-east-1, raw bodies, 2026-10-29 to 2026-11-20).
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
    let rows: [(DynamoDbError, &[&str]); 7] = [
        (cancelled, &["__type", "CancellationReasons", "Message"]),
        (
            DynamoDbError::ConditionalCheckFailedException(failed.into(), Some(old_item)),
            &["__type", "Item", "message"],
        ),
        (
            DynamoDbError::ConditionalCheckFailedException(failed.into(), None),
            &["__type", "message"],
        ),
        (
            DynamoDbError::IdempotentParameterMismatchException(
                "Specified idempotent token was used with different request parameters \
                 within the idempotency window"
                    .to_owned(),
            ),
            &["__type", "Message"],
        ),
        (
            DynamoDbError::SerializationException(
                "STRING_VALUE cannot be converted to Integer".to_owned(),
            ),
            &["__type", "Message"],
        ),
        (
            DynamoDbError::SerializationException(String::new()),
            &["__type"],
        ),
        (
            DynamoDbError::ValidationException("One or more parameter values were invalid".into()),
            &["__type", "message"],
        ),
    ];
    let mut got = Vec::new();
    let mut want: Vec<(&str, Vec<String>)> = Vec::new();
    for (error, members) in &rows {
        got.push((error.error_type(), body_members(error).await));
        want.push((
            error.error_type(),
            members.iter().map(ToString::to_string).collect(),
        ));
    }
    assert_eq!(got, want);
}
