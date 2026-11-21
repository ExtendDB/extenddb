// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB's answers to `CreateTable` and `UpdateTable` requests with a
//! `ProjectionType` or `StreamViewType` outside its set (measured in
//! us-east-1 on 2026-11-13 and 2026-11-14, and beside a member of the wrong
//! JSON type on 2026-11-17).

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use crate::dispatch;
use crate::partiql::test_context::context;

const PROJECTION_SET: &str = "ALL, INCLUDE, KEYS_ONLY";
const VIEW_SET: &str = "OLD_IMAGE, KEYS_ONLY, NEW_AND_OLD_IMAGES, NEW_IMAGE";
const VIEW: &str = "streamSpecification.streamViewType";

/// The text of the `ValidationException`, or `None` for a
/// `SerializationException`.
fn refused(value: &str, path: &str, set: &str) -> Option<String> {
    Some(format!(
        "1 validation error detected: Value '{value}' at '{path}' failed to satisfy \
         constraint: Member must satisfy enum value set: [{set}]"
    ))
}

fn projection(value: &str, member: &str) -> Option<String> {
    let path = format!("{member}.projection.projectionType");
    refused(value, &path, PROJECTION_SET)
}

fn defs(names: &[&str]) -> Value {
    names
        .iter()
        .map(|n| json!({"AttributeName": n, "AttributeType": "S"}))
        .collect()
}

fn index(name: &str, attr: &str, projection: &str) -> Value {
    json!({
        "IndexName": name,
        "KeySchema": [{"AttributeName": attr, "KeyType": "HASH"}],
        "Projection": {"ProjectionType": projection},
    })
}

/// `base` with `members` set on top.
fn with(mut base: Value, members: Value) -> Value {
    for (member, value) in members.as_object().expect("members are an object") {
        base[member] = value.clone();
    }
    base
}

/// A `CreateTable` of an on-demand table keyed on `pk`, with `members`.
fn create(members: Value) -> (&'static str, Value) {
    let base = json!({
        "TableName": "new",
        "AttributeDefinitions": defs(&["pk", "g"]),
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
    });
    ("CreateTable", with(base, members))
}

/// An `UpdateTable` of a table that does not exist, with `members`.
fn update(members: Value) -> (&'static str, Value) {
    (
        "UpdateTable",
        with(json!({"TableName": "missing"}), members),
    )
}

fn stream(enabled: bool, view: &str) -> Value {
    json!({"StreamSpecification": {"StreamEnabled": enabled, "StreamViewType": view}})
}

fn rows() -> Vec<((&'static str, Value), Option<String>)> {
    let bad_gsi = || json!({"GlobalSecondaryIndexes": [index("gsi", "g", "EVERY_ATTRIBUTE")]});
    let bad_create = || {
        json!({
            "AttributeDefinitions": defs(&["g"]),
            "GlobalSecondaryIndexUpdates": [{"Create": index("gsi", "g", "ALL_ATTRIBUTES")}],
        })
    };
    let mut rows = vec![
        (
            create(bad_gsi()),
            projection("EVERY_ATTRIBUTE", "globalSecondaryIndexes.1.member"),
        ),
        (
            create(json!({
                "AttributeDefinitions": defs(&["pk", "g", "l"]),
                "GlobalSecondaryIndexes": [
                    index("gsi", "g", "ALL"),
                    index("gsi2", "l", "keys_only"),
                ],
            })),
            projection("keys_only", "globalSecondaryIndexes.2.member"),
        ),
        (
            create(json!({
                "AttributeDefinitions": defs(&["pk", "sk", "l"]),
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "LocalSecondaryIndexes": [{
                    "IndexName": "lsi",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "l", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "INCLUDES"},
                }],
            })),
            projection("INCLUDES", "localSecondaryIndexes.1.member"),
        ),
        (
            update(bad_create()),
            projection(
                "ALL_ATTRIBUTES",
                "globalSecondaryIndexUpdates.1.member.create",
            ),
        ),
        (
            create(stream(false, "NEW_IMAGES")),
            refused("NEW_IMAGES", VIEW, VIEW_SET),
        ),
        (
            create(stream(true, "NEW_IMAGES")),
            refused("NEW_IMAGES", VIEW, VIEW_SET),
        ),
    ];
    for view in ["NEW_AND_OLD_IMAGE", "keys_only", "", "TOTAL"] {
        rows.push((update(stream(true, view)), refused(view, VIEW, VIEW_SET)));
    }
    // A member of the wrong JSON type answers first.
    for (operation, body) in [
        create(with(bad_gsi(), json!({"AttributeDefinitions": "x"}))),
        create(with(bad_gsi(), json!({"Tags": "x"}))),
        update(with(bad_create(), json!({"AttributeDefinitions": "x"}))),
        update(with(bad_create(), json!({"ReplicaUpdates": "x"}))),
    ] {
        rows.push(((operation, body), None));
    }
    rows
}

/// Every row is refused before the engine reads or writes a table, so the
/// context's empty catalog is not read.
#[tokio::test]
async fn refused_enum_members() {
    let ctx = context(None).await;
    for ((operation, body), expected) in rows() {
        let shown = body.to_string();
        match (dispatch(operation, body, &ctx, "").await, expected) {
            (Err(DynamoDbError::ValidationException(m)), Some(expected)) => {
                assert_eq!(m, expected, "{operation} {shown}");
            }
            (Err(DynamoDbError::SerializationException(_)), None) => {}
            (Err(other), _) => panic!("{operation} answered {other:?} to {shown}"),
            (Ok(_), _) => panic!("{operation} accepted {shown}"),
        }
    }
}
