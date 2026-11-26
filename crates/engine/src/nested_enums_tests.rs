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
const ONE: &str = "1 validation error detected: ";

/// One fault of a `ValidationException` that lists member faults.
fn clause(value: &str, path: &str, constraint: &str) -> String {
    format!("Value '{value}' at '{path}' failed to satisfy constraint: {constraint}")
}

/// The text of the `ValidationException`, or `None` for a
/// `SerializationException`.
fn refused(value: &str, path: &str, set: &str) -> Option<String> {
    let constraint = format!("Member must satisfy enum value set: [{set}]");
    Some(format!("{ONE}{}", clause(value, path, &constraint)))
}

/// The faults of `first`, then of `second`, each the text of a
/// `ValidationException` with one fault.
fn both(first: Option<String>, second: Option<String>) -> Option<String> {
    let fault = |text: Option<String>| text.expect("one fault").replacen(ONE, "", 1);
    let (first, second) = (fault(first), fault(second));
    Some(format!("2 validation errors detected: {first}; {second}"))
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
    // A table keyed on pk and sk, with an LSI on l.
    let bad_lsi = || {
        json!({
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
        })
    };
    let gsi_fault = projection("EVERY_ATTRIBUTE", "globalSecondaryIndexes.1.member");
    let create_fault = projection(
        "ALL_ATTRIBUTES",
        "globalSecondaryIndexUpdates.1.member.create",
    );
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
            create(with(
                bad_lsi(),
                json!({"AttributeDefinitions": defs(&["pk", "sk", "l"])}),
            )),
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
    // Two members outside their sets (measured 2026-11-17). The service lists
    // the GSIs, then the stream view type, then the LSIs, and on UpdateTable
    // the `Create` entries, then the stream view type.
    rows.extend([
        (
            create(json!({
                "AttributeDefinitions": defs(&["pk", "g", "l"]),
                "GlobalSecondaryIndexes": [
                    index("gsi", "g", "EVERY_ATTRIBUTE"),
                    index("gsi2", "l", "keys_only"),
                ],
            })),
            both(
                gsi_fault.clone(),
                projection("keys_only", "globalSecondaryIndexes.2.member"),
            ),
        ),
        (
            create(with(bad_gsi(), stream(true, "NEW_IMAGES"))),
            both(gsi_fault.clone(), refused("NEW_IMAGES", VIEW, VIEW_SET)),
        ),
        (
            create(with(
                bad_lsi(),
                with(
                    bad_gsi(),
                    json!({"AttributeDefinitions": defs(&["pk", "sk", "g", "l"])}),
                ),
            )),
            both(
                gsi_fault.clone(),
                projection("INCLUDES", "localSecondaryIndexes.1.member"),
            ),
        ),
        (
            update(with(bad_create(), stream(true, "NEW_IMAGES"))),
            both(create_fault.clone(), refused("NEW_IMAGES", VIEW, VIEW_SET)),
        ),
        (
            update(json!({
                "AttributeDefinitions": defs(&["g", "l"]),
                "GlobalSecondaryIndexUpdates": [
                    {"Create": index("gsi", "g", "ALL_ATTRIBUTES")},
                    {"Create": index("gsi2", "l", "keys_only")},
                ],
            })),
            both(
                create_fault,
                projection("keys_only", "globalSecondaryIndexUpdates.2.member.create"),
            ),
        ),
    ]);
    // A member of the wrong JSON type answers first. `ReplicaUpdates` holds
    // objects (measured 2026-11-25).
    for (operation, body) in [
        create(with(bad_gsi(), json!({"AttributeDefinitions": "x"}))),
        create(with(bad_gsi(), json!({"Tags": "x"}))),
        update(with(bad_create(), json!({"AttributeDefinitions": "x"}))),
        update(with(bad_create(), json!({"ReplicaUpdates": "x"}))),
        update(json!({"ReplicaUpdates": ["x"]})),
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
