// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB's answers to `CreateTable` and `UpdateTable` requests with a
//! `ProjectionType` or `StreamViewType` outside its set (measured in
//! us-east-1 on 2026-11-13 and 2026-11-14, and beside a member of the wrong
//! JSON type on 2026-11-17), and with a `TableClass` or an
//! `SSESpecification` the service refuses (measured on 2026-11-13,
//! 2026-11-14 and 2026-11-18).

use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use crate::dispatch;
use crate::partiql::test_context::context;

const PROJECTION_SET: &str = "ALL, INCLUDE, KEYS_ONLY";
const VIEW_SET: &str = "OLD_IMAGE, KEYS_ONLY, NEW_AND_OLD_IMAGES, NEW_IMAGE";
const VIEW: &str = "streamSpecification.streamViewType";
const SSE_SET: &str = "AES256, KMS";
const SSE_TYPE: &str = "sSESpecification.sSEType";
const TABLE_CLASS: &str = "Invalid table-class parameter provided. Please try again with a \
                           valid table-class value: [STANDARD, STANDARD_INFREQUENT_ACCESS].";
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

/// The service's sentence for an `SSESpecification` whose members do not go
/// together.
fn invalid(reason: &str) -> Option<String> {
    Some(format!(
        "One or more parameter values were invalid: {reason}"
    ))
}

fn too_long(value: &str, path: &str, max: usize) -> Option<String> {
    let constraint = format!("Member must have length less than or equal to {max}");
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

/// A `CreateTable` of the issue's table, keyed on `pk` alone, with
/// `members`.
fn plain(members: Value) -> (&'static str, Value) {
    create(with(
        json!({"AttributeDefinitions": defs(&["pk"])}),
        members,
    ))
}

fn sse(spec: Value) -> (&'static str, Value) {
    plain(json!({"SSESpecification": spec}))
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
    let sse_bogus = refused("BOGUS", SSE_TYPE, SSE_SET);
    let kms_key = |key: &str| json!({"Enabled": true, "KMSMasterKeyId": key});
    let key_2049 = "k".repeat(2049);
    let key_fault = too_long(&key_2049, "sSESpecification.kMSMasterKeyId", 2048);
    let name_256 = "t".repeat(256);
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
        // A `TableClass` or an `SSESpecification` the service refuses.
        (
            sse(json!({"Enabled": true, "SSEType": "AES256"})),
            invalid("SSEType AES256 is not supported"),
        ),
        (
            sse(json!({"Enabled": false, "SSEType": "KMS"})),
            invalid("SSEType can not be specified if Enabled is false"),
        ),
        (
            sse(kms_key("alias/no-such-key")),
            invalid("SSEType KMS is required if KMSMasterKeyId is specified"),
        ),
        (
            sse(json!({"Enabled": false, "KMSMasterKeyId": "alias/no-such-key"})),
            invalid("KMSMasterKeyId can not be specified if Enabled is false"),
        ),
        (
            sse(json!({"Enabled": true, "SSEType": "KMS", "KMSMasterKeyId": key_2049})),
            key_fault.clone(),
        ),
        // The service lists the key's length ahead of the SSEType.
        (
            sse(json!({"Enabled": true, "SSEType": "BOGUS", "KMSMasterKeyId": key_2049})),
            both(key_fault.clone(), sse_bogus.clone()),
        ),
        // The service counts the key's length in characters.
        (
            sse(kms_key(&"k".repeat(2048))),
            invalid("SSEType KMS is required if KMSMasterKeyId is specified"),
        ),
        (
            sse(kms_key(&"\u{1F600}".repeat(1025))),
            invalid("SSEType KMS is required if KMSMasterKeyId is specified"),
        ),
        (
            create(with(
                bad_gsi(),
                json!({"SSESpecification": {"Enabled": true, "SSEType": "BOGUS"}}),
            )),
            both(gsi_fault.clone(), sse_bogus.clone()),
        ),
        (
            plain(with(
                stream(true, "NEW_IMAGES"),
                json!({"SSESpecification": {"Enabled": true, "SSEType": "BOGUS"}}),
            )),
            both(sse_bogus.clone(), refused("NEW_IMAGES", VIEW, VIEW_SET)),
        ),
        (
            create(with(
                bad_gsi(),
                json!({"SSESpecification": {"Enabled": true, "SSEType": "AES256"}}),
            )),
            gsi_fault.clone(),
        ),
        (
            plain(json!({"TableClass": "STANDARD_IA"})),
            Some(TABLE_CLASS.to_owned()),
        ),
        (
            plain(json!({"BillingMode": "ON_DEMAND", "TableClass": "PAY_PER_REQUEST"})),
            Some(TABLE_CLASS.to_owned()),
        ),
        (
            create(with(bad_gsi(), json!({"TableClass": "BOGUS"}))),
            Some(TABLE_CLASS.to_owned()),
        ),
        (
            plain(json!({"TableName": name_256, "TableClass": "BOGUS"})),
            too_long(&name_256, "tableName", 255),
        ),
        (plain(json!({"TableClass": "BOGUS", "Tags": "x"})), None),
        (
            update(json!({"TableClass": "STANDARD_IA"})),
            Some(TABLE_CLASS.to_owned()),
        ),
        (
            update(json!({"BillingMode": "ALSO_BOGUS", "TableClass": "BOGUS"})),
            Some(TABLE_CLASS.to_owned()),
        ),
        (
            update(json!({"TableClass": "BOGUS", "AttributeDefinitions": "x"})),
            None,
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
    for sse_type in ["BOGUS", "AES"] {
        rows.push((
            sse(json!({"Enabled": true, "SSEType": sse_type})),
            refused(sse_type, SSE_TYPE, SSE_SET),
        ));
    }
    // UpdateTable lists the SSE faults ahead of the stream view type and the
    // index faults (measured 2026-11-25).
    rows.extend([
        (
            update(with(
                stream(true, "BOGUS"),
                json!({"SSESpecification": {"Enabled": true, "SSEType": "BOGUS"}}),
            )),
            both(sse_bogus.clone(), refused("BOGUS", VIEW, VIEW_SET)),
        ),
        (
            update(json!({
                "GlobalSecondaryIndexUpdates": [{"Create": index("gsi1", "g", "BOGUS")}],
                "AttributeDefinitions": defs(&["g"]),
                "SSESpecification": {"Enabled": true, "SSEType": "BOGUS"},
            })),
            both(
                sse_bogus.clone(),
                projection("BOGUS", "globalSecondaryIndexUpdates.1.member.create"),
            ),
        ),
    ]);
    for (spec, expected) in [
        (
            json!({"Enabled": true, "SSEType": "AES256"}),
            invalid("SSEType AES256 is not supported"),
        ),
        (json!({"Enabled": true, "SSEType": "BOGUS"}), sse_bogus),
        (
            kms_key("alias/no-such-key"),
            invalid("SSEType KMS is required if KMSMasterKeyId is specified"),
        ),
        // Without Enabled, an SSEType or a KMSMasterKeyId reads as Enabled
        // true.
        (
            json!({"SSEType": "AES256"}),
            invalid("SSEType AES256 is not supported"),
        ),
        (
            json!({"KMSMasterKeyId": "alias/no-such-key"}),
            invalid("SSEType KMS is required if KMSMasterKeyId is specified"),
        ),
        // An Enabled written as a string reads as false when it is "false",
        // and as true otherwise (measured 2026-11-25).
        (
            json!({"Enabled": "true", "SSEType": "AES256"}),
            invalid("SSEType AES256 is not supported"),
        ),
        (
            json!({"Enabled": "yes", "SSEType": "AES256"}),
            invalid("SSEType AES256 is not supported"),
        ),
        (
            json!({"Enabled": "false", "SSEType": "KMS"}),
            invalid("SSEType can not be specified if Enabled is false"),
        ),
        // A specification of the wrong JSON type (measured 2026-11-25).
        (json!("x"), None),
        (json!({"Enabled": true, "SSEType": 5}), None),
    ] {
        rows.push((update(json!({"SSESpecification": spec})), expected));
    }
    // The service refuses a specification beside another change.
    let only =
        invalid("Server-Side Encryption modification must be the only operation in the request");
    let units = json!({"ReadCapacityUnits": 5, "WriteCapacityUnits": 5});
    for companion in [
        json!({"DeletionProtectionEnabled": false}),
        json!({"TableClass": "STANDARD_INFREQUENT_ACCESS"}),
        stream(true, "NEW_IMAGE"),
        json!({
            "AttributeDefinitions": defs(&["pk"]),
            "GlobalSecondaryIndexUpdates": [{"Create": index("gsi1", "pk", "ALL")}],
        }),
        json!({"ProvisionedThroughput": units}),
        json!({"OnDemandThroughput": {"MaxReadRequestUnits": 100}}),
        json!({"BillingMode": "PROVISIONED", "ProvisionedThroughput": units}),
        json!({"BillingMode": "PAY_PER_REQUEST"}),
    ] {
        let kms = json!({"SSESpecification": {"Enabled": true, "SSEType": "KMS"}});
        rows.push((update(with(companion, kms)), only.clone()));
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
