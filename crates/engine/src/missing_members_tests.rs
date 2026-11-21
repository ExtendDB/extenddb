// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Requests without their `TableName`, `Key` or `Item`, with one of them as
//! JSON null, or with a member of the wrong JSON type. Each row is the
//! service's answer, measured on 2026-11-17, 2026-11-18 and 2026-11-20. The
//! context's catalog is an empty in-memory database, so the table `missing`
//! does not exist.

use extenddb_core::error::DynamoDbError;

use crate::partiql::test_context::{block_on, context};

/// The service's answer to a row.
#[derive(Debug)]
enum Want {
    /// `ValidationException` with a `Value null` clause for each path.
    Null(&'static [&'static str]),
    /// `ValidationException` with the text CreateTable gives a missing name.
    Required,
    /// `SerializationException`, in the words of the service's JSON reader.
    Serialization,
    /// An answer of another type: the body passes the read.
    Read,
}

use Want::{Null, Read, Required, Serialization};

/// In a body, `$K` stands for a key, `$Q` for a key condition and `$D` for a
/// table definition.
#[rustfmt::skip]
const ROWS: &[(&str, &str, Want)] = &[
    ("CreateTable", "{$D}", Required),
    ("CreateTable", r#"{"TableName": null, $D}"#, Required),
    ("CreateTable", r#"{"TableName": null, $D, "SSESpecification": 5}"#, Serialization),
    ("CreateTable", r#"{"TableName": null, $D, "ResourcePolicy": 5}"#, Serialization),
    ("CreateTable", r#"{"TableName": null, $D, "ResourcePolicy": "x"}"#, Required),
    ("CreateTable", r#"{"TableName": null, $D, "SSESpecification": {"Enabled": true}}"#, Required),
    ("UpdateTable", r#"{"BillingMode": "PAY_PER_REQUEST"}"#, Null(&["tableName"])),
    ("UpdateTable", r#"{"TableName": null, "BillingMode": "PAY_PER_REQUEST"}"#, Null(&["tableName"])),
    ("UpdateTable", r#"{"BillingMode": "BOGUS"}"#, Null(&["tableName"])),
    ("UpdateTable", r#"{"TableName": null, "BillingMode": "BOGUS"}"#, Null(&["tableName"])),
    ("UpdateTable", r#"{"BillingMode": 5}"#, Serialization),
    ("UpdateTable", r#"{"SSESpecification": 5}"#, Serialization),
    ("UpdateTable", r#"{"ReplicaUpdates": 5}"#, Serialization),
    ("UpdateTable", r#"{"TableName": "missing", "SSESpecification": 5}"#, Serialization),
    ("UpdateTable", r#"{"TableName": "missing", "ReplicaUpdates": 5}"#, Serialization),
    ("UpdateTable", r#"{"TableName": "missing", "MultiRegionConsistency": 5}"#, Serialization),
    ("UpdateTable", r#"{"TableName": "missing", "GlobalTableWitnessUpdates": 5}"#, Serialization),
    ("UpdateTable", r#"{"MultiRegionConsistency": 5}"#, Serialization),
    ("UpdateTable", r#"{"GlobalTableWitnessUpdates": 5}"#, Serialization),
    ("UpdateTable", r#"{"SSESpecification": "x"}"#, Serialization),
    ("UpdateTable", r#"{"SSESpecification": []}"#, Serialization),
    ("UpdateTable", r#"{"ReplicaUpdates": [5]}"#, Serialization),
    ("UpdateTable", r#"{"ReplicaUpdates": {}}"#, Serialization),
    ("UpdateTable", r#"{"TableName": "missing", "SSESpecification": {}}"#, Read),
    ("UpdateTable", r#"{"SSESpecification": {}}"#, Null(&["tableName"])),
    ("DescribeTable", "{}", Null(&["tableName"])),
    ("DescribeTable", r#"{"TableName": null}"#, Null(&["tableName"])),
    ("DeleteTable", "{}", Null(&["tableName"])),
    ("DeleteTable", r#"{"TableName": null}"#, Null(&["tableName"])),
    ("Query", "{$Q}", Null(&["tableName"])),
    ("Query", r#"{"TableName": null, $Q}"#, Null(&["tableName"])),
    ("Scan", "{}", Null(&["tableName"])),
    ("Scan", r#"{"TableName": null}"#, Null(&["tableName"])),
    ("Scan", r#"{"Limit": "x"}"#, Serialization),
    ("Scan", r#"{"TableName": null, "Limit": "x"}"#, Serialization),
    ("GetItem", "{}", Null(&["tableName", "key"])),
    ("GetItem", r#"{"Key": $K}"#, Null(&["tableName"])),
    ("GetItem", r#"{"TableName": null, "Key": $K}"#, Null(&["tableName"])),
    ("GetItem", r#"{"TableName": null, "Key": null}"#, Null(&["tableName", "key"])),
    ("GetItem", r#"{"TableName": "missing"}"#, Null(&["key"])),
    ("GetItem", r#"{"TableName": "missing", "Key": null}"#, Null(&["key"])),
    ("GetItem", r#"{"TableName": "missing", "ProjectionExpression": "a", "AttributesToGet": ["a"]}"#, Null(&["key"])),
    ("PutItem", "{}", Null(&["item", "tableName"])),
    ("PutItem", r#"{"Item": $K}"#, Null(&["tableName"])),
    ("PutItem", r#"{"TableName": null, "Item": $K}"#, Null(&["tableName"])),
    ("PutItem", r#"{"TableName": "missing"}"#, Null(&["item"])),
    ("PutItem", r#"{"TableName": "missing", "Item": null}"#, Null(&["item"])),
    ("PutItem", r#"{"TableName": "missing", "ReturnValues": 5}"#, Serialization),
    ("PutItem", r#"{"ReturnValues": 5}"#, Serialization),
    ("PutItem", r#"{"Item": $K, "ReturnValues": 5}"#, Serialization),
    ("DeleteItem", "{}", Null(&["key", "tableName"])),
    ("DeleteItem", r#"{"TableName": "missing"}"#, Null(&["key"])),
    ("DeleteItem", r#"{"TableName": "missing", "Key": null}"#, Null(&["key"])),
    ("UpdateItem", "{}", Null(&["tableName", "key"])),
    ("UpdateItem", r#"{"TableName": "missing"}"#, Null(&["key"])),
    ("UpdateItem", r#"{"TableName": "missing", "Key": null}"#, Null(&["key"])),
    ("UpdateItem", r#"{"TableName": "missing", "UpdateExpression": "SET a = :x", "ExpressionAttributeValues": {":x": {"S": "x"}}}"#, Null(&["key"])),
];

/// The service's message for the null clauses of `paths`.
fn null_message(paths: &[&str]) -> String {
    let clauses: Vec<String> = paths
        .iter()
        .map(|p| {
            format!("Value null at '{p}' failed to satisfy constraint: Member must not be null")
        })
        .collect();
    let s = if paths.len() == 1 { "" } else { "s" };
    format!(
        "{} validation error{s} detected: {}",
        paths.len(),
        clauses.join("; ")
    )
}

#[test]
fn a_missing_or_null_member_is_answered_as_the_service_answers_it() {
    block_on(async {
        let ctx = context(None).await;
        let mut wrong = Vec::new();
        for (operation, text, want) in ROWS {
            let text = text
                .replace("$K", r#"{"pk": {"S": "a"}}"#)
                .replace("$Q", r#""KeyConditionExpression": "pk = :v", "ExpressionAttributeValues": {":v": {"S": "a"}}"#)
                .replace("$D", r#""AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}], "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "BillingMode": "PAY_PER_REQUEST""#);
            let body = serde_json::from_str(&text).expect("a row's body is JSON");
            let got = crate::dispatch(operation, body, &ctx, "127.0.0.1:8000")
                .await
                .map(|r| r.body);
            let same = match (want, &got) {
                (Null(paths), Err(DynamoDbError::ValidationException(m))) => {
                    *m == null_message(paths)
                }
                (Required, Err(DynamoDbError::ValidationException(m))) => {
                    m == "The parameter 'TableName' is required but was not present in the request"
                }
                (Serialization, Err(DynamoDbError::SerializationException(_))) => true,
                (Read, _) => !matches!(got, Err(DynamoDbError::SerializationException(_))),
                _ => false,
            };
            if !same {
                wrong.push(format!("{operation} {text}: want {want:?}, got {got:?}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    });
}
