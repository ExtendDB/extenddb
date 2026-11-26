// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Table names shorter than 3 or longer than 255 characters (issue #1056).
//!
//! Each row is the service's answer, measured on 2026-11-13, 2026-11-14 and
//! 2026-11-18, except that a `RequestItems` row renders the key alone where
//! the service quotes its rendering of the whole map. The context's catalog
//! is an empty in-memory database, so a request that reached a table lookup
//! would get none of these answers.

use std::sync::Arc;

use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage_sqlite::SqliteConfig;

use crate::OperationContext;
use crate::partiql::test_context::{block_on, context};

const LEN3: &str = "Member must have length greater than or equal to 3";
const LEN255: &str = "Member must have length less than or equal to 255";
const MAP_KEYS: &str = "Map keys must satisfy constraint: \
    [Member must have length less than or equal to 255, \
    Member must have length greater than or equal to 3, \
    Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]";

fn at(value: &str, location: &str, clause: &str) -> String {
    format!("Value '{value}' at '{location}' failed to satisfy constraint: {clause}")
}

fn one(value: &str, location: &str, clause: &str) -> Option<String> {
    Some(format!(
        "1 validation error detected: {}",
        at(value, location, clause)
    ))
}

fn key(pk: &str) -> Value {
    json!({ "pk": { "S": pk }, "sk": { "S": "b" } })
}

/// `None` stands for `SerializationException`: the service answers a member
/// of the wrong type ahead of the name, with a message ExtendDB does not
/// match.
fn rows() -> Vec<(&'static str, Value, Option<String>)> {
    let n256 = "n".repeat(256);
    let k256 = "k".repeat(256);
    let put = |t: &str, pk: &str| json!({ "Put": { "TableName": t, "Item": key(pk) } });
    vec![
        (
            "GetItem",
            json!({ "TableName": "ab", "Key": { "pk": { "S": "a" } } }),
            one("ab", "tableName", LEN3),
        ),
        (
            "BatchGetItem",
            json!({ "RequestItems": { k256.as_str(): { "ConsistentRead": true,
                "ExpressionAttributeNames": { "#p": "pk" }, "Keys": [{ "pk": { "S": "a" } }],
                "ProjectionExpression": "#p" } } }),
            one(&k256, "requestItems", MAP_KEYS),
        ),
        (
            "BatchWriteItem",
            json!({ "RequestItems": { "ab": [{ "PutRequest": { "Item": { "pk": { "S": "a" } } } }] } }),
            one("ab", "requestItems", MAP_KEYS),
        ),
        (
            "TransactWriteItems",
            json!({ "TransactItems": [put("ab", "a"),
                { "Delete": { "TableName": n256, "Key": key("c") } }] }),
            Some(format!(
                "2 validation errors detected: {}; {}",
                at("ab", "transactItems.1.member.put.tableName", LEN3),
                at(&n256, "transactItems.2.member.delete.tableName", LEN255)
            )),
        ),
        ("GetItem", json!({ "TableName": "ab", "Key": "x" }), None),
        (
            "BatchGetItem",
            json!({ "RequestItems": { "ab": { "Keys": "x" } } }),
            None,
        ),
        (
            "BatchWriteItem",
            json!({ "RequestItems": { "ab": "x" } }),
            None,
        ),
        (
            "TransactWriteItems",
            json!({ "TransactItems": [{ "Put": { "TableName": "ab", "Item": "x" } }] }),
            None,
        ),
    ]
}

#[test]
fn a_name_of_a_bad_length_is_refused_before_the_lookup() {
    block_on(async {
        let ctx = context(None).await;
        for (op, body, expected) in rows() {
            let row = format!("{op} {body}");
            match (crate::dispatch(op, body, &ctx, "").await, expected) {
                (Err(DynamoDbError::ValidationException(m)), Some(want)) => {
                    assert_eq!(m, want, "{row}");
                }
                (Err(DynamoDbError::SerializationException(_)), None) => {}
                (Err(other), _) => panic!("{row}: {other:?}"),
                (Ok(_), _) => panic!("{row}: answered without an error"),
            }
        }
    });
}

/// A deployment whose `[limits]` bounds are 1 and 300 characters creates the
/// tables `ab` and one of 256 characters, and each operation accepts them.
#[test]
fn the_deployment_bounds_decide_which_lengths_are_refused() {
    block_on(async {
        let config = SqliteConfig {
            path: ":memory:".to_owned(),
            pool_size: 1,
        };
        let mut options = ServerComponentsOptions::default();
        options.release = "test";
        let components =
            (extenddb_storage_sqlite::backend().server_components)(&config, "us-east-1", options)
                .await
                .unwrap_or_else(|e| panic!("server components: {e:?}"));
        let account_id = components.catalog_store.default_account_id().await;
        let account_id = account_id.unwrap().expect("the bootstrapped account");
        components
            .catalog_store
            .set_setting("control_plane_delay_seconds", "0")
            .await
            .unwrap();
        let ctx = OperationContext {
            storage: components.engine,
            limits: Arc::new(LimitsConfig {
                min_table_name_length: 1,
                max_table_name_length: 300,
                ..LimitsConfig::default()
            }),
            account_id: Arc::from(account_id),
            ..context(None).await
        };
        let n256 = "n".repeat(256);
        let item = json!({ "pk": { "S": "a" } });
        for name in ["ab", n256.as_str()] {
            for (op, body) in [
                (
                    "CreateTable",
                    json!({ "TableName": name, "BillingMode": "PAY_PER_REQUEST",
                        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                        "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }] }),
                ),
                ("GetItem", json!({ "TableName": name, "Key": item })),
                (
                    "BatchGetItem",
                    json!({ "RequestItems": { name: { "Keys": [item] } } }),
                ),
                (
                    "BatchWriteItem",
                    json!({ "RequestItems": { name: [{ "PutRequest": { "Item": item } }] } }),
                ),
                (
                    "TransactWriteItems",
                    json!({ "TransactItems": [{ "Put": { "TableName": name, "Item": item } }] }),
                ),
            ] {
                if let Err(e) = crate::dispatch(op, body, &ctx, "").await {
                    panic!("{op} {name}: {e:?}");
                }
            }
        }
    });
}
