// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A key element whose type does not match the table's key schema, on the
//! single-item operations (issue #701).
//!
//! Measured on DynamoDB (us-east-1) on 2026-10-31 on a table keyed on `pk`
//! (S), with `pk` sent as N 1: `GetItem`, `DeleteItem` and `UpdateItem`
//! answer `The provided key element does not match the schema`, and `PutItem`
//! answers `One or more parameter values were invalid: Type mismatch for key
//! pk expected: S actual: N`.

use serde_json::json;

use super::{validate_delete_item, validate_get_item, validate_put_item, validate_update_item};
use crate::error::DynamoDbError;
use crate::limits::LimitsConfig;
use crate::types::{
    AttributeDefinition, DeleteItemInput, GetItemInput, KeySchemaElement, KeyType, PutItemInput,
    ScalarAttributeType, UpdateItemInput,
};

const SCHEMA_MISMATCH: &str = "The provided key element does not match the schema";

fn key_schema() -> Vec<KeySchemaElement> {
    vec![KeySchemaElement {
        attribute_name: "pk".to_owned(),
        key_type: KeyType::Hash,
    }]
}

fn attr_defs() -> Vec<AttributeDefinition> {
    vec![AttributeDefinition {
        attribute_name: "pk".to_owned(),
        attribute_type: ScalarAttributeType::S,
    }]
}

fn validation_message(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        other => panic!("expected a ValidationException, got {other:?}"),
    }
}

#[test]
fn get_item_answers_a_wrong_typed_key_with_the_schema_message() {
    let input: GetItemInput = serde_json::from_value(json!({
        "TableName": "TestTable",
        "Key": {"pk": {"N": "1"}}
    }))
    .expect("input");
    let result = validate_get_item(
        &input,
        &LimitsConfig::default(),
        &key_schema(),
        &attr_defs(),
    );
    assert_eq!(validation_message(result), SCHEMA_MISMATCH);
}

#[test]
fn delete_item_answers_a_wrong_typed_key_with_the_schema_message() {
    let input: DeleteItemInput = serde_json::from_value(json!({
        "TableName": "TestTable",
        "Key": {"pk": {"N": "1"}}
    }))
    .expect("input");
    let result = validate_delete_item(
        &input,
        &LimitsConfig::default(),
        &key_schema(),
        &attr_defs(),
    );
    assert_eq!(validation_message(result), SCHEMA_MISMATCH);
}

#[test]
fn update_item_answers_a_wrong_typed_key_with_the_schema_message() {
    let input: UpdateItemInput = serde_json::from_value(json!({
        "TableName": "TestTable",
        "Key": {"pk": {"N": "1"}},
        "UpdateExpression": "SET v = :v",
        "ExpressionAttributeValues": {":v": {"S": "x"}}
    }))
    .expect("input");
    let result = validate_update_item(
        &input,
        &LimitsConfig::default(),
        &key_schema(),
        &attr_defs(),
    );
    assert_eq!(validation_message(result), SCHEMA_MISMATCH);
}

#[test]
fn put_item_keeps_the_type_mismatch_message() {
    let input: PutItemInput = serde_json::from_value(json!({
        "TableName": "TestTable",
        "Item": {"pk": {"N": "1"}}
    }))
    .expect("input");
    let result = validate_put_item(
        &input,
        &LimitsConfig::default(),
        &key_schema(),
        &attr_defs(),
    );
    assert_eq!(
        validation_message(result),
        "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N"
    );
}
