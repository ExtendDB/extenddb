// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the helpers the batch and transaction executors share.

use super::*;
use serde_json::json;

#[test]
fn a_conditional_failure_carries_its_item_on_the_error() {
    let mut item = extenddb_core::types::Item::new();
    item.insert("pk".to_owned(), AttributeValue::S("k".to_owned()));
    let err = DynamoDbError::ConditionalCheckFailedException(
        "The conditional request failed".to_owned(),
        Some(item),
    );
    assert_eq!(
        StatementError::from_error(&err).into_entry(Some("t")),
        json!({
            "Error": {
                "Code": "ConditionalCheckFailed",
                "Message": "The conditional request failed",
                "Item": { "pk": {"S": "k"} }
            },
            "TableName": "t"
        })
    );
}

fn where_of(condition: &str) -> WhereExpr {
    match extenddb_core::partiql::parse(&format!("SELECT * FROM \"t\" WHERE {condition}")) {
        Ok(Statement::Select(s)) => s.where_clause.unwrap(),
        other => panic!("not a select: {other:?}"),
    }
}

#[test]
fn a_key_with_two_values_keeps_the_first_values() {
    let a = Operand::Literal(AttributeValue::S("a".to_owned()));
    let x = Operand::Literal(AttributeValue::S("x".to_owned()));
    let w = where_of("pk = 'a' AND a = 1 AND pk = 'b'");
    match where_key(&w, "pk", None, &[]) {
        WhereKey::Conflict {
            name,
            first: Some((partition, None)),
        } => {
            assert_eq!(name, "pk");
            assert_eq!(partition, &a);
        }
        _ => panic!("expected a conflict with a full key"),
    }
    let w = where_of("sk = 'x' AND pk = 'a' AND sk = 'y' AND pk = 'b'");
    match where_key(&w, "pk", Some("sk"), &[]) {
        WhereKey::Conflict {
            name,
            first: Some((partition, Some(sort))),
        } => {
            assert_eq!(name, "sk");
            assert_eq!((partition, sort), (&a, &x));
        }
        _ => panic!("expected a conflict with a full key"),
    }
    let w = where_of("pk = 'a' AND pk = 'b' AND a = 1");
    assert!(matches!(
        where_key(&w, "pk", Some("sk"), &[]),
        WhereKey::Conflict { first: None, .. }
    ));
    let w = where_of("pk = 'a' AND 'a' = pk AND a = 1");
    assert!(matches!(
        where_key(&w, "pk", None, &[]),
        WhereKey::Pinned { .. }
    ));
}

#[test]
fn a_key_past_its_size_limit_names_the_batch_table() {
    let size = DynamoDbError::ValidationException(
        "One or more parameter values were invalid: Size of hashkey has exceeded the \
         maximum size limit of2048 bytes"
            .to_owned(),
    );
    let key_type = DynamoDbError::ValidationException(key_type_error("pk").message().to_owned());
    assert_eq!(
        StatementError::from_key_error(&size).into_entry(Some("t"))["TableName"],
        json!("t")
    );
    assert!(
        StatementError::from_key_error(&key_type)
            .into_entry(Some("t"))
            .get("TableName")
            .is_none()
    );
    assert!(
        StatementError::from_error(&size)
            .into_entry(Some("t"))
            .get("TableName")
            .is_none()
    );
}

#[test]
fn a_conditional_failure_without_an_item_has_none() {
    let err = DynamoDbError::ConditionalCheckFailedException(
        "The conditional request failed".to_owned(),
        None,
    );
    assert_eq!(
        StatementError::from_error(&err).into_entry(Some("t")),
        json!({
            "Error": {
                "Code": "ConditionalCheckFailed",
                "Message": "The conditional request failed"
            },
            "TableName": "t"
        })
    );
}

#[test]
fn a_wire_key_value_has_the_type_the_table_defines() {
    use extenddb_core::types::AttributeDefinition;
    let key_info = TableKeyInfo {
        table_name: "t".to_owned(),
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::N,
        }],
        ..TableKeyInfo::default()
    };
    assert!(wire_key_value_matches(&key_info, "pk", &json!({"N": "1"})));
    assert!(!wire_key_value_matches(&key_info, "pk", &json!({"S": "1"})));
    // A value that is no attribute value has no type.
    assert!(!wire_key_value_matches(
        &key_info,
        "pk",
        &json!({"N": "1", "S": "1"})
    ));
    assert!(!wire_key_value_matches(&key_info, "pk", &json!("1")));
    // An attribute the table defines no type for takes any value.
    assert!(wire_key_value_matches(&key_info, "sk", &json!({"S": "1"})));
}
