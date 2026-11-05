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
fn a_path_into_a_key_tested_against_values_is_refused_in_statement_order() {
    let into = |condition: &str| match where_key(&where_of(condition), "pk", Some("sk"), &[]) {
        WhereKey::IntoKey { name } => Some(name),
        WhereKey::Conflict { name, .. } => Some(format!("conflict {name}")),
        WhereKey::Pinned { .. } | WhereKey::Partial => None,
    };
    for (condition, want) in [
        ("pk = 'k' AND sk = 'b' AND pk.c = 1", Some("pk")),
        ("pk = 'k' AND sk = 'b' AND 1 = sk[0]", Some("sk")),
        ("pk = 'k' AND sk = 'b' AND pk.c BETWEEN 1 AND 2", Some("pk")),
        ("pk = 'k' AND sk = 'b' AND pk.c IN [1, ?]", Some("pk")),
        (
            "pk = 'k' AND sk = 'b' AND begins_with(pk.c, 'x')",
            Some("pk"),
        ),
        ("pk = 'k' AND sk = 'b' AND pk.c IS NULL", Some("pk")),
        ("pk = 'k' AND sk = 'b' AND pk.c = 1 = false", Some("pk")),
        ("pk = 'k' AND pk.c = 1", Some("pk")),
        (
            "pk = 'k' AND sk = 'b' AND sk.c = 1 AND pk.d = 1",
            Some("sk"),
        ),
        (
            "pk = 'k' AND sk = 'b' AND pk.c = 1 AND pk = 'z'",
            Some("pk"),
        ),
        (
            "pk = 'k' AND pk = 'z' AND sk = 'b' AND sk.c = 1",
            Some("conflict pk"),
        ),
        // The other conditions with a path into a key are left to the
        // refusal made once the key is checked.
        ("pk = 'k' AND sk = 'b' AND NOT pk.c = 1", None),
        ("pk = 'k' AND sk = 'b' AND pk.c = a", None),
        ("pk = 'k' AND sk = 'b' AND pk.c IS NOT NULL", None),
        ("pk = 'k' AND sk = 'b' AND size(pk.c) > 1", None),
        ("pk = 'k' AND sk = 'b' AND a.c = 1", None),
    ] {
        assert_eq!(into(condition).as_deref(), want, "{condition}");
    }
}

#[test]
fn a_path_into_a_key_among_the_other_conditions_names_the_sort_key_first() {
    let refusal = |condition: &str| {
        condition_key_refusal(&where_of(condition), "pk", Some("sk"), &[])
            .map(|m| m.strip_prefix(CONDITION_KEY_REFUSAL).unwrap().to_owned())
    };
    for (condition, want) in [
        ("pk = 'k' AND sk = 'b' AND NOT pk.c = 1", Some("pk")),
        ("pk = 'k' AND sk = 'b' AND (a = 1 OR pk.c = 1)", Some("pk")),
        (
            "pk = 'k' AND sk = 'b' AND attribute_exists(pk.c)",
            Some("pk"),
        ),
        ("pk = 'k' AND sk = 'b' AND pk.c IS MISSING", Some("pk")),
        (
            "pk = 'k' AND sk = 'b' AND NOT pk.c = 1 AND NOT sk.d = 1",
            Some("sk"),
        ),
        (
            "pk = 'k' AND sk = 'b' AND NOT sk.d = 1 AND NOT pk.c = 1",
            Some("sk"),
        ),
        ("pk = 'k' AND sk = 'b' AND pk.c = sk.d", Some("sk")),
        (
            "pk = 'k' AND sk = 'b' AND (pk.c = 1 OR sk = 'b')",
            Some("pk"),
        ),
        ("pk = 'k' AND sk = 'b' AND NOT a.c = 1", None),
        // Not a pinned item: refused before.
        ("pk = 'k' AND NOT pk.c = 1", None),
        ("pk = 'k' AND sk = 'b' AND pk.c = 1", None),
    ] {
        assert_eq!(refusal(condition).as_deref(), want, "{condition}");
    }
}

#[test]
fn a_path_into_a_key_among_the_other_conditions_names_the_batch_table() {
    let entry = |message: String| {
        StatementError::from_key_error(&DynamoDbError::ValidationException(message))
            .into_entry(Some("t"))
    };
    assert_eq!(
        entry(format!("{CONDITION_KEY_REFUSAL}pk"))["TableName"],
        json!("t")
    );
    assert!(
        entry(format!(
            "{}pk",
            extenddb_core::partiql::plan::SCALAR_KEY_REFUSAL
        ))
        .get("TableName")
        .is_none()
    );
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

/// An INSERT whose VALUE gives `pk` the values `first` then `last`, built by
/// hand: the parser refuses such a VALUE.
fn insert_naming_pk_twice(first: &str, last: &str) -> Statement {
    let s = |v: &str| Operand::Literal(AttributeValue::S(v.to_owned()));
    Statement::Insert(InsertStatement {
        table: "t".to_owned(),
        value: vec![
            ("pk".to_owned(), s(first)),
            ("sk".to_owned(), s("x")),
            ("pk".to_owned(), s(last)),
        ],
    })
}

#[test]
fn an_insert_has_one_value_for_each_name_the_last_given() {
    let stmt = insert_naming_pk_twice("a", "b");
    let Statement::Insert(insert) = &stmt else {
        unreachable!("built as an INSERT");
    };
    let values = insert_values(insert);
    assert_eq!(values.len(), 2);
    assert_eq!(
        values.get("pk"),
        Some(&&Operand::Literal(AttributeValue::S("b".to_owned())))
    );
    // The key the duplicate check reads is the one the item stores.
    let key = write_key(&stmt, "pk", Some("sk"), &[]).unwrap().unwrap();
    assert_eq!(
        key,
        json!({"pk": {"S": "b"}, "sk": {"S": "x"}})
            .as_object()
            .unwrap()
            .clone()
    );
}
