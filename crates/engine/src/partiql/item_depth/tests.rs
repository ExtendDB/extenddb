// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the item nesting check on a PartiQL INSERT or UPDATE.

use super::*;
use extenddb_core::partiql::{Statement, parse};

fn deep(levels: usize) -> AttributeValue {
    let mut value = AttributeValue::S("leaf".to_owned());
    for _ in 0..levels {
        value = AttributeValue::M(std::iter::once(("n".to_owned(), value)).collect());
    }
    value
}

fn update(sql: &str) -> UpdateStatement {
    match parse(sql).unwrap() {
        Statement::Update(u) => u,
        other => panic!("expected an UPDATE, got {other:?}"),
    }
}

#[test]
fn the_engine_item_refusal_gets_the_partiql_wording() {
    let mut item = Item::new();
    item.insert("x".to_owned(), deep(32));
    let refused = extenddb_core::validation::validate_item_nesting_depth(&item).unwrap_err();
    match item_nesting_error(refused) {
        DynamoDbError::ValidationException(m) => assert_eq!(m, ITEM_NESTING_MESSAGE),
        other => panic!("expected ValidationException, got {other:?}"),
    }
    let prefixed = DynamoDbError::ValidationException(format!(
        "1 validation error detected: {NESTING_MESSAGE}"
    ));
    assert!(matches!(
        item_nesting_error(prefixed),
        DynamoDbError::ValidationException(m) if m == ITEM_NESTING_MESSAGE
    ));
}

#[test]
fn other_errors_keep_their_wording() {
    let key = DynamoDbError::ValidationException(
        "One or more parameter values were invalid: Missing the key pk in the item".to_owned(),
    );
    assert!(matches!(
        item_nesting_error(key),
        DynamoDbError::ValidationException(m) if m.starts_with("One or more parameter values")
    ));
    assert!(matches!(
        item_nesting_error(DynamoDbError::DuplicateItemException("d".to_owned())),
        DynamoDbError::DuplicateItemException(_)
    ));
}

#[test]
fn a_set_value_of_32_levels_cannot_be_written() {
    let stmt = update("UPDATE \"t\" SET x = ?, y = ? WHERE pk = 'p'");
    assert!(!sets_a_value_past_the_item_limit(
        &stmt,
        &[deep(31), deep(31)]
    ));
    assert!(sets_a_value_past_the_item_limit(
        &stmt,
        &[deep(31), deep(32)]
    ));
}

#[test]
fn a_deep_value_in_the_where_clause_is_not_a_set_value() {
    let stmt = update("UPDATE \"t\" SET x = ? WHERE pk = 'p' AND y <> ?");
    assert!(!sets_a_value_past_the_item_limit(
        &stmt,
        &[deep(1), deep(32)]
    ));
}

#[test]
fn a_transaction_takes_the_item_check_path_only_for_a_value_it_stores() {
    let insert = parse("INSERT INTO \"t\" VALUE {'pk': 'p', 'x': ?}").unwrap();
    let condition = parse("UPDATE \"t\" SET a = 1 WHERE pk = 'p' AND x <> ?").unwrap();
    let member = |stmt: Statement, levels: usize| {
        let req = SingleRequest {
            statement: String::new(),
            parameters: vec![deep(levels)],
            parameter_numbers: vec![None],
            consistent_read: false,
            limit: None,
            next_token: None,
            return_consumed_capacity: None,
            return_values_on_condition_check_failure: None,
        };
        (req, stmt)
    };
    assert!(has_member_past_the_item_limit(&[member(
        insert.clone(),
        32
    )]));
    assert!(!has_member_past_the_item_limit(&[member(insert, 31)]));
    assert!(!has_member_past_the_item_limit(&[member(condition, 32)]));
}

#[test]
fn a_failed_condition_reports_the_stored_item_only_for_all_old() {
    assert_eq!(
        return_values_on_failure(Some("ALL_OLD")),
        ReturnValuesOnConditionCheckFailure::AllOld
    );
    assert_eq!(
        return_values_on_failure(Some("NONE")),
        ReturnValuesOnConditionCheckFailure::None
    );
    assert_eq!(
        return_values_on_failure(None),
        ReturnValuesOnConditionCheckFailure::None
    );
}
