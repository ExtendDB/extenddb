// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The update-expression parser tests.

use super::*;
use crate::expression::ast::{ArithOp, Expr, PathElement, UpdateAction};
use crate::expression::tokenizer::tokenize;

fn parse(input: &str) -> Result<Vec<UpdateAction>, DynamoDbError> {
    let tokens = tokenize(input)?;
    parse_update(&tokens)
}

#[test]
fn parse_simple_set() {
    let actions = parse("SET price = :p").unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], UpdateAction::Set { path, .. } if path.len() == 1));
}

#[test]
fn parse_multiple_set_actions() {
    let actions = parse("SET a = :v1, b = :v2, c = :v3").unwrap();
    assert_eq!(actions.len(), 3);
    assert!(
        actions
            .iter()
            .all(|a| matches!(a, UpdateAction::Set { .. }))
    );
}

#[test]
fn parse_set_with_arithmetic_add() {
    let actions = parse("SET price = price + :inc").unwrap();
    assert_eq!(actions.len(), 1);
    if let UpdateAction::Set { value, .. } = &actions[0] {
        assert!(matches!(
            value,
            Expr::Arithmetic {
                op: ArithOp::Add,
                ..
            }
        ));
    } else {
        panic!("Expected Set action");
    }
}

#[test]
fn parse_set_with_arithmetic_sub() {
    let actions = parse("SET stock = stock - :dec").unwrap();
    if let UpdateAction::Set { value, .. } = &actions[0] {
        assert!(matches!(
            value,
            Expr::Arithmetic {
                op: ArithOp::Sub,
                ..
            }
        ));
    } else {
        panic!("Expected Set action");
    }
}

#[test]
fn parse_set_with_if_not_exists() {
    let actions = parse("SET price = if_not_exists(price, :default)").unwrap();
    if let UpdateAction::Set { value, .. } = &actions[0] {
        if let Expr::Function { name, args } = value {
            assert_eq!(name, "if_not_exists");
            assert_eq!(args.len(), 2);
        } else {
            panic!("Expected Function expression");
        }
    } else {
        panic!("Expected Set action");
    }
}

#[test]
fn parse_set_with_list_append() {
    let actions = parse("SET tags = list_append(tags, :newtags)").unwrap();
    if let UpdateAction::Set { value, .. } = &actions[0] {
        assert!(matches!(value, Expr::Function { name, .. } if name == "list_append"));
    } else {
        panic!("Expected Set action");
    }
}

#[test]
fn parse_simple_remove() {
    let actions = parse("REMOVE oldattr").unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], UpdateAction::Remove { .. }));
}

#[test]
fn parse_multiple_remove() {
    let actions = parse("REMOVE a, b, c").unwrap();
    assert_eq!(actions.len(), 3);
}

#[test]
fn parse_set_and_remove_combined() {
    let actions = parse("SET a = :v REMOVE b").unwrap();
    assert_eq!(actions.len(), 2);
    assert!(matches!(&actions[0], UpdateAction::Set { .. }));
    assert!(matches!(&actions[1], UpdateAction::Remove { .. }));
}

#[test]
fn parse_add_action() {
    let actions = parse("ADD counter :inc").unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], UpdateAction::Add { .. }));
}

#[test]
fn parse_delete_action() {
    let actions = parse("DELETE colors :remove_colors").unwrap();
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], UpdateAction::Delete { .. }));
}

#[test]
fn parse_all_four_action_types() {
    let actions = parse("SET a = :v ADD b :inc DELETE c :rm REMOVE d").unwrap();
    assert_eq!(actions.len(), 4);
}

#[test]
fn parse_nested_path_in_set() {
    let actions = parse("SET address.city = :city").unwrap();
    if let UpdateAction::Set { path, .. } = &actions[0] {
        assert_eq!(path.len(), 2);
        assert_eq!(path[0], PathElement::Attribute("address".into()));
        assert_eq!(path[1], PathElement::Attribute("city".into()));
    } else {
        panic!("Expected Set action");
    }
}

#[test]
fn parse_list_index_in_set() {
    let actions = parse("SET items[0] = :v").unwrap();
    if let UpdateAction::Set { path, .. } = &actions[0] {
        assert_eq!(path.len(), 2);
        assert_eq!(path[1], PathElement::Index(0));
    } else {
        panic!("Expected Set action");
    }
}

#[test]
fn parse_empty_expression_rejected() {
    let tokens = tokenize("").unwrap();
    // Empty tokens, so parse_update must fail.
    let err = parse_update(&tokens).unwrap_err();
    assert!(
        matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("at least one action"))
    );
}

#[test]
fn parse_invalid_action_keyword_rejected() {
    let err = parse("price = :v").unwrap_err();
    assert!(matches!(err, DynamoDbError::ValidationException(msg) if msg.contains("expected SET")));
}

#[test]
fn parse_name_ref_in_path() {
    let actions = parse("SET #n = :v").unwrap();
    if let UpdateAction::Set { path, .. } = &actions[0] {
        assert_eq!(path[0], PathElement::Attribute("#n".into()));
    } else {
        panic!("Expected Set action");
    }
}

#[test]
fn redundant_parens_rejected() {
    let err = parse("SET c = ((c - :v))").unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg) if msg.contains("redundant parentheses")),
        "Expected redundant parens error, got: {err:?}"
    );
}

#[test]
fn single_parens_accepted() {
    let actions = parse("SET c = (c - :v)").unwrap();
    assert!(matches!(&actions[0], UpdateAction::Set { .. }));
}

#[test]
fn unmatched_paren_rejected() {
    // Opening paren with no matching close must be a ValidationException,
    // not a panic or a silently-accepted expression.
    let err = parse("SET c = (c - :v").unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(msg) if msg.contains("closing")),
        "Expected unmatched-paren error, got: {err:?}"
    );
}

#[test]
fn unmatched_close_paren_rejected() {
    // A stray closing paren with no matching open must also be rejected.
    let err = parse("SET c = c - :v)").unwrap_err();
    assert!(
        matches!(&err, DynamoDbError::ValidationException(_)),
        "Expected validation error for stray ')', got: {err:?}"
    );
}
