// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The list indexes Amazon DynamoDB refuses in the paths of each expression
//! (measured in us-east-1 on 2026-11-13, 2026-11-14 and 2026-11-17).

use crate::error::DynamoDbError;
use crate::expression::key_condition::parse_key_condition;
use crate::expression::parser::parse_condition;
use crate::expression::projection::parse_projection;
use crate::expression::tokenizer::tokenize;
use crate::expression::update_parser::parse_update;

/// A parser whose paths `parse_path` reads.
#[derive(Debug, Clone, Copy)]
enum Grammar {
    Projection,
    Update,
    Condition,
    KeyCondition,
}

use Grammar::{Condition, KeyCondition, Projection, Update};

/// The message `grammar` refuses `text` with, or `None` when it parses.
fn refusal(grammar: Grammar, text: &str) -> Option<String> {
    let tokens = tokenize(text).expect("tokens");
    let parsed = match grammar {
        Projection => parse_projection(&tokens).map(drop),
        Update => parse_update(&tokens).map(drop),
        Condition => parse_condition(&tokens).map(drop),
        KeyCondition => parse_key_condition(&tokens).map(drop),
    };
    match parsed {
        Ok(()) => None,
        Err(DynamoDbError::ValidationException(message)) => Some(message),
        Err(other) => panic!("{text}: {other:?}"),
    }
}

/// An index up to 2147483647 parses. A larger one is refused with the index as
/// written, the first one of the expression.
#[test]
fn an_index_above_2147483647_is_refused() {
    let rows = [
        (Projection, "l[2147483647]", None),
        (Projection, "l[2147483648]", Some("2147483648")),
        (Projection, "l[4294967296]", Some("4294967296")),
        (
            Projection,
            "l[99999999999999999999]",
            Some("99999999999999999999"),
        ),
        (
            Projection,
            "a[1], l[99999999999], m[99999999999999]",
            Some("99999999999"),
        ),
        (Projection, "l[0][99999999999]", Some("99999999999")),
        (Update, "SET l[2147483647] = :v", None),
        (Update, "SET l[2147483648] = :v", Some("2147483648")),
        (Update, "SET x = l[2147483648]", Some("2147483648")),
        (
            Update,
            "SET a[1] = :v, l[99999999999] = :v, m[99999999999999] = :v",
            Some("99999999999"),
        ),
        (Update, "REMOVE l[2147483647]", None),
        (
            Update,
            "REMOVE l[99999999999999999999]",
            Some("99999999999999999999"),
        ),
        (Update, "ADD l[2147483648] :n", Some("2147483648")),
        (Update, "DELETE l[2147483648] :s", Some("2147483648")),
        (Condition, "l[2147483647] = :v", None),
        (Condition, "l[2147483648] = :v", Some("2147483648")),
        (
            Condition,
            "attribute_exists(l[2147483648])",
            Some("2147483648"),
        ),
        (
            KeyCondition,
            "pk = :p AND l[2147483648] = :v",
            Some("2147483648"),
        ),
    ];
    for (grammar, text, index) in rows {
        let expected = index.map(|index| {
            format!(
                "Invalid expression: List index is not within the allowable range; index: [{index}]"
            )
        });
        assert_eq!(refusal(grammar, text), expected, "{grammar:?} {text}");
    }
}

/// An index of two or more digits that starts with `0` is a syntax error, ahead
/// of an index out of range anywhere in the expression.
#[test]
fn an_index_with_a_leading_zero_is_a_syntax_error() {
    let rows = [
        (Projection, "l[01]", "1", "01]"),
        (Projection, "l[0000000001]", "0", "000"),
        (Projection, "l[00]", "0", "00]"),
        (Projection, "l[012]", "12", "012]"),
        (Projection, "l[01].x", "1", "01]"),
        (Projection, "l[02147483648]", "2147483648", "02147483648]"),
        (Projection, "l[99999999999], l[01]", "1", "01]"),
        (Projection, "l[01], l[99999999999]", "1", "01]"),
        (Update, "SET l[01] = :v", "1", "01]"),
        (Update, "SET x = l[00]", "0", "00]"),
        (Update, "REMOVE l[00]", "0", "00]"),
        (Update, "ADD l[01] :n", "1", "01]"),
        (Update, "DELETE l[01] :s", "1", "01]"),
        (Condition, "l[00] = :v", "0", "00]"),
        (Condition, "attribute_exists(l[01])", "1", "01]"),
        (KeyCondition, "pk = :p AND l[01] = :v", "1", "01]"),
    ];
    for (grammar, text, token, near) in rows {
        let expected =
            format!("Invalid expression: Syntax error; token: \"{token}\", near: \"{near}\"");
        assert_eq!(refusal(grammar, text), Some(expected), "{grammar:?} {text}");
    }
}
