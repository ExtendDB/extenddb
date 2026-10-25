// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The operator cap Amazon `DynamoDB` applies to condition, filter and key
//! condition expressions.
//!
//! Measured 2026-10-25 (us-east-1) with `PutItem`, `Scan` and `Query`: every
//! comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`), `AND`, `OR`, `NOT`, `BETWEEN`
//! (its inner `AND` not counted), `IN` (list elements not counted) and every
//! function call (`size` included) counts one; parentheses, paths and
//! placeholders count zero. An expression whose count exceeds 300 is refused
//! with `Invalid <Kind>: The expression contains too many operators; operator
//! count: <count>`; a count of exactly 300 is accepted.

use super::ast::Expr;
use super::kind::ExpressionKind;
use crate::error::DynamoDbError;

/// The largest operator count the service accepts.
pub const MAX_EXPRESSION_OPERATORS: usize = 300;

/// Count the operators in a parsed condition as the service counts them.
#[must_use]
pub fn count_operators(expr: &Expr) -> usize {
    match expr {
        Expr::Path(_) | Expr::Placeholder(_) => 0,
        Expr::Not(inner) => 1 + count_operators(inner),
        Expr::And(left, right) | Expr::Or(left, right) => {
            1 + count_operators(left) + count_operators(right)
        }
        Expr::Compare { left, right, .. } | Expr::Arithmetic { left, right, .. } => {
            1 + count_operators(left) + count_operators(right)
        }
        Expr::Between { operand, low, high } => {
            1 + count_operators(operand) + count_operators(low) + count_operators(high)
        }
        Expr::In { operand, list } => {
            1 + count_operators(operand) + list.iter().map(count_operators).sum::<usize>()
        }
        Expr::Function { args, .. } => 1 + args.iter().map(count_operators).sum::<usize>(),
    }
}

/// Refuse an expression with more than [`MAX_EXPRESSION_OPERATORS`] operators.
///
/// # Errors
///
/// Returns `ValidationException` carrying the `Invalid <kind>:` prefix and the
/// full operator count.
pub fn validate_operator_count(expr: &Expr, kind: ExpressionKind) -> Result<(), DynamoDbError> {
    let count = count_operators(expr);
    if count > MAX_EXPRESSION_OPERATORS {
        return Err(DynamoDbError::ValidationException(format!(
            "Invalid {kind}: The expression contains too many operators; operator count: {count}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::parser::parse_condition;
    use crate::expression::tokenizer::tokenize;

    fn count(src: &str) -> usize {
        count_operators(&parse_condition(&tokenize(src).unwrap()).unwrap())
    }

    fn chain(term: &str, n: usize, joiner: &str) -> String {
        vec![term; n].join(joiner)
    }

    // Expected counts are the service's answers of 2026-10-25 (probe cases in
    // brackets).

    #[test]
    fn comparisons_and_joiners_count_one_each() {
        assert_eq!(count(&chain("#v = :v", 300, " OR ")), 599); // [a02]
        assert_eq!(count(&chain("#v <> :v", 300, " OR ")), 599); // [a05]
        assert_eq!(count(&chain("#v >= :v", 300, " AND ")), 599); // [a09, a03]
        assert_eq!(count(&format!("attribute_not_exists(pk) OR {}", chain("#v = :v", 300, " OR "))), 601); // [a01]
    }

    #[test]
    fn not_functions_between_in_and_size_count_one_each() {
        assert_eq!(count(&chain("NOT #v = :v", 200, " OR ")), 599); // [a10]
        assert_eq!(count(&chain("NOT NOT #v = :v", 200, " OR ")), 799); // [a22]
        assert_eq!(count(&chain("#v BETWEEN :v AND :v", 160, " OR ")), 319); // [a40]
        assert_eq!(count(&chain("#v IN (:v, :v, :v)", 170, " OR ")), 339); // [a41]
        assert_eq!(count(&chain("#v IN (:v)", 200, " OR ")), 399); // [a12]
        assert_eq!(count(&chain("attribute_exists(#v)", 170, " OR ")), 339); // [a43]
        assert_eq!(count(&chain("contains(#v, :v)", 200, " OR ")), 399); // [a45]
        assert_eq!(count(&chain("size(#v) = :v", 230, " OR ")), 689); // [a47]
        assert_eq!(count(&chain("size(#v) BETWEEN :v AND :v", 130, " OR ")), 389); // [a53]
    }

    #[test]
    fn parentheses_count_zero() {
        assert_eq!(count(&chain("(#v = :v)", 300, " OR ")), 599); // [a19]
        assert_eq!(count(&chain("((#v = :v) OR (#v = :v))", 140, " OR ")), 559); // [a51]
        assert_eq!(count(&format!("attribute_exists(pk) AND ({})", chain("#v = :v", 300, " OR "))), 601); // [a04]
        assert_eq!(count(&format!("attribute_exists(pk) AND NOT ({})", chain("#v = :v", 150, " OR "))), 302); // [a52]
    }

    #[test]
    fn cap_is_exclusive_at_300() {
        let kind = ExpressionKind::Condition;
        let at_cap = format!("NOT attribute_not_exists(pk) OR {}", chain("#v = :v", 149, " OR ")); // [a49]
        assert_eq!(count(&at_cap), 300);
        assert!(validate_operator_count(&parse_condition(&tokenize(&at_cap).unwrap()).unwrap(), kind).is_ok());

        let over = chain("#v = :v", 151, " OR "); // [a30]
        let err = validate_operator_count(&parse_condition(&tokenize(&over).unwrap()).unwrap(), kind).unwrap_err();
        assert!(matches!(err, DynamoDbError::ValidationException(m)
            if m == "Invalid ConditionExpression: The expression contains too many operators; operator count: 301"));

        let under = chain("#v = :v", 150, " OR "); // [a30]
        assert!(validate_operator_count(&parse_condition(&tokenize(&under).unwrap()).unwrap(), kind).is_ok());
    }

    #[test]
    fn message_carries_the_kind() {
        let expr = parse_condition(&tokenize(&chain("#v = :v", 300, " OR ")).unwrap()).unwrap();
        let err = validate_operator_count(&expr, ExpressionKind::Filter).unwrap_err();
        assert!(matches!(err, DynamoDbError::ValidationException(m)
            if m == "Invalid FilterExpression: The expression contains too many operators; operator count: 599")); // [b01]
    }
}
