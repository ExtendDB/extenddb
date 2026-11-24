// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The placeholder and operand type checks Amazon `DynamoDB` runs on a parsed
//! condition or filter before it counts the operators.

use super::ast::{Expr, PathElement};
use super::resolver::{
    ExpressionMaps, attribute_type_code, ordering_op_symbol, reject_non_orderable_literal,
};
use crate::error::DynamoDbError;
use crate::types::AttributeValue;

/// Check a condition or filter the way Amazon `DynamoDB` checks it before it
/// counts operators: every `#name` and `:value` must be defined, an ordering
/// comparison or `BETWEEN` must not have a literal of a type that does not
/// order, and `begins_with` must have a string or binary prefix.
///
/// The tree is walked as written, left to right, and the operands of a node
/// are checked before the node, so the first fault in that order is returned.
/// Measured 2026-11-19 (us-east-1) on Scan and PutItem: `#v < :b OR #v = :zz`,
/// with `:b` a BOOL and `:zz` undefined, answers the operand type, `#v = :zz
/// OR #v < :b` answers the undefined value, and `:b < #x` answers the
/// undefined name. The check reads no item, so it also finds a placeholder in
/// a branch that evaluation skips.
///
/// The messages carry no expression-kind prefix; the caller adds it.
///
/// # Errors
///
/// Returns `ValidationException` for the first undefined name or value, or
/// the first operand of a type the operator or function refuses.
pub fn validate_placeholders_and_operands(
    expr: &Expr,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    match expr {
        Expr::Path(elements) => {
            for element in elements {
                if let PathElement::Attribute(name) = element
                    && let Some(name_ref) = name.strip_prefix('#')
                {
                    maps.resolve_name(name_ref)?;
                }
            }
            Ok(())
        }
        Expr::Placeholder(name) => maps.resolve_value(name).map(drop),
        Expr::Compare { left, op, right } => {
            validate_placeholders_and_operands(left, maps)?;
            validate_placeholders_and_operands(right, maps)?;
            if let Some(symbol) = ordering_op_symbol(*op) {
                reject_non_orderable_literal(left, maps, symbol)?;
                reject_non_orderable_literal(right, maps, symbol)?;
            }
            Ok(())
        }
        Expr::Between { operand, low, high } => {
            for part in [operand, low, high] {
                validate_placeholders_and_operands(part, maps)?;
            }
            for part in [operand, low, high] {
                reject_non_orderable_literal(part, maps, "BETWEEN")?;
            }
            Ok(())
        }
        Expr::Function { name, args } => {
            for arg in args {
                validate_placeholders_and_operands(arg, maps)?;
            }
            if name == "begins_with" {
                reject_begins_with_prefix(args, maps)?;
            }
            Ok(())
        }
        Expr::And(left, right) | Expr::Or(left, right) | Expr::Arithmetic { left, right, .. } => {
            validate_placeholders_and_operands(left, maps)?;
            validate_placeholders_and_operands(right, maps)
        }
        Expr::Not(inner) => validate_placeholders_and_operands(inner, maps),
        Expr::In { operand, list } => {
            validate_placeholders_and_operands(operand, maps)?;
            list.iter()
                .try_for_each(|item| validate_placeholders_and_operands(item, maps))
        }
    }
}

/// Reject a `begins_with` call whose prefix is a literal (`:value`) of a type
/// other than S or B.
pub(super) fn reject_begins_with_prefix(
    args: &[Expr],
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    if args.len() == 2
        && let Expr::Placeholder(ref placeholder) = args[1]
        && let Some(val) = maps.values.get(placeholder)
        && !matches!(val, AttributeValue::S(_) | AttributeValue::B(_))
    {
        let type_code = attribute_type_code(val);
        return Err(DynamoDbError::ValidationException(format!(
            "Incorrect operand type for operator or function; operator or function: begins_with, operand type: {type_code}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::expression::parser::parse_condition;
    use crate::expression::tokenizer::tokenize;

    const UNDEFINED_NAME: &str =
        "An expression attribute name used in the document path is not defined; attribute name: ";
    const UNDEFINED_VALUE: &str =
        "An expression attribute value used in expression is not defined; attribute value: ";
    const OPERAND: &str = "Incorrect operand type for operator or function; operator or function: ";

    /// `#v` is defined; `:v` is an S, `:b` a BOOL and `:n` an N.
    fn maps() -> ExpressionMaps {
        ExpressionMaps::new(
            HashMap::from([("v".to_owned(), "v".to_owned())]),
            HashMap::from([
                ("v".to_owned(), AttributeValue::S("x".to_owned())),
                ("b".to_owned(), AttributeValue::Bool(true)),
                ("n".to_owned(), AttributeValue::N("1".to_owned())),
            ]),
        )
    }

    fn check(src: &str) -> Result<(), String> {
        let expr = parse_condition(&tokenize(src).unwrap()).unwrap();
        validate_placeholders_and_operands(&expr, &maps()).map_err(|e| match e {
            DynamoDbError::ValidationException(m) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        })
    }

    #[test]
    fn the_first_fault_as_written_is_reported() {
        // The service's answers of 2026-11-19 (us-east-1); probe cases in the
        // first column.
        let rows = [
            (
                "s09",
                "#v < :b OR #v = :zz",
                format!("{OPERAND}<, operand type: BOOL"),
            ),
            (
                "s10",
                "#v = :zz OR #v < :b",
                format!("{UNDEFINED_VALUE}:zz"),
            ),
            ("s11", "#x = :zz", format!("{UNDEFINED_NAME}#x")),
            ("s12", ":zz = #x", format!("{UNDEFINED_VALUE}:zz")),
            (
                "s29",
                "begins_with(#v, :n) OR #v < :b",
                format!("{OPERAND}begins_with, operand type: N"),
            ),
            (
                "s30",
                "#v < :b OR begins_with(#v, :n)",
                format!("{OPERAND}<, operand type: BOOL"),
            ),
            ("w02", ":b < #x", format!("{UNDEFINED_NAME}#x")),
            (
                "w03",
                "#v BETWEEN :b AND :zz",
                format!("{UNDEFINED_VALUE}:zz"),
            ),
            ("w05", "begins_with(#x, :n)", format!("{UNDEFINED_NAME}#x")),
            ("w07", "#x BETWEEN :b AND :v", format!("{UNDEFINED_NAME}#x")),
            ("w08", "size(#x) < :b", format!("{UNDEFINED_NAME}#x")),
            ("w10", ":b < :zz", format!("{UNDEFINED_VALUE}:zz")),
            (
                "p04",
                "attribute_not_exists(pk) OR #v = :zz",
                format!("{UNDEFINED_VALUE}:zz"),
            ),
            (
                "f11",
                "size(#v) < :b",
                format!("{OPERAND}<, operand type: BOOL"),
            ),
        ];
        for (case, src, want) in rows {
            assert_eq!(check(src), Err(want), "{case}: {src}");
        }
    }

    #[test]
    fn a_name_inside_a_document_path_is_checked() {
        assert_eq!(check("#v.#w[0] = :v"), Err(format!("{UNDEFINED_NAME}#w")));
    }

    #[test]
    fn defined_placeholders_and_accepted_types_pass() {
        for src in [
            "#v = :v AND begins_with(#v, :v) AND #v BETWEEN :v AND :n",
            "#v = :b OR #v <> :b OR #v IN (:b, :n) OR contains(#v, :b)",
            "NOT attribute_exists(#v) AND size(#v) > :n",
        ] {
            assert_eq!(check(src), Ok(()), "{src}");
        }
    }
}
