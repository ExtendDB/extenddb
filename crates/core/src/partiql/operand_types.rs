// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The operand-type check on ordering comparisons.
//!
//! An ordering operator (`<`, `<=`, `>`, `>=`, `BETWEEN`) refuses a value
//! operand whose type has no ordering: anything but `S`, `N` and `B`. The
//! service applies the rule to the statement itself, so it answers before the
//! table is resolved, and the message is the bare sentence with no expression
//! prefix (measured 2026-11-02). A document path is not checked: its type is
//! only known per item.

use crate::types::AttributeValue;

use super::ast::{CompareOp, Operand, Statement, WhereExpr};

/// Check every ordering comparison in the statement's `WHERE` clause. A `?`
/// operand is checked against its bound parameter; a missing parameter is left
/// to the binding step, which reports the count mismatch.
///
/// # Errors
///
/// Returns the service's operand-type message for the first refused operand,
/// left operand before right.
pub fn check_ordering_operands(stmt: &Statement, params: &[AttributeValue]) -> Result<(), String> {
    let where_clause = match stmt {
        Statement::Select(select) | Statement::Exists(select) => select.where_clause.as_ref(),
        Statement::Update(update) => update.where_clause.as_ref(),
        Statement::Delete(delete) => Some(&delete.where_clause),
        Statement::Insert(_) => None,
    };
    match where_clause {
        Some(expr) => check_where(expr, params),
        None => Ok(()),
    }
}

fn check_where(expr: &WhereExpr, params: &[AttributeValue]) -> Result<(), String> {
    match expr {
        WhereExpr::Compare(left, op, right) => {
            if let Some(symbol) = ordering_symbol(*op) {
                check_operand(left, symbol, params)?;
                check_operand(right, symbol, params)?;
            }
            Ok(())
        }
        WhereExpr::Between(value, low, high) => {
            check_operand(value, "BETWEEN", params)?;
            check_operand(low, "BETWEEN", params)?;
            check_operand(high, "BETWEEN", params)
        }
        WhereExpr::And(left, right) | WhereExpr::Or(left, right) => {
            check_where(left, params)?;
            check_where(right, params)
        }
        WhereExpr::Not(inner) | WhereExpr::ComparedWithBoolean(inner, _) => {
            check_where(inner, params)
        }
        WhereExpr::In(..)
        | WhereExpr::BeginsWith(..)
        | WhereExpr::Contains(..)
        | WhereExpr::AttributeType(..)
        | WhereExpr::Function(..)
        | WhereExpr::IsMissing(..)
        | WhereExpr::IsType(..) => Ok(()),
    }
}

/// The operator as the message names it, or `None` for `=` and `<>`, which
/// accept every type.
fn ordering_symbol(op: CompareOp) -> Option<&'static str> {
    match op {
        CompareOp::Lt => Some("<"),
        CompareOp::Le => Some("<="),
        CompareOp::Gt => Some(">"),
        CompareOp::Ge => Some(">="),
        CompareOp::Eq | CompareOp::Ne => None,
    }
}

fn check_operand(operand: &Operand, symbol: &str, params: &[AttributeValue]) -> Result<(), String> {
    let value = match operand {
        Operand::Literal(value) => value,
        Operand::Parameter(index) => match params.get(*index) {
            Some(value) => value,
            None => return Ok(()),
        },
        Operand::Path(_) | Operand::Arith(..) | Operand::Function(..) => return Ok(()),
    };
    let type_code = match value {
        AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_) => return Ok(()),
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
    };
    Err(format!(
        "Incorrect operand type for operator or function; operator or function: {symbol}, \
         operand type: {type_code}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partiql::parse;

    fn check(sql: &str, params: &[AttributeValue]) -> Result<(), String> {
        check_ordering_operands(&parse(sql).unwrap(), params)
    }

    #[test]
    fn each_ordering_operator_is_named_as_written() {
        for op in ["<", "<=", ">", ">="] {
            let sql = format!("SELECT pk FROM \"t\" WHERE pk = ? AND val {op} ?");
            let params = [
                AttributeValue::S("k".to_owned()),
                AttributeValue::Bool(true),
            ];
            assert_eq!(
                check(&sql, &params),
                Err(format!(
                    "Incorrect operand type for operator or function; operator or function: \
                     {op}, operand type: BOOL"
                ))
            );
        }
    }

    #[test]
    fn between_names_itself() {
        let params = [AttributeValue::Bool(true), AttributeValue::Bool(false)];
        let err = check("SELECT * FROM \"t\" WHERE val BETWEEN ? AND ?", &params).unwrap_err();
        assert!(
            err.ends_with("operator or function: BETWEEN, operand type: BOOL"),
            "{err}"
        );
    }

    #[test]
    fn every_unordered_type_is_refused_and_ordered_types_pass() {
        let unordered = [
            (AttributeValue::Null, "NULL"),
            (AttributeValue::L(Vec::new()), "L"),
            (AttributeValue::M(std::collections::BTreeMap::new()), "M"),
            (AttributeValue::SS(["a".to_owned()].into()), "SS"),
            (AttributeValue::NS(["1".to_owned()].into()), "NS"),
            (AttributeValue::BS([vec![1u8]].into()), "BS"),
        ];
        for (value, code) in unordered {
            let err = check("SELECT * FROM \"t\" WHERE val < ?", &[value]).unwrap_err();
            assert!(err.ends_with(&format!("operand type: {code}")), "{err}");
        }
        for value in [
            AttributeValue::S("a".to_owned()),
            AttributeValue::N("1".to_owned()),
            AttributeValue::B(vec![1]),
        ] {
            assert_eq!(check("SELECT * FROM \"t\" WHERE val < ?", &[value]), Ok(()));
        }
    }

    #[test]
    fn equality_paths_and_missing_parameters_are_left_alone() {
        let flag = [AttributeValue::Bool(true)];
        assert_eq!(check("SELECT * FROM \"t\" WHERE val = ?", &flag), Ok(()));
        assert_eq!(check("SELECT * FROM \"t\" WHERE val <> ?", &flag), Ok(()));
        assert_eq!(check("SELECT * FROM \"t\" WHERE a < b", &[]), Ok(()));
        assert_eq!(check("SELECT * FROM \"t\" WHERE a < ?", &[]), Ok(()));
    }

    #[test]
    fn literals_writes_and_nested_conditions_are_checked() {
        assert!(check("SELECT * FROM \"t\" WHERE pk = 'a' AND val > true", &[]).is_err());
        let flag = [AttributeValue::Bool(true)];
        assert!(check("UPDATE \"t\" SET a = 1 WHERE pk = 'a' AND val < ?", &flag).is_err());
        assert!(check("DELETE FROM \"t\" WHERE pk = 'a' AND NOT (val >= ?)", &flag).is_err());
        assert!(check("SELECT * FROM \"t\" WHERE pk = 'a' OR val <= ?", &flag).is_err());
    }

    #[test]
    fn the_left_operand_is_reported_first() {
        let params = [AttributeValue::Null, AttributeValue::Bool(true)];
        let err = check("SELECT * FROM \"t\" WHERE ? < ?", &params).unwrap_err();
        assert!(err.ends_with("operand type: NULL"), "{err}");
    }
}
