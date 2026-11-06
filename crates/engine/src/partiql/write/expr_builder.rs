// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The builder of the classic expressions and placeholder maps a write is translated into.

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{ArithOp, Operand, Path, PathStep, TypeName, WhereExpr};
use extenddb_core::types::AttributeValue;

use super::{av_type_label, to_json_value};
use crate::partiql::bind_parameter;
use crate::partiql::expr_bridge::compare_symbol;

// --- expression builder ---------------------------------------------------

/// Allocates `#name` / `:value` placeholders and the maps the classic handlers
/// expect, shared across a statement's update and condition expressions.
#[derive(Default)]
pub(super) struct ExprBuilder {
    pub(super) names: Map<String, Value>,
    pub(super) values: Map<String, Value>,
    next_name: usize,
    next_value: usize,
}

impl ExprBuilder {
    /// Allocate a name placeholder for a real attribute name.
    pub(super) fn name(&mut self, real: &str) -> String {
        let placeholder = format!("#n{}", self.next_name);
        self.next_name += 1;
        self.names
            .insert(placeholder.clone(), Value::String(real.to_owned()));
        placeholder
    }

    /// Allocate a value placeholder for a typed AttributeValue.
    pub(super) fn value(&mut self, av: &AttributeValue) -> Result<String, DynamoDbError> {
        let placeholder = format!(":v{}", self.next_value);
        self.next_value += 1;
        self.values.insert(placeholder.clone(), to_json_value(av)?);
        Ok(placeholder)
    }

    /// Translate a document path to placeholder form (`#n0.#n1[2]`).
    pub(super) fn path(&mut self, path: &Path) -> String {
        let mut rendered = self.name(&path.root);
        for step in &path.steps {
            match step {
                PathStep::Attribute(attr) => {
                    rendered.push('.');
                    rendered.push_str(&self.name(attr));
                }
                PathStep::Index(index) => {
                    rendered.push_str(&format!("[{index}]"));
                }
            }
        }
        rendered
    }

    /// Translate an operand to expression text.
    pub(super) fn operand(
        &mut self,
        operand: &Operand,
        params: &[AttributeValue],
    ) -> Result<String, DynamoDbError> {
        Ok(match operand {
            Operand::Literal(av) => self.value(av)?,
            Operand::Parameter(index) => {
                let av = bind_parameter(params, *index)?;
                self.value(&av)?
            }
            Operand::Path(path) => self.path(path),
            Operand::Arith(left, op, right) => {
                let symbol = match op {
                    ArithOp::Add => "+",
                    ArithOp::Sub => "-",
                };
                // The service type-checks an inline literal operand of an
                // arithmetic expression up front: a non-number literal is an
                // operand-type error naming the operator and the type, distinct
                // from the generic runtime message a string ATTRIBUTE operand
                // produces in the classic evaluator.
                for side in [left.as_ref(), right.as_ref()] {
                    if let Operand::Literal(av) = side
                        && !matches!(av, AttributeValue::N(_))
                    {
                        return Err(DynamoDbError::ValidationException(format!(
                            "Incorrect operand type for operator or function; operator or function: {symbol}, operand type: {}",
                            av_type_label(av)
                        )));
                    }
                }
                let left = self.grouped(left, params)?;
                let right = self.grouped(right, params)?;
                format!("{left} {symbol} {right}")
            }
            Operand::Function(name, args) => {
                let mut rendered = Vec::new();
                for arg in args {
                    rendered.push(self.grouped(arg, params)?);
                }
                format!("{}({})", name, rendered.join(", "))
            }
        })
    }

    /// An operand of an arithmetic operator or an argument of a call, in
    /// parentheses when it is an arithmetic expression itself: a SET value
    /// of the classic grammar takes one operator, and parentheses group. The
    /// service runs a statement with arithmetic in an operand or an argument
    /// (measured 2026-11-06: `SET a = (a + 1) + 1`, `1 + (a + 1)`, `a - (1 -
    /// 1)`, `(a + 1) - (a - 1)` and `if_not_exists(q, (a + 1) + 1)` set the
    /// sums).
    fn grouped(
        &mut self,
        operand: &Operand,
        params: &[AttributeValue],
    ) -> Result<String, DynamoDbError> {
        let text = self.operand(operand, params)?;
        Ok(if matches!(operand, Operand::Arith(..)) {
            format!("({text})")
        } else {
            text
        })
    }

    /// Translate a boolean WHERE/condition expression to expression text,
    /// written by `expr_bridge::condition_text`. An OR at the top is not in
    /// parentheses: the callers put each term in parentheses themselves.
    pub(super) fn condition(
        &mut self,
        expr: &WhereExpr,
        params: &[AttributeValue],
    ) -> Result<String, DynamoDbError> {
        crate::partiql::expr_bridge::condition_text(expr, false, |predicate, negated| {
            self.predicate(predicate, negated, params)
        })
    }

    /// Translate one predicate of a condition, negated when `negated` is set.
    fn predicate(
        &mut self,
        expr: &WhereExpr,
        negated: bool,
        params: &[AttributeValue],
    ) -> Result<String, DynamoDbError> {
        let text = match expr {
            WhereExpr::Compare(left, op, right) => {
                let left = self.operand(left, params)?;
                let right = self.operand(right, params)?;
                format!("{left} {} {right}", compare_symbol(*op))
            }
            WhereExpr::Between(value, low, high) => {
                let value = self.operand(value, params)?;
                let low = self.operand(low, params)?;
                let high = self.operand(high, params)?;
                format!("{value} BETWEEN {low} AND {high}")
            }
            WhereExpr::In(value, items) => {
                let value = self.operand(value, params)?;
                let mut rendered = Vec::new();
                for item in items {
                    rendered.push(self.operand(item, params)?);
                }
                format!("{value} IN ({})", rendered.join(", "))
            }
            WhereExpr::BeginsWith(value, prefix) => {
                let value = self.operand(value, params)?;
                let prefix = self.operand(prefix, params)?;
                format!("begins_with({value}, {prefix})")
            }
            WhereExpr::Contains(value, operand) => {
                let value = self.operand(value, params)?;
                let operand = self.operand(operand, params)?;
                format!("contains({value}, {operand})")
            }
            WhereExpr::AttributeType(value, kind) => {
                let value = self.operand(value, params)?;
                let kind = self.operand(kind, params)?;
                format!("attribute_type({value}, {kind})")
            }
            WhereExpr::Function(name, args) => {
                let mut rendered = Vec::new();
                for arg in args {
                    rendered.push(self.operand(arg, params)?);
                }
                format!("{}({})", name, rendered.join(", "))
            }
            WhereExpr::IsMissing(path, missing) => {
                let rendered = self.path(path);
                return Ok(if *missing == negated {
                    format!("attribute_exists({rendered})")
                } else {
                    format!("attribute_not_exists({rendered})")
                });
            }
            // As on a read: `IS NULL` is an equality with NULL, `IS <type>`
            // an attribute_type test.
            WhereExpr::IsType(operand, ty, is_not) => {
                let value = self.operand(operand, params)?;
                let test = if *ty == TypeName::Null {
                    let null = self.value(&AttributeValue::Null)?;
                    format!("{value} = {null}")
                } else {
                    let code = ty.attribute_type().ok_or_else(|| {
                        DynamoDbError::ValidationException(ty.unsupported_filter_message())
                    })?;
                    let code = self.value(&AttributeValue::S(code.to_owned()))?;
                    format!("attribute_type({value}, {code})")
                };
                return Ok(if *is_not == negated {
                    test
                } else {
                    format!("NOT {test}")
                });
            }
            WhereExpr::And(..)
            | WhereExpr::Or(..)
            | WhereExpr::Not(_)
            | WhereExpr::ComparedWithBoolean(..) => {
                unreachable!("condition_text writes AND, OR and NOT itself")
            }
        };
        Ok(if negated { format!("NOT {text}") } else { text })
    }
}
