// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL operand/condition bridge.
//!
//! Translates a PartiQL [`WhereExpr`] (or an [`Operand`] or [`Path`]) into the
//! engine's existing classic expression forms: a `KeyConditionExpression`,
//! `FilterExpression`, `ConditionExpression`, or `UpdateExpression` fragment
//! plus the `ExpressionAttributeNames` / `ExpressionAttributeValues` maps those
//! expressions reference. Translating into the classic forms reuses the
//! measured classic semantics rather than re-evaluating PartiQL expressions.
//!
//! The SELECT executor (this phase) uses it for key conditions and filters; the
//! write executor reuses the same accumulator for conditions and update
//! expressions. Every attribute reference is emitted as a `#name` placeholder,
//! so a reserved word used as an attribute name never needs special handling.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{
    ArithOp, CompareOp, Operand, Path, PathStep, TypeName, WhereExpr,
};
use extenddb_core::types::AttributeValue;

use super::bind_parameter;

/// Accumulates expression-attribute maps and renders classic expression
/// fragments for a PartiQL statement. One bridge is shared across the key
/// condition and filter of a single request so the two fragments draw from one
/// set of `#name` / `:value` placeholders.
pub(crate) struct ExprBridge<'p> {
    params: &'p [AttributeValue],
    names: BTreeMap<String, String>,
    values: BTreeMap<String, AttributeValue>,
    name_seq: usize,
    value_seq: usize,
}

impl<'p> ExprBridge<'p> {
    /// Create a bridge bound to the request's positional `Parameters`.
    pub(crate) fn new(params: &'p [AttributeValue]) -> Self {
        Self {
            params,
            names: BTreeMap::new(),
            values: BTreeMap::new(),
            name_seq: 0,
            value_seq: 0,
        }
    }

    /// Mint a fresh `#name` placeholder for an attribute name.
    pub(crate) fn name_ref(&mut self, attr: &str) -> String {
        let placeholder = format!("#n{}", self.name_seq);
        self.name_seq += 1;
        self.names.insert(placeholder.clone(), attr.to_owned());
        placeholder
    }

    /// Mint a fresh `:value` placeholder for a literal attribute value.
    pub(crate) fn value_ref(&mut self, value: AttributeValue) -> String {
        let placeholder = format!(":v{}", self.value_seq);
        self.value_seq += 1;
        self.values.insert(placeholder.clone(), value);
        placeholder
    }

    /// Render a document path as `#n0.#n1[2]...`, one name placeholder per
    /// attribute step and a literal `[index]` per list-index step.
    pub(crate) fn path(&mut self, path: &Path) -> String {
        let mut rendered = self.name_ref(&path.root);
        for step in &path.steps {
            match step {
                PathStep::Attribute(name) => {
                    rendered.push('.');
                    rendered.push_str(&self.name_ref(name));
                }
                PathStep::Index(i) => {
                    rendered.push_str(&format!("[{i}]"));
                }
            }
        }
        rendered
    }

    /// Render an operand (value, parameter, path, arithmetic, or function).
    ///
    /// # Errors
    ///
    /// Returns the service's count-mismatch `ValidationException` when a `?`
    /// parameter has no corresponding entry in `Parameters`.
    pub(crate) fn operand(&mut self, op: &Operand) -> Result<String, DynamoDbError> {
        Ok(match op {
            Operand::Literal(av) => self.value_ref(av.clone()),
            Operand::Parameter(i) => {
                let av = bind_parameter(self.params, *i)?;
                self.value_ref(av)
            }
            Operand::Path(p) => self.path(p),
            Operand::Arith(l, arith, r) => {
                let l = self.operand(l)?;
                let r = self.operand(r)?;
                let sym = match arith {
                    ArithOp::Add => "+",
                    ArithOp::Sub => "-",
                };
                format!("{l} {sym} {r}")
            }
            Operand::Function(name, args) => {
                let rendered: Vec<String> = args
                    .iter()
                    .map(|a| self.operand(a))
                    .collect::<Result<_, _>>()?;
                format!("{name}({})", rendered.join(", "))
            }
        })
    }

    /// Render a boolean `WHERE` expression as a classic filter / condition
    /// expression string, written by [`condition_text`]. An OR at the top is
    /// in parentheses, so the text can be joined to others with AND.
    ///
    /// # Errors
    ///
    /// Propagates the parameter count-mismatch `ValidationException` from
    /// [`ExprBridge::operand`].
    pub(crate) fn render_condition(&mut self, expr: &WhereExpr) -> Result<String, DynamoDbError> {
        condition_text(expr, true, |predicate, negated| {
            self.predicate(predicate, negated)
        })
    }

    /// Render one predicate of a condition, negated when `negated` is set.
    fn predicate(&mut self, expr: &WhereExpr, negated: bool) -> Result<String, DynamoDbError> {
        let text = match expr {
            WhereExpr::Compare(l, op, r) => {
                let l = self.operand(l)?;
                let r = self.operand(r)?;
                format!("{l} {} {r}", compare_symbol(*op))
            }
            WhereExpr::Between(v, low, high) => {
                let v = self.operand(v)?;
                let low = self.operand(low)?;
                let high = self.operand(high)?;
                format!("{v} BETWEEN {low} AND {high}")
            }
            WhereExpr::In(v, items) => {
                let v = self.operand(v)?;
                let rendered: Vec<String> = items
                    .iter()
                    .map(|i| self.operand(i))
                    .collect::<Result<_, _>>()?;
                format!("{v} IN ({})", rendered.join(", "))
            }
            WhereExpr::BeginsWith(a, b) => {
                let a = self.operand(a)?;
                let b = self.operand(b)?;
                format!("begins_with({a}, {b})")
            }
            WhereExpr::Contains(a, b) => {
                let a = self.operand(a)?;
                let b = self.operand(b)?;
                format!("contains({a}, {b})")
            }
            WhereExpr::AttributeType(a, b) => {
                let a = self.operand(a)?;
                let b = self.operand(b)?;
                format!("attribute_type({a}, {b})")
            }
            WhereExpr::Function(name, args) => {
                let rendered: Vec<String> = args
                    .iter()
                    .map(|a| self.operand(a))
                    .collect::<Result<_, _>>()?;
                format!("{name}({})", rendered.join(", "))
            }
            // `IS MISSING` means the attribute is absent, `IS NOT MISSING` that
            // it is present; the classic equivalents are attribute_not_exists /
            // attribute_exists, and each is the negation of the other.
            WhereExpr::IsMissing(path, is_missing) => {
                let p = self.path(path);
                return Ok(if *is_missing == negated {
                    format!("attribute_exists({p})")
                } else {
                    format!("attribute_not_exists({p})")
                });
            }
            // `IS NULL` is an equality with NULL and `IS <type>` an
            // attribute_type test, which is how the service filters on them
            // (measured 2026-11-02: `IS NULL`, `IS NOT NULL`, `IS BOOL` and
            // `IS STRING` filter rows).
            WhereExpr::IsType(operand, ty, is_not) => {
                let value = self.operand(operand)?;
                let test = if *ty == TypeName::Null {
                    let null = self.value_ref(AttributeValue::Null);
                    format!("{value} = {null}")
                } else {
                    let code = ty.attribute_type().ok_or_else(|| {
                        DynamoDbError::ValidationException(ty.unsupported_filter_message())
                    })?;
                    let code = self.value_ref(AttributeValue::S(code.to_owned()));
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

    /// The accumulated `ExpressionAttributeNames` as a JSON object, or `None`
    /// when no attribute placeholders were emitted.
    pub(crate) fn names_value(&self) -> Option<Value> {
        if self.names.is_empty() {
            return None;
        }
        let map: serde_json::Map<String, Value> = self
            .names
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        Some(Value::Object(map))
    }

    /// The accumulated name placeholders, for evaluating a rendered expression
    /// in this module rather than sending it in a request.
    pub(crate) fn names_map(&self) -> HashMap<String, String> {
        self.names
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// The accumulated value placeholders, for the same use as [`Self::names_map`].
    pub(crate) fn values_map(&self) -> HashMap<String, AttributeValue> {
        self.values
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// The accumulated `ExpressionAttributeValues` as a JSON object of wire-form
    /// attribute values, or `None` when no value placeholders were emitted.
    ///
    /// # Errors
    ///
    /// Returns `InternalServerError` if an accumulated value fails to serialize
    /// (not reachable for well-formed attribute values).
    pub(crate) fn values_value(&self) -> Result<Option<Value>, DynamoDbError> {
        if self.values.is_empty() {
            return Ok(None);
        }
        let mut map = serde_json::Map::new();
        for (k, v) in &self.values {
            let wire = serde_json::to_value(v).map_err(|e| {
                DynamoDbError::InternalServerError(format!("failed to encode parameter: {e}"))
            })?;
            map.insert(k.clone(), wire);
        }
        Ok(Some(Value::Object(map)))
    }
}

/// Write a WHERE condition as classic condition text, with `predicate`
/// writing each predicate (anything but AND, OR and NOT), negated when its
/// second argument is set.
///
/// A condition the service runs can be nested deeper than the classic parser
/// takes (150 levels of parentheses and NOTs, the `max_expression_depth`
/// limit): the service runs any condition within its 300 operators, such as
/// 99 levels of `NOT (a = 1 AND ...)` (measured 2026-11-02). So the text is
/// written with as little nesting as the classic grammar allows: a NOT is
/// moved onto the predicates under it (NOT of an AND is the OR of the
/// negated sides, and NOT of an OR their AND), and parentheses go only around
/// an OR inside an AND, and around an OR at the top when
/// `parenthesize_top_or` is set. A condition of 300 operators then nests at
/// most about 75 levels. The meaning is unchanged: a classic condition is
/// true or false, and the predicates are written, and so evaluated, in the
/// order of the statement.
///
/// The walk keeps its own stack rather than recursing, so a deep condition
/// takes no more of the thread's stack than a shallow one (a recursive walk
/// took about 9 KB per level on a debug build).
///
/// # Errors
///
/// Returns the first error `predicate` returns.
pub(crate) fn condition_text<E>(
    expr: &WhereExpr,
    parenthesize_top_or: bool,
    mut predicate: impl FnMut(&WhereExpr, bool) -> Result<String, E>,
) -> Result<String, E> {
    enum Step<'e> {
        /// A condition to write, whether it is negated, and whether an OR
        /// there needs parentheses.
        Write(&'e WhereExpr, bool, bool),
        Text(&'static str),
    }
    let mut text = String::new();
    let mut steps = vec![Step::Write(expr, false, parenthesize_top_or)];
    while let Some(step) = steps.pop() {
        let (expr, negated, in_and) = match step {
            Step::Text(part) => {
                text.push_str(part);
                continue;
            }
            Step::Write(expr, negated, in_and) => (expr, negated, in_and),
        };
        let (odd, expr) = expr.without_nots();
        let negated = negated != odd;
        let (left, right, and) = match expr {
            WhereExpr::And(left, right) => (left, right, !negated),
            WhereExpr::Or(left, right) => (left, right, negated),
            other => {
                text.push_str(&predicate(other, negated)?);
                continue;
            }
        };
        let parenthesize = in_and && !and;
        if parenthesize {
            steps.push(Step::Text(")"));
        }
        steps.push(Step::Write(right, negated, and));
        steps.push(Step::Text(if and { " AND " } else { " OR " }));
        steps.push(Step::Write(left, negated, and));
        if parenthesize {
            steps.push(Step::Text("("));
        }
    }
    Ok(text)
}

/// The classic comparison symbol for a PartiQL compare operator.
pub(super) fn compare_symbol(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "<>",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
    }
}

#[cfg(test)]
mod tests;
