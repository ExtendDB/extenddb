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

use std::collections::BTreeMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{ArithOp, CompareOp, Operand, Path, PathStep, WhereExpr};
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
    /// expression string.
    ///
    /// # Errors
    ///
    /// Propagates the parameter count-mismatch `ValidationException` from
    /// [`ExprBridge::operand`].
    pub(crate) fn render_condition(&mut self, expr: &WhereExpr) -> Result<String, DynamoDbError> {
        Ok(match expr {
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
            // attribute_exists.
            WhereExpr::IsMissing(path, is_missing) => {
                let p = self.path(path);
                if *is_missing {
                    format!("attribute_not_exists({p})")
                } else {
                    format!("attribute_exists({p})")
                }
            }
            WhereExpr::And(a, b) => {
                let a = self.render_condition(a)?;
                let b = self.render_condition(b)?;
                format!("({a} AND {b})")
            }
            WhereExpr::Or(a, b) => {
                let a = self.render_condition(a)?;
                let b = self.render_condition(b)?;
                format!("({a} OR {b})")
            }
            WhereExpr::Not(a) => {
                let a = self.render_condition(a)?;
                format!("(NOT {a})")
            }
        })
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

/// The classic comparison symbol for a PartiQL compare operator.
fn compare_symbol(op: CompareOp) -> &'static str {
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
mod tests {
    use super::*;
    use extenddb_core::partiql::ast::Statement;
    use extenddb_core::partiql::parse;

    fn where_of(sql: &str) -> WhereExpr {
        match parse(sql).unwrap() {
            Statement::Select(s) => s.where_clause.unwrap(),
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn renders_equality_with_name_and_value_placeholders() {
        let w = where_of("SELECT * FROM \"t\" WHERE pk = 'user1'");
        let mut b = ExprBridge::new(&[]);
        let s = b.render_condition(&w).unwrap();
        assert_eq!(s, "#n0 = :v0");
        assert_eq!(b.names_value().unwrap()["#n0"], "pk");
        assert_eq!(b.values_value().unwrap().unwrap()[":v0"]["S"], "user1");
    }

    #[test]
    fn renders_nested_path_on_leaf_name_placeholders() {
        let w = where_of("SELECT * FROM \"t\" WHERE addr.city = 'NYC'");
        let mut b = ExprBridge::new(&[]);
        let s = b.render_condition(&w).unwrap();
        assert_eq!(s, "#n0.#n1 = :v0");
    }

    #[test]
    fn renders_begins_with_and_between_and_in() {
        let mut b = ExprBridge::new(&[]);
        let w = where_of("SELECT * FROM \"t\" WHERE begins_with(\"sk\", 'p')");
        assert_eq!(b.render_condition(&w).unwrap(), "begins_with(#n0, :v0)");

        let mut b = ExprBridge::new(&[]);
        let w = where_of("SELECT * FROM \"t\" WHERE sk BETWEEN 'a' AND 'z'");
        assert_eq!(b.render_condition(&w).unwrap(), "#n0 BETWEEN :v0 AND :v1");

        let mut b = ExprBridge::new(&[]);
        let w = where_of("SELECT * FROM \"t\" WHERE sk IN ['a','b']");
        assert_eq!(b.render_condition(&w).unwrap(), "#n0 IN (:v0, :v1)");
    }

    #[test]
    fn renders_is_missing_as_attribute_not_exists() {
        let mut b = ExprBridge::new(&[]);
        let w = where_of("SELECT * FROM \"t\" WHERE addr IS MISSING");
        assert_eq!(b.render_condition(&w).unwrap(), "attribute_not_exists(#n0)");

        let mut b = ExprBridge::new(&[]);
        let w = where_of("SELECT * FROM \"t\" WHERE addr IS NOT MISSING");
        assert_eq!(b.render_condition(&w).unwrap(), "attribute_exists(#n0)");
    }

    #[test]
    fn binds_positional_parameter() {
        let params = [AttributeValue::S("p1".to_owned())];
        let mut b = ExprBridge::new(&params);
        let w = where_of("SELECT * FROM \"t\" WHERE pk = ?");
        assert_eq!(b.render_condition(&w).unwrap(), "#n0 = :v0");
        assert_eq!(b.values_value().unwrap().unwrap()[":v0"]["S"], "p1");
    }

    #[test]
    fn too_few_parameters_is_count_mismatch() {
        let mut b = ExprBridge::new(&[]);
        let w = where_of("SELECT * FROM \"t\" WHERE pk = ?");
        let err = b.render_condition(&w).unwrap_err();
        match err {
            DynamoDbError::ValidationException(m) => {
                assert_eq!(
                    m,
                    "Number of parameters in request and statement don't match."
                );
            }
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }
}
