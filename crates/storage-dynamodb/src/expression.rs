// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Renders ExtendDB expression ASTs back into DynamoDB expression strings,
//! building fresh `ExpressionAttributeNames` and `ExpressionAttributeValues`
//! maps suitable for forwarding to the AWS SDK DynamoDB client.

use std::collections::HashMap;

use extenddb_core::expression::{
    ArithOp, CompareOp, Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
    UpdateAction, resolve_name_ref,
};
use extenddb_storage::error::StorageError;

use crate::encoding::to_sdk;

/// Renders ExtendDB AST expressions into DynamoDB expression strings.
///
/// Allocates fresh `#n{N}` name tokens and `:v{N}` value tokens, and populates
/// the corresponding `ExpressionAttributeNames` and `ExpressionAttributeValues`
/// maps. These maps are forwarded directly to the AWS SDK DynamoDB client.
pub struct Renderer {
    names: HashMap<String, String>,
    values: HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    n_counter: usize,
    v_counter: usize,
}

impl Default for Renderer {
    fn default() -> Self {
        Self::new()
    }
}

impl Renderer {
    /// Create a new empty `Renderer`.
    pub fn new() -> Self {
        Self {
            names: HashMap::new(),
            values: HashMap::new(),
            n_counter: 0,
            v_counter: 0,
        }
    }

    /// The accumulated `ExpressionAttributeNames` map.
    pub fn names(&self) -> &HashMap<String, String> {
        &self.names
    }

    /// The accumulated `ExpressionAttributeValues` map (SDK type).
    pub fn values(&self) -> &HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
        &self.values
    }

    /// Render a condition/filter expression AST node into a DynamoDB expression string.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::Validation` if a name reference or value placeholder
    /// cannot be resolved from `maps`.
    pub fn render_condition(&mut self, e: &Expr, maps: &ExpressionMaps) -> Result<String, StorageError> {
        self.render_expr(e, maps)
    }

    /// Render a `KeyConditionExpression` into a DynamoDB expression string.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::Validation` if resolution fails.
    pub fn render_key_condition(
        &mut self,
        kc: &KeyCondition,
        maps: &ExpressionMaps,
    ) -> Result<String, StorageError> {
        // pk = value
        let pk_path_str = self.render_path(&kc.pk_path, maps)?;
        let pk_val_str = self.render_expr(&kc.pk_value, maps)?;
        let mut parts = vec![format!("{pk_path_str} = {pk_val_str}")];

        // extra_pk_conditions: rendered as equality conditions
        for (path, value) in &kc.extra_pk_conditions {
            let p = self.render_path(path, maps)?;
            let v = self.render_expr(value, maps)?;
            parts.push(format!("{p} = {v}"));
        }

        // sk_condition
        if let Some(sk) = &kc.sk_condition {
            let sk_str = self.render_sk_condition(sk, maps)?;
            parts.push(sk_str);
        }

        // extra_sk_conditions: rendered as equality conditions
        for (path, value) in &kc.extra_sk_conditions {
            let p = self.render_path(path, maps)?;
            let v = self.render_expr(value, maps)?;
            parts.push(format!("{p} = {v}"));
        }

        Ok(parts.join(" AND "))
    }

    /// Render a slice of `UpdateAction`s into a DynamoDB `UpdateExpression` string.
    ///
    /// Groups actions by type (SET, REMOVE, ADD, DELETE) and joins them as
    /// `SET a, b REMOVE c ADD d e DELETE f g`.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::Validation` if resolution fails.
    pub fn render_update(
        &mut self,
        actions: &[UpdateAction],
        maps: &ExpressionMaps,
    ) -> Result<String, StorageError> {
        let mut set_parts: Vec<String> = Vec::new();
        let mut remove_parts: Vec<String> = Vec::new();
        let mut add_parts: Vec<String> = Vec::new();
        let mut delete_parts: Vec<String> = Vec::new();

        for action in actions {
            match action {
                UpdateAction::Set { path, value } => {
                    let p = self.render_path(path, maps)?;
                    let v = self.render_expr(value, maps)?;
                    set_parts.push(format!("{p} = {v}"));
                }
                UpdateAction::Remove { path } => {
                    let p = self.render_path(path, maps)?;
                    remove_parts.push(p);
                }
                UpdateAction::Add { path, value } => {
                    let p = self.render_path(path, maps)?;
                    let v = self.render_expr(value, maps)?;
                    add_parts.push(format!("{p} {v}"));
                }
                UpdateAction::Delete { path, value } => {
                    let p = self.render_path(path, maps)?;
                    let v = self.render_expr(value, maps)?;
                    delete_parts.push(format!("{p} {v}"));
                }
            }
        }

        let mut groups: Vec<String> = Vec::new();
        if !set_parts.is_empty() {
            groups.push(format!("SET {}", set_parts.join(", ")));
        }
        if !remove_parts.is_empty() {
            groups.push(format!("REMOVE {}", remove_parts.join(", ")));
        }
        if !add_parts.is_empty() {
            groups.push(format!("ADD {}", add_parts.join(", ")));
        }
        if !delete_parts.is_empty() {
            groups.push(format!("DELETE {}", delete_parts.join(", ")));
        }

        Ok(groups.join(" "))
    }

    // --- private helpers ---

    fn render_expr(&mut self, e: &Expr, maps: &ExpressionMaps) -> Result<String, StorageError> {
        match e {
            Expr::Path(elements) => self.render_path(elements, maps),

            Expr::Placeholder(name) => {
                let core_val = maps
                    .resolve_value(name)
                    .map_err(|err: extenddb_core::error::DynamoDbError| StorageError::Validation(err.to_string()))?;
                let sdk_val = to_sdk(core_val);
                let token = format!(":v{}", self.v_counter);
                self.v_counter += 1;
                self.values.insert(token.clone(), sdk_val);
                Ok(token)
            }

            Expr::Compare { left, op, right } => {
                let l = self.render_expr(left, maps)?;
                let r = self.render_expr(right, maps)?;
                let op_str = match op {
                    CompareOp::Eq => "=",
                    CompareOp::Ne => "<>",
                    CompareOp::Lt => "<",
                    CompareOp::Le => "<=",
                    CompareOp::Gt => ">",
                    CompareOp::Ge => ">=",
                };
                Ok(format!("{l} {op_str} {r}"))
            }

            Expr::And(left, right) => {
                let l = self.render_expr(left, maps)?;
                let r = self.render_expr(right, maps)?;
                Ok(format!("({l} AND {r})"))
            }

            Expr::Or(left, right) => {
                let l = self.render_expr(left, maps)?;
                let r = self.render_expr(right, maps)?;
                Ok(format!("({l} OR {r})"))
            }

            Expr::Not(inner) => {
                let s = self.render_expr(inner, maps)?;
                Ok(format!("(NOT {s})"))
            }

            Expr::Function { name, args } => {
                let mut rendered_args = Vec::with_capacity(args.len());
                for arg in args {
                    rendered_args.push(self.render_expr(arg, maps)?);
                }
                Ok(format!("{}({})", name, rendered_args.join(", ")))
            }

            Expr::Arithmetic { left, op, right } => {
                let l = self.render_expr(left, maps)?;
                let r = self.render_expr(right, maps)?;
                let op_str = match op {
                    ArithOp::Add => "+",
                    ArithOp::Sub => "-",
                };
                Ok(format!("{l} {op_str} {r}"))
            }

            Expr::Between { operand, low, high } => {
                let e_str = self.render_expr(operand, maps)?;
                let lo = self.render_expr(low, maps)?;
                let hi = self.render_expr(high, maps)?;
                Ok(format!("{e_str} BETWEEN {lo} AND {hi}"))
            }

            Expr::In { operand, list } => {
                let e_str = self.render_expr(operand, maps)?;
                let mut items = Vec::with_capacity(list.len());
                for item in list {
                    items.push(self.render_expr(item, maps)?);
                }
                Ok(format!("{e_str} IN ({})", items.join(", ")))
            }
        }
    }

    /// Render a document path into a DynamoDB token string.
    ///
    /// Each `PathElement::Attribute` is resolved to its real name (via
    /// `resolve_name_ref`), allocated a fresh `#n{N}` token, and stored in
    /// `self.names`. Index elements become `[i]`.
    ///
    /// Path format: `#n0` for a single attribute; `#n0.#n1[2].#n3` for nested.
    fn render_path(
        &mut self,
        elements: &[PathElement],
        maps: &ExpressionMaps,
    ) -> Result<String, StorageError> {
        let mut result = String::new();

        for element in elements {
            match element {
                PathElement::Attribute(name) => {
                    let real_name = resolve_name_ref(name, maps)
                        .map_err(|err: extenddb_core::error::DynamoDbError| StorageError::Validation(err.to_string()))?;
                    let token = format!("#n{}", self.n_counter);
                    self.n_counter += 1;
                    self.names.insert(token.clone(), real_name.into_owned());
                    if result.is_empty() {
                        result.push_str(&token);
                    } else {
                        result.push('.');
                        result.push_str(&token);
                    }
                }
                PathElement::Index(idx) => {
                    result.push_str(&format!("[{idx}]"));
                }
            }
        }

        Ok(result)
    }

    fn render_sk_condition(
        &mut self,
        sk: &SortKeyCondition,
        maps: &ExpressionMaps,
    ) -> Result<String, StorageError> {
        match sk {
            SortKeyCondition::Compare { path, op, value } => {
                let p = self.render_path(path, maps)?;
                let v = self.render_expr(value, maps)?;
                let op_str = match op {
                    CompareOp::Eq => "=",
                    CompareOp::Ne => "<>",
                    CompareOp::Lt => "<",
                    CompareOp::Le => "<=",
                    CompareOp::Gt => ">",
                    CompareOp::Ge => ">=",
                };
                Ok(format!("{p} {op_str} {v}"))
            }
            SortKeyCondition::Between { path, low, high } => {
                let p = self.render_path(path, maps)?;
                let lo = self.render_expr(low, maps)?;
                let hi = self.render_expr(high, maps)?;
                Ok(format!("{p} BETWEEN {lo} AND {hi}"))
            }
            SortKeyCondition::BeginsWith { path, prefix } => {
                let p = self.render_path(path, maps)?;
                let pref = self.render_expr(prefix, maps)?;
                Ok(format!("begins_with({p}, {pref})"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::expression::{Expr, CompareOp, ExpressionMaps, PathElement, UpdateAction};
    use extenddb_core::types::AttributeValue as Av;
    use std::collections::HashMap;

    fn maps_with(values: &[(&str, Av)], names: &[(&str, &str)]) -> ExpressionMaps {
        let v = values.iter().map(|(k, val)| (k.to_string(), val.clone())).collect::<HashMap<_, _>>();
        let n = names.iter().map(|(k, val)| (k.to_string(), val.to_string())).collect::<HashMap<_, _>>();
        ExpressionMaps::new(n, v)
    }

    #[test]
    fn condition_attribute_exists_bare_name() {
        let e = Expr::Function {
            name: "attribute_exists".into(),
            args: vec![Expr::Path(vec![PathElement::Attribute("pk".into())])],
        };
        let mut r = Renderer::new();
        let s = r.render_condition(&e, &ExpressionMaps::default()).unwrap();
        assert_eq!(s, "attribute_exists(#n0)");
        assert_eq!(r.names().get("#n0").map(String::as_str), Some("pk"));
    }

    #[test]
    fn condition_compare_with_value_placeholder() {
        // age >= :min  where :min resolves to N "21"
        let e = Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute("age".into())])),
            op: CompareOp::Ge,
            right: Box::new(Expr::Placeholder("min".into())),
        };
        let maps = maps_with(&[("min", Av::N("21".into()))], &[]);
        let mut r = Renderer::new();
        let s = r.render_condition(&e, &maps).unwrap();
        assert_eq!(s, "#n0 >= :v0");
        assert_eq!(r.names().get("#n0").map(String::as_str), Some("age"));
        assert!(r.values().contains_key(":v0"));
    }

    #[test]
    fn hash_reference_resolves_via_names_map() {
        // path "#a" should resolve through maps.names to the real attribute
        let e = Expr::Function {
            name: "attribute_not_exists".into(),
            args: vec![Expr::Path(vec![PathElement::Attribute("#a".into())])],
        };
        let maps = maps_with(&[], &[("a", "status")]);
        let mut r = Renderer::new();
        let s = r.render_condition(&e, &maps).unwrap();
        assert_eq!(s, "attribute_not_exists(#n0)");
        assert_eq!(r.names().get("#n0").map(String::as_str), Some("status"));
    }

    #[test]
    fn update_set_and_remove() {
        let actions = vec![
            UpdateAction::Set {
                path: vec![PathElement::Attribute("name".into())],
                value: Expr::Placeholder("nm".into()),
            },
            UpdateAction::Remove {
                path: vec![PathElement::Attribute("temp".into())],
            },
        ];
        let maps = maps_with(&[("nm", Av::S("Bob".into()))], &[]);
        let mut r = Renderer::new();
        let s = r.render_update(&actions, &maps).unwrap();
        assert_eq!(s, "SET #n0 = :v0 REMOVE #n1");
    }
}
