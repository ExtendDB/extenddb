// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Expression attribute name and value resolution.
//!
//! Resolves `#name` references and `:value` placeholders against the maps
//! provided in `ExpressionAttributeNames` and `ExpressionAttributeValues`.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use crate::error::DynamoDbError;
use crate::types::AttributeValue;

use super::ast::Expr;
use super::ast::PathElement;

/// Resolved expression attribute names and values.
///
/// Constructed from the request's `ExpressionAttributeNames` and
/// `ExpressionAttributeValues` maps. Used by the parser and evaluator.
///
/// Numeric placeholder values are pre-parsed into `BigDecimal` at construction
/// time so that filter expressions comparing a placeholder against many items
/// parse the placeholder only once per request.
#[derive(Debug, Clone, Default)]
pub struct ExpressionMaps {
    pub names: HashMap<String, String>,
    pub values: HashMap<String, AttributeValue>,
    /// Pre-parsed `BigDecimal` values for numeric placeholders.
    /// Populated eagerly by `new()` and refreshed by `pre_parse_numerics()`.
    parsed_numerics: HashMap<String, bigdecimal::BigDecimal>,
}

impl ExpressionMaps {
    /// Create a new `ExpressionMaps` with the given names and values.
    ///
    /// Pre-parses all numeric placeholder values into `BigDecimal`.
    #[must_use]
    pub fn new(names: HashMap<String, String>, values: HashMap<String, AttributeValue>) -> Self {
        let mut maps = Self {
            names,
            values,
            parsed_numerics: HashMap::new(),
        };
        maps.pre_parse_numerics();
        maps
    }

    /// Resolve a `#name` reference to an attribute name.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` if the name is not in the map.
    pub fn resolve_name(&self, name_ref: &str) -> Result<&str, DynamoDbError> {
        self.names.get(name_ref).map(String::as_str).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "An expression attribute name used in the document path is not defined; attribute name: #{name_ref}"
            ))
        })
    }

    /// Resolve a `:value` placeholder to an `AttributeValue`.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` if the placeholder is not in the map.
    pub fn resolve_value(&self, placeholder: &str) -> Result<&AttributeValue, DynamoDbError> {
        self.values.get(placeholder).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "An expression attribute value used in expression is not defined; attribute value: :{placeholder}"
            ))
        })
    }

    /// Like [`resolve_value`](Self::resolve_value) but prefixes the error with the expression type.
    pub fn resolve_value_for(
        &self,
        placeholder: &str,
        expr_type: &str,
    ) -> Result<&AttributeValue, DynamoDbError> {
        self.values.get(placeholder).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "Invalid {expr_type}: An expression attribute value used in expression is not defined; attribute value: :{placeholder}"
            ))
        })
    }

    /// Pre-parse all numeric placeholder values into `BigDecimal`.
    ///
    /// Call once per request before evaluating filter expressions against
    /// multiple items. Invalid numeric strings are silently skipped — they
    /// will produce `false` comparisons at evaluation time, matching the
    /// existing behavior.
    pub fn pre_parse_numerics(&mut self) {
        for (key, value) in &self.values {
            if let AttributeValue::N(n) = value
                && let Ok(d) = n.parse::<bigdecimal::BigDecimal>()
            {
                self.parsed_numerics.insert(key.clone(), d);
            }
        }
    }

    /// Look up a pre-parsed `BigDecimal` for a numeric placeholder.
    ///
    /// Returns `None` if the placeholder was not numeric or failed to parse.
    #[must_use]
    pub fn get_parsed_numeric(&self, placeholder: &str) -> Option<&bigdecimal::BigDecimal> {
        self.parsed_numerics.get(placeholder)
    }
}

/// Resolve a `#name` reference or return the bare name.
///
/// Shared by the condition evaluator and update evaluator for resolving
/// attribute names in document paths.
///
/// # Errors
///
/// Returns `ValidationException` if a `#name` reference is not in the map.
pub fn resolve_name_ref<'a>(
    name: &'a str,
    maps: &'a ExpressionMaps,
) -> Result<Cow<'a, str>, DynamoDbError> {
    if let Some(ref_name) = name.strip_prefix('#') {
        Ok(Cow::Owned(maps.resolve_name(ref_name)?.to_owned()))
    } else {
        Ok(Cow::Borrowed(name))
    }
}

/// Walk a document path to find the target attribute value.
///
/// Resolves `#name` references along the way. Returns `None` for missing paths.
/// Shared by the condition evaluator and update evaluator.
///
/// # Errors
///
/// Returns `ValidationException` for unresolvable `#name` references.
pub fn resolve_path<'a>(
    elements: &[PathElement],
    item: &'a BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<Option<&'a AttributeValue>, DynamoDbError> {
    if elements.is_empty() {
        return Ok(None);
    }

    let first_name = resolve_element_name(&elements[0], maps)?;
    let Some(mut current) = item.get(first_name.as_ref()) else {
        return Ok(None);
    };

    for element in &elements[1..] {
        match element {
            PathElement::Attribute(name) => {
                let resolved = resolve_name_ref(name, maps)?;
                match current {
                    AttributeValue::M(map) => {
                        current = match map.get(resolved.as_ref()) {
                            Some(v) => v,
                            None => return Ok(None),
                        };
                    }
                    _ => return Ok(None),
                }
            }
            PathElement::Index(idx) => match current {
                AttributeValue::L(list) => {
                    current = match list.get(*idx) {
                        Some(v) => v,
                        None => return Ok(None),
                    };
                }
                _ => return Ok(None),
            },
        }
    }

    Ok(Some(current))
}

/// Resolve the attribute name from a path element, handling `#name` references.
///
/// Returns an error if the path starts with an index.
///
/// # Errors
///
/// Returns `ValidationException` for unresolvable references or index-start paths.
pub fn resolve_element_name<'a>(
    element: &'a PathElement,
    maps: &'a ExpressionMaps,
) -> Result<Cow<'a, str>, DynamoDbError> {
    match element {
        PathElement::Attribute(name) => resolve_name_ref(name, maps),
        PathElement::Index(_) => Err(DynamoDbError::ValidationException(
            "Invalid expression: path cannot start with an index".to_owned(),
        )),
    }
}

/// Reject `ExpressionAttributeNames`/`Values` supplied with no expression that
/// can reference them. Matches Amazon DynamoDB:
///
/// - names: `ExpressionAttributeNames can only be specified when using expressions`
///   (no suffix), for every API.
/// - values: `ExpressionAttributeValues can only be specified when using
///   expressions: <params> is/are null`, listing the value-capable expression
///   parameter(s) of the API.
///
/// `name_expr_present` is true when any expression that can carry a `#name` is
/// present (projection, condition, filter, key condition, update).
/// `value_expr_present` is true when any expression that can carry a `:value` is
/// present (condition, filter, key condition, update; not projection).
/// `value_expr_params` is the API's static list of value-capable expression
/// parameter kinds, used to build the values suffix. An empty list yields no
/// suffix (matching Query).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when a map is supplied with no
/// referencing expression.
pub fn validate_expression_param_usage(
    names: Option<&HashMap<String, String>>,
    name_expr_present: bool,
    values: Option<&HashMap<String, AttributeValue>>,
    value_expr_present: bool,
    value_expr_params: &[super::ExpressionKind],
) -> Result<(), DynamoDbError> {
    if !name_expr_present && names.is_some_and(|m| !m.is_empty()) {
        return Err(DynamoDbError::ValidationException(
            "ExpressionAttributeNames can only be specified when using expressions".to_owned(),
        ));
    }
    if !value_expr_present && values.is_some_and(|m| !m.is_empty()) {
        let base = "ExpressionAttributeValues can only be specified when using expressions";
        let msg = match value_expr_params {
            [] => base.to_owned(),
            [one] => format!("{base}: {one} is null"),
            many => {
                let joined = many
                    .iter()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join(" and ");
                format!("{base}: {joined} are null")
            }
        };
        return Err(DynamoDbError::ValidationException(msg));
    }
    Ok(())
}

/// Validate that all provided ExpressionAttributeNames/Values were used.
///
/// Collects `#name` and `:value` references from the given expressions,
/// then checks that every key in `names`/`values` maps was referenced.
pub fn validate_unused_attributes(
    names: &HashMap<String, String>,
    values: &HashMap<String, AttributeValue>,
    exprs: &[&super::ast::Expr],
    update_actions: &[&super::ast::UpdateAction],
    key_condition_names: &std::collections::HashSet<String>,
    key_condition_values: &std::collections::HashSet<String>,
) -> Result<(), DynamoDbError> {
    let mut used_names = key_condition_names.clone();
    let mut used_values = key_condition_values.clone();

    for expr in exprs {
        collect_expr_refs(expr, &mut used_names, &mut used_values);
    }
    for action in update_actions {
        collect_action_refs(action, &mut used_names, &mut used_values);
    }

    for key in names.keys() {
        if !used_names.contains(key.strip_prefix('#').unwrap_or(key)) {
            let display_key = if key.starts_with('#') {
                key.clone()
            } else {
                format!("#{key}")
            };
            return Err(DynamoDbError::ValidationException(format!(
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {{{display_key}}}"
            )));
        }
    }
    for key in values.keys() {
        if !used_values.contains(key.strip_prefix(':').unwrap_or(key)) {
            let display_key = if key.starts_with(':') {
                key.clone()
            } else {
                format!(":{key}")
            };
            return Err(DynamoDbError::ValidationException(format!(
                "Value provided in ExpressionAttributeValues unused in expressions: keys: {{{display_key}}}"
            )));
        }
    }
    Ok(())
}

fn collect_expr_refs(
    expr: &super::ast::Expr,
    names: &mut std::collections::HashSet<String>,
    values: &mut std::collections::HashSet<String>,
) {
    use super::ast::Expr;
    match expr {
        Expr::Path(elements) => collect_path_refs(elements, names),
        Expr::Placeholder(name) => {
            values.insert(name.clone());
        }
        Expr::Compare { left, right, .. } | Expr::Arithmetic { left, right, .. } => {
            collect_expr_refs(left, names, values);
            collect_expr_refs(right, names, values);
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            collect_expr_refs(l, names, values);
            collect_expr_refs(r, names, values);
        }
        Expr::Not(inner) => collect_expr_refs(inner, names, values),
        Expr::Function { args, .. } => {
            for arg in args {
                collect_expr_refs(arg, names, values);
            }
        }
        Expr::Between { operand, low, high } => {
            collect_expr_refs(operand, names, values);
            collect_expr_refs(low, names, values);
            collect_expr_refs(high, names, values);
        }
        Expr::In { operand, list } => {
            collect_expr_refs(operand, names, values);
            for item in list {
                collect_expr_refs(item, names, values);
            }
        }
    }
}

/// Walk an `Expr` tree and append every `Expr::Placeholder(name)` reference to `out`.
///
/// Used by `UpdateItem` depth validation: for each `SET` action's right-hand
/// side, collect the EAV placeholders referenced (directly or via
/// `if_not_exists`, `list_append`, arithmetic, etc.). Resolving those names
/// against the `ExpressionMaps` yields the set of attribute values that will
/// be stored, so their nesting depth must be validated.
pub fn collect_value_placeholders(expr: &super::ast::Expr, out: &mut Vec<String>) {
    use super::ast::Expr;
    match expr {
        Expr::Placeholder(name) => out.push(name.clone()),
        Expr::Path(_) => {}
        Expr::Compare { left, right, .. } | Expr::Arithmetic { left, right, .. } => {
            collect_value_placeholders(left, out);
            collect_value_placeholders(right, out);
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            collect_value_placeholders(l, out);
            collect_value_placeholders(r, out);
        }
        Expr::Not(inner) => collect_value_placeholders(inner, out),
        Expr::Function { args, .. } => {
            for a in args {
                collect_value_placeholders(a, out);
            }
        }
        Expr::Between { operand, low, high } => {
            collect_value_placeholders(operand, out);
            collect_value_placeholders(low, out);
            collect_value_placeholders(high, out);
        }
        Expr::In { operand, list } => {
            collect_value_placeholders(operand, out);
            for i in list {
                collect_value_placeholders(i, out);
            }
        }
    }
}

fn collect_action_refs(
    action: &super::ast::UpdateAction,
    names: &mut std::collections::HashSet<String>,
    values: &mut std::collections::HashSet<String>,
) {
    use super::ast::UpdateAction;
    match action {
        UpdateAction::Set { path, value }
        | UpdateAction::Add { path, value }
        | UpdateAction::Delete { path, value } => {
            collect_path_refs(path, names);
            collect_expr_refs(value, names, values);
        }
        UpdateAction::Remove { path } => collect_path_refs(path, names),
    }
}

fn collect_path_refs(elements: &[PathElement], names: &mut std::collections::HashSet<String>) {
    for el in elements {
        if let PathElement::Attribute(name) = el
            && let Some(ref_name) = name.strip_prefix('#')
        {
            names.insert(ref_name.to_owned());
        }
    }
}

/// Collect `#name` and `:value` references from a `KeyCondition`.
///
/// Returns `(used_names, used_values)` sets suitable for passing to
/// `validate_unused_attributes`.
pub fn collect_key_condition_refs(
    kc: &super::key_condition::KeyCondition,
) -> (
    std::collections::HashSet<String>,
    std::collections::HashSet<String>,
) {
    let mut names = std::collections::HashSet::new();
    let mut values = std::collections::HashSet::new();

    collect_path_refs(&kc.pk_path, &mut names);
    collect_expr_refs(&kc.pk_value, &mut names, &mut values);

    if let Some(ref sk) = kc.sk_condition {
        match sk {
            super::key_condition::SortKeyCondition::Compare { path, value, .. } => {
                collect_path_refs(path, &mut names);
                collect_expr_refs(value, &mut names, &mut values);
            }
            super::key_condition::SortKeyCondition::Between { path, low, high } => {
                collect_path_refs(path, &mut names);
                collect_expr_refs(low, &mut names, &mut values);
                collect_expr_refs(high, &mut names, &mut values);
            }
            super::key_condition::SortKeyCondition::BeginsWith { path, prefix } => {
                collect_path_refs(path, &mut names);
                collect_expr_refs(prefix, &mut names, &mut values);
            }
        }
    }

    for (path, expr) in &kc.extra_pk_conditions {
        collect_path_refs(path, &mut names);
        collect_expr_refs(expr, &mut names, &mut values);
    }
    for (path, expr) in &kc.extra_sk_conditions {
        collect_path_refs(path, &mut names);
        collect_expr_refs(expr, &mut names, &mut values);
    }

    (names, values)
}

/// Validate `begins_with` operand types in a parsed expression.
///
/// DynamoDB rejects `begins_with(path, value)` upfront when `value` is not
/// a string or binary type. This validation runs before evaluation so that
/// empty scans/queries still reject invalid operand types.
///
/// Returns `Ok(())` if all `begins_with` calls have valid operand types,
/// or `Err(ValidationException)` with the appropriate error message.
pub fn validate_begins_with_operands(
    expr: &Expr,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    match expr {
        Expr::Function { name, args } if name == "begins_with" => {
            if args.len() == 2
                && let Expr::Placeholder(ref placeholder) = args[1]
                && let Some(val) = maps.values.get(placeholder)
                && !matches!(val, AttributeValue::S(_) | AttributeValue::B(_))
            {
                let type_code = match val {
                    AttributeValue::N(_) => "N",
                    AttributeValue::Bool(_) => "BOOL",
                    AttributeValue::Null => "NULL",
                    AttributeValue::L(_) => "L",
                    AttributeValue::M(_) => "M",
                    AttributeValue::SS(_) => "SS",
                    AttributeValue::NS(_) => "NS",
                    AttributeValue::BS(_) => "BS",
                    _ => "UNKNOWN",
                };
                return Err(DynamoDbError::ValidationException(format!(
                    "Incorrect operand type for operator or function; operator or function: begins_with, operand type: {type_code}"
                )));
            }
            Ok(())
        }
        Expr::And(left, right) | Expr::Or(left, right) => {
            validate_begins_with_operands(left, maps)?;
            validate_begins_with_operands(right, maps)
        }
        Expr::Not(inner) => validate_begins_with_operands(inner, maps),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::ast::{ArithOp, Expr, PathElement};
    use crate::expression::parser::parse_condition;
    use crate::expression::tokenizer::tokenize;
    use std::collections::BTreeSet;

    fn parse(input: &str) -> Expr {
        let tokens = tokenize(input).unwrap();
        parse_condition(&tokens).unwrap()
    }

    fn maps_with(key: &str, val: AttributeValue) -> ExpressionMaps {
        let mut values = HashMap::new();
        values.insert(key.to_owned(), val);
        ExpressionMaps::new(HashMap::new(), values)
    }

    #[test]
    fn begins_with_rejects_number_upfront() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::N("1".into()));
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: N"));
    }

    #[test]
    fn begins_with_rejects_bool_upfront() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::Bool(true));
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: BOOL"));
    }

    #[test]
    fn begins_with_rejects_null_upfront() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::Null);
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: NULL"));
    }

    #[test]
    fn begins_with_rejects_list_upfront() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::L(vec![]));
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: L"));
    }

    #[test]
    fn begins_with_rejects_map_upfront() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::M(BTreeMap::new()));
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: M"));
    }

    #[test]
    fn begins_with_rejects_string_set_upfront() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::SS(BTreeSet::from(["a".into()])));
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: SS"));
    }

    #[test]
    fn begins_with_rejects_number_set_upfront() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::NS(BTreeSet::from(["1".into()])));
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: NS"));
    }

    #[test]
    fn begins_with_rejects_binary_set_upfront() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::BS(BTreeSet::from([vec![1u8]])));
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: BS"));
    }

    #[test]
    fn begins_with_accepts_string() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::S("hello".into()));
        assert!(validate_begins_with_operands(&expr, &maps).is_ok());
    }

    #[test]
    fn begins_with_accepts_binary() {
        let expr = parse("begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::B(vec![1, 2, 3]));
        assert!(validate_begins_with_operands(&expr, &maps).is_ok());
    }

    #[test]
    fn begins_with_nested_in_and_rejected() {
        let expr = parse("pk = :pk AND begins_with(sk, :n)");
        let mut values = HashMap::new();
        values.insert("pk".to_owned(), AttributeValue::S("x".into()));
        values.insert("n".to_owned(), AttributeValue::N("1".into()));
        let maps = ExpressionMaps::new(HashMap::new(), values);
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: N"));
    }

    #[test]
    fn begins_with_nested_in_or_rejected() {
        let expr = parse("pk = :pk OR begins_with(sk, :n)");
        let mut values = HashMap::new();
        values.insert("pk".to_owned(), AttributeValue::S("x".into()));
        values.insert("n".to_owned(), AttributeValue::N("1".into()));
        let maps = ExpressionMaps::new(HashMap::new(), values);
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: N"));
    }

    #[test]
    fn begins_with_nested_in_not_rejected() {
        let expr = parse("NOT begins_with(pk, :n)");
        let maps = maps_with("n", AttributeValue::N("1".into()));
        let err = validate_begins_with_operands(&expr, &maps).unwrap_err();
        assert!(err.to_string().contains("operand type: N"));
    }

    fn placeholder(s: &str) -> Expr {
        Expr::Placeholder(s.to_owned())
    }

    #[test]
    fn collect_value_placeholders_finds_direct_reference() {
        let mut out = Vec::new();
        collect_value_placeholders(&placeholder(":d"), &mut out);
        assert_eq!(out, vec![":d".to_owned()]);
    }

    #[test]
    fn collect_value_placeholders_walks_function_args() {
        // SET v = if_not_exists(path, :default)
        let expr = Expr::Function {
            name: "if_not_exists".to_owned(),
            args: vec![
                Expr::Path(vec![PathElement::Attribute("path".to_owned())]),
                placeholder(":default"),
            ],
        };
        let mut out = Vec::new();
        collect_value_placeholders(&expr, &mut out);
        assert_eq!(out, vec![":default".to_owned()]);
    }

    #[test]
    fn collect_value_placeholders_walks_arithmetic() {
        // SET v = :a + :b
        let expr = Expr::Arithmetic {
            left: Box::new(placeholder(":a")),
            op: ArithOp::Add,
            right: Box::new(placeholder(":b")),
        };
        let mut out = Vec::new();
        collect_value_placeholders(&expr, &mut out);
        assert_eq!(out, vec![":a".to_owned(), ":b".to_owned()]);
    }

    #[test]
    fn collect_value_placeholders_walks_nested_function() {
        // SET v = list_append(:base, list_append(:extra, :more))
        let expr = Expr::Function {
            name: "list_append".to_owned(),
            args: vec![
                placeholder(":base"),
                Expr::Function {
                    name: "list_append".to_owned(),
                    args: vec![placeholder(":extra"), placeholder(":more")],
                },
            ],
        };
        let mut out = Vec::new();
        collect_value_placeholders(&expr, &mut out);
        assert_eq!(
            out,
            vec![":base".to_owned(), ":extra".to_owned(), ":more".to_owned()]
        );
    }

    #[test]
    fn collect_value_placeholders_path_only_yields_nothing() {
        let expr = Expr::Path(vec![PathElement::Attribute("address".to_owned())]);
        let mut out = Vec::new();
        collect_value_placeholders(&expr, &mut out);
        assert!(out.is_empty());
    }
}
