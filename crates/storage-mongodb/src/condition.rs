// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Condition expression to `MongoDB` filter compiler.
//!
//! Translates `extenddb_core::expression::Expr` AST into a `bson::Document`
//! query filter for use with `MongoDB`'s filter pushdown (findOneAndReplace, etc.).

use bson::{Bson, Document, doc};

use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, PathElement};
use extenddb_core::types::AttributeValue;
use extenddb_storage::error::StorageError;

/// Compile a condition expression to a `MongoDB` filter document.
///
/// The resulting filter operates on the `item_data` field of the `MongoDB` document.
/// Returns an empty document if no condition is provided.
pub fn condition_to_filter(expr: &Expr, maps: &ExpressionMaps) -> Result<Document, StorageError> {
    compile_expr(expr, maps)
}

/// Resolve a path to the `MongoDB` field path within `item_data`.
///
/// `DynamoDB` paths like `address.city` or `tags[0]` become
/// `item_data.address.city` or `item_data.tags.L.0` in `MongoDB`.
fn resolve_path_to_field(
    elements: &[PathElement],
    maps: &ExpressionMaps,
) -> Result<String, StorageError> {
    let mut parts = vec!["item_data".to_string()];
    for elem in elements {
        match elem {
            PathElement::Attribute(name) => {
                let resolved = if let Some(stripped) = name.strip_prefix('#') {
                    maps.resolve_name(stripped)
                        .map_err(|e| StorageError::Validation(e.to_string()))?
                        .to_string()
                } else {
                    name.clone()
                };
                parts.push(resolved);
            }
            PathElement::Index(idx) => {
                // For list index access: item_data.attr.L.<idx>
                parts.push("L".to_string());
                parts.push(idx.to_string());
            }
        }
    }
    Ok(parts.join("."))
}

/// Convert an `AttributeValue` to a BSON value for filter comparisons.
///
/// The value must match how `data/mod.rs::item_to_document` stores the
/// value inside `item_data`, otherwise the compiled filter will not
/// match real stored items. Storage serializes each attribute value via
/// the `AttributeValue` `Serialize` impl (JSON-shape) and then converts
/// to BSON — so:
///
/// - `S(s)` → JSON string → BSON string
/// - `N(n)` → JSON string (numbers are wire-encoded as strings) → BSON string
/// - `B(b)` → JSON string (base64) → BSON string
/// - `Bool(b)` → BSON boolean
/// - `Null` → JSON true (the `{"NULL": true}` tag) → BSON boolean true
///
/// Emitting raw BSON `Binary` for `.B` here (as an earlier version did)
/// produced a filter that never matched real stored items because
/// storage writes the base64 string form.
fn av_to_bson(av: &AttributeValue) -> Bson {
    use base64::Engine;
    match av {
        AttributeValue::S(s) => Bson::String(s.clone()),
        AttributeValue::N(n) => Bson::String(n.clone()),
        AttributeValue::B(b) => Bson::String(base64::engine::general_purpose::STANDARD.encode(b)),
        AttributeValue::Bool(b) => Bson::Boolean(*b),
        AttributeValue::Null => Bson::Boolean(true), // NULL type stores {"NULL": true}
        _ => Bson::Null,                             // Sets and complex types handled differently
    }
}

/// Get the type suffix for a `DynamoDB` `AttributeValue` (S, N, B, BOOL, NULL, L, M, SS, NS, BS).
fn av_type_suffix(av: &AttributeValue) -> &'static str {
    match av {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
    }
}

/// Resolve a value expression (Path or Placeholder) to the field path and BSON value.
///
/// Returns (`field_path_for_filter`, `bson_value`) or just the `bson_value` for placeholders.
enum ResolvedValue {
    /// A field path in the document (e.g., "`item_data.age.N`")
    Field(String),
    /// A literal BSON value with its type suffix
    Literal(Bson, &'static str),
}

fn resolve_value(expr: &Expr, maps: &ExpressionMaps) -> Result<ResolvedValue, StorageError> {
    match expr {
        Expr::Path(elements) => {
            let field = resolve_path_to_field(elements, maps)?;
            Ok(ResolvedValue::Field(field))
        }
        Expr::Placeholder(name) => {
            let av = maps
                .resolve_value(name)
                .map_err(|e| StorageError::Validation(e.to_string()))?;
            let suffix = av_type_suffix(av);
            let bson_val = av_to_bson(av);
            Ok(ResolvedValue::Literal(bson_val, suffix))
        }
        _ => Err(StorageError::Validation(
            "Unexpected expression type in condition value position".to_string(),
        )),
    }
}

/// Build a comparison filter between two expressions.
fn build_comparison(
    left: &Expr,
    op: CompareOp,
    right: &Expr,
    maps: &ExpressionMaps,
) -> Result<Document, StorageError> {
    let left_resolved = resolve_value(left, maps)?;
    let right_resolved = resolve_value(right, maps)?;

    // Determine the field path and value for the comparison
    let (field_path, value) = match (left_resolved, right_resolved) {
        (ResolvedValue::Field(path), ResolvedValue::Literal(val, suffix)) => {
            // field op :value -> item_data.field.TYPE op val
            let typed_path = format!("{path}.{suffix}");
            (typed_path, val)
        }
        (ResolvedValue::Literal(val, suffix), ResolvedValue::Field(path)) => {
            // :value op field -> reverse the comparison
            let typed_path = format!("{path}.{suffix}");
            let reversed_op = reverse_op(op);
            return build_field_comparison(&typed_path, reversed_op, val);
        }
        (ResolvedValue::Field(left_path), ResolvedValue::Field(right_path)) => {
            // field op field -> use $expr
            return build_field_vs_field_comparison(&left_path, op, &right_path);
        }
        (ResolvedValue::Literal(_, _), ResolvedValue::Literal(_, _)) => {
            // literal op literal -> evaluate statically (unusual case)
            // For simplicity, just return an empty filter (always true)
            return Ok(doc! {});
        }
    };

    build_field_comparison(&field_path, op, value)
}

fn build_field_comparison(
    field: &str,
    op: CompareOp,
    value: Bson,
) -> Result<Document, StorageError> {
    let filter = match op {
        CompareOp::Eq => doc! { field: value },
        CompareOp::Ne => doc! { field: { "$ne": value } },
        CompareOp::Lt => doc! { field: { "$lt": value } },
        CompareOp::Le => doc! { field: { "$lte": value } },
        CompareOp::Gt => doc! { field: { "$gt": value } },
        CompareOp::Ge => doc! { field: { "$gte": value } },
    };
    Ok(filter)
}

fn build_field_vs_field_comparison(
    left_field: &str,
    op: CompareOp,
    right_field: &str,
) -> Result<Document, StorageError> {
    let mongo_op = match op {
        CompareOp::Eq => "$eq",
        CompareOp::Ne => "$ne",
        CompareOp::Lt => "$lt",
        CompareOp::Le => "$lte",
        CompareOp::Gt => "$gt",
        CompareOp::Ge => "$gte",
    };
    Ok(doc! {
        "$expr": {
            mongo_op: [format!("${left_field}"), format!("${right_field}")]
        }
    })
}

fn reverse_op(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
    }
}

/// Compile an expression AST node to a `MongoDB` filter document.
fn compile_expr(expr: &Expr, maps: &ExpressionMaps) -> Result<Document, StorageError> {
    match expr {
        Expr::Compare { left, op, right } => build_comparison(left, *op, right, maps),

        Expr::And(left, right) => {
            let left_filter = compile_expr(left, maps)?;
            let right_filter = compile_expr(right, maps)?;
            Ok(doc! { "$and": [left_filter, right_filter] })
        }

        Expr::Or(left, right) => {
            let left_filter = compile_expr(left, maps)?;
            let right_filter = compile_expr(right, maps)?;
            Ok(doc! { "$or": [left_filter, right_filter] })
        }

        Expr::Not(inner) => {
            let inner_filter = compile_expr(inner, maps)?;
            Ok(doc! { "$nor": [inner_filter] })
        }

        Expr::Function { name, args } => compile_function(name, args, maps),

        Expr::Between { operand, low, high } => compile_between(operand, low, high, maps),

        Expr::In { operand, list } => compile_in(operand, list, maps),

        _ => Err(StorageError::Validation(
            "Unsupported expression type in condition filter".to_string(),
        )),
    }
}

fn compile_function(
    name: &str,
    args: &[Expr],
    maps: &ExpressionMaps,
) -> Result<Document, StorageError> {
    match name.to_lowercase().as_str() {
        "attribute_exists" => {
            if args.len() != 1 {
                return Err(StorageError::Validation(
                    "attribute_exists requires exactly one argument".to_string(),
                ));
            }
            let field = resolve_path_from_expr(&args[0], maps)?;
            Ok(doc! { &field: { "$exists": true } })
        }

        "attribute_not_exists" => {
            if args.len() != 1 {
                return Err(StorageError::Validation(
                    "attribute_not_exists requires exactly one argument".to_string(),
                ));
            }
            let field = resolve_path_from_expr(&args[0], maps)?;
            Ok(doc! { &field: { "$exists": false } })
        }

        "begins_with" => {
            if args.len() != 2 {
                return Err(StorageError::Validation(
                    "begins_with requires exactly two arguments".to_string(),
                ));
            }
            let field = resolve_path_from_expr(&args[0], maps)?;
            let prefix_val = resolve_literal(&args[1], maps)?;
            match prefix_val {
                AttributeValue::S(prefix) => {
                    let escaped = regex_escape(&prefix);
                    let typed_field = format!("{field}.S");
                    Ok(doc! { &typed_field: { "$regex": format!("^{escaped}") } })
                }
                _ => Err(StorageError::Validation(
                    "begins_with requires a string prefix".to_string(),
                )),
            }
        }

        "contains" => {
            if args.len() != 2 {
                return Err(StorageError::Validation(
                    "contains requires exactly two arguments".to_string(),
                ));
            }
            let field = resolve_path_from_expr(&args[0], maps)?;
            let val = resolve_literal(&args[1], maps)?;
            match &val {
                AttributeValue::S(substr) => {
                    // String contains: check substring in string field OR membership in SS/L
                    let escaped = regex_escape(substr);
                    let string_field = format!("{field}.S");
                    let ss_field = format!("{field}.SS");
                    let list_field = format!("{field}.L");
                    let bson_val = av_to_bson(&val);
                    let list_elem = doc! { "S": substr.as_str() };
                    Ok(doc! { "$or": [
                        { &string_field: { "$regex": &escaped } },
                        { &ss_field: &bson_val },
                        { &list_field: &list_elem },
                    ] })
                }
                AttributeValue::N(n) => {
                    // Number membership in NS or L
                    let ns_field = format!("{field}.NS");
                    let list_field = format!("{field}.L");
                    let bson_val = av_to_bson(&val);
                    let list_elem = doc! { "N": n.as_str() };
                    Ok(doc! { "$or": [
                        { &ns_field: &bson_val },
                        { &list_field: &list_elem },
                    ] })
                }
                AttributeValue::B(_) => {
                    // Binary membership in BS or L. Storage serializes B
                    // as base64 strings inside item_data (matches wire
                    // format via the JSON serializer), so both the field
                    // predicate and the list-element predicate use the
                    // base64 form.
                    let bs_field = format!("{field}.BS");
                    let list_field = format!("{field}.L");
                    let bson_val = av_to_bson(&val);
                    let list_elem = doc! { "B": bson_val.clone() };
                    Ok(doc! { "$or": [
                        { &bs_field: &bson_val },
                        { &list_field: &list_elem },
                    ] })
                }
                _ => {
                    // For other types, check membership in L (list)
                    let list_field = format!("{field}.L");
                    let suffix = av_type_suffix(&val);
                    let list_elem = doc! { suffix: av_to_bson(&val) };
                    Ok(doc! { &list_field: &list_elem })
                }
            }
        }

        "attribute_type" => {
            if args.len() != 2 {
                return Err(StorageError::Validation(
                    "attribute_type requires exactly two arguments".to_string(),
                ));
            }
            let field = resolve_path_from_expr(&args[0], maps)?;
            let type_val = resolve_literal(&args[1], maps)?;
            match type_val {
                AttributeValue::S(type_name) => {
                    let typed_field = format!("{field}.{type_name}");
                    Ok(doc! { &typed_field: { "$exists": true } })
                }
                _ => Err(StorageError::Validation(
                    "attribute_type requires a string type argument".to_string(),
                )),
            }
        }

        "size" => {
            // size() is used in comparisons, not standalone.
            // This case handles it if it appears as a standalone function call,
            // which shouldn't happen in well-formed expressions.
            Err(StorageError::Validation(
                "size() cannot be used as a standalone condition".to_string(),
            ))
        }

        _ => Err(StorageError::Validation(format!(
            "Unsupported function in condition: {name}"
        ))),
    }
}

fn compile_between(
    operand: &Expr,
    low: &Expr,
    high: &Expr,
    maps: &ExpressionMaps,
) -> Result<Document, StorageError> {
    let operand_resolved = resolve_value(operand, maps)?;
    let low_resolved = resolve_value(low, maps)?;
    let high_resolved = resolve_value(high, maps)?;

    if let (
        ResolvedValue::Field(path),
        ResolvedValue::Literal(low_val, suffix),
        ResolvedValue::Literal(high_val, _),
    ) = (operand_resolved, low_resolved, high_resolved)
    {
        let typed_path = format!("{path}.{suffix}");
        Ok(doc! { &typed_path: { "$gte": low_val, "$lte": high_val } })
    } else {
        // Fallback: compile as AND of two comparisons
        let gte = build_comparison(operand, CompareOp::Ge, low, maps)?;
        let lte = build_comparison(operand, CompareOp::Le, high, maps)?;
        Ok(doc! { "$and": [gte, lte] })
    }
}

fn compile_in(
    operand: &Expr,
    list: &[Expr],
    maps: &ExpressionMaps,
) -> Result<Document, StorageError> {
    let operand_resolved = resolve_value(operand, maps)?;

    match operand_resolved {
        ResolvedValue::Field(path) => {
            // Collect all values, assuming they are the same type
            if list.is_empty() {
                // Empty IN list never matches — use $and with contradictory conditions
                return Ok(
                    doc! { "$and": [ { "_id": { "$exists": true } }, { "_id": { "$type": "null" } } ] },
                );
            }

            let first_literal = resolve_literal(&list[0], maps)?;
            let suffix = av_type_suffix(&first_literal);
            let typed_path = format!("{path}.{suffix}");

            let values: Vec<Bson> = list
                .iter()
                .map(|expr| {
                    let av = resolve_literal(expr, maps)?;
                    Ok(av_to_bson(&av))
                })
                .collect::<Result<Vec<_>, StorageError>>()?;

            Ok(doc! { &typed_path: { "$in": values } })
        }
        ResolvedValue::Literal(_, _) => {
            // Literal IN list of fields — unusual, compile as OR
            let mut or_clauses = Vec::new();
            for item in list {
                let eq = build_comparison(operand, CompareOp::Eq, item, maps)?;
                or_clauses.push(Bson::Document(eq));
            }
            Ok(doc! { "$or": or_clauses })
        }
    }
}

fn resolve_path_from_expr(expr: &Expr, maps: &ExpressionMaps) -> Result<String, StorageError> {
    match expr {
        Expr::Path(elements) => resolve_path_to_field(elements, maps),
        _ => Err(StorageError::Validation(
            "Expected a path expression".to_string(),
        )),
    }
}

fn resolve_literal(expr: &Expr, maps: &ExpressionMaps) -> Result<AttributeValue, StorageError> {
    match expr {
        Expr::Placeholder(name) => maps
            .resolve_value(name)
            .cloned()
            .map_err(|e| StorageError::Validation(e.to_string())),
        _ => Err(StorageError::Validation(
            "Expected a value placeholder".to_string(),
        )),
    }
}

/// Escape special regex characters in a string.
fn regex_escape(s: &str) -> String {
    let special = [
        '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\',
    ];
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        if special.contains(&c) {
            result.push('\\');
        }
        result.push(c);
    }
    result
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_core::types::AttributeValue;
    use std::collections::HashMap;

    fn make_maps(names: Vec<(&str, &str)>, values: Vec<(&str, AttributeValue)>) -> ExpressionMaps {
        let names_map: HashMap<String, String> = names
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let values_map: HashMap<String, AttributeValue> = values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        ExpressionMaps::new(names_map, values_map)
    }

    #[test]
    fn test_attribute_exists() {
        let maps = make_maps(vec![], vec![]);
        let expr = Expr::Function {
            name: "attribute_exists".to_string(),
            args: vec![Expr::Path(vec![PathElement::Attribute("foo".to_string())])],
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.foo": { "$exists": true } });
    }

    #[test]
    fn test_attribute_not_exists() {
        let maps = make_maps(vec![], vec![]);
        let expr = Expr::Function {
            name: "attribute_not_exists".to_string(),
            args: vec![Expr::Path(vec![PathElement::Attribute("bar".to_string())])],
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.bar": { "$exists": false } });
    }

    #[test]
    fn test_equality_comparison_string() {
        let maps = make_maps(
            vec![],
            vec![(":val", AttributeValue::S("hello".to_string()))],
        );
        let expr = Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute("name".to_string())])),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":val".to_string())),
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.name.S": "hello" });
    }

    #[test]
    fn test_less_than_comparison_number() {
        let maps = make_maps(vec![], vec![(":min", AttributeValue::N("100".to_string()))]);
        let expr = Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute(
                "price".to_string(),
            )])),
            op: CompareOp::Lt,
            right: Box::new(Expr::Placeholder(":min".to_string())),
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.price.N": { "$lt": "100" } });
    }

    #[test]
    fn test_and_condition() {
        let maps = make_maps(
            vec![],
            vec![
                (":v1", AttributeValue::S("active".to_string())),
                (":v2", AttributeValue::N("5".to_string())),
            ],
        );
        let expr = Expr::And(
            Box::new(Expr::Compare {
                left: Box::new(Expr::Path(vec![PathElement::Attribute(
                    "status".to_string(),
                )])),
                op: CompareOp::Eq,
                right: Box::new(Expr::Placeholder(":v1".to_string())),
            }),
            Box::new(Expr::Compare {
                left: Box::new(Expr::Path(vec![PathElement::Attribute(
                    "count".to_string(),
                )])),
                op: CompareOp::Gt,
                right: Box::new(Expr::Placeholder(":v2".to_string())),
            }),
        );
        let filter = condition_to_filter(&expr, &maps).unwrap();
        let expected = doc! {
            "$and": [
                { "item_data.status.S": "active" },
                { "item_data.count.N": { "$gt": "5" } }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_or_condition() {
        let maps = make_maps(
            vec![],
            vec![
                (":a", AttributeValue::S("x".to_string())),
                (":b", AttributeValue::S("y".to_string())),
            ],
        );
        let expr = Expr::Or(
            Box::new(Expr::Compare {
                left: Box::new(Expr::Path(vec![PathElement::Attribute("f".to_string())])),
                op: CompareOp::Eq,
                right: Box::new(Expr::Placeholder(":a".to_string())),
            }),
            Box::new(Expr::Compare {
                left: Box::new(Expr::Path(vec![PathElement::Attribute("f".to_string())])),
                op: CompareOp::Eq,
                right: Box::new(Expr::Placeholder(":b".to_string())),
            }),
        );
        let filter = condition_to_filter(&expr, &maps).unwrap();
        let expected = doc! {
            "$or": [
                { "item_data.f.S": "x" },
                { "item_data.f.S": "y" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_not_condition() {
        let maps = make_maps(vec![], vec![]);
        let expr = Expr::Not(Box::new(Expr::Function {
            name: "attribute_exists".to_string(),
            args: vec![Expr::Path(vec![PathElement::Attribute(
                "deleted".to_string(),
            )])],
        }));
        let filter = condition_to_filter(&expr, &maps).unwrap();
        let expected = doc! {
            "$nor": [{ "item_data.deleted": { "$exists": true } }]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_begins_with() {
        let maps = make_maps(
            vec![],
            vec![(":prefix", AttributeValue::S("user#".to_string()))],
        );
        let expr = Expr::Function {
            name: "begins_with".to_string(),
            args: vec![
                Expr::Path(vec![PathElement::Attribute("sk".to_string())]),
                Expr::Placeholder(":prefix".to_string()),
            ],
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.sk.S": { "$regex": "^user#" } });
    }

    #[test]
    fn test_between() {
        let maps = make_maps(
            vec![],
            vec![
                (":lo", AttributeValue::N("10".to_string())),
                (":hi", AttributeValue::N("20".to_string())),
            ],
        );
        let expr = Expr::Between {
            operand: Box::new(Expr::Path(vec![PathElement::Attribute("age".to_string())])),
            low: Box::new(Expr::Placeholder(":lo".to_string())),
            high: Box::new(Expr::Placeholder(":hi".to_string())),
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(
            filter,
            doc! { "item_data.age.N": { "$gte": "10", "$lte": "20" } }
        );
    }

    #[test]
    fn test_in_condition() {
        let maps = make_maps(
            vec![],
            vec![
                (":v1", AttributeValue::S("a".to_string())),
                (":v2", AttributeValue::S("b".to_string())),
                (":v3", AttributeValue::S("c".to_string())),
            ],
        );
        let expr = Expr::In {
            operand: Box::new(Expr::Path(vec![PathElement::Attribute("x".to_string())])),
            list: vec![
                Expr::Placeholder(":v1".to_string()),
                Expr::Placeholder(":v2".to_string()),
                Expr::Placeholder(":v3".to_string()),
            ],
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.x.S": { "$in": ["a", "b", "c"] } });
    }

    #[test]
    fn test_name_ref_resolution() {
        let maps = make_maps(
            vec![("n", "status")],
            vec![(":v", AttributeValue::S("active".to_string()))],
        );
        let expr = Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute("#n".to_string())])),
            op: CompareOp::Eq,
            right: Box::new(Expr::Placeholder(":v".to_string())),
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.status.S": "active" });
    }

    #[test]
    fn test_ne_comparison() {
        let maps = make_maps(
            vec![],
            vec![(":v", AttributeValue::S("deleted".to_string()))],
        );
        let expr = Expr::Compare {
            left: Box::new(Expr::Path(vec![PathElement::Attribute(
                "status".to_string(),
            )])),
            op: CompareOp::Ne,
            right: Box::new(Expr::Placeholder(":v".to_string())),
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.status.S": { "$ne": "deleted" } });
    }

    #[test]
    fn test_regex_escape() {
        assert_eq!(regex_escape("user.name"), "user\\.name");
        assert_eq!(regex_escape("a+b"), "a\\+b");
        assert_eq!(regex_escape("normal"), "normal");
    }

    #[test]
    fn test_attribute_type() {
        let maps = make_maps(vec![], vec![(":t", AttributeValue::S("S".to_string()))]);
        let expr = Expr::Function {
            name: "attribute_type".to_string(),
            args: vec![
                Expr::Path(vec![PathElement::Attribute("field".to_string())]),
                Expr::Placeholder(":t".to_string()),
            ],
        };
        let filter = condition_to_filter(&expr, &maps).unwrap();
        assert_eq!(filter, doc! { "item_data.field.S": { "$exists": true } });
    }
}
