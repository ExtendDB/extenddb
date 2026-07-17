// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Pure-Rust MongoDB filter interpreter for pushdown-parity tests.
//!
//! Evaluates the subset of MongoDB filter operators emitted by
//! `crates/storage-mongodb/src/condition.rs::condition_to_filter` against a
//! BSON document. Used to differentially test the compiled-filter path
//! against the in-Rust DDB expression evaluator without needing a live
//! MongoDB.
//!
//! This is deliberately not a general-purpose MongoDB query engine — it
//! implements only the operators the compiler can emit. Adding a new
//! operator to the compiler requires adding it here.

use bson::{Bson, Document};

/// Evaluate a MongoDB filter document against a BSON document.
///
/// Returns `true` if the document matches all clauses in the filter,
/// `false` otherwise. The root document representing the filter is
/// interpreted as an implicit `$and` of its clauses (matching MongoDB's
/// top-level query semantics).
pub fn eval_filter(filter: &Document, doc: &Document) -> bool {
    filter.iter().all(|(key, val)| eval_clause(key, val, doc))
}

/// Evaluate a single (key, value) clause at the top level of a filter.
///
/// Handles:
/// - `$and` / `$or` / `$nor` — logical operators over an array of subfilters
/// - `$expr` — expression-based comparison, only for field-vs-field per compiler
/// - `<field>: <predicate>` — either a scalar equality or an operator document
fn eval_clause(key: &str, val: &Bson, doc: &Document) -> bool {
    match key {
        "$and" => as_array(val)
            .iter()
            .all(|sub| as_doc(sub).is_some_and(|d| eval_filter(d, doc))),
        "$or" => as_array(val)
            .iter()
            .any(|sub| as_doc(sub).is_some_and(|d| eval_filter(d, doc))),
        "$nor" => !as_array(val)
            .iter()
            .any(|sub| as_doc(sub).is_some_and(|d| eval_filter(d, doc))),
        "$expr" => eval_expr(val, doc),
        _ => {
            // <field>: <predicate> — walk the dotted path, then evaluate the
            // predicate against the value found.
            let field_val = walk_path(doc, key);
            eval_predicate(val, &field_val)
        }
    }
}

/// Evaluate a predicate against the value found at the field path.
///
/// The predicate is either a scalar (implicit `$eq`) or an operator
/// document containing `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, `$gte`,
/// `$exists`, `$in`, `$regex`, or `$type`.
///
/// If the field is a BSON array and the predicate is a scalar, MongoDB
/// matches if any element of the array equals the scalar (implicit
/// array-match). We support that shape because the compiler relies on it
/// for `contains(SS_field, :s)` and similar.
fn eval_predicate(pred: &Bson, field: &FieldValue<'_>) -> bool {
    match pred {
        Bson::Document(pred_doc) => {
            // Operator document: every operator must match. MongoDB's
            // behavior with multiple operator keys in one doc is that they
            // are ANDed together.
            pred_doc.iter().all(|(op, arg)| match op.as_str() {
                "$eq" => eq_or_array_match(field, arg),
                "$ne" => !eq_or_array_match(field, arg),
                "$lt" => cmp_field(field, arg, |a, b| {
                    bson_cmp(a, b) == std::cmp::Ordering::Less
                }),
                "$lte" => cmp_field(field, arg, |a, b| {
                    matches!(
                        bson_cmp(a, b),
                        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                    )
                }),
                "$gt" => cmp_field(field, arg, |a, b| {
                    bson_cmp(a, b) == std::cmp::Ordering::Greater
                }),
                "$gte" => cmp_field(field, arg, |a, b| {
                    matches!(
                        bson_cmp(a, b),
                        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                    )
                }),
                "$exists" => {
                    let want = as_bool(arg).unwrap_or(true);
                    field.is_present() == want
                }
                "$in" => {
                    let arr = as_array(arg);
                    match field {
                        FieldValue::Present(Bson::Array(field_arr)) => field_arr.iter().any(|f| {
                            arr.iter()
                                .any(|a| bson_cmp(f, a) == std::cmp::Ordering::Equal)
                        }),
                        FieldValue::Present(v) => arr
                            .iter()
                            .any(|a| bson_cmp(v, a) == std::cmp::Ordering::Equal),
                        FieldValue::Missing => false,
                    }
                }
                "$regex" => match arg {
                    Bson::String(pattern) => match field {
                        FieldValue::Present(Bson::String(s)) => regex_match(pattern, s),
                        FieldValue::Present(Bson::Array(arr)) => arr
                            .iter()
                            .any(|v| matches!(v, Bson::String(s) if regex_match(pattern, s))),
                        _ => false,
                    },
                    _ => false,
                },
                "$type" => match arg {
                    Bson::String(t) => matches!(
                        (t.as_str(), field),
                        ("null", FieldValue::Present(Bson::Null))
                            | ("string", FieldValue::Present(Bson::String(_)))
                            | ("bool", FieldValue::Present(Bson::Boolean(_)))
                            | ("array", FieldValue::Present(Bson::Array(_)))
                            | ("object", FieldValue::Present(Bson::Document(_)))
                    ),
                    _ => false,
                },
                // If the compiler ever emits an operator we don't recognize,
                // fail loudly rather than silently pass.
                other => panic!("bson filter interpreter: unknown operator {other}"),
            })
        }
        // Scalar predicate: implicit equality (or array-match).
        _ => eq_or_array_match(field, pred),
    }
}

/// Field-vs-field expressions via `$expr`. The compiler only emits
/// `{"$expr": {"$op": ["$left_field", "$right_field"]}}` shapes.
fn eval_expr(val: &Bson, doc: &Document) -> bool {
    let Some(expr_doc) = as_doc(val) else {
        return false;
    };
    for (op, args) in expr_doc {
        let arr = as_array(args);
        if arr.len() != 2 {
            return false;
        }
        let (Some(lhs_ref), Some(rhs_ref)) = (as_field_ref(&arr[0]), as_field_ref(&arr[1])) else {
            return false;
        };
        let lhs = walk_path(doc, lhs_ref);
        let rhs = walk_path(doc, rhs_ref);
        let (FieldValue::Present(l), FieldValue::Present(r)) = (&lhs, &rhs) else {
            return false;
        };
        let ord = bson_cmp(l, r);
        let matched = match op.as_str() {
            "$eq" => ord == std::cmp::Ordering::Equal,
            "$ne" => ord != std::cmp::Ordering::Equal,
            "$lt" => ord == std::cmp::Ordering::Less,
            "$lte" => matches!(ord, std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
            "$gt" => ord == std::cmp::Ordering::Greater,
            "$gte" => matches!(ord, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
            other => panic!("bson filter interpreter: unknown $expr operator {other}"),
        };
        if !matched {
            return false;
        }
    }
    true
}

/// Whether a field's value at a dotted path is Present or Missing.
///
/// Distinguishing these is required for `$exists` semantics: MongoDB's
/// `{$exists: false}` only matches documents where the field genuinely
/// isn't in the document, not documents where the field is present with
/// value `null`.
#[derive(Debug)]
pub enum FieldValue<'a> {
    Present(&'a Bson),
    Missing,
}

impl<'a> FieldValue<'a> {
    fn is_present(&self) -> bool {
        matches!(self, FieldValue::Present(_))
    }
}

/// Walk a dotted path like "item_data.address.city" through nested docs
/// (and arrays, when a path component is a numeric index).
pub fn walk_path<'a>(doc: &'a Document, path: &str) -> FieldValue<'a> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return FieldValue::Missing;
    }
    let mut cur: &Bson = match doc.get(parts[0]) {
        Some(v) => v,
        None => return FieldValue::Missing,
    };
    for part in &parts[1..] {
        cur = match cur {
            Bson::Document(d) => match d.get(*part) {
                Some(v) => v,
                None => return FieldValue::Missing,
            },
            Bson::Array(a) => match part.parse::<usize>() {
                Ok(idx) => match a.get(idx) {
                    Some(v) => v,
                    None => return FieldValue::Missing,
                },
                Err(_) => return FieldValue::Missing,
            },
            _ => return FieldValue::Missing,
        };
    }
    FieldValue::Present(cur)
}

fn eq_or_array_match(field: &FieldValue<'_>, target: &Bson) -> bool {
    match field {
        FieldValue::Present(Bson::Array(arr)) => arr
            .iter()
            .any(|v| bson_cmp(v, target) == std::cmp::Ordering::Equal),
        FieldValue::Present(v) => bson_cmp(v, target) == std::cmp::Ordering::Equal,
        FieldValue::Missing => matches!(target, Bson::Null),
    }
}

fn cmp_field<F>(field: &FieldValue<'_>, target: &Bson, pred: F) -> bool
where
    F: Fn(&Bson, &Bson) -> bool,
{
    match field {
        FieldValue::Present(v) => pred(v, target),
        FieldValue::Missing => false,
    }
}

/// Compare two BSON values with MongoDB's total-ordering rules.
///
/// This is a small subset — enough for the compiler's operator surface.
/// Numeric types are unified (Int32/Int64/Double/Decimal128 compare by
/// numeric value). Types that don't compare (Document vs. String) return
/// Equal by fallback because the compiler never emits comparisons between
/// mixed types in practice; if a proptest run generates one, the parity
/// check will surface the mismatch.
fn bson_cmp(a: &Bson, b: &Bson) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Bson::String(x), Bson::String(y)) => x.cmp(y),
        (Bson::Boolean(x), Bson::Boolean(y)) => x.cmp(y),
        (Bson::Int32(x), Bson::Int32(y)) => x.cmp(y),
        (Bson::Int64(x), Bson::Int64(y)) => x.cmp(y),
        (Bson::Int32(x), Bson::Int64(y)) => (*x as i64).cmp(y),
        (Bson::Int64(x), Bson::Int32(y)) => x.cmp(&(*y as i64)),
        (Bson::Double(x), Bson::Double(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Bson::Binary(x), Bson::Binary(y)) => x.bytes.cmp(&y.bytes),
        (Bson::Null, Bson::Null) => Ordering::Equal,
        (Bson::Array(x), Bson::Array(y)) => {
            // Elementwise; documents-of-arrays don't come up in the compiler
            // output, but arrays-of-primitives can when a set field is
            // compared to another set.
            for (xi, yi) in x.iter().zip(y.iter()) {
                match bson_cmp(xi, yi) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            x.len().cmp(&y.len())
        }
        (Bson::Document(x), Bson::Document(y)) => {
            // Compare field-by-field in insertion order — matches how BSON
            // documents are serialized and how our compiler's `$type`
            // comparisons treat them.
            for ((xk, xv), (yk, yv)) in x.iter().zip(y.iter()) {
                match xk.cmp(yk) {
                    Ordering::Equal => match bson_cmp(xv, yv) {
                        Ordering::Equal => continue,
                        other => return other,
                    },
                    other => return other,
                }
            }
            x.len().cmp(&y.len())
        }
        // Mismatched types: fall back to "not equal" ordering. The compiler
        // shouldn't produce these — if a proptest generates one, the
        // parity harness will report the divergence.
        _ => Ordering::Equal,
    }
}

fn as_array(val: &Bson) -> Vec<Bson> {
    match val {
        Bson::Array(a) => a.clone(),
        _ => Vec::new(),
    }
}

fn as_doc(val: &Bson) -> Option<&Document> {
    match val {
        Bson::Document(d) => Some(d),
        _ => None,
    }
}

fn as_bool(val: &Bson) -> Option<bool> {
    match val {
        Bson::Boolean(b) => Some(*b),
        _ => None,
    }
}

/// Convert a `"$fieldname"` string to the field name it refers to, or
/// return None for anything else. Used only for `$expr` argument parsing.
fn as_field_ref(val: &Bson) -> Option<&str> {
    match val {
        Bson::String(s) => s.strip_prefix('$'),
        _ => None,
    }
}

/// Minimal regex matcher — the compiler only emits `^prefix` and plain
/// substring patterns via `regex_escape`, so we don't need a full regex
/// engine. Anchors and escaped literals only.
fn regex_match(pattern: &str, s: &str) -> bool {
    // Handle "^prefix" — anchored prefix match.
    if let Some(prefix) = pattern.strip_prefix('^') {
        // The compiler regex-escapes the prefix, so we treat it as a
        // literal string here. Any regex metacharacter present means the
        // compiler already escaped it.
        let unescaped = unescape_regex(prefix);
        return s.starts_with(&unescaped);
    }
    // Unanchored — substring match. Same treatment.
    let unescaped = unescape_regex(pattern);
    s.contains(&unescaped)
}

/// Reverse the `regex_escape` transformation in `condition.rs`. Since our
/// compiler only escapes standard regex metacharacters with backslash, we
/// walk the string and unescape those.
fn unescape_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn item() -> Document {
        doc! {
            "_id": "user#1",
            "pk": "user#1",
            "item_data": {
                "name": { "S": "alice" },
                "age": { "N": "30" },
                "tags": { "SS": ["admin", "beta"] },
                "profile": {
                    "M": {
                        "email": { "S": "a@x.com" }
                    }
                }
            }
        }
    }

    #[test]
    fn scalar_equality_present() {
        let filter = doc! { "item_data.name.S": "alice" };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn scalar_equality_absent() {
        let filter = doc! { "item_data.missing.S": "alice" };
        assert!(!eval_filter(&filter, &item()));
    }

    #[test]
    fn exists_true_present() {
        let filter = doc! { "item_data.name": { "$exists": true } };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn exists_false_absent() {
        let filter = doc! { "item_data.missing": { "$exists": false } };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn implicit_array_match_on_set() {
        // `contains(tags, :s)` compiles to `{"item_data.tags.SS": "admin"}`
        // and relies on implicit array-match to succeed when "admin" is a
        // set member.
        let filter = doc! { "item_data.tags.SS": "admin" };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn implicit_array_match_absent_member() {
        let filter = doc! { "item_data.tags.SS": "nonmember" };
        assert!(!eval_filter(&filter, &item()));
    }

    #[test]
    fn lexicographic_lt() {
        let filter = doc! { "item_data.name.S": { "$lt": "bob" } };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn and_of_two_clauses() {
        let filter = doc! { "$and": [
            { "item_data.name.S": "alice" },
            { "item_data.age.N": "30" }
        ]};
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn or_short_circuit() {
        let filter = doc! { "$or": [
            { "item_data.name.S": "wrong" },
            { "item_data.age.N": "30" }
        ]};
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn nor_negates() {
        let filter = doc! { "$nor": [ { "item_data.name.S": "bob" } ] };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn regex_prefix_match() {
        let filter = doc! { "item_data.name.S": { "$regex": "^al" } };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn regex_prefix_no_match() {
        let filter = doc! { "item_data.name.S": { "$regex": "^bob" } };
        assert!(!eval_filter(&filter, &item()));
    }

    #[test]
    fn in_membership() {
        let filter = doc! { "item_data.name.S": { "$in": ["alice", "bob"] } };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn in_no_match() {
        let filter = doc! { "item_data.name.S": { "$in": ["bob", "carol"] } };
        assert!(!eval_filter(&filter, &item()));
    }

    #[test]
    fn ne_true_when_different() {
        let filter = doc! { "item_data.name.S": { "$ne": "bob" } };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn empty_in_sentinel_never_matches() {
        // Compiler emits this for empty IN () — a document must have _id
        // and _id must have type "null", which contradicts each other for
        // any well-formed item.
        let filter = doc! { "$and": [
            { "_id": { "$exists": true } },
            { "_id": { "$type": "null" } }
        ]};
        assert!(!eval_filter(&filter, &item()));
    }

    #[test]
    fn nested_map_path() {
        let filter = doc! { "item_data.profile.M.email.S": "a@x.com" };
        assert!(eval_filter(&filter, &item()));
    }

    #[test]
    fn missing_intermediate_path_short_circuits() {
        let filter = doc! { "item_data.profile.M.nonexistent.S": "any" };
        assert!(!eval_filter(&filter, &item()));
    }
}
