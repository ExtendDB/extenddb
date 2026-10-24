// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `KeyConditionExpression` with no equality clause on the HASH attribute.
//!
//! Which message the service returns for `pk > :x` depends on the key schema
//! of the table or index being queried, and the parser does not know it. The
//! parser therefore accepts the clauses and records them in a
//! [`KeyCondition`] without a partition key condition (`pk_path` is empty).
//! The engine calls [`KeyCondition::refuse_without_hash_equality`] once the
//! key schema is known; that call is the only place the wording is chosen.
//! Every such condition is refused, so no storage backend receives one.
//!
//! Two shapes reach the refusal:
//!
//! - one or two clauses, none an equality (`pk > :x`, `sk > :x AND zz > :y`).
//!   `sk_condition` is the first clause. A second clause is kept as its
//!   operands in `extra_sk_conditions`, one `(path, operand)` entry per
//!   operand (two for `BETWEEN`), so that the references of both clauses are
//!   counted as used.
//! - an equality on another attribute plus a non-equality clause on the HASH
//!   attribute of a single-HASH schema (`sk = :x AND pk > :y`). The parser
//!   classifies the pair as it does any other, equality first, and
//!   [`KeyCondition::lacks_hash_equality`] spots it once the schema is known.
//!
//! The wordings, measured on the service on 2026-10-21 (table with HASH `pk`
//! and RANGE `sk`, index with HASH `g` and RANGE `s`, multi-attribute index
//! with HASH `h` and RANGE `a`, `b`, `c`, `d`) and on 2026-10-23 (every other
//! row, recorded in the issue's fold files):
//!
//! | clauses | key schema | message |
//! |---|---|---|
//! | a non-equality clause names the HASH attribute (`pk > :x`, `begins_with(pk, :x)`, `pk BETWEEN :x AND :y`, `sk = :x AND pk > :y`, `pk > :x AND zz > :y`) | one HASH attribute | `Query key condition not supported` |
//! | no clause names the HASH attribute (`sk > :x`, `z > :x`, `sk > :x AND zz > :y`) | one HASH attribute | `Query condition missed key schema element: pk` |
//! | any clauses (`h > :h`, `a > :x`, `z > :x`, `h1 > :x`, `r > :x`) | several key attributes | `HASH key attributes h must have equality conditions specified in the query` (every HASH attribute, comma separated) |
//! | two non-equality clauses on one attribute (`pk > :x AND pk < :y`) | any | `Invalid KeyConditionExpression: KeyConditionExpressions must only contain one condition per key` |
//!
//! The service reports an undefined `#name` or `:value` first, with the
//! `Invalid KeyConditionExpression: ` prefix; see
//! [`KeyCondition::validate_references`]. It reports an unused value, an
//! invalid `FilterExpression` or `ProjectionExpression` and an invalid
//! `Select` ahead of the wording, and an `ExclusiveStartKey` or a key value
//! size after it, so the engine calls the refusal between those.

use std::collections::HashMap;

use super::{CompareOp, Expr, KeyCondition, PathElement, SortKeyCondition};
use crate::error::DynamoDbError;
use crate::types::{KeySchemaElement, hash_key_elements, is_multipart_key_schema};

#[cfg(test)]
mod tests;

const NOT_SUPPORTED: &str = "Query key condition not supported";
const ONE_CONDITION_PER_KEY: &str = "Invalid KeyConditionExpression: KeyConditionExpressions \
     must only contain one condition per key";

impl KeyCondition {
    /// The condition for one or two clauses, none of them an equality.
    ///
    /// `pk_path` is empty because no partition key condition was given.
    /// `pk_value` repeats the first clause's first operand so the field holds
    /// a well-formed expression; it carries no meaning while `pk_path` is
    /// empty.
    pub(super) fn without_equality(
        first: SortKeyCondition,
        second: Option<SortKeyCondition>,
    ) -> Self {
        let pk_value = first_operand(&first).clone();
        let extra_sk_conditions = second.map_or_else(Vec::new, |clause| operands(&clause));
        Self {
            pk_path: Vec::new(),
            pk_value,
            extra_pk_conditions: Vec::new(),
            sk_condition: Some(first),
            extra_sk_conditions,
        }
    }

    /// Whether the key schema has no equality clause on its HASH attribute
    /// in this condition, in a shape [`Self::refuse_without_hash_equality`]
    /// refuses.
    ///
    /// True for a condition with no equality clause at all. On a schema with
    /// one HASH attribute it is also true for two clauses where the equality
    /// names another attribute and the other clause names the HASH attribute
    /// with a non-equality (`sk = :x AND pk > :y`). An equality on the HASH
    /// attribute makes it false, so `pk = :x AND pk > :y` keeps its existing
    /// handling.
    #[must_use]
    pub fn lacks_hash_equality(
        &self,
        key_schema: &[KeySchemaElement],
        names: &HashMap<String, String>,
    ) -> bool {
        if self.pk_path.is_empty() {
            return self.sk_condition.is_some();
        }
        if is_multipart_key_schema(key_schema)
            || !self.extra_pk_conditions.is_empty()
            || !self.extra_sk_conditions.is_empty()
        {
            return false;
        }
        let Some(hash_attr) = single_hash_attr(key_schema) else {
            return false;
        };
        let names_hash = |path: &[PathElement]| attr_of(path, names).as_deref() == Some(hash_attr);
        let Some(sk) = &self.sk_condition else {
            return false;
        };
        let sk_is_equality = matches!(
            sk,
            SortKeyCondition::Compare {
                op: CompareOp::Eq,
                ..
            }
        );
        // `pk_path` holds an equality whenever it is set. A second equality
        // on the HASH attribute (`sk = :x AND pk = :y`) also satisfies it.
        if names_hash(&self.pk_path) {
            return false;
        }
        !sk_is_equality && names_hash(clause_path(sk))
    }

    /// Refuse a key condition that has no equality clause on the HASH
    /// attribute, choosing the message by the key schema being queried.
    ///
    /// Returns `Ok(())` for every other key condition. Call this after the
    /// expression validations (unused names and values, `FilterExpression`,
    /// `ProjectionExpression`, `Select`) and the references check, and before
    /// the storage read.
    ///
    /// - Two clauses on one attribute: `Invalid KeyConditionExpression:
    ///   KeyConditionExpressions must only contain one condition per key`.
    /// - A schema with several HASH or several RANGE attributes: the service
    ///   names every HASH attribute, because none has an equality condition.
    ///   Measured on 2026-10-23 on an index with HASH `h` and RANGE `a`, `b`,
    ///   `c`, `d` (`h > :x`, `a > :x`, `d > :x`, `z > :x`, `begins_with(a, :x)`,
    ///   `a BETWEEN :x AND :y`, and `h > :n` with an N value) and on an index
    ///   with HASH `h1`, `h2` and RANGE `r` (`h1`, `h2`, `r`, `z` and a
    ///   `#name` for `h1`).
    /// - A schema with one HASH attribute and at most one RANGE attribute: if
    ///   a clause names the HASH attribute (`<`, `<=`, `>`, `>=`,
    ///   `begins_with`, `BETWEEN`), `Query key condition not supported`;
    ///   otherwise `Query condition missed key schema element: <HASH
    ///   attribute>`. Measured on a table, a hash-only table, a GSI and an
    ///   LSI.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` for the conditions above, and when a
    /// `#name` on a clause is not defined in `names`.
    pub fn refuse_without_hash_equality(
        &self,
        key_schema: &[KeySchemaElement],
        names: &HashMap<String, String>,
    ) -> Result<(), DynamoDbError> {
        if !self.lacks_hash_equality(key_schema, names) {
            return Ok(());
        }
        let attrs = self
            .clause_paths()
            .into_iter()
            .map(|path| resolve_clause_attr(path, names))
            .collect::<Result<Vec<_>, _>>()?;

        if self.pk_path.is_empty() && attrs.len() == 2 && attrs[0] == attrs[1] {
            return Err(DynamoDbError::ValidationException(
                ONE_CONDITION_PER_KEY.to_owned(),
            ));
        }

        let hash_attrs: Vec<&str> = hash_key_elements(key_schema)
            .iter()
            .map(|ks| ks.attribute_name.as_str())
            .collect();

        if is_multipart_key_schema(key_schema) {
            return Err(DynamoDbError::ValidationException(format!(
                "HASH key attributes {} must have equality conditions specified in the query",
                hash_attrs.join(", ")
            )));
        }

        let pk_attr = hash_attrs.first().copied().unwrap_or_default();
        let message = if attrs.iter().any(|attr| attr == pk_attr) {
            NOT_SUPPORTED.to_owned()
        } else {
            format!("Query condition missed key schema element: {pk_attr}")
        };
        Err(DynamoDbError::ValidationException(message))
    }

    /// The path of each clause, in classification order: the equality, the
    /// `sk_condition` and, for two clauses with no equality, the second one.
    fn clause_paths(&self) -> Vec<&[PathElement]> {
        let mut paths: Vec<&[PathElement]> = Vec::new();
        if !self.pk_path.is_empty() {
            paths.push(&self.pk_path);
        }
        if let Some(sk) = &self.sk_condition {
            paths.push(clause_path(sk));
        }
        if let Some((path, _)) = self.extra_sk_conditions.first() {
            paths.push(path);
        }
        paths
    }
}

/// The only HASH attribute of a schema, or `None` for zero or several.
fn single_hash_attr(key_schema: &[KeySchemaElement]) -> Option<&str> {
    match hash_key_elements(key_schema).as_slice() {
        [only] => Some(only.attribute_name.as_str()),
        _ => None,
    }
}

/// The top-level attribute a path names, with `#name` resolved; `None` when
/// the name is not defined or the path is not an attribute.
fn attr_of(path: &[PathElement], names: &HashMap<String, String>) -> Option<String> {
    match path.first()? {
        PathElement::Attribute(name) => match name.strip_prefix('#') {
            Some(ref_name) => names.get(ref_name).cloned(),
            None => Some(name.clone()),
        },
        PathElement::Index(_) => None,
    }
}

/// The top-level attribute a clause names, with `#name` resolved.
///
/// An undefined `#name` is refused with the `KeyConditionExpression` form of
/// the service's sentence.
fn resolve_clause_attr(
    path: &[PathElement],
    names: &HashMap<String, String>,
) -> Result<String, DynamoDbError> {
    match path.first() {
        Some(PathElement::Attribute(name)) => match name.strip_prefix('#') {
            Some(ref_name) => names.get(ref_name).cloned().ok_or_else(|| {
                DynamoDbError::ValidationException(format!(
                    "Invalid KeyConditionExpression: An expression attribute name used in \
                     the document path is not defined; attribute name: #{ref_name}"
                ))
            }),
            None => Ok(name.clone()),
        },
        _ => Err(DynamoDbError::ValidationException(
            "Invalid KeyConditionExpression: Invalid key condition path".to_owned(),
        )),
    }
}

/// The attribute path a clause applies to.
fn clause_path(clause: &SortKeyCondition) -> &[PathElement] {
    match clause {
        SortKeyCondition::Compare { path, .. }
        | SortKeyCondition::Between { path, .. }
        | SortKeyCondition::BeginsWith { path, .. } => path,
    }
}

/// The first value operand of a clause.
fn first_operand(clause: &SortKeyCondition) -> &Expr {
    match clause {
        SortKeyCondition::Compare { value, .. } => value,
        SortKeyCondition::Between { low, .. } => low,
        SortKeyCondition::BeginsWith { prefix, .. } => prefix,
    }
}

/// Every value operand of a clause as a `(path, operand)` entry.
fn operands(clause: &SortKeyCondition) -> Vec<(Vec<PathElement>, Expr)> {
    match clause {
        SortKeyCondition::Compare { path, value, .. } => vec![(path.clone(), value.clone())],
        SortKeyCondition::Between { path, low, high } => {
            vec![(path.clone(), low.clone()), (path.clone(), high.clone())]
        }
        SortKeyCondition::BeginsWith { path, prefix } => vec![(path.clone(), prefix.clone())],
    }
}
