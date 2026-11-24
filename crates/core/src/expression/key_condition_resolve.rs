// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key schema resolution for a parsed [`KeyCondition`]: correcting the
//! partition and sort key assignment once the table's key schema is known.

use std::collections::HashMap;

use super::ast::{CompareOp, Expr, PathElement};
use super::key_condition::{KeyCondition, SortKeyCondition};
use crate::error::DynamoDbError;
use crate::types::{
    KeySchemaElement, KeyType, hash_key_elements, is_multipart_key_schema, range_key_elements,
};

impl KeyCondition {
    /// Correct PK/SK assignment using the table's key schema.
    ///
    /// For single-attribute keys: when both the PK and SK conditions are equality
    /// comparisons, the parser cannot distinguish which is which without knowing
    /// the key schema. This method resolves `#name` references and swaps PK/SK
    /// if the current PK path actually refers to the sort key attribute.
    ///
    /// For multi-part keys: all equality conditions are initially parsed as
    /// PK + SK pairs. This method reclassifies them based on the key schema,
    /// moving HASH attribute conditions to `extra_pk_conditions` and keeping
    /// the RANGE condition as `sk_condition`.
    ///
    /// Call this after parsing and before passing to the storage layer.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` if a `#name` reference cannot be resolved.
    pub fn resolve_pk_sk(
        &mut self,
        pk_attr: &str,
        names: &HashMap<String, String>,
    ) -> Result<(), DynamoDbError> {
        // Only ambiguous when SK is an Eq comparison (both clauses were Eq).
        let sk_is_eq = matches!(
            self.sk_condition,
            Some(SortKeyCondition::Compare {
                op: CompareOp::Eq,
                ..
            })
        );
        if !sk_is_eq {
            return Ok(());
        }

        // Resolve the current PK path's top-level attribute name.
        let pk_name = match self.pk_path.first() {
            Some(PathElement::Attribute(name)) => {
                if let Some(ref_name) = name.strip_prefix('#') {
                    names.get(ref_name).map(String::as_str).ok_or_else(|| {
                        DynamoDbError::ValidationException(format!(
                            "Invalid KeyConditionExpression: An expression attribute name used in \
                             the document path is not defined; attribute name: #{ref_name}"
                        ))
                    })?
                } else {
                    name.as_str()
                }
            }
            _ => return Ok(()),
        };

        // If the current PK path already matches the actual PK attribute, no swap needed.
        if pk_name == pk_attr {
            return Ok(());
        }

        // Swap: current PK is actually the SK, and the SK condition holds the real PK.
        if let Some(SortKeyCondition::Compare {
            path: sk_path,
            op: CompareOp::Eq,
            value: sk_value,
        }) = self.sk_condition.take()
        {
            let old_pk_path = std::mem::replace(&mut self.pk_path, sk_path);
            let old_pk_value = std::mem::replace(&mut self.pk_value, sk_value);
            self.sk_condition = Some(SortKeyCondition::Compare {
                path: old_pk_path,
                op: CompareOp::Eq,
                value: old_pk_value,
            });
        }

        Ok(())
    }

    /// Classify and validate the conditions of a multi-attribute key schema.
    ///
    /// A multi-attribute key schema has more than one HASH attribute, more
    /// than one RANGE attribute, or both. `hash_attrs` and `range_attrs` are
    /// the index's HASH and RANGE attribute names in key-schema order. The
    /// parser cannot tell HASH from RANGE equalities, and clause order in the
    /// expression is free, so every clause is assigned here by name.
    ///
    /// The rules the service applies:
    ///
    /// - Each key attribute may carry one condition; a second one is refused
    ///   as `KeyConditionExpressions must only contain one condition per key`.
    /// - Every HASH attribute needs an equality condition. Every one without
    ///   it is named, in key-schema order, in `HASH key attributes ... must
    ///   have equality conditions specified in the query`.
    /// - Conditions on RANGE attributes must cover a leading run of them in
    ///   key-schema order. Every RANGE attribute before the last one that has
    ///   a condition needs an equality; only that last one may use `<`, `<=`,
    ///   `>`, `>=`, `BETWEEN` or `begins_with`. `h = :h AND b = :b` on a schema
    ///   whose RANGE attributes are `a, b` is refused, and so is
    ///   `h = :h AND a = :a AND b > :b AND c = :c`.
    ///
    /// On return the HASH conditions are in key-schema order (`pk_path` and
    /// `extra_pk_conditions`, matching the composite partition key the write
    /// path builds). When a RANGE attribute carries a non-equality condition,
    /// that condition is `sk_condition` and the equalities before it are
    /// `extra_sk_conditions`; otherwise `sk_condition` is the equality on the
    /// first RANGE attribute and the rest follow in `extra_sk_conditions`.
    /// Either way `extra_sk_conditions` is in key-schema order. A storage
    /// backend maps each condition to its column by attribute name.
    ///
    /// Call this instead of [`KeyCondition::resolve_pk_sk`] when the key
    /// schema is multi-attribute. A schema with one HASH and at most one
    /// RANGE attribute is left unchanged.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` when a `#name` reference cannot be
    /// resolved, a HASH attribute has no equality condition, the RANGE
    /// conditions skip an attribute or put a non-equality before the last
    /// one, or a non-equality names a HASH attribute, or a non-key attribute
    /// while a RANGE attribute also has a condition.
    pub fn resolve_multipart(
        &mut self,
        hash_attrs: &[&str],
        range_attrs: &[&str],
        names: &HashMap<String, String>,
    ) -> Result<(), DynamoDbError> {
        self.resolve_multipart_with(hash_attrs, range_attrs, names, |_, _| Ok(()))
    }

    /// [`Self::resolve_multipart`] with a check on every clause's value,
    /// called with the clause's path and value after the structural rules
    /// (one condition per key, condition count, HASH coverage, RANGE gap) and
    /// before the non-key rule. That is where the service reports a value of
    /// the wrong type (measured 2026-10-21: a wrong-typed value loses to a
    /// duplicate, a count overflow, a missing HASH attribute and a RANGE gap,
    /// and wins over a condition on a non-key attribute).
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_multipart`], plus whatever `value_check` returns.
    pub fn resolve_multipart_with<F>(
        &mut self,
        hash_attrs: &[&str],
        range_attrs: &[&str],
        names: &HashMap<String, String>,
        mut value_check: F,
    ) -> Result<(), DynamoDbError>
    where
        F: FnMut(&[PathElement], &Expr) -> Result<(), DynamoDbError>,
    {
        if hash_attrs.len() <= 1 && range_attrs.len() <= 1 {
            return Ok(());
        }

        // Every equality clause, whichever field the parser put it in.
        let mut all_eq: Vec<(Vec<PathElement>, Expr)> = Vec::new();
        all_eq.push((self.pk_path.clone(), self.pk_value.clone()));
        all_eq.append(&mut self.extra_pk_conditions);
        all_eq.append(&mut self.extra_sk_conditions);

        // The parser allows at most one non-equality clause.
        let mut non_eq: Option<SortKeyCondition> = None;
        if let Some(sk) = self.sk_condition.take() {
            match sk {
                SortKeyCondition::Compare {
                    path,
                    op: CompareOp::Eq,
                    value,
                } => all_eq.push((path, value)),
                other => non_eq = Some(other),
            }
        }

        let mut hash_eqs: Vec<(usize, Vec<PathElement>, Expr)> = Vec::new();
        let mut range_eqs: Vec<(usize, Vec<PathElement>, Expr)> = Vec::new();
        let mut other_eqs: Vec<(Vec<PathElement>, Expr)> = Vec::new();
        for (path, value) in all_eq {
            let attr_name = resolve_path_attr(&path, names)?;
            if let Some(pos) = hash_attrs.iter().position(|a| *a == attr_name) {
                hash_eqs.push((pos, path, value));
            } else if let Some(pos) = range_attrs.iter().position(|a| *a == attr_name) {
                range_eqs.push((pos, path, value));
            } else {
                other_eqs.push((path, value));
            }
        }

        // The non-equality clause's attribute: its RANGE position when it
        // names a RANGE attribute, its HASH position when it names a HASH
        // attribute, neither when it names a non-key attribute.
        let non_eq_attr = match &non_eq {
            Some(cond) => Some(resolve_path_attr(cond.path(), names)?),
            None => None,
        };
        let non_eq_pos = non_eq_attr
            .as_deref()
            .and_then(|a| range_attrs.iter().position(|r| *r == a));
        let non_eq_hash_pos = non_eq_attr
            .as_deref()
            .and_then(|a| hash_attrs.iter().position(|h| *h == a));

        // One condition per key attribute. A second condition on one HASH or
        // RANGE attribute, equality or not, is refused before any coverage
        // check (measured on the service, 2026-10-21).
        hash_eqs.sort_by_key(|(pos, _, _)| *pos);
        range_eqs.sort_by_key(|(pos, _, _)| *pos);
        let duplicated = hash_eqs.windows(2).any(|w| w[0].0 == w[1].0)
            || range_eqs.windows(2).any(|w| w[0].0 == w[1].0)
            || non_eq_pos.is_some_and(|p| range_eqs.iter().any(|(q, _, _)| *q == p))
            || non_eq_hash_pos.is_some_and(|p| hash_eqs.iter().any(|(q, _, _)| *q == p));
        if duplicated {
            return Err(DynamoDbError::ValidationException(
                "Invalid KeyConditionExpression: KeyConditionExpressions must only contain \
                 one condition per key"
                    .to_string(),
            ));
        }

        // More conditions than key attributes (measured 2026-10-21: the count
        // is reported ahead of which attribute is missing).
        let condition_count =
            hash_eqs.len() + range_eqs.len() + other_eqs.len() + usize::from(non_eq.is_some());
        let key_attribute_count = hash_attrs.len() + range_attrs.len();
        if condition_count > key_attribute_count {
            return Err(DynamoDbError::ValidationException(format!(
                "The number of query conditions ({condition_count}) exceeds the number of key \
                 attributes defined in the schema ({key_attribute_count})"
            )));
        }

        // Every HASH attribute needs an equality condition. On a
        // multi-attribute key schema the service names every HASH attribute
        // without one, in key-schema order (measured 2026-10-21); the
        // single-attribute `Query condition missed key schema element`
        // wording belongs to the single-attribute path in the engine.
        let missing_hash: Vec<&str> = hash_attrs
            .iter()
            .enumerate()
            .filter(|(pos, _)| !hash_eqs.iter().any(|(p, _, _)| p == pos))
            .map(|(_, attr)| *attr)
            .collect();
        if !missing_hash.is_empty() {
            return Err(DynamoDbError::ValidationException(format!(
                "HASH key attributes {} must have equality conditions specified in the query",
                missing_hash.join(", ")
            )));
        }

        // RANGE conditions must cover a leading run of the RANGE attributes,
        // with an equality on every one before the last.
        let last_pos = range_eqs.iter().map(|(p, _, _)| *p).chain(non_eq_pos).max();
        if let Some(last_pos) = last_pos {
            let without_eq: Vec<&str> = range_attrs[..last_pos]
                .iter()
                .enumerate()
                .filter(|(pos, _)| !range_eqs.iter().any(|(p, _, _)| p == pos))
                .map(|(_, attr)| *attr)
                .collect();
            if !without_eq.is_empty() {
                return Err(DynamoDbError::ValidationException(format!(
                    "RANGE key attributes {} must have equality conditions specified in the \
                     query because a condition is present on key attribute {}",
                    without_eq.join(", "),
                    range_attrs[last_pos]
                )));
            }
        }

        for (_, path, value) in hash_eqs.iter().chain(range_eqs.iter()) {
            value_check(path, value)?;
        }
        for (path, value) in &other_eqs {
            value_check(path, value)?;
        }
        match &non_eq {
            Some(SortKeyCondition::Compare { path, value, .. }) => value_check(path, value)?,
            Some(SortKeyCondition::Between { path, low, high }) => {
                value_check(path, low)?;
                value_check(path, high)?;
            }
            Some(SortKeyCondition::BeginsWith { path, prefix }) => value_check(path, prefix)?,
            None => {}
        }

        // A condition on an attribute outside the index key schema. The
        // service refuses it by naming the first RANGE attribute that has no
        // condition (measured 2026-10-21: `h = :h AND z = :z` on RANGE
        // `a, b, c, d` is `Query condition missed key schema element: a`;
        // with `a` given, `b`). The count check above has already refused
        // the case where every key attribute has a condition as well.
        let non_key_non_eq = non_eq.is_some() && non_eq_pos.is_none() && non_eq_hash_pos.is_none();
        if !other_eqs.is_empty() || non_key_non_eq {
            let missing_range = range_attrs
                .iter()
                .enumerate()
                .find(|(pos, _)| {
                    !range_eqs.iter().any(|(p, _, _)| p == pos) && non_eq_pos != Some(*pos)
                })
                .map(|(_, attr)| *attr)
                .unwrap_or(hash_attrs[0]);
            return Err(DynamoDbError::ValidationException(format!(
                "Query condition missed key schema element: {missing_range}"
            )));
        }

        let mut hash_iter = hash_eqs.into_iter().map(|(_, path, value)| (path, value));
        if let Some((first_path, first_value)) = hash_iter.next() {
            self.pk_path = first_path;
            self.pk_value = first_value;
            self.extra_pk_conditions = hash_iter.collect();
        }

        let mut range_eqs: Vec<(Vec<PathElement>, Expr)> = range_eqs
            .into_iter()
            .map(|(_, path, value)| (path, value))
            .collect();

        if let Some(cond) = non_eq {
            self.sk_condition = Some(cond);
        } else if range_eqs.is_empty() {
            self.sk_condition = None;
        } else {
            let (path, value) = range_eqs.remove(0);
            self.sk_condition = Some(SortKeyCondition::Compare {
                path,
                op: CompareOp::Eq,
                value,
            });
        }
        self.extra_sk_conditions = range_eqs;

        Ok(())
    }

    /// Assign every clause to its key attribute under `key_schema`, the key
    /// schema of the table or index the query reads.
    ///
    /// A multi-attribute key schema goes through
    /// [`Self::resolve_multipart_with`], which classifies every clause by name,
    /// applies the service's rules for such a schema and calls `value_check`
    /// where the service reports a value of the wrong type. On any other key
    /// schema `value_check` runs on every clause first, which is where the
    /// service reports it for such a schema (measured 2026-10-21), and then
    /// [`Self::resolve_pk_sk`] picks the partition key clause.
    ///
    /// The engine reads a `Query` key condition through this, and so does
    /// authorization when it finds the partition key value for
    /// `dynamodb:LeadingKeys`, so both read the same clause as the partition
    /// key. On a multi-attribute key schema `pk_path` then names the first
    /// HASH attribute.
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_multipart_with`] on a multi-attribute key schema and
    /// as [`Self::resolve_pk_sk`] on any other, plus whatever `value_check`
    /// returns.
    pub fn resolve_key_schema_with<F>(
        &mut self,
        key_schema: &[KeySchemaElement],
        names: &HashMap<String, String>,
        mut value_check: F,
    ) -> Result<(), DynamoDbError>
    where
        F: FnMut(&[PathElement], &Expr) -> Result<(), DynamoDbError>,
    {
        if is_multipart_key_schema(key_schema) {
            let hash_attrs: Vec<&str> = hash_key_elements(key_schema)
                .iter()
                .map(|ks| ks.attribute_name.as_str())
                .collect();
            let range_attrs: Vec<&str> = range_key_elements(key_schema)
                .iter()
                .map(|ks| ks.attribute_name.as_str())
                .collect();
            return self.resolve_multipart_with(&hash_attrs, &range_attrs, names, value_check);
        }
        self.for_each_clause(&mut value_check)?;
        match key_schema.iter().find(|ks| ks.key_type == KeyType::Hash) {
            Some(hash) => self.resolve_pk_sk(&hash.attribute_name, names),
            None => Ok(()),
        }
    }

    /// [`Self::resolve_key_schema_with`] without a value check.
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_key_schema_with`].
    pub fn resolve_key_schema(
        &mut self,
        key_schema: &[KeySchemaElement],
        names: &HashMap<String, String>,
    ) -> Result<(), DynamoDbError> {
        self.resolve_key_schema_with(key_schema, names, |_, _| Ok(()))
    }

    /// Call `f` with the path and value of every clause: the partition key
    /// clause, the extra HASH and RANGE equalities, and each operand of the
    /// sort key condition.
    fn for_each_clause<F>(&self, f: &mut F) -> Result<(), DynamoDbError>
    where
        F: FnMut(&[PathElement], &Expr) -> Result<(), DynamoDbError>,
    {
        f(&self.pk_path, &self.pk_value)?;
        for (path, value) in &self.extra_pk_conditions {
            f(path, value)?;
        }
        for (path, value) in &self.extra_sk_conditions {
            f(path, value)?;
        }
        match &self.sk_condition {
            Some(SortKeyCondition::Compare { path, value, .. }) => f(path, value),
            Some(SortKeyCondition::Between { path, low, high }) => {
                f(path, low)?;
                f(path, high)
            }
            Some(SortKeyCondition::BeginsWith { path, prefix }) => f(path, prefix),
            None => Ok(()),
        }
    }
}

/// Resolve a path's top-level attribute name, handling `#name` references.
fn resolve_path_attr(
    path: &[PathElement],
    names: &HashMap<String, String>,
) -> Result<String, DynamoDbError> {
    match path.first() {
        Some(PathElement::Attribute(name)) => {
            if let Some(ref_name) = name.strip_prefix('#') {
                names.get(ref_name).cloned().ok_or_else(|| {
                    DynamoDbError::ValidationException(format!(
                        "Invalid KeyConditionExpression: An expression attribute name used in \
                         the document path is not defined; attribute name: #{ref_name}"
                    ))
                })
            } else {
                Ok(name.clone())
            }
        }
        _ => Err(DynamoDbError::ValidationException(
            "Invalid key condition path".to_owned(),
        )),
    }
}
