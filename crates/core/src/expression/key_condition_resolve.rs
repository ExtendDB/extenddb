// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key schema resolution for a parsed [`KeyCondition`]: correcting the
//! partition and sort key assignment once the table's key schema is known.

use std::collections::HashMap;

use super::ast::{CompareOp, Expr, PathElement};
use super::key_condition::{KeyCondition, SortKeyCondition};
use crate::error::DynamoDbError;

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
                            "An expression attribute name used in the document path \
                             is not defined; attribute name: #{ref_name}"
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

    /// Reclassify conditions for multi-part key schemas.
    ///
    /// Given the full set of HASH attribute names, moves any equality conditions
    /// on HASH attributes from `sk_condition` into `extra_pk_conditions`.
    /// The remaining non-HASH equality or range condition stays as `sk_condition`.
    ///
    /// Call this after `resolve_pk_sk()` for multi-part key queries.
    pub fn resolve_multipart(
        &mut self,
        hash_attrs: &[&str],
        names: &HashMap<String, String>,
    ) -> Result<(), DynamoDbError> {
        if hash_attrs.len() <= 1 {
            return Ok(());
        }

        // Collect all equality conditions (pk + extra_pk + sk if Eq)
        let mut all_eq: Vec<(Vec<PathElement>, Expr)> = Vec::new();
        all_eq.push((self.pk_path.clone(), self.pk_value.clone()));
        all_eq.append(&mut self.extra_pk_conditions);

        let mut sk_non_eq: Option<SortKeyCondition> = None;

        if let Some(sk) = self.sk_condition.take() {
            match sk {
                SortKeyCondition::Compare {
                    path,
                    op: CompareOp::Eq,
                    value,
                } => {
                    all_eq.push((path, value));
                }
                other => {
                    sk_non_eq = Some(other);
                }
            }
        }

        // Classify each equality condition as HASH or RANGE.
        // Index by position in hash_attrs so we can reorder to key schema order.
        let mut pk_by_position: Vec<(usize, Vec<PathElement>, Expr)> = Vec::new();
        let mut sk_eqs: Vec<(Vec<PathElement>, Expr)> = Vec::new();

        for (path, value) in all_eq {
            let attr_name = resolve_path_attr(&path, names)?;
            if let Some(pos) = hash_attrs.iter().position(|a| *a == attr_name.as_str()) {
                pk_by_position.push((pos, path, value));
            } else {
                sk_eqs.push((path, value));
            }
        }

        // Sort PK conditions by key schema order to ensure composite PK
        // concatenation matches the write path regardless of expression order.
        pk_by_position.sort_by_key(|(pos, _, _)| *pos);

        // Assign first PK condition as primary, rest as extra
        let mut pk_iter = pk_by_position
            .into_iter()
            .map(|(_, path, value)| (path, value));
        if let Some((first_path, first_value)) = pk_iter.next() {
            self.pk_path = first_path;
            self.pk_value = first_value;
            self.extra_pk_conditions = pk_iter.collect();
        }

        // Assign SK conditions: non-eq condition takes priority as primary SK,
        // otherwise first equality RANGE condition is primary, rest are extra.
        if let Some(non_eq) = sk_non_eq {
            self.sk_condition = Some(non_eq);
            self.extra_sk_conditions = sk_eqs;
        } else if let Some((path, value)) = sk_eqs.first().cloned() {
            self.sk_condition = Some(SortKeyCondition::Compare {
                path,
                op: CompareOp::Eq,
                value,
            });
            self.extra_sk_conditions = sk_eqs.into_iter().skip(1).collect();
        } else {
            self.sk_condition = None;
            self.extra_sk_conditions = Vec::new();
        }

        Ok(())
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
                        "An expression attribute name used in the document path \
                         is not defined; attribute name: #{ref_name}"
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
