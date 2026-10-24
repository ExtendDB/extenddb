// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `#name` and `:value` references of a key condition.

use super::KeyCondition;
use crate::error::DynamoDbError;
use crate::expression::{ExpressionMaps, collect_key_condition_refs};

impl KeyCondition {
    /// Refuse a key condition that names a `#name` or `:value` the request
    /// does not define.
    ///
    /// The service reports these ahead of every other key condition error,
    /// the key schema wording included, and always with the
    /// `Invalid KeyConditionExpression: ` prefix (measured 2026-10-23 for
    /// `pk = :x`, `pk > :x`, `sk > :x`, `pk > :x AND sk > :y` and
    /// `#n > :x AND sk > :y` with the name or value left out). Names are
    /// checked before values; within each kind the references are checked in
    /// sorted order, so the answer does not depend on hash order.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` naming the first undefined reference.
    pub fn validate_references(&self, maps: &ExpressionMaps) -> Result<(), DynamoDbError> {
        let (names, values) = collect_key_condition_refs(self);
        let mut names: Vec<String> = names.into_iter().collect();
        names.sort();
        for name in names {
            if !maps.names.contains_key(&name) {
                return Err(DynamoDbError::ValidationException(format!(
                    "Invalid KeyConditionExpression: An expression attribute name used in \
                     the document path is not defined; attribute name: #{name}"
                )));
            }
        }
        let mut values: Vec<String> = values.into_iter().collect();
        values.sort();
        for value in values {
            maps.resolve_value_for(&value, "KeyConditionExpression")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::expression::{parse_key_condition, tokenize};
    use crate::types::AttributeValue;

    fn maps(names: &[(&str, &str)], values: &[&str]) -> ExpressionMaps {
        ExpressionMaps::new(
            names
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            values
                .iter()
                .map(|k| ((*k).to_owned(), AttributeValue::S("a".to_owned())))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn check(expr: &str, maps: &ExpressionMaps) -> Result<(), String> {
        let kc = parse_key_condition(&tokenize(expr).unwrap()).unwrap();
        kc.validate_references(maps).map_err(|e| match e {
            DynamoDbError::ValidationException(msg) => msg,
            other => panic!("{other:?}"),
        })
    }

    const NO_VALUE: &str = "Invalid KeyConditionExpression: An expression attribute value used \
                            in expression is not defined; attribute value: :x";

    #[test]
    fn an_undefined_value_is_refused_with_the_prefix_for_every_shape() {
        // Rows P3, 1, 1b and `pk > :x AND sk > :y` (only `:y` given) of the
        // review rows, measured 2026-10-23.
        let none = maps(&[], &[]);
        for expr in ["pk = :x", "pk > :x", "sk > :x"] {
            assert_eq!(check(expr, &none).unwrap_err(), NO_VALUE, "{expr}");
        }
        let only_y = maps(&[], &["y"]);
        assert_eq!(check("pk > :x AND sk > :y", &only_y).unwrap_err(), NO_VALUE);
        assert_eq!(check("pk = :x AND sk > :y", &only_y).unwrap_err(), NO_VALUE);
    }

    #[test]
    fn an_undefined_name_is_refused_with_the_prefix() {
        let values = maps(&[], &["x", "y"]);
        assert_eq!(
            check("#n > :x AND sk > :y", &values).unwrap_err(),
            "Invalid KeyConditionExpression: An expression attribute name used in the \
             document path is not defined; attribute name: #n"
        );
    }

    #[test]
    fn a_name_is_checked_before_a_value() {
        let none = maps(&[], &[]);
        let err = check("#n > :x", &none).unwrap_err();
        assert!(err.contains("attribute name: #n"), "{err}");
    }

    #[test]
    fn defined_references_pass() {
        let defined = maps(&[("p", "pk")], &["x", "y"]);
        for expr in [
            "#p = :x",
            "#p = :x AND sk > :y",
            "pk > :x",
            "sk BETWEEN :x AND :y",
        ] {
            assert!(check(expr, &defined).is_ok(), "{expr}");
        }
    }
}
