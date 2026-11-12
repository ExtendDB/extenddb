// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Length limit of a document path in an expression.
//!
//! Amazon `DynamoDB` refuses a document path of more than 32 elements, each
//! attribute name and each list index counting as one, in every expression
//! kind: `Invalid <Kind>: The document path has too many nesting levels;
//! nesting levels: <n>`. It checks a path's `#name` references first and then
//! its length, path by path in the order the paths appear, so an undefined
//! `#name` in an earlier path or inside the long path is reported instead
//! (measured 2026-10-26, us-east-1). The semantic walk and the projection
//! resolution run the check right after each path's names; the key condition
//! parser runs it before the names are checked, ahead of its nested-attribute
//! refusal.

use super::ast::PathElement;

/// Most elements a document path may have.
pub const MAX_DOCUMENT_PATH_ELEMENTS: usize = 32;

/// The service's sentence for `path` when it is too long, without the
/// `Invalid <Kind>: ` label the caller adds.
pub(crate) fn too_many_levels(path: &[PathElement]) -> Option<String> {
    (path.len() > MAX_DOCUMENT_PATH_ELEMENTS).then(|| {
        format!(
            "The document path has too many nesting levels; nesting levels: {}",
            path.len()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(n: usize) -> Vec<PathElement> {
        (0..n)
            .map(|_| PathElement::Attribute("k".to_owned()))
            .collect()
    }

    #[test]
    fn thirty_two_elements_pass_and_thirty_three_are_refused() {
        assert_eq!(too_many_levels(&names(32)), None);
        assert_eq!(
            too_many_levels(&names(33)).as_deref(),
            Some("The document path has too many nesting levels; nesting levels: 33")
        );
    }

    #[test]
    fn a_list_index_counts_as_an_element() {
        let mut path = names(1);
        path.extend((0..32).map(PathElement::Index));
        assert_eq!(path.len(), 33);
        assert!(too_many_levels(&path).is_some());
        path.pop();
        assert_eq!(too_many_levels(&path), None);
    }

    fn path_text(first: &str, n: usize) -> String {
        format!("{first}{}", ".k".repeat(n - 1))
    }

    fn message(result: Result<(), crate::error::DynamoDbError>) -> String {
        match result {
            Err(crate::error::DynamoDbError::ValidationException(m)) => m,
            other => panic!("{other:?}"),
        }
    }

    fn maps() -> crate::expression::ExpressionMaps {
        crate::expression::ExpressionMaps::new(
            std::collections::HashMap::from([("m".to_owned(), "m".to_owned())]),
            std::collections::HashMap::from([(
                "v".to_owned(),
                crate::types::AttributeValue::S("v".to_owned()),
            )]),
        )
    }

    const LEVELS_33: &str = "The document path has too many nesting levels; nesting levels: 33";

    #[test]
    fn a_key_condition_checks_names_values_and_lengths_in_text_order() {
        let maps = crate::expression::ExpressionMaps::new(
            std::collections::HashMap::from([("m".to_owned(), "m".to_owned())]),
            ["p", "s"]
                .into_iter()
                .map(|k| {
                    (
                        k.to_owned(),
                        crate::types::AttributeValue::S("v".to_owned()),
                    )
                })
                .collect(),
        );
        let parse = |expr: &str| {
            let tokens = crate::expression::tokenize(expr).unwrap();
            crate::expression::parse_key_condition_with_maps(&tokens, &maps).map(|_| ())
        };
        let (long, at_limit) = (path_text("#m", 33), path_text("#m", 32));
        let length = format!("Invalid KeyConditionExpression: {LEVELS_33}");
        let name = "Invalid KeyConditionExpression: An expression attribute name used in the \
                    document path is not defined; attribute name: #zz";
        let value = "Invalid KeyConditionExpression: An expression attribute value used in \
                     expression is not defined; attribute value: :nope";
        let nested = "Invalid KeyConditionExpression: KeyConditionExpressions cannot have \
                      conditions on nested attributes";
        // Measured 2026-11-12 (us-east-1), each request twice.
        for (expr, want) in [
            (format!("pk = :p AND {long} = :s"), length.as_str()),
            (format!("pk = :p AND {} = :s", path_text("#zz", 33)), name),
            (format!("pk = :nope AND {long} = :s"), value),
            (format!("{long} = :s AND pk = :nope"), length.as_str()),
            (format!("#zz = :p AND {long} = :s"), name),
            (format!("{long} = :s AND #zz = :p"), length.as_str()),
            (format!("pk = :p AND {long} = :nope"), length.as_str()),
            (
                format!("pk = :p AND begins_with({long}, :nope)"),
                length.as_str(),
            ),
            (format!("pk = :p AND {at_limit}.#zz = :s"), name),
            (format!("pk = :p AND {at_limit} = :s"), nested),
            (format!("#zz = :p AND {at_limit} = :s"), name),
            (format!("{at_limit} = :s AND pk = :nope"), value),
            ("pk < :nope".to_owned(), value),
        ] {
            assert_eq!(message(parse(&expr)), want, "{expr}");
        }
    }

    #[test]
    fn a_projection_path_is_checked_after_its_names_and_before_its_overlap() {
        let compile = |text: &str| {
            let paths =
                crate::expression::parse_projection(&crate::expression::tokenize(text).unwrap())
                    .unwrap();
            crate::expression::Projection::compile(&paths, &maps(), true).map(|_| ())
        };
        let long = path_text("#m", 33);
        let length = format!("Invalid ProjectionExpression: {LEVELS_33}");
        let overlap = "Invalid ProjectionExpression: Two document paths overlap with each other; \
                       must remove or rewrite one of these paths; path one: [k], path two: [k]";
        // Measured 2026-11-12 (us-east-1), each request twice.
        for (text, want) in [
            (long.clone(), length.as_str()),
            (format!("k, k, {long}"), overlap),
            (format!("{long}, k, k"), length.as_str()),
            (format!("k, {long}, k"), length.as_str()),
            (format!("#m, {long}"), length.as_str()),
            (format!("{}, k, k", path_text("#m", 32)), overlap),
        ] {
            assert_eq!(message(compile(&text)), want, "{text}");
        }
        assert!(message(compile(&path_text("#zz", 33))).contains("attribute name: #zz"));
        assert!(compile(&path_text("#m", 32)).is_ok());
    }

    #[test]
    fn an_update_path_is_checked_after_its_names_in_text_order() {
        let check = |text: &str| {
            let tokens = crate::expression::tokenize(text).unwrap();
            let parsed = crate::expression::parse_update_sections(&tokens, Some(text)).unwrap();
            crate::expression::validate_update_semantics(&parsed, &maps())
        };
        let long = path_text("#m", 33);
        assert_eq!(
            message(check(&format!("SET {long} = :v"))),
            format!("Invalid UpdateExpression: {LEVELS_33}")
        );
        assert!(message(check(&format!("SET #zz = :v, {long} = :v"))).contains("#zz"));
        assert!(message(check(&format!("SET {} = :v", path_text("#zz", 33)))).contains("#zz"));
        assert!(message(check(&format!("SET {long} = :v, #zz = :v"))).contains("33"));
    }
}
