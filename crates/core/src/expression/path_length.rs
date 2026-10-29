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
//! (measured 2026-10-26, us-east-1). The callers run the check where they
//! resolve each path's names.

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
    fn a_key_condition_path_is_checked_before_the_nested_refusal() {
        let parse = |n: usize| {
            let expr = format!("pk = :v AND {} = :v", path_text("m", n));
            crate::expression::parse_key_condition(&crate::expression::tokenize(&expr).unwrap())
                .map(|_| ())
        };
        assert_eq!(
            message(parse(33)),
            format!("Invalid KeyConditionExpression: {LEVELS_33}")
        );
        assert!(message(parse(32)).contains("cannot have conditions on nested attributes"));
    }

    #[test]
    fn a_projection_path_is_checked_after_its_names() {
        let compile = |text: &str| {
            let paths =
                crate::expression::parse_projection(&crate::expression::tokenize(text).unwrap())
                    .unwrap();
            crate::expression::Projection::compile(&paths, &maps(), true).map(|_| ())
        };
        assert_eq!(
            message(compile(&path_text("#m", 33))),
            format!("Invalid ProjectionExpression: {LEVELS_33}")
        );
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
