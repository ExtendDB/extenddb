// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The name and value usage checks shared by both passes: every `#name`
//! element of a document path, and every `:value` placeholder, is defined.

use super::super::ast::PathElement;
use super::super::kind::ExpressionKind;
use super::super::resolver::ExpressionMaps;
use super::errors::validation;
use crate::error::DynamoDbError;

pub(super) fn check_path_refs(
    path: &[PathElement],
    maps: &ExpressionMaps,
    kind: ExpressionKind,
) -> Result<(), DynamoDbError> {
    for element in path {
        if let PathElement::Attribute(name) = element
            && let Some(ref_name) = name.strip_prefix('#')
            && !maps.names.contains_key(ref_name)
        {
            return Err(validation(
                kind,
                &format!(
                    "An expression attribute name used in the document path is not defined; attribute name: #{ref_name}"
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn check_value_defined(
    name: &str,
    maps: &ExpressionMaps,
    kind: ExpressionKind,
) -> Result<(), DynamoDbError> {
    if maps.values.contains_key(name) {
        Ok(())
    } else {
        Err(validation(
            kind,
            &format!(
                "An expression attribute value used in expression is not defined; attribute value: :{name}"
            ),
        ))
    }
}
