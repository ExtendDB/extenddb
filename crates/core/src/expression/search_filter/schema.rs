// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The search condition against the index search schema.
//!
//! The HASH checks run first. Then each condition in written order is checked
//! for schema membership, then an ordering operator against the attribute
//! type, then the type of each value. The first failure is the answer. These
//! messages carry no expression prefix.

use super::rules::{Condition, Operator};
use crate::types::{
    AttributeDefinition, AttributeValue, ScalarAttributeType, SearchSchemaElement,
    SearchSchemaElementType,
};

const HASH_MISSING: &str =
    "SearchConditionExpression must have all HASH attributes in configured SearchSchema";
const HASH_MORE_THAN_ONCE: &str = "SearchConditionExpression must contain only one condition \
                                   on the HASH attribute; to search more than one HASH value, \
                                   make one SearchVectors call for each value";

/// How the conditions use one HASH attribute.
enum HashUse<'a> {
    Missing,
    /// Exactly one condition on it, a must-match `=`: the condition.
    Selects(&'a Condition),
    /// A condition on it that is not a must-match `=`.
    NotMustMatchEquals,
    /// More than one condition on it, each a must-match `=`.
    MoreThanOnce,
}

fn hash_use<'a>(conditions: &'a [Condition], attribute: &str) -> HashUse<'a> {
    let on = || conditions.iter().filter(move |c| c.attribute == attribute);
    if on().any(|c| !(c.operator == Operator::Eq && c.must_match)) {
        return HashUse::NotMustMatchEquals;
    }
    let mut equals = on().filter(|c| !c.values.is_empty());
    match (equals.next(), equals.next()) {
        (None, _) => HashUse::Missing,
        (Some(condition), None) => HashUse::Selects(condition),
        (Some(_), Some(_)) => HashUse::MoreThanOnce,
    }
}

/// Validate `conditions` against the index search schema and return the
/// HASH attribute and the value that selects its partition, if the schema has
/// a HASH element.
///
/// # Errors
///
/// Returns the message of the first failed check.
pub(super) fn validate<'a>(
    conditions: &'a [Condition],
    schema: &[SearchSchemaElement],
    definitions: &[AttributeDefinition],
) -> Result<Option<(&'a str, &'a AttributeValue)>, String> {
    let mut selected = None;
    for element in schema
        .iter()
        .filter(|e| e.element_type == SearchSchemaElementType::Hash)
    {
        match hash_use(conditions, &element.attribute_name) {
            HashUse::Selects(condition) => {
                if let Some(value) = condition.values.first() {
                    selected.get_or_insert((condition.attribute.as_str(), value));
                }
            }
            HashUse::Missing => return Err(HASH_MISSING.to_owned()),
            HashUse::NotMustMatchEquals => {
                return Err(format!(
                    "SearchConditionExpression must use = on the HASH attribute, not inside OR \
                     or NOT; HASH attribute: {}",
                    element.attribute_name
                ));
            }
            HashUse::MoreThanOnce => return Err(HASH_MORE_THAN_ONCE.to_owned()),
        }
    }

    for condition in conditions {
        let Some(element) = schema
            .iter()
            .find(|e| e.attribute_name == condition.attribute)
        else {
            return Err(format!(
                "SearchConditionExpression must not contain any attributes that is not in \
                 SearchSchema. Invalid attribute: {}",
                condition.attribute
            ));
        };
        let Some(declared) = definitions
            .iter()
            .find(|d| d.attribute_name == element.attribute_name)
            .map(|d| d.attribute_type)
        else {
            continue;
        };
        if condition.operator.is_ordering() && declared != ScalarAttributeType::N {
            return Err(format!(
                "SearchConditionExpression allows this operator only on Number (N) attributes; \
                 operator: {}, attribute: {}, attribute type: {}",
                condition.operator.as_str(),
                element.attribute_name,
                type_code(declared)
            ));
        }
        for value in &condition.values {
            let given = crate::expression::resolver::attribute_type_code(value);
            if given != type_code(declared) {
                return Err(format!(
                    "Type of '{}' attribute in SearchConditionExpression ({given}) does not \
                     match type in search schema ({})",
                    condition.attribute,
                    type_code(declared)
                ));
            }
        }
    }
    Ok(selected)
}

fn type_code(t: ScalarAttributeType) -> &'static str {
    match t {
        ScalarAttributeType::S => "S",
        ScalarAttributeType::N => "N",
        ScalarAttributeType::B => "B",
    }
}
