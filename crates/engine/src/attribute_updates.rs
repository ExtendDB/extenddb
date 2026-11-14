// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request-time checks of the legacy `AttributeUpdates` parameter of
//! `UpdateItem`, run before the parameter is desugared into an
//! `UpdateExpression`.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::attribute_type_code;
use extenddb_core::types::{AttributeValue, AttributeValueUpdate};

/// Refuse an `ADD` whose value is not a number or a set, and a `DELETE` with a
/// value that is not a set, with the service's text (measured 2026-10-27):
/// `1 validation error detected: One or more parameter values were invalid:
/// ADD action is not supported for the type S` and `... DELETE action with
/// value is not supported for the type N`. The check runs ahead of the table
/// lookup and of key validation.
///
/// The service names a different entry from one call to the next when several
/// are wrong; entries are visited in attribute name order here so the answer
/// is repeatable. The service also accepts an empty list for `ADD`, which this
/// check refuses like every other type.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for the first offending entry.
pub(crate) fn validate_action_value_types(
    updates: &HashMap<String, AttributeValueUpdate>,
) -> Result<(), DynamoDbError> {
    let mut names: Vec<&String> = updates.keys().collect();
    names.sort();
    for name in names {
        let update = &updates[name];
        let Some(value) = update.value.as_ref() else {
            continue;
        };
        let refusal = match update.action.to_uppercase().as_str() {
            "ADD" if !matches!(value, AttributeValue::N(_)) && !is_set(value) => {
                "ADD action is not supported for the type"
            }
            "DELETE" if !is_set(value) => "DELETE action with value is not supported for the type",
            _ => continue,
        };
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: One or more parameter values were invalid: {refusal} {}",
            attribute_type_code(value)
        )));
    }
    Ok(())
}

fn is_set(value: &AttributeValue) -> bool {
    matches!(
        value,
        AttributeValue::SS(_) | AttributeValue::NS(_) | AttributeValue::BS(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal(action: &str, value: AttributeValue) -> String {
        let updates = HashMap::from([(
            "a".to_owned(),
            AttributeValueUpdate {
                value: Some(value),
                action: action.to_owned(),
            },
        )]);
        match validate_action_value_types(&updates) {
            Ok(()) => "OK".to_owned(),
            Err(DynamoDbError::ValidationException(msg)) => msg,
            Err(other) => panic!("unexpected {other:?}"),
        }
    }

    const ADD: &str = "1 validation error detected: One or more parameter values were invalid: ADD action is not supported for the type";
    const DELETE: &str = "1 validation error detected: One or more parameter values were invalid: DELETE action with value is not supported for the type";

    // Captured from Amazon DynamoDB on 2026-10-27.
    #[test]
    fn add_takes_numbers_and_sets_and_delete_takes_sets() {
        assert_eq!(
            refusal("ADD", AttributeValue::S("x".into())),
            format!("{ADD} S")
        );
        assert_eq!(
            refusal("ADD", AttributeValue::M(Default::default())),
            format!("{ADD} M")
        );
        assert_eq!(
            refusal("ADD", AttributeValue::Bool(true)),
            format!("{ADD} BOOL")
        );
        assert_eq!(
            refusal("DELETE", AttributeValue::N("1".into())),
            format!("{DELETE} N")
        );
        assert_eq!(
            refusal("DELETE", AttributeValue::S("x".into())),
            format!("{DELETE} S")
        );
        assert_eq!(
            refusal("DELETE", AttributeValue::L(vec![])),
            format!("{DELETE} L")
        );
        assert_eq!(refusal("ADD", AttributeValue::N("1".into())), "OK");
        assert_eq!(
            refusal("ADD", AttributeValue::SS(["a".to_owned()].into())),
            "OK"
        );
        assert_eq!(
            refusal("DELETE", AttributeValue::NS(["1".to_owned()].into())),
            "OK"
        );
        assert_eq!(refusal("PUT", AttributeValue::S("x".into())), "OK");
    }
}
