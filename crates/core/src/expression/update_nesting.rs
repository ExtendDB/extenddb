// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Nesting depth of the attributes an update expression writes.

use std::collections::BTreeMap;

use crate::error::DynamoDbError;
use crate::types::AttributeValue;

use super::ast::UpdateAction;
use super::resolver::{ExpressionMaps, resolve_element_name};

/// Most M or L wrappers an attribute value may have: 31 wrappers around a
/// scalar is 32 levels, the service's limit.
const MAX_WRAPPERS: usize = 31;

/// Refuse an updated item whose SET targets now nest beyond the limit.
///
/// Only the top-level attributes a SET writes are checked. REMOVE, ADD and
/// DELETE cannot deepen a value, and an attribute the update does not touch
/// is left alone, so an item stored deeper than the limit before this check
/// existed stays updatable through its other attributes.
///
/// # Errors
///
/// `ValidationException` `Nesting Levels have exceeded supported limits`, the
/// service's text for UpdateItem and for a TransactWriteItems Update
/// cancellation reason (measured 2026-10-25).
pub(super) fn validate_written_nesting_depth(
    actions: &[UpdateAction],
    item: &BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    for action in actions {
        let UpdateAction::Set { path, .. } = action else {
            continue;
        };
        let Some(first) = path.first() else {
            continue;
        };
        let name = resolve_element_name(first, maps)?;
        if item
            .get(name.as_ref())
            .is_some_and(|v| wrappers_exceed(v, 0))
        {
            return Err(DynamoDbError::ValidationException(
                "Nesting Levels have exceeded supported limits".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Whether `value`, sitting under `above` wrappers, has more than
/// [`MAX_WRAPPERS`] wrappers on any path down to a scalar.
fn wrappers_exceed(value: &AttributeValue, above: usize) -> bool {
    let children: Box<dyn Iterator<Item = &AttributeValue>> = match value {
        AttributeValue::M(map) => Box::new(map.values()),
        AttributeValue::L(list) => Box::new(list.iter()),
        _ => return false,
    };
    let here = above + 1;
    here > MAX_WRAPPERS || children.into_iter().any(|v| wrappers_exceed(v, here))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use crate::expression::{parse_update, tokenize};

    fn nmap(wrappers: usize) -> AttributeValue {
        let mut v = AttributeValue::S("leaf".to_owned());
        for _ in 0..wrappers {
            v = AttributeValue::M(BTreeMap::from([("k".to_owned(), v)]));
        }
        v
    }

    fn check(expr: &str, item: &BTreeMap<String, AttributeValue>) -> Result<(), DynamoDbError> {
        let actions = parse_update(&tokenize(expr).unwrap()).unwrap();
        // The names map is keyed without the leading `#`.
        let maps = ExpressionMaps::new(
            HashMap::from([("m".to_owned(), "m".to_owned())]),
            HashMap::new(),
        );
        validate_written_nesting_depth(&actions, item, &maps)
    }

    fn is_nesting_error(result: Result<(), DynamoDbError>) -> bool {
        matches!(
            result,
            Err(DynamoDbError::ValidationException(m))
                if m == "Nesting Levels have exceeded supported limits"
        )
    }

    #[test]
    fn thirty_one_wrappers_pass_and_thirty_two_are_refused() {
        let at = BTreeMap::from([("m".to_owned(), nmap(31))]);
        assert!(check("SET #m = :v", &at).is_ok());
        let over = BTreeMap::from([("m".to_owned(), nmap(32))]);
        assert!(is_nesting_error(check("SET #m.k = :v", &over)));
    }

    #[test]
    fn a_list_counts_as_a_wrapper_and_every_branch_is_checked() {
        let mut deep = AttributeValue::S("leaf".to_owned());
        for _ in 0..32 {
            deep = AttributeValue::L(vec![AttributeValue::N("1".to_owned()), deep]);
        }
        let item = BTreeMap::from([("m".to_owned(), deep)]);
        assert!(is_nesting_error(check("SET #m[0] = :v", &item)));
    }

    #[test]
    fn attributes_the_update_does_not_set_are_not_checked() {
        let item = BTreeMap::from([
            ("m".to_owned(), nmap(40)),
            ("x".to_owned(), AttributeValue::S("y".to_owned())),
        ]);
        assert!(check("SET x = :v", &item).is_ok());
        assert!(check("REMOVE #m.k", &item).is_ok());
        assert!(is_nesting_error(check("SET #m.k = :v", &item)));
    }
}
