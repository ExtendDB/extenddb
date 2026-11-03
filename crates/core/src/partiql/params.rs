// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL parameter checks: nesting depth and number conversion.
//!
//! The service converts each statement's `Parameters` before it looks at the
//! statement, and reports the first problem it reaches walking the values in
//! document order: a map or list nested past the item limit, or an `N` (or
//! `NS` member) that cannot be converted or breaks a number rule (measured
//! 2026-11-02). So `[deep, {"N": "abc"}]` is a nesting error and
//! `[{"N": "abc"}, deep]` a conversion error.
//!
//! The walk here runs over the parsed request body: parameters and list
//! elements in request order, the members of a map in the order the parsed
//! JSON object holds them. That is the request's order when the build
//! includes the MongoDB storage crate, whose JSON dependency makes every JSON
//! map keep it (any build of the whole workspace), and name order otherwise.
//! In name order, when two members of one map both have a problem, the one
//! reported can differ from the service's, which walks the request's order
//! (measured 2026-11-02).

use serde_json::Value;

use super::number::{check_number, parameter_message};
use crate::validation::MAX_ITEM_NESTING_DEPTH;

/// The service's nesting-depth message, the same for parameters as for items.
pub const NESTING_MESSAGE: &str = "Nesting Levels have exceeded supported limits: \
     Attributes in the item have nested levels beyond supported limit";

/// Check a statement's parameters (wire attribute values) in order.
///
/// # Errors
///
/// Returns the service's message for the first problem: the nesting-depth
/// message, or the parameter number message for an `N` or `NS` member.
pub fn check_parameter_values(parameters: &[Value]) -> Result<(), String> {
    parameters.iter().try_for_each(|p| check_value(p, 0))
}

/// Walk one wire attribute value. `depth` counts the maps and lists around
/// it. A parameter may hold 32 of them around its leaf, one more than an item
/// attribute; 33 is refused (measured 2026-11-02). The walk stops there, so
/// it recurses at most that deep.
fn check_value(value: &Value, depth: usize) -> Result<(), String> {
    let Value::Object(members) = value else {
        return Ok(());
    };
    for (tag, payload) in members {
        match (tag.as_str(), payload) {
            ("M", Value::Object(entries)) => {
                let next = enter(depth)?;
                for member in entries.values() {
                    check_value(member, next)?;
                }
            }
            ("L", Value::Array(items)) => {
                let next = enter(depth)?;
                for item in items {
                    check_value(item, next)?;
                }
            }
            ("N", Value::String(text)) => check_text(text)?,
            ("NS", Value::Array(items)) => {
                for item in items {
                    if let Value::String(text) = item {
                        check_text(text)?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn enter(depth: usize) -> Result<usize, String> {
    let next = depth + 1;
    if next > MAX_ITEM_NESTING_DEPTH {
        return Err(NESTING_MESSAGE.to_owned());
    }
    Ok(next)
}

fn check_text(text: &str) -> Result<(), String> {
    check_number(text).map_err(|problem| parameter_message(text, problem))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partiql::number::UNDERFLOW_MESSAGE;

    fn doc(json: &str) -> Vec<Value> {
        match serde_json::from_str::<Value>(json).unwrap() {
            Value::Array(items) => items,
            other => panic!("expected an array, got {other:?}"),
        }
    }

    fn deep(levels: usize, leaf: &str) -> String {
        let mut v = leaf.to_owned();
        for _ in 0..levels {
            v = format!("{{\"M\":{{\"n\":{v}}}}}");
        }
        v
    }

    const ABC: &str = "The parameter cannot be converted to a numeric value: abc";

    #[test]
    fn parameters_are_walked_in_request_order_and_map_members_as_the_map_holds_them() {
        let d = deep(40, "{\"S\":\"leaf\"}");
        let bad = "{\"N\":\"abc\"}";
        assert_eq!(
            check_parameter_values(&doc(&format!("[{d},{bad}]"))),
            Err(NESTING_MESSAGE.to_owned())
        );
        assert_eq!(
            check_parameter_values(&doc(&format!("[{bad},{d}]"))),
            Err(ABC.to_owned())
        );
        // Within one map the member the parsed map holds first is reached
        // first: the one whose name sorts first, or the one the request wrote
        // first in a build whose JSON maps keep the request's order.
        for members in [
            format!("\"a\":{d},\"b\":{bad}"),
            format!("\"b\":{bad},\"a\":{d}"),
        ] {
            let params = doc(&format!("[{{\"M\":{{{members}}}}}]"));
            let first = params[0]["M"].as_object().unwrap().keys().next().unwrap();
            let want = if first == "a" { NESTING_MESSAGE } else { ABC };
            assert_eq!(
                check_parameter_values(&params),
                Err(want.to_owned()),
                "{members}"
            );
        }
        assert_eq!(
            check_parameter_values(&doc(&format!("[{}]", deep(40, bad)))),
            Err(NESTING_MESSAGE.to_owned())
        );
    }

    #[test]
    fn a_parameter_holds_one_level_more_than_an_item_attribute() {
        let ok = deep(MAX_ITEM_NESTING_DEPTH, "{\"S\":\"x\"}");
        assert_eq!(check_parameter_values(&doc(&format!("[{ok}]"))), Ok(()));
        let over = deep(MAX_ITEM_NESTING_DEPTH + 1, "{\"S\":\"x\"}");
        assert_eq!(
            check_parameter_values(&doc(&format!("[{over}]"))),
            Err(NESTING_MESSAGE.to_owned())
        );
        let mut list = "{\"S\":\"x\"}".to_owned();
        for _ in 0..MAX_ITEM_NESTING_DEPTH {
            list = format!("{{\"L\":[{list}]}}");
        }
        assert_eq!(check_parameter_values(&doc(&format!("[{list}]"))), Ok(()));
        let list = format!("{{\"L\":[{list}]}}");
        assert_eq!(
            check_parameter_values(&doc(&format!("[{list}]"))),
            Err(NESTING_MESSAGE.to_owned())
        );
    }

    #[test]
    fn numbers_are_checked_in_every_position() {
        for (json, message) in [
            (
                "[{\"N\":\"1e99999999999\"}]",
                "The parameter cannot be converted to a numeric value: 1e99999999999",
            ),
            (
                "[{\"N\":\"\"}]",
                "The parameter cannot be converted to a numeric value",
            ),
            ("[{\"N\":\"1e2147483647\"}]", UNDERFLOW_MESSAGE),
            (
                "[{\"L\":[{\"N\":\"1\"},{\"N\":\"1e99999999999\"}]}]",
                "The parameter cannot be converted to a numeric value: 1e99999999999",
            ),
            (
                "[{\"M\":{\"x\":{\"N\":\"1e99999999999\"}}}]",
                "The parameter cannot be converted to a numeric value: 1e99999999999",
            ),
            (
                "[{\"NS\":[\"1\",\"1e99999999999\"]}]",
                "The parameter cannot be converted to a numeric value: 1e99999999999",
            ),
        ] {
            assert_eq!(
                check_parameter_values(&doc(json)),
                Err(message.to_owned()),
                "{json}"
            );
        }
    }

    #[test]
    fn other_types_and_good_numbers_pass() {
        let params = doc(
            "[{\"S\":\"1e99999999999\"},{\"N\":\"12.5\"},{\"NS\":[\"1\",\"2\"]},\
             {\"SS\":[\"abc\"]},{\"BOOL\":true},{\"NULL\":true},{\"B\":\"AQI=\"}]",
        );
        assert_eq!(check_parameter_values(&params), Ok(()));
    }
}
