// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL parameter checks: the JSON types of the values, then nesting
//! depth, number conversion, and the values the service refuses for their
//! content.
//!
//! The service reads the parameters of a request in two passes (measured
//! 2026-11-06 in all three operations):
//!
//! - It first reads the JSON of the whole request into attribute values. A
//!   value of the wrong JSON type anywhere in any statement's `Parameters`,
//!   such as `{"BOOL": "x"}`, `{"S": 1}`, `{"SS": [1]}`, `{"BS": [1]}` or a
//!   `B` that is not base64, fails the whole request with a
//!   `SerializationException`, ahead of every other check of the request
//!   (see [`check_parameter_types`]). In a `BatchExecuteStatement` no
//!   statement runs. A member that the attribute value model does not name,
//!   such as `X`, is ignored whatever its value is, and so is a member whose
//!   value is null: `{"X": "1"}` and `{"S": null}` are values with no type,
//!   and `{"S": "a", "X": "1"}` is the string `a`.
//! - It then converts each statement's parameters before it looks at the
//!   statement, and reports the first problem it reaches walking the values
//!   in document order (see [`check_parameter_values`]): a map or list nested
//!   past the item limit, an `N` (or `NS` member) that cannot be converted or
//!   breaks a number rule, an attribute value with no type or with two, a
//!   null member of a set, map or list, an empty set, a set that holds a
//!   member twice, and a NULL that is not true. These are refused as the
//!   attribute value deserializer refuses them, but a number set whose
//!   members have one value, which is "Input collection contains
//!   duplicates", and a null member, which has a message for each kind of
//!   value. So `[deep, {"N": "abc"}]` is a nesting error and
//!   `[{"N": "abc"}, deep]` a conversion error (measured 2026-11-02), and
//!   `[{"SS": []}, deep]` and `{"L": [{"SS": []}, deep]}` are refused for the
//!   set, `[deep, {"SS": []}]` and `{"L": [deep, {"SS": []}]}` for the
//!   nesting, and the same with a number past the limits (measured
//!   2026-11-06). Such a problem is the statement's own: in a
//!   `BatchExecuteStatement` the other statements run.
//!
//! The walks here run over the parsed request body: parameters and list
//! elements in request order, the members of a map in the order the parsed
//! JSON object holds them. That is the request's order when the build
//! includes the MongoDB storage crate, whose JSON dependency makes every JSON
//! map keep it (any build of the whole workspace), and name order otherwise.
//! In name order, when two members of one map both have a problem, the one
//! reported can differ from the service's, which walks the request's order
//! (measured 2026-11-02).

use std::collections::HashSet;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use serde_json::{Map, Value};

use super::number::{check_number, parameter_message};
use crate::types::AttributeValue;
use crate::validation::MAX_ITEM_NESTING_DEPTH;
use crate::validation::number::validate_and_normalize_number;

/// The refusal of a number set parameter two of whose members have one
/// value, the same at any depth and for members written alike (measured
/// 2026-11-06: `{"NS": ["1", "1.0"]}`, `{"NS": ["1", "1"]}`, and inside a
/// map and a list).
pub const NUMBER_SET_DUPLICATES: &str = "Input collection contains duplicates";

/// The service's nesting-depth message, the same for parameters as for items.
pub const NESTING_MESSAGE: &str = "Nesting Levels have exceeded supported limits: \
     Attributes in the item have nested levels beyond supported limit";

/// The refusal of an attribute value with no type: `{}`, a value whose only
/// members the model does not name, or whose members are all null.
const EMPTY_ATTRIBUTE_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";

/// The refusal of an attribute value with two types.
const MULTI_ATTRIBUTE_VALUE: &str = "Supplied AttributeValue has more than one datatypes \
     set, must contain exactly one of the supported datatypes";

/// The refusals of a null member, for each kind of value that holds one
/// (measured 2026-11-06). A null member of a map is named after it.
const NULL_STRING_SET_MEMBER: &str = "One or more parameter values were invalid: \
     An string set may not have a null string as a member";
const NULL_NUMBER_SET_MEMBER: &str =
    "One or more parameter values were invalid: Numeric sets may not contain null";
const NULL_BINARY_SET_MEMBER: &str =
    "One or more parameter values were invalid: Binary sets may not contain null or empty values";
const NULL_MEMBER: &str = "The supplied Item contains a null AttributeValue";

/// The members an attribute value may hold. Any other member is ignored.
const TYPES: [&str; 10] = ["S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"];

/// Check the JSON types of a statement's parameters (wire attribute values),
/// the first of the two passes (see the module documentation). The caller
/// checks every statement of the request this way before it checks
/// anything else of the request.
///
/// The walk visits every map and list. The request body's JSON nesting
/// limit bounds its depth, at most one level for each two JSON levels.
///
/// # Errors
///
/// Returns a description of the first value whose JSON the attribute value
/// model cannot read: a value that is not a JSON object, or a member with a
/// payload of the wrong JSON type.
pub fn check_parameter_types(parameters: &[Value]) -> Result<(), String> {
    parameters.iter().try_for_each(check_types)
}

fn check_types(value: &Value) -> Result<(), String> {
    let Value::Object(members) = value else {
        return Err(AttributeValue::deserialize(value).err().map_or_else(
            || "an attribute value must be a JSON object".to_owned(),
            |e| e.to_string(),
        ));
    };
    for (tag, payload) in typed_members(members) {
        match (tag, payload) {
            ("S" | "N", Value::String(_)) | ("NULL" | "BOOL", Value::Bool(_)) => {}
            ("B", Value::String(text)) => check_base64(text)?,
            ("SS" | "NS", Value::Array(items)) => {
                if items
                    .iter()
                    .any(|item| !item.is_string() && !item.is_null())
                {
                    return Err(format!("{tag} member must be a string"));
                }
            }
            ("BS", Value::Array(items)) => {
                for item in items {
                    match item {
                        Value::String(text) => check_base64(text)?,
                        Value::Null => {}
                        _ => return Err("BS member must be a base64 string".to_owned()),
                    }
                }
            }
            ("M", Value::Object(entries)) => {
                for member in entries.values().filter(|member| !member.is_null()) {
                    check_types(member)?;
                }
            }
            ("L", Value::Array(items)) => {
                for item in items.iter().filter(|item| !item.is_null()) {
                    check_types(item)?;
                }
            }
            (tag, _) => return Err(wrong_payload(tag)),
        }
    }
    Ok(())
}

/// The description of a payload of the wrong JSON type, in the attribute
/// value deserializer's words.
fn wrong_payload(tag: &str) -> String {
    let expected = match tag {
        "S" | "N" => "a string",
        "B" => "a base64 string",
        "NULL" | "BOOL" => "a boolean",
        "M" => "an object",
        _ => "an array",
    };
    format!("{tag} value must be {expected}")
}

fn check_base64(text: &str) -> Result<(), String> {
    BASE64
        .decode(text)
        .map(drop)
        .map_err(|e| format!("invalid base64: {e}"))
}

/// The members of an attribute value the model reads: those it names whose
/// value is not null, in the order the value holds them.
fn typed_members(members: &Map<String, Value>) -> impl Iterator<Item = (&str, &Value)> {
    members
        .iter()
        .filter(|(tag, payload)| !payload.is_null() && TYPES.contains(&tag.as_str()))
        .map(|(tag, payload)| (tag.as_str(), payload))
}

/// `value` as the attribute value model reads it: without the members it
/// ignores, at every level. Its parameters passed [`check_parameter_types`]
/// and [`check_parameter_values`], so the result deserializes.
#[must_use]
pub fn modelled_value(value: &Value) -> Value {
    let Value::Object(members) = value else {
        return value.clone();
    };
    let modelled = typed_members(members).map(|(tag, payload)| {
        let payload = match payload {
            Value::Object(entries) if tag == "M" => Value::Object(
                entries
                    .iter()
                    .map(|(name, member)| (name.clone(), modelled_value(member)))
                    .collect(),
            ),
            Value::Array(items) if tag == "L" => {
                Value::Array(items.iter().map(modelled_value).collect())
            }
            other => other.clone(),
        };
        (tag.to_owned(), payload)
    });
    Value::Object(modelled.collect())
}

/// Check a statement's parameters (wire attribute values) in order, the
/// second of the two passes (see the module documentation). The values passed
/// [`check_parameter_types`].
///
/// # Errors
///
/// Returns the service's message for the first problem: the nesting-depth
/// message, the parameter number message for an `N` or `NS` member, or the
/// refusal of a value for its content (see the module documentation).
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
    let mut typed = typed_members(members);
    let (tag, payload) = match (typed.next(), typed.next()) {
        (None, _) => return Err(EMPTY_ATTRIBUTE_VALUE.to_owned()),
        (Some(_), Some(_)) => return Err(MULTI_ATTRIBUTE_VALUE.to_owned()),
        (Some(member), None) => member,
    };
    match (tag, payload) {
        ("M", Value::Object(entries)) => {
            let next = enter(depth)?;
            for (name, member) in entries {
                if member.is_null() {
                    return Err(format!("{NULL_MEMBER}: {name} is null"));
                }
                check_value(member, next)?;
            }
        }
        ("L", Value::Array(items)) => {
            let next = enter(depth)?;
            for item in items {
                if item.is_null() {
                    return Err(NULL_MEMBER.to_owned());
                }
                check_value(item, next)?;
            }
        }
        ("N", Value::String(text)) => check_text(text)?,
        // The members are converted in order, and a null member is refused
        // where it stands (measured 2026-11-06: `["abc", null]` is refused
        // for `abc`, `[null, "abc"]` for the null).
        ("NS", Value::Array(items)) => {
            for item in items {
                match item {
                    Value::String(text) => check_text(text)?,
                    _ => return Err(NULL_NUMBER_SET_MEMBER.to_owned()),
                }
            }
            if has_equal_numbers(items) {
                return Err(NUMBER_SET_DUPLICATES.to_owned());
            }
            content_refusal(tag, payload)?;
        }
        // A null member is refused ahead of a member given twice (measured
        // 2026-11-06: `["a", "a", null]`).
        ("SS" | "BS", Value::Array(items)) => {
            if items.iter().any(Value::is_null) {
                return Err(if tag == "SS" {
                    NULL_STRING_SET_MEMBER
                } else {
                    NULL_BINARY_SET_MEMBER
                }
                .to_owned());
            }
            content_refusal(tag, payload)?;
        }
        ("NULL", _) => content_refusal(tag, payload)?,
        _ => {}
    }
    Ok(())
}

/// The refusal the attribute value deserializer gives the value of type
/// `tag` that holds `payload`: an empty set, a set that holds a member twice,
/// or a NULL that is not true. The payload has the JSON type its type takes
/// and no null member, so every refusal of the deserializer is one for the
/// content.
fn content_refusal(tag: &str, payload: &Value) -> Result<(), String> {
    let value = Value::Object(Map::from_iter([(tag.to_owned(), payload.clone())]));
    AttributeValue::deserialize(&value)
        .map(drop)
        .map_err(|e| e.to_string())
}

/// Whether two members of a number set have one value.
fn has_equal_numbers(items: &[Value]) -> bool {
    let mut values = HashSet::with_capacity(items.len());
    items.iter().filter_map(Value::as_str).any(|text| {
        !values.insert(validate_and_normalize_number(text).unwrap_or_else(|_| text.to_owned()))
    })
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
    fn a_value_refused_for_its_content_is_met_in_its_place() {
        let empty_ss = "One or more parameter values were invalid: An string set  may not be empty";
        let null_in_map = |name: &str| format!("{NULL_MEMBER}: {name} is null");
        for (json, message) in [
            ("[{\"SS\":[]}]", empty_ss),
            (
                "[{\"NS\":[]}]",
                "One or more parameter values were invalid: An number set  may not be empty",
            ),
            (
                "[{\"BS\":[]}]",
                "One or more parameter values were invalid: Binary sets should not be empty",
            ),
            (
                "[{\"SS\":[\"a\",\"a\"]}]",
                "One or more parameter values were invalid: Input collection [a, a] contains \
                 duplicates.",
            ),
            (
                "[{\"BS\":[\"AQ==\",\"AQ==\"]}]",
                "One or more parameter values were invalid: Input collection [AQ==, AQ==]of type \
                 BS contains duplicates.",
            ),
            ("[{\"NS\":[\"1\",\"1.0\"]}]", NUMBER_SET_DUPLICATES),
            ("[{\"NS\":[\"1\",\"1\"]}]", NUMBER_SET_DUPLICATES),
            ("[{\"NS\":[\"2\",\"1\",\"1.0\"]}]", NUMBER_SET_DUPLICATES),
            (
                "[{\"M\":{\"x\":{\"NS\":[\"1\",\"1.0\"]}}}]",
                NUMBER_SET_DUPLICATES,
            ),
            (
                "[{\"L\":[{\"NS\":[\"1\",\"1.0\"]}]}]",
                NUMBER_SET_DUPLICATES,
            ),
            ("[{\"M\":{\"x\":{\"SS\":[]}}}]", empty_ss),
            ("[{\"L\":[{\"SS\":[]}]}]", empty_ss),
            (
                "[{\"NULL\":false}]",
                "One or more parameter values were invalid: Null attribute value types must have \
                 the value of true",
            ),
            ("[{}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"S\":\"a\",\"N\":\"1\"}]", MULTI_ATTRIBUTE_VALUE),
            ("[{\"S\":\"a\"},{\"SS\":[]}]", empty_ss),
            // A null member of a set (measured 2026-11-06).
            ("[{\"SS\":[\"a\",null]}]", NULL_STRING_SET_MEMBER),
            ("[{\"SS\":[null]}]", NULL_STRING_SET_MEMBER),
            ("[{\"SS\":[\"a\",\"a\",null]}]", NULL_STRING_SET_MEMBER),
            ("[{\"NS\":[\"1\",null]}]", NULL_NUMBER_SET_MEMBER),
            ("[{\"NS\":[null,\"abc\"]}]", NULL_NUMBER_SET_MEMBER),
            ("[{\"NS\":[\"abc\",null]}]", ABC),
            ("[{\"BS\":[\"AQ==\",null]}]", NULL_BINARY_SET_MEMBER),
            (
                "[{\"M\":{\"x\":{\"SS\":[\"a\",null]}}}]",
                NULL_STRING_SET_MEMBER,
            ),
            ("[{\"L\":[{\"NS\":[\"1\",null]}]}]", NULL_NUMBER_SET_MEMBER),
            // A member the model does not name, or whose value is null, is
            // not a type (measured 2026-11-06).
            ("[{\"X\":\"1\"}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"X\":1}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"X\":{\"a\":1}}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"s\":\"a\"}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"S\":null}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"BOOL\":null}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"NULL\":null}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"M\":{\"x\":{\"X\":\"1\"}}}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"L\":[{\"X\":\"1\"}]}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"X\":\"1\"},{\"SS\":[]}]", EMPTY_ATTRIBUTE_VALUE),
            ("[{\"SS\":[]},{\"X\":\"1\"}]", empty_ss),
            ("[{\"M\":{\"a\":{},\"b\":null}}]", EMPTY_ATTRIBUTE_VALUE),
            // A null member of a list, in its place.
            ("[{\"L\":[null]}]", NULL_MEMBER),
            ("[{\"L\":[{\"L\":[null]}]}]", NULL_MEMBER),
            ("[{\"L\":[null,{\"SS\":[]}]}]", NULL_MEMBER),
            ("[{\"L\":[{\"SS\":[]},null]}]", empty_ss),
            ("[{\"L\":[null]},{\"SS\":[]}]", NULL_MEMBER),
            ("[{\"SS\":[]},{\"L\":[null]}]", empty_ss),
            ("[{\"M\":{\"a\":{\"SS\":[]},\"b\":null}}]", empty_ss),
        ] {
            assert_eq!(
                check_parameter_values(&doc(json)),
                Err(message.to_owned()),
                "{json}"
            );
        }
        // A null member of a map is named, at any depth (measured 2026-11-06).
        for (json, name) in [
            ("[{\"M\":{\"x\":null}}]", "x"),
            ("[{\"M\":{\"a\":{\"M\":{\"b\":null}}}}]", "b"),
            ("[{\"L\":[{\"M\":{\"b\":null}}]}]", "b"),
            ("[{\"M\":{\"x\":null,\"y\":{\"SS\":[]}}}]", "x"),
        ] {
            assert_eq!(
                check_parameter_values(&doc(json)),
                Err(null_in_map(name)),
                "{json}"
            );
        }
        let deep_list = deep(40, "{\"S\":\"leaf\"}");
        let empty = "{\"SS\":[]}";
        let bad = "{\"N\":\"abc\"}";
        for (json, message) in [
            (format!("[{empty},{deep_list}]"), empty_ss),
            (format!("[{deep_list},{empty}]"), NESTING_MESSAGE),
            (format!("[{{\"L\":[{empty},{deep_list}]}}]"), empty_ss),
            (
                format!("[{{\"L\":[{deep_list},{empty}]}}]"),
                NESTING_MESSAGE,
            ),
            (format!("[{empty},{bad}]"), empty_ss),
            (format!("[{bad},{empty}]"), ABC),
            (
                format!("[{deep_list},{{\"M\":{{\"x\":null}}}}]"),
                NESTING_MESSAGE,
            ),
        ] {
            assert_eq!(
                check_parameter_values(&doc(&json)),
                Err(message.to_owned()),
                "{json}"
            );
        }
    }

    #[test]
    fn a_value_of_the_wrong_json_type_is_found_ahead_of_any_other_problem() {
        let deep_bool = {
            let mut v = "{\"BOOL\":\"x\"}".to_owned();
            for _ in 0..40 {
                v = format!("{{\"L\":[{v}]}}");
            }
            format!("[{v}]")
        };
        for (json, message) in [
            ("[{\"BOOL\":\"x\"}]", "BOOL value must be a boolean"),
            ("[{\"NULL\":\"x\"}]", "NULL value must be a boolean"),
            ("[{\"BOOL\":1}]", "BOOL value must be a boolean"),
            ("[{\"S\":1}]", "S value must be a string"),
            ("[{\"N\":true}]", "N value must be a string"),
            ("[{\"B\":1}]", "B value must be a base64 string"),
            ("[{\"SS\":\"a\"}]", "SS value must be an array"),
            ("[{\"L\":\"x\"}]", "L value must be an array"),
            ("[{\"M\":\"x\"}]", "M value must be an object"),
            ("[{\"M\":[]}]", "M value must be an object"),
            ("[{\"SS\":[1]}]", "SS member must be a string"),
            ("[{\"SS\":[\"a\",null,1]}]", "SS member must be a string"),
            ("[{\"SS\":[{\"S\":\"a\"}]}]", "SS member must be a string"),
            ("[{\"NS\":[1]}]", "NS member must be a string"),
            ("[{\"NS\":[true]}]", "NS member must be a string"),
            ("[{\"BS\":[1]}]", "BS member must be a base64 string"),
            (
                "[{\"M\":{\"x\":{\"SS\":[1]}}}]",
                "SS member must be a string",
            ),
            (
                "[{\"L\":[{\"BS\":[1]}]}]",
                "BS member must be a base64 string",
            ),
            // The content and the nesting of a value come after its JSON
            // types, in every parameter (measured 2026-11-06).
            (
                "[{\"SS\":[]},{\"BOOL\":\"x\"}]",
                "BOOL value must be a boolean",
            ),
            (
                "[{\"N\":\"abc\"},{\"BOOL\":\"x\"}]",
                "BOOL value must be a boolean",
            ),
            (
                "[{\"L\":[{\"SS\":[]},{\"BOOL\":\"x\"}]}]",
                "BOOL value must be a boolean",
            ),
            (deep_bool.as_str(), "BOOL value must be a boolean"),
        ] {
            assert_eq!(
                check_parameter_types(&doc(json)),
                Err(message.to_owned()),
                "{json}"
            );
        }
        for json in [
            "[{\"B\":\"%%%\"}]",
            "[{\"BS\":[\"%%%\"]}]",
            "[{\"B\":\"AQ=\"}]",
        ] {
            let refusal = check_parameter_types(&doc(json)).unwrap_err();
            assert!(refusal.starts_with("invalid base64: "), "{json}: {refusal}");
        }
        let refusal = check_parameter_types(&doc("[\"a\"]")).unwrap_err();
        assert!(refusal.starts_with("invalid type: string"), "{refusal}");
    }

    #[test]
    fn the_type_check_reads_only_the_members_the_model_names() {
        // A member the model does not name is not read, nor is a null
        // member; the walk of the content refuses what it has to.
        for json in [
            "[{\"SS\":[\"a\",null]},{\"NS\":[null]},{\"BS\":[null]}]",
            "[{\"X\":1},{\"X\":[1,{\"S\":1}]},{\"s\":{\"S\":1}}]",
            "[{\"S\":null},{\"S\":\"a\",\"N\":null},{\"S\":\"a\",\"X\":\"1\"}]",
            "[{\"M\":{\"x\":null}},{\"L\":[null]},{}]",
            "[{\"SS\":[]},{\"N\":\"abc\"},{\"NULL\":false}]",
        ] {
            assert_eq!(check_parameter_types(&doc(json)), Ok(()), "{json}");
        }
    }

    #[test]
    fn a_member_the_model_ignores_is_dropped_at_every_level() {
        // (measured 2026-11-06: each is stored as the value without it)
        for (json, modelled) in [
            ("{\"S\":\"a\",\"X\":\"1\"}", "{\"S\":\"a\"}"),
            ("{\"S\":\"a\",\"N\":null}", "{\"S\":\"a\"}"),
            (
                "{\"M\":{\"x\":{\"S\":\"a\",\"X\":\"1\"}}}",
                "{\"M\":{\"x\":{\"S\":\"a\"}}}",
            ),
            (
                "{\"L\":[{\"S\":\"a\",\"X\":[1]},{\"N\":\"1\",\"B\":null}]}",
                "{\"L\":[{\"S\":\"a\"},{\"N\":\"1\"}]}",
            ),
            ("{\"SS\":[\"a\"]}", "{\"SS\":[\"a\"]}"),
        ] {
            let value: Value = serde_json::from_str(json).unwrap();
            let params = [value.clone()];
            assert_eq!(check_parameter_types(&params), Ok(()), "{json}");
            assert_eq!(check_parameter_values(&params), Ok(()), "{json}");
            let want: Value = serde_json::from_str(modelled).unwrap();
            assert_eq!(modelled_value(&value), want, "{json}");
            assert!(
                serde_json::from_value::<AttributeValue>(modelled_value(&value)).is_ok(),
                "{json}"
            );
        }
    }

    #[test]
    fn other_types_and_good_numbers_pass() {
        let params = doc(
            "[{\"S\":\"1e99999999999\"},{\"N\":\"12.5\"},{\"NS\":[\"1\",\"2\"]},\
             {\"SS\":[\"abc\"]},{\"BOOL\":true},{\"NULL\":true},{\"B\":\"AQI=\"},\
             {\"SS\":[\"\"]},{\"BS\":[\"\"]}]",
        );
        assert_eq!(check_parameter_types(&params), Ok(()));
        assert_eq!(check_parameter_values(&params), Ok(()));
    }
}
