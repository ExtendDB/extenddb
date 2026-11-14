// SPDX-License-Identifier: Apache-2.0

//! `ExpressionAttributeValues` entries the service cannot read as one type: a
//! null, no type descriptor it knows, more than one, or one it does not know
//! beside one it does, in the value or in any value an `L` or `M` holds. A
//! `Query` with such an entry takes its own order of checks (see
//! [`unreadable_value_checks`]), and the shape fault check reads the entry with
//! those parts replaced (see [`readable_for_shape_check`]).

use std::borrow::Cow;

use extenddb_core::error::DynamoDbError;
use serde_json::{Map, Value};

use super::{
    RETURN_CONSUMED_CAPACITY, SELECT, TYPE_DESCRIPTORS, first_fault, sorted_entries,
    validate_no_parameter_mix,
};

/// Whether the service cannot read `value` as exactly one AttributeValue type:
/// a null, no type descriptor it knows, more than one, or one it does not know
/// beside one it does, in the value or in any value an `L` or `M` holds. A
/// value of another JSON type is a shape fault instead.
pub(super) fn unreadable(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Object(map) => {
            let mut known = map
                .iter()
                .filter(|(key, _)| TYPE_DESCRIPTORS.contains(&key.as_str()));
            match (known.next(), known.next()) {
                (Some((key, inner)), None) => {
                    map.len() > 1
                        || match (key.as_str(), inner) {
                            ("L", Value::Array(items)) => items.iter().any(unreadable),
                            ("M", Value::Object(members)) => members.values().any(unreadable),
                            _ => false,
                        }
                }
                _ => true,
            }
        }
        _ => false,
    }
}

/// `body` for the shape fault check. When its `ExpressionAttributeValues`
/// holds a value the service cannot read as one type, each part the service
/// cannot read is replaced (see [`make_readable`]) and the rest of the value
/// stays, so the deserializer still reports a JSON-type fault in it. Measured
/// 2026-11-11: `{"L": [null, "x"]}`, `{"L": [{"X": "1"}, {"N": 5}]}` and
/// `{"L": [null, {"B": "!!"}]}`, and `{"S": 5, "X": "1"}` and `{"S": 5, "N":
/// "1"}` beside a `Limit` of 0, all answer `SerializationException`.
pub(super) fn readable_for_shape_check(body: &Value) -> Cow<'_, Value> {
    match body
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object)
    {
        Some(values) if values.values().any(unreadable) => {
            let mut body = body.clone();
            if let Some(Value::Object(values)) = body.get_mut("ExpressionAttributeValues") {
                values.values_mut().for_each(make_readable);
            }
            Cow::Owned(body)
        }
        _ => Cow::Borrowed(body),
    }
}

/// Replace each part of `value` the service cannot read as one type, in the
/// value and in the values an `L` or `M` holds. A null, or an object with no
/// type descriptor the service knows, becomes `{"NULL": true}`. An unknown
/// descriptor beside a known one is dropped. An object with several known
/// descriptors becomes an `L` of one value per descriptor, so the deserializer
/// still reads each payload (measured 2026-11-11: `{"L": [{"S": "a", "N":
/// "1"}, "x"]}` and `{"S": "a", "N": 1}` answer `SerializationException`). A
/// value of another JSON type is left for the deserializer to report.
fn make_readable(value: &mut Value) {
    let replacement = match value {
        Value::Null => Some(null_value()),
        Value::Object(map) => {
            map.retain(|key, _| TYPE_DESCRIPTORS.contains(&key.as_str()));
            if map.len() > 1 {
                let mut parts: Vec<Value> = std::mem::take(map)
                    .into_iter()
                    .map(|entry| Value::Object(Map::from_iter([entry])))
                    .collect();
                parts.iter_mut().for_each(make_readable);
                Some(Value::Object(Map::from_iter([(
                    "L".to_owned(),
                    Value::Array(parts),
                )])))
            } else if let Some((key, payload)) = map.iter_mut().next() {
                match (key.as_str(), payload) {
                    ("L", Value::Array(items)) => items.iter_mut().for_each(make_readable),
                    ("M", Value::Object(members)) => members.values_mut().for_each(make_readable),
                    _ => {}
                }
                None
            } else {
                Some(null_value())
            }
        }
        _ => None,
    };
    if let Some(replacement) = replacement {
        *value = replacement;
    }
}

/// `{"NULL": true}`, a value the deserializer reads.
fn null_value() -> Value {
    Value::Object(Map::from_iter([("NULL".to_owned(), Value::Bool(true))]))
}

/// The checks for a request whose `ExpressionAttributeValues` holds a value
/// the service cannot read as one type. Measured 2026-11-11, the service then
/// answers in this order:
///
/// 1. an invalid `Select`, an invalid `ReturnConsumedCapacity` and a `Limit`
///    below 1, reported together in that order, with the `Limit` text naming
///    the value: `2 validation errors detected: Value 'bad' at
///    'returnConsumedCapacity' failed to satisfy constraint: ...; Value '0' at
///    'limit' failed to satisfy constraint: Member must have value greater
///    than or equal to 1`;
/// 2. the expression and non-expression parameter mix;
/// 3. the first fault inside a value, in document order for `L` items and in
///    key order for `M` members, as `ExpressionAttributeValues contains
///    invalid value: <reason> for key <placeholder>`: a null (`The supplied
///    Item contains a null AttributeValue`, with `: <member> is null` when it
///    is the value of an `M` member), no type descriptor, more than one, or an
///    `N` that is not a number. Such an `N` is echoed as written, and an empty
///    one has no colon after the message. No `M` with more than one member was
///    measured.
///
/// When several values have a fault, the service names a value it cannot read
/// as one type ahead of a number fault in another value. `:u` with `{"N":
/// "bob"}` beside `:z` with `{"X": "1"}` names `:z` in 28 of 28 requests, and
/// twelve other shapes of `:z` beside that `:u` name `:z` too. Five shapes of
/// `:a` beside that `:u` name `:a`. So the values the service cannot read are
/// visited first, then the others, each group in placeholder order. Three
/// measured requests name another value than this order does. `:a` and `:b`
/// with numbers that are not numbers beside `:z` with `{"X": "1"}` name `:b`.
/// `:a` with `{"X": "1"}` beside `:b` with a null names `:b`, and `:a` with a
/// null beside `:b` with `{"X": "1"}` names `:b`. This order names `:z`, `:a`
/// and `:a` for them.
pub(super) fn unreadable_value_checks(
    body: &Value,
    values: &Map<String, Value>,
) -> Result<(), DynamoDbError> {
    let mut violations: Vec<String> = [
        enum_violation(body, "Select", "select", SELECT),
        enum_violation(
            body,
            "ReturnConsumedCapacity",
            "returnConsumedCapacity",
            RETURN_CONSUMED_CAPACITY,
        ),
    ]
    .into_iter()
    .flatten()
    .collect();
    if let Some(limit) = body.get("Limit").and_then(Value::as_i64)
        && limit < 1
    {
        violations.push(format!(
            "Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value \
             greater than or equal to 1"
        ));
    }
    if !violations.is_empty() {
        let count = violations.len();
        return Err(DynamoDbError::ValidationException(format!(
            "{count} validation error{} detected: {}",
            if count == 1 { "" } else { "s" },
            violations.join("; ")
        )));
    }

    validate_no_parameter_mix(body)?;

    // The values the service cannot read first; the sort is stable, so each
    // group keeps the placeholder order.
    let mut entries = sorted_entries(values);
    entries.sort_by_key(|(_, value)| !unreadable(value));
    for (key, value) in entries {
        if let Some(fault) = first_fault(value, None) {
            return Err(DynamoDbError::ValidationException(format!(
                "ExpressionAttributeValues contains invalid value: {} for key {key}",
                fault.reason()
            )));
        }
    }
    Ok(())
}

/// The enum violation clause for `field` when the body holds a string that is
/// not one of `valid`.
fn enum_violation(body: &Value, field: &str, api_name: &str, valid: &[&str]) -> Option<String> {
    let value = body.get(field)?.as_str()?;
    (!valid.contains(&value)).then(|| {
        format!(
            "Value '{value}' at '{api_name}' failed to satisfy constraint: Member must satisfy \
             enum value set: [{}]",
            valid.join(", ")
        )
    })
}
