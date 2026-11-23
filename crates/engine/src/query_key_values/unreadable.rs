// SPDX-License-Identifier: Apache-2.0

//! The service's older validation path. A `Query` or `Scan` takes it when an
//! `AttributeValue` in the request is one the service cannot read as one type
//! (a null, no type descriptor it knows, more than one, one it does not know
//! beside one it does, or one with a null payload, in the value or in any
//! value an `L` or `M` holds) or when `Limit`, `Segment` or `TotalSegments`
//! is a float or an integer outside int32. The values read are the
//! `ExpressionAttributeValues` entries, the `ExclusiveStartKey` values and
//! the `AttributeValueList` entries of the condition maps (see
//! [`has_unreadable`]). On that path the checks take another order (see
//! [`older_form_checks`]), and the shape fault check reads each such value
//! with the parts the service cannot read replaced (see
//! [`readable_for_shape_check`]).

use std::borrow::Cow;

use extenddb_core::error::{DynamoDbError, validation_errors};
use serde_json::{Map, Value};

use super::{
    RETURN_CONSUMED_CAPACITY, SELECT, TYPE_DESCRIPTORS, first_fault, sorted_entries,
    validate_name_keys, validate_names,
};
use crate::int32_member::{
    AT_LEAST_ONE_MEMBER, LIMIT, LengthField, RangeFault, RangeField, SEGMENT, TOTAL_SEGMENTS,
};

/// The condition maps of `Scan` whose `AttributeValueList` entries the
/// service reads.
pub(crate) const SCAN_CONDITIONS: &[&str] = &["ScanFilter"];

/// The condition maps of `Query` whose `AttributeValueList` entries the
/// service reads.
pub(crate) const QUERY_CONDITIONS: &[&str] = &["KeyConditions", "QueryFilter"];

/// Whether the service cannot read `value` as exactly one AttributeValue type:
/// a null, no type descriptor it knows, more than one, one it does not know
/// beside one it does, or one with a null payload, in the value or in any
/// value an `L` or `M` holds (measured 2026-11-19: `{"S": null}` beside a
/// `Limit` of 0 answers `Value '0' at 'limit'`, as `{"X": "1"}` does, and
/// 2026-11-12 and 2026-11-23: a null entry, `{"L": [null]}` and a null `S`
/// payload in an `ExclusiveStartKey` value do the same). A value of another
/// JSON type is a shape fault instead.
pub(crate) fn unreadable(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Object(map) => {
            let mut known = map
                .iter()
                .filter(|(key, _)| TYPE_DESCRIPTORS.contains(&key.as_str()));
            match (known.next(), known.next()) {
                (Some((key, inner)), None) => {
                    map.len() > 1
                        || inner.is_null()
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

/// Whether `body` holds a value the service cannot read as one type (see
/// [`unreadable`]) among its `ExpressionAttributeValues` entries, its
/// `ExclusiveStartKey` values or the `AttributeValueList` entries of its
/// `conditions` maps. Measured 2026-10-25, 2026-10-30 and 2026-10-31
/// (us-east-1): beside such a value a `Limit` or `Segment` fault takes the
/// older form on `Query` and `Scan`, for `{}`, `{"X": "1"}`, `{"S": "x",
/// "N": "1"}`, `{"L": [{}]}`, `{"M": {"k": {"X": "1"}}}` and `{"s": "x"}`
/// as an `ExpressionAttributeValues` entry, and for `{}` and `{"X": "1"}` as
/// an `ExclusiveStartKey` value or a `ScanFilter`, `KeyConditions` or
/// `QueryFilter` list entry; beside a readable value, of the wrong type for
/// a key or not, the newer form. Measured 2026-11-23 (us-east-1): `{"M":
/// {"k": {}}}` as a key value and `{"L": [{}]}` as a `ScanFilter` list entry
/// take the older form too.
pub(crate) fn has_unreadable(body: &Value, conditions: &[&str]) -> bool {
    attribute_values(body, conditions).any(unreadable)
}

/// The `AttributeValue`s of `body` the service reads before its checks: the
/// `ExpressionAttributeValues` entries, the `ExclusiveStartKey` values and
/// the `AttributeValueList` entries of the `conditions` maps.
fn attribute_values<'a>(
    body: &'a Value,
    conditions: &'a [&str],
) -> impl Iterator<Item = &'a Value> {
    let in_maps = ["ExpressionAttributeValues", "ExclusiveStartKey"]
        .into_iter()
        .filter_map(move |field| body.get(field).and_then(Value::as_object))
        .flat_map(Map::values);
    let in_lists = conditions
        .iter()
        .filter_map(move |field| body.get(*field).and_then(Value::as_object))
        .flat_map(Map::values)
        .filter_map(|condition| {
            condition
                .get("AttributeValueList")
                .and_then(Value::as_array)
        })
        .flatten();
    in_maps.chain(in_lists)
}

/// Apply `f` to each `AttributeValue` of [`attribute_values`], in place.
fn attribute_values_mut(body: &mut Value, conditions: &[&str], mut f: impl FnMut(&mut Value)) {
    for field in ["ExpressionAttributeValues", "ExclusiveStartKey"] {
        if let Some(Value::Object(values)) = body.get_mut(field) {
            values.values_mut().for_each(&mut f);
        }
    }
    for field in conditions {
        if let Some(Value::Object(conditions)) = body.get_mut(*field) {
            for condition in conditions.values_mut() {
                if let Some(Value::Array(entries)) = condition.get_mut("AttributeValueList") {
                    entries.iter_mut().for_each(&mut f);
                }
            }
        }
    }
}

/// `body` for the shape fault check. When a value the service cannot read as
/// one type sits among the values [`has_unreadable`] reads, each part the
/// service cannot read is replaced (see [`make_readable`]) and the rest of the
/// value stays, so the deserializer still reports a JSON-type fault in it.
/// Measured 2026-11-11: `{"L": [null, "x"]}`, `{"L": [{"X": "1"}, {"N":
/// 5}]}` and `{"L": [null, {"B": "!!"}]}`, and `{"S": 5, "X": "1"}` and
/// `{"S": 5, "N": "1"}` beside a `Limit` of 0, all answer
/// `SerializationException`.
pub(crate) fn readable_for_shape_check<'a>(body: &'a Value, conditions: &[&str]) -> Cow<'a, Value> {
    if has_unreadable(body, conditions) {
        let mut body = body.clone();
        attribute_values_mut(&mut body, conditions, make_readable);
        Cow::Owned(body)
    } else {
        Cow::Borrowed(body)
    }
}

/// Replace each part of `value` the service cannot read as one type, in the
/// value and in the values an `L` or `M` holds. A null, or an object with no
/// type descriptor the service knows, becomes `{"NULL": true}`. An unknown
/// descriptor beside a known one, and a known one with a null payload, are
/// dropped. An object with several known descriptors becomes an `L` of one
/// value per descriptor, so the deserializer still reads each payload
/// (measured 2026-11-11: `{"L": [{"S": "a", "N": "1"}, "x"]}` and `{"S": "a",
/// "N": 1}` answer `SerializationException`). A
/// value of another JSON type is left for the deserializer to report.
fn make_readable(value: &mut Value) {
    let replacement = match value {
        Value::Null => Some(null_value()),
        Value::Object(map) => {
            map.retain(|key, payload| {
                TYPE_DESCRIPTORS.contains(&key.as_str()) && !payload.is_null()
            });
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

/// The checks for a request on the service's older validation path: one that
/// holds a value the service cannot read as one type (see [`has_unreadable`])
/// or whose `Limit`, `Segment` or `TotalSegments` is a float or an integer
/// outside int32 (`ranges` holds their faults; see `int32_member::ranges`).
/// Measured 2026-11-11 and 2026-11-23, the service then answers in this
/// order:
///
/// 1. an invalid `Select`, the `TotalSegments` fault, an invalid
///    `ReturnConsumedCapacity`, the `Segment` fault and the `Limit` fault,
///    reported together in that order, each value echoed as the service reads
///    it: `5 validation errors detected: Value 'BAD' at 'select' failed to
///    satisfy constraint: ...; Value '1000001' at 'totalSegments' ...; Value
///    'BAD' at 'returnConsumedCapacity' ...; Value '-1' at 'segment' ...;
///    Value '0' at 'limit' failed to satisfy constraint: Member must have
///    value greater than or equal to 1` (measured on `Scan` 2026-11-23 14:51
///    UTC for `Limit: 0.0, Segment: -1, TotalSegments: 1000001` beside both
///    enums; on `Query` 18:45 UTC, `Limit: -2147483649` beside both enums
///    answers `select`, `returnConsumedCapacity`, then `limit` with
///    `-2147483648`). Two invalid enums beside a float `Limit` that satisfies
///    its constraint answer the two enum clauses (measured 2026-11-23 on
///    `Scan` and `Query`). On `Scan` an empty `AttributesToGet` adds `Value
///    '[]' at 'attributesToGet' failed to satisfy constraint: Member must have
///    length greater than or equal to 1` after the `Limit` fault (measured
///    2026-11-23 21:49 UTC, with the five other faults);
/// 2. the expression and non-expression parameter mix, which
///    `validate_parameter_mix` checks with the operation's members (measured
///    2026-11-23 18:45 UTC: on `Scan` a mix beside `Limit: 1.5` and an
///    invalid `Select` answers the `select` clause alone, and on `Scan` and
///    `Query` a mix beside `Limit: 0.0` answers the `limit` clause alone);
/// 3. the `ExpressionAttributeNames` key checks: an invalid key, a key over
///    255 bytes and an empty map (measured 2026-11-19: an invalid names key
///    beside `{"X": "1"}`, `{}` or `{"N": "1", "S": "x"}` answers the names
///    text, in either member order, and beside such a value a `Limit` of 0,
///    an invalid `Select` or `ReturnConsumedCapacity` and a mix are reported
///    ahead of it; 2026-11-23: an invalid key beside `Limit: 0.0` answers the
///    `limit` clause); see [`validate_name_keys`];
/// 4. the first fault inside an `ExpressionAttributeValues` entry, in
///    document order for `L` items and in the map's iteration order for `M`
///    members (see [`first_fault`]), as `ExpressionAttributeValues contains
///    invalid value: <reason> for key <placeholder>`: a null (`The supplied
///    Item contains a null AttributeValue`, with `: <member> is null` when it
///    is the value of an `M` member), no type descriptor, more than one, or
///    an `N` that is not a number. Such an `N` is echoed as written, and an
///    empty one has no colon after the message. No `M` with more than one
///    member was measured. A float `Limit` that satisfies its constraint
///    beside `{"N": "bob"}` answers this wrapped text too, on `Scan` and
///    `Query` (measured 2026-11-23 18:45 UTC);
/// 5. an empty name in `ExpressionAttributeNames` (measured 2026-11-19 in
///    either member order). `{"#a": ""}` beside `{"X": "1"}`, `{}`, `{"N":
///    "1", "S": "x"}`, a null or `{"S": null}` answers that value's text.
///    Beside `{"S": "a", "X": "1"}`, which has no fault of its own, it answers
///    the empty name text. `{"#a": "", "b": "c"}` beside `{"X": "1"}` answers
///    the invalid key text for `b`.
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
///
/// Measured on `Scan` 2026-11-23, each request twice: an invalid `Select` and
/// `ReturnConsumedCapacity` beside `{}` answer both clauses in one message,
/// and a null, `{"L": [null]}` or `{"S": null}` beside an invalid `Select`
/// answers the `Select` text. `:a` with `{}` beside `:n` with `{"N": "bob"}`
/// names `:n` there, where this order names `:a`.
///
/// An `ExclusiveStartKey` value the service cannot read is reported by the
/// handler's start key check, after these. The service reports it ahead of
/// item 4 (measured 2026-11-23 18:52 UTC on `Scan` and `Query`: `{"pk": {}}`
/// beside `{"N": "bob"}` answers `The provided starting key is invalid:
/// Supplied AttributeValue is empty, must contain exactly one of the
/// supported datatypes`). `Scan` makes items 1 to 3 here
/// ([`older_form_request_checks`]) and items 4 and 5 in its handler, after the
/// key's faults, with the entries in the hash order of
/// `scan_request::fitted_member_order`, which names `:z`, `:b`, `:b`, `:b`
/// and `:n` in the rows above, as the service does; see
/// `scan_request::ExpressionValuesBody`.
pub(crate) fn older_form_checks(
    body: &Value,
    values: Option<&Map<String, Value>>,
    ranges: &[RangeFault],
    validate_parameter_mix: fn(&Value) -> Result<(), DynamoDbError>,
) -> Result<(), DynamoDbError> {
    older_form_request_checks(body, ranges, None, validate_parameter_mix)?;

    if let Some(values) = values {
        // The values the service cannot read first; the sort is stable, so
        // each group keeps the placeholder order.
        let mut entries = sorted_entries(values);
        entries.sort_by_key(|(_, value)| !unreadable(value));
        for (key, value) in entries {
            if let Some(fault) = first_fault(value, None, false) {
                return Err(DynamoDbError::ValidationException(format!(
                    "ExpressionAttributeValues contains invalid value: {} for key {key}",
                    fault.reason()
                )));
            }
        }
    }
    validate_names(body)
}

/// Items 1 to 3 of [`older_form_checks`]. `empty_list` is a list member whose
/// length constraint fails (`Scan`'s `AttributesToGet`, see
/// `int32_member::ATTRIBUTES_TO_GET`); its clause comes last in item 1.
pub(crate) fn older_form_request_checks(
    body: &Value,
    ranges: &[RangeFault],
    empty_list: Option<&LengthField>,
    validate_parameter_mix: fn(&Value) -> Result<(), DynamoDbError>,
) -> Result<(), DynamoDbError> {
    let range = |field: &RangeField| {
        ranges
            .iter()
            .find(|fault| fault.field.name == field.name)
            .map(|fault| {
                (
                    fault.value.to_string(),
                    fault.field.older_name,
                    fault.constraint.clone(),
                )
            })
    };
    let violations: Vec<(String, &str, String)> = [
        enum_violation(body, "Select", "select", SELECT),
        range(&TOTAL_SEGMENTS),
        enum_violation(
            body,
            "ReturnConsumedCapacity",
            "returnConsumedCapacity",
            RETURN_CONSUMED_CAPACITY,
        ),
        range(&SEGMENT),
        range(&LIMIT),
        empty_list.map(|list| {
            (
                "[]".to_owned(),
                list.older_name,
                AT_LEAST_ONE_MEMBER.to_owned(),
            )
        }),
    ]
    .into_iter()
    .flatten()
    .collect();
    if !violations.is_empty() {
        let clauses: Vec<(&str, &str, &str)> = violations
            .iter()
            .map(|(value, field, constraint)| (value.as_str(), *field, constraint.as_str()))
            .collect();
        return Err(DynamoDbError::ValidationException(validation_errors(
            &clauses,
        )));
    }

    validate_parameter_mix(body)?;
    validate_name_keys(body)
}

/// The enum violation for `field` when the body holds a string that is not one
/// of `valid`: the value, the field's name in the message and the constraint,
/// for `validation_errors`.
fn enum_violation(
    body: &Value,
    field: &str,
    api_name: &'static str,
    valid: &[&str],
) -> Option<(String, &'static str, String)> {
    let value = body.get(field)?.as_str()?;
    (!valid.contains(&value)).then(|| {
        (
            value.to_owned(),
            api_name,
            format!("Member must satisfy enum value set: [{}]", valid.join(", ")),
        )
    })
}
