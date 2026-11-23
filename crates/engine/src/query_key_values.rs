// SPDX-License-Identifier: Apache-2.0

//! Checks on the values a `Query` carries, as the service applies them before
//! any row is read.
//!
//! Measured on 2026-10-24 (tables with S/S, S/N and B/B keys, a GSI and an
//! LSI) and on 2026-11-11:
//!
//! 1. an `N` value in `ExpressionAttributeValues` that is not a number answers
//!    `The parameter cannot be converted to a numeric value: <value>` (or the
//!    overflow, underflow and 38-digit messages). Every value in the map is
//!    checked, whether the key condition, the filter or nothing reads it. The
//!    check runs at request time, before the table is looked up, in the order
//!    [`validate_query_request_body`] gives;
//! 2. a key condition value whose type is not the key attribute's
//!    `AttributeDefinitions` type answers `One or more parameter values were
//!    invalid: Condition parameter type does not match schema type`, on the
//!    partition and the sort key, on a table, a GSI and an LSI, for `=`, the
//!    ordering operators, `BETWEEN` and `begins_with`. The check runs after the
//!    key schema, filter, projection, unused value and `Select` checks and
//!    before the `ExclusiveStartKey` check; see
//!    [`validate_key_condition_types`].
//!
//! Without these checks a backend compares the value's inner text with the
//! stored key and ignores the type tag, so `{"N": "bob"}` on an S key reads the
//! `bob` partition, while authorization binds no `dynamodb:LeadingKeys` value
//! to a number it cannot parse.

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionMaps, KeyCondition, PathElement, SortKeyCondition,
};
use extenddb_core::types::{
    AttributeDefinition, EMPTY_ATTRIBUTE_VALUE, MULTI_ATTRIBUTE_VALUE, QueryInput,
};
use extenddb_core::validation::number::NOT_A_NUMBER;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

mod unreadable;

pub(crate) use unreadable::{readable_for_shape_check, unreadable, unreadable_value_checks};

/// The service's text for a key condition value of the wrong type.
pub const KEY_CONDITION_TYPE_MISMATCH: &str = "One or more parameter values were invalid: Condition parameter type does not match schema type";

/// The service's text for a `Limit` below 1 on `Query`.
pub const LIMIT_BELOW_ONE: &str = "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1";

/// The service's text for a null value. Core's copy is a private constant of
/// its `partiql` module, which a build without the `partiql` feature leaves
/// out.
const NULL_VALUE: &str = "The supplied Item contains a null AttributeValue";

/// The type descriptors of an AttributeValue, in the order the service reads
/// the payloads of a value with several type descriptors (see
/// [`first_fault`]). Core's list is a private constant of its `partiql`
/// module, as for [`NULL_VALUE`].
const TYPE_DESCRIPTORS: [&str; 10] = ["S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"];

/// The members of the two enums the request-time checks read.
pub(crate) const RETURN_CONSUMED_CAPACITY: &[&str] = &["INDEXES", "TOTAL", "NONE"];
pub(crate) const SELECT: &[&str] = &[
    "SPECIFIC_ATTRIBUTES",
    "COUNT",
    "ALL_ATTRIBUTES",
    "ALL_PROJECTED_ATTRIBUTES",
];

/// The `Query` checks that run on the request body alone, before the body is
/// deserialized and before the table is looked up, in the service's order:
/// `Limit`, the `ReturnConsumedCapacity` enum, the expression and
/// non-expression parameter mix, the `ExpressionAttributeNames` map (see
/// `validate_names`), every number in `ExpressionAttributeValues`, then the
/// `Select` enum (measured 2026-10-24 and 2026-11-11: a `Limit` of 0 is
/// reported ahead of an invalid enum value of either field and of a parameter
/// mix, an invalid `ReturnConsumedCapacity` ahead of a mix with
/// `AttributesToGet`, `QueryFilter`, `ConditionalOperator` or `KeyConditions`
/// and ahead of an `N` value that is not a number, a mix ahead of that value,
/// and that value ahead of an invalid `Select`. Measured 2026-11-19 in either
/// member order: an invalid names map is reported after a `Limit` of 0, an
/// invalid `ReturnConsumedCapacity` and a mix, and ahead of an `N` value that
/// is not a number, a set with a duplicate member and an invalid `Select`).
///
/// A request whose `ExpressionAttributeValues` holds a value the service
/// cannot read as one type takes another order; see
/// `unreadable_value_checks`.
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. When a check
/// fails, each member of the body is deserialized on its own as part of a
/// [`QueryInput`] (see `shape_fault_first`), and a `SerializationException`
/// from that (a member of the wrong JSON type, in any member order) is the
/// answer instead, as the service reports a shape fault ahead of every
/// validation (measured 2026-10-25: a `Limit` of 0 with a
/// string `ExpressionAttributeValues`, a string `ExpressionAttributeNames` with
/// an `N` value that is not a number, and a `Limit` of 0 with `Select: 5` all
/// answer `SerializationException`). A `ValidationException` the deserializer
/// raises (an empty set, a duplicate, an invalid `AttributeValue`) does not
/// preempt the check. In a value the service cannot read as one type, only the
/// parts it cannot read are replaced for that deserialization, so a JSON-type
/// fault in the rest of the value is still a shape fault; see
/// `readable_for_shape_check`. A body that passes every check is not
/// deserialized here.
///
/// The server runs the same function ahead of the IAM decision, so a principal
/// whose `dynamodb:LeadingKeys` fence cannot bind a number that does not parse
/// gets the request's `ValidationException` rather than
/// `AccessDeniedException`. That call goes through this function too, so it
/// answers the shape fault first as well. It comes after the server's
/// cross-account refusal, so a `Query` that names a table ARN in another
/// account answers `AccessDeniedException` without these checks, where the
/// service makes them first for a body it can read (measured 2026-11-12: with
/// such an ARN, `{"N": "bob"}` answers the numeric message and a `Limit` of 0
/// the `Limit` text).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub fn validate_query_request_body(body: &Value) -> Result<(), DynamoDbError> {
    query_request_checks(body)
        .map_err(|check| shape_fault_first::<QueryInput>(&readable_for_shape_check(body), check))
}

/// When a request-time check has failed with `check`, the deserializer's
/// `SerializationException` for a member of `body` if there is one, else
/// `check`. The service reports a member of the wrong JSON type ahead of the
/// checks whatever the member order (measured 2026-11-23, each body twice:
/// `Select: 5` beside `{"X": "1"}` or an empty `NS`, and `{"S": 5}` after an
/// empty `NS` in another value, each with a `Limit` of 0, answer
/// `SerializationException: NUMBER_VALUE cannot be converted to String` on
/// `Query`, and the first of them on `Scan` too). So each member is
/// deserialized on its own as a `T`, beside the body's `TableName` (the one
/// member `T` must have), and each `ExpressionAttributeValues` entry on its
/// own. The answer is the first shape fault in the map's iteration order (see
/// [`first_fault`]). Inside one member or entry, the deserializer stops at the
/// first fault.
///
/// These are not shape faults here:
///
/// - an enum value that is a string but not a member, which deserializes to a
///   `ValidationException`. One of another JSON type is a shape fault
///   (measured 2026-10-25: `Select: 5` with a `Limit` of 0 answers
///   `SerializationException: NUMBER_VALUE cannot be converted to String`);
/// - an AttributeValue with an unknown type descriptor, a value the service
///   cannot read as one type: `Limit: 0` beside `{"X": "1"}` answers the
///   `Limit` `ValidationException` (measured 2026-10-25 and 2026-11-23);
/// - a member the service reads from a JSON type that `T` refuses: a string
///   in `ConsistentRead` or `ScanIndexForward` that the service reads as a
///   boolean (see [`BOOLEAN_TEXTS`]), and any number in `Limit`, `Segment` or
///   `TotalSegments`. Measured 2026-11-23, beside `Select: "NOPE"`:
///   `ConsistentRead: "true"` or `"T"`, `ScanIndexForward: "true"` or `"0"`,
///   and `Limit: 1.5` answer the `Select` text on `Query`, and
///   `ConsistentRead: "true"`, `Segment: 1.5` with `TotalSegments: 2`, and
///   `TotalSegments: 2.5` with `Segment: 0` answer it on `Scan`.
///   `ConsistentRead: "yes"` with a table that does not exist answers
///   `ResourceNotFoundException`. `ConsistentRead: "on"` answers
///   `SerializationException: Unexpected token received from parser`, and
///   `ConsistentRead: 1` `SerializationException: NUMBER_VALUE cannot be
///   converted to Boolean`. The handler's own deserialization still refuses
///   every string in these members.
pub(crate) fn shape_fault_first<T: DeserializeOwned>(
    body: &Value,
    check: DynamoDbError,
) -> DynamoDbError {
    let read = |name: &str, value: Value| {
        let mut one: Map<String, Value> = body
            .get("TableName")
            .map(|table_name| ("TableName".to_owned(), table_name.clone()))
            .into_iter()
            .collect();
        one.insert(name.to_owned(), value);
        shape_fault::<T>(Value::Object(one))
    };
    body.as_object()
        .into_iter()
        .flatten()
        .find_map(|(name, value)| match (name.as_str(), value) {
            ("ConsistentRead" | "ScanIndexForward", Value::String(text))
                if BOOLEAN_TEXTS
                    .iter()
                    .any(|word| word.eq_ignore_ascii_case(text)) =>
            {
                None
            }
            ("Limit" | "Segment" | "TotalSegments", Value::Number(_)) => None,
            ("ExpressionAttributeValues", Value::Object(entries)) => {
                entries.iter().find_map(|(key, entry)| {
                    read(
                        name,
                        Value::Object(Map::from_iter([(key.clone(), entry.clone())])),
                    )
                })
            }
            _ => read(name, value.clone()),
        })
        .unwrap_or(check)
}

/// The strings the service reads as a boolean, in any ASCII case: the first
/// five as true, the others as false. It refuses every other string with
/// `SerializationException`. Measured 2026-11-10 for each of them in
/// `UpdateContinuousBackups` `PointInTimeRecoveryEnabled`, 2026-11-11 for
/// `"yes"` and `"TRUE"` in `GetItem` `ConsistentRead` and `"no"` in `Query`
/// `ScanIndexForward`, and 2026-11-23 on `Query` for `"T"`, `"0"` and the
/// refused `"on"`.
const BOOLEAN_TEXTS: [&str; 10] = ["true", "t", "yes", "y", "1", "false", "f", "no", "n", "0"];

/// The deserializer's `SerializationException` for `body` as a `T`, unless it
/// is the one for an AttributeValue with an unknown type descriptor.
fn shape_fault<T: DeserializeOwned>(body: Value) -> Option<DynamoDbError> {
    match serde_json::from_value::<T>(body).map_err(crate::deserialize_error) {
        Err(DynamoDbError::SerializationException(msg))
            if msg.contains(UNKNOWN_TYPE_DESCRIPTOR) =>
        {
            None
        }
        Err(shape @ DynamoDbError::SerializationException(_)) => Some(shape),
        _ => None,
    }
}

/// The deserializer's text for an AttributeValue whose type descriptor is not
/// one of the service's.
const UNKNOWN_TYPE_DESCRIPTOR: &str = "unknown AttributeValue type descriptor";

/// The `Query` checks in the service's order; see
/// [`validate_query_request_body`].
fn query_request_checks(body: &Value) -> Result<(), DynamoDbError> {
    let values = body
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object);
    if let Some(values) = values
        && values.values().any(unreadable)
    {
        return unreadable_value_checks(body, values, validate_no_parameter_mix);
    }

    if let Some(limit) = body.get("Limit").and_then(Value::as_i64)
        && limit < 1
    {
        // Beside a value that is not one AttributeValue the service answers
        // its older form, with the value echoed (measured 2026-10-25 12:56 UTC
        // on Query and Scan, and 2026-10-31 for the key and the lists).
        if has_unreadable_attribute_value(body, QUERY_CONDITIONS) {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{}' at 'limit' failed to satisfy constraint: \
                 Member must have value greater than or equal to 1",
                limit.clamp(i64::from(i32::MIN), i64::from(i32::MAX))
            )));
        }
        return Err(DynamoDbError::ValidationException(
            LIMIT_BELOW_ONE.to_owned(),
        ));
    }

    // Query stops at the first invalid enum: ReturnConsumedCapacity ahead of
    // the parameter mix, Select after the numbers (measured 2026-08-24 and
    // 2026-11-11).
    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: RETURN_CONSUMED_CAPACITY,
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;

    validate_no_parameter_mix(body)?;
    validate_names(body)?;

    if let Some(values) = values {
        validate_expression_attribute_value_numbers(values)?;
    }

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "Select",
            valid: SELECT,
            clause: crate::EnumClause::Named("select"),
        }],
    )
}

/// The expression and non-expression parameter mix check.
fn validate_no_parameter_mix(body: &Value) -> Result<(), DynamoDbError> {
    let kce_present = present(body, "KeyConditionExpression");
    // KeyConditions combines with FilterExpression and ProjectionExpression, so
    // it joins the mix check only when KeyConditionExpression is also present.
    extenddb_core::validation::validate_no_expression_param_mixing(
        &[
            ("AttributesToGet", non_empty(body, "AttributesToGet")),
            ("QueryFilter", non_empty(body, "QueryFilter")),
            ("ConditionalOperator", present(body, "ConditionalOperator")),
            (
                "KeyConditions",
                non_empty(body, "KeyConditions") && kce_present,
            ),
        ],
        &[
            (
                "ProjectionExpression",
                present(body, "ProjectionExpression"),
            ),
            ("FilterExpression", present(body, "FilterExpression")),
            ("KeyConditionExpression", kce_present),
        ],
    )
}

/// The checks the `ExpressionAttributeNames` deserializer makes, on the raw
/// body: an empty map, a key that is not `#` and a name, a key over 255 bytes,
/// and an empty name, with the deserializer's text. A map or name of another
/// JSON type is a shape fault, which this leaves to the shape fault check.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the deserializer's text.
pub(crate) fn validate_names(body: &Value) -> Result<(), DynamoDbError> {
    let Some(names) = body.get("ExpressionAttributeNames") else {
        return Ok(());
    };
    match extenddb_core::serde_helpers::deserialize_expression_names(names)
        .map_err(crate::deserialize_error)
    {
        Err(fault @ DynamoDbError::ValidationException(_)) => Err(fault),
        _ => Ok(()),
    }
}

/// The key checks of [`validate_names`] alone: an empty map, a key that is not
/// `#` and a name, and a key over 255 bytes. Each name is replaced with a
/// name that is not empty, so an empty name passes here. Beside a value the
/// service cannot read as one type, the service makes the key checks ahead of
/// the values and reports an empty name after them; see
/// `unreadable_value_checks`.
fn validate_name_keys(body: &Value) -> Result<(), DynamoDbError> {
    match body.get("ExpressionAttributeNames") {
        Some(Value::Object(names)) => {
            let keys: Map<String, Value> = names
                .keys()
                .map(|key| (key.clone(), Value::from("a")))
                .collect();
            validate_names(&serde_json::json!({ "ExpressionAttributeNames": keys }))
        }
        _ => validate_names(body),
    }
}

/// Whether `field` is in the body with a value other than `null`, as a typed
/// `Option` field would deserialize to `Some`.
pub(crate) fn present(body: &Value, field: &str) -> bool {
    body.get(field).is_some_and(|v| !v.is_null())
}

/// Whether `field` is a non-empty JSON object or array in the body.
pub(crate) fn non_empty(body: &Value, field: &str) -> bool {
    match body.get(field) {
        Some(Value::Object(map)) => !map.is_empty(),
        Some(Value::Array(items)) => !items.is_empty(),
        _ => false,
    }
}

/// The legacy condition maps of `Scan` whose `AttributeValueList` entries
/// [`has_unreadable_attribute_value`] reads.
pub(crate) const SCAN_CONDITIONS: &[&str] = &["ScanFilter"];

/// The legacy condition maps of `Query` whose `AttributeValueList` entries
/// [`has_unreadable_attribute_value`] reads.
pub(crate) const QUERY_CONDITIONS: &[&str] = &["KeyConditions", "QueryFilter"];

/// Whether the request holds an object that is not one AttributeValue: empty,
/// with a type descriptor the service does not have (a lower-case `s`
/// included), or with more than one, at the top or as a member of an `L` or
/// `M` at any depth. The values read are the `ExpressionAttributeValues`
/// entries, the `ExclusiveStartKey` values and the `AttributeValueList`
/// entries of the `conditions` maps. Beside such a value the service reports a
/// range fault in its older form, and beside a readable value, of the wrong
/// type for a key or not, the newer form.
///
/// Measured on 2026-10-25 at 12:56 UTC: `Limit: 0` beside an entry `{}`,
/// `{"X": "1"}` or `{"S": "x", "N": "1"}` answers `Value '0' at 'limit'` on
/// Query and Scan, and beside an empty or duplicate `NS` the newer form. On
/// 2026-10-30 at 12:40 UTC on Scan and 2026-10-31 at 12:36 UTC on Query:
/// beside an entry `{"L": [{}]}`, `{"M": {"k": {"X": "1"}}}` or `{"s": "x"}`,
/// the older form. On 2026-10-31 between 23:54 and 23:56 UTC: beside `{}` or
/// `{"X": "1"}` as an `ExclusiveStartKey` value or a `ScanFilter`
/// `AttributeValueList` entry on Scan, and `{}` as an `ExclusiveStartKey`
/// value or a `KeyConditions` or `QueryFilter` `AttributeValueList` entry on
/// Query, the older form, for a `Segment` fault too; beside `{"N": "1"}` as
/// the value of an `S` key, `{"N": "1"}` in a `ScanFilter` list and `{"S":
/// "p0"}` in a `KeyConditions` list, the newer form. An `L` or `M` member of
/// an `ExclusiveStartKey` value or of a list entry is read as one of an
/// `ExpressionAttributeValues` entry is; that is not measured.
///
/// A value that is not an object is a shape fault, reported ahead of the
/// checks.
pub(crate) fn has_unreadable_attribute_value(body: &Value, conditions: &[&str]) -> bool {
    let in_map = |field: &str| {
        body.get(field)
            .and_then(Value::as_object)
            .is_some_and(|values| values.values().any(is_unreadable_attribute_value))
    };
    let in_lists = |field: &&str| {
        body.get(*field)
            .and_then(Value::as_object)
            .is_some_and(|conditions| {
                conditions.values().any(|condition| {
                    condition
                        .get("AttributeValueList")
                        .and_then(Value::as_array)
                        .is_some_and(|list| list.iter().any(is_unreadable_attribute_value))
                })
            })
    };
    in_map("ExpressionAttributeValues")
        || in_map("ExclusiveStartKey")
        || conditions.iter().any(in_lists)
}

/// Whether `entry` is an object that is not one AttributeValue, or an `L` or
/// `M` with such a member; see [`has_unreadable_attribute_value`].
fn is_unreadable_attribute_value(entry: &Value) -> bool {
    let Some(descriptors) = entry.as_object() else {
        return false;
    };
    let mut fields = descriptors.iter();
    match (fields.next(), fields.next()) {
        (Some((descriptor, value)), None) => match descriptor.as_str() {
            "L" => value
                .as_array()
                .is_some_and(|members| members.iter().any(is_unreadable_attribute_value)),
            "M" => value
                .as_object()
                .is_some_and(|members| members.values().any(is_unreadable_attribute_value)),
            descriptor => !TYPE_DESCRIPTORS.contains(&descriptor),
        },
        _ => true,
    }
}

/// The entries of `values` by placeholder, so the reported value is
/// deterministic when several are invalid.
pub(crate) fn sorted_entries(values: &Map<String, Value>) -> Vec<(&String, &Value)> {
    let mut entries: Vec<(&String, &Value)> = values.iter().collect();
    entries.sort_by_key(|(key, _)| *key);
    entries
}

/// The first fault inside one value, in document order for `L` items and in
/// the map's iteration order for `M` members (see [`first_fault`]).
enum Fault<'a> {
    /// A null, with the member name when it is the value of an `M` member.
    Null(Option<&'a str>),
    /// No type descriptor the service knows.
    Empty,
    /// More than one type descriptor, with no fault in their payloads.
    SeveralTypes,
    /// An `N` text or `NS` member that is not a number, as the request wrote it.
    NotANumber(&'a str),
    /// A number the number rules refuse for its magnitude or precision, with
    /// their text.
    Refused(String),
}

impl Fault<'_> {
    /// The request-time number check's text: the value echoed with each
    /// decimal digit folded to ASCII (measured 2026-11-11: `{"N": "\u{ff11}bob"}`
    /// answers `The parameter cannot be converted to a numeric value: 1bob`,
    /// `\u{661}\u{662}x` answers `12x`), and the colon kept after an empty one.
    fn request_time_text(self) -> String {
        match self {
            Fault::NotANumber(text) => format!(
                "{NOT_A_NUMBER}: {}",
                crate::decimal_digits::fold_decimal_digits(text)
            ),
            other => other.reason(),
        }
    }

    /// The text [`unreadable_value_checks`] wraps: a number echoed as written
    /// and no colon after an empty one (measured 2026-11-11).
    fn reason(self) -> String {
        match self {
            Fault::Null(None) => NULL_VALUE.to_owned(),
            Fault::Null(Some(member)) => format!("{NULL_VALUE}: {member} is null"),
            Fault::Empty => EMPTY_ATTRIBUTE_VALUE.to_owned(),
            Fault::SeveralTypes => MULTI_ATTRIBUTE_VALUE.to_owned(),
            Fault::NotANumber("") => NOT_A_NUMBER.to_owned(),
            Fault::NotANumber(text) => format!("{NOT_A_NUMBER}: {text}"),
            Fault::Refused(msg) => msg,
        }
    }
}

/// The first fault in `value` (`member` is its name when it is the value of an
/// `M` member): a null or no type descriptor, then the numbers in `N`, `NS`
/// and every value an `L` or `M` holds, then more than one type descriptor.
/// `L` items are visited in document order and `M` members in the map's
/// iteration order: sorted by key without the `preserve_order` feature of
/// `serde_json`, in document order with it, as in the MongoDB build (bson
/// turns the feature on). A value of another JSON type, and an `N` or `NS`
/// payload of the wrong JSON type, are shape faults the deserializer
/// reports. A type descriptor with a null payload counts as absent, as the
/// service reads it (measured 2026-11-19: `{"S": null}` answers the empty
/// value text, and `{"S": null, "N": "1"}` in a `FilterExpression` is
/// accepted).
///
/// With several type descriptors, the service reads each payload in the order
/// of [`TYPE_DESCRIPTORS`], whatever order the request wrote them in, and
/// answers the more than one datatypes text only when no payload has a fault
/// (measured 2026-11-19: `{"N": "bob", "S": "x"}` and `{"S": "x", "N": "bob"}`
/// answer the numeric text for `bob`, `{"N": "1", "S": "x"}` the more than one
/// datatypes text, `{"S": "x", "L": [null]}` the null text, and `N` is read
/// ahead of `NS`, `NS` and `M` ahead of `L`).
fn first_fault<'a>(value: &'a Value, member: Option<&'a str>) -> Option<Fault<'a>> {
    let Value::Object(map) = value else {
        return value.is_null().then_some(Fault::Null(member));
    };
    let payloads: Vec<(&str, &Value)> = TYPE_DESCRIPTORS
        .iter()
        .filter_map(|&descriptor| Some((descriptor, map.get(descriptor).filter(|p| !p.is_null())?)))
        .collect();
    let fault = payloads
        .iter()
        .find_map(|&(descriptor, payload)| match (descriptor, payload) {
            ("N", Value::String(text)) => number_fault(text),
            ("NS", Value::Array(members)) => members
                .iter()
                .filter_map(Value::as_str)
                .find_map(number_fault),
            ("L", Value::Array(items)) => items.iter().find_map(|item| first_fault(item, None)),
            ("M", Value::Object(members)) => members
                .iter()
                .find_map(|(name, inner)| first_fault(inner, Some(name))),
            _ => None,
        });
    match payloads.len() {
        0 => Some(Fault::Empty),
        1 => fault,
        _ => fault.or(Some(Fault::SeveralTypes)),
    }
}

/// `text` against the number rules, with Unicode decimal digits folded for the
/// check; see [`crate::decimal_digits`].
fn number_fault(text: &str) -> Option<Fault<'_>> {
    let folded = crate::decimal_digits::fold_decimal_digits(text);
    match extenddb_core::validation::number::validate_and_normalize_number(&folded) {
        Ok(_) => None,
        Err(DynamoDbError::ValidationException(msg)) if msg.starts_with(NOT_A_NUMBER) => {
            Some(Fault::NotANumber(text))
        }
        Err(DynamoDbError::ValidationException(msg)) => Some(Fault::Refused(msg)),
        Err(other) => Some(Fault::Refused(other.to_string())),
    }
}

/// Refuse the first `N` value in `ExpressionAttributeValues` (including `NS`
/// members and values nested in `L` and `M`) that is not a number, with the
/// bare numeric message. On `Query` the service adds the
/// `ExpressionAttributeValues contains invalid value` wrapper only for a value
/// it cannot read as one type or beside a `Limit` above 2147483647 (measured
/// 2026-11-12). The placeholders are visited in sorted
/// order and each value on its own, so a value of the wrong JSON shape does
/// not stop the check of another (the service picks among several invalid
/// values by an order of its own: measured 2026-11-11, `:a` and `:b` with
/// numbers that are not numbers named either over ten requests). An empty `N`
/// answers the message with the colon and nothing after it, as the service
/// does on `Query`.
///
/// A Unicode decimal digit in the Basic Multilingual Plane counts as the
/// digit it stands for, as the service reads it (measured 2026-10-25: a
/// fullwidth or Arabic-Indic digit is accepted, thirty nine Devanagari ones
/// answer the 38 digit message); see [`crate::decimal_digits`]. The message
/// echoes the value with each such digit folded to ASCII.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub(crate) fn validate_expression_attribute_value_numbers(
    values: &Map<String, Value>,
) -> Result<(), DynamoDbError> {
    match sorted_entries(values)
        .into_iter()
        .find_map(|(_, value)| first_fault(value, None))
    {
        Some(fault) => Err(DynamoDbError::ValidationException(
            fault.request_time_text(),
        )),
        None => Ok(()),
    }
}

/// Refuse a key condition value whose type is not the key's
/// `AttributeDefinitions` type.
///
/// Every value the condition compares is checked: the partition key value(s),
/// the sort key value (or both `BETWEEN` bounds, or the `begins_with` prefix)
/// and the extra equality conditions of a multi-attribute key schema. Each one
/// goes through the check the key schema classifier applies to a clause
/// (`check_key_value_type` in `query.rs`). A path that does not resolve to an
/// attribute name, a value that is not a placeholder, and a key attribute with
/// no definition are left to the checks that already refuse them. When any
/// compared placeholder is undefined the whole check is skipped: the service
/// reports the undefined placeholder first, and the storage layer does so
/// here.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with
/// [`KEY_CONDITION_TYPE_MISMATCH`].
pub fn validate_key_condition_types(
    key_condition: &KeyCondition,
    maps: &ExpressionMaps,
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let mut pairs: Vec<(&[PathElement], &Expr)> =
        vec![(&key_condition.pk_path, &key_condition.pk_value)];
    for (path, value) in &key_condition.extra_pk_conditions {
        pairs.push((path, value));
    }
    match &key_condition.sk_condition {
        Some(SortKeyCondition::Compare { path, value, .. }) => pairs.push((path, value)),
        Some(SortKeyCondition::Between { path, low, high }) => {
            pairs.push((path, low));
            pairs.push((path, high));
        }
        Some(SortKeyCondition::BeginsWith { path, prefix }) => pairs.push((path, prefix)),
        None => {}
    }
    for (path, value) in &key_condition.extra_sk_conditions {
        pairs.push((path, value));
    }

    let any_undefined = pairs.iter().any(
        |(_, value)| matches!(value, Expr::Placeholder(name) if !maps.values.contains_key(name)),
    );
    if any_undefined {
        return Ok(());
    }

    for (path, value) in pairs {
        crate::query::check_key_value_type(path, value, attribute_definitions, maps)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod request_order_tests;

#[cfg(test)]
mod unreadable_tests;

#[cfg(test)]
mod names_tests;
