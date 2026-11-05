// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up, in the service's order.
//!
//! Measured 2026-10-25, 2026-10-30 and 2026-10-31 (us-east-1, each request
//! twice):
//!
//! 1. the constraints the service aggregates into one message: the length of
//!    `AttributesToGet` (at least 1) and the ranges of `Limit` (at least 1),
//!    `Segment` (0 to 999999) and `TotalSegments` (1 to 1000000), each checked
//!    when present, whether or not the other segment field is, every fault in
//!    one message: `1 validation error detected: Value at 'Limit' failed to
//!    satisfy constraint: Member must have value greater than or equal to 1`,
//!    or `2 validation errors detected: ...; ...` in the order
//!    `AttributesToGet`, `Limit`, `Segment`, `TotalSegments`. `Limit: 0` with
//!    `AttributesToGet: []` answers the length clause and then the `Limit`
//!    clause; the empty list alone, with a `FilterExpression`, and with a
//!    `FilterExpression` and a number fault answers the length clause alone,
//!    so the length comes ahead of the number check. A float or a value
//!    outside int32 in any of the three number fields, or a value that is not
//!    one AttributeValue in `ExpressionAttributeValues`, in
//!    `ExclusiveStartKey` or in a `ScanFilter` `AttributeValueList`,
//!    switches the message to the older form, `Value '-2147483648' at 'limit'
//!    ...`, with the value saturated or truncated, in the order
//!    `totalSegments`, `segment`, `limit`; see `validate_constraints`;
//! 2. the expression and non-expression parameter mix, in which a `ScanFilter`
//!    object counts as present whether or not it is empty (`ScanFilter: {}`
//!    with a `FilterExpression` answers the mix, `ScanFilter: {}` alone is
//!    accepted); `AttributesToGet: []` with `ScanFilter: {}` and a
//!    `FilterExpression` answers the length clause, so the constraints come
//!    ahead of the mix;
//! 3. the `ReturnConsumedCapacity` enum;
//! 4. every number in `ExpressionAttributeValues`, `NS` members and values
//!    nested in `L` and `M` included, whether a `FilterExpression` reads the
//!    value, nothing reads it, or the request carries no expression at all; a
//!    Unicode decimal digit counts as the digit it stands for;
//! 5. the nesting depth of `ExpressionAttributeValues`: a value nested 32
//!    levels deep in `L` answers `Nesting Levels have exceeded supported
//!    limits: Attributes in the item have nested levels beyond supported
//!    limit`, 31 levels pass, and a malformed number anywhere is reported
//!    first. The check counts `M` levels the same way as `L` levels; only `L`
//!    was measured with a valid number at the bottom;
//! 6. the `Select` enum, after the nesting check (32 levels with `Select:
//!    "BAD"` answers the nesting text). Both enums invalid answers the
//!    `ReturnConsumedCapacity` error alone, so the two are not aggregated.
//!
//! The faults of `ExclusiveStartKey` come at two of the handler's stages,
//! measured 2026-11-05 (us-east-1, each request twice; a request parameter
//! check, an expression check and the table's indexes beside each class of
//! fault, and each key alone):
//!
//! - the shape faults, after the request parameter checks
//!   (`ExpressionAttributeValues` or `ExpressionAttributeNames` with no
//!   expression, `ConditionalOperator` with no `ScanFilter`, a `ScanFilter`
//!   condition's value count, the parameter mix) and ahead of the expression
//!   checks: a value that is empty, carries a type descriptor the service does
//!   not have (`X`, a lower-case `s`) or more than one, at the top of a member
//!   or inside its `L` or `M`, in a key member or in a member that is not a key
//!   attribute. Text: `The provided starting key is invalid: Supplied
//!   AttributeValue is empty, must contain exactly one of the supported
//!   datatypes`, or the same prefix and `Supplied AttributeValue has more than
//!   one datatypes set, must contain exactly one of the supported datatypes`.
//!   Beside such a value the constraint clauses take the older form (D1);
//! - the value faults, after the expression and request checks (the filter
//!   syntax, an undefined value, the operand types, the projection syntax, the
//!   `Select` rules, the unused values, the `Segment` pairing) and ahead of the
//!   table lookup, with no prefix: a number that does not parse (`The parameter
//!   cannot be converted to a numeric value: bob`, in an `N`, an `NS` member,
//!   an `L` or `M` member, a non-key member or an index key), an empty set
//!   (`One or more parameter values were invalid: An number set  may not be
//!   empty`, `... An string set  may not be empty`, `... Binary sets should
//!   not be empty`) and a set with duplicates (`... Input collection contains
//!   duplicates.` for an `NS`, `["1", "1"]` and `["1", "1.0"]` alike; `...
//!   Input collection ["a", "a"] contains duplicates.` for an `SS`). A value
//!   of `ExpressionAttributeValues` the deserializer refuses (an empty value)
//!   is reported after them and ahead of the table lookup.
//!
//! See [`StartKeyBody`] and [`ExpressionValuesBody`]. With faults in two
//! members the service names one of them by the attribute names and, on some
//! tables, the request's order: `{"pk": {}, "sk": {"N": "bob"}}` answered the
//! number on 2026-10-31 and `{"a": {}, "b": {"N": "bob"}}` the empty value on
//! 2026-11-05. The code reports a shape fault in any member ahead of a value
//! fault in any member, then the members in sorted order; no test pins which
//! member. Inside one value the members of an `M` are read in the JSON map's
//! order and those of an `NS` in the request's order; the service named `bob`
//! and then `fred` for `{"M": {"b": {"N": "fred"}, "a": {"N": "bob"}}}` and
//! `fred` twice for `{"NS": ["fred", "bob"]}` in a non-key member.
//!
//! Not measured, and checked in the order listed above: the `AttributesToGet`
//! length clause beside a `Segment` or `TotalSegments` fault without a `Limit`
//! fault (listed first, as beside `Limit`), the older form of the length
//! clause (the empty list echoed, `Value '[]' at 'attributesToGet' ...`, and
//! listed last, as the older form reverses the newer one), the length clause
//! against the enums, `M` levels in the nesting count, and the nesting of a
//! value an expression reads or of a request with no expression. Not measured
//! at the key's stages: a `BS` with duplicates (the deserializer's text); a
//! number fault and a duplicate in one `NS` (the number first); an `SS` or `BS`
//! member that is not a string (the deserializer's text at the value stage); a
//! value of `ExpressionAttributeValues` the deserializer refuses for a reason
//! other than an empty value (two descriptors, an unknown descriptor, a set),
//! reported with the empty value's; and the nesting check beside a key shape
//! fault, where the service answered the key's text for a value the filter
//! reads (2026-11-05) and the nesting text ahead of the `Select` enum for an
//! unused value (2026-10-30): the check keeps its request-time place.
//!
//! The JSON parser accepts 128 levels of nesting, which is 62 `L` levels; a
//! body nested deeper is a `SerializationException` from the parser, ahead of
//! every check here. The service answers the nesting text from 32 to 62 levels
//! and, from 63 to 100 levels, `ExpressionAttributeValues contains invalid
//! value: Nesting Levels have exceeded supported limits: Attributes in the
//! item have nested levels beyond supported limit for key :d` (measured
//! 2026-10-30), which this module does not reach.
//!
//! Each of these is reported ahead of the next one, ahead of the table lookup,
//! and ahead of the checks the handler makes after deserializing the body
//! (Segment without TotalSegments and the reverse, Segment not below
//! TotalSegments, the undefined placeholder, unused value, filter syntax,
//! IndexName, ConsistentRead, Select with ProjectionExpression and
//! ExclusiveStartKey checks). A field of the wrong JSON type anywhere in the
//! body is reported ahead of all of them, as a `SerializationException`.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, Item, ScanInput, saturated_int32};
use serde_json::Value;

use crate::query_key_values::{
    SCAN_CONDITIONS, TYPE_DESCRIPTORS, has_unreadable_attribute_value, is_shape_fault, non_empty,
    present, shape_fault_first, validate_expression_attribute_value_numbers, validate_number_text,
};

/// The `Scan` checks that run on the raw request body, in the service's order:
/// the `AttributesToGet` length and the `Limit`, `Segment` and `TotalSegments`
/// ranges in one message, the expression and non-expression parameter mix, the
/// `ReturnConsumedCapacity` enum, every number in `ExpressionAttributeValues`,
/// the nesting depth of `ExpressionAttributeValues`, then the `Select` enum.
/// The numbers in `ExclusiveStartKey` are checked later, by [`StartKeyBody`].
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. When a check
/// fails, the body is deserialized as a [`ScanInput`] and a
/// `SerializationException` from that (a field of the wrong JSON shape
/// anywhere in the body) is the answer instead, as the service reports a shape
/// fault ahead of every validation; see [`shape_fault_first`]. A body that
/// passes every check is not deserialized here.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub fn validate_scan_request_body(body: &Value) -> Result<(), DynamoDbError> {
    scan_request_checks(body).map_err(|check| shape_fault_first::<ScanInput>(body, check))
}

/// The `Scan` checks in the service's order; see
/// [`validate_scan_request_body`].
fn scan_request_checks(body: &Value) -> Result<(), DynamoDbError> {
    validate_constraints(body)?;

    extenddb_core::validation::validate_no_expression_param_mixing(
        &[
            ("AttributesToGet", non_empty(body, "AttributesToGet")),
            // An empty ScanFilter object counts as present, as a typed field
            // that deserialized to an empty map is.
            ("ScanFilter", present(body, "ScanFilter")),
            ("ConditionalOperator", present(body, "ConditionalOperator")),
        ],
        &[
            (
                "ProjectionExpression",
                present(body, "ProjectionExpression"),
            ),
            ("FilterExpression", present(body, "FilterExpression")),
        ],
    )?;

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: &["INDEXES", "TOTAL", "NONE"],
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;

    if let Some(values) = attribute_value_map(body, "ExpressionAttributeValues") {
        validate_expression_attribute_value_numbers(&values)?;
        extenddb_core::validation::validate_attribute_values_nesting_depth(values.values())?;
    }

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "Select",
            valid: &[
                "SPECIFIC_ATTRIBUTES",
                "COUNT",
                "ALL_ATTRIBUTES",
                "ALL_PROJECTED_ATTRIBUTES",
            ],
            clause: crate::EnumClause::Named("select"),
        }],
    )
}

/// `field` of the body read as a map of attribute values, or `None` when the
/// field is absent or is not one; [`shape_fault_first`] reports the shape.
fn attribute_value_map(body: &Value, field: &str) -> Option<HashMap<String, AttributeValue>> {
    serde_json::from_value(body.get(field)?.clone()).ok()
}

/// The service's prefix on a shape fault in an `ExclusiveStartKey` value
/// (measured 2026-10-31 and 2026-11-05: `{"pk": {}, "sk": {"S": "x"}}` answers
/// `The provided starting key is invalid: Supplied AttributeValue is empty,
/// must contain exactly one of the supported datatypes`). A number or set
/// fault in the key carries no prefix (measured 2026-11-05).
const START_KEY_PREFIX: &str = "The provided starting key is invalid: ";

/// The service's text for a value that is empty or carries a type descriptor
/// it does not have (an unknown or lower-case one, measured 2026-11-05).
const EMPTY_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";

/// The service's text for a value with more than one type descriptor.
const MORE_THAN_ONE_DESCRIPTOR: &str = "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes";

/// The prefix of the service's set texts.
const INVALID_PARAMETER_VALUES: &str = "One or more parameter values were invalid: ";

/// The `ExclusiveStartKey` of a `Scan` body as the request wrote it, with the
/// checks the service makes on the key at the key's two stages (measured
/// 2026-11-05, us-east-1; see the module doc):
///
/// - [`StartKeyBody::validate_shape`], after the request parameter checks and
///   ahead of the expression checks: a value that is empty, carries an unknown
///   or lower-case type descriptor or more than one descriptor, at the top of a
///   member or inside its `L` or `M`, in any member of the key, with the prefix
///   `The provided starting key is invalid: `;
/// - [`StartKeyBody::validate_values`], after the expression and request checks
///   and ahead of the table lookup: every number in the key with the value
///   check's rules and text, and the set faults (an empty `NS`, `SS` or `BS`,
///   a set with duplicates), with no prefix.
///
/// A key with such a value does not deserialize as a map of attribute values.
/// [`StartKeyBody::take`] takes it out of the body, so that the typed
/// deserialization of the body does not report it ahead of the checks the
/// service makes first, and keeps the deserializer's fault for a value the two
/// stages do not classify. A key of the wrong JSON shape (not an object, a
/// value that is not an object, an `N` that is not a string) stays in the body,
/// as the service reports a shape fault ahead of every check; see
/// [`shape_fault_first`].
///
/// With faults in two members the service's choice follows the attribute names
/// and, on some tables, the request's member order (measured 2026-11-05 on
/// three tables); the stages here read the members in sorted order, and a
/// shape fault in any member comes ahead of a value fault in any member.
pub(crate) struct StartKeyBody {
    /// The key as the request wrote it; `None` when the body has none.
    raw: Option<Value>,
    /// The deserializer's fault in a key taken out of the body, reported at the
    /// value stage when neither stage classifies the value (an `SS` member that
    /// is not a string, for one).
    deferred: Option<DynamoDbError>,
}

impl StartKeyBody {
    /// Reads the key from `body` and takes it out of the body when it does not
    /// deserialize for a reason that is not a shape fault.
    pub(crate) fn take(body: &mut Value) -> Self {
        let raw = body.get("ExclusiveStartKey").cloned();
        let deferred = match raw
            .as_ref()
            .map(|key| serde_json::from_value::<Item>(key.clone()))
        {
            Some(Err(e)) => match crate::deserialize_error(e) {
                shape if is_shape_fault(&shape) => None,
                DynamoDbError::ValidationException(msg)
                    if msg == EMPTY_VALUE || msg == MORE_THAN_ONE_DESCRIPTOR =>
                {
                    Some(DynamoDbError::ValidationException(format!(
                        "{START_KEY_PREFIX}{msg}"
                    )))
                }
                other => Some(other),
            },
            Some(Ok(_)) | None => None,
        };
        if deferred.is_some()
            && let Some(fields) = body.as_object_mut()
        {
            fields.remove("ExclusiveStartKey");
        }
        Self { raw, deferred }
    }

    /// The key's members in sorted order, so that two faults name the same
    /// member on every backend (the MongoDB build keeps the request's order in
    /// its JSON maps, the others sort them).
    fn members(&self) -> Vec<&Value> {
        let Some(members) = self.raw.as_ref().and_then(Value::as_object) else {
            return Vec::new();
        };
        let mut names: Vec<&String> = members.keys().collect();
        names.sort();
        names.into_iter().map(|name| &members[name]).collect()
    }

    /// The key's shape stage: the first member, in sorted order, whose value is
    /// empty, carries a type descriptor the service does not have or more than
    /// one, at its top or inside an `L` or `M` member, answers the service's
    /// text with the key prefix.
    ///
    /// # Errors
    ///
    /// Returns `DynamoDbError::ValidationException` with the prefixed text.
    pub(crate) fn validate_shape(&self) -> Result<(), DynamoDbError> {
        for member in self.members() {
            if let Some(text) = shape_fault(member) {
                return Err(DynamoDbError::ValidationException(format!(
                    "{START_KEY_PREFIX}{text}"
                )));
            }
        }
        Ok(())
    }

    /// The key's value stage: every number in the key with the value check's
    /// rules and text (an `N`, the members of an `NS`, and the values nested in
    /// `L` and `M`), and the set faults, member by member in sorted order; then
    /// the deserializer's fault in a key [`StartKeyBody::take`] took out of the
    /// body that neither stage classified.
    ///
    /// Inside one value the members of an `M` are read in the JSON map's order
    /// (the request's order on the MongoDB build) and those of an `NS` in the
    /// request's order: the service named `bob` and then `fred` on two sends
    /// of `{"M": {"b": {"N": "fred"}, "a": {"N": "bob"}}}` and `fred` twice for
    /// `{"NS": ["fred", "bob"]}` (2026-11-05), so no order is pinned.
    ///
    /// # Errors
    ///
    /// Returns `DynamoDbError::ValidationException` with the service's text.
    pub(crate) fn validate_values(&self) -> Result<(), DynamoDbError> {
        for member in self.members() {
            validate_key_value(member)?;
        }
        match &self.deferred {
            Some(fault) => Err(fault.clone()),
            None => Ok(()),
        }
    }
}

/// The service's text for the first shape fault in one raw key value, depth
/// first: an object with no type descriptor or with one the service does not
/// have is an empty value, one with more than one descriptor is the
/// more-than-one text, and the members of an `L` or `M` are read the same way.
/// A value that is not an object is a shape fault the deserializer reports.
fn shape_fault(value: &Value) -> Option<&'static str> {
    let fields = value.as_object()?;
    let mut fields = fields.iter();
    match (fields.next(), fields.next()) {
        (None, _) => Some(EMPTY_VALUE),
        (Some(_), Some(_)) => Some(MORE_THAN_ONE_DESCRIPTOR),
        (Some((descriptor, inner)), None) => match (descriptor.as_str(), inner) {
            ("L", Value::Array(members)) => members.iter().find_map(shape_fault),
            ("M", Value::Object(members)) => members.values().find_map(shape_fault),
            (descriptor, _) if TYPE_DESCRIPTORS.contains(&descriptor) => None,
            _ => Some(EMPTY_VALUE),
        },
    }
}

/// The value faults of one raw key value: an `N` text, the members of an `NS`
/// (every number first, then an empty set, then duplicates by numeric value:
/// `["1", "1.0"]` has a duplicate), an empty `SS` or `BS` and their duplicates,
/// and the values nested in `L` and `M`. A member of a set that is not a string
/// holds nothing to check here; the deserializer reports it.
fn validate_key_value(value: &Value) -> Result<(), DynamoDbError> {
    let Some(fields) = value.as_object() else {
        return Ok(());
    };
    let mut fields = fields.iter();
    let (Some((descriptor, inner)), None) = (fields.next(), fields.next()) else {
        return Ok(());
    };
    match (descriptor.as_str(), inner) {
        ("N", Value::String(text)) => validate_number_text(text),
        ("NS", Value::Array(members)) => validate_number_set(members),
        ("SS", Value::Array(members)) => validate_string_set(members),
        ("BS", Value::Array(members)) => validate_binary_set(members),
        ("L", Value::Array(members)) => members.iter().try_for_each(validate_key_value),
        ("M", Value::Object(members)) => members.values().try_for_each(validate_key_value),
        _ => Ok(()),
    }
}

/// A `ValidationException` with the service's prefix for an invalid parameter
/// value.
fn invalid_parameter(text: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("{INVALID_PARAMETER_VALUES}{text}"))
}

/// An `NS` of the key: every member's number text, then `One or more parameter
/// values were invalid: An number set  may not be empty` (the service's two
/// spaces), then `... Input collection contains duplicates.` for two members
/// with one numeric value (measured 2026-11-05 for `["1", "1"]` and `["1",
/// "1.0"]`).
fn validate_number_set(members: &[Value]) -> Result<(), DynamoDbError> {
    let texts: Vec<&str> = members.iter().filter_map(Value::as_str).collect();
    texts
        .iter()
        .try_for_each(|text| validate_number_text(text))?;
    if members.is_empty() {
        return Err(invalid_parameter("An number set  may not be empty"));
    }
    let mut normalized: Vec<String> = texts
        .iter()
        .map(|text| {
            let folded = crate::decimal_digits::fold_decimal_digits(text);
            extenddb_core::validation::number::validate_and_normalize_number(&folded)
                .unwrap_or_else(|_| folded.into_owned())
        })
        .collect();
    normalized.sort_unstable();
    normalized.dedup();
    if normalized.len() != texts.len() {
        return Err(invalid_parameter("Input collection contains duplicates."));
    }
    Ok(())
}

/// An `SS` of the key: `One or more parameter values were invalid: An string
/// set  may not be empty`, then `... Input collection ["a", "a"] contains
/// duplicates.` with every member quoted (measured 2026-11-05).
fn validate_string_set(members: &[Value]) -> Result<(), DynamoDbError> {
    if members.is_empty() {
        return Err(invalid_parameter("An string set  may not be empty"));
    }
    let texts: Vec<&str> = members.iter().filter_map(Value::as_str).collect();
    let mut distinct = texts.clone();
    distinct.sort_unstable();
    distinct.dedup();
    if distinct.len() != texts.len() {
        let listed: Vec<String> = texts
            .iter()
            .map(|text| serde_json::to_string(text).unwrap_or_default())
            .collect();
        return Err(invalid_parameter(&format!(
            "Input collection [{}] contains duplicates.",
            listed.join(", ")
        )));
    }
    Ok(())
}

/// A `BS` of the key: `One or more parameter values were invalid: Binary sets
/// should not be empty` (measured 2026-11-05); duplicates answer the
/// deserializer's text for a binary set, which is not measured for the key.
fn validate_binary_set(members: &[Value]) -> Result<(), DynamoDbError> {
    if members.is_empty() {
        return Err(invalid_parameter("Binary sets should not be empty"));
    }
    let texts: Vec<&str> = members.iter().filter_map(Value::as_str).collect();
    let mut distinct = texts.clone();
    distinct.sort_unstable();
    distinct.dedup();
    if distinct.len() != texts.len() {
        return Err(invalid_parameter(&format!(
            "Input collection [{}]of type BS contains duplicates.",
            texts.join(", ")
        )));
    }
    Ok(())
}

/// The `ExpressionAttributeValues` of a `Scan` body whose values do not all
/// deserialize: an empty value, an unknown type descriptor, two descriptors, an
/// empty or duplicate set. The service reports such a value after the key's
/// value stage and ahead of the table lookup (measured 2026-11-05: `{":a": {}}`
/// read by the filter answers `ExpressionAttributeValues contains invalid
/// value: Supplied AttributeValue is empty, must contain exactly one of the
/// supported datatypes for key :a` with no key and with a valid key, and the
/// key's number or set text beside a key with such a fault). [`Self::take`]
/// puts a readable stand-in in the body for each such value, so that the typed
/// deserialization and the expression checks run on the other values, and
/// keeps the deserializer's fault for the map as the request wrote it;
/// [`Self::validate`] reports it. The request is refused before the scan reads
/// the stand-in. A value of the wrong JSON shape stays in the body.
pub(crate) struct ExpressionValuesBody {
    deferred: Option<DynamoDbError>,
}

impl ExpressionValuesBody {
    /// Replaces each value the deserializer refuses for a reason that is not a
    /// shape fault with a readable stand-in and keeps the deserializer's fault.
    pub(crate) fn take(body: &mut Value) -> Self {
        let Some(Value::Object(entries)) = body.get_mut("ExpressionAttributeValues") else {
            return Self { deferred: None };
        };
        let raw = Value::Object(entries.clone());
        let mut substituted = false;
        for value in entries.values_mut() {
            if let Err(e) = serde_json::from_value::<AttributeValue>(value.clone())
                && !is_shape_fault(&crate::deserialize_error(e))
            {
                *value = serde_json::json!({"S": ""});
                substituted = true;
            }
        }
        if !substituted {
            return Self { deferred: None };
        }
        // The deserializer's own wording and key order for the map as written.
        let probe = serde_json::json!({"TableName": "t", "ExpressionAttributeValues": raw});
        let deferred = serde_json::from_value::<ScanInput>(probe)
            .err()
            .map(crate::deserialize_error);
        Self { deferred }
    }

    /// The deferred fault, at the stage after the key's value checks.
    ///
    /// # Errors
    ///
    /// Returns the deserializer's fault for the first value it refused.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        match &self.deferred {
            Some(fault) => Err(fault.clone()),
            None => Ok(()),
        }
    }
}

/// The constraints the service aggregates into one message: the length of
/// `AttributesToGet` and the ranges of `Limit`, `Segment` and `TotalSegments`,
/// every fault in one message, in one of the service's two forms (measured
/// 2026-10-25 and 2026-10-30, us-east-1).
///
/// When each number field present is an integer inside the int32 range, the
/// newer form: `Value at '<Field>' failed to satisfy constraint: ...` with the
/// field capitalised and no value echoed, in the order `AttributesToGet`,
/// `Limit`, `Segment`, `TotalSegments` (`Limit: 0` with `AttributesToGet: []`
/// answers the length clause, then the `Limit` clause, measured 2026-10-30
/// 12:40 UTC; the length clause beside a `Segment` or `TotalSegments` fault
/// alone is not measured). When any of the three is a float or outside int32,
/// or a value in `ExpressionAttributeValues`, `ExclusiveStartKey` or a
/// `ScanFilter` `AttributeValueList` is not one AttributeValue (see
/// [`has_unreadable_attribute_value`]), the older form: `Value '<n>' at
/// '<field>' ...` with the value as [`saturated_int32`] reads it and the field
/// in lower camel case, in the reverse of the newer form's order:
/// `totalSegments`, `segment`, `limit` (measured 2026-10-30 12:40 UTC with a
/// fault in each of the three, and on 2026-10-25 with `segment` then `limit`
/// and `totalSegments` then `limit`). The older form of the length clause is
/// not measured: the code echoes the empty list, `Value '[]' at
/// 'attributesToGet' ...`, and lists it last.
fn validate_constraints(body: &Value) -> Result<(), DynamoDbError> {
    let numbers: Vec<(&RangeField, &serde_json::Number)> = RANGE_FIELDS
        .iter()
        .filter_map(|range| {
            body.get(range.field)
                .and_then(Value::as_number)
                .map(|number| (range, number))
        })
        .collect();
    let older_form = has_unreadable_attribute_value(body, SCAN_CONDITIONS)
        || numbers.iter().any(|(_, number)| {
            number
                .as_i64()
                .is_none_or(|value| i32::try_from(value).is_err())
        });
    // Each fault as its newer-form and its older-form clause, in the newer
    // form's order.
    let mut faults: Vec<(String, String)> = Vec::new();
    if body
        .get("AttributesToGet")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        faults.push((
            "Value at 'AttributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
            "Value '[]' at 'attributesToGet' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
        ));
    }
    for (range, number) in numbers {
        let value = saturated_int32(number);
        let constraint = if value < range.min {
            format!("greater than or equal to {}", range.min)
        } else if let Some(max) = range.max.filter(|max| value > *max) {
            format!("less than or equal to {max}")
        } else {
            continue;
        };
        faults.push((
            format!(
                "Value at '{}' failed to satisfy constraint: Member must have value {constraint}",
                range.field
            ),
            format!(
                "Value '{value}' at '{}' failed to satisfy constraint: Member must have value {constraint}",
                range.older_field
            ),
        ));
    }
    if faults.is_empty() {
        return Ok(());
    }
    let clauses: Vec<&str> = if older_form {
        faults
            .iter()
            .rev()
            .map(|(_, older)| older.as_str())
            .collect()
    } else {
        faults.iter().map(|(newer, _)| newer.as_str()).collect()
    };
    let count = clauses.len();
    Err(DynamoDbError::ValidationException(format!(
        "{count} validation error{} detected: {}",
        if count == 1 { "" } else { "s" },
        clauses.join("; ")
    )))
}

/// One range-checked `Scan` field: its name in the newer and the older form of
/// the fault, and its bounds.
struct RangeField {
    field: &'static str,
    older_field: &'static str,
    min: i32,
    max: Option<i32>,
}

/// The range-checked fields in the order the newer form reports them.
const RANGE_FIELDS: [RangeField; 3] = [
    RangeField {
        field: "Limit",
        older_field: "limit",
        min: 1,
        max: None,
    },
    RangeField {
        field: "Segment",
        older_field: "segment",
        min: 0,
        max: Some(999_999),
    },
    RangeField {
        field: "TotalSegments",
        older_field: "totalSegments",
        min: 1,
        max: Some(1_000_000),
    },
];

#[cfg(test)]
mod range_tests;
#[cfg(test)]
mod request_tests;
#[cfg(test)]
mod tests;
