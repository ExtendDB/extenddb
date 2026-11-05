// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up, and the checks on the
//! `ExclusiveStartKey` and `ExpressionAttributeValues` values the handler makes
//! later, each in the service's order.
//!
//! The service follows one of two orders, measured 2026-10-25 to 2026-11-05
//! (us-east-1, each request twice; see [`CheckOrder`]). A request that holds a
//! value that is not one AttributeValue anywhere (empty, with a type descriptor
//! the service does not have, or with two, at the top or inside an `L` or `M`,
//! in `ExpressionAttributeValues`, `ExclusiveStartKey` or a `ScanFilter` list;
//! see [`has_unreadable_attribute_value`]) follows order U; every other request
//! follows order R. No single order fits the measured rows: `{"pk": {}, "sk":
//! {"S": "x"}}` with `Select: "BAD"` answers the `Select` clause, a value nested
//! 32 levels with `Select: "BAD"` answers the nesting text, and the same nested
//! value beside that key answers the key's text.
//!
//! Order R (every value readable):
//!
//! 1. the constraints the service aggregates into one message, in the newer
//!    form: the length of `AttributesToGet` and the ranges of `Limit`,
//!    `Segment` and `TotalSegments` (see [`validate_constraints`]); then the
//!    `ReturnConsumedCapacity` enum, ahead of the parameter mix (2026-11-05);
//! 2. the expression and non-expression parameter mix, in which a `ScanFilter`
//!    object counts as present whether or not it is empty;
//! 3. every number in `ExpressionAttributeValues`, `NS` members and values
//!    nested in `L` and `M` included, whether an expression reads the value or
//!    not; a Unicode decimal digit counts as the digit it stands for;
//! 4. the nesting depth of `ExpressionAttributeValues`: a value nested 32
//!    levels deep answers `Nesting Levels have exceeded supported limits:
//!    Attributes in the item have nested levels beyond supported limit`, 31
//!    levels pass, read or unused, with or without an expression (2026-10-30
//!    and 2026-10-31: ahead of the no-expression check, B4-5a; 2026-11-05:
//!    ahead of the filter syntax, the `Select` enum and the key's number);
//! 5. an empty or duplicate set in `ExpressionAttributeValues` (2026-11-05:
//!    `{":e": {"NS": []}}` with no expression answers the set text ahead of
//!    the no-expression check), see [`ExpressionValuesBody`];
//! 6. the request parameter checks: `ExpressionAttributeNames` with no
//!    expression, then `ExpressionAttributeValues` with no expression, then
//!    `ConditionalOperator` with no `ScanFilter`, then a `ScanFilter`
//!    condition's value count (2026-11-05);
//! 7. the `Select` enum (2026-11-05: after the no-expression check and the
//!    nesting check, ahead of the filter syntax);
//! 8. the expression and request checks: the `FilterExpression` syntax, the
//!    `ProjectionExpression` syntax, an undefined value, the operand types, the
//!    three `Select` rules, the unused values, `COUNT` with `AttributesToGet`,
//!    the `Segment` pairing (2026-11-05, where two were sent together: the
//!    filter syntax first; an undefined value and an operand type ahead of the
//!    unused values, the `Segment` pairing and `SPECIFIC_ATTRIBUTES`; `COUNT`
//!    with `ProjectionExpression` ahead of the unused values; the unused
//!    values and `COUNT` with `AttributesToGet` ahead of the `Segment`
//!    pairing);
//! 9. the `ExclusiveStartKey` value faults: a number that does not parse, an
//!    empty set, a set with duplicates, in the member order of
//!    [`StartKeyBody`], with no prefix and the set members JSON-quoted;
//! 10. the table lookup, the index, `ConsistentRead` on a GSI, the key schema.
//!
//! Order U (the request holds an unreadable value):
//!
//! 1. the constraints in the older form (the value echoed, the field in lower
//!    camel case, `totalSegments`, `segment`, `limit`), then the
//!    `ReturnConsumedCapacity` enum, then the `Select` enum (2026-11-05: ahead
//!    of the key's shape fault and of an unreadable expression value);
//! 2. the parameter mix, then the request parameter checks of order R step 6,
//!    with the mix first (2026-11-05: each ahead of the key's faults);
//! 3. the `ExclusiveStartKey` faults, shape and value together, the first
//!    faulty member in the member order of [`StartKeyBody`]: a shape fault
//!    with the prefix `The provided starting key is invalid: `, a number with
//!    the value check's text, a set fault with the texts of order U (the
//!    members unquoted, the `NS` duplicate text bare, the empty `SS` text with
//!    the key prefix);
//! 4. the `ExpressionAttributeValues` entries the deserializer refuses, read by
//!    an expression or not: `ExpressionAttributeValues contains invalid value:
//!    Supplied AttributeValue is empty, must contain exactly one of the
//!    supported datatypes for key :e` for an empty value and for an unknown
//!    descriptor, the more-than-one text for two descriptors (2026-10-31 B4-7
//!    and 2026-11-05: ahead of the nesting check and after the key's faults);
//! 5. the nesting depth, then the expression and request checks of order R
//!    step 8, then the table.
//!
//! Pairs never sent together keep the code's order: in order R the nesting
//! check against `ExpressionAttributeNames` with no expression,
//! `ConditionalOperator` and the `ScanFilter` value count, a set fault in
//! `ExpressionAttributeValues` against the nesting check and against the
//! `ExpressionAttributeNames` check, the `ProjectionExpression` syntax against
//! an undefined value, an operand type and the unused values; in order U the
//! `Select` enum against the mix, a malformed `ExpressionAttributeValues`
//! number against the key's faults, a set fault in `ExpressionAttributeValues`
//! against the key's faults (reported with the unreadable entries, step 4), an
//! empty `BS` or a `BS` with duplicates in the key (no key prefix, the members
//! unquoted), an empty `SS` in the key beside an unreadable expression value
//! (the key prefix, as beside an unreadable key member). Which entry is named
//! when several `ExpressionAttributeValues` entries are faulty is not measured:
//! the first in sorted order.
//!
//! A field of the wrong JSON type anywhere in the body is reported ahead of
//! all of them, as a `SerializationException` (`"Limit": "5"` with `Select:
//! "BAD"`, 2026-11-05; the service's texts differ from the deserializer's). The
//! service reports a key shape fault ahead of `"ConsistentRead": "yes"`
//! (2026-11-05); the code reads the whole body first and does not.
//!
//! The JSON parser accepts 128 levels of nesting, which is 62 `L` levels; a
//! body nested deeper is a `SerializationException` from the parser, ahead of
//! every check here. The service answers the nesting text from 32 to 62 levels
//! and, from 63 to 100 levels, `ExpressionAttributeValues contains invalid
//! value: Nesting Levels have exceeded supported limits: Attributes in the
//! item have nested levels beyond supported limit for key :d` (measured
//! 2026-10-30), which this module does not reach.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, Item, ScanInput, saturated_int32};
use serde::Deserialize;
use serde_json::Value;

use crate::query_key_values::{
    SCAN_CONDITIONS, TYPE_DESCRIPTORS, has_unreadable_attribute_value, is_shape_fault, non_empty,
    present, shape_fault_first, validate_expression_attribute_value_numbers, validate_number_text,
};

/// Which of the service's two check orders a `Scan` request follows (see the
/// module doc): order U when the request holds a value that is not one
/// AttributeValue anywhere [`has_unreadable_attribute_value`] reads
/// (`ExpressionAttributeValues`, `ExclusiveStartKey`, the `ScanFilter` lists),
/// order R otherwise. The same test picks the older form of the constraint
/// clauses (563, D1). Computed once per request by [`CheckOrder::of`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckOrder {
    /// Every value is readable.
    Readable,
    /// The request holds an unreadable value.
    Unreadable,
}

impl CheckOrder {
    /// The order `body` follows.
    pub(crate) fn of(body: &Value) -> Self {
        if has_unreadable_attribute_value(body, SCAN_CONDITIONS) {
            Self::Unreadable
        } else {
            Self::Readable
        }
    }
}

/// The `Scan` checks that run on the raw request body, in the service's order
/// for `order` (see the module doc): the `AttributesToGet` length and the
/// `Limit`, `Segment` and `TotalSegments` ranges in one message, the
/// `ReturnConsumedCapacity` enum, in order U the `Select` enum, the expression
/// and non-expression parameter mix, every number in
/// `ExpressionAttributeValues`, and in order R the nesting depth of
/// `ExpressionAttributeValues`. The `ExclusiveStartKey` values are checked
/// later, by [`StartKeyBody`], and in order R the `Select` enum by
/// [`SelectBody`].
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
pub(crate) fn validate_scan_request_body(
    body: &Value,
    order: CheckOrder,
) -> Result<(), DynamoDbError> {
    scan_request_checks(body, order).map_err(|check| shape_fault_first::<ScanInput>(body, check))
}

/// The `Scan` checks in the service's order; see
/// [`validate_scan_request_body`].
fn scan_request_checks(body: &Value, order: CheckOrder) -> Result<(), DynamoDbError> {
    validate_constraints(body, order)?;

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: &["INDEXES", "TOTAL", "NONE"],
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;

    if order == CheckOrder::Unreadable {
        validate_select_enum(body)?;
    }

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

    if let Some(values) = attribute_value_map(body, "ExpressionAttributeValues") {
        validate_expression_attribute_value_numbers(&values)?;
        if order == CheckOrder::Readable {
            extenddb_core::validation::validate_attribute_values_nesting_depth(values.values())?;
        }
    }
    Ok(())
}

/// The `Select` enum clause: `1 validation error detected: Value 'BAD' at
/// 'select' failed to satisfy constraint: Member must satisfy enum value set:
/// [...]`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the clause.
fn validate_select_enum(body: &Value) -> Result<(), DynamoDbError> {
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
/// must contain exactly one of the supported datatypes`), and on an empty `SS`
/// in the key in order U (2026-11-05: `{"pk": {}, "sk": {"SS": []}}` answers
/// `The provided starting key is invalid: One or more parameter values were
/// invalid: An string set  may not be empty`). A number, an empty `NS` or a
/// set with duplicates in the key carries no prefix in either order.
const START_KEY_PREFIX: &str = "The provided starting key is invalid: ";

/// The service's text for a value that is empty or carries a type descriptor
/// it does not have (an unknown or lower-case one, measured 2026-11-05).
const EMPTY_VALUE: &str =
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes";

/// The service's text for a value with more than one type descriptor.
const MORE_THAN_ONE_DESCRIPTOR: &str = "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes";

/// The prefix of the service's set texts.
const INVALID_PARAMETER_VALUES: &str = "One or more parameter values were invalid: ";

/// The member names of the body's `ExclusiveStartKey` object in the order the
/// request wrote them, read from the raw request text, since the parsed body
/// sorts object members on every build but the MongoDB one. `None` when the
/// text does not parse, has no key, or the key is not an object; a name that
/// appears twice keeps its first position.
pub(crate) fn start_key_member_order(raw_body: &[u8]) -> Option<Vec<String>> {
    /// The member names of one JSON object, in document order.
    struct MemberNames(Vec<String>);

    impl<'de> Deserialize<'de> for MemberNames {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct NamesVisitor;

            impl<'de> serde::de::Visitor<'de> for NamesVisitor {
                type Value = MemberNames;

                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("an object")
                }

                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> Result<MemberNames, A::Error> {
                    let mut names: Vec<String> = Vec::new();
                    while let Some(name) = map.next_key::<String>()? {
                        map.next_value::<serde::de::IgnoredAny>()?;
                        if !names.contains(&name) {
                            names.push(name);
                        }
                    }
                    Ok(MemberNames(names))
                }
            }

            deserializer.deserialize_map(NamesVisitor)
        }
    }

    /// The body with only the key's member names read.
    #[derive(Deserialize)]
    struct KeyNames {
        #[serde(rename = "ExclusiveStartKey", default)]
        key: Option<MemberNames>,
    }

    serde_json::from_slice::<KeyNames>(raw_body)
        .ok()?
        .key
        .map(|names| names.0)
}

/// The order in which the service visits the members of an `ExclusiveStartKey`
/// with faults in more than one member, fitted to the measured rows: the order
/// of a Java `HashMap` of capacity 4 over the member names, that is
/// `String.hashCode()` of the name, spread as `h ^ (h >>> 16)`, masked to four
/// buckets, the buckets in ascending order and the request's order inside a
/// bucket. `sk` falls in bucket 0, `a` in 1, `b` in 2, `pk`, `gpk`, `id` and
/// `ts` in 3.
///
/// Measured (2026-10-31, 2026-11-04 and 2026-11-05, five tables, every row
/// with a shape fault in one of two faulty members, both request orders): on
/// the `pk`, `sk` table the service names `sk`; on HASH `a`, RANGE `b` it
/// names `a`; on HASH `pk`, RANGE `a` it names `a`; on `ts`, `id` and on `id`,
/// `ts` it names the member first in the request; on the GSI `{"gpk": {},
/// "pk": {"N": "bob"}, "sk": {"S": "x"}}` it names `gpk`. The rule is fitted
/// to those rows, not documented by the service. With two value faults and no
/// shape fault the service's own two sends disagreed on 8 of 22 rows, so no
/// order is pinned there. Keys of four members (an index scan with a range key
/// on the index and on the table) are not measured; the same rule applies.
fn fitted_member_order<'a>(names_in_request_order: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut names: Vec<(u32, usize, &str)> = names_in_request_order
        .enumerate()
        .map(|(position, name)| (java_hash_bucket(name), position, name))
        .collect();
    names.sort_unstable();
    names
        .into_iter()
        .map(|(_, _, name)| name.to_owned())
        .collect()
}

/// The bucket of `name` in a Java `HashMap` of capacity 4:
/// `String.hashCode()` over the UTF-16 code units, spread as `h ^ (h >>> 16)`,
/// masked with 3. The arithmetic wraps as Java's `int` does.
fn java_hash_bucket(name: &str) -> u32 {
    let hash = name.encode_utf16().fold(0u32, |h, unit| {
        h.wrapping_mul(31).wrapping_add(u32::from(unit))
    });
    (hash ^ (hash >> 16)) & 3
}

/// The `ExclusiveStartKey` of a `Scan` body as the request wrote it, with the
/// checks the service makes on the key (measured 2026-10-31 to 2026-11-05,
/// us-east-1; see the module doc):
///
/// - in order U, after the request parameter checks and ahead of the
///   expression values and checks: the first faulty member in the fitted
///   member order ([`fitted_member_order`]), a shape fault (a value that is
///   empty, carries an unknown or lower-case type descriptor or more than one
///   descriptor, at the top of the member or inside its `L` or `M`) with the
///   prefix `The provided starting key is invalid: `, a number that does not
///   parse with the value check's text, a set fault with the order-U texts;
/// - in order R, after the expression and request checks and ahead of the
///   table lookup: the first member in the same order with a number or set
///   fault, with the order-R texts.
///
/// A key with an unreadable or set-faulty value does not deserialize as a map
/// of attribute values. [`StartKeyBody::take`] takes it out of the body, so
/// that the typed deserialization of the body does not report it ahead of the
/// checks the service makes first, and keeps the deserializer's fault for a
/// value the checks here do not classify. A key of the wrong JSON shape (not
/// an object, a value that is not an object, an `N` that is not a string)
/// stays in the body, as the service reports a shape fault ahead of every
/// check; see [`shape_fault_first`].
pub(crate) struct StartKeyBody {
    /// The key as the request wrote it; `None` when the body has none.
    raw: Option<Value>,
    /// The member names in the fitted visiting order.
    members: Vec<String>,
    /// The deserializer's fault in a key taken out of the body, reported at the
    /// key's stage when the checks here classify no member (an `SS` member
    /// that is not a string, for one).
    deferred: Option<DynamoDbError>,
}

impl StartKeyBody {
    /// Reads the key from `body` and takes it out of the body when it does not
    /// deserialize for a reason that is not a shape fault. `request_order` is
    /// the key's member names as the request wrote them
    /// ([`start_key_member_order`]); when it is `None` or does not name the
    /// key's members, the parsed body's order stands in for it.
    pub(crate) fn take(body: &mut Value, request_order: Option<&[String]>) -> Self {
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
        let members = match raw.as_ref().and_then(Value::as_object) {
            Some(key) => {
                let parsed: Vec<&str> = key.keys().map(String::as_str).collect();
                let in_request: Vec<&str> = match request_order {
                    Some(names)
                        if names.len() == parsed.len()
                            && names.iter().all(|name| key.contains_key(name)) =>
                    {
                        names.iter().map(String::as_str).collect()
                    }
                    _ => parsed,
                };
                fitted_member_order(in_request.into_iter())
            }
            None => Vec::new(),
        };
        Self {
            raw,
            members,
            deferred,
        }
    }

    /// The key's members as `(name, value)` in the fitted visiting order.
    fn members(&self) -> impl Iterator<Item = (&str, &Value)> {
        let key = self.raw.as_ref().and_then(Value::as_object);
        self.members
            .iter()
            .filter_map(move |name| key?.get(name).map(|value| (name.as_str(), value)))
    }

    /// The key's checks for `order`: the first faulty member in the fitted
    /// order, a shape fault (order U only) ahead of a value fault inside one
    /// member; then the deserializer's fault in a key [`StartKeyBody::take`]
    /// took out of the body that the checks here did not classify.
    ///
    /// # Errors
    ///
    /// Returns `DynamoDbError::ValidationException` with the service's text.
    pub(crate) fn validate(&self, order: CheckOrder) -> Result<(), DynamoDbError> {
        for (_, value) in self.members() {
            if order == CheckOrder::Unreadable
                && let Some(text) = shape_fault(value)
            {
                return Err(DynamoDbError::ValidationException(format!(
                    "{START_KEY_PREFIX}{text}"
                )));
            }
            if let Some(fault) = value_fault(value) {
                return Err(fault.text(order, Place::Key));
            }
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

/// Where a faulty value sits, for the texts that differ by place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    /// A member of `ExclusiveStartKey`.
    Key,
    /// An entry of `ExpressionAttributeValues`.
    ExpressionValue,
}

/// A value fault in one AttributeValue: a number that does not parse (with the
/// value check's error), an empty set, or a set with duplicates (with its
/// members as written).
enum ValueFault {
    Number(DynamoDbError),
    NumberSetEmpty,
    StringSetEmpty,
    BinarySetEmpty,
    NumberSetDuplicates,
    StringSetDuplicates(Vec<String>),
    BinarySetDuplicates(Vec<String>),
}

impl ValueFault {
    /// The service's text for the fault in `order` at `place` (measured
    /// 2026-11-05 unless noted):
    ///
    /// - a number: `The parameter cannot be converted to a numeric value: bob`;
    /// - an empty `NS`: `One or more parameter values were invalid: An number
    ///   set  may not be empty` (the service's two spaces), both orders;
    /// - an empty `SS`: `... An string set  may not be empty`, with the key
    ///   prefix for a key member in order U;
    /// - an empty `BS`: `... Binary sets should not be empty` (order R; order
    ///   U not measured);
    /// - an `NS` with duplicates: `... Input collection contains duplicates.`
    ///   in order R, `Input collection contains duplicates` in order U;
    /// - an `SS` with duplicates: `... Input collection ["a", "a"] contains
    ///   duplicates.` in order R (the members JSON-quoted, `["a\"b", "a\"b"]`
    ///   for a member with a quote), `... Input collection [a, a] contains
    ///   duplicates.` in order U;
    /// - a `BS` with duplicates: `... Input collection ["YQ==","YQ=="]of type
    ///   BS contains duplicates.` in order R (no space after the comma or
    ///   before `of`); order U is not measured and gets the members unquoted.
    fn text(&self, order: CheckOrder, place: Place) -> DynamoDbError {
        let quoted = |members: &[String], separator: &str| -> String {
            members
                .iter()
                .map(|member| match order {
                    CheckOrder::Readable => serde_json::to_string(member).unwrap_or_default(),
                    CheckOrder::Unreadable => member.clone(),
                })
                .collect::<Vec<String>>()
                .join(separator)
        };
        match self {
            Self::Number(fault) => fault.clone(),
            Self::NumberSetEmpty => invalid_parameter("An number set  may not be empty"),
            Self::StringSetEmpty => {
                let text = invalid_parameter("An string set  may not be empty");
                match (order, place, text) {
                    (
                        CheckOrder::Unreadable,
                        Place::Key,
                        DynamoDbError::ValidationException(msg),
                    ) => DynamoDbError::ValidationException(format!("{START_KEY_PREFIX}{msg}")),
                    (_, _, text) => text,
                }
            }
            Self::BinarySetEmpty => invalid_parameter("Binary sets should not be empty"),
            Self::NumberSetDuplicates => match order {
                CheckOrder::Readable => invalid_parameter("Input collection contains duplicates."),
                CheckOrder::Unreadable => DynamoDbError::ValidationException(
                    "Input collection contains duplicates".to_owned(),
                ),
            },
            Self::StringSetDuplicates(members) => invalid_parameter(&format!(
                "Input collection [{}] contains duplicates.",
                quoted(members, ", ")
            )),
            Self::BinarySetDuplicates(members) => {
                let separator = match order {
                    CheckOrder::Readable => ",",
                    CheckOrder::Unreadable => ", ",
                };
                invalid_parameter(&format!(
                    "Input collection [{}]of type BS contains duplicates.",
                    quoted(members, separator)
                ))
            }
        }
    }
}

/// The first value fault in one raw AttributeValue, depth first: an `N` text,
/// the members of an `NS` (every number first, then an empty set, then
/// duplicates by numeric value: `["1", "1.0"]` has a duplicate), an empty `SS`
/// or `BS` and their duplicates, and the values nested in `L` and `M`. A
/// member of a set that is not a string holds nothing to check here; the
/// deserializer reports it.
fn value_fault(value: &Value) -> Option<ValueFault> {
    let fields = value.as_object()?;
    let mut fields = fields.iter();
    let (Some((descriptor, inner)), None) = (fields.next(), fields.next()) else {
        return None;
    };
    match (descriptor.as_str(), inner) {
        ("N", Value::String(text)) => validate_number_text(text).err().map(ValueFault::Number),
        ("NS", Value::Array(members)) => number_set_fault(members),
        ("SS", Value::Array(members)) => string_set_fault(members),
        ("BS", Value::Array(members)) => binary_set_fault(members),
        ("L", Value::Array(members)) => members.iter().find_map(value_fault),
        ("M", Value::Object(members)) => members.values().find_map(value_fault),
        _ => None,
    }
}

/// A `ValidationException` with the service's prefix for an invalid parameter
/// value.
fn invalid_parameter(text: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("{INVALID_PARAMETER_VALUES}{text}"))
}

/// The members of a set as written, skipping members that are not strings.
fn set_members(members: &[Value]) -> Vec<String> {
    members
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

/// An `NS`: every member's number text first (2026-11-05: `["bob", "1", "1"]`
/// and `["1", "1", "bob"]` answer `bob`), then an empty set, then two members
/// with one numeric value (`["1", "1"]` and `["1", "1.0"]`).
fn number_set_fault(members: &[Value]) -> Option<ValueFault> {
    let texts = set_members(members);
    if let Some(fault) = texts
        .iter()
        .find_map(|text| validate_number_text(text).err())
    {
        return Some(ValueFault::Number(fault));
    }
    if members.is_empty() {
        return Some(ValueFault::NumberSetEmpty);
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
    (normalized.len() != texts.len()).then_some(ValueFault::NumberSetDuplicates)
}

/// An `SS`: an empty set, then duplicates.
fn string_set_fault(members: &[Value]) -> Option<ValueFault> {
    if members.is_empty() {
        return Some(ValueFault::StringSetEmpty);
    }
    let texts = set_members(members);
    if has_duplicates(&texts) {
        return Some(ValueFault::StringSetDuplicates(texts));
    }
    None
}

/// A `BS`: an empty set, then duplicates.
fn binary_set_fault(members: &[Value]) -> Option<ValueFault> {
    if members.is_empty() {
        return Some(ValueFault::BinarySetEmpty);
    }
    let texts = set_members(members);
    if has_duplicates(&texts) {
        return Some(ValueFault::BinarySetDuplicates(texts));
    }
    None
}

/// Whether two members of a set are equal.
fn has_duplicates(texts: &[String]) -> bool {
    let mut distinct: Vec<&String> = texts.iter().collect();
    distinct.sort_unstable();
    distinct.dedup();
    distinct.len() != texts.len()
}

/// The `ExpressionAttributeValues` of a `Scan` body whose entries do not all
/// deserialize: in order U an entry that is not one AttributeValue (empty, an
/// unknown or lower-case type descriptor, two descriptors, at the top or inside
/// an `L` or `M`), in either order an empty or duplicate set. The service
/// reports such an entry at its own stage: in order R the set faults ahead of
/// the no-expression check (2026-11-05, `{":e": {"NS": []}}` with no
/// expression answers `One or more parameter values were invalid: An number
/// set  may not be empty`; `{":a": {"SS": ["a", "a"]}}` read by the filter
/// answers `... Input collection ["a", "a"] contains duplicates.`); in order U
/// the unreadable entries after the key's faults and ahead of the nesting
/// check, read by an expression or not (2026-10-31 B4-7 and 2026-11-05):
/// `ExpressionAttributeValues contains invalid value: Supplied AttributeValue
/// is empty, must contain exactly one of the supported datatypes for key :e`
/// for `{}` and for `{"X": "1"}`, and `... has more than one datatypes set,
/// ... for key :a` for two descriptors. A set fault in order U is not measured
/// and is reported with the unreadable entries. Which entry is named when
/// several are faulty is not measured: the first in sorted order.
///
/// [`Self::take`] puts a readable stand-in in the body for each such entry, so
/// that the typed deserialization and the expression checks run on the other
/// values, and keeps the entry's fault; [`Self::validate`] reports it. The
/// request is refused before the scan reads the stand-in. An entry of the wrong
/// JSON shape stays in the body.
pub(crate) struct ExpressionValuesBody {
    /// The faults of the entries taken out, in sorted order of their names.
    faults: Vec<DynamoDbError>,
}

impl ExpressionValuesBody {
    /// Replaces each entry the checks here classify with a readable stand-in
    /// and keeps its fault for `order`.
    pub(crate) fn take(body: &mut Value, order: CheckOrder) -> Self {
        let Some(Value::Object(entries)) = body.get_mut("ExpressionAttributeValues") else {
            return Self { faults: Vec::new() };
        };
        let mut names: Vec<String> = entries.keys().cloned().collect();
        names.sort_unstable();
        let mut faults = Vec::new();
        for name in names {
            let Some(value) = entries.get_mut(&name) else {
                continue;
            };
            let fault = match shape_fault(value) {
                Some(text) => Some(DynamoDbError::ValidationException(format!(
                    "ExpressionAttributeValues contains invalid value: {text} for key {name}"
                ))),
                None => value_fault(value).map(|fault| fault.text(order, Place::ExpressionValue)),
            };
            if let Some(fault) = fault {
                *value = serde_json::json!({"S": ""});
                faults.push(fault);
            }
        }
        Self { faults }
    }

    /// The first entry's fault, at the stage of `order`.
    ///
    /// # Errors
    ///
    /// Returns the fault of the first entry taken out, in sorted order.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        match self.faults.first() {
            Some(fault) => Err(fault.clone()),
            None => Ok(()),
        }
    }
}

/// The `Select` field of a `Scan` body in order R, where the service checks
/// the enum after the no-expression check and the nesting check and ahead of
/// the filter syntax (2026-11-05): [`Self::take`] takes an invalid value out of
/// the body, so that the typed deserialization does not report it first, and
/// keeps the clause; [`Self::validate`] reports it. In order U the enum is a
/// request-time check ([`validate_scan_request_body`]) and nothing is taken.
pub(crate) struct SelectBody {
    clause: Option<DynamoDbError>,
}

impl SelectBody {
    /// Takes an invalid `Select` string out of `body` in order R.
    pub(crate) fn take(body: &mut Value, order: CheckOrder) -> Self {
        if order == CheckOrder::Unreadable {
            return Self { clause: None };
        }
        let clause = validate_select_enum(body).err();
        if clause.is_some()
            && let Some(fields) = body.as_object_mut()
        {
            fields.remove("Select");
        }
        Self { clause }
    }

    /// The enum clause, at its stage.
    ///
    /// # Errors
    ///
    /// Returns `DynamoDbError::ValidationException` with the clause.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        match &self.clause {
            Some(clause) => Err(clause.clone()),
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
/// or the request follows order U (a value in `ExpressionAttributeValues`,
/// `ExclusiveStartKey` or a `ScanFilter` `AttributeValueList` is not one
/// AttributeValue, see [`CheckOrder`]), the older form: `Value '<n>' at
/// '<field>' ...` with the value as [`saturated_int32`] reads it and the field
/// in lower camel case, in the reverse of the newer form's order:
/// `totalSegments`, `segment`, `limit` (measured 2026-10-30 12:40 UTC with a
/// fault in each of the three, and on 2026-10-25 with `segment` then `limit`
/// and `totalSegments` then `limit`). The older form of the length clause is
/// not measured: the code echoes the empty list, `Value '[]' at
/// 'attributesToGet' ...`, and lists it last.
fn validate_constraints(body: &Value, order: CheckOrder) -> Result<(), DynamoDbError> {
    let numbers: Vec<(&RangeField, &serde_json::Number)> = RANGE_FIELDS
        .iter()
        .filter_map(|range| {
            body.get(range.field)
                .and_then(Value::as_number)
                .map(|number| (range, number))
        })
        .collect();
    let older_form = order == CheckOrder::Unreadable
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
