// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `Scan` checks that run on the request body alone, before the body is
//! deserialized and before the table is looked up, and the checks on the
//! `ExclusiveStartKey`, `ExpressionAttributeValues`, `ScanFilter` and `Select`
//! values the handler makes later, each in the service's order.
//!
//! The service follows one of two orders, measured 2026-10-25 to 2026-11-23
//! (us-east-1, each request twice; see [`CheckOrder`]). A request on the
//! service's older validation path follows order U: one that holds a value
//! the service cannot read as one type in `ExpressionAttributeValues`,
//! `ExclusiveStartKey` or a `ScanFilter` list (a null, no type descriptor it
//! knows, more than one, a null payload, at the top or inside an `L` or `M`;
//! see `has_unreadable`), or whose `Limit`, `Segment` or `TotalSegments` is a
//! float or an integer outside int32. Every other request follows order R. No
//! single order fits the measured rows: `{"pk": {}, "sk": {"S": "x"}}` with
//! `Select: "BAD"` answers the `Select` clause, a value nested 32 levels with
//! `Select: "BAD"` answers the nesting text, and the same nested value beside
//! that key answers the key's text.
//!
//! Order R:
//!
//! 1. the constraints the service aggregates into one message, in the newer
//!    form, the field capitalised and no value echoed: the length of
//!    `AttributesToGet` (at least one name), then the ranges of `Limit` (at
//!    least 1), `Segment` (0 to 999999) and `TotalSegments` (1 to 1000000),
//!    each checked when present (see `int32_member::newer_form`);
//! 2. the `ReturnConsumedCapacity` enum (2026-11-05, 2026-11-19 and
//!    2026-11-23: ahead of each form of the parameter mix);
//! 3. the expression and non-expression parameter mix, in which a `ScanFilter`
//!    object counts as present whether or not it is empty (2026-10-30);
//! 4. the `ExpressionAttributeNames` checks (2026-11-19; see
//!    `validate_names`);
//! 5. every number in `ExpressionAttributeValues`, `NS` members and values
//!    nested in `L` and `M` included, whether an expression reads the value or
//!    not, the entries in placeholder order; a Unicode decimal digit counts as
//!    the digit it stands for (see
//!    `validate_expression_attribute_value_numbers`). The service's answer
//!    varies for a number beside another entry's fault: `{":n": {"N":
//!    "fred"}, ":a": {"N": "bob"}}` answered `fred` in 4 of 6 sends and `bob`
//!    in 2 on 2026-11-23 (7 of 14 each in the review of 2026-11-12), and
//!    `{"NS": ["bob", "1", "1"]}` beside a value nested 32 levels answered
//!    `bob` in 4 of 4 sends on 2026-11-23 and the nesting text in 3 of 6 on
//!    2026-11-12. The numbers of the `ScanFilter` lists are #507's check (#564
//!    item 3, measured 2026-11-05: the number text ahead of a number in the
//!    key);
//! 6. an empty set and a set with a duplicate member in
//!    `ExpressionAttributeValues`, the entries in placeholder order (see
//!    [`validate_sets`]);
//! 7. the nesting depth of `ExpressionAttributeValues`: a value nested 32
//!    levels deep answers `Nesting Levels have exceeded supported limits:
//!    Attributes in the item have nested levels beyond supported limit`, 31
//!    levels pass, read or unused, with or without an expression (2026-10-30
//!    and 2026-10-31: ahead of the no-expression check, B4-5a; 2026-11-05:
//!    ahead of the filter syntax, the `Select` enum and the key's number);
//!    see [`ValuesDepth`];
//! 8. the request parameter checks: `ExpressionAttributeNames` with no
//!    expression, then `ExpressionAttributeValues` with no expression, then
//!    `ConditionalOperator` with no `ScanFilter`, then a `ScanFilter`
//!    condition's value count (2026-11-05);
//! 9. the `Select` enum (2026-11-05: after the no-expression check and the
//!    nesting check, ahead of the filter syntax; see [`SelectBody`]);
//! 10. the expression and request checks: the `FilterExpression` syntax, the
//!     `ProjectionExpression` syntax, an undefined value, the operand types,
//!     the three `Select` rules, the unused values, `COUNT` with
//!     `AttributesToGet`, the `Segment` pairing (2026-11-05, where two were
//!     sent together: the filter syntax first; an undefined value and an
//!     operand type ahead of the unused values, the `Segment` pairing and
//!     `SPECIFIC_ATTRIBUTES`; the unused values ahead of `COUNT` with
//!     `ProjectionExpression`; the unused values and `COUNT` with
//!     `AttributesToGet` ahead of the `Segment` pairing);
//! 11. the `ExclusiveStartKey` value faults: a number that does not parse, an
//!     empty set, a set with duplicates, in the member order of
//!     [`StartKeyBody`], with no prefix;
//! 12. the table lookup, the index, `ConsistentRead` on a GSI, the key schema.
//!
//! Order U:
//!
//! 1. the older form of the constraints, with the enum clauses in the same
//!    message (see `older_form_request_checks`): an invalid `Select`, the
//!    `TotalSegments` fault, an invalid `ReturnConsumedCapacity`, the
//!    `Segment` fault, the `Limit` fault, then an empty `AttributesToGet`,
//!    each value echoed as the service reads it, the field in lower camel
//!    case;
//! 2. the parameter mix, then the `ExpressionAttributeNames` key checks;
//! 3. `ExpressionAttributeNames` and `ExpressionAttributeValues` with no
//!    expression, then the `ScanFilter` list values (see [`ScanFilterBody`]),
//!    then `ConditionalOperator` with no `ScanFilter` and a `ScanFilter`
//!    condition's value count (2026-11-05: each ahead of the key's faults);
//! 4. the `ExclusiveStartKey` faults, the first faulty member in the member
//!    order of [`StartKeyBody`]: a value the service cannot read as one type
//!    with the prefix `The provided starting key is invalid: `, a number with
//!    the number check's text, a set fault with the texts of the older path;
//! 5. the `ExpressionAttributeValues` entries, in the same member order
//!    ([`member_order::fitted_member_order`]), the first faulty one named, every fault in
//!    the form `ExpressionAttributeValues contains invalid value: <text> for
//!    key :e`, then an empty name in `ExpressionAttributeNames` (see
//!    [`ExpressionValuesBody`]);
//! 6. the nesting depth, then the expression and request checks of order R
//!    step 10, then the table.
//!
//! Pairs never sent together keep the code's order: in order R the nesting
//! check against `ExpressionAttributeNames` with no expression,
//! `ConditionalOperator` and the `ScanFilter` value count; which of two set
//! faults in `ExpressionAttributeValues` is named (placeholder order; the
//! service's two sends disagreed, 2026-11-05); the `ProjectionExpression`
//! syntax against an undefined value, an operand type and the unused values.
//! In order U the `Select` enum against the mix (the enum first; the mix
//! beside `Select: "BAD"` and an empty entry answers the `Select` clause,
//! 2026-11-05); an empty value ahead of a malformed number inside one
//! `ExpressionAttributeValues` entry (the empty value, the element order; the
//! other way round is measured); a set fault in a `ScanFilter` list value
//! (the texts of the older path, at the list's step). The service refuses a
//! member or entry name written twice as a `SerializationException`
//! (`Duplicate key found in payload`, 2026-11-05, #630); the parser here keeps
//! the last value.
//!
//! A member of the wrong JSON type anywhere in the body is reported ahead of
//! all of them, as a `SerializationException` (see `shape_fault_first`; the
//! service's texts differ from the deserializer's). The service reports a key
//! shape fault ahead of `"ConsistentRead": "yes"` (2026-11-05); the code reads
//! the whole body first and does not. In order U the service reports an
//! invalid `ScanFilter` `ComparisonOperator` as a constraint clause (`Value
//! 'BAD' at 'scanFilter.a.member.comparisonOperator' failed to satisfy
//! constraint: Member must satisfy enum value set: [...]`, 2026-11-05) ahead
//! of the list's values; the legacy condition checks are #507's and the
//! clause is not made here.
//!
//! The JSON parser accepts 128 levels of nesting, which is 62 `L` levels; a
//! body nested deeper is a `SerializationException` from the parser, ahead of
//! every check here. The service answers the nesting text from 32 to 62 levels
//! and, from 63 to 100 levels, `ExpressionAttributeValues contains invalid
//! value: Nesting Levels have exceeded supported limits: Attributes in the
//! item have nested levels beyond supported limit for key :d` (measured
//! 2026-10-30), which this module does not reach.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, ScanInput};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::int32_member::{ATTRIBUTES_TO_GET, SCAN_RANGES, newer_form, ranges};
use crate::query_key_values::{
    Fault, RETURN_CONSUMED_CAPACITY, SCAN_CONDITIONS, first_fault, has_unreadable, non_empty,
    older_form_request_checks, present, readable_for_shape_check, shape_fault_first,
    sorted_entries, validate_expression_attribute_value_numbers, validate_names, value_shape_fault,
};

mod holders;
mod member_order;

pub(crate) use holders::{ExpressionValuesBody, ScanFilterBody, SelectBody, StartKeyBody};
pub(crate) use member_order::member_orders;

/// Which of the service's two check orders a `Scan` request follows (see the
/// module doc): order U on the service's older validation path, when the
/// request holds a value the service cannot read as one type anywhere
/// `has_unreadable` reads (`ExpressionAttributeValues`, `ExclusiveStartKey`,
/// the `ScanFilter` lists) or a `Limit`, `Segment` or `TotalSegments` that is a
/// float or an integer outside int32 (measured 2026-11-23: `Limit: 1.5` beside
/// `{"N": "bob"}` answers the number in the form of order U step 5); order R
/// otherwise. The same test picks the older form of the constraint clauses
/// (#563).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckOrder {
    /// Every value is readable and every range member an integer inside int32.
    Readable,
    /// The request is on the service's older validation path.
    Unreadable,
}

impl CheckOrder {
    /// The order `body` follows.
    fn of(body: &Value) -> Self {
        if ranges(body, &SCAN_RANGES).older_form || has_unreadable(body, SCAN_CONDITIONS) {
            Self::Unreadable
        } else {
            Self::Readable
        }
    }
}

/// Whether the `Scan` checks include the nesting depth of the
/// `ExpressionAttributeValues` entries, in either order. A `Scan` request's
/// checks do: the service refuses an entry nested 32 levels deep. The checks
/// of the Scan a PartiQL `SELECT` is translated into do not: the service
/// accepts a parameter nested 32 levels deep in the `SELECT`'s `WHERE` clause
/// and refuses one nested 33 levels (measured 2026-11-09). The PartiQL
/// parameter check refuses 33 levels before the statement is read (see
/// `extenddb_core::partiql::params`), and the parser refuses a literal inside
/// 32 maps or lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValuesDepth {
    /// Refuse an entry nested 32 levels deep or more.
    Checked,
    /// Leave the depth of the entries unchecked.
    Unchecked,
}

/// The `Scan` checks that run on the raw request body, in the order the
/// module documentation gives, with the nesting depth checked.
///
/// The checks read the raw body, so a field that does not deserialize (an
/// invalid `Select`, for one) does not stop the ones before it. When a check
/// fails, each member of the body is deserialized on its own as part of a
/// [`ScanInput`], and a `SerializationException` from that is the answer
/// instead, as the service reports a shape fault ahead of every validation;
/// see `shape_fault_first`. For that read the parts of a value that the
/// service cannot read are replaced (see `readable_for_shape_check`), so a
/// null value is not a shape fault. Ahead of the checks each value of
/// `ExpressionAttributeValues`, `ExclusiveStartKey` and the `ScanFilter` lists
/// is deserialized on its own (see [`request_checks`]); the rest of a body
/// that passes every check is not deserialized here, and the handler reads
/// `Limit`, `Segment` and `TotalSegments` with `int32_member::saturate`
/// before it deserializes.
///
/// The server runs the same function ahead of the IAM decision.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text, or
/// `DynamoDbError::SerializationException` for a body of the wrong shape.
pub fn validate_scan_request_body(body: &Value) -> Result<(), DynamoDbError> {
    request_checks(body, ValuesDepth::Checked).map(drop)
}

/// [`validate_scan_request_body`] with the nesting depth checked as `depth`
/// says, and the order the request follows when every check passes.
///
/// A member of the wrong JSON type inside an `ExpressionAttributeValues`
/// entry, an `ExclusiveStartKey` value or a `ScanFilter` list value is found
/// first, each value read on its own (see `value_shape_fault`), so a value
/// fault ahead of it in the same map or list does not hide it, and the
/// holders of the handler never take it out of the body unread.
///
/// # Errors
///
/// As [`validate_scan_request_body`].
pub(crate) fn request_checks(
    body: &Value,
    depth: ValuesDepth,
) -> Result<CheckOrder, DynamoDbError> {
    if let Some(shape) = value_shape_fault(body, SCAN_CONDITIONS) {
        return Err(shape);
    }
    scan_request_checks(body, depth).map_err(|check| {
        shape_fault_first::<ScanInput>(&readable_for_shape_check(body, SCAN_CONDITIONS), check)
    })
}

/// The `Scan` checks in the service's order; see
/// [`validate_scan_request_body`].
fn scan_request_checks(body: &Value, depth: ValuesDepth) -> Result<CheckOrder, DynamoDbError> {
    let order = CheckOrder::of(body);
    let ranges = ranges(body, &SCAN_RANGES);
    let empty_list = body
        .get(ATTRIBUTES_TO_GET.name)
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
        .then_some(&ATTRIBUTES_TO_GET);
    if order == CheckOrder::Unreadable {
        older_form_request_checks(body, &ranges.faults, empty_list, validate_parameter_mix)?;
        return Ok(order);
    }
    if empty_list.is_some() || !ranges.faults.is_empty() {
        return Err(newer_form(empty_list, &ranges.faults));
    }

    crate::validate_enum_fields(
        body,
        &[crate::EnumField {
            json_name: "ReturnConsumedCapacity",
            valid: RETURN_CONSUMED_CAPACITY,
            clause: crate::EnumClause::Named("returnConsumedCapacity"),
        }],
    )?;

    validate_parameter_mix(body)?;
    validate_names(body)?;

    if let Some(values) = body
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object)
    {
        validate_expression_attribute_value_numbers(values)?;
        validate_sets(values)?;
        if depth == ValuesDepth::Checked {
            // Every entry deserializes once the numbers and the sets pass,
            // but one of the wrong JSON type, which the shape fault check
            // reports.
            let values: Vec<AttributeValue> = values
                .values()
                .filter_map(|value| AttributeValue::deserialize(value).ok())
                .collect();
            extenddb_core::validation::validate_attribute_values_nesting_depth(&values)?;
        }
    }
    Ok(order)
}

/// The expression and non-expression parameter mix check. A `ScanFilter`
/// object counts as present whether or not it is empty, as a typed field that
/// deserialized to an empty map is (measured 2026-10-30: `ScanFilter: {}`
/// with a `FilterExpression` answers `Can not use both expression and
/// non-expression parameters in the same request: Non-expression parameters:
/// {ScanFilter} Expression parameters: {FilterExpression}`, and `ScanFilter:
/// {}` alone is accepted).
fn validate_parameter_mix(body: &Value) -> Result<(), DynamoDbError> {
    extenddb_core::validation::validate_no_expression_param_mixing(
        &[
            ("AttributesToGet", non_empty(body, "AttributesToGet")),
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
    )
}

/// The first empty set or set with a duplicate member among the
/// `ExpressionAttributeValues` entries, in placeholder order, with the texts
/// of the newer path (see `SetFault::text`). The service reports these after
/// the `ExpressionAttributeNames` checks and the numbers and ahead of the
/// nesting depth and the `Select` enum (measured 2026-11-19 and 2026-11-23: a
/// duplicate or an empty `NS`, a duplicate `SS` or `BS`, and a duplicate `NS`
/// in an `L` beside `Select: "NOPE"` answer the set text). Beside a value
/// nested 32 levels in another entry the answer varies: `{":e": {"NS": []}}`
/// answered the empty-set text in both sends on 2026-11-05 and in 4 of 4 on
/// 2026-11-23, and the nesting text in 12 of 14 sends on 2026-11-12, and
/// `{":s": {"SS": ["a", "a"]}}` the duplicates text in one of two sends
/// (2026-11-05). Beside a number fault in another value, the service named
/// either value between two identical requests (measured 2026-11-05 and
/// 2026-11-19), and this check comes after the numbers. With two set faults,
/// `{":n": {"NS": []}, ":a": {"SS": []}}`, the service answered the `SS` text
/// in one send and the `NS` text in the other (2026-11-05); the placeholder
/// order names `:a`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the set text.
fn validate_sets(values: &Map<String, Value>) -> Result<(), DynamoDbError> {
    match sorted_entries(values).into_iter().find_map(|(_, value)| {
        match first_fault(value, None, true) {
            Some(Fault::Set(set)) => Some(set.text(false)),
            _ => None,
        }
    }) {
        Some(text) => Err(DynamoDbError::ValidationException(text)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod holder_tests;
#[cfg(test)]
mod key_tests;
#[cfg(test)]
mod range_tests;
#[cfg(test)]
mod request_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
