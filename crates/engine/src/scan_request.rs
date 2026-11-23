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
//!    `validate_expression_attribute_value_numbers`). The numbers of the
//!    `ScanFilter` lists are #507's check (#564 item 3, measured 2026-11-05:
//!    the number text ahead of a number in the key);
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
//!    ([`fitted_member_order`]), the first faulty one named, every fault in
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
//! (the texts of the older path, at the list's step); an empty name against
//! the key's faults (the key first); nine or more names in one hash bucket
//! (see [`fitted_member_order`]). The service refuses a member or entry name
//! written twice as a `SerializationException` (`Duplicate key found in
//! payload`, 2026-11-05, #630); the parser here keeps the last value.
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

use std::collections::HashSet;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    AttributeValue, EMPTY_ATTRIBUTE_VALUE, Item, MULTI_ATTRIBUTE_VALUE, ScanInput,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::int32_member::{ATTRIBUTES_TO_GET, SCAN_RANGES, newer_form, ranges};
use crate::query_key_values::{
    Fault, RETURN_CONSUMED_CAPACITY, SCAN_CONDITIONS, SELECT, SetFault, first_fault,
    has_unreadable, is_shape_fault, non_empty, older_form_request_checks, present,
    readable_for_shape_check, shape_fault_first, sorted_entries,
    validate_expression_attribute_value_numbers, validate_names, value_shape_fault,
};

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
/// in an `L` beside `Select: "NOPE"` answer the set text; 2026-11-05: `{":d":
/// <32 levels>, ":e": {"NS": []}}` answered the empty-set text in both sends,
/// and `{":d": <32 levels>, ":s": {"SS": ["a", "a"]}}` the duplicates text in
/// one of two). Beside a number fault in another value, the service named
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

/// The service's prefix on a fault in an `ExclusiveStartKey` value it cannot
/// read as one type (measured 2026-10-31 and 2026-11-05: `{"pk": {}, "sk":
/// {"S": "x"}}` answers `The provided starting key is invalid: Supplied
/// AttributeValue is empty, must contain exactly one of the supported
/// datatypes`), and on an empty `SS` or `BS` in the key in order U
/// (2026-11-05: `{"pk": {}, "sk": {"SS": []}}` answers `The provided starting
/// key is invalid: One or more parameter values were invalid: An string set
/// may not be empty`). A number, an empty `NS` or a set with duplicates in the
/// key carries no prefix in either order.
const START_KEY_PREFIX: &str = "The provided starting key is invalid: ";

/// The member names of the body's `ExclusiveStartKey`, `ExpressionAttributeValues`
/// and `ScanFilter` in the order the request wrote them, read from the raw
/// request text in one pass ([`member_orders`]), since the parsed body sorts
/// object members on every build but the MongoDB one. A field is `None` when
/// the body has no such field; every field is `None` when the text does not
/// parse or one of the three is not an object, which the shape fault check
/// refuses first. A name that appears twice keeps its first position, and a
/// field that appears twice is read at its first position.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct MemberOrders {
    pub(crate) key: Option<Vec<String>>,
    pub(crate) values: Option<Vec<String>>,
    pub(crate) filter: Option<Vec<String>>,
}

/// Reads the [`MemberOrders`] of `raw_body`, in time linear in its length.
pub(crate) fn member_orders(raw_body: &[u8]) -> MemberOrders {
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
                    let mut seen: HashSet<String> = HashSet::new();
                    while let Some(name) = map.next_key::<String>()? {
                        map.next_value::<serde::de::IgnoredAny>()?;
                        if seen.insert(name.clone()) {
                            names.push(name);
                        }
                    }
                    Ok(MemberNames(names))
                }
            }

            deserializer.deserialize_map(NamesVisitor)
        }
    }

    /// The top-level object, with only the member names of the three fields
    /// read.
    struct FieldNames;

    impl<'de> serde::de::Visitor<'de> for FieldNames {
        type Value = MemberOrders;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an object")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut orders = MemberOrders::default();
            while let Some(name) = map.next_key::<String>()? {
                let slot = match name.as_str() {
                    "ExclusiveStartKey" => &mut orders.key,
                    "ExpressionAttributeValues" => &mut orders.values,
                    "ScanFilter" => &mut orders.filter,
                    _ => {
                        map.next_value::<serde::de::IgnoredAny>()?;
                        continue;
                    }
                };
                if slot.is_none() {
                    *slot = Some(map.next_value::<MemberNames>()?.0);
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            Ok(orders)
        }
    }

    let mut deserializer = serde_json::Deserializer::from_slice(raw_body);
    serde::Deserializer::deserialize_map(&mut deserializer, FieldNames).unwrap_or_default()
}

/// The order in which the service visits the members of an `ExclusiveStartKey`
/// with faults in more than one member, and the entries of
/// `ExpressionAttributeValues` and the `ScanFilter` conditions in order U,
/// fitted to the measured rows: the iteration order of a Java `HashMap` over
/// the names, that is `String.hashCode()` of the name, spread as `h ^ (h >>>
/// 16)`, masked to the table's buckets, the buckets in ascending order and the
/// request's order inside a bucket. The table has four buckets for up to three
/// names and doubles when it holds more than three quarters of its size
/// ([`hash_table_mask`]): eight buckets for four to six names. With four
/// buckets `sk` falls in bucket 0, `a` in 1, `b` in 2, `pk`, `gpk`, `id` and
/// `ts` in 3; with eight, `sk` in 0, `gpk` and `pk` in 3, `gsk` in 6.
///
/// Measured (2026-10-31, 2026-11-04 and 2026-11-05, six tables, every row
/// with a shape fault in one of two faulty members, both request orders): on
/// the `pk`, `sk` table the service names `sk`; on HASH `a`, RANGE `b` it
/// names `a`; on HASH `pk`, RANGE `a` it names `a`; on `ts`, `id` and on `id`,
/// `ts` it names the member first in the request; on HASH `ä`, RANGE `é` it
/// names `ä`; on the GSI `{"gpk": {}, "pk": {"N": "bob"}, "sk": {"S": "x"}}`
/// it names `gpk`; on a GSI with a range key, where the key has four members,
/// it names `gpk` ahead of `gsk` and `pk` ahead of `gsk`, which four buckets
/// would reverse. In `ExpressionAttributeValues` (order U, 2026-11-05) it
/// names `:n` ahead of `:e`, `:b` ahead of `:p` and `:a`, and `:a` ahead of
/// `:e` when `:a` comes first in the request; on 2026-11-11 and 2026-11-23,
/// `:z` ahead of `:u`, `:b` ahead of `:a` and `:n` ahead of `:a` (see
/// `older_form_checks`). The rule is fitted to those rows. The service does
/// not document it. With two value faults and no shape fault the service's
/// own two sends disagreed on 19 of 40 matrix cells, so no order is pinned
/// there. Measured 2026-11-05 as well: a key of five members and one of seven
/// on the same GSI (eight and sixteen buckets), four and seven
/// `ExpressionAttributeValues` entries, two `ScanFilter` conditions, a key
/// name outside the Basic Multilingual Plane (`𝄞`, hashed as its two UTF-16
/// code units) and the request's order inside one bucket for the entries
/// (`:e` ahead of `:a` when `:e` comes first); each as the rule gives. Java's
/// `HashMap` also doubles a table under 64 buckets when one bucket reaches
/// nine names; the model sizes the table by the count of names alone and
/// departs from the JDK there (not measured).
fn fitted_member_order<'a>(names_in_request_order: impl Iterator<Item = &'a str>) -> Vec<String> {
    let names: Vec<&str> = names_in_request_order.collect();
    let mask = hash_table_mask(names.len());
    let mut names: Vec<(u32, usize, &str)> = names
        .into_iter()
        .enumerate()
        .map(|(position, name)| (java_hash_bucket(name, mask), position, name))
        .collect();
    names.sort_unstable();
    names
        .into_iter()
        .map(|(_, _, name)| name.to_owned())
        .collect()
}

/// The bucket mask of a Java `HashMap` that starts with four buckets and
/// doubles whenever it holds more than three quarters of its size: 3 for up to
/// three names, 7 for four to six, 15 for seven to twelve, and so on.
fn hash_table_mask(names: usize) -> u32 {
    let mut buckets: usize = 4;
    while names > buckets * 3 / 4 {
        buckets *= 2;
    }
    u32::try_from(buckets - 1).unwrap_or(u32::MAX)
}

/// The bucket of `name` in a Java `HashMap` whose bucket mask is `mask`:
/// `String.hashCode()` over the UTF-16 code units, spread as `h ^ (h >>> 16)`,
/// masked. The arithmetic wraps as Java's `int` does.
fn java_hash_bucket(name: &str, mask: u32) -> u32 {
    let hash = name.encode_utf16().fold(0u32, |h, unit| {
        h.wrapping_mul(31).wrapping_add(u32::from(unit))
    });
    (hash ^ (hash >> 16)) & mask
}

/// The names of the object `field` of the body in the fitted order
/// ([`fitted_member_order`]): the request's order when `request_order` names
/// exactly the object's members, else the parsed body's order.
fn fitted_order_of(body: &Value, field: &str, request_order: Option<&[String]>) -> Vec<String> {
    let Some(members) = body.get(field).and_then(Value::as_object) else {
        return Vec::new();
    };
    let parsed: Vec<&str> = members.keys().map(String::as_str).collect();
    let in_request: Vec<&str> = match request_order {
        Some(names)
            if names.len() == parsed.len()
                && names.iter().all(|name| members.contains_key(name)) =>
        {
            names.iter().map(String::as_str).collect()
        }
        _ => parsed,
    };
    fitted_member_order(in_request.into_iter())
}

/// A value that the typed deserialization reads, put in the body in place of
/// a value a check here holds back. The request is refused before the scan
/// reads it.
fn stand_in() -> Value {
    serde_json::json!({"S": ""})
}

/// The `ExclusiveStartKey` of a `Scan` body as the request wrote it, with the
/// checks the service makes on the key (measured 2026-10-31 to 2026-11-05,
/// us-east-1; see the module doc), the first faulty member in the fitted
/// member order ([`fitted_member_order`]) and, inside one member, the first
/// fault in the value's element order (see `first_fault`):
///
/// - in order U, after the request parameter checks and ahead of the
///   expression values and checks: a value the service cannot read as one
///   type (empty, an unknown or lower-case type descriptor, more than one, at
///   the top of the member or inside its `L` or `M`) with the prefix `The
///   provided starting key is invalid: `, a number with the number check's
///   text, a set fault with the texts of the older path (see
///   `SetFault::text`) and the prefix on an empty `SS` or `BS`;
/// - in order R, after the expression and request checks and ahead of the
///   table lookup: a number or a set fault, with the texts of the newer path.
///
/// A key with an unreadable or set-faulty value does not deserialize as a map
/// of attribute values. [`StartKeyBody::take`] takes it out of the body, so
/// that the typed deserialization of the body does not report it ahead of the
/// checks the service makes first, and keeps the deserializer's fault for a
/// value the checks here do not classify. A key of the wrong JSON shape (not
/// an object, a value that is not an object, an `N` that is not a string)
/// stays in the body, as the service reports a shape fault ahead of every
/// check; see `shape_fault_first`.
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
    /// the key's member names as the request wrote them ([`member_orders`]);
    /// when it is `None` or does not name the key's members, the parsed body's
    /// order stands in for it.
    pub(crate) fn take(body: &mut Value, request_order: Option<&[String]>) -> Self {
        let members = fitted_order_of(body, "ExclusiveStartKey", request_order);
        let raw = body.get("ExclusiveStartKey").cloned();
        let deferred = match raw
            .as_ref()
            .map(|key| serde_json::from_value::<Item>(key.clone()))
        {
            Some(Err(e)) => match crate::deserialize_error(e) {
                shape if is_shape_fault(&shape) => None,
                DynamoDbError::ValidationException(msg)
                    if msg == EMPTY_ATTRIBUTE_VALUE || msg == MULTI_ATTRIBUTE_VALUE =>
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
        Self {
            raw,
            members,
            deferred,
        }
    }

    /// The key's checks for `order`: the first faulty member in the fitted
    /// order; then the deserializer's fault in a key [`StartKeyBody::take`]
    /// took out of the body that the checks here did not classify.
    ///
    /// # Errors
    ///
    /// Returns `DynamoDbError::ValidationException` with the service's text.
    pub(crate) fn validate(&self, order: CheckOrder) -> Result<(), DynamoDbError> {
        let key = self.raw.as_ref().and_then(Value::as_object);
        let fault = self
            .members
            .iter()
            .filter_map(|name| key?.get(name))
            .find_map(|value| first_fault(value, None, true));
        match (fault, &self.deferred) {
            (Some(fault), _) => Err(key_fault_text(fault, order)),
            (None, Some(deferred)) => Err(deferred.clone()),
            (None, None) => Ok(()),
        }
    }
}

/// The service's text for the first fault in a key member in `order`: the
/// number check's text for a number (2026-10-31: `" 1"` and `""` echoed, 39
/// digits the 38-digit text), the set texts of the path, with the key prefix
/// on an empty `SS` or `BS` in order U (2026-11-05, beside an unreadable key
/// member and beside an unreadable expression value, inside an `L` and in a
/// member that is not a key attribute), and the prefix on a value the service
/// cannot read as one type.
fn key_fault_text(fault: Fault<'_>, order: CheckOrder) -> DynamoDbError {
    let older = order == CheckOrder::Unreadable;
    let text = match fault {
        Fault::NotANumber(_) | Fault::Refused(_) => fault.request_time_text(),
        Fault::Set(set) => {
            let prefix = if older && matches!(set, SetFault::Empty("SS" | "BS")) {
                START_KEY_PREFIX
            } else {
                ""
            };
            format!("{prefix}{}", set.text(older))
        }
        shape => format!("{START_KEY_PREFIX}{}", shape.reason()),
    };
    DynamoDbError::ValidationException(text)
}

/// The `ExpressionAttributeValues` of a `Scan` body in order U, whose faults
/// the service reports at the map's own step: after the key's faults and ahead
/// of the nesting check, read by an expression or not (2026-10-31 B4-7 and
/// 2026-11-05), every fault in the form `ExpressionAttributeValues contains
/// invalid value: <text> for key :e`. The first fault inside one entry is the
/// one `first_fault` finds in the value's element order: a value the service
/// cannot read as one type (`Supplied AttributeValue is empty, must contain
/// exactly one of the supported datatypes` for `{}` and for `{"X": "1"}`, the
/// more-than-one text for two descriptors), a malformed number at the top or
/// inside an `L` or `NS` (`The parameter cannot be converted to a numeric
/// value: bob`, and the 38-digit text), a set fault with the texts of the
/// older path (`One or more parameter values were invalid: An number set  may
/// not be empty`, `... Input collection [a, a] contains duplicates.`;
/// 2026-11-05: `{"L": [{"N": "bob"}, {}]}` answers the number). The entries
/// are visited in the fitted order ([`fitted_member_order`]), and the first
/// faulty one is named (2026-11-05: `:n` ahead of `:e`, `:b` ahead of `:p` and
/// `:a`, `:a` ahead of `:e` when `:a` comes first in the request and `:e`
/// ahead of `:a` when `:e` does, `:s` ahead of `:e`; with four and seven
/// entries the eight and sixteen buckets). In order R the numbers and the set
/// faults are request-time checks ([`validate_scan_request_body`]) and nothing
/// is taken.
///
/// After the entries the service reports an empty name in
/// `ExpressionAttributeNames` (measured 2026-11-19: `{"#a": ""}` beside `{"X":
/// "1"}` or `{}` answers the value's text); the name keys are request-time
/// checks.
///
/// [`Self::take`] puts a readable stand-in in the body for each faulty entry
/// and each empty name, so that the typed deserialization and the expression
/// checks run on the other values, and keeps the first fault;
/// [`Self::validate`] reports it. An entry of the wrong JSON shape stays in
/// the body.
pub(crate) struct ExpressionValuesBody {
    /// The first entry's fault in the fitted order, else the empty name's.
    fault: Option<DynamoDbError>,
}

impl ExpressionValuesBody {
    /// Replaces each faulty entry and each empty name with a readable stand-in
    /// in order U and keeps the first fault. `request_order` is the entry
    /// names as the request wrote them ([`member_orders`]).
    pub(crate) fn take(
        body: &mut Value,
        order: CheckOrder,
        request_order: Option<&[String]>,
    ) -> Self {
        let mut fault = None;
        if order == CheckOrder::Readable {
            return Self { fault };
        }
        let names = fitted_order_of(body, "ExpressionAttributeValues", request_order);
        if let Some(Value::Object(entries)) = body.get_mut("ExpressionAttributeValues") {
            for name in names {
                let Some(value) = entries.get_mut(&name) else {
                    continue;
                };
                if let Some(text) = first_fault(value, None, true).map(Fault::reason) {
                    fault.get_or_insert_with(|| {
                        DynamoDbError::ValidationException(format!(
                            "ExpressionAttributeValues contains invalid value: {text} for key {name}"
                        ))
                    });
                    *value = stand_in();
                }
            }
        }
        if let Err(empty_name) = validate_names(body) {
            if let Some(Value::Object(names)) = body.get_mut("ExpressionAttributeNames") {
                for name in names.values_mut().filter(|name| name.as_str() == Some("")) {
                    *name = Value::from("a");
                }
            }
            fault.get_or_insert(empty_name);
        }
        Self { fault }
    }

    /// The fault [`Self::take`] kept.
    ///
    /// # Errors
    ///
    /// Returns the first faulty entry's fault in the fitted order, else the
    /// empty name's.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        self.fault.clone().map_or(Ok(()), Err)
    }
}

/// The `ScanFilter` lists of a `Scan` body in order U, whose values the service
/// checks after the no-expression checks and ahead of `ConditionalOperator`,
/// the lists' value counts, the key's faults and the table (2026-11-05). The
/// conditions are visited in the fitted order ([`fitted_member_order`]), each
/// list in its order, and the first fault inside one value is the one
/// `first_fault` finds in the value's element order, with no prefix: a value
/// the service cannot read as one type (`Supplied AttributeValue is empty,
/// must contain exactly one of the supported datatypes` for `{}` and for
/// `{"X": "1"}`, the more-than-one text for two descriptors), a malformed
/// number, a set fault with the texts of the older path. Measured on
/// 2026-11-05 at 05:31 and 07:38 UTC: two descriptors, an empty value inside
/// an `L`, the condition `pk` ahead of `sk`, and `ts` ahead of `id` when `ts`
/// comes first; on 2026-11-23: `[{}, {"NS": []}]` answers the empty value
/// and `[{"N": "bob"}, {}]` the number. [`Self::take`] puts a readable
/// stand-in in the body for each faulty value and keeps the first fault;
/// [`Self::validate`] reports it. In order R nothing is taken: a malformed
/// number in a list is #507's check (#564 item 3), and a set fault the typed
/// deserialization reports with the text of the newer path (2026-11-05).
pub(crate) struct ScanFilterBody {
    fault: Option<DynamoDbError>,
}

impl ScanFilterBody {
    /// Replaces each faulty list value with a readable stand-in in order U and
    /// keeps the first fault. `request_order` is the condition names as the
    /// request wrote them ([`member_orders`]).
    pub(crate) fn take(
        body: &mut Value,
        order: CheckOrder,
        request_order: Option<&[String]>,
    ) -> Self {
        let mut fault = None;
        if order == CheckOrder::Readable {
            return Self { fault };
        }
        let names = fitted_order_of(body, "ScanFilter", request_order);
        let Some(Value::Object(conditions)) = body.get_mut("ScanFilter") else {
            return Self { fault };
        };
        for name in names {
            let Some(Value::Array(values)) = conditions
                .get_mut(&name)
                .and_then(|condition| condition.get_mut("AttributeValueList"))
            else {
                continue;
            };
            for value in values.iter_mut() {
                if let Some(text) = first_fault(value, None, true).map(Fault::reason) {
                    fault.get_or_insert(DynamoDbError::ValidationException(text));
                    *value = stand_in();
                }
            }
        }
        Self { fault }
    }

    /// The first list value's fault, at the step of order U.
    ///
    /// # Errors
    ///
    /// Returns the fault of the first value taken out.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        self.fault.clone().map_or(Ok(()), Err)
    }
}

/// The `Select` field of a `Scan` body in order R, where the service checks
/// the enum after the no-expression check and the nesting check and ahead of
/// the filter syntax (2026-11-05): [`Self::take`] takes an invalid value out of
/// the body, so that the typed deserialization does not report it first, and
/// keeps the clause; [`Self::validate`] reports it. In order U the enum is a
/// clause of the request-time aggregate (see `older_form_request_checks`) and
/// nothing is taken.
pub(crate) struct SelectBody {
    clause: Option<DynamoDbError>,
}

impl SelectBody {
    /// Takes an invalid `Select` string out of `body` in order R.
    pub(crate) fn take(body: &mut Value, order: CheckOrder) -> Self {
        if order == CheckOrder::Unreadable {
            return Self { clause: None };
        }
        let clause = crate::validate_enum_fields(
            body,
            &[crate::EnumField {
                json_name: "Select",
                valid: SELECT,
                clause: crate::EnumClause::Named("select"),
            }],
        )
        .err();
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
        self.clause.clone().map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod range_tests;
#[cfg(test)]
mod request_tests;
#[cfg(test)]
mod tests;
