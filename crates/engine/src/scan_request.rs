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
//!    not; a Unicode decimal digit counts as the digit it stands for. The
//!    entries are visited in the hash order of [`fitted_member_order`]
//!    (2026-11-05: `{":n": {"N": "fred"}, ":a": {"N": "bob"}}` answers `fred`,
//!    which sorted order would not). An entry the deserializer refuses (a set
//!    fault anywhere in it) is skipped here: its set fault is reported at step
//!    4, and a malformed number inside it after the typed deserialization
//!    (2026-11-05: `{"NS": ["bob", "1", "1"]}` beside a value nested 32 levels
//!    answers the nesting text; `{"L": [{"N": "bob"}, {"NS": []}]}` beside it
//!    answered the number in one send and the nesting text in the other). The
//!    numbers of the `ScanFilter` lists are #507's check (#564 item 3, measured
//!    2026-11-05: the number text ahead of a number in the key) and are not
//!    checked on this branch;
//! 4. an empty or duplicate set in `ExpressionAttributeValues`, the entries in
//!    sorted name order whatever order the backend's parsed body keeps
//!    (2026-11-05: `{":e": {"NS": []}}` with no expression answers the set text
//!    ahead of the no-expression check; beside a value nested 32 levels in
//!    another entry the empty `NS` text comes ahead of the nesting text in both
//!    sends and the `SS` duplicates text did in one of two; `{":n": {"NS": []},
//!    ":a": {"SS": []}}` answered the `SS` text in one send and the `NS` text
//!    in the other, so the sorted order matches one of the two), see
//!    [`expression_values_set_fault`] and [`ExpressionValuesBody`];
//! 5. the nesting depth of `ExpressionAttributeValues`: a value nested 32
//!    levels deep answers `Nesting Levels have exceeded supported limits:
//!    Attributes in the item have nested levels beyond supported limit`, 31
//!    levels pass, read or unused, with or without an expression (2026-10-30
//!    and 2026-10-31: ahead of the no-expression check, B4-5a; 2026-11-05:
//!    ahead of the filter syntax, the `Select` enum and the key's number). The
//!    Scan a PartiQL `SELECT` is translated into skips this check in both
//!    orders (see [`ValuesDepth`]);
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
//!    unused values, the `Segment` pairing and `SPECIFIC_ATTRIBUTES`; the
//!    unused values ahead of `COUNT` with `ProjectionExpression`; the unused
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
//! 2. the parameter mix, then `ExpressionAttributeNames` and
//!    `ExpressionAttributeValues` with no expression, then the `ScanFilter`
//!    list values (see [`ScanFilterBody`]: a value that is not one
//!    AttributeValue answers `Supplied AttributeValue is empty, must contain
//!    exactly one of the supported datatypes` with no prefix, for `{}` and for
//!    an unknown descriptor alike, 2026-11-05), then `ConditionalOperator`
//!    with no `ScanFilter` and a `ScanFilter` condition's value count
//!    (2026-11-05: each ahead of the key's faults). The numbers of
//!    `ExpressionAttributeValues` are not checked at request time in this
//!    order;
//! 3. the `ExclusiveStartKey` faults, shape and value together, the first
//!    faulty member in the member order of [`StartKeyBody`]: a shape fault
//!    with the prefix `The provided starting key is invalid: `, a number with
//!    the value check's text, a set fault with the texts of order U (the
//!    members unquoted, the `NS` duplicate text bare, the empty `SS` and `BS`
//!    texts with the key prefix);
//! 4. the `ExpressionAttributeValues` entries, in the same member order
//!    ([`fitted_member_order`]), the first faulty one named, every fault in
//!    the form `ExpressionAttributeValues contains invalid value: <text> for
//!    key :e`: an entry that is not one AttributeValue, read by an expression
//!    or not, `Supplied AttributeValue is empty, must contain exactly one of
//!    the supported datatypes` for an empty value and for an unknown
//!    descriptor, the more-than-one text for two descriptors (2026-10-31 B4-7
//!    and 2026-11-05: ahead of the nesting check and after the key's faults);
//!    a malformed number, `The parameter cannot be converted to a numeric
//!    value: bob`, at the top of the entry or inside its `L` or `NS`, and the
//!    38-digit text (2026-11-05: after the key's faults); a set fault with the
//!    texts of order U, `One or more parameter values were invalid: An number
//!    set  may not be empty` and `... Input collection [a, a] contains
//!    duplicates.` (2026-11-05). Inside one entry the first fault in the
//!    value's own element order is named, a number or a shape fault
//!    (2026-11-05: `{"L": [{"N": "bob"}, {}]}` answers the number);
//! 5. the nesting depth, then the expression and request checks of order R
//!    step 8, then the table.
//!
//! Pairs never sent together keep the code's order: in order R the nesting
//! check against `ExpressionAttributeNames` with no expression,
//! `ConditionalOperator` and the `ScanFilter` value count; a set fault in
//! `ExpressionAttributeValues` against a malformed number in another entry
//! (the number first, at request time; the service's two sends disagreed,
//! 2026-11-05); which of two set faults in `ExpressionAttributeValues` is
//! named (sorted name order; the service's two sends disagreed, 2026-11-05);
//! the `ProjectionExpression` syntax against an undefined value, an operand
//! type and the unused values. In order U the `Select` enum against the mix
//! (the enum first; the mix beside `Select: "BAD"` and an empty entry answers
//! the `Select` clause, 2026-11-05); an empty value ahead of a malformed
//! number inside one `ExpressionAttributeValues` entry (the empty value, the
//! element order; the other way round is measured); a set fault in a
//! `ScanFilter` list value in order U (at the list's step, the deserializer's
//! text in order R, 2026-11-05); a `BS` with duplicates in the key (no key
//! prefix, the members unquoted); nine or more names in one hash bucket (see
//! [`fitted_member_order`]). The service refuses a member or entry name written
//! twice as a `SerializationException` (`Duplicate key found in payload`,
//! 2026-11-05, #630); the parser here keeps the last value.
//!
//! A field of the wrong JSON type anywhere in the body is reported ahead of
//! all of them, as a `SerializationException` (`"Limit": "5"` with `Select:
//! "BAD"`, 2026-11-05; the service's texts differ from the deserializer's). The
//! service reports a key shape fault ahead of `"ConsistentRead": "yes"`
//! (2026-11-05); the code reads the whole body first and does not. In order U
//! the service reports an invalid `ScanFilter` `ComparisonOperator` as a
//! constraint clause (`Value 'BAD' at 'scanFilter.a.member.comparisonOperator'
//! failed to satisfy constraint: Member must satisfy enum value set: [...]`,
//! 2026-11-05) ahead of the list's values; the legacy condition checks are
//! #507's and the clause is not made here.
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
    present, shape_fault_first, validate_number_text, validate_value_numbers,
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

/// The `Scan` checks that run on the raw request body, in the service's order
/// for `order` (see the module doc): the `AttributesToGet` length and the
/// `Limit`, `Segment` and `TotalSegments` ranges in one message, the
/// `ReturnConsumedCapacity` enum, in order U the `Select` enum, the expression
/// and non-expression parameter mix, every number in
/// `ExpressionAttributeValues`, and in order R, when `depth` is
/// [`ValuesDepth::Checked`], the nesting depth of `ExpressionAttributeValues`.
/// The `ExclusiveStartKey` values are checked later, by [`StartKeyBody`], and
/// in order R the `Select` enum by [`SelectBody`].
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
    values_order: Option<&[String]>,
    depth: ValuesDepth,
) -> Result<(), DynamoDbError> {
    scan_request_checks(body, order, values_order, depth)
        .map_err(|check| shape_fault_first::<ScanInput>(body, check))
}

/// The `Scan` checks in the service's order; see
/// [`validate_scan_request_body`]. `values_order` is the
/// `ExpressionAttributeValues` entry names as the request wrote them
/// ([`member_order`]), for the hash order of the number check.
fn scan_request_checks(
    body: &Value,
    order: CheckOrder,
    values_order: Option<&[String]>,
    depth: ValuesDepth,
) -> Result<(), DynamoDbError> {
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

    if order == CheckOrder::Readable {
        // Every number in ExpressionAttributeValues, the entries in the hash
        // order the key and the order-U step use (an entry the deserializer
        // refuses, one with a set fault, is skipped here), then the set faults
        // in sorted name order, then the nesting depth of the entries that
        // deserialize when `depth` asks for it. In order U the numbers and the
        // set faults are reported at the map's own step (see the module doc).
        // The numbers of the ScanFilter lists are #507's check (#564 item 3).
        let values = readable_attribute_values(body, "ExpressionAttributeValues");
        for name in fitted_order_of(body, "ExpressionAttributeValues", values_order) {
            if let Some(value) = values.get(&name) {
                validate_value_numbers(value)?;
            }
        }
        expression_values_set_fault(body)?;
        if depth == ValuesDepth::Checked {
            extenddb_core::validation::validate_attribute_values_nesting_depth(values.values())?;
        }
    }
    Ok(())
}

/// The entries of the object `field` of the body that deserialize as one
/// attribute value each; the others are left to [`ExpressionValuesBody`].
fn readable_attribute_values(body: &Value, field: &str) -> HashMap<String, AttributeValue> {
    body.get(field)
        .and_then(Value::as_object)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|(name, value)| {
                    serde_json::from_value::<AttributeValue>(value.clone())
                        .ok()
                        .map(|value| (name.clone(), value))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The first empty or duplicate set among the `ExpressionAttributeValues`
/// entries, in sorted name order whatever order the backend's parsed body
/// keeps, with the texts of order R (see [`ValueFault::text`]). Measured
/// 2026-11-05: `{":d": <32 levels>, ":e": {"NS": []}}` answers the empty-set
/// text ahead of the nesting text in both sends, and `{":d": <32 levels>, ":s":
/// {"SS": ["a", "a"]}}` the duplicates text in one of two; beside a malformed
/// number in another entry the service's two sends disagreed, and the number
/// comes first here. With two set faults, `{":n": {"NS": []}, ":a": {"SS":
/// []}}`, the service answered the `SS` text in one send and the `NS` text in
/// the other; the sorted order names `:a`. An entry whose first fault, depth
/// first, is a malformed number is left to [`ExpressionValuesBody`].
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the set text.
fn expression_values_set_fault(body: &Value) -> Result<(), DynamoDbError> {
    let Some(entries) = body
        .get("ExpressionAttributeValues")
        .and_then(Value::as_object)
    else {
        return Ok(());
    };
    let mut names: Vec<&String> = entries.keys().collect();
    names.sort_unstable();
    for name in names {
        match value_fault(&entries[name.as_str()]) {
            None | Some(ValueFault::Number(_)) => {}
            Some(fault) => return Err(fault.text(CheckOrder::Readable, Place::ExpressionValue)),
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

/// The member names of the body's object `field` (`ExclusiveStartKey`,
/// `ExpressionAttributeValues` or `ScanFilter`) in the order the request wrote
/// them, read from the raw request text, since the parsed body sorts object
/// members on every build but the MongoDB one. `None` when the text does not
/// parse, has no such field, or the field is not an object; a name that
/// appears twice keeps its first position; a field that appears twice is read
/// at its first position.
pub(crate) fn member_order(raw_body: &[u8], field: &str) -> Option<Vec<String>> {
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

    /// The top-level object, with only the member names of `field` read.
    struct FieldNames<'f>(&'f str);

    impl<'de> serde::de::Visitor<'de> for FieldNames<'_> {
        type Value = Option<Vec<String>>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an object")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut found: Option<Vec<String>> = None;
            while let Some(name) = map.next_key::<String>()? {
                if name == self.0 && found.is_none() {
                    found = Some(map.next_value::<MemberNames>()?.0);
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            Ok(found)
        }
    }

    let mut deserializer = serde_json::Deserializer::from_slice(raw_body);
    serde::Deserializer::deserialize_map(&mut deserializer, FieldNames(field))
        .ok()
        .flatten()
}

/// The order in which the service visits the members of an `ExclusiveStartKey`
/// with faults in more than one member, and the entries of
/// `ExpressionAttributeValues` in order U, fitted to the measured rows: the
/// iteration order of a Java `HashMap` over the names, that is
/// `String.hashCode()` of the name, spread as `h ^ (h >>> 16)`, masked to the
/// table's buckets, the buckets in ascending order and the request's order
/// inside a bucket. The table has four buckets for up to three names and
/// doubles when it holds more than three quarters of its size
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
/// `:e` when `:a` comes first in the request. The rule is fitted to those
/// rows. The service does not document it. With two value faults and no shape
/// fault the service's own two sends disagreed on 19 of 40 matrix cells, so no
/// order is pinned there. Measured 2026-11-05 as well: a key of five members
/// and one of seven on the same GSI (eight and sixteen buckets), four and
/// seven `ExpressionAttributeValues` entries, two `ScanFilter` conditions, a
/// key name outside the Basic Multilingual Plane (`𝄞`, hashed as its two
/// UTF-16 code units) and the request's order inside one bucket for the
/// entries (`:e` ahead of `:a` when `:e` comes first); each as the rule gives.
/// Java's `HashMap` also doubles a table under 64 buckets when one bucket
/// reaches nine names; the model sizes the table by the count of names alone
/// and departs from the JDK there (not measured).
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
    /// the key's member names as the request wrote them ([`member_order`]);
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
    /// - an empty `BS`: `... Binary sets should not be empty`, with the key
    ///   prefix for a key member in order U (2026-11-05, `{"pk": {}, "sk":
    ///   {"BS": []}}`); an empty `NS` carries no prefix in either order;
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
            Self::StringSetEmpty => key_prefixed_in_order_u(
                invalid_parameter("An string set  may not be empty"),
                order,
                place,
            ),
            Self::BinarySetEmpty => key_prefixed_in_order_u(
                invalid_parameter("Binary sets should not be empty"),
                order,
                place,
            ),
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

/// `text` with the key prefix when it is a key member's fault in order U
/// (measured 2026-11-05 for an empty `SS` and an empty `BS` beside an
/// unreadable key member), else as it is.
fn key_prefixed_in_order_u(text: DynamoDbError, order: CheckOrder, place: Place) -> DynamoDbError {
    match (order, place, text) {
        (CheckOrder::Unreadable, Place::Key, DynamoDbError::ValidationException(msg)) => {
            DynamoDbError::ValidationException(format!("{START_KEY_PREFIX}{msg}"))
        }
        (_, _, text) => text,
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

/// A shape fault ([`shape_fault`]) or a value fault ([`value_fault`]) in one
/// raw AttributeValue.
enum Fault {
    Shape(&'static str),
    Value(ValueFault),
}

/// The first fault of either kind in one raw AttributeValue, depth first in
/// the value's own element order: at each value, a shape fault of the value
/// itself (empty, an unknown descriptor, two descriptors), else the members of
/// an `L` or `M` in turn, else the value's number or set fault. Measured
/// 2026-11-05 (order U, `ExpressionAttributeValues`): `{"L": [{"N": "bob"},
/// {}]}` answers the number, which [`shape_fault`] alone would pass over. The
/// empty value ahead of the number is not measured.
fn first_fault(value: &Value) -> Option<Fault> {
    let fields = value.as_object()?;
    let mut fields = fields.iter();
    match (fields.next(), fields.next()) {
        (None, _) => Some(Fault::Shape(EMPTY_VALUE)),
        (Some(_), Some(_)) => Some(Fault::Shape(MORE_THAN_ONE_DESCRIPTOR)),
        (Some((descriptor, inner)), None) => match (descriptor.as_str(), inner) {
            ("L", Value::Array(members)) => members.iter().find_map(first_fault),
            ("M", Value::Object(members)) => members.values().find_map(first_fault),
            (descriptor, _) if TYPE_DESCRIPTORS.contains(&descriptor) => {
                value_fault(value).map(Fault::Value)
            }
            _ => Some(Fault::Shape(EMPTY_VALUE)),
        },
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

/// The `ExpressionAttributeValues` of a `Scan` body whose entries the handler
/// reports at the map's own step rather than at request time. In order R the
/// numbers, the set faults and the nesting depth are request-time checks
/// ([`scan_request_checks`]); an empty or duplicate set is reported there ahead
/// of the no-expression check (2026-11-05, `{":e": {"NS": []}}` with no
/// expression answers `One or more parameter values were invalid: An number set
///  may not be empty`; `{":a": {"SS": ["a", "a"]}}` read by the filter answers
/// `... Input collection ["a", "a"] contains duplicates.`), and here only an
/// entry the request-time checks did not reach is held back, with the same
/// texts: a malformed number inside an entry the deserializer refuses for a set
/// fault (2026-11-05: `{"NS": ["bob", "1", "1"]}` beside a value nested 32
/// levels answers the nesting text, so the number is reported after it). In
/// order U, after the key's faults and ahead of the nesting check, read by an
/// expression or not (2026-10-31 B4-7 and 2026-11-05), every fault in the form
/// `ExpressionAttributeValues contains invalid value: <text> for key :e`: an
/// entry that is not one AttributeValue (empty, an unknown or lower-case type
/// descriptor, two descriptors, at the top or inside an `L` or `M`), `Supplied
/// AttributeValue is empty, must contain exactly one of the supported datatypes`
/// for `{}` and for `{"X": "1"}`, `... has more than one datatypes set, ...`
/// for two descriptors; a malformed number, `The parameter cannot be converted
/// to a numeric value: bob`, at the top or inside an `L` or `NS`, and the
/// 38-digit text; a set fault with the set texts of order U (`One or more
/// parameter values were invalid: An number set  may not be empty`, `... Input
/// collection [a, a] contains duplicates.`). Inside one entry the first fault
/// in the value's element order is named ([`first_fault`]). The entries are
/// visited in the fitted order ([`fitted_member_order`]), and the first faulty
/// one is named (2026-11-05: `:n` ahead of `:e`, `:b` ahead of `:p` and `:a`,
/// `:a` ahead of `:e` when `:a` comes first in the request and `:e` ahead of
/// `:a` when `:e` does, `:s` ahead of `:e`; with four and seven entries the
/// eight and sixteen buckets).
///
/// [`Self::take`] puts a readable stand-in in the body for each such entry, so
/// that the typed deserialization and the expression checks run on the other
/// values, and keeps the entry's fault; [`Self::validate`] reports it. The
/// request is refused before the scan reads the stand-in. An entry of the wrong
/// JSON shape stays in the body.
pub(crate) struct ExpressionValuesBody {
    /// The faults of the entries taken out, in the fitted order of their names.
    faults: Vec<DynamoDbError>,
}

impl ExpressionValuesBody {
    /// Replaces each entry the checks here classify with a readable stand-in
    /// and keeps its fault for `order`. `request_order` is the entry names as
    /// the request wrote them ([`member_order`]).
    pub(crate) fn take(
        body: &mut Value,
        order: CheckOrder,
        request_order: Option<&[String]>,
    ) -> Self {
        let names = fitted_order_of(body, "ExpressionAttributeValues", request_order);
        let Some(Value::Object(entries)) = body.get_mut("ExpressionAttributeValues") else {
            return Self { faults: Vec::new() };
        };
        let mut faults = Vec::new();
        for name in names {
            let Some(value) = entries.get_mut(&name) else {
                continue;
            };
            let fault = match (order, first_fault(value)) {
                (_, None) => None,
                (_, Some(Fault::Shape(text))) => Some(invalid_expression_value(text, &name)),
                // In order R a number is reported here only when the request-time
                // check skipped its entry (a set fault below it), with the plain
                // text; the set faults were reported at request time.
                (CheckOrder::Readable, Some(Fault::Value(ValueFault::Number(fault)))) => {
                    Some(fault)
                }
                (CheckOrder::Readable, Some(Fault::Value(fault))) => {
                    Some(fault.text(order, Place::ExpressionValue))
                }
                (CheckOrder::Unreadable, Some(Fault::Value(fault))) => {
                    match fault.text(order, Place::ExpressionValue) {
                        DynamoDbError::ValidationException(msg) => {
                            Some(invalid_expression_value(&msg, &name))
                        }
                        other => Some(other),
                    }
                }
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
    /// Returns the fault of the first entry taken out, in the fitted order.
    pub(crate) fn validate(&self) -> Result<(), DynamoDbError> {
        match self.faults.first() {
            Some(fault) => Err(fault.clone()),
            None => Ok(()),
        }
    }
}

/// The service's text for a faulty `ExpressionAttributeValues` entry at the
/// map's own step: `ExpressionAttributeValues contains invalid value: <text>
/// for key <name>`.
fn invalid_expression_value(text: &str, name: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "ExpressionAttributeValues contains invalid value: {text} for key {name}"
    ))
}

/// The `ScanFilter` lists of a `Scan` body in order U, whose values the service
/// checks after the no-expression checks and ahead of `ConditionalOperator`,
/// the lists' value counts, the key's faults and the table (2026-11-05): a
/// value that is not one AttributeValue answers `Supplied AttributeValue is
/// empty, must contain exactly one of the supported datatypes` for `{}` and
/// for `{"X": "1"}`, with no prefix. A malformed number in a list is #507's
/// check (#564 item 3) and stays in the body. Two descriptors in a list value,
/// a set fault in a list value, and which value is named when several are
/// faulty (the conditions in the fitted order, the list in its order) are not
/// measured. [`Self::take`] puts a readable stand-in in the body for each such
/// value and keeps the fault; [`Self::validate`] reports it. In order R nothing
/// is taken.
pub(crate) struct ScanFilterBody {
    faults: Vec<DynamoDbError>,
}

impl ScanFilterBody {
    /// Replaces each list value that is not one AttributeValue with a readable
    /// stand-in in order U and keeps its fault. `request_order` is the
    /// condition names as the request wrote them ([`member_order`]).
    pub(crate) fn take(
        body: &mut Value,
        order: CheckOrder,
        request_order: Option<&[String]>,
    ) -> Self {
        if order == CheckOrder::Readable {
            return Self { faults: Vec::new() };
        }
        let names = fitted_order_of(body, "ScanFilter", request_order);
        let Some(Value::Object(conditions)) = body.get_mut("ScanFilter") else {
            return Self { faults: Vec::new() };
        };
        let mut faults = Vec::new();
        for name in names {
            let Some(Value::Array(values)) = conditions
                .get_mut(&name)
                .and_then(|condition| condition.get_mut("AttributeValueList"))
            else {
                continue;
            };
            for value in values.iter_mut() {
                if let Some(text) = shape_fault(value) {
                    *value = serde_json::json!({"S": ""});
                    faults.push(DynamoDbError::ValidationException(text.to_owned()));
                }
            }
        }
        Self { faults }
    }

    /// The first list value's fault, at the step of order U.
    ///
    /// # Errors
    ///
    /// Returns the fault of the first value taken out.
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
