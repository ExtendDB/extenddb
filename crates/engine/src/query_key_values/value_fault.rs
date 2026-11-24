// SPDX-License-Identifier: Apache-2.0

//! The first fault inside one `AttributeValue` of a `Query` or `Scan` body: a
//! null, no type descriptor or more than one, a number the number rules
//! refuse and, when asked for, an empty or duplicate set (see
//! [`first_fault`]). [`Fault`] renders the fault with the text of the
//! request-time number check or the text the service's older validation path
//! wraps, and [`SetFault::text`] a set fault with the text of either path.

use std::collections::HashSet;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{EMPTY_ATTRIBUTE_VALUE, MULTI_ATTRIBUTE_VALUE};
use extenddb_core::validation::number::{NOT_A_NUMBER, NUMBER_SET_DUPLICATES, has_equal_numbers};
use serde_json::{Map, Value};

/// The service's text for a null value. Core's copy is a private constant of
/// its `partiql` module, which a build without the `partiql` feature leaves
/// out.
const NULL_VALUE: &str = "The supplied Item contains a null AttributeValue";

/// The type descriptors of an AttributeValue, in the order the service checks
/// the payloads of a value with several type descriptors (see
/// [`first_fault`]): a duplicate `SS` member ahead of the numbers (measured
/// 2026-11-24), `N` ahead of `NS`, `NS` ahead of `BS`, and `NS` and `M` ahead
/// of `L`. Core's list is a private constant of its `partiql` module, as for
/// [`NULL_VALUE`].
pub(super) const TYPE_DESCRIPTORS: [&str; 10] =
    ["SS", "S", "N", "B", "NS", "BS", "M", "L", "NULL", "BOOL"];

/// The first fault inside one value, in document order for `L` items and in
/// the map's iteration order for `M` members (see [`first_fault`]).
pub(crate) enum Fault<'a> {
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
    /// An empty set or a set with a duplicate member, which [`first_fault`]
    /// reports only when it is asked for set faults.
    Set(SetFault<'a>),
}

impl Fault<'_> {
    /// The request-time number check's text: the value echoed with each
    /// decimal digit folded to ASCII (measured 2026-11-11: `{"N": "\u{ff11}bob"}`
    /// answers `The parameter cannot be converted to a numeric value: 1bob`,
    /// `\u{661}\u{662}x` answers `12x`), and the colon kept after an empty one.
    pub(crate) fn request_time_text(self) -> String {
        match self {
            Fault::NotANumber(text) => format!(
                "{NOT_A_NUMBER}: {}",
                crate::decimal_digits::fold_decimal_digits(text)
            ),
            other => other.reason(),
        }
    }

    /// The text `older_form_checks` wraps: a number echoed as written
    /// and no colon after an empty one (measured 2026-11-11), and a set fault
    /// in the older path's form (see [`SetFault::text`]).
    pub(crate) fn reason(self) -> String {
        match self {
            Fault::Null(None) => NULL_VALUE.to_owned(),
            Fault::Null(Some(member)) => format!("{NULL_VALUE}: {member} is null"),
            Fault::Empty => EMPTY_ATTRIBUTE_VALUE.to_owned(),
            Fault::SeveralTypes => MULTI_ATTRIBUTE_VALUE.to_owned(),
            Fault::NotANumber("") => NOT_A_NUMBER.to_owned(),
            Fault::NotANumber(text) => format!("{NOT_A_NUMBER}: {text}"),
            Fault::Refused(msg) => msg,
            Fault::Set(set) => set.text(true),
        }
    }
}

/// An empty set, or a set with two members of one value.
pub(crate) enum SetFault<'a> {
    /// An empty set, with its type descriptor (`NS`, `SS` or `BS`).
    Empty(&'static str),
    /// An `NS` with two members of one numeric value (`["1", "1.0"]`).
    NumberDuplicates,
    /// An `SS`, or a `BS` when `binary`, with two equal members, the members
    /// as the request wrote them.
    Duplicates { members: Vec<&'a str>, binary: bool },
}

/// The prefix of the service's set texts.
const INVALID_PARAMETER_VALUES: &str = "One or more parameter values were invalid: ";

impl SetFault<'_> {
    /// The service's text for the fault (measured on `Scan` 2026-11-05),
    /// on the service's older validation path when `older_path` is true:
    ///
    /// - an empty `NS`, `SS` or `BS`: `One or more parameter values were
    ///   invalid: An number set  may not be empty` (with the service's two
    ///   spaces), `... An string set  may not be empty` and `... Binary sets
    ///   should not be empty`, on both paths;
    /// - an `NS` with duplicates: `... Input collection contains duplicates.`,
    ///   and `Input collection contains duplicates` (no prefix, no full stop) on
    ///   the older path;
    /// - an `SS` with duplicates: `... Input collection ["a", "a"] contains
    ///   duplicates.`, and `... Input collection [a, a] contains duplicates.`
    ///   on the older path;
    /// - a `BS` with duplicates: `... Input collection ["YQ==","YQ=="]of type
    ///   BS contains duplicates.` (no space after the comma or before `of`),
    ///   and `... Input collection [YQ==, YQ==]of type BS contains duplicates.`
    ///   on the older path.
    ///
    /// Measured 2026-11-24 05:41 UTC on the older path beside `Limit: 1.5`, on
    /// `Scan` and `Query`: `{"NS": ["1", "1"]}` as an entry and inside an `L`,
    /// `{"NS": []}` and `{"SS": ["x", "x"]}` answer these texts, wrapped.
    ///
    /// The members are quoted as JSON strings, in the request's order. For
    /// members such as these the service answers the same text. It sorts the
    /// members ahead of the first repeated one and escapes a control character
    /// as `\u{1}` (measured 2026-11-23: `["b", "a", "b"]` answers `["a", "b",
    /// "b"]`), which this text does not; #1368 builds that form for the other
    /// operations.
    pub(crate) fn text(&self, older_path: bool) -> String {
        let listed = |members: &[&str], separator: &str| -> String {
            members
                .iter()
                .map(|member| {
                    if older_path {
                        (*member).to_owned()
                    } else {
                        serde_json::to_string(member).unwrap_or_default()
                    }
                })
                .collect::<Vec<String>>()
                .join(separator)
        };
        match self {
            SetFault::Empty("NS") => {
                format!("{INVALID_PARAMETER_VALUES}An number set  may not be empty")
            }
            SetFault::Empty("SS") => {
                format!("{INVALID_PARAMETER_VALUES}An string set  may not be empty")
            }
            SetFault::Empty(_) => {
                format!("{INVALID_PARAMETER_VALUES}Binary sets should not be empty")
            }
            SetFault::NumberDuplicates if older_path => NUMBER_SET_DUPLICATES.to_owned(),
            SetFault::NumberDuplicates => {
                format!("{INVALID_PARAMETER_VALUES}{NUMBER_SET_DUPLICATES}.")
            }
            SetFault::Duplicates {
                members,
                binary: false,
            } => format!(
                "{INVALID_PARAMETER_VALUES}Input collection [{}] contains duplicates.",
                listed(members, ", ")
            ),
            SetFault::Duplicates {
                members,
                binary: true,
            } => format!(
                "{INVALID_PARAMETER_VALUES}Input collection [{}]of type BS contains duplicates.",
                listed(members, if older_path { ", " } else { "," })
            ),
        }
    }
}

/// The order in which a walk visits the members of an `M`.
pub(crate) type MemberOrder = for<'m> fn(&'m Map<String, Value>) -> Vec<(&'m String, &'m Value)>;

/// The members of an `M` in the map's iteration order: sorted by key without
/// the `preserve_order` feature of `serde_json`, in document order with it, as
/// in the MongoDB build (bson turns the feature on).
fn map_order(members: &Map<String, Value>) -> Vec<(&String, &Value)> {
    members.iter().collect()
}

/// [`first_fault_in`] with the members of each `M` in the map's iteration
/// order. The service visits the members of an `M` in an
/// `ExpressionAttributeValues` entry in an order of its own (#1013): measured
/// 2026-11-23, `{"M": {"z": {"N": "bob"}, "b": {"N": "fred"}}}` answers
/// `bob`, as document order does. For the members of an `M` in an
/// `ExclusiveStartKey` value see `scan_request::StartKeyBody`.
pub(crate) fn first_fault<'a>(
    value: &'a Value,
    member: Option<&'a str>,
    sets: bool,
) -> Option<Fault<'a>> {
    first_fault_in(value, member, sets, map_order)
}

/// The first fault in `value` (`member` is its name when it is the value of an
/// `M` member): a null or no type descriptor, then the payloads in the order
/// of [`TYPE_DESCRIPTORS`] (the numbers in `N` and `NS`, and every value an
/// `L` or `M` holds), and with `sets` the set faults in the same walk (`SS`
/// ahead of the numbers, an `NS` after its own numbers; see [`set_fault`]),
/// then more than one type descriptor. `L` items are visited in document
/// order and `M` members in the order `members_in_order` gives. A value of
/// another JSON type, and an `N` or `NS` payload of the wrong JSON type, are
/// shape faults the deserializer reports. A type descriptor with a null
/// payload counts as absent, as the service reads it (measured 2026-11-19:
/// `{"S": null}` answers the empty value text, and `{"S": null, "N": "1"}` in
/// a `FilterExpression` is accepted).
///
/// With several type descriptors, the service reads each payload in the order
/// of [`TYPE_DESCRIPTORS`], whatever order the request wrote them in, and
/// answers the more than one datatypes text only when no payload has a fault
/// (measured 2026-11-19: `{"N": "bob", "S": "x"}` and `{"S": "x", "N": "bob"}`
/// answer the numeric text for `bob`, `{"N": "1", "S": "x"}` the more than one
/// datatypes text, `{"S": "x", "L": [null]}` the null text, and `N` is read
/// ahead of `NS`, `NS` and `M` ahead of `L`; measured 2026-11-24: `{"SS":
/// ["a", "a"], "N": "bob"}` answers the duplicate text, `{"NS": ["1", "1"],
/// "N": "bob"}` the numeric text, and `{"NS": ["1", "1"], "L": [{"N":
/// "bob"}]}` the duplicate text).
pub(crate) fn first_fault_in<'a>(
    value: &'a Value,
    member: Option<&'a str>,
    sets: bool,
    members_in_order: MemberOrder,
) -> Option<Fault<'a>> {
    let Value::Object(map) = value else {
        return value.is_null().then_some(Fault::Null(member));
    };
    let payloads: Vec<(&'static str, &Value)> = TYPE_DESCRIPTORS
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
                .find_map(number_fault)
                .or_else(|| sets.then(|| set_fault(descriptor, members)).flatten()),
            ("SS" | "BS", Value::Array(members)) if sets => set_fault(descriptor, members),
            ("L", Value::Array(items)) => items
                .iter()
                .find_map(|item| first_fault_in(item, None, sets, members_in_order)),
            ("M", Value::Object(members)) => {
                members_in_order(members)
                    .into_iter()
                    .find_map(|(name, inner)| {
                        first_fault_in(inner, Some(name), sets, members_in_order)
                    })
            }
            _ => None,
        });
    match payloads.len() {
        0 => Some(Fault::Empty),
        1 => fault,
        _ => fault.or(Some(Fault::SeveralTypes)),
    }
}

/// An empty set, or two members of one value (an `NS`'s by numeric value, see
/// `has_equal_numbers`), in the payload of the set type `descriptor`. A member
/// that is not a string is a shape fault the deserializer reports. Beside a
/// second type descriptor the service answers the older path's texts, wrapped
/// (measured 2026-11-24 on `Query`, each body twice: `{"SS": ["a", "a"], "N":
/// "1"}`, `{"NS": ["1", "1"], "S": "x"}`, `{"BS": ["YQ==", "YQ=="], "N":
/// "1"}`, and an empty `SS`, `NS` or `BS` beside a second descriptor).
fn set_fault<'a>(descriptor: &'static str, members: &'a [Value]) -> Option<Fault<'a>> {
    if members.is_empty() {
        return Some(Fault::Set(SetFault::Empty(descriptor)));
    }
    if descriptor == "NS" {
        return has_equal_numbers(members).then_some(Fault::Set(SetFault::NumberDuplicates));
    }
    let texts: Vec<&str> = members.iter().filter_map(Value::as_str).collect();
    let mut seen = HashSet::with_capacity(texts.len());
    (!texts.iter().all(|text| seen.insert(*text))).then(|| {
        Fault::Set(SetFault::Duplicates {
            members: texts,
            binary: descriptor == "BS",
        })
    })
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
