// SPDX-License-Identifier: Apache-2.0

//! The first fault inside one `AttributeValue` of a `Query` or `Scan` body: a
//! null, no type descriptor or more than one, a number the number rules
//! refuse and, on the service's older validation path, an empty or duplicate
//! set (see [`first_fault`]). [`Fault`] renders the fault with the text of
//! the request-time number check or the text that path wraps.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{AttributeValue, EMPTY_ATTRIBUTE_VALUE, MULTI_ATTRIBUTE_VALUE};
use extenddb_core::validation::number::{NOT_A_NUMBER, NUMBER_SET_DUPLICATES, has_equal_numbers};
use serde::Deserialize;
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
pub(super) enum Fault<'a> {
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
    /// An empty set, or a set two of whose members have one value, with the
    /// text of the service's older validation path (see [`set_fault`]).
    Set(String),
}

impl Fault<'_> {
    /// The request-time number check's text: the value echoed with each
    /// decimal digit folded to ASCII (measured 2026-11-11: `{"N": "\u{ff11}bob"}`
    /// answers `The parameter cannot be converted to a numeric value: 1bob`,
    /// `\u{661}\u{662}x` answers `12x`), and the colon kept after an empty one.
    pub(super) fn request_time_text(self) -> String {
        match self {
            Fault::NotANumber(text) => format!(
                "{NOT_A_NUMBER}: {}",
                crate::decimal_digits::fold_decimal_digits(text)
            ),
            other => other.reason(),
        }
    }

    /// The text `older_form_checks` wraps: a number echoed as written
    /// and no colon after an empty one (measured 2026-11-11).
    pub(super) fn reason(self) -> String {
        match self {
            Fault::Null(None) => NULL_VALUE.to_owned(),
            Fault::Null(Some(member)) => format!("{NULL_VALUE}: {member} is null"),
            Fault::Empty => EMPTY_ATTRIBUTE_VALUE.to_owned(),
            Fault::SeveralTypes => MULTI_ATTRIBUTE_VALUE.to_owned(),
            Fault::NotANumber("") => NOT_A_NUMBER.to_owned(),
            Fault::NotANumber(text) => format!("{NOT_A_NUMBER}: {text}"),
            Fault::Refused(msg) | Fault::Set(msg) => msg,
        }
    }
}

/// The first fault in `value` (`member` is its name when it is the value of an
/// `M` member): a null or no type descriptor, then the payloads in the order
/// of [`TYPE_DESCRIPTORS`] (the numbers in `N` and `NS`, and every value an
/// `L` or `M` holds), then more than one type descriptor. With `sets`, an
/// empty set or a set with a duplicate member is a fault too, `SS` ahead of
/// the numbers and `NS` after its own numbers (see [`set_fault`]); without
/// it, the deserializer reports them after the request-time checks. Both
/// paths check the numbers of a value the service reads as one type ahead of
/// its sets, the newer one with the deserializer's texts (see `validate_sets`
/// in `scan_request`).
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
/// ahead of `NS`, `NS` and `M` ahead of `L`; measured 2026-11-24: `{"SS":
/// ["a", "a"], "N": "bob"}` answers the duplicate text, `{"NS": ["1", "1"],
/// "N": "bob"}` the numeric text, and `{"NS": ["1", "1"], "L": [{"N":
/// "bob"}]}` the duplicate text).
pub(super) fn first_fault<'a>(
    value: &'a Value,
    member: Option<&'a str>,
    sets: bool,
) -> Option<Fault<'a>> {
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
                .find_map(number_fault)
                .or_else(|| sets.then(|| set_fault(descriptor, members)).flatten()),
            ("SS" | "BS", Value::Array(members)) if sets => set_fault(descriptor, members),
            ("L", Value::Array(items)) => {
                items.iter().find_map(|item| first_fault(item, None, sets))
            }
            ("M", Value::Object(members)) => members
                .iter()
                .find_map(|(name, inner)| first_fault(inner, Some(name), sets)),
            _ => None,
        });
    match payloads.len() {
        0 => Some(Fault::Empty),
        1 => fault,
        _ => fault.or(Some(Fault::SeveralTypes)),
    }
}

/// The service's older-path refusal of a set of type `descriptor` whose
/// `members` are strings: `Input collection contains duplicates` for a number
/// set two of whose members have one value, else the deserializer's text for
/// an empty set or a string or binary set that holds a member twice (see
/// [`deserializer_fault`]). Measured 2026-11-24 05:41 UTC (us-east-1, each
/// request twice, a table that does not exist), each beside `Limit: 1.5` and
/// a `FilterExpression` that reads the value, on `Scan`: `{"NS": ["1",
/// "1"]}` as an entry and inside an `L` answers `ExpressionAttributeValues
/// contains invalid value: Input collection contains duplicates for key :a`,
/// `{"NS": []}` answers `... One or more parameter values were invalid: An
/// number set  may not be empty for key :a`, and `{"SS": ["x", "x"]}`
/// answers `... One or more parameter values were invalid: Input collection
/// [x, x] contains duplicates. for key :a`; on `Query` the duplicate `NS`
/// answers the same as on `Scan`. Beside a second type descriptor the texts
/// are the same (measured 2026-11-24 on `Query`, each body twice, as `...
/// for key :z`): `{"SS": ["a", "a"], "N": "1"}` answers `One or more
/// parameter values were invalid: Input collection [a, a] contains
/// duplicates.`, `{"NS": ["1", "1"], "S": "x"}` `Input collection contains
/// duplicates`, `{"BS": ["YQ==", "YQ=="], "N": "1"}` `One or more parameter
/// values were invalid: Input collection [YQ==, YQ==]of type BS contains
/// duplicates.`, and an empty `SS`, `NS` or `BS` beside a second descriptor
/// the deserializer's empty set text. A member of another JSON type is left
/// to the shape fault check.
fn set_fault<'a>(descriptor: &str, members: &[Value]) -> Option<Fault<'a>> {
    if !members.iter().all(Value::is_string) {
        return None;
    }
    if descriptor == "NS" && has_equal_numbers(members) {
        return Some(Fault::Set(NUMBER_SET_DUPLICATES.to_owned()));
    }
    let set = Value::Object(Map::from_iter([(
        descriptor.to_owned(),
        Value::Array(members.to_vec()),
    )]));
    match deserializer_fault(&set) {
        Some(DynamoDbError::ValidationException(msg)) => Some(Fault::Set(msg)),
        _ => None,
    }
}

/// The `ValidationException` the `AttributeValue` deserializer reports for
/// `value`, such as an empty set and a set with a duplicate member, with the
/// deserializer's text, in the value or in any value an `L` or `M` holds.
/// `None` for a value it reads, and for one of the wrong JSON shape, which
/// the shape fault check reports.
pub(crate) fn deserializer_fault(value: &Value) -> Option<DynamoDbError> {
    match AttributeValue::deserialize(value).map_err(crate::deserialize_error) {
        Err(fault @ DynamoDbError::ValidationException(_)) => Some(fault),
        _ => None,
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
