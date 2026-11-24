// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request members that the service reads as an int32, and the range
//! constraints on `Limit`, `Segment` and `TotalSegments`.

use std::collections::HashMap;

use extenddb_core::error::DynamoDbError;
use extenddb_core::error::validation_errors_detected;
use extenddb_core::types::saturated_int32;
use serde_json::Value;
use serde_json::value::RawValue;

/// Read a request member that the service reads as an int32, from a JSON
/// number as [`saturated_int32`] reads it (measured on the `Limit` of
/// `ExecuteStatement`, 2026-11-14, 2026-11-18 and 2026-11-20, us-east-1).
/// A number beyond the range of a double fails the parse of the request
/// body before this reader runs, where the service accepts `1e309`. A decimal
/// of 16 or more significant digits reaches this reader as the nearest
/// double; for `Scan` and `Query`, [`read_floats_as_written`] has put the
/// value of the written digits in the body ahead of it, and for
/// `ExecuteStatement` it has not (#1203).
/// A batch or transaction member's `Limit` is read but not used.
///
/// # Errors
///
/// The service's `SerializationException` for a member of another JSON type
/// (measured 2026-11-23 18:45 UTC on `Scan` and `Query` too: `Limit: "5"`,
/// `Segment: "1"`, `Limit: true` and `Limit: [1]`, each beside an invalid
/// `Select`, answer these texts; the array one has no message).
pub(crate) fn read(member: Option<&Value>) -> Result<Option<i32>, DynamoDbError> {
    let message = match member {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Number(number)) => return Ok(Some(saturated_int32(number))),
        Some(Value::String(_)) => "STRING_VALUE cannot be converted to Integer",
        Some(Value::Bool(true)) => "TRUE_VALUE cannot be converted to Integer",
        Some(Value::Bool(false)) => "FALSE_VALUE cannot be converted to Integer",
        Some(Value::Object(_)) => "Start of structure or map found where not expected",
        Some(Value::Array(_)) => "",
    };
    Err(DynamoDbError::SerializationException(message.to_owned()))
}

/// Replace each of `fields` in `body` with the int32 the service reads from
/// it (see [`read`]), so the operation's typed deserializer, which reads an
/// integer, gets the value the service uses for any JSON number the body
/// parser reads: `Limit: 1.5` reads as 1 and `Limit: 2147483648` as
/// 2147483647 (measured on `Scan` 2026-10-25, us-east-1: `Limit: 1.5`
/// returns one item and `Limit: 2147483648` is accepted). A float member
/// holds the value of its written digits by then; see
/// [`read_floats_as_written`]. A number beyond the
/// range of a double, which the service accepts, fails the body parse before
/// this runs (#1203), as [`read`] says. A null stays a null.
///
/// # Errors
///
/// [`read`]'s `SerializationException` for a member of another JSON type.
pub(crate) fn saturate(body: &mut Value, fields: &[RangeField]) -> Result<(), DynamoDbError> {
    for field in fields {
        if let Some(member) = body.get_mut(field.name)
            && let Some(value) = read(Some(member))?
        {
            *member = Value::from(value);
        }
    }
    Ok(())
}

/// Read each `Limit`, `Segment` or `TotalSegments` of a `Scan`, or the
/// `Limit` of a `Query`, that the body parser read as a float from the digits
/// the request wrote. The parser keeps a number with a fraction or an
/// exponent as the nearest double, so `0.99999999999999999999` reaches
/// [`saturated_int32`] as 1.0 and reads as 1, where the service truncates the
/// written decimal to 0. Measured 2026-11-24 05:41 UTC (us-east-1, each
/// request twice, a table that does not exist): `Limit: 0.99999999999999999999`
/// and `Limit: -0.99999999999999999999` on `Scan` and `Limit:
/// 0.99999999999999999999` on `Query` answer `Value '0' at 'limit' ...`,
/// `TotalSegments: 0.99999999999999999999` answers `Value '0' at
/// 'totalSegments' ...`, and `Segment: -0.99999999999999999999` of
/// `TotalSegments: 2` and a `Query` `Limit: 2.99999999999999999999` are
/// accepted (#1203).
///
/// Each such member becomes the float of the value the service reads (see
/// [`int32_from_decimal_text`]), so [`ranges`], [`read`] and [`saturate`]
/// give that value and the member is still a float, which keeps the request
/// on the older validation path (see [`Ranges::older_form`]). An integer
/// member is left as it is: the parser reads it exactly, and one beyond the
/// range of u64 saturates either way. `raw` is the request body `body` was
/// parsed from. A body that is not an object, and an operation other than
/// `Scan` and `Query`, are left as they are. A number beyond the range of a
/// double fails the body parse before this runs, where the service reads it
/// (#1203).
pub fn read_floats_as_written(operation: &str, body: &mut Value, raw: &[u8]) {
    let fields: &[RangeField] = match operation {
        "Scan" => &SCAN_RANGES,
        "Query" => &QUERY_RANGES,
        _ => return,
    };
    let floats: Vec<&RangeField> = fields
        .iter()
        .filter(|field| body.get(field.name).is_some_and(Value::is_f64))
        .collect();
    if floats.is_empty() {
        return;
    }
    let Ok(members) = serde_json::from_slice::<HashMap<String, &RawValue>>(raw) else {
        return;
    };
    for field in floats {
        if let Some(text) = members.get(field.name)
            && let Some(value) = int32_from_decimal_text(text.get())
            && let Some(member) = body.get_mut(field.name)
        {
            *member = Value::from(f64::from(value));
        }
    }
}

/// The int32 the service reads from a JSON number written as `text`: the
/// decimal truncated toward zero, then saturated to int32. `1.5` reads as 1,
/// `-0.99999999999999999999` as 0, `1e2` as 100, `2147483648.5` as
/// 2147483647 and `-1e300` as -2147483648. `None` when `text` is not a JSON
/// number or its exponent does not fit an i64.
fn int32_from_decimal_text(text: &str) -> Option<i32> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
        None => (unsigned, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    // The count of digits ahead of the decimal point once the exponent is
    // applied; none when the exponent moves the point to the left of them all.
    let point = i64::try_from(integer.len()).ok()?.checked_add(exponent)?;
    let Ok(point) = usize::try_from(point) else {
        return Some(0);
    };
    let mut magnitude = 0_i64;
    for digit in integer.chars().chain(fraction.chars()).take(point) {
        magnitude = magnitude
            .saturating_mul(10)
            .saturating_add(i64::from(digit.to_digit(10)?));
    }
    // The zeros the exponent adds past the written digits.
    let pad = point.saturating_sub(integer.len() + fraction.len());
    if magnitude > 0 && pad > 0 {
        magnitude = u32::try_from(pad)
            .ok()
            .and_then(|pad| 10_i64.checked_pow(pad))
            .map_or(i64::MAX, |scale| magnitude.saturating_mul(scale));
    }
    let value = if negative { -magnitude } else { magnitude };
    i32::try_from(value.clamp(i64::from(i32::MIN), i64::from(i32::MAX))).ok()
}

/// A member with a range constraint: its name in the request and in the newer
/// form of the fault, its name in the older form, and its bounds.
pub(crate) struct RangeField {
    pub(crate) name: &'static str,
    pub(crate) older_name: &'static str,
    pub(crate) min: i32,
    pub(crate) max: Option<i32>,
}

pub(crate) const LIMIT: RangeField = RangeField {
    name: "Limit",
    older_name: "limit",
    min: 1,
    max: None,
};

pub(crate) const SEGMENT: RangeField = RangeField {
    name: "Segment",
    older_name: "segment",
    min: 0,
    max: Some(999_999),
};

pub(crate) const TOTAL_SEGMENTS: RangeField = RangeField {
    name: "TotalSegments",
    older_name: "totalSegments",
    min: 1,
    max: Some(1_000_000),
};

/// The range-checked members of `Scan`, in the order the newer form reports
/// them.
pub(crate) const SCAN_RANGES: [RangeField; 3] = [LIMIT, SEGMENT, TOTAL_SEGMENTS];

/// The range-checked member of `Query`.
pub(crate) const QUERY_RANGES: [RangeField; 1] = [LIMIT];

/// A range fault: the member, the value the service reads (see
/// [`saturated_int32`]) and the constraint it fails.
pub(crate) struct RangeFault {
    pub(crate) field: &'static RangeField,
    pub(crate) value: i32,
    pub(crate) constraint: String,
}

impl RangeFault {
    /// The newer form's clause, the field capitalised and no value echoed:
    /// `Value at 'Limit' failed to satisfy constraint: Member must have value
    /// greater than or equal to 1`.
    fn newer_clause(&self) -> String {
        format!(
            "Value at '{}' failed to satisfy constraint: {}",
            self.field.name, self.constraint
        )
    }
}

/// The range members of `fields` read from `body` as the service reads them.
pub(crate) struct Ranges {
    /// Whether a member is a float or an integer outside int32. Such a member
    /// puts the request on the service's older validation path, whatever the
    /// other members hold; see `older_form_checks`.
    pub(crate) older_form: bool,
    /// Each fault, in the order of `fields`.
    pub(crate) faults: Vec<RangeFault>,
}

/// Check the range members of `fields` in `body`, each when present, whether
/// or not the others are. A member that is not a JSON number is left to the
/// shape fault check (see `shape_fault_first`).
pub(crate) fn ranges(body: &Value, fields: &'static [RangeField]) -> Ranges {
    let mut ranges = Ranges {
        older_form: false,
        faults: Vec::new(),
    };
    for field in fields {
        let Some(number) = body.get(field.name).and_then(Value::as_number) else {
            continue;
        };
        ranges.older_form |= number
            .as_i64()
            .is_none_or(|value| i32::try_from(value).is_err());
        let value = saturated_int32(number);
        let constraint = if value < field.min {
            Some(format!(
                "Member must have value greater than or equal to {}",
                field.min
            ))
        } else {
            field
                .max
                .filter(|max| value > *max)
                .map(|max| format!("Member must have value less than or equal to {max}"))
        };
        if let Some(constraint) = constraint {
            ranges.faults.push(RangeFault {
                field,
                value,
                constraint,
            });
        }
    }
    ranges
}

/// A list member whose length the service checks with the range constraints,
/// in the same message: its name in the newer and in the older form.
pub(crate) struct LengthField {
    pub(crate) name: &'static str,
    pub(crate) older_name: &'static str,
}

/// `Scan`'s `AttributesToGet`, which must hold one name or more: an empty
/// list answers `Value at 'AttributesToGet' failed to satisfy constraint:
/// Member must have length greater than or equal to 1` ahead of the range
/// clauses in the newer form (measured 2026-10-30 12:40 UTC: `Limit: 0` with
/// `AttributesToGet: []` answers the length clause, then the `Limit` clause),
/// and `Value '[]' at 'attributesToGet' ...` after every other clause in the
/// older form (measured 2026-11-23 21:49 UTC: beside `Limit: 0.0`, beside
/// `Select: "BAD"` and an empty value, and with the five other faults; see
/// `older_form_request_checks`).
pub(crate) const ATTRIBUTES_TO_GET: LengthField = LengthField {
    name: "AttributesToGet",
    older_name: "attributesToGet",
};

/// The length constraint of a [`LengthField`].
pub(crate) const AT_LEAST_ONE_MEMBER: &str = "Member must have length greater than or equal to 1";

/// The newer form of the range faults, for a request whose range members are
/// integers inside int32 and whose values the service can read: every fault
/// in one message, in the order of the fields, `1 validation error detected:
/// Value at 'Limit' failed to satisfy constraint: Member must have value
/// greater than or equal to 1`, or `3 validation errors detected: Value at
/// 'Limit' ...; Value at 'Segment' ...; Value at 'TotalSegments' ...`
/// (measured on `Scan` 2026-10-25 and 2026-10-31, us-east-1, each request
/// twice). The clause of `empty_list`, a list member whose length constraint
/// fails, comes first (see [`ATTRIBUTES_TO_GET`]).
pub(crate) fn newer_form(empty_list: Option<&LengthField>, faults: &[RangeFault]) -> DynamoDbError {
    let clauses: Vec<String> = empty_list
        .map(|list| {
            format!(
                "Value at '{}' failed to satisfy constraint: {AT_LEAST_ONE_MEMBER}",
                list.name
            )
        })
        .into_iter()
        .chain(faults.iter().map(RangeFault::newer_clause))
        .collect();
    DynamoDbError::ValidationException(validation_errors_detected(&clauses))
}

#[cfg(test)]
mod tests;
