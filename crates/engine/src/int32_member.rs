// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request members that the service reads as an int32, and the range
//! constraints on `Limit`, `Segment` and `TotalSegments`.

use extenddb_core::error::DynamoDbError;
use extenddb_core::error::validation_error_envelope;
use extenddb_core::types::saturated_int32;
use serde_json::Value;

/// Read a request member that the service reads as an int32, from a JSON
/// number as [`saturated_int32`] reads it (measured on the `Limit` of
/// `ExecuteStatement`, 2026-11-14, 2026-11-18 and 2026-11-20, us-east-1).
/// A number beyond the range of a double fails the parse of the request
/// body before this reader runs, where the service accepts `1e309`.
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
/// returns one item and `Limit: 2147483648` is accepted). A number beyond the
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
    DynamoDbError::ValidationException(validation_error_envelope(&clauses))
}

#[cfg(test)]
mod tests;
