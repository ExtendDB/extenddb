// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Number checks for the keys, items and expression attribute values that the
//! `PutItem` item check and the `UpdateItem` value checks do not cover, with
//! the text each operation answers.
//!
//! Every operation here reads a number as [`validate_and_normalize_number`]
//! does, with a 64-bit exponent. Measured in us-east-1, every request twice
//! with the same answer both times: `GetItem`, `BatchGetItem`, `UpdateItem`
//! and `DeleteItem` on 2026-10-27, and `PutItem`, `BatchWriteItem`,
//! `TransactWriteItems` and `TransactGetItems` on 2026-11-11, where
//! `10E2147483647` and `1E2147483648` are a `Number overflow` on all four
//! paths. The operations differ in the text for an empty value: keys and
//! items of `GetItem`, `BatchGetItem`, `PutItem`, `UpdateItem` and
//! `DeleteItem` answer `The parameter cannot be converted to a numeric value: `
//! with nothing after the colon, and keys and items of `BatchWriteItem`,
//! `TransactGetItems` and `TransactWriteItems` answer the message without the
//! colon (measured 2026-10-27). An empty value in `ExpressionAttributeValues`
//! answers with the colon on `UpdateItem` and `TransactWriteItems` (measured
//! 2026-11-11).
//!
//! `ImportTable` reads the numbers of a row with a 32-bit exponent and scale
//! (measured 2026-11-03 and 2026-11-11; see [`import_row`] and
//! [`validate_number_i32_scale`]).
//!
//! `PutItem`, `UpdateItem` and `DeleteItem` start the message with
//! `1 validation error detected: `, and the other operations do not. A
//! number in `ExpressionAttributeValues`, or inside an `M`, `L` or `NS` value
//! there, answers the number message in the same form on each operation, with
//! no wrapper (measured 2026-11-11; on 2026-10-27 `TransactWriteItems`
//! answered `ExpressionAttributeValues contains invalid value: <message> for
//! key <placeholder>`). `TransactWriteItems` checks those values for every
//! `Update` and `ConditionCheck`, and for a `Put` or `Delete` only when the
//! action has a `ConditionExpression`, empty or not.
//!
//! When several placeholders in `ExpressionAttributeValues` are malformed, the
//! service named a different one on two sends of the same request, so these
//! checks visit placeholders in name order. The attributes of a key or an item
//! are visited in the order of the map.
//!
//! [`validate_and_normalize_number`]: super::validate_and_normalize_number

use std::collections::HashMap;

use super::{MAX_MAGNITUDE, MAX_SIGNIFICANT_DIGITS, MIN_MAGNITUDE, NumberFault, normalize_number};
use crate::error::DynamoDbError;
use crate::types::{
    AttributeValue, BatchWriteItemInput, DeleteItemInput, ExpectedAttributeValue, GetItemInput,
    Item, PutItemInput, TransactGetItem, TransactWriteItem, UpdateItemInput,
};

/// The prefix `PutItem`, `UpdateItem` and `DeleteItem` put on the message.
const SINGLE_WRITE_PREFIX: &str = "1 validation error detected: ";

/// The text an operation gives for an empty number.
#[derive(Clone, Copy)]
enum EmptyText {
    /// The message and a colon with nothing after it: keys and items of
    /// `GetItem`, `BatchGetItem`, `PutItem`, `UpdateItem` and `DeleteItem`
    /// (measured 2026-10-27), and `ExpressionAttributeValues` (measured on
    /// `UpdateItem` and on a `TransactWriteItems` `Update` on 2026-11-11).
    WithColon,
    /// The message without the colon: keys and items of `BatchWriteItem`,
    /// `TransactGetItems` and `TransactWriteItems` (measured 2026-10-27).
    WithoutColon,
}

/// Refuse a `GetItem` key holding a number the service cannot read.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn get_item(input: &GetItemInput) -> Result<(), DynamoDbError> {
    item_numbers(EmptyText::WithColon, &input.key).map_err(bare)
}

/// Refuse one `BatchGetItem` table's keys when one holds a number the service
/// cannot read.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn batch_get_keys(keys: &[Item]) -> Result<(), DynamoDbError> {
    keys.iter()
        .try_for_each(|key| item_numbers(EmptyText::WithColon, key))
        .map_err(bare)
}

/// Refuse a `DeleteItem` key holding a number the service cannot read.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn delete_item_key(input: &DeleteItemInput) -> Result<(), DynamoDbError> {
    item_numbers(EmptyText::WithColon, &input.key).map_err(single_write)
}

/// Refuse `DeleteItem` `ExpressionAttributeValues` holding a value nested too
/// deep or a number the service cannot read, whether or not an expression
/// reads the value.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn delete_item_values(input: &DeleteItemInput) -> Result<(), DynamoDbError> {
    values_nesting_then_numbers(input.expression_attribute_values.as_ref()).map_err(single_write)
}

/// Refuse an `UpdateItem` key holding a number the service cannot read.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn update_item_key(input: &UpdateItemInput) -> Result<(), DynamoDbError> {
    item_numbers(EmptyText::WithColon, &input.key).map_err(single_write)
}

/// Refuse `PutItem` `ExpressionAttributeValues` holding a value nested too
/// deep or a number the service cannot read, whether or not an expression
/// reads the value.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn put_item_values(input: &PutItemInput) -> Result<(), DynamoDbError> {
    values_nesting_then_numbers(input.expression_attribute_values.as_ref()).map_err(single_write)
}

/// Refuse a legacy `Expected` map of `PutItem`, `DeleteItem` or `UpdateItem`
/// holding a number the service cannot read in a `Value` or an
/// `AttributeValueList` member.
///
/// Measured 2026-11-11 (us-east-1, each request twice) on items that do not
/// exist: `abc` and `1E2147483648` in `Expected {a: {Value: N}}` answer a
/// `ValidationException` on `PutItem`, `DeleteItem` and `UpdateItem`, and in
/// `Expected {a: {ComparisonOperator: EQ, AttributeValueList: [N]}}` on
/// `PutItem`, with the prefix and the number message. `1E125` in the same
/// `PutItem` request answers `ConditionalCheckFailedException`, so the number
/// is read ahead of the condition. Attributes are visited in name order, and
/// a `Value` ahead of the list.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn expected_values(
    expected: Option<&HashMap<String, ExpectedAttributeValue>>,
) -> Result<(), DynamoDbError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let mut names: Vec<&String> = expected.keys().collect();
    names.sort();
    for name in names {
        let entry = &expected[name];
        entry
            .value
            .iter()
            .chain(entry.attribute_value_list.iter().flatten())
            .try_for_each(|value| attribute_numbers(EmptyText::WithColon, value))
            .map_err(single_write)?;
    }
    Ok(())
}

/// Refuse a `PutItem` item holding a number the service cannot read or a
/// value nested too deep, ahead of the mixed-parameter check, the table lookup
/// and the key schema checks (measured 2026-10-27).
///
/// Numbers come before nesting depth, the order `validate_put_item` checks
/// them in and so the order `PutItem` answered before this early check. The
/// service reports whichever of the two comes first in the request body,
/// which `Item` does not keep, on the two rows measured that answered the
/// same on both sends; two other rows answered nesting on one send and the
/// number on the other.
///
/// The message carries no prefix, as `PutItem`'s item errors already ship
/// without one.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the item's text.
pub fn put_item_item(input: &PutItemInput) -> Result<(), DynamoDbError> {
    crate::validation::validate_item_numbers(&input.item)?;
    crate::validation::validate_item_nesting_depth(&input.item)
}

/// Refuse a `TransactGetItems` key holding a number the service cannot read.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn transact_get_item(item: &TransactGetItem) -> Result<(), DynamoDbError> {
    item_numbers(EmptyText::WithoutColon, &item.get.key).map_err(bare)
}

/// Refuse a `BatchWriteItem` request when a put item or a delete key holds a
/// value nested too deep or a number the service cannot read.
///
/// Within one write request the service reports a value nested too deep ahead
/// of a malformed number, and across requests the earlier request's fault
/// (measured 2026-10-27). Tables are visited in name order so that a request
/// answers the same way each time `RequestItems`, a map, is read. The
/// service's order for two tables was measured as neither body order nor
/// name order: with two tables each holding a different malformed value, it
/// named the same table's value for both body orders (2026-10-27, four sends
/// each).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn batch_write_item(input: &BatchWriteItemInput) -> Result<(), DynamoDbError> {
    let mut table_names: Vec<&String> = input.request_items.keys().collect();
    table_names.sort();
    for table_name in table_names {
        for request in &input.request_items[table_name] {
            if let Some(put) = &request.put_request {
                crate::validation::validate_item_nesting_depth(&put.item)?;
                item_numbers(EmptyText::WithoutColon, &put.item).map_err(bare)?;
            }
            if let Some(delete) = &request.delete_request {
                item_numbers(EmptyText::WithoutColon, &delete.key).map_err(bare)?;
            }
        }
    }
    Ok(())
}

/// Refuse one `TransactWriteItems` action holding a number the service cannot
/// read: its `ExpressionAttributeValues` first, then its key or item. Both
/// answer the number message with no prefix and no wrapper (measured
/// 2026-11-11 on `Update`, `Put`, `Delete` and `ConditionCheck` values and on
/// a `Delete` key).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn transact_write_item(item: &TransactWriteItem) -> Result<(), DynamoDbError> {
    let (values, key_or_item) = if let Some(put) = &item.put {
        let values = put.expression_attribute_values.as_ref();
        (
            values.filter(|_| put.condition_expression.is_some()),
            &put.item,
        )
    } else if let Some(delete) = &item.delete {
        let values = delete.expression_attribute_values.as_ref();
        (
            values.filter(|_| delete.condition_expression.is_some()),
            &delete.key,
        )
    } else if let Some(update) = &item.update {
        (update.expression_attribute_values.as_ref(), &update.key)
    } else if let Some(check) = &item.condition_check {
        (check.expression_attribute_values.as_ref(), &check.key)
    } else {
        return Ok(());
    };
    value_numbers(EmptyText::WithColon, values).map_err(bare)?;
    item_numbers(EmptyText::WithoutColon, key_or_item).map_err(bare)
}

/// Refuse `UpdateItem` `ExpressionAttributeValues` holding a number the
/// service cannot read, with the prefix and the number message. Placeholders
/// are visited in name order.
///
/// Measured 2026-11-11 (us-east-1, each request twice) with `SET n = :v`:
/// `abc` and `12e` answer `1 validation error detected: The parameter cannot
/// be converted to a numeric value: abc` (`12e` likewise), an empty `N` the
/// same message with nothing after the colon, and `1E126` and
/// `1E2147483648` the prefix and `Number overflow`. A number inside an `M`,
/// `L` or `NS` value answers the inner number's message in the same form. No
/// answer names the placeholder.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn update_item_values(input: &UpdateItemInput) -> Result<(), DynamoDbError> {
    value_numbers(
        EmptyText::WithColon,
        input.expression_attribute_values.as_ref(),
    )
    .map_err(single_write)
}

/// Why `ImportTable` refuses a row holding a number the service cannot read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportRowFault {
    /// A value that is not a number. The service refuses the row and does not
    /// process the rows after it ("Expected a valid numeric. Remainder of the
    /// file will not be processed.").
    Malformed,
    /// A number outside the supported range or with more than 38 digits, with
    /// its fault. The service refuses the row and imports the rows after it.
    OutOfRange(NumberFault),
}

/// Refuse an `ImportTable` row holding a number the service cannot read.
///
/// Measured 2026-11-03 (us-east-1), one import from S3 per value, each file a
/// row with a valid number, the row under test, and another valid row:
///
/// - The service read a row's numbers as `BatchWriteItem` then did, with a
///   32-bit exponent and scale; see [`validate_number_i32_scale`]. `abc`,
///   ` 5`, an empty value, `12e`, `1E2147483648` and `1E-2147483648` are not
///   numbers, and `1E2147483647` is a `Number underflow`.
/// - A row holding a value that is not a number stops the import of the
///   file: the row and the rows after it are not imported, and the row is
///   the last one counted as processed. This holds whatever else the row
///   holds: a number out of range ahead of it in the row, or no key.
/// - A row holding a number out of range, or with more than 38 digits, is
///   not imported and the rows after it are.
///
/// Either way the row counts as one error, and numbers in an `NS`, `L` or `M`
/// count as the row's numbers. A row with no key and the number `1E126` was
/// logged as `Number overflow`, and a row whose key has the wrong type and
/// the number `1E126` as the type mismatch; the caller checks numbers before
/// keys, so only the second row's log line differs.
///
/// Two values are read differently from the service:
///
/// - A zero whose exponent fits in 32 bits reaches this check as `0` (see
///   [`validate_number_i32_scale`]), so a row holding `0E126`, which the
///   service refuses as `Number overflow`, is imported with `0`.
/// - The service reads a Unicode decimal digit as the digit (a row holding
///   `\u{ff15}` was imported with `5`). This check does not, as no other
///   number check does, so such a row ends the import of the file.
///
/// # Errors
///
/// Returns the row's [`ImportRowFault`]: `Malformed` when any value is not a
/// number, and otherwise the first number out of range, in the order of the
/// item's map.
pub fn import_row(item: &Item) -> Result<(), ImportRowFault> {
    let mut numbers = Vec::new();
    for value in item.values() {
        collect_numbers(value, &mut numbers);
    }
    let mut out_of_range = None;
    for n in numbers {
        match validate_number_i32_scale(n) {
            Ok(()) => {}
            Err(fault) if fault.is_not_a_number() => return Err(ImportRowFault::Malformed),
            Err(fault) => {
                out_of_range.get_or_insert(fault);
            }
        }
    }
    out_of_range.map_or(Ok(()), |fault| Err(ImportRowFault::OutOfRange(fault)))
}

/// Every number in `value`: an `N`, the members of an `NS`, and the numbers
/// nested in an `L` or `M`.
fn collect_numbers<'a>(value: &'a AttributeValue, numbers: &mut Vec<&'a str>) {
    match value {
        AttributeValue::N(n) => numbers.push(n),
        AttributeValue::NS(set) => numbers.extend(set.iter().map(String::as_str)),
        AttributeValue::L(list) => list.iter().for_each(|v| collect_numbers(v, numbers)),
        AttributeValue::M(map) => map.values().for_each(|v| collect_numbers(v, numbers)),
        _ => {}
    }
}

/// The first fault in an `ExpressionAttributeValues` map: a value nested too
/// deep, then a number the service cannot read. That is the order `PutItem`
/// and `DeleteItem` answered before these checks; the service answered the
/// nesting on one send and the number on the other (measured 2026-10-27).
fn values_nesting_then_numbers(
    values: Option<&HashMap<String, AttributeValue>>,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    crate::validation::validate_attribute_values_nesting_depth(values.values()).map_err(message)?;
    value_numbers(EmptyText::WithColon, Some(values))
}

/// The first number in `item` the service cannot read, as its message.
fn item_numbers(empty: EmptyText, item: &Item) -> Result<(), String> {
    item.values()
        .try_for_each(|value| attribute_numbers(empty, value))
}

/// The first number in `values` the service cannot read, as its message.
/// Placeholders are visited in name order.
fn value_numbers(
    empty: EmptyText,
    values: Option<&HashMap<String, AttributeValue>>,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    let mut keys: Vec<&String> = values.keys().collect();
    keys.sort();
    keys.into_iter()
        .try_for_each(|key| attribute_numbers(empty, &values[key]))
}

/// The first number inside `value` (an `N`, an `NS` member, or a value nested
/// in an `L` or `M`) the service cannot read, as its message.
fn attribute_numbers(empty: EmptyText, value: &AttributeValue) -> Result<(), String> {
    match value {
        AttributeValue::N(n) => number_text(empty, n),
        AttributeValue::NS(set) => set.iter().try_for_each(|n| number_text(empty, n)),
        AttributeValue::L(list) => list.iter().try_for_each(|v| attribute_numbers(empty, v)),
        AttributeValue::M(map) => map.values().try_for_each(|v| attribute_numbers(empty, v)),
        _ => Ok(()),
    }
}

/// One number's text, as the message the service gives when it cannot read
/// it, with the operation's text for an empty value.
fn number_text(empty: EmptyText, n: &str) -> Result<(), String> {
    match empty {
        EmptyText::WithColon if n.is_empty() => Err(format!("{}: ", NumberFault::Empty.text())),
        // `NumberFault::message` gives an empty value without the colon.
        _ => normalize_number(n)
            .map(drop)
            .map_err(|fault| fault.message(n)),
    }
}

/// Check `s` as the service reads a number in an `ImportTable` row, and
/// return the message for the first rule it breaks. These rules were measured
/// on `BatchWriteItem` on 2026-10-27 (us-east-1, every value twice), which
/// read numbers this way then, and the `ImportTable` rows measured on
/// 2026-11-03 follow them (see [`import_row`]). On 2026-11-11 `BatchWriteItem`
/// read numbers as `PutItem` does; `ImportTable` was not measured again.
///
/// 1. `s` is an optional sign, digits with at most one point and at least one
///    digit, and an optional exponent: `e` or `E`, an optional sign and
///    digits. The exponent must fit in a 32-bit integer, and so must the
///    scale, the number of fraction digits less the exponent. Otherwise the
///    value cannot be converted, as `1E2147483648`, `1E-2147483648` and
///    `1.5E-2147483648` cannot.
/// 2. The magnitude is the precision less the scale, where the precision is
///    the number of digits without the leading zeros (1 for a zero), computed
///    in 32-bit arithmetic that wraps. Above 126 is a `Number overflow`
///    (`1E126`) and below -129 a `Number underflow` (`1E-131`). The magnitude
///    of `1E2147483647` and `10E2147483647` wraps past the 32-bit maximum, so
///    each is an underflow.
/// 3. More than 38 digits, not counting leading and trailing zeros, is refused
///    after the magnitude: 39 ones answer the 38-digit message, and 39 ones
///    with the exponent 200 answer `Number overflow`.
///
/// The service applies these rules to a zero too (`0E126` is an overflow), but
/// a zero whose exponent fits in 32 bits does not reach this check: the
/// request's `AttributeValue` holds `0` for it, since
/// [`validate_and_normalize_number`] range checks only a zero whose exponent
/// does not fit (#618).
///
/// Java's `new BigDecimal(s)` reads a number this way. It also reads any
/// Unicode decimal digit as a digit, and so does the service (`\u{ff15}` is
/// stored as `5` on every operation measured). This check refuses those, as
/// [`validate_and_normalize_number`] does.
///
/// [`validate_and_normalize_number`]: super::validate_and_normalize_number
fn validate_number_i32_scale(s: &str) -> Result<(), NumberFault> {
    if s.is_empty() {
        return Err(NumberFault::Empty);
    }
    let unsigned = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (
            mantissa,
            exponent
                .parse::<i32>()
                .map_err(|_| NumberFault::Malformed)?,
        ),
        None => (unsigned, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits_only = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if (int_part.is_empty() && frac_part.is_empty())
        || !digits_only(int_part)
        || !digits_only(frac_part)
    {
        return Err(NumberFault::Malformed);
    }
    let scale = i64::try_from(frac_part.len())
        .ok()
        .and_then(|fraction_digits| i32::try_from(fraction_digits - i64::from(exponent)).ok())
        .ok_or(NumberFault::Malformed)?;

    let digits = format!("{int_part}{frac_part}");
    let unscaled = digits.trim_start_matches('0');
    let precision = i32::try_from(unscaled.len().max(1)).map_err(|_| NumberFault::Malformed)?;
    let magnitude = precision.wrapping_sub(scale);
    if i64::from(magnitude) > MAX_MAGNITUDE {
        return Err(NumberFault::Overflow);
    }
    if i64::from(magnitude) < MIN_MAGNITUDE {
        return Err(NumberFault::Underflow);
    }
    if unscaled.trim_end_matches('0').len() > MAX_SIGNIFICANT_DIGITS {
        return Err(NumberFault::TooManyDigits);
    }
    Ok(())
}

/// The text of a validation error.
fn message(err: DynamoDbError) -> String {
    match err {
        DynamoDbError::ValidationException(message) => message,
        other => other.to_string(),
    }
}

/// A message the operation reports as it is.
fn bare(message: String) -> DynamoDbError {
    DynamoDbError::ValidationException(message)
}

/// A message with the prefix `PutItem`, `UpdateItem` and `DeleteItem` use.
fn single_write(message: String) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("{SINGLE_WRITE_PREFIX}{message}"))
}

#[cfg(test)]
mod tests;
