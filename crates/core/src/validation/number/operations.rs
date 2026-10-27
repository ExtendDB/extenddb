// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Number checks for the keys, items and expression attribute values that the
//! `PutItem` item check and the `UpdateItem` value checks do not cover, with
//! the text each operation answers.
//!
//! Measured 2026-10-27 (us-east-1, every request twice, the same answer both
//! times), the service reads a number one of two ways:
//!
//! - `GetItem`, `BatchGetItem`, `PutItem`, `UpdateItem` and `DeleteItem` read
//!   it as [`validate_and_normalize_number`] does, with a 64-bit exponent. An
//!   empty value answers `The parameter cannot be converted to a numeric
//!   value: ` with nothing after the colon.
//! - `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems` read it with
//!   a 32-bit exponent and scale; see [`validate_number_i32_scale`]. An empty
//!   value answers the message without the colon.
//!
//! `PutItem`, `UpdateItem` and `DeleteItem` start the message with
//! `1 validation error detected: `, and the other operations do not.
//! `TransactWriteItems` reports a value in `ExpressionAttributeValues` as
//! `ExpressionAttributeValues contains invalid value: <message> for key
//! <placeholder>`. It checks those values for every `Update` and
//! `ConditionCheck`, and for a `Put` or `Delete` only when the action has a
//! `ConditionExpression`, empty or not.
//!
//! When several placeholders in `ExpressionAttributeValues` are malformed, the
//! service named a different one on two sends of the same request, so these
//! checks visit placeholders in name order. The attributes of a key or an item
//! are visited in the order of the map.

use std::collections::HashMap;

use super::{
    MAX_MAGNITUDE, MAX_SIGNIFICANT_DIGITS, MIN_MAGNITUDE, overflow_err, too_many_digits_err,
    underflow_err, validate_and_normalize_number,
};
use crate::error::DynamoDbError;
use crate::types::{
    AttributeValue, BatchWriteItemInput, DeleteItemInput, GetItemInput, Item, PutItemInput,
    TransactGetItem, TransactWriteItem, UpdateItemInput,
};

/// The service's text for a value that is not a number.
const NUMERIC_VALUE: &str = "The parameter cannot be converted to a numeric value";

/// The prefix `PutItem`, `UpdateItem` and `DeleteItem` put on the message.
const SINGLE_WRITE_PREFIX: &str = "1 validation error detected: ";

/// How an operation reads a number.
#[derive(Clone, Copy)]
enum Rules {
    /// `GetItem`, `BatchGetItem`, `PutItem`, `UpdateItem` and `DeleteItem`.
    I64Exponent,
    /// `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems`.
    I32Scale,
}

/// Refuse a `GetItem` key holding a number the service cannot read.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn get_item(input: &GetItemInput) -> Result<(), DynamoDbError> {
    item_numbers(Rules::I64Exponent, &input.key).map_err(bare)
}

/// Refuse one `BatchGetItem` table's keys when one holds a number the service
/// cannot read.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn batch_get_keys(keys: &[Item]) -> Result<(), DynamoDbError> {
    keys.iter()
        .try_for_each(|key| item_numbers(Rules::I64Exponent, key))
        .map_err(bare)
}

/// Refuse a `DeleteItem` key holding a number the service cannot read.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn delete_item_key(input: &DeleteItemInput) -> Result<(), DynamoDbError> {
    item_numbers(Rules::I64Exponent, &input.key).map_err(single_write)
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
    item_numbers(Rules::I64Exponent, &input.key).map_err(single_write)
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
    item_numbers(Rules::I32Scale, &item.get.key).map_err(bare)
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
                item_numbers(Rules::I32Scale, &put.item).map_err(bare)?;
            }
            if let Some(delete) = &request.delete_request {
                item_numbers(Rules::I32Scale, &delete.key).map_err(bare)?;
            }
        }
    }
    Ok(())
}

/// Refuse one `TransactWriteItems` action holding a number the service cannot
/// read: its `ExpressionAttributeValues` first, then its key or item.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's text.
pub fn transact_write_item(item: &TransactWriteItem) -> Result<(), DynamoDbError> {
    let (values, key_or_item) = if let Some(put) = &item.put {
        let values = put.condition_expression.as_ref();
        (
            values.and(put.expression_attribute_values.as_ref()),
            &put.item,
        )
    } else if let Some(delete) = &item.delete {
        let values = delete.condition_expression.as_ref();
        (
            values.and(delete.expression_attribute_values.as_ref()),
            &delete.key,
        )
    } else if let Some(update) = &item.update {
        (update.expression_attribute_values.as_ref(), &update.key)
    } else if let Some(check) = &item.condition_check {
        (check.expression_attribute_values.as_ref(), &check.key)
    } else {
        return Ok(());
    };
    value_numbers(Rules::I32Scale, values).map_err(|(message, key)| {
        DynamoDbError::ValidationException(format!(
            "ExpressionAttributeValues contains invalid value: {message} for key {key}"
        ))
    })?;
    item_numbers(Rules::I32Scale, key_or_item).map_err(bare)
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
    value_numbers(Rules::I64Exponent, Some(values)).map_err(|(message, _)| message)
}

/// The first number in `item` the service cannot read, as its message.
fn item_numbers(rules: Rules, item: &Item) -> Result<(), String> {
    item.values()
        .try_for_each(|value| attribute_numbers(rules, value))
}

/// The first number in `values` the service cannot read, as its message and
/// placeholder. Placeholders are visited in name order.
fn value_numbers(
    rules: Rules,
    values: Option<&HashMap<String, AttributeValue>>,
) -> Result<(), (String, &str)> {
    let Some(values) = values else {
        return Ok(());
    };
    let mut keys: Vec<&String> = values.keys().collect();
    keys.sort();
    for key in keys {
        attribute_numbers(rules, &values[key]).map_err(|message| (message, key.as_str()))?;
    }
    Ok(())
}

/// The first number inside `value` (an `N`, an `NS` member, or a value nested
/// in an `L` or `M`) the service cannot read, as its message.
fn attribute_numbers(rules: Rules, value: &AttributeValue) -> Result<(), String> {
    match value {
        AttributeValue::N(n) => number_text(rules, n),
        AttributeValue::NS(set) => set.iter().try_for_each(|n| number_text(rules, n)),
        AttributeValue::L(list) => list.iter().try_for_each(|v| attribute_numbers(rules, v)),
        AttributeValue::M(map) => map.values().try_for_each(|v| attribute_numbers(rules, v)),
        _ => Ok(()),
    }
}

/// One number's text under `rules`, as the message the service gives when it
/// cannot read it.
fn number_text(rules: Rules, n: &str) -> Result<(), String> {
    match rules {
        Rules::I64Exponent if n.is_empty() => Err(format!("{NUMERIC_VALUE}: ")),
        Rules::I64Exponent => validate_and_normalize_number(n).map(drop).map_err(message),
        Rules::I32Scale => validate_number_i32_scale(n),
    }
}

/// Check `s` as the service reads a number on `BatchWriteItem`,
/// `TransactGetItems` and `TransactWriteItems`, and return the message for the
/// first rule it breaks (measured 2026-10-27, us-east-1, every value twice):
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
fn validate_number_i32_scale(s: &str) -> Result<(), String> {
    if s.is_empty() {
        return Err(NUMERIC_VALUE.to_owned());
    }
    let malformed = || format!("{NUMERIC_VALUE}: {s}");
    let unsigned = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().map_err(|_| malformed())?),
        None => (unsigned, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits_only = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if (int_part.is_empty() && frac_part.is_empty())
        || !digits_only(int_part)
        || !digits_only(frac_part)
    {
        return Err(malformed());
    }
    let scale = i64::try_from(frac_part.len())
        .ok()
        .and_then(|fraction_digits| i32::try_from(fraction_digits - i64::from(exponent)).ok())
        .ok_or_else(malformed)?;

    let digits = format!("{int_part}{frac_part}");
    let unscaled = digits.trim_start_matches('0');
    let precision = i32::try_from(unscaled.len().max(1)).map_err(|_| malformed())?;
    let magnitude = precision.wrapping_sub(scale);
    if i64::from(magnitude) > MAX_MAGNITUDE {
        return Err(message(overflow_err()));
    }
    if i64::from(magnitude) < MIN_MAGNITUDE {
        return Err(message(underflow_err()));
    }
    if unscaled.trim_end_matches('0').len() > MAX_SIGNIFICANT_DIGITS {
        return Err(message(too_many_digits_err()));
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
