// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A table name of a bad length, in the operations that look a table up by
//! it. These checks take the deployment's `[limits]` bounds, 3 to 255
//! characters by default, as [`validate_table_name`] does.
//!
//! The service refuses such a name with `ValidationException` before the
//! lookup, and answers a member of the wrong JSON type ahead of it: `GetItem`,
//! `BatchGetItem`, `BatchWriteItem` and `TransactWriteItems` answered
//! `SerializationException` for a short name beside a member of the wrong
//! type (measured 2026-11-18). So the handlers run these checks after the
//! request is read and before the first lookup. A name outside the pattern is
//! refused earlier, before authorization, by the server's table name gate,
//! which writes its length clauses with [`length_clause`] and the service's
//! bounds.
//!
//! An empty name gets other sentences from the service, which these checks
//! leave to the code after them, except in `GetItem` and `PutItem`.

use crate::error::{
    DynamoDbError, ErrorMessageKey, error_message, validation_error, validation_errors,
};
use crate::limits::LimitsConfig;
use crate::types::TransactWriteItem;

use super::validate_table_name;

/// The constraint list of a `RequestItems` key that is not a table name the
/// service accepts (measured 2026-10-24 for a key outside the pattern and
/// 2026-11-18 for a key of a bad length).
pub const MAP_KEYS_NAME: &str = "Map keys must satisfy constraint: \
    [Member must have length less than or equal to 255, \
    Member must have length greater than or equal to 3, \
    Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]";

/// The clause of a table name under the minimum length. It names the
/// service's minimum, 3, whatever the deployment's is, as the sentence of
/// [`validate_table_name`] does.
pub(crate) const TOO_SHORT: &str = "Member must have length greater than or equal to 3";

/// The clause of a table name over the maximum length, which names the
/// service's maximum, 255, in the same way.
pub(crate) const TOO_LONG: &str = "Member must have length less than or equal to 255";

/// The length clause a table name fails under the bounds `(min, max)`, in
/// characters, or `None` for a name within them.
#[must_use]
pub fn length_clause(name: &str, (min, max): (usize, usize)) -> Option<&'static str> {
    match name.chars().count() {
        n if n < min => Some(TOO_SHORT),
        n if n > max => Some(TOO_LONG),
        _ => None,
    }
}

/// The deployment's table name bounds `(min, max)`: the `[limits]` values
/// `min_table_name_length` and `max_table_name_length`, 3 and 255 by default.
#[must_use]
pub fn table_name_bounds(limits: &LimitsConfig) -> (usize, usize) {
    (limits.min_table_name_length, limits.max_table_name_length)
}

/// Where the service reports the table name of a `TransactItems` action:
/// `transactItems.<n>.member.<op>.tableName`, with `n` counted from 1 and the
/// action's member name (`ConditionCheck`) in lower camel case.
#[must_use]
pub fn transact_name_location(index: usize, member: &str) -> String {
    let mut chars = member.chars();
    let op = match chars.next() {
        Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    };
    format!("transactItems.{}.member.{op}.tableName", index + 1)
}

/// The table name check `GetItem` and `PutItem` run before the lookup. The
/// service answers an empty name in both with `Value at 'TableName'` and a
/// length of at least 1 (measured 2026-11-18). Any other name gets the answer
/// of [`validate_table_name`].
///
/// # Errors
///
/// `ValidationException` for an empty name or one that
/// [`validate_table_name`] refuses.
pub fn validate_item_table_name(name: &str, limits: &LimitsConfig) -> Result<(), DynamoDbError> {
    if name.is_empty() {
        return Err(DynamoDbError::ValidationException(error_message(
            ErrorMessageKey::ItemTableNameEmpty,
            &[],
        )));
    }
    validate_table_name(name, limits)
}

/// A `RequestItems` key of a bad length, refused with the service's sentence
/// at `requestItems`. The service quotes its rendering of the whole map in
/// `Value '...'`, and this renders the key alone, as the server's table name
/// gate does for a key outside the pattern. An empty key and a table ARN are
/// left to the code after this.
///
/// # Errors
///
/// `ValidationException` for the smallest such key. The keys come from a
/// `HashMap`, whose order changes from one request to the next.
pub fn validate_request_items_keys<'a>(
    keys: impl IntoIterator<Item = &'a String>,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    let bounds = table_name_bounds(limits);
    let refused = keys
        .into_iter()
        .filter(|k| !k.is_empty() && !k.starts_with("arn:") && length_clause(k, bounds).is_some())
        .min();
    if let Some(key) = refused {
        return Err(DynamoDbError::ValidationException(validation_error(
            key,
            "requestItems",
            MAP_KEYS_NAME,
        )));
    }
    Ok(())
}

/// The table names of a bad length in the actions of a `TransactWriteItems`
/// request. The service lists every action's fault in one message, in action
/// order (measured 2026-11-18). An empty name and a table ARN are left to the
/// code after this.
///
/// # Errors
///
/// `ValidationException` listing each such name.
pub fn validate_transact_write_names(
    items: &[TransactWriteItem],
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    let mut names: Vec<(&str, String)> = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let members = [
            (
                "ConditionCheck",
                item.condition_check.as_ref().map(|o| &o.table_name),
            ),
            ("Put", item.put.as_ref().map(|o| &o.table_name)),
            ("Delete", item.delete.as_ref().map(|o| &o.table_name)),
            ("Update", item.update.as_ref().map(|o| &o.table_name)),
        ];
        for (member, name) in members {
            if let Some(name) = name.filter(|n| !n.is_empty() && !n.starts_with("arn:")) {
                names.push((name, transact_name_location(i, member)));
            }
        }
    }
    let bounds = table_name_bounds(limits);
    let faults: Vec<(&str, &str, &str)> = names
        .iter()
        .filter_map(|(name, location)| {
            Some((*name, location.as_str(), length_clause(name, bounds)?))
        })
        .collect();
    if faults.is_empty() {
        return Ok(());
    }
    Err(DynamoDbError::ValidationException(validation_errors(
        &faults,
    )))
}
