// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A request member that the service reads as an int32.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::saturated_int32;
use serde_json::Value;

/// Read a request member that the service reads as an int32, from any JSON
/// number as [`saturated_int32`] reads it (measured on the `Limit` of
/// `ExecuteStatement`, 2026-11-14 and 2026-11-18, us-east-1).
/// A batch or transaction member's `Limit` is read but not used.
///
/// # Errors
///
/// The service's `SerializationException` for a member of another JSON type.
#[cfg_attr(not(feature = "partiql"), allow(dead_code))]
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
