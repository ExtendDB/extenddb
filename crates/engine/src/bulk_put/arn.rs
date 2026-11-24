// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk operation ARN construction.
//!
//! An operation ARN is `arn:aws:dynamodb:{region}:{account}:bulk-operation/
//! {operation-id}` and an operation id is 14 decimal digits, a hyphen, and
//! 8 lowercase hex digits. Callers treat the id as opaque; this module fills
//! it with the start time in epoch milliseconds zero-padded to 14 digits and
//! 32 random bits.
//!
//! Parsing lives in `extenddb_core::arn`, because the server's
//! authorization layer reads the same ARNs when deriving policy resources
//! and the two readings must not drift apart. `own_arn` below is the one
//! the handlers call: it runs the shape check, then the account and region
//! comparisons, then the operation id check, which is the order the hosted
//! service answers a request wrong at two layers (measured on the
//! service). `parse_arn` is the same reading without a caller to compare
//! against, kept for callers that have no account in hand. Whether the
//! operation exists belongs to the handler, which looks up by the caller's
//! account so a foreign ARN never resolves.

use std::time::{SystemTime, UNIX_EPOCH};

/// The no-caller-context parse. Every handler reads an ARN through
/// `own_arn` below, so this spelling is reached only by the tests that pin
/// layer 1's length bounds and layer 2's id shape from inside, where no
/// caller account is in play.
#[cfg(test)]
pub(crate) use extenddb_core::arn::parse_bulk_operation_arn as parse_arn;

use extenddb_core::error::DynamoDbError;

/// Read an operation ARN on behalf of the caller, in the order the hosted
/// service applies the layers: shape, then the account and region
/// comparisons, then the partition, service, and operation id (measured
/// on the service). A foreign-account ARN is denied even when its id
/// is malformed, so the caller learns nothing about another account's
/// operations or id shapes from the answer. The region answer is the
/// not-found error, decided here without a storage read.
///
/// # Errors
///
/// Returns the layer's own wire error.
pub(crate) fn own_arn(
    value: &str,
    region: &str,
    account: &str,
) -> Result<extenddb_core::arn::BulkOperationArn, DynamoDbError> {
    extenddb_core::arn::own_bulk_operation_arn(value, region, account).map_err(
        |error| match error {
            extenddb_core::arn::BulkOperationArnError::Shape => {
                extenddb_core::arn::bulk_operation_arn_shape_error()
            }
            // The server's authorization layer answers a foreign account
            // first, with the full denial format; this branch serves dev mode.
            extenddb_core::arn::BulkOperationArnError::ForeignAccount => {
                DynamoDbError::AccessDeniedException("Access is denied".to_owned())
            }
            extenddb_core::arn::BulkOperationArnError::ForeignRegion => {
                extenddb_core::arn::bulk_operation_does_not_exist(value)
            }
            extenddb_core::arn::BulkOperationArnError::InvalidId => {
                extenddb_core::arn::bulk_operation_arn_id_error()
            }
        },
    )
}

/// Build an operation id from a start time and 32 random bits: the time in
/// epoch milliseconds zero-padded to 14 decimal digits, a hyphen, and the
/// random bits as 8 lowercase hex digits.
pub(crate) fn new_operation_id(start_time_ms: u64, random: u32) -> String {
    // A 15-digit timestamp would break the 14-digit id shape; the bound is
    // unreachable before the year 5138.
    debug_assert!(start_time_ms < 100_000_000_000_000);
    format!("{start_time_ms:014}-{random:08x}")
}

/// Build an operation id from the current time and fresh random bits.
pub(crate) fn generate_operation_id() -> String {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    new_operation_id(now_ms, rand::random::<u32>())
}

/// Render the ARN for one bulk operation.
pub(crate) fn format_arn(region: &str, account: &str, operation_id: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account}:bulk-operation/{operation_id}")
}

/// Resolve a `TableName` value that may be a bare table name or a table ARN.
///
/// A bare name (any string not starting with `arn:`) passes through
/// unchanged. A string starting with `arn:` resolves only under the accept
/// rules of `extenddb_core::arn::own_table_arn_name`, which this function
/// shares with the server's authorization layer: exactly six
/// colon-separated parts, partition `aws`, service `dynamodb`, the
/// caller's region and account, resource `table/` plus a non-empty name
/// containing neither `/` nor `:`. A name never resolves across account,
/// region, or partition boundaries.
///
/// # Errors
///
/// A well-formed table ARN naming another account is denied with `Access
/// is denied` (measured), the string the engine already uses for the same
/// case on backups and tagging. One of the caller's account naming
/// another region is `ValidationException`, `1 validation error detected:
/// Invalid AWS region in '{arn}'` (measured), at validation step 2. An
/// own-account ARN whose name breaks the table-name rule answers the
/// measured `Table name of ARN` clause. Every other non-resolving `arn:`
/// spelling is `ValidationException` naming the value. The texts come from
/// `extenddb_core::arn`, which the server's authorization layer answers
/// with too.
pub(crate) fn table_name_from_name_or_arn(
    value: &str,
    region: &str,
    account: &str,
) -> Result<String, DynamoDbError> {
    if !value.starts_with("arn:") {
        return Ok(value.to_owned());
    }
    let name = extenddb_core::arn::own_table_arn_name(value, region, account)
        .map_err(|error| extenddb_core::arn::table_arn_refusal(value, region, account, error))?;
    extenddb_core::arn::check_table_arn_name(value, &name)?;
    Ok(name)
}
