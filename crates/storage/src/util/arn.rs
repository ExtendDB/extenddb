// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for constructing and parsing resource ARNs.

use crate::error::StorageError;

/// Returns an ARN for the specified `DynamoDB` index.
#[must_use]
pub fn index_arn(region: &str, account_id: &str, table_name: &str, index_name: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/index/{index_name}")
}

/// Returns an ARN for the specified `DynamoDB` stream.
#[must_use]
pub fn stream_arn(region: &str, account_id: &str, table_name: &str, stream_label: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/stream/{stream_label}")
}

/// Returns an ARN for the specified `DynamoDB` table.
#[must_use]
pub fn table_arn(region: &str, account_id: &str, table_name: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account_id}:table/{table_name}")
}

/// Parse a stream ARN into (`table_name`, `stream_label`).
///
/// Stream ARNs contain ISO 8601 timestamps with colons in the stream label,
/// e.g. `arn:aws:dynamodb:us-east-1:<account-id>:table/T/stream/2026-04-08T08:40:22`.
/// We use `splitn(6, ':')` so the 6th element preserves everything after the 5th
/// colon delimiter, including colons within the stream label.
pub fn parse_stream_arn(arn: &str) -> Result<(String, String), StorageError> {
    let segments: Vec<&str> = arn.splitn(6, ':').collect();
    let resource = segments
        .get(5)
        .ok_or_else(|| StorageError::Validation(format!("Invalid stream ARN: {arn}")))?;
    let parts: Vec<&str> = resource.splitn(4, '/').collect();
    if parts.len() == 4 && parts[0] == "table" && parts[2] == "stream" {
        Ok((parts[1].to_owned(), parts[3].to_owned()))
    } else {
        Err(StorageError::Validation(format!(
            "Invalid stream ARN format: {arn}"
        )))
    }
}
