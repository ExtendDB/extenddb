// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `DynamoDB` Streams operation handlers.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{
    DescribeStreamInput, DescribeStreamOutput, GetRecordsInput, GetRecordsOutput,
    GetShardIteratorInput, GetShardIteratorOutput, ListStreamsInput, ListStreamsOutput,
    ShardIteratorType,
};
use extenddb_storage::error::StorageError;
use serde_json::Value;

use crate::OperationContext;
use crate::serialize_output;

/// Handle `DescribeStream`.
///
/// # Errors
///
/// Returns [`DynamoDbError::ResourceNotFoundException`] if the stream does not exist.
/// Returns [`DynamoDbError::ValidationException`] on invalid input.
pub async fn handle_describe_stream(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: DescribeStreamInput = serde_json::from_value(body)
        .map_err(|e| DynamoDbError::SerializationException(e.to_string()))?;

    let desc = ctx
        .storage
        .describe_stream(&ctx.account_id, &input)
        .await
        .map_err(storage_to_dynamo)?;

    let output = DescribeStreamOutput {
        stream_description: desc,
    };
    serialize_output(&output)
}

/// Handle `ListStreams`.
///
/// # Errors
///
/// Returns [`DynamoDbError::ValidationException`] on invalid input.
pub async fn handle_list_streams(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: ListStreamsInput = serde_json::from_value(body)
        .map_err(|e| DynamoDbError::SerializationException(e.to_string()))?;

    let limit = input.limit.unwrap_or(100).min(100);
    let (streams, last_arn) = ctx
        .storage
        .list_streams(
            &ctx.account_id,
            input.table_name.as_deref(),
            limit,
            input.exclusive_start_stream_arn.as_deref(),
        )
        .await
        .map_err(storage_to_dynamo)?;

    let output = ListStreamsOutput {
        streams,
        last_evaluated_stream_arn: last_arn,
    };
    serialize_output(&output)
}

/// Handle `GetShardIterator`.
///
/// Encodes the shard ID and starting position into a base64 iterator token.
///
/// # Errors
///
/// Returns [`DynamoDbError::ResourceNotFoundException`] if the stream/shard does not exist.
/// Returns [`DynamoDbError::ValidationException`] on invalid input.
pub async fn handle_get_shard_iterator(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: GetShardIteratorInput = serde_json::from_value(body)
        .map_err(|e| DynamoDbError::SerializationException(e.to_string()))?;

    // Validate the stream and shard exist before issuing an iterator.
    ctx.storage
        .validate_shard(&ctx.account_id, &input.stream_arn, &input.shard_id)
        .await
        .map_err(storage_to_dynamo)?;

    let seq = match input.shard_iterator_type {
        ShardIteratorType::TrimHorizon => String::new(),
        ShardIteratorType::Latest => {
            // Resolve to the current max sequence number so only records
            // written after this point are returned by GetRecords.
            ctx.storage
                .latest_sequence_number(&input.shard_id)
                .await
                .map_err(storage_to_dynamo)?
                .unwrap_or_default()
        }
        ShardIteratorType::AtSequenceNumber => {
            // Convert to AFTER_SEQUENCE_NUMBER by subtracting 1, so the
            // exclusive "after" semantics produce inclusive "at" behavior.
            let raw = input.sequence_number.clone().ok_or_else(|| {
                DynamoDbError::ValidationException(
                    "SequenceNumber is required for AT_SEQUENCE_NUMBER iterator type".to_owned(),
                )
            })?;
            let n = raw.parse::<u128>().map_err(|_| {
                DynamoDbError::ValidationException("Invalid SequenceNumber".to_owned())
            })?;
            // n == 0: sequence 0 is the first possible record, so "at 0"
            // means "read from the beginning" — same as TRIM_HORIZON.
            if n > 0 {
                // Pad to the backend's stored width so lexicographic order
                // matches numeric order against stored sequence numbers.
                let width = ctx.storage.sequence_number_width();
                format!("{:0>width$}", n - 1)
            } else {
                String::new()
            }
        }
        ShardIteratorType::AfterSequenceNumber => {
            input.sequence_number.clone().ok_or_else(|| {
                DynamoDbError::ValidationException(
                    "SequenceNumber is required for AFTER_SEQUENCE_NUMBER iterator type".to_owned(),
                )
            })?
        }
    };

    // All iterator types are encoded as AFTER_SEQUENCE_NUMBER in the token.
    // TRIM_HORIZON has seq="" which means "read from beginning".
    // LATEST has seq=<current max> which means "read after current position".
    let type_str = "AFTER_SEQUENCE_NUMBER";

    // Encode creation timestamp (seconds since epoch) for 15-minute expiration.
    // unwrap_or_default: returns epoch 0 if system clock is before 1970 — safe
    // because the iterator would just expire immediately on the next GetRecords.
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let encoded = encode_shard_iterator(
        &input.shard_id,
        type_str,
        &seq,
        created_at,
        &input.stream_arn,
    );

    let output = GetShardIteratorOutput {
        shard_iterator: Some(encoded),
    };
    serialize_output(&output)
}

/// Shard iterator expiration: 15 minutes (900 seconds), matching real `DynamoDB`.
const SHARD_ITERATOR_EXPIRY_SECS: u64 = 900;

/// A decoded shard iterator token.
///
/// The token is base64 of `shard_id|type|seq|created_at|stream_arn`, joined by
/// `|` (no field contains `|`). `stream_arn` is `None` for a token issued by a
/// build before the stream ARN was carried in the token, and `created_at` is
/// `None` for a token issued before the expiry timestamp was added.
pub struct ShardIterator {
    pub shard_id: String,
    pub iter_type: String,
    pub sequence: String,
    pub created_at: Option<u64>,
    pub stream_arn: Option<String>,
}

/// Encode a shard iterator token that carries the stream ARN it reads.
#[must_use]
pub fn encode_shard_iterator(
    shard_id: &str,
    iter_type: &str,
    sequence: &str,
    created_at: u64,
    stream_arn: &str,
) -> String {
    let token = format!("{shard_id}|{iter_type}|{sequence}|{created_at}|{stream_arn}");
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, token)
}

/// Decode a shard iterator token. Returns `None` when the token is not valid
/// base64 UTF-8 or carries fewer than the two required fields, so callers fail
/// closed on a token that does not decode.
#[must_use]
pub fn decode_shard_iterator(encoded: &str) -> Option<ShardIterator> {
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).ok()?;
    let token = String::from_utf8(bytes).ok()?;
    let parts: Vec<&str> = token.splitn(5, '|').collect();
    if parts.len() < 2 {
        return None;
    }
    Some(ShardIterator {
        shard_id: parts[0].to_owned(),
        iter_type: parts[1].to_owned(),
        sequence: parts.get(2).copied().unwrap_or("").to_owned(),
        created_at: parts.get(3).and_then(|s| s.parse::<u64>().ok()),
        stream_arn: parts
            .get(4)
            .map(|s| (*s).to_owned())
            .filter(|s| !s.is_empty()),
    })
}

/// Handle `GetRecords`.
///
/// Decodes the shard iterator, checks expiration, reads records, and returns
/// a new iterator.
///
/// # Errors
///
/// Returns [`DynamoDbError::ExpiredIteratorException`] if the iterator is older
/// than 15 minutes.
/// Returns [`DynamoDbError::ValidationException`] on invalid iterator.
pub async fn handle_get_records(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    let input: GetRecordsInput = serde_json::from_value(body)
        .map_err(|e| DynamoDbError::SerializationException(e.to_string()))?;

    let it = decode_shard_iterator(&input.shard_iterator)
        .ok_or_else(|| DynamoDbError::ValidationException("Invalid ShardIterator".to_owned()))?;

    // Check iterator expiration (15 minutes).
    if let Some(created_at) = it.created_at {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now.saturating_sub(created_at) > SHARD_ITERATOR_EXPIRY_SECS {
            return Err(DynamoDbError::ExpiredIteratorException(
                "The shard iterator has expired and can no longer be \
                     used to retrieve stream records. A new shard iterator \
                     must be obtained by calling GetShardIterator."
                    .to_owned(),
            ));
        }
    }

    // Confirm the shard belongs to the stream named in the token before
    // reading. The request is authorized against that stream ARN, so this
    // stops a forged token that pairs an authorized stream ARN with another
    // stream's shard id from reading the other stream's records.
    if let Some(ref stream_arn) = it.stream_arn {
        ctx.storage
            .validate_shard(&ctx.account_id, stream_arn, &it.shard_id)
            .await
            .map_err(storage_to_dynamo)?;
    }

    let limit = input.limit.unwrap_or(1000).min(1000);

    // All iterator types are resolved to AFTER_SEQUENCE_NUMBER at
    // GetShardIterator time. Empty seq means "read from beginning".
    let after_sequence: Option<String> = if it.sequence.is_empty() {
        None
    } else {
        Some(it.sequence.clone())
    };

    let (records, last_seq) = ctx
        .storage
        .get_stream_records(
            &ctx.account_id,
            &it.shard_id,
            after_sequence.as_deref(),
            limit,
        )
        .await
        .map_err(storage_to_dynamo)?;

    // Build next iterator, carrying the same stream ARN and a fresh timestamp
    // so the 15-minute window resets.
    let next_iterator = {
        let next_seq = last_seq.unwrap_or_else(|| it.sequence.clone());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Some(encode_shard_iterator(
            &it.shard_id,
            "AFTER_SEQUENCE_NUMBER",
            &next_seq,
            now,
            it.stream_arn.as_deref().unwrap_or(""),
        ))
    };

    let output = GetRecordsOutput {
        records,
        next_shard_iterator: next_iterator,
    };
    serialize_output(&output)
}

fn storage_to_dynamo(e: StorageError) -> DynamoDbError {
    match e {
        StorageError::Validation(msg) => DynamoDbError::ValidationException(msg),
        StorageError::TableNotFound(name) => DynamoDbError::ResourceNotFoundException(name),
        other => {
            tracing::error!(internal_error = %other, "storage internal error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    /// AT_SEQUENCE_NUMBER converts to AFTER by subtracting 1 and padding to
    /// the backend's stored width. Verify that an unpadded client input is
    /// normalised correctly for both the 21-digit (postgres/sqlite/mongodb)
    /// and 23-digit (cassandra) cases, and that the edge case n=0 maps to
    /// TRIM_HORIZON (empty string).
    #[test]
    fn at_sequence_number_padding() {
        let cases: &[(&str, usize, &str)] = &[
            ("5", 21, "000000000000000000004"),
            ("000000000000000000005", 21, "000000000000000000004"),
            ("5", 23, "00000000000000000000004"),
            ("00000000000000000000005", 23, "00000000000000000000004"),
            ("0", 21, ""),
            ("1", 21, "000000000000000000000"),
        ];
        for (input, width, expected) in cases {
            let n: u128 = input.parse().unwrap();
            let result = if n > 0 {
                format!("{:0>width$}", n - 1)
            } else {
                String::new()
            };
            assert_eq!(&result, expected, "input={input} width={width}");
        }
    }

    #[test]
    fn shard_iterator_round_trip_backward_compat_and_fail_closed() {
        let stream = "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188";
        let enc =
            super::encode_shard_iterator("shardId-1", "AFTER_SEQUENCE_NUMBER", "42", 1000, stream);
        let d = super::decode_shard_iterator(&enc).expect("decode");
        assert_eq!(d.shard_id, "shardId-1");
        assert_eq!(d.sequence, "42");
        assert_eq!(d.created_at, Some(1000));
        assert_eq!(d.stream_arn.as_deref(), Some(stream));

        // A token from an older build (four fields, no stream ARN) still
        // decodes, with stream_arn None, so the authorization layer can fail
        // closed on it.
        let old = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "shardId-2|AFTER_SEQUENCE_NUMBER|7|1000",
        );
        let d2 = super::decode_shard_iterator(&old).expect("decode old");
        assert_eq!(d2.shard_id, "shardId-2");
        assert_eq!(d2.created_at, Some(1000));
        assert_eq!(d2.stream_arn, None);

        // Garbage does not decode.
        assert!(super::decode_shard_iterator("!!!not base64!!!").is_none());
    }
}
