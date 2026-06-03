// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB Streams operation handlers.

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

struct ShardIteratorToken {
    shard_id: String,
    sequence: String,
    created_at: u64,
    stream_arn: String,
}

fn stream_limit_or_default(
    limit: Option<i64>,
    default: i64,
    max: i64,
) -> Result<i64, DynamoDbError> {
    let raw_limit = limit.unwrap_or(default);
    if raw_limit < 1 {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{raw_limit}' at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
        )));
    }
    Ok(raw_limit.min(max))
}

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
    let mut input: DescribeStreamInput = serde_json::from_value(body)
        .map_err(|e| DynamoDbError::SerializationException(e.to_string()))?;
    input.limit = Some(stream_limit_or_default(input.limit, 100, 100)?);

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

    let limit = stream_limit_or_default(input.limit, 100, 100)?;
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
            previous_decimal_sequence(&raw)?
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
    // Encode creation timestamp (seconds since epoch) for 15-minute expiration.
    // unwrap_or_default: returns epoch 0 if system clock is before 1970 — safe
    // because the iterator would just expire immediately on the next GetRecords.
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let output = GetShardIteratorOutput {
        shard_iterator: Some(encode_shard_iterator(
            &input.shard_id,
            &seq,
            created_at,
            &input.stream_arn,
        )),
    };
    serialize_output(&output)
}

/// Shard iterator expiration: 15 minutes (900 seconds), matching real DynamoDB.
const SHARD_ITERATOR_EXPIRY_SECS: u64 = 900;

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

    let token = decode_shard_iterator(&input.shard_iterator)?;

    // Check iterator expiration (15 minutes).
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if now.saturating_sub(token.created_at) > SHARD_ITERATOR_EXPIRY_SECS {
        return Err(DynamoDbError::ExpiredIteratorException(
            "The shard iterator has expired and can no longer be \
             used to retrieve stream records. A new shard iterator \
             must be obtained by calling GetShardIterator."
                .to_owned(),
        ));
    }

    // The iterator token is opaque to clients but not self-authorizing. Rebind
    // it to the authenticated account and original stream before trusting the
    // shard id for a records read.
    ctx.storage
        .validate_shard(&ctx.account_id, &token.stream_arn, &token.shard_id)
        .await
        .map_err(storage_to_dynamo)?;

    let limit = stream_limit_or_default(input.limit, 1000, 1000)?;

    // All iterator types are now resolved to AFTER_SEQUENCE_NUMBER at
    // GetShardIterator time. Empty seq means "read from beginning".
    let after_sequence: Option<String> = if token.sequence.is_empty() {
        None
    } else {
        Some(token.sequence.clone())
    };

    let (records, last_seq) = ctx
        .storage
        .get_stream_records(&token.shard_id, after_sequence.as_deref(), limit)
        .await
        .map_err(storage_to_dynamo)?;

    // Build next iterator — points to after the last record read.
    // Carries a fresh creation timestamp so the 15-minute window resets.
    let next_iterator = {
        let next_seq = last_seq.unwrap_or(token.sequence);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Some(encode_shard_iterator(
            &token.shard_id,
            &next_seq,
            now,
            &token.stream_arn,
        ))
    };

    let output = GetRecordsOutput {
        records,
        next_shard_iterator: next_iterator,
    };
    serialize_output(&output)
}

fn encode_shard_iterator(
    shard_id: &str,
    sequence: &str,
    created_at: u64,
    stream_arn: &str,
) -> String {
    let token = format!("{shard_id}|AFTER_SEQUENCE_NUMBER|{sequence}|{created_at}|{stream_arn}");
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, token)
}

fn decode_shard_iterator(encoded: &str) -> Result<ShardIteratorToken, DynamoDbError> {
    let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
        .map_err(|_| DynamoDbError::ValidationException("Invalid shard iterator".to_owned()))?;
    let token = String::from_utf8(decoded).map_err(|_| {
        DynamoDbError::ValidationException("Invalid shard iterator encoding".to_owned())
    })?;

    let parts: Vec<&str> = token.splitn(5, '|').collect();
    if parts.len() != 5 || parts[0].is_empty() || parts[1] != "AFTER_SEQUENCE_NUMBER" {
        return Err(DynamoDbError::ValidationException(
            "Invalid shard iterator format".to_owned(),
        ));
    }
    let created_at = parts[3].parse::<u64>().map_err(|_| {
        DynamoDbError::ValidationException("Invalid shard iterator format".to_owned())
    })?;
    if parts[4].is_empty() {
        return Err(DynamoDbError::ValidationException(
            "Invalid shard iterator format".to_owned(),
        ));
    }

    Ok(ShardIteratorToken {
        shard_id: parts[0].to_owned(),
        sequence: parts[2].to_owned(),
        created_at,
        stream_arn: parts[4].to_owned(),
    })
}

fn previous_decimal_sequence(raw: &str) -> Result<String, DynamoDbError> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(DynamoDbError::ValidationException(
            "Invalid SequenceNumber".to_owned(),
        ));
    }

    let Some(last_non_zero) = raw.as_bytes().iter().rposition(|b| *b != b'0') else {
        // Sequence zero is the first possible position, so "at 0" means
        // "read from the beginning", the same as TRIM_HORIZON.
        return Ok(String::new());
    };

    let mut previous = raw.as_bytes().to_vec();
    previous[last_non_zero] -= 1;
    for digit in previous.iter_mut().skip(last_non_zero + 1) {
        *digit = b'9';
    }

    String::from_utf8(previous)
        .map_err(|_| DynamoDbError::ValidationException("Invalid SequenceNumber".to_owned()))
}

fn storage_to_dynamo(e: StorageError) -> DynamoDbError {
    match e {
        StorageError::Validation(msg) => DynamoDbError::ValidationException(msg),
        StorageError::TableNotFound(name) => DynamoDbError::ResourceNotFoundException(name),
        other => crate::storage_other_to_dynamo(other, "streams storage error"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_shard_iterator, encode_shard_iterator, previous_decimal_sequence,
        stream_limit_or_default,
    };

    #[test]
    fn stream_limit_defaults_and_caps_large_values() {
        assert_eq!(
            stream_limit_or_default(None, 100, 100).expect("default"),
            100
        );
        assert_eq!(
            stream_limit_or_default(Some(7), 100, 100).expect("limit"),
            7
        );
        assert_eq!(
            stream_limit_or_default(Some(1_000), 100, 100).expect("capped"),
            100
        );
    }

    #[test]
    fn stream_limit_rejects_non_positive_values_before_storage() {
        for limit in [0, -1] {
            let err = stream_limit_or_default(Some(limit), 100, 100)
                .expect_err("non-positive stream limits must fail");
            assert!(err.to_string().contains("greater than or equal to 1"));
        }
    }

    #[test]
    fn sequence_predecessor_preserves_width_for_tidb_tso_ordinals() {
        assert_eq!(
            previous_decimal_sequence("000000000000000000042000000").expect("previous"),
            "000000000000000000041999999"
        );
        assert_eq!(
            previous_decimal_sequence("000000000000000000042000001").expect("previous"),
            "000000000000000000042000000"
        );
    }

    #[test]
    fn sequence_predecessor_handles_zero_as_trim_horizon() {
        assert_eq!(previous_decimal_sequence("0").expect("zero"), "");
        assert_eq!(previous_decimal_sequence("000000").expect("zero"), "");
    }

    #[test]
    fn sequence_predecessor_rejects_non_decimal_input() {
        assert!(previous_decimal_sequence("").is_err());
        assert!(previous_decimal_sequence("123abc").is_err());
    }

    #[test]
    fn shard_iterator_round_trips_stream_binding() {
        let stream_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/t/stream/label";
        let encoded =
            encode_shard_iterator("shardId-000000000001-label-table", "42", 123, stream_arn);
        let decoded = decode_shard_iterator(&encoded).expect("valid iterator");

        assert_eq!(decoded.shard_id, "shardId-000000000001-label-table");
        assert_eq!(decoded.sequence, "42");
        assert_eq!(decoded.created_at, 123);
        assert_eq!(decoded.stream_arn, stream_arn);
    }

    #[test]
    fn shard_iterator_rejects_legacy_unbound_tokens() {
        let legacy = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "shardId-000000000001-label-table|AFTER_SEQUENCE_NUMBER|42|123",
        );

        assert!(decode_shard_iterator(&legacy).is_err());
    }
}
