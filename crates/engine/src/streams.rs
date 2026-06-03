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
    let type_str = "AFTER_SEQUENCE_NUMBER";

    // Encode creation timestamp (seconds since epoch) for 15-minute expiration.
    // unwrap_or_default: returns epoch 0 if system clock is before 1970 — safe
    // because the iterator would just expire immediately on the next GetRecords.
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let token = format!("{}|{}|{}|{}", input.shard_id, type_str, seq, created_at);
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, token);

    let output = GetShardIteratorOutput {
        shard_iterator: Some(encoded),
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

    let decoded = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &input.shard_iterator,
    )
    .map_err(|_| DynamoDbError::ValidationException("Invalid shard iterator".to_owned()))?;
    let token = String::from_utf8(decoded).map_err(|_| {
        DynamoDbError::ValidationException("Invalid shard iterator encoding".to_owned())
    })?;

    let parts: Vec<&str> = token.splitn(4, '|').collect();
    if parts.len() < 2 {
        return Err(DynamoDbError::ValidationException(
            "Invalid shard iterator format".to_owned(),
        ));
    }

    let shard_id = parts[0];
    // The type field is parsed but unused — all iterators are now normalized to
    // AFTER_SEQUENCE_NUMBER at GetShardIterator time. We keep the field in the
    // token format for backward compatibility with any iterators created before
    // this normalization was introduced.
    let _iter_type = parts[1];
    let seq = if parts.len() >= 3 { parts[2] } else { "" };

    // Check iterator expiration (15 minutes).
    if parts.len() >= 4 {
        if let Ok(created_at) = parts[3].parse::<u64>() {
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
    }

    let limit = stream_limit_or_default(input.limit, 1000, 1000)?;

    // All iterator types are now resolved to AFTER_SEQUENCE_NUMBER at
    // GetShardIterator time. Empty seq means "read from beginning".
    let after_sequence: Option<String> = if seq.is_empty() {
        None
    } else {
        Some(seq.to_owned())
    };

    let (records, last_seq) = ctx
        .storage
        .get_stream_records(shard_id, after_sequence.as_deref(), limit)
        .await
        .map_err(storage_to_dynamo)?;

    // Build next iterator — points to after the last record read.
    // Carries a fresh creation timestamp so the 15-minute window resets.
    let next_iterator = {
        let next_seq = last_seq.unwrap_or_else(|| seq.to_owned());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let next_token = format!("{shard_id}|AFTER_SEQUENCE_NUMBER|{next_seq}|{now}");
        Some(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            next_token,
        ))
    };

    let output = GetRecordsOutput {
        records,
        next_shard_iterator: next_iterator,
    };
    serialize_output(&output)
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
        other => {
            tracing::error!(internal_error = %other, "storage internal error");
            DynamoDbError::InternalServerError("Internal server error".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{previous_decimal_sequence, stream_limit_or_default};

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
}
