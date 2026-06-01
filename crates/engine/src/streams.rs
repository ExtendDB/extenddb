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
            let n = raw.parse::<u64>().map_err(|_| {
                DynamoDbError::ValidationException("Invalid SequenceNumber".to_owned())
            })?;
            // n == 0: sequence 0 is the first possible record, so "at 0"
            // means "read from the beginning" — same as TRIM_HORIZON.
            if n > 0 {
                format!("{:021}", n - 1)
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
    if parts.len() >= 4
        && let Ok(created_at) = parts[3].parse::<u64>()
    {
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

    let limit = input.limit.unwrap_or(1000).min(1000);

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
