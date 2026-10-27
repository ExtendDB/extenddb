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

/// The model's constraint sentence for a member value, as the service renders
/// it for the streams API (no value quoted; measured 2026-10-24 on `shardId`,
/// `exclusiveStartShardId`, `sequenceNumber`, `limit` and
/// `exclusiveStartStreamArn`).
fn constraint(member: &str, rule: &str) -> String {
    format!(
        "1 validation error detected: Value at '{member}' failed to satisfy constraint: Member must {rule}"
    )
}

/// The message `DynamoDB` returns for an expired shard iterator (measured live,
/// `live-r2/iter-live.json`: an iterator 990 seconds old returns
/// `ExpiredIteratorException` with exactly this text). Shared so the
/// authorization layer answers a pre-upgrade iterator with the same text the
/// engine uses when a current iterator ages out, so both match the service.
pub const SHARD_ITERATOR_EXPIRED_MESSAGE: &str = "Shard iterator has expired";

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

/// How `GetRecords` must answer a shard iterator, decided before authorization.
#[derive(Debug)]
pub enum ShardIteratorClass {
    /// A current-format token carrying the stream ARN it reads.
    Stream(String),
    /// A token issued by a build before the stream ARN was carried in the
    /// token: it decodes to exactly the four pre-upgrade fields with a
    /// parseable timestamp and no stream ARN. Answered with
    /// `ExpiredIteratorException` so a stream client renews it, rather than the
    /// non-retryable "Invalid ShardIterator" that would break every consumer
    /// reading across the upgrade.
    Expired,
    /// A token that does not decode, or decodes to neither of the above.
    Invalid,
}

/// Classify a `GetRecords` shard iterator for the authorization layer, before
/// the resource is authorized.
///
/// A current token carries its stream ARN. A token with no stream ARN but the
/// four pre-upgrade fields and a parseable timestamp is an old-format iterator
/// ([`ShardIteratorClass::Expired`]). Anything that does not decode, or decodes
/// to fewer fields or an unparseable timestamp, is
/// [`ShardIteratorClass::Invalid`].
#[must_use]
pub fn classify_shard_iterator(token: &str) -> ShardIteratorClass {
    match decode_shard_iterator(token) {
        None => ShardIteratorClass::Invalid,
        Some(it) => match it.stream_arn {
            Some(arn) => ShardIteratorClass::Stream(arn),
            // No stream ARN: an old-format token carries the four pre-upgrade
            // fields with a parseable timestamp (created_at present); a shorter
            // or unparseable token stays Invalid.
            None if it.created_at.is_some() => ShardIteratorClass::Expired,
            None => ShardIteratorClass::Invalid,
        },
    }
}

/// Handle `GetRecords`.
///
/// Decodes the shard iterator, checks expiration, reads records from the
/// shard as one of the shards of the stream the iterator names, and returns
/// a new iterator.
///
/// # Errors
///
/// Returns [`DynamoDbError::ExpiredIteratorException`] if the iterator is older
/// than 15 minutes, or carries a timestamp and no stream ARN.
/// Returns [`DynamoDbError::ResourceNotFoundException`] if the stream does not
/// exist for the caller's account or the shard is not one of its shards.
/// Returns [`DynamoDbError::ValidationException`] on an invalid iterator,
/// which includes one with neither a stream ARN nor a timestamp.
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
                SHARD_ITERATOR_EXPIRED_MESSAGE.to_owned(),
            ));
        }
    }

    // The request was authorized against the stream ARN carried in the token.
    // The storage read below resolves the shard against that stream (table
    // name and stream label, within the caller's account) as part of the read:
    // SQLite in the statement that reads the records, PostgreSQL, MongoDB and
    // Cassandra after reading the table. A forged token that pairs an
    // authorized stream ARN with another stream's shard id reads nothing and
    // gets the "stream does not exist" answer. A token with no stream ARN gets
    // the answer the authorization layer gives it (`classify_shard_iterator`);
    // the layer is skipped in developer mode, so the engine answers the same.
    let Some(stream_arn) = it.stream_arn.as_deref() else {
        return Err(no_stream_arn_error(&it));
    };

    let limit = get_records_limit(input.limit)?;

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
            stream_arn,
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
            stream_arn,
        ))
    };

    let output = GetRecordsOutput {
        records,
        next_shard_iterator: next_iterator,
    };
    serialize_output(&output)
}

/// The answer for a token that carries no stream ARN, the same one
/// [`classify_shard_iterator`] gives the authorization layer. A token with a
/// timestamp comes from a build before the ARN was carried and is expired; one
/// without a timestamp is not a token this server issued and is invalid.
fn no_stream_arn_error(it: &ShardIterator) -> DynamoDbError {
    if it.created_at.is_some() {
        DynamoDbError::ExpiredIteratorException(SHARD_ITERATOR_EXPIRED_MESSAGE.to_owned())
    } else {
        DynamoDbError::ValidationException("Invalid ShardIterator".to_owned())
    }
}

/// `GetRecords` with a `Limit` over 1000 (measured 2026-10-25 with 1001).
pub const GET_RECORDS_LIMIT_TOO_LARGE: &str =
    "One or more parameter values were invalid: Limit should be less than or equal to 1000";

/// The largest `Limit` `GetRecords` accepts.
const GET_RECORDS_MAX_LIMIT: i64 = 1000;

/// The page size a `GetRecords` call asked for, or the service's refusal.
///
/// Measured 2026-10-25: `Limit` 0 and -1 get the model's constraint sentence,
/// 1001 the parameter sentence; 1 and 1000 are accepted. The bounds are
/// checked before storage runs, so the records read never carries a limit
/// under 1, which the backends read as a shard that is not the stream's (no
/// row) or refuse outright.
fn get_records_limit(limit: Option<i64>) -> Result<i64, DynamoDbError> {
    match limit {
        Some(l) if l < 1 => Err(DynamoDbError::ValidationException(constraint(
            "limit",
            "have value greater than or equal to 1",
        ))),
        Some(l) if l > GET_RECORDS_MAX_LIMIT => Err(DynamoDbError::ValidationException(
            GET_RECORDS_LIMIT_TOO_LARGE.to_owned(),
        )),
        Some(l) => Ok(l),
        None => Ok(GET_RECORDS_MAX_LIMIT),
    }
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

    #[test]
    fn stream_not_found_maps_to_resource_not_found_with_the_arn() {
        // `describe_stream` answers a stream ARN that names no stream with the
        // storage not-found error that carries the ARN.
        let arn = "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188";
        match super::storage_to_dynamo(extenddb_storage::util::stream_not_found(arn)) {
            super::DynamoDbError::ResourceNotFoundException(msg) => assert_eq!(
                msg,
                format!("Requested resource not found: Stream: {arn} not found")
            ),
            other => panic!("expected ResourceNotFoundException, got {other:?}"),
        }
    }

    #[test]
    fn stream_does_not_exist_maps_to_resource_not_found() {
        // The answer for a stream ARN that names no stream, and for a shard
        // that is not one of the stream's (`get_stream_records`).
        match super::storage_to_dynamo(extenddb_storage::util::stream_does_not_exist()) {
            super::DynamoDbError::ResourceNotFoundException(msg) => {
                assert_eq!(msg, "Requested resource not found: Stream does not exist");
            }
            other => panic!("expected ResourceNotFoundException, got {other:?}"),
        }
    }

    #[test]
    fn get_records_limit_bounds_carry_the_measured_sentences() {
        let refused = |l: i64| match super::get_records_limit(Some(l)) {
            Err(super::DynamoDbError::ValidationException(msg)) => msg,
            other => panic!("expected ValidationException for Limit {l}, got {other:?}"),
        };
        for l in [0, -1] {
            assert_eq!(
                refused(l),
                "1 validation error detected: Value at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
            );
        }
        assert_eq!(
            refused(1001),
            "One or more parameter values were invalid: Limit should be less than or equal to 1000"
        );
        assert_eq!(super::get_records_limit(Some(1)).unwrap(), 1);
        assert_eq!(super::get_records_limit(Some(1000)).unwrap(), 1000);
        assert_eq!(super::get_records_limit(None).unwrap(), 1000);
    }

    fn b64(s: &str) -> String {
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, s)
    }

    #[test]
    fn classify_current_token_is_stream() {
        let stream = "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-20T16:17:40.188";
        let enc =
            super::encode_shard_iterator("shardId-1", "AFTER_SEQUENCE_NUMBER", "42", 1000, stream);
        match super::classify_shard_iterator(&enc) {
            super::ShardIteratorClass::Stream(arn) => assert_eq!(arn, stream),
            other => panic!("expected Stream, got {other:?}"),
        }
    }

    #[test]
    fn classify_old_format_four_fields_is_expired() {
        // Four pre-upgrade fields with a parseable timestamp, no stream ARN.
        let old = b64("shardId-2|AFTER_SEQUENCE_NUMBER|7|1000");
        assert!(matches!(
            super::classify_shard_iterator(&old),
            super::ShardIteratorClass::Expired
        ));
    }

    #[test]
    fn classify_undecodable_or_short_or_unparseable_is_invalid() {
        // Not base64.
        assert!(matches!(
            super::classify_shard_iterator("!!!not base64!!!"),
            super::ShardIteratorClass::Invalid
        ));
        // Three fields: decodes but is not the old four-field shape.
        let three = b64("shardId-3|AFTER_SEQUENCE_NUMBER|7");
        assert!(matches!(
            super::classify_shard_iterator(&three),
            super::ShardIteratorClass::Invalid
        ));
        // Four fields but an unparseable timestamp.
        let bad_ts = b64("shardId-4|AFTER_SEQUENCE_NUMBER|7|notanumber");
        assert!(matches!(
            super::classify_shard_iterator(&bad_ts),
            super::ShardIteratorClass::Invalid
        ));
    }

    #[test]
    fn a_token_without_a_stream_arn_gets_the_answer_the_authorization_layer_gives() {
        // Two and three fields have no timestamp (invalid), four fields with a
        // timestamp come from before the ARN was carried (expired), and a
        // timestamp that is not a number is invalid.
        for token in [
            b64("shardId-5|AFTER_SEQUENCE_NUMBER"),
            b64("shardId-3|AFTER_SEQUENCE_NUMBER|7"),
            b64("shardId-2|AFTER_SEQUENCE_NUMBER|7|1000"),
            b64("shardId-4|AFTER_SEQUENCE_NUMBER|7|notanumber"),
        ] {
            let it = super::decode_shard_iterator(&token).expect("the token decodes");
            let expected = match super::classify_shard_iterator(&token) {
                super::ShardIteratorClass::Expired => {
                    super::DynamoDbError::ExpiredIteratorException(
                        super::SHARD_ITERATOR_EXPIRED_MESSAGE.to_owned(),
                    )
                }
                super::ShardIteratorClass::Invalid => {
                    super::DynamoDbError::ValidationException("Invalid ShardIterator".to_owned())
                }
                super::ShardIteratorClass::Stream(_) => panic!("the token carries no stream ARN"),
            };
            assert_eq!(
                format!("{:?}", super::no_stream_arn_error(&it)),
                format!("{expected:?}")
            );
        }
    }
}
