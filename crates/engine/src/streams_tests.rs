// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the streams request handlers: iterator tokens, the measured
//! constraint sentences and the not-found answers.

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
fn the_shard_id_maximum_applies_once_the_shard_is_not_found() {
    // A shard of a table named with 41 characters, as a release before
    // v0.1.14 wrote it: 8 + 41 + 1 + 16 = 66 characters. Found, it is not
    // this function's case at all; not found, the model's sentence.
    let long = format!("shardId-{}-{:016}", "t".repeat(41), 3);
    assert_eq!(long.chars().count(), 66);
    let sentence = "1 validation error detected: Value at 'shardId' failed to satisfy constraint: Member must have length less than or equal to 65";
    for err in [
        extenddb_storage::util::shard_does_not_exist(),
        extenddb_storage::util::stream_does_not_exist(),
    ] {
        match super::shard_lookup_error(long.chars().count(), err) {
            super::DynamoDbError::ValidationException(msg) => assert_eq!(msg, sentence),
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }
    // Within the bound, the lookup's own answer stands.
    match super::shard_lookup_error(65, extenddb_storage::util::shard_does_not_exist()) {
        super::DynamoDbError::ResourceNotFoundException(msg) => {
            assert_eq!(msg, extenddb_storage::util::SHARD_DOES_NOT_EXIST);
        }
        other => panic!("expected ResourceNotFoundException, got {other:?}"),
    }
}

/// An absent, null or empty `StreamArn` is `Invalid StreamArn` before any
/// clause, and every constraint violation of a body joins one message, in the
/// service's order (measured 2026-11-20 and 2026-11-23): sequenceNumber,
/// shardIteratorType, shardId (null before length).
#[test]
fn constraint_violations_join_one_message_in_the_service_order() {
    use serde_json::json;
    const SEQ: &str = "Value at 'sequenceNumber' failed to satisfy constraint: Member must have length greater than or equal to 21";
    const NULL: &str =
        "Value at 'shardIteratorType' failed to satisfy constraint: Member must not be null";
    const SHARD: &str = "Value at 'shardId' failed to satisfy constraint: Member must have length greater than or equal to 28";
    const SHARD_NULL: &str =
        "Value at 'shardId' failed to satisfy constraint: Member must not be null";
    const INVALID_ARN: &str = "Invalid StreamArn";
    let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/T/stream/2026-10-23T14:03:56.871";
    let long = "shardId-00000001792764241200-b719fef1";
    let cases: &[(serde_json::Value, Option<String>)] = &[
        (
            json!({"StreamArn": arn, "ShardId": "shardId-short"}),
            Some(format!("2 validation errors detected: {NULL}; {SHARD}")),
        ),
        (
            json!({"StreamArn": arn, "ShardId": "shardId-short", "ShardIteratorType": null}),
            Some(format!("2 validation errors detected: {NULL}; {SHARD}")),
        ),
        (
            json!({"StreamArn": arn, "ShardId": "shardId-short", "ShardIteratorType": "BOGUS"}),
            Some(format!(
                "2 validation errors detected: {}; {SHARD}",
                super::iterator_params::SHARD_ITERATOR_TYPE_ENUM_CLAUSE
            )),
        ),
        (
            json!({"StreamArn": arn, "ShardId": long, "SequenceNumber": "0"}),
            Some(format!("2 validation errors detected: {SEQ}; {NULL}")),
        ),
        (
            json!({"StreamArn": arn, "ShardId": "shardId-short", "SequenceNumber": "0"}),
            Some(format!(
                "3 validation errors detected: {SEQ}; {NULL}; {SHARD}"
            )),
        ),
        (
            json!({"StreamArn": arn, "ShardId": long, "Timestamp": 1792764248.0}),
            Some(format!("1 validation error detected: {NULL}")),
        ),
        (
            json!({"StreamArn": arn, "ShardId": long, "ShardIteratorType": "at_timestamp"}),
            Some(format!(
                "1 validation error detected: {}",
                super::iterator_params::SHARD_ITERATOR_TYPE_ENUM_CLAUSE
            )),
        ),
        (
            json!({"StreamArn": arn, "ShardId": "shardId-short", "ShardIteratorType": "TRIM_HORIZON"}),
            Some(format!("1 validation error detected: {SHARD}")),
        ),
        // A missing or null ShardId is its null clause, in the shardId position.
        (
            json!({"StreamArn": arn, "ShardIteratorType": "TRIM_HORIZON"}),
            Some(format!("1 validation error detected: {SHARD_NULL}")),
        ),
        (
            json!({"StreamArn": arn, "ShardId": null, "ShardIteratorType": "TRIM_HORIZON"}),
            Some(format!("1 validation error detected: {SHARD_NULL}")),
        ),
        (
            json!({"StreamArn": arn, "SequenceNumber": "0"}),
            Some(format!(
                "3 validation errors detected: {SEQ}; {NULL}; {SHARD_NULL}"
            )),
        ),
        // An absent, null or empty StreamArn is refused alone, whatever else
        // the body would violate.
        (
            json!({"ShardId": long, "ShardIteratorType": "TRIM_HORIZON"}),
            Some(INVALID_ARN.to_owned()),
        ),
        (
            json!({"ShardId": "shardId-short"}),
            Some(INVALID_ARN.to_owned()),
        ),
        (
            json!({"StreamArn": null, "ShardId": long, "ShardIteratorType": "TRIM_HORIZON"}),
            Some(INVALID_ARN.to_owned()),
        ),
        (
            json!({"StreamArn": "", "ShardId": long, "ShardIteratorType": "TRIM_HORIZON"}),
            Some(INVALID_ARN.to_owned()),
        ),
        (
            json!({"StreamArn": null, "SequenceNumber": "0"}),
            Some(INVALID_ARN.to_owned()),
        ),
        (
            json!({"StreamArn": "", "ShardId": "shardId-short"}),
            Some(INVALID_ARN.to_owned()),
        ),
        (
            json!({"StreamArn": arn, "ShardId": long, "ShardIteratorType": "AT_TIMESTAMP", "SequenceNumber": "000000000000000000001"}),
            None,
        ),
        // A ShardIteratorType of another JSON type is the deserializer's case.
        (
            json!({"StreamArn": arn, "ShardId": long, "ShardIteratorType": 5}),
            None,
        ),
    ];
    for (body, expected) in cases {
        match (super::constraint_violations(body), expected) {
            (Ok(()), None) => {}
            (Err(super::DynamoDbError::ValidationException(m)), Some(e)) => {
                assert_eq!(&m, e, "body={body}");
            }
            (got, _) => panic!("body={body}: expected {expected:?}, got {got:?}"),
        }
    }
}

#[test]
fn the_constraint_sentences_match_the_measured_ones() {
    // `GetShardIterator` with a `ShardId` under 28 characters (measured
    // 2026-10-24 with `nope`).
    assert_eq!(
        super::constraint("shardId", "have length greater than or equal to 28"),
        "1 validation error detected: Value at 'shardId' failed to satisfy constraint: Member must have length greater than or equal to 28"
    );
    assert_eq!(
        super::constraint("limit", "have value greater than or equal to 1"),
        "1 validation error detected: Value at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1"
    );
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
            super::ShardIteratorClass::Expired => super::DynamoDbError::ExpiredIteratorException(
                super::SHARD_ITERATOR_EXPIRED_MESSAGE.to_owned(),
            ),
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

// A backend-classified error reaches the client unchanged through the
// streams mapper, keeping both its variant and its message.
mod storage_error_mapping {
    use super::super::storage_to_dynamo;
    use extenddb_core::error::DynamoDbError;
    use extenddb_storage::error::StorageError;

    #[test]
    fn service_internal_server_error_passes_through() {
        let err = storage_to_dynamo(StorageError::Service(DynamoDbError::InternalServerError(
            "backend internal".to_owned(),
        )));
        match err {
            DynamoDbError::InternalServerError(msg) => assert_eq!(msg, "backend internal"),
            other => panic!("expected InternalServerError, got {other:?}"),
        }
    }

    #[test]
    fn service_service_unavailable_passes_through() {
        let err = storage_to_dynamo(StorageError::Service(DynamoDbError::ServiceUnavailable(
            "retry shortly".to_owned(),
        )));
        match err {
            DynamoDbError::ServiceUnavailable(msg) => assert_eq!(msg, "retry shortly"),
            other => panic!("expected ServiceUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn service_expired_iterator_passes_through() {
        let err = storage_to_dynamo(StorageError::Service(
            DynamoDbError::ExpiredIteratorException("Iterator expired.".to_owned()),
        ));
        match err {
            DynamoDbError::ExpiredIteratorException(msg) => assert_eq!(msg, "Iterator expired."),
            other => panic!("expected ExpiredIteratorException, got {other:?}"),
        }
    }

    // Control: an unclassified driver error is still redacted behind the fixed
    // string, with the driver text absent from the client message.
    #[test]
    fn internal_driver_text_is_redacted() {
        let err = storage_to_dynamo(StorageError::Internal("driver text".to_owned()));
        match err {
            DynamoDbError::InternalServerError(msg) => assert_eq!(msg, "Internal server error"),
            other => panic!("expected InternalServerError, got {other:?}"),
        }
    }
}
