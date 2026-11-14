// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the streams request handlers: iterator tokens, the measured
//! constraint sentences and the not-found answers.

/// `DescribeStream` renders `CreationRequestDateTime` as the label's
/// instant in epoch seconds, next to the members it already had, and
/// omits the member only when the label is not a timestamp.
#[test]
fn describe_stream_output_carries_creation_request_date_time() {
    use extenddb_core::types::{
        DescribeStreamOutput, StreamDescription, StreamStatus, StreamViewType,
    };
    let desc = StreamDescription {
        creation_request_date_time: None,
        stream_arn: "arn:aws:dynamodb:us-east-1:1:table/T/stream/2026-10-24T11:31:45.748"
            .to_owned(),
        stream_label: "2026-10-24T11:31:45.748".to_owned(),
        stream_status: StreamStatus::Disabled,
        stream_view_type: StreamViewType::KeysOnly,
        table_name: "T".to_owned(),
        key_schema: Vec::new(),
        shards: Vec::new(),
        last_evaluated_shard_id: None,
    };
    let v = crate::serialize_output(&DescribeStreamOutput {
        stream_description: super::with_creation_request_date_time(desc.clone()),
    })
    .unwrap();
    let sd = &v["StreamDescription"];
    assert_eq!(sd["CreationRequestDateTime"], 1_792_841_505.748);
    assert_eq!(sd["StreamLabel"], "2026-10-24T11:31:45.748");
    assert_eq!(sd["StreamStatus"], "DISABLED");

    let v = crate::serialize_output(&DescribeStreamOutput {
        stream_description: super::with_creation_request_date_time(StreamDescription {
            stream_label: "not-a-label".to_owned(),
            ..desc
        }),
    })
    .unwrap();
    assert!(
        v["StreamDescription"]
            .get("CreationRequestDateTime")
            .is_none()
    );
}

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

#[test]
fn the_constraint_sentences_match_the_measured_ones() {
    assert_eq!(
        super::constraint("shardId", "have length greater than or equal to 28"),
        super::SHARD_ID_TOO_SHORT
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
