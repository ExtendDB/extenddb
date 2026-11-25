// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `GetShardIterator` parameter rules and position conversions.
//!
//! Messages and the order of the checks were measured on the service
//! 2026-10-23 and 2026-11-20 (us-east-1); see `tests/test_streams_at_timestamp.py`
//! for the wire-level expectations taken from the same runs.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{GetShardIteratorInput, ShardIteratorType};

/// The `ShardIteratorType` members, in the order the service lists them in
/// [`SHARD_ITERATOR_TYPE_ENUM_CLAUSE`].
pub(crate) const SHARD_ITERATOR_TYPES: &[&str] = &[
    "AFTER_SEQUENCE_NUMBER",
    "LATEST",
    "AT_TIMESTAMP",
    "AT_SEQUENCE_NUMBER",
    "TRIM_HORIZON",
];

/// How the service renders a `ShardIteratorType` that is not a member: no
/// value, and the set in this order. Measured 2026-10-23 (us-east-1) for an
/// unknown name and for a lowercase one alike.
pub(crate) const SHARD_ITERATOR_TYPE_ENUM_CLAUSE: &str = "Value at 'shardIteratorType' failed to \
     satisfy constraint: Member must satisfy enum value set: [AFTER_SEQUENCE_NUMBER, LATEST, \
     AT_TIMESTAMP, AT_SEQUENCE_NUMBER, TRIM_HORIZON]";

/// The members whose JSON type the service checks as it reads the body, with
/// the type name its texts use, in the model's order. The service reports the
/// first member in body order that does not fit (measured 2026-11-20:
/// `"SequenceNumber": 5, "Timestamp": "x"` is the String text and the other
/// way round the Date text). The parsed body keeps no order, so the model
/// order stands in; it is the order the SDKs send.
const TYPED_MEMBERS: &[(&str, &str)] = &[("SequenceNumber", "String"), ("Timestamp", "Date")];

/// Refuse the first member of [`TYPED_MEMBERS`] whose JSON type does not fit,
/// as [`check_member_types`] says. The service runs this before every
/// constraint and parameter check, the `ShardIteratorType` ones and the
/// `StreamArn` presence (measured 2026-11-23) included.
pub(crate) fn check_member_shapes(body: &serde_json::Value) -> Result<(), DynamoDbError> {
    check_member_types(body, TYPED_MEMBERS)
}

/// Refuse the first of `members`, each a member name and the type name of
/// the service's texts, whose JSON type in `body` does not fit, with the text
/// the service's deserializer renders for it (measured 2026-10-23 and
/// 2026-11-20): `STRING_VALUE`, `NUMBER_VALUE`, `TRUE_VALUE` or `FALSE_VALUE`
/// `cannot be converted to <type>`, an object the structure message, an
/// array a `SerializationException` with no message. Null passes through to
/// the later checks, which treat it as absent.
pub(crate) fn check_member_types(
    body: &serde_json::Value,
    members: &[(&str, &str)],
) -> Result<(), DynamoDbError> {
    use serde_json::Value;
    for (member, kind) in members {
        let Some(v) = body.get(member) else {
            continue;
        };
        let msg = match (*kind, v) {
            (_, Value::Null) | ("String", Value::String(_)) | ("Date", Value::Number(_)) => {
                continue;
            }
            (_, Value::String(_)) => format!("STRING_VALUE cannot be converted to {kind}"),
            (_, Value::Number(_)) => format!("NUMBER_VALUE cannot be converted to {kind}"),
            (_, Value::Bool(true)) => format!("TRUE_VALUE cannot be converted to {kind}"),
            (_, Value::Bool(false)) => format!("FALSE_VALUE cannot be converted to {kind}"),
            (_, Value::Array(_)) => String::new(),
            (_, Value::Object(_)) => {
                "Start of structure or map found where not expected".to_owned()
            }
        };
        return Err(DynamoDbError::SerializationException(msg));
    }
    Ok(())
}

/// The service's prefix on every parameter-value refusal below.
const INVALID_PARAMETER_PREFIX: &str = "One or more parameter values were invalid: ";

/// A future `Timestamp` is accepted up to this far ahead of the server clock.
/// Measured 2026-10-23 (us-east-1): now + 1 s accepted, now + 2 s refused.
const AT_TIMESTAMP_FUTURE_BUFFER_MS: i64 = 1_000;

const SEQUENCE_NUMBER_REFUSED: &str = "SequenceNumber should not be passed for this iterator type";

fn refuse<T>(msg: &str) -> Result<T, DynamoDbError> {
    Err(DynamoDbError::ValidationException(format!(
        "{INVALID_PARAMETER_PREFIX}{msg}"
    )))
}

/// Check the per-type parameter rules that the service runs before it looks
/// the shard up, in its order, and return the `AT_TIMESTAMP` position in
/// milliseconds when that is the type.
///
/// Measured 2026-10-23 and 2026-11-20 (us-east-1). `AT_TIMESTAMP`: a valid
/// `Timestamp`, then no `SequenceNumber`, then the future check.
/// `AT_SEQUENCE_NUMBER` and `AFTER_SEQUENCE_NUMBER`: a `SequenceNumber`, then
/// no `Timestamp`. `TRIM_HORIZON` and `LATEST`: no `Timestamp`; their
/// `SequenceNumber` refusal comes after the shard check
/// ([`refuse_sequence_number_after_shard`]).
pub(crate) fn validate_iterator_parameters(
    input: &GetShardIteratorInput,
) -> Result<Option<i64>, DynamoDbError> {
    match input.shard_iterator_type {
        ShardIteratorType::AtTimestamp => {
            let Some(at_ms) = input.timestamp.and_then(timestamp_to_millis) else {
                return refuse(
                    "Valid timestamp must be provided when the iterator type is AT_TIMESTAMP",
                );
            };
            if input.sequence_number.is_some() {
                return refuse(SEQUENCE_NUMBER_REFUSED);
            }
            let now_ms = i64::try_from(
                web_time::SystemTime::now()
                    .duration_since(web_time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(i64::MAX);
            if at_ms > now_ms.saturating_add(AT_TIMESTAMP_FUTURE_BUFFER_MS) {
                return refuse("Timestamp parameter cannot be greater than the current timestamp");
            }
            Ok(Some(at_ms))
        }
        ShardIteratorType::AtSequenceNumber | ShardIteratorType::AfterSequenceNumber => {
            if input.sequence_number.is_none() {
                return refuse("SequenceNumber cannot be null");
            }
            if input.timestamp.is_some() {
                return refuse("Timestamp should not be passed for this iterator type");
            }
            Ok(None)
        }
        ShardIteratorType::TrimHorizon | ShardIteratorType::Latest => {
            if input.timestamp.is_some() {
                return refuse("Timestamp should not be passed for this iterator type");
            }
            Ok(None)
        }
    }
}

/// `TRIM_HORIZON` and `LATEST` refuse a `SequenceNumber` only once the shard
/// exists: on a shard that does not, the service answers `Shard does not
/// exist` (measured 2026-11-20, us-east-1).
pub(crate) fn refuse_sequence_number_after_shard(
    input: &GetShardIteratorInput,
) -> Result<(), DynamoDbError> {
    if matches!(
        input.shard_iterator_type,
        ShardIteratorType::TrimHorizon | ShardIteratorType::Latest
    ) && input.sequence_number.is_some()
    {
        return refuse(SEQUENCE_NUMBER_REFUSED);
    }
    Ok(())
}

/// Epoch seconds with an optional fraction to whole milliseconds, rounded
/// half up as the service does: for a record at millisecond ...389,
/// `...389.4900` returns it and `...389.5000` does not, and `0.0004` is
/// refused while `0.0005` is accepted (measured 2026-11-20, us-east-1). `None`
/// when the result is not at least one millisecond: zero, negative, NaN. The
/// `i64` edge saturates; the future check refuses it.
pub(crate) fn timestamp_to_millis(secs: f64) -> Option<i64> {
    let ms = (secs * 1000.0).round();
    if ms.is_nan() || ms < 1.0 {
        return None;
    }
    Some(if ms >= 9.2e18 {
        i64::MAX
    } else {
        // Positive and in range after the guards above.
        #[allow(clippy::cast_possible_truncation)]
        let v = ms as i64;
        v
    })
}

/// Convert an AT_SEQUENCE_NUMBER position to the AFTER position the token
/// carries: `n - 1`, zero-padded to the backend's stored width so lexicographic
/// order matches numeric order. `n == 0` is the first possible record, so
/// "at 0" reads from the beginning, the same as TRIM_HORIZON.
pub(crate) fn at_sequence_to_after(raw: &str, width: usize) -> Result<String, DynamoDbError> {
    let n = raw
        .parse::<u128>()
        .map_err(|_| DynamoDbError::ValidationException("Invalid SequenceNumber".to_owned()))?;
    Ok(if n > 0 {
        format!("{:0>width$}", n - 1)
    } else {
        String::new()
    })
}

#[cfg(test)]
mod tests {
    use extenddb_core::error::DynamoDbError;
    use extenddb_core::types::{GetShardIteratorInput, ShardIteratorType};

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
            let result = super::at_sequence_to_after(input, *width).unwrap();
            assert_eq!(&result, expected, "input={input} width={width}");
        }
        assert!(super::at_sequence_to_after("abc", 21).is_err());
    }

    /// Epoch seconds round half up to whole milliseconds, as the service
    /// does. Rows measured 2026-11-20 (us-east-1) on a record at ms ...389:
    /// `.3894900` returned it, `.3895000` did not (so did `.3894999`, whose
    /// double is the same as `.3895`), `.3885100` did. `0.0004` and
    /// `0.00049` were refused, `0.0005` and `0.00051` accepted. A floor fails
    /// the `.3895` and `.3885100` rows.
    #[test]
    fn timestamp_rounds_half_up_to_the_millisecond() {
        let cases: &[(f64, Option<i64>)] = &[
            (1_795_171_973.389_49, Some(1_795_171_973_389)),
            (1_795_171_973.389_5, Some(1_795_171_973_390)),
            (1_795_171_973.388_51, Some(1_795_171_973_389)),
            (1_792_764_248.0, Some(1_792_764_248_000)),
            (1_792_764_248.123_456, Some(1_792_764_248_123)),
            (0.0004, None),
            (0.00049, None),
            (0.0005, Some(1)),
            (0.00051, Some(1)),
            (0.0009, Some(1)),
            (1.0005, Some(1_001)),
            (0.0, None),
            (-5.0, None),
            (f64::NAN, None),
            (1e18, Some(i64::MAX)),
        ];
        for (secs, expected) in cases {
            assert_eq!(super::timestamp_to_millis(*secs), *expected, "secs={secs}");
        }
    }

    fn input(t: ShardIteratorType, seq: Option<&str>, ts: Option<f64>) -> GetShardIteratorInput {
        GetShardIteratorInput {
            stream_arn:
                "arn:aws:dynamodb:us-east-1:123456789012:table/T/stream/2026-10-23T14:03:56.871"
                    .to_owned(),
            shard_id: "shardId-00000001792764241200-b719fef1".to_owned(),
            shard_iterator_type: t,
            sequence_number: seq.map(str::to_owned),
            timestamp: ts,
        }
    }

    fn message(r: Result<Option<i64>, DynamoDbError>) -> String {
        match r {
            Err(DynamoDbError::ValidationException(m)) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    const TS_REQUIRED: &str =
        "Valid timestamp must be provided when the iterator type is AT_TIMESTAMP";
    const SEQ_REFUSED: &str = "SequenceNumber should not be passed for this iterator type";
    const TS_REFUSED: &str = "Timestamp should not be passed for this iterator type";
    const SEQ_NULL: &str = "SequenceNumber cannot be null";
    const FUTURE: &str = "Timestamp parameter cannot be greater than the current timestamp";

    /// The pre-lookup rules, their prefix and their order per type, as
    /// measured on the service 2026-10-23 and 2026-11-20 (us-east-1). `Err`
    /// rows carry the sentence after the prefix; `Ok` rows the position.
    #[test]
    fn parameter_validation_matches_the_service_order_and_text() {
        use ShardIteratorType::{
            AfterSequenceNumber, AtSequenceNumber, AtTimestamp, Latest, TrimHorizon,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let past = 1_792_764_148.0;
        let future = now + 3_600.0;
        type Row = (
            ShardIteratorType,
            Option<&'static str>,
            Option<f64>,
            Result<Option<i64>, &'static str>,
        );
        let cases: &[Row] = &[
            // AT_TIMESTAMP: a valid Timestamp first; null, zero, 0.0004 and negative refuse.
            (AtTimestamp, None, None, Err(TS_REQUIRED)),
            (AtTimestamp, None, Some(0.0), Err(TS_REQUIRED)),
            (AtTimestamp, None, Some(0.0004), Err(TS_REQUIRED)),
            (AtTimestamp, None, Some(-1.0), Err(TS_REQUIRED)),
            (AtTimestamp, None, Some(f64::NAN), Err(TS_REQUIRED)),
            (AtTimestamp, Some("1"), None, Err(TS_REQUIRED)),
            // Then no SequenceNumber, before the future check.
            (AtTimestamp, Some("1"), Some(past), Err(SEQ_REFUSED)),
            (AtTimestamp, Some("1"), Some(future), Err(SEQ_REFUSED)),
            // Then the future check; a past Timestamp resolves to milliseconds.
            (AtTimestamp, None, Some(future), Err(FUTURE)),
            (AtTimestamp, None, Some(past), Ok(Some(1_792_764_148_000))),
            (AtTimestamp, None, Some(0.0005), Ok(Some(1))),
            // AT_/AFTER_SEQUENCE_NUMBER: a SequenceNumber first, then no Timestamp.
            (AtSequenceNumber, None, None, Err(SEQ_NULL)),
            (AtSequenceNumber, None, Some(past), Err(SEQ_NULL)),
            (AfterSequenceNumber, None, None, Err(SEQ_NULL)),
            (AfterSequenceNumber, None, Some(past), Err(SEQ_NULL)),
            (AtSequenceNumber, Some("1"), Some(past), Err(TS_REFUSED)),
            (AfterSequenceNumber, Some("1"), Some(past), Err(TS_REFUSED)),
            (AtSequenceNumber, Some("1"), None, Ok(None)),
            (AfterSequenceNumber, Some("1"), None, Ok(None)),
            // TRIM_HORIZON and LATEST: no Timestamp; a SequenceNumber passes
            // here and is refused after the shard check.
            (TrimHorizon, None, Some(past), Err(TS_REFUSED)),
            (Latest, None, Some(past), Err(TS_REFUSED)),
            (TrimHorizon, Some("1"), Some(past), Err(TS_REFUSED)),
            (Latest, Some("1"), Some(past), Err(TS_REFUSED)),
            (TrimHorizon, Some("1"), None, Ok(None)),
            (Latest, Some("1"), None, Ok(None)),
            (TrimHorizon, None, None, Ok(None)),
            (Latest, None, None, Ok(None)),
        ];
        for (t, seq, ts, expected) in cases {
            let got = super::validate_iterator_parameters(&input(*t, *seq, *ts));
            match expected {
                Ok(pos) => assert_eq!(got.unwrap(), *pos, "type={t:?} seq={seq:?} ts={ts:?}"),
                Err(sentence) => assert_eq!(
                    message(got),
                    format!("One or more parameter values were invalid: {sentence}"),
                    "type={t:?} seq={seq:?} ts={ts:?}"
                ),
            }
        }
    }

    /// Only TRIM_HORIZON and LATEST refuse a SequenceNumber after the shard
    /// check; the other types never reach it with one to refuse.
    #[test]
    fn the_positional_types_refuse_a_sequence_number_after_the_shard_check() {
        use ShardIteratorType::{AtSequenceNumber, AtTimestamp, Latest, TrimHorizon};
        for t in [TrimHorizon, Latest] {
            let got = super::refuse_sequence_number_after_shard(&input(t, Some("1"), None));
            assert_eq!(
                message(got.map(|()| None)),
                format!("One or more parameter values were invalid: {SEQ_REFUSED}")
            );
            assert!(super::refuse_sequence_number_after_shard(&input(t, None, None)).is_ok());
        }
        for t in [AtTimestamp, AtSequenceNumber] {
            assert!(super::refuse_sequence_number_after_shard(&input(t, Some("1"), None)).is_ok());
        }
    }

    /// The deserializer texts for a member whose JSON type does not fit,
    /// measured 2026-10-23 and 2026-11-20: SequenceNumber is checked before
    /// Timestamp (the model order), an array has no message, and null or a
    /// fitting type passes to the parameter rules.
    #[test]
    fn member_shape_texts_match_the_service() {
        use serde_json::{Value, json};
        let cases: &[(Value, Option<&str>)] = &[
            (
                json!({"Timestamp": "1792764248.5"}),
                Some("STRING_VALUE cannot be converted to Date"),
            ),
            (
                json!({"Timestamp": ""}),
                Some("STRING_VALUE cannot be converted to Date"),
            ),
            (
                json!({"Timestamp": true}),
                Some("TRUE_VALUE cannot be converted to Date"),
            ),
            (
                json!({"Timestamp": false}),
                Some("FALSE_VALUE cannot be converted to Date"),
            ),
            (
                json!({"Timestamp": {"S": "x"}}),
                Some("Start of structure or map found where not expected"),
            ),
            (json!({"Timestamp": []}), Some("")),
            (json!({"Timestamp": [1]}), Some("")),
            (
                json!({"SequenceNumber": 5}),
                Some("NUMBER_VALUE cannot be converted to String"),
            ),
            (
                json!({"SequenceNumber": true}),
                Some("TRUE_VALUE cannot be converted to String"),
            ),
            (json!({"SequenceNumber": []}), Some("")),
            (
                json!({"SequenceNumber": {}}),
                Some("Start of structure or map found where not expected"),
            ),
            (
                json!({"SequenceNumber": 5, "Timestamp": "x"}),
                Some("NUMBER_VALUE cannot be converted to String"),
            ),
            (
                json!({"ShardIteratorType": "BOGUS", "Timestamp": "x"}),
                Some("STRING_VALUE cannot be converted to Date"),
            ),
            (json!({"Timestamp": 1.5}), None),
            (json!({"Timestamp": null, "SequenceNumber": null}), None),
            (json!({"SequenceNumber": "1"}), None),
            (json!({}), None),
        ];
        for (body, expected) in cases {
            match (super::check_member_shapes(body), expected) {
                (Ok(()), None) => {}
                (Err(DynamoDbError::SerializationException(m)), Some(e)) => {
                    assert_eq!(m, *e, "body={body}");
                }
                (got, _) => panic!("body={body}: expected {expected:?}, got {got:?}"),
            }
        }
    }

    /// The future buffer is about one second: now + 0.5 s is accepted, now +
    /// 2 s is refused (measured; the 1 s to 2 s band depends on clock skew).
    #[test]
    fn future_buffer_admits_half_a_second_and_refuses_two() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let v = super::validate_iterator_parameters;
        assert!(
            v(&input(
                ShardIteratorType::AtTimestamp,
                None,
                Some(now + 0.5)
            ))
            .is_ok()
        );
        assert_eq!(
            message(v(&input(
                ShardIteratorType::AtTimestamp,
                None,
                Some(now + 2.0)
            ))),
            format!("One or more parameter values were invalid: {FUTURE}")
        );
    }

    /// A write that lands while the tip is being read is still ahead of the
    /// resolved position. The double performs that write inside
    /// `latest_sequence_number` and reports the tip including it. Tip first:
    /// the seek then finds the new record and the iterator positions AT it.
    /// Seek first: the seek finds nothing, the tip read then includes the new
    /// record, and the iterator starts after it, skipping it.
    #[tokio::test]
    async fn at_timestamp_positions_before_a_write_that_lands_between_the_two_reads() {
        use std::sync::Mutex;

        use extenddb_storage::BoxedFuture as BoxFuture;
        use extenddb_storage::error::StorageError;
        use extenddb_storage::{StreamEngine, StreamListResult, StreamRecordsResult};

        struct RacingShard {
            /// Sequence numbers in the shard, with their creation ms.
            records: Mutex<Vec<(String, i64)>>,
            /// The record the racing writer adds during the tip read.
            late: (String, i64),
        }
        impl StreamEngine for RacingShard {
            fn write_stream_record(
                &self,
                _: &str,
                _: &extenddb_core::types::StreamRecord,
                _: &str,
                _: &str,
            ) -> BoxFuture<'_, Result<(), StorageError>> {
                unreachable!()
            }
            fn get_stream_records(
                &self,
                _: &str,
                _: &str,
                _: &str,
                _: Option<&str>,
                _: i64,
            ) -> BoxFuture<'_, StreamRecordsResult> {
                unreachable!()
            }
            fn describe_stream(
                &self,
                _: &str,
                _: &extenddb_core::types::DescribeStreamInput,
            ) -> BoxFuture<'_, Result<extenddb_core::types::StreamDescription, StorageError>>
            {
                unreachable!()
            }
            fn list_streams(
                &self,
                _: &str,
                _: Option<&str>,
                _: i64,
                _: Option<&str>,
            ) -> BoxFuture<'_, StreamListResult> {
                unreachable!()
            }
            fn cleanup_expired_stream_records(
                &self,
                _: i64,
            ) -> BoxFuture<'_, Result<u64, StorageError>> {
                unreachable!()
            }
            fn assign_shard(
                &self,
                _: &str,
                _: &str,
                _: &str,
            ) -> BoxFuture<'_, Result<String, StorageError>> {
                unreachable!()
            }
            fn next_sequence_number(&self, _: &str) -> BoxFuture<'_, Result<String, StorageError>> {
                unreachable!()
            }
            fn validate_shard(
                &self,
                _: &str,
                _: &str,
                _: &str,
            ) -> BoxFuture<'_, Result<(), StorageError>> {
                Box::pin(async { Ok(()) })
            }
            fn latest_sequence_number(
                &self,
                _: &str,
            ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
                Box::pin(async move {
                    let mut recs = self.records.lock().unwrap();
                    // The racing write lands while this read runs; the store
                    // reports the tip as it stands, which includes it.
                    recs.push(self.late.clone());
                    Ok(recs.last().map(|(s, _)| s.clone()))
                })
            }
            fn first_sequence_number_at_or_after(
                &self,
                _: &str,
                at_ms: i64,
            ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
                Box::pin(async move {
                    let recs = self.records.lock().unwrap();
                    Ok(recs
                        .iter()
                        .filter(|(_, ms)| *ms >= at_ms)
                        .map(|(s, _)| s.clone())
                        .min())
                })
            }
        }

        let shard = RacingShard {
            records: Mutex::new(vec![("000000000000000000005".to_owned(), 1_000)]),
            late: ("000000000000000000006".to_owned(), 2_500),
        };
        // T is after the only stored record and before the racing write.
        let pos = crate::streams::resolve_at_timestamp(&shard, "shardId-x", 2_000, 21)
            .await
            .unwrap();
        // AT record 6 is encoded as AFTER record 5: GetRecords returns 6 first.
        assert_eq!(pos, "000000000000000000005");
    }
}
