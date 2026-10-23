// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `GetShardIterator` parameter rules and position conversions.
//!
//! Messages and the order of the checks were measured on the service
//! 2026-10-23 (us-east-1); see `tests/test_streams_at_timestamp.py` for the
//! wire-level expectations taken from the same run.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::{GetShardIteratorInput, ShardIteratorType};

/// How the service renders a `ShardIteratorType` that is not a member: no
/// value, and the set in this order. Measured 2026-10-23 (us-east-1) for an
/// unknown name and for a lowercase one alike.
pub(crate) const SHARD_ITERATOR_TYPE_ENUM_CLAUSE: &str = "Value at 'shardIteratorType' failed to \
     satisfy constraint: Member must satisfy enum value set: [AFTER_SEQUENCE_NUMBER, LATEST, \
     AT_TIMESTAMP, AT_SEQUENCE_NUMBER, TRIM_HORIZON]";

/// Refuse a `Timestamp` whose JSON type is not a number with the text the
/// service's deserializer renders for it (measured 2026-10-23): a string is
/// `STRING_VALUE`, a boolean `TRUE_VALUE` or `FALSE_VALUE`, an array or object
/// the structure message. Numbers and null pass through to the parameter
/// rules, which treat null as absent.
pub(crate) fn check_timestamp_shape(body: &serde_json::Value) -> Result<(), DynamoDbError> {
    use serde_json::Value;
    let Some(v) = body.get("Timestamp") else {
        return Ok(());
    };
    let msg = match v {
        Value::Null | Value::Number(_) => return Ok(()),
        Value::String(_) => "STRING_VALUE cannot be converted to Date",
        Value::Bool(true) => "TRUE_VALUE cannot be converted to Date",
        Value::Bool(false) => "FALSE_VALUE cannot be converted to Date",
        Value::Array(_) | Value::Object(_) => "Start of structure or map found where not expected",
    };
    Err(DynamoDbError::SerializationException(msg.to_owned()))
}

/// The service's prefix on every parameter-value refusal below.
const INVALID_PARAMETER_PREFIX: &str = "One or more parameter values were invalid: ";

/// A future `Timestamp` is accepted up to this far ahead of the server clock.
/// Measured 2026-10-23 (us-east-1): now + 1 s accepted, now + 2 s refused.
const AT_TIMESTAMP_FUTURE_BUFFER_MS: i64 = 1_000;

/// Check the per-type parameter rules in the service's order and return the
/// `AT_TIMESTAMP` position in milliseconds when that is the type.
///
/// Messages and order measured on the service 2026-10-23 (us-east-1).
pub(crate) fn validate_iterator_parameters(
    input: &GetShardIteratorInput,
) -> Result<Option<i64>, DynamoDbError> {
    let refuse = |msg: &str| {
        Err(DynamoDbError::ValidationException(format!(
            "{INVALID_PARAMETER_PREFIX}{msg}"
        )))
    };
    let is_at_timestamp = input.shard_iterator_type == ShardIteratorType::AtTimestamp;
    let takes_sequence = matches!(
        input.shard_iterator_type,
        ShardIteratorType::AtSequenceNumber | ShardIteratorType::AfterSequenceNumber
    );

    // Timestamp presence or absence for the type comes first.
    let at_ms = if is_at_timestamp {
        match input.timestamp {
            Some(t) if t > 0.0 && t.is_finite() => Some(timestamp_to_millis(t)),
            _ => {
                return refuse(
                    "Valid timestamp must be provided when the iterator type is AT_TIMESTAMP",
                );
            }
        }
    } else {
        if input.timestamp.is_some() {
            return refuse("Timestamp should not be passed for this iterator type");
        }
        None
    };

    // Then SequenceNumber presence or absence for the type.
    if takes_sequence {
        if input.sequence_number.is_none() {
            return refuse("SequenceNumber cannot be null");
        }
    } else if input.sequence_number.is_some() {
        return refuse("SequenceNumber should not be passed for this iterator type");
    }

    // Then the future check.
    if let Some(at_ms) = at_ms {
        let now_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        if at_ms > now_ms.saturating_add(AT_TIMESTAMP_FUTURE_BUFFER_MS) {
            return refuse("Timestamp parameter cannot be greater than the current timestamp");
        }
    }
    Ok(at_ms)
}

/// Epoch seconds with an optional fraction to whole milliseconds, floored.
/// Saturates at the `i64` edge; the caller has already refused values that
/// are not positive and finite.
pub(crate) fn timestamp_to_millis(secs: f64) -> i64 {
    let ms = (secs * 1000.0).floor();
    if ms >= 9.2e18 {
        i64::MAX
    } else {
        // Non-negative and in range after the guard above.
        #[allow(clippy::cast_possible_truncation)]
        let v = ms as i64;
        v
    }
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

    /// Epoch seconds with a fraction floor to whole milliseconds, as the
    /// service seeks at millisecond precision (measured: a `Timestamp` of
    /// `.5` skipped records written at `.050`).
    #[test]
    fn timestamp_to_millis_floors_to_the_millisecond() {
        assert_eq!(
            super::timestamp_to_millis(1_792_764_248.0),
            1_792_764_248_000
        );
        assert_eq!(
            super::timestamp_to_millis(1_792_764_248.05),
            1_792_764_248_050
        );
        assert_eq!(
            super::timestamp_to_millis(1_792_764_248.999),
            1_792_764_248_999
        );
        assert_eq!(
            super::timestamp_to_millis(1_792_764_248.123_456),
            1_792_764_248_123
        );
        // Floors, never rounds: .9996 ms stays at 123.
        assert_eq!(
            super::timestamp_to_millis(1_792_764_248.123_999_6),
            1_792_764_248_123
        );
        assert_eq!(super::timestamp_to_millis(1e18), i64::MAX);
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

    /// Messages, their prefix, and the order of the checks, as measured on
    /// the service 2026-10-23 (us-east-1).
    #[test]
    fn parameter_validation_matches_the_service_order_and_text() {
        use ShardIteratorType::{
            AfterSequenceNumber, AtSequenceNumber, AtTimestamp, Latest, TrimHorizon,
        };
        const P: &str = "One or more parameter values were invalid: ";
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let past = 1_792_764_148.0;
        let v = super::validate_iterator_parameters;

        // AT_TIMESTAMP needs a positive Timestamp; null, zero and negative all refuse.
        for ts in [None, Some(0.0), Some(-1.0), Some(f64::NAN)] {
            assert_eq!(
                message(v(&input(AtTimestamp, None, ts))),
                format!(
                    "{P}Valid timestamp must be provided when the iterator type is AT_TIMESTAMP"
                ),
                "ts={ts:?}"
            );
        }
        // The Timestamp check runs before the SequenceNumber check.
        assert_eq!(
            message(v(&input(AtTimestamp, Some("1"), None))),
            format!("{P}Valid timestamp must be provided when the iterator type is AT_TIMESTAMP")
        );
        // SequenceNumber with AT_TIMESTAMP refuses, and does so before the future check.
        for ts in [past, now + 3_600.0] {
            assert_eq!(
                message(v(&input(AtTimestamp, Some("1"), Some(ts)))),
                format!("{P}SequenceNumber should not be passed for this iterator type")
            );
        }
        // Timestamp with any other type refuses, before the SequenceNumber check.
        for t in [TrimHorizon, Latest, AtSequenceNumber, AfterSequenceNumber] {
            for seq in [None, Some("1")] {
                assert_eq!(
                    message(v(&input(t, seq, Some(past)))),
                    format!("{P}Timestamp should not be passed for this iterator type"),
                    "type={t:?} seq={seq:?}"
                );
            }
        }
        // SequenceNumber rules for the four other types.
        for t in [TrimHorizon, Latest] {
            assert_eq!(
                message(v(&input(t, Some("1"), None))),
                format!("{P}SequenceNumber should not be passed for this iterator type")
            );
            assert_eq!(v(&input(t, None, None)).unwrap(), None);
        }
        for t in [AtSequenceNumber, AfterSequenceNumber] {
            assert_eq!(
                message(v(&input(t, None, None))),
                format!("{P}SequenceNumber cannot be null")
            );
            assert_eq!(v(&input(t, Some("1"), None)).unwrap(), None);
        }
        // A far-future Timestamp refuses; a past one resolves to milliseconds.
        assert_eq!(
            message(v(&input(AtTimestamp, None, Some(now + 3_600.0)))),
            format!("{P}Timestamp parameter cannot be greater than the current timestamp")
        );
        assert_eq!(
            v(&input(AtTimestamp, None, Some(past))).unwrap(),
            Some(1_792_764_148_000)
        );
    }

    /// Malformed `Timestamp` JSON types render the service's deserializer
    /// texts (measured); numbers and null pass to the parameter rules.
    #[test]
    fn timestamp_shape_texts_match_the_service() {
        use serde_json::json;
        let msg =
            |v: serde_json::Value| match super::check_timestamp_shape(&json!({"Timestamp": v})) {
                Err(DynamoDbError::SerializationException(m)) => m,
                other => panic!("expected SerializationException, got {other:?}"),
            };
        assert_eq!(
            msg(json!("1792764248.5")),
            "STRING_VALUE cannot be converted to Date"
        );
        assert_eq!(msg(json!("")), "STRING_VALUE cannot be converted to Date");
        assert_eq!(msg(json!(true)), "TRUE_VALUE cannot be converted to Date");
        assert_eq!(msg(json!(false)), "FALSE_VALUE cannot be converted to Date");
        assert_eq!(
            msg(json!({"S": "x"})),
            "Start of structure or map found where not expected"
        );
        assert_eq!(
            msg(json!([1])),
            "Start of structure or map found where not expected"
        );
        assert!(super::check_timestamp_shape(&json!({"Timestamp": 1.5})).is_ok());
        assert!(super::check_timestamp_shape(&json!({"Timestamp": null})).is_ok());
        assert!(super::check_timestamp_shape(&json!({})).is_ok());
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
        assert!(
            v(&input(
                ShardIteratorType::AtTimestamp,
                None,
                Some(now + 2.0)
            ))
            .is_err()
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
