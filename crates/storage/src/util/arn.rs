// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for constructing and parsing resource ARNs.

use crate::error::StorageError;

/// The partition of every ARN this server issues, and of the `DynamoDB` ARNs
/// these helpers and `backup_arn` format. A backup ARN in another
/// partition can name nothing here, so the engine refuses one as malformed.
/// Other sites still spell `aws` out themselves (the tagging ARN parser, the
/// PostgreSQL backup queries, the IAM and KMS ARN formats).
pub const ARN_PARTITION: &str = "aws";

/// Returns an ARN for the specified `DynamoDB` index.
#[must_use]
pub fn index_arn(region: &str, account_id: &str, table_name: &str, index_name: &str) -> String {
    format!(
        "arn:{ARN_PARTITION}:dynamodb:{region}:{account_id}:table/{table_name}/index/{index_name}"
    )
}

/// The label that tells one stream on a table name from the next, in the
/// shape the service uses: `YYYY-MM-DDThh:mm:ss.sss`, UTC, millisecond
/// precision, no zone suffix. It is part of the stream ARN
/// (`.../table/<name>/stream/<label>`), so every backend must produce the
/// same bytes for an ARN issued by one to resolve on another. Backends call
/// this and bind the result rather than formatting a timestamp in SQL, so
/// the shape lives in one place.
///
/// Milliseconds rather than seconds because a table deleted and recreated
/// within the same second would otherwise get the same stream ARN, and the
/// old ARN would resolve to the new table's stream. The `time` crate's
/// `Iso8601::DEFAULT` emits nanoseconds with a trailing `Z`, which AWS SDK
/// parsers do not accept here.
#[must_use]
pub fn format_stream_label(now: time::OffsetDateTime) -> String {
    let now = now.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.millisecond(),
    )
}

/// [`format_stream_label`] for the current instant.
#[must_use]
pub fn new_stream_label() -> String {
    format_stream_label(time::OffsetDateTime::now_utc())
}

/// Returns an ARN for the specified `DynamoDB` stream.
#[must_use]
pub fn stream_arn(region: &str, account_id: &str, table_name: &str, stream_label: &str) -> String {
    format!(
        "arn:{ARN_PARTITION}:dynamodb:{region}:{account_id}:table/{table_name}/stream/{stream_label}"
    )
}

/// Returns an ARN for the specified `DynamoDB` table.
#[must_use]
pub fn table_arn(region: &str, account_id: &str, table_name: &str) -> String {
    format!("arn:{ARN_PARTITION}:dynamodb:{region}:{account_id}:table/{table_name}")
}

/// Parse a stream ARN into (`table_name`, `stream_label`).
///
/// Stream ARNs contain ISO 8601 timestamps with colons in the stream label,
/// e.g. `arn:aws:dynamodb:us-east-1:<account-id>:table/T/stream/2026-04-08T08:40:22`.
/// Parsing goes through [`extenddb_core::arn::parse`], the one parser shared
/// with request validation and authorization, so a string the engine resolves
/// to a stream is exactly a string authorization canonicalizes to the same
/// stream. A missing `arn:` prefix, fewer than five fields, or a resource that
/// is not `table/<name>/stream/<label>` is rejected.
pub fn parse_stream_arn(arn: &str) -> Result<(String, String), StorageError> {
    match extenddb_core::arn::parse(arn) {
        Some(extenddb_core::arn::TableResource {
            table,
            sub: Some(extenddb_core::arn::SubResource::Stream(label)),
            ..
        }) => Ok((table, label)),
        _ => Err(StorageError::Validation(format!(
            "Invalid stream ARN format: {arn}"
        ))),
    }
}

/// What `DescribeStream` answers for a stream ARN that names no stream of the
/// caller's account. Measured 2026-10-24 on an existing table with its stream
/// label changed: the sentence names the ARN and has no trailing period.
#[must_use]
pub fn stream_not_found(stream_arn: &str) -> StorageError {
    StorageError::TableNotFound(format!(
        "Requested resource not found: Stream: {stream_arn} not found"
    ))
}

/// What `GetShardIterator` answers for a stream ARN that names no stream of the
/// caller's account. Measured 2026-10-24 with a real shard id and a stream
/// label three digits off.
///
/// `GetRecords` answers with the same sentence, both for such an ARN and for a
/// shard that is not one of the named stream's shards. The service's iterator
/// tokens are opaque, so a token that names such a stream or pairs a stream
/// with another stream's shard cannot be built against it and the answer was
/// not measured.
pub const STREAM_DOES_NOT_EXIST: &str = "Requested resource not found: Stream does not exist";

/// [`STREAM_DOES_NOT_EXIST`] as the storage error the engine maps to a
/// `ResourceNotFoundException`.
#[must_use]
pub fn stream_does_not_exist() -> StorageError {
    StorageError::TableNotFound(STREAM_DOES_NOT_EXIST.to_owned())
}

/// What `GetShardIterator` answers for a shard that is not one of the named
/// stream's shards: the stream-not-found sentence with a trailing period. The
/// service answers "Requested resource not found: Shard does not exist"
/// (measured 2026-10-24), so this text differs from it.
#[must_use]
pub fn shard_not_in_stream(stream_arn: &str) -> StorageError {
    StorageError::TableNotFound(format!(
        "Requested resource not found: Stream: {stream_arn} not found."
    ))
}

#[cfg(test)]
mod stream_not_found_tests {
    use super::{
        STREAM_DOES_NOT_EXIST, shard_not_in_stream, stream_does_not_exist, stream_not_found,
    };
    use crate::error::StorageError;

    const ARN: &str =
        "arn:aws:dynamodb:us-east-1:111111111111:table/T/stream/2026-01-01T00:00:00.000";

    fn message(error: StorageError) -> String {
        match error {
            StorageError::TableNotFound(msg) => msg,
            other => panic!("expected TableNotFound, got {other:?}"),
        }
    }

    #[test]
    fn describe_stream_names_the_arn_and_has_no_trailing_period() {
        assert_eq!(
            message(stream_not_found(ARN)),
            format!("Requested resource not found: Stream: {ARN} not found")
        );
    }

    #[test]
    fn get_shard_iterator_and_get_records_say_the_stream_does_not_exist() {
        assert_eq!(
            STREAM_DOES_NOT_EXIST,
            "Requested resource not found: Stream does not exist"
        );
        assert_eq!(message(stream_does_not_exist()), STREAM_DOES_NOT_EXIST);
    }

    #[test]
    fn a_shard_of_no_stream_of_the_table_keeps_the_sentence_with_the_period() {
        assert_eq!(
            message(shard_not_in_stream(ARN)),
            format!("Requested resource not found: Stream: {ARN} not found.")
        );
    }
}

#[cfg(test)]
mod stream_label_tests {
    use super::format_stream_label;
    use time::{Date, Month, OffsetDateTime, Time, UtcOffset};

    /// `(y, mo, d)`, `(h, mi, s, micros)`, and a whole-hour UTC offset.
    fn at(ymd: (i32, u8, u8), hms_micro: (u8, u8, u8, u32), offset_h: i8) -> OffsetDateTime {
        let (y, mo, d) = ymd;
        let (h, mi, s, micros) = hms_micro;
        let date =
            Date::from_calendar_date(y, Month::try_from(mo).expect("month"), d).expect("date");
        let time = Time::from_hms_micro(h, mi, s, micros).expect("time");
        date.with_time(time)
            .assume_offset(UtcOffset::from_hms(offset_h, 0, 0).expect("offset"))
    }

    #[test]
    fn has_the_service_shape_with_three_fractional_digits() {
        assert_eq!(
            format_stream_label(at((2026, 1, 2), (3, 4, 5, 7_000), 0)),
            "2026-01-02T03:04:05.007"
        );
        assert_eq!(
            format_stream_label(at((2026, 12, 31), (23, 59, 59, 0), 0)),
            "2026-12-31T23:59:59.000"
        );
        // Sub-millisecond digits are dropped, not rounded.
        assert_eq!(
            format_stream_label(at((2026, 1, 2), (3, 4, 5, 999_900), 0)),
            "2026-01-02T03:04:05.999"
        );
    }

    #[test]
    fn is_always_utc() {
        assert_eq!(
            format_stream_label(at((2026, 1, 2), (3, 4, 5, 500_000), 5)),
            "2026-01-01T22:04:05.500"
        );
    }
}
