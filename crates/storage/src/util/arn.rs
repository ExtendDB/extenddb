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

/// The instant a [`format_stream_label`] label names, as the epoch-seconds
/// number `DescribeStream` renders in `CreationRequestDateTime` (measured on
/// the service 2026-10-24: present on every stream, enabled or disabled). The
/// label is the stream's creation instant to the millisecond, so no stored
/// column is needed.
///
/// Two shapes are accepted: the 23-byte `YYYY-MM-DDThh:mm:ss.sss` this module
/// writes, and the 19-byte `YYYY-MM-DDThh:mm:ss` that SQLite, PostgreSQL and
/// MongoDB wrote on v0.1.3 to v0.1.13 and Cassandra up to v0.1.14, which a
/// stream enabled on those versions and never re-enabled still carries. The
/// legacy shape names the instant with a `.000` fraction. `None` for any
/// other shape.
#[must_use]
#[allow(clippy::cast_precision_loss)] // epoch ms fit f64's 53-bit mantissa
pub fn stream_label_creation_date_time(label: &str) -> Option<f64> {
    let b = label.as_bytes();
    let has_fraction = match b.len() {
        19 => false,
        23 if b[19] == b'.' => true,
        _ => return None,
    };
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let num = |range: std::ops::Range<usize>| -> Option<u32> {
        let s = &label[range];
        if s.bytes().all(|c| c.is_ascii_digit()) {
            s.parse().ok()
        } else {
            None
        }
    };
    let year = i32::try_from(num(0..4)?).ok()?;
    let month = time::Month::try_from(u8::try_from(num(5..7)?).ok()?).ok()?;
    let date = time::Date::from_calendar_date(year, month, u8::try_from(num(8..10)?).ok()?).ok()?;
    let millis = if has_fraction {
        u16::try_from(num(20..23)?).ok()?
    } else {
        0
    };
    let clock = time::Time::from_hms_milli(
        u8::try_from(num(11..13)?).ok()?,
        u8::try_from(num(14..16)?).ok()?,
        u8::try_from(num(17..19)?).ok()?,
        millis,
    )
    .ok()?;
    let secs = date.with_time(clock).assume_utc().unix_timestamp();
    Some((secs * 1000 + i64::from(millis)) as f64 / 1000.0)
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

    /// `CreationRequestDateTime` is the label's instant, to the millisecond,
    /// and round-trips through the formatter.
    #[test]
    fn creation_date_time_is_the_label_instant() {
        use super::stream_label_creation_date_time;
        assert_eq!(
            stream_label_creation_date_time("1970-01-01T00:00:00.000"),
            Some(0.0)
        );
        assert_eq!(
            stream_label_creation_date_time("1970-01-01T00:00:01.748"),
            Some(1.748)
        );
        // 2026-10-24T11:31:45.748Z, the label in the issue.
        assert_eq!(
            stream_label_creation_date_time("2026-10-24T11:31:45.748"),
            Some(1_792_841_505.748)
        );
        // The 19-byte legacy label (v0.1.3 to v0.1.13 writers) is the instant
        // with a .000 fraction.
        assert_eq!(
            stream_label_creation_date_time("2026-07-04T10:20:30"),
            Some(1_783_160_430.0)
        );
        // 2026-01-02T03:04:05.007 UTC, through the formatter.
        let label = format_stream_label(at((2026, 1, 2), (3, 4, 5, 7_000), 0));
        assert_eq!(
            stream_label_creation_date_time(&label),
            Some(1_767_323_045.007)
        );
    }

    #[test]
    fn creation_date_time_refuses_other_shapes() {
        use super::stream_label_creation_date_time;
        for bad in [
            "",
            "2026-10-24T11:31:4",
            "2026-10-24T11:31:4S",
            "2026-10-24T11:31:45.7480",
            "2026-10-24T11:31:45.748Z",
            "2026-10-24 11:31:45.748",
            "2026-10-24 11:31:45",
            "2026-13-24T11:31:45.748",
            "2026-10-24T25:31:45.748",
            "2026-1O-24T11:31:45.748",
            "2026-10-24T11:31:45:748",
        ] {
            assert_eq!(stream_label_creation_date_time(bad), None, "{bad:?}");
        }
    }
}
