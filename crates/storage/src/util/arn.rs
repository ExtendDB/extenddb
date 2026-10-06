// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for constructing and parsing resource ARNs.

use crate::error::StorageError;

/// Returns an ARN for the specified `DynamoDB` index.
#[must_use]
pub fn index_arn(region: &str, account_id: &str, table_name: &str, index_name: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/index/{index_name}")
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
    format!("arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/stream/{stream_label}")
}

/// Returns an ARN for the specified `DynamoDB` table.
#[must_use]
pub fn table_arn(region: &str, account_id: &str, table_name: &str) -> String {
    format!("arn:aws:dynamodb:{region}:{account_id}:table/{table_name}")
}

/// Parse a stream ARN into (`table_name`, `stream_label`).
///
/// Stream ARNs contain ISO 8601 timestamps with colons in the stream label,
/// e.g. `arn:aws:dynamodb:us-east-1:<account-id>:table/T/stream/2026-04-08T08:40:22`.
/// We use `splitn(6, ':')` so the 6th element preserves everything after the 5th
/// colon delimiter, including colons within the stream label.
pub fn parse_stream_arn(arn: &str) -> Result<(String, String), StorageError> {
    let segments: Vec<&str> = arn.splitn(6, ':').collect();
    let resource = segments
        .get(5)
        .ok_or_else(|| StorageError::Validation(format!("Invalid stream ARN: {arn}")))?;
    let parts: Vec<&str> = resource.splitn(4, '/').collect();
    if parts.len() == 4 && parts[0] == "table" && parts[2] == "stream" {
        Ok((parts[1].to_owned(), parts[3].to_owned()))
    } else {
        Err(StorageError::Validation(format!(
            "Invalid stream ARN format: {arn}"
        )))
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
