// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the stream retention cutoff and sweep in `super`.

use super::retention_cutoff;
use crate::SqliteEngine;
use time::OffsetDateTime;

/// 2026-10-24T14:19:07Z: a `now` with a zero sub-second, so the cutoff 24
/// hours earlier lands on the whole second 2026-10-23T14:19:07Z.
const WHOLE_SECOND_NOW: i64 = 1_792_851_547;

/// The cutoff carries the column's three fraction digits even on a whole
/// second, so same-second rows do not sort before it.
#[test]
fn cutoff_on_a_whole_second_keeps_the_column_shape() {
    let now = OffsetDateTime::from_unix_timestamp(WHOLE_SECOND_NOW).expect("now");
    let cutoff = retention_cutoff(now, 24);
    assert_eq!(cutoff, "2026-10-23T14:19:07.000Z");
    assert!("2026-10-23T14:19:07.000Z" >= cutoff.as_str());
    assert!("2026-10-23T14:19:07.123Z" > cutoff.as_str());
    assert!("2026-10-23T14:19:06.999Z" < cutoff.as_str());
}

/// A `now` with a fraction renders the fraction, same width.
#[test]
fn cutoff_with_a_fraction_keeps_the_column_shape() {
    let now = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(WHOLE_SECOND_NOW) * 1_000_000_000 + 456_000_000,
    )
    .expect("now");
    assert_eq!(retention_cutoff(now, 24), "2026-10-23T14:19:07.456Z");
    assert_eq!(retention_cutoff(now, 0), "2026-10-24T14:19:07.456Z");

    // A fraction with a trailing zero keeps all three digits. RFC 3339
    // rendering dropped it (`07.45Z`), and `07.450Z` to `07.459Z` sort before
    // `07.45Z` as text, so those rows were deleted early as well.
    let now_450 = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(WHOLE_SECOND_NOW) * 1_000_000_000 + 450_000_000,
    )
    .expect("now");
    let cutoff = retention_cutoff(now_450, 24);
    assert_eq!(cutoff, "2026-10-23T14:19:07.450Z");
    assert!("2026-10-23T14:19:07.450Z" >= cutoff.as_str());
    assert!("2026-10-23T14:19:07.459Z" > cutoff.as_str());
}

/// Rows in the cutoff's own second survive the sweep; only the row in the
/// previous second is deleted.
#[tokio::test]
async fn sweep_on_a_whole_second_keeps_same_second_rows() {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::schema::apply(&engine.pool).await.expect("schema");
    sqlx::query(
        "INSERT INTO accounts (account_id, account_name) VALUES ('000000000000', 'default')",
    )
    .execute(&engine.pool)
    .await
    .expect("account");
    sqlx::query(
        "INSERT INTO tables (account_id, table_name, key_schema, attribute_definitions, \
         table_arn, table_id) VALUES ('000000000000', 't', '[]', '[]', 'arn', 'tid')",
    )
    .execute(&engine.pool)
    .await
    .expect("table");
    sqlx::query(
        "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
         VALUES ('shard-1', 'tid', '1')",
    )
    .execute(&engine.pool)
    .await
    .expect("shard");
    for (seq, created_at) in [
        ("1", "2026-10-23T14:19:06.999Z"),
        ("2", "2026-10-23T14:19:07.000Z"),
        ("3", "2026-10-23T14:19:07.123Z"),
    ] {
        sqlx::query(
            "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, \
             record_data, created_at) VALUES ('shard-1', ?, 'tid', 'INSERT', '{}', ?)",
        )
        .bind(seq)
        .bind(created_at)
        .execute(&engine.pool)
        .await
        .expect("record");
    }

    let now = OffsetDateTime::from_unix_timestamp(WHOLE_SECOND_NOW).expect("now");
    let deleted = engine
        .cleanup_expired_stream_records_at(now, 24)
        .await
        .expect("sweep");
    assert_eq!(deleted, 1);

    let remaining: Vec<(String,)> =
        sqlx::query_as("SELECT created_at FROM stream_records ORDER BY created_at")
            .fetch_all(&engine.pool)
            .await
            .expect("remaining");
    let remaining: Vec<&str> = remaining.iter().map(|(s,)| s.as_str()).collect();
    assert_eq!(
        remaining,
        ["2026-10-23T14:19:07.000Z", "2026-10-23T14:19:07.123Z"]
    );
}
