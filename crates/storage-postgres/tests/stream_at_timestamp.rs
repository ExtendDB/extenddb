// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `AT_TIMESTAMP` seek answers the first record of the shard created at
//! or after the millisecond asked for: a record's own millisecond returns it,
//! the next one does not, and another shard's records do not count. The rows
//! are those of the SQLite `stream_tests`, written here with known
//! `created_at` values, so the `>=` of the seek is pinned on this backend.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. The test builds
//! its own scratch database and drops it afterwards.

#[path = "common/stream_disable_fixture.rs"]
mod fixture;

use fixture::*;

/// A record with the `created_at` the write path stores: the clock read, in
/// milliseconds.
async fn write_record_at(s: &Scratch, shard_id: &str, table_id: &str, seq: u64, ms: i64) {
    let created_at =
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000).expect("ms");
    sqlx::query(
        "INSERT INTO stream_records (shard_id, sequence_number, table_id, event_name, \
         record_data, created_at) VALUES ($1, $2, $3, 'INSERT', $4::jsonb, $5)",
    )
    .bind(shard_id)
    .bind(format!("{seq:021}"))
    .bind(table_id)
    .bind(serde_json::json!({"eventID": format!("e{seq}")}).to_string())
    .bind(created_at)
    .execute(&s.db)
    .await
    .expect("record");
}

/// The other shard's record has the lowest sequence number and a `created_at`
/// inside the table's range, so without the shard filter it is the answer to
/// the first five rows.
#[tokio::test]
async fn at_timestamp_seek_is_at_or_after_the_records_millisecond() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    let desc = s
        .engine
        .create_table(ACCOUNT, table_input("t", true))
        .await
        .expect("create a stream-enabled table");
    let table_id = desc.table_id.clone();
    let shards = open_shard_ids(&s, &table_id).await;
    let (own, other) = (shards[0].as_str(), shards[1].as_str());
    let base = 1_700_000_000_250;
    write_record_at(&s, own, &table_id, 5, base).await;
    write_record_at(&s, own, &table_id, 6, base + 1).await;
    write_record_at(&s, own, &table_id, 7, base + 750).await;
    write_record_at(&s, other, &table_id, 4, base + 2).await;

    let cases: &[(i64, Option<u64>)] = &[
        (0, Some(5)),
        (base - 1, Some(5)),
        (base, Some(5)),
        (base + 1, Some(6)),
        (base + 2, Some(7)),
        (base + 750, Some(7)),
        (base + 751, None),
    ];
    for (at_ms, expected) in cases {
        let got = s
            .engine
            .first_sequence_number_at_or_after(own, *at_ms)
            .await
            .expect("seek");
        assert_eq!(
            got,
            expected.map(|seq| format!("{seq:021}")),
            "at_ms={at_ms}"
        );
    }
    s.cleanup().await;
}
