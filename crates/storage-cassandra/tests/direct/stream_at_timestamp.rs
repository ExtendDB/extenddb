// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for the `AT_TIMESTAMP` seek
//! (`first_sequence_number_at_or_after`) on the Cassandra engine.

use std::collections::BTreeMap;

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{AttributeValue, StreamViewType};
use extenddb_storage::DataEngine;

use crate::helpers::setup_engine;
use crate::streams::{capture, setup_stream_table, shard_for, stream_shard_for};

/// A record written under a clamped clock (after a backwards step) carries a
/// sequence prefix later than its `created_at`. The `AT_TIMESTAMP` seek must
/// not return it for a `T` after its `created_at`, and must still return a
/// record created at or after `T`.
#[tokio::test]
async fn test_at_timestamp_seek_skips_a_clamped_record_older_than_t() {
    use extenddb_storage::StreamEngine;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (key_info, label) = setup_stream_table(&engine, "StreamClampSeek").await;
    let view = StreamViewType::NewAndOldImages;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("pk-clamp".to_string()));
    item.insert("val".to_string(), AttributeValue::S("real".to_string()));
    engine
        .put_item(
            &key_info,
            item,
            false,
            None,
            &Default::default(),
            Some(&capture(view)),
        )
        .await
        .unwrap();

    // The records sit in the partition `shard_for` names; the seek takes the
    // stream's shard id, as a client does.
    let partition = shard_for("pk-clamp", &key_info.table_id);
    let shard_id = stream_shard_for("pk-clamp", &key_info.table_id, &label, view);
    let keyspace = format!("extenddb_ttl_test_account_{}", key_info.account_id);
    let rows = engine
        .session_arc()
        .query_with_values(
            &format!(
                "SELECT sequence_number, created_at, record_data FROM {keyspace}.stream_records \
                 WHERE shard_id = ?"
            ),
            cdrs_tokio::query_values!(partition.as_str()),
        )
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default();
    assert_eq!(rows.len(), 1);
    let real_seq: String = rows[0].get_r_by_name("sequence_number").unwrap();
    let real_created: i64 = rows[0].get_r_by_name("created_at").unwrap();
    let data: String = rows[0].get_r_by_name("record_data").unwrap();

    // Model the clamped record: sequence prefix 8 s after the real record,
    // created_at 8 s before it. T sits between the two: after the clamped
    // created_at, before the real record's.
    let clamped_hlc = real_created + 8_000;
    let clamped_created = real_created - 8_000;
    let clamped_seq = format!("{clamped_hlc:013}{:06}{:04}", 0, 9999);
    engine
        .session_arc()
        .query_with_values(
            &format!(
                "INSERT INTO {keyspace}.stream_records \
                 (shard_id, sequence_number, table_id, event_name, record_data, created_at) \
                 VALUES (?, ?, ?, 'Insert', ?, ?)"
            ),
            cdrs_tokio::query_values!(
                partition.as_str(),
                clamped_seq.as_str(),
                key_info.table_id.as_str(),
                data.as_str(),
                clamped_created
            ),
        )
        .await
        .unwrap();

    // T after the clamped created_at and before the real record: the real
    // record qualifies, the clamped one does not.
    let t = real_created - 1_000;
    let got = engine
        .first_sequence_number_at_or_after(&shard_id, t)
        .await
        .unwrap();
    assert_eq!(
        got.as_deref(),
        Some(real_seq.as_str()),
        "the real record is first at or after T"
    );

    // T after every created_at: nothing qualifies, even though the clamped
    // sequence prefix clears the bound.
    let t = real_created + 1_000;
    let got = engine
        .first_sequence_number_at_or_after(&shard_id, t)
        .await
        .unwrap();
    assert_eq!(got, None, "a clamped record older than T is not returned");

    // T before everything: both records pass the filter, and the first by
    // sequence is the real one, whose prefix is 8 s before the clamped one.
    let got = engine
        .first_sequence_number_at_or_after(&shard_id, clamped_created - 1)
        .await
        .unwrap();
    assert_eq!(
        got.as_deref(),
        Some(real_seq.as_str()),
        "the first record by sequence, the real one, is the answer"
    );
}

/// Every stream of a table keeps its records in the partition of the shard
/// index, and the seek reads only the shard's own range of it. A closed shard
/// answers nothing for a time after its close, even once the next stream has
/// written to the same partition; the next stream's shard does not answer the
/// record of the stream before it.
#[tokio::test]
async fn test_at_timestamp_seek_reads_only_the_shards_range() {
    use crate::stream_disable::{activate, put, set_stream};
    use extenddb_storage::StreamEngine;
    use extenddb_storage_cassandra::cassandra_util::now_millis;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let name = "StreamSeekRange";
    let (key_info, old_label) = setup_stream_table(&engine, name).await;
    activate(&engine).await;
    let account = key_info.account_id.as_str();
    let view = StreamViewType::NewAndOldImages;

    let start = now_millis() - 1_000;
    put(&engine, account, name, "pk-range", view).await;
    let old_shard = stream_shard_for("pk-range", &key_info.table_id, &old_label, view);
    let old_seq = engine
        .first_sequence_number_at_or_after(&old_shard, start)
        .await
        .unwrap()
        .expect("the first stream's record");

    set_stream(
        &engine,
        account,
        name,
        serde_json::json!({"StreamEnabled": false}),
    )
    .await
    .unwrap();
    let after_close = now_millis();
    let enabled = set_stream(
        &engine,
        account,
        name,
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}),
    )
    .await
    .unwrap();
    let new_label = enabled.latest_stream_label.expect("the new stream's label");
    put(&engine, account, name, "pk-range", view).await;
    let new_shard = stream_shard_for("pk-range", &key_info.table_id, &new_label, view);

    // The closed shard keeps its record and ends at it, so the new stream's
    // record in the same partition is not its answer after the close.
    assert_eq!(
        engine
            .first_sequence_number_at_or_after(&old_shard, start)
            .await
            .unwrap(),
        Some(old_seq.clone())
    );
    assert_eq!(
        engine
            .first_sequence_number_at_or_after(&old_shard, after_close)
            .await
            .unwrap(),
        None
    );
    // The new stream's shard starts after the enable: for a time before the
    // first stream's record, its answer is its own record.
    let new_seq = engine
        .first_sequence_number_at_or_after(&new_shard, start)
        .await
        .unwrap()
        .expect("the new stream's record");
    assert_ne!(new_seq, old_seq);
    assert_eq!(
        engine.latest_sequence_number(&new_shard).await.unwrap(),
        Some(new_seq)
    );
}
