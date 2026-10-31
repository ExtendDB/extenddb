// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Disabling a table's stream closes its shards, and enabling it again
//! creates a new stream, against the rows measured on the service on
//! 2026-10-24.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. Each test builds
//! its own scratch database and drops it afterwards.

#[path = "common/stream_disable_fixture.rs"]
mod fixture;

use fixture::*;

#[tokio::test]
async fn disable_closes_every_shard_and_re_enable_creates_a_new_stream() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("t482", true))
        .await
        .expect("create");
    activate(&s, "t482").await;
    for pk in ["k0", "k1", "k2"] {
        put(&s, "t482", pk, StreamViewType::NewAndOldImages).await;
    }
    let enabled = describe_table(&s, "t482").await;
    let old_arn = enabled.latest_stream_arn.clone().expect("stream arn");

    let disabled = set_stream(&s, "t482", serde_json::json!({"StreamEnabled": false}))
        .await
        .expect("disable");
    let mut disabled = disabled;
    disabled.prepare_for_response();
    assert!(disabled.stream_specification.is_none());
    assert_eq!(
        disabled.latest_stream_arn.as_deref(),
        Some(old_arn.as_str())
    );
    let mut table = describe_table(&s, "t482").await;
    table.prepare_for_response();
    assert!(table.stream_specification.is_none());
    assert_eq!(table.latest_stream_label, enabled.latest_stream_label);

    let closed = describe_stream(&s, &old_arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert_eq!(closed.stream_view_type, StreamViewType::NewAndOldImages);
    assert_eq!(closed.shards.len(), 4);
    for shard in &closed.shards {
        let range = &shard.sequence_number_range;
        let expected = last_record(&s, &shard.shard_id)
            .await
            .unwrap_or_else(|| range.starting_sequence_number.clone());
        assert_eq!(
            range.ending_sequence_number.as_deref(),
            Some(expected.as_str())
        );
    }

    // A write still carrying the enabled view type records nothing.
    put(&s, "t482", "stale", StreamViewType::NewAndOldImages).await;
    assert_eq!(record_count(&s).await, 3);

    match set_stream(&s, "t482", serde_json::json!({"StreamEnabled": false})).await {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table has no stream to disable: TableName: t482");
        }
        other => panic!("{other:?}"),
    }

    let reenabled = set_stream(
        &s,
        "t482",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .expect("re-enable");
    let new_arn = reenabled.latest_stream_arn.clone().expect("new arn");
    assert_ne!(new_arn, old_arn);
    let new = describe_stream(&s, &new_arn).await;
    assert!(matches!(new.stream_status, StreamStatus::Enabled));
    assert_eq!(new.stream_view_type, StreamViewType::KeysOnly);
    assert!(
        new.shards
            .iter()
            .all(|n| !closed.shards.iter().any(|o| o.shard_id == n.shard_id))
    );
    match set_stream(
        &s,
        "t482",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}),
    )
    .await
    {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table already has an enabled stream: TableName: t482");
        }
        other => panic!("{other:?}"),
    }

    put(&s, "t482", "after", StreamViewType::KeysOnly).await;
    assert_eq!(record_count(&s).await, 4);
    let old_again = describe_stream(&s, &old_arn).await;
    assert!(matches!(old_again.stream_status, StreamStatus::Disabled));
    for (a, b) in old_again.shards.iter().zip(&closed.shards) {
        assert_eq!(a.shard_id, b.shard_id);
        assert_eq!(
            a.sequence_number_range.ending_sequence_number,
            b.sequence_number_range.ending_sequence_number
        );
    }

    let (listed, _) = s
        .engine
        .list_streams(ACCOUNT, Some("t482"), 100, None)
        .await
        .expect("list");
    let arns: Vec<&str> = listed.iter().map(|x| x.stream_arn.as_str()).collect();
    assert_eq!(arns, [new_arn.as_str(), old_arn.as_str()]);
    match s
        .engine
        .validate_shard(ACCOUNT, &old_arn, &new.shards[0].shard_id)
        .await
    {
        Err(StorageError::TableNotFound(m)) => {
            assert_eq!(m, "Requested resource not found: Shard does not exist");
        }
        other => panic!("{other:?}"),
    }
    s.cleanup().await;
}

#[tokio::test]
async fn disable_on_a_table_that_never_had_a_stream_is_refused() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("nostream", false))
        .await
        .expect("create");
    activate(&s, "nostream").await;
    match set_stream(&s, "nostream", serde_json::json!({"StreamEnabled": false})).await {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table has no stream to disable: TableName: nostream");
        }
        other => panic!("{other:?}"),
    }
    // An UpdateTable that enables a stream and is then refused for an index
    // change in the same call leaves no shards and no label behind.
    let input = serde_json::json!({
        "TableName": "nostream",
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"},
        "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "missing"}}],
    });
    let refused = s
        .engine
        .update_table(ACCOUNT, serde_json::from_value(input).expect("input"))
        .await;
    assert!(refused.is_err(), "{refused:?}");
    let shards: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM stream_shards")
        .fetch_one(&s.db)
        .await
        .expect("count shards");
    assert_eq!(shards, 0);
    assert!(
        describe_table(&s, "nostream")
            .await
            .latest_stream_arn
            .is_none()
    );
    s.cleanup().await;
}

/// Writers racing a disable: every record a write lands is in the closed
/// stream at or below its shard's `EndingSequenceNumber`, or the write records
/// nothing. The write path locks only its own shard row, `FOR KEY SHARE`, and
/// the close locks the open rows `FOR UPDATE` before taking the ending
/// numbers, so a record is never written past them.
#[tokio::test]
async fn a_write_racing_a_disable_lands_before_the_ending_number_or_not_at_all() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("race", true))
        .await
        .expect("create");
    activate(&s, "race").await;
    let key_info = s
        .engine
        .table_key_info(ACCOUNT, "race")
        .await
        .expect("key info");
    let arn = describe_table(&s, "race")
        .await
        .latest_stream_arn
        .expect("stream arn");

    let mut writers = tokio::task::JoinSet::new();
    for w in 0..8u32 {
        let engine = s.engine.clone();
        let key_info = key_info.clone();
        writers.spawn(async move {
            let mut acked = 0u32;
            let mut failed = 0u32;
            for i in 0..40u32 {
                let item =
                    serde_json::from_value(serde_json::json!({"pk": {"S": format!("w{w}-{i}")}}))
                        .expect("item");
                let capture = StreamCapture {
                    view_type: StreamViewType::NewAndOldImages,
                    user_identity: None,
                    region: REGION.into(),
                };
                match engine
                    .put_item(
                        &key_info,
                        item,
                        false,
                        None,
                        &ExpressionMaps::default(),
                        Some(&capture),
                    )
                    .await
                {
                    Ok(_) => acked += 1,
                    Err(_) => failed += 1,
                }
            }
            (acked, failed)
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    set_stream(&s, "race", serde_json::json!({"StreamEnabled": false}))
        .await
        .expect("disable");
    let (mut acked, mut failed) = (0u32, 0u32);
    while let Some(done) = writers.join_next().await {
        let (a, f) = done.expect("writer task");
        acked += a;
        failed += f;
    }
    assert_eq!(acked + failed, 320);

    let closed = describe_stream(&s, &arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert!(
        closed
            .shards
            .iter()
            .all(|x| x.sequence_number_range.ending_sequence_number.is_some())
    );
    let past_the_end: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM stream_records r JOIN stream_shards s ON s.shard_id = r.shard_id \
         WHERE s.ending_sequence_number IS NULL OR r.sequence_number > s.ending_sequence_number",
    )
    .fetch_one(&s.db)
    .await
    .expect("count records past an ending number");
    assert_eq!(past_the_end, 0, "{acked} acked, {failed} failed");
    let recorded = record_count(&s).await;
    assert!(
        recorded >= 1 && recorded <= i64::from(acked),
        "{recorded} records for {acked} acked writes"
    );
    s.cleanup().await;
}

/// A disable whose close failed after its catalog write, then an enable whose
/// close of the label it read failed too: the table is enabled under a later
/// label with no shards of its own, and the earlier stream's shards are still
/// open. The next enable closes every earlier stream's open shards, whatever
/// label it read.
#[tokio::test]
async fn an_enable_closes_an_earlier_stream_left_open() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("leftopen", true))
        .await
        .expect("create");
    activate(&s, "leftopen").await;
    put(&s, "leftopen", "k0", StreamViewType::NewAndOldImages).await;
    let table = describe_table(&s, "leftopen").await;
    let first_arn = table.latest_stream_arn.clone().expect("stream arn");
    let first = describe_stream(&s, &first_arn).await;
    let first_ids: Vec<String> = first.shards.iter().map(|x| x.shard_id.clone()).collect();
    let later =
        extenddb_storage::stream_lineage::next_stream_label(Some(first.stream_label.as_str()));
    sqlx::query("UPDATE tables SET stream_label = $1 WHERE table_name = 'leftopen'")
        .bind(&later)
        .execute(&s.db)
        .await
        .expect("move the label on, as the enable's catalog write did");
    assert_eq!(open_shard_ids(&s, &table.table_id).await, first_ids);

    set_stream(&s, "leftopen", serde_json::json!({"StreamEnabled": false}))
        .await
        .expect("disable");
    let reenabled = set_stream(
        &s,
        "leftopen",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .expect("enable");
    let new = describe_stream(&s, &reenabled.latest_stream_arn.expect("new arn")).await;
    let new_ids: Vec<String> = new.shards.iter().map(|x| x.shard_id.clone()).collect();
    assert_eq!(
        open_shard_ids(&s, &table.table_id).await,
        new_ids,
        "only the new stream's shards are open"
    );
    for id in &first_ids {
        let (start, end): (String, Option<String>) = sqlx::query_as(
            "SELECT starting_sequence_number, ending_sequence_number \
             FROM stream_shards WHERE shard_id = $1",
        )
        .bind(id)
        .fetch_one(&s.db)
        .await
        .expect("shard row");
        let expected = last_record(&s, id).await.unwrap_or(start);
        assert_eq!(end.as_deref(), Some(expected.as_str()));
    }
    s.cleanup().await;
}

/// A disable that read stream A and applies its shard changes only after an
/// overlapping enable has closed A and created stream B: it closes A's
/// shards, which are closed already, and leaves B's open.
#[tokio::test]
async fn a_disable_applying_after_an_overlapping_enable_leaves_the_new_stream_open() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("overlap", true))
        .await
        .expect("create");
    activate(&s, "overlap").await;
    put(&s, "overlap", "k0", StreamViewType::NewAndOldImages).await;
    let table = describe_table(&s, "overlap").await;
    let old_label = table.latest_stream_label.clone().expect("label");
    set_stream(&s, "overlap", serde_json::json!({"StreamEnabled": false}))
        .await
        .expect("disable");
    let closed = describe_stream(&s, &table.latest_stream_arn.clone().expect("arn")).await;
    let reenabled = set_stream(
        &s,
        "overlap",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .expect("enable");
    let new = describe_stream(&s, &reenabled.latest_stream_arn.expect("new arn")).await;
    let new_ids: Vec<String> = new.shards.iter().map(|x| x.shard_id.clone()).collect();
    put(&s, "overlap", "k1", StreamViewType::KeysOnly).await;

    s.engine
        .apply_disable_shard_changes_for_tests(ACCOUNT, "overlap", &table.table_id, &old_label)
        .await
        .expect("the late disable's shard changes");
    assert_eq!(
        open_shard_ids(&s, &table.table_id).await,
        new_ids,
        "the new stream's four shards stay open"
    );
    let old_again = describe_stream(&s, &closed.stream_arn).await;
    for (a, b) in old_again.shards.iter().zip(&closed.shards) {
        assert_eq!(
            a.sequence_number_range.ending_sequence_number,
            b.sequence_number_range.ending_sequence_number
        );
    }
    s.cleanup().await;
}
