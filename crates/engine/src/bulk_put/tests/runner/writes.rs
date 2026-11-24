// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Write semantics.

#[tokio::test]
async fn a_bulk_row_replaces_the_stored_item_in_full() {
    let h = Harness::new().await;
    let mut stored = item("k0", "1");
    stored.insert(
        "legacy".to_owned(),
        AttributeValue::S("kept-before".to_owned()),
    );
    h.put_direct(stored).await;

    h.write_input("part-000.json", &file_of(&[row(0)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    let after = h.get("k0").await.expect("item present");
    assert!(
        !after.contains_key("legacy"),
        "attributes absent from the input row are gone after the write"
    );
    assert_eq!(after.get("v"), Some(&AttributeValue::N("0".to_owned())));
}

// Stream emission.

#[tokio::test]
async fn a_bulk_run_over_a_stream_enabled_table_emits_one_insert_per_applied_row() {
    let h = Harness::new_with_stream().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);

    // Each record lands on the shard its partition key hashes to, so
    // collect across every shard, the way apply_put's unit test does.
    let (streams, _) = h
        .storage
        .list_streams(ACCOUNT, Some(TABLE), 10, None)
        .await
        .expect("list streams");
    let stream_arn = streams.first().expect("one stream").stream_arn.clone();
    let description = h
        .storage
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn,
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("describe stream");
    let mut records = Vec::new();
    for shard in &description.shards {
        let (shard_records, _) = h
            .storage
            .get_stream_records(ACCOUNT, &description.stream_arn, &shard.shard_id, None, 100)
            .await
            .expect("get records");
        records.extend(shard_records);
    }

    assert_eq!(
        records.len(),
        2,
        "one record per applied row, none for the failed row"
    );
    let mut keys = Vec::new();
    for record in &records {
        assert_eq!(record.event_name, StreamEventName::Insert);
        assert_eq!(record.aws_region, REGION);
        assert!(record.user_identity.is_none(), "no user identity");
        let new_image = record
            .dynamodb
            .new_image
            .as_ref()
            .expect("NewImage populated");
        assert!(
            record.dynamodb.old_image.is_none(),
            "fresh keys carry no old image"
        );
        keys.push(match new_image.get("pk") {
            Some(AttributeValue::S(pk)) => pk.clone(),
            other => panic!("string pk expected, got {other:?}"),
        });
    }
    keys.sort_unstable();
    assert_eq!(keys, ["k0", "k2"], "exactly the two applied rows streamed");
}

/// The runner re-reads the table's key metadata at every checkpoint
/// boundary before writing the rows staged since the last one, so a stream
/// enabled while the operation runs records every row from the next
/// boundary on, as it would for a `PutItem` issued after the enable. With
/// the metadata read once at the start, the rows after the enable would
/// produce no records at all.
#[tokio::test]
async fn a_stream_enabled_during_the_run_records_every_row_written_after_it() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(20)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    // Rows 0 to 9 are written across two boundaries with no stream; the
    // enable lands just before line 10 is handed to the runner, so rows 10
    // to 19 are staged after it and flushed at the two boundaries that
    // follow.
    let storage = Arc::clone(&h.storage);
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        Box::pin(async move {
            let input: UpdateTableInput = serde_json::from_value(json!({
                "TableName": TABLE,
                "StreamSpecification": {
                    "StreamEnabled": true,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            }))
            .expect("update table input");
            storage
                .update_table(ACCOUNT, input)
                .await
                .expect("enable the stream");
        })
    });
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 10,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 20);
    for i in 0..20 {
        assert!(h.get(&format!("k{i}")).await.is_some(), "k{i} written");
    }

    let (streams, _) = h
        .storage
        .list_streams(ACCOUNT, Some(TABLE), 10, None)
        .await
        .expect("list streams");
    let stream_arn = streams.first().expect("one stream").stream_arn.clone();
    let description = h
        .storage
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn,
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("describe stream");
    let mut keys = Vec::new();
    for shard in &description.shards {
        let (shard_records, _) = h
            .storage
            .get_stream_records(ACCOUNT, &description.stream_arn, &shard.shard_id, None, 100)
            .await
            .expect("get records");
        for record in shard_records {
            assert_eq!(record.event_name, StreamEventName::Insert);
            let new_image = record.dynamodb.new_image.expect("NewImage populated");
            keys.push(match new_image.get("pk") {
                Some(AttributeValue::S(pk)) => pk.clone(),
                other => panic!("string pk expected, got {other:?}"),
            });
        }
    }
    keys.sort_unstable();
    let expected: Vec<String> = (10..20).map(|i| format!("k{i}")).collect();
    assert_eq!(
        keys, expected,
        "one record for every row staged after the enable, none before it"
    );
}

// Conditions.

#[tokio::test]
async fn a_condition_failed_row_emits_no_stream_record() {
    let h = Harness::new_with_stream().await;
    // The seed goes through the harness's direct put, which passes no
    // stream capture, so the stream starts empty: the run adds exactly one
    // INSERT (row 0) and nothing for the condition row, whose write never
    // reaches the table.
    h.put_direct(item("c", "1")).await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), condition_row("2", "attribute_not_exists(pk)")]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 0);
    let stored = h.get("c").await.expect("seed present");
    assert_eq!(stored.get("v"), Some(&AttributeValue::N("1".to_owned())));

    let (streams, _) = h
        .storage
        .list_streams(ACCOUNT, Some(TABLE), 10, None)
        .await
        .expect("list streams");
    let stream_arn = streams.first().expect("one stream").stream_arn.clone();
    let description = h
        .storage
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn,
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("describe stream");
    let mut records = Vec::new();
    for shard in &description.shards {
        let (shard_records, _) = h
            .storage
            .get_stream_records(ACCOUNT, &description.stream_arn, &shard.shard_id, None, 100)
            .await
            .expect("get records");
        records.extend(shard_records);
    }
    let keys: Vec<String> = records
        .iter()
        .map(|r| match r.dynamodb.keys.get("pk") {
            Some(AttributeValue::S(s)) => s.clone(),
            other => panic!("unexpected key {other:?}"),
        })
        .collect();
    assert_eq!(
        keys,
        vec!["k0".to_owned()],
        "one record for the applied row; none for the condition-failed row"
    );
}
#[tokio::test]
async fn a_true_condition_writes_the_row_and_leaves_no_report() {
    let h = Harness::new().await;
    let conditional =
        "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}, \"v\": {\"N\": \"1\"}}, \
                       \"ConditionExpression\": \"attribute_not_exists(pk)\"}"
            .to_owned();
    h.write_input("part-000.json", &file_of(&[conditional]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.failed_item_count, 0);
    assert!(h.get("c").await.is_some(), "the condition passed and wrote");
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none()
    );
    assert!(
        h.read_report(&record.operation_id, ReportKind::ConditionCheckFailure)
            .is_none()
    );
}

#[tokio::test]
async fn a_false_condition_with_ccf_requested_routes_to_the_condition_report() {
    let h = Harness::new().await;
    h.put_direct(item("c", "1")).await;
    let conditional =
        "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}, \"v\": {\"N\": \"2\"}}, \
                       \"ConditionExpression\": \"attribute_not_exists(pk)\"}"
            .to_owned();
    h.write_input("part-000.json", &file_of(&[conditional]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED", "condition failures do not fail");
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.failed_item_count, 0);
    let stored = h.get("c").await.expect("target item present");
    assert_eq!(
        stored.get("v"),
        Some(&AttributeValue::N("1".to_owned())),
        "the target item is untouched"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::ConditionCheckFailure)
        .expect("condition report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([CONDITION_FAILED]));
    assert_eq!(report[0]["newImage"]["Item"]["v"]["N"], json!("2"));
    assert_eq!(report[0]["oldImage"]["Item"]["v"]["N"], json!("1"));
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none(),
        "nothing lands in the error report"
    );
}

#[tokio::test]
async fn a_false_condition_on_a_large_row_is_charged_the_floor_alone() {
    // Measured on the service: 1,000 condition-failed rows of about
    // 382,300 logical bytes charged 1,024,000 in total, so a rejected row
    // costs the one kilobyte floor whatever its size, and no index.
    let h = Harness::new_with_gsi().await;
    h.put_direct(item("c", "1")).await;
    let pad = "p".repeat(300_000);
    let conditional = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"c\"}}, \"sk\": {{\"S\": \"s\"}}, \"g\": {{\"S\": \"x\"}}, \"pad\": {{\"S\": \"{pad}\"}}}}, \
         \"ConditionExpression\": \"attribute_not_exists(pk)\"}}"
    );
    h.write_input("part-000.json", &file_of(&[conditional]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.usage["Table"]["BulkPutBytes"], json!(1024));
    assert_eq!(record.usage["BulkPutBytes"], json!(1024));
    assert!(
        record
            .usage
            .get("GlobalSecondaryIndexes")
            .is_none_or(|m| m.as_object().is_some_and(serde_json::Map::is_empty)),
        "a rejected row charges no index"
    );
}

#[tokio::test]
async fn a_false_condition_with_no_report_bucket_keeps_counters_and_writes_no_report() {
    let h = Harness::new().await;
    h.put_direct(item("c", "1")).await;
    let conditional =
        "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}, \"v\": {\"N\": \"2\"}}, \
                       \"ConditionExpression\": \"attribute_not_exists(pk)\"}"
            .to_owned();
    h.write_input("part-000.json", &file_of(&[conditional]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(
            files,
            input_config(false),
            output_config(None, &["ERROR", "CONDITION_CHECK_FAILURE"]),
            config(),
        )
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.failed_item_count, 0);
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none()
    );
    assert!(
        h.read_report(&record.operation_id, ReportKind::ConditionCheckFailure)
            .is_none()
    );
    let report_tree = h.store.0.join(ACCOUNT).join(OUT_BUCKET).join(OUT_PREFIX);
    assert!(
        !report_tree.exists(),
        "no report bucket configured: no report tree is created"
    );
}

#[tokio::test]
async fn a_false_condition_with_only_error_requested_is_written_nowhere() {
    // Measured on the service: with only ERROR requested the condition
    // record appears in no output file (the September re-route into
    // errors/ is gone from the service); the counters are unchanged by it.
    let h = Harness::new().await;
    h.put_direct(item("c", "1")).await;
    let conditional =
        "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}, \"v\": {\"N\": \"2\"}}, \
                       \"ConditionExpression\": \"attribute_not_exists(pk)\"}"
            .to_owned();
    h.write_input("part-000.json", &file_of(&[conditional]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(
            files,
            input_config(false),
            output_config(Some(OUT_BUCKET), &["ERROR"]),
            config(),
        )
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.failed_item_count, 0);
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none(),
        "the condition record is not re-routed into errors/"
    );
    assert!(
        h.read_report(&record.operation_id, ReportKind::ConditionCheckFailure)
            .is_none(),
        "no condition-check-failures file when the type was not requested"
    );
}

#[tokio::test]
async fn an_error_record_with_only_ccf_requested_is_written_nowhere_but_still_counts() {
    // Measured on the service: with only CONDITION_CHECK_FAILURE
    // requested an item-level error record appears in no output file, and
    // the row still counts as failed.
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(
            files,
            input_config(false),
            output_config(Some(OUT_BUCKET), &["CONDITION_CHECK_FAILURE"]),
            config(),
        )
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(
        h.read_report(&record.operation_id, ReportKind::ConditionCheckFailure)
            .is_none(),
        "the error record is not re-routed into condition-check-failures/"
    );
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none(),
        "no errors file when ERROR was not requested"
    );
}
