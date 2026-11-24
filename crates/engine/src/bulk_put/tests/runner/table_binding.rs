// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Table binding.

#[tokio::test]
async fn a_table_deleted_mid_run_fails_with_resource_not_found() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(15)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    let storage = Arc::clone(&h.storage);
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        Box::pin(async move {
            storage
                .delete_table(
                    ACCOUNT,
                    DeleteTableInput {
                        table_name: TABLE.to_owned(),
                    },
                )
                .await
                .expect("delete table");
        })
    });
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 7,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("DynamoDBResourceNotFoundException")
    );
    assert_eq!(record.failure_message.as_deref(), Some(TABLE_NOT_FOUND));
}

// Table binding, continued.

#[tokio::test]
async fn a_table_deleted_between_batches_is_caught_at_the_boundary() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(15)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    // The delete fires on the end-of-file read: every row is already
    // written and no write ever fails, so only the boundary check can
    // notice the table is gone.
    let storage = Arc::clone(&h.storage);
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        Box::pin(async move {
            storage
                .delete_table(
                    ACCOUNT,
                    DeleteTableInput {
                        table_name: TABLE.to_owned(),
                    },
                )
                .await
                .expect("delete table");
        })
    });
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 15,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("DynamoDBResourceNotFoundException")
    );
    assert_eq!(record.failure_message.as_deref(), Some(TABLE_NOT_FOUND));
    assert_eq!(
        record.processed_item_count, 15,
        "every row was written before the delete; the boundary caught it"
    );
}

#[tokio::test]
async fn a_table_recreated_under_the_same_name_is_never_written() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(15)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    // Mid-run the table is deleted and a new one is created under the same
    // name. The binding is by table id, so the operation must fail rather
    // than write into the impostor.
    let storage = Arc::clone(&h.storage);
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        Box::pin(async move {
            storage
                .delete_table(
                    ACCOUNT,
                    DeleteTableInput {
                        table_name: TABLE.to_owned(),
                    },
                )
                .await
                .expect("delete table");
            Harness::create_table(&storage, TABLE).await;
        })
    });
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 7,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("DynamoDBResourceNotFoundException")
    );
    assert_eq!(record.failure_message.as_deref(), Some(TABLE_NOT_FOUND));
    assert_eq!(
        h.count_items().await,
        0,
        "the recreated table holds nothing: the operation never wrote to it"
    );
}
