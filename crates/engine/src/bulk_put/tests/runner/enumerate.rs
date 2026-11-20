// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The first claim of an operation the Start handler inserted with no
//! files lists the input and records the files and their total before any
//! row is read (`runner::enumerate`).

use super::*;

#[tokio::test]
async fn the_first_claim_records_the_listing_then_reads_it() {
    let h = Harness::new().await;
    let body = file_of(&plain_rows(3));
    h.write_input("part-000.json", &body);
    let op = h.start_unlisted(input_config(false), both_reports()).await;
    let started = h.record(&op).await;
    assert!(started.input_files.is_empty());
    assert_eq!(started.total_input_bytes, None);

    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    let size = i64::try_from(body.len()).unwrap();
    let files: Vec<(&str, i64)> = record
        .input_files
        .iter()
        .map(|f| (f.path.as_str(), f.size))
        .collect();
    assert_eq!(files, vec![("part-000.json", size)]);
    assert_eq!(record.total_input_bytes, Some(size));
    assert_eq!(record.processed_input_bytes, size);
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(h.count_items().await, 3);
}

/// A cancel that lands before the first claim ends the operation
/// `CANCELLED` with nothing listed and no output object (measured on the
/// service: operations cancelled 3.2 s or less after Start carry neither
/// byte member and wrote no output object).
#[tokio::test]
async fn a_cancel_before_the_first_claim_lists_and_writes_nothing() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    let op = h.start_unlisted(input_config(false), both_reports()).await;
    let cancelled = h
        .storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");
    assert!(matches!(cancelled, CancelOutcome::Cancelling));

    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert!(record.input_files.is_empty());
    assert_eq!(record.total_input_bytes, None);
    assert!(h.read_manifest(&op).is_none(), "no output object");
    assert_eq!(h.count_items().await, 0);
}
