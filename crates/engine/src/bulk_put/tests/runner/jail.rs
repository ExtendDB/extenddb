// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Filesystem jail.

#[tokio::test]
async fn enumeration_skips_symlinked_files_and_directories() {
    let h = Harness::new().await;
    h.write_input("a.json", &file_of(&[row(0)]));
    h.write_input("b.json", &file_of(&[row(1)]));

    // Content outside the bucket that the symlinks point at. Writing any of
    // it would be an escape from the enumerated set.
    let outside = std::env::temp_dir().join(format!("eb-bulk-outside-{}", uuid::Uuid::new_v4()));
    let outside_dir = outside.join("dir");
    std::fs::create_dir_all(&outside_dir).unwrap();
    let evil = "{\"Item\": {\"pk\": {\"S\": \"evil\"}, \"sk\": {\"S\": \"s\"}}}\n";
    std::fs::write(outside.join("linked.json"), evil).unwrap();
    std::fs::write(outside_dir.join("nested.json"), evil).unwrap();
    let bucket_dir = h.store.0.join(ACCOUNT).join(IN_BUCKET);
    std::os::unix::fs::symlink(outside.join("linked.json"), bucket_dir.join("c.json")).unwrap();
    std::os::unix::fs::symlink(&outside_dir, bucket_dir.join("sub")).unwrap();

    let files = h.enumerate().await;
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        ["a.json", "b.json"],
        "the symlinked file and directory are not enumerated"
    );

    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 2);
    assert!(
        h.get("evil").await.is_none(),
        "no linked content is written"
    );

    let _ = std::fs::remove_dir_all(&outside);
}

#[tokio::test]
async fn a_symlink_planted_at_awsdynamodb_fails_the_operation_and_creates_nothing() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&[row_missing_sk(0)]));
    let files = h.enumerate().await;

    // The attacker owns the export prefix directory and plants a symlink
    // where the report tree would be created.
    let foreign = std::env::temp_dir().join(format!("eb-bulk-foreign-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&foreign).unwrap();
    let prefix_dir = h.store.0.join(ACCOUNT).join(OUT_BUCKET).join(OUT_PREFIX);
    std::fs::create_dir_all(&prefix_dir).unwrap();
    std::os::unix::fs::symlink(&foreign, prefix_dir.join("AWSDynamoDB")).unwrap();

    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("S3AccessDenied"),
        "a refused report path is the caller's to fix, not a server fault"
    );
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Access Denied"),
        "no path detail reaches the client"
    );
    let created: Vec<_> = std::fs::read_dir(&foreign).unwrap().collect();
    assert!(
        created.is_empty(),
        "nothing is created behind the planted symlink"
    );

    let _ = std::fs::remove_dir_all(&foreign);
}

/// An input file swapped for a symlink after Start is refused when the
/// runner opens it. The refusal is the caller's tree, not a server fault,
/// so the operation fails with the access-denied code and nothing is
/// written; the symlink target is never read.
#[tokio::test]
async fn an_input_file_swapped_for_a_symlink_after_start_fails_with_access_denied() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    let files = h.enumerate().await;

    let elsewhere =
        std::env::temp_dir().join(format!("eb-bulk-elsewhere-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join("part-000.json"), file_of(&plain_rows(3))).unwrap();
    let input = h
        .store
        .0
        .join(ACCOUNT)
        .join(IN_BUCKET)
        .join("part-000.json");
    std::fs::remove_file(&input).unwrap();
    std::os::unix::fs::symlink(elsewhere.join("part-000.json"), &input).unwrap();

    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
    assert_eq!(record.processed_item_count, 0);
    assert_eq!(h.count_items().await, 0, "the symlink target is never read");
}
