// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cases for the output side: report objects, the manifest, and the write
//! check the runner makes at the first report record and before the
//! manifest.

use serde_json::{Value, json};

use extenddb_storage::ReportKind;

use super::super::{OutputSse, ReportWriter};
use super::*;
use crate::bulk_put::report::{FileProcessingStatus, manifest_key, manifest_line, report_key};
use crate::bulk_put::s3::tests::smoke::{gunzip, noisy_records};

/// Error report records `first` to `first + count - 1`.
fn records(first: usize, count: usize) -> Vec<Value> {
    (first..first + count)
        .map(|index| json!({"itemLocation": {"itemIndex": index}, "errorMessages": ["e"]}))
        .collect()
}

fn error_key(prefix: &str, index: u32) -> String {
    report_key(prefix, ACCOUNT, OPERATION, ReportKind::Error, index)
}

async fn open_writer<F: StoreFixture>(f: &F, bucket: &str, prefix: &str) -> Box<dyn ReportWriter> {
    f.sink()
        .open(
            &at(bucket),
            prefix,
            OPERATION,
            ReportKind::Error,
            LEASE,
            &OutputSse::None,
        )
        .await
        .unwrap()
}

/// The records of the report object at `key`, or `None` when there is no
/// object there.
async fn report_records<F: StoreFixture>(f: &F, bucket: &str, key: &str) -> Option<Vec<Value>> {
    let body = f.get(bucket, key).await?;
    Some(
        gunzip(&body)
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect(),
    )
}

/// `pending` once the store has had time to abort what a dropped writer
/// started: the S3 writer aborts its uploads on a task of its own.
async fn settled_pending<F: StoreFixture>(f: &F, bucket: &str) -> Vec<String> {
    for _ in 0..100 {
        let pending = f.pending(bucket).await;
        if pending.is_empty() {
            return pending;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    f.pending(bucket).await
}

/// A report is installed under the key the service spells for its prefix,
/// for no prefix, one ending in `/`, one without, and one with a doubled
/// `/`, and holds the records written.
pub(super) async fn a_report_is_installed_under_the_service_key_for_each_prefix_form<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    for (index, prefix) in ["", "reports/", "reports", "deep//er"]
        .into_iter()
        .enumerate()
    {
        let written = records(index * 10, 3);
        let mut writer = open_writer(f, &bucket, prefix).await;
        writer.write_page(written.clone()).await.unwrap();
        let key = writer.finish().await.unwrap();
        assert_eq!(key, error_key(prefix, 0), "{prefix:?}");
        assert_eq!(
            report_records(f, &bucket, &key).await,
            Some(written),
            "{prefix:?}"
        );
    }
    assert!(f.pending(&bucket).await.is_empty());
}

/// With the rotation threshold lowered, two pages give two objects; neither
/// is visible before `finish`, which installs both in index order and no
/// third.
pub(super) async fn rotation_installs_two_objects_and_neither_is_visible_before_finish<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    let mut writer = open_writer(f, &bucket, "reports").await;
    writer.set_rotation_threshold(1);
    writer.write_page(records(0, 3)).await.unwrap();
    writer.write_page(records(3, 2)).await.unwrap();
    let keys: Vec<String> = (0..3).map(|index| error_key("reports", index)).collect();
    for key in &keys {
        assert_eq!(f.get(&bucket, key).await, None, "{key} before finish");
    }
    assert_eq!(writer.finish().await.unwrap(), keys[0]);
    assert_eq!(
        report_records(f, &bucket, &keys[0]).await,
        Some(records(0, 3))
    );
    assert_eq!(
        report_records(f, &bucket, &keys[1]).await,
        Some(records(3, 2))
    );
    assert_eq!(f.get(&bucket, &keys[2]).await, None);
    assert!(f.pending(&bucket).await.is_empty());
}

/// A writer dropped without `finish` leaves no object and no write in
/// progress, after enough records that the S3 writer had a multipart
/// upload under way.
pub(super) async fn a_writer_dropped_without_finish_leaves_nothing_behind<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    let mut writer = open_writer(f, &bucket, "reports").await;
    for seed in 0..30 {
        writer
            .write_page(noisy_records(2_000, 100 + seed))
            .await
            .unwrap();
    }
    drop(writer);
    let pending = settled_pending(f, &bucket).await;
    assert!(pending.is_empty(), "{pending:?}");
    let keys = f.keys(&bucket).await;
    assert!(keys.is_empty(), "{keys:?}");
}

/// A report larger than one upload part (8 MiB on S3, which sends it as a
/// multipart upload and completes it at `finish`) is one object holding
/// every record in order, with no write left in progress.
pub(super) async fn a_report_larger_than_one_upload_part_is_one_object<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    let mut writer = open_writer(f, &bucket, "reports").await;
    let mut written = Vec::new();
    for seed in 0..30 {
        let page = noisy_records(2_000, 200 + seed);
        written.extend(page.iter().cloned());
        writer.write_page(page).await.unwrap();
    }
    let key = writer.finish().await.unwrap();
    assert_eq!(f.keys(&bucket).await, std::slice::from_ref(&key));
    let body = f.get(&bucket, &key).await.unwrap();
    assert!(body.len() > 8 * 1024 * 1024, "{} bytes", body.len());
    assert_eq!(report_records(f, &bucket, &key).await, Some(written));
    assert!(f.pending(&bucket).await.is_empty());
}

/// A second render of the same report replaces the first.
pub(super) async fn a_second_render_replaces_the_first<F: StoreFixture>(f: &F) {
    let bucket = f.bucket().await;
    for (first, count) in [(0, 5), (100, 2)] {
        let mut writer = open_writer(f, &bucket, "reports").await;
        writer.write_page(records(first, count)).await.unwrap();
        let key = writer.finish().await.unwrap();
        assert_eq!(
            report_records(f, &bucket, &key).await,
            Some(records(first, count))
        );
    }
    assert_eq!(f.keys(&bucket).await, [error_key("reports", 0)]);
}

/// The lines of the manifest at `key`.
async fn manifest_lines<F: StoreFixture>(f: &F, bucket: &str, key: &str) -> Vec<Value> {
    let body = f.get(bucket, key).await.expect("manifest written");
    String::from_utf8(body)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The manifest is installed under the key the service spells, one JSON
/// object per line in order, and a second write replaces it.
pub(super) async fn the_manifest_is_installed_under_the_service_key_and_replaced<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    let first = vec![
        manifest_line(
            "loads",
            "in/part-0.json",
            FileProcessingStatus::FullyProcessed,
            Some(4),
            "etag-0",
        ),
        manifest_line(
            "loads",
            "in/part-1.json",
            FileProcessingStatus::NotProcessed,
            None,
            "etag-1",
        ),
    ];
    let second = vec![manifest_line(
        "loads",
        "in/part-0.json",
        FileProcessingStatus::PartiallyProcessed,
        Some(1),
        "etag-0",
    )];
    for lines in [first, second] {
        let key = f
            .sink()
            .write_manifest(
                &at(&bucket),
                "reports",
                OPERATION,
                LEASE,
                &OutputSse::None,
                lines.clone(),
            )
            .await
            .unwrap();
        assert_eq!(key, manifest_key("reports", ACCOUNT, OPERATION));
        assert_eq!(manifest_lines(f, &bucket, &key).await, lines);
    }
    assert!(f.pending(&bucket).await.is_empty());
}

/// The write check passes a bucket that exists, for a report key and for
/// the manifest key, and leaves no object and no write in progress.
pub(super) async fn the_location_check_passes_a_bucket_that_exists_and_writes_nothing<
    F: StoreFixture,
>(
    f: &F,
) {
    let bucket = f.bucket().await;
    for key in [
        error_key("reports", 0),
        manifest_key("reports", ACCOUNT, OPERATION),
    ] {
        f.sink()
            .check_location(&at(&bucket), &key, &OutputSse::None)
            .await
            .unwrap();
    }
    assert!(f.keys(&bucket).await.is_empty());
    assert!(settled_pending(f, &bucket).await.is_empty());
}

/// The write check answers a bucket that does not exist with
/// `S3NoSuchBucket`, with the report writer's tail for a report key and the
/// manifest write's for the manifest key.
pub(super) async fn the_location_check_answers_a_missing_bucket_with_no_such_bucket<
    F: StoreFixture,
>(
    f: &F,
) {
    // A bucket of the caller's exists, as it does once Start has run; the
    // check names another.
    f.bucket().await;
    let missing = f.missing_bucket();
    let error = f
        .sink()
        .check_location(&at(&missing), &error_key("reports", 0), &OutputSse::None)
        .await
        .expect_err("the bucket does not exist");
    assert_location_fault::<F>(
        &sink_fault(error),
        "S3NoSuchBucket",
        NO_SUCH_BUCKET,
        Tail::S3 { status: 404 },
    );
    let error = f
        .sink()
        .check_location(
            &at(&missing),
            &manifest_key("reports", ACCOUNT, OPERATION),
            &OutputSse::None,
        )
        .await
        .expect_err("the bucket does not exist");
    assert_location_fault::<F>(
        &sink_fault(error),
        "S3NoSuchBucket",
        NO_SUCH_BUCKET,
        Tail::AmazonS3 {
            status: 404,
            code: "NoSuchBucket",
        },
    );
}
