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
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 1, "{manifest:?}");
    let source = manifest[0]["inputSourceArn"].as_str().unwrap_or_default();
    assert!(source.ends_with("/part-000.json"), "{source}");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("FULLY_PROCESSED")
    );
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

/// A source whose listing runs `action` and then never answers, so the
/// listing ends only when the lease renewal sees what the action did.
struct ActThenPendListSource {
    inner: FsSource,
    action: LineAction,
}

impl BulkSource for ActThenPendListSource {
    fn check_bucket(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>> {
        self.inner.check_bucket(bucket, prefix)
    }

    fn etag(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>> {
        self.inner.etag(bucket, file)
    }

    fn line_count(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>> {
        self.inner.line_count(bucket, file, compression)
    }

    fn list(
        &self,
        _bucket: &BucketRef<'_>,
        _prefix: &str,
        _max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        let action = Arc::clone(&self.action);
        Box::pin(async move {
            action().await;
            std::future::pending().await
        })
    }

    fn open(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        self.inner.open(bucket, file, compression)
    }
}

/// A cancel, or a deleted table, during the first claim's listing ends
/// the operation as at a boundary: `CANCELLED`, or `FAILED` with the
/// table's code, with no file recorded and no output object.
#[tokio::test]
async fn a_cancel_or_a_deleted_table_during_the_listing_ends_the_operation() {
    for cancel in [true, false] {
        let h = Harness::new().await;
        h.write_input("part-000.json", &file_of(&plain_rows(3)));
        let op = h.start_unlisted(input_config(false), both_reports()).await;
        let storage = Arc::clone(&h.storage);
        let id = op.clone();
        let action: LineAction = Arc::new(move || {
            let storage = Arc::clone(&storage);
            let id = id.clone();
            Box::pin(async move {
                if cancel {
                    storage
                        .request_cancel(ACCOUNT, &id)
                        .await
                        .expect("request cancel");
                } else {
                    storage
                        .delete_table(
                            ACCOUNT,
                            DeleteTableInput {
                                table_name: TABLE.to_owned(),
                            },
                        )
                        .await
                        .expect("delete table");
                }
            })
        });
        let source = Arc::new(ActThenPendListSource {
            inner: h.fs_source(),
            action,
        });
        let mut cfg = config();
        cfg.lease = Duration::from_secs(1);
        let record = h.claim("runner-1", cfg.lease).await;
        let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
        tokio::time::timeout(Duration::from_secs(60), runner::run(&deps, record))
            .await
            .expect("a renewal ends the listing")
            .expect("runner run");

        let record = h.record(&op).await;
        if cancel {
            assert_eq!(record.status, "CANCELLED");
        } else {
            assert_eq!(record.status, "FAILED");
            assert_eq!(
                record.failure_code.as_deref(),
                Some("DynamoDBResourceNotFoundException")
            );
        }
        assert!(record.input_files.is_empty(), "cancel {cancel}");
        assert_eq!(record.total_input_bytes, None, "cancel {cancel}");
        assert!(h.read_manifest(&op).is_none(), "no output object");
    }
}
