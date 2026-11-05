// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// The manifest line of a file listed without an entity tag, on a store
// whose tags the service assigns (an S3-compatible service whose listing
// leaves the ETag out). The line names the tag of the first answer the run
// accepted for the file, or none when the run accepted no answer for it. It
// never names the object at the key when the manifest is written, which may
// be a replacement the run did not read.

/// The tag the store asks for when the manifest is written, standing for
/// the object now at the key: a replacement.
const REPLACEMENT: &str = "replacement-etag";

/// How the source treats one input file.
#[derive(Clone, Copy)]
enum Answer {
    /// The first answer is at another size than the listed one, which the
    /// S3 store reports as an overwritten object.
    OverwrittenAtOpen,
    /// The open fails.
    RefuseOpen,
    /// The open is accepted and the read fails as a deleted object does
    /// once this many lines were handed out.
    GoneAfter(u64),
}

/// A filesystem source that reports its tags the way the S3 store does:
/// the service assigns them, each accepted open carries the tag of its
/// first answer (`first-<path>`), and `etag` answers the replacement's tag
/// for a file listed without one and counts the calls.
struct ServiceTagSource {
    inner: FsSource,
    answers: Vec<(&'static str, Answer)>,
    etag_calls: Arc<AtomicU64>,
}

struct ServiceTagReader {
    inner: Box<dyn LineReader>,
    tag: String,
    gone_after: Option<u64>,
    handed: u64,
}

impl ServiceTagSource {
    fn new(h: &Harness, answers: Vec<(&'static str, Answer)>) -> Self {
        Self {
            inner: h.fs_source(),
            answers,
            etag_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    fn answer(&self, path: &str) -> Option<Answer> {
        self.answers
            .iter()
            .find(|(p, _)| *p == path)
            .map(|(_, answer)| *answer)
    }
}

impl BulkSource for ServiceTagSource {
    fn check_bucket(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>> {
        self.inner.check_bucket(bucket, prefix)
    }

    fn list(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
        max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        self.inner.list(bucket, prefix, max_files)
    }

    fn open(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        let answer = self.answer(&file.path);
        if matches!(answer, Some(Answer::OverwrittenAtOpen)) {
            return Box::pin(async { Err(SourceError::Overwritten) });
        }
        if matches!(answer, Some(Answer::RefuseOpen)) {
            return Box::pin(async {
                Err(SourceError::Io(std::io::Error::other(
                    "the open could not complete",
                )))
            });
        }
        let gone_after = match answer {
            Some(Answer::GoneAfter(n)) => Some(n),
            _ => None,
        };
        let tag = format!("first-{}", file.path);
        let fut = self.inner.open(bucket, file, compression);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(ServiceTagReader {
                inner,
                tag,
                gone_after,
                handed: 0,
            }) as Box<dyn LineReader>)
        })
    }

    fn etag(
        &self,
        _bucket: &BucketRef<'_>,
        file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>> {
        let answer = match &file.etag {
            Some(listed) => listed.clone(),
            None => {
                self.etag_calls.fetch_add(1, Ordering::SeqCst);
                REPLACEMENT.to_owned()
            }
        };
        Box::pin(async move { Ok(answer) })
    }

    fn line_count(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>> {
        self.inner.line_count(bucket, file, compression)
    }

    fn service_assigns_etags(&self) -> bool {
        true
    }
}

impl LineReader for ServiceTagReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if self.gone_after.is_some_and(|n| self.handed >= n) {
                return Err(SourceError::Io(std::io::Error::from(
                    std::io::ErrorKind::NotFound,
                )));
            }
            let line = self.inner.next_line(max_line_bytes).await;
            if matches!(&line, Ok(Some(_))) {
                self.handed += 1;
            }
            line
        })
    }

    fn bytes_consumed(&self) -> u64 {
        self.inner.bytes_consumed()
    }

    fn last_line_end(&self) -> u64 {
        self.inner.last_line_end()
    }

    fn etag(&self) -> Option<String> {
        self.inner.etag().map(|_| self.tag.clone())
    }

    fn opened_etag(&self) -> Option<String> {
        Some(self.tag.clone())
    }
}

/// Run one operation over `source` to its end and return the final record
/// and its manifest lines as `(path, status, etag)`.
async fn run_with(
    h: &Harness,
    source: Arc<ServiceTagSource>,
    storage: Option<Arc<dyn StorageEngine>>,
) -> (BulkOperationRecord, Vec<(String, String, String)>) {
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let mut deps = h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink()));
    if let Some(storage) = storage {
        deps.storage = storage;
    }
    runner::run(&deps, record).await.expect("runner run");
    let record = h.record(&op).await;
    let lines = h
        .read_manifest(&op)
        .expect("manifest written")
        .into_iter()
        .map(|line| {
            let arn = line["inputSourceArn"].as_str().unwrap_or_default();
            let path = arn.rsplit('/').next().unwrap_or_default().to_owned();
            (
                path,
                line["fileProcessingStatus"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                line["etag"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    (record, lines)
}

fn line(path: &str, status: &str, etag: &str) -> (String, String, String) {
    (path.to_owned(), status.to_owned(), etag.to_owned())
}

#[tokio::test]
async fn a_file_whose_open_accepted_no_answer_carries_no_tag() {
    // The second file was replaced by a longer object before its open and
    // the whole answer is found at another size (the S3 store reports it
    // overwritten); the third file's open failed. The run accepted no
    // answer for either.
    let h = Harness::new().await;
    h.write_input("0.json", &file_of(&plain_rows(3)));
    h.write_input("1.json", &file_of(&(3..6).map(row).collect::<Vec<_>>()));
    h.write_input("2.json", &file_of(&(6..9).map(row).collect::<Vec<_>>()));
    let source = Arc::new(ServiceTagSource::new(
        &h,
        vec![
            ("1.json", Answer::OverwrittenAtOpen),
            ("2.json", Answer::RefuseOpen),
        ],
    ));
    let calls = Arc::clone(&source.etag_calls);
    let (record, lines) = run_with(&h, source, None).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        lines,
        vec![
            line("0.json", "FULLY_PROCESSED", "first-0.json"),
            line("1.json", "PARTIALLY_PROCESSED", ""),
            line("2.json", "NOT_PROCESSED", ""),
        ]
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the manifest never asks for the object now at the key"
    );
}

#[tokio::test]
async fn a_file_whose_read_failed_mid_file_carries_its_first_answers_tag() {
    // The first file's read failed after three lines, as a replaced or
    // deleted object does; the next file was never reached.
    let h = Harness::new().await;
    h.write_input("0.json", &file_of(&plain_rows(6)));
    h.write_input("1.json", &file_of(&(6..9).map(row).collect::<Vec<_>>()));
    let source = Arc::new(ServiceTagSource::new(
        &h,
        vec![("0.json", Answer::GoneAfter(3))],
    ));
    let calls = Arc::clone(&source.etag_calls);
    let (record, lines) = run_with(&h, source, None).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        lines,
        vec![
            line("0.json", "NOT_AVAILABLE", "first-0.json"),
            line("1.json", "NOT_PROCESSED", ""),
        ]
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the manifest never asks for the object now at the key"
    );
}

#[tokio::test]
async fn a_file_whose_first_batch_failed_keeps_its_first_answers_tag() {
    // A batch that fails ends the run with its state reverted to the last
    // checkpoint, here before the file's first row, so the file reads as
    // not processed; the tag accepted at its open is kept through the
    // revert.
    let h = Harness::new().await;
    h.write_input("0.json", &file_of(&plain_rows(6)));
    let source = Arc::new(ServiceTagSource::new(&h, Vec::new()));
    let calls = Arc::clone(&source.etag_calls);
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_put_pk = Some("k3".to_owned());
    faulting.fail_put_pk_internal = Some("SQLSTATE 23505: duplicate key".to_owned());
    let (record, lines) = run_with(&h, source, Some(Arc::new(faulting))).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(lines, vec![line("0.json", "NOT_PROCESSED", "first-0.json")]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
