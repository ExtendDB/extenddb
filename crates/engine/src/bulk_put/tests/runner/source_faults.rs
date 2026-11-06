// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Test doubles around the filesystem source and sink. Each wraps the real
// implementation and adds one controlled behaviour at a chosen line index,
// so the interleavings the spec describes (a cancel landing mid-batch, a
// table deleted mid-run, a process dying between two persists) happen at a
// deterministic point in the row stream.

/// Hands out lines from the real reader until `after` lines have been
/// yielded, then signals `notify` and pends forever. Dropping the runner
/// future at that point models a process crash: everything checkpointed is
/// durable, everything since the last checkpoint is not.
pub(super) struct PendAfterSource {
    pub(super) inner: FsSource,
    pub(super) after: u64,
    pub(super) handed: Arc<AtomicU64>,
    pub(super) notify: Arc<tokio::sync::Notify>,
}

struct PendAfterReader {
    inner: Box<dyn LineReader>,
    after: u64,
    handed: Arc<AtomicU64>,
    notify: Arc<tokio::sync::Notify>,
}

impl BulkSource for PendAfterSource {
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
        let fut = self.inner.open(bucket, file, compression);
        let after = self.after;
        let handed = Arc::clone(&self.handed);
        let notify = Arc::clone(&self.notify);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(PendAfterReader {
                inner,
                after,
                handed,
                notify,
            }) as Box<dyn LineReader>)
        })
    }
}

impl LineReader for PendAfterReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if self.handed.load(Ordering::SeqCst) >= self.after {
                self.notify.notify_one();
                std::future::pending::<()>().await;
            }
            let line = self.inner.next_line(max_line_bytes).await;
            if matches!(&line, Ok(Some(_))) {
                self.handed.fetch_add(1, Ordering::SeqCst);
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
        self.inner.etag()
    }
}

pub(super) type LineAction = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Runs `action` once, just before handing out line index `at`. The action
/// completes before the runner sees the line, so its effect (a cancel, a
/// table deletion) lands mid-batch at a fixed row.
pub(super) struct ActAtSource {
    pub(super) inner: FsSource,
    pub(super) at: u64,
    pub(super) handed: Arc<AtomicU64>,
    pub(super) fired: Arc<AtomicBool>,
    pub(super) action: LineAction,
}

struct ActAtReader {
    inner: Box<dyn LineReader>,
    at: u64,
    handed: Arc<AtomicU64>,
    fired: Arc<AtomicBool>,
    action: LineAction,
}

impl BulkSource for ActAtSource {
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
        let fut = self.inner.open(bucket, file, compression);
        let at = self.at;
        let handed = Arc::clone(&self.handed);
        let fired = Arc::clone(&self.fired);
        let action = Arc::clone(&self.action);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(ActAtReader {
                inner,
                at,
                handed,
                fired,
                action,
            }) as Box<dyn LineReader>)
        })
    }
}

impl LineReader for ActAtReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if self.handed.load(Ordering::SeqCst) >= self.at
                && !self.fired.swap(true, Ordering::SeqCst)
            {
                (self.action)().await;
            }
            let line = self.inner.next_line(max_line_bytes).await;
            if matches!(&line, Ok(Some(_))) {
                self.handed.fetch_add(1, Ordering::SeqCst);
            }
            line
        })
    }

    fn bytes_consumed(&self) -> u64 {
        self.inner.bytes_consumed()
    }

    fn etag(&self) -> Option<String> {
        self.inner.etag()
    }
}

/// Counts `list` and `open` calls, to prove a claimed CANCELLING operation
/// reads no input, and `line_count` and `etag` calls, to prove the
/// manifest reads no finished file a second time.
pub(super) struct CountingSource {
    pub(super) inner: FsSource,
    pub(super) lists: Arc<AtomicU64>,
    pub(super) opens: Arc<AtomicU64>,
    pub(super) counts: Arc<AtomicU64>,
    pub(super) etags: Arc<AtomicU64>,
}

impl CountingSource {
    pub(super) fn new(inner: FsSource) -> Self {
        Self {
            inner,
            lists: Arc::new(AtomicU64::new(0)),
            opens: Arc::new(AtomicU64::new(0)),
            counts: Arc::new(AtomicU64::new(0)),
            etags: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl BulkSource for CountingSource {
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
        self.etags.fetch_add(1, Ordering::SeqCst);
        self.inner.etag(bucket, file)
    }

    fn line_count(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>> {
        self.counts.fetch_add(1, Ordering::SeqCst);
        self.inner.line_count(bucket, file, compression)
    }

    fn list(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
        max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list(bucket, prefix, max_files)
    }

    fn open(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.open(bucket, file, compression)
    }
}

/// Serves lines from the real reader until `at` lines have been handed
/// out, then returns `SourceError::Io` on every later call. On a resume
/// whose cursor is above `at`, the fault fires inside the skip region: a
/// read error that is not a changed-file signal must fail the operation,
/// not abandon the file the way an early end of file or an over-cap line
/// does.
pub(super) struct IoAtSource {
    pub(super) inner: FsSource,
    pub(super) at: u64,
    pub(super) handed: Arc<AtomicU64>,
}

struct IoAtReader {
    inner: Box<dyn LineReader>,
    at: u64,
    handed: Arc<AtomicU64>,
}

impl BulkSource for IoAtSource {
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
        let fut = self.inner.open(bucket, file, compression);
        let at = self.at;
        let handed = Arc::clone(&self.handed);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(IoAtReader { inner, at, handed }) as Box<dyn LineReader>)
        })
    }
}

impl LineReader for IoAtReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if self.handed.load(Ordering::SeqCst) >= self.at {
                return Err(SourceError::Io(std::io::Error::other(
                    "injected read error inside the covered region",
                )));
            }
            let line = self.inner.next_line(max_line_bytes).await;
            if matches!(&line, Ok(Some(_))) {
                self.handed.fetch_add(1, Ordering::SeqCst);
            }
            line
        })
    }

    fn bytes_consumed(&self) -> u64 {
        self.inner.bytes_consumed()
    }

    fn etag(&self) -> Option<String> {
        self.inner.etag()
    }
}

/// Delegates `open` until `pend_at` opens have been requested, then signals
/// `notify` and pends inside that open. Dropping the runner there models a
/// crash whose last committed state is the previous file's end-of-file
/// checkpoint: the cursor already points at the next file.
pub(super) struct PendAtOpenSource {
    pub(super) inner: FsSource,
    pub(super) pend_at: u64,
    pub(super) opens: Arc<AtomicU64>,
    pub(super) notify: Arc<tokio::sync::Notify>,
}

impl BulkSource for PendAtOpenSource {
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
        let count = self.opens.fetch_add(1, Ordering::SeqCst) + 1;
        if count >= self.pend_at {
            let notify = Arc::clone(&self.notify);
            return Box::pin(async move {
                notify.notify_one();
                std::future::pending::<()>().await;
                unreachable!("pending never resolves")
            });
        }
        self.inner.open(bucket, file, compression)
    }
}
