// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The S3 store: inputs listed and read from, and reports written to, the
//! buckets the requests name, in AWS S3 or an S3-compatible service, with
//! the server's own credentials. A request's `S3BucketOwner`, when it names
//! an account other than the caller's, travels as
//! `x-amz-expected-bucket-owner` on every request, and S3 decides. S3's
//! answers are rendered for the client by [`error`]; the decode chain is
//! the one the filesystem store reads through, fed by a reader over ranged
//! GETs ([`read`]).

mod client;
mod error;
mod list;
mod manifest;
mod probe;
mod read;
mod write;

#[cfg(test)]
pub(crate) mod tests;

use std::sync::Arc;

use extenddb_s3::retry;
use serde_json::Value;

use extenddb_storage::BoxedFuture as BoxFuture;
use extenddb_storage::{InputFile, ReportKind};

use crate::bulk_put::source::{
    BucketRef, BulkSink, BulkSource, InputCodec, LineReader, OutputSse, ReportWriter, SinkError,
    SourceError, SourceFile, StoreUnavailable,
};

use client::Phase;
#[cfg(test)]
pub(crate) use client::connect_with;
pub(crate) use client::{S3Clients, connect};
use error::{Outcome, Tail};

/// The last segment of the manifest's key (`report::manifest_key`). The
/// service writes the manifest as it reads its input, so a check of the
/// manifest key carries that call's tail; a report key carries the report
/// writer's.
const MANIFEST_SUFFIX: &str = "/manifest-files.json";

/// The key a request for `key` names in the store: on AWS S3 the key as
/// given; against a configured endpoint the key with empty and `.` segments
/// removed and `..` resolved, as S3-compatible services with a file backend
/// store it. The clients send checksums only when required exactly when
/// the store has a configured endpoint (`connect`).
fn key_in_store(clients: &S3Clients, key: &str) -> String {
    if clients.checksums_when_required() {
        crate::bulk_put::source::stored_form(key, true)
    } else {
        key.to_owned()
    }
}

/// One bucket as the S3 calls name it, owned so a call's future can hold
/// it.
#[derive(Debug, Clone)]
pub(crate) struct Location {
    /// The caller's account, for the log lines.
    pub(crate) account: String,
    pub(crate) bucket: String,
    /// The `x-amz-expected-bucket-owner` value, when one is sent.
    pub(crate) owner: Option<String>,
}

impl Location {
    pub(crate) fn of(at: &BucketRef<'_>) -> Self {
        Self {
            account: at.account.to_owned(),
            bucket: at.bucket.to_owned(),
            owner: at.expected_owner.map(str::to_owned),
        }
    }
}

/// An S3 `ETag` without its quotes, as the manifest records it; a
/// multipart object's `{md5}-{parts}` form is kept.
pub(crate) fn unquoted(etag: &str) -> String {
    etag.trim_matches('"').to_owned()
}

/// A failed input-side call as a [`SourceError`]: a rendered fault, or an
/// I/O error of kind `Other` for the server's own failures, which the
/// runner answers with `InternalServerError`, carrying a
/// [`StoreUnavailable`] when the failure is transient and outlasted the
/// retries, which the runner resumes after. The detail is logged here.
fn source_error<E>(
    error: &aws_sdk_s3::error::SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
    location: &Location,
    key: &str,
    after_first_byte: bool,
    what: &'static str,
) -> SourceError
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match error::classify(error, Tail::AmazonS3, after_first_byte) {
        Outcome::Fault(fault) => {
            tracing::info!(
                account_id = %location.account,
                bucket = %location.bucket,
                key,
                code = fault.code,
                message = %fault.message,
                "S3 {what} failed"
            );
            SourceError::Store(fault)
        }
        Outcome::PastEnd | Outcome::Internal => {
            log_internal(error, location, key, what);
            // A transient failure here outlasted the store's retries.
            if retry::is_transient(error) {
                SourceError::Io(StoreUnavailable::io(format!("S3 {what} failed")))
            } else {
                SourceError::Io(std::io::Error::other(format!("S3 {what} failed")))
            }
        }
    }
}

/// The ERROR line for an S3 failure that is the server's: S3's code and
/// message when S3 answered, and a line of its own when the code is about
/// the server's own credentials, account or clock.
pub(super) fn log_internal<E>(
    error: &aws_sdk_s3::error::SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
    location: &Location,
    key: &str,
    what: &'static str,
) where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    use aws_sdk_s3::error::ProvideErrorMetadata;
    let s3_code = error.code().unwrap_or_default();
    let s3_message = error.message().unwrap_or_default();
    if extenddb_s3::is_server_credential_error(error) {
        tracing::error!(
            account_id = %location.account,
            bucket = %location.bucket,
            key,
            s3_code,
            s3_message,
            "S3 {what} failed: S3 refused the server's own credentials, account or clock; \
             the caller is answered InternalServerError"
        );
    } else {
        tracing::error!(
            account_id = %location.account,
            bucket = %location.bucket,
            key,
            s3_code,
            s3_message,
            error = %aws_sdk_s3::error::DisplayErrorContext(error),
            "S3 {what} failed"
        );
    }
}

/// The input side of the S3 store.
pub(crate) struct S3Source {
    pub(crate) clients: Arc<S3Clients>,
}

impl BulkSource for S3Source {
    fn check_bucket(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>> {
        let location = Location::of(at);
        let prefix = prefix.to_owned();
        Box::pin(async move { list::check_bucket(&self.clients, &location, &prefix).await })
    }

    fn list(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        let location = Location::of(at);
        let prefix = prefix.to_owned();
        Box::pin(async move { list::list(&self.clients, &location, &prefix, max_files).await })
    }

    fn open(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        let location = Location::of(at);
        let file = file.clone();
        Box::pin(async move {
            let reader =
                read::open(Arc::clone(&self.clients), location, &file, compression).await?;
            Ok(Box::new(reader) as Box<dyn LineReader>)
        })
    }

    fn etag(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>> {
        let location = Location::of(at);
        let file = file.clone();
        Box::pin(async move { read::etag(&self.clients, &location, &file).await })
    }

    fn line_count(
        &self,
        at: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>> {
        let location = Location::of(at);
        let file = file.clone();
        Box::pin(async move {
            read::line_count(Arc::clone(&self.clients), location, &file, compression).await
        })
    }

    fn listed_prefix(&self, prefix: &str) -> String {
        key_in_store(&self.clients, prefix)
    }

    fn service_assigns_etags(&self) -> bool {
        true
    }
}

/// The output side of the S3 store.
pub(crate) struct S3Sink {
    pub(crate) clients: Arc<S3Clients>,
}

impl BulkSink for S3Sink {
    fn check_location(
        &self,
        at: &BucketRef<'_>,
        key: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<(), SinkError>> {
        let location = Location::of(at);
        let key = key.to_owned();
        let sse = sse.clone();
        let tail = if key.ends_with(MANIFEST_SUFFIX) {
            Tail::AmazonS3
        } else {
            Tail::S3
        };
        Box::pin(async move {
            probe::probe(&self.clients, &location, &key, &sse, tail, Phase::Run).await
        })
    }

    fn check_encryption(
        &self,
        at: &BucketRef<'_>,
        key: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<(), SinkError>> {
        let location = Location::of(at);
        let key = key.to_owned();
        let sse = sse.clone();
        Box::pin(async move {
            // Only an SSE-KMS key is checked at Start (measured on the
            // service: a disabled key and a key of the wrong usage fail
            // the operation before the input walk). A bucket or owner fault
            // the same check meets is left to the first report record, as
            // the service leaves it.
            if !matches!(sse, OutputSse::Kms { .. }) {
                return Ok(());
            }
            let checked = probe::probe(
                &self.clients,
                &location,
                &key,
                &sse,
                Tail::AmazonS3,
                Phase::Start,
            );
            match checked.await {
                Err(SinkError::Store(fault)) if !fault.code.starts_with("Kms") => Ok(()),
                other => other,
            }
        })
    }

    fn open(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        kind: ReportKind,
        _owner: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<Box<dyn ReportWriter>, SinkError>> {
        let writer = write::S3ReportWriter::new(
            Arc::clone(&self.clients),
            Location::of(at),
            prefix,
            operation_id,
            kind,
            sse,
        );
        Box::pin(async move { Ok(Box::new(writer) as Box<dyn ReportWriter>) })
    }

    fn write_manifest(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        _owner: &str,
        sse: &OutputSse,
        lines: Vec<Value>,
    ) -> BoxFuture<'_, Result<String, SinkError>> {
        let location = Location::of(at);
        let prefix = prefix.to_owned();
        let operation_id = operation_id.to_owned();
        let sse = sse.clone();
        Box::pin(async move {
            manifest::write_manifest(
                &self.clients,
                &location,
                &prefix,
                &operation_id,
                &sse,
                &lines,
            )
            .await
        })
    }

    fn stored_key(&self, key: &str) -> String {
        key_in_store(&self.clients, key)
    }
}
