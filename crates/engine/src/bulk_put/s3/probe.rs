// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The output side's shared pieces: the encryption fields a write sends,
//! the rendering of a failed write, and the write check. The check starts
//! a multipart upload on the key the operation is about to write, with the
//! expected owner and the encryption fields, and aborts it at once, so S3
//! answers a missing bucket, an owner mismatch, an `s3:PutObject` denial
//! and an unusable KMS key with its own code and message, and nothing is
//! written.

use aws_sdk_s3::types::ServerSideEncryption;

use super::client::Phase;
use super::error::{Outcome, Tail, classify};
use super::{Location, S3Clients};
use crate::bulk_put::source::{OutputSse, SinkError, StoreUnavailable};

/// The `x-amz-server-side-encryption` and
/// `x-amz-server-side-encryption-aws-kms-key-id` values for `sse`.
pub(super) fn sse_fields(sse: &OutputSse) -> (Option<ServerSideEncryption>, Option<String>) {
    match sse {
        OutputSse::None => (None, None),
        OutputSse::Aes256 => (Some(ServerSideEncryption::Aes256), None),
        OutputSse::Kms { key_id } => (Some(ServerSideEncryption::AwsKms), key_id.clone()),
    }
}

/// A failed output-side call as a [`SinkError`]: a rendered fault with
/// `tail`, or an I/O error of kind `Other` for the server's own failures,
/// which the runner answers with `InternalServerError`, carrying a
/// [`StoreUnavailable`] when the failure is transient and outlasted the
/// retries, which the runner resumes after. The detail is logged here.
pub(super) fn sink_error<E>(
    error: &aws_sdk_s3::error::SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
    location: &Location,
    key: &str,
    tail: Tail,
    what: &'static str,
) -> SinkError
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match classify(error, tail, false) {
        Outcome::Fault(fault) => {
            tracing::info!(
                account_id = %location.account,
                bucket = %location.bucket,
                key,
                code = fault.code,
                message = %fault.message,
                "S3 {what} failed"
            );
            SinkError::Store(fault)
        }
        Outcome::PastEnd | Outcome::Internal => {
            super::log_internal(error, location, key, what);
            // A transient failure here outlasted the store's retries.
            if super::retry::is_transient(error) {
                SinkError::Io(StoreUnavailable::io(format!("S3 {what} failed")))
            } else {
                SinkError::Io(std::io::Error::other(format!("S3 {what} failed")))
            }
        }
    }
}

/// The write check for `key`: CreateMultipartUpload with the owner and
/// encryption fields, then AbortMultipartUpload, both under the bound of
/// `phase`.
pub(super) async fn probe(
    clients: &S3Clients,
    location: &Location,
    key: &str,
    sse: &OutputSse,
    tail: Tail,
    phase: Phase,
) -> Result<(), SinkError> {
    let (encryption, kms_key) = sse_fields(sse);
    let created = clients
        .call_in(phase, &location.bucket, |client| {
            client
                .create_multipart_upload()
                .bucket(&location.bucket)
                .key(key)
                .set_expected_bucket_owner(location.owner.clone())
                .set_server_side_encryption(encryption.clone())
                .set_ssekms_key_id(kms_key.clone())
                .send()
        })
        .await
        .map_err(|error| sink_error(&error, location, key, tail, "write check"))?;
    if let Some(upload_id) = created.upload_id() {
        abort(clients, location, key, upload_id, phase).await;
    }
    Ok(())
}

/// Abort one multipart upload under the bound of `phase`. A failure
/// leaves the upload to the bucket's lifecycle rule; it is logged, never
/// the operation's fault.
pub(super) async fn abort(
    clients: &S3Clients,
    location: &Location,
    key: &str,
    upload_id: &str,
    phase: Phase,
) {
    let aborted = clients
        .call_in(phase, &location.bucket, |client| {
            client
                .abort_multipart_upload()
                .bucket(&location.bucket)
                .key(key)
                .upload_id(upload_id)
                .set_expected_bucket_owner(location.owner.clone())
                .send()
        })
        .await;
    if let Err(error) = aborted {
        tracing::warn!(
            account_id = %location.account,
            bucket = %location.bucket,
            key,
            error = %aws_sdk_s3::error::DisplayErrorContext(&error),
            "S3 multipart upload could not be aborted; a lifecycle rule removes it"
        );
    }
}
