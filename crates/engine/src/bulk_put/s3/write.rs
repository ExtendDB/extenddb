// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Report objects on the S3 store. Pages are gzip-encoded on the blocking
//! pool into the open object's part buffer; each full 8 MiB part is sent
//! with UploadPart on the object's multipart upload, which starts with the
//! first full part. A page that takes the object's compressed bytes to or
//! past the rotation threshold (200 MiB, as on the filesystem store and the
//! service) closes the object, and the next page opens
//! `output-0000N.json.gz` with N incremented. `finish` completes the
//! uploads in index order, or sends a single PutObject for an object that
//! never filled a part, so no object is visible before `finish`. A writer
//! dropped without `finish` aborts every upload it started. Every request
//! carries the expected owner, and the encryption fields go on
//! CreateMultipartUpload and PutObject.

use std::io::Write;
use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::Value;

use extenddb_storage::BoxedFuture as BoxFuture;
use extenddb_storage::ReportKind;

use super::client::Phase;
use super::error::Tail;
use super::probe::{abort, sink_error, sse_fields};
use super::retry::is_transient;
use super::{Location, S3Clients};
use crate::bulk_put::report;
use crate::bulk_put::source::{OutputSse, ROTATION_THRESHOLD_BYTES, ReportWriter, SinkError};

/// The compressed bytes of one object as the encoder writes them, and the
/// count written so far, uploaded parts included.
#[derive(Default)]
struct PartBuffer {
    bytes: Vec<u8>,
    written: u64,
}

impl Write for PartBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(buf);
        self.written = self.written.saturating_add(buf.len() as u64);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A started multipart upload and the parts it holds.
struct Upload {
    id: String,
    parts: Vec<CompletedPart>,
    /// The bytes of the parts uploaded, the size of the completed object.
    bytes: u64,
}

/// The object pages are being written to.
struct OpenObject {
    key: String,
    encoder: GzEncoder<PartBuffer>,
    upload: Option<Upload>,
}

/// An object whose bytes are all written or uploaded, waiting for
/// `finish`.
enum Closed {
    Multipart { key: String, upload: Upload },
    Single { key: String, body: Vec<u8> },
}

/// The report writer of one report kind on the S3 store.
pub(super) struct S3ReportWriter {
    clients: Arc<S3Clients>,
    location: Location,
    prefix: String,
    operation_id: String,
    kind: ReportKind,
    sse: OutputSse,
    threshold: u64,
    open: Option<OpenObject>,
    closed: Vec<Closed>,
    /// Every upload started and not yet completed, as (key, upload id), so
    /// a writer dropped without `finish` can abort them.
    started: Vec<(String, String)>,
    /// A page or `finish` failed: nothing more is written.
    failed: bool,
}

fn join_error(error: tokio::task::JoinError) -> SinkError {
    SinkError::Io(std::io::Error::other(error))
}

impl S3ReportWriter {
    pub(super) fn new(
        clients: Arc<S3Clients>,
        location: Location,
        prefix: &str,
        operation_id: &str,
        kind: ReportKind,
        sse: &OutputSse,
    ) -> Self {
        Self {
            clients,
            location,
            prefix: prefix.to_owned(),
            operation_id: operation_id.to_owned(),
            kind,
            sse: sse.clone(),
            threshold: ROTATION_THRESHOLD_BYTES,
            open: None,
            closed: Vec::new(),
            started: Vec::new(),
            failed: false,
        }
    }

    fn key(&self, index: usize) -> String {
        report::report_key(
            &self.prefix,
            &self.location.account,
            &self.operation_id,
            self.kind,
            u32::try_from(index).unwrap_or(u32::MAX),
        )
    }

    fn new_object(&self) -> OpenObject {
        OpenObject {
            key: self.key(self.closed.len()),
            encoder: GzEncoder::new(PartBuffer::default(), Compression::default()),
            upload: None,
        }
    }

    /// Upload one part of the object at `key`, starting its multipart
    /// upload first when this is its first part.
    async fn upload_part(
        &mut self,
        key: &str,
        upload: &mut Option<Upload>,
        body: Vec<u8>,
    ) -> Result<(), SinkError> {
        let checksum =
            (!self.clients.checksums_when_required()).then_some(ChecksumAlgorithm::Crc32);
        let location = &self.location;
        if upload.is_none() {
            let (encryption, kms_key) = sse_fields(&self.sse);
            let created = self
                .clients
                .call(&location.bucket, |client| {
                    client
                        .create_multipart_upload()
                        .bucket(&location.bucket)
                        .key(key)
                        .set_expected_bucket_owner(location.owner.clone())
                        .set_server_side_encryption(encryption.clone())
                        .set_ssekms_key_id(kms_key.clone())
                        .set_checksum_algorithm(checksum.clone())
                        .send()
                })
                .await
                .map_err(|error| sink_error(&error, location, key, Tail::S3, "report write"))?;
            let id = created.upload_id().unwrap_or_default().to_owned();
            self.started.push((key.to_owned(), id.clone()));
            *upload = Some(Upload {
                id,
                parts: Vec::new(),
                bytes: 0,
            });
        }
        let Some(upload) = upload.as_mut() else {
            return Ok(());
        };
        let number = i32::try_from(upload.parts.len() + 1).unwrap_or(i32::MAX);
        let id = upload.id.clone();
        let size = body.len() as u64;
        let uploaded = self
            .clients
            .call(&location.bucket, |client| {
                client
                    .upload_part()
                    .bucket(&location.bucket)
                    .key(key)
                    .upload_id(&id)
                    .part_number(number)
                    .body(ByteStream::from(body.clone()))
                    .set_expected_bucket_owner(location.owner.clone())
                    .set_checksum_algorithm(checksum.clone())
                    .send()
            })
            .await
            .map_err(|error| sink_error(&error, location, key, Tail::S3, "report write"))?;
        upload.parts.push(
            CompletedPart::builder()
                .part_number(number)
                .set_e_tag(uploaded.e_tag().map(str::to_owned))
                .set_checksum_crc32(uploaded.checksum_crc32().map(str::to_owned))
                .build(),
        );
        upload.bytes = upload.bytes.saturating_add(size);
        Ok(())
    }

    /// Send every full part the open object's buffer holds.
    async fn upload_full_parts(&mut self, object: &mut OpenObject) -> Result<(), SinkError> {
        let part_bytes = usize::try_from(self.clients.part_bytes()).unwrap_or(usize::MAX);
        while object.encoder.get_ref().bytes.len() >= part_bytes {
            let body: Vec<u8> = object.encoder.get_mut().bytes.drain(..part_bytes).collect();
            self.upload_part(&object.key, &mut object.upload, body)
                .await?;
        }
        Ok(())
    }

    /// Finish `object`'s gzip stream and send what is left of it: its last
    /// part when it has an upload, otherwise keep the whole body for one
    /// PutObject at `finish`.
    async fn close(&mut self, object: OpenObject) -> Result<Closed, SinkError> {
        let OpenObject {
            key,
            encoder,
            upload,
        } = object;
        let buffer = tokio::task::spawn_blocking(move || encoder.finish())
            .await
            .map_err(join_error)?
            .map_err(SinkError::Io)?;
        match upload {
            None => Ok(Closed::Single {
                key,
                body: buffer.bytes,
            }),
            Some(upload) => {
                let mut upload = Some(upload);
                // Full parts first, so every part but the last is one part
                // size, then the rest.
                let part_bytes = usize::try_from(self.clients.part_bytes()).unwrap_or(usize::MAX);
                let mut rest = buffer.bytes;
                while rest.len() > part_bytes {
                    let body: Vec<u8> = rest.drain(..part_bytes).collect();
                    self.upload_part(&key, &mut upload, body).await?;
                }
                if !rest.is_empty() {
                    self.upload_part(&key, &mut upload, rest).await?;
                }
                let upload = upload.unwrap_or(Upload {
                    id: String::new(),
                    parts: Vec::new(),
                    bytes: 0,
                });
                Ok(Closed::Multipart { key, upload })
            }
        }
    }

    async fn write(&mut self, records: Vec<Value>) -> Result<(), SinkError> {
        let object = match self.open.take() {
            Some(object) => object,
            None => self.new_object(),
        };
        let threshold = self.threshold;
        let (mut object, roll) = tokio::task::spawn_blocking(move || {
            let mut object = object;
            report::encode_ndjson_page(&mut object.encoder, &records).map(|()| {
                let roll = object.encoder.get_ref().written >= threshold;
                (object, roll)
            })
        })
        .await
        .map_err(join_error)?
        .map_err(SinkError::Io)?;
        self.upload_full_parts(&mut object).await?;
        if roll {
            let closed = self.close(object).await?;
            self.closed.push(closed);
        } else {
            self.open = Some(object);
        }
        Ok(())
    }

    async fn complete(&mut self) -> Result<String, SinkError> {
        // A writer that was given no page still writes its first object,
        // an empty gzip stream, as the filesystem store does.
        if self.open.is_none() && self.closed.is_empty() {
            self.open = Some(self.new_object());
        }
        if let Some(object) = self.open.take() {
            let closed = self.close(object).await?;
            self.closed.push(closed);
        }
        let (encryption, kms_key) = sse_fields(&self.sse);
        let location = self.location.clone();
        for object in std::mem::take(&mut self.closed) {
            match object {
                Closed::Multipart { key, upload } => {
                    let parts = CompletedMultipartUpload::builder()
                        .set_parts(Some(upload.parts))
                        .build();
                    let completed = self
                        .clients
                        .call(&location.bucket, |client| {
                            client
                                .complete_multipart_upload()
                                .bucket(&location.bucket)
                                .key(&key)
                                .upload_id(&upload.id)
                                .multipart_upload(parts.clone())
                                .set_expected_bucket_owner(location.owner.clone())
                                .send()
                        })
                        .await;
                    if let Err(error) = completed
                        && !self.completed_before(&error, &key, upload.bytes).await?
                    {
                        return Err(sink_error(
                            &error,
                            &location,
                            &key,
                            Tail::S3,
                            "report write",
                        ));
                    }
                    self.started.retain(|(_, id)| *id != upload.id);
                    self.audit(&key);
                }
                Closed::Single { key, body } => {
                    self.clients
                        .call(&location.bucket, |client| {
                            client
                                .put_object()
                                .bucket(&location.bucket)
                                .key(&key)
                                .body(ByteStream::from(body.clone()))
                                .set_expected_bucket_owner(location.owner.clone())
                                .set_server_side_encryption(encryption.clone())
                                .set_ssekms_key_id(kms_key.clone())
                                .send()
                        })
                        .await
                        .map_err(|e| sink_error(&e, &location, &key, Tail::S3, "report write"))?;
                    self.audit(&key);
                }
            }
        }
        Ok(self.key(0))
    }

    /// The audit event the import and export operations emit, once per
    /// report object installed. Keys stay in the log.
    fn audit(&self, key: &str) {
        tracing::info!(
            account_id = %self.location.account,
            operation = "StartBulkPutItems",
            object = %format!("s3://{}/{key}", self.location.bucket),
            "import/export file access"
        );
    }

    /// Whether a `CompleteMultipartUpload` that failed with `error` had in
    /// fact completed. When the answer to a completion that landed is lost,
    /// the request is sent again and S3 answers `NoSuchUpload`, because the
    /// upload is gone once it completes; the object is then at `key`. The
    /// key carries the operation and the object index, but an earlier run
    /// of the same operation or another writer can have put an object
    /// there, so the object counts as this upload's only when its length is
    /// `bytes`, the bytes of the parts this upload sent. A `HeadObject` that
    /// still fails transiently at the end of its retries is the server's
    /// failure, as for every other call; a 404, a 403 (no `s3:GetObject`)
    /// or any other client error leaves the `NoSuchUpload` to surface.
    async fn completed_before<E>(
        &self,
        error: &aws_sdk_s3::error::SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
        key: &str,
        bytes: u64,
    ) -> Result<bool, SinkError>
    where
        E: aws_sdk_s3::error::ProvideErrorMetadata,
    {
        use aws_sdk_s3::error::ProvideErrorMetadata as _;
        if error.code() != Some("NoSuchUpload") {
            return Ok(false);
        }
        let location = &self.location;
        let head = match self
            .clients
            .call(&location.bucket, |client| {
                client
                    .head_object()
                    .bucket(&location.bucket)
                    .key(key)
                    .set_expected_bucket_owner(location.owner.clone())
                    .send()
            })
            .await
        {
            Ok(head) => head,
            Err(head_error) if is_transient(&head_error) => {
                return Err(sink_error(
                    &head_error,
                    location,
                    key,
                    Tail::S3,
                    "report completion check",
                ));
            }
            Err(_) => return Ok(false),
        };
        let length = head.content_length();
        if length.and_then(|length| u64::try_from(length).ok()) != Some(bytes) {
            tracing::warn!(
                account_id = %location.account,
                bucket = %location.bucket,
                key = %key,
                object_bytes = length.unwrap_or(-1),
                uploaded_bytes = bytes,
                "S3 report upload completion was answered NoSuchUpload and the object at the key is not the size this upload wrote"
            );
            return Ok(false);
        }
        tracing::warn!(
            account_id = %location.account,
            bucket = %location.bucket,
            key = %key,
            "S3 report upload completion was answered NoSuchUpload and the object is in place; the earlier completion landed"
        );
        Ok(true)
    }
}

impl ReportWriter for S3ReportWriter {
    fn write_page(&mut self, records: Vec<Value>) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            if records.is_empty() {
                return Ok(());
            }
            if self.failed {
                return Err(SinkError::Io(std::io::Error::other(
                    "report writer already failed",
                )));
            }
            let written = self.write(records).await;
            self.failed = written.is_err();
            written
        })
    }

    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<String, SinkError>> {
        Box::pin(async move {
            let mut this = self;
            if this.failed {
                return Err(SinkError::Io(std::io::Error::other(
                    "report writer already failed",
                )));
            }
            // On an error the writer is dropped here, which aborts the
            // uploads not yet completed.
            this.complete().await
        })
    }

    fn object_index(&self) -> u32 {
        // The open object was created as index `closed.len()`, and once it
        // rolls into `closed` the next object takes that index.
        u32::try_from(self.closed.len()).unwrap_or(u32::MAX)
    }

    #[cfg(test)]
    fn set_rotation_threshold(&mut self, bytes: u64) {
        self.threshold = bytes;
    }

    #[cfg(test)]
    fn open_object_bytes(&self) -> u64 {
        self.open
            .as_ref()
            .map_or(0, |object| object.encoder.get_ref().written)
    }
}

impl Drop for S3ReportWriter {
    fn drop(&mut self) {
        if self.started.is_empty() {
            return;
        }
        let started = std::mem::take(&mut self.started);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                account_id = %self.location.account,
                bucket = %self.location.bucket,
                uploads = started.len(),
                "S3 multipart uploads left for the bucket's lifecycle rule: no runtime to abort them"
            );
            return;
        };
        let clients = Arc::clone(&self.clients);
        let location = self.location.clone();
        runtime.spawn(async move {
            for (key, id) in started {
                abort(&clients, &location, &key, &id, Phase::Run).await;
            }
        });
    }
}
