// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! S3-backed store built on the official `aws-sdk-s3` crate.
//!
//! Credentials come from the standard AWS SDK chain (environment, shared
//! config, IMDS, IRSA, container credentials); the store never holds them
//! itself. Uploads larger than the configured part size use multipart, and a
//! failed multipart upload is aborted so no orphaned parts accrue storage.
//! Listing paginates; prefix deletion batches `DeleteObjects` requests.

use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};

use crate::config::S3StoreConfig;
use crate::key::{key_matches_prefix, normalize_prefix, validate_key, validate_prefix};
use crate::{BackupStore, ByteStream, ObjectMeta, StoreError};

/// The smallest part size S3 accepts for any part except the last.
pub const MIN_PART_SIZE_BYTES: u64 = 5 * 1024 * 1024;

/// Largest number of keys one `DeleteObjects` request accepts.
const DELETE_BATCH: usize = 1000;

/// Store writing objects to an S3 bucket (or an S3-compatible endpoint).
pub struct S3Store {
    client: aws_sdk_s3::Client,
    bucket: String,
    /// Normalized bucket-side key prefix: empty, or ending in `/`.
    prefix: String,
    part_size: usize,
    max_concurrent_uploads: usize,
}

impl S3Store {
    /// Open a store against the configured bucket.
    ///
    /// Enforces the part size floor and resolves credentials, region, and
    /// retry behavior through the SDK default chain. `endpoint` and
    /// `force_path_style` support S3-compatible stores such as `MinIO`.
    ///
    /// # Errors
    ///
    /// Returns an error when the part size is below the 5 MiB floor, the
    /// concurrency is zero, or the configured prefix fails key validation.
    pub async fn open(config: &S3StoreConfig) -> Result<Self, StoreError> {
        Self::open_inner(config, None).await
    }

    /// Open with an additional client interceptor. This is the failure
    /// injection seam the test suite uses to make a chosen operation fail;
    /// it is not part of the supported API surface.
    #[doc(hidden)]
    pub async fn open_with_interceptor(
        config: &S3StoreConfig,
        interceptor: impl aws_sdk_s3::config::Intercept + 'static,
    ) -> Result<Self, StoreError> {
        Self::open_inner(
            config,
            Some(aws_sdk_s3::config::SharedInterceptor::new(interceptor)),
        )
        .await
    }

    async fn open_inner(
        config: &S3StoreConfig,
        interceptor: Option<aws_sdk_s3::config::SharedInterceptor>,
    ) -> Result<Self, StoreError> {
        if config.upload_part_size_bytes < MIN_PART_SIZE_BYTES {
            return Err(StoreError::Other(format!(
                "upload_part_size_bytes is {}; the minimum is {MIN_PART_SIZE_BYTES} (5 MiB)",
                config.upload_part_size_bytes
            )));
        }
        if config.max_concurrent_uploads == 0 {
            return Err(StoreError::Other(
                "max_concurrent_uploads must be at least 1".to_owned(),
            ));
        }
        let part_size = usize::try_from(config.upload_part_size_bytes).map_err(|_| {
            StoreError::Other(format!(
                "upload_part_size_bytes {} does not fit this platform",
                config.upload_part_size_bytes
            ))
        })?;
        let prefix = normalize_store_prefix(&config.prefix)?;

        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &config.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &config.endpoint {
            loader = loader.endpoint_url(endpoint.clone());
        }
        let shared = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared);
        if config.force_path_style {
            builder = builder.force_path_style(true);
        }
        if let Some(interceptor) = interceptor {
            builder.push_interceptor(interceptor);
        }
        let client = aws_sdk_s3::Client::from_conf(builder.build());

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix,
            part_size,
            max_concurrent_uploads: config.max_concurrent_uploads,
        })
    }

    /// Bucket-side key for a store key.
    fn full_key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    async fn put_impl(&self, key: &str, body: ByteStream) -> Result<(), StoreError> {
        validate_key(key)?;
        let full_key = self.full_key(key);
        let mut chunks = std::pin::pin!(chunk_stream(body, self.part_size));

        let first = match chunks.try_next().await? {
            Some(chunk) => chunk,
            None => Bytes::new(),
        };
        let second = chunks.try_next().await?;

        let Some(second) = second else {
            // The whole body fits in one part: a single PutObject.
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&full_key)
                .body(first.into())
                .send()
                .await
                .map_err(|e| map_sdk_error("PutObject", e))?;
            return Ok(());
        };

        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| map_sdk_error("CreateMultipartUpload", e))?
            .upload_id()
            .ok_or_else(|| {
                StoreError::Other("CreateMultipartUpload returned no upload id".to_owned())
            })?
            .to_owned();

        // Every failure after CreateMultipartUpload must reach the abort,
        // including a failed CompleteMultipartUpload, so no orphaned parts
        // accrue storage. A complete that failed after S3 committed it makes
        // the abort answer NoSuchUpload, which the best-effort warn tolerates.
        let outcome: Result<(), StoreError> = async {
            let parts = self
                .upload_parts(&full_key, &upload_id, first, second, chunks)
                .await?;
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&full_key)
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .map_err(|e| map_sdk_error("CompleteMultipartUpload", e))?;
            Ok(())
        }
        .await;

        if let Err(e) = outcome {
            // Best effort: the original failure is what the caller needs.
            if let Err(abort_err) = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&full_key)
                .upload_id(&upload_id)
                .send()
                .await
            {
                tracing::warn!(
                    key = %full_key,
                    error = %map_sdk_error("AbortMultipartUpload", abort_err),
                    "failed to abort multipart upload after a put failure"
                );
            }
            return Err(e);
        }
        Ok(())
    }

    /// Upload every part with bounded concurrency; returns the completed part
    /// list sorted by part number.
    async fn upload_parts(
        &self,
        full_key: &str,
        upload_id: &str,
        first: Bytes,
        second: Bytes,
        rest: impl futures::Stream<Item = Result<Bytes, StoreError>> + Send,
    ) -> Result<Vec<CompletedPart>, StoreError> {
        let all_chunks = futures::stream::iter([Ok(first), Ok(second)]).chain(rest);
        let mut parts: Vec<CompletedPart> = all_chunks
            .enumerate()
            .map(|(index, chunk)| async move {
                let chunk = chunk?;
                let part_number = i32::try_from(index + 1).map_err(|_| {
                    StoreError::Other("multipart upload exceeds the part number range".to_owned())
                })?;
                let output = self
                    .client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(full_key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(chunk.into())
                    .send()
                    .await
                    .map_err(|e| map_sdk_error("UploadPart", e))?;
                let e_tag = output.e_tag().ok_or_else(|| {
                    StoreError::Other("UploadPart response carried no ETag".to_owned())
                })?;
                Ok::<_, StoreError>(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(e_tag)
                        .build(),
                )
            })
            .buffered(self.max_concurrent_uploads)
            .try_collect()
            .await?;
        parts.sort_by_key(CompletedPart::part_number);
        Ok(parts)
    }

    async fn get_impl(&self, key: &str) -> Result<ByteStream, StoreError> {
        validate_key(key)?;
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
            .map_err(|e| map_sdk_error("GetObject", e))?;
        Ok(Box::pin(futures::stream::unfold(
            output.body,
            |mut body| async move {
                match body.next().await {
                    Some(Ok(bytes)) => Some((Ok(bytes), body)),
                    Some(Err(e)) => Some((
                        Err(StoreError::Transport {
                            source: Box::new(e),
                        }),
                        body,
                    )),
                    None => None,
                }
            },
        )))
    }

    async fn head_impl(&self, key: &str) -> Result<Option<ObjectMeta>, StoreError> {
        validate_key(key)?;
        let output = match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                return match map_sdk_error("HeadObject", e) {
                    StoreError::NotFound => Ok(None),
                    other => Err(other),
                };
            }
        };
        let size = output
            .content_length()
            .and_then(|len| u64::try_from(len).ok());
        Ok(Some(ObjectMeta {
            key: key.to_owned(),
            size: size.unwrap_or(0),
            last_modified: output.last_modified().map(smithy_time_to_system),
        }))
    }

    /// One page of matching objects plus the continuation token, already
    /// filtered by the component-aligned matching rule and mapped back to
    /// store keys.
    async fn list_page(
        &self,
        match_prefix: &str,
        token: Option<String>,
    ) -> Result<(Vec<ObjectMeta>, Option<String>), StoreError> {
        let request_prefix = self.full_key(match_prefix);
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(request_prefix)
            .set_continuation_token(token)
            .send()
            .await
            .map_err(|e| map_sdk_error("ListObjectsV2", e))?;
        let metas = output
            .contents()
            .iter()
            .filter_map(|object| {
                let full = object.key()?;
                let key = full.strip_prefix(&self.prefix)?;
                if !key_matches_prefix(key, match_prefix) {
                    return None;
                }
                Some(ObjectMeta {
                    key: key.to_owned(),
                    size: object
                        .size()
                        .and_then(|s| u64::try_from(s).ok())
                        .unwrap_or(0),
                    last_modified: object.last_modified().map(smithy_time_to_system),
                })
            })
            .collect();
        Ok((metas, output.next_continuation_token().map(str::to_owned)))
    }

    async fn delete_prefix_impl(&self, prefix: &str) -> Result<u64, StoreError> {
        validate_prefix(prefix)?;
        let match_prefix = normalize_prefix(prefix);
        let mut deleted: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            let (metas, next) = self.list_page(match_prefix, token).await?;
            for batch in metas.chunks(DELETE_BATCH) {
                if batch.is_empty() {
                    continue;
                }
                let objects = batch
                    .iter()
                    .map(|meta| {
                        ObjectIdentifier::builder()
                            .key(self.full_key(&meta.key))
                            .build()
                            .map_err(|e| StoreError::Other(format!("building delete request: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let count = objects.len() as u64;
                let delete = Delete::builder()
                    .set_objects(Some(objects))
                    .quiet(true)
                    .build()
                    .map_err(|e| StoreError::Other(format!("building delete request: {e}")))?;
                let output = self
                    .client
                    .delete_objects()
                    .bucket(&self.bucket)
                    .delete(delete)
                    .send()
                    .await
                    .map_err(|e| map_sdk_error("DeleteObjects", e))?;
                let errors = output.errors();
                if let Some(first) = errors.first() {
                    return Err(StoreError::Other(format!(
                        "DeleteObjects failed for {} of {count} keys; first: {} on {}",
                        errors.len(),
                        first.code().unwrap_or("unknown"),
                        first.key().unwrap_or("unknown"),
                    )));
                }
                deleted += count;
            }
            match next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        Ok(deleted)
    }

    async fn validate_impl(&self) -> Result<(), StoreError> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|e| {
                StoreError::Other(format!(
                    "backup bucket {} failed validation: {}",
                    self.bucket,
                    map_sdk_error("HeadBucket", e)
                ))
            })?;
        Ok(())
    }
}

impl BackupStore for S3Store {
    fn put<'a>(&'a self, key: &'a str, body: ByteStream) -> BoxFuture<'a, Result<(), StoreError>> {
        self.put_impl(key, body).boxed()
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ByteStream, StoreError>> {
        self.get_impl(key).boxed()
    }

    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
        self.head_impl(key).boxed()
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxStream<'a, Result<ObjectMeta, StoreError>> {
        enum State {
            Start,
            Next(String),
            Done,
        }
        let pages = futures::stream::try_unfold(State::Start, move |state| async move {
            let token = match state {
                State::Start => {
                    validate_prefix(prefix)?;
                    None
                }
                State::Next(token) => Some(token),
                State::Done => return Ok(None),
            };
            let (metas, next) = self.list_page(normalize_prefix(prefix), token).await?;
            let next_state = match next {
                Some(token) => State::Next(token),
                None => State::Done,
            };
            Ok(Some((metas, next_state)))
        });
        Box::pin(
            pages
                .map_ok(|metas| futures::stream::iter(metas.into_iter().map(Ok)))
                .try_flatten(),
        )
    }

    fn delete_prefix<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<u64, StoreError>> {
        self.delete_prefix_impl(prefix).boxed()
    }

    fn validate(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        self.validate_impl().boxed()
    }
}

/// Normalize the configured bucket prefix: strip leading `/`, require a
/// trailing `/` when non-empty, and hold it to the same component rules as
/// keys.
fn normalize_store_prefix(prefix: &str) -> Result<String, StoreError> {
    let trimmed = prefix.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    let without_slash = trimmed.strip_suffix('/').unwrap_or(trimmed);
    validate_key(without_slash)
        .map_err(|e| StoreError::Other(format!("invalid [backup.s3] prefix {prefix:?}: {e}")))?;
    Ok(format!("{without_slash}/"))
}

/// Split a byte stream into chunks of exactly `size` bytes, with a final
/// short chunk carrying the remainder. An empty body yields no chunks.
fn chunk_stream(
    body: ByteStream,
    size: usize,
) -> impl futures::Stream<Item = Result<Bytes, StoreError>> + Send {
    futures::stream::try_unfold(
        (body, BytesMut::new(), false),
        move |(mut body, mut buffer, mut exhausted)| async move {
            loop {
                if buffer.len() >= size {
                    let chunk = buffer.split_to(size).freeze();
                    return Ok(Some((chunk, (body, buffer, exhausted))));
                }
                if exhausted {
                    if buffer.is_empty() {
                        return Ok(None);
                    }
                    let chunk = buffer.split().freeze();
                    return Ok(Some((chunk, (body, buffer, exhausted))));
                }
                match body.next().await {
                    Some(Ok(bytes)) => buffer.extend_from_slice(&bytes),
                    Some(Err(e)) => return Err(e),
                    None => exhausted = true,
                }
            }
        },
    )
}

fn smithy_time_to_system(dt: &aws_sdk_s3::primitives::DateTime) -> std::time::SystemTime {
    std::time::SystemTime::try_from(*dt).unwrap_or(std::time::UNIX_EPOCH)
}

/// Reduce an SDK error to a [`StoreError`].
///
/// Service errors are classified by HTTP status and error code and formatted
/// from the code and service message only, so no request or credential
/// material reaches the Display output. Everything else (connection,
/// timeout, response deserialization) becomes [`StoreError::Transport`].
fn map_sdk_error<E>(operation: &'static str, err: SdkError<E>) -> StoreError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match err {
        SdkError::ServiceError(ctx) => {
            let status = ctx.raw().status().as_u16();
            let code = ctx.err().code().unwrap_or("unknown").to_owned();
            match (status, code.as_str()) {
                (404, _) | (_, "NoSuchKey" | "NotFound" | "NoSuchBucket") => StoreError::NotFound,
                (403, _) | (_, "AccessDenied") => {
                    StoreError::PermissionDenied(format!("s3 {operation} was denied"))
                }
                _ => {
                    let message = ctx.err().message().unwrap_or("no message").to_owned();
                    StoreError::Other(format!("s3 {operation} failed: {code}: {message}"))
                }
            }
        }
        other => StoreError::Transport {
            source: Box::new(other),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_prefix_normalization() {
        assert_eq!(normalize_store_prefix("").unwrap(), "");
        assert_eq!(normalize_store_prefix("extenddb/").unwrap(), "extenddb/");
        assert_eq!(normalize_store_prefix("extenddb").unwrap(), "extenddb/");
        assert_eq!(normalize_store_prefix("/a/b").unwrap(), "a/b/");
        assert!(normalize_store_prefix("a//b").is_err());
        assert!(normalize_store_prefix("a/../b").is_err());
    }

    #[tokio::test]
    async fn chunker_splits_exactly() {
        let body = crate::byte_stream_from(vec![7u8; 10]);
        let chunks: Vec<Bytes> = chunk_stream(body, 4).try_collect().await.unwrap();
        assert_eq!(
            chunks.iter().map(Bytes::len).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
    }

    #[tokio::test]
    async fn chunker_handles_exact_multiple_and_empty() {
        let body = crate::byte_stream_from(vec![1u8; 8]);
        let chunks: Vec<Bytes> = chunk_stream(body, 4).try_collect().await.unwrap();
        assert_eq!(chunks.len(), 2);

        let empty = crate::byte_stream_from(Vec::new());
        let chunks: Vec<Bytes> = chunk_stream(empty, 4).try_collect().await.unwrap();
        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn chunker_propagates_source_errors() {
        let body: ByteStream = Box::pin(futures::stream::iter([
            Ok(Bytes::from_static(b"abcd")),
            Err(StoreError::Other("injected".to_owned())),
        ]));
        let result: Result<Vec<Bytes>, StoreError> = chunk_stream(body, 2).try_collect().await;
        assert!(matches!(result, Err(StoreError::Other(msg)) if msg == "injected"));
    }
}
