// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! S3-backed store built on the official `aws-sdk-s3` crate.
//!
//! Credentials come from the AWS SDK's default chain; the store never holds
//! them itself. The chain this build has (aws-config 1.8.13 with the `sso`
//! and `credentials-process` features) tries, in order: the environment
//! (`AWS_ACCESS_KEY_ID` and friends); the selected profile of the shared
//! config files, which may hold static keys, a `role_arn` (STS
//! `AssumeRole`), a `web_identity_token_file` (STS
//! `AssumeRoleWithWebIdentity`), an `sso_session` (SSO and SSO OIDC), a
//! legacy SSO profile (`sso_start_url` without `sso_session`), or a
//! `credential_process`, which makes the server run the command named in
//! the config file and read credentials from its output; web identity from
//! the environment (`AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`);
//! container credentials (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
//! `AWS_CONTAINER_CREDENTIALS_FULL_URI`); and IMDS. A profile with a
//! `login_session` (the shape `aws login` writes) resolves to an error and
//! sends nothing: this build has no `credentials-login` feature. The SSO
//! provider refreshes an expired access token through SSO OIDC and rewrites
//! the cached token file, `$HOME/.aws/sso/cache/<sha1 of the session
//! name>.json`; a legacy SSO profile reads
//! `$HOME/.aws/sso/cache/<sha1 of the start URL>.json` and never refreshes
//! it. Credentials are resolved at the first request, not at `open`, and
//! the whole chain (every `AssumeRole` hop and an SSO refresh included)
//! must answer within the SDK's 5 s identity-cache load timeout, a bound
//! separate from the request timeouts below: a slow credential path fails
//! the request with `identity resolver timed out after 5s`. The region is
//! resolved at `open`, and when neither `region` nor `AWS_REGION`, then
//! `AWS_DEFAULT_REGION`, nor the profile names one, the SDK's region chain
//! queries IMDS during `open` (through the proxy named in the environment,
//! when one is set and no endpoint of the chain is `http://`): off EC2,
//! set `region` or `AWS_EC2_METADATA_DISABLED=true`. That lookup runs only
//! after every endpoint check has passed (the settings are read from a load
//! that resolves no region), so a configuration `open` refuses sends
//! nothing. The config module doc states which endpoints of these providers
//! `open` checks and which HTTP client their requests use.
//!
//! Uploads larger than the configured part size use multipart, and a
//! failed multipart upload is aborted, and a put whose future is dropped
//! mid-upload (a cancelled job), including while `CreateMultipartUpload` is
//! in flight, aborts its upload from a drop guard, so no orphaned parts
//! accrue storage. The guard's abort is a task spawned on the runtime the
//! put ran on, so it covers a future dropped while that runtime keeps
//! running, and not runtime shutdown: a runtime that shuts down with puts
//! in flight drops their futures and the guards' spawned aborts unpolled
//! (and a `CreateMultipartUpload` task in flight with them), so their
//! uploads, parts included, stay open until a `delete_prefix` covering the
//! key sweeps them or the bucket's `AbortIncompleteMultipartUpload` rule
//! removes them. A caller that must leave nothing awaits its puts, or runs
//! its failure path's `delete_prefix`, before the runtime shuts down.
//! `delete_prefix` also aborts any in-progress multipart
//! upload under the prefix, so a crashed process's uploads are swept by the
//! next prefix deletion. Listing paginates and skips keys that
//! fail the shared key rule (console folder markers, foreign objects); prefix
//! deletion batches `DeleteObjects` requests. Every request has an attempt
//! and an operation timeout (`ATTEMPT_TIMEOUT`, `OPERATION_TIMEOUT`; the
//! credential chain's own 5 s bound is stated above); a `GetObject` body,
//! which the SDK streams after those have ended, is bounded per chunk
//! instead (`BODY_IDLE_TIMEOUT`).
//!
//! The store targets general-purpose buckets: `open` refuses the two bucket
//! names the SDK's endpoint rules (aws-sdk-s3 1.122.0,
//! `config/endpoint/internals.rs`) route to the S3 Express backend, an S3
//! Express One Zone directory bucket (`<base>--<zone id>--x-s3`) and the
//! access point of one (`<base>--<zone id>--xa-s3`, which S3 takes in the
//! bucket parameter), since such buckets list in no defined order and the
//! trait promises key order. The client is also built with S3 Express
//! session authentication disabled, so a client that does reach an S3
//! Express name sends no `CreateSession` (a request the SDK sends from a
//! client rebuilt without the store's interceptors);
//! `directory_bucket_client_sends_no_create_session` in the conformance
//! suite reaches the flag through a test-only `open` that skips the
//! refusal.
//!
//! Defence in depth without a test. These are kept although no test fails
//! when one is removed:
//!
//! - `open`'s syntax check of the configured `endpoint` before the loader
//!   sees it: `effective_endpoint` returns the same value (source `Config`)
//!   and `check_endpoint` repeats the syntax check, and the settings load
//!   it precedes sends no request, so no input changes the outcome or what
//!   is sent;
//! - `set_endpoint_url` on the S3 client with the endpoint `open` checked:
//!   `aws_sdk_s3::config::Builder::from(&SdkConfig)` performs the same
//!   lookup at the same moment, so no input reaches a different endpoint;
//!   this keeps the checked value the one requests go to if a later SDK
//!   resolved it in another order. `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS`
//!   does not release an operator from the refusal: it clears only the
//!   general `endpoint_url` the loader copies, and both lookups read the
//!   S3-specific setting and `AWS_ENDPOINT_URL` through the service key
//!   anyway (a cell of `endpoint_from_the_sdk_environment_follows_the_http_rule`
//!   sets the flag and is refused all the same).

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

/// The largest part size S3 accepts.
pub const MAX_PART_SIZE_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Largest number of keys one `DeleteObjects` request accepts.
const DELETE_BATCH: usize = 1000;

/// Largest part number S3 accepts in a multipart upload.
const MAX_PARTS: usize = 10_000;

/// The name suffixes the SDK's endpoint rules route to the S3 Express
/// backend, both refused by `open`: an S3 Express One Zone directory bucket
/// (`<base>--<zone id>--x-s3`) and the access point of one
/// (`<base>--<zone id>--xa-s3`), which S3 takes in the bucket parameter.
const S3_EXPRESS_BUCKET_SUFFIXES: [&str; 2] = ["--x-s3", "--xa-s3"];

/// Longest configured bucket prefix (normalized, trailing `/` included). The
/// backup layout's longest key must fit under the prefix within S3's
/// 1024-byte key limit; 512 leaves it room (the assertion below computes the
/// layout's longest key) and refuses a prefix under which no backup could be
/// written at `open` rather than at the first put.
pub const MAX_STORE_PREFIX_LEN: usize = 512;

// The backup layout of `extenddb-core`'s `backup_format::layout` (#394):
// `<12-digit account>/<table name>/<backup id>/<file>`. This crate does not
// depend on core, so its lengths are restated here; `layout.rs` keeps the
// table-name bound as `MAX_TABLE_NAME_LEN` (255, the same value as
// `key::MAX_COMPONENT_LEN`, and for the same reason), the backup id is 14
// digits of epoch milliseconds, `-` and 8 hex digits, and the longest file
// name is the manifest checksum (24 bytes; a data file is `data/` plus a
// `u32` index padded to 6 and at most 10 digits plus `.json.gz`, 23 bytes).
// `layout.rs` is the other copy of these values, and only these comments
// keep the two agreeing: the test that binds them belongs in the first
// crate that depends on both (the backup job PR), which can build the
// layout's longest key from `layout.rs`'s own constants and assert it fits
// under `MAX_STORE_PREFIX_LEN` within `key::MAX_KEY_LEN`, and that
// `MAX_TABLE_NAME_LEN == key::MAX_COMPONENT_LEN`.
const _: () = {
    let account_id = 12;
    let table_name = crate::key::MAX_COMPONENT_LEN;
    let backup_id = 14 + 1 + 8;
    let manifest_checksum = "extenddb-manifest.sha256".len();
    let data_file = "data/".len() + 10 + ".json.gz".len();
    let longest_file = if manifest_checksum > data_file {
        manifest_checksum
    } else {
        data_file
    };
    let longest_key = account_id + 1 + table_name + 1 + backup_id + 1 + longest_file;
    assert!(
        MAX_STORE_PREFIX_LEN + longest_key <= crate::key::MAX_KEY_LEN,
        "the backup layout's longest key must fit under the longest store prefix"
    );
};

/// Per-attempt and whole-operation timeouts on every request. The SDK's
/// default chain sets only a connect timeout, so a stalled `UploadPart`
/// would otherwise hang a put for as long as the server keeps the socket.
/// The attempt timeout includes sending the request body, so each part must
/// upload within 60 s (see `upload_part_size_bytes` in the config module).
const ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Longest wait for the next chunk of a `GetObject` body. The two timeouts
/// above cover the request and the response headers only: the SDK returns
/// the `GetObject` output as soon as the headers arrive and streams the body
/// afterwards. The SDK's own stalled-stream protection fails a body whose
/// throughput stays under 1 B/s for its 5 s grace period (observed against
/// a proxy that holds the body: "minimum throughput was specified at 1 B/s,
/// but throughput of 0 B/s was observed" after about 6 s); this bound is the
/// store's own, independent of that SDK default, so a reader is never left
/// waiting on a peer that keeps the socket open. It is per chunk, not per
/// object: a slow but moving body is fine.
const BODY_IDLE_TIMEOUT: std::time::Duration = ATTEMPT_TIMEOUT;

/// What `validate` sends after its `ListObjectsV2`, in order, with the grant
/// each request exercises; the operation name is also the one error messages
/// carry. Documented once, on `validate` in `impl BackupStore for S3Store`.
struct Probe {
    operation: &'static str,
    grant: &'static str,
}

const PROBE_LIST_OBJECTS: Probe = Probe {
    operation: "ListObjectsV2",
    grant: "s3:ListBucket",
};
const PROBE_DELETE: Probe = Probe {
    operation: "DeleteObject",
    grant: "s3:DeleteObject",
};
const PROBE_GET: Probe = Probe {
    operation: "GetObject",
    grant: "s3:GetObject",
};
const PROBE_CREATE_UPLOAD: Probe = Probe {
    operation: "CreateMultipartUpload",
    grant: "s3:PutObject (or kms:GenerateDataKey on the bucket's KMS key)",
};
const PROBE_ABORT_UPLOAD: Probe = Probe {
    operation: "AbortMultipartUpload",
    grant: "s3:AbortMultipartUpload",
};
const PROBE_LIST_UPLOADS: Probe = Probe {
    operation: "ListMultipartUploads",
    grant: "s3:ListBucketMultipartUploads",
};

/// Store writing objects to an S3 bucket (or an S3-compatible endpoint).
pub struct S3Store {
    client: aws_sdk_s3::Client,
    bucket: String,
    /// Normalized bucket-side key prefix: empty, or ending in `/`.
    prefix: String,
    part_size: usize,
    max_concurrent_uploads: usize,
    /// Largest number of parts one put may upload; [`MAX_PARTS`] except in
    /// the crate's own tests.
    max_parts: usize,
    /// `max-keys` / `max-uploads` on listing requests; `None` leaves S3's
    /// default (1000). Set only by the crate's own tests, to make paging
    /// observable with a handful of keys.
    page_size: Option<i32>,
    /// Longest wait for the next chunk of a `GetObject` body;
    /// [`BODY_IDLE_TIMEOUT`] except in the crate's own tests.
    body_idle_timeout: std::time::Duration,
}

impl S3Store {
    /// Open a store against the configured bucket.
    ///
    /// Enforces the configuration rules and resolves the region and retry
    /// behavior through the SDK default chain (the module doc says when
    /// that sends an IMDS request); credentials are resolved at the first
    /// request. `endpoint` and `force_path_style` support S3-compatible
    /// servers (the test suite runs against moto).
    ///
    /// # Errors
    ///
    /// Returns an error when the bucket is empty or names an S3 Express
    /// directory bucket or directory-bucket access point, the part size is
    /// outside 5 MiB to 5 GiB, the concurrency is zero, the configured
    /// prefix fails key validation, or an endpoint a client of the store
    /// would use (the S3 client's, the configured one or one the SDK reads
    /// from the environment or the shared config; those of the SDK's STS,
    /// SSO and SSO OIDC clients; and the IMDS endpoint setting) breaks the
    /// endpoint rules in the config module doc.
    pub async fn open(config: &S3StoreConfig) -> Result<Self, StoreError> {
        Self::open_inner(config, None, false).await
    }

    /// Open with an additional client interceptor. This is the failure
    /// injection seam the crate's own test suite uses to make a chosen
    /// operation fail; it compiles only with the `test-util` feature and is
    /// not part of the supported API surface.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub async fn open_with_interceptor(
        config: &S3StoreConfig,
        interceptor: impl aws_sdk_s3::config::Intercept + 'static,
    ) -> Result<Self, StoreError> {
        Self::open_inner(
            config,
            Some(aws_sdk_s3::config::SharedInterceptor::new(interceptor)),
            false,
        )
        .await
    }

    /// Open without the refusal of an S3 Express bucket name, so the crate's
    /// own test suite can check what the client sends to such a bucket
    /// (`disable_s3_express_session_auth` keeps it from sending
    /// `CreateSession`). Every other rule applies. Compiles only with the
    /// `test-util` feature; not part of the supported API surface.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub async fn open_accepting_s3_express_names(
        config: &S3StoreConfig,
    ) -> Result<Self, StoreError> {
        Self::open_inner(config, None, true).await
    }

    /// The client configuration the store built, so the crate's own tests
    /// can check the timeouts and region it applied. Compiles only with the
    /// `test-util` feature; not part of the supported API surface.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn client_config(&self) -> &aws_sdk_s3::Config {
        self.client.config()
    }

    /// Lower the part limit (10,000 in production) so a test can reach it
    /// with a few parts. Compiles only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn set_max_parts(&mut self, max_parts: usize) {
        self.max_parts = max_parts;
    }

    /// Set the page size of `ListObjectsV2` and `ListMultipartUploads`
    /// requests, so a test can make paging happen with a few entries.
    /// Compiles only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn set_page_size(&mut self, page_size: i32) {
        self.page_size = Some(page_size);
    }

    /// Lower the wait for the next `GetObject` body chunk (60 s in
    /// production) so a test can see a stalled body fail in a second.
    /// Compiles only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn set_body_idle_timeout(&mut self, timeout: std::time::Duration) {
        self.body_idle_timeout = timeout;
    }

    /// The HTTP client `open` uses for an `http://` endpoint and for the
    /// credential chain when one of its endpoints is `http://` (proxies
    /// disabled), so the crate's own tests can reach their local endpoint
    /// with a raw client while a proxy variable is set. Compiles only with
    /// the `test-util` feature.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn http_client_without_proxy() -> aws_sdk_s3::config::SharedHttpClient {
        http_client_without_proxy()
    }

    async fn open_inner(
        config: &S3StoreConfig,
        interceptor: Option<aws_sdk_s3::config::SharedInterceptor>,
        accept_s3_express_names: bool,
    ) -> Result<Self, StoreError> {
        let (part_size, prefix) = check_config(config, accept_s3_express_names)?;

        // The configured endpoint is checked before the SDK sees it.
        if let Some(endpoint) = &config.endpoint {
            check_endpoint_syntax(endpoint, Client::S3, EndpointSource::Config)?;
        }
        let loader = |http_client: Option<aws_sdk_s3::config::SharedHttpClient>| {
            let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
            if let Some(region) = &config.region {
                loader = loader.region(aws_config::Region::new(region.clone()));
            }
            if let Some(endpoint) = &config.endpoint {
                loader = loader.endpoint_url(endpoint.clone());
            }
            if let Some(http_client) = http_client {
                loader = loader.http_client(http_client);
            }
            loader
        };
        // The settings are read from a first load that resolves no region:
        // the loader, its credentials chain and its token chain each run the
        // SDK's region chain when they are given none, and that chain
        // queries IMDS, so this load is given a region (its value is not
        // used: the settings read below come from the environment and the
        // shared config files, and this load sends nothing). The checks
        // below run on it, so a refused configuration sends no request at
        // all (the second load would otherwise query IMDS, at the very
        // endpoint the IMDS check refuses, before the refusal).
        let settings = loader(None)
            .region(aws_config::Region::from_static("us-east-1"))
            .load()
            .await;
        // The selected profile, for the two settings the SDK reads from the
        // environment and the profile directly rather than through a
        // service key (`general_endpoint_setting`, `imds_endpoint_setting`);
        // the loaded config does not expose the profile it parsed.
        let profile = load_profile().await;
        let profile = profile.as_ref();
        // The endpoint can come from the config file or from the SDK's own
        // sources (`AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, a profile's
        // `endpoint_url` or `services` section). The rules apply to the one
        // the client uses, so it is read here, checked before the client is
        // built, and set on the client explicitly; the interceptor below
        // checks every request's URI as well, in case the SDK builds one
        // this check did not foresee.
        let endpoint = effective_endpoint(&settings, profile);
        if let Some((endpoint, source)) = &endpoint {
            check_endpoint(
                endpoint,
                Client::S3,
                *source,
                config.allow_insecure_endpoint,
                config.force_path_style,
            )?;
        }
        // The SDK's credential providers (web identity, assume role, SSO,
        // container, IMDS) send their requests through clients they build
        // themselves, from the same environment and profile but without
        // `endpoint` or the interceptor. Their endpoints get the same rules
        // here, whether or not the configured credentials use them.
        let chain_cleartext =
            check_credential_endpoints(&settings, profile, config.allow_insecure_endpoint)?;
        // The second load is the one the clients are built from: it resolves
        // the region through the SDK's chain when none is configured (the
        // module doc says when that queries IMDS). The credential providers'
        // clients take the HTTP client the loader was given (aws-config
        // 1.8.13, `ConfigLoader::load` hands it to `ProviderConfig`, whose
        // `client_config` sets it on the STS, SSO and SSO OIDC clients and
        // whose `http_client` the IMDS and HTTP credential providers read).
        // The default client sends an `http://` request through the proxy
        // named in `HTTP_PROXY`, `http_proxy`, `ALL_PROXY` or `all_proxy`,
        // whatever the endpoint's host is, so a credential request to a
        // loopback emulator (a web identity token, an SSO access token, a
        // container authorization token) would reach the proxy first. When
        // any endpoint of the chain is `http://`, the chain gets the
        // proxy-less client instead.
        let shared = loader(chain_cleartext.then(http_client_without_proxy))
            .load()
            .await;
        let cleartext = endpoint
            .as_ref()
            .is_some_and(|(endpoint, _)| http_host(endpoint).is_some());
        let mut builder = aws_sdk_s3::config::Builder::from(&shared)
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_attempt_timeout(ATTEMPT_TIMEOUT)
                    .operation_timeout(OPERATION_TIMEOUT)
                    .build(),
            )
            // S3 Express session authentication sends `CreateSession` from
            // a client the SDK rebuilds without interceptors, so no request
            // check would see it. `open` refuses S3 Express names above;
            // this keeps a client that reaches one (the test-only `open`)
            // from sending the request at all.
            .disable_s3_express_session_auth(true);
        builder.set_endpoint_url(endpoint.map(|(endpoint, _)| endpoint));
        // The SDK's default client sends through the proxy named in
        // `HTTP_PROXY`, `http_proxy`, `ALL_PROXY` or `all_proxy`, which
        // would carry a signed clear-text request off the host whatever the
        // endpoint's host is. An `http://` endpoint is reached directly. An
        // `https://` endpoint keeps the SDK's default client, proxy settings
        // included, even when the loader above was given the proxy-less one
        // (`Builder::from(&SdkConfig)` copies it; `None` restores the
        // default).
        if cleartext {
            builder = builder.http_client(http_client_without_proxy());
        } else {
            builder.set_http_client(None);
        }
        if config.force_path_style {
            builder = builder.force_path_style(true);
        }
        if !config.allow_insecure_endpoint {
            builder.push_interceptor(aws_sdk_s3::config::SharedInterceptor::new(
                RefuseCleartextRequests,
            ));
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
            max_parts: MAX_PARTS,
            page_size: None,
            body_idle_timeout: BODY_IDLE_TIMEOUT,
        })
    }

    /// Bucket-side key for a store key. S3 bounds the whole object key at
    /// [`crate::key::MAX_KEY_LEN`] bytes, prefix included, so a key that fits
    /// the shared rule can still be too long under a long prefix; refuse it
    /// here with the same error class rather than letting S3 answer 400.
    fn full_key(&self, key: &str) -> Result<String, StoreError> {
        let full = format!("{}{key}", self.prefix);
        if full.len() > crate::key::MAX_KEY_LEN {
            return Err(StoreError::InvalidKey(format!(
                "key is {} bytes under the {}-byte store prefix; S3 caps the whole key at {}",
                key.len(),
                self.prefix.len(),
                crate::key::MAX_KEY_LEN
            )));
        }
        Ok(full)
    }

    async fn put_impl(&self, key: &str, body: ByteStream) -> Result<ObjectMeta, StoreError> {
        validate_key(key)?;
        let full_key = self.full_key(key)?;
        let mut chunks = std::pin::pin!(chunk_stream(body, self.part_size));

        let first = match chunks.try_next().await? {
            Some(chunk) => chunk,
            None => Bytes::new(),
        };
        let second = chunks.try_next().await?;

        let Some(second) = second else {
            // The whole body fits in one part: a single PutObject.
            let size = first.len() as u64;
            let output = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&full_key)
                .body(first.into())
                .send()
                .await
                .map_err(|e| map_sdk_error("PutObject", e))?;
            return Ok(ObjectMeta {
                key: key.to_owned(),
                size,
                last_modified: None,
                etag: output.e_tag().map(strip_etag_quotes),
            });
        };

        // Every failure after CreateMultipartUpload must reach the abort,
        // including a failed CompleteMultipartUpload, so no orphaned parts
        // accrue storage. A complete that failed after S3 committed it makes
        // the abort answer NoSuchUpload, which the best-effort abort logs.
        // The guard covers the other way a put ends: its future dropped
        // mid-upload (the backup job cancelled, a caller's timeout). Dropping
        // the guard spawns the abort, because nothing else would ever send
        // it (an in-progress upload is not an object `delete_prefix`'s
        // listing sees, and S3 bills its parts until it is aborted). The
        // create itself runs in a spawned task (`create_upload_guarded`) so
        // the guard exists even when the put is dropped while that request
        // is in flight.
        let mut abort_guard = self.create_upload_guarded(&full_key).await?;
        let upload_id = abort_guard.upload_id.clone();
        let outcome: Result<ObjectMeta, StoreError> = async {
            let (parts, size) = self
                .upload_parts(&full_key, &upload_id, first, second, chunks)
                .await?;
            let output = self
                .client
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
            Ok(ObjectMeta {
                key: key.to_owned(),
                size,
                last_modified: None,
                etag: output.e_tag().map(strip_etag_quotes),
            })
        }
        .await;

        // The guard stays armed until the inline abort has returned: a put
        // dropped while that abort is in flight (a caller's timeout) still
        // has the guard abort the upload.
        let meta = match outcome {
            Ok(meta) => meta,
            Err(e) => {
                // Best effort: the original failure is what the caller needs.
                abort_upload(&self.client, &self.bucket, &full_key, &upload_id).await;
                abort_guard.disarm();
                return Err(e);
            }
        };
        abort_guard.disarm();
        Ok(meta)
    }

    /// Send `CreateMultipartUpload` for the bucket-side key `full_key` and
    /// return a guard armed for the new upload.
    ///
    /// The guard has to exist even when the caller's future is dropped
    /// while the request is in flight: S3 may already have created the
    /// upload, and the response carrying its id would never be read. So the
    /// request runs in its own task, which arms the guard and hands it over
    /// a oneshot channel. If the caller is gone, either the send fails and
    /// the task drops the guard, or the guard sits in the channel and is
    /// dropped with the receiver; both abort the upload. (Runtime shutdown
    /// drops the task itself mid-request, so no guard is created; see the
    /// module doc.)
    async fn create_upload_guarded(&self, full_key: &str) -> Result<AbortOnDrop, StoreError> {
        let (guard_tx, guard_rx) = tokio::sync::oneshot::channel();
        let client = self.client.clone();
        let (task_bucket, task_key) = (self.bucket.clone(), full_key.to_owned());
        tokio::spawn(async move {
            let outcome = match client
                .create_multipart_upload()
                .bucket(&task_bucket)
                .key(&task_key)
                .send()
                .await
            {
                Ok(output) => match output.upload_id() {
                    Some(id) => Ok(AbortOnDrop::new(
                        client.clone(),
                        &task_bucket,
                        &task_key,
                        id,
                    )),
                    None => Err(StoreError::Other(
                        "CreateMultipartUpload returned no upload id".to_owned(),
                    )),
                },
                Err(e) => Err(map_sdk_error("CreateMultipartUpload", e)),
            };
            // A failed send hands the outcome back; dropping it here drops
            // the guard, which aborts the upload.
            let _ = guard_tx.send(outcome);
        });
        guard_rx.await.map_err(|_| {
            StoreError::Other("CreateMultipartUpload task ended without a result".to_owned())
        })?
    }

    /// Upload every part with bounded concurrency; returns the completed part
    /// list sorted by part number and the total number of body bytes sent.
    async fn upload_parts(
        &self,
        full_key: &str,
        upload_id: &str,
        first: Bytes,
        second: Bytes,
        rest: impl futures::Stream<Item = Result<Bytes, StoreError>> + Send,
    ) -> Result<(Vec<CompletedPart>, u64), StoreError> {
        let all_chunks = futures::stream::iter([Ok(first), Ok(second)]).chain(rest);
        let mut uploaded: Vec<(CompletedPart, u64)> = all_chunks
            .enumerate()
            .map(|(index, chunk)| async move {
                let chunk = chunk?;
                let len = chunk.len() as u64;
                // S3 allows 10,000 parts, so with the 8 MiB default part
                // size a single object is capped at about 78 GiB. The backup
                // format bounds data files at 64 MiB, far below that; a
                // caller putting larger objects must raise the part size.
                // This check is reached only after 10,000 parts have been
                // sent, as S3's own refusal of part 10,001 would be; it
                // saves that one part and gives a message naming the
                // setting to change.
                if index >= self.max_parts {
                    return Err(StoreError::Other(format!(
                        "object exceeds {} parts of {} bytes; raise upload_part_size_bytes",
                        self.max_parts, self.part_size
                    )));
                }
                let part_number =
                    i32::try_from(index + 1).expect("part numbers are bounded by MAX_PARTS");
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
                Ok::<_, StoreError>((
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(e_tag)
                        .build(),
                    len,
                ))
            })
            .buffered(self.max_concurrent_uploads)
            .try_collect()
            .await?;
        uploaded.sort_by_key(|(part, _)| part.part_number());
        let size = uploaded.iter().map(|(_, len)| len).sum();
        Ok((uploaded.into_iter().map(|(part, _)| part).collect(), size))
    }

    async fn get_impl(&self, key: &str) -> Result<ByteStream, StoreError> {
        validate_key(key)?;
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.full_key(key)?)
            .send()
            .await
            .map_err(|e| map_sdk_error("GetObject", e))?;
        // The SDK's timeouts ended with the response headers; each chunk of
        // the body gets its own bound (`BODY_IDLE_TIMEOUT`) beside the SDK's
        // stalled-stream protection, so a peer that stops sending mid-body
        // fails the read instead of hanging it. An elapsed wait ends the
        // stream: the SDK body cannot resume.
        let idle = self.body_idle_timeout;
        let key = key.to_owned();
        Ok(Box::pin(futures::stream::unfold(
            Some(output.body),
            move |body| {
                let key = key.clone();
                async move {
                    let mut body = body?;
                    match tokio::time::timeout(idle, body.next()).await {
                        Ok(Some(Ok(bytes))) => Some((Ok(bytes), Some(body))),
                        Ok(Some(Err(e))) => Some((
                            Err(StoreError::Transport {
                                source: Box::new(e),
                            }),
                            None,
                        )),
                        Ok(None) => None,
                        Err(_elapsed) => Some((
                            Err(StoreError::Transport {
                                source: Box::new(std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    format!(
                                        "GetObject body for {key} stalled: no bytes arrived \
                                         within {} s",
                                        idle.as_secs_f64()
                                    ),
                                )),
                            }),
                            None,
                        )),
                    }
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
            .key(self.full_key(key)?)
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
            .and_then(|len| u64::try_from(len).ok())
            .ok_or_else(|| {
                StoreError::Other(format!("HeadObject for {key} returned no Content-Length"))
            })?;
        Ok(Some(ObjectMeta {
            key: key.to_owned(),
            size,
            last_modified: output.last_modified().and_then(smithy_time_to_system),
            etag: output.e_tag().map(strip_etag_quotes),
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
        let request_prefix = format!("{}{match_prefix}", self.prefix);
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(request_prefix)
            .set_continuation_token(token)
            .set_max_keys(self.page_size)
            .send()
            .await
            .map_err(|e| map_sdk_error("ListObjectsV2", e))?;
        let mut metas = Vec::new();
        for object in output.contents() {
            let Some(full) = object.key() else { continue };
            let Some(key) = full.strip_prefix(&self.prefix) else {
                continue;
            };
            if !key_matches_prefix(key, match_prefix) {
                continue;
            }
            // A bucket can hold objects this store never wrote: console
            // "folder" markers (`acct/`), objects with a backslash in the
            // name. Their keys fail the shared rule, so `get`/`head` on them
            // would answer `InvalidKey`; a listing that reported them would
            // hand a caller keys it cannot use. `delete_prefix` still removes
            // them (it deletes the bucket-side keys it listed, below).
            if validate_key(key).is_err() {
                tracing::debug!(key = %full, "skipping object whose key fails the store's key rule");
                continue;
            }
            let size = object
                .size()
                .and_then(|s| u64::try_from(s).ok())
                .ok_or_else(|| {
                    StoreError::Other(format!("ListObjectsV2 returned no size for {key}"))
                })?;
            metas.push(ObjectMeta {
                key: key.to_owned(),
                size,
                last_modified: object.last_modified().and_then(smithy_time_to_system),
                etag: object.e_tag().map(strip_etag_quotes),
            });
        }
        Ok((metas, output.next_continuation_token().map(str::to_owned)))
    }

    async fn delete_prefix_impl(&self, prefix: &str) -> Result<u64, StoreError> {
        validate_prefix(prefix)?;
        let match_prefix = normalize_prefix(prefix);
        let request_prefix = format!("{}{match_prefix}", self.prefix);
        let mut deleted: u64 = 0;
        let mut token: Option<String> = None;
        loop {
            // Raw bucket-side keys, not `list_page`'s filtered view: an
            // object whose key fails the store rule is still under the
            // prefix and still has to go.
            let output = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&request_prefix)
                .set_continuation_token(token)
                .set_max_keys(self.page_size)
                .send()
                .await
                .map_err(|e| map_sdk_error("ListObjectsV2", e))?;
            let keys: Vec<&str> = output
                .contents()
                .iter()
                .filter_map(|o| o.key())
                .filter(|full| {
                    full.strip_prefix(&self.prefix)
                        .is_some_and(|k| key_matches_prefix(k, match_prefix))
                })
                .collect();
            for batch in keys.chunks(DELETE_BATCH) {
                let objects = batch
                    .iter()
                    .map(|key| {
                        ObjectIdentifier::builder()
                            .key(*key)
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
                    let code = first.code().unwrap_or("unknown");
                    let key = first.key().map_or("unknown", |k| {
                        k.strip_prefix(self.prefix.as_str()).unwrap_or(k)
                    });
                    let message = format!(
                        "DeleteObjects failed for {} of {count} keys; first: {code} on {key}",
                        errors.len()
                    );
                    // A per-key denial is the same operator problem as a
                    // denied request.
                    return Err(if code == "AccessDenied" {
                        StoreError::PermissionDenied(message)
                    } else {
                        StoreError::Other(message)
                    });
                }
                deleted += count;
            }
            match output.next_continuation_token() {
                Some(next) => token = Some(next.to_owned()),
                None => break,
            }
        }
        // In-progress multipart uploads are not objects: the listing above
        // does not see them and S3 bills their parts until they are aborted.
        // A process that died mid-put left one; the reconciler's prefix
        // deletion is the one place that sweeps it.
        self.abort_uploads_under(&request_prefix, match_prefix)
            .await?;
        Ok(deleted)
    }

    /// Abort every in-progress multipart upload under the normalized store
    /// prefix `match_prefix` (same component-aligned rule as the object
    /// deletion), paging through `ListMultipartUploads` with the bucket-side
    /// string prefix `request_prefix`. The string prefix also returns
    /// siblings (`a/bc` for `a/b`), which the filter skips.
    ///
    /// A denied `ListMultipartUploads` (credentials without
    /// `s3:ListBucketMultipartUploads`) or a denied `AbortMultipartUpload`
    /// (without `s3:AbortMultipartUpload`) is logged and not an error: the
    /// objects are already gone by the time this runs, so failing would make
    /// every retry fail the same way with nothing left to delete. A denied
    /// abort does not stop the sweep; the next upload is still aborted. The
    /// bucket's `AbortIncompleteMultipartUpload` lifecycle rule is the
    /// backstop for such uploads, and `validate` reports either missing
    /// grant at startup (it probes both). A token code (`ExpiredToken` and
    /// the others in `is_token_code`) is not a denial and fails the call.
    async fn abort_uploads_under(
        &self,
        request_prefix: &str,
        match_prefix: &str,
    ) -> Result<(), StoreError> {
        let mut key_marker: Option<String> = None;
        let mut id_marker: Option<String> = None;
        loop {
            let output = match self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket)
                .prefix(request_prefix)
                .set_key_marker(key_marker)
                .set_upload_id_marker(id_marker)
                .set_max_uploads(self.page_size)
                .send()
                .await
            {
                Ok(output) => output,
                Err(e) if is_access_denied(&e) => {
                    tracing::warn!(
                        prefix = %request_prefix,
                        error = %map_sdk_error("ListMultipartUploads", e),
                        "cannot list multipart uploads under a deleted prefix (is \
                         s3:ListBucketMultipartUploads granted?); uploads a crashed put left \
                         there stay open until the bucket's AbortIncompleteMultipartUpload \
                         lifecycle rule removes them"
                    );
                    return Ok(());
                }
                Err(e) => return Err(map_sdk_error("ListMultipartUploads", e)),
            };
            for upload in output.uploads() {
                let (Some(key), Some(id)) = (upload.key(), upload.upload_id()) else {
                    continue;
                };
                let matches = key
                    .strip_prefix(self.prefix.as_str())
                    .is_some_and(|k| key_matches_prefix(k, match_prefix));
                if !matches {
                    continue;
                }
                match self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(id)
                    .send()
                    .await
                {
                    Ok(_) => {}
                    // A denial: the objects are gone, and failing here would
                    // fail every retry the same way.
                    Err(e) if is_access_denied(&e) => {
                        tracing::warn!(
                            key = %key,
                            error = %map_sdk_error("AbortMultipartUpload", e),
                            "cannot abort a multipart upload under a deleted prefix (is \
                             s3:AbortMultipartUpload granted?); it stays open until the \
                             bucket's AbortIncompleteMultipartUpload lifecycle rule removes it"
                        );
                    }
                    Err(e) => match map_sdk_error("AbortMultipartUpload", e) {
                        // Completed or aborted by its owner between the list
                        // and this call.
                        StoreError::NotFound => {}
                        StoreError::Other(msg) if msg.contains("NoSuchUpload") => {}
                        other => return Err(other),
                    },
                }
            }
            if output.is_truncated() == Some(true) {
                key_marker = output.next_key_marker().map(str::to_owned);
                id_marker = output.next_upload_id_marker().map(str::to_owned);
                if key_marker.is_none() && id_marker.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    async fn validate_impl(&self) -> Result<(), StoreError> {
        // `ListObjectsV2` with one key rather than `HeadBucket`: it needs the
        // same `s3:ListBucket` permission the store uses, and its error
        // responses carry a body, so a 403 names the credential problem
        // (`AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch`)
        // and a 404 says `NoSuchBucket`. A HEAD response has no body and the
        // SDK can only synthesize a code for its 404.
        self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .max_keys(1)
            .send()
            .await
            .map_err(|e| {
                // A bucket in another region answers with 301 and names its
                // region in a header (and `PermanentRedirect` in the body);
                // say so rather than letting it fall through as an
                // unexplained redirect.
                if let SdkError::ServiceError(ctx) = &e
                    && (ctx.raw().status().as_u16() == 301
                        || ctx.err().code() == Some("PermanentRedirect"))
                {
                    let region = ctx
                        .raw()
                        .headers()
                        .get("x-amz-bucket-region")
                        .unwrap_or("an unknown region");
                    return StoreError::Other(format!(
                        "backup bucket {} is in {region}, not the configured region",
                        self.bucket
                    ));
                }
                let missing = matches!(&e, SdkError::ServiceError(ctx)
                    if ctx.raw().status().as_u16() == 404 || ctx.err().code() == Some("NoSuchBucket"));
                if missing {
                    return StoreError::Other(format!(
                        "backup bucket {} does not exist",
                        self.bucket
                    ));
                }
                match map_sdk_error(PROBE_LIST_OBJECTS.operation, e) {
                    StoreError::PermissionDenied(reason) => StoreError::PermissionDenied(format!(
                        "access to backup bucket {} is denied for the configured credentials \
                         ({}; {reason})",
                        self.bucket, PROBE_LIST_OBJECTS.grant
                    )),
                    other => StoreError::Other(format!(
                        "backup bucket {} failed validation: {other}",
                        self.bucket
                    )),
                }
            })?;
        self.validate_write_grants().await
    }

    /// The probes after the `ListObjectsV2` (the list and its grants are on
    /// `validate` in `impl BackupStore for S3Store`). Each denial names the
    /// missing grant from the `PROBE_*` table.
    async fn validate_write_grants(&self) -> Result<(), StoreError> {
        let probe = format!("{}.extenddb-validate-{}", self.prefix, uuid::Uuid::new_v4());
        let fail = |grant: &str, error: StoreError, extra: &str| match error {
            StoreError::PermissionDenied(reason) => StoreError::PermissionDenied(format!(
                "the configured credentials lack {grant} on backup bucket {} ({reason}){extra}",
                self.bucket
            )),
            other => StoreError::Other(format!(
                "backup bucket {} failed validation: {other}",
                self.bucket
            )),
        };
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&probe)
            .send()
            .await
            .map_err(|e| {
                fail(
                    PROBE_DELETE.grant,
                    map_sdk_error(PROBE_DELETE.operation, e),
                    "",
                )
            })?;
        // The key does not exist, so an allowed read answers 404 NoSuchKey.
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&probe)
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => match map_sdk_error(PROBE_GET.operation, e) {
                StoreError::NotFound => {}
                other => {
                    return Err(fail(
                        PROBE_GET.grant,
                        other,
                        "; S3 also answers a read of a missing key with 403 rather than 404 \
                         when s3:ListBucket does not cover the key, and the store needs a \
                         missing key to read as absent (the reconciler and the sync rely on it)",
                    ));
                }
            },
        }
        // The create runs in a spawned task that arms the guard, as a put's
        // does, so a validation dropped while the request is in flight still
        // aborts the probe upload; the guard stays armed until the abort has
        // returned.
        let mut guard = self
            .create_upload_guarded(&probe)
            .await
            .map_err(|e| fail(PROBE_CREATE_UPLOAD.grant, e, ""))?;
        let aborted = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&probe)
            .upload_id(&guard.upload_id)
            .send()
            .await;
        guard.disarm();
        match aborted {
            Ok(_) => {}
            // The upload is gone already: a `delete_prefix` whose sweep
            // covers the probe key (`delete_prefix("")`) aborted it first.
            // The grant is what the probe checks, and the request reached
            // the bucket.
            Err(e) if is_no_such_upload(&e) => {}
            Err(e) => {
                return Err(fail(
                    PROBE_ABORT_UPLOAD.grant,
                    map_sdk_error(PROBE_ABORT_UPLOAD.operation, e),
                    &format!(
                        "; the probe upload of {probe} stays open until the bucket's \
                         AbortIncompleteMultipartUpload lifecycle rule removes it"
                    ),
                ));
            }
        }
        self.client
            .list_multipart_uploads()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .max_uploads(1)
            .send()
            .await
            .map_err(|e| {
                fail(
                    PROBE_LIST_UPLOADS.grant,
                    map_sdk_error(PROBE_LIST_UPLOADS.operation, e),
                    "",
                )
            })?;
        Ok(())
    }
}

impl BackupStore for S3Store {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: ByteStream,
    ) -> BoxFuture<'a, Result<ObjectMeta, StoreError>> {
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

    /// Lists one key under the prefix (`ListObjectsV2`, `s3:ListBucket`)
    /// rather than calling `HeadBucket`, since only a GET-shaped error has a
    /// body: a 403 keeps S3's code, a 404 names the missing bucket, a 301 the
    /// bucket's region. Then, on a fresh key
    /// `<prefix>.extenddb-validate-<uuid>`, it sends:
    ///
    /// 1. `DeleteObject` of the absent key (`s3:DeleteObject`);
    /// 2. `GetObject` of it, which must answer `NoSuchKey`
    ///    (`s3:GetObject`; a missing key also reads as 403 when
    ///    `s3:ListBucket` does not cover it);
    /// 3. `CreateMultipartUpload` (`s3:PutObject`, and `kms:GenerateDataKey`
    ///    on an SSE-KMS bucket) and its `AbortMultipartUpload`
    ///    (`s3:AbortMultipartUpload`);
    /// 4. a one-entry `ListMultipartUploads`
    ///    (`s3:ListBucketMultipartUploads`).
    ///
    /// A denial names the missing grant. `kms:Decrypt`, which an SSE-KMS
    /// bucket needs for `UploadPart` and `GetObject`, is not exercised.
    /// No object or upload is left; on a versioned or Object Lock bucket the
    /// `DeleteObject` leaves one delete marker under the probe key, which
    /// only an `Expiration { ExpiredObjectDeleteMarker: true }` lifecycle
    /// rule removes. A validation dropped at any point aborts its probe
    /// upload from a drop guard, while `CreateMultipartUpload` is in flight
    /// included (the request runs in a spawned task, as a put's does), with
    /// the runtime-shutdown exception in the module doc. A create the SDK
    /// retried after the server had created the first leaves an empty
    /// upload that only the `AbortIncompleteMultipartUpload` lifecycle rule
    /// removes. A probe abort answered `NoSuchUpload` passes: a concurrent
    /// `delete_prefix("")` sweeps the probe upload too.
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
    validate_key(without_slash).map_err(|e| {
        StoreError::Other(format!("invalid [backup.store.s3] prefix {prefix:?}: {e}"))
    })?;
    let normalized = format!("{without_slash}/");
    if normalized.len() > MAX_STORE_PREFIX_LEN {
        return Err(StoreError::Other(format!(
            "[backup.store.s3] prefix is {} bytes; the maximum is {MAX_STORE_PREFIX_LEN}, so \
             every key of the backup layout fits under it within S3's 1024-byte key limit",
            normalized.len()
        )));
    }
    Ok(normalized)
}

/// The configuration rules `open` applies before anything is loaded: the
/// bucket name, the part size, the concurrency and the prefix. Returns the
/// part size as a `usize` and the normalized prefix. The S3 Express name
/// refusal is skipped only by the test-only `open` that checks what the
/// client sends to such a name.
fn check_config(
    config: &S3StoreConfig,
    accept_s3_express_names: bool,
) -> Result<(usize, String), StoreError> {
    if config.bucket.trim().is_empty() {
        return Err(StoreError::Other(
            "[backup.store.s3] bucket is empty; name the destination bucket".to_owned(),
        ));
    }
    if config.upload_part_size_bytes < MIN_PART_SIZE_BYTES {
        return Err(StoreError::Other(format!(
            "upload_part_size_bytes is {}; the minimum is {MIN_PART_SIZE_BYTES} (5 MiB)",
            config.upload_part_size_bytes
        )));
    }
    if config.upload_part_size_bytes > MAX_PART_SIZE_BYTES {
        return Err(StoreError::Other(format!(
            "upload_part_size_bytes is {}; the maximum is {MAX_PART_SIZE_BYTES} (5 GiB), \
             the largest part S3 accepts",
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
    let s3_express_suffix = S3_EXPRESS_BUCKET_SUFFIXES
        .iter()
        .find(|suffix| config.bucket.ends_with(*suffix));
    if let Some(suffix) = s3_express_suffix.filter(|_| !accept_s3_express_names) {
        return Err(StoreError::Other(format!(
            "[backup.store.s3] bucket names an S3 Express One Zone directory bucket or a \
             directory-bucket access point (the {suffix} suffix); the store supports \
             general-purpose buckets only, because a directory bucket lists keys in no \
             defined order and the store promises key order"
        )));
    }
    Ok((part_size, prefix))
}

/// A client that sends requests on the store's behalf, for the endpoint
/// rules and the refusal messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Client {
    /// The store's S3 client.
    S3,
    /// The STS client of the SDK's web identity and assume-role providers.
    Sts,
    /// The SSO client of the SDK's SSO provider.
    Sso,
    /// The SSO OIDC client the SDK's SSO token provider refreshes with.
    SsoOidc,
    /// The general endpoint setting (`AWS_ENDPOINT_URL`, a profile's
    /// `endpoint_url`), which a client without its own setting falls back
    /// to. Not a service setting: `general_endpoint_setting` reads it.
    General,
    /// The IMDS client of the SDK's IMDS credential and region providers.
    /// Its endpoint is not an `endpoint_url` setting: it comes from
    /// `AWS_EC2_METADATA_SERVICE_ENDPOINT` or the profile's
    /// `ec2_metadata_service_endpoint` (`imds_endpoint_setting`).
    Imds,
}

impl Client {
    /// The SDK's service id, which names the service-specific settings
    /// (`AWS_ENDPOINT_URL_<SERVICE>`, the `services` entry). `None` for the
    /// general and IMDS settings, which have none: the SDK reads those two
    /// from the environment and the profile directly, and so does the
    /// store (`general_endpoint_setting`, `imds_endpoint_setting`).
    fn service_id(self) -> Option<&'static str> {
        match self {
            Self::S3 => Some("S3"),
            Self::Sts => Some("STS"),
            Self::Sso => Some("SSO"),
            Self::SsoOidc => Some("SSO OIDC"),
            Self::General | Self::Imds => None,
        }
    }

    /// The environment variable the SDK reads for this client's endpoint.
    fn env_var(self) -> Option<&'static str> {
        match self {
            Self::S3 => Some("AWS_ENDPOINT_URL_S3"),
            Self::Sts => Some("AWS_ENDPOINT_URL_STS"),
            Self::Sso => Some("AWS_ENDPOINT_URL_SSO"),
            Self::SsoOidc => Some("AWS_ENDPOINT_URL_SSO_OIDC"),
            Self::Imds => Some(IMDS_ENDPOINT_ENV),
            Self::General => None,
        }
    }

    /// The entry of a profile's `services` section the SDK reads.
    fn services_entry(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Sts => "sts",
            Self::Sso => "sso",
            Self::SsoOidc => "sso-oidc",
            Self::General | Self::Imds => "",
        }
    }
}

/// The environment variable and the profile key the SDK's IMDS client reads
/// its endpoint from (aws-config 1.8.13, `imds/client.rs`); the variable
/// wins.
const IMDS_ENDPOINT_ENV: &str = "AWS_EC2_METADATA_SERVICE_ENDPOINT";
const IMDS_ENDPOINT_PROFILE_KEY: &str = "ec2_metadata_service_endpoint";

/// The environment variable and the profile key of the general endpoint
/// setting (aws-config 1.8.13, `default_provider/endpoint_url.rs`); the
/// variable wins. The service clients' lookup falls back to the same two.
const GENERAL_ENDPOINT_ENV: &str = "AWS_ENDPOINT_URL";
const GENERAL_ENDPOINT_PROFILE_KEY: &str = "endpoint_url";

impl std::fmt::Display for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::S3 => "the S3 endpoint",
            Self::Sts => "the STS endpoint",
            Self::Sso => "the SSO endpoint",
            Self::SsoOidc => "the SSO OIDC endpoint",
            Self::General => "the general AWS endpoint",
            Self::Imds => "the IMDS endpoint",
        })
    }
}

/// Where an endpoint came from, for the refusal messages: an operator who
/// never set `endpoint` must be pointed at the setting that supplied the
/// value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointSource {
    /// `[backup.store.s3] endpoint`.
    Config,
    /// The client's own environment variable (`AWS_ENDPOINT_URL_S3`,
    /// `AWS_ENDPOINT_URL_STS`, ...).
    EnvService(Client),
    /// The `AWS_ENDPOINT_URL` environment variable.
    Env,
    /// `endpoint_url` in the selected profile of the shared AWS config file.
    Profile,
    /// The client's entry (`s3`, `sts`, ...) in the selected profile's
    /// `services` section of the shared AWS config file.
    ProfileServices(Client),
    /// `ec2_metadata_service_endpoint` in the selected profile of the shared
    /// AWS config file.
    ImdsProfile,
}

impl std::fmt::Display for EndpointSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config => f.write_str("[backup.store.s3] endpoint"),
            Self::EnvService(client) => {
                f.write_str(client.env_var().unwrap_or(GENERAL_ENDPOINT_ENV))
            }
            Self::Env => f.write_str(GENERAL_ENDPOINT_ENV),
            Self::Profile => write!(
                f,
                "{GENERAL_ENDPOINT_PROFILE_KEY} in the shared AWS config file's profile"
            ),
            Self::ProfileServices(client) => write!(
                f,
                "endpoint_url under {} in the shared AWS config file's services section",
                client.services_entry()
            ),
            Self::ImdsProfile => write!(
                f,
                "{IMDS_ENDPOINT_PROFILE_KEY} in the shared AWS config file's profile"
            ),
        }
    }
}

/// The endpoint the S3 client uses and its source, read from the loaded
/// shared config the way `aws_sdk_s3::config::Builder::from(&SdkConfig)`
/// reads it (aws-sdk-s3 1.122.0, `config.rs`): an endpoint set in code (the
/// config file's `endpoint`, passed to the loader) wins; otherwise the
/// S3-specific setting, then the general one, where the SDK reads the
/// environment (`AWS_ENDPOINT_URL_S3`, then `AWS_ENDPOINT_URL`) before the
/// shared config file (the profile's `services` section, then its
/// `endpoint_url`; aws-runtime 1.6.0, `EnvConfigValue::load`). `open` sets
/// the result on the client explicitly, so the endpoint `check_endpoint`
/// saw is the one requests go to even if a later SDK changed that order.
fn effective_endpoint(
    shared: &aws_config::SdkConfig,
    profile: Option<&aws_config::profile::ProfileSet>,
) -> Option<(String, EndpointSource)> {
    if shared.get_origin("endpoint_url").is_client_config() {
        return shared
            .endpoint_url()
            .map(|endpoint| (endpoint.to_owned(), EndpointSource::Config));
    }
    let endpoint = endpoint_setting(shared, Client::S3)
        .or_else(|| shared.endpoint_url().map(str::to_owned))?;
    let source = endpoint_source(profile, Client::S3, &endpoint);
    Some((endpoint, source))
}

/// The endpoint the SDK's environment and shared config give the service
/// client `client`: its own environment variable, then `AWS_ENDPOINT_URL`,
/// then its entry in the profile's `services` section, then the profile's
/// `endpoint_url` (aws-runtime 1.6.0, `EnvConfigValue::load`). The STS, SSO
/// and SSO OIDC clients of the credential providers resolve their endpoint
/// with exactly this lookup (`aws-sdk-sts` 1.97.0, `aws-sdk-sso` 1.93.0 and
/// `aws-sdk-ssooidc` 1.95.0, `config.rs`), from a config that never carries
/// `endpoint` (aws-config 1.8.13, `ProviderConfig::client_config`). `None`
/// for the general and IMDS settings, which have no service id: given an
/// empty one, the lookup would read `AWS_ENDPOINT_URL_` (the variable with
/// the service id appended), which the SDK gives no meaning to, ahead of
/// the variable it does read; `general_endpoint_setting` and
/// `imds_endpoint_setting` read those two settings the way the SDK does.
fn endpoint_setting(shared: &aws_config::SdkConfig, client: Client) -> Option<String> {
    let key = aws_types::service_config::ServiceConfigKey::builder()
        .service_id(client.service_id()?)
        .env(GENERAL_ENDPOINT_ENV)
        .profile(GENERAL_ENDPOINT_PROFILE_KEY)
        .build()
        .expect("every field of the key is set");
    shared.service_config()?.load_config(key)
}

/// Which setting supplied `endpoint` for the service client `client`. The
/// SDK does not report it; since it reads the environment first, a value
/// equal to one of the two variables came from that variable. Otherwise it
/// came from the file: the profile's own `endpoint_url` when that key holds
/// the same value, and the client's `services` entry when not.
fn endpoint_source(
    profile: Option<&aws_config::profile::ProfileSet>,
    client: Client,
    endpoint: &str,
) -> EndpointSource {
    let from_env = |name: &str| std::env::var_os(name).is_some_and(|value| value == *endpoint);
    if client.env_var().is_some_and(from_env) {
        EndpointSource::EnvService(client)
    } else if from_env(GENERAL_ENDPOINT_ENV) {
        EndpointSource::Env
    } else if profile.and_then(|profile| profile.get(GENERAL_ENDPOINT_PROFILE_KEY))
        == Some(endpoint)
    {
        EndpointSource::Profile
    } else {
        EndpointSource::ProfileServices(client)
    }
}

/// The selected profile of the shared config files, parsed as the SDK
/// parses it for the loader (the default file set, `AWS_CONFIG_FILE`,
/// `AWS_SHARED_CREDENTIALS_FILE` and `AWS_PROFILE` honoured; aws-config
/// 1.8.13, `ProviderConfig::profile`). The loaded `SdkConfig` keeps its
/// parse behind the service-key lookup only, so the two settings the SDK
/// reads by key are read from this one. `None` when the files cannot be
/// read or parsed: the SDK's loads log that, and its clients then read no
/// profile either, so the check and the clients agree.
async fn load_profile() -> Option<aws_config::profile::ProfileSet> {
    aws_config::profile::load(
        &aws_types::os_shim_internal::Fs::default(),
        &aws_types::os_shim_internal::Env::default(),
        &aws_runtime::env_config::file::EnvConfigFiles::default(),
        None,
    )
    .await
    .ok()
}

/// The general endpoint setting and its source, read as the SDK's endpoint
/// provider reads it (aws-config 1.8.13, `default_provider/endpoint_url.rs`):
/// `AWS_ENDPOINT_URL`, else the selected profile's `endpoint_url`.
fn general_endpoint_setting(
    profile: Option<&aws_config::profile::ProfileSet>,
) -> Option<(String, EndpointSource)> {
    if let Ok(endpoint) = std::env::var(GENERAL_ENDPOINT_ENV) {
        return Some((endpoint, EndpointSource::Env));
    }
    profile?
        .get(GENERAL_ENDPOINT_PROFILE_KEY)
        .map(|endpoint| (endpoint.to_owned(), EndpointSource::Profile))
}

/// Apply the endpoint rules to every endpoint the SDK's credential and
/// region providers can send to: the STS, SSO and SSO OIDC settings; the
/// general setting (`AWS_ENDPOINT_URL` or a profile's `endpoint_url`), which
/// any client without its own setting falls back to; and the IMDS endpoint
/// setting, with IMDS's own `http://` rule. Those clients have no request
/// check, so this check at `open` is the only one their endpoints get. It
/// runs whether or not the configured credentials use the service (an
/// `http://` setting meant for another tool fails `open`), since which
/// provider answers is decided only when the first request needs
/// credentials. `AWS_CONTAINER_CREDENTIALS_FULL_URI` keeps the SDK's own
/// rule (loopback `http://`, the two container addresses, or `https://`) and
/// is not checked here.
///
/// Returns whether any of those settings, the container URI included, is
/// `http://`, in which case `open` gives the chain the proxy-less client:
/// the default client would send such a request to the proxy named in the
/// environment before the endpoint's host saw it.
fn check_credential_endpoints(
    shared: &aws_config::SdkConfig,
    profile: Option<&aws_config::profile::ProfileSet>,
    allow_insecure: bool,
) -> Result<bool, StoreError> {
    let mut cleartext = false;
    for client in [Client::Sts, Client::Sso, Client::SsoOidc] {
        if let Some(endpoint) = endpoint_setting(shared, client) {
            let source = endpoint_source(profile, client, &endpoint);
            check_endpoint(&endpoint, client, source, allow_insecure, false)?;
            cleartext |= http_host(&endpoint).is_some();
        }
    }
    // Read from the environment and the profile rather than from
    // `shared.endpoint_url()`, which holds `endpoint` instead when that is
    // set.
    if let Some((endpoint, source)) = general_endpoint_setting(profile) {
        check_endpoint(&endpoint, Client::General, source, allow_insecure, false)?;
        cleartext |= http_host(&endpoint).is_some();
    }
    if let Some((endpoint, source)) = imds_endpoint_setting(profile) {
        check_endpoint(&endpoint, Client::Imds, source, allow_insecure, false)?;
        cleartext |= http_host(&endpoint).is_some();
    }
    if let Some(uri) = std::env::var_os("AWS_CONTAINER_CREDENTIALS_FULL_URI") {
        cleartext |= uri.to_str().is_some_and(|uri| http_host(uri).is_some());
    }
    Ok(cleartext)
}

/// The IMDS endpoint setting and its source, read as the SDK's IMDS client
/// reads it (aws-config 1.8.13, `imds/client.rs`, `EndpointSource::Env`):
/// `AWS_EC2_METADATA_SERVICE_ENDPOINT`, else the selected profile's
/// `ec2_metadata_service_endpoint`. `None` when neither is set; the client
/// then uses `http://169.254.169.254` or `http://[fd00:ec2::254]` by its
/// endpoint mode, which is not checked.
fn imds_endpoint_setting(
    profile: Option<&aws_config::profile::ProfileSet>,
) -> Option<(String, EndpointSource)> {
    if let Ok(endpoint) = std::env::var(IMDS_ENDPOINT_ENV) {
        return Some((endpoint, EndpointSource::EnvService(Client::Imds)));
    }
    profile?
        .get(IMDS_ENDPOINT_PROFILE_KEY)
        .map(|endpoint| (endpoint.to_owned(), EndpointSource::ImdsProfile))
}

/// An HTTPS-capable client with the SDK's default TLS stack (rustls with
/// aws-lc) and no proxy: `aws_smithy_http_client::Builder::build_https`
/// leaves the proxy disabled when none is configured, where the SDK's
/// default client reads it from `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and
/// `NO_PROXY` (and their lowercase forms). `open` uses it for an `http://`
/// S3 endpoint, and for the credential chain's clients when one of their
/// endpoints is `http://`, so a clear-text request goes to the endpoint's
/// host and never to a proxy;
/// `http_endpoint_is_reached_without_the_environment_proxy` and
/// `credential_provider_requests_bypass_the_environment_proxy` in the
/// conformance suite hold it to that.
fn http_client_without_proxy() -> aws_sdk_s3::config::SharedHttpClient {
    use aws_smithy_http_client::tls::{Provider, rustls_provider::CryptoMode};
    aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
        .build_https()
}

/// The host of an `http://` URI (scheme matched in any case), or `None` for
/// any other scheme. User information, a port, a path, a query and a
/// fragment are stripped; an IPv6 host is returned without its brackets.
fn http_host(uri: &str) -> Option<&str> {
    let trimmed = uri.trim();
    let rest = trimmed
        .get(..7)
        .filter(|scheme| scheme.eq_ignore_ascii_case("http://"))
        .map(|_| &trimmed[7..])?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    Some(match host_port.strip_prefix('[') {
        Some(bracketed) => bracketed
            .split_once(']')
            .map_or(bracketed, |(host, _)| host),
        None => host_port
            .split_once(':')
            .map_or(host_port, |(host, _)| host),
    })
}

/// Apply the `http://` rule of the config module doc to an endpoint
/// `client` uses: an `http://` endpoint must name a loopback host (for the
/// IMDS endpoint, a loopback host or one of IMDS's two link-local
/// addresses), and the S3 endpoint needs `force_path_style` as well
/// (without it the SDK may address the bucket as a host name,
/// `<bucket>.<host>`, depending on the bucket name), unless the operator set
/// `allow_insecure_endpoint`, which logs a warning. `force_path_style` is
/// ignored for the other clients, which address no bucket. The endpoint is
/// first held to the syntax rule (`check_endpoint_syntax`). Every message
/// names the client and the source the endpoint came from, and the host
/// only, or the problem class, never the whole endpoint.
fn check_endpoint(
    endpoint: &str,
    client: Client,
    source: EndpointSource,
    allow_insecure: bool,
    force_path_style: bool,
) -> Result<(), StoreError> {
    check_endpoint_syntax(endpoint, client, source)?;
    let Some(host) = http_host(endpoint) else {
        return Ok(());
    };
    let allowed_host =
        is_loopback_host(host) || (client == Client::Imds && is_imds_link_local_host(host));
    let without_path_style = client == Client::S3 && !force_path_style;
    if allowed_host && !without_path_style {
        return Ok(());
    }
    if allow_insecure {
        tracing::warn!(
            host = %host,
            source = %source,
            client = %client,
            force_path_style,
            "an endpoint the backup store sends to is plain http:// to a host that is not a \
             recognised loopback form (or, for IMDS, its link-local address), or an http:// S3 \
             endpoint without force_path_style; requests and backup objects may cross the \
             network unencrypted (allow_insecure_endpoint = true)"
        );
        return Ok(());
    }
    if !allowed_host {
        let who = match client {
            Client::S3 => {
                "requests and backup objects would cross the network unencrypted".to_owned()
            }
            Client::General => "an AWS SDK client without its own endpoint setting, such as the \
                                credential providers' STS and SSO clients, would send to it \
                                unencrypted; open checks it whether or not the configured \
                                credentials use those services"
                .to_owned(),
            Client::Sts | Client::Sso | Client::SsoOidc => format!(
                "the AWS SDK's credential providers would send credential requests (a web \
                 identity token, an SSO access token, a signed AssumeRole) to it and read \
                 credentials from its answers unencrypted; open checks it whether or not the \
                 configured credentials use {}",
                client.service_id().unwrap_or_default()
            ),
            Client::Imds => "the AWS SDK's IMDS client would send its session token request to \
                             it and read credentials and the region from its answers \
                             unencrypted; open checks it whether or not the configured \
                             credentials or region come from IMDS"
                .to_owned(),
        };
        let (forms, fix) = if client == Client::Imds {
            (
                "a recognised loopback form or an IMDS address (use 127.x.y.z, ::1, localhost, \
                 169.254.169.254 or fd00:ec2::254)",
                "Remove the setting (the SDK then uses 169.254.169.254), name one of those \
                 hosts, or set allow_insecure_endpoint = true to accept it",
            )
        } else {
            (
                "a recognised loopback form (use 127.x.y.z, ::1 or localhost)",
                "Use https://, or set allow_insecure_endpoint = true to accept it",
            )
        };
        return Err(StoreError::Other(format!(
            "{client} (from {source}) uses http:// to {host}, which is not {forms}; {who}. {fix}"
        )));
    }
    Err(StoreError::Other(format!(
        "{client} (from {source}) uses http:// without force_path_style = true; the SDK then \
         chooses the addressing by bucket name and may send to <bucket>.{host} (it does for a \
         virtual-hostable bucket name on localhost and for an access point ARN on any custom \
         endpoint, and its choice may change), a name the resolver may look up in DNS rather \
         than map to loopback. Set force_path_style = true, or set allow_insecure_endpoint = \
         true to accept it"
    )))
}

/// `127.0.0.0/8`, `::1` and `localhost`, in the forms listed in the config
/// module doc; other spellings of a loopback address (`127.1`,
/// `[::ffff:127.0.0.1]`, a trailing dot) are not recognised.
fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.is_loopback())
        || host
            .parse::<std::net::Ipv6Addr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The two addresses IMDS answers at (`169.254.169.254` and
/// `[fd00:ec2::254]`; aws-config 1.8.13, `imds/client.rs`, `EndpointMode`),
/// which an `http://` IMDS endpoint setting may name besides a loopback
/// host: they are link-local and never leave the host's network segment.
fn is_imds_link_local_host(host: &str) -> bool {
    host.parse::<std::net::Ipv4Addr>()
        .is_ok_and(|ip| ip == std::net::Ipv4Addr::new(169, 254, 169, 254))
        || host
            .parse::<std::net::Ipv6Addr>()
            .is_ok_and(|ip| ip == std::net::Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x254))
}

/// Apply the endpoint syntax rule of the config module doc. The SDK accepts
/// any string at `open` and fails the first request with an error that
/// quotes the value, so each shape it would refuse, and each shape that
/// carries material that does not belong in an endpoint, is refused here:
/// a scheme-less `host:port` (the form some S3 clients' alias commands
/// take), a stray character (a trailing space pasted after the host), user
/// information (credentials come from the SDK's chain, never from the
/// endpoint), a query or a fragment, an IPv6 zone id, and anything else
/// `url::Url` refuses (the SDK's endpoint rules parse the endpoint with it
/// and with `http::Uri`). The
/// messages name the client, the source and the problem class and never
/// repeat the value; the scheme is not repeated either.
fn check_endpoint_syntax(
    endpoint: &str,
    client: Client,
    source: EndpointSource,
) -> Result<(), StoreError> {
    const FIX: &str = "write an absolute http:// or https:// URI with a host, an optional port \
                       and an optional path, such as https://s3.example.com:9000 (the value is \
                       not repeated here, since it may carry a credential)";
    let what = format!("{client} (from {source})");
    let refuse = |problem: &str| StoreError::Other(format!("{what} {problem}; {FIX}"));
    if endpoint.is_empty() {
        return Err(refuse("is empty"));
    }
    // `http://` alone parses as an empty string in `http::Uri`'s eyes; name
    // the missing host instead.
    let after_scheme = ["http://", "https://"].iter().find_map(|scheme| {
        endpoint
            .get(..scheme.len())
            .filter(|s| s.eq_ignore_ascii_case(scheme))
            .map(|_| &endpoint[scheme.len()..])
    });
    if after_scheme.is_some_and(|rest| rest.is_empty() || rest.starts_with(['/', '?', '#'])) {
        return Err(refuse("has no host"));
    }
    // `InvalidUri`'s Display names the class (invalid character, invalid
    // port, too long) without the input.
    let uri: http::Uri = endpoint
        .parse()
        .map_err(|e: http::uri::InvalidUri| refuse(&format!("is not a valid URI: {e}")))?;
    match uri.scheme_str() {
        None => {
            return Err(StoreError::Other(format!(
                "{what} has no scheme (a bare host or host:port is not accepted); write http:// \
                 or https:// in front of the host"
            )));
        }
        Some(scheme)
            if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") => {}
        Some(_) => {
            return Err(StoreError::Other(format!(
                "{what} uses a scheme other than http:// and https://, the only two supported \
                 (the value is not repeated here, since it may carry a credential)"
            )));
        }
    }
    if uri.host().is_none_or(str::is_empty) {
        return Err(refuse("has no host"));
    }
    let authority = uri.authority().map_or("", |a| a.as_str());
    // Any `@` in the authority: `user@`, `user:password@`, `:password@`.
    if authority.contains('@') {
        return Err(refuse(
            "carries user information (user@ or user:password@ before the host); the AWS SDK's \
             credential chain supplies the credentials, so remove it",
        ));
    }
    // `http::Uri` drops a port it cannot represent (`:99999` parses with no
    // port), so the port text is checked from the authority itself.
    let port_text = match authority.strip_prefix('[') {
        Some(bracketed) => {
            let (host, rest) = bracketed.split_once(']').unwrap_or((bracketed, ""));
            if host.contains('%') {
                return Err(refuse("has an IPv6 zone id (%25 in the bracketed host)"));
            }
            rest.strip_prefix(':')
        }
        None => authority.split_once(':').map(|(_, port)| port),
    };
    if port_text.is_some_and(|port| port.parse::<u16>().is_err()) {
        return Err(refuse("has a port outside 0-65535"));
    }
    // `ParseError`'s Display names the class (invalid IPv4 address, invalid
    // international domain name) without the input.
    let url = url::Url::parse(endpoint).map_err(|e| refuse(&format!("is not a valid URL: {e}")))?;
    if url.query().is_some() {
        return Err(refuse("has a query component (? after the host or path)"));
    }
    if url.fragment().is_some() {
        return Err(refuse("has a fragment (# after the host or path)"));
    }
    Ok(())
}

/// The refusal `RefuseCleartextRequests` returns for a request URI: `None`
/// for `https://` and for an `http://` request to a recognised loopback
/// host.
fn cleartext_refusal(uri: &str) -> Option<String> {
    let host = http_host(uri)?;
    if is_loopback_host(host) {
        return None;
    }
    Some(format!(
        "refusing to send a plain http:// request to {host}, which is not a recognised \
         loopback form; requests and backup objects would cross the network unencrypted. Use \
         https://, or set allow_insecure_endpoint = true to accept it"
    ))
}

/// Installed on every client opened without `allow_insecure_endpoint`:
/// refuses, before it is sent, any `http://` request whose host is not a
/// recognised loopback form. `open` already applies the rule to the endpoint
/// it read; this checks the URI the SDK actually built (the host after
/// virtual-hosted addressing, an endpoint source `effective_endpoint` did
/// not know about).
#[derive(Debug)]
struct RefuseCleartextRequests;

impl aws_sdk_s3::config::Intercept for RefuseCleartextRequests {
    fn name(&self) -> &'static str {
        "RefuseCleartextRequests"
    }

    fn read_before_transmit(
        &self,
        context: &aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &aws_sdk_s3::config::RuntimeComponents,
        _cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        match cleartext_refusal(context.request().uri()) {
            Some(refusal) => Err(refusal.into()),
            None => Ok(()),
        }
    }
}

/// A request the bucket policy or IAM refused (`403` or `AccessDenied`), as
/// opposed to a credential problem such as an expired token.
fn is_access_denied<E: ProvideErrorMetadata>(err: &SdkError<E>) -> bool {
    matches!(err, SdkError::ServiceError(ctx)
        if ctx.err().code() == Some("AccessDenied")
            || (ctx.raw().status().as_u16() == 403 && !is_token_code(ctx.err().code())))
}

/// Codes S3 returns for a session token that is malformed, expired, or must
/// be refreshed. They mean the credentials are unusable, whatever the status.
fn is_token_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("InvalidToken" | "ExpiredToken" | "TokenRefreshRequired")
    )
}

/// An `AbortMultipartUpload` (or any upload request) answered `NoSuchUpload`:
/// the upload was completed or aborted already.
fn is_no_such_upload<E: ProvideErrorMetadata>(err: &SdkError<E>) -> bool {
    matches!(err, SdkError::ServiceError(ctx) if ctx.err().code() == Some("NoSuchUpload"))
}

/// Best-effort abort of a multipart upload, after a put failure or from a
/// drop guard; the original failure (if any) is what the caller needs, so
/// an abort error is only logged. `NoSuchUpload` means the upload is no
/// longer open (a `CompleteMultipartUpload` that S3 committed before the put
/// was dropped, a sweep that aborted it first), which is not a failure.
async fn abort_upload(client: &aws_sdk_s3::Client, bucket: &str, full_key: &str, upload_id: &str) {
    match client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(full_key)
        .upload_id(upload_id)
        .send()
        .await
    {
        Ok(_) => {}
        Err(abort_err) if is_no_such_upload(&abort_err) => {
            tracing::info!(key = %full_key, "abort skipped: upload no longer open");
        }
        Err(abort_err) => {
            tracing::warn!(
                key = %full_key,
                error = %map_sdk_error("AbortMultipartUpload", abort_err),
                "failed to abort multipart upload"
            );
        }
    }
}

/// Aborts a multipart upload from `Drop` unless disarmed. A put whose future
/// is dropped between `CreateMultipartUpload` and the inline abort or
/// complete would otherwise leave the upload open forever (S3 bills its
/// parts and `delete_prefix`'s object listing cannot see it). The abort is
/// spawned, since `Drop` cannot await; it runs on the runtime the put ran on,
/// and only while that runtime keeps running (see `Drop` below).
struct AbortOnDrop {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    upload_id: String,
    armed: bool,
}

impl AbortOnDrop {
    fn new(client: aws_sdk_s3::Client, bucket: &str, key: &str, upload_id: &str) -> Self {
        Self {
            client,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let client = self.client.clone();
        let (bucket, key, upload_id) = (
            self.bucket.clone(),
            self.key.clone(),
            self.upload_id.clone(),
        );
        // During runtime shutdown tokio drops each task's future with the
        // runtime context still entered, so `try_current` succeeds and the
        // task spawned here is dropped without ever being polled: no abort
        // is sent. A `delete_prefix` covering the key, or the bucket's
        // lifecycle rule, removes such an upload (module doc). A drop with
        // no runtime at all (`try_current` fails) has nowhere to run it
        // either.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                abort_upload(&client, &bucket, &key, &upload_id).await;
            });
        }
    }
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

/// S3 returns `ETag`s wrapped in double quotes (`"d41d8..."`); callers store the
/// bare value, which is also how `DynamoDB` export manifests record it.
fn strip_etag_quotes(etag: &str) -> String {
    etag.trim_matches('"').to_owned()
}

/// A timestamp outside `SystemTime`'s range is reported as absent rather than
/// as the epoch.
fn smithy_time_to_system(dt: &aws_sdk_s3::primitives::DateTime) -> Option<std::time::SystemTime> {
    std::time::SystemTime::try_from(*dt).ok()
}

/// Reduce an SDK error to a [`StoreError`].
///
/// Service errors are classified by HTTP status and error code and formatted
/// from the code and S3's service message only (a denial's message names the
/// action, the principal's ARN and an explicit deny, which an operator needs
/// to fix a KMS key or bucket policy; it carries no credential), so no
/// request id, request or credential material reaches the Display output.
/// Everything else (connection, timeout, response deserialization) becomes
/// [`StoreError::Transport`].
fn map_sdk_error<E>(operation: &'static str, err: SdkError<E>) -> StoreError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match err {
        SdkError::ServiceError(ctx) => {
            let status = ctx.raw().status().as_u16();
            // Without an S3 error code (a HEAD response has no body; a proxy
            // or load balancer answered with an HTML page) the status is the
            // one fact the response carried, so it stands in for the code.
            let code = ctx.err().code().map_or_else(
                || format!("HTTP {status} (no error code in the response)"),
                str::to_owned,
            );
            let denied = || match ctx.err().message().filter(|m| !m.is_empty()) {
                Some(message) => format!("s3 {operation} was denied: {code}: {message}"),
                None => format!("s3 {operation} was denied: {code}"),
            };
            match (status, ctx.err().code()) {
                // A missing bucket is not a missing object: `get`, `list`,
                // `put` and `delete_prefix` must not say "object not found"
                // when the bucket has gone away after startup. HEAD responses
                // carry no error body, so `head` on a missing bucket still
                // reads as a 404 and reports the key absent; `validate` is
                // the check that names a missing bucket.
                (_, Some("NoSuchBucket")) => StoreError::Other(format!(
                    "s3 {operation} failed: NoSuchBucket: the backup bucket does not exist"
                )),
                // An upload aborted elsewhere (a lifecycle rule, a second
                // abort): not a missing object, which the trait reserves
                // for `get`.
                (_, Some("NoSuchUpload")) => StoreError::Other(format!(
                    "s3 {operation} failed: NoSuchUpload: the multipart upload is no longer open"
                )),
                // A malformed, expired or stale session token: the
                // credentials are unusable, which is the operator problem a
                // denial names. S3 sends these with 400 as well as 403.
                (_, token) if is_token_code(token) => StoreError::PermissionDenied(denied()),
                (404, _) | (_, Some("NoSuchKey" | "NotFound")) => StoreError::NotFound,
                // The code tells an operator which credential problem it is
                // (AccessDenied, InvalidAccessKeyId, SignatureDoesNotMatch,
                // RequestTimeTooSkewed); keep it. A HEAD response has no body
                // and the SDK synthesizes a code only for 404, so `head` on a
                // denied bucket reads "HTTP 403 (no error code in the
                // response)"; `validate` uses a GET-shaped request so startup
                // names the problem.
                (403, _) | (_, Some("AccessDenied")) => StoreError::PermissionDenied(denied()),
                _ => match ctx.err().message().filter(|m| !m.is_empty()) {
                    Some(message) => {
                        StoreError::Other(format!("s3 {operation} failed: {code}: {message}"))
                    }
                    None => StoreError::Other(format!("s3 {operation} failed: {code}")),
                },
            }
        }
        other => StoreError::Transport {
            source: Box::new(other),
        },
    }
}

/// The SDK's non-service errors display as a bare category ("dispatch
/// failure"); the cause (no region configured, no credentials, connection
/// refused) is in the `source()` chain. Join the chain so an operator reads
/// the cause. Each link's Display is a message without request or credential
/// material (the SDK keeps those in Debug output, which this never uses).
pub(crate) fn describe_error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![error.to_string()];
    let mut current = error.source();
    while let Some(next) = current {
        let text = next.to_string();
        if parts.last() != Some(&text) {
            parts.push(text);
        }
        current = next.source();
    }
    parts.join(": ")
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

    /// A service error as the SDK builds it from a response: the status of
    /// the raw response and the code and message parsed from its body.
    fn service_error(
        status: u16,
        code: Option<&str>,
    ) -> SdkError<aws_sdk_s3::operation::get_object::GetObjectError> {
        service_error_with(status, code, Some("service message"))
    }

    fn service_error_with(
        status: u16,
        code: Option<&str>,
        message: Option<&str>,
    ) -> SdkError<aws_sdk_s3::operation::get_object::GetObjectError> {
        let mut meta = aws_sdk_s3::error::ErrorMetadata::builder();
        if let Some(message) = message {
            meta = meta.message(message);
        }
        if let Some(code) = code {
            meta = meta.code(code);
        }
        let raw = aws_sdk_s3::config::http::HttpResponse::new(
            status.try_into().expect("valid status"),
            aws_sdk_s3::primitives::SdkBody::empty(),
        );
        SdkError::service_error(
            aws_sdk_s3::operation::get_object::GetObjectError::generic(meta.build()),
            raw,
        )
    }

    /// Every cell of `map_sdk_error`'s table, from synthetic service errors,
    /// so the mapping is pinned without an endpoint.
    #[test]
    fn sdk_error_mapping_table() {
        let other = |status, code: Option<&str>, needle: &str| match map_sdk_error(
            "Op",
            service_error(status, code),
        ) {
            StoreError::Other(msg) => assert!(msg.contains(needle), "{status} {code:?}: {msg}"),
            e => panic!("{status} {code:?}: expected Other, got {e:?}"),
        };
        let denied = |status, code: Option<&str>, needle: &str| match map_sdk_error(
            "Op",
            service_error(status, code),
        ) {
            StoreError::PermissionDenied(msg) => {
                assert!(msg.contains(needle), "{status} {code:?}: {msg}");
            }
            e => panic!("{status} {code:?}: expected PermissionDenied, got {e:?}"),
        };
        let not_found = |status, code: Option<&str>| {
            let e = map_sdk_error("Op", service_error(status, code));
            assert!(
                matches!(e, StoreError::NotFound),
                "{status} {code:?}: {e:?}"
            );
        };

        // A missing bucket or upload is never a missing object.
        other(404, Some("NoSuchBucket"), "NoSuchBucket");
        other(404, Some("NoSuchUpload"), "NoSuchUpload");
        other(400, Some("NoSuchUpload"), "NoSuchUpload");
        // A bare 404 (a HEAD response has no body) and the object codes.
        not_found(404, None);
        not_found(404, Some("NoSuchKey"));
        not_found(400, Some("NoSuchKey"));
        not_found(404, Some("NotFound"));
        // A denial keeps the code and S3's message (which names the action
        // and the policy or KMS key behind it), or says "unknown" without a
        // code.
        denied(403, Some("AccessDenied"), "AccessDenied: service message");
        denied(400, Some("AccessDenied"), "AccessDenied: service message");
        denied(
            403,
            Some("InvalidAccessKeyId"),
            "InvalidAccessKeyId: service message",
        );
        denied(
            403,
            Some("SignatureDoesNotMatch"),
            "SignatureDoesNotMatch: service message",
        );
        denied(
            403,
            None,
            "HTTP 403 (no error code in the response): service message",
        );
        // A HEAD response has no body: no code and no message, so the status
        // is what the message carries.
        match map_sdk_error("Op", service_error_with(403, None, None)) {
            StoreError::PermissionDenied(msg) => assert_eq!(
                msg,
                "s3 Op was denied: HTTP 403 (no error code in the response)"
            ),
            e => panic!("bare 403: expected PermissionDenied, got {e:?}"),
        }
        // Session token problems, at any status.
        for code in ["InvalidToken", "ExpiredToken", "TokenRefreshRequired"] {
            for status in [400, 403] {
                denied(status, Some(code), &format!("{code}: service message"));
            }
        }
        // Anything else keeps code and message.
        other(500, Some("InternalError"), "InternalError: service message");
        other(503, Some("SlowDown"), "SlowDown");
        other(400, Some("InvalidArgument"), "InvalidArgument");
        // No code (an HTML error page from a proxy, or moto's 500 on an
        // aborted upload): the status stands in for it, with or without a
        // message.
        other(
            500,
            None,
            "HTTP 500 (no error code in the response): service message",
        );
        match map_sdk_error("Op", service_error_with(500, None, None)) {
            StoreError::Other(msg) => assert_eq!(
                msg,
                "s3 Op failed: HTTP 500 (no error code in the response)"
            ),
            e => panic!("bare 500: expected Other, got {e:?}"),
        }
        match map_sdk_error("Op", service_error_with(502, None, Some(""))) {
            StoreError::Other(msg) => assert_eq!(
                msg,
                "s3 Op failed: HTTP 502 (no error code in the response)"
            ),
            e => panic!("bare 502: expected Other, got {e:?}"),
        }
        match map_sdk_error("Op", service_error_with(503, Some("SlowDown"), None)) {
            StoreError::Other(msg) => assert_eq!(msg, "s3 Op failed: SlowDown"),
            e => panic!("SlowDown without a message: expected Other, got {e:?}"),
        }

        // Non-service failures are transport errors.
        let timeout: SdkError<aws_sdk_s3::operation::get_object::GetObjectError> =
            SdkError::timeout_error("slow");
        assert!(matches!(
            map_sdk_error("Op", timeout),
            StoreError::Transport { .. }
        ));
    }

    /// The sweep tolerates only a real denial of `ListMultipartUploads`, not
    /// an expired token or a server error.
    #[test]
    fn access_denied_excludes_token_codes() {
        assert!(is_access_denied(&service_error(403, Some("AccessDenied"))));
        assert!(is_access_denied(&service_error(403, None)));
        assert!(is_access_denied(&service_error(400, Some("AccessDenied"))));
        for code in ["InvalidToken", "ExpiredToken", "TokenRefreshRequired"] {
            assert!(!is_access_denied(&service_error(403, Some(code))), "{code}");
            assert!(!is_access_denied(&service_error(400, Some(code))), "{code}");
        }
        assert!(!is_access_denied(&service_error(
            500,
            Some("InternalError")
        )));
        assert!(!is_access_denied(&service_error(404, Some("NoSuchBucket"))));
    }

    /// An `http://` S3 endpoint needs path-style addressing, whatever its
    /// host: without it the SDK chooses the addressing by bucket name
    /// (aws-sdk-s3 1.122.0, `config/endpoint/internals.rs`), and sends to
    /// `<bucket>.<host>` for a virtual-hostable name on `localhost` and for
    /// an access point ARN on any custom endpoint (`<name>-<account>.<host>`);
    /// on an IP host it uses path style for every plain bucket name, a
    /// dotted one included, and never virtual-hosts a dotted name anywhere.
    /// The rule is keyed on the scheme, not on the host's form or the
    /// bucket's, because that choice is the SDK's and may change. Each cell
    /// goes through `open` with its bucket: refused without path style
    /// (naming the setting), accepted with it or with the override;
    /// `https://` is not clear text and needs neither.
    #[tokio::test]
    async fn http_without_path_style_is_refused() {
        let config =
            |bucket: &str, endpoint: &str, path_style: bool, insecure: bool| crate::S3StoreConfig {
                bucket: bucket.to_owned(),
                prefix: String::new(),
                region: Some("us-east-1".to_owned()),
                endpoint: Some(endpoint.to_owned()),
                allow_insecure_endpoint: insecure,
                force_path_style: path_style,
                upload_part_size_bytes: MIN_PART_SIZE_BYTES,
                max_concurrent_uploads: 1,
            };
        for (bucket, endpoint) in [
            ("b", "http://localhost:9000"),
            ("b", "http://LocalHost"),
            ("b", "http://localhost:9/"),
            // A dotted name on an IP host: the SDK addresses it path-style
            // (http://127.0.0.1:9/evil.example/k); the rule refuses it all
            // the same, since it is keyed on the scheme.
            ("evil.example", "http://127.0.0.1:9"),
            ("my.backups.example", "http://[::1]:9"),
            // An access point ARN on a custom endpoint: the SDK builds
            // http://ap-123456789012.127.0.0.1:9/k.
            (
                "arn:aws:s3:us-east-1:123456789012:accesspoint/ap",
                "http://127.0.0.1:9",
            ),
            ("b", "http://127.0.0.1:9000"),
        ] {
            let cell = format!("{bucket} on {endpoint}");
            let text = match S3Store::open(&config(bucket, endpoint, false, false)).await {
                Ok(_) => panic!("{cell} without path style must be refused"),
                Err(e) => e.to_string(),
            };
            assert!(
                text.contains("without force_path_style = true")
                    && text.contains("<bucket>.")
                    && text.contains("allow_insecure_endpoint")
                    && text.contains("(from [backup.store.s3] endpoint)"),
                "{cell}: {text}"
            );
            S3Store::open(&config(bucket, endpoint, true, false))
                .await
                .unwrap_or_else(|e| panic!("{cell} with path style: {e}"));
            S3Store::open(&config(bucket, endpoint, false, true))
                .await
                .unwrap_or_else(|e| panic!("{cell} with the override: {e}"));
        }
        for (bucket, endpoint) in [
            ("b", "https://localhost:9000"),
            ("evil.example", "https://127.0.0.1:9000"),
        ] {
            S3Store::open(&config(bucket, endpoint, false, false))
                .await
                .unwrap_or_else(|e| panic!("{bucket} on {endpoint} must be accepted: {e}"));
        }
        // The credential clients address no bucket: path style is not part
        // of their rule.
        check_endpoint(
            "http://localhost:9",
            Client::Sts,
            EndpointSource::Env,
            false,
            false,
        )
        .expect("a loopback STS endpoint needs no path style");
    }

    /// Every refusal names the client and the source the endpoint came
    /// from, so an operator who never set `endpoint` is pointed at the
    /// setting that supplied it.
    #[test]
    fn refusals_name_the_endpoint_source() {
        for (source, name) in [
            (EndpointSource::Config, "[backup.store.s3] endpoint"),
            (
                EndpointSource::EnvService(Client::S3),
                "AWS_ENDPOINT_URL_S3",
            ),
            (
                EndpointSource::EnvService(Client::Sts),
                "AWS_ENDPOINT_URL_STS",
            ),
            (
                EndpointSource::EnvService(Client::Sso),
                "AWS_ENDPOINT_URL_SSO",
            ),
            (
                EndpointSource::EnvService(Client::SsoOidc),
                "AWS_ENDPOINT_URL_SSO_OIDC",
            ),
            (EndpointSource::Env, "AWS_ENDPOINT_URL"),
            (
                EndpointSource::Profile,
                "endpoint_url in the shared AWS config file's profile",
            ),
            (
                EndpointSource::ProfileServices(Client::S3),
                "endpoint_url under s3 in the shared AWS config file's services section",
            ),
            (
                EndpointSource::ProfileServices(Client::SsoOidc),
                "endpoint_url under sso-oidc in the shared AWS config file's services section",
            ),
        ] {
            let needle = format!("(from {name})");
            for client in [
                Client::S3,
                Client::Sts,
                Client::Sso,
                Client::SsoOidc,
                Client::General,
            ] {
                let mut cases = vec![
                    "http://10.0.0.5:9",
                    "127.0.0.1:9",
                    "https://u:S3cretPW@127.0.0.1:9",
                ];
                if client == Client::S3 {
                    cases.push("http://localhost:9");
                }
                for endpoint in cases {
                    let text = check_endpoint(endpoint, client, source, false, false)
                        .expect_err(endpoint)
                        .to_string();
                    assert!(
                        text.contains(&needle) && text.starts_with(&client.to_string()),
                        "{endpoint} for {client} from {name}: {text}"
                    );
                }
            }
        }
    }

    /// The request-time check sees the URI the SDK built, virtual-hosted
    /// host included.
    #[test]
    fn cleartext_requests_to_other_hosts_are_refused() {
        for uri in [
            "http://10.0.0.5:9/b/k",
            "HTTP://10.0.0.5/b/k",
            "http://cfg-bucket.localhost:9/k",
            "http://s3.example.com/k?x-id=GetObject",
            "http://127.0.0.1.evil.example/k",
        ] {
            let refusal = cleartext_refusal(uri).unwrap_or_else(|| panic!("{uri} must be refused"));
            assert!(
                refusal.contains("allow_insecure_endpoint"),
                "{uri}: {refusal}"
            );
        }
        for uri in [
            "https://10.0.0.5/b/k",
            "http://127.0.0.1:9/b/k",
            "http://localhost:9/b/k",
            "http://[::1]:9/b/k",
        ] {
            assert_eq!(cleartext_refusal(uri), None, "{uri}");
        }
    }

    #[test]
    fn loopback_hosts() {
        for host in ["127.0.0.1", "127.1.2.3", "localhost", "LocalHost", "::1"] {
            assert!(is_loopback_host(host), "{host}");
        }
        for host in [
            "128.0.0.1",
            "10.0.0.1",
            "::2",
            "example.com",
            "localhost.example",
            "",
        ] {
            assert!(!is_loopback_host(host), "{host}");
        }
    }

    /// `open` refuses an endpoint that breaks the syntax rule, names the
    /// problem class, and never repeats the value, so a password in a
    /// refused endpoint stays out of the log. One case per class, and a
    /// secret placed where each class carries it.
    #[test]
    fn malformed_endpoints_are_refused_without_being_echoed() {
        let cases: [(&str, &str); 24] = [
            ("", "is empty"),
            ("127.0.0.1:9200", "has no scheme"),
            ("minio.example", "has no scheme"),
            ("ftp://127.0.0.1", "a scheme other than"),
            ("S3cretPW://127.0.0.1:9", "a scheme other than"),
            ("https://u:S3cretPW@h:1 ", "not a valid URI"),
            ("https://u:S3cretPW@h:1/a b", "not a valid URI"),
            ("http://", "has no host"),
            ("https:///bucket", "has no host"),
            ("http://u:S3cretPW@h:99999", "user information"),
            ("http://h:99999/S3cretPW", "port outside"),
            // User information in every form.
            ("https://u:S3cretPW@127.0.0.1:19219", "user information"),
            ("http://u:S3cretPW@127.0.0.1:19219", "user information"),
            ("https://S3cretPW@127.0.0.1:19219", "user information"),
            ("https://:S3cretPW@127.0.0.1:19219", "user information"),
            (
                "https://u:S3cretPW@127.0.0.1:19219/?a=b",
                "user information",
            ),
            (
                "http://u:S3cretPW@127.0.0.1:19219/?X-Amz-Signature=abc",
                "user information",
            ),
            ("https://u:S3cretPW@[::1%25lo]:19219", "user information"),
            // The other classes, each alone.
            ("https://127.0.0.1:19219/?a=S3cretPW", "query component"),
            ("http://evil.example?@127.0.0.1", "query component"),
            ("https://127.0.0.1:19219/#S3cretPW", "fragment"),
            ("http://evil.example#@127.0.0.1", "fragment"),
            ("https://[::1%25S3cretPW]:19219", "IPv6 zone id"),
            ("https://256.0.0.1:19219/S3cretPW", "not a valid URL"),
        ];
        for (endpoint, needle) in cases {
            let err = check_endpoint(endpoint, Client::S3, EndpointSource::Config, false, true)
                .expect_err(endpoint);
            let text = err.to_string();
            assert!(text.contains(needle), "{endpoint:?}: {text}");
            assert!(
                text.contains("http://") && text.contains("https://"),
                "{endpoint:?}: the message names the accepted schemes: {text}"
            );
            // (`http://` itself is a scheme the message names on purpose.)
            if endpoint.len() > "https://".len() {
                assert!(
                    !text.contains(endpoint.trim()),
                    "{endpoint:?}: the message repeats the endpoint: {text}"
                );
            }
            assert!(
                !text.contains("S3cretPW"),
                "{endpoint:?}: the message carries the secret: {text}"
            );
            // The override is about clear text, not syntax.
            check_endpoint(endpoint, Client::S3, EndpointSource::Config, true, true)
                .expect_err(endpoint);
        }
        for endpoint in [
            "http://127.0.0.1:9200",
            "HTTP://127.0.0.1:9200/",
            "https://minio.example:9000/path",
            "https://[::1]:9000",
            "https://s3.us-east-1.amazonaws.com",
        ] {
            check_endpoint_syntax(endpoint, Client::S3, EndpointSource::Config)
                .unwrap_or_else(|e| panic!("{endpoint} is a valid endpoint: {e}"));
        }
    }
}
