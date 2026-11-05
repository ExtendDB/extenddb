// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The S3 clients of one store: the client for the configured region, and
//! one per region a bucket was found in. AWS S3 answers a request signed
//! for the wrong region with a 301 `PermanentRedirect` (or a 400
//! `AuthorizationHeaderMalformed`) naming the bucket's region in
//! `x-amz-bucket-region`; the clients remember that region for the bucket
//! and send the request once more there (measured on the service: an input
//! bucket and an output bucket in another region both work).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Mutex, MutexGuard, PoisonError};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::{Region, SharedHttpClient};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};

use crate::retry::{Backoff, RetryPolicy, is_transient};

/// Bucket regions remembered at most. Past it the map starts again empty,
/// so a caller naming many buckets cannot grow it without bound.
const MAX_REMEMBERED_BUCKETS: usize = 1024;

/// The clients of one S3 store.
pub struct S3Clients {
    config: aws_sdk_s3::Config,
    default: Client,
    /// Whether region redirects are followed: on AWS S3, not against a
    /// configured endpoint.
    follow_redirects: bool,
    /// Whether the client sends checksums only when an operation requires
    /// them, the setting for an S3-compatible endpoint.
    checksums_when_required: bool,
    bucket_regions: Mutex<HashMap<String, Region>>,
    regional: Mutex<HashMap<String, Client>>,
    /// The bound on each call's retries of transient failures.
    retry: RetryPolicy,
    /// The access key ID of the credentials the startup check resolved.
    access_key_id: Option<String>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl S3Clients {
    /// The clients over `config`, the configuration of the store's region.
    pub(crate) fn from_config(
        config: aws_sdk_s3::Config,
        follow_redirects: bool,
        checksums_when_required: bool,
    ) -> Self {
        Self {
            default: Client::from_conf(config.clone()),
            config,
            follow_redirects,
            checksums_when_required,
            bucket_regions: Mutex::new(HashMap::new()),
            regional: Mutex::new(HashMap::new()),
            retry: RetryPolicy::STORE,
            access_key_id: None,
        }
    }

    /// The clients with the access key ID the startup check resolved.
    pub(crate) fn with_access_key_id(mut self, access_key_id: Option<String>) -> Self {
        self.access_key_id = access_key_id;
        self
    }

    /// The access key ID of the credentials the startup check resolved, or
    /// `None` when they did not resolve, for a caller that compares it with
    /// keys of its own. Nothing in this crate logs it.
    #[must_use]
    pub fn access_key_id(&self) -> Option<&str> {
        self.access_key_id.as_deref()
    }

    /// The retries of one call, from now under the store's bound.
    #[must_use]
    pub fn backoff(&self) -> Backoff {
        self.retry.start()
    }

    /// The HTTP client the clients send over, when one was set on their
    /// configuration rather than left to the SDK's default.
    pub(crate) fn http_client(&self) -> Option<SharedHttpClient> {
        self.config.http_client()
    }

    /// Whether the client sends checksums only when an operation requires
    /// them; multipart uploads then declare no checksum algorithm.
    #[must_use]
    pub fn checksums_when_required(&self) -> bool {
        self.checksums_when_required
    }

    /// The client of the region `bucket` was found in, or of the store's
    /// region for a bucket no redirect has named. The region is learned by
    /// [`Self::call`] and [`Self::call_within`], which follow a redirect
    /// once; a request sent on this client alone gets the redirect as its
    /// answer.
    #[must_use]
    pub fn client_for(&self, bucket: &str) -> Client {
        let region = lock(&self.bucket_regions).get(bucket).cloned();
        match region {
            Some(region) => self.regional_client(region),
            None => self.default.clone(),
        }
    }

    fn regional_client(&self, region: Region) -> Client {
        if self.config.region() == Some(&region) {
            return self.default.clone();
        }
        lock(&self.regional)
            .entry(region.to_string())
            .or_insert_with(|| Client::from_conf(self.config.to_builder().region(region).build()))
            .clone()
    }

    /// Run one S3 call for `bucket` on the client of the bucket's region.
    /// A redirect naming another region is remembered for the bucket, and
    /// the call runs once more on that region's client. A transient
    /// failure is sent again under the store's bound (`retry`).
    pub async fn call<T, E, F, Fut>(
        &self,
        bucket: &str,
        call: F,
    ) -> Result<T, SdkError<E, HttpResponse>>
    where
        F: Fn(Client) -> Fut,
        Fut: Future<Output = Result<T, SdkError<E, HttpResponse>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        self.call_within(bucket, &mut self.backoff(), &call).await
    }

    /// [`Self::call`] with the retries of a backoff the caller holds, so a
    /// ranged GET that is requested again after a cut body stays under one
    /// bound with the retries of its requests.
    pub async fn call_within<T, E, F, Fut>(
        &self,
        bucket: &str,
        backoff: &mut Backoff,
        call: &F,
    ) -> Result<T, SdkError<E, HttpResponse>>
    where
        F: Fn(Client) -> Fut,
        Fut: Future<Output = Result<T, SdkError<E, HttpResponse>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        loop {
            let result = self.call_once(bucket, call).await;
            let Err(error) = &result else {
                return result;
            };
            if !is_transient(error) {
                return result;
            }
            let Some(wait) = backoff.next_wait() else {
                return result;
            };
            tracing::warn!(
                bucket,
                retry = backoff.retries(),
                wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                error = %aws_sdk_s3::error::DisplayErrorContext(error),
                "S3 call failed with a transient error; it is sent again after the wait"
            );
            tokio::time::sleep(wait).await;
        }
    }

    async fn call_once<T, E, F, Fut>(
        &self,
        bucket: &str,
        call: &F,
    ) -> Result<T, SdkError<E, HttpResponse>>
    where
        F: Fn(Client) -> Fut,
        Fut: Future<Output = Result<T, SdkError<E, HttpResponse>>>,
        E: ProvideErrorMetadata,
    {
        let client = self.client_for(bucket);
        let used = client.config().region().cloned();
        let first = call(client).await;
        let Err(error) = &first else {
            return first;
        };
        let Some(region) = self
            .redirect_region(error)
            .filter(|r| Some(r) != used.as_ref())
        else {
            return first;
        };
        tracing::info!(
            bucket,
            region = %region,
            "S3 bucket is in another region; the request is sent there"
        );
        {
            let mut regions = lock(&self.bucket_regions);
            if regions.len() >= MAX_REMEMBERED_BUCKETS && !regions.contains_key(bucket) {
                regions.clear();
            }
            regions.insert(bucket.to_owned(), region.clone());
        }
        call(self.regional_client(region)).await
    }

    fn redirect_region<E: ProvideErrorMetadata>(
        &self,
        error: &SdkError<E, HttpResponse>,
    ) -> Option<Region> {
        if !self.follow_redirects {
            return None;
        }
        let response = error.raw_response()?;
        let status = response.status().as_u16();
        let redirect = matches!(status, 301 | 307)
            || (status == 400 && error.code() == Some("AuthorizationHeaderMalformed"));
        if !redirect {
            return None;
        }
        let region = response.headers().get("x-amz-bucket-region")?.trim();
        (!region.is_empty()).then(|| Region::new(region.to_owned()))
    }
}

/// The hooks a store's tests build clients with, behind the `test-util`
/// feature: production builds its clients through [`crate::clients_from`]
/// under the store's retry bound.
#[cfg(any(test, feature = "test-util"))]
impl S3Clients {
    /// The clients over `config`, the configuration of the store's region,
    /// for a test that builds its own configuration.
    #[must_use]
    pub fn new(
        config: aws_sdk_s3::Config,
        follow_redirects: bool,
        checksums_when_required: bool,
    ) -> Self {
        Self::from_config(config, follow_redirects, checksums_when_required)
    }

    /// Another bound on each call's retries, so a test sees retries without
    /// waiting seconds, or none at all.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// The configuration of the store's region, for the tests that check
    /// what the clients are built with.
    #[must_use]
    pub fn config(&self) -> &aws_sdk_s3::Config {
        &self.config
    }
}
