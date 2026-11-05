// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bulk put store's view of its S3 clients: the shared clients of
//! `extenddb-s3` (the region redirects, the retry bound, the startup
//! credential check), with the bytes of each ranged GET and uploaded part
//! and the deadline of the calls a Start makes.

use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use extenddb_s3::Startup;
#[cfg(test)]
pub(crate) use extenddb_s3::{CREDENTIALS_LATER, behavior_version, timeouts};

use super::retry::Backoff;
use crate::bulk_put::store::{S3_STORE_LABEL, S3StoreSettings};

/// How the bulk store names itself in its startup lines.
const STARTUP: Startup<'static> = Startup {
    label: S3_STORE_LABEL,
    until_resolved: "Start answers InternalServerError until they resolve",
};

/// How long one S3 call a Start makes may run, its retries included: the
/// bucket check, each page of the listing, and the encryption check with
/// its abort. A Start then answers within the time an SDK client waits
/// for a response. A call still running at the deadline is dropped, which
/// closes its connection, and fails as the server's, so the Start answers
/// `InternalServerError` and creates no operation. The runner's calls are
/// bound by the retry total alone.
pub(crate) const START_DEADLINE: Duration = Duration::from_secs(10);

/// Who makes an S3 call, which sets the bound it runs under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// A Start: the retry bound and [`START_DEADLINE`].
    Start,
    /// The runner: the retry bound alone.
    Run,
}

/// Build the store's clients from its settings and the standard AWS
/// credential chain (`extenddb_s3::connect`), the startup lines naming the
/// S3 bulk store.
pub(crate) async fn connect(settings: &S3StoreSettings) -> S3Clients {
    connect_with(settings, |builder| builder).await
}

/// [`connect`], with `adjust` applied to the S3 client configuration
/// before it is built, so the store contract tests run on the clients the
/// settings describe with an interceptor of their own.
pub(crate) async fn connect_with(
    settings: &S3StoreSettings,
    adjust: impl FnOnce(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder,
) -> S3Clients {
    S3Clients::from_shared(extenddb_s3::connect_with(settings, &STARTUP, adjust).await)
}

/// The store's clients over a loaded SDK configuration
/// (`extenddb_s3::clients_from`).
#[cfg(test)]
pub(crate) fn clients_from(
    settings: &S3StoreSettings,
    sdk: &aws_config::SdkConfig,
    adjust: impl FnOnce(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder,
) -> S3Clients {
    S3Clients::from_shared(extenddb_s3::clients_from(settings, sdk, adjust))
}

/// The clients of the S3 bulk store.
pub(crate) struct S3Clients {
    shared: extenddb_s3::S3Clients,
    /// Bytes asked for per ranged GET and sent per uploaded part.
    part_bytes: u64,
    /// How long one call a Start makes may run ([`START_DEADLINE`]).
    start_deadline: Duration,
}

impl S3Clients {
    /// The clients over `config`, the configuration of the store's region,
    /// for the tests that build their own configuration.
    #[cfg(test)]
    pub(crate) fn new(
        config: aws_sdk_s3::Config,
        follow_redirects: bool,
        checksums_when_required: bool,
    ) -> Self {
        Self::from_shared(extenddb_s3::S3Clients::new(
            config,
            follow_redirects,
            checksums_when_required,
        ))
    }

    fn from_shared(shared: extenddb_s3::S3Clients) -> Self {
        Self {
            shared,
            part_bytes: super::read::GET_PART_BYTES,
            start_deadline: START_DEADLINE,
        }
    }

    /// The retries of one call, from now under the store's bound.
    pub(crate) fn backoff(&self) -> Backoff {
        self.shared.backoff()
    }

    /// Test hook: another bound on each call's retries, so a test sees
    /// retries without waiting seconds, or none at all.
    #[cfg(test)]
    pub(crate) fn with_retry(mut self, retry: extenddb_s3::retry::RetryPolicy) -> Self {
        self.shared = self.shared.with_retry(retry);
        self
    }

    /// Test hook: another deadline for the calls a Start makes, so a test
    /// sees it end a call without waiting ten seconds.
    #[cfg(test)]
    pub(crate) fn with_start_deadline(mut self, deadline: Duration) -> Self {
        self.start_deadline = deadline;
        self
    }

    /// Bytes asked for per ranged GET and sent per uploaded part.
    pub(crate) fn part_bytes(&self) -> u64 {
        self.part_bytes
    }

    /// Test hook: smaller ranged GETs and upload parts, so a test reads
    /// and writes an object in several parts without megabytes of data.
    #[cfg(test)]
    pub(crate) fn with_part_bytes(mut self, bytes: u64) -> Self {
        self.part_bytes = bytes.max(1);
        self
    }

    /// Whether the client sends checksums only when an operation requires
    /// them; multipart uploads then declare no checksum algorithm.
    pub(crate) fn checksums_when_required(&self) -> bool {
        self.shared.checksums_when_required()
    }

    /// The access key ID of the credentials the startup check resolved
    /// (`extenddb_s3::S3Clients::access_key_id`).
    pub(crate) fn access_key_id(&self) -> Option<&str> {
        self.shared.access_key_id()
    }

    /// Run one S3 call for `bucket` (`extenddb_s3::S3Clients::call`): on
    /// the client of the bucket's region, a redirect followed once, a
    /// transient failure sent again under the store's bound.
    pub(crate) async fn call<T, E, F, Fut>(
        &self,
        bucket: &str,
        call: F,
    ) -> Result<T, SdkError<E, HttpResponse>>
    where
        F: Fn(Client) -> Fut,
        Fut: Future<Output = Result<T, SdkError<E, HttpResponse>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        self.shared.call(bucket, call).await
    }

    /// [`Self::call`] under the bound of `phase`. A Start's call still
    /// running at the Start deadline is dropped and answered as an SDK
    /// timeout, which the store maps to the server's failure.
    pub(crate) async fn call_in<T, E, F, Fut>(
        &self,
        phase: Phase,
        bucket: &str,
        call: F,
    ) -> Result<T, SdkError<E, HttpResponse>>
    where
        F: Fn(Client) -> Fut,
        Fut: Future<Output = Result<T, SdkError<E, HttpResponse>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        let bounded = self.call(bucket, call);
        match phase {
            Phase::Run => bounded.await,
            Phase::Start => match tokio::time::timeout(self.start_deadline, bounded).await {
                Ok(result) => result,
                Err(elapsed) => Err(SdkError::timeout_error(elapsed)),
            },
        }
    }

    /// [`Self::call`] with the retries of a backoff the caller holds, so a
    /// ranged GET that is requested again after a cut body stays under one
    /// bound with the retries of its requests.
    pub(crate) async fn call_within<T, E, F, Fut>(
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
        self.shared.call_within(bucket, backoff, call).await
    }
}
