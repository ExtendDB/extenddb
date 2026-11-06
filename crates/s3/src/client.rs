// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Building one store's clients: the pinned behaviour version, the
//! deadlines, the region and endpoint of the settings, and the startup
//! credential check.

use std::time::Duration;

use aws_config::{ConfigLoader, SdkConfig};
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{
    BehaviorVersion, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};

use crate::S3StoreSettings;
use crate::clients::S3Clients;
use crate::credentials::{Startup, check_credentials, warn_of_temporary_keys};
use crate::identity;

/// The SDK behaviour version every store client is built and tested with.
/// It is pinned rather than `BehaviorVersion::latest()`, so an SDK update
/// cannot change the clients' retry, timeout or stalled-stream defaults
/// without a change here: a store may render what they decide, as the bulk
/// put store states one attempt for every mapped 4xx of a report write.
#[must_use]
pub fn behavior_version() -> BehaviorVersion {
    BehaviorVersion::v2026_01_12()
}

/// The connect timeout of each attempt: the SDK's default for the pinned
/// behaviour version, stated here so an SDK update cannot change it.
pub const CONNECT_TIMEOUT: Duration = Duration::from_millis(3100);

/// The wait for a response's head on each attempt, from the request's
/// dispatch. A body is not under this deadline: one that stops is ended by
/// the SDK's stalled-stream protection, on by default for the pinned
/// behaviour version (`aws-smithy-runtime` `client::defaults`, a 5-second
/// grace period for uploads and downloads).
pub const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// The deadlines every store client is built with.
#[must_use]
pub fn timeouts() -> TimeoutConfig {
    TimeoutConfig::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .build()
}

/// Build a store's clients from its settings and the standard AWS
/// credential chain. The region is set on the loader, so `AWS_REGION` does
/// not change it. Credentials are resolved once, within five seconds, and
/// the outcome logged under `startup`'s label; a failure is a warning, so a
/// slow instance metadata service does not stop the server. Resolved
/// temporary keys the SDK cannot renew are a warning too. Without an
/// `endpoint_url`, one STS GetCallerIdentity request is sent at startup to
/// log the identity the credentials sign as, in a task of its own that
/// never delays the return.
pub async fn connect(settings: &S3StoreSettings, startup: &Startup<'_>) -> S3Clients {
    connect_with(settings, startup, |builder| builder).await
}

/// [`connect`], with `adjust` applied to the S3 client configuration
/// before it is built, so a store's contract tests run on the clients the
/// settings describe with an interceptor of their own. Without an
/// `endpoint_url`, the startup GetCallerIdentity request goes over the
/// HTTP client of the built S3 clients, so an HTTP client `adjust` sets
/// carries it too: a test that gives the clients an in-process client
/// keeps that request in the process.
pub async fn connect_with(
    settings: &S3StoreSettings,
    startup: &Startup<'_>,
    adjust: impl FnOnce(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder,
) -> S3Clients {
    let sdk = loader(settings).load().await;
    connect_from(settings, &sdk, startup, adjust).await
}

/// The loader of the SDK configuration [`connect_with`] builds the clients
/// over: the pinned behaviour version and the store's region, which
/// `AWS_REGION` and a profile's region do not change, with the credential
/// chain and everything else read from the environment and the AWS config
/// files.
pub(crate) fn loader(settings: &S3StoreSettings) -> ConfigLoader {
    aws_config::defaults(behavior_version()).region(Region::new(settings.region.clone()))
}

/// [`connect_with`] over a loaded SDK configuration: the startup checks
/// of the credentials the configuration resolves, then the clients, which
/// keep the resolved access key ID. The identity check runs against AWS S3
/// only, in a task of its own, over the HTTP client of the clients.
pub(crate) async fn connect_from(
    settings: &S3StoreSettings,
    sdk: &SdkConfig,
    startup: &Startup<'_>,
    adjust: impl FnOnce(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder,
) -> S3Clients {
    let resolved = check_credentials(sdk, startup).await;
    if let Some(credentials) = &resolved {
        warn_of_temporary_keys(credentials, startup.label);
    }
    let access_key_id = resolved
        .as_ref()
        .map(|credentials| credentials.access_key_id().to_owned());
    let clients = clients_from(settings, sdk, adjust).with_access_key_id(access_key_id);
    if let (Some(credentials), None) = (resolved, &settings.endpoint_url) {
        identity::spawn_check(
            sdk,
            clients.http_client(),
            &settings.region,
            credentials,
            startup.label,
        );
    }
    clients
}

/// A store's clients over a loaded SDK configuration: the settings'
/// addressing style, the deadlines of [`timeouts`], and the endpoint of
/// `endpoint_url` alone. Against a configured endpoint checksums are sent
/// and checked only when an operation requires them, since several
/// S3-compatible services refuse the trailing checksums the SDK otherwise
/// sends, and no region redirects are followed. Without one, an endpoint
/// the SDK read from `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` or a
/// profile (`endpoint_url`, a `services` section) is cleared, so requests
/// go to AWS S3 for the region, as a store's startup log line says.
pub fn clients_from(
    settings: &S3StoreSettings,
    sdk: &SdkConfig,
    adjust: impl FnOnce(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder,
) -> S3Clients {
    let mut builder = aws_sdk_s3::config::Builder::from(sdk)
        .force_path_style(settings.force_path_style)
        .timeout_config(timeouts());
    let endpoint = settings.endpoint_url.is_some();
    if let Some(url) = &settings.endpoint_url {
        builder = builder
            .endpoint_url(url)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
    } else {
        builder.set_endpoint_url(None);
    }
    S3Clients::from_config(adjust(builder).build(), !endpoint, endpoint)
}
