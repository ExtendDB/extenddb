// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The startup identity check: STS GetCallerIdentity in the store's region,
//! signed with the credentials the startup check resolved and sent over
//! the HTTP client of the store's S3 clients. It runs in a task of its own
//! under a bound of its own, so it never delays startup, and only against
//! AWS S3: an S3-compatible service has no STS. The outcome is a log line
//! under the store's label, and no line carries the access key ID or the
//! secret.

use std::time::Duration;

use aws_config::SdkConfig;
use aws_sdk_sts::config::http::HttpResponse;
use aws_sdk_sts::config::{Credentials, Region, SharedHttpClient};
use aws_sdk_sts::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_sts::operation::get_caller_identity::{
    GetCallerIdentityError, GetCallerIdentityOutput,
};
use aws_types::service_config::ServiceConfigKey;

/// How long the identity check waits for STS.
const IDENTITY_CHECK: Duration = Duration::from_secs(5);

/// The codes STS refuses credentials with, as measured on AWS STS:
/// `InvalidClientTokenId` for an access key ID AWS does not know and for a
/// session token altered or left out, `SignatureDoesNotMatch` for a wrong
/// secret key, and `ExpiredToken` for a session token past its expiry. Every
/// other answer goes to the line that the credentials could not be checked,
/// with its code.
const REFUSED: &[&str] = &[
    "InvalidClientTokenId",
    "SignatureDoesNotMatch",
    "ExpiredToken",
];

/// How STS's `SignatureDoesNotMatch` message starts for a request signed
/// more than 15 minutes before or after STS's clock, as measured: the clock
/// is off, not the credentials.
const CLOCK_OFF: &[&str] = &["Signature expired:", "Signature not yet current:"];

/// What the logged text says in place of the access key ID, should an
/// answer repeat it.
const KEY_ELIDED: &str = "<access key ID>";

/// Start the identity check of `credentials` in `region` in a task of its
/// own, over `http_client`, the S3 clients' HTTP client (the SDK's default
/// when `None`); the outcome is logged under `label`. The call goes to the
/// endpoint set for STS alone ([`sts_endpoint`]), or to AWS STS for the
/// region: the global endpoint the SDK read from the environment or a
/// profile is cleared, as it is for the store's S3 clients.
pub(crate) fn spawn_check(
    sdk: &SdkConfig,
    http_client: Option<SharedHttpClient>,
    region: &str,
    credentials: Credentials,
    label: &str,
) {
    let key = credentials.access_key_id().to_owned();
    let mut builder = aws_sdk_sts::config::Builder::from(sdk)
        .region(Region::new(region.to_owned()))
        .credentials_provider(credentials);
    builder.set_http_client(http_client);
    builder.set_endpoint_url(sts_endpoint(sdk));
    let client = aws_sdk_sts::Client::from_conf(builder.build());
    let label = label.to_owned();
    tokio::spawn(async move {
        let answer =
            tokio::time::timeout(IDENTITY_CHECK, client.get_caller_identity().send()).await;
        log_identity(&label, &key, answer.ok());
    });
}

/// The endpoint the configuration sets for STS alone: `AWS_ENDPOINT_URL_STS`,
/// or the `sts` entry of the profile's `services` section. The SDK answers
/// a service's endpoint with that service's own setting when there is one
/// and with the global one otherwise (`AWS_ENDPOINT_URL`, then the
/// profile's `endpoint_url`), which clients set to ExtendDB; an answer
/// equal to the global endpoint is that one, and is not used. The SDK reads
/// `AWS_ENDPOINT_URL` before the profile, so with it set a profile's `sts`
/// entry is not seen.
fn sts_endpoint(sdk: &SdkConfig) -> Option<String> {
    let key = ServiceConfigKey::builder()
        .service_id("STS")
        .env("AWS_ENDPOINT_URL")
        .profile("endpoint_url")
        .build()
        .ok()?;
    let endpoint = sdk.service_config()?.load_config(key)?;
    (Some(endpoint.as_str()) != sdk.endpoint_url()).then_some(endpoint)
}

/// Log the outcome of one identity check: STS's answer, or `None` when no
/// answer came within the bound.
fn log_identity(
    label: &str,
    key: &str,
    answer: Option<Result<GetCallerIdentityOutput, SdkError<GetCallerIdentityError, HttpResponse>>>,
) {
    let elide = |text: &str| {
        if key.is_empty() {
            text.to_owned()
        } else {
            text.replace(key, KEY_ELIDED)
        }
    };
    match answer {
        Some(Ok(identity)) => tracing::info!(
            "{label} signs as {arn} (AWS account {account})",
            arn = elide(identity.arn().unwrap_or_default()),
            account = identity.account().unwrap_or_default(),
        ),
        Some(Err(error)) => match reading(&error) {
            Reading::Refused(code) => {
                // STS ends its message with a full stop; the line goes on
                // with a sentence of its own.
                let message = elide(error.message().unwrap_or_default());
                let message = message.trim_end().trim_end_matches('.');
                tracing::warn!(
                    "AWS refused the {label} credentials ({code}): {message}. Its S3 requests \
                     fail until the server restarts with credentials AWS accepts"
                );
            }
            Reading::ClockOff(code) => {
                let message = elide(error.message().unwrap_or_default());
                tracing::warn!(
                    "{label} credentials could not be checked with AWS STS: the server clock is \
                     more than 15 minutes off ({code}): {message}"
                );
            }
            Reading::Unchecked => {
                let cause = match (error.code(), error.raw_response()) {
                    (Some(code), Some(response)) => format!(
                        "{code} (HTTP {status}): {message}",
                        status = response.status().as_u16(),
                        message = error.message().unwrap_or_default(),
                    ),
                    _ => DisplayErrorContext(&error).to_string(),
                };
                let cause = elide(&cause);
                tracing::warn!("{label} credentials could not be checked with AWS STS: {cause}");
            }
        },
        None => tracing::warn!(
            "{label} credentials could not be checked with AWS STS: no answer within five seconds"
        ),
    }
}

/// What an STS error says of the credentials.
enum Reading<'a> {
    /// STS refused them, with this code.
    Refused(&'a str),
    /// STS refused the request's time, with this code: the server clock is
    /// off, which says nothing of the credentials.
    ClockOff(&'a str),
    /// Anything else, an answer STS did not give included.
    Unchecked,
}

/// Read an STS error: one of [`REFUSED`], a [`CLOCK_OFF`] message, or
/// neither.
fn reading<E: ProvideErrorMetadata>(error: &SdkError<E, HttpResponse>) -> Reading<'_> {
    let SdkError::ServiceError(_) = error else {
        return Reading::Unchecked;
    };
    let Some(code) = error.code() else {
        return Reading::Unchecked;
    };
    let message = error.message().unwrap_or_default();
    if code == "SignatureDoesNotMatch" && CLOCK_OFF.iter().any(|start| message.starts_with(start)) {
        Reading::ClockOff(code)
    } else if REFUSED.contains(&code) {
        Reading::Refused(code)
    } else {
        Reading::Unchecked
    }
}
