// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The startup identity check. Each case builds the clients over an SDK
//! configuration with static keys whose HTTP client answers in-process (a
//! replay client, or one that never answers), so no request leaves the
//! test. The clock is paused: the five-second bound passes without a
//! five-second sleep, and a startup that waited for the check would show
//! it in the paused clock.

use std::time::Duration;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{Credentials, HttpClient, Region, SharedCredentialsProvider};
use aws_smithy_http_client::test_util::NeverClient;
use tracing::Level;

use super::logs::{Capture, Logged};
use super::*;
use crate::client::connect_from;

const KEY: &str = "AKIDIDENTITYTEST";
const SECRET: &str = "identity-test-secret";

/// The store's region, which is not the SDK configuration's.
const STORE_REGION: &str = "eu-central-1";

/// An SDK configuration with static keys, region us-east-1 and an endpoint
/// as `AWS_ENDPOINT_URL` would give it, over `http`.
fn sdk_over(http: impl HttpClient + 'static) -> SdkConfig {
    SdkConfig::builder()
        .behavior_version(behavior_version())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://127.0.0.1:9/environment")
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            KEY, SECRET, None, None, "test",
        )))
        .http_client(http)
        .retry_config(RetryConfig::disabled())
        .build()
}

/// The settings of a store on AWS S3 in [`STORE_REGION`].
fn on_aws_s3() -> S3StoreSettings {
    S3StoreSettings {
        region: STORE_REGION.to_owned(),
        endpoint_url: None,
        force_path_style: false,
    }
}

/// Build the clients, then let the identity check run to its end, and
/// return this crate's lines past the credential check's own. No line
/// carries the access key ID or the secret.
async fn check(settings: &S3StoreSettings, http: impl HttpClient + 'static) -> Vec<Logged> {
    let (capture, _guard) = Capture::install();
    let _clients = connect_from(settings, &sdk_over(http), &STARTUP, |builder| builder).await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    identity_lines(&capture)
}

fn identity_lines(capture: &Capture) -> Vec<Logged> {
    let lines: Vec<_> = capture
        .ours()
        .into_iter()
        .filter(|line| line.message() != "S3 test store credentials resolved")
        .collect();
    for line in &lines {
        let text = format!("{line:?}");
        assert!(!text.contains(KEY) && !text.contains(SECRET), "{text}");
    }
    lines
}

fn sts(status: u16, body: &str) -> ReplayEvent {
    ReplayEvent::new(
        HttpRequest::new(SdkBody::empty()),
        HttpResponse::new(status.try_into().unwrap(), SdkBody::from(body.to_owned())),
    )
}

fn sts_error(status: u16, code: &str, message: &str) -> ReplayEvent {
    sts(
        status,
        &format!(
            "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error>\
             <Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error>\
             <RequestId>0f6c2a1e-identity-test</RequestId></ErrorResponse>"
        ),
    )
}

fn only(lines: &[Logged], level: Level) -> &str {
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(lines[0].level, level, "{lines:?}");
    lines[0].message()
}

/// STS's answer to GetCallerIdentity for the store's IAM user.
fn signed_as() -> ReplayEvent {
    sts(
        200,
        "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
         <GetCallerIdentityResult><Arn>arn:aws:iam::123456789012:user/extenddb-store</Arn>\
         <UserId>AIDAIDENTITYTESTUSER</UserId><Account>123456789012</Account>\
         </GetCallerIdentityResult><ResponseMetadata><RequestId>0f6c2a1e-identity-test\
         </RequestId></ResponseMetadata></GetCallerIdentityResponse>",
    )
}

/// The line logged for [`signed_as`].
const SIGNS_AS: &str = "S3 test store signs as arn:aws:iam::123456789012:user/extenddb-store \
                        (AWS account 123456789012)";

#[tokio::test(start_paused = true)]
async fn the_identity_the_credentials_sign_as_is_logged() {
    let replay = StaticReplayClient::new(vec![signed_as()]);
    let lines = check(&on_aws_s3(), replay.clone()).await;
    assert_eq!(only(&lines, Level::INFO), SIGNS_AS);
    // One GetCallerIdentity to AWS STS in the store's region, past the
    // environment's endpoint, signed with the resolved keys.
    let requests: Vec<_> = replay.actual_requests().collect();
    assert_eq!(requests.len(), 1);
    let request = requests[0];
    assert!(
        request
            .uri()
            .starts_with("https://sts.eu-central-1.amazonaws.com"),
        "{}",
        request.uri()
    );
    let body = String::from_utf8_lossy(request.body().bytes().unwrap_or_default()).into_owned();
    assert!(body.contains("Action=GetCallerIdentity"), "{body}");
    let signed = request.headers().get("authorization").unwrap_or_default();
    assert!(
        signed.contains(&format!("Credential={KEY}/")) && signed.contains("/eu-central-1/sts/"),
        "{signed}"
    );
}

/// STS's answers for credentials it refuses, as measured on AWS STS: an
/// access key ID AWS does not know, or a session token altered or left out
/// (`InvalidClientTokenId`), a wrong secret key (`SignatureDoesNotMatch`),
/// and a session token past its expiry (`ExpiredToken`). The last case
/// repeats the access key ID.
#[tokio::test(start_paused = true)]
async fn credentials_sts_refuses_are_a_warning_with_its_code() {
    for (code, message, logged) in [
        (
            "InvalidClientTokenId",
            "The security token included in the request is invalid.",
            "The security token included in the request is invalid",
        ),
        (
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature you provided. \
             Check your AWS Secret Access Key and signing method. Consult the service \
             documentation for details.",
            "The request signature we calculated does not match the signature you provided. \
             Check your AWS Secret Access Key and signing method. Consult the service \
             documentation for details",
        ),
        (
            "ExpiredToken",
            "The security token included in the request is expired",
            "The security token included in the request is expired",
        ),
        (
            "InvalidClientTokenId",
            "The security token included in the request is invalid: AKIDIDENTITYTEST.",
            "The security token included in the request is invalid: <access key ID>",
        ),
    ] {
        let replay = StaticReplayClient::new(vec![sts_error(403, code, message)]);
        let lines = check(&on_aws_s3(), replay).await;
        assert_eq!(
            only(&lines, Level::WARN),
            format!(
                "AWS refused the S3 test store credentials ({code}): {logged}. Its S3 requests \
                 fail until the server restarts with credentials AWS accepts"
            )
        );
    }
}

/// STS's answers for a request signed more than 15 minutes before or after
/// its clock, as measured on AWS STS: `SignatureDoesNotMatch`, with a
/// message that says so.
#[tokio::test(start_paused = true)]
async fn a_clock_more_than_15_minutes_off_is_a_warning_of_the_clock() {
    for message in [
        "Signature expired: 20261027T113503Z is now earlier than 20261027T114003Z \
         (20261027T115503Z - 15 min.)",
        "Signature not yet current: 20261027T121502Z is still later than 20261027T121002Z \
         (20261027T115502Z + 15 min.)",
    ] {
        let replay =
            StaticReplayClient::new(vec![sts_error(403, "SignatureDoesNotMatch", message)]);
        let lines = check(&on_aws_s3(), replay).await;
        assert_eq!(
            only(&lines, Level::WARN),
            format!(
                "S3 test store credentials could not be checked with AWS STS: the server clock \
                 is more than 15 minutes off (SignatureDoesNotMatch): {message}"
            )
        );
    }
}

/// Every other answer, a malformed signature included, says nothing measured
/// about the credentials. The last case repeats the access key ID.
#[tokio::test(start_paused = true)]
async fn an_answer_that_says_nothing_of_the_credentials_is_a_warning_that_they_were_not_checked() {
    for (status, code, message, logged) in [
        (400, "Throttling", "Rate exceeded", "Rate exceeded"),
        (429, "Throttling", "Rate exceeded", "Rate exceeded"),
        (
            500,
            "InternalFailure",
            "Try again later.",
            "Try again later.",
        ),
        (
            400,
            "IncompleteSignature",
            "Credential=AKIDIDENTITYTEST/20261027/eu-central-1/sts is incomplete.",
            "Credential=<access key ID>/20261027/eu-central-1/sts is incomplete.",
        ),
    ] {
        let replay = StaticReplayClient::new(vec![sts_error(status, code, message)]);
        let lines = check(&on_aws_s3(), replay).await;
        assert_eq!(
            only(&lines, Level::WARN),
            format!(
                "S3 test store credentials could not be checked with AWS STS: {code} (HTTP \
                 {status}): {logged}"
            )
        );
    }
    // A transport failure: the replay client has nothing to answer with.
    let lines = check(&on_aws_s3(), StaticReplayClient::new(Vec::new())).await;
    let line = only(&lines, Level::WARN);
    assert!(
        line.starts_with("S3 test store credentials could not be checked with AWS STS: "),
        "{line}"
    );
}

#[tokio::test(start_paused = true)]
async fn no_answer_within_five_seconds_is_a_warning_and_startup_does_not_wait_for_it() {
    let never = NeverClient::new();
    let (capture, _guard) = Capture::install();
    let started = tokio::time::Instant::now();
    let _clients = connect_from(
        &on_aws_s3(),
        &sdk_over(never.clone()),
        &STARTUP,
        |builder| builder,
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    tokio::time::sleep(Duration::from_millis(4900)).await;
    assert_eq!(identity_lines(&capture).len(), 0);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        only(&identity_lines(&capture), Level::WARN),
        "S3 test store credentials could not be checked with AWS STS: no answer within five \
         seconds"
    );
    assert_eq!(never.num_calls(), 1);
}

/// An identity that repeats the access key ID, as a role session named
/// after it does, is logged without it.
#[tokio::test(start_paused = true)]
async fn an_access_key_id_in_the_identity_is_elided() {
    let replay = StaticReplayClient::new(vec![sts(
        200,
        &format!(
            "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <GetCallerIdentityResult>\
             <Arn>arn:aws:sts::123456789012:assumed-role/loader/{KEY}</Arn>\
             <UserId>AROAIDENTITYTESTROLE:{KEY}</UserId><Account>123456789012</Account>\
             </GetCallerIdentityResult></GetCallerIdentityResponse>"
        ),
    )]);
    let lines = check(&on_aws_s3(), replay).await;
    assert_eq!(
        only(&lines, Level::INFO),
        "S3 test store signs as arn:aws:sts::123456789012:assumed-role/loader/<access key ID> \
         (AWS account 123456789012)"
    );
}

#[tokio::test(start_paused = true)]
async fn the_check_goes_over_the_http_client_the_s3_clients_are_given() {
    // The configuration's own client never answers; the one the caller
    // gives the S3 clients answers the identity.
    let never = NeverClient::new();
    let given = StaticReplayClient::new(vec![signed_as()]);
    let http = given.clone();
    let (capture, _guard) = Capture::install();
    let _clients = connect_from(
        &on_aws_s3(),
        &sdk_over(never.clone()),
        &STARTUP,
        move |builder| builder.http_client(http),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(only(&identity_lines(&capture), Level::INFO), SIGNS_AS);
    assert_eq!(given.actual_requests().count(), 1);
    assert_eq!(never.num_calls(), 0);
}

#[tokio::test(start_paused = true)]
async fn with_an_endpoint_url_no_sts_request_is_sent() {
    let never = NeverClient::new();
    let lines = check(&settings(Some("http://127.0.0.1:9")), never.clone()).await;
    assert_eq!(lines.len(), 0, "{lines:?}");
    assert_eq!(never.num_calls(), 0);
}
