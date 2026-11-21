// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Region redirects: a bucket in another region is found through the
//! redirect AWS S3 answers, once, and remembered.

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};

use super::*;

fn empty_listing() -> HttpResponse {
    HttpResponse::new(
        200.try_into().unwrap(),
        SdkBody::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult><Name>loads</Name>\
             <IsTruncated>false</IsTruncated></ListBucketResult>",
        ),
    )
}

fn redirect(status: u16, code: &str, region: &str) -> HttpResponse {
    with_headers(
        s3_error(status, code, "The bucket is in another region."),
        &[("x-amz-bucket-region", region)],
    )
}

#[tokio::test]
async fn a_redirect_is_followed_once_and_the_bucket_region_remembered() {
    let (clients, replay) = clients(vec![
        redirect(301, "PermanentRedirect", "us-west-2"),
        empty_listing(),
        empty_listing(),
    ]);
    list_one(&clients, "loads").await.unwrap();
    list_one(&clients, "loads").await.unwrap();
    assert!(
        uri(&replay, 0).contains("s3.us-east-1.amazonaws.com"),
        "{}",
        uri(&replay, 0)
    );
    assert!(
        uri(&replay, 1).contains("s3.us-west-2.amazonaws.com"),
        "{}",
        uri(&replay, 1)
    );
    assert!(
        uri(&replay, 2).contains("s3.us-west-2.amazonaws.com"),
        "{}",
        uri(&replay, 2)
    );
    assert_eq!(replay.actual_requests().count(), 3);
}

#[tokio::test]
async fn client_for_gives_the_client_of_the_remembered_region() {
    let (clients, _replay) = clients(vec![
        redirect(301, "PermanentRedirect", "us-west-2"),
        empty_listing(),
    ]);
    let region = |client: aws_sdk_s3::Client| client.config().region().cloned();
    assert_eq!(
        region(clients.client_for("loads")),
        Some(Region::new("us-east-1"))
    );
    list_one(&clients, "loads").await.unwrap();
    assert_eq!(
        region(clients.client_for("loads")),
        Some(Region::new("us-west-2"))
    );
    assert_eq!(
        region(clients.client_for("reports")),
        Some(Region::new("us-east-1"))
    );
}

#[tokio::test]
async fn an_authorization_header_naming_the_wrong_region_is_followed() {
    let (clients, replay) = clients(vec![
        redirect(400, "AuthorizationHeaderMalformed", "eu-west-1"),
        empty_listing(),
    ]);
    list_one(&clients, "loads").await.unwrap();
    assert!(
        uri(&replay, 1).contains("s3.eu-west-1.amazonaws.com"),
        "{}",
        uri(&replay, 1)
    );
}

#[tokio::test]
async fn a_redirect_is_the_answer_against_a_configured_endpoint_and_after_one_retry() {
    // Against an S3-compatible endpoint redirects are not followed.
    let replay = StaticReplayClient::new(vec![ReplayEvent::new(
        HttpRequest::new(SdkBody::empty()),
        redirect(301, "PermanentRedirect", "us-west-2"),
    )]);
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(behavior_version())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://127.0.0.1:9")
        .force_path_style(true)
        .credentials_provider(Credentials::new("AKIDTEST", "secret", None, None, "test"))
        .http_client(replay.clone())
        .retry_config(RetryConfig::disabled())
        .build();
    let endpoint_clients = S3Clients::new(config, false, true).with_retry(RetryPolicy::NONE);
    let refused = list_one(&endpoint_clients, "loads").await.unwrap_err();
    assert_eq!(refused.code(), Some("PermanentRedirect"));
    assert_eq!(replay.actual_requests().count(), 1);

    // On AWS S3 a second redirect after the retry is the answer.
    let (aws, replay) = clients(vec![
        redirect(301, "PermanentRedirect", "us-west-2"),
        redirect(301, "PermanentRedirect", "eu-west-1"),
    ]);
    let again = list_one(&aws, "loads").await.unwrap_err();
    assert_eq!(again.code(), Some("PermanentRedirect"));
    assert_eq!(replay.actual_requests().count(), 2);
}
