// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the shared clients. Each canned S3 response goes through the
//! SDK's replay HTTP client, which records each request and sends nothing.

mod child;
mod codes;
mod deadlines;
mod endpoints;
mod identity;
mod logs;
mod redirect;
mod settings;
mod temporary;

use aws_config::SdkConfig;
use aws_sdk_s3::config::http::{HttpRequest, HttpResponse};
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{Credentials, Region, SharedCredentialsProvider};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output};
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_http_client::test_util::{NeverClient, ReplayEvent, StaticReplayClient};

use crate::retry::RetryPolicy;
use crate::{S3Clients, S3StoreSettings, Startup, behavior_version};

pub(super) const REQUEST_ID: &str = "4KQS8G3V7XJ2M1PA";
pub(super) const EXTENDED_ID: &str = "pQ8x1T0z+7mY/2abcdEF3ghIJ4klMN5opQR6stUV7wxYZ8=";

/// How the test store names itself in its startup lines.
pub(super) const STARTUP: Startup<'static> = Startup {
    label: "S3 test store",
    until_resolved: "its calls fail until they resolve",
};

/// A store's clients over a replay client answering `responses` in order,
/// region us-east-1, virtual-hosted addressing, redirects followed, no
/// retries by the SDK or by the store.
pub(super) fn clients(responses: Vec<HttpResponse>) -> (S3Clients, StaticReplayClient) {
    let replay = StaticReplayClient::new(
        responses
            .into_iter()
            .map(|response| ReplayEvent::new(HttpRequest::new(SdkBody::empty()), response))
            .collect(),
    );
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(behavior_version())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("AKIDTEST", "secret", None, None, "test"))
        .http_client(replay.clone())
        .retry_config(RetryConfig::disabled())
        .build();
    (
        S3Clients::new(config, true, false).with_retry(RetryPolicy::NONE),
        replay,
    )
}

pub(super) fn with_headers(mut response: HttpResponse, headers: &[(&str, &str)]) -> HttpResponse {
    for (name, value) in headers {
        response
            .headers_mut()
            .insert((*name).to_owned(), (*value).to_owned());
    }
    response
}

/// An S3 error response with its XML body and both request ids.
pub(super) fn s3_error(status: u16, code: &str, message: &str) -> HttpResponse {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code>\
         <Message>{message}</Message><RequestId>{REQUEST_ID}</RequestId>\
         <HostId>{EXTENDED_ID}</HostId></Error>"
    );
    with_headers(
        HttpResponse::new(status.try_into().unwrap(), SdkBody::from(body)),
        &[
            ("content-type", "application/xml"),
            ("x-amz-request-id", REQUEST_ID),
            ("x-amz-id-2", EXTENDED_ID),
        ],
    )
}

pub(super) fn uri(replay: &StaticReplayClient, index: usize) -> String {
    replay
        .actual_requests()
        .nth(index)
        .map(|request| request.uri().to_owned())
        .unwrap_or_default()
}

/// One ListObjectsV2 of at most one key under `in/` in `bucket`, through
/// the clients' own call.
pub(super) async fn list_one(
    clients: &S3Clients,
    bucket: &str,
) -> Result<ListObjectsV2Output, SdkError<ListObjectsV2Error, HttpResponse>> {
    clients
        .call(bucket, |client| {
            client
                .list_objects_v2()
                .bucket(bucket)
                .prefix("in/")
                .max_keys(1)
                .send()
        })
        .await
}

/// An SDK configuration as the loader would give it, with static keys,
/// built without reading the environment, over an HTTP client that never
/// answers, so a request a test does not expect stays in the process.
pub(super) fn sdk_config() -> SdkConfig {
    SdkConfig::builder()
        .behavior_version(behavior_version())
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "AKIDTEST", "secret", None, None, "test",
        )))
        .http_client(NeverClient::new())
        .build()
}

pub(super) fn settings(endpoint_url: Option<&str>) -> S3StoreSettings {
    S3StoreSettings {
        region: "us-east-1".to_owned(),
        endpoint_url: endpoint_url.map(str::to_owned),
        force_path_style: endpoint_url.is_some(),
    }
}
