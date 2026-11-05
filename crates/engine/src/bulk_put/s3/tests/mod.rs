// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Fault tests for the S3 store over the SDK's replay HTTP client: each
//! canned S3 response (status, request ids, S3's XML error body) goes
//! through the store's own call, and the tests assert the `FailureCode`,
//! the exact `FailureMessage` with its tail, and `after_first_byte`.

mod checksums;
mod child;
mod complete;
mod credentials;
mod deadlines;
mod dropped;
mod list;
mod logs;
mod read;
mod redirect;
mod replaced;
pub(crate) mod smoke;
mod start_deadline;
mod startup;
mod stub;
mod transient;
mod write;

use std::sync::Arc;

use aws_sdk_s3::config::http::{HttpRequest, HttpResponse};
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};

use extenddb_storage::InputFile;

use super::client::behavior_version;
use super::retry::RetryPolicy;
use super::{Location, S3Clients};
use crate::bulk_put::source::{SourceError, StoreFault};

pub(super) const REQUEST_ID: &str = "4KQS8G3V7XJ2M1PA";
pub(super) const EXTENDED_ID: &str = "pQ8x1T0z+7mY/2abcdEF3ghIJ4klMN5opQR6stUV7wxYZ8=";

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

/// The tail the service renders on input reads, the Start-time list and
/// the manifest write.
pub(super) fn amazon_s3_tail(status: u16, code: &str) -> String {
    format!(
        " (Service: Amazon S3; Status Code: {status}; Error Code: {code}; Request ID: \
         {REQUEST_ID}; S3 Extended Request ID: {EXTENDED_ID}; Proxy: null)"
    )
}

/// The tail the service renders on report writes.
pub(super) fn s3_tail(status: u16) -> String {
    format!(
        " (Service: S3, Status Code: {status}, Request ID: {REQUEST_ID}, Extended Request ID: \
         {EXTENDED_ID}) (SDK Attempt Count: 1)"
    )
}

pub(super) fn location(bucket: &str, owner: Option<&str>) -> Location {
    Location {
        account: "000000000001".to_owned(),
        bucket: bucket.to_owned(),
        owner: owner.map(str::to_owned),
    }
}

pub(super) fn input_file(path: &str, size: usize, etag: Option<&str>) -> InputFile {
    InputFile {
        path: path.to_owned(),
        size: i64::try_from(size).unwrap(),
        rows: None,
        etag: etag.map(str::to_owned),
        abandon: None,
    }
}

/// The fault a source error carries, or a panic naming the error.
pub(super) fn fault(error: SourceError) -> StoreFault {
    match error {
        SourceError::Store(fault) => fault,
        other => panic!("expected a store fault, got {other:?}"),
    }
}

/// The value of one request header the replay client recorded.
pub(super) fn header(replay: &StaticReplayClient, index: usize, name: &str) -> Option<String> {
    replay
        .actual_requests()
        .nth(index)
        .and_then(|request| request.headers().get(name).map(str::to_owned))
}

pub(super) fn uri(replay: &StaticReplayClient, index: usize) -> String {
    replay
        .actual_requests()
        .nth(index)
        .map(|request| request.uri().to_owned())
        .unwrap_or_default()
}

pub(super) fn shared(clients: S3Clients) -> Arc<S3Clients> {
    Arc::new(clients)
}
