// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A redirect the store does not follow is the caller's fault, rendered
//! with S3's code and message: against a configured endpoint, and on AWS
//! S3 once one redirect was followed (the redirects themselves are tested
//! with the shared clients, `extenddb-s3`).

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};

use super::*;
use crate::bulk_put::s3::list::check_bucket;

fn redirect(region: &str) -> HttpResponse {
    with_headers(
        s3_error(301, "PermanentRedirect", "The bucket is in another region."),
        &[("x-amz-bucket-region", region)],
    )
}

#[tokio::test]
async fn a_redirect_against_a_configured_endpoint_is_rendered_for_the_caller() {
    let replay = StaticReplayClient::new(vec![ReplayEvent::new(
        HttpRequest::new(SdkBody::empty()),
        redirect("us-west-2"),
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
    let refused = fault(
        check_bucket(&endpoint_clients, &location("loads", None), "")
            .await
            .unwrap_err(),
    );
    assert_eq!(refused.code, "S3PermanentRedirect");
    assert_eq!(
        refused.message,
        format!(
            "The bucket is in another region.{}",
            amazon_s3_tail(301, "PermanentRedirect")
        )
    );
    assert_eq!(replay.actual_requests().count(), 1);
}

#[tokio::test]
async fn a_second_redirect_on_aws_s3_is_rendered_for_the_caller() {
    let (aws, replay) = clients(vec![redirect("us-west-2"), redirect("eu-west-1")]);
    let again = fault(
        check_bucket(&aws, &location("loads", None), "")
            .await
            .unwrap_err(),
    );
    assert_eq!(again.code, "S3PermanentRedirect");
    assert_eq!(
        again.message,
        format!(
            "The bucket is in another region.{}",
            amazon_s3_tail(301, "PermanentRedirect")
        )
    );
    assert_eq!(replay.actual_requests().count(), 2);
}
