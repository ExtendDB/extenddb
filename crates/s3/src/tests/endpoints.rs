// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A store's endpoint comes from its `endpoint_url` setting alone, and its
//! region from its settings. Each case runs in a child process of this
//! test binary (`child::run`) whose environment holds static keys, a region
//! that is not the store's, and the case's endpoint variables or profile,
//! and nothing of the host's. The configuration is loaded by the loader
//! `connect_with` uses, with an HTTP client that never answers; the store's
//! S3 requests and the identity check's GetCallerIdentity are answered in
//! the process by the client the test gives the store's clients.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::config::http::{HttpRequest, HttpResponse};
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{Region, SharedHttpClient};
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_http_client::test_util::{
    NeverClient, ReplayEvent, StaticReplayClient, infallible_client_fn,
};

use super::child;
use super::*;
use crate::client::{connect_from, loader};

/// The endpoint each case names. Nothing is sent to it: an in-process
/// client answers every request.
const ELSEWHERE: &str = "http://127.0.0.1:9";

/// AWS S3's endpoint for a virtual-hosted bucket `loads` in us-east-1, the
/// store's region.
const AWS_S3: &str = "https://loads.s3.us-east-1.amazonaws.com/";

/// AWS STS in the store's region.
const AWS_STS: &str = "https://sts.us-east-1.amazonaws.com/";

/// Static keys, so the credential chain resolves from the environment, and
/// a region that is not the store's.
const KEYS: [(&str, &str); 3] = [
    ("AWS_ACCESS_KEY_ID", "AKIDENDPOINTTEST"),
    ("AWS_SECRET_ACCESS_KEY", "endpoint-test-secret"),
    ("AWS_REGION", "eu-west-1"),
];

/// One way the environment names an endpoint: the variables it sets, the
/// AWS config file it writes, and where the identity check then sends
/// GetCallerIdentity.
struct Case {
    name: &'static str,
    variables: &'static [(&'static str, &'static str)],
    config: &'static str,
    sts: &'static str,
}

const CASES: &[Case] = &[
    Case {
        name: "AWS_ENDPOINT_URL",
        variables: &[("AWS_ENDPOINT_URL", "http://127.0.0.1:9/all")],
        config: "",
        sts: AWS_STS,
    },
    Case {
        name: "AWS_ENDPOINT_URL_S3",
        variables: &[("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:9/s3")],
        config: "",
        sts: AWS_STS,
    },
    Case {
        name: "profile endpoint_url",
        variables: &[],
        config: "[default]\nendpoint_url = http://127.0.0.1:9/profile\n",
        sts: AWS_STS,
    },
    Case {
        name: "profile services",
        variables: &[],
        config: "[default]\nservices = local\n\n[services local]\ns3 =\n  endpoint_url = \
                 http://127.0.0.1:9/services\n",
        sts: AWS_STS,
    },
    Case {
        name: "every source",
        variables: &[
            ("AWS_ENDPOINT_URL", "http://127.0.0.1:9/all"),
            ("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:9/s3"),
        ],
        config: "[default]\nendpoint_url = http://127.0.0.1:9/profile\nservices = local\n\n\
                 [services local]\ns3 =\n  endpoint_url = http://127.0.0.1:9/services\n",
        sts: AWS_STS,
    },
    // An endpoint set for STS alone is honoured by the identity check.
    Case {
        name: "AWS_ENDPOINT_URL_STS",
        variables: &[
            ("AWS_ENDPOINT_URL", "http://127.0.0.1:9/all"),
            ("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:9/sts"),
        ],
        config: "",
        sts: "http://127.0.0.1:9/sts",
    },
    Case {
        name: "profile services sts",
        variables: &[],
        config: "[default]\nendpoint_url = http://127.0.0.1:9/profile\nservices = local\n\n\
                 [services local]\nsts =\n  endpoint_url = http://127.0.0.1:9/profile-sts\n",
        sts: "http://127.0.0.1:9/profile-sts",
    },
    // The SDK reads AWS_ENDPOINT_URL before a profile's services section,
    // so STS's endpoint is then the global one, which the check ignores.
    Case {
        name: "AWS_ENDPOINT_URL beside profile services sts",
        variables: &[("AWS_ENDPOINT_URL", "http://127.0.0.1:9/all")],
        config: "[default]\nservices = local\n\n[services local]\nsts =\n  endpoint_url = \
                 http://127.0.0.1:9/profile-sts\n",
        sts: AWS_STS,
    },
];

/// An empty listing of `loads`.
fn listing() -> HttpResponse {
    HttpResponse::new(
        200.try_into().unwrap(),
        SdkBody::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult><Name>loads</Name>\
             <IsTruncated>false</IsTruncated></ListBucketResult>",
        ),
    )
}

/// STS's answer to GetCallerIdentity.
const IDENTITY: &str = "<GetCallerIdentityResponse \
     xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><GetCallerIdentityResult>\
     <Arn>arn:aws:iam::123456789012:user/extenddb-store</Arn><UserId>AIDAENDPOINTTEST</UserId>\
     <Account>123456789012</Account></GetCallerIdentityResult></GetCallerIdentityResponse>";

/// The URI of each request the routing client answered, in order.
#[derive(Debug, Default)]
struct Asked {
    s3: Vec<String>,
    sts: Vec<String>,
}

/// An HTTP client that answers in the process: GetCallerIdentity with an
/// identity, any other request with an empty listing.
fn routing(asked: &Arc<Mutex<Asked>>) -> SharedHttpClient {
    let asked = asked.clone();
    infallible_client_fn(move |request| {
        let uri = request.uri().to_string();
        let identity = request
            .body()
            .bytes()
            .is_some_and(|body| String::from_utf8_lossy(body).contains("Action=GetCallerIdentity"));
        let mut asked = asked.lock().unwrap();
        let response = if identity {
            asked.sts.push(uri);
            HttpResponse::new(200.try_into().unwrap(), SdkBody::from(IDENTITY))
        } else {
            asked.s3.push(uri);
            listing()
        };
        response.try_into_http1x().unwrap()
    })
}

#[test]
fn without_endpoint_url_the_store_sends_to_aws_s3_whatever_the_environment_names() {
    if child::is_child() {
        child::block_on(in_child());
        return;
    }
    for case in CASES {
        let variables: Vec<_> = KEYS.iter().chain(case.variables).copied().collect();
        child::run(
            module_path!(),
            "without_endpoint_url_the_store_sends_to_aws_s3_whatever_the_environment_names",
            case.name,
            &variables,
            case.config,
        );
    }
}

/// In the child: the store's clients built from this process's environment
/// with no `endpoint_url` send to AWS S3 in the store's region, the
/// identity check sends to the case's STS endpoint over the same client,
/// and the SDK's own client over a configuration loaded from the same
/// environment sends to the endpoint the case names, which shows the case
/// reaches the SDK.
async fn in_child() {
    let case = CASES
        .iter()
        .find(|case| case.name == child::case())
        .expect("the child runs a case of CASES");
    let never = NeverClient::new();
    let loaded = loader(&settings(None))
        .http_client(never.clone())
        .load()
        .await;
    let asked = Arc::new(Mutex::new(Asked::default()));
    let http = routing(&asked);
    let clients = connect_from(&settings(None), &loaded, &STARTUP, move |builder| {
        builder
            .http_client(http)
            .retry_config(RetryConfig::disabled())
    })
    .await
    .with_retry(RetryPolicy::NONE);
    list_one(&clients, "loads").await.unwrap();
    // The identity check runs in a task of its own.
    for _ in 0..500 {
        if !asked.lock().unwrap().sts.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    {
        let asked = asked.lock().unwrap();
        assert_eq!(asked.s3.len(), 1, "{asked:?}");
        assert!(
            asked.s3[0].starts_with(AWS_S3),
            "the store sent to {asked:?}"
        );
        assert_eq!(asked.sts.len(), 1, "{asked:?}");
        assert!(
            asked.sts[0].starts_with(case.sts),
            "the identity check sent to {asked:?}"
        );
    }
    assert_eq!(never.num_calls(), 0);

    let sdk = aws_config::defaults(behavior_version())
        .region(Region::new("us-east-1"))
        .load()
        .await;
    let plain = StaticReplayClient::new(vec![ReplayEvent::new(
        HttpRequest::new(SdkBody::empty()),
        listing(),
    )]);
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk)
            .http_client(plain.clone())
            .retry_config(RetryConfig::disabled())
            .build(),
    );
    client
        .list_objects_v2()
        .bucket("loads")
        .send()
        .await
        .unwrap();
    let sdk_uri = uri(&plain, 0);
    assert!(sdk_uri.starts_with(ELSEWHERE), "the SDK sent to {sdk_uri}");
}
