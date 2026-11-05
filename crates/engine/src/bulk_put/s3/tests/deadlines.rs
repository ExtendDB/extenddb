// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The deadlines of the store's clients on the store's calls: a request
//! whose answer never comes, and a body that stops after its head, which
//! the SDK's stalled-stream protection ends. The connect and read timeouts
//! every client is built with are tested with the shared clients
//! (`extenddb-s3`).

use std::time::{Duration, Instant};

use aws_config::SdkConfig;
use aws_sdk_s3::config::{Credentials, Region, SharedCredentialsProvider};

use super::read::drain;
use super::stub::{self, Answer, Stub};
use super::*;
use crate::bulk_put::s3::client::{clients_from, timeouts};
use crate::bulk_put::s3::list::check_bucket;
use crate::bulk_put::s3::read::open;
use crate::bulk_put::s3::retry::RetryPolicy;
use crate::bulk_put::source::InputCodec;
use crate::bulk_put::store::S3StoreSettings;

/// An SDK configuration as the loader would give it, with static keys,
/// built without reading the environment.
pub(super) fn sdk_config() -> SdkConfig {
    SdkConfig::builder()
        .behavior_version(behavior_version())
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "AKIDTEST", "secret", None, None, "test",
        )))
        .build()
}

pub(super) fn settings(endpoint_url: Option<&str>) -> S3StoreSettings {
    S3StoreSettings {
        region: "us-east-1".to_owned(),
        endpoint_url: endpoint_url.map(str::to_owned),
        force_path_style: endpoint_url.is_some(),
    }
}

#[tokio::test]
async fn an_answer_that_never_comes_ends_at_the_read_deadline_after_the_sdks_attempts() {
    // The listener accepts every request and never answers. With the read
    // deadline at one second, each of the SDK's three attempts ends at it,
    // and the call fails as the server's failure (the store's own retries
    // are off here; they are tested on their own).
    let stub = Stub::start(vec![Answer::Hold, Answer::Hold, Answer::Hold]).await;
    let short = timeouts()
        .to_builder()
        .read_timeout(Duration::from_secs(1))
        .build();
    let clients = clients_from(&settings(Some(&stub.url)), &sdk_config(), |builder| {
        builder.timeout_config(short)
    })
    .with_retry(RetryPolicy::NONE);
    let started = Instant::now();
    let checked = tokio::time::timeout(
        Duration::from_secs(30),
        check_bucket(&clients, &location("loads", None), "in/"),
    )
    .await
    .expect("the call ends within its deadlines");
    let elapsed = started.elapsed();
    assert!(
        matches!(&checked, Err(SourceError::Io(io)) if io.kind() == std::io::ErrorKind::Other),
        "{checked:?}"
    );
    assert_eq!(stub.heads().len(), 3, "the SDK's three attempts");
    assert!(
        elapsed >= Duration::from_secs(3) && elapsed < Duration::from_secs(15),
        "{elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_that_stops_after_its_head_is_ended_by_stalled_stream_protection() {
    // The clients are built as the store builds them, so the protection
    // in force is the pinned behaviour version's default.
    let head = stub::head(
        "206 Partial Content",
        &[
            ("content-length", "40"),
            ("content-range", "bytes 0-39/40"),
            ("etag", "\"e1\""),
        ],
    );
    let stub = Stub::start(vec![Answer::Stall {
        head,
        body: b"line-0001\n".to_vec(),
    }])
    .await;
    let clients = clients_from(&settings(Some(&stub.url)), &sdk_config(), |builder| builder)
        .with_retry(RetryPolicy::NONE);
    let started = Instant::now();
    let opened = tokio::time::timeout(
        Duration::from_secs(60),
        open(
            shared(clients),
            location("loads", None),
            &input_file("in/part-0.json", 40, Some("e1")),
            InputCodec::None,
        ),
    )
    .await
    .expect("the stalled body is ended");
    let elapsed = started.elapsed();
    let error = match opened {
        Ok(mut reader) => drain(&mut reader).await.1.expect("a fault"),
        Err(error) => error,
    };
    assert!(
        matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
        "{error:?}"
    );
    assert!(elapsed < Duration::from_secs(30), "{elapsed:?}");
}
