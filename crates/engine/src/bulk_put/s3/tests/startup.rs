// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The startup credential check and calls made without credentials:
//! credentials that cannot be resolved are a warning that says which later
//! change needs a restart, and a call made without them is answered at
//! once, not retried. Each test runs in a child process of this test
//! binary (`child::run`) with no keys, an empty config file and instance
//! metadata off, so the chain fails without sending a request.

use std::time::Duration;

use aws_smithy_http_client::test_util::StaticReplayClient;
use tracing::Level;

use super::child;
use super::deadlines::settings;
use super::logs::Capture;
use super::*;
use crate::bulk_put::s3::client::{CREDENTIALS_LATER, connect, connect_with};
use crate::bulk_put::s3::list::check_bucket;

#[test]
fn credentials_missing_at_startup_are_a_warning_that_says_a_file_change_needs_a_restart() {
    if child::is_child() {
        child::block_on(warning());
        return;
    }
    child::run(
        module_path!(),
        "credentials_missing_at_startup_are_a_warning_that_says_a_file_change_needs_a_restart",
        "no credentials",
        &[],
        "",
    );
}

async fn warning() {
    let (capture, _guard) = Capture::install();
    let _clients = connect(&settings(None)).await;
    let warnings: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|event| event.level == Level::WARN && event.message().starts_with("S3 bulk store"))
        .collect();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(
        warnings[0].message(),
        format!(
            "S3 bulk store credentials cannot be resolved; Start answers InternalServerError \
             until they resolve. {CREDENTIALS_LATER}"
        )
    );
    assert!(
        warnings[0]
            .message()
            .contains("so a change to them needs a restart"),
        "{warnings:?}"
    );
}

#[test]
fn a_call_without_credentials_is_the_servers_failure_at_once() {
    if child::is_child() {
        child::block_on(call_without_credentials());
        return;
    }
    child::run(
        module_path!(),
        "a_call_without_credentials_is_the_servers_failure_at_once",
        "no credentials",
        &[],
        "",
    );
}

/// The SDK reports credentials it cannot resolve as a dispatch failure of
/// no kind. A call that took it for a transient failure would still be
/// retrying under the store's 120-second bound when the ten seconds below
/// run out.
async fn call_without_credentials() {
    let replay = StaticReplayClient::new(Vec::new());
    let http = replay.clone();
    let clients = connect_with(&settings(None), move |builder| builder.http_client(http)).await;
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        check_bucket(&clients, &location("loads", None), "in/"),
    )
    .await
    .expect("answered within ten seconds")
    .unwrap_err();
    assert!(
        matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
        "{error:?}"
    );
    assert_eq!(
        replay.actual_requests().count(),
        0,
        "no request is sent without credentials"
    );
}
