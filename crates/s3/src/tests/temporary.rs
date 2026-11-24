// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The warning for resolved temporary keys the SDK cannot renew: a session
//! token and no expiry, from any source but a `credential_process`. The
//! in-process cases build the clients over an SDK configuration that
//! resolves the case's keys, against a configured endpoint, so no request
//! is sent; the configuration's HTTP client never answers and records each
//! call, and none is made. The profile cases run the SDK's own chain in a
//! child process, against a configured endpoint too.

use std::time::{Duration, SystemTime};

use aws_credential_types::credential_feature::AwsCredentialFeature;
use aws_sdk_s3::config::{Credentials, Region, SharedCredentialsProvider};
use aws_smithy_http_client::test_util::NeverClient;
use tracing::Level;

use super::child;
use super::logs::{Capture, Logged};
use super::*;
use crate::client::connect_from;
use crate::connect_with;

const SECRET: &str = "temporary-test-secret";

const CANNOT_RENEW: &str = "The S3 test store credentials are temporary keys the AWS SDK cannot \
     renew. When they expire, its S3 requests fail until the server restarts with new keys. Give \
     the server a role, web identity, IAM Identity Center or a credential_process profile instead";

/// An SDK configuration that resolves `credentials` (none when `None`),
/// over `never`.
fn resolving(credentials: Option<Credentials>, never: &NeverClient) -> SdkConfig {
    let mut builder = SdkConfig::builder()
        .behavior_version(behavior_version())
        .region(Region::new("us-east-1"))
        .http_client(never.clone());
    if let Some(credentials) = credentials {
        builder = builder.credentials_provider(SharedCredentialsProvider::new(credentials));
    }
    builder.build()
}

/// The lines this crate logs while the clients are built over a
/// configuration that resolves `credentials`. No line carries the access
/// key ID or the secret, and no request is sent.
async fn startup_lines(credentials: Credentials) -> Vec<Logged> {
    let key = credentials.access_key_id().to_owned();
    let never = NeverClient::new();
    let sdk = resolving(Some(credentials), &never);
    let (capture, _guard) = Capture::install();
    let _clients = connect_from(
        &settings(Some("http://127.0.0.1:9")),
        &sdk,
        &STARTUP,
        |builder| builder,
    )
    .await;
    // Past the identity check's own bound, had one started.
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(never.num_calls(), 0);
    let lines = capture.ours();
    for line in &lines {
        let text = format!("{line:?}");
        assert!(!text.contains(&key) && !text.contains(SECRET), "{text}");
    }
    lines
}

fn warnings(lines: &[Logged]) -> Vec<&str> {
    lines
        .iter()
        .filter(|line| line.level == Level::WARN)
        .map(Logged::message)
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_session_token_without_an_expiry_is_a_warning() {
    let lines = startup_lines(Credentials::new(
        "ASIATEMPORARYTEST",
        SECRET,
        Some("session-token".to_owned()),
        None,
        "test",
    ))
    .await;
    assert_eq!(warnings(&lines), [CANNOT_RENEW]);
}

#[tokio::test(start_paused = true)]
async fn keys_with_an_expiry_or_without_a_session_token_are_no_warning() {
    let in_an_hour = SystemTime::now() + Duration::from_secs(3600);
    for credentials in [
        Credentials::new(
            "ASIAEXPIRINGTEST",
            SECRET,
            Some("session-token".to_owned()),
            Some(in_an_hour),
            "test",
        ),
        Credentials::new("AKIALONGTERMTEST", SECRET, None, None, "test"),
    ] {
        let lines = startup_lines(credentials).await;
        assert_eq!(warnings(&lines), Vec::<&str>::new());
        let resolved: Vec<_> = lines.iter().map(Logged::message).collect();
        assert_eq!(resolved, ["S3 test store credentials resolved"]);
    }
}

/// Keys from a `credential_process` are run for again when the SDK's cache
/// lets them go, so a session token without an expiry from one is no
/// warning.
#[tokio::test(start_paused = true)]
async fn keys_a_credential_process_gives_without_an_expiry_are_no_warning() {
    let mut credentials = Credentials::new(
        "ASIAPROCESSTEST",
        SECRET,
        Some("session-token".to_owned()),
        None,
        "test",
    );
    credentials.set_property(vec![AwsCredentialFeature::CredentialsProcess]);
    let lines = startup_lines(credentials).await;
    assert_eq!(warnings(&lines), Vec::<&str>::new());
}

/// The same through the SDK's own credential chain, in a child process of
/// this test binary (`child::run`) whose AWS config file is the case's: a
/// `credential_process` that prints a session token and no `Expiration`,
/// and the same kind of keys written into the profile.
const PROFILES: [(&str, &str); 2] = [
    (
        "credential_process",
        "[default]\ncredential_process = printf '{\"Version\": 1, \"AccessKeyId\": \
         \"ASIAPROCESSTEST\", \"SecretAccessKey\": \"temporary-test-secret\", \"SessionToken\": \
         \"process-token\"}'\n",
    ),
    (
        "keys in the profile",
        "[default]\naws_access_key_id = ASIAPROFILETEST\naws_secret_access_key = \
         temporary-test-secret\naws_session_token = profile-token\n",
    ),
];

#[test]
fn a_credential_process_profile_is_no_warning_and_keys_in_the_profile_are() {
    if child::is_child() {
        child::block_on(from_a_profile());
        return;
    }
    for (case, config) in PROFILES {
        child::run(
            module_path!(),
            "a_credential_process_profile_is_no_warning_and_keys_in_the_profile_are",
            case,
            &[],
            config,
        );
    }
}

/// In the child: the lines of a store built from this process's profile,
/// against a configured endpoint, so nothing is sent.
async fn from_a_profile() {
    let (capture, _guard) = Capture::install();
    let _clients = connect_with(&settings(Some("http://127.0.0.1:9")), &STARTUP, |builder| {
        builder
    })
    .await;
    let lines = capture.ours();
    let messages: Vec<_> = lines.iter().map(Logged::message).collect();
    let resolved = "S3 test store credentials resolved";
    if child::case() == "credential_process" {
        assert_eq!(messages, [resolved]);
    } else {
        assert_eq!(messages, [resolved, CANNOT_RENEW]);
    }
    for line in &lines {
        let text = format!("{line:?}");
        assert!(!text.contains("ASIA") && !text.contains(SECRET), "{text}");
    }
}

#[tokio::test]
async fn the_clients_keep_the_resolved_access_key_id_for_the_caller() {
    let endpoint = settings(Some("http://127.0.0.1:9"));
    let never = NeverClient::new();
    let resolved = Credentials::new("AKIARESOLVEDTEST", SECRET, None, None, "test");
    let clients = connect_from(
        &endpoint,
        &resolving(Some(resolved), &never),
        &STARTUP,
        |builder| builder,
    )
    .await;
    assert_eq!(clients.access_key_id(), Some("AKIARESOLVEDTEST"));
    // Credentials that do not resolve leave none.
    let unresolved = resolving(None, &never);
    let clients = connect_from(&endpoint, &unresolved, &STARTUP, |builder| builder).await;
    assert_eq!(clients.access_key_id(), None);
    assert_eq!(never.num_calls(), 0);
}
