// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_http_client::test_util::StaticReplayClient;

use super::*;

use crate::bulk_put::authorize::{AuthorizeError, BulkPrincipal, S3Action};
use crate::bulk_put::s3::{S3Clients, S3Sink};

// s3:PutObject is evaluated on the output key as the request spells it and on
// the key the store writes the object under. The filesystem store drops
// empty and `.` segments; an S3-compatible endpoint also resolves `..`. A
// Deny on one spelling of an output location holds for every spelling the
// store writes to the same place.

/// Deny `s3:PutObject` on every object under `arn:aws:s3:::{OUT_BUCKET}/
/// {denied}/`, allow everything else, and record each PutObject resource.
struct DenyUnder {
    denied: String,
    put_checks: Mutex<Vec<String>>,
}

impl DenyUnder {
    fn new(denied: &str) -> Arc<Self> {
        Arc::new(Self {
            denied: format!("arn:aws:s3:::{OUT_BUCKET}/{denied}/"),
            put_checks: Mutex::new(Vec::new()),
        })
    }
}

impl BulkAuthorizer for DenyUnder {
    fn authorize<'a>(
        &'a self,
        _principal: &'a BulkPrincipal,
        action: S3Action,
        resource_arn: &'a str,
        _s3_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), AuthorizeError>> {
        let answer = if action == S3Action::PutObject {
            self.put_checks
                .lock()
                .unwrap()
                .push(resource_arn.to_owned());
            if resource_arn.starts_with(&self.denied) {
                Err(AuthorizeError::Denied { explicit: true })
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };
        Box::pin(async move { answer })
    }
}

/// The explicit denial text for the test caller on `key` of the output
/// bucket.
fn explicit_put_denial(key: &str) -> String {
    format!(
        "User: arn:aws:iam::{ACCOUNT}:user/{CALLER} is not authorized to perform: s3:PutObject \
         on resource: \"arn:aws:s3:::{OUT_BUCKET}/{key}\" with an explicit deny in an \
         identity-based policy"
    )
}

/// Run three rows, the second without its sort key, with the output prefix
/// `prefix`, through `sink` and the authorizer `deny`.
async fn run_with_prefix(
    h: &Harness,
    prefix: &str,
    sink: Arc<dyn BulkSink>,
    deny: Arc<DenyUnder>,
) -> BulkOperationRecord {
    let mut rows = plain_rows(3);
    rows[1] = row_missing_sk(1);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let output = json!({ "S3Bucket": OUT_BUCKET, "S3KeyPrefix": prefix, "OutputType": ["ERROR"] });
    let op = h
        .start_with_run_context(
            files,
            input_config(false),
            output,
            Some(run_context_with_caller()),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let mut deps = h.deps_with("runner-1", config(), Arc::new(h.fs_source()), sink);
    deps.authorizer = Some(deny);
    runner::run(&deps, record).await.expect("runner run");
    h.record(&op).await
}

/// The held denial shape, with the message naming `key`, and nothing
/// written to the output bucket.
fn assert_held_denial(h: &Harness, record: &BulkOperationRecord, key: &str) {
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(explicit_put_denial(key).as_str())
    );
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    let out = h.store.0.join(ACCOUNT).join(OUT_BUCKET);
    let written: Vec<_> = std::fs::read_dir(&out).unwrap().collect();
    assert!(written.is_empty(), "nothing is written: {written:?}");
}

fn error_key(prefix: &str, operation_id: &str) -> String {
    report::report_key(prefix, ACCOUNT, operation_id, ReportKind::Error, 0)
}

#[tokio::test]
async fn a_deny_on_an_output_prefix_holds_for_every_spelling_the_filesystem_store_writes_there() {
    for (prefix, denied) in [
        ("protected", "protected"),
        ("./protected", "protected"),
        (".//protected", "protected"),
        ("x/./protected", "x/protected"),
    ] {
        let h = Harness::new().await;
        let deny = DenyUnder::new(denied);
        let record = run_with_prefix(&h, prefix, Arc::new(h.fs_sink()), Arc::clone(&deny)).await;
        let literal = error_key(prefix, &record.operation_id);
        let stored = error_key(denied, &record.operation_id);
        // The literal key is evaluated first; where it passes, the key the
        // store writes is evaluated and named in the denial.
        assert_held_denial(&h, &record, &stored);
        let checks = deny.put_checks.lock().unwrap().clone();
        assert_eq!(
            checks.first().map(String::as_str),
            Some(format!("arn:aws:s3:::{OUT_BUCKET}/{literal}").as_str()),
            "{prefix}: the literal key first"
        );
    }
}

#[tokio::test]
async fn a_denial_of_the_literal_key_names_the_literal_key() {
    let h = Harness::new().await;
    let deny = DenyUnder::new(".");
    let record = run_with_prefix(&h, "./protected", Arc::new(h.fs_sink()), Arc::clone(&deny)).await;
    let literal = error_key("./protected", &record.operation_id);
    assert_held_denial(&h, &record, &literal);
    assert_eq!(
        deny.put_checks.lock().unwrap().len(),
        1,
        "the stored key is not evaluated after a denial"
    );
}

#[tokio::test]
async fn a_spelling_the_filesystem_store_writes_elsewhere_is_allowed() {
    let h = Harness::new().await;
    let deny = DenyUnder::new("protected");
    let record = run_with_prefix(&h, "x/./protected", Arc::new(h.fs_sink()), deny).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert!(
        h.store
            .0
            .join(ACCOUNT)
            .join(OUT_BUCKET)
            .join("x/protected/AWSDynamoDB/BulkOperation")
            .is_dir(),
        "the report lands under x/protected"
    );
}

/// An S3 sink whose clients reach no network: a replay client with no
/// answers, so any request fails. `endpoint` is whether the store has a
/// configured endpoint.
fn offline_s3_sink(endpoint: bool) -> S3Sink {
    let mut config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(REGION))
        .credentials_provider(Credentials::new("AKIDTEST", "secret", None, None, "test"))
        .http_client(StaticReplayClient::new(Vec::new()))
        .retry_config(RetryConfig::disabled());
    if endpoint {
        config = config.endpoint_url("http://127.0.0.1:9");
    }
    S3Sink {
        clients: Arc::new(S3Clients::new(config.build(), !endpoint, endpoint)),
    }
}

#[tokio::test]
async fn a_deny_holds_for_a_parent_segment_an_s3_compatible_endpoint_resolves() {
    let h = Harness::new().await;
    let deny = DenyUnder::new("protected");
    let sink: Arc<dyn BulkSink> = Arc::new(offline_s3_sink(true));
    let record = run_with_prefix(&h, "x/../protected", sink, deny).await;
    let stored = error_key("protected", &record.operation_id);
    assert_held_denial(&h, &record, &stored);
}

#[test]
fn on_aws_s3_the_key_as_spelled_is_the_key_written() {
    let aws = offline_s3_sink(false);
    for key in ["x/../protected/k", "./protected/k", "a//b"] {
        assert_eq!(aws.stored_key(key), key);
    }
    let endpoint = offline_s3_sink(true);
    assert_eq!(endpoint.stored_key("x/../protected/k"), "protected/k");
    assert_eq!(endpoint.stored_key(".//a/./b"), "a/b");
}

#[tokio::test]
async fn on_aws_s3_a_parent_segment_names_another_object() {
    // The literal key is the only one evaluated; the Deny under `protected`
    // does not reach `x/../protected`, so the run goes on to the location
    // check, which this offline sink cannot answer.
    let h = Harness::new().await;
    let deny = DenyUnder::new("protected");
    let sink: Arc<dyn BulkSink> = Arc::new(offline_s3_sink(false));
    let record = run_with_prefix(&h, "x/../protected", sink, Arc::clone(&deny)).await;
    assert_ne!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    let literal = error_key("x/../protected", &record.operation_id);
    assert_eq!(
        deny.put_checks.lock().unwrap().clone(),
        vec![format!("arn:aws:s3:::{OUT_BUCKET}/{literal}")]
    );
}
