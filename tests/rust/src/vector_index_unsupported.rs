// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the vector index surface.
//!
//! These go over real HTTP rather than through the engine in-process, because the
//! contract's purpose is byte-level parity and the parts that only exist on the
//! wire cannot be tested any other way: the HTTP status, the `__type` in the body,
//! and the serialized shape of the response.
//!
//! The AWS SDK is not usable here. `aws-sdk-dynamodb` has no vector types at any
//! published version (checked to 1.119.0), so every request is hand-built and
//! SigV4-signed. That is also closer to what a non-SDK client would send.
//!
//! Scope: the NEGATIVE path only, which is the whole of what the contract
//! guarantees today. No in-tree backend implements vector search, so every vector
//! request must be refused, and refused identically whichever backend is running.
//! The positive path belongs with the first backend that implements it.

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use std::time::SystemTime;

use crate::test_base::is_real_dynamodb;

fn endpoint() -> String {
    std::env::var("EXTENDDB_TEST_ENDPOINT")
        .unwrap_or_else(|_| "https://dynamodb.us-east-1.amazonaws.com".into())
}

fn region() -> String {
    std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into())
}

fn http_client() -> reqwest::Client {
    let mut builder = reqwest::Client::builder().danger_accept_invalid_certs(true);
    if let Ok(ca_path) = std::env::var("EXTENDDB_CA_CERT") {
        if let Ok(pem) = std::fs::read(&ca_path) {
            if let Ok(cert) = reqwest::Certificate::from_pem(&pem) {
                builder = builder.add_root_certificate(cert);
            }
        }
    }
    builder.build().unwrap()
}

/// A signed request for an operation the SDK cannot model.
///
/// Signing matters rather than being incidental: authorization runs before
/// dispatch, so an unsigned request never reaches the capability gate and the
/// test would pass for the wrong reason, asserting an auth failure while
/// believing it asserted a vector refusal.
pub(crate) async fn call(target: &str, body: &str) -> (u16, String) {
    let access_key = std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let secret_key =
        std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY must be set");
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

    let creds = Credentials::new(
        access_key,
        secret_key,
        session_token,
        None,
        "extenddb-integration-tests",
    );
    let identity = Identity::from(creds);
    let region = region();
    let params = v4::signing_params::Builder::default()
        .identity(&identity)
        .region(&region)
        .name("dynamodb")
        .time(SystemTime::now())
        .settings(SigningSettings::default())
        .build()
        .expect("signing params")
        .into();

    let url = endpoint();
    let amz_target = format!("DynamoDB_20120810.{target}");
    let host = url
        .split("://")
        .nth(1)
        .expect("endpoint has a scheme")
        .trim_end_matches('/')
        .to_owned();
    let headers: Vec<(&str, &str)> = vec![
        ("content-type", "application/x-amz-json-1.0"),
        ("x-amz-target", &amz_target),
        ("host", &host),
    ];
    let signable = SignableRequest::new(
        "POST",
        &url,
        headers.into_iter(),
        SignableBody::Bytes(body.as_bytes()),
    )
    .expect("signable request");

    let (instructions, _sig) = sign(signable, &params).expect("sign").into_parts();

    let mut req = http_client()
        .post(&url)
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", &amz_target)
        .body(body.to_owned());
    for (name, value) in instructions.headers() {
        req = req.header(name, value);
    }

    let resp = req.send().await.expect("request sent");
    let status = resp.status().as_u16();
    let text = resp.text().await.expect("body read");
    (status, text)
}

/// The exact refusal the contract promises for the table paths. Compared in full
/// rather than by substring, so the wording cannot drift unnoticed: a backend
/// author reads this message and has to reproduce it.
const NOT_SUPPORTED_INDEXES: &str = "Vector indexes are not supported by this storage backend";
/// The refusal for the read path, which is worded for the operation rather than
/// the feature, matching what a caller of `SearchVectors` would expect to see.
const NOT_SUPPORTED_SEARCH: &str = "SearchVectors is not supported by this storage backend";

fn assert_validation_exception(status: u16, body: &str, expected_message: &str) {
    assert_eq!(status, 400, "expected HTTP 400, body: {body}");
    let json: serde_json::Value = serde_json::from_str(body).expect("body is JSON");
    let type_field = json
        .get("__type")
        .and_then(|t| t.as_str())
        .unwrap_or_else(|| panic!("no __type in body: {body}"));
    assert!(
        type_field.ends_with("ValidationException"),
        "expected ValidationException, got {type_field}"
    );
    let message = json
        .get("message")
        .or_else(|| json.get("Message"))
        .and_then(|m| m.as_str())
        .unwrap_or_else(|| panic!("no message in body: {body}"));
    assert_eq!(message, expected_message);
}

pub(crate) fn table_name(suffix: &str) -> String {
    format!("vec_wire_{}_{}", suffix, uuid::Uuid::new_v4().simple())
}

/// Whether the running backend implements vector indexes.
///
/// Probed by attempting the smallest real vector `CreateTable` and reading the
/// answer, because the capability is not otherwise observable over the wire.
/// Every test here asserts a refusal, so on a backend that *does* implement
/// vector search they must skip rather than fail: the refusals are the contract
/// for non-participating backends only.
///
/// Deliberately distinguishes the refusal from any other failure. Treating "not a
/// 200" as unsupported would make the whole suite skip silently the first time an
/// unrelated error appeared, which is the failure mode a self-skipping suite is
/// most prone to.
async fn backend_supports_vectors() -> bool {
    let name = table_name("probe");
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{{
            "IndexName": "probeidx",
            "Dimensions": 4,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "Projection": {{"ProjectionType": "ALL"}}
        }}]
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    if status == 200 {
        let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
        return true;
    }
    if text.contains(NOT_SUPPORTED_INDEXES) {
        return false;
    }
    panic!("vector support probe failed for an unrelated reason: {status} {text}");
}

/// Whether the running backend implements vector indexes, for the suite that
/// asserts the participating behaviour. Named positively so the caller reads as
/// an opt-in rather than as a double negative.
pub(crate) async fn vectors_supported() -> bool {
    !is_real_dynamodb() && backend_supports_vectors().await
}

/// What this run asserts about the backend's vector capability.
///
/// Three states on purpose. Both vector suites otherwise adapt to whatever the
/// backend reports, so no run asserts *which* backend is under test, and a backend
/// that silently lost vector support would skip the entire positive suite and still
/// report green. `EXTENDDB_EXPECT_VECTORS` lets a CI job state its expectation:
///
/// - `1`: the backend must support vectors. The positive suite failing to run is an
///   error rather than a skip.
/// - `0`: the backend must not. The refusal suite failing to run is an error.
/// - unset: adapt quietly, so a plain local `cargo test` works against either
///   backend without ceremony.
///
/// An unrecognised value panics rather than being read as one of the two, because
/// a typo that silently meant "unset" would disable the guard it was added for.
pub(crate) fn expect_vectors() -> Option<bool> {
    match std::env::var("EXTENDDB_EXPECT_VECTORS").ok()?.as_str() {
        "1" => Some(true),
        "0" => Some(false),
        other => panic!("EXTENDDB_EXPECT_VECTORS must be 0 or 1, got {other:?}"),
    }
}

/// Skip guard for every refusal test in this file.
///
/// If this run expects vector support, the refusal tests are correctly
/// inapplicable and skip. If the run and the backend disagree in either
/// direction, that is a surprise worth failing on rather than skipping past.
///
/// The expectation is read *before* probing, not inside the assertion. Reading it
/// inside a short-circuiting `&&` meant an invalid value was never validated on a
/// backend without vector support, so the typo guard was dead in the Postgres job,
/// which is the one job these refusal tests exist for.
async fn skip_if_supported() -> bool {
    if is_real_dynamodb() {
        return true;
    }
    let expected = expect_vectors();
    let supported = backend_supports_vectors().await;
    assert!(
        !(supported && expected == Some(false)),
        "EXTENDDB_EXPECT_VECTORS=0 but the backend supports vector indexes, so \
         these refusal tests would skip: either a backend gained vector support \
         unnoticed, or this job sets the wrong expectation"
    );
    // The converse is asserted here too rather than left to the positive suite
    // alone: a run claiming support against a backend that refuses is wrong
    // whichever suite happens to notice first.
    assert!(
        !(!supported && expected == Some(true)),
        "EXTENDDB_EXPECT_VECTORS=1 but the backend refuses vector indexes: either \
         a backend lost vector support unnoticed, or this job sets the wrong \
         expectation"
    );
    supported
}

/// A `CreateTable` naming vector indexes is refused before the table is created.
///
/// This is the hole the gate exists to close. Without it the request passes shape
/// validation, reaches a backend that ignores `VectorIndexes`, and produces a
/// table with no index: the caller is told the index exists and only learns
/// otherwise on the first search.
#[tokio::test]
async fn create_table_with_vector_indexes_is_refused() {
    if skip_if_supported().await {
        return;
    }
    let name = table_name("create");
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST",
        "VectorIndexes": [{{
            "IndexName": "vidx",
            "Dimensions": 4,
            "DistanceFunction": "COSINE",
            "VectorAttribute": {{"AttributeName": "emb"}},
            "Projection": {{"ProjectionType": "ALL"}}
        }}]
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_validation_exception(status, &text, NOT_SUPPORTED_INDEXES);

    // The refusal must also be effective, not merely worded: no table may exist.
    let (desc_status, desc_body) =
        call("DescribeTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
    assert_eq!(
        desc_status, 400,
        "a refused CreateTable must leave no table behind, got: {desc_body}"
    );
    assert!(
        desc_body.contains("ResourceNotFoundException"),
        "expected ResourceNotFoundException, got: {desc_body}"
    );
}

/// An ordinary `CreateTable` still succeeds, proving the gate does not
/// over-reject. Without this the suite would pass just as well if the gate
/// refused every table.
///
/// Guarded on the endpoint only, not on `skip_if_supported`. This assertion holds
/// on every backend, so skipping it once a backend implements vector search would
/// lose coverage for no reason. The same applies to the two tests below it. Only
/// the tests that assert a *refusal* are specific to non-participating backends.
#[tokio::test]
async fn create_table_without_vector_indexes_still_succeeds() {
    if is_real_dynamodb() {
        return;
    }
    let name = table_name("control");
    let body = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST"
    }}"#
    );
    let (status, text) = call("CreateTable", &body).await;
    assert_eq!(status, 200, "control table must be created, body: {text}");

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// `UpdateTable` is the second creation path, and the one whose backfill
/// lifecycle the contract models, so leaving it ungated would reopen the same
/// silent-drop hole on a different operation.
#[tokio::test]
async fn update_table_creating_a_vector_index_is_refused() {
    if skip_if_supported().await {
        return;
    }
    let name = table_name("update");
    let create = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST"
    }}"#
    );
    let (status, text) = call("CreateTable", &create).await;
    assert_eq!(status, 200, "setup table must be created, body: {text}");

    let body = format!(
        r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [{{
            "Create": {{
                "IndexName": "vidx",
                "Dimensions": 4,
                "DistanceFunction": "COSINE",
                "VectorAttribute": {{"AttributeName": "emb"}},
                "Projection": {{"ProjectionType": "ALL"}}
            }}
        }}]
    }}"#
    );
    let (status, text) = call("UpdateTable", &body).await;
    assert_validation_exception(status, &text, NOT_SUPPORTED_INDEXES);

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// Deleting is refused too, deliberately. A backend with no vector support cannot
/// hold an index to delete, so "not supported here" is the honest answer rather
/// than letting the request through to be read as a no-op or a not-found.
#[tokio::test]
async fn update_table_deleting_a_vector_index_is_refused() {
    if skip_if_supported().await {
        return;
    }
    let name = table_name("delete");
    let create = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST"
    }}"#
    );
    let (status, text) = call("CreateTable", &create).await;
    assert_eq!(status, 200, "setup table must be created, body: {text}");

    let body = format!(
        r#"{{
        "TableName": "{name}",
        "VectorIndexUpdates": [{{"Delete": {{"IndexName": "vidx"}}}}]
    }}"#
    );
    let (status, text) = call("UpdateTable", &body).await;
    assert_validation_exception(status, &text, NOT_SUPPORTED_INDEXES);

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// An `UpdateTable` carrying an empty list is not a request for vector indexes.
/// It must not draw the vector refusal, and because an empty list is also not a
/// change, the correct answer is the ordinary "nothing specified" error. This is
/// the boundary the gate is most likely to get wrong in a future edit, in either
/// direction.
#[tokio::test]
async fn update_table_with_an_empty_vector_list_is_not_a_vector_request() {
    if is_real_dynamodb() {
        return;
    }
    let name = table_name("empty");
    let create = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST"
    }}"#
    );
    let (status, text) = call("CreateTable", &create).await;
    assert_eq!(status, 200, "setup table must be created, body: {text}");

    let body = format!(r#"{{"TableName": "{name}", "VectorIndexUpdates": []}}"#);
    let (_status, text) = call("UpdateTable", &body).await;
    assert!(
        !text.contains("not supported by this storage backend"),
        "an empty list must not draw the vector refusal: {text}"
    );
    assert!(
        text.contains("At least one of"),
        "an empty list is no change, so the nothing-specified error is expected: {text}"
    );

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// `SearchVectors` is refused with its own wording. Checked against a table that
/// exists, so the refusal cannot be a disguised ResourceNotFound.
#[tokio::test]
async fn search_vectors_is_refused() {
    if skip_if_supported().await {
        return;
    }
    let name = table_name("search");
    let create = format!(
        r#"{{
        "TableName": "{name}",
        "AttributeDefinitions": [{{"AttributeName": "pk", "AttributeType": "S"}}],
        "KeySchema": [{{"AttributeName": "pk", "KeyType": "HASH"}}],
        "BillingMode": "PAY_PER_REQUEST"
    }}"#
    );
    let (status, text) = call("CreateTable", &create).await;
    assert_eq!(status, 200, "setup table must be created, body: {text}");

    let body = format!(
        r#"{{
        "TableName": "{name}",
        "IndexName": "vidx",
        "SearchVector": [{{"N": "0.1"}}, {{"N": "0.2"}}, {{"N": "0.3"}}, {{"N": "0.4"}}],
        "TopK": 5
    }}"#
    );
    let (status, text) = call("SearchVectors", &body).await;
    assert_validation_exception(status, &text, NOT_SUPPORTED_SEARCH);

    let _ = call("DeleteTable", &format!(r#"{{"TableName": "{name}"}}"#)).await;
}

/// `SearchVectors` is a known operation, so an unauthenticated call must fail on
/// authentication rather than on the operation name. This pins the ordering the
/// other tests depend on: if the operation were unknown, or if auth ran after
/// dispatch, the refusals above would be asserting something else.
#[tokio::test]
async fn search_vectors_is_a_known_operation_and_requires_auth() {
    if is_real_dynamodb() {
        return;
    }
    let resp = http_client()
        .post(endpoint())
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", "DynamoDB_20120810.SearchVectors")
        .body(r#"{"TableName": "t", "IndexName": "i", "SearchVector": [{"N": "0.1"}], "TopK": 1}"#)
        .send()
        .await
        .expect("request sent");
    let status = resp.status().as_u16();
    let body = resp.text().await.expect("body read");
    assert!(
        !body.contains("UnknownOperationException"),
        "SearchVectors must be a recognized operation, got: {body}"
    );
    assert_eq!(status, 400, "expected an auth failure, body: {body}");
}
