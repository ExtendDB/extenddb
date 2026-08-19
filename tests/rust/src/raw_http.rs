// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Raw HTTP protocol tests — mirrors Java `RawHttpTests`.
//! Bypasses the AWS SDK to test protocol-level behavior directly.

use crate::test_base::is_real_dynamodb;

fn endpoint() -> String {
    std::env::var("EXTENDDB_TEST_ENDPOINT").unwrap_or_else(|_| {
        let region =
            std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into());
        format!("https://dynamodb.{region}.amazonaws.com")
    })
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

#[tokio::test]
async fn invalid_json_body() {
    let c = http_client();
    let resp = c
        .post(endpoint())
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", "DynamoDB_20120810.GetItem")
        .body("{invalid json!!!")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body = resp.text().await.unwrap();
    assert!(body.contains("SerializationException"), "body: {body}");
}

#[tokio::test]
async fn empty_body() {
    // Real DynamoDB requires SigV4 auth; raw unauthenticated requests get
    // MissingAuthenticationToken instead of SerializationException.
    if is_real_dynamodb() {
        return;
    }
    let c = http_client();
    let resp = c
        .post(endpoint())
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", "DynamoDB_20120810.GetItem")
        .body("")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body = resp.text().await.unwrap();
    assert!(body.contains("SerializationException"), "body: {body}");
}

#[tokio::test]
async fn missing_target_header() {
    // Real DynamoDB requires SigV4 auth on all requests.
    if is_real_dynamodb() {
        return;
    }
    let c = http_client();
    let resp = c
        .post(endpoint())
        .header("Content-Type", "application/x-amz-json-1.0")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Missing"), "body: {body}");
}

#[tokio::test]
async fn unknown_operation() {
    let c = http_client();
    let resp = c
        .post(endpoint())
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", "DynamoDB_20120810.FakeOperation")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        400,
        "body: {}",
        resp.text().await.unwrap_or_default()
    );
}

#[tokio::test]
async fn get_method_not_allowed() {
    // Real DynamoDB returns 200 for GET (health check); extenddb returns 405.
    if is_real_dynamodb() {
        return;
    }
    let c = http_client();
    let resp = c.get(endpoint()).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 405);
}

#[tokio::test]
async fn crc32_header_on_error() {
    let c = http_client();
    let resp = c
        .post(endpoint())
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", "DynamoDB_20120810.GetItem")
        .body("{invalid")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let crc32 = resp.headers().get("x-amz-crc32");
    assert!(crc32.is_some(), "Error response should include x-amz-crc32");
    let val: u64 = crc32.unwrap().to_str().unwrap().parse().unwrap();
    assert!(val > 0, "CRC32 should be non-zero");
}

#[tokio::test]
async fn auth_error_response_format() {
    let c = http_client();
    let resp = c
        .post(endpoint())
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", "DynamoDB_20120810.ListTables")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(ct, "application/x-amz-json-1.0");
}

#[tokio::test]
async fn health_endpoint() {
    // /health is a extenddb-specific extension, not present on real DynamoDB.
    if is_real_dynamodb() {
        return;
    }
    let c = http_client();
    let resp = c
        .get(format!("{}/health", endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("\"status\":\"healthy\""), "body: {body}");
}

#[tokio::test]
async fn metrics_endpoint() {
    // /metrics is an extenddb-specific extension, not present on real DynamoDB.
    // It requires admin authentication; an unauthenticated request returns 401.
    if is_real_dynamodb() {
        return;
    }
    let c = http_client();
    let resp = c
        .get(format!("{}/metrics", endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
}

#[tokio::test]
async fn response_content_type() {
    let c = http_client();
    let resp = c
        .post(endpoint())
        .header("Content-Type", "application/x-amz-json-1.0")
        .header("X-Amz-Target", "DynamoDB_20120810.GetItem")
        .body("not json")
        .send()
        .await
        .unwrap();
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert_eq!(ct, "application/x-amz-json-1.0");
}
