// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization tests — mirrors Java `AuthorizationTests`.
//! Tests SigV4 validation, invalid credentials, unknown access keys.

use crate::test_base::*;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::Region;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use aws_smithy_http_client::tls;

fn client_with(access_key: &str, secret_key: &str) -> Client {
    let endpoint = std::env::var("EXTENDDB_TEST_ENDPOINT").ok();
    let region = std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into());

    let creds = Credentials::new(access_key, secret_key, None, None, "test");

    let mut trust_store = tls::TrustStore::empty().with_native_roots(true);
    if let Ok(ca_path) = std::env::var("EXTENDDB_CA_CERT") {
        if let Ok(pem) = std::fs::read(&ca_path) {
            trust_store = trust_store.with_pem_certificate(pem);
        }
    }
    let tls_context = tls::TlsContext::builder()
        .with_trust_store(trust_store)
        .build()
        .expect("TLS context build failed");
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::Ring,
        ))
        .tls_context(tls_context)
        .build_https();

    let mut config_builder = aws_sdk_dynamodb::Config::builder()
        .behavior_version_latest()
        .region(Region::new(region))
        .credentials_provider(SharedCredentialsProvider::new(creds))
        .http_client(http_client);

    if let Some(ep) = endpoint {
        config_builder = config_builder.endpoint_url(ep);
    }

    Client::from_conf(config_builder.build())
}

#[tokio::test]
async fn invalid_access_key_rejected() {
    let bad = client_with("INVALID_KEY", "INVALID_SECRET");
    let err = bad.list_tables().send().await.unwrap_err();
    // Should get an auth error — any error code is acceptable as long as it fails
    assert!(
        err_code(&err).is_some(),
        "Expected auth error, got: {err:?}"
    );
}

#[tokio::test]
async fn unknown_access_key_rejected() {
    let c = client();
    let table = format!("AuthTest_{}", ts());
    c.create_table()
        .table_name(&table)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    let bad = client_with("unknown-key-12345", "some-secret");
    let err = bad
        .get_item()
        .table_name(&table)
        .key("pk", s("test"))
        .send()
        .await
        .unwrap_err();
    assert!(
        err_code(&err).is_some(),
        "Expected auth error, got: {err:?}"
    );
}

#[tokio::test]
async fn wrong_secret_key_rejected() {
    let bad = client_with("dummy-key", "wrong-secret-key");
    let err = bad.list_tables().send().await.unwrap_err();
    assert!(
        err_code(&err).is_some(),
        "Expected auth error, got: {err:?}"
    );
}

#[tokio::test]
async fn valid_credentials_succeed() {
    let c = client();
    // Default client uses valid credentials — should succeed
    let resp = c.list_tables().send().await;
    assert!(resp.is_ok(), "Valid credentials should succeed");
}

#[tokio::test]
async fn valid_credentials_put_and_get() {
    let c = client();
    let table = format!("AuthPutGet_{}", ts());
    c.create_table()
        .table_name(&table)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    let key = uuid::Uuid::new_v4().to_string();
    c.put_item()
        .table_name(&table)
        .item("pk", s(&key))
        .item("data", s("auth_test"))
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(&table)
        .key("pk", s(&key))
        .send()
        .await
        .unwrap();
    let item = resp.item().expect("Item should exist");
    assert_eq!(item.get("data").unwrap().as_s().unwrap(), "auth_test");
}
