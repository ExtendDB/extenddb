// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Batch and transaction operations authorize each table individually.
//!
//! A `Deny` on one table must block a `BatchGetItem`/`BatchWriteItem`/
//! `TransactGetItems`/`TransactWriteItems` that touches it, even when the same
//! request also touches an allowed table (all-or-nothing) — the request must
//! not be evaluated against a single `table/*` wildcard.
//!
//! The IAM action evaluated per table follows the AWS IAM Service Authorization
//! Reference: `BatchGetItem`/`BatchWriteItem` are their own actions;
//! `TransactGetItems` decomposes to `GetItem`; `TransactWriteItems` decomposes
//! to `PutItem`/`DeleteItem`/`UpdateItem`/`ConditionCheckItem` per sub-op. The
//! guard tests assert that distinction (a `GetItem` deny must NOT block
//! `BatchGetItem`, but MUST block `TransactGetItems`).

use crate::test_base::*;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::Region;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, KeysAndAttributes, Put,
    PutRequest, ScalarAttributeType, TransactGetItem, TransactWriteItem, WriteRequest,
};
use aws_sdk_dynamodb::Client;
use aws_smithy_http_client::tls;
use serde_json::{json, Value};
use std::collections::HashMap;

const TEST_ACCOUNT: &str = "123456789012";

fn endpoint() -> String {
    std::env::var("EXTENDDB_TEST_ENDPOINT").expect("EXTENDDB_TEST_ENDPOINT must be set")
}

/// Build a SigV4 DynamoDB client for an arbitrary access key / secret.
fn client_for(access_key: &str, secret_key: &str) -> Client {
    let region = std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into());
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
        .tls_provider(tls::Provider::Rustls(tls::rustls_provider::CryptoMode::Ring))
        .tls_context(tls_context)
        .build_https();
    let conf = aws_sdk_dynamodb::Config::builder()
        .behavior_version_latest()
        .region(Region::new(region))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            access_key, secret_key, None, None, "test",
        )))
        .http_client(http_client)
        .endpoint_url(endpoint())
        .build();
    Client::from_conf(conf)
}

/// Management HTTP client (admin Basic auth, self-signed cert accepted).
fn mgmt() -> (reqwest::Client, String, (String, String)) {
    let base = format!("{}/management", endpoint().trim_end_matches('/'));
    let user = std::env::var("EXTENDDB_ADMIN_USER").unwrap_or_else(|_| "admin".into());
    let pass = std::env::var("EXTENDDB_ADMIN_PASSWORD").expect("EXTENDDB_ADMIN_PASSWORD must be set");
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true) // localhost self-signed; test only
        .build()
        .expect("reqwest build");
    (client, base, (user, pass))
}

/// Create a fresh IAM user with the given inline policy and return a SigV4
/// client authenticated as that user.
async fn user_with_policy(policy: Value) -> Client {
    let (http, base, (au, ap)) = mgmt();
    let user = format!("bta-{}", ts());

    let r = http
        .post(format!("{base}/accounts/{TEST_ACCOUNT}/users"))
        .basic_auth(&au, Some(&ap))
        .json(&json!({ "user_name": user }))
        .send()
        .await
        .expect("create_user send");
    assert!(
        r.status().is_success(),
        "create_user failed: {}",
        r.text().await.unwrap_or_default()
    );

    let r = http
        .put(format!("{base}/accounts/{TEST_ACCOUNT}/users/{user}/policy/p"))
        .basic_auth(&au, Some(&ap))
        .json(&policy)
        .send()
        .await
        .expect("put_user_policy send");
    assert!(
        r.status().is_success(),
        "put_user_policy failed: {}",
        r.text().await.unwrap_or_default()
    );

    let r = http
        .post(format!("{base}/accounts/{TEST_ACCOUNT}/users/{user}/access-keys"))
        .basic_auth(&au, Some(&ap))
        .send()
        .await
        .expect("create_access_key send");
    assert!(r.status().is_success(), "create_access_key failed");
    let creds: Value = r.json().await.expect("access key json");
    let ak = creds["access_key_id"].as_str().expect("access_key_id");
    let sk = creds["secret_access_key"].as_str().expect("secret_access_key");
    client_for(ak, sk)
}

async fn make_table(c: &Client, name: &str) {
    c.create_table()
        .table_name(name)
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
    wait_for_active(c, name).await;
}

fn deny_policy(action: &str, table_arn: &str) -> Value {
    json!({
        "Version": "2012-10-17",
        "Statement": [
            { "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*" },
            { "Effect": "Deny", "Action": action, "Resource": table_arn }
        ]
    })
}

fn key(pk: &str) -> HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
    HashMap::from([(
        "pk".to_owned(),
        aws_sdk_dynamodb::types::AttributeValue::S(pk.to_owned()),
    )])
}

fn is_denied(code: Option<&str>) -> bool {
    code == Some("AccessDeniedException")
}

/// Two shared tables for the suite; items seeded by the admin client.
/// Returns `(allowed_name, secret_name, secret_table_arn)`.
async fn tables() -> (String, String, String) {
    let admin = client();
    let allowed = format!("BtaAllowed_{}", ts());
    let secret = format!("BtaSecret_{}", ts());
    make_table(admin, &allowed).await;
    make_table(admin, &secret).await;
    admin
        .put_item()
        .table_name(&allowed)
        .set_item(Some(key("a1")))
        .send()
        .await
        .unwrap();
    admin
        .put_item()
        .table_name(&secret)
        .set_item(Some(key("s1")))
        .send()
        .await
        .unwrap();
    let secret_arn = admin
        .describe_table()
        .table_name(&secret)
        .send()
        .await
        .unwrap()
        .table()
        .unwrap()
        .table_arn()
        .unwrap()
        .to_owned();
    (allowed, secret, secret_arn)
}

// --- A: Deny dynamodb:* on the secret table -------------------------------

/// Skip when admin creds are unavailable, matching the Python auth suite
/// (`test_abac.py`, `test_auth_integration.py`). `devtools/run-tests --extenddb
/// --rust-integration` sets `EXTENDDB_ADMIN_PASSWORD`, so CI still runs these
/// for real; a bare `cargo test` without it skips rather than hard-failing.
fn skip_no_admin() -> bool {
    if std::env::var("EXTENDDB_ADMIN_PASSWORD").is_err() {
        eprintln!(
            "SKIP: EXTENDDB_ADMIN_PASSWORD not set; run via \
             devtools/run-tests --extenddb --rust-integration"
        );
        return true;
    }
    false
}

#[tokio::test]
async fn batch_get_including_denied_table_is_denied() {
    if skip_no_admin() {
        return;
    }
    let (allowed, secret, secret_arn) = tables().await;
    let u = user_with_policy(deny_policy("dynamodb:*", &secret_arn)).await;
    let err = u
        .batch_get_item()
        .request_items(&allowed, KeysAndAttributes::builder().keys(key("a1")).build().unwrap())
        .request_items(&secret, KeysAndAttributes::builder().keys(key("s1")).build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert!(
        is_denied(err_code(&err)),
        "batch touching denied table must be AccessDenied, got {err:?}"
    );
    // Positive control: allowed-only batch succeeds.
    u.batch_get_item()
        .request_items(&allowed, KeysAndAttributes::builder().keys(key("a1")).build().unwrap())
        .send()
        .await
        .expect("allowed-only batch must succeed");
}

#[tokio::test]
async fn batch_write_including_denied_table_is_denied() {
    if skip_no_admin() {
        return;
    }
    let (_allowed, secret, secret_arn) = tables().await;
    let u = user_with_policy(deny_policy("dynamodb:*", &secret_arn)).await;
    let err = u
        .batch_write_item()
        .request_items(
            &secret,
            vec![WriteRequest::builder()
                .put_request(PutRequest::builder().set_item(Some(key("z1"))).build().unwrap())
                .build()],
        )
        .send()
        .await
        .unwrap_err();
    assert!(is_denied(err_code(&err)), "got {err:?}");
}

#[tokio::test]
async fn transact_get_including_denied_table_is_denied() {
    if skip_no_admin() {
        return;
    }
    let (_allowed, secret, secret_arn) = tables().await;
    let u = user_with_policy(deny_policy("dynamodb:*", &secret_arn)).await;
    let err = u
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    aws_sdk_dynamodb::types::Get::builder()
                        .table_name(&secret)
                        .set_key(Some(key("s1")))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert!(is_denied(err_code(&err)), "got {err:?}");
}

#[tokio::test]
async fn transact_write_including_denied_table_is_denied() {
    if skip_no_admin() {
        return;
    }
    let (_allowed, secret, secret_arn) = tables().await;
    let u = user_with_policy(deny_policy("dynamodb:*", &secret_arn)).await;
    let err = u
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(Put::builder().table_name(&secret).set_item(Some(key("z2"))).build().unwrap())
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert!(is_denied(err_code(&err)), "got {err:?}");
}

// --- B/C: action-decomposition guards -------------------------------------
// These pin that we authorize each op under the CORRECT IAM action, not a
// naive one. A GetItem/PutItem deny must NOT leak onto the distinct batch
// actions, but MUST hit the transaction sub-op actions.

#[tokio::test]
async fn getitem_deny_does_not_block_batch_get() {
    if skip_no_admin() {
        return;
    }
    let (_allowed, secret, secret_arn) = tables().await;
    let u = user_with_policy(deny_policy("dynamodb:GetItem", &secret_arn)).await;
    // BatchGetItem is its own action; a GetItem deny does not apply.
    u.batch_get_item()
        .request_items(&secret, KeysAndAttributes::builder().keys(key("s1")).build().unwrap())
        .send()
        .await
        .expect("GetItem deny must not block BatchGetItem");
}

#[tokio::test]
async fn getitem_deny_blocks_transact_get() {
    if skip_no_admin() {
        return;
    }
    let (_allowed, secret, secret_arn) = tables().await;
    let u = user_with_policy(deny_policy("dynamodb:GetItem", &secret_arn)).await;
    // TransactGetItems decomposes to GetItem, so the deny applies.
    let err = u
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    aws_sdk_dynamodb::types::Get::builder()
                        .table_name(&secret)
                        .set_key(Some(key("s1")))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert!(is_denied(err_code(&err)), "got {err:?}");
}

#[tokio::test]
async fn putitem_deny_does_not_block_batch_write() {
    if skip_no_admin() {
        return;
    }
    let (_allowed, secret, secret_arn) = tables().await;
    let u = user_with_policy(deny_policy("dynamodb:PutItem", &secret_arn)).await;
    u.batch_write_item()
        .request_items(
            &secret,
            vec![WriteRequest::builder()
                .put_request(PutRequest::builder().set_item(Some(key("z3"))).build().unwrap())
                .build()],
        )
        .send()
        .await
        .expect("PutItem deny must not block BatchWriteItem");
}

#[tokio::test]
async fn putitem_deny_blocks_transact_write_put() {
    if skip_no_admin() {
        return;
    }
    let (_allowed, secret, secret_arn) = tables().await;
    let u = user_with_policy(deny_policy("dynamodb:PutItem", &secret_arn)).await;
    let err = u
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(Put::builder().table_name(&secret).set_item(Some(key("z4"))).build().unwrap())
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert!(is_denied(err_code(&err)), "got {err:?}");
}
