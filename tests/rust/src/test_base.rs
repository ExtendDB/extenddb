// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared test infrastructure for Rust integration tests.
//!
//! Mirrors the Java `DynamoDBTestBase` class: creates shared tables,
//! provides helper functions for item creation, key extraction, and assertions.

use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::Region;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, Projection, ProjectionType, ScalarAttributeType, TableStatus,
};
use aws_sdk_dynamodb::Client;
use aws_smithy_http_client::tls;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::OnceCell as AsyncOnceCell;
use uuid::Uuid;

// Re-export helpers so `use crate::test_base::*` still works.
pub use crate::helpers::*;

// Key attribute names (matching Java test base).
pub const HASH_KEY_S: &str = "hashKey";
pub const RANGE_KEY_N: &str = "rangeKey";
pub const RANGE_KEY_S: &str = "rangeKeyS";
pub const HASH_KEY_B: &str = "hashKeyB";
pub const HASH_KEY_N: &str = "hashKeyN";
pub const GSI_HASH_KEY: &str = "gsiHashKey";
pub const GSI_RANGE_KEY: &str = "gsiRangeKey";
pub const GSI_NAME: &str = "gsi_index";

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared test tables — initialized once per test run.
pub struct TestTables {
    pub simple_key_string: String,
    pub comp_key_string_number: String,
    pub comp_key_blob_number: String,
    pub comp_key_number_string: String,
    pub simple_key_string_gsi: String,
    pub comp_key_string_string_gsi: String,
}

static TEST_TABLES: AsyncOnceCell<TestTables> = AsyncOnceCell::const_new();
static CLIENT: OnceLock<Client> = OnceLock::new();

/// Build a DynamoDB client configured from environment variables.
///
/// - `EXTENDDB_TEST_ENDPOINT` set → use that endpoint (local extenddb mode).
///   Not set → use SDK default endpoint resolution (real DynamoDB mode).
/// - `AWS_ACCESS_KEY_ID` set → use explicit credentials from env vars
///   (requires `AWS_SECRET_ACCESS_KEY`). Not set → use SDK default
///   credential provider chain (`~/.aws/credentials`, instance profile, etc.).
/// - `AWS_DEFAULT_REGION` → region (defaults to `us-east-1`).
/// - `EXTENDDB_CA_CERT` → path to CA cert PEM for self-signed TLS (local mode).
fn build_client() -> Client {
    let endpoint = std::env::var("EXTENDDB_TEST_ENDPOINT").ok();
    let region = std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".into());

    let creds_provider: SharedCredentialsProvider =
        if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
            let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| {
                panic!(
                    "AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is not. \
                     Both must be set for explicit credentials."
                )
            });
            SharedCredentialsProvider::new(Credentials::new(
                access_key, secret_key, None, None, "env",
            ))
        } else {
            // No explicit credentials — use the SDK default credential chain.
            // DefaultCredentialsChain::builder().build() is async; run it on a
            // separate thread with its own runtime since build_client() is sync
            // and called from within a #[tokio::test] current_thread runtime.
            let chain = std::thread::spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create credential-chain runtime")
                    .block_on(
                        aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
                            .build(),
                    )
            })
            .join()
            .expect("credential chain thread panicked");
            SharedCredentialsProvider::new(chain)
        };

    // Build HTTP client with custom TLS trust store for self-signed certs.
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
        .credentials_provider(creds_provider)
        .http_client(http_client);

    if let Some(ep) = endpoint {
        config_builder = config_builder.endpoint_url(ep);
    }

    Client::from_conf(config_builder.build())
}

/// Get or initialize the shared DynamoDB client.
pub fn client() -> &'static Client {
    CLIENT.get_or_init(build_client)
}

/// Returns `true` when tests target real DynamoDB (no `EXTENDDB_TEST_ENDPOINT`).
pub fn is_real_dynamodb() -> bool {
    std::env::var("EXTENDDB_TEST_ENDPOINT").is_err()
}

/// Get or initialize the shared test tables.
pub async fn tables() -> &'static TestTables {
    TEST_TABLES
        .get_or_init(|| async {
            let c = client();
            let suffix = format!(
                "_{}_{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                COUNTER.fetch_add(1, Ordering::SeqCst)
            );

            let t = TestTables {
                simple_key_string: format!("SimpleKeyString{suffix}"),
                comp_key_string_number: format!("CompKeyStringNumber{suffix}"),
                comp_key_blob_number: format!("CompKeyBlobNumber{suffix}"),
                comp_key_number_string: format!("CompKeyNumberString{suffix}"),
                simple_key_string_gsi: format!("SimpleKeyStringGSI{suffix}"),
                comp_key_string_string_gsi: format!("CompKeyStringStringGSI{suffix}"),
            };

            create_table(
                c,
                &t.simple_key_string,
                HASH_KEY_S,
                ScalarAttributeType::S,
                None,
                None,
                None,
            )
            .await;
            create_table(
                c,
                &t.comp_key_string_number,
                HASH_KEY_S,
                ScalarAttributeType::S,
                Some(RANGE_KEY_N),
                Some(ScalarAttributeType::N),
                None,
            )
            .await;
            create_table(
                c,
                &t.comp_key_blob_number,
                HASH_KEY_B,
                ScalarAttributeType::B,
                Some(RANGE_KEY_N),
                Some(ScalarAttributeType::N),
                None,
            )
            .await;
            create_table(
                c,
                &t.comp_key_number_string,
                HASH_KEY_N,
                ScalarAttributeType::N,
                Some(RANGE_KEY_S),
                Some(ScalarAttributeType::S),
                None,
            )
            .await;
            create_table_with_gsi(
                c,
                &t.simple_key_string_gsi,
                HASH_KEY_S,
                ScalarAttributeType::S,
                None,
                None,
            )
            .await;
            create_table_with_gsi(
                c,
                &t.comp_key_string_string_gsi,
                HASH_KEY_S,
                ScalarAttributeType::S,
                Some(RANGE_KEY_S),
                Some(ScalarAttributeType::S),
            )
            .await;

            t
        })
        .await
}

// ========== Table creation helpers ==========
//
// Tables created during tests are NOT deleted by Rust code. Cleanup is handled
// by `devtools/run-tests` which bulk-deletes all test tables after the test
// binary exits (see the "Post-test table cleanup" section in that script).
//
// This avoids per-test `wait_for_deleted` polling loops, which on real DynamoDB
// can take up to 60 seconds per table — a significant overhead when many tests
// each create their own table. On ExtendDB the tables vanish with the database
// anyway (CI uses a fresh PostgreSQL container per run).
//
// The only tests that issue DeleteTable are those in `table_operations.rs` which
// specifically verify deletion behavior.

async fn create_table(
    c: &Client,
    name: &str,
    hash_key: &str,
    hash_type: ScalarAttributeType,
    range_key: Option<&str>,
    range_type: Option<ScalarAttributeType>,
    gsis: Option<Vec<GlobalSecondaryIndex>>,
) {
    let mut attr_defs = vec![AttributeDefinition::builder()
        .attribute_name(hash_key)
        .attribute_type(hash_type)
        .build()
        .unwrap()];

    if let (Some(rk), Some(rt)) = (range_key, range_type) {
        attr_defs.push(
            AttributeDefinition::builder()
                .attribute_name(rk)
                .attribute_type(rt)
                .build()
                .unwrap(),
        );
    }

    create_table_raw(c, name, hash_key, range_key, attr_defs, gsis).await;
}

async fn create_table_raw(
    c: &Client,
    name: &str,
    hash_key: &str,
    range_key: Option<&str>,
    attr_defs: Vec<AttributeDefinition>,
    gsis: Option<Vec<GlobalSecondaryIndex>>,
) {
    let mut key_schema = vec![KeySchemaElement::builder()
        .attribute_name(hash_key)
        .key_type(KeyType::Hash)
        .build()
        .unwrap()];

    if let Some(rk) = range_key {
        key_schema.push(
            KeySchemaElement::builder()
                .attribute_name(rk)
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        );
    }

    let mut req = c
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .set_key_schema(Some(key_schema))
        .set_attribute_definitions(Some(attr_defs));

    if let Some(g) = gsis {
        req = req.set_global_secondary_indexes(Some(g));
    }

    match req.send().await {
        Ok(_) => {}
        Err(e) => {
            if err_code(&e) != Some("ResourceInUseException") {
                panic!("Failed to create table {name}: {e:?}");
            }
        }
    }
    wait_for_active(c, name).await;
}

async fn create_table_with_gsi(
    c: &Client,
    name: &str,
    hash_key: &str,
    hash_type: ScalarAttributeType,
    range_key: Option<&str>,
    range_type: Option<ScalarAttributeType>,
) {
    let mut attr_defs = vec![
        AttributeDefinition::builder()
            .attribute_name(hash_key)
            .attribute_type(hash_type.clone())
            .build()
            .unwrap(),
        AttributeDefinition::builder()
            .attribute_name(GSI_HASH_KEY)
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
        AttributeDefinition::builder()
            .attribute_name(GSI_RANGE_KEY)
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
    ];
    if let (Some(rk), Some(rt)) = (range_key, range_type.clone()) {
        attr_defs.push(
            AttributeDefinition::builder()
                .attribute_name(rk)
                .attribute_type(rt)
                .build()
                .unwrap(),
        );
    }

    let gsi = GlobalSecondaryIndex::builder()
        .index_name(GSI_NAME)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(GSI_HASH_KEY)
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(GSI_RANGE_KEY)
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .projection(
            Projection::builder()
                .projection_type(ProjectionType::All)
                .build(),
        )
        .build()
        .unwrap();

    create_table_raw(c, name, hash_key, range_key, attr_defs, Some(vec![gsi])).await;
}

/// Poll DescribeTable until ACTIVE (up to 60s).
///
/// Uses 100ms polling for local ExtendDB (tables are instantly active)
/// and 1s polling for real DynamoDB (async table creation).
pub async fn wait_for_active(c: &Client, name: &str) {
    let interval = if is_real_dynamodb() {
        Duration::from_secs(1)
    } else {
        Duration::from_millis(100)
    };
    let max_attempts = if is_real_dynamodb() { 60 } else { 100 };

    for _ in 0..max_attempts {
        if let Ok(resp) = c.describe_table().table_name(name).send().await {
            if let Some(table) = resp.table() {
                if table.table_status() == Some(&TableStatus::Active) {
                    return;
                }
            }
        }
        tokio::time::sleep(interval).await;
    }
    panic!("Table {name} did not become ACTIVE within timeout");
}

/// Poll DescribeTable until the table no longer exists (up to 60s).
///
/// Uses adaptive polling interval like `wait_for_active`.
pub async fn wait_for_deleted(c: &Client, name: &str) {
    let interval = if is_real_dynamodb() {
        Duration::from_secs(1)
    } else {
        Duration::from_millis(100)
    };
    let max_attempts = if is_real_dynamodb() { 60 } else { 100 };

    for _ in 0..max_attempts {
        match c.describe_table().table_name(name).send().await {
            Err(e) => {
                if err_code(&e) == Some("ResourceNotFoundException") {
                    return;
                }
            }
            Ok(_) => {}
        }
        tokio::time::sleep(interval).await;
    }
    panic!("Table {name} was not deleted within timeout");
}

// ========== Item helpers ==========

/// Unique counter for range key values — avoids collision risk of timestamp modulo.
fn unique_range() -> i64 {
    COUNTER.fetch_add(1, Ordering::SeqCst) as i64
}

/// Create an item with appropriate keys for the given table.
pub fn create_item(table_name: &str) -> HashMap<String, AttributeValue> {
    let unique = Uuid::new_v4().to_string();
    let mut item = HashMap::new();

    if table_name.starts_with("SimpleKeyString") {
        item.insert(HASH_KEY_S.into(), s(&unique));
    } else if table_name.starts_with("CompKeyStringNumber") {
        item.insert(HASH_KEY_S.into(), s(&unique));
        item.insert(RANGE_KEY_N.into(), n(unique_range()));
    } else if table_name.starts_with("CompKeyBlobNumber") {
        item.insert(HASH_KEY_B.into(), b(&unique));
        item.insert(RANGE_KEY_N.into(), n(unique_range()));
    } else if table_name.starts_with("CompKeyNumberString") {
        item.insert(HASH_KEY_N.into(), n(unique_range()));
        item.insert(RANGE_KEY_S.into(), s(&unique));
    } else if table_name.starts_with("CompKeyStringStringGSI") {
        item.insert(HASH_KEY_S.into(), s(&unique));
        item.insert(RANGE_KEY_S.into(), s(&format!("range_{unique}")));
    } else {
        item.insert(HASH_KEY_S.into(), s(&unique));
    }

    item.insert("str".into(), s(&format!("testValue_{}", &unique[..8])));
    item.insert("num".into(), n(42));
    item
}

/// Create an item with GSI key attributes.
pub fn create_item_with_gsi(table_name: &str) -> HashMap<String, AttributeValue> {
    let mut item = create_item(table_name);
    let unique = Uuid::new_v4().to_string();
    item.insert(GSI_HASH_KEY.into(), s(&format!("gsi_{unique}")));
    item.insert(GSI_RANGE_KEY.into(), s(&format!("gsir_{unique}")));
    item
}

/// Extract key attributes from an item for the given table.
pub fn get_key(
    table_name: &str,
    item: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    if table_name.starts_with("SimpleKeyString") {
        key.insert(HASH_KEY_S.into(), item[HASH_KEY_S].clone());
    } else if table_name.starts_with("CompKeyStringNumber") {
        key.insert(HASH_KEY_S.into(), item[HASH_KEY_S].clone());
        key.insert(RANGE_KEY_N.into(), item[RANGE_KEY_N].clone());
    } else if table_name.starts_with("CompKeyBlobNumber") {
        key.insert(HASH_KEY_B.into(), item[HASH_KEY_B].clone());
        key.insert(RANGE_KEY_N.into(), item[RANGE_KEY_N].clone());
    } else if table_name.starts_with("CompKeyNumberString") {
        key.insert(HASH_KEY_N.into(), item[HASH_KEY_N].clone());
        key.insert(RANGE_KEY_S.into(), item[RANGE_KEY_S].clone());
    } else if table_name.starts_with("CompKeyStringStringGSI") {
        key.insert(HASH_KEY_S.into(), item[HASH_KEY_S].clone());
        key.insert(RANGE_KEY_S.into(), item[RANGE_KEY_S].clone());
    } else {
        key.insert(HASH_KEY_S.into(), item[HASH_KEY_S].clone());
    }
    key
}
