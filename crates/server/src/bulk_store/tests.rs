// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the bulk store resolution at startup.

use std::future::Future;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};

use extenddb_storage::bulk_operation_store::{
    CancelOutcome, InitialFailure, NewBulkOperation, StartOutcome,
};
use extenddb_storage::server_components::ServerComponentsOptions;

use super::*;
use crate::test_logs::logged;

/// The `[server] region` of every case, so a region taken from it is told
/// apart from the SDK's and the configuration's.
const SERVER_REGION: &str = "eu-west-3";

/// A `[bulk]` section, written as the configuration file would hold it.
fn bulk(section: Value) -> config::BulkConfig {
    serde_json::from_value(section).expect("a [bulk] section")
}

/// An in-memory catalog and storage engine.
async fn components() -> (Arc<dyn CatalogStore>, Arc<dyn StorageEngine>) {
    let config = extenddb_storage_sqlite::SqliteConfig {
        path: ":memory:".to_owned(),
        pool_size: 2,
    };
    let components = (extenddb_storage_sqlite::backend().server_components)(
        &config,
        SERVER_REGION,
        ServerComponentsOptions::default(),
    )
    .await
    .expect("server components");
    (components.catalog_store, components.engine)
}

/// A fresh directory under the system temp directory.
fn scratch() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("eb-bulk-store-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn the_s3_settings_take_the_server_region_when_the_section_names_none() {
    for section in [json!({"store": "s3"}), json!({"store": "s3", "s3": {}})] {
        let settings = s3_settings(&bulk(section), SERVER_REGION).unwrap();
        assert_eq!(settings.region, SERVER_REGION);
        assert_eq!(settings.endpoint_url, None);
        assert!(!settings.force_path_style);
    }
}

#[test]
fn the_s3_settings_carry_every_key_of_the_section() {
    let section = json!({
        "store": "s3",
        "s3": {
            "region": "ap-south-2",
            "endpoint_url": "http://127.0.0.1:7070",
            "force_path_style": true
        },
        "filesystem": {"path": "/unused"}
    });
    let settings = s3_settings(&bulk(section), SERVER_REGION).unwrap();
    assert_eq!(settings.region, "ap-south-2");
    assert_eq!(
        settings.endpoint_url.as_deref(),
        Some("http://127.0.0.1:7070")
    );
    assert!(settings.force_path_style);
}

/// The variable that tells a child process of this test binary to run the
/// case's body, holding the case's scratch directory.
const CHILD: &str = "EXTENDDB_TEST_BULK_STORE_RESOLUTION_CHILD";

/// Static keys the S3 store's credential chain resolves.
const KEYS: &[(&str, &str)] = &[
    ("AWS_ACCESS_KEY_ID", "AKIDRESOLUTIONTEST"),
    ("AWS_SECRET_ACCESS_KEY", "resolution-test-secret"),
];

/// Run `body` for the test `test` in a child process of this test binary
/// whose environment holds fixed values only: the variables `keys` names,
/// no config or credentials file, instance metadata off. A case that hands
/// an S3 section to the resolution runs there, because building the S3
/// store reaches the standard AWS credential chain, which must stay away
/// from a developer's profiles and from any instance metadata service. A
/// case that expects a refusal runs there with no keys, so a refusal that
/// stops working builds a store whose credentials do not resolve, and no
/// identity check starts. Every S3 section of a case that builds a store
/// names an endpoint, which is never contacted: building the store sends no
/// request. Returns the child's output, its log lines included, to the
/// parent, and `None` in the child.
fn in_fixed_aws_environment<F: Future<Output = ()>>(
    test: &str,
    keys: &[(&str, &str)],
    body: impl FnOnce(PathBuf) -> F,
) -> Option<String> {
    if let Some(dir) = std::env::var_os(CHILD) {
        let _ = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(std::io::stdout)
            .try_init();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(body(PathBuf::from(dir)));
        return None;
    }
    let dir = scratch();
    let module = module_path!().split_once("::").map_or("", |(_, rest)| rest);
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            format!("{module}::{test}").as_str(),
            "--exact",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env_clear()
        .env(CHILD, &dir)
        .env("HOME", &dir)
        .envs(keys.iter().copied())
        .env("AWS_CONFIG_FILE", dir.join("no-config"))
        .env("AWS_SHARED_CREDENTIALS_FILE", dir.join("no-credentials"))
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("1 passed"), "{stdout}\n{stderr}");
    Some(stdout.into_owned())
}

#[test]
fn an_s3_section_that_fails_its_checks_stops_the_server() {
    in_fixed_aws_environment(
        "an_s3_section_that_fails_its_checks_stops_the_server",
        &[],
        |_| refusals(),
    );
}

async fn refusals() {
    let (catalog, storage) = components().await;
    for (s3, refusal) in [
        (
            json!({"secret_access_key": "never-read"}),
            "[bulk.s3] secret_access_key is not accepted: the S3 store reads its credentials \
             from the standard AWS credential chain",
        ),
        (
            json!({"region": "us east-1"}),
            "[bulk.s3] region must not contain whitespace",
        ),
        (
            json!({"endpoint_url": "ftp://objects.example"}),
            "[bulk.s3] endpoint_url must be an absolute http or https URL with a host",
        ),
        (
            json!({"endpoint_url": "https://key:secret@objects.example"}),
            "[bulk.s3] endpoint_url must not carry user information",
        ),
        (
            json!({"endpoint_url": "http://objects.example:9000", "force_path_style": true}),
            "[bulk.s3] endpoint_url uses http:// to a host that is not a recognised loopback \
             form",
        ),
    ] {
        let section = bulk(json!({"store": "s3", "s3": s3}));
        for dev_mode in [false, true] {
            let error = resolve(Some(&section), SERVER_REGION, dev_mode, &catalog, &storage)
                .await
                .err()
                .unwrap_or_else(|| panic!("{s3} must stop the server"));
            let message = error.to_string();
            assert!(message.starts_with(refusal), "{message}");
            assert!(!message.contains("never-read") && !message.contains("secret@"));
        }
    }
}

#[tokio::test]
async fn an_s3_section_beside_the_filesystem_store_is_checked() {
    // The filesystem branch builds no S3 client, so no AWS environment is
    // read here.
    let (catalog, storage) = components().await;
    let path = scratch();
    for (s3, refusal) in [
        (
            json!({"secret_access_key": "never-read"}),
            "[bulk.s3] secret_access_key is not accepted",
        ),
        (
            json!({"endpoint_url": "http://key:secret@127.0.0.1:9"}),
            "[bulk.s3] endpoint_url must not carry user information",
        ),
        (
            json!({"endpoint_url": "http://objects.example:9"}),
            "[bulk.s3] endpoint_url uses http:// to a host that is not a recognised loopback \
             form",
        ),
    ] {
        let section = bulk(json!({"store": "filesystem", "filesystem": {"path": path}, "s3": s3}));
        let message = resolve(Some(&section), SERVER_REGION, false, &catalog, &storage)
            .await
            .err()
            .unwrap_or_else(|| panic!("{s3} must stop the server"))
            .to_string();
        assert!(message.starts_with(refusal), "{message}");
        assert!(!message.contains("never-read") && !message.contains("secret@"));
    }
    let section = bulk(json!({
        "filesystem": {"path": path},
        "s3": {"endpoint_url": "http://127.0.0.1:9"}
    }));
    let store = resolve(Some(&section), SERVER_REGION, false, &catalog, &storage)
        .await
        .unwrap()
        .expect("the filesystem store");
    assert!(store.filesystem_root().is_some());
    assert_eq!(store.s3_access_key_id(), None);
}

#[tokio::test]
async fn an_s3_section_with_no_store_and_no_filesystem_section_names_both_ways_out() {
    let (catalog, storage) = components().await;
    let section = bulk(json!({"s3": {"endpoint_url": "http://127.0.0.1:9"}}));
    let message = resolve(Some(&section), SERVER_REGION, false, &catalog, &storage)
        .await
        .err()
        .expect("no store can be built")
        .to_string();
    assert_eq!(
        message,
        "[bulk.filesystem] path is required when store = \"filesystem\", the default; to use \
         the [bulk.s3] section, set store = \"s3\" in [bulk], or add [bulk.filesystem]"
    );
    let bare = bulk(json!({}));
    let message = resolve(Some(&bare), SERVER_REGION, false, &catalog, &storage)
        .await
        .err()
        .expect("no store can be built")
        .to_string();
    assert_eq!(
        message,
        "[bulk.filesystem] path is required when store = \"filesystem\""
    );
}

#[test]
fn the_s3_branch_builds_the_s3_store() {
    let Some(output) =
        in_fixed_aws_environment("the_s3_branch_builds_the_s3_store", KEYS, s3_branch)
    else {
        return;
    };
    // The two operations recorded on the filesystem store, once per
    // resolution.
    let warning = "2 non-terminal bulk operation(s) were started on the filesystem store, but \
                   the s3 store is configured";
    assert_eq!(output.matches(warning).count(), 2, "{output}");
}

/// `store = "s3"` with a `[bulk.filesystem]` section beside it, in and out
/// of dev mode, gives the S3 store, which has no directory tree, and the
/// filesystem section's path is never created.
async fn s3_branch(dir: PathBuf) {
    let (catalog, storage) = catalog_with_operations().await;
    let unused = dir.join("filesystem-store");
    let section = bulk(json!({
        "store": "s3",
        "s3": {"endpoint_url": "http://127.0.0.1:9", "force_path_style": true},
        "filesystem": {"path": unused}
    }));
    for dev_mode in [false, true] {
        let store = resolve(Some(&section), SERVER_REGION, dev_mode, &catalog, &storage)
            .await
            .unwrap()
            .expect("the S3 store");
        assert!(store.filesystem_root().is_none(), "dev mode {dev_mode}");
        // The key the environment gives the store, for the startup check
        // against the keys the server accepts.
        assert_eq!(
            store.s3_access_key_id(),
            Some("AKIDRESOLUTIONTEST"),
            "dev mode {dev_mode}"
        );
    }
    assert!(!unused.exists(), "the [bulk.filesystem] path is ignored");
}

#[test]
fn an_insecure_endpoint_the_section_allows_builds_the_s3_store_with_a_warning() {
    let Some(output) = in_fixed_aws_environment(
        "an_insecure_endpoint_the_section_allows_builds_the_s3_store_with_a_warning",
        KEYS,
        insecure_endpoint,
    ) else {
        return;
    };
    let warning = "[bulk.s3] endpoint_url uses plain http to a host that is not loopback; S3 \
                   requests and responses cross the network unencrypted";
    assert_eq!(output.matches(warning).count(), 2, "{output}");
}

/// `allow_insecure_endpoint = true` with a plain `http` endpoint on a host
/// that is not loopback gives the S3 store, in and out of dev mode, and
/// each resolution logs the warning once.
async fn insecure_endpoint(_dir: PathBuf) {
    let (catalog, storage) = components().await;
    let section = bulk(json!({
        "store": "s3",
        "s3": {
            "endpoint_url": "http://objects.example:9",
            "allow_insecure_endpoint": true,
            "force_path_style": true
        }
    }));
    for dev_mode in [false, true] {
        let store = resolve(Some(&section), SERVER_REGION, dev_mode, &catalog, &storage)
            .await
            .unwrap()
            .expect("the S3 store");
        assert!(store.filesystem_root().is_none(), "dev mode {dev_mode}");
    }
}

const ACCOUNT_A: &str = "111111111111";
const ACCOUNT_B: &str = "222222222222";
const ACCOUNT_C: &str = "333333333333";

/// The run context Start records for an operation on the store `store`.
fn on(store: &str) -> Option<Value> {
    Some(json!({"store": store}))
}

/// Creates `table` for `account` and returns its id: the start transaction
/// refuses a table whose row is absent.
async fn create_table(storage: &Arc<dyn StorageEngine>, account: &str, table: &str) -> String {
    let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
        "TableName": table,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("create table input");
    storage
        .create_table(account, input)
        .await
        .expect("create table")
        .table_id
}

/// Start one operation of `account` on `table`, recording `run_context`,
/// born terminal when `terminal`; returns its id.
async fn start(
    storage: &Arc<dyn StorageEngine>,
    account: &str,
    table: &str,
    run_context: Option<Value>,
    terminal: bool,
) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let start_time_ms = 1_789_060_589_767 + i64::try_from(n).unwrap();
    let operation_id = format!("{start_time_ms:014}-{n:08x}");
    let initial_failure = terminal.then(|| InitialFailure {
        code: "S3NoSuchBucket".to_owned(),
        message: "The specified bucket does not exist".to_owned(),
        cap: 1_000,
        window_start_ms: 0,
    });
    let operation = NewBulkOperation {
        operation_id: operation_id.clone(),
        table_id: create_table(storage, account, table).await,
        table_name: table.to_owned(),
        table_arn: format!("arn:aws:dynamodb:{SERVER_REGION}:{account}:table/{table}"),
        client_token: None,
        client_token_hash: None,
        request_fingerprint: "resolution".to_owned(),
        input_config: json!({}),
        output_config: json!({}),
        input_files: Vec::new(),
        max_write_request_units: None,
        performance_tier: None,
        start_time_ms,
        total_input_bytes: None,
        initial_failure,
        run_context,
    };
    let outcome = storage
        .start_bulk_operation(account, operation, 50)
        .await
        .unwrap();
    assert!(matches!(outcome, StartOutcome::Created(_)), "{table}");
    operation_id
}

/// A catalog of three accounts. A holds, each on its own table, operations
/// in progress recorded on S3, on the filesystem store, and with no run
/// context (started before run contexts were recorded, so on the
/// filesystem store); one recorded on S3 and cancelled, so `CANCELLING`; one
/// recorded on S3 and born terminal; and one in progress whose run context
/// does not parse. B holds one in progress on S3 and one terminal on the
/// filesystem store; C holds one terminal on S3.
async fn catalog_with_operations() -> (Arc<dyn CatalogStore>, Arc<dyn StorageEngine>) {
    let (catalog, storage) = components().await;
    for account in [ACCOUNT_A, ACCOUNT_B, ACCOUNT_C] {
        catalog.create_account(account, account).await.unwrap();
    }
    start(&storage, ACCOUNT_A, "t1", on("s3"), false).await;
    start(&storage, ACCOUNT_A, "t2", on("filesystem"), false).await;
    start(&storage, ACCOUNT_A, "t3", None, false).await;
    let cancelled = start(&storage, ACCOUNT_A, "t4", on("s3"), false).await;
    let outcome = storage.request_cancel(ACCOUNT_A, &cancelled).await.unwrap();
    assert!(matches!(outcome, CancelOutcome::Cancelling));
    start(&storage, ACCOUNT_A, "t5", on("s3"), true).await;
    start(&storage, ACCOUNT_A, "t6", Some(json!({"store": 7})), false).await;
    start(&storage, ACCOUNT_B, "t1", on("s3"), false).await;
    start(&storage, ACCOUNT_B, "t2", on("filesystem"), true).await;
    start(&storage, ACCOUNT_C, "t1", on("s3"), true).await;
    (catalog, storage)
}

#[tokio::test]
async fn the_operations_recorded_on_the_other_store_are_counted_by_the_store_they_name() {
    let (catalog, storage) = catalog_with_operations().await;
    for page in [1, 2, 100] {
        let on_s3 = other_store_operations(&catalog, &storage, "filesystem", page).await;
        assert_eq!(on_s3, BTreeMap::from([("s3".to_owned(), 3)]), "page {page}");
        let on_filesystem = other_store_operations(&catalog, &storage, "s3", page).await;
        assert_eq!(
            on_filesystem,
            BTreeMap::from([("filesystem".to_owned(), 2)]),
            "page {page}"
        );
    }
}

#[test]
fn startup_on_the_filesystem_store_warns_of_the_operations_recorded_on_s3() {
    let dir = scratch();
    let section = bulk(json!({"store": "filesystem", "filesystem": {"path": dir.join("buckets")}}));
    let (resolved, log) = logged(async {
        let (catalog, storage) = catalog_with_operations().await;
        resolve(Some(&section), SERVER_REGION, false, &catalog, &storage).await
    });
    let _ = std::fs::remove_dir_all(&dir);
    let store = resolved.unwrap().expect("the filesystem store");
    assert!(store.filesystem_root().is_some());
    let warning = "3 non-terminal bulk operation(s) were started on the s3 store, but the \
                   filesystem store is configured; the bulk operation worker fails them with \
                   InternalServerError without reading their input, except cancellations, which \
                   it finishes";
    assert!(log.contains(warning), "{log}");
    assert_eq!(
        log.matches("bulk operation(s) were started on").count(),
        1,
        "{log}"
    );
}

/// An AWS key imported into the server for one of its users, as an operator
/// imports a client's key.
const IMPORTED: &str = "AKIAIMPORTEDTEST0001";

#[test]
fn startup_warns_when_the_s3_store_signs_with_a_key_the_server_accepts() {
    let Some(output) = in_fixed_aws_environment(
        "startup_warns_when_the_s3_store_signs_with_a_key_the_server_accepts",
        &[
            ("AWS_ACCESS_KEY_ID", IMPORTED),
            ("AWS_SECRET_ACCESS_KEY", "imported-test-secret"),
        ],
        signing_with_an_imported_key,
    ) else {
        return;
    };
    let warning = "The S3 bulk store credentials are an access key this server also accepts \
                   from its clients. S3 refuses a key ExtendDB created, and accepts an imported \
                   AWS key as that key's identity. Start the server without the clients' AWS \
                   variables or profile";
    assert_eq!(output.matches(warning).count(), 1, "{output}");
}

/// The resolution and the startup check of an S3 store whose credentials,
/// from this process's environment, are a key imported into the catalog.
async fn signing_with_an_imported_key(_dir: PathBuf) {
    let config = extenddb_storage_sqlite::SqliteConfig {
        path: ":memory:".to_owned(),
        pool_size: 2,
    };
    let components = (extenddb_storage_sqlite::backend().server_components)(
        &config,
        SERVER_REGION,
        ServerComponentsOptions::default(),
    )
    .await
    .expect("server components");
    let catalog = components.catalog_store;
    catalog.create_account(ACCOUNT_A, ACCOUNT_A).await.unwrap();
    catalog
        .create_user(ACCOUNT_A, "loader", None)
        .await
        .unwrap();
    catalog
        .import_access_key(ACCOUNT_A, "loader", IMPORTED, "imported-test-secret")
        .await
        .unwrap();
    let section = bulk(json!({
        "store": "s3",
        "s3": {"endpoint_url": "http://127.0.0.1:9", "force_path_style": true}
    }));
    let store = resolve_checked(
        Some(&section),
        SERVER_REGION,
        false,
        &catalog,
        &components.engine,
        &components.credential_store,
    )
    .await
    .unwrap()
    .expect("the S3 store");
    assert_eq!(store.s3_access_key_id(), Some(IMPORTED));
}
