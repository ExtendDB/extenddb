// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The member order a parsed request keeps in this crate's build.

use std::sync::Arc;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_engine::{AuthorizedKeyInfo, BackupRuntime, OperationContext};
use extenddb_storage_sqlite::SqliteEngine;
use serde_json::Value;

/// This crate turns on serde_json's `preserve_order` feature, so a body parsed
/// in its build keeps the members in the order the client wrote them, and the
/// typed input of `dispatch` stops at the first member that fails. So a body
/// with two type faults answers the fault of the member written first, as the
/// service does (measured 2026-11-12). The texts are serde_json's. The test
/// parses the body itself and so checks the feature and `dispatch`, not
/// `handle_request`: `tests/test_request_member_order.py` sends the bodies
/// through the handler. In a build of the whole workspace `bson` and the
/// engine's dev-dependency on serde_json turn the feature on too, so there the
/// test checks `dispatch` alone. A build of this crate alone, `cargo test -p
/// extenddb-server`, checks the feature too. The Python file checks it on a
/// server built without them, such as `-p extenddb --no-default-features
/// --features sqlite`.
#[tokio::test]
async fn two_type_faults_answer_the_member_written_first() {
    let storage = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("an in-memory database opens");
    let ctx = OperationContext {
        storage: Arc::new(storage),
        backup: Arc::new(BackupRuntime::new(1)),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from("000000000000"),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info: AuthorizedKeyInfo::new(),
        auth_cache: extenddb_auth::AuthCacheRegistry::default(),
        table_key_info_lookup: None,
        item_authorizer: None,
    };
    let query = r#""TableName":"t","KeyConditionExpression":"pk = :p""#;
    for (operation, body, fault) in [
        (
            "Query",
            format!(r#"{{{query},"Limit":"0","ConsistentRead":"x"}}"#),
            r#"string "0", expected i64"#,
        ),
        (
            "Query",
            format!(r#"{{{query},"ConsistentRead":"x","Limit":"0"}}"#),
            r#"string "x", expected a boolean"#,
        ),
        (
            "TransactWriteItems",
            r#"{"TransactItems":[{"Put":{"TableName":5,"Item":"x"}}]}"#.to_owned(),
            "integer `5`, expected a string",
        ),
        (
            "TransactWriteItems",
            r#"{"TransactItems":[{"Put":{"Item":"x","TableName":5}}]}"#.to_owned(),
            r#"string "x", expected a map"#,
        ),
    ] {
        let input: Value = serde_json::from_slice(body.as_bytes()).expect("the body is JSON");
        let want =
            format!("Start of structure or map found where not expected: invalid type: {fault}");
        match extenddb_engine::dispatch(operation, input, &ctx, "").await {
            Err(DynamoDbError::SerializationException(message)) => {
                assert_eq!(message, want, "{body}");
            }
            Err(other) => panic!("{body}: {other:?}"),
            Ok(_) => panic!("{body}: accepted"),
        }
    }
}
