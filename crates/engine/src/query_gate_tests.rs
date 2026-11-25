// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the split of the key condition type check in [`handle_query`]:
//! the classifier checks the values of legacy `KeyConditions` and of a
//! multi-attribute key schema, and `validate_key_condition_types` checks the
//! others after the Select check. The multi-attribute row fails when the
//! classifier stops checking a multi-attribute key schema. The legacy row pins
//! only that legacy values are type-checked: with the legacy term of the split
//! dropped, `validate_key_condition_types` answers the same text. The expected
//! answers were measured on the service in us-east-1 on 2026-11-11, the
//! multi-attribute ones on a GSI.

use std::sync::Arc;

use serde_json::{Value, json};

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage_sqlite::SqliteEngine;

use super::handle_query;
use crate::query_key_values::KEY_CONDITION_TYPE_MISMATCH;
use crate::{AuthorizedKeyInfo, BackupRuntime, OperationContext};

/// A context whose authorization snapshot holds table `kvt` with `key_schema`
/// and `definitions`, so the request reads no catalog. Every request here is
/// refused before a row is read, so the store stays empty. The key schema is
/// the one the query reads, so a multi-attribute one stands for a GSI's.
async fn context(
    key_schema: &[(&str, KeyType)],
    definitions: &[(&str, ScalarAttributeType)],
) -> OperationContext {
    let storage = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("an in-memory database opens");
    let key_schema: Vec<KeySchemaElement> = key_schema
        .iter()
        .map(|(name, key_type)| KeySchemaElement {
            attribute_name: (*name).to_owned(),
            key_type: *key_type,
        })
        .collect();
    let info = TableKeyInfo {
        table_name: "kvt".to_owned(),
        account_id: "000000000000".to_owned(),
        table_id: "kvt-id".to_owned(),
        key_schema: key_schema.clone(),
        base_key_schema: key_schema,
        attribute_definitions: definitions
            .iter()
            .map(|(name, attribute_type)| AttributeDefinition {
                attribute_name: (*name).to_owned(),
                attribute_type: *attribute_type,
            })
            .collect(),
        has_lsi: false,
        global_secondary_indexes: Vec::new(),
        local_secondary_indexes: Vec::new(),
        stream_specification: None,
        vector_indexes: Vec::new(),
    };
    let mut authorized_key_info = AuthorizedKeyInfo::new();
    authorized_key_info.insert("kvt".to_owned(), Some(info));
    OperationContext {
        storage: Arc::new(storage),
        backup: Arc::new(BackupRuntime::new(1)),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from("000000000000"),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info,
        auth_cache: extenddb_auth::AuthCacheRegistry::default(),
        table_key_info_lookup: None,
        item_authorizer: None,
    }
}

/// The `ValidationException` text `body` answers.
async fn refusal(ctx: &OperationContext, body: Value) -> String {
    match handle_query(body, ctx).await {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        Err(other) => panic!("unexpected error {other:?}"),
        Ok(_) => panic!("the request was accepted"),
    }
}

#[tokio::test]
async fn legacy_key_conditions_value_of_another_type_is_refused() {
    // `KeyConditions` with `{"N": "5"}` for the S partition key.
    let ctx = context(
        &[("pk", KeyType::Hash), ("sk", KeyType::Range)],
        &[
            ("pk", ScalarAttributeType::S),
            ("sk", ScalarAttributeType::S),
        ],
    )
    .await;
    let body = json!({
        "TableName": "kvt",
        "KeyConditions": {
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "5"}]},
        },
    });
    assert_eq!(refusal(&ctx, body).await, KEY_CONDITION_TYPE_MISMATCH);
}

#[tokio::test]
async fn multi_attribute_value_of_another_type_is_refused_before_the_non_key_rule() {
    // HASH `h1` (S) and `h2` (N), RANGE `r` (S): an S value for `h2` beside a
    // clause on the non-key attribute `z` answers the type, and with an N
    // value the clause on `z` answers the missing RANGE attribute.
    let ctx = context(
        &[
            ("h1", KeyType::Hash),
            ("h2", KeyType::Hash),
            ("r", KeyType::Range),
        ],
        &[
            ("h1", ScalarAttributeType::S),
            ("h2", ScalarAttributeType::N),
            ("r", ScalarAttributeType::S),
        ],
    )
    .await;
    let query = |h2: Value| {
        json!({
            "TableName": "kvt",
            "KeyConditionExpression": "h1 = :a AND h2 = :b AND z = :c",
            "ExpressionAttributeValues": {":a": {"S": "x"}, ":b": h2, ":c": {"S": "x"}},
        })
    };
    assert_eq!(
        refusal(&ctx, query(json!({"S": "1"}))).await,
        KEY_CONDITION_TYPE_MISMATCH
    );
    assert_eq!(
        refusal(&ctx, query(json!({"N": "1"}))).await,
        "Query condition missed key schema element: r"
    );
}
