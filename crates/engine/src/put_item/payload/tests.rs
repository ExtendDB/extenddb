// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use extenddb_core::types::{
    AttributeDefinition, CreateTableInput, DescribeStreamInput, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamEventName,
};
use extenddb_storage::server_components::ServerComponentsOptions;
use extenddb_storage::{BulkRowMarker, RowOutcome, StorageEngine};
use serde_json::json;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

fn limits() -> LimitsConfig {
    LimitsConfig::default()
}

fn key_info() -> TableKeyInfo {
    TableKeyInfo {
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        ..TableKeyInfo::default()
    }
}

fn string_item(entries: &[(&str, &str)]) -> Item {
    let mut item = Item::new();
    for (name, value) in entries {
        item.insert((*name).to_owned(), AttributeValue::S((*value).to_owned()));
    }
    item
}

/// An attribute value nested `depth` levels deep in lists.
fn nested_value(depth: usize) -> AttributeValue {
    let mut value = AttributeValue::S("leaf".to_owned());
    for _ in 0..depth {
        value = AttributeValue::L(vec![value]);
    }
    value
}

// resolve_put_condition: the resolve, unused-attribute, nesting order.

#[test]
fn a_valid_condition_resolves_to_an_expression() {
    let mut values = HashMap::new();
    values.insert(":v".to_owned(), AttributeValue::S("a".to_owned()));
    let (condition, _maps) =
        resolve_put_condition(Some("pk = :v"), None, Some(&values), None, None, &limits())
            .expect("the condition resolves");
    assert!(condition.is_some());
}

#[test]
fn an_unused_expression_value_is_rejected() {
    let mut values = HashMap::new();
    values.insert(":v".to_owned(), AttributeValue::S("a".to_owned()));
    let error = resolve_put_condition(
        Some("attribute_not_exists(pk)"),
        None,
        Some(&values),
        None,
        None,
        &limits(),
    )
    .expect_err("the unused value is rejected");
    assert!(
        error.to_string().contains("unused"),
        "unexpected message: {error}"
    );
}

#[test]
fn an_expression_value_past_the_nesting_limit_is_rejected() {
    let mut values = HashMap::new();
    values.insert(":v".to_owned(), nested_value(32));
    let error = resolve_put_condition(Some("v = :v"), None, Some(&values), None, None, &limits())
        .expect_err("the over-deep value is rejected");
    assert!(
        error.to_string().contains("nested"),
        "unexpected message: {error}"
    );
}

#[test]
fn an_unused_value_is_reported_before_its_nesting_depth() {
    // The unused-attribute check runs before the nesting-depth check,
    // so a value that is both unused and over-deep reports the unused
    // message.
    let mut values = HashMap::new();
    values.insert(":v".to_owned(), nested_value(32));
    let error = resolve_put_condition(
        Some("attribute_not_exists(pk)"),
        None,
        Some(&values),
        None,
        None,
        &limits(),
    )
    .expect_err("the row is rejected");
    assert!(
        error.to_string().contains("unused"),
        "unexpected message: {error}"
    );
}

// validate_put_payload: the fired kinds and their messages.

#[test]
fn a_missing_key_fires_the_missing_key_kind() {
    let fault = validate_put_payload(&string_item(&[("pk", "a")]), &key_info(), &limits())
        .expect_err("the item is rejected");
    assert_eq!(fault.kind, PutPayloadFault::MissingKey);
    assert_eq!(
        fault.error.to_string(),
        "One or more parameter values were invalid: Missing the key sk in the item"
    );
}

#[test]
fn a_key_type_mismatch_fires_the_mismatch_kind() {
    let mut item = string_item(&[("sk", "b")]);
    item.insert("pk".to_owned(), AttributeValue::N("1".to_owned()));
    let fault =
        validate_put_payload(&item, &key_info(), &limits()).expect_err("the item is rejected");
    assert_eq!(fault.kind, PutPayloadFault::KeyTypeMismatch);
    assert_eq!(
        fault.error.to_string(),
        "One or more parameter values were invalid: Type mismatch for key pk expected: S \
         actual: N"
    );
}

#[test]
fn a_type_mismatch_fires_before_a_later_missing_key() {
    // The schema walk reaches the mismatched first key before the
    // absent second key, so the fired kind is the mismatch; the bulk
    // error report attaches no image for it.
    let mut item = Item::new();
    item.insert("pk".to_owned(), AttributeValue::N("1".to_owned()));
    let fault =
        validate_put_payload(&item, &key_info(), &limits()).expect_err("the item is rejected");
    assert_eq!(fault.kind, PutPayloadFault::KeyTypeMismatch);
}

#[test]
fn an_oversized_item_fires_the_oversized_kind() {
    let tight = LimitsConfig {
        max_item_size_bytes: 8,
        ..LimitsConfig::default()
    };
    let fault = validate_put_payload(
        &string_item(&[("pk", "a"), ("sk", "b"), ("v", "cccccccccc")]),
        &key_info(),
        &tight,
    )
    .expect_err("the item is rejected");
    assert_eq!(fault.kind, PutPayloadFault::Oversized);
    assert_eq!(
        fault.error.to_string(),
        "Item size has exceeded the maximum allowed size"
    );
}

#[test]
fn an_overdeep_item_fires_the_nesting_depth_kind() {
    let mut item = string_item(&[("pk", "a"), ("sk", "b")]);
    item.insert("deep".to_owned(), nested_value(33));
    let fault =
        validate_put_payload(&item, &key_info(), &limits()).expect_err("the item is rejected");
    assert_eq!(fault.kind, PutPayloadFault::NestingDepth);
}

#[test]
fn a_non_key_fault_fires_the_other_kind() {
    let mut item = string_item(&[("pk", "a"), ("sk", "b")]);
    item.insert("n".to_owned(), AttributeValue::N("not-a-number".to_owned()));
    let fault =
        validate_put_payload(&item, &key_info(), &limits()).expect_err("the item is rejected");
    assert_eq!(fault.kind, PutPayloadFault::Other);
}

#[test]
fn a_valid_item_passes() {
    assert!(
        validate_put_payload(
            &string_item(&[("pk", "a"), ("sk", "b")]),
            &key_info(),
            &limits()
        )
        .is_ok()
    );
}

// apply_put: stream capture and marker passthrough, against the
// in-memory SQLite engine.

async fn engine() -> Arc<dyn StorageEngine> {
    let config = extenddb_storage_sqlite::SqliteConfig {
        path: ":memory:".to_owned(),
        pool_size: 2,
    };
    let components = (extenddb_storage_sqlite::backend().server_components)(
        &config,
        REGION,
        ServerComponentsOptions::default(),
    )
    .await
    .expect("server components");
    components
        .catalog_store
        .set_setting("control_plane_delay_seconds", "0")
        .await
        .expect("set control plane delay");
    components
        .catalog_store
        .create_account(ACCOUNT, "put-item-tests")
        .await
        .expect("create account");
    components.engine
}

async fn create_table(storage: &Arc<dyn StorageEngine>, name: &str, stream: bool) {
    let mut body = json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST"
    });
    if stream {
        body["StreamSpecification"] = json!({
            "StreamEnabled": true,
            "StreamViewType": "NEW_AND_OLD_IMAGES"
        });
    }
    let input: CreateTableInput = serde_json::from_value(body).expect("create table input");
    storage
        .create_table(ACCOUNT, input)
        .await
        .expect("create table");
}

#[tokio::test]
async fn apply_put_captures_a_stream_record_the_way_handle_put_item_does() {
    let storage = engine().await;
    create_table(&storage, "streamed", true).await;
    let key_info = storage
        .table_key_info(ACCOUNT, "streamed")
        .await
        .expect("key info");
    let region: Arc<str> = Arc::from(REGION);

    apply_put(
        storage.as_ref(),
        &region,
        &key_info,
        string_item(&[("pk", "a"), ("sk", "b")]),
        None,
        &ExpressionMaps::default(),
        false,
        None,
    )
    .await
    .expect("the write applies");

    let (summaries, _) = storage
        .list_streams(ACCOUNT, Some("streamed"), 10, None)
        .await
        .expect("list streams");
    let stream_arn = summaries.first().expect("one stream").stream_arn.clone();
    let description = storage
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn,
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("describe stream");
    // The record lands on the shard the partition key hashes to, so
    // collect across every shard.
    let mut records = Vec::new();
    for shard in &description.shards {
        let (shard_records, _) = storage
            .get_stream_records(ACCOUNT, &description.stream_arn, &shard.shard_id, None, 100)
            .await
            .expect("get records");
        records.extend(shard_records);
    }

    assert_eq!(records.len(), 1, "one record for one write");
    let record = &records[0];
    assert_eq!(record.event_name, StreamEventName::Insert);
    assert_eq!(record.aws_region, REGION);
    assert!(record.user_identity.is_none(), "no user identity");
    assert!(record.dynamodb.new_image.is_some(), "NewImage populated");
    assert!(
        record.dynamodb.old_image.is_none(),
        "fresh key, no old image"
    );
}

#[tokio::test]
async fn apply_put_records_the_marker_in_the_row_outcome_ledger() {
    let storage = engine().await;
    create_table(&storage, "marked", false).await;
    let key_info = storage
        .table_key_info(ACCOUNT, "marked")
        .await
        .expect("key info");
    let region: Arc<str> = Arc::from(REGION);
    let (condition, maps) = resolve_put_condition(
        Some("attribute_not_exists(pk)"),
        None,
        None,
        None,
        None,
        &limits(),
    )
    .expect("the condition resolves");
    let marker = BulkRowMarker {
        account_id: ACCOUNT.to_owned(),
        operation_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        file_index: 0,
        row_index: 3,
        owner: String::new(),
        keep_old: false,
        settled: (0, 0),
    };

    apply_put(
        storage.as_ref(),
        &region,
        &key_info,
        string_item(&[("pk", "a"), ("sk", "b")]),
        condition.as_ref(),
        &maps,
        false,
        Some(&marker),
    )
    .await
    .expect("the write applies");

    let outcome = storage
        .bulk_row_outcome(ACCOUNT, &marker.operation_id, 0, 3)
        .await
        .expect("ledger read");
    assert_eq!(outcome, Some(RowOutcome::Applied));
}

#[tokio::test]
async fn apply_put_returns_the_old_item_only_when_asked() {
    let storage = engine().await;
    create_table(&storage, "plain", false).await;
    let key_info = storage
        .table_key_info(ACCOUNT, "plain")
        .await
        .expect("key info");
    let region: Arc<str> = Arc::from(REGION);
    let maps = ExpressionMaps::default();

    let first = apply_put(
        storage.as_ref(),
        &region,
        &key_info,
        string_item(&[("pk", "a"), ("sk", "b"), ("v", "1")]),
        None,
        &maps,
        false,
        None,
    )
    .await
    .expect("the first write applies");
    assert!(first.is_none(), "no stream, not asked: no old item fetched");

    let second = apply_put(
        storage.as_ref(),
        &region,
        &key_info,
        string_item(&[("pk", "a"), ("sk", "b"), ("v", "2")]),
        None,
        &maps,
        true,
        None,
    )
    .await
    .expect("the second write applies");
    let old = second.expect("the replaced item is returned when asked");
    assert_eq!(old.get("v"), Some(&AttributeValue::S("1".to_owned())));
}
