// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ReadOptions::consistent_read` reaches every read path of the Cassandra
//! backend: GetItem, Query and Scan return the item with either value, and a
//! keyspace raised to replication factor 3 on the one-node test cluster shows
//! that `true` reads at `LOCAL_QUORUM` and `false` at `LOCAL_ONE`.

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
    TableKeyInfo,
};
use extenddb_storage::{DataEngine, ReadOptions, TableEngine};
use std::collections::BTreeMap;

use crate::helpers::{TestTable, ensure_test_account, setup_engine, unique_test_account};

#[tokio::test]
async fn test_get_query_and_scan_return_the_item_with_both_consistent_read_values() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;

    // Both table shapes: GetItem binds a sort key or reads by partition only.
    for has_sort_key in [true, false] {
        let table = TestTable::new(&engine, "ConsistentReadTable", has_sort_key).await;

        let mut item = BTreeMap::new();
        item.insert("id".to_owned(), AttributeValue::S("p1".to_owned()));
        if has_sort_key {
            item.insert("sort".to_owned(), AttributeValue::S("s1".to_owned()));
        }
        item.insert("data".to_owned(), AttributeValue::S("v1".to_owned()));
        engine
            .put_item(
                &table.key_info,
                item.clone(),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put_item should succeed");

        let key = extenddb_core::types::extract_key(&item, &table.key_info.key_schema);
        let key_condition = KeyCondition {
            pk_path: vec![PathElement::Attribute("id".to_owned())],
            pk_value: Expr::Placeholder(":pk".to_owned()),
            sk_condition: None,
            extra_pk_conditions: vec![],
            extra_sk_conditions: vec![],
        };
        let mut maps = ExpressionMaps::default();
        maps.values
            .insert(":pk".to_owned(), AttributeValue::S("p1".to_owned()));

        for consistent_read in [true, false] {
            let opts = ReadOptions::new(consistent_read);
            let case = format!("sort key {has_sort_key}, consistent_read {consistent_read}");

            let got = engine
                .get_item(&table.key_info, &key, opts)
                .await
                .expect("get_item should succeed");
            assert_eq!(got.as_ref(), Some(&item), "get_item, {case}");

            let (items, last_key) = engine
                .query(
                    &table.key_info,
                    &key_condition,
                    &maps,
                    true,
                    None,
                    None,
                    None,
                    opts,
                )
                .await
                .expect("query should succeed");
            assert_eq!(items, vec![item.clone()], "query, {case}");
            assert!(last_key.is_none(), "query, {case}");

            let (items, last_key) = engine
                .scan(&table.key_info, None, None, None, None, None, opts)
                .await
                .expect("scan should succeed");
            assert_eq!(items, vec![item.clone()], "scan, {case}");
            assert!(last_key.is_none(), "scan, {case}");
        }
    }
}

/// A keyspace with replication factor 3 on a one-node cluster has one live
/// replica, so `LOCAL_ONE` can be met and `LOCAL_QUORUM` (two replicas)
/// cannot. Raising the account keyspace to 3 after the write shows the level
/// each read runs at: with `ConsistentRead: false` GetItem, Query and Scan
/// return the item, and with `true` each one is refused.
#[tokio::test]
async fn test_consistent_read_true_reads_at_local_quorum_and_false_at_local_one() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QuorumReadTable", true).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_owned(), AttributeValue::S("p1".to_owned()));
    item.insert("sort".to_owned(), AttributeValue::S("s1".to_owned()));
    engine
        .put_item(
            &table.key_info,
            item.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("put_item should succeed");

    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    engine
        .session_arc()
        .query(format!(
            "ALTER KEYSPACE {keyspace} WITH replication = \
             {{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}}"
        ))
        .await
        .expect("raise the replication factor");

    let key = extenddb_core::types::extract_key(&item, &table.key_info.key_schema);
    let key_condition = KeyCondition {
        pk_path: vec![PathElement::Attribute("id".to_owned())],
        pk_value: Expr::Placeholder(":pk".to_owned()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };
    let mut maps = ExpressionMaps::default();
    maps.values
        .insert(":pk".to_owned(), AttributeValue::S("p1".to_owned()));

    let eventual = ReadOptions::new(false);
    let got = engine.get_item(&table.key_info, &key, eventual).await;
    assert_eq!(got.expect("get_item at LOCAL_ONE"), Some(item.clone()));
    let queried = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            None,
            eventual,
        )
        .await;
    assert_eq!(queried.expect("query at LOCAL_ONE").0, vec![item.clone()]);
    let scanned = engine
        .scan(&table.key_info, None, None, None, None, None, eventual)
        .await;
    assert_eq!(scanned.expect("scan at LOCAL_ONE").0, vec![item.clone()]);

    let consistent = ReadOptions::new(true);
    let refusals = [
        engine
            .get_item(&table.key_info, &key, consistent)
            .await
            .map(|_| ()),
        engine
            .query(
                &table.key_info,
                &key_condition,
                &maps,
                true,
                None,
                None,
                None,
                consistent,
            )
            .await
            .map(|_| ()),
        engine
            .scan(&table.key_info, None, None, None, None, None, consistent)
            .await
            .map(|_| ()),
    ];
    for (read, result) in ["get_item", "query", "scan"].iter().zip(refusals) {
        // Cassandra answers the read as unavailable; the driver retries it on
        // the next node, finds none, and returns that as the error.
        assert!(result.is_err(), "{read} with ConsistentRead true succeeded");
    }

    // The node still serves the same reads at `LOCAL_ONE`.
    let got = engine.get_item(&table.key_info, &key, eventual).await;
    assert_eq!(got.expect("get_item at LOCAL_ONE"), Some(item));

    // Leave the node as the next test expects it: the keyspace is only
    // queued for a drop, so lower it back to replication factor 1.
    crate::helpers::ensure_keyspace_rf(&engine, &keyspace)
        .await
        .expect("restore the replication factor");
}

/// A base table whose key has two HASH attributes stores each row under the
/// netstring composite of both parts. GetItem derives the partition key the
/// way the write path does, so it reads the row back with either
/// `ConsistentRead` value; a derivation from the first attribute alone would
/// read `None`. The TTL worker's quorum read shares this derivation.
#[tokio::test]
async fn test_get_item_reads_a_two_part_hash_key_row() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = unique_test_account();
    ensure_test_account(&engine, &account_id)
        .await
        .expect("test account");
    let key_schema = vec![
        KeySchemaElement {
            attribute_name: "tenant".to_owned(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "id".to_owned(),
            key_type: KeyType::Hash,
        },
    ];
    let attribute_definitions = vec![
        AttributeDefinition {
            attribute_name: "tenant".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "id".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
    ];
    let input = extenddb_core::types::CreateTableInput {
        table_name: "TwoPartHashTable".to_owned(),
        key_schema: key_schema.clone(),
        attribute_definitions: attribute_definitions.clone(),
        local_secondary_indexes: None,
        global_secondary_indexes: None,
        vector_indexes: None,
        billing_mode: None,
        table_throughput_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        stream_specification: None,
        sse_specification: None,
        tags: None,
        table_class: None,
        deletion_protection_enabled: None,
    };
    let desc = engine
        .create_table(&account_id, input)
        .await
        .expect("create_table");
    let key_info = TableKeyInfo {
        table_name: "TwoPartHashTable".to_owned(),
        account_id: account_id.clone(),
        table_id: desc.table_id,
        key_schema: key_schema.clone(),
        base_key_schema: key_schema.clone(),
        attribute_definitions,
        has_lsi: false,
        global_secondary_indexes: Vec::new(),
        local_secondary_indexes: Vec::new(),
        vector_indexes: Vec::new(),
        stream_specification: None,
    };

    let mut item = BTreeMap::new();
    item.insert("tenant".to_owned(), AttributeValue::S("t1".to_owned()));
    item.insert("id".to_owned(), AttributeValue::S("p1".to_owned()));
    item.insert("data".to_owned(), AttributeValue::S("v1".to_owned()));
    engine
        .put_item(
            &key_info,
            item.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("put_item should succeed");

    let key = extenddb_core::types::extract_key(&item, &key_schema);
    for consistent_read in [true, false] {
        let got = engine
            .get_item(&key_info, &key, ReadOptions::new(consistent_read))
            .await
            .expect("get_item should succeed");
        assert_eq!(
            got.as_ref(),
            Some(&item),
            "two-part hash key, consistent_read {consistent_read}"
        );
    }

    engine
        .delete_table(
            &account_id,
            extenddb_core::types::DeleteTableInput {
                table_name: "TwoPartHashTable".to_owned(),
            },
        )
        .await
        .expect("delete_table");
}
