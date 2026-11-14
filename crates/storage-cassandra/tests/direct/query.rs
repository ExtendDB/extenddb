// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for Query operation.

use extenddb_core::expression::{CompareOp, Expr, ExpressionMaps, KeyCondition};
use extenddb_core::types::AttributeValue;
use extenddb_storage::DataEngine;
use std::collections::BTreeMap;

use crate::helpers::{TestTable, setup_engine};

#[tokio::test]
async fn test_query_pk_only() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryTestTable", false).await;

    // Put three items with the same partition key (PK-only table, so overwrites)
    let pk_value = AttributeValue::S("user123".to_string());

    for i in 1..=3 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert("name".to_string(), AttributeValue::S(format!("Item {}", i)));
        item.insert("value".to_string(), AttributeValue::N(i.to_string()));

        let maps = ExpressionMaps::default();
        engine
            .put_item(&table.key_info, item, false, None, &maps, None)
            .await
            .expect("put_item should succeed");
    }

    // Query by partition key only
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());

    let (items, last_key) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            None,
        )
        .await
        .expect("query should succeed");

    // For PK-only table, we should get back just one item (the last write wins)
    assert_eq!(
        items.len(),
        1,
        "PK-only table should return single item per partition key"
    );

    // Verify the item contains our partition key and last written data
    assert_eq!(items[0].get("id"), Some(&pk_value));
    assert_eq!(
        items[0].get("value"),
        Some(&AttributeValue::N("3".to_string()))
    );

    // No pagination for single item
    assert!(last_key.is_none(), "Should not have pagination key");

    println!("✓ PK-only query test passed");
}

#[tokio::test]
async fn test_query_with_sk_equals() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QuerySkTable", true).await;

    // Put multiple items with same PK but different SKs
    let pk_value = AttributeValue::S("user123".to_string());

    for i in 1..=5 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert(
            "sort".to_string(),
            AttributeValue::S(format!("item-{:02}", i)),
        );
        item.insert("data".to_string(), AttributeValue::N(i.to_string()));

        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Query for specific PK + SK combination
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: Some(extenddb_core::expression::SortKeyCondition::Compare {
            path: vec![extenddb_core::expression::PathElement::Attribute(
                "sort".to_string(),
            )],
            op: extenddb_core::expression::CompareOp::Eq,
            value: Expr::Placeholder(":sk".to_string()),
        }),
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());
    maps.values
        .insert(":sk".to_string(), AttributeValue::S("item-03".to_string()));

    let (items, last_key) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            None,
        )
        .await
        .expect("query should succeed");

    // Should get exactly one item
    assert_eq!(items.len(), 1, "Should return exactly one item");
    assert_eq!(items[0].get("id"), Some(&pk_value));
    assert_eq!(
        items[0].get("sort"),
        Some(&AttributeValue::S("item-03".to_string()))
    );
    assert_eq!(
        items[0].get("data"),
        Some(&AttributeValue::N("3".to_string()))
    );
    assert!(last_key.is_none(), "Should not have pagination key");

    println!("✓ SK equality query test passed");
}

#[tokio::test]
async fn test_query_with_sk_comparison() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QuerySkCompareTable", true).await;

    // Put items with numeric sort keys
    let pk_value = AttributeValue::S("partition1".to_string());

    for i in 1..=10 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert("sort".to_string(), AttributeValue::S(i.to_string()));
        item.insert("value".to_string(), AttributeValue::N((i * 10).to_string()));

        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Test GT (greater than) - should get items > 5
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: Some(extenddb_core::expression::SortKeyCondition::Compare {
            path: vec![extenddb_core::expression::PathElement::Attribute(
                "sort".to_string(),
            )],
            op: extenddb_core::expression::CompareOp::Gt,
            value: Expr::Placeholder(":sk".to_string()),
        }),
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());
    maps.values
        .insert(":sk".to_string(), AttributeValue::S("5".to_string()));

    let (items, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            None,
        )
        .await
        .expect("query should succeed");

    // String comparison: "6", "7", "8", "9" are > "5", but "10" is not (lexicographic order)
    // So we should get 4 items
    println!("Got {} items", items.len());
    for item in &items {
        if let Some(AttributeValue::S(s)) = item.get("sort") {
            println!("  sort: {}", s);
        }
    }

    assert_eq!(
        items.len(),
        4,
        "Should return 4 items with sort > '5' (string comparison)"
    );

    // Verify all returned items have sort > "5"
    for item in &items {
        let sort_val = item.get("sort").expect("item should have sort key");
        if let AttributeValue::S(s) = sort_val {
            assert!(s.as_str() > "5", "Item sort key '{}' should be > '5'", s);
        }
    }

    println!("✓ SK comparison query test passed");
}
#[tokio::test]
async fn test_query_with_numeric_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use crate::helpers::TestTable;

    let engine = setup_engine().await;

    // Create table with numeric sort key
    let table = TestTable::with_sort_key_type(
        &engine,
        "QueryNumericSkTable",
        extenddb_core::types::ScalarAttributeType::N,
    )
    .await;

    // Put items with numeric sort keys 1-10
    let pk_value = AttributeValue::S("sensor1".to_string());

    for i in 1..=10 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert("sort".to_string(), AttributeValue::N(i.to_string()));
        item.insert(
            "reading".to_string(),
            AttributeValue::N((i * 100).to_string()),
        );

        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Query for sort > 5 (numeric comparison)
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: Some(extenddb_core::expression::SortKeyCondition::Compare {
            path: vec![extenddb_core::expression::PathElement::Attribute(
                "sort".to_string(),
            )],
            op: extenddb_core::expression::CompareOp::Gt,
            value: Expr::Placeholder(":sk".to_string()),
        }),
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());
    maps.values
        .insert(":sk".to_string(), AttributeValue::N("5".to_string()));

    let (items, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            None,
        )
        .await
        .expect("query should succeed");

    // With numeric comparison: 6, 7, 8, 9, 10 are all > 5, so we should get 5 items
    println!("Got {} items with numeric sort keys", items.len());
    for item in &items {
        if let Some(AttributeValue::N(n)) = item.get("sort") {
            println!("  sort: {}", n);
        }
    }

    assert_eq!(
        items.len(),
        5,
        "Should return 5 items with sort > 5 (numeric comparison)"
    );

    // Verify all returned items have sort > 5
    for item in &items {
        let sk_val = item.get("sort").expect("item should have sort key");
        if let AttributeValue::N(n) = sk_val {
            let num: i32 = n.parse().expect("should be valid number");
            assert!(num > 5, "Item sort {} should be > 5", num);
        }
    }

    println!("✓ Numeric SK comparison query test passed");
}

#[tokio::test]
async fn test_query_with_decimal_sk_range_and_order() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // Verifies that fractional decimal sort keys compare and order correctly at
    // the column level (Technical Debt #1). The previous varint column + string
    // binding could not represent fractions and broke `sk_n` comparisons; this
    // exercises a real numeric range predicate over decimals plus ascending
    // clustering order.
    let engine = setup_engine().await;
    let table = TestTable::with_sort_key_type(
        &engine,
        "QueryDecimalSkTable",
        extenddb_core::types::ScalarAttributeType::N,
    )
    .await;

    let pk_value = AttributeValue::S("sensor-d".to_string());

    // Insert out of order to prove ordering comes from the decimal column, not
    // insertion order. Mix of fractions and integers.
    let sort_values = ["10.5", "0.25", "2.5", "2.05", "100", "2.500001"];
    for sk in sort_values {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert("sort".to_string(), AttributeValue::N(sk.to_string()));
        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Query: sort > 2.5  → expect 2.500001, 10.5, 100 (NOT 2.5, 2.05, 0.25).
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: Some(extenddb_core::expression::SortKeyCondition::Compare {
            path: vec![extenddb_core::expression::PathElement::Attribute(
                "sort".to_string(),
            )],
            op: extenddb_core::expression::CompareOp::Gt,
            value: Expr::Placeholder(":sk".to_string()),
        }),
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());
    maps.values
        .insert(":sk".to_string(), AttributeValue::N("2.5".to_string()));

    let (items, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true, // ascending
            None,
            None,
            None,
        )
        .await
        .expect("decimal range query should succeed");

    let got: Vec<String> = items
        .iter()
        .map(|item| match item.get("sort") {
            Some(AttributeValue::N(n)) => n.clone(),
            other => panic!("expected numeric sort, got {other:?}"),
        })
        .collect();

    // Strictly greater than 2.5, returned in ascending decimal order.
    assert_eq!(
        got,
        vec![
            "2.500001".to_string(),
            "10.5".to_string(),
            "100".to_string()
        ],
        "decimal range filter + ordering incorrect"
    );

    println!("✓ Decimal SK range + ordering query test passed");
}

#[tokio::test]
async fn test_query_ordering() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryOrderTable", true).await;

    // Put items out of order
    let pk_value = AttributeValue::S("partition1".to_string());
    let sort_values = vec!["apple", "zebra", "banana", "mango", "cherry"];

    for sort in &sort_values {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert("sort".to_string(), AttributeValue::S(sort.to_string()));
        item.insert(
            "data".to_string(),
            AttributeValue::S(format!("Item {}", sort)),
        );

        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Query with forward=true (ascending order)
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());

    let (items_asc, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            None,
        )
        .await
        .expect("forward query should succeed");

    // Verify ascending order
    let sort_keys_asc: Vec<String> = items_asc
        .iter()
        .map(|item| {
            if let Some(AttributeValue::S(s)) = item.get("sort") {
                s.clone()
            } else {
                panic!("Expected string sort key")
            }
        })
        .collect();

    println!("Ascending order: {:?}", sort_keys_asc);
    assert_eq!(
        sort_keys_asc,
        vec!["apple", "banana", "cherry", "mango", "zebra"]
    );

    // Query with forward=false (descending order)
    let (items_desc, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            false,
            None,
            None,
            None,
        )
        .await
        .expect("reverse query should succeed");

    // Verify descending order
    let sort_keys_desc: Vec<String> = items_desc
        .iter()
        .map(|item| {
            if let Some(AttributeValue::S(s)) = item.get("sort") {
                s.clone()
            } else {
                panic!("Expected string sort key")
            }
        })
        .collect();

    println!("Descending order: {:?}", sort_keys_desc);
    assert_eq!(
        sort_keys_desc,
        vec!["zebra", "mango", "cherry", "banana", "apple"]
    );

    println!("✓ Query ordering test passed");
}

#[tokio::test]
async fn test_query_limit() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryLimitTable", true).await;

    // Put 10 items
    let pk_value = AttributeValue::S("partition1".to_string());

    for i in 1..=10 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert(
            "sort".to_string(),
            AttributeValue::S(format!("item-{:02}", i)),
        );
        item.insert("data".to_string(), AttributeValue::N(i.to_string()));

        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Query with limit=3
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());

    let (items, last_key) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            None,
            None,
        )
        .await
        .expect("query should succeed");

    println!("Got {} items with limit=3", items.len());
    assert_eq!(items.len(), 3, "Should return exactly 3 items");

    // Should have last_key since there are more items
    assert!(last_key.is_some(), "Should have LastEvaluatedKey");

    // Verify we got the first 3 items
    assert_eq!(
        items[0].get("sort"),
        Some(&AttributeValue::S("item-01".to_string()))
    );
    assert_eq!(
        items[1].get("sort"),
        Some(&AttributeValue::S("item-02".to_string()))
    );
    assert_eq!(
        items[2].get("sort"),
        Some(&AttributeValue::S("item-03".to_string()))
    );

    println!("✓ Query limit test passed");
}

#[tokio::test]
async fn test_query_pagination() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryPaginationTable", true).await;

    // Put 10 items
    let pk_value = AttributeValue::S("partition1".to_string());

    for i in 1..=10 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert(
            "sort".to_string(),
            AttributeValue::S(format!("item-{:02}", i)),
        );
        item.insert("data".to_string(), AttributeValue::N(i.to_string()));

        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());

    // First page: get 3 items
    let (page1, last_key1) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            None,
            None,
        )
        .await
        .expect("first query should succeed");

    assert_eq!(page1.len(), 3);
    assert!(
        last_key1.is_some(),
        "Should have LastEvaluatedKey after page 1"
    );
    println!("Page 1: {} items", page1.len());

    // Second page: use last_key as exclusive start
    let (page2, last_key2) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            last_key1.as_ref(),
            None,
        )
        .await
        .expect("second query should succeed");

    assert_eq!(page2.len(), 3);
    assert!(
        last_key2.is_some(),
        "Should have LastEvaluatedKey after page 2"
    );
    println!("Page 2: {} items", page2.len());

    // Third page
    let (page3, last_key3) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            last_key2.as_ref(),
            None,
        )
        .await
        .expect("third query should succeed");

    assert_eq!(page3.len(), 3);
    assert!(
        last_key3.is_some(),
        "Should have LastEvaluatedKey after page 3"
    );
    println!("Page 3: {} items", page3.len());

    // Fourth page: should get remaining item
    let (page4, last_key4) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            last_key3.as_ref(),
            None,
        )
        .await
        .expect("fourth query should succeed");

    assert_eq!(page4.len(), 1);
    assert!(
        last_key4.is_none(),
        "Should NOT have LastEvaluatedKey after last page"
    );
    println!("Page 4: {} items (final)", page4.len());

    // Verify no duplicates and correct ordering
    let all_items: Vec<String> = [page1, page2, page3, page4]
        .concat()
        .iter()
        .map(|item| {
            if let Some(AttributeValue::S(s)) = item.get("sort") {
                s.clone()
            } else {
                panic!("Expected string sort key")
            }
        })
        .collect();

    assert_eq!(all_items.len(), 10, "Should have retrieved all 10 items");
    assert_eq!(
        all_items,
        vec![
            "item-01", "item-02", "item-03", "item-04", "item-05", "item-06", "item-07", "item-08",
            "item-09", "item-10"
        ]
    );

    println!("✓ Query pagination test passed");
}

#[tokio::test]
async fn test_query_between() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryBetweenTable", true).await;

    // Put 10 items
    let pk_value = AttributeValue::S("partition1".to_string());

    for i in 1..=10 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert(
            "sort".to_string(),
            AttributeValue::S(format!("item-{:02}", i)),
        );
        item.insert("data".to_string(), AttributeValue::N(i.to_string()));

        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Query for items with sort BETWEEN "item-03" AND "item-07"
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: Some(extenddb_core::expression::SortKeyCondition::Between {
            path: vec![extenddb_core::expression::PathElement::Attribute(
                "sort".to_string(),
            )],
            low: Expr::Placeholder(":low".to_string()),
            high: Expr::Placeholder(":high".to_string()),
        }),
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());
    maps.values
        .insert(":low".to_string(), AttributeValue::S("item-03".to_string()));
    maps.values.insert(
        ":high".to_string(),
        AttributeValue::S("item-07".to_string()),
    );

    let (items, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            None,
        )
        .await
        .expect("query should succeed");

    // Should get items 03, 04, 05, 06, 07 (5 items)
    println!("Got {} items with BETWEEN", items.len());
    assert_eq!(
        items.len(),
        5,
        "Should return 5 items between item-03 and item-07"
    );

    let sort_keys: Vec<String> = items
        .iter()
        .map(|item| {
            if let Some(AttributeValue::S(s)) = item.get("sort") {
                s.clone()
            } else {
                panic!("Expected string sort key")
            }
        })
        .collect();

    assert_eq!(
        sort_keys,
        vec!["item-03", "item-04", "item-05", "item-06", "item-07"]
    );

    println!("✓ Query BETWEEN test passed");
}

#[tokio::test]
async fn test_query_begins_with() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "QueryBeginsWithTable", true).await;

    // Put items with various prefixes
    let pk_value = AttributeValue::S("partition1".to_string());
    let items = vec!["apple", "apricot", "application", "banana", "berry", "cat"];

    for item_name in &items {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), pk_value.clone());
        item.insert("sort".to_string(), AttributeValue::S(item_name.to_string()));
        item.insert(
            "data".to_string(),
            AttributeValue::S(format!("Item {}", item_name)),
        );

        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("put_item should succeed");
    }

    // Query for items that begin with "app"
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":pk".to_string()),
        sk_condition: Some(extenddb_core::expression::SortKeyCondition::BeginsWith {
            path: vec![extenddb_core::expression::PathElement::Attribute(
                "sort".to_string(),
            )],
            prefix: Expr::Placeholder(":prefix".to_string()),
        }),
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(":pk".to_string(), pk_value.clone());
    maps.values
        .insert(":prefix".to_string(), AttributeValue::S("app".to_string()));

    let (items_result, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            None,
        )
        .await
        .expect("query should succeed");

    // Should get apple, application (both start with "app")
    println!("Got {} items with begins_with('app')", items_result.len());
    assert_eq!(
        items_result.len(),
        2,
        "Should return 2 items beginning with 'app'"
    );

    let sort_keys: Vec<String> = items_result
        .iter()
        .map(|item| {
            if let Some(AttributeValue::S(s)) = item.get("sort") {
                s.clone()
            } else {
                panic!("Expected string sort key")
            }
        })
        .collect();

    assert_eq!(sort_keys, vec!["apple", "application"]);

    println!("✓ Query begins_with test passed");
}

#[tokio::test]
async fn test_query_gsi() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "QueryGsiTable", "StatusIndex", "status").await;

    // Set GSI to synchronous (propagation_delay_ms = 0) for testing
    let catalog_keyspace = engine.catalog_keyspace();
    let update_delay = format!(
        "UPDATE {}.indexes SET propagation_delay_ms = 0 WHERE table_id = ? AND index_name = ?",
        catalog_keyspace
    );
    engine
        .session()
        .query_with_values(
            &update_delay,
            cdrs_tokio::query_values!(table.key_info.table_id.as_str(), "StatusIndex"),
        )
        .await
        .expect("Failed to set GSI to synchronous");

    println!("✓ Table created with synchronous GSI: StatusIndex");
    println!("  Table ID: {}", table.key_info.table_id);
    println!("  Account ID: {}", table.key_info.account_id);

    // Put items with different status values
    let items_data = vec![
        ("item1", "active"),
        ("item2", "pending"),
        ("item3", "active"),
        ("item4", "inactive"),
    ];

    let maps = ExpressionMaps::default();
    for (id, status) in items_data {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert("status".to_string(), AttributeValue::S(status.to_string()));
        item.insert(
            "data".to_string(),
            AttributeValue::S(format!("Data for {}", id)),
        );

        engine
            .put_item(&table.key_info, item, false, None, &maps, None)
            .await
            .expect("put_item should succeed");

        println!("✓ Put item: {} with status={}", id, status);
    }

    // Query GSI by status = "active"
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "status".to_string(),
        )],
        pk_value: Expr::Placeholder(":status".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(
        ":status".to_string(),
        AttributeValue::S("active".to_string()),
    );

    let (items, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            Some("StatusIndex"),
        )
        .await
        .expect("query GSI should succeed");

    assert_eq!(items.len(), 2, "Should return 2 items with status=active");

    let ids: Vec<String> = items
        .iter()
        .map(|item| {
            if let Some(AttributeValue::S(s)) = item.get("id") {
                s.clone()
            } else {
                panic!("Expected string id")
            }
        })
        .collect();

    assert!(ids.contains(&"item1".to_string()));
    assert!(ids.contains(&"item3".to_string()));

    println!("✓ Query GSI test passed");
}

#[tokio::test]
async fn test_query_gsi_pagination() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table =
        TestTable::with_gsi(&engine, "QueryGsiPaginationTable", "StatusIndex", "status").await;

    // Set GSI to synchronous for testing
    let catalog_keyspace = engine.catalog_keyspace();
    let update_delay = format!(
        "UPDATE {}.indexes SET propagation_delay_ms = 0 WHERE table_id = ? AND index_name = ?",
        catalog_keyspace
    );
    engine
        .session()
        .query_with_values(
            &update_delay,
            cdrs_tokio::query_values!(table.key_info.table_id.as_str(), "StatusIndex"),
        )
        .await
        .expect("Failed to set GSI to synchronous");

    println!("✓ Table created with synchronous GSI for pagination test");

    // Put 10 items all with status="active" to test pagination with same index PK
    // This forces the two-query pagination logic (base table keys as tie-breakers)
    let maps = ExpressionMaps::default();
    for i in 1..=10 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S(format!("item{:02}", i)));
        item.insert(
            "status".to_string(),
            AttributeValue::S("active".to_string()),
        );
        item.insert("data".to_string(), AttributeValue::N(i.to_string()));

        engine
            .put_item(&table.key_info, item, false, None, &maps, None)
            .await
            .expect("put_item should succeed");
    }

    println!("✓ Put 10 items all with status=active");

    // Query GSI with pagination
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "status".to_string(),
        )],
        pk_value: Expr::Placeholder(":status".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(
        ":status".to_string(),
        AttributeValue::S("active".to_string()),
    );

    // Page 1: Get 3 items
    let (page1, last_key1) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            None,
            Some("StatusIndex"),
        )
        .await
        .expect("first GSI query should succeed");

    assert_eq!(page1.len(), 3, "Page 1 should have 3 items");
    assert!(last_key1.is_some(), "Should have LastEvaluatedKey");
    println!("✓ Page 1: {} items", page1.len());

    // Page 2: Continue pagination
    let (page2, last_key2) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            last_key1.as_ref(),
            Some("StatusIndex"),
        )
        .await
        .expect("second GSI query should succeed");

    assert_eq!(page2.len(), 3, "Page 2 should have 3 items");
    assert!(last_key2.is_some(), "Should have LastEvaluatedKey");
    println!("✓ Page 2: {} items", page2.len());

    // Page 3: Continue pagination
    let (page3, last_key3) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            last_key2.as_ref(),
            Some("StatusIndex"),
        )
        .await
        .expect("third GSI query should succeed");

    assert_eq!(page3.len(), 3, "Page 3 should have 3 items");
    assert!(last_key3.is_some(), "Should have LastEvaluatedKey");
    println!("✓ Page 3: {} items", page3.len());

    // Page 4: Get remaining item
    let (page4, last_key4) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            Some(3),
            last_key3.as_ref(),
            Some("StatusIndex"),
        )
        .await
        .expect("fourth GSI query should succeed");

    assert_eq!(page4.len(), 1, "Page 4 should have 1 remaining item");
    assert!(last_key4.is_none(), "Should not have more pages");
    println!("✓ Page 4: {} items (final page)", page4.len());

    // Verify all items are unique (no duplicates from pagination)
    let mut all_ids: Vec<String> = Vec::new();
    for items in &[&page1, &page2, &page3, &page4] {
        for item in items.iter() {
            if let Some(AttributeValue::S(id)) = item.get("id") {
                all_ids.push(id.clone());
            }
        }
    }

    all_ids.sort();
    let unique_count = all_ids
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(
        unique_count, 10,
        "Should have 10 unique items across all pages"
    );
    assert_eq!(all_ids.len(), 10, "Should have exactly 10 items total");

    println!("✓ GSI pagination test passed - all 10 items retrieved without duplicates");
}

#[tokio::test]
async fn test_query_lsi() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_lsi(&engine, "QueryLsiTable", "LsiPriority", "priority").await;

    println!("✓ Table created with LSI: LsiPriority");

    // Put items with same id but different sort keys and priorities
    let items_data = vec![
        ("user1", 1000, 3), // sort=1000, priority=3
        ("user1", 2000, 1), // sort=2000, priority=1 (highest priority)
        ("user1", 3000, 5), // sort=3000, priority=5 (lowest priority)
        ("user1", 4000, 2), // sort=4000, priority=2
    ];

    let maps = ExpressionMaps::default();
    for (id, sort, priority) in items_data {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert("sort".to_string(), AttributeValue::N(sort.to_string()));
        item.insert(
            "priority".to_string(),
            AttributeValue::N(priority.to_string()),
        );

        engine
            .put_item(&table.key_info, item, false, None, &maps, None)
            .await
            .expect("put_item should succeed");
    }

    println!("✓ Put 4 items with different priorities");

    // Query LSI by id, ordered by priority
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "id".to_string(),
        )],
        pk_value: Expr::Placeholder(":id".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values
        .insert(":id".to_string(), AttributeValue::S("user1".to_string()));

    let (items, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            Some("LsiPriority"),
        )
        .await
        .expect("query LSI should succeed");

    assert_eq!(items.len(), 4, "Should return 4 items");

    // Verify items are ordered by priority
    let priorities: Vec<i64> = items
        .iter()
        .map(|item| {
            if let Some(AttributeValue::N(n)) = item.get("priority") {
                n.parse().unwrap()
            } else {
                panic!("Expected numeric priority")
            }
        })
        .collect();

    assert_eq!(
        priorities,
        vec![1, 2, 3, 5],
        "Should be ordered by priority ASC"
    );

    println!("✓ LSI query test passed - items ordered by priority");
}

#[tokio::test]
async fn test_query_gsi_with_sort_key() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi_with_sk(
        &engine,
        "QueryGsiSkTable",
        "CategoryTimeIndex",
        "category",
        "created_at",
    )
    .await;

    // Set GSI to synchronous
    let catalog_keyspace = engine.catalog_keyspace();
    let update_delay = format!(
        "UPDATE {}.indexes SET propagation_delay_ms = 0 WHERE table_id = ? AND index_name = ?",
        catalog_keyspace
    );
    engine
        .session()
        .query_with_values(
            &update_delay,
            cdrs_tokio::query_values!(table.key_info.table_id.as_str(), "CategoryTimeIndex"),
        )
        .await
        .expect("Failed to set GSI to synchronous");

    println!("✓ Table created with GSI with sort key: CategoryTimeIndex");

    // Put items with same category but different timestamps
    let items_data = vec![
        ("item1", "books", 1000),
        ("item2", "books", 2000),
        ("item3", "books", 3000),
        ("item4", "books", 4000),
        ("item5", "electronics", 1500),
    ];

    let maps = ExpressionMaps::default();
    for (id, category, timestamp) in items_data {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert(
            "category".to_string(),
            AttributeValue::S(category.to_string()),
        );
        item.insert(
            "created_at".to_string(),
            AttributeValue::N(timestamp.to_string()),
        );

        engine
            .put_item(&table.key_info, item, false, None, &maps, None)
            .await
            .expect("put_item should succeed");
    }

    println!("✓ Put 5 items with different categories and timestamps");

    // Query GSI without SK condition first to see if items are in the index
    let key_condition_simple = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "category".to_string(),
        )],
        pk_value: Expr::Placeholder(":category".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps_simple = ExpressionMaps::default();
    maps_simple.values.insert(
        ":category".to_string(),
        AttributeValue::S("books".to_string()),
    );

    let (items_simple, _) = engine
        .query(
            &table.key_info,
            &key_condition_simple,
            &maps_simple,
            true,
            None,
            None,
            Some("CategoryTimeIndex"),
        )
        .await
        .expect("simple GSI query should succeed");

    println!("✓ Simple query returned {} items", items_simple.len());
    assert_eq!(
        items_simple.len(),
        4,
        "Should have 4 items with category=books"
    );

    // Query GSI: category = "books" AND created_at >= 2000
    let key_condition = KeyCondition {
        pk_path: vec![extenddb_core::expression::PathElement::Attribute(
            "category".to_string(),
        )],
        pk_value: Expr::Placeholder(":category".to_string()),
        sk_condition: Some(extenddb_core::expression::SortKeyCondition::Compare {
            path: vec![extenddb_core::expression::PathElement::Attribute(
                "created_at".to_string(),
            )],
            op: CompareOp::Ge,
            value: Expr::Placeholder(":min_time".to_string()),
        }),
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };

    let mut maps = ExpressionMaps::default();
    maps.values.insert(
        ":category".to_string(),
        AttributeValue::S("books".to_string()),
    );
    maps.values.insert(
        ":min_time".to_string(),
        AttributeValue::N("2000".to_string()),
    );

    let (items, _) = engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            Some("CategoryTimeIndex"),
        )
        .await
        .expect("query GSI with SK condition should succeed");

    assert_eq!(items.len(), 3, "Should return 3 items (2000, 3000, 4000)");

    // Verify items are in the correct range
    let timestamps: Vec<i64> = items
        .iter()
        .map(|item| {
            if let Some(AttributeValue::N(n)) = item.get("created_at") {
                n.parse().unwrap()
            } else {
                panic!("Expected numeric created_at")
            }
        })
        .collect();

    assert_eq!(
        timestamps,
        vec![2000, 3000, 4000],
        "Should have timestamps >= 2000 in order"
    );

    println!("✓ GSI with sort key range query test passed");
}
