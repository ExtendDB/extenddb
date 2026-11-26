// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the read of a SELECT's disjuncts one after another.

use serde_json::json;

use super::*;
use crate::partiql::PartiqlExecutor;
use crate::partiql::test_context::{block_on, catalog_context};
use crate::{handle_create_table, handle_put_item};

/// Two walks of a SELECT that names whole items of one partition with `Limit`,
/// and the pages the service gives, written as [`walk`] writes them.
/// Partition or-page-missing holds row-1 to row-4, so the key row-0 has no
/// item and takes no unit (measured 2026-11-14). Partition or-page-filter
/// holds row-1, and row-2 with v = 1, so the item the filter drops takes a
/// unit (measured 2026-11-18).
const WALKS: [(&str, &str, i64, &str); 2] = [
    (
        "or-page-missing",
        "(sk = 'row-0' OR sk = 'row-2')",
        1,
        "[row-2] 1.0",
    ),
    (
        "or-page-filter",
        "(sk = 'row-1' OR sk = 'row-2') AND v = 1",
        1,
        "[] 0.5 next | [row-2] 0.5",
    ),
];

#[test]
fn only_a_key_that_has_an_item_takes_a_unit_of_the_limit() {
    block_on(async {
        let ctx = catalog_context().await;
        let attribute = |name: &str| json!({ "AttributeName": name, "AttributeType": "S" });
        let table = json!({
            "TableName": "pages",
            "KeySchema": [
                { "AttributeName": "pk", "KeyType": "HASH" },
                { "AttributeName": "sk", "KeyType": "RANGE" },
            ],
            "AttributeDefinitions": [attribute("pk"), attribute("sk")],
            "BillingMode": "PAY_PER_REQUEST",
        });
        handle_create_table(table, &ctx).await.unwrap();
        let missing = ["row-1", "row-2", "row-3", "row-4"]
            .map(|sk| json!({ "pk": { "S": "or-page-missing" }, "sk": { "S": sk } }));
        let filtered = [
            json!({ "pk": { "S": "or-page-filter" }, "sk": { "S": "row-1" } }),
            json!({ "pk": { "S": "or-page-filter" }, "sk": { "S": "row-2" }, "v": { "N": "1" } }),
        ];
        for item in missing.into_iter().chain(filtered) {
            let put = json!({ "TableName": "pages", "Item": item });
            let _ = handle_put_item(put, &ctx).await.unwrap();
        }
        for (partition, condition, limit, pages) in WALKS {
            let statement =
                format!("SELECT * FROM \"pages\" WHERE pk = '{partition}' AND {condition}");
            assert_eq!(walk(&ctx, &statement, limit).await, pages, "{statement}");
        }
    });
}

/// The pages of `statement` with `limit`, following NextToken: each written
/// as the sort keys of its items and the capacity units it reports, then
/// `next` when it has a NextToken.
async fn walk(ctx: &OperationContext, statement: &str, limit: i64) -> String {
    let mut pages = Vec::new();
    let mut token = None;
    while pages.len() < 10 {
        let mut body = json!({
            "Statement": statement, "Limit": limit, "ReturnConsumedCapacity": "TOTAL"
        });
        if let Some(token) = token.take() {
            body["NextToken"] = Value::String(token);
        }
        let page = ctx.execute_statement(body).await.unwrap().body;
        let items = page["Items"].as_array().unwrap();
        let keys: Vec<&str> = items.iter().filter_map(|i| i["sk"]["S"].as_str()).collect();
        let units = &page["ConsumedCapacity"]["CapacityUnits"];
        token = page
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let next = if token.is_some() { " next" } else { "" };
        pages.push(format!("[{}] {units}{next}", keys.join(" ")));
        if token.is_none() {
            break;
        }
    }
    pages.join(" | ")
}

#[test]
fn a_full_page_names_the_key_of_its_last_item() {
    let item = |pk: &str, sk: &str| serde_json::json!({ "pk": { "S": pk }, "sk": { "S": sk }, "n": { "N": "1" } });
    let key = |pk: &str, sk: &str| serde_json::json!({ "pk": { "S": pk }, "sk": { "S": sk } });
    let two = [item("a", "x"), item("b", "x")];
    let keys = ["pk", "sk"];
    assert_eq!(page_key(&two, &keys, Some(2)), Some(key("b", "x")));
    assert_eq!(page_key(&two[..1], &keys, Some(1)), Some(key("a", "x")));
    // A page with fewer items than the limit, an empty one, and one read
    // without a limit have none.
    assert_eq!(page_key(&two[..1], &keys, Some(2)), None);
    assert_eq!(page_key(&[], &keys, Some(1)), None);
    assert_eq!(page_key(&two, &keys, None), None);
    // A table keyed on the partition key alone.
    assert_eq!(
        page_key(&two[..1], &["pk"], Some(1)),
        Some(serde_json::json!({ "pk": { "S": "a" } }))
    );
    // An index keyed on n, read with the table's key after its own.
    assert_eq!(
        page_key(&two[..1], &["n", "pk", "sk"], Some(1)),
        Some(serde_json::json!({ "n": { "N": "1" }, "pk": { "S": "a" }, "sk": { "S": "x" } }))
    );
}

#[test]
fn a_page_takes_items_up_to_the_one_that_passes_its_budget() {
    let item = 380 * 1024;
    // Two items read, the next one passes 1 MB and is kept.
    assert_eq!(items_to_budget(2 * item, &[item, item]), 1);
    // One item read, the second of the next two passes 1 MB.
    assert_eq!(items_to_budget(item, &[item, item, item]), 2);
    // Exactly 1 MB does not pass it.
    assert_eq!(items_to_budget(PAGE_BYTES - 10, &[10, 1]), 2);
    // A page that stays within its budget takes them all.
    assert_eq!(items_to_budget(10, &[10, 10]), 2);
    assert_eq!(items_to_budget(10, &[]), 0);
}

#[test]
fn a_wire_item_is_sized_as_a_page_counts_it() {
    let item = serde_json::json!({
        "pk": { "S": "a" }, "n": { "N": "12" }, "b": { "B": "AAEC" }, "t": { "BOOL": true }
    });
    // pk 2 + 1, n 1 + 2, b 1 + 3 bytes, t 1 + 1.
    assert_eq!(wire_item_size(&item), 12);
    assert_eq!(wire_item_size(&serde_json::json!("not an item")), 0);
}

#[test]
fn consumed_capacity_adds_up_number_by_number() {
    let mut total = None;
    let read = serde_json::json!({
        "TableName": "t", "CapacityUnits": 0.5, "Table": { "CapacityUnits": 0.5 }
    });
    add_capacity(&mut total, Some(read.clone()));
    add_capacity(&mut total, None);
    add_capacity(&mut total, Some(read));
    assert_eq!(
        total,
        Some(serde_json::json!({
            "TableName": "t", "CapacityUnits": 1.0, "Table": { "CapacityUnits": 1.0 }
        }))
    );
}
